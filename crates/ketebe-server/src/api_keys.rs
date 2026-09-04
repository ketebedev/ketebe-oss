use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::{AuthenticationError, Credential, CredentialAuthenticator, Principal};

const STORE_VERSION: u32 = 1;
const KEY_PREFIX: &str = "ktb_";
const KEY_ID_BYTES: usize = 12;
const SECRET_BYTES: usize = 32;
const SALT_BYTES: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ApiKeyId(String);

impl ApiKeyId {
    fn generate() -> Result<Self, ApiKeyError> {
        let mut bytes = [0_u8; KEY_ID_BYTES];
        getrandom::fill(&mut bytes).map_err(|_| ApiKeyError::EntropyUnavailable)?;
        Ok(Self(URL_SAFE_NO_PAD.encode(bytes)))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyMetadata {
    pub id: ApiKeyId,
    pub project_id: String,
    pub created_at_unix: u64,
    pub updated_at_unix: u64,
    pub expires_at_unix: Option<u64>,
    pub revoked_at_unix: Option<u64>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct IssuedApiKey {
    pub metadata: ApiKeyMetadata,
    pub credential: Credential,
}

impl fmt::Debug for IssuedApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IssuedApiKey")
            .field("metadata", &self.metadata)
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug)]
pub enum ApiKeyError {
    Io(std::io::Error),
    Json(serde_json::Error),
    UnsupportedVersion(u32),
    InvalidProject,
    NotFound,
    Revoked,
    Expired,
    EntropyUnavailable,
    LockPoisoned,
}

impl fmt::Display for ApiKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "API key store I/O error: {error}"),
            Self::Json(error) => write!(f, "API key store JSON error: {error}"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported API key store version {version}")
            }
            Self::InvalidProject => f.write_str("project id must not be empty"),
            Self::NotFound => f.write_str("API key not found"),
            Self::Revoked => f.write_str("API key is revoked"),
            Self::Expired => f.write_str("API key is expired"),
            Self::EntropyUnavailable => f.write_str("secure random source is unavailable"),
            Self::LockPoisoned => f.write_str("API key store lock poisoned"),
        }
    }
}

impl std::error::Error for ApiKeyError {}

impl From<std::io::Error> for ApiKeyError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<serde_json::Error> for ApiKeyError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredKey {
    metadata: ApiKeyMetadata,
    salt: String,
    verifier: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoreFile {
    version: u32,
    keys: BTreeMap<ApiKeyId, StoredKey>,
}

impl Default for StoreFile {
    fn default() -> Self {
        Self {
            version: STORE_VERSION,
            keys: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct ApiKeyStore {
    path: Arc<PathBuf>,
    state: Arc<Mutex<StoreFile>>,
    audit: Arc<crate::AuditService>,
}

impl fmt::Debug for ApiKeyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiKeyStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl ApiKeyStore {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, ApiKeyError> {
        let path = data_dir.as_ref().join("security").join("api-keys.json");
        let state = if path.exists() {
            let decoded: StoreFile = serde_json::from_slice(&fs::read(&path)?)?;
            if decoded.version != STORE_VERSION {
                return Err(ApiKeyError::UnsupportedVersion(decoded.version));
            }
            decoded
        } else {
            StoreFile::default()
        };
        Ok(Self {
            path: Arc::new(path),
            state: Arc::new(Mutex::new(state)),
            audit: Arc::new(crate::AuditService::noop()),
        })
    }

    #[must_use]
    pub fn with_audit(mut self, audit: crate::AuditService) -> Self {
        self.audit = Arc::new(audit);
        self
    }

    fn audit_lifecycle(&self, action: &str, metadata: &ApiKeyMetadata) {
        let event = crate::AuditEvent::new(
            crate::AuditCategory::Authentication,
            action,
            crate::AuditResult::Allowed,
            crate::AuditOrigin::Internal,
        )
        .with_project(&metadata.project_id)
        .with_resource("api_key", metadata.id.as_str());
        let _ = self.audit.record(&event);
    }

    pub fn create(
        &self,
        project_id: impl Into<String>,
        expires_at_unix: Option<u64>,
    ) -> Result<IssuedApiKey, ApiKeyError> {
        let project_id = normalize_project(project_id.into())?;
        let now = unix_now();
        let id = ApiKeyId::generate()?;
        let (credential, salt, verifier) = generate_credential(&id)?;
        let metadata = ApiKeyMetadata {
            id: id.clone(),
            project_id,
            created_at_unix: now,
            updated_at_unix: now,
            expires_at_unix,
            revoked_at_unix: None,
        };
        let mut state = self.state.lock().map_err(|_| ApiKeyError::LockPoisoned)?;
        state.keys.insert(
            id,
            StoredKey {
                metadata: metadata.clone(),
                salt,
                verifier,
            },
        );
        persist(&self.path, &state)?;
        drop(state);
        self.audit_lifecycle("api_key_create", &metadata);
        Ok(IssuedApiKey {
            metadata,
            credential,
        })
    }

    pub fn revoke(&self, id: &ApiKeyId) -> Result<ApiKeyMetadata, ApiKeyError> {
        let now = unix_now();
        let mut state = self.state.lock().map_err(|_| ApiKeyError::LockPoisoned)?;
        let key = state.keys.get_mut(id).ok_or(ApiKeyError::NotFound)?;
        if key.metadata.revoked_at_unix.is_none() {
            key.metadata.revoked_at_unix = Some(now);
            key.metadata.updated_at_unix = now;
        }
        let metadata = key.metadata.clone();
        persist(&self.path, &state)?;
        drop(state);
        self.audit_lifecycle("api_key_revoke", &metadata);
        Ok(metadata)
    }

    pub fn rotate(&self, id: &ApiKeyId) -> Result<IssuedApiKey, ApiKeyError> {
        let now = unix_now();
        let mut state = self.state.lock().map_err(|_| ApiKeyError::LockPoisoned)?;
        let key = state.keys.get_mut(id).ok_or(ApiKeyError::NotFound)?;
        validate_active(&key.metadata, now)?;
        let (credential, salt, verifier) = generate_credential(id)?;
        key.salt = salt;
        key.verifier = verifier;
        key.metadata.updated_at_unix = now;
        let metadata = key.metadata.clone();
        persist(&self.path, &state)?;
        drop(state);
        self.audit_lifecycle("api_key_rotate", &metadata);
        Ok(IssuedApiKey {
            metadata,
            credential,
        })
    }

    pub fn metadata(&self, id: &ApiKeyId) -> Result<ApiKeyMetadata, ApiKeyError> {
        let state = self.state.lock().map_err(|_| ApiKeyError::LockPoisoned)?;
        state
            .keys
            .get(id)
            .map(|key| key.metadata.clone())
            .ok_or(ApiKeyError::NotFound)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    fn authenticate_token(&self, token: &str) -> Result<Principal, AuthenticationError> {
        let (id, secret) = parse_token(token).ok_or(AuthenticationError::InvalidCredential)?;
        let state = self
            .state
            .lock()
            .map_err(|_| AuthenticationError::InvalidCredential)?;
        let key = state
            .keys
            .get(&id)
            .ok_or(AuthenticationError::InvalidCredential)?;
        validate_active(&key.metadata, unix_now())
            .map_err(|_| AuthenticationError::InvalidCredential)?;
        let salt = URL_SAFE_NO_PAD
            .decode(&key.salt)
            .map_err(|_| AuthenticationError::InvalidCredential)?;
        let expected = URL_SAFE_NO_PAD
            .decode(&key.verifier)
            .map_err(|_| AuthenticationError::InvalidCredential)?;
        let actual = digest(&salt, secret.as_bytes());
        if expected.len() != actual.len() || expected.ct_eq(actual.as_slice()).unwrap_u8() != 1 {
            return Err(AuthenticationError::InvalidCredential);
        }
        Principal::for_project(
            format!("api-key:{}", key.metadata.id.as_str()),
            key.metadata.project_id.clone(),
        )
    }
}

impl CredentialAuthenticator for ApiKeyStore {
    fn authenticate(&self, credential: &Credential) -> Result<Principal, AuthenticationError> {
        self.authenticate_token(credential.expose_secret())
    }
}

fn normalize_project(project_id: String) -> Result<String, ApiKeyError> {
    let project_id = project_id.trim().to_string();
    if project_id.is_empty() {
        Err(ApiKeyError::InvalidProject)
    } else {
        Ok(project_id)
    }
}

fn validate_active(metadata: &ApiKeyMetadata, now: u64) -> Result<(), ApiKeyError> {
    if metadata.revoked_at_unix.is_some() {
        return Err(ApiKeyError::Revoked);
    }
    if metadata
        .expires_at_unix
        .is_some_and(|expires| expires <= now)
    {
        return Err(ApiKeyError::Expired);
    }
    Ok(())
}

fn generate_credential(id: &ApiKeyId) -> Result<(Credential, String, String), ApiKeyError> {
    let mut secret = [0_u8; SECRET_BYTES];
    let mut salt = [0_u8; SALT_BYTES];
    getrandom::fill(&mut secret).map_err(|_| ApiKeyError::EntropyUnavailable)?;
    getrandom::fill(&mut salt).map_err(|_| ApiKeyError::EntropyUnavailable)?;
    let secret = URL_SAFE_NO_PAD.encode(secret);
    let token = format!("{KEY_PREFIX}{}.{}", id.as_str(), secret);
    let verifier = URL_SAFE_NO_PAD.encode(digest(&salt, secret.as_bytes()));
    Ok((
        Credential::new(token).map_err(|_| ApiKeyError::EntropyUnavailable)?,
        URL_SAFE_NO_PAD.encode(salt),
        verifier,
    ))
}

fn parse_token(token: &str) -> Option<(ApiKeyId, &str)> {
    let token = token.strip_prefix(KEY_PREFIX)?;
    let (id, secret) = token.split_once('.')?;
    if id.is_empty() || secret.is_empty() {
        return None;
    }
    Some((ApiKeyId(id.to_string()), secret))
}

fn digest(salt: &[u8], secret: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(salt);
    hasher.update(secret);
    hasher.finalize().to_vec()
}

fn persist(path: &Path, state: &StoreFile) -> Result<(), ApiKeyError> {
    let parent = path.parent().expect("API key path has parent");
    fs::create_dir_all(parent)?;
    let tmp = path.with_extension("json.tmp");
    let encoded = serde_json::to_vec_pretty(state)?;
    fs::write(&tmp, encoded)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
    }
    fs::rename(tmp, path)?;
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AuthenticationService;

    fn temp_dir(name: &str) -> PathBuf {
        let mut random = [0_u8; 8];
        getrandom::fill(&mut random).unwrap();
        std::env::temp_dir().join(format!(
            "ketebe-api-key-{name}-{}",
            URL_SAFE_NO_PAD.encode(random)
        ))
    }

    #[test]
    fn lifecycle_audit_contains_identity_but_never_raw_secret() {
        let dir = temp_dir("audit");
        let audit = crate::AuditService::durable(&dir).unwrap();
        let store = ApiKeyStore::open(&dir).unwrap().with_audit(audit);
        let issued = store.create("project-a", None).unwrap();
        let raw = issued.credential.expose_secret().to_string();
        store.rotate(&issued.metadata.id).unwrap();
        store.revoke(&issued.metadata.id).unwrap();
        let text = fs::read_to_string(dir.join("security/audit.jsonl")).unwrap();
        assert!(text.contains("api_key_create"));
        assert!(text.contains("api_key_rotate"));
        assert!(text.contains("api_key_revoke"));
        assert!(text.contains("project-a"));
        assert!(text.contains(issued.metadata.id.as_str()));
        assert!(!text.contains(&raw));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn create_persists_only_one_way_verifier_and_authenticates_project() {
        let dir = temp_dir("create");
        let store = ApiKeyStore::open(&dir).unwrap();
        let issued = store.create("project-a", None).unwrap();
        let raw = issued.credential.expose_secret().to_string();
        let persisted = fs::read_to_string(store.path()).unwrap();
        assert!(!persisted.contains(&raw));
        let auth = AuthenticationService::required(Arc::new(store.clone()));
        let principal = auth
            .authenticate_authorization_value(Some(&format!("Bearer {raw}")))
            .unwrap();
        assert_eq!(principal.project_id(), Some("project-a"));
        assert_eq!(
            principal.subject(),
            format!("api-key:{}", issued.metadata.id.as_str())
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn restart_preserves_key_and_revocation_is_immediate_and_persistent() {
        let dir = temp_dir("restart");
        let store = ApiKeyStore::open(&dir).unwrap();
        let issued = store.create("project-b", None).unwrap();
        let raw = issued.credential.expose_secret().to_string();
        drop(store);

        let reopened = ApiKeyStore::open(&dir).unwrap();
        assert!(reopened.authenticate_token(&raw).is_ok());
        reopened.revoke(&issued.metadata.id).unwrap();
        assert!(reopened.authenticate_token(&raw).is_err());
        drop(reopened);

        let reopened = ApiKeyStore::open(&dir).unwrap();
        assert!(reopened.authenticate_token(&raw).is_err());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn rotation_invalidates_old_secret_and_does_not_revive_revoked_key() {
        let dir = temp_dir("rotate");
        let store = ApiKeyStore::open(&dir).unwrap();
        let issued = store.create("project-c", None).unwrap();
        let old = issued.credential.expose_secret().to_string();
        let rotated = store.rotate(&issued.metadata.id).unwrap();
        let new = rotated.credential.expose_secret().to_string();
        assert_ne!(old, new);
        assert!(store.authenticate_token(&old).is_err());
        assert!(store.authenticate_token(&new).is_ok());
        store.revoke(&issued.metadata.id).unwrap();
        assert!(matches!(
            store.rotate(&issued.metadata.id),
            Err(ApiKeyError::Revoked)
        ));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn expired_keys_are_rejected() {
        let dir = temp_dir("expire");
        let store = ApiKeyStore::open(&dir).unwrap();
        let issued = store.create("project-d", Some(unix_now())).unwrap();
        assert!(
            store
                .authenticate_token(issued.credential.expose_secret())
                .is_err()
        );
        let _ = fs::remove_dir_all(dir);
    }
}
