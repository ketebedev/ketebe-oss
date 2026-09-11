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

use ketebe_core::{Organization, Project, ProjectId, ResourceLifecycleState};
use ketebe_storage::ControlPlaneStore;

use crate::{
    AuthenticationError, Credential, CredentialAuthenticator, Principal, ServiceAccountId,
    ServiceAccountStore,
};

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_id: Option<String>,
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
    ProjectNotFound,
    ProjectUnavailable,
    ControlPlane(String),
    ServiceAccount(String),
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
            Self::InvalidProject => f.write_str("invalid project id"),
            Self::ProjectNotFound => f.write_str("project not found"),
            Self::ProjectUnavailable => f.write_str("project is not active"),
            Self::ControlPlane(message) => write!(f, "control-plane resolution error: {message}"),
            Self::ServiceAccount(message) => {
                write!(f, "service account resolution error: {message}")
            }
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
    control_plane_path: Arc<PathBuf>,
    data_dir: Arc<PathBuf>,
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
        let control_plane_path = data_dir.as_ref().join("control-plane");
        ControlPlaneStore::open(&control_plane_path)
            .map_err(|error| ApiKeyError::ControlPlane(error.to_string()))?;
        Ok(Self {
            path: Arc::new(path),
            state: Arc::new(Mutex::new(state)),
            control_plane_path: Arc::new(control_plane_path),
            data_dir: Arc::new(data_dir.as_ref().to_path_buf()),
            audit: Arc::new(crate::AuditService::noop()),
        })
    }

    #[must_use]
    pub fn with_audit(mut self, audit: crate::AuditService) -> Self {
        self.audit = Arc::new(audit);
        self
    }

    fn audit_lifecycle(&self, action: &str, metadata: &ApiKeyMetadata) {
        let mut event = crate::AuditEvent::new(
            crate::AuditCategory::Authentication,
            action,
            crate::AuditResult::Allowed,
            crate::AuditOrigin::Internal,
        )
        .with_project(&metadata.project_id)
        .with_resource("api_key", metadata.id.as_str());
        if let Some(service_account_id) = metadata.service_account_id.as_deref() {
            event = event.with_actor(format!("service-account:{service_account_id}"));
        }
        if let Ok((_, organization)) = self.resolve_project(&metadata.project_id) {
            event = event.with_organization(organization.id().as_str());
        }
        let _ = self.audit.record(&event);
    }

    pub fn create(
        &self,
        project_id: impl Into<String>,
        expires_at_unix: Option<u64>,
    ) -> Result<IssuedApiKey, ApiKeyError> {
        let project_id = normalize_project(project_id.into())?;
        self.resolve_active_project(&project_id)?;
        let now = unix_now();
        let id = ApiKeyId::generate()?;
        let (credential, salt, verifier) = generate_credential(&id)?;
        let metadata = ApiKeyMetadata {
            id: id.clone(),
            project_id,
            service_account_id: None,
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

    pub fn create_for_service_account(
        &self,
        service_account_id: &ServiceAccountId,
        project_id: impl Into<String>,
        expires_at_unix: Option<u64>,
    ) -> Result<IssuedApiKey, ApiKeyError> {
        let project_id = normalize_project(project_id.into())?;
        self.resolve_active_project(&project_id)?;
        let project =
            ProjectId::new(project_id.clone()).map_err(|_| ApiKeyError::InvalidProject)?;
        let service_accounts = ServiceAccountStore::open(self.data_dir.as_path())
            .map_err(|error| ApiKeyError::ServiceAccount(error.to_string()))?;
        service_accounts
            .authorize_project(service_account_id, &project)
            .map_err(|error| ApiKeyError::ServiceAccount(error.to_string()))?;

        let now = unix_now();
        let id = ApiKeyId::generate()?;
        let (credential, salt, verifier) = generate_credential(&id)?;
        let metadata = ApiKeyMetadata {
            id: id.clone(),
            project_id,
            service_account_id: Some(service_account_id.as_str().to_string()),
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
        self.resolve_active_project(&key.metadata.project_id)?;
        self.resolve_service_account_role(&key.metadata)?;
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

    pub fn project_ids(&self) -> Result<Vec<String>, ApiKeyError> {
        let state = self.state.lock().map_err(|_| ApiKeyError::LockPoisoned)?;
        let mut project_ids = state
            .keys
            .values()
            .map(|key| key.metadata.project_id.clone())
            .collect::<Vec<_>>();
        project_ids.sort();
        project_ids.dedup();
        Ok(project_ids)
    }

    pub fn validate_project_bindings(&self) -> Result<usize, ApiKeyError> {
        let project_ids = self.project_ids()?;
        for project_id in &project_ids {
            self.resolve_project(project_id)?;
        }
        Ok(project_ids.len())
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    fn resolve_project(&self, project_id: &str) -> Result<(Project, Organization), ApiKeyError> {
        let project_id =
            ProjectId::new(project_id.to_string()).map_err(|_| ApiKeyError::InvalidProject)?;
        let control_plane = ControlPlaneStore::open(self.control_plane_path.as_path())
            .map_err(|error| ApiKeyError::ControlPlane(error.to_string()))?;
        let project = control_plane
            .project(&project_id)
            .map_err(|error| ApiKeyError::ControlPlane(error.to_string()))?
            .ok_or(ApiKeyError::ProjectNotFound)?;
        let organization = control_plane
            .resolve_project_organization(&project_id)
            .map_err(|error| ApiKeyError::ControlPlane(error.to_string()))?;
        Ok((project, organization))
    }

    fn resolve_active_project(
        &self,
        project_id: &str,
    ) -> Result<(Project, Organization), ApiKeyError> {
        let (project, organization) = self.resolve_project(project_id)?;
        if project.lifecycle() != ResourceLifecycleState::Active {
            return Err(ApiKeyError::ProjectUnavailable);
        }
        Ok((project, organization))
    }

    fn resolve_service_account_role(
        &self,
        metadata: &ApiKeyMetadata,
    ) -> Result<Option<crate::ProjectRole>, ApiKeyError> {
        let Some(service_account_id) = metadata.service_account_id.as_deref() else {
            return Ok(None);
        };
        let service_account_id = ServiceAccountId::new(service_account_id.to_string())
            .map_err(|error| ApiKeyError::ServiceAccount(error.to_string()))?;
        let project_id =
            ProjectId::new(metadata.project_id.clone()).map_err(|_| ApiKeyError::InvalidProject)?;
        let service_accounts = ServiceAccountStore::open(self.data_dir.as_path())
            .map_err(|error| ApiKeyError::ServiceAccount(error.to_string()))?;
        let role = service_accounts
            .authorize_project(&service_account_id, &project_id)
            .map_err(|error| ApiKeyError::ServiceAccount(error.to_string()))?;
        Ok(Some(role))
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
        self.resolve_active_project(&key.metadata.project_id)
            .map_err(|_| AuthenticationError::InvalidCredential)?;
        let service_account_role = self
            .resolve_service_account_role(&key.metadata)
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
        if let Some(role) = service_account_role {
            Principal::for_workload_project_with_role(
                format!(
                    "service-account:{}",
                    key.metadata
                        .service_account_id
                        .as_deref()
                        .expect("role requires service account id")
                ),
                key.metadata.project_id.clone(),
                role,
            )
        } else {
            Principal::for_workload_project(
                format!("api-key:{}", key.metadata.id.as_str()),
                key.metadata.project_id.clone(),
            )
        }
    }
}

impl CredentialAuthenticator for ApiKeyStore {
    fn authenticate(&self, credential: &Credential) -> Result<Principal, AuthenticationError> {
        self.authenticate_token(credential.expose_secret())
    }
}

fn normalize_project(project_id: String) -> Result<String, ApiKeyError> {
    let project_id = project_id.trim().to_string();
    ProjectId::new(project_id.clone()).map_err(|_| ApiKeyError::InvalidProject)?;
    Ok(project_id)
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
    use crate::{
        AuthenticationService, AuthorizationAction, AuthorizationError, AuthorizationService,
        ServiceAccountLifecycle, ServiceAccountStore,
    };
    use ketebe_core::{OrganizationId, ResourceTimestamps};

    fn temp_dir(name: &str) -> PathBuf {
        let mut random = [0_u8; 8];
        getrandom::fill(&mut random).unwrap();
        std::env::temp_dir().join(format!(
            "ketebe-api-key-{name}-{}",
            URL_SAFE_NO_PAD.encode(random)
        ))
    }

    fn provision_project(dir: &Path, project_id: &str, lifecycle: ResourceLifecycleState) {
        let control_plane = ControlPlaneStore::open(dir.join("control-plane")).unwrap();
        let organization_id = OrganizationId::new("org-a").unwrap();
        if control_plane
            .organization(&organization_id)
            .unwrap()
            .is_none()
        {
            control_plane
                .create_organization(
                    Organization::new(
                        organization_id.clone(),
                        "ACME",
                        "acme",
                        ResourceLifecycleState::Active,
                        ResourceTimestamps::new(10, 10).unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let project_id = ProjectId::new(project_id).unwrap();
        let project = Project::new(
            project_id.clone(),
            organization_id,
            project_id.as_str(),
            project_id.as_str(),
            lifecycle,
            ResourceTimestamps::new(10, 20).unwrap(),
        )
        .unwrap();
        if control_plane.project(&project_id).unwrap().is_some() {
            control_plane.update_project(project).unwrap();
        } else {
            control_plane.create_project(project).unwrap();
        }
    }

    #[test]
    fn lifecycle_audit_contains_identity_but_never_raw_secret() {
        let dir = temp_dir("audit");
        provision_project(&dir, "project-a", ResourceLifecycleState::Active);
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
        assert!(text.contains("org-a"));
        assert!(text.contains("project-a"));
        assert!(text.contains(issued.metadata.id.as_str()));
        assert!(!text.contains(&raw));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn create_persists_only_one_way_verifier_and_authenticates_project() {
        let dir = temp_dir("create");
        provision_project(&dir, "project-a", ResourceLifecycleState::Active);
        let store = ApiKeyStore::open(&dir).unwrap();
        let issued = store.create("project-a", None).unwrap();
        let raw = issued.credential.expose_secret().to_string();
        let persisted = fs::read_to_string(store.path()).unwrap();
        assert!(!persisted.contains(&raw));
        let auth = AuthenticationService::required(Arc::new(store.clone()));
        let principal = auth
            .authenticate_authorization_value(Some(&format!("Bearer {raw}")))
            .unwrap();
        assert_eq!(principal.workload_project_id(), Some("project-a"));
        assert_eq!(
            principal.subject(),
            format!("api-key:{}", issued.metadata.id.as_str())
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn restart_preserves_key_and_revocation_is_immediate_and_persistent() {
        let dir = temp_dir("restart");
        provision_project(&dir, "project-b", ResourceLifecycleState::Active);
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
        provision_project(&dir, "project-c", ResourceLifecycleState::Active);
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
        provision_project(&dir, "project-d", ResourceLifecycleState::Active);
        let store = ApiKeyStore::open(&dir).unwrap();
        let issued = store.create("project-d", Some(unix_now())).unwrap();
        assert!(
            store
                .authenticate_token(issued.credential.expose_secret())
                .is_err()
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn issuance_rejects_forged_or_nonexistent_project_binding() {
        let dir = temp_dir("missing-project");
        let store = ApiKeyStore::open(&dir).unwrap();
        assert!(matches!(
            store.create("project-does-not-exist", None),
            Err(ApiKeyError::ProjectNotFound)
        ));
        assert!(matches!(
            store.create("../forged", None),
            Err(ApiKeyError::InvalidProject)
        ));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn suspended_and_deleting_projects_deny_authentication_and_rotation_but_allow_revocation() {
        for (label, lifecycle) in [
            ("suspended", ResourceLifecycleState::Suspended),
            ("deleting", ResourceLifecycleState::Deleting),
        ] {
            let dir = temp_dir(label);
            provision_project(&dir, "project-a", ResourceLifecycleState::Active);
            let store = ApiKeyStore::open(&dir).unwrap();
            let issued = store.create("project-a", None).unwrap();
            let raw = issued.credential.expose_secret().to_string();

            provision_project(&dir, "project-a", lifecycle);
            assert!(store.authenticate_token(&raw).is_err());
            assert!(matches!(
                store.rotate(&issued.metadata.id),
                Err(ApiKeyError::ProjectUnavailable)
            ));
            assert!(store.revoke(&issued.metadata.id).is_ok());
            let _ = fs::remove_dir_all(dir);
        }
    }

    #[test]
    fn existing_metadata_binding_is_deterministic_and_missing_project_fails_closed() {
        let dir = temp_dir("legacy-binding");
        provision_project(&dir, "project-a", ResourceLifecycleState::Active);
        let store = ApiKeyStore::open(&dir).unwrap();
        let issued = store.create("project-a", None).unwrap();
        let raw = issued.credential.expose_secret().to_string();
        let before = fs::read(store.path()).unwrap();
        drop(store);

        fs::remove_dir_all(dir.join("control-plane")).unwrap();
        let reopened = ApiKeyStore::open(&dir).unwrap();
        assert_eq!(reopened.project_ids().unwrap(), vec!["project-a"]);
        assert!(matches!(
            reopened.validate_project_bindings(),
            Err(ApiKeyError::ProjectNotFound)
        ));
        assert!(reopened.authenticate_token(&raw).is_err());
        drop(reopened);

        provision_project(&dir, "project-a", ResourceLifecycleState::Active);
        let reopened = ApiKeyStore::open(&dir).unwrap();
        assert_eq!(reopened.validate_project_bindings().unwrap(), 1);
        assert!(reopened.authenticate_token(&raw).is_ok());
        assert_eq!(fs::read(reopened.path()).unwrap(), before);
        assert_eq!(
            reopened.metadata(&issued.metadata.id).unwrap(),
            issued.metadata
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn service_account_credential_authenticates_as_identity_and_disable_revokes_admission() {
        let dir = temp_dir("service-account");
        provision_project(&dir, "project-a", ResourceLifecycleState::Active);
        let service_accounts = ServiceAccountStore::open(&dir).unwrap();
        let service_account_id = ServiceAccountId::new("svc-search").unwrap();
        service_accounts
            .create(service_account_id.clone(), "Search Worker")
            .unwrap();
        service_accounts
            .assign_project(
                &service_account_id,
                &ProjectId::new("project-a").unwrap(),
                crate::ProjectRole::Editor,
            )
            .unwrap();

        let keys = ApiKeyStore::open(&dir).unwrap();
        let issued = keys
            .create_for_service_account(&service_account_id, "project-a", None)
            .unwrap();
        let raw = issued.credential.expose_secret().to_string();
        assert_eq!(
            issued.metadata.service_account_id.as_deref(),
            Some("svc-search")
        );

        let principal = keys.authenticate_token(&raw).unwrap();
        assert_eq!(principal.subject(), "service-account:svc-search");
        assert_eq!(principal.workload_project_id(), Some("project-a"));
        assert_eq!(
            principal.workload_project_role(),
            Some(crate::ProjectRole::Editor)
        );

        service_accounts
            .set_lifecycle(&service_account_id, ServiceAccountLifecycle::Disabled)
            .unwrap();
        assert!(keys.authenticate_token(&raw).is_err());
        assert!(matches!(
            keys.rotate(&issued.metadata.id),
            Err(ApiKeyError::ServiceAccount(_))
        ));
        assert!(keys.revoke(&issued.metadata.id).is_ok());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn service_account_assignment_role_limits_authorization_and_cross_project_use() {
        let dir = temp_dir("service-account-role");
        provision_project(&dir, "project-a", ResourceLifecycleState::Active);
        provision_project(&dir, "project-b", ResourceLifecycleState::Active);
        let service_accounts = ServiceAccountStore::open(&dir).unwrap();
        let service_account_id = ServiceAccountId::new("svc-reader").unwrap();
        service_accounts
            .create(service_account_id.clone(), "Read Worker")
            .unwrap();
        service_accounts
            .assign_project(
                &service_account_id,
                &ProjectId::new("project-a").unwrap(),
                crate::ProjectRole::Reader,
            )
            .unwrap();

        let keys = ApiKeyStore::open(&dir).unwrap();
        let issued = keys
            .create_for_service_account(&service_account_id, "project-a", None)
            .unwrap();
        assert!(matches!(
            keys.create_for_service_account(&service_account_id, "project-b", None),
            Err(ApiKeyError::ServiceAccount(_))
        ));

        let principal = keys
            .authenticate_token(issued.credential.expose_secret())
            .unwrap();
        let authorization = AuthorizationService::required(&dir).unwrap();
        authorization
            .authorize_project(&principal, AuthorizationAction::CollectionRead, "project-a")
            .unwrap();
        assert!(matches!(
            authorization.authorize_project(
                &principal,
                AuthorizationAction::CollectionWrite,
                "project-a",
            ),
            Err(AuthorizationError::Undiscoverable)
        ));
        assert!(matches!(
            authorization.authorize_project(
                &principal,
                AuthorizationAction::CollectionRead,
                "project-b",
            ),
            Err(AuthorizationError::Undiscoverable)
        ));
        let _ = fs::remove_dir_all(dir);
    }
}
