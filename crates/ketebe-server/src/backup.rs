use crate::integrity::{IntegrityError, IntegrityStatus, IntegrityVerifier};
use crate::runtime::AppState;
use ketebe_core::CollectionId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub const BACKUP_MANIFEST_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivedIndexBackupPolicy {
    Rebuild,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupFileEntry {
    pub path: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub version: u8,
    pub backup_id: String,
    pub collection_id: String,
    pub created_unix_ms: u64,
    pub snapshot_sequence: u64,
    pub checkpoint_sequence: Option<u64>,
    pub derived_index_policy: DerivedIndexBackupPolicy,
    pub files: Vec<BackupFileEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RestoreResult {
    pub backup_id: String,
    pub collection_id: String,
    pub snapshot_sequence: u64,
    pub integrity_status: IntegrityStatus,
}

pub trait BackupRepository: Send + Sync {
    fn publish(&self, backup_id: &str, staging_dir: &Path) -> Result<(), BackupError>;
    fn materialize(&self, backup_id: &str, target_dir: &Path) -> Result<(), BackupError>;
}

#[derive(Debug, Clone)]
pub struct LocalBackupRepository {
    root: PathBuf,
}

impl LocalBackupRepository {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl BackupRepository for LocalBackupRepository {
    fn publish(&self, backup_id: &str, staging_dir: &Path) -> Result<(), BackupError> {
        fs::create_dir_all(&self.root)?;
        let destination = self.root.join(backup_id);
        if destination.exists() {
            return Err(BackupError::AlreadyExists(backup_id.to_string()));
        }
        fs::rename(staging_dir, destination)?;
        Ok(())
    }

    fn materialize(&self, backup_id: &str, target_dir: &Path) -> Result<(), BackupError> {
        let source = self.root.join(backup_id);
        if !source.is_dir() {
            return Err(BackupError::NotFound(backup_id.to_string()));
        }
        copy_tree(&source, target_dir, false)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum BackupError {
    CollectionNotFound(CollectionId),
    AlreadyExists(String),
    NotFound(String),
    CorruptSource(String),
    UnsupportedManifestVersion(u8),
    InvalidManifest(String),
    ChecksumMismatch(String),
    TargetNotEmpty(CollectionId),
    Runtime(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Integrity(IntegrityError),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CollectionNotFound(id) => write!(f, "collection not found: {id}"),
            Self::AlreadyExists(id) => write!(f, "backup already exists: {id}"),
            Self::NotFound(id) => write!(f, "backup not found: {id}"),
            Self::CorruptSource(message) => write!(f, "backup source is not verifiable: {message}"),
            Self::UnsupportedManifestVersion(version) => {
                write!(f, "unsupported backup manifest version: {version}")
            }
            Self::InvalidManifest(message) => write!(f, "invalid backup manifest: {message}"),
            Self::ChecksumMismatch(path) => write!(f, "backup checksum mismatch: {path}"),
            Self::TargetNotEmpty(id) => write!(f, "restore target already exists: {id}"),
            Self::Runtime(message) => write!(f, "restore runtime recovery failed: {message}"),
            Self::Io(error) => write!(f, "backup I/O error: {error}"),
            Self::Json(error) => write!(f, "backup JSON error: {error}"),
            Self::Integrity(error) => write!(f, "backup integrity error: {error}"),
        }
    }
}

impl std::error::Error for BackupError {}
impl From<std::io::Error> for BackupError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<serde_json::Error> for BackupError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
impl From<IntegrityError> for BackupError {
    fn from(value: IntegrityError) -> Self {
        Self::Integrity(value)
    }
}

#[derive(Clone)]
pub struct BackupService {
    state: AppState,
    repository: Arc<dyn BackupRepository>,
}

impl BackupService {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        let repository = Arc::new(LocalBackupRepository::new(state.data_dir.join("backups")));
        Self { state, repository }
    }

    #[must_use]
    pub fn with_repository(state: AppState, repository: Arc<dyn BackupRepository>) -> Self {
        Self { state, repository }
    }

    pub async fn create(
        &self,
        collection_id: &CollectionId,
    ) -> Result<BackupManifest, BackupError> {
        let backup_id = backup_id(collection_id);
        let staging_root = self.state.data_dir.join(".backup-staging").join(&backup_id);
        if staging_root.exists() {
            fs::remove_dir_all(&staging_root)?;
        }
        let staging_collection = staging_root
            .join("collections")
            .join(collection_id.as_str());

        let (snapshot_sequence, checkpoint_sequence) = {
            // A write lock defines the v0 consistent-read boundary. Foreground writes and
            // collection mutations use the same catalog boundary, so collection.json,
            // checkpoint-authoritative segments and the WAL tail are copied as one state.
            let catalog = self.state.catalog.write().await;
            let runtime = catalog
                .collections
                .get(collection_id)
                .ok_or_else(|| BackupError::CollectionNotFound(collection_id.clone()))?;
            let snapshot_sequence = runtime.next_sequence.saturating_sub(1);
            let checkpoint_sequence = runtime
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.sequence_number().get());
            let legacy_source = self
                .state
                .data_dir
                .join("collections")
                .join(collection_id.as_str());
            let source = match crate::data_plane_request::scope_for_collection_id(
                &self.state,
                collection_id,
            )
            .map_err(|error| BackupError::Runtime(error.to_string()))?
            {
                Some(scope) => match ketebe_storage::ScopedStorageNamespace::open_existing(
                    &*self.state.data_dir,
                    scope,
                ) {
                    Ok(namespace) => namespace.root().to_path_buf(),
                    Err(ketebe_storage::NamespaceError::MissingNamespace(_))
                        if !collection_id.as_str().starts_with("c_") =>
                    {
                        legacy_source
                    }
                    Err(error) => return Err(BackupError::Runtime(error.to_string())),
                },
                None if collection_id.as_str().starts_with("c_") => {
                    return Err(BackupError::Runtime(
                        "stable collection identity has no project namespace binding".to_string(),
                    ));
                }
                None => legacy_source,
            };
            copy_tree(&source, &staging_collection, true)?;
            (snapshot_sequence, checkpoint_sequence)
        };

        let report = IntegrityVerifier::new(&staging_root).verify_collection(collection_id)?;
        if !report.authoritative_ok {
            let _ = fs::remove_dir_all(&staging_root);
            return Err(BackupError::CorruptSource(
                "authoritative integrity verification failed".to_string(),
            ));
        }

        let files = inventory_files(&staging_root)?;
        let manifest = BackupManifest {
            version: BACKUP_MANIFEST_VERSION,
            backup_id: backup_id.clone(),
            collection_id: collection_id.as_str().to_string(),
            created_unix_ms: unix_ms(),
            snapshot_sequence,
            checkpoint_sequence,
            derived_index_policy: DerivedIndexBackupPolicy::Rebuild,
            files,
        };
        fs::write(
            staging_root.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )?;
        self.repository.publish(&backup_id, &staging_root)?;
        Ok(manifest)
    }

    pub async fn restore(&self, backup_id: &str) -> Result<RestoreResult, BackupError> {
        let staging_root = self.state.data_dir.join(".restore-staging").join(backup_id);
        if staging_root.exists() {
            fs::remove_dir_all(&staging_root)?;
        }
        fs::create_dir_all(&staging_root)?;
        if let Err(error) = self.repository.materialize(backup_id, &staging_root) {
            let _ = fs::remove_dir_all(&staging_root);
            return Err(error);
        }

        let result = self.restore_materialized(backup_id, &staging_root).await;
        if staging_root.exists() {
            let _ = fs::remove_dir_all(&staging_root);
        }
        result
    }

    async fn restore_materialized(
        &self,
        requested_backup_id: &str,
        staging_root: &Path,
    ) -> Result<RestoreResult, BackupError> {
        let manifest: BackupManifest =
            serde_json::from_slice(&fs::read(staging_root.join("manifest.json"))?)?;
        validate_manifest(&manifest, requested_backup_id)?;
        verify_inventory(staging_root, &manifest)?;

        let collection_id = CollectionId::new(manifest.collection_id.clone())
            .map_err(|error| BackupError::InvalidManifest(error.to_string()))?;
        let staged_collection = staging_root
            .join("collections")
            .join(collection_id.as_str());
        if !staged_collection.is_dir() {
            return Err(BackupError::InvalidManifest(format!(
                "missing collection directory for {}",
                collection_id.as_str()
            )));
        }
        if staged_collection.join("indexes").exists() {
            return Err(BackupError::InvalidManifest(
                "v0 rebuild backups must not contain derived indexes".to_string(),
            ));
        }

        let report = IntegrityVerifier::new(staging_root).verify_collection(&collection_id)?;
        if !report.authoritative_ok {
            return Err(BackupError::CorruptSource(
                "restore staging failed authoritative integrity verification".to_string(),
            ));
        }

        let target = self
            .state
            .data_dir
            .join("collections")
            .join(collection_id.as_str());
        fs::create_dir_all(self.state.data_dir.join("collections"))?;

        // The catalog write lock is the publication gate. Until the staged directory has
        // passed checksum + integrity verification, no target path or runtime entry exists.
        let mut catalog = self.state.catalog.write().await;
        if catalog.collections.contains_key(&collection_id) || target.exists() {
            return Err(BackupError::TargetNotEmpty(collection_id));
        }

        fs::rename(&staged_collection, &target)?;
        let recovered_state = match AppState::recover_with_threshold(
            self.state.data_dir.as_ref(),
            self.state.seal_threshold,
        ) {
            Ok(state) => state,
            Err(error) => {
                let rollback = staging_root
                    .join("collections")
                    .join(collection_id.as_str());
                let _ = fs::create_dir_all(rollback.parent().unwrap_or(staging_root));
                let _ = fs::rename(&target, &rollback);
                return Err(BackupError::Runtime(error.to_string()));
            }
        };
        let mut recovered_catalog = recovered_state.catalog.write().await;
        let runtime = match recovered_catalog.collections.remove(&collection_id) {
            Some(runtime) => runtime,
            None => {
                let rollback = staging_root
                    .join("collections")
                    .join(collection_id.as_str());
                let _ = fs::create_dir_all(rollback.parent().unwrap_or(staging_root));
                let _ = fs::rename(&target, &rollback);
                return Err(BackupError::Runtime(
                    "recovered collection missing from runtime catalog".to_string(),
                ));
            }
        };
        catalog.collections.insert(collection_id.clone(), runtime);

        Ok(RestoreResult {
            backup_id: manifest.backup_id,
            collection_id: collection_id.as_str().to_string(),
            snapshot_sequence: manifest.snapshot_sequence,
            integrity_status: report.status,
        })
    }
}

fn validate_manifest(
    manifest: &BackupManifest,
    requested_backup_id: &str,
) -> Result<(), BackupError> {
    if manifest.version != BACKUP_MANIFEST_VERSION {
        return Err(BackupError::UnsupportedManifestVersion(manifest.version));
    }
    if manifest.backup_id != requested_backup_id {
        return Err(BackupError::InvalidManifest(format!(
            "manifest backup id '{}' does not match requested id '{}'",
            manifest.backup_id, requested_backup_id
        )));
    }
    if manifest.derived_index_policy != DerivedIndexBackupPolicy::Rebuild {
        return Err(BackupError::InvalidManifest(
            "unsupported derived-index restore policy".to_string(),
        ));
    }
    let collection_id = CollectionId::new(manifest.collection_id.clone())
        .map_err(|error| BackupError::InvalidManifest(error.to_string()))?;
    let expected_prefix = format!("collections/{}/", collection_id.as_str());
    for file in &manifest.files {
        let path = Path::new(&file.path);
        if path.is_absolute()
            || path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
            || !file.path.starts_with(&expected_prefix)
        {
            return Err(BackupError::InvalidManifest(format!(
                "unsafe or cross-collection file path: {}",
                file.path
            )));
        }
    }
    Ok(())
}

fn verify_inventory(root: &Path, manifest: &BackupManifest) -> Result<(), BackupError> {
    let mut actual = inventory_files(root)?;
    actual.retain(|entry| entry.path != "manifest.json");
    if actual.len() != manifest.files.len() {
        return Err(BackupError::InvalidManifest(format!(
            "file inventory count differs: manifest={}, materialized={}",
            manifest.files.len(),
            actual.len()
        )));
    }
    for (expected, observed) in manifest.files.iter().zip(actual.iter()) {
        if expected.path != observed.path || expected.size_bytes != observed.size_bytes {
            return Err(BackupError::InvalidManifest(format!(
                "file inventory differs at {}",
                expected.path
            )));
        }
        if expected.sha256 != observed.sha256 {
            return Err(BackupError::ChecksumMismatch(expected.path.clone()));
        }
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path, exclude_derived: bool) -> Result<(), BackupError> {
    if !source.is_dir() {
        return Err(BackupError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("source directory does not exist: {}", source.display()),
        )));
    }
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        if exclude_derived && name == "indexes" {
            continue;
        }
        if name.to_string_lossy().starts_with(".tmp") {
            continue;
        }
        if file_type.is_symlink() {
            return Err(BackupError::CorruptSource(format!(
                "symbolic links are not allowed in backup state: {}",
                entry.path().display()
            )));
        }
        let target = destination.join(&name);
        if file_type.is_dir() {
            copy_tree(&entry.path(), &target, false)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn inventory_files(root: &Path) -> Result<Vec<BackupFileEntry>, BackupError> {
    fn walk(
        root: &Path,
        current: &Path,
        out: &mut Vec<BackupFileEntry>,
    ) -> Result<(), BackupError> {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                walk(root, &entry.path(), out)?;
            } else if file_type.is_file() {
                let path = entry.path();
                let relative = path
                    .strip_prefix(root)
                    .map_err(|error| BackupError::CorruptSource(error.to_string()))?;
                let metadata = entry.metadata()?;
                out.push(BackupFileEntry {
                    path: relative.to_string_lossy().replace('\\', "/"),
                    size_bytes: metadata.len(),
                    sha256: sha256_file(&path)?,
                });
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    walk(root, root, &mut files)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn sha256_file(path: &Path) -> Result<String, BackupError> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn backup_id(collection_id: &CollectionId) -> String {
    format!("{}-{}", collection_id.as_str(), unix_ms())
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
