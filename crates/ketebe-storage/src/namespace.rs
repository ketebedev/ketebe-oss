use ketebe_core::DataPlaneScope;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const SCOPE_MANIFEST_VERSION: u32 = 1;
const SCOPE_MANIFEST_FILE: &str = "scope.meta";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopeManifest {
    version: u32,
    project_id: String,
    collection_id: String,
}

impl ScopeManifest {
    fn from_scope(scope: &DataPlaneScope) -> Self {
        Self {
            version: SCOPE_MANIFEST_VERSION,
            project_id: scope.project_id().as_str().to_string(),
            collection_id: scope.collection_id().as_str().to_string(),
        }
    }

    fn encode(&self) -> String {
        format!(
            "version={}\nproject_id={}\ncollection_id={}\n",
            self.version, self.project_id, self.collection_id
        )
    }

    fn decode(value: &str) -> Result<Self, NamespaceError> {
        let mut version = None;
        let mut project_id = None;
        let mut collection_id = None;
        for line in value.lines() {
            if let Some(value) = line.strip_prefix("version=") {
                version = Some(
                    value
                        .parse::<u32>()
                        .map_err(|_| NamespaceError::CorruptManifest)?,
                );
            } else if let Some(value) = line.strip_prefix("project_id=") {
                project_id = Some(value.to_string());
            } else if let Some(value) = line.strip_prefix("collection_id=") {
                collection_id = Some(value.to_string());
            }
        }
        Ok(Self {
            version: version.ok_or(NamespaceError::CorruptManifest)?,
            project_id: project_id.ok_or(NamespaceError::CorruptManifest)?,
            collection_id: collection_id.ok_or(NamespaceError::CorruptManifest)?,
        })
    }

    fn matches(&self, scope: &DataPlaneScope) -> bool {
        self.version == SCOPE_MANIFEST_VERSION
            && self.project_id == scope.project_id().as_str()
            && self.collection_id == scope.collection_id().as_str()
    }
}

/// Durable directory namespace for one immutable project+collection scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedStorageNamespace {
    scope: DataPlaneScope,
    root: PathBuf,
}

impl ScopedStorageNamespace {
    /// Opens or creates a scoped namespace and validates its durable ownership marker.
    pub fn open(data_dir: impl AsRef<Path>, scope: DataPlaneScope) -> Result<Self, NamespaceError> {
        let root = scoped_path(data_dir.as_ref(), &scope);
        fs::create_dir_all(&root)?;
        validate_or_initialize_manifest(&root, &scope)?;
        Ok(Self { scope, root })
    }

    /// Opens an already-existing namespace without creating missing state.
    pub fn open_existing(
        data_dir: impl AsRef<Path>,
        scope: DataPlaneScope,
    ) -> Result<Self, NamespaceError> {
        let root = scoped_path(data_dir.as_ref(), &scope);
        if !root.is_dir() {
            return Err(NamespaceError::MissingNamespace(root));
        }
        validate_existing_manifest(&root, &scope)?;
        Ok(Self { scope, root })
    }

    /// Deterministically moves pre-scope single-tenant storage into the default project.
    /// No non-default project may claim legacy storage.
    pub fn migrate_legacy_default(
        data_dir: impl AsRef<Path>,
        scope: DataPlaneScope,
    ) -> Result<Self, NamespaceError> {
        if scope.project_id().as_str() != "default" {
            return Err(NamespaceError::LegacyProjectMismatch);
        }
        let data_dir = data_dir.as_ref();
        let target = scoped_path(data_dir, &scope);
        if target.exists() {
            return Self::open_existing(data_dir, scope);
        }
        let legacy = data_dir
            .join("collections")
            .join(scope.collection_id().as_str());
        if !legacy.exists() {
            return Self::open(data_dir, scope);
        }
        let parent = target
            .parent()
            .expect("scoped collection path always has a parent");
        fs::create_dir_all(parent)?;
        fs::rename(&legacy, &target)?;
        validate_or_initialize_manifest(&target, &scope)?;
        Ok(Self {
            scope,
            root: target,
        })
    }

    #[must_use]
    pub fn scope(&self) -> &DataPlaneScope {
        &self.scope
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn wal_path(&self) -> PathBuf {
        self.root.join("wal.log")
    }

    #[must_use]
    pub fn segments_dir(&self) -> PathBuf {
        self.root.join("segments")
    }

    #[must_use]
    pub fn checkpoints_dir(&self) -> PathBuf {
        self.root.join("checkpoints")
    }

    #[must_use]
    pub fn indexes_dir(&self) -> PathBuf {
        self.root.join("indexes")
    }
}

fn scoped_path(data_dir: &Path, scope: &DataPlaneScope) -> PathBuf {
    data_dir
        .join("projects")
        .join(scope.project_id().as_str())
        .join("collections")
        .join(scope.collection_id().as_str())
}

fn validate_or_initialize_manifest(
    root: &Path,
    scope: &DataPlaneScope,
) -> Result<(), NamespaceError> {
    let path = root.join(SCOPE_MANIFEST_FILE);
    if path.exists() {
        return validate_existing_manifest(root, scope);
    }
    let manifest = ScopeManifest::from_scope(scope);
    let temporary = root.join("scope.meta.tmp");
    fs::write(&temporary, manifest.encode())?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn validate_existing_manifest(root: &Path, scope: &DataPlaneScope) -> Result<(), NamespaceError> {
    let path = root.join(SCOPE_MANIFEST_FILE);
    let value = fs::read_to_string(&path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            NamespaceError::MissingOwnershipManifest(path.clone())
        } else {
            NamespaceError::Io(error)
        }
    })?;
    let manifest = ScopeManifest::decode(&value)?;
    if manifest.version != SCOPE_MANIFEST_VERSION {
        return Err(NamespaceError::UnsupportedVersion(manifest.version));
    }
    if !manifest.matches(scope) {
        return Err(NamespaceError::OwnershipMismatch {
            expected_project: scope.project_id().as_str().to_string(),
            expected_collection: scope.collection_id().as_str().to_string(),
            actual_project: manifest.project_id,
            actual_collection: manifest.collection_id,
        });
    }
    Ok(())
}

#[derive(Debug)]
pub enum NamespaceError {
    Io(io::Error),
    MissingNamespace(PathBuf),
    MissingOwnershipManifest(PathBuf),
    CorruptManifest,
    UnsupportedVersion(u32),
    OwnershipMismatch {
        expected_project: String,
        expected_collection: String,
        actual_project: String,
        actual_collection: String,
    },
    LegacyProjectMismatch,
}

impl fmt::Display for NamespaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "storage namespace I/O failed: {error}"),
            Self::MissingNamespace(path) => {
                write!(
                    formatter,
                    "storage namespace '{}' does not exist",
                    path.display()
                )
            }
            Self::MissingOwnershipManifest(path) => write!(
                formatter,
                "storage namespace ownership manifest '{}' is missing",
                path.display()
            ),
            Self::CorruptManifest => {
                formatter.write_str("storage namespace ownership manifest is corrupt")
            }
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported storage scope manifest version {version}"
                )
            }
            Self::OwnershipMismatch {
                expected_project,
                expected_collection,
                actual_project,
                actual_collection,
            } => write!(
                formatter,
                "storage scope ownership mismatch: expected {expected_project}/{expected_collection}, found {actual_project}/{actual_collection}"
            ),
            Self::LegacyProjectMismatch => formatter.write_str(
                "legacy single-tenant storage may only migrate into the default project",
            ),
        }
    }
}

impl std::error::Error for NamespaceError {}

impl From<io::Error> for NamespaceError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ketebe_core::{CollectionId, ProjectId};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn scope(project: &str, collection: &str) -> DataPlaneScope {
        DataPlaneScope::new(
            ProjectId::new(project).unwrap(),
            CollectionId::new(collection).unwrap(),
        )
    }

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ketebe-{label}-{}-{nonce}", std::process::id()))
    }

    #[test]
    fn identical_collection_ids_use_distinct_project_directories() {
        let root = temp_root("scope-distinct");
        let a = ScopedStorageNamespace::open(&root, scope("project-a", "documents")).unwrap();
        let b = ScopedStorageNamespace::open(&root, scope("project-b", "documents")).unwrap();
        assert_ne!(a.root(), b.root());
        assert!(a.root().join(SCOPE_MANIFEST_FILE).exists());
        assert!(b.root().join(SCOPE_MANIFEST_FILE).exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn injected_mismatched_manifest_fails_closed() {
        let root = temp_root("scope-mismatch");
        let a_scope = scope("project-a", "documents");
        let a = ScopedStorageNamespace::open(&root, a_scope.clone()).unwrap();
        let forged = ScopeManifest::from_scope(&scope("project-b", "documents"));
        fs::write(a.root().join(SCOPE_MANIFEST_FILE), forged.encode()).unwrap();
        let error = ScopedStorageNamespace::open_existing(&root, a_scope).unwrap_err();
        assert!(matches!(error, NamespaceError::OwnershipMismatch { .. }));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_data_migrates_only_to_default_project() {
        let root = temp_root("scope-legacy");
        let legacy = root.join("collections").join("documents");
        fs::create_dir_all(&legacy).unwrap();
        fs::write(legacy.join("wal.log"), b"legacy").unwrap();
        let migrated = ScopedStorageNamespace::migrate_legacy_default(
            &root,
            DataPlaneScope::legacy_default(CollectionId::new("documents").unwrap()),
        )
        .unwrap();
        assert_eq!(fs::read(migrated.wal_path()).unwrap(), b"legacy");
        let denied =
            ScopedStorageNamespace::migrate_legacy_default(&root, scope("project-b", "other"))
                .unwrap_err();
        assert!(matches!(denied, NamespaceError::LegacyProjectMismatch));
        fs::remove_dir_all(root).unwrap();
    }
}
