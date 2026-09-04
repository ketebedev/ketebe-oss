use ketebe_core::{CollectionId, CollectionName, DataPlaneScope, ProjectId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const CATALOG_VERSION: u32 = 1;
const COLLECTION_ID_BYTES: usize = 16;

#[derive(Clone, Serialize, Deserialize)]
struct CatalogFile {
    version: u32,
    projects: BTreeMap<String, BTreeMap<String, String>>,
}

impl Default for CatalogFile {
    fn default() -> Self {
        Self {
            version: CATALOG_VERSION,
            projects: BTreeMap::new(),
        }
    }
}

/// Durable project-scoped mapping from user-visible collection names to stable collection IDs.
#[derive(Clone)]
pub struct CollectionNamespaceCatalog {
    path: Arc<PathBuf>,
    state: Arc<Mutex<CatalogFile>>,
}

impl fmt::Debug for CollectionNamespaceCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CollectionNamespaceCatalog")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl CollectionNamespaceCatalog {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, CollectionNamespaceError> {
        let path = data_dir.as_ref().join("catalog").join("collections.json");
        let state = if path.exists() {
            let decoded: CatalogFile = serde_json::from_slice(&fs::read(&path)?)?;
            if decoded.version != CATALOG_VERSION {
                return Err(CollectionNamespaceError::UnsupportedVersion(
                    decoded.version,
                ));
            }
            decoded
        } else {
            CatalogFile::default()
        };
        Ok(Self {
            path: Arc::new(path),
            state: Arc::new(Mutex::new(state)),
        })
    }

    /// Resolves a name only within the supplied project namespace.
    pub fn resolve(
        &self,
        project_id: &ProjectId,
        name: &CollectionName,
    ) -> Result<Option<DataPlaneScope>, CollectionNamespaceError> {
        let state = self
            .state
            .lock()
            .map_err(|_| CollectionNamespaceError::LockPoisoned)?;
        state
            .projects
            .get(project_id.as_str())
            .and_then(|collections| collections.get(name.as_str()))
            .map(|value| {
                CollectionId::new(value.clone())
                    .map(|collection_id| DataPlaneScope::new(project_id.clone(), collection_id))
                    .map_err(|error| CollectionNamespaceError::CorruptId(error.to_string()))
            })
            .transpose()
    }

    /// Resolves a stable collection ID back to its immutable project scope.
    pub fn find_scope_by_collection_id(
        &self,
        collection_id: &CollectionId,
    ) -> Result<Option<DataPlaneScope>, CollectionNamespaceError> {
        let state = self
            .state
            .lock()
            .map_err(|_| CollectionNamespaceError::LockPoisoned)?;
        let mut found = None;
        for (project, collections) in &state.projects {
            if collections
                .values()
                .any(|value| value == collection_id.as_str())
            {
                let project_id = ProjectId::new(project.clone())
                    .map_err(|error| CollectionNamespaceError::CorruptId(error.to_string()))?;
                let scope = DataPlaneScope::new(project_id, collection_id.clone());
                if found.replace(scope).is_some() {
                    return Err(CollectionNamespaceError::CorruptId(
                        "stable collection ID is bound to multiple projects".to_string(),
                    ));
                }
            }
        }
        Ok(found)
    }

    /// Lists every durable project+collection scope for restart/recovery.
    pub fn list_all_scopes(&self) -> Result<Vec<DataPlaneScope>, CollectionNamespaceError> {
        let state = self
            .state
            .lock()
            .map_err(|_| CollectionNamespaceError::LockPoisoned)?;
        let mut scopes = Vec::new();
        for (project, collections) in &state.projects {
            let project_id = ProjectId::new(project.clone())
                .map_err(|error| CollectionNamespaceError::CorruptId(error.to_string()))?;
            for collection_id in collections.values() {
                let collection_id = CollectionId::new(collection_id.clone())
                    .map_err(|error| CollectionNamespaceError::CorruptId(error.to_string()))?;
                scopes.push(DataPlaneScope::new(project_id.clone(), collection_id));
            }
        }
        Ok(scopes)
    }

    /// Creates a new stable collection ID. Duplicate names are rejected only inside the project.
    pub fn create(
        &self,
        project_id: &ProjectId,
        name: &CollectionName,
    ) -> Result<DataPlaneScope, CollectionNamespaceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| CollectionNamespaceError::LockPoisoned)?;
        let collections = state
            .projects
            .entry(project_id.as_str().to_string())
            .or_default();
        if collections.contains_key(name.as_str()) {
            return Err(CollectionNamespaceError::NameAlreadyExists);
        }
        let collection_id = generate_collection_id()?;
        collections.insert(
            name.as_str().to_string(),
            collection_id.as_str().to_string(),
        );
        persist(&self.path, &state)?;
        Ok(DataPlaneScope::new(project_id.clone(), collection_id))
    }

    /// Lists all visible names and stable scopes for one project.
    pub fn list_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Vec<(CollectionName, DataPlaneScope)>, CollectionNamespaceError> {
        let state = self
            .state
            .lock()
            .map_err(|_| CollectionNamespaceError::LockPoisoned)?;
        let Some(collections) = state.projects.get(project_id.as_str()) else {
            return Ok(Vec::new());
        };
        collections
            .iter()
            .map(|(name, collection_id)| {
                let name = CollectionName::new(name.clone())
                    .map_err(|error| CollectionNamespaceError::CorruptId(error.to_string()))?;
                let collection_id = CollectionId::new(collection_id.clone())
                    .map_err(|error| CollectionNamespaceError::CorruptId(error.to_string()))?;
                Ok((name, DataPlaneScope::new(project_id.clone(), collection_id)))
            })
            .collect()
    }

    /// Removes a visible-name binding only when it still points at the expected stable ID.
    pub fn remove(
        &self,
        project_id: &ProjectId,
        name: &CollectionName,
        expected_collection_id: &CollectionId,
    ) -> Result<bool, CollectionNamespaceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| CollectionNamespaceError::LockPoisoned)?;
        let Some(collections) = state.projects.get_mut(project_id.as_str()) else {
            return Ok(false);
        };
        if collections.get(name.as_str()).map(String::as_str)
            != Some(expected_collection_id.as_str())
        {
            return Ok(false);
        }
        collections.remove(name.as_str());
        if collections.is_empty() {
            state.projects.remove(project_id.as_str());
        }
        persist(&self.path, &state)?;
        Ok(true)
    }

    /// Binds a legacy collection ID to the same visible name in the default project.
    pub fn bind_legacy_default(
        &self,
        collection_id: CollectionId,
    ) -> Result<DataPlaneScope, CollectionNamespaceError> {
        let project_id = ProjectId::default_project();
        let name = CollectionName::new(collection_id.as_str())
            .map_err(|error| CollectionNamespaceError::CorruptId(error.to_string()))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| CollectionNamespaceError::LockPoisoned)?;
        let collections = state
            .projects
            .entry(project_id.as_str().to_string())
            .or_default();
        match collections.get(name.as_str()) {
            Some(existing) if existing != collection_id.as_str() => {
                return Err(CollectionNamespaceError::LegacyBindingConflict);
            }
            Some(_) => {}
            None => {
                collections.insert(
                    name.as_str().to_string(),
                    collection_id.as_str().to_string(),
                );
                persist(&self.path, &state)?;
            }
        }
        Ok(DataPlaneScope::new(project_id, collection_id))
    }
}

fn generate_collection_id() -> Result<CollectionId, CollectionNamespaceError> {
    let mut bytes = [0_u8; COLLECTION_ID_BYTES];
    getrandom::fill(&mut bytes).map_err(|_| CollectionNamespaceError::EntropyUnavailable)?;
    let mut value = String::with_capacity(2 + COLLECTION_ID_BYTES * 2);
    value.push_str("c_");
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    CollectionId::new(value).map_err(|error| CollectionNamespaceError::CorruptId(error.to_string()))
}

fn persist(path: &Path, state: &CatalogFile) -> Result<(), CollectionNamespaceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(state)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

#[derive(Debug)]
pub enum CollectionNamespaceError {
    Io(std::io::Error),
    Json(serde_json::Error),
    UnsupportedVersion(u32),
    NameAlreadyExists,
    LegacyBindingConflict,
    EntropyUnavailable,
    CorruptId(String),
    LockPoisoned,
}

impl fmt::Display for CollectionNamespaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "collection namespace I/O error: {error}"),
            Self::Json(error) => write!(formatter, "collection namespace JSON error: {error}"),
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported collection namespace version {version}"
                )
            }
            Self::NameAlreadyExists => {
                formatter.write_str("collection name already exists in project")
            }
            Self::LegacyBindingConflict => {
                formatter.write_str("legacy collection binding conflicts with existing stable ID")
            }
            Self::EntropyUnavailable => formatter.write_str("secure random source is unavailable"),
            Self::CorruptId(message) => {
                write!(formatter, "invalid stored collection identity: {message}")
            }
            Self::LockPoisoned => formatter.write_str("collection namespace lock poisoned"),
        }
    }
}

impl std::error::Error for CollectionNamespaceError {}

impl From<std::io::Error> for CollectionNamespaceError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for CollectionNamespaceError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ketebe-{label}-{}-{nonce}", std::process::id()))
    }

    #[test]
    fn same_name_is_independent_across_projects_and_survives_restart() {
        let root = temp_root("collection-namespace");
        let project_a = ProjectId::new("project-a").unwrap();
        let project_b = ProjectId::new("project-b").unwrap();
        let name = CollectionName::new("documents").unwrap();
        let catalog = CollectionNamespaceCatalog::open(&root).unwrap();
        let a = catalog.create(&project_a, &name).unwrap();
        let b = catalog.create(&project_b, &name).unwrap();
        assert_ne!(a.collection_id(), b.collection_id());
        drop(catalog);
        let recovered = CollectionNamespaceCatalog::open(&root).unwrap();
        assert_eq!(recovered.resolve(&project_a, &name).unwrap(), Some(a));
        assert_eq!(recovered.resolve(&project_b, &name).unwrap(), Some(b));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn duplicate_name_is_rejected_only_within_same_project() {
        let root = temp_root("collection-duplicate");
        let project = ProjectId::new("project-a").unwrap();
        let name = CollectionName::new("documents").unwrap();
        let catalog = CollectionNamespaceCatalog::open(&root).unwrap();
        catalog.create(&project, &name).unwrap();
        assert!(matches!(
            catalog.create(&project, &name),
            Err(CollectionNamespaceError::NameAlreadyExists)
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_binding_is_deterministic_default_project() {
        let root = temp_root("collection-legacy");
        let catalog = CollectionNamespaceCatalog::open(&root).unwrap();
        let id = CollectionId::new("documents").unwrap();
        let scope = catalog.bind_legacy_default(id.clone()).unwrap();
        assert_eq!(scope.project_id().as_str(), "default");
        assert_eq!(scope.collection_id(), &id);
        let resolved = catalog
            .resolve(
                &ProjectId::default_project(),
                &CollectionName::new("documents").unwrap(),
            )
            .unwrap();
        assert_eq!(resolved, Some(scope));
        fs::remove_dir_all(root).unwrap();
    }
}
