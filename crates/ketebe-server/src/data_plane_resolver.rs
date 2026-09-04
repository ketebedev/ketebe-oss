use crate::{CollectionNamespaceCatalog, CollectionNamespaceError, Principal};
use ketebe_core::{CollectionName, DataPlaneScope, ProjectId};
use std::fmt;

/// Resolves user-visible collection names into stable data-plane scopes using only the
/// authenticated principal's project identity. Callers cannot override the effective project.
#[derive(Clone, Debug)]
pub struct DataPlaneResolver {
    catalog: CollectionNamespaceCatalog,
}

impl DataPlaneResolver {
    #[must_use]
    pub fn new(catalog: CollectionNamespaceCatalog) -> Self {
        Self { catalog }
    }

    pub fn resolve(
        &self,
        principal: &Principal,
        collection_name: &CollectionName,
    ) -> Result<Option<DataPlaneScope>, DataPlaneResolutionError> {
        let project_id = principal_project(principal)?;
        self.catalog
            .resolve(&project_id, collection_name)
            .map_err(DataPlaneResolutionError::Catalog)
    }

    pub fn create(
        &self,
        principal: &Principal,
        collection_name: &CollectionName,
    ) -> Result<DataPlaneScope, DataPlaneResolutionError> {
        let project_id = principal_project(principal)?;
        self.catalog
            .create(&project_id, collection_name)
            .map_err(DataPlaneResolutionError::Catalog)
    }
}

fn principal_project(principal: &Principal) -> Result<ProjectId, DataPlaneResolutionError> {
    let project_id = principal
        .project_id()
        .ok_or(DataPlaneResolutionError::MissingProjectScope)?;
    ProjectId::new(project_id)
        .map_err(|error| DataPlaneResolutionError::InvalidProjectScope(error.to_string()))
}

#[derive(Debug)]
pub enum DataPlaneResolutionError {
    MissingProjectScope,
    InvalidProjectScope(String),
    Catalog(CollectionNamespaceError),
}

impl fmt::Display for DataPlaneResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingProjectScope => {
                formatter.write_str("authenticated principal has no project data-plane scope")
            }
            Self::InvalidProjectScope(message) => {
                write!(formatter, "invalid authenticated project scope: {message}")
            }
            Self::Catalog(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for DataPlaneResolutionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Principal;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ketebe-{label}-{}-{nonce}", std::process::id()))
    }

    #[test]
    fn principal_project_is_the_only_effective_project_scope() {
        let root = temp_root("principal-scope");
        let catalog = CollectionNamespaceCatalog::open(&root).unwrap();
        let resolver = DataPlaneResolver::new(catalog.clone());
        let name = CollectionName::new("documents").unwrap();
        let principal_a = Principal::for_project("key-a", "project-a").unwrap();
        let principal_b = Principal::for_project("key-b", "project-b").unwrap();

        let scope_a = resolver.create(&principal_a, &name).unwrap();
        let scope_b = resolver.create(&principal_b, &name).unwrap();
        assert_ne!(scope_a.collection_id(), scope_b.collection_id());
        assert_eq!(scope_a.project_id().as_str(), "project-a");
        assert_eq!(scope_b.project_id().as_str(), "project-b");
        assert_eq!(
            resolver.resolve(&principal_a, &name).unwrap(),
            Some(scope_a)
        );
        assert_eq!(
            resolver.resolve(&principal_b, &name).unwrap(),
            Some(scope_b)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn principal_without_project_fails_closed() {
        let root = temp_root("principal-missing-scope");
        let resolver = DataPlaneResolver::new(CollectionNamespaceCatalog::open(&root).unwrap());
        let principal = Principal::new("development", crate::PrincipalKind::Development).unwrap();
        let error = resolver
            .resolve(&principal, &CollectionName::new("documents").unwrap())
            .unwrap_err();
        assert!(matches!(
            error,
            DataPlaneResolutionError::MissingProjectScope
        ));
        if root.exists() {
            fs::remove_dir_all(root).unwrap();
        }
    }
}
