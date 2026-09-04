use crate::{
    AppState, CollectionNamespaceCatalog, CollectionNamespaceError, DataPlaneResolutionError,
    DataPlaneResolver, Principal,
};
use ketebe_core::{CollectionId, CollectionName, DataPlaneScope, ProjectId};
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};

static CATALOGS: LazyLock<Mutex<BTreeMap<PathBuf, CollectionNamespaceCatalog>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn catalog_for(state: &AppState) -> Result<CollectionNamespaceCatalog, DataPlaneRequestError> {
    let path = state.data_dir.as_ref().clone();
    let mut catalogs = CATALOGS
        .lock()
        .map_err(|_| DataPlaneRequestError::CatalogLockPoisoned)?;
    if let Some(catalog) = catalogs.get(&path) {
        return Ok(catalog.clone());
    }
    let catalog =
        CollectionNamespaceCatalog::open(&path).map_err(DataPlaneRequestError::NamespaceCatalog)?;
    catalogs.insert(path, catalog.clone());
    Ok(catalog)
}

pub(crate) fn create_scope(
    state: &AppState,
    principal: &Principal,
    collection_name: &str,
) -> Result<DataPlaneScope, DataPlaneRequestError> {
    let name = CollectionName::new(collection_name)
        .map_err(|error| DataPlaneRequestError::InvalidCollectionName(error.to_string()))?;
    DataPlaneResolver::new(catalog_for(state)?)
        .create(principal, &name)
        .map_err(DataPlaneRequestError::Resolution)
}

pub(crate) async fn resolve_existing_scope(
    state: &AppState,
    principal: &Principal,
    collection_name: &str,
) -> Result<DataPlaneScope, DataPlaneRequestError> {
    let name = CollectionName::new(collection_name)
        .map_err(|error| DataPlaneRequestError::InvalidCollectionName(error.to_string()))?;
    let catalog = catalog_for(state)?;
    let resolver = DataPlaneResolver::new(catalog.clone());
    if let Some(scope) = resolver
        .resolve(principal, &name)
        .map_err(DataPlaneRequestError::Resolution)?
    {
        return Ok(scope);
    }

    // Deterministic compatibility bridge for pre-scope OSS collections. The authenticated
    // development principal is pinned to the default project, so legacy data can never be
    // claimed by a different project.
    if principal.project_id() == Some(ProjectId::default_project().as_str())
        && let Ok(collection_id) = CollectionId::new(collection_name)
        && state
            .catalog
            .read()
            .await
            .collections
            .contains_key(&collection_id)
    {
        return catalog
            .bind_legacy_default(collection_id)
            .map_err(DataPlaneRequestError::NamespaceCatalog);
    }

    Err(DataPlaneRequestError::CollectionNotFound)
}

pub(crate) async fn list_project_scopes(
    state: &AppState,
    principal: &Principal,
) -> Result<Vec<(String, DataPlaneScope)>, DataPlaneRequestError> {
    let project = principal
        .project_id()
        .ok_or(DataPlaneRequestError::Resolution(
            DataPlaneResolutionError::MissingProjectScope,
        ))?;
    let project = ProjectId::new(project).map_err(|error| {
        DataPlaneRequestError::Resolution(DataPlaneResolutionError::InvalidProjectScope(
            error.to_string(),
        ))
    })?;
    let namespace_catalog = catalog_for(state)?;
    // Legacy runtimes are materialized into the deterministic default-project namespace before
    // management adapters enumerate them; scoped/stable collections never use this fallback.
    if project == ProjectId::default_project() {
        let legacy_ids = {
            let runtime_catalog = state.catalog.read().await;
            runtime_catalog
                .collections
                .iter()
                .filter(|(_, runtime)| runtime.scope.is_none())
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>()
        };
        for collection_id in legacy_ids {
            if namespace_catalog
                .find_scope_by_collection_id(&collection_id)
                .map_err(DataPlaneRequestError::NamespaceCatalog)?
                .is_none()
            {
                namespace_catalog
                    .bind_legacy_default(collection_id)
                    .map_err(DataPlaneRequestError::NamespaceCatalog)?;
            }
        }
    }
    namespace_catalog
        .list_project(&project)
        .map(|entries| {
            entries
                .into_iter()
                .map(|(name, scope)| (name.as_str().to_string(), scope))
                .collect()
        })
        .map_err(DataPlaneRequestError::NamespaceCatalog)
}

pub(crate) fn scope_for_collection_id(
    state: &AppState,
    collection_id: &CollectionId,
) -> Result<Option<DataPlaneScope>, DataPlaneRequestError> {
    catalog_for(state)?
        .find_scope_by_collection_id(collection_id)
        .map_err(DataPlaneRequestError::NamespaceCatalog)
}

pub(crate) fn remove_scope(
    state: &AppState,
    principal: &Principal,
    collection_name: &str,
    collection_id: &CollectionId,
) -> Result<(), DataPlaneRequestError> {
    let project = principal
        .project_id()
        .ok_or(DataPlaneRequestError::Resolution(
            DataPlaneResolutionError::MissingProjectScope,
        ))?;
    let project = ProjectId::new(project).map_err(|error| {
        DataPlaneRequestError::Resolution(DataPlaneResolutionError::InvalidProjectScope(
            error.to_string(),
        ))
    })?;
    let name = CollectionName::new(collection_name)
        .map_err(|error| DataPlaneRequestError::InvalidCollectionName(error.to_string()))?;
    catalog_for(state)?
        .remove(&project, &name, collection_id)
        .map_err(DataPlaneRequestError::NamespaceCatalog)?;
    Ok(())
}

#[derive(Debug)]
pub(crate) enum DataPlaneRequestError {
    InvalidCollectionName(String),
    Resolution(DataPlaneResolutionError),
    NamespaceCatalog(CollectionNamespaceError),
    CatalogLockPoisoned,
    CollectionNotFound,
}

impl fmt::Display for DataPlaneRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCollectionName(message) => {
                write!(formatter, "invalid collection name: {message}")
            }
            Self::Resolution(error) => error.fmt(formatter),
            Self::NamespaceCatalog(error) => error.fmt(formatter),
            Self::CatalogLockPoisoned => formatter.write_str("data-plane catalog lock poisoned"),
            Self::CollectionNotFound => formatter.write_str("collection was not found"),
        }
    }
}

impl std::error::Error for DataPlaneRequestError {}
