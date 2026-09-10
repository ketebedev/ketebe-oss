use std::fmt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use ketebe_core::ProjectId;
use ketebe_storage::{ControlPlaneStore, ControlPlaneStoreError};

use crate::{
    ApiKeyError, ApiKeyStore, AuthorizationError, AuthorizationService, CollectionNamespaceCatalog,
    CollectionNamespaceError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapMigrationResult {
    pub default_resources_created: bool,
    pub validated_project_references: usize,
}

pub fn bootstrap_default_control_plane(
    data_dir: impl AsRef<Path>,
) -> Result<BootstrapMigrationResult, BootstrapMigrationError> {
    let data_dir = data_dir.as_ref();
    let store = ControlPlaneStore::open(data_dir.join("control-plane"))?;

    let authorization = AuthorizationService::required(data_dir)?;
    let api_keys = ApiKeyStore::open(data_dir)?;
    let catalog = CollectionNamespaceCatalog::open(data_dir)?;

    let mut project_ids = authorization.project_ids()?;
    project_ids.extend(api_keys.project_ids()?);
    project_ids.extend(
        catalog
            .project_ids()?
            .into_iter()
            .map(|project_id| project_id.as_str().to_string()),
    );
    project_ids.sort();
    project_ids.dedup();

    for raw_project_id in &project_ids {
        let project_id = ProjectId::new(raw_project_id.clone()).map_err(|error| {
            BootstrapMigrationError::InvalidLegacyProject {
                project_id: raw_project_id.clone(),
                reason: error.to_string(),
            }
        })?;
        if project_id == ProjectId::default_project() {
            continue;
        }
        if store.project(&project_id)?.is_none() {
            return Err(BootstrapMigrationError::UnmappedLegacyProject(
                raw_project_id.clone(),
            ));
        }
    }

    let default_resources_created = store.bootstrap_default(unix_now())?;
    api_keys.validate_project_bindings()?;

    Ok(BootstrapMigrationResult {
        default_resources_created,
        validated_project_references: project_ids.len(),
    })
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug)]
pub enum BootstrapMigrationError {
    ControlPlane(ControlPlaneStoreError),
    Authorization(AuthorizationError),
    ApiKey(ApiKeyError),
    Catalog(CollectionNamespaceError),
    InvalidLegacyProject { project_id: String, reason: String },
    UnmappedLegacyProject(String),
}

impl fmt::Display for BootstrapMigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ControlPlane(error) => {
                write!(formatter, "control-plane bootstrap error: {error}")
            }
            Self::Authorization(error) => {
                write!(
                    formatter,
                    "authorization migration validation error: {error}"
                )
            }
            Self::ApiKey(error) => write!(formatter, "API key migration validation error: {error}"),
            Self::Catalog(error) => {
                write!(formatter, "catalog migration validation error: {error}")
            }
            Self::InvalidLegacyProject { project_id, reason } => write!(
                formatter,
                "legacy project reference '{project_id}' is invalid: {reason}"
            ),
            Self::UnmappedLegacyProject(project_id) => write!(
                formatter,
                "legacy project reference '{project_id}' has no first-class control-plane Project"
            ),
        }
    }
}

impl std::error::Error for BootstrapMigrationError {}

impl From<ControlPlaneStoreError> for BootstrapMigrationError {
    fn from(value: ControlPlaneStoreError) -> Self {
        Self::ControlPlane(value)
    }
}

impl From<AuthorizationError> for BootstrapMigrationError {
    fn from(value: AuthorizationError) -> Self {
        Self::Authorization(value)
    }
}

impl From<ApiKeyError> for BootstrapMigrationError {
    fn from(value: ApiKeyError) -> Self {
        Self::ApiKey(value)
    }
}

impl From<CollectionNamespaceError> for BootstrapMigrationError {
    fn from(value: CollectionNamespaceError) -> Self {
        Self::Catalog(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProjectRole;
    use ketebe_core::{CollectionId, OrganizationId};
    use std::fs;
    use std::path::PathBuf;

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ketebe-bootstrap-{label}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    #[test]
    fn fresh_bootstrap_creates_real_default_resources_and_is_idempotent() {
        let dir = temp_dir("fresh");
        let first = bootstrap_default_control_plane(&dir).unwrap();
        assert!(first.default_resources_created);

        let store = ControlPlaneStore::open(dir.join("control-plane")).unwrap();
        let organization = store
            .organization(&OrganizationId::default_organization())
            .unwrap()
            .unwrap();
        let project = store
            .project(&ProjectId::default_project())
            .unwrap()
            .unwrap();
        assert_eq!(organization.id().as_str(), "default");
        assert_eq!(project.organization_id(), organization.id());

        let second = bootstrap_default_control_plane(&dir).unwrap();
        assert!(!second.default_resources_created);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn legacy_default_authorization_api_keys_and_catalog_map_without_rewrite() {
        let dir = temp_dir("legacy-default");
        let authorization = AuthorizationService::required(&dir).unwrap();
        authorization
            .set_project_role("default", "operator", ProjectRole::Owner)
            .unwrap();

        let control_plane = ControlPlaneStore::open(dir.join("control-plane")).unwrap();
        control_plane.bootstrap_default(unix_now()).unwrap();
        let api_keys = ApiKeyStore::open(&dir).unwrap();
        let issued = api_keys.create("default", None).unwrap();
        assert_eq!(issued.metadata.project_id, "default");
        drop(api_keys);
        drop(control_plane);
        fs::remove_dir_all(dir.join("control-plane")).unwrap();

        let catalog = CollectionNamespaceCatalog::open(&dir).unwrap();
        catalog
            .bind_legacy_default(CollectionId::new("documents").unwrap())
            .unwrap();

        let result = bootstrap_default_control_plane(&dir).unwrap();
        assert_eq!(result.validated_project_references, 1);

        let reopened_keys = ApiKeyStore::open(&dir).unwrap();
        assert_eq!(reopened_keys.project_ids().unwrap(), vec!["default"]);
        let reopened_catalog = CollectionNamespaceCatalog::open(&dir).unwrap();
        assert_eq!(
            reopened_catalog.project_ids().unwrap(),
            vec![ProjectId::default_project()]
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unknown_non_default_legacy_project_fails_before_default_bootstrap() {
        let dir = temp_dir("unknown-project");
        let authorization = AuthorizationService::required(&dir).unwrap();
        authorization
            .set_project_role("project-a", "operator", ProjectRole::Owner)
            .unwrap();

        let error = bootstrap_default_control_plane(&dir).unwrap_err();
        assert!(matches!(
            error,
            BootstrapMigrationError::UnmappedLegacyProject(project) if project == "project-a"
        ));

        let store = ControlPlaneStore::open(dir.join("control-plane")).unwrap();
        assert!(
            store
                .organization(&OrganizationId::default_organization())
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .project(&ProjectId::default_project())
                .unwrap()
                .is_none()
        );
        fs::remove_dir_all(dir).unwrap();
    }
}
