use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use ketebe_core::{ProjectId, ResourceLifecycleState};
use ketebe_storage::ControlPlaneStore;
use serde::{Deserialize, Serialize};

use crate::{AuditCategory, AuditEvent, AuditOrigin, AuditResult, AuditService, ProjectRole};

const STORE_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ServiceAccountId(String);

impl ServiceAccountId {
    pub fn new(value: impl Into<String>) -> Result<Self, ServiceAccountError> {
        let value = value.into();
        let id = ProjectId::new(value.clone()).map_err(|_| ServiceAccountError::InvalidId)?;
        Ok(Self(id.as_str().to_string()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceAccountLifecycle {
    Active,
    Disabled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceAccount {
    id: ServiceAccountId,
    name: String,
    lifecycle: ServiceAccountLifecycle,
    created_at_unix: u64,
    updated_at_unix: u64,
    assignments: BTreeMap<String, ProjectRole>,
}

impl ServiceAccount {
    fn new(
        id: ServiceAccountId,
        name: impl Into<String>,
        now: u64,
    ) -> Result<Self, ServiceAccountError> {
        let name = name.into();
        let name = name.trim();
        if name.is_empty() {
            return Err(ServiceAccountError::InvalidName);
        }
        Ok(Self {
            id,
            name: name.to_string(),
            lifecycle: ServiceAccountLifecycle::Active,
            created_at_unix: now,
            updated_at_unix: now,
            assignments: BTreeMap::new(),
        })
    }

    #[must_use]
    pub fn id(&self) -> &ServiceAccountId {
        &self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn lifecycle(&self) -> ServiceAccountLifecycle {
        self.lifecycle
    }

    #[must_use]
    pub fn created_at_unix(&self) -> u64 {
        self.created_at_unix
    }

    #[must_use]
    pub fn updated_at_unix(&self) -> u64 {
        self.updated_at_unix
    }

    #[must_use]
    pub fn project_role(&self, project_id: &str) -> Option<ProjectRole> {
        self.assignments.get(project_id).copied()
    }

    #[must_use]
    pub fn assignments(&self) -> &BTreeMap<String, ProjectRole> {
        &self.assignments
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ServiceAccountFile {
    version: u32,
    accounts: BTreeMap<ServiceAccountId, ServiceAccount>,
}

impl Default for ServiceAccountFile {
    fn default() -> Self {
        Self {
            version: STORE_VERSION,
            accounts: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub enum ServiceAccountError {
    InvalidId,
    InvalidName,
    NotFound,
    Disabled,
    ProjectNotFound,
    ProjectUnavailable,
    AssignmentNotFound,
    AlreadyExists,
    Io(std::io::Error),
    Json(serde_json::Error),
    UnsupportedVersion(u32),
    ControlPlane(String),
    LockPoisoned,
}

impl fmt::Display for ServiceAccountError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidId => formatter.write_str("invalid service account id"),
            Self::InvalidName => formatter.write_str("service account name must not be empty"),
            Self::NotFound => formatter.write_str("service account not found"),
            Self::Disabled => formatter.write_str("service account is disabled"),
            Self::ProjectNotFound => formatter.write_str("project not found"),
            Self::ProjectUnavailable => formatter.write_str("project is not active"),
            Self::AssignmentNotFound => {
                formatter.write_str("service account project assignment not found")
            }
            Self::AlreadyExists => formatter.write_str("service account already exists"),
            Self::Io(error) => write!(formatter, "service account I/O error: {error}"),
            Self::Json(error) => write!(formatter, "service account JSON error: {error}"),
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported service account store version {version}"
                )
            }
            Self::ControlPlane(message) => {
                write!(formatter, "service account control-plane error: {message}")
            }
            Self::LockPoisoned => formatter.write_str("service account store lock poisoned"),
        }
    }
}

impl std::error::Error for ServiceAccountError {}

impl From<std::io::Error> for ServiceAccountError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for ServiceAccountError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[derive(Clone)]
pub struct ServiceAccountStore {
    path: Arc<PathBuf>,
    control_plane_path: Arc<PathBuf>,
    audit: Arc<AuditService>,
    state: Arc<Mutex<ServiceAccountFile>>,
}

impl fmt::Debug for ServiceAccountStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceAccountStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl ServiceAccountStore {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, ServiceAccountError> {
        let data_dir = data_dir.as_ref();
        let path = data_dir.join("security").join("service-accounts.json");
        let state = if path.exists() {
            let decoded: ServiceAccountFile = serde_json::from_slice(&fs::read(&path)?)?;
            if decoded.version != STORE_VERSION {
                return Err(ServiceAccountError::UnsupportedVersion(decoded.version));
            }
            decoded
        } else {
            ServiceAccountFile::default()
        };
        let control_plane_path = data_dir.join("control-plane");
        ControlPlaneStore::open(&control_plane_path)
            .map_err(|error| ServiceAccountError::ControlPlane(error.to_string()))?;
        Ok(Self {
            path: Arc::new(path),
            control_plane_path: Arc::new(control_plane_path),
            audit: Arc::new(AuditService::noop()),
            state: Arc::new(Mutex::new(state)),
        })
    }

    #[must_use]
    pub fn with_audit(mut self, audit: AuditService) -> Self {
        self.audit = Arc::new(audit);
        self
    }

    pub fn create(
        &self,
        id: ServiceAccountId,
        name: impl Into<String>,
    ) -> Result<ServiceAccount, ServiceAccountError> {
        let now = unix_now();
        let account = ServiceAccount::new(id.clone(), name, now)?;
        let mut state = self.lock()?;
        if state.accounts.contains_key(&id) {
            return Err(ServiceAccountError::AlreadyExists);
        }
        let mut next = state.clone();
        next.accounts.insert(id, account.clone());
        self.persist(&next)?;
        *state = next;
        self.audit_account("service_account_create", &account, None);
        Ok(account)
    }

    pub fn set_lifecycle(
        &self,
        id: &ServiceAccountId,
        lifecycle: ServiceAccountLifecycle,
    ) -> Result<ServiceAccount, ServiceAccountError> {
        let mut state = self.lock()?;
        let current = state
            .accounts
            .get(id)
            .ok_or(ServiceAccountError::NotFound)?;
        let mut account = current.clone();
        account.lifecycle = lifecycle;
        account.updated_at_unix = unix_now();
        let mut next = state.clone();
        next.accounts.insert(id.clone(), account.clone());
        self.persist(&next)?;
        *state = next;
        self.audit_account("service_account_lifecycle", &account, None);
        Ok(account)
    }

    pub fn delete(&self, id: &ServiceAccountId) -> Result<ServiceAccount, ServiceAccountError> {
        let mut state = self.lock()?;
        let mut next = state.clone();
        let account = next
            .accounts
            .remove(id)
            .ok_or(ServiceAccountError::NotFound)?;
        self.persist(&next)?;
        *state = next;
        self.audit_account("service_account_delete", &account, None);
        Ok(account)
    }

    pub fn assign_project(
        &self,
        id: &ServiceAccountId,
        project_id: &ProjectId,
        role: ProjectRole,
    ) -> Result<ServiceAccount, ServiceAccountError> {
        self.ensure_active_project(project_id)?;
        let mut state = self.lock()?;
        let current = state
            .accounts
            .get(id)
            .ok_or(ServiceAccountError::NotFound)?;
        let mut account = current.clone();
        account
            .assignments
            .insert(project_id.as_str().to_string(), role);
        account.updated_at_unix = unix_now();
        let mut next = state.clone();
        next.accounts.insert(id.clone(), account.clone());
        self.persist(&next)?;
        *state = next;
        self.audit_account(
            "service_account_project_assign",
            &account,
            Some(project_id.as_str()),
        );
        Ok(account)
    }

    pub fn remove_project_assignment(
        &self,
        id: &ServiceAccountId,
        project_id: &ProjectId,
    ) -> Result<ServiceAccount, ServiceAccountError> {
        let mut state = self.lock()?;
        let current = state
            .accounts
            .get(id)
            .ok_or(ServiceAccountError::NotFound)?;
        let mut account = current.clone();
        if account.assignments.remove(project_id.as_str()).is_none() {
            return Err(ServiceAccountError::AssignmentNotFound);
        }
        account.updated_at_unix = unix_now();
        let mut next = state.clone();
        next.accounts.insert(id.clone(), account.clone());
        self.persist(&next)?;
        *state = next;
        self.audit_account(
            "service_account_project_unassign",
            &account,
            Some(project_id.as_str()),
        );
        Ok(account)
    }

    pub fn account(&self, id: &ServiceAccountId) -> Result<ServiceAccount, ServiceAccountError> {
        let state = self.lock()?;
        state
            .accounts
            .get(id)
            .cloned()
            .ok_or(ServiceAccountError::NotFound)
    }

    pub fn authorize_project(
        &self,
        id: &ServiceAccountId,
        project_id: &ProjectId,
    ) -> Result<ProjectRole, ServiceAccountError> {
        let account = self.account(id)?;
        if account.lifecycle != ServiceAccountLifecycle::Active {
            return Err(ServiceAccountError::Disabled);
        }
        self.ensure_active_project(project_id)?;
        account
            .project_role(project_id.as_str())
            .ok_or(ServiceAccountError::AssignmentNotFound)
    }

    fn ensure_active_project(&self, project_id: &ProjectId) -> Result<(), ServiceAccountError> {
        let store = ControlPlaneStore::open(self.control_plane_path.as_path())
            .map_err(|error| ServiceAccountError::ControlPlane(error.to_string()))?;
        let project = store
            .project(project_id)
            .map_err(|error| ServiceAccountError::ControlPlane(error.to_string()))?
            .ok_or(ServiceAccountError::ProjectNotFound)?;
        if project.lifecycle() != ResourceLifecycleState::Active {
            return Err(ServiceAccountError::ProjectUnavailable);
        }
        Ok(())
    }

    fn audit_account(&self, action: &str, account: &ServiceAccount, project_id: Option<&str>) {
        let mut event = AuditEvent::new(
            AuditCategory::Authorization,
            action,
            AuditResult::Allowed,
            AuditOrigin::Internal,
        )
        .with_actor(format!("service-account:{}", account.id.as_str()))
        .with_resource("service_account", account.id.as_str());
        if let Some(project_id) = project_id {
            event = event.with_project(project_id);
            if let Ok(store) = ControlPlaneStore::open(self.control_plane_path.as_path())
                && let Ok(project_id) = ProjectId::new(project_id)
                && let Ok(organization) = store.resolve_project_organization(&project_id)
            {
                event = event.with_organization(organization.id().as_str());
            }
        }
        let _ = self.audit.record(&event);
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, ServiceAccountFile>, ServiceAccountError> {
        self.state
            .lock()
            .map_err(|_| ServiceAccountError::LockPoisoned)
    }

    fn persist(&self, state: &ServiceAccountFile) -> Result<(), ServiceAccountError> {
        let parent = self.path.parent().expect("service account path has parent");
        fs::create_dir_all(parent)?;
        let temp = self.path.with_extension("json.tmp");
        fs::write(&temp, serde_json::to_vec_pretty(state)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))?;
        }
        fs::rename(temp, self.path.as_path())?;
        Ok(())
    }
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
    use ketebe_core::{Organization, OrganizationId, Project, ResourceTimestamps};

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ketebe-service-account-{label}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    fn provision_project(dir: &Path, organization: &str, project: &str) {
        let control_plane = ControlPlaneStore::open(dir.join("control-plane")).unwrap();
        let organization_id = OrganizationId::new(organization).unwrap();
        if control_plane
            .organization(&organization_id)
            .unwrap()
            .is_none()
        {
            control_plane
                .create_organization(
                    Organization::new(
                        organization_id.clone(),
                        organization,
                        organization,
                        ResourceLifecycleState::Active,
                        ResourceTimestamps::new(10, 10).unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let project_id = ProjectId::new(project).unwrap();
        control_plane
            .create_project(
                Project::new(
                    project_id,
                    organization_id,
                    project,
                    project,
                    ResourceLifecycleState::Active,
                    ResourceTimestamps::new(10, 10).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
    }

    #[test]
    fn service_account_is_durable_and_secret_free() {
        let dir = temp_dir("durable");
        provision_project(&dir, "org-a", "project-a");
        let store = ServiceAccountStore::open(&dir).unwrap();
        let id = ServiceAccountId::new("svc-search").unwrap();
        store.create(id.clone(), "Search Worker").unwrap();
        store
            .assign_project(
                &id,
                &ProjectId::new("project-a").unwrap(),
                ProjectRole::Editor,
            )
            .unwrap();
        drop(store);

        let reopened = ServiceAccountStore::open(&dir).unwrap();
        let account = reopened.account(&id).unwrap();
        assert_eq!(account.name(), "Search Worker");
        assert_eq!(account.project_role("project-a"), Some(ProjectRole::Editor));
        let text = fs::read_to_string(dir.join("security/service-accounts.json")).unwrap();
        assert!(!text.contains("secret"));
        assert!(!text.contains("credential"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn assignment_is_explicit_and_cross_project_misuse_fails() {
        let dir = temp_dir("cross-project");
        provision_project(&dir, "org-a", "project-a");
        provision_project(&dir, "org-a", "project-b");
        let store = ServiceAccountStore::open(&dir).unwrap();
        let id = ServiceAccountId::new("svc-search").unwrap();
        store.create(id.clone(), "Search Worker").unwrap();
        store
            .assign_project(
                &id,
                &ProjectId::new("project-a").unwrap(),
                ProjectRole::Reader,
            )
            .unwrap();

        assert_eq!(
            store
                .authorize_project(&id, &ProjectId::new("project-a").unwrap())
                .unwrap(),
            ProjectRole::Reader
        );
        assert!(matches!(
            store.authorize_project(&id, &ProjectId::new("project-b").unwrap()),
            Err(ServiceAccountError::AssignmentNotFound)
        ));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn disabling_or_deleting_service_account_denies_future_admission() {
        let dir = temp_dir("disable");
        provision_project(&dir, "org-a", "project-a");
        let store = ServiceAccountStore::open(&dir).unwrap();
        let id = ServiceAccountId::new("svc-search").unwrap();
        store.create(id.clone(), "Search Worker").unwrap();
        store
            .assign_project(
                &id,
                &ProjectId::new("project-a").unwrap(),
                ProjectRole::Editor,
            )
            .unwrap();

        store
            .set_lifecycle(&id, ServiceAccountLifecycle::Disabled)
            .unwrap();
        assert!(matches!(
            store.authorize_project(&id, &ProjectId::new("project-a").unwrap()),
            Err(ServiceAccountError::Disabled)
        ));

        store.delete(&id).unwrap();
        assert!(matches!(
            store.authorize_project(&id, &ProjectId::new("project-a").unwrap()),
            Err(ServiceAccountError::NotFound)
        ));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn lifecycle_and_assignment_events_are_audited_with_project_and_organization() {
        let dir = temp_dir("audit");
        provision_project(&dir, "org-a", "project-a");
        let audit = AuditService::durable(&dir).unwrap();
        let store = ServiceAccountStore::open(&dir).unwrap().with_audit(audit);
        let id = ServiceAccountId::new("svc-search").unwrap();
        store.create(id.clone(), "Search Worker").unwrap();
        store
            .assign_project(
                &id,
                &ProjectId::new("project-a").unwrap(),
                ProjectRole::Owner,
            )
            .unwrap();
        store
            .set_lifecycle(&id, ServiceAccountLifecycle::Disabled)
            .unwrap();

        let text = fs::read_to_string(dir.join("security/audit.jsonl")).unwrap();
        assert!(text.contains("service_account_create"));
        assert!(text.contains("service_account_project_assign"));
        assert!(text.contains("service_account_lifecycle"));
        assert!(text.contains("service-account:svc-search"));
        assert!(text.contains("project-a"));
        assert!(text.contains("org-a"));
        assert!(!text.contains("secret"));
        let _ = fs::remove_dir_all(dir);
    }
}
