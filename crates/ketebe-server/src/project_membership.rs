use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ketebe_core::{ProjectId, ResourceLifecycleState};
use ketebe_storage::{ControlPlaneStore, ControlPlaneStoreError};
use serde::{Deserialize, Serialize};

use crate::{
    AuditCategory, AuditContext, AuditEvent, AuditOrigin, AuditResult, AuditService,
    AuthorizationAction, CollectionPermission, Principal, ProjectRole,
};

const STORE_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectMembership {
    project_id: String,
    subject: String,
    role: ProjectRole,
}

impl ProjectMembership {
    pub fn new(
        project_id: &ProjectId,
        subject: impl Into<String>,
        role: ProjectRole,
    ) -> Result<Self, ProjectMembershipError> {
        let subject = subject.into();
        if subject.trim().is_empty() {
            return Err(ProjectMembershipError::InvalidSubject);
        }
        Ok(Self {
            project_id: project_id.as_str().to_string(),
            subject,
            role,
        })
    }

    #[must_use]
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    #[must_use]
    pub fn role(&self) -> ProjectRole {
        self.role
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct MembershipFile {
    version: u32,
    memberships: BTreeMap<String, BTreeMap<String, ProjectMembership>>,
}

impl Default for MembershipFile {
    fn default() -> Self {
        Self {
            version: STORE_VERSION,
            memberships: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub enum ProjectMembershipError {
    InvalidSubject,
    ProjectNotFound,
    Undiscoverable,
    MembershipAlreadyExists,
    LastOwner,
    Io(std::io::Error),
    Json(serde_json::Error),
    UnsupportedVersion(u32),
    ControlPlane(ControlPlaneStoreError),
    LockPoisoned,
}

impl fmt::Display for ProjectMembershipError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSubject => formatter.write_str("project membership subject is invalid"),
            Self::ProjectNotFound => formatter.write_str("project was not found"),
            Self::Undiscoverable => formatter.write_str("project membership is not discoverable"),
            Self::MembershipAlreadyExists => {
                formatter.write_str("project membership already exists")
            }
            Self::LastOwner => formatter.write_str("project must retain at least one owner"),
            Self::Io(error) => write!(formatter, "project membership I/O error: {error}"),
            Self::Json(error) => write!(formatter, "project membership JSON error: {error}"),
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported project membership store version {version}"
                )
            }
            Self::ControlPlane(error) => {
                write!(formatter, "project membership control-plane error: {error}")
            }
            Self::LockPoisoned => formatter.write_str("project membership store lock poisoned"),
        }
    }
}

impl std::error::Error for ProjectMembershipError {}

impl From<std::io::Error> for ProjectMembershipError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for ProjectMembershipError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<ControlPlaneStoreError> for ProjectMembershipError {
    fn from(value: ControlPlaneStoreError) -> Self {
        Self::ControlPlane(value)
    }
}

#[derive(Clone)]
pub struct ProjectMembershipService {
    path: Arc<PathBuf>,
    control_plane: ControlPlaneStore,
    audit: Arc<AuditService>,
    audit_context: AuditContext,
    state: Arc<Mutex<MembershipFile>>,
}

impl fmt::Debug for ProjectMembershipService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectMembershipService")
            .finish_non_exhaustive()
    }
}

impl ProjectMembershipService {
    pub fn open(
        data_dir: impl AsRef<Path>,
        control_plane: ControlPlaneStore,
        audit: Arc<AuditService>,
    ) -> Result<Self, ProjectMembershipError> {
        let path = data_dir
            .as_ref()
            .join("security")
            .join("project-memberships.json");
        let state = if path.exists() {
            let decoded: MembershipFile = serde_json::from_slice(&fs::read(&path)?)?;
            if decoded.version != STORE_VERSION {
                return Err(ProjectMembershipError::UnsupportedVersion(decoded.version));
            }
            decoded
        } else {
            MembershipFile::default()
        };
        Ok(Self {
            path: Arc::new(path),
            control_plane,
            audit,
            audit_context: AuditContext::default(),
            state: Arc::new(Mutex::new(state)),
        })
    }

    #[must_use]
    pub fn with_audit_context(mut self, audit_context: AuditContext) -> Self {
        self.audit_context = audit_context;
        self
    }

    pub fn bootstrap_owner(
        &self,
        project_id: &ProjectId,
        subject: &str,
    ) -> Result<ProjectMembership, ProjectMembershipError> {
        self.ensure_project_exists(project_id)?;
        let mut state = self.lock()?;
        if state
            .memberships
            .get(project_id.as_str())
            .is_some_and(|memberships| !memberships.is_empty())
        {
            return Err(ProjectMembershipError::MembershipAlreadyExists);
        }

        let membership = ProjectMembership::new(project_id, subject, ProjectRole::Owner)?;
        let mut next = state.clone();
        next.memberships
            .entry(project_id.as_str().to_string())
            .or_default()
            .insert(subject.to_string(), membership.clone());
        self.persist(&next)?;
        *state = next;
        self.audit_change("project_membership_bootstrap", subject, &membership);
        Ok(membership)
    }

    pub fn create_membership(
        &self,
        actor: &Principal,
        project_id: &ProjectId,
        subject: &str,
        role: ProjectRole,
    ) -> Result<ProjectMembership, ProjectMembershipError> {
        self.authorize(actor, project_id, AuthorizationAction::ProjectAdmin)?;
        let membership = ProjectMembership::new(project_id, subject, role)?;
        let mut state = self.lock()?;
        if state
            .memberships
            .get(project_id.as_str())
            .is_some_and(|memberships| memberships.contains_key(subject))
        {
            return Err(ProjectMembershipError::MembershipAlreadyExists);
        }

        let mut next = state.clone();
        next.memberships
            .entry(project_id.as_str().to_string())
            .or_default()
            .insert(subject.to_string(), membership.clone());
        self.persist(&next)?;
        *state = next;
        self.audit_change("project_membership_create", actor.subject(), &membership);
        Ok(membership)
    }

    pub fn update_role(
        &self,
        actor: &Principal,
        project_id: &ProjectId,
        subject: &str,
        role: ProjectRole,
    ) -> Result<ProjectMembership, ProjectMembershipError> {
        self.authorize(actor, project_id, AuthorizationAction::ProjectAdmin)?;
        let mut state = self.lock()?;
        let memberships = state
            .memberships
            .get(project_id.as_str())
            .ok_or(ProjectMembershipError::Undiscoverable)?;
        let current = memberships
            .get(subject)
            .cloned()
            .ok_or(ProjectMembershipError::Undiscoverable)?;
        if current.role() == ProjectRole::Owner
            && role != ProjectRole::Owner
            && owner_count(memberships) == 1
        {
            return Err(ProjectMembershipError::LastOwner);
        }

        let updated = ProjectMembership::new(project_id, subject, role)?;
        let mut next = state.clone();
        next.memberships
            .get_mut(project_id.as_str())
            .expect("validated project membership state")
            .insert(subject.to_string(), updated.clone());
        self.persist(&next)?;
        *state = next;
        self.audit_change("project_membership_role_update", actor.subject(), &updated);
        Ok(updated)
    }

    pub fn remove_membership(
        &self,
        actor: &Principal,
        project_id: &ProjectId,
        subject: &str,
    ) -> Result<(), ProjectMembershipError> {
        self.authorize(actor, project_id, AuthorizationAction::ProjectAdmin)?;
        let mut state = self.lock()?;
        let memberships = state
            .memberships
            .get(project_id.as_str())
            .ok_or(ProjectMembershipError::Undiscoverable)?;
        let membership = memberships
            .get(subject)
            .cloned()
            .ok_or(ProjectMembershipError::Undiscoverable)?;
        if membership.role() == ProjectRole::Owner && owner_count(memberships) == 1 {
            return Err(ProjectMembershipError::LastOwner);
        }

        let mut next = state.clone();
        next.memberships
            .get_mut(project_id.as_str())
            .expect("validated project membership state")
            .remove(subject);
        self.persist(&next)?;
        *state = next;
        self.audit_change("project_membership_remove", actor.subject(), &membership);
        Ok(())
    }

    pub fn list_memberships(
        &self,
        actor: &Principal,
        project_id: &ProjectId,
    ) -> Result<Vec<ProjectMembership>, ProjectMembershipError> {
        self.authorize(actor, project_id, AuthorizationAction::ProjectAdmin)?;
        let state = self.lock()?;
        Ok(state
            .memberships
            .get(project_id.as_str())
            .map(|memberships| memberships.values().cloned().collect())
            .unwrap_or_default())
    }

    pub fn membership(
        &self,
        actor: &Principal,
        project_id: &ProjectId,
        subject: &str,
    ) -> Result<ProjectMembership, ProjectMembershipError> {
        self.authorize(actor, project_id, AuthorizationAction::ProjectAdmin)?;
        let state = self.lock()?;
        state
            .memberships
            .get(project_id.as_str())
            .and_then(|memberships| memberships.get(subject))
            .cloned()
            .ok_or(ProjectMembershipError::Undiscoverable)
    }

    pub fn authorize(
        &self,
        principal: &Principal,
        project_id: &ProjectId,
        action: AuthorizationAction,
    ) -> Result<(), ProjectMembershipError> {
        let project = self
            .control_plane
            .project(project_id)?
            .ok_or(ProjectMembershipError::Undiscoverable)?;
        if project.lifecycle() != ResourceLifecycleState::Active {
            return Err(ProjectMembershipError::Undiscoverable);
        }
        let state = self.lock()?;
        let membership = state
            .memberships
            .get(project_id.as_str())
            .and_then(|memberships| memberships.get(principal.subject()))
            .ok_or(ProjectMembershipError::Undiscoverable)?;
        if project_role_allows(membership.role(), action) {
            Ok(())
        } else {
            Err(ProjectMembershipError::Undiscoverable)
        }
    }

    pub fn authorize_collection(
        &self,
        principal: &Principal,
        project_id: &ProjectId,
        action: AuthorizationAction,
        override_permission: Option<CollectionPermission>,
    ) -> Result<(), ProjectMembershipError> {
        let project = self
            .control_plane
            .project(project_id)?
            .ok_or(ProjectMembershipError::Undiscoverable)?;
        if project.lifecycle() != ResourceLifecycleState::Active {
            return Err(ProjectMembershipError::Undiscoverable);
        }
        let state = self.lock()?;
        let membership = state
            .memberships
            .get(project_id.as_str())
            .and_then(|memberships| memberships.get(principal.subject()))
            .ok_or(ProjectMembershipError::Undiscoverable)?;

        let allowed = override_permission.map_or_else(
            || project_role_allows(membership.role(), action),
            |permission| collection_permission_allows(permission, action),
        );
        if allowed {
            Ok(())
        } else {
            Err(ProjectMembershipError::Undiscoverable)
        }
    }

    fn ensure_project_exists(&self, project_id: &ProjectId) -> Result<(), ProjectMembershipError> {
        if self.control_plane.project(project_id)?.is_none() {
            return Err(ProjectMembershipError::ProjectNotFound);
        }
        Ok(())
    }

    pub(crate) fn purge_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<(), ProjectMembershipError> {
        let mut state = self.lock()?;
        if !state.memberships.contains_key(project_id.as_str()) {
            return Ok(());
        }
        let mut next = state.clone();
        next.memberships.remove(project_id.as_str());
        self.persist(&next)?;
        *state = next;
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, MembershipFile>, ProjectMembershipError> {
        self.state
            .lock()
            .map_err(|_| ProjectMembershipError::LockPoisoned)
    }

    fn persist(&self, state: &MembershipFile) -> Result<(), ProjectMembershipError> {
        let parent = self
            .path
            .parent()
            .expect("project membership path has parent");
        fs::create_dir_all(parent)?;
        let temp = self.path.with_extension("json.tmp");
        fs::write(&temp, serde_json::to_vec_pretty(state)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))?;
        }
        fs::rename(temp, self.path.as_ref())?;
        Ok(())
    }

    fn audit_change(&self, action: &str, actor_subject: &str, membership: &ProjectMembership) {
        let mut event = AuditEvent::new(
            AuditCategory::Authorization,
            action,
            AuditResult::Allowed,
            AuditOrigin::Internal,
        )
        .with_actor(actor_subject)
        .with_project(membership.project_id())
        .with_resource(
            "project_membership",
            format!("{}/{}", membership.project_id(), membership.subject()),
        );
        if let Ok(project_id) = ProjectId::new(membership.project_id().to_string())
            && let Ok(organization) = self.control_plane.resolve_project_organization(&project_id)
        {
            event = event.with_organization(organization.id().as_str());
        }
        let event = self.audit_context.apply(event);
        let _ = self.audit.record(&event);
    }
}

fn owner_count(memberships: &BTreeMap<String, ProjectMembership>) -> usize {
    memberships
        .values()
        .filter(|membership| membership.role() == ProjectRole::Owner)
        .count()
}

fn project_role_allows(role: ProjectRole, action: AuthorizationAction) -> bool {
    match role {
        ProjectRole::Owner => true,
        ProjectRole::Editor => matches!(
            action,
            AuthorizationAction::CollectionDiscover
                | AuthorizationAction::CollectionRead
                | AuthorizationAction::CollectionCreate
                | AuthorizationAction::CollectionWrite
        ),
        ProjectRole::Reader => matches!(
            action,
            AuthorizationAction::CollectionDiscover | AuthorizationAction::CollectionRead
        ),
    }
}

fn collection_permission_allows(
    permission: CollectionPermission,
    action: AuthorizationAction,
) -> bool {
    match permission {
        CollectionPermission::Admin => !matches!(action, AuthorizationAction::ProjectAdmin),
        CollectionPermission::Write => matches!(
            action,
            AuthorizationAction::CollectionDiscover
                | AuthorizationAction::CollectionRead
                | AuthorizationAction::CollectionWrite
        ),
        CollectionPermission::Read => matches!(
            action,
            AuthorizationAction::CollectionDiscover | AuthorizationAction::CollectionRead
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuditError, AuditSink, PrincipalKind};
    use ketebe_core::{
        Organization, OrganizationId, Project, ResourceLifecycleState, ResourceTimestamps,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct CountingAuditSink {
        count: AtomicUsize,
    }

    impl AuditSink for CountingAuditSink {
        fn append(&self, _event: &AuditEvent) -> Result<(), AuditError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ketebe-project-membership-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn setup(
        label: &str,
    ) -> (
        PathBuf,
        ProjectId,
        ProjectId,
        ControlPlaneStore,
        ProjectMembershipService,
    ) {
        let dir = temp_dir(label);
        let control_plane = ControlPlaneStore::open(dir.join("control-plane")).unwrap();
        let organization_id = OrganizationId::new("org-a").unwrap();
        let timestamps = ResourceTimestamps::new(10, 10).unwrap();
        control_plane
            .create_organization(
                Organization::new(
                    organization_id.clone(),
                    "ACME",
                    "acme",
                    ResourceLifecycleState::Active,
                    timestamps,
                )
                .unwrap(),
            )
            .unwrap();

        let project_a = ProjectId::new("project-a").unwrap();
        let project_b = ProjectId::new("project-b").unwrap();
        control_plane
            .create_project(
                Project::new(
                    project_a.clone(),
                    organization_id.clone(),
                    "Search",
                    "search",
                    ResourceLifecycleState::Active,
                    timestamps,
                )
                .unwrap(),
            )
            .unwrap();
        control_plane
            .create_project(
                Project::new(
                    project_b.clone(),
                    organization_id,
                    "Support",
                    "support",
                    ResourceLifecycleState::Active,
                    timestamps,
                )
                .unwrap(),
            )
            .unwrap();

        let service = ProjectMembershipService::open(
            &dir,
            control_plane.clone(),
            Arc::new(AuditService::noop()),
        )
        .unwrap();
        (dir, project_a, project_b, control_plane, service)
    }

    #[test]
    fn membership_persists_across_restart() {
        let (dir, project_a, _, control_plane, service) = setup("restart");
        service.bootstrap_owner(&project_a, "owner-a").unwrap();
        drop(service);

        let reopened =
            ProjectMembershipService::open(&dir, control_plane, Arc::new(AuditService::noop()))
                .unwrap();
        let owner = Principal::new("owner-a", PrincipalKind::Credential).unwrap();
        assert_eq!(
            reopened
                .membership(&owner, &project_a, "owner-a")
                .unwrap()
                .role(),
            ProjectRole::Owner
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn one_subject_can_hold_different_roles_across_projects() {
        let (dir, project_a, project_b, _, service) = setup("multi-project");
        service.bootstrap_owner(&project_a, "owner-a").unwrap();
        service.bootstrap_owner(&project_b, "owner-b").unwrap();

        let owner_a = Principal::new("owner-a", PrincipalKind::Credential).unwrap();
        let owner_b = Principal::new("owner-b", PrincipalKind::Credential).unwrap();
        service
            .create_membership(&owner_a, &project_a, "user-a", ProjectRole::Reader)
            .unwrap();
        service
            .create_membership(&owner_b, &project_b, "user-a", ProjectRole::Editor)
            .unwrap();

        let user = Principal::new("user-a", PrincipalKind::Credential).unwrap();
        assert!(
            service
                .authorize(&user, &project_a, AuthorizationAction::CollectionRead)
                .is_ok()
        );
        assert!(
            service
                .authorize(&user, &project_a, AuthorizationAction::CollectionWrite)
                .is_err()
        );
        assert!(
            service
                .authorize(&user, &project_b, AuthorizationAction::CollectionWrite)
                .is_ok()
        );
        assert!(
            service
                .authorize(&user, &project_b, AuthorizationAction::ProjectAdmin)
                .is_err()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn collection_override_composes_with_project_membership() {
        let (dir, project_a, _, _, service) = setup("collection-override");
        service.bootstrap_owner(&project_a, "owner-a").unwrap();
        let owner = Principal::new("owner-a", PrincipalKind::Credential).unwrap();
        service
            .create_membership(&owner, &project_a, "editor-a", ProjectRole::Editor)
            .unwrap();
        let editor = Principal::new("editor-a", PrincipalKind::Credential).unwrap();

        assert!(
            service
                .authorize_collection(
                    &editor,
                    &project_a,
                    AuthorizationAction::CollectionWrite,
                    None
                )
                .is_ok()
        );
        assert!(
            service
                .authorize_collection(
                    &editor,
                    &project_a,
                    AuthorizationAction::CollectionWrite,
                    Some(CollectionPermission::Read)
                )
                .is_err()
        );
        assert!(
            service
                .authorize_collection(
                    &editor,
                    &project_a,
                    AuthorizationAction::CollectionRead,
                    Some(CollectionPermission::Read)
                )
                .is_ok()
        );
        assert!(
            service
                .authorize_collection(
                    &editor,
                    &project_a,
                    AuthorizationAction::ProjectAdmin,
                    Some(CollectionPermission::Admin)
                )
                .is_err()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn cross_project_membership_is_not_discoverable() {
        let (dir, project_a, project_b, _, service) = setup("cross-project");
        service.bootstrap_owner(&project_a, "owner-a").unwrap();
        service.bootstrap_owner(&project_b, "owner-b").unwrap();

        let owner_a = Principal::new("owner-a", PrincipalKind::Credential).unwrap();
        assert!(matches!(
            service.membership(&owner_a, &project_b, "owner-b"),
            Err(ProjectMembershipError::Undiscoverable)
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn last_owner_cannot_be_removed_or_downgraded() {
        let (dir, project_a, _, _, service) = setup("last-owner");
        service.bootstrap_owner(&project_a, "owner-a").unwrap();
        let owner = Principal::new("owner-a", PrincipalKind::Credential).unwrap();

        assert!(matches!(
            service.update_role(&owner, &project_a, "owner-a", ProjectRole::Editor),
            Err(ProjectMembershipError::LastOwner)
        ));
        assert!(matches!(
            service.remove_membership(&owner, &project_a, "owner-a"),
            Err(ProjectMembershipError::LastOwner)
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn membership_lifecycle_changes_are_audited() {
        let dir = temp_dir("audit");
        let control_plane = ControlPlaneStore::open(dir.join("control-plane")).unwrap();
        let organization_id = OrganizationId::new("org-a").unwrap();
        let timestamps = ResourceTimestamps::new(10, 10).unwrap();
        control_plane
            .create_organization(
                Organization::new(
                    organization_id.clone(),
                    "ACME",
                    "acme",
                    ResourceLifecycleState::Active,
                    timestamps,
                )
                .unwrap(),
            )
            .unwrap();
        let project_id = ProjectId::new("project-a").unwrap();
        control_plane
            .create_project(
                Project::new(
                    project_id.clone(),
                    organization_id,
                    "Search",
                    "search",
                    ResourceLifecycleState::Active,
                    timestamps,
                )
                .unwrap(),
            )
            .unwrap();

        let sink = Arc::new(CountingAuditSink {
            count: AtomicUsize::new(0),
        });
        let service = ProjectMembershipService::open(
            &dir,
            control_plane,
            Arc::new(AuditService::with_sink(sink.clone())),
        )
        .unwrap();

        service.bootstrap_owner(&project_id, "owner-a").unwrap();
        let owner = Principal::new("owner-a", PrincipalKind::Credential).unwrap();
        service
            .create_membership(&owner, &project_id, "member-a", ProjectRole::Reader)
            .unwrap();
        service
            .update_role(&owner, &project_id, "member-a", ProjectRole::Editor)
            .unwrap();
        service
            .remove_membership(&owner, &project_id, "member-a")
            .unwrap();

        assert_eq!(sink.count.load(Ordering::SeqCst), 4);
        fs::remove_dir_all(dir).unwrap();
    }
}
