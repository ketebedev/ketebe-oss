use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ketebe_core::OrganizationId;
use ketebe_storage::{ControlPlaneStore, ControlPlaneStoreError};
use serde::{Deserialize, Serialize};

use crate::{
    AuditCategory, AuditContext, AuditEvent, AuditOrigin, AuditResult, AuditService, Principal,
};

const STORE_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationRole {
    Owner,
    Admin,
    Member,
    BillingAdmin,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrganizationAction {
    OrganizationRead,
    OrganizationUpdate,
    MembershipRead,
    MembershipWrite,
    BillingManage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrganizationMembership {
    organization_id: String,
    subject: String,
    role: OrganizationRole,
}

impl OrganizationMembership {
    pub fn new(
        organization_id: &OrganizationId,
        subject: impl Into<String>,
        role: OrganizationRole,
    ) -> Result<Self, OrganizationMembershipError> {
        let subject = subject.into();
        if subject.trim().is_empty() {
            return Err(OrganizationMembershipError::InvalidSubject);
        }
        Ok(Self {
            organization_id: organization_id.as_str().to_string(),
            subject,
            role,
        })
    }

    #[must_use]
    pub fn organization_id(&self) -> &str {
        &self.organization_id
    }

    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    #[must_use]
    pub fn role(&self) -> OrganizationRole {
        self.role
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct MembershipFile {
    version: u32,
    memberships: BTreeMap<String, BTreeMap<String, OrganizationMembership>>,
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
pub enum OrganizationMembershipError {
    InvalidSubject,
    OrganizationNotFound,
    Undiscoverable,
    MembershipAlreadyExists,
    MembershipNotFound,
    LastOwner,
    Io(std::io::Error),
    Json(serde_json::Error),
    UnsupportedVersion(u32),
    ControlPlane(ControlPlaneStoreError),
    LockPoisoned,
}

impl fmt::Display for OrganizationMembershipError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSubject => {
                formatter.write_str("organization membership subject is invalid")
            }
            Self::OrganizationNotFound => formatter.write_str("organization was not found"),
            Self::Undiscoverable => {
                formatter.write_str("organization membership is not discoverable")
            }
            Self::MembershipAlreadyExists => {
                formatter.write_str("organization membership already exists")
            }
            Self::MembershipNotFound => {
                formatter.write_str("organization membership was not found")
            }
            Self::LastOwner => formatter.write_str("organization must retain at least one owner"),
            Self::Io(error) => write!(formatter, "organization membership I/O error: {error}"),
            Self::Json(error) => write!(formatter, "organization membership JSON error: {error}"),
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported organization membership store version {version}"
                )
            }
            Self::ControlPlane(error) => write!(
                formatter,
                "organization membership control-plane error: {error}"
            ),
            Self::LockPoisoned => {
                formatter.write_str("organization membership store lock poisoned")
            }
        }
    }
}

impl std::error::Error for OrganizationMembershipError {}

impl From<std::io::Error> for OrganizationMembershipError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for OrganizationMembershipError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<ControlPlaneStoreError> for OrganizationMembershipError {
    fn from(value: ControlPlaneStoreError) -> Self {
        Self::ControlPlane(value)
    }
}

#[derive(Clone)]
pub struct OrganizationMembershipService {
    path: Arc<PathBuf>,
    control_plane: ControlPlaneStore,
    audit: Arc<AuditService>,
    audit_context: AuditContext,
    state: Arc<Mutex<MembershipFile>>,
}

impl fmt::Debug for OrganizationMembershipService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrganizationMembershipService")
            .finish_non_exhaustive()
    }
}

impl OrganizationMembershipService {
    pub fn open(
        data_dir: impl AsRef<Path>,
        control_plane: ControlPlaneStore,
        audit: Arc<AuditService>,
    ) -> Result<Self, OrganizationMembershipError> {
        let path = data_dir
            .as_ref()
            .join("security")
            .join("organization-memberships.json");
        let state = if path.exists() {
            let decoded: MembershipFile = serde_json::from_slice(&fs::read(&path)?)?;
            if decoded.version != STORE_VERSION {
                return Err(OrganizationMembershipError::UnsupportedVersion(
                    decoded.version,
                ));
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
        organization_id: &OrganizationId,
        subject: &str,
    ) -> Result<OrganizationMembership, OrganizationMembershipError> {
        self.ensure_organization_exists(organization_id)?;
        if subject.trim().is_empty() {
            return Err(OrganizationMembershipError::InvalidSubject);
        }

        let mut state = self.lock()?;
        if state
            .memberships
            .get(organization_id.as_str())
            .is_some_and(|memberships| !memberships.is_empty())
        {
            return Err(OrganizationMembershipError::MembershipAlreadyExists);
        }

        let membership =
            OrganizationMembership::new(organization_id, subject, OrganizationRole::Owner)?;
        let mut next = state.clone();
        next.memberships
            .entry(organization_id.as_str().to_string())
            .or_default()
            .insert(subject.to_string(), membership.clone());
        self.persist(&next)?;
        *state = next;
        self.audit_change("organization_membership_bootstrap", subject, &membership);
        Ok(membership)
    }

    pub fn create_membership(
        &self,
        actor: &Principal,
        organization_id: &OrganizationId,
        subject: &str,
        role: OrganizationRole,
    ) -> Result<OrganizationMembership, OrganizationMembershipError> {
        self.authorize(actor, organization_id, OrganizationAction::MembershipWrite)?;
        let membership = OrganizationMembership::new(organization_id, subject, role)?;
        let mut state = self.lock()?;
        if state
            .memberships
            .get(organization_id.as_str())
            .is_some_and(|memberships| memberships.contains_key(subject))
        {
            return Err(OrganizationMembershipError::MembershipAlreadyExists);
        }
        let mut next = state.clone();
        next.memberships
            .entry(organization_id.as_str().to_string())
            .or_default()
            .insert(subject.to_string(), membership.clone());
        self.persist(&next)?;
        *state = next;
        self.audit_change(
            "organization_membership_create",
            actor.subject(),
            &membership,
        );
        Ok(membership)
    }

    pub fn update_role(
        &self,
        actor: &Principal,
        organization_id: &OrganizationId,
        subject: &str,
        role: OrganizationRole,
    ) -> Result<OrganizationMembership, OrganizationMembershipError> {
        self.authorize(actor, organization_id, OrganizationAction::MembershipWrite)?;
        let mut state = self.lock()?;
        let organization_memberships = state
            .memberships
            .get(organization_id.as_str())
            .ok_or(OrganizationMembershipError::Undiscoverable)?;
        let current = organization_memberships
            .get(subject)
            .cloned()
            .ok_or(OrganizationMembershipError::Undiscoverable)?;
        if current.role() == OrganizationRole::Owner
            && role != OrganizationRole::Owner
            && owner_count(organization_memberships) == 1
        {
            return Err(OrganizationMembershipError::LastOwner);
        }

        let updated = OrganizationMembership::new(organization_id, subject, role)?;
        let mut next = state.clone();
        next.memberships
            .get_mut(organization_id.as_str())
            .expect("validated organization membership state")
            .insert(subject.to_string(), updated.clone());
        self.persist(&next)?;
        *state = next;
        self.audit_change(
            "organization_membership_role_update",
            actor.subject(),
            &updated,
        );
        Ok(updated)
    }

    pub fn remove_membership(
        &self,
        actor: &Principal,
        organization_id: &OrganizationId,
        subject: &str,
    ) -> Result<(), OrganizationMembershipError> {
        self.authorize(actor, organization_id, OrganizationAction::MembershipWrite)?;
        let mut state = self.lock()?;
        let organization_memberships = state
            .memberships
            .get(organization_id.as_str())
            .ok_or(OrganizationMembershipError::Undiscoverable)?;
        let membership = organization_memberships
            .get(subject)
            .cloned()
            .ok_or(OrganizationMembershipError::Undiscoverable)?;
        if membership.role() == OrganizationRole::Owner
            && owner_count(organization_memberships) == 1
        {
            return Err(OrganizationMembershipError::LastOwner);
        }

        let mut next = state.clone();
        next.memberships
            .get_mut(organization_id.as_str())
            .expect("validated organization membership state")
            .remove(subject);
        self.persist(&next)?;
        *state = next;
        self.audit_change(
            "organization_membership_remove",
            actor.subject(),
            &membership,
        );
        Ok(())
    }

    pub fn list_memberships(
        &self,
        actor: &Principal,
        organization_id: &OrganizationId,
    ) -> Result<Vec<OrganizationMembership>, OrganizationMembershipError> {
        self.authorize(actor, organization_id, OrganizationAction::MembershipRead)?;
        let state = self.lock()?;
        Ok(state
            .memberships
            .get(organization_id.as_str())
            .map(|memberships| memberships.values().cloned().collect())
            .unwrap_or_default())
    }

    pub fn membership(
        &self,
        actor: &Principal,
        organization_id: &OrganizationId,
        subject: &str,
    ) -> Result<OrganizationMembership, OrganizationMembershipError> {
        self.authorize(actor, organization_id, OrganizationAction::MembershipRead)?;
        let state = self.lock()?;
        state
            .memberships
            .get(organization_id.as_str())
            .and_then(|memberships| memberships.get(subject))
            .cloned()
            .ok_or(OrganizationMembershipError::Undiscoverable)
    }

    pub fn authorize(
        &self,
        principal: &Principal,
        organization_id: &OrganizationId,
        action: OrganizationAction,
    ) -> Result<(), OrganizationMembershipError> {
        if self.control_plane.organization(organization_id)?.is_none() {
            return Err(OrganizationMembershipError::Undiscoverable);
        }
        let state = self.lock()?;
        let membership = state
            .memberships
            .get(organization_id.as_str())
            .and_then(|memberships| memberships.get(principal.subject()))
            .ok_or(OrganizationMembershipError::Undiscoverable)?;
        if role_allows(membership.role(), action) {
            Ok(())
        } else {
            Err(OrganizationMembershipError::Undiscoverable)
        }
    }

    fn ensure_organization_exists(
        &self,
        organization_id: &OrganizationId,
    ) -> Result<(), OrganizationMembershipError> {
        if self.control_plane.organization(organization_id)?.is_none() {
            return Err(OrganizationMembershipError::OrganizationNotFound);
        }
        Ok(())
    }

    pub(crate) fn purge_organization(
        &self,
        organization_id: &OrganizationId,
    ) -> Result<(), OrganizationMembershipError> {
        let mut state = self.lock()?;
        if !state.memberships.contains_key(organization_id.as_str()) {
            return Ok(());
        }
        let mut next = state.clone();
        next.memberships.remove(organization_id.as_str());
        self.persist(&next)?;
        *state = next;
        Ok(())
    }

    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, MembershipFile>, OrganizationMembershipError> {
        self.state
            .lock()
            .map_err(|_| OrganizationMembershipError::LockPoisoned)
    }

    fn persist(&self, state: &MembershipFile) -> Result<(), OrganizationMembershipError> {
        let parent = self
            .path
            .parent()
            .expect("organization membership path has parent");
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

    fn audit_change(&self, action: &str, actor_subject: &str, membership: &OrganizationMembership) {
        let event = self.audit_context.apply(
            AuditEvent::new(
                AuditCategory::Authorization,
                action,
                AuditResult::Allowed,
                AuditOrigin::Internal,
            )
            .with_actor(actor_subject)
            .with_organization(membership.organization_id())
            .with_resource(
                "organization_membership",
                format!("{}/{}", membership.organization_id(), membership.subject()),
            ),
        );
        let _ = self.audit.record(&event);
    }
}

fn owner_count(memberships: &BTreeMap<String, OrganizationMembership>) -> usize {
    memberships
        .values()
        .filter(|membership| membership.role() == OrganizationRole::Owner)
        .count()
}

fn role_allows(role: OrganizationRole, action: OrganizationAction) -> bool {
    match role {
        OrganizationRole::Owner => true,
        OrganizationRole::Admin => !matches!(action, OrganizationAction::BillingManage),
        OrganizationRole::Member => matches!(action, OrganizationAction::OrganizationRead),
        OrganizationRole::BillingAdmin => matches!(
            action,
            OrganizationAction::OrganizationRead | OrganizationAction::BillingManage
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuditError, AuditSink, AuthorizationAction, AuthorizationService, PrincipalKind};
    use ketebe_core::{Organization, ResourceLifecycleState, ResourceTimestamps};
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
            "ketebe-organization-membership-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn setup(
        label: &str,
    ) -> (
        PathBuf,
        OrganizationId,
        ControlPlaneStore,
        OrganizationMembershipService,
    ) {
        let dir = temp_dir(label);
        let organization_id = OrganizationId::new("org-a").unwrap();
        let control_plane = ControlPlaneStore::open(dir.join("control-plane")).unwrap();
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
        let service = OrganizationMembershipService::open(
            &dir,
            control_plane.clone(),
            Arc::new(AuditService::noop()),
        )
        .unwrap();
        (dir, organization_id, control_plane, service)
    }

    #[test]
    fn membership_persists_across_restart() {
        let (dir, organization_id, control_plane, service) = setup("restart");
        service
            .bootstrap_owner(&organization_id, "owner-a")
            .unwrap();
        drop(service);

        let reopened = OrganizationMembershipService::open(
            &dir,
            control_plane,
            Arc::new(AuditService::noop()),
        )
        .unwrap();
        let owner = Principal::new("owner-a", PrincipalKind::Credential).unwrap();
        assert_eq!(
            reopened
                .membership(&owner, &organization_id, "owner-a")
                .unwrap()
                .role(),
            OrganizationRole::Owner
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn last_owner_cannot_be_removed_or_downgraded() {
        let (dir, organization_id, _, service) = setup("last-owner");
        service
            .bootstrap_owner(&organization_id, "owner-a")
            .unwrap();
        let owner = Principal::new("owner-a", PrincipalKind::Credential).unwrap();

        assert!(matches!(
            service.update_role(&owner, &organization_id, "owner-a", OrganizationRole::Admin),
            Err(OrganizationMembershipError::LastOwner)
        ));
        assert!(matches!(
            service.remove_membership(&owner, &organization_id, "owner-a"),
            Err(OrganizationMembershipError::LastOwner)
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn organization_membership_does_not_grant_project_data_access() {
        let (dir, organization_id, _, service) = setup("no-project-access");
        service
            .bootstrap_owner(&organization_id, "owner-a")
            .unwrap();
        let owner = Principal::new("owner-a", PrincipalKind::Credential).unwrap();
        assert!(
            service
                .authorize(
                    &owner,
                    &organization_id,
                    OrganizationAction::OrganizationUpdate
                )
                .is_ok()
        );

        let authorization = AuthorizationService::required(&dir).unwrap();
        assert!(
            authorization
                .authorize_project(&owner, AuthorizationAction::CollectionRead, "project-a")
                .is_err()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn cross_organization_membership_is_not_discoverable() {
        let (dir, organization_id, control_plane, service) = setup("cross-org");
        service
            .bootstrap_owner(&organization_id, "owner-a")
            .unwrap();

        let organization_b = OrganizationId::new("org-b").unwrap();
        control_plane
            .create_organization(
                Organization::new(
                    organization_b.clone(),
                    "Beta",
                    "beta",
                    ResourceLifecycleState::Active,
                    ResourceTimestamps::new(20, 20).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
        let outsider = Principal::new("owner-a", PrincipalKind::Credential).unwrap();
        assert!(matches!(
            service.membership(&outsider, &organization_b, "owner-a"),
            Err(OrganizationMembershipError::Undiscoverable)
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn admin_and_billing_roles_remain_separate() {
        let (dir, organization_id, _, service) = setup("role-separation");
        service
            .bootstrap_owner(&organization_id, "owner-a")
            .unwrap();
        let owner = Principal::new("owner-a", PrincipalKind::Credential).unwrap();
        service
            .create_membership(&owner, &organization_id, "admin-a", OrganizationRole::Admin)
            .unwrap();
        service
            .create_membership(
                &owner,
                &organization_id,
                "billing-a",
                OrganizationRole::BillingAdmin,
            )
            .unwrap();

        let admin = Principal::new("admin-a", PrincipalKind::Credential).unwrap();
        let billing = Principal::new("billing-a", PrincipalKind::Credential).unwrap();
        assert!(
            service
                .authorize(
                    &admin,
                    &organization_id,
                    OrganizationAction::MembershipWrite
                )
                .is_ok()
        );
        assert!(
            service
                .authorize(&admin, &organization_id, OrganizationAction::BillingManage)
                .is_err()
        );
        assert!(
            service
                .authorize(
                    &billing,
                    &organization_id,
                    OrganizationAction::BillingManage
                )
                .is_ok()
        );
        assert!(
            service
                .authorize(
                    &billing,
                    &organization_id,
                    OrganizationAction::MembershipWrite
                )
                .is_err()
        );
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn membership_lifecycle_changes_are_audited() {
        let dir = temp_dir("audit");
        let organization_id = OrganizationId::new("org-a").unwrap();
        let control_plane = ControlPlaneStore::open(dir.join("control-plane")).unwrap();
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

        let sink = Arc::new(CountingAuditSink {
            count: AtomicUsize::new(0),
        });
        let audit = Arc::new(AuditService::with_sink(sink.clone()));
        let service = OrganizationMembershipService::open(&dir, control_plane, audit).unwrap();

        service
            .bootstrap_owner(&organization_id, "owner-a")
            .unwrap();
        let owner = Principal::new("owner-a", PrincipalKind::Credential).unwrap();
        service
            .create_membership(
                &owner,
                &organization_id,
                "member-a",
                OrganizationRole::Member,
            )
            .unwrap();
        service
            .update_role(
                &owner,
                &organization_id,
                "member-a",
                OrganizationRole::Admin,
            )
            .unwrap();
        service
            .remove_membership(&owner, &organization_id, "member-a")
            .unwrap();

        assert_eq!(sink.count.load(Ordering::SeqCst), 4);
        fs::remove_dir_all(dir).unwrap();
    }
}
