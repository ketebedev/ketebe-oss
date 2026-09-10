use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ketebe_core::{OrganizationId, ProjectId};
use ketebe_storage::ControlPlaneStore;
use serde::{Deserialize, Serialize};

use crate::{
    GovernanceError, GovernancePolicy, GovernanceService, InMemoryResourceGovernor, ProjectQuota,
    ProjectResourceBudget, RateLimit, ResourceGovernanceError, ThroughputBudget,
};

const STORE_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrganizationGovernancePolicy {
    pub governance: GovernancePolicy,
    pub resources: ProjectResourceBudget,
    pub project_overrides_allowed: bool,
}

impl OrganizationGovernancePolicy {
    pub fn new(
        governance: GovernancePolicy,
        resources: ProjectResourceBudget,
        project_overrides_allowed: bool,
    ) -> Result<Self, OrganizationGovernanceError> {
        governance
            .validate()
            .map_err(OrganizationGovernanceError::Governance)?;
        resources
            .validate()
            .map_err(OrganizationGovernanceError::Resource)?;
        Ok(Self {
            governance,
            resources,
            project_overrides_allowed,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectGovernanceOverride {
    pub governance: Option<GovernancePolicy>,
    pub resources: Option<ProjectResourceBudget>,
}

impl ProjectGovernanceOverride {
    pub fn validate(&self) -> Result<(), OrganizationGovernanceError> {
        if let Some(governance) = &self.governance {
            governance
                .validate()
                .map_err(OrganizationGovernanceError::Governance)?;
        }
        if let Some(resources) = self.resources {
            resources
                .validate()
                .map_err(OrganizationGovernanceError::Resource)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveProjectGovernancePolicy {
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub governance: GovernancePolicy,
    pub resources: ProjectResourceBudget,
    pub constrained_by_organization: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GovernancePolicyFile {
    version: u32,
    organization_policies: BTreeMap<String, OrganizationGovernancePolicy>,
    project_overrides: BTreeMap<String, ProjectGovernanceOverride>,
}

impl Default for GovernancePolicyFile {
    fn default() -> Self {
        Self {
            version: STORE_VERSION,
            organization_policies: BTreeMap::new(),
            project_overrides: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub enum OrganizationGovernanceError {
    OrganizationNotFound,
    ProjectNotFound,
    ProjectOverridesDisabled,
    UnsupportedVersion(u32),
    ControlPlane(String),
    Governance(GovernanceError),
    Resource(ResourceGovernanceError),
    Io(std::io::Error),
    Json(serde_json::Error),
    LockPoisoned,
}

impl fmt::Display for OrganizationGovernanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OrganizationNotFound => formatter.write_str("organization not found"),
            Self::ProjectNotFound => formatter.write_str("project not found"),
            Self::ProjectOverridesDisabled => {
                formatter.write_str("project governance overrides are disabled")
            }
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported governance policy store version {version}"
                )
            }
            Self::ControlPlane(message) => write!(formatter, "control-plane error: {message}"),
            Self::Governance(error) => write!(formatter, "governance error: {error}"),
            Self::Resource(error) => write!(formatter, "resource governance error: {error}"),
            Self::Io(error) => write!(formatter, "governance policy I/O error: {error}"),
            Self::Json(error) => write!(formatter, "governance policy JSON error: {error}"),
            Self::LockPoisoned => {
                formatter.write_str("organization governance state lock poisoned")
            }
        }
    }
}

impl std::error::Error for OrganizationGovernanceError {}

impl From<std::io::Error> for OrganizationGovernanceError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for OrganizationGovernanceError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[derive(Clone)]
pub struct OrganizationGovernanceService {
    control_plane_path: Arc<PathBuf>,
    policy_path: Arc<PathBuf>,
    state: Arc<Mutex<GovernancePolicyFile>>,
    resolutions: Arc<AtomicU64>,
    constrained_resolutions: Arc<AtomicU64>,
    applications: Arc<AtomicU64>,
}

impl fmt::Debug for OrganizationGovernanceService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrganizationGovernanceService")
            .field("control_plane_path", &self.control_plane_path)
            .field("policy_path", &self.policy_path)
            .finish_non_exhaustive()
    }
}

impl OrganizationGovernanceService {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, OrganizationGovernanceError> {
        let control_plane_path = data_dir.as_ref().join("control-plane");
        ControlPlaneStore::open(&control_plane_path)
            .map_err(|error| OrganizationGovernanceError::ControlPlane(error.to_string()))?;
        let policy_path = control_plane_path.join("governance-policies.json");
        let state = if policy_path.exists() {
            let decoded: GovernancePolicyFile = serde_json::from_slice(&fs::read(&policy_path)?)?;
            if decoded.version != STORE_VERSION {
                return Err(OrganizationGovernanceError::UnsupportedVersion(
                    decoded.version,
                ));
            }
            validate_state(&decoded)?;
            decoded
        } else {
            GovernancePolicyFile::default()
        };
        Ok(Self {
            control_plane_path: Arc::new(control_plane_path),
            policy_path: Arc::new(policy_path),
            state: Arc::new(Mutex::new(state)),
            resolutions: Arc::new(AtomicU64::new(0)),
            constrained_resolutions: Arc::new(AtomicU64::new(0)),
            applications: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn set_organization_policy(
        &self,
        organization_id: &OrganizationId,
        policy: OrganizationGovernancePolicy,
    ) -> Result<(), OrganizationGovernanceError> {
        self.ensure_organization_exists(organization_id)?;
        policy
            .governance
            .validate()
            .map_err(OrganizationGovernanceError::Governance)?;
        policy
            .resources
            .validate()
            .map_err(OrganizationGovernanceError::Resource)?;
        let mut state = self.lock()?;
        let mut next = state.clone();
        next.organization_policies
            .insert(organization_id.as_str().to_string(), policy);
        self.persist(&next)?;
        *state = next;
        Ok(())
    }

    pub fn remove_organization_policy(
        &self,
        organization_id: &OrganizationId,
    ) -> Result<(), OrganizationGovernanceError> {
        self.ensure_organization_exists(organization_id)?;
        let mut state = self.lock()?;
        if !state
            .organization_policies
            .contains_key(organization_id.as_str())
        {
            return Ok(());
        }
        let mut next = state.clone();
        next.organization_policies.remove(organization_id.as_str());
        self.persist(&next)?;
        *state = next;
        Ok(())
    }

    pub fn set_project_override(
        &self,
        project_id: &ProjectId,
        project_override: ProjectGovernanceOverride,
    ) -> Result<(), OrganizationGovernanceError> {
        project_override.validate()?;
        let organization_id = self.resolve_project_organization(project_id)?;
        let mut state = self.lock()?;
        if state
            .organization_policies
            .get(organization_id.as_str())
            .is_some_and(|policy| !policy.project_overrides_allowed)
        {
            return Err(OrganizationGovernanceError::ProjectOverridesDisabled);
        }
        let mut next = state.clone();
        next.project_overrides
            .insert(project_id.as_str().to_string(), project_override);
        self.persist(&next)?;
        *state = next;
        Ok(())
    }

    pub fn remove_project_override(
        &self,
        project_id: &ProjectId,
    ) -> Result<(), OrganizationGovernanceError> {
        self.resolve_project_organization(project_id)?;
        let mut state = self.lock()?;
        if !state.project_overrides.contains_key(project_id.as_str()) {
            return Ok(());
        }
        let mut next = state.clone();
        next.project_overrides.remove(project_id.as_str());
        self.persist(&next)?;
        *state = next;
        Ok(())
    }

    pub fn resolve_project_policy(
        &self,
        project_id: &ProjectId,
    ) -> Result<EffectiveProjectGovernancePolicy, OrganizationGovernanceError> {
        let organization_id = self.resolve_project_organization(project_id)?;
        let state = self.lock()?;
        let organization_policy = state
            .organization_policies
            .get(organization_id.as_str())
            .cloned();
        let project_override = state
            .project_overrides
            .get(project_id.as_str())
            .cloned()
            .unwrap_or_default();
        drop(state);

        let (governance, resources, constrained_by_organization) = match organization_policy {
            Some(organization_policy) => {
                let requested_governance = project_override
                    .governance
                    .clone()
                    .unwrap_or_else(|| organization_policy.governance.clone());
                let requested_resources = project_override
                    .resources
                    .unwrap_or(organization_policy.resources);
                let governance = constrain_governance_policy(
                    &organization_policy.governance,
                    &requested_governance,
                );
                let resources =
                    constrain_resource_budget(organization_policy.resources, requested_resources);
                let constrained =
                    governance != requested_governance || resources != requested_resources;
                (governance, resources, constrained)
            }
            None => (
                project_override.governance.unwrap_or_default(),
                project_override.resources.unwrap_or_default(),
                false,
            ),
        };

        self.resolutions.fetch_add(1, Ordering::Relaxed);
        if constrained_by_organization {
            self.constrained_resolutions.fetch_add(1, Ordering::Relaxed);
        }
        Ok(EffectiveProjectGovernancePolicy {
            organization_id,
            project_id: project_id.clone(),
            governance,
            resources,
            constrained_by_organization,
        })
    }

    pub fn apply_project_policy(
        &self,
        project_id: &ProjectId,
        governance: &GovernanceService,
        resource_governor: &InMemoryResourceGovernor,
    ) -> Result<EffectiveProjectGovernancePolicy, OrganizationGovernanceError> {
        let effective = self.resolve_project_policy(project_id)?;
        governance
            .set_project_policy(project_id.as_str(), effective.governance.clone())
            .map_err(OrganizationGovernanceError::Governance)?;
        resource_governor
            .set_project_budget(project_id.as_str(), effective.resources)
            .map_err(OrganizationGovernanceError::Resource)?;
        self.applications.fetch_add(1, Ordering::Relaxed);
        Ok(effective)
    }

    #[must_use]
    pub fn prometheus_metrics(&self) -> String {
        format!(
            concat!(
                "ketebe_organization_governance_resolutions_total {}\n",
                "ketebe_organization_governance_constrained_resolutions_total {}\n",
                "ketebe_organization_governance_applications_total {}\n"
            ),
            self.resolutions.load(Ordering::Relaxed),
            self.constrained_resolutions.load(Ordering::Relaxed),
            self.applications.load(Ordering::Relaxed),
        )
    }

    fn ensure_organization_exists(
        &self,
        organization_id: &OrganizationId,
    ) -> Result<(), OrganizationGovernanceError> {
        let store = self.control_plane()?;
        if store
            .organization(organization_id)
            .map_err(|error| OrganizationGovernanceError::ControlPlane(error.to_string()))?
            .is_none()
        {
            return Err(OrganizationGovernanceError::OrganizationNotFound);
        }
        Ok(())
    }

    fn resolve_project_organization(
        &self,
        project_id: &ProjectId,
    ) -> Result<OrganizationId, OrganizationGovernanceError> {
        let store = self.control_plane()?;
        let project = store
            .project(project_id)
            .map_err(|error| OrganizationGovernanceError::ControlPlane(error.to_string()))?
            .ok_or(OrganizationGovernanceError::ProjectNotFound)?;
        Ok(project.organization_id().clone())
    }

    fn control_plane(&self) -> Result<ControlPlaneStore, OrganizationGovernanceError> {
        ControlPlaneStore::open(self.control_plane_path.as_path())
            .map_err(|error| OrganizationGovernanceError::ControlPlane(error.to_string()))
    }

    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, GovernancePolicyFile>, OrganizationGovernanceError> {
        self.state
            .lock()
            .map_err(|_| OrganizationGovernanceError::LockPoisoned)
    }

    fn persist(&self, state: &GovernancePolicyFile) -> Result<(), OrganizationGovernanceError> {
        let parent = self
            .policy_path
            .parent()
            .expect("governance policy path has parent");
        fs::create_dir_all(parent)?;
        let temp = self.policy_path.with_extension("json.tmp");
        fs::write(&temp, serde_json::to_vec_pretty(state)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))?;
        }
        fs::rename(temp, self.policy_path.as_path())?;
        Ok(())
    }
}

fn validate_state(state: &GovernancePolicyFile) -> Result<(), OrganizationGovernanceError> {
    for policy in state.organization_policies.values() {
        policy
            .governance
            .validate()
            .map_err(OrganizationGovernanceError::Governance)?;
        policy
            .resources
            .validate()
            .map_err(OrganizationGovernanceError::Resource)?;
    }
    for project_override in state.project_overrides.values() {
        project_override.validate()?;
    }
    Ok(())
}

fn constrain_governance_policy(
    organization: &GovernancePolicy,
    project: &GovernancePolicy,
) -> GovernancePolicy {
    GovernancePolicy {
        read: constrain_rate_limit(organization.read, project.read),
        write: constrain_rate_limit(organization.write, project.write),
        admin: constrain_rate_limit(organization.admin, project.admin),
        quota: ProjectQuota {
            max_collections: constrain_optional_u64(
                organization.quota.max_collections,
                project.quota.max_collections,
            ),
            max_records: constrain_optional_u64(
                organization.quota.max_records,
                project.quota.max_records,
            ),
        },
    }
}

fn constrain_rate_limit(
    organization: Option<RateLimit>,
    project: Option<RateLimit>,
) -> Option<RateLimit> {
    match (organization, project) {
        (Some(organization), Some(project)) => Some(RateLimit {
            requests: organization.requests.min(project.requests),
            window: organization.window.max(project.window),
        }),
        (Some(organization), None) => Some(organization),
        (None, Some(project)) => Some(project),
        (None, None) => None,
    }
}

fn constrain_optional_u64(organization: Option<u64>, project: Option<u64>) -> Option<u64> {
    match (organization, project) {
        (Some(organization), Some(project)) => Some(organization.min(project)),
        (Some(organization), None) => Some(organization),
        (None, Some(project)) => Some(project),
        (None, None) => None,
    }
}

fn constrain_resource_budget(
    organization: ProjectResourceBudget,
    project: ProjectResourceBudget,
) -> ProjectResourceBudget {
    ProjectResourceBudget {
        max_concurrent_queries: organization
            .max_concurrent_queries
            .min(project.max_concurrent_queries),
        max_concurrent_writes: organization
            .max_concurrent_writes
            .min(project.max_concurrent_writes),
        max_concurrent_ingestion: organization
            .max_concurrent_ingestion
            .min(project.max_concurrent_ingestion),
        max_concurrent_background: organization
            .max_concurrent_background
            .min(project.max_concurrent_background),
        ingestion_throughput: constrain_throughput(
            organization.ingestion_throughput,
            project.ingestion_throughput,
        ),
    }
}

fn constrain_throughput(
    organization: Option<ThroughputBudget>,
    project: Option<ThroughputBudget>,
) -> Option<ThroughputBudget> {
    match (organization, project) {
        (Some(organization), Some(project)) => Some(ThroughputBudget {
            units: organization.units.min(project.units),
            window: organization.window.max(project.window),
        }),
        (Some(organization), None) => Some(organization),
        (None, Some(project)) => Some(project),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource_governance::ResourceGovernor;
    use ketebe_core::{
        CollectionId, DataPlaneScope, Organization, Project, ResourceLifecycleState,
        ResourceTimestamps,
    };
    use std::time::Duration;

    fn temp_dir(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ketebe-organization-governance-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn setup(
        label: &str,
    ) -> (
        PathBuf,
        OrganizationId,
        OrganizationId,
        ProjectId,
        ProjectId,
    ) {
        let dir = temp_dir(label);
        let store = ControlPlaneStore::open(dir.join("control-plane")).unwrap();
        let timestamps = ResourceTimestamps::new(10, 10).unwrap();
        let org_a = OrganizationId::new("org-a").unwrap();
        let org_b = OrganizationId::new("org-b").unwrap();
        for organization_id in [&org_a, &org_b] {
            store
                .create_organization(
                    Organization::new(
                        organization_id.clone(),
                        organization_id.as_str(),
                        organization_id.as_str(),
                        ResourceLifecycleState::Active,
                        timestamps,
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let project_a = ProjectId::new("project-a").unwrap();
        let project_b = ProjectId::new("project-b").unwrap();
        store
            .create_project(
                Project::new(
                    project_a.clone(),
                    org_a.clone(),
                    "Project A",
                    "project-a",
                    ResourceLifecycleState::Active,
                    timestamps,
                )
                .unwrap(),
            )
            .unwrap();
        store
            .create_project(
                Project::new(
                    project_b.clone(),
                    org_b.clone(),
                    "Project B",
                    "project-b",
                    ResourceLifecycleState::Active,
                    timestamps,
                )
                .unwrap(),
            )
            .unwrap();
        (dir, org_a, org_b, project_a, project_b)
    }

    fn governance_policy(requests: u64, collections: u64) -> GovernancePolicy {
        GovernancePolicy {
            read: Some(RateLimit::new(requests, Duration::from_secs(60)).unwrap()),
            write: Some(RateLimit::new(requests, Duration::from_secs(60)).unwrap()),
            admin: None,
            quota: ProjectQuota {
                max_collections: Some(collections),
                max_records: Some(100),
            },
        }
    }

    fn resource_budget(concurrency: u32, units: u64) -> ProjectResourceBudget {
        ProjectResourceBudget {
            max_concurrent_queries: concurrency,
            max_concurrent_writes: concurrency,
            max_concurrent_ingestion: concurrency,
            max_concurrent_background: concurrency,
            ingestion_throughput: Some(
                ThroughputBudget::new(units, Duration::from_secs(60)).unwrap(),
            ),
        }
    }

    #[test]
    fn policies_are_durable_and_provider_neutral() {
        let (dir, org_a, _, project_a, _) = setup("durable");
        let service = OrganizationGovernanceService::open(&dir).unwrap();
        service
            .set_organization_policy(
                &org_a,
                OrganizationGovernancePolicy::new(
                    governance_policy(100, 10),
                    resource_budget(8, 1_000),
                    true,
                )
                .unwrap(),
            )
            .unwrap();
        service
            .set_project_override(
                &project_a,
                ProjectGovernanceOverride {
                    governance: Some(governance_policy(50, 5)),
                    resources: Some(resource_budget(4, 500)),
                },
            )
            .unwrap();
        drop(service);

        let reopened = OrganizationGovernanceService::open(&dir).unwrap();
        let effective = reopened.resolve_project_policy(&project_a).unwrap();
        assert_eq!(effective.governance.read.unwrap().requests, 50);
        assert_eq!(effective.resources.max_concurrent_queries, 4);

        let persisted =
            fs::read_to_string(dir.join("control-plane/governance-policies.json")).unwrap();
        assert!(persisted.contains("organization_policies"));
        assert!(persisted.contains("project_overrides"));
        assert!(!persisted.contains("paddle"));
        assert!(!persisted.contains("billing_provider"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn effective_policy_is_deterministic_and_project_override_is_bounded() {
        let (dir, org_a, _, project_a, _) = setup("precedence");
        let service = OrganizationGovernanceService::open(&dir).unwrap();
        service
            .set_organization_policy(
                &org_a,
                OrganizationGovernancePolicy::new(
                    governance_policy(100, 10),
                    resource_budget(8, 1_000),
                    true,
                )
                .unwrap(),
            )
            .unwrap();
        service
            .set_project_override(
                &project_a,
                ProjectGovernanceOverride {
                    governance: Some(governance_policy(200, 20)),
                    resources: Some(resource_budget(16, 2_000)),
                },
            )
            .unwrap();

        let first = service.resolve_project_policy(&project_a).unwrap();
        let second = service.resolve_project_policy(&project_a).unwrap();
        assert_eq!(first, second);
        assert!(first.constrained_by_organization);
        assert_eq!(first.governance.read.unwrap().requests, 100);
        assert_eq!(first.governance.quota.max_collections, Some(10));
        assert_eq!(first.resources.max_concurrent_queries, 8);
        assert_eq!(first.resources.ingestion_throughput.unwrap().units, 1_000);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn stricter_project_override_is_preserved() {
        let (dir, org_a, _, project_a, _) = setup("stricter");
        let service = OrganizationGovernanceService::open(&dir).unwrap();
        service
            .set_organization_policy(
                &org_a,
                OrganizationGovernancePolicy::new(
                    governance_policy(100, 10),
                    resource_budget(8, 1_000),
                    true,
                )
                .unwrap(),
            )
            .unwrap();
        service
            .set_project_override(
                &project_a,
                ProjectGovernanceOverride {
                    governance: Some(governance_policy(25, 3)),
                    resources: Some(resource_budget(2, 200)),
                },
            )
            .unwrap();
        let effective = service.resolve_project_policy(&project_a).unwrap();
        assert!(!effective.constrained_by_organization);
        assert_eq!(effective.governance.read.unwrap().requests, 25);
        assert_eq!(effective.governance.quota.max_collections, Some(3));
        assert_eq!(effective.resources.max_concurrent_queries, 2);
        assert_eq!(effective.resources.ingestion_throughput.unwrap().units, 200);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn project_resolution_uses_authoritative_organization_ownership() {
        let (dir, org_a, org_b, project_a, project_b) = setup("isolation");
        let service = OrganizationGovernanceService::open(&dir).unwrap();
        service
            .set_organization_policy(
                &org_a,
                OrganizationGovernancePolicy::new(
                    governance_policy(10, 2),
                    resource_budget(1, 10),
                    true,
                )
                .unwrap(),
            )
            .unwrap();
        service
            .set_organization_policy(
                &org_b,
                OrganizationGovernancePolicy::new(
                    governance_policy(200, 20),
                    resource_budget(20, 2_000),
                    true,
                )
                .unwrap(),
            )
            .unwrap();

        let a = service.resolve_project_policy(&project_a).unwrap();
        let b = service.resolve_project_policy(&project_b).unwrap();
        assert_eq!(a.organization_id, org_a);
        assert_eq!(b.organization_id, org_b);
        assert_eq!(a.governance.read.unwrap().requests, 10);
        assert_eq!(b.governance.read.unwrap().requests, 200);
        assert_eq!(a.resources.max_concurrent_queries, 1);
        assert_eq!(b.resources.max_concurrent_queries, 20);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn apply_keeps_existing_project_scoped_enforcement_points() {
        let (dir, org_a, _, project_a, _) = setup("apply");
        let service = OrganizationGovernanceService::open(&dir).unwrap();
        service
            .set_organization_policy(
                &org_a,
                OrganizationGovernancePolicy::new(
                    governance_policy(1, 2),
                    resource_budget(1, 5),
                    false,
                )
                .unwrap(),
            )
            .unwrap();

        let governance = GovernanceService::new();
        let resource_governor = InMemoryResourceGovernor::default();
        let effective = service
            .apply_project_policy(&project_a, &governance, &resource_governor)
            .unwrap();
        assert_eq!(effective.project_id, project_a);
        assert!(
            governance
                .admit(project_a.as_str(), crate::AdmissionClass::Read)
                .is_ok()
        );
        assert!(matches!(
            governance.admit(project_a.as_str(), crate::AdmissionClass::Read),
            Err(GovernanceError::RateLimited { .. })
        ));

        let scope = DataPlaneScope::new(
            project_a.clone(),
            CollectionId::new("collection-a").unwrap(),
        );
        let permit = resource_governor
            .admit(&scope, crate::ResourceWorkClass::Query, 1)
            .unwrap();
        assert!(matches!(
            resource_governor.admit(&scope, crate::ResourceWorkClass::Query, 1),
            Err(ResourceGovernanceError::ConcurrencyExceeded { .. })
        ));
        drop(permit);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn accounting_metrics_are_bounded_and_do_not_export_tenant_ids() {
        let (dir, org_a, _, project_a, _) = setup("metrics");
        let service = OrganizationGovernanceService::open(&dir).unwrap();
        service
            .set_organization_policy(
                &org_a,
                OrganizationGovernancePolicy::new(
                    governance_policy(10, 2),
                    resource_budget(1, 10),
                    true,
                )
                .unwrap(),
            )
            .unwrap();
        service.resolve_project_policy(&project_a).unwrap();
        let metrics = service.prometheus_metrics();
        assert!(metrics.contains("ketebe_organization_governance_resolutions_total 1"));
        assert!(!metrics.contains("org-a"));
        assert!(!metrics.contains("project-a"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn project_override_can_be_disabled_by_organization_policy() {
        let (dir, org_a, _, project_a, _) = setup("override-disabled");
        let service = OrganizationGovernanceService::open(&dir).unwrap();
        service
            .set_organization_policy(
                &org_a,
                OrganizationGovernancePolicy::new(
                    governance_policy(100, 10),
                    resource_budget(8, 1_000),
                    false,
                )
                .unwrap(),
            )
            .unwrap();

        assert!(matches!(
            service.set_project_override(
                &project_a,
                ProjectGovernanceOverride {
                    governance: Some(governance_policy(10, 2)),
                    resources: None,
                }
            ),
            Err(OrganizationGovernanceError::ProjectOverridesDisabled)
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
