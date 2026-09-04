use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AdmissionClass {
    Read,
    Write,
    Admin,
}

impl AdmissionClass {
    pub fn from_authorization_action(action: crate::AuthorizationAction) -> Self {
        match action {
            crate::AuthorizationAction::CollectionDiscover
            | crate::AuthorizationAction::CollectionRead => Self::Read,
            crate::AuthorizationAction::CollectionCreate
            | crate::AuthorizationAction::CollectionWrite => Self::Write,
            crate::AuthorizationAction::CollectionDelete
            | crate::AuthorizationAction::ProjectAdmin => Self::Admin,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "admin",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateLimit {
    pub requests: u64,
    pub window: Duration,
}

impl RateLimit {
    pub fn new(requests: u64, window: Duration) -> Result<Self, GovernanceError> {
        if requests == 0 || window.is_zero() {
            return Err(GovernanceError::InvalidPolicy);
        }
        Ok(Self { requests, window })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProjectQuota {
    pub max_collections: Option<u64>,
    pub max_records: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GovernancePolicy {
    pub read: Option<RateLimit>,
    pub write: Option<RateLimit>,
    pub admin: Option<RateLimit>,
    pub quota: ProjectQuota,
}

impl GovernancePolicy {
    fn limit(&self, class: AdmissionClass) -> Option<RateLimit> {
        match class {
            AdmissionClass::Read => self.read,
            AdmissionClass::Write => self.write,
            AdmissionClass::Admin => self.admin,
        }
    }

    fn validate(&self) -> Result<(), GovernanceError> {
        for limit in [self.read, self.write, self.admin].into_iter().flatten() {
            if limit.requests == 0 || limit.window.is_zero() {
                return Err(GovernanceError::InvalidPolicy);
            }
        }
        if self.quota.max_collections == Some(0) || self.quota.max_records == Some(0) {
            return Err(GovernanceError::InvalidPolicy);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionDecision {
    pub remaining: Option<u64>,
    pub reset_after: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GovernanceError {
    InvalidPolicy,
    RateLimited { retry_after: Duration },
    CollectionQuotaExceeded { limit: u64 },
    RecordQuotaExceeded { limit: u64 },
    LockPoisoned,
}

impl fmt::Display for GovernanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy => f.write_str("governance policy is invalid"),
            Self::RateLimited { retry_after } => {
                write!(
                    f,
                    "request rate limit exceeded; retry after {retry_after:?}"
                )
            }
            Self::CollectionQuotaExceeded { limit } => {
                write!(f, "collection quota exceeded; limit is {limit}")
            }
            Self::RecordQuotaExceeded { limit } => {
                write!(f, "record quota exceeded; limit is {limit}")
            }
            Self::LockPoisoned => f.write_str("governance state lock poisoned"),
        }
    }
}

impl std::error::Error for GovernanceError {}

#[derive(Clone, Copy, Debug)]
struct WindowState {
    started_at: Instant,
    used: u64,
}

#[derive(Default)]
struct GovernanceMetrics {
    allowed: BTreeMap<AdmissionClass, u64>,
    denied: BTreeMap<AdmissionClass, u64>,
}

#[derive(Clone, Default)]
pub struct GovernanceService {
    policies: Arc<Mutex<BTreeMap<String, GovernancePolicy>>>,
    windows: Arc<Mutex<BTreeMap<(String, AdmissionClass), WindowState>>>,
    metrics: Arc<Mutex<GovernanceMetrics>>,
}

impl GovernanceService {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_project_policy(
        &self,
        project_id: impl Into<String>,
        policy: GovernancePolicy,
    ) -> Result<(), GovernanceError> {
        policy.validate()?;
        let project_id = project_id.into();
        if project_id.trim().is_empty() {
            return Err(GovernanceError::InvalidPolicy);
        }
        self.policies
            .lock()
            .map_err(|_| GovernanceError::LockPoisoned)?
            .insert(project_id, policy);
        Ok(())
    }

    pub fn remove_project_policy(&self, project_id: &str) -> Result<(), GovernanceError> {
        self.policies
            .lock()
            .map_err(|_| GovernanceError::LockPoisoned)?
            .remove(project_id);
        self.windows
            .lock()
            .map_err(|_| GovernanceError::LockPoisoned)?
            .retain(|(project, _), _| project != project_id);
        Ok(())
    }

    pub fn admit(
        &self,
        project_id: &str,
        class: AdmissionClass,
    ) -> Result<AdmissionDecision, GovernanceError> {
        self.admit_at(project_id, class, Instant::now())
    }

    pub fn admit_at(
        &self,
        project_id: &str,
        class: AdmissionClass,
        now: Instant,
    ) -> Result<AdmissionDecision, GovernanceError> {
        let limit = self
            .policies
            .lock()
            .map_err(|_| GovernanceError::LockPoisoned)?
            .get(project_id)
            .and_then(|policy| policy.limit(class));

        let Some(limit) = limit else {
            self.record_metric(class, true)?;
            return Ok(AdmissionDecision {
                remaining: None,
                reset_after: None,
            });
        };

        let mut windows = self
            .windows
            .lock()
            .map_err(|_| GovernanceError::LockPoisoned)?;
        let state = windows
            .entry((project_id.to_string(), class))
            .or_insert(WindowState {
                started_at: now,
                used: 0,
            });
        let elapsed = now.saturating_duration_since(state.started_at);
        if elapsed >= limit.window {
            *state = WindowState {
                started_at: now,
                used: 0,
            };
        }
        let elapsed = now.saturating_duration_since(state.started_at);
        let reset_after = limit.window.saturating_sub(elapsed);
        if state.used >= limit.requests {
            drop(windows);
            self.record_metric(class, false)?;
            return Err(GovernanceError::RateLimited {
                retry_after: reset_after,
            });
        }
        state.used = state.used.saturating_add(1);
        let remaining = limit.requests.saturating_sub(state.used);
        drop(windows);
        self.record_metric(class, true)?;
        Ok(AdmissionDecision {
            remaining: Some(remaining),
            reset_after: Some(reset_after),
        })
    }

    pub fn check_collection_quota(
        &self,
        project_id: &str,
        current_collections: u64,
        additional: u64,
    ) -> Result<(), GovernanceError> {
        let limit = self
            .policies
            .lock()
            .map_err(|_| GovernanceError::LockPoisoned)?
            .get(project_id)
            .and_then(|policy| policy.quota.max_collections);
        if let Some(limit) = limit
            && current_collections.saturating_add(additional) > limit
        {
            return Err(GovernanceError::CollectionQuotaExceeded { limit });
        }
        Ok(())
    }

    pub fn check_record_quota(
        &self,
        project_id: &str,
        current_records: u64,
        additional: u64,
    ) -> Result<(), GovernanceError> {
        let limit = self
            .policies
            .lock()
            .map_err(|_| GovernanceError::LockPoisoned)?
            .get(project_id)
            .and_then(|policy| policy.quota.max_records);
        if let Some(limit) = limit
            && current_records.saturating_add(additional) > limit
        {
            return Err(GovernanceError::RecordQuotaExceeded { limit });
        }
        Ok(())
    }

    pub fn prometheus_metrics(&self) -> Result<String, GovernanceError> {
        let metrics = self
            .metrics
            .lock()
            .map_err(|_| GovernanceError::LockPoisoned)?;
        let mut output = String::new();
        output.push_str("# TYPE ketebe_governance_admissions_total counter\n");
        for class in [
            AdmissionClass::Read,
            AdmissionClass::Write,
            AdmissionClass::Admin,
        ] {
            let allowed = metrics.allowed.get(&class).copied().unwrap_or(0);
            let denied = metrics.denied.get(&class).copied().unwrap_or(0);
            output.push_str(&format!(
                "ketebe_governance_admissions_total{{class=\"{}\",result=\"allowed\"}} {}\n",
                class.as_str(),
                allowed
            ));
            output.push_str(&format!(
                "ketebe_governance_admissions_total{{class=\"{}\",result=\"denied\"}} {}\n",
                class.as_str(),
                denied
            ));
        }
        Ok(output)
    }

    fn record_metric(&self, class: AdmissionClass, allowed: bool) -> Result<(), GovernanceError> {
        let mut metrics = self
            .metrics
            .lock()
            .map_err(|_| GovernanceError::LockPoisoned)?;
        let target = if allowed {
            &mut metrics.allowed
        } else {
            &mut metrics.denied
        };
        let value = target.entry(class).or_default();
        *value = value.saturating_add(1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(requests: u64, window: Duration) -> GovernancePolicy {
        GovernancePolicy {
            read: Some(RateLimit::new(requests, window).unwrap()),
            write: Some(RateLimit::new(requests, window).unwrap()),
            admin: Some(RateLimit::new(requests, window).unwrap()),
            quota: ProjectQuota {
                max_collections: Some(2),
                max_records: Some(10),
            },
        }
    }

    #[test]
    fn project_scopes_are_independent_and_refill_after_window() {
        let service = GovernanceService::new();
        service
            .set_project_policy("project-a", policy(2, Duration::from_secs(10)))
            .unwrap();
        service
            .set_project_policy("project-b", policy(2, Duration::from_secs(10)))
            .unwrap();
        let now = Instant::now();

        assert_eq!(
            service
                .admit_at("project-a", AdmissionClass::Read, now)
                .unwrap()
                .remaining,
            Some(1)
        );
        assert_eq!(
            service
                .admit_at("project-a", AdmissionClass::Read, now)
                .unwrap()
                .remaining,
            Some(0)
        );
        assert!(matches!(
            service.admit_at("project-a", AdmissionClass::Read, now),
            Err(GovernanceError::RateLimited { .. })
        ));
        assert!(
            service
                .admit_at("project-b", AdmissionClass::Read, now)
                .is_ok()
        );
        assert!(
            service
                .admit_at(
                    "project-a",
                    AdmissionClass::Read,
                    now + Duration::from_secs(10)
                )
                .is_ok()
        );
    }

    #[test]
    fn admission_classes_use_independent_windows() {
        let service = GovernanceService::new();
        service
            .set_project_policy("project-a", policy(1, Duration::from_secs(60)))
            .unwrap();
        let now = Instant::now();
        assert!(
            service
                .admit_at("project-a", AdmissionClass::Read, now)
                .is_ok()
        );
        assert!(
            service
                .admit_at("project-a", AdmissionClass::Write, now)
                .is_ok()
        );
        assert!(matches!(
            service.admit_at("project-a", AdmissionClass::Read, now),
            Err(GovernanceError::RateLimited { .. })
        ));
    }

    #[test]
    fn collection_and_record_quotas_are_enforced_before_mutation() {
        let service = GovernanceService::new();
        service
            .set_project_policy("project-a", policy(10, Duration::from_secs(60)))
            .unwrap();
        assert!(service.check_collection_quota("project-a", 1, 1).is_ok());
        assert_eq!(
            service.check_collection_quota("project-a", 2, 1),
            Err(GovernanceError::CollectionQuotaExceeded { limit: 2 })
        );
        assert!(service.check_record_quota("project-a", 8, 2).is_ok());
        assert_eq!(
            service.check_record_quota("project-a", 10, 1),
            Err(GovernanceError::RecordQuotaExceeded { limit: 10 })
        );
    }

    #[test]
    fn metrics_have_bounded_labels_and_never_export_project_ids() {
        let service = GovernanceService::new();
        service
            .set_project_policy("secret-project", policy(1, Duration::from_secs(60)))
            .unwrap();
        let now = Instant::now();
        service
            .admit_at("secret-project", AdmissionClass::Admin, now)
            .unwrap();
        let _ = service.admit_at("secret-project", AdmissionClass::Admin, now);
        let metrics = service.prometheus_metrics().unwrap();
        assert!(metrics.contains("class=\"admin\""));
        assert!(metrics.contains("result=\"allowed\""));
        assert!(metrics.contains("result=\"denied\""));
        assert!(!metrics.contains("secret-project"));
    }

    #[test]
    fn invalid_limits_are_rejected() {
        assert_eq!(
            RateLimit::new(0, Duration::from_secs(1)),
            Err(GovernanceError::InvalidPolicy)
        );
        assert_eq!(
            RateLimit::new(1, Duration::ZERO),
            Err(GovernanceError::InvalidPolicy)
        );
    }
}
