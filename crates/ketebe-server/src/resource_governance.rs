use ketebe_core::DataPlaneScope;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResourceWorkClass {
    Query,
    Write,
    Ingestion,
    Background,
}

impl ResourceWorkClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Write => "write",
            Self::Ingestion => "ingestion",
            Self::Background => "background",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ThroughputBudget {
    pub units: u64,
    pub window: Duration,
}

impl ThroughputBudget {
    pub fn new(units: u64, window: Duration) -> Result<Self, ResourceGovernanceError> {
        if units == 0 || window.is_zero() {
            return Err(ResourceGovernanceError::InvalidPolicy);
        }
        Ok(Self { units, window })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectResourceBudget {
    pub max_concurrent_queries: u32,
    pub max_concurrent_writes: u32,
    pub max_concurrent_ingestion: u32,
    pub max_concurrent_background: u32,
    pub ingestion_throughput: Option<ThroughputBudget>,
}

impl Default for ProjectResourceBudget {
    fn default() -> Self {
        Self {
            max_concurrent_queries: 32,
            max_concurrent_writes: 16,
            max_concurrent_ingestion: 8,
            max_concurrent_background: 4,
            ingestion_throughput: None,
        }
    }
}

impl ProjectResourceBudget {
    fn validate(self) -> Result<Self, ResourceGovernanceError> {
        if self.max_concurrent_queries == 0
            || self.max_concurrent_writes == 0
            || self.max_concurrent_ingestion == 0
            || self.max_concurrent_background == 0
        {
            return Err(ResourceGovernanceError::InvalidPolicy);
        }
        if let Some(limit) = self.ingestion_throughput
            && (limit.units == 0 || limit.window.is_zero())
        {
            return Err(ResourceGovernanceError::InvalidPolicy);
        }
        Ok(self)
    }

    #[must_use]
    pub const fn concurrency_limit(self, class: ResourceWorkClass) -> u32 {
        match class {
            ResourceWorkClass::Query => self.max_concurrent_queries,
            ResourceWorkClass::Write => self.max_concurrent_writes,
            ResourceWorkClass::Ingestion => self.max_concurrent_ingestion,
            ResourceWorkClass::Background => self.max_concurrent_background,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceAdmission {
    pub class: ResourceWorkClass,
    pub retry_after: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceGovernanceError {
    InvalidPolicy,
    ConcurrencyExceeded {
        class: ResourceWorkClass,
        limit: u32,
        retry_after: Duration,
    },
    ThroughputExceeded {
        class: ResourceWorkClass,
        limit: u64,
        retry_after: Duration,
    },
    LockPoisoned,
}

impl ResourceGovernanceError {
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::ConcurrencyExceeded { retry_after, .. }
            | Self::ThroughputExceeded { retry_after, .. } => Some(*retry_after),
            Self::InvalidPolicy | Self::LockPoisoned => None,
        }
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        matches!(
            self,
            Self::ConcurrencyExceeded { .. } | Self::ThroughputExceeded { .. }
        )
    }
}

impl fmt::Display for ResourceGovernanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy => formatter.write_str("resource governance policy is invalid"),
            Self::ConcurrencyExceeded {
                class,
                limit,
                retry_after,
            } => write!(
                formatter,
                "{} concurrency limit exceeded; limit={limit}; retry after {retry_after:?}",
                class.as_str()
            ),
            Self::ThroughputExceeded {
                class,
                limit,
                retry_after,
            } => write!(
                formatter,
                "{} throughput limit exceeded; limit={limit}; retry after {retry_after:?}",
                class.as_str()
            ),
            Self::LockPoisoned => formatter.write_str("resource governance state lock poisoned"),
        }
    }
}

impl std::error::Error for ResourceGovernanceError {}

pub trait ResourceGovernor: Send + Sync {
    fn admit(
        &self,
        scope: &DataPlaneScope,
        class: ResourceWorkClass,
        units: u64,
    ) -> Result<ResourcePermit, ResourceGovernanceError>;
}

#[derive(Clone, Copy, Debug)]
struct ThroughputWindow {
    started_at: Instant,
    used: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct ProjectUsage {
    queries: u32,
    writes: u32,
    ingestion: u32,
    background: u32,
}

impl ProjectUsage {
    fn current(self, class: ResourceWorkClass) -> u32 {
        match class {
            ResourceWorkClass::Query => self.queries,
            ResourceWorkClass::Write => self.writes,
            ResourceWorkClass::Ingestion => self.ingestion,
            ResourceWorkClass::Background => self.background,
        }
    }

    fn increment(&mut self, class: ResourceWorkClass) {
        match class {
            ResourceWorkClass::Query => self.queries = self.queries.saturating_add(1),
            ResourceWorkClass::Write => self.writes = self.writes.saturating_add(1),
            ResourceWorkClass::Ingestion => self.ingestion = self.ingestion.saturating_add(1),
            ResourceWorkClass::Background => {
                self.background = self.background.saturating_add(1);
            }
        }
    }

    fn decrement(&mut self, class: ResourceWorkClass) {
        match class {
            ResourceWorkClass::Query => self.queries = self.queries.saturating_sub(1),
            ResourceWorkClass::Write => self.writes = self.writes.saturating_sub(1),
            ResourceWorkClass::Ingestion => self.ingestion = self.ingestion.saturating_sub(1),
            ResourceWorkClass::Background => {
                self.background = self.background.saturating_sub(1);
            }
        }
    }

    fn idle(self) -> bool {
        self.queries == 0 && self.writes == 0 && self.ingestion == 0 && self.background == 0
    }
}

#[derive(Default)]
struct ResourceGovernanceMetrics {
    allowed: BTreeMap<ResourceWorkClass, u64>,
    denied: BTreeMap<ResourceWorkClass, u64>,
}

struct ResourceGovernorInner {
    default_budget: ProjectResourceBudget,
    policies: Mutex<BTreeMap<String, ProjectResourceBudget>>,
    usage: Mutex<BTreeMap<String, ProjectUsage>>,
    throughput: Mutex<BTreeMap<String, ThroughputWindow>>,
    metrics: Mutex<ResourceGovernanceMetrics>,
}

#[derive(Clone)]
pub struct InMemoryResourceGovernor {
    inner: Arc<ResourceGovernorInner>,
}

impl Default for InMemoryResourceGovernor {
    fn default() -> Self {
        Self::new(ProjectResourceBudget::default())
            .expect("default project resource budget is valid")
    }
}

impl InMemoryResourceGovernor {
    pub fn new(default_budget: ProjectResourceBudget) -> Result<Self, ResourceGovernanceError> {
        let default_budget = default_budget.validate()?;
        Ok(Self {
            inner: Arc::new(ResourceGovernorInner {
                default_budget,
                policies: Mutex::new(BTreeMap::new()),
                usage: Mutex::new(BTreeMap::new()),
                throughput: Mutex::new(BTreeMap::new()),
                metrics: Mutex::new(ResourceGovernanceMetrics::default()),
            }),
        })
    }

    pub fn set_project_budget(
        &self,
        project_id: impl Into<String>,
        budget: ProjectResourceBudget,
    ) -> Result<(), ResourceGovernanceError> {
        let project_id = project_id.into();
        if project_id.trim().is_empty() {
            return Err(ResourceGovernanceError::InvalidPolicy);
        }
        let budget = budget.validate()?;
        self.inner
            .policies
            .lock()
            .map_err(|_| ResourceGovernanceError::LockPoisoned)?
            .insert(project_id, budget);
        Ok(())
    }

    pub fn remove_project_budget(&self, project_id: &str) -> Result<(), ResourceGovernanceError> {
        self.inner
            .policies
            .lock()
            .map_err(|_| ResourceGovernanceError::LockPoisoned)?
            .remove(project_id);
        self.inner
            .throughput
            .lock()
            .map_err(|_| ResourceGovernanceError::LockPoisoned)?
            .remove(project_id);
        Ok(())
    }

    fn budget_for(
        &self,
        project_id: &str,
    ) -> Result<ProjectResourceBudget, ResourceGovernanceError> {
        Ok(self
            .inner
            .policies
            .lock()
            .map_err(|_| ResourceGovernanceError::LockPoisoned)?
            .get(project_id)
            .copied()
            .unwrap_or(self.inner.default_budget))
    }

    pub fn admit_at(
        &self,
        scope: &DataPlaneScope,
        class: ResourceWorkClass,
        units: u64,
        now: Instant,
    ) -> Result<ResourcePermit, ResourceGovernanceError> {
        let project_id = scope.project_id().as_str();
        let budget = self.budget_for(project_id)?;
        let limit = budget.concurrency_limit(class);

        {
            let mut usage = self
                .inner
                .usage
                .lock()
                .map_err(|_| ResourceGovernanceError::LockPoisoned)?;
            let state = usage.entry(project_id.to_string()).or_default();
            if state.current(class) >= limit {
                drop(usage);
                self.record_metric(class, false)?;
                return Err(ResourceGovernanceError::ConcurrencyExceeded {
                    class,
                    limit,
                    retry_after: Duration::from_millis(100),
                });
            }

            if class == ResourceWorkClass::Ingestion
                && let Some(throughput) = budget.ingestion_throughput
            {
                self.admit_ingestion_units(project_id, class, units, throughput, now)?;
            }
            state.increment(class);
        }

        self.record_metric(class, true)?;
        Ok(ResourcePermit {
            governor: self.clone(),
            project_id: project_id.to_string(),
            class,
            released: false,
        })
    }

    fn admit_ingestion_units(
        &self,
        project_id: &str,
        class: ResourceWorkClass,
        units: u64,
        budget: ThroughputBudget,
        now: Instant,
    ) -> Result<(), ResourceGovernanceError> {
        let mut windows = self
            .inner
            .throughput
            .lock()
            .map_err(|_| ResourceGovernanceError::LockPoisoned)?;
        let state = windows
            .entry(project_id.to_string())
            .or_insert(ThroughputWindow {
                started_at: now,
                used: 0,
            });
        let elapsed = now.saturating_duration_since(state.started_at);
        if elapsed >= budget.window {
            *state = ThroughputWindow {
                started_at: now,
                used: 0,
            };
        }
        let reset_after = budget
            .window
            .saturating_sub(now.saturating_duration_since(state.started_at));
        if units > budget.units.saturating_sub(state.used) {
            drop(windows);
            self.record_metric(class, false)?;
            return Err(ResourceGovernanceError::ThroughputExceeded {
                class,
                limit: budget.units,
                retry_after: reset_after,
            });
        }
        state.used = state.used.saturating_add(units);
        Ok(())
    }

    fn release(&self, project_id: &str, class: ResourceWorkClass) {
        let Ok(mut usage) = self.inner.usage.lock() else {
            return;
        };
        if let Some(state) = usage.get_mut(project_id) {
            state.decrement(class);
            if state.idle() {
                usage.remove(project_id);
            }
        }
    }

    fn record_metric(
        &self,
        class: ResourceWorkClass,
        allowed: bool,
    ) -> Result<(), ResourceGovernanceError> {
        let mut metrics = self
            .inner
            .metrics
            .lock()
            .map_err(|_| ResourceGovernanceError::LockPoisoned)?;
        let target = if allowed {
            &mut metrics.allowed
        } else {
            &mut metrics.denied
        };
        let value = target.entry(class).or_default();
        *value = value.saturating_add(1);
        Ok(())
    }

    pub fn prometheus_metrics(&self) -> Result<String, ResourceGovernanceError> {
        let metrics = self
            .inner
            .metrics
            .lock()
            .map_err(|_| ResourceGovernanceError::LockPoisoned)?;
        let mut output =
            String::from("# TYPE ketebe_resource_governance_admissions_total counter\n");
        for class in [
            ResourceWorkClass::Query,
            ResourceWorkClass::Write,
            ResourceWorkClass::Ingestion,
            ResourceWorkClass::Background,
        ] {
            for (result, value) in [
                ("allowed", metrics.allowed.get(&class).copied().unwrap_or(0)),
                ("denied", metrics.denied.get(&class).copied().unwrap_or(0)),
            ] {
                output.push_str(&format!(
                    "ketebe_resource_governance_admissions_total{{class=\"{}\",result=\"{result}\"}} {value}\n",
                    class.as_str()
                ));
            }
        }
        Ok(output)
    }
}

impl ResourceGovernor for InMemoryResourceGovernor {
    fn admit(
        &self,
        scope: &DataPlaneScope,
        class: ResourceWorkClass,
        units: u64,
    ) -> Result<ResourcePermit, ResourceGovernanceError> {
        self.admit_at(scope, class, units, Instant::now())
    }
}

pub struct ResourcePermit {
    governor: InMemoryResourceGovernor,
    project_id: String,
    class: ResourceWorkClass,
    released: bool,
}

impl ResourcePermit {
    #[must_use]
    pub const fn class(&self) -> ResourceWorkClass {
        self.class
    }

    pub fn release(mut self) {
        self.governor.release(&self.project_id, self.class);
        self.released = true;
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        if !self.released {
            self.governor.release(&self.project_id, self.class);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ketebe_core::{CollectionId, ProjectId};

    fn scope(project: &str, collection: &str) -> DataPlaneScope {
        DataPlaneScope::new(
            ProjectId::new(project).expect("project"),
            CollectionId::new(collection).expect("collection"),
        )
    }

    fn constrained_budget() -> ProjectResourceBudget {
        ProjectResourceBudget {
            max_concurrent_queries: 1,
            max_concurrent_writes: 1,
            max_concurrent_ingestion: 1,
            max_concurrent_background: 1,
            ingestion_throughput: Some(
                ThroughputBudget::new(5, Duration::from_secs(10)).expect("throughput"),
            ),
        }
    }

    #[test]
    fn noisy_neighbor_isolated_by_project_scope() {
        let governor = InMemoryResourceGovernor::new(constrained_budget()).expect("governor");
        let a = scope("p_a", "c_docs");
        let b = scope("p_b", "c_docs");
        let _a_permit = governor
            .admit(&a, ResourceWorkClass::Query, 1)
            .expect("first query");
        assert!(matches!(
            governor.admit(&a, ResourceWorkClass::Query, 1),
            Err(ResourceGovernanceError::ConcurrencyExceeded { .. })
        ));
        assert!(governor.admit(&b, ResourceWorkClass::Query, 1).is_ok());
    }

    #[test]
    fn permit_release_restores_concurrency_capacity() {
        let governor = InMemoryResourceGovernor::new(constrained_budget()).expect("governor");
        let scope = scope("p_a", "c_docs");
        let permit = governor
            .admit(&scope, ResourceWorkClass::Write, 1)
            .expect("write permit");
        assert!(matches!(
            governor.admit(&scope, ResourceWorkClass::Write, 1),
            Err(ResourceGovernanceError::ConcurrencyExceeded { .. })
        ));
        drop(permit);
        assert!(governor.admit(&scope, ResourceWorkClass::Write, 1).is_ok());
    }

    #[test]
    fn ingestion_throughput_is_project_scoped_and_refills() {
        let governor = InMemoryResourceGovernor::new(constrained_budget()).expect("governor");
        let a = scope("p_a", "c_docs");
        let b = scope("p_b", "c_docs");
        let now = Instant::now();
        drop(
            governor
                .admit_at(&a, ResourceWorkClass::Ingestion, 5, now)
                .expect("first batch"),
        );
        assert!(matches!(
            governor.admit_at(&a, ResourceWorkClass::Ingestion, 1, now),
            Err(ResourceGovernanceError::ThroughputExceeded { .. })
        ));
        assert!(
            governor
                .admit_at(&b, ResourceWorkClass::Ingestion, 5, now)
                .is_ok()
        );
        assert!(
            governor
                .admit_at(
                    &a,
                    ResourceWorkClass::Ingestion,
                    5,
                    now + Duration::from_secs(10)
                )
                .is_ok()
        );
    }

    #[test]
    fn background_work_has_explicit_project_aware_extension_point() {
        let governor = InMemoryResourceGovernor::new(constrained_budget()).expect("governor");
        let scope = scope("p_a", "c_docs");
        let _permit = governor
            .admit(&scope, ResourceWorkClass::Background, 1)
            .expect("background permit");
        assert!(matches!(
            governor.admit(&scope, ResourceWorkClass::Background, 1),
            Err(ResourceGovernanceError::ConcurrencyExceeded {
                class: ResourceWorkClass::Background,
                ..
            })
        ));
    }

    #[test]
    fn throttle_semantics_are_stable_and_retryable() {
        let governor = InMemoryResourceGovernor::new(constrained_budget()).expect("governor");
        let scope = scope("p_a", "c_docs");
        let _permit = governor
            .admit(&scope, ResourceWorkClass::Query, 1)
            .expect("query permit");
        let error = match governor.admit(&scope, ResourceWorkClass::Query, 1) {
            Ok(_) => panic!("must throttle"),
            Err(error) => error,
        };
        assert!(error.retryable());
        assert_eq!(error.retry_after(), Some(Duration::from_millis(100)));
    }

    #[test]
    fn metrics_are_bounded_cardinality_and_secret_safe() {
        let governor = InMemoryResourceGovernor::new(constrained_budget()).expect("governor");
        let scope = scope("p_secret_customer", "c_docs");
        let _permit = governor
            .admit(&scope, ResourceWorkClass::Query, 1)
            .expect("query permit");
        let _ = governor.admit(&scope, ResourceWorkClass::Query, 1);
        let metrics = governor.prometheus_metrics().expect("metrics");
        assert!(metrics.contains("class=\"query\""));
        assert!(metrics.contains("result=\"allowed\""));
        assert!(metrics.contains("result=\"denied\""));
        assert!(!metrics.contains("p_secret_customer"));
        assert!(!metrics.contains("c_docs"));
    }
}
