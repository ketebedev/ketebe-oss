use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkPriority {
    High,
    Normal,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkKind {
    Embedding,
    LexicalBuild,
    Compaction,
    Migration,
    Job,
}

impl WorkKind {
    #[must_use]
    pub const fn priority(self) -> WorkPriority {
        match self {
            Self::Embedding => WorkPriority::High,
            Self::LexicalBuild | Self::Migration => WorkPriority::Normal,
            Self::Compaction | Self::Job => WorkPriority::Low,
        }
    }

    #[must_use]
    pub const fn resources(self) -> ResourceRequest {
        match self {
            Self::Embedding => ResourceRequest::new(1, 2, 0),
            Self::LexicalBuild => ResourceRequest::new(2, 2, 2),
            Self::Compaction => ResourceRequest::new(2, 2, 2),
            Self::Migration => ResourceRequest::new(2, 2, 1),
            Self::Job => ResourceRequest::new(1, 1, 1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceRequest {
    pub cpu: u32,
    pub memory: u32,
    pub io: u32,
}

impl ResourceRequest {
    #[must_use]
    pub const fn new(cpu: u32, memory: u32, io: u32) -> Self {
        Self { cpu, memory, io }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceBudget {
    pub cpu: u32,
    pub memory: u32,
    pub io: u32,
    pub foreground_cpu_reserve: u32,
    pub foreground_memory_reserve: u32,
    pub foreground_io_reserve: u32,
    pub max_background_tasks: usize,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            cpu: 8,
            memory: 8,
            io: 8,
            foreground_cpu_reserve: 2,
            foreground_memory_reserve: 2,
            foreground_io_reserve: 2,
            max_background_tasks: 4,
        }
    }
}

impl ResourceBudget {
    pub fn validate(self) -> Result<Self, ResourceSchedulerError> {
        if self.cpu == 0 || self.memory == 0 || self.io == 0 || self.max_background_tasks == 0 {
            return Err(ResourceSchedulerError::InvalidBudget(
                "resource totals and max_background_tasks must be greater than zero".to_string(),
            ));
        }
        if self.foreground_cpu_reserve >= self.cpu
            || self.foreground_memory_reserve >= self.memory
            || self.foreground_io_reserve >= self.io
        {
            return Err(ResourceSchedulerError::InvalidBudget(
                "foreground reserves must leave at least one background resource unit".to_string(),
            ));
        }
        Ok(self)
    }

    #[must_use]
    pub const fn background_cpu(self) -> u32 {
        self.cpu - self.foreground_cpu_reserve
    }

    #[must_use]
    pub const fn background_memory(self) -> u32 {
        self.memory - self.foreground_memory_reserve
    }

    #[must_use]
    pub const fn background_io(self) -> u32 {
        self.io - self.foreground_io_reserve
    }
}

#[derive(Debug)]
pub enum ResourceSchedulerError {
    InvalidBudget(String),
    RequestExceedsBudget {
        kind: WorkKind,
        request: ResourceRequest,
    },
    Overloaded(WorkKind),
    Closed,
}

impl fmt::Display for ResourceSchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBudget(message) => write!(f, "invalid resource budget: {message}"),
            Self::RequestExceedsBudget { kind, request } => write!(
                f,
                "resource request for {kind:?} exceeds background budget: cpu={}, memory={}, io={}",
                request.cpu, request.memory, request.io
            ),
            Self::Overloaded(kind) => write!(f, "resource scheduler is overloaded for {kind:?}"),
            Self::Closed => write!(f, "resource scheduler is closed"),
        }
    }
}
impl std::error::Error for ResourceSchedulerError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceSchedulerSnapshot {
    pub active: u64,
    pub queued: u64,
    pub admitted_total: u64,
    pub rejected_total: u64,
    pub peak_active: u64,
    pub embedding_admitted: u64,
    pub lexical_admitted: u64,
    pub compaction_admitted: u64,
    pub migration_admitted: u64,
    pub job_admitted: u64,
    pub background_cpu_available: usize,
    pub background_memory_available: usize,
    pub background_io_available: usize,
    pub background_slots_available: usize,
    pub non_high_slots_available: usize,
    pub low_slots_available: usize,
}

#[derive(Debug, Default)]
struct SchedulerMetrics {
    active: AtomicU64,
    queued: AtomicU64,
    admitted_total: AtomicU64,
    rejected_total: AtomicU64,
    peak_active: AtomicU64,
    embedding_admitted: AtomicU64,
    lexical_admitted: AtomicU64,
    compaction_admitted: AtomicU64,
    migration_admitted: AtomicU64,
    job_admitted: AtomicU64,
}

impl SchedulerMetrics {
    fn admitted(&self, kind: WorkKind) {
        self.admitted_total.fetch_add(1, Ordering::Relaxed);
        match kind {
            WorkKind::Embedding => &self.embedding_admitted,
            WorkKind::LexicalBuild => &self.lexical_admitted,
            WorkKind::Compaction => &self.compaction_admitted,
            WorkKind::Migration => &self.migration_admitted,
            WorkKind::Job => &self.job_admitted,
        }
        .fetch_add(1, Ordering::Relaxed);
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_active.fetch_max(active, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct SchedulerInner {
    budget: ResourceBudget,
    background_slots: Arc<Semaphore>,
    non_high_slots: Arc<Semaphore>,
    low_slots: Arc<Semaphore>,
    cpu: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    io: Arc<Semaphore>,
    metrics: SchedulerMetrics,
}

#[derive(Debug, Clone)]
pub struct ResourceScheduler {
    inner: Arc<SchedulerInner>,
}

impl Default for ResourceScheduler {
    fn default() -> Self {
        Self::new(ResourceBudget::default()).expect("default resource budget is valid")
    }
}

impl ResourceScheduler {
    pub fn new(budget: ResourceBudget) -> Result<Self, ResourceSchedulerError> {
        let budget = budget.validate()?;
        let non_high_limit = (budget.max_background_tasks / 2).max(1);
        let low_limit = (non_high_limit / 2).max(1);
        Ok(Self {
            inner: Arc::new(SchedulerInner {
                background_slots: Arc::new(Semaphore::new(budget.max_background_tasks)),
                non_high_slots: Arc::new(Semaphore::new(non_high_limit)),
                low_slots: Arc::new(Semaphore::new(low_limit)),
                cpu: Arc::new(Semaphore::new(budget.background_cpu() as usize)),
                memory: Arc::new(Semaphore::new(budget.background_memory() as usize)),
                io: Arc::new(Semaphore::new(budget.background_io() as usize)),
                budget,
                metrics: SchedulerMetrics::default(),
            }),
        })
    }

    #[must_use]
    pub fn budget(&self) -> ResourceBudget {
        self.inner.budget
    }

    pub async fn acquire(&self, kind: WorkKind) -> Result<ResourcePermit, ResourceSchedulerError> {
        let request = kind.resources();
        self.validate_request(kind, request)?;
        self.inner.metrics.queued.fetch_add(1, Ordering::Relaxed);
        let result = self.acquire_inner(kind, request).await;
        self.inner.metrics.queued.fetch_sub(1, Ordering::Relaxed);
        result
    }

    pub fn try_acquire(&self, kind: WorkKind) -> Result<ResourcePermit, ResourceSchedulerError> {
        let request = kind.resources();
        self.validate_request(kind, request)?;
        let (non_high_slot, low_slot) = match kind.priority() {
            WorkPriority::High => (None, None),
            WorkPriority::Normal => (
                Some(
                    Arc::clone(&self.inner.non_high_slots)
                        .try_acquire_owned()
                        .map_err(|error| self.map_try_error(kind, error))?,
                ),
                None,
            ),
            WorkPriority::Low => {
                let low = Arc::clone(&self.inner.low_slots)
                    .try_acquire_owned()
                    .map_err(|error| self.map_try_error(kind, error))?;
                let non_high = Arc::clone(&self.inner.non_high_slots)
                    .try_acquire_owned()
                    .map_err(|error| self.map_try_error(kind, error))?;
                (Some(non_high), Some(low))
            }
        };
        let slot = Arc::clone(&self.inner.background_slots)
            .try_acquire_owned()
            .map_err(|error| self.map_try_error(kind, error))?;
        let cpu = try_many(&self.inner.cpu, request.cpu)
            .map_err(|error| self.map_try_error(kind, error))?;
        let memory = try_many(&self.inner.memory, request.memory)
            .map_err(|error| self.map_try_error(kind, error))?;
        let io = try_many(&self.inner.io, request.io)
            .map_err(|error| self.map_try_error(kind, error))?;
        self.inner.metrics.admitted(kind);
        Ok(ResourcePermit {
            inner: Arc::clone(&self.inner),
            kind,
            _slot: slot,
            _non_high_slot: non_high_slot,
            _low_slot: low_slot,
            _cpu: cpu,
            _memory: memory,
            _io: io,
        })
    }

    async fn acquire_inner(
        &self,
        kind: WorkKind,
        request: ResourceRequest,
    ) -> Result<ResourcePermit, ResourceSchedulerError> {
        let (non_high_slot, low_slot) = match kind.priority() {
            WorkPriority::High => (None, None),
            WorkPriority::Normal => (
                Some(
                    Arc::clone(&self.inner.non_high_slots)
                        .acquire_owned()
                        .await
                        .map_err(|_| ResourceSchedulerError::Closed)?,
                ),
                None,
            ),
            WorkPriority::Low => {
                let low = Arc::clone(&self.inner.low_slots)
                    .acquire_owned()
                    .await
                    .map_err(|_| ResourceSchedulerError::Closed)?;
                let non_high = Arc::clone(&self.inner.non_high_slots)
                    .acquire_owned()
                    .await
                    .map_err(|_| ResourceSchedulerError::Closed)?;
                (Some(non_high), Some(low))
            }
        };
        let slot = Arc::clone(&self.inner.background_slots)
            .acquire_owned()
            .await
            .map_err(|_| ResourceSchedulerError::Closed)?;
        let cpu = acquire_many(&self.inner.cpu, request.cpu).await?;
        let memory = acquire_many(&self.inner.memory, request.memory).await?;
        let io = acquire_many(&self.inner.io, request.io).await?;
        self.inner.metrics.admitted(kind);
        Ok(ResourcePermit {
            inner: Arc::clone(&self.inner),
            kind,
            _slot: slot,
            _non_high_slot: non_high_slot,
            _low_slot: low_slot,
            _cpu: cpu,
            _memory: memory,
            _io: io,
        })
    }

    fn validate_request(
        &self,
        kind: WorkKind,
        request: ResourceRequest,
    ) -> Result<(), ResourceSchedulerError> {
        let budget = self.inner.budget;
        if request.cpu > budget.background_cpu()
            || request.memory > budget.background_memory()
            || request.io > budget.background_io()
        {
            return Err(ResourceSchedulerError::RequestExceedsBudget { kind, request });
        }
        Ok(())
    }

    fn map_try_error(&self, kind: WorkKind, error: TryAcquireError) -> ResourceSchedulerError {
        match error {
            TryAcquireError::NoPermits => {
                self.inner
                    .metrics
                    .rejected_total
                    .fetch_add(1, Ordering::Relaxed);
                ResourceSchedulerError::Overloaded(kind)
            }
            TryAcquireError::Closed => ResourceSchedulerError::Closed,
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> ResourceSchedulerSnapshot {
        let metrics = &self.inner.metrics;
        ResourceSchedulerSnapshot {
            active: metrics.active.load(Ordering::Relaxed),
            queued: metrics.queued.load(Ordering::Relaxed),
            admitted_total: metrics.admitted_total.load(Ordering::Relaxed),
            rejected_total: metrics.rejected_total.load(Ordering::Relaxed),
            peak_active: metrics.peak_active.load(Ordering::Relaxed),
            embedding_admitted: metrics.embedding_admitted.load(Ordering::Relaxed),
            lexical_admitted: metrics.lexical_admitted.load(Ordering::Relaxed),
            compaction_admitted: metrics.compaction_admitted.load(Ordering::Relaxed),
            migration_admitted: metrics.migration_admitted.load(Ordering::Relaxed),
            job_admitted: metrics.job_admitted.load(Ordering::Relaxed),
            background_cpu_available: self.inner.cpu.available_permits(),
            background_memory_available: self.inner.memory.available_permits(),
            background_io_available: self.inner.io.available_permits(),
            background_slots_available: self.inner.background_slots.available_permits(),
            non_high_slots_available: self.inner.non_high_slots.available_permits(),
            low_slots_available: self.inner.low_slots.available_permits(),
        }
    }
}

#[derive(Debug)]
pub struct ResourcePermit {
    inner: Arc<SchedulerInner>,
    kind: WorkKind,
    _slot: OwnedSemaphorePermit,
    _non_high_slot: Option<OwnedSemaphorePermit>,
    _low_slot: Option<OwnedSemaphorePermit>,
    _cpu: Option<OwnedSemaphorePermit>,
    _memory: Option<OwnedSemaphorePermit>,
    _io: Option<OwnedSemaphorePermit>,
}

impl ResourcePermit {
    #[must_use]
    pub const fn kind(&self) -> WorkKind {
        self.kind
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        self.inner.metrics.active.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn acquire_many(
    semaphore: &Arc<Semaphore>,
    permits: u32,
) -> Result<Option<OwnedSemaphorePermit>, ResourceSchedulerError> {
    if permits == 0 {
        return Ok(None);
    }
    Arc::clone(semaphore)
        .acquire_many_owned(permits)
        .await
        .map(Some)
        .map_err(|_| ResourceSchedulerError::Closed)
}

fn try_many(
    semaphore: &Arc<Semaphore>,
    permits: u32,
) -> Result<Option<OwnedSemaphorePermit>, TryAcquireError> {
    if permits == 0 {
        return Ok(None);
    }
    Arc::clone(semaphore)
        .try_acquire_many_owned(permits)
        .map(Some)
}

static GLOBAL_SCHEDULER: OnceLock<ResourceScheduler> = OnceLock::new();

#[must_use]
pub fn global_resource_scheduler() -> &'static ResourceScheduler {
    GLOBAL_SCHEDULER.get_or_init(ResourceScheduler::default)
}

#[must_use]
pub fn resource_scheduler_prometheus_metrics() -> String {
    let scheduler = global_resource_scheduler();
    let snapshot = scheduler.snapshot();
    let budget = scheduler.budget();
    format!(
        concat!(
            "ketebe_resource_scheduler_active {}\n",
            "ketebe_resource_scheduler_queued {}\n",
            "ketebe_resource_scheduler_admitted_total {}\n",
            "ketebe_resource_scheduler_rejected_total {}\n",
            "ketebe_resource_scheduler_peak_active {}\n",
            "ketebe_resource_scheduler_admitted_total{{kind=\"embedding\"}} {}\n",
            "ketebe_resource_scheduler_admitted_total{{kind=\"lexical\"}} {}\n",
            "ketebe_resource_scheduler_admitted_total{{kind=\"compaction\"}} {}\n",
            "ketebe_resource_scheduler_admitted_total{{kind=\"migration\"}} {}\n",
            "ketebe_resource_scheduler_admitted_total{{kind=\"job\"}} {}\n",
            "ketebe_resource_scheduler_background_cpu_available {}\n",
            "ketebe_resource_scheduler_background_memory_available {}\n",
            "ketebe_resource_scheduler_background_io_available {}\n",
            "ketebe_resource_scheduler_background_slots_available {}\n",
            "ketebe_resource_scheduler_non_high_slots_available {}\n",
            "ketebe_resource_scheduler_low_slots_available {}\n",
            "ketebe_resource_scheduler_foreground_cpu_reserve {}\n",
            "ketebe_resource_scheduler_foreground_memory_reserve {}\n",
            "ketebe_resource_scheduler_foreground_io_reserve {}\n"
        ),
        snapshot.active,
        snapshot.queued,
        snapshot.admitted_total,
        snapshot.rejected_total,
        snapshot.peak_active,
        snapshot.embedding_admitted,
        snapshot.lexical_admitted,
        snapshot.compaction_admitted,
        snapshot.migration_admitted,
        snapshot.job_admitted,
        snapshot.background_cpu_available,
        snapshot.background_memory_available,
        snapshot.background_io_available,
        snapshot.background_slots_available,
        snapshot.non_high_slots_available,
        snapshot.low_slots_available,
        budget.foreground_cpu_reserve,
        budget.foreground_memory_reserve,
        budget.foreground_io_reserve,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn tiny_budget() -> ResourceBudget {
        ResourceBudget {
            cpu: 4,
            memory: 4,
            io: 4,
            foreground_cpu_reserve: 2,
            foreground_memory_reserve: 2,
            foreground_io_reserve: 1,
            max_background_tasks: 1,
        }
    }

    #[test]
    fn foreground_reserve_is_never_available_to_background_work() {
        let scheduler = ResourceScheduler::new(tiny_budget()).unwrap();
        assert_eq!(scheduler.budget().background_cpu(), 2);
        assert_eq!(scheduler.budget().background_memory(), 2);
        assert_eq!(scheduler.budget().background_io(), 3);
        let permit = scheduler.try_acquire(WorkKind::Embedding).unwrap();
        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.background_cpu_available, 1);
        assert_eq!(snapshot.background_memory_available, 0);
        drop(permit);
    }

    #[test]
    fn cross_subsystem_overload_is_rejected_by_one_global_limit() {
        let scheduler = ResourceScheduler::new(tiny_budget()).unwrap();
        let lexical = scheduler.try_acquire(WorkKind::LexicalBuild).unwrap();
        let error = scheduler.try_acquire(WorkKind::Embedding).unwrap_err();
        assert!(matches!(
            error,
            ResourceSchedulerError::Overloaded(WorkKind::Embedding)
        ));
        assert_eq!(scheduler.snapshot().rejected_total, 1);
        drop(lexical);
        assert!(scheduler.try_acquire(WorkKind::Embedding).is_ok());
    }

    #[test]
    fn high_priority_keeps_capacity_when_non_high_pool_is_saturated() {
        let scheduler = ResourceScheduler::default();
        let first = scheduler.try_acquire(WorkKind::LexicalBuild).unwrap();
        let second = scheduler.try_acquire(WorkKind::LexicalBuild).unwrap();
        assert!(matches!(
            scheduler.try_acquire(WorkKind::LexicalBuild).unwrap_err(),
            ResourceSchedulerError::Overloaded(WorkKind::LexicalBuild)
        ));
        let high = scheduler.try_acquire(WorkKind::Embedding).unwrap();
        assert_eq!(high.kind(), WorkKind::Embedding);
        drop((first, second, high));
    }

    #[test]
    fn low_priority_pool_preserves_capacity_for_normal_work() {
        let scheduler = ResourceScheduler::default();
        let low = scheduler.try_acquire(WorkKind::Compaction).unwrap();
        assert!(matches!(
            scheduler.try_acquire(WorkKind::Compaction).unwrap_err(),
            ResourceSchedulerError::Overloaded(WorkKind::Compaction)
        ));
        let normal = scheduler.try_acquire(WorkKind::LexicalBuild).unwrap();
        assert_eq!(normal.kind(), WorkKind::LexicalBuild);
        drop((low, normal));
    }

    #[tokio::test]
    async fn queued_work_is_backpressured_and_runs_after_release() {
        let scheduler = ResourceScheduler::new(tiny_budget()).unwrap();
        let first = scheduler.acquire(WorkKind::LexicalBuild).await.unwrap();
        let waiting_scheduler = scheduler.clone();
        let waiter = tokio::spawn(async move {
            waiting_scheduler
                .acquire(WorkKind::Embedding)
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(scheduler.snapshot().queued, 1);
        assert!(!waiter.is_finished());
        drop(first);
        let second = tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("queued work should make progress")
            .expect("waiter task");
        assert_eq!(second.kind(), WorkKind::Embedding);
    }

    #[test]
    fn scheduler_decisions_are_observable_by_kind() {
        let scheduler = ResourceScheduler::new(tiny_budget()).unwrap();
        let permit = scheduler.try_acquire(WorkKind::Embedding).unwrap();
        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.active, 1);
        assert_eq!(snapshot.admitted_total, 1);
        assert_eq!(snapshot.embedding_admitted, 1);
        assert_eq!(snapshot.peak_active, 1);
        drop(permit);
        assert_eq!(scheduler.snapshot().active, 0);
    }
}
