use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub const DEFAULT_QUERY_CONCURRENCY: usize = 64;
pub const DEFAULT_MAX_QUERY_TOP_K: usize = 1_000;
pub const DEFAULT_MAX_QUERY_CANDIDATES: usize = 10_000;
pub const DEFAULT_QUERY_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_MAX_QUERY_TIMEOUT_MS: u64 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimits {
    pub max_concurrency: usize,
    pub max_top_k: usize,
    pub max_candidates: usize,
    pub default_timeout_ms: u64,
    pub max_timeout_ms: u64,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            max_concurrency: DEFAULT_QUERY_CONCURRENCY,
            max_top_k: DEFAULT_MAX_QUERY_TOP_K,
            max_candidates: DEFAULT_MAX_QUERY_CANDIDATES,
            default_timeout_ms: DEFAULT_QUERY_TIMEOUT_MS,
            max_timeout_ms: DEFAULT_MAX_QUERY_TIMEOUT_MS,
        }
    }
}

impl QueryLimits {
    pub fn validate(self) -> Result<Self, QueryAdmissionError> {
        if self.max_concurrency == 0 {
            return Err(QueryAdmissionError::InvalidConfiguration(
                "max_concurrency must be greater than zero",
            ));
        }
        if self.max_top_k == 0 {
            return Err(QueryAdmissionError::InvalidConfiguration(
                "max_top_k must be greater than zero",
            ));
        }
        if self.max_candidates < self.max_top_k {
            return Err(QueryAdmissionError::InvalidConfiguration(
                "max_candidates must be greater than or equal to max_top_k",
            ));
        }
        if self.default_timeout_ms == 0 || self.max_timeout_ms == 0 {
            return Err(QueryAdmissionError::InvalidConfiguration(
                "query timeout limits must be greater than zero",
            ));
        }
        if self.default_timeout_ms > self.max_timeout_ms {
            return Err(QueryAdmissionError::InvalidConfiguration(
                "default_timeout_ms must not exceed max_timeout_ms",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QueryAdmissionRequest {
    pub top_k: usize,
    pub candidate_budget: usize,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug)]
pub enum QueryAdmissionError {
    Overloaded,
    TopKExceeded { requested: usize, maximum: usize },
    CandidateBudgetExceeded { requested: usize, maximum: usize },
    TimeoutExceeded { requested_ms: u64, maximum_ms: u64 },
    InvalidConfiguration(&'static str),
}

impl fmt::Display for QueryAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overloaded => f.write_str("query concurrency limit reached"),
            Self::TopKExceeded { requested, maximum } => {
                write!(f, "top_k {requested} exceeds maximum {maximum}")
            }
            Self::CandidateBudgetExceeded { requested, maximum } => write!(
                f,
                "query candidate budget {requested} exceeds maximum {maximum}"
            ),
            Self::TimeoutExceeded {
                requested_ms,
                maximum_ms,
            } => write!(f, "timeout_ms {requested_ms} exceeds maximum {maximum_ms}"),
            Self::InvalidConfiguration(message) => {
                write!(f, "invalid query runtime configuration: {message}")
            }
        }
    }
}

impl std::error::Error for QueryAdmissionError {}

#[derive(Default)]
struct QueryCounters {
    admitted: AtomicU64,
    rejected: AtomicU64,
    timed_out: AtomicU64,
    cancelled: AtomicU64,
}

#[derive(Clone)]
pub struct QueryRuntime {
    semaphore: Arc<Semaphore>,
    limits: QueryLimits,
    counters: Arc<QueryCounters>,
}

impl Default for QueryRuntime {
    fn default() -> Self {
        Self::new(QueryLimits::default()).expect("default query limits are valid")
    }
}

impl QueryRuntime {
    pub fn new(limits: QueryLimits) -> Result<Self, QueryAdmissionError> {
        let limits = limits.validate()?;
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(limits.max_concurrency)),
            limits,
            counters: Arc::new(QueryCounters::default()),
        })
    }

    #[must_use]
    pub const fn limits(&self) -> QueryLimits {
        self.limits
    }

    pub fn admit(
        &self,
        request: QueryAdmissionRequest,
    ) -> Result<QueryAdmission, QueryAdmissionError> {
        if request.top_k > self.limits.max_top_k {
            self.reject();
            return Err(QueryAdmissionError::TopKExceeded {
                requested: request.top_k,
                maximum: self.limits.max_top_k,
            });
        }
        if request.candidate_budget > self.limits.max_candidates {
            self.reject();
            return Err(QueryAdmissionError::CandidateBudgetExceeded {
                requested: request.candidate_budget,
                maximum: self.limits.max_candidates,
            });
        }
        let timeout_ms = request.timeout_ms.unwrap_or(self.limits.default_timeout_ms);
        if timeout_ms > self.limits.max_timeout_ms {
            self.reject();
            return Err(QueryAdmissionError::TimeoutExceeded {
                requested_ms: timeout_ms,
                maximum_ms: self.limits.max_timeout_ms,
            });
        }
        let permit = Arc::clone(&self.semaphore)
            .try_acquire_owned()
            .map_err(|_| {
                self.reject();
                QueryAdmissionError::Overloaded
            })?;
        self.counters.admitted.fetch_add(1, Ordering::Relaxed);
        Ok(QueryAdmission { permit, timeout_ms })
    }

    pub fn mark_timeout(&self) {
        self.counters.timed_out.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_cancelled(&self) {
        self.counters.cancelled.fetch_add(1, Ordering::Relaxed);
    }

    fn reject(&self) {
        self.counters.rejected.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn prometheus_metrics(&self) -> String {
        format!(
            concat!(
                "ketebe_query_admitted_total {}\n",
                "ketebe_query_rejected_total {}\n",
                "ketebe_query_timeout_total {}\n",
                "ketebe_query_cancelled_total {}\n",
                "ketebe_query_in_flight {}\n"
            ),
            self.counters.admitted.load(Ordering::Relaxed),
            self.counters.rejected.load(Ordering::Relaxed),
            self.counters.timed_out.load(Ordering::Relaxed),
            self.counters.cancelled.load(Ordering::Relaxed),
            self.limits
                .max_concurrency
                .saturating_sub(self.semaphore.available_permits()),
        )
    }
}

pub struct QueryAdmission {
    permit: OwnedSemaphorePermit,
    timeout_ms: u64,
}

impl QueryAdmission {
    #[must_use]
    pub const fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    pub fn permit(&self) -> &OwnedSemaphorePermit {
        &self.permit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> QueryLimits {
        QueryLimits {
            max_concurrency: 1,
            max_top_k: 10,
            max_candidates: 20,
            default_timeout_ms: 100,
            max_timeout_ms: 200,
        }
    }

    fn request(
        top_k: usize,
        candidate_budget: usize,
        timeout_ms: Option<u64>,
    ) -> QueryAdmissionRequest {
        QueryAdmissionRequest {
            top_k,
            candidate_budget,
            timeout_ms,
        }
    }

    #[test]
    fn admission_rejects_before_expensive_work_when_capacity_is_full() {
        let runtime = QueryRuntime::new(limits()).unwrap();
        let request = request(2, 4, None);
        let first = runtime.admit(request).unwrap();
        assert!(matches!(
            runtime.admit(request),
            Err(QueryAdmissionError::Overloaded)
        ));
        drop(first);
        assert!(runtime.admit(request).is_ok());
    }

    #[test]
    fn explicit_resource_limits_are_stable() {
        let runtime = QueryRuntime::new(limits()).unwrap();
        let top_k = runtime.admit(request(11, 11, None));
        assert!(matches!(
            top_k,
            Err(QueryAdmissionError::TopKExceeded { .. })
        ));

        let candidates = runtime.admit(request(1, 21, None));
        assert!(matches!(
            candidates,
            Err(QueryAdmissionError::CandidateBudgetExceeded { .. })
        ));

        let timeout = runtime.admit(request(1, 1, Some(201)));
        assert!(matches!(
            timeout,
            Err(QueryAdmissionError::TimeoutExceeded { .. })
        ));
    }

    #[test]
    fn metrics_report_admission_rejection_timeout_cancellation_and_in_flight() {
        let runtime = QueryRuntime::new(limits()).unwrap();
        let admission = runtime.admit(request(1, 1, None)).unwrap();
        assert!(matches!(
            runtime.admit(request(1, 1, None)),
            Err(QueryAdmissionError::Overloaded)
        ));
        runtime.mark_timeout();
        runtime.mark_cancelled();
        let metrics = runtime.prometheus_metrics();
        assert!(metrics.contains("ketebe_query_admitted_total 1\n"));
        assert!(metrics.contains("ketebe_query_rejected_total 1\n"));
        assert!(metrics.contains("ketebe_query_timeout_total 1\n"));
        assert!(metrics.contains("ketebe_query_cancelled_total 1\n"));
        assert!(metrics.contains("ketebe_query_in_flight 1\n"));
        drop(admission);
        assert!(
            runtime
                .prometheus_metrics()
                .contains("ketebe_query_in_flight 0\n")
        );
    }
}
