use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryControlError {
    Cancelled,
    DeadlineExceeded,
}

impl fmt::Display for QueryControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => f.write_str("query execution was cancelled"),
            Self::DeadlineExceeded => f.write_str("query execution deadline exceeded"),
        }
    }
}

impl std::error::Error for QueryControlError {}

#[derive(Debug, Clone)]
pub struct QueryControl {
    cancelled: Arc<AtomicBool>,
    deadline: Option<Instant>,
}

impl Default for QueryControl {
    fn default() -> Self {
        Self::unbounded()
    }
}

impl QueryControl {
    #[must_use]
    pub fn unbounded() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            deadline: None,
        }
    }

    #[must_use]
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            deadline: Instant::now().checked_add(timeout),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn check(&self) -> Result<(), QueryControlError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(QueryControlError::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(QueryControlError::DeadlineExceeded);
        }
        Ok(())
    }

    #[must_use]
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_is_shared_across_clones() {
        let control = QueryControl::unbounded();
        let clone = control.clone();
        clone.cancel();
        assert_eq!(control.check(), Err(QueryControlError::Cancelled));
    }

    #[test]
    fn zero_timeout_is_immediately_expired() {
        let control = QueryControl::with_timeout(Duration::ZERO);
        assert_eq!(control.check(), Err(QueryControlError::DeadlineExceeded));
    }
}
