use crate::resource_scheduler::{ResourcePermit, WorkKind, global_resource_scheduler};
use ketebe_core::CollectionId;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) const DEFAULT_LEXICAL_BUILD_CONCURRENCY: usize = 2;
pub(crate) const DEFAULT_LEXICAL_BUILD_ATTEMPTS: u32 = 3;
pub(crate) const DEFAULT_LEXICAL_RETRY_BASE_MS: u64 = 25;

#[derive(Debug)]
pub(crate) struct LexicalBuildPermit {
    _local: OwnedSemaphorePermit,
    _global: ResourcePermit,
}

/// Process-local coordinator for bounded, deduplicated lexical snapshot construction.
#[derive(Debug)]
pub(crate) struct LexicalBuildScheduler {
    slots: Arc<Semaphore>,
    latest: Mutex<BTreeMap<CollectionId, u64>>,
    max_attempts: u32,
    retry_base: Duration,
}

impl Default for LexicalBuildScheduler {
    fn default() -> Self {
        Self::new(
            DEFAULT_LEXICAL_BUILD_CONCURRENCY,
            DEFAULT_LEXICAL_BUILD_ATTEMPTS,
            Duration::from_millis(DEFAULT_LEXICAL_RETRY_BASE_MS),
        )
    }
}

impl LexicalBuildScheduler {
    pub(crate) fn new(concurrency: usize, max_attempts: u32, retry_base: Duration) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(concurrency.max(1))),
            latest: Mutex::new(BTreeMap::new()),
            max_attempts: max_attempts.max(1),
            retry_base,
        }
    }

    pub(crate) fn register(&self, collection_id: CollectionId, fingerprint: u64) -> bool {
        let Ok(mut latest) = self.latest.lock() else {
            return false;
        };
        if latest.get(&collection_id) == Some(&fingerprint) {
            return false;
        }
        latest.insert(collection_id, fingerprint);
        true
    }

    pub(crate) fn is_current(&self, collection_id: &CollectionId, fingerprint: u64) -> bool {
        self.latest
            .lock()
            .is_ok_and(|latest| latest.get(collection_id) == Some(&fingerprint))
    }

    pub(crate) fn finish(&self, collection_id: &CollectionId, fingerprint: u64) {
        if let Ok(mut latest) = self.latest.lock()
            && latest.get(collection_id) == Some(&fingerprint)
        {
            latest.remove(collection_id);
        }
    }

    pub(crate) fn cancel(&self, collection_id: &CollectionId) {
        if let Ok(mut latest) = self.latest.lock() {
            latest.remove(collection_id);
        }
    }

    pub(crate) async fn acquire(&self) -> Option<LexicalBuildPermit> {
        let local = Arc::clone(&self.slots).acquire_owned().await.ok()?;
        let global = global_resource_scheduler()
            .acquire(WorkKind::LexicalBuild)
            .await
            .ok()?;
        Some(LexicalBuildPermit {
            _local: local,
            _global: global,
        })
    }

    pub(crate) const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub(crate) fn retry_delay(&self, failed_attempt: u32) -> Duration {
        let exponent = failed_attempt.saturating_sub(1).min(16);
        self.retry_base.saturating_mul(1_u32 << exponent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collection() -> CollectionId {
        CollectionId::new("docs").unwrap()
    }

    #[test]
    fn duplicate_fingerprint_is_not_registered_twice() {
        let scheduler = LexicalBuildScheduler::default();
        assert!(scheduler.register(collection(), 7));
        assert!(!scheduler.register(collection(), 7));
        assert!(scheduler.register(collection(), 8));
        assert!(!scheduler.is_current(&collection(), 7));
        assert!(scheduler.is_current(&collection(), 8));
    }

    #[test]
    fn retry_backoff_is_bounded_and_deterministic() {
        let scheduler = LexicalBuildScheduler::new(2, 4, Duration::from_millis(10));
        assert_eq!(scheduler.retry_delay(1), Duration::from_millis(10));
        assert_eq!(scheduler.retry_delay(2), Duration::from_millis(20));
        assert_eq!(scheduler.retry_delay(3), Duration::from_millis(40));
        assert_eq!(scheduler.max_attempts(), 4);
    }

    #[tokio::test]
    async fn concurrency_budget_bounds_active_build_slots() {
        let scheduler = Arc::new(LexicalBuildScheduler::new(1, 1, Duration::from_millis(1)));
        let first = scheduler.acquire().await.expect("first permit");
        let blocked = tokio::time::timeout(Duration::from_millis(10), scheduler.acquire()).await;
        assert!(blocked.is_err());
        drop(first);
        assert!(scheduler.acquire().await.is_some());
    }
}
