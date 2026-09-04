use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LifecyclePhase {
    Starting = 0,
    Ready = 1,
    Draining = 2,
    Stopped = 3,
}

impl fmt::Display for LifecyclePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
        })
    }
}

#[derive(Clone)]
pub struct Lifecycle {
    sender: watch::Sender<LifecyclePhase>,
    write_gate: Arc<Mutex<()>>,
    active_foreground_writes: Arc<AtomicUsize>,
    foreground_writes_drained: Arc<Notify>,
}

pub struct LifecycleWriteGuard {
    active_foreground_writes: Arc<AtomicUsize>,
    foreground_writes_drained: Arc<Notify>,
}

impl Drop for LifecycleWriteGuard {
    fn drop(&mut self) {
        let previous = self.active_foreground_writes.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "foreground write counter underflow");
        if previous == 1 {
            self.foreground_writes_drained.notify_waiters();
        }
    }
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::ready()
    }
}

impl Lifecycle {
    #[must_use]
    pub fn starting() -> Self {
        let (sender, _) = watch::channel(LifecyclePhase::Starting);
        Self {
            sender,
            write_gate: Arc::new(Mutex::new(())),
            active_foreground_writes: Arc::new(AtomicUsize::new(0)),
            foreground_writes_drained: Arc::new(Notify::new()),
        }
    }

    #[must_use]
    pub fn ready() -> Self {
        let lifecycle = Self::starting();
        lifecycle.mark_ready();
        lifecycle
    }

    #[must_use]
    pub fn phase(&self) -> LifecyclePhase {
        *self.sender.borrow()
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.phase() == LifecyclePhase::Ready
    }

    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.phase() >= LifecyclePhase::Draining
    }

    #[must_use]
    pub fn active_foreground_writes(&self) -> usize {
        self.active_foreground_writes.load(Ordering::Acquire)
    }

    pub fn try_admit_foreground_write(&self) -> Option<LifecycleWriteGuard> {
        let _gate = self
            .write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.phase() != LifecyclePhase::Ready {
            return None;
        }
        self.active_foreground_writes.fetch_add(1, Ordering::AcqRel);
        Some(LifecycleWriteGuard {
            active_foreground_writes: Arc::clone(&self.active_foreground_writes),
            foreground_writes_drained: Arc::clone(&self.foreground_writes_drained),
        })
    }

    pub fn mark_ready(&self) {
        let _gate = self
            .write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.phase() < LifecyclePhase::Draining {
            self.sender.send_replace(LifecyclePhase::Ready);
        }
    }

    pub fn begin_draining(&self) {
        let _gate = self
            .write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.phase() < LifecyclePhase::Draining {
            self.sender.send_replace(LifecyclePhase::Draining);
        }
    }

    pub fn mark_stopped(&self) {
        let _gate = self
            .write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.sender.send_replace(LifecyclePhase::Stopped);
    }

    pub async fn wait_for_draining(&self) {
        let mut receiver = self.sender.subscribe();
        if *receiver.borrow() >= LifecyclePhase::Draining {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow() >= LifecyclePhase::Draining {
                return;
            }
        }
    }

    pub async fn wait_for_foreground_writes_drained(&self) {
        loop {
            let notified = self.foreground_writes_drained.notified();
            if self.active_foreground_writes() == 0 {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn readiness_becomes_false_before_drain_waiters_are_released() {
        let lifecycle = Lifecycle::ready();
        assert!(lifecycle.is_ready());
        let waiter = lifecycle.clone();
        let task = tokio::spawn(async move {
            waiter.wait_for_draining().await;
            waiter.phase()
        });
        lifecycle.begin_draining();
        assert!(!lifecycle.is_ready());
        assert_eq!(task.await.unwrap(), LifecyclePhase::Draining);
        lifecycle.mark_stopped();
        assert_eq!(lifecycle.phase(), LifecyclePhase::Stopped);
    }

    #[test]
    fn draining_cannot_transition_back_to_ready() {
        let lifecycle = Lifecycle::ready();
        lifecycle.begin_draining();
        lifecycle.mark_ready();
        assert_eq!(lifecycle.phase(), LifecyclePhase::Draining);
    }

    #[tokio::test]
    async fn drain_rejects_new_writes_and_waits_for_admitted_write() {
        let lifecycle = Lifecycle::ready();
        let guard = lifecycle
            .try_admit_foreground_write()
            .expect("ready runtime admits write");
        assert_eq!(lifecycle.active_foreground_writes(), 1);

        lifecycle.begin_draining();
        assert!(lifecycle.try_admit_foreground_write().is_none());

        let waiter = lifecycle.clone();
        let mut task = tokio::spawn(async move {
            waiter.wait_for_foreground_writes_drained().await;
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut task)
                .await
                .is_err()
        );

        drop(guard);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("drain waiter completes")
            .expect("drain waiter task succeeds");
        assert_eq!(lifecycle.active_foreground_writes(), 0);
    }
}
