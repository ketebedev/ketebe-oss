use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::ketebe::KetebeApi;

#[derive(Clone, Debug, Default)]
pub struct Readiness(Arc<AtomicBool>);

impl Readiness {
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn set(&self, ready: bool) {
        self.0.store(ready, Ordering::Release);
    }
}

pub fn spawn_probe(
    api: KetebeApi,
    readiness: Readiness,
    interval: Duration,
    ct: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let ready = api.probe().await.is_ok();
            let previous = readiness.is_ready();
            readiness.set(ready);
            if ready != previous {
                tracing::info!(ready, "Ketebe backend readiness changed");
            }
            tokio::select! {
                _ = ct.cancelled() => break,
                _ = tokio::time::sleep(interval) => {}
            }
        }
        readiness.set(false);
    })
}
