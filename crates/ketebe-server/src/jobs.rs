use crate::{
    AppState, BackupService, EmbeddingMigrationService, WorkKind, global_resource_scheduler,
};
use ketebe_core::CollectionId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use tracing::Instrument as _;

const JOB_VERSION: u32 = 1;
pub const DEFAULT_JOB_CONCURRENCY: usize = 4;

static JOBS_QUEUED: AtomicU64 = AtomicU64::new(0);
static JOBS_RUNNING: AtomicU64 = AtomicU64::new(0);
static JOBS_COMPLETED: AtomicU64 = AtomicU64::new(0);
static JOBS_FAILED: AtomicU64 = AtomicU64::new(0);
static JOBS_CANCELLED: AtomicU64 = AtomicU64::new(0);
static JOBS_INTERRUPTED: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(u64);

impl JobId {
    pub fn new(value: u64) -> Result<Self, JobServiceError> {
        if value == 0 {
            return Err(JobServiceError::InvalidJobId);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobKind {
    EmbeddingMigrationCatchUp { collection_id: String },
    BackupCreate { collection_id: String },
    BackupRestore { backup_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobResult {
    BackupCreated {
        backup_id: String,
    },
    BackupRestored {
        backup_id: String,
        collection_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobProgress {
    pub completed: u64,
    pub total: Option<u64>,
    pub message: Option<String>,
}

impl JobProgress {
    fn queued() -> Self {
        Self {
            completed: 0,
            total: None,
            message: Some("queued".to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFailure {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecord {
    pub version: u32,
    pub id: JobId,
    pub kind: JobKind,
    pub state: JobState,
    pub progress: JobProgress,
    pub error: Option<JobFailure>,
    #[serde(default)]
    pub result: Option<JobResult>,
    pub cancel_requested: bool,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

#[derive(Debug)]
pub enum JobServiceError {
    InvalidJobId,
    RuntimeDraining,
    JobNotFound(JobId),
    Io(std::io::Error),
    Json(serde_json::Error),
    Corrupt(String),
}

impl fmt::Display for JobServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJobId => write!(f, "job id must be greater than zero"),
            Self::RuntimeDraining => {
                f.write_str("background job executor is draining and rejects new jobs")
            }
            Self::JobNotFound(id) => write!(f, "job {id} was not found"),
            Self::Io(error) => write!(f, "job store I/O failure: {error}"),
            Self::Json(error) => write!(f, "job store JSON failure: {error}"),
            Self::Corrupt(message) => write!(f, "job store is corrupt: {message}"),
        }
    }
}

impl std::error::Error for JobServiceError {}

impl From<std::io::Error> for JobServiceError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for JobServiceError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

struct JobStore {
    directory: PathBuf,
    lock: Mutex<()>,
}

impl JobStore {
    fn new(data_dir: &Path) -> Self {
        Self {
            directory: data_dir.join("jobs"),
            lock: Mutex::new(()),
        }
    }

    fn create(&self, kind: JobKind) -> Result<JobRecord, JobServiceError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| JobServiceError::Corrupt("job store lock poisoned".to_string()))?;
        fs::create_dir_all(&self.directory)?;
        let next = self.next_id_unlocked()?;
        let now = unix_ms();
        let record = JobRecord {
            version: JOB_VERSION,
            id: JobId::new(next)?,
            kind,
            state: JobState::Queued,
            progress: JobProgress::queued(),
            error: None,
            result: None,
            cancel_requested: false,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        self.persist_unlocked(&record)?;
        Ok(record)
    }

    fn get(&self, id: JobId) -> Result<JobRecord, JobServiceError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| JobServiceError::Corrupt("job store lock poisoned".to_string()))?;
        self.read_unlocked(id)
    }

    fn update<F>(&self, id: JobId, update: F) -> Result<JobRecord, JobServiceError>
    where
        F: FnOnce(&mut JobRecord),
    {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| JobServiceError::Corrupt("job store lock poisoned".to_string()))?;
        let mut record = self.read_unlocked(id)?;
        update(&mut record);
        record.updated_at_unix_ms = unix_ms();
        self.persist_unlocked(&record)?;
        Ok(record)
    }

    fn recover_interrupted(&self) -> Result<u64, JobServiceError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| JobServiceError::Corrupt("job store lock poisoned".to_string()))?;
        if !self.directory.exists() {
            return Ok(0);
        }
        let mut interrupted = 0_u64;
        for entry in fs::read_dir(&self.directory)? {
            let path = entry?.path();
            if !is_job_path(&path) {
                continue;
            }
            let mut record: JobRecord = serde_json::from_slice(&fs::read(&path)?)?;
            validate_record(&record)?;
            if matches!(record.state, JobState::Queued | JobState::Running) {
                record.state = JobState::Failed;
                record.error = Some(JobFailure {
                    code: "interrupted_by_restart".to_string(),
                    message: "job did not complete before the previous process stopped".to_string(),
                });
                record.progress.message = Some("interrupted by restart".to_string());
                record.updated_at_unix_ms = unix_ms();
                self.persist_unlocked(&record)?;
                interrupted = interrupted.saturating_add(1);
            }
        }
        Ok(interrupted)
    }

    fn next_id_unlocked(&self) -> Result<u64, JobServiceError> {
        let mut max_id = 0_u64;
        for entry in fs::read_dir(&self.directory)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            let Some(raw) = name
                .strip_prefix("job-")
                .and_then(|value| value.strip_suffix(".json"))
            else {
                continue;
            };
            if let Ok(id) = raw.parse::<u64>() {
                max_id = max_id.max(id);
            }
        }
        max_id
            .checked_add(1)
            .filter(|value| *value > 0)
            .ok_or_else(|| JobServiceError::Corrupt("job id space exhausted".to_string()))
    }

    fn read_unlocked(&self, id: JobId) -> Result<JobRecord, JobServiceError> {
        let path = self.path(id);
        if !path.exists() {
            return Err(JobServiceError::JobNotFound(id));
        }
        let record: JobRecord = serde_json::from_slice(&fs::read(path)?)?;
        validate_record(&record)?;
        if record.id != id {
            return Err(JobServiceError::Corrupt(format!(
                "job file identity mismatch: requested {id}, found {}",
                record.id
            )));
        }
        Ok(record)
    }

    fn persist_unlocked(&self, record: &JobRecord) -> Result<(), JobServiceError> {
        fs::create_dir_all(&self.directory)?;
        let path = self.path(record.id);
        let temporary = self.directory.join(format!("job-{}.json.tmp", record.id));
        let bytes = serde_json::to_vec_pretty(record)?;
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        if let Ok(directory) = File::open(&self.directory) {
            let _ = directory.sync_all();
        }
        Ok(())
    }

    fn path(&self, id: JobId) -> PathBuf {
        self.directory.join(format!("job-{id}.json"))
    }
}

fn is_job_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|name| name.starts_with("job-") && name.ends_with(".json"))
}

fn validate_record(record: &JobRecord) -> Result<(), JobServiceError> {
    if record.version != JOB_VERSION {
        return Err(JobServiceError::Corrupt(format!(
            "unsupported job version {}",
            record.version
        )));
    }
    JobId::new(record.id.get())?;
    Ok(())
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

pub(crate) struct JobRuntime {
    store: JobStore,
    limiter: Arc<Semaphore>,
    cancellation: Mutex<BTreeMap<JobId, Arc<AtomicBool>>>,
}

impl JobRuntime {
    #[must_use]
    pub(crate) fn new(data_dir: &Path, max_concurrency: usize) -> Self {
        Self {
            store: JobStore::new(data_dir),
            limiter: Arc::new(Semaphore::new(max_concurrency.max(1))),
            cancellation: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn recover_interrupted(&self) -> Result<u64, JobServiceError> {
        let count = self.store.recover_interrupted()?;
        if count > 0 {
            JOBS_INTERRUPTED.fetch_add(count, Ordering::Relaxed);
            JOBS_FAILED.fetch_add(count, Ordering::Relaxed);
        }
        Ok(count)
    }

    fn install_cancellation(&self, id: JobId) -> Result<Arc<AtomicBool>, JobServiceError> {
        let cancellation = Arc::new(AtomicBool::new(false));
        self.cancellation
            .lock()
            .map_err(|_| JobServiceError::Corrupt("job cancellation lock poisoned".to_string()))?
            .insert(id, cancellation.clone());
        Ok(cancellation)
    }

    fn request_cancel(&self, id: JobId) -> Result<(), JobServiceError> {
        if let Some(value) = self
            .cancellation
            .lock()
            .map_err(|_| JobServiceError::Corrupt("job cancellation lock poisoned".to_string()))?
            .get(&id)
        {
            value.store(true, Ordering::Release);
        }
        Ok(())
    }

    fn remove_cancellation(&self, id: JobId) {
        if let Ok(mut values) = self.cancellation.lock() {
            values.remove(&id);
        }
    }
}

#[derive(Clone)]
pub struct JobService {
    state: AppState,
}

impl JobService {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    pub fn recover_interrupted_jobs(&self) -> Result<u64, JobServiceError> {
        self.state.job_runtime().recover_interrupted()
    }

    pub fn get(&self, id: JobId) -> Result<JobRecord, JobServiceError> {
        self.state.job_runtime().store.get(id)
    }

    pub fn cancel(&self, id: JobId) -> Result<JobRecord, JobServiceError> {
        let runtime = self.state.job_runtime();
        let current = runtime.store.get(id)?;
        if matches!(
            current.state,
            JobState::Completed | JobState::Failed | JobState::Cancelled
        ) {
            return Ok(current);
        }
        if current.state == JobState::Running
            && matches!(
                &current.kind,
                JobKind::BackupCreate { .. } | JobKind::BackupRestore { .. }
            )
        {
            return Ok(current);
        }
        let was_queued = current.state == JobState::Queued;
        runtime.request_cancel(id)?;
        let record = runtime.store.update(id, |record| {
            if matches!(record.state, JobState::Queued | JobState::Running) {
                record.cancel_requested = true;
                if record.state == JobState::Queued {
                    record.state = JobState::Cancelled;
                    record.progress.message = Some("cancelled before execution".to_string());
                    record.error = None;
                }
            }
        })?;
        if was_queued && record.state == JobState::Cancelled {
            decrement(&JOBS_QUEUED);
            JOBS_CANCELLED.fetch_add(1, Ordering::Relaxed);
        }
        Ok(record)
    }

    pub fn submit_embedding_migration_catch_up(
        &self,
        collection_id: CollectionId,
    ) -> Result<JobRecord, JobServiceError> {
        if !self.state.is_ready() {
            return Err(JobServiceError::RuntimeDraining);
        }
        let runtime = self.state.job_runtime();
        let record = runtime.store.create(JobKind::EmbeddingMigrationCatchUp {
            collection_id: collection_id.as_str().to_string(),
        })?;
        let cancellation = runtime.install_cancellation(record.id)?;
        JOBS_QUEUED.fetch_add(1, Ordering::Relaxed);

        let state = self.state.clone();
        let runtime = runtime.clone();
        let id = record.id;
        let span = tracing::info_span!(
            "ketebe.background.job",
            component = "job",
            job.kind = "embedding_migration_catch_up"
        );
        tokio::spawn(
            async move {
                let permit = match runtime.limiter.clone().acquire_owned().await {
                    Ok(value) => value,
                    Err(_) => {
                        let _ = fail_job(
                            &runtime,
                            id,
                            "executor_shutdown",
                            "background job executor is shutting down",
                        );
                        return;
                    }
                };

                let _resource_permit =
                    match global_resource_scheduler().acquire(WorkKind::Job).await {
                        Ok(value) => value,
                        Err(error) => {
                            let _ = fail_job(
                                &runtime,
                                id,
                                "resource_scheduler_unavailable",
                                &format!("background job resource admission failed: {error}"),
                            );
                            runtime.remove_cancellation(id);
                            drop(permit);
                            return;
                        }
                    };

                let current = runtime.store.get(id);
                if cancellation.load(Ordering::Acquire)
                    || current
                        .as_ref()
                        .is_ok_and(|record| record.state == JobState::Cancelled)
                {
                    if current
                        .as_ref()
                        .is_ok_and(|record| record.state == JobState::Queued)
                    {
                        let _ = runtime.store.update(id, |record| {
                            record.state = JobState::Cancelled;
                            record.cancel_requested = true;
                            record.progress.message =
                                Some("cancelled before execution".to_string());
                        });
                        decrement(&JOBS_QUEUED);
                        JOBS_CANCELLED.fetch_add(1, Ordering::Relaxed);
                    }
                    runtime.remove_cancellation(id);
                    drop(permit);
                    return;
                }

                if runtime
                    .store
                    .update(id, |record| {
                        record.state = JobState::Running;
                        record.progress.message =
                            Some("embedding migration catch-up running".to_string());
                    })
                    .is_err()
                {
                    runtime.remove_cancellation(id);
                    drop(permit);
                    return;
                }
                decrement(&JOBS_QUEUED);
                JOBS_RUNNING.fetch_add(1, Ordering::Relaxed);

                let result = EmbeddingMigrationService::new(state)
                    .catch_up(&collection_id)
                    .await;
                match result {
                    Ok(migration) => {
                        let _ = runtime.store.update(id, |record| {
                            record.state = JobState::Completed;
                            record.progress.completed = migration.completed_records as u64;
                            record.progress.total = Some(migration.total_managed_records as u64);
                            record.progress.message =
                                Some("embedding migration catch-up completed".to_string());
                            record.error = None;
                        });
                        decrement(&JOBS_RUNNING);
                        JOBS_COMPLETED.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => {
                        let _ = fail_job(
                            &runtime,
                            id,
                            "embedding_migration_catch_up_failed",
                            &error.to_string(),
                        );
                    }
                }
                runtime.remove_cancellation(id);
                drop(permit);
            }
            .instrument(span),
        );

        Ok(record)
    }

    pub fn submit_backup_create(
        &self,
        collection_id: CollectionId,
    ) -> Result<JobRecord, JobServiceError> {
        self.submit_backup_operation(JobKind::BackupCreate {
            collection_id: collection_id.as_str().to_string(),
        })
    }

    pub fn submit_backup_restore(
        &self,
        backup_id: impl Into<String>,
    ) -> Result<JobRecord, JobServiceError> {
        self.submit_backup_operation(JobKind::BackupRestore {
            backup_id: backup_id.into(),
        })
    }

    fn submit_backup_operation(&self, kind: JobKind) -> Result<JobRecord, JobServiceError> {
        if !self.state.is_ready() {
            return Err(JobServiceError::RuntimeDraining);
        }
        debug_assert!(matches!(
            &kind,
            JobKind::BackupCreate { .. } | JobKind::BackupRestore { .. }
        ));
        let runtime = self.state.job_runtime();
        let record = runtime.store.create(kind.clone())?;
        let cancellation = runtime.install_cancellation(record.id)?;
        JOBS_QUEUED.fetch_add(1, Ordering::Relaxed);

        let state = self.state.clone();
        let runtime = runtime.clone();
        let id = record.id;
        let kind_name = match &kind {
            JobKind::BackupCreate { .. } => "backup_create",
            JobKind::BackupRestore { .. } => "backup_restore",
            JobKind::EmbeddingMigrationCatchUp { .. } => unreachable!(),
        };
        let span = tracing::info_span!(
            "ketebe.background.job",
            component = "job",
            job.kind = kind_name
        );
        tokio::spawn(
            async move {
                let permit = match runtime.limiter.clone().acquire_owned().await {
                    Ok(value) => value,
                    Err(_) => {
                        let _ = fail_job(
                            &runtime,
                            id,
                            "executor_shutdown",
                            "background job executor is shutting down",
                        );
                        return;
                    }
                };

                let _resource_permit =
                    match global_resource_scheduler().acquire(WorkKind::Job).await {
                        Ok(value) => value,
                        Err(error) => {
                            let _ = fail_job(
                                &runtime,
                                id,
                                "resource_scheduler_unavailable",
                                &format!("background job resource admission failed: {error}"),
                            );
                            runtime.remove_cancellation(id);
                            drop(permit);
                            return;
                        }
                    };

                let current = runtime.store.get(id);
                if cancellation.load(Ordering::Acquire)
                    || current
                        .as_ref()
                        .is_ok_and(|record| record.state == JobState::Cancelled)
                {
                    if current
                        .as_ref()
                        .is_ok_and(|record| record.state == JobState::Queued)
                    {
                        let _ = runtime.store.update(id, |record| {
                            record.state = JobState::Cancelled;
                            record.cancel_requested = true;
                            record.progress.message =
                                Some("cancelled before execution".to_string());
                        });
                        decrement(&JOBS_QUEUED);
                        JOBS_CANCELLED.fetch_add(1, Ordering::Relaxed);
                    }
                    runtime.remove_cancellation(id);
                    drop(permit);
                    return;
                }

                let running_message = match &kind {
                    JobKind::BackupCreate { .. } => "backup snapshot running",
                    JobKind::BackupRestore { .. } => "backup restore running",
                    JobKind::EmbeddingMigrationCatchUp { .. } => unreachable!(),
                };
                if runtime
                    .store
                    .update(id, |record| {
                        record.state = JobState::Running;
                        record.progress.total = Some(1);
                        record.progress.message = Some(running_message.to_string());
                    })
                    .is_err()
                {
                    runtime.remove_cancellation(id);
                    drop(permit);
                    return;
                }
                decrement(&JOBS_QUEUED);
                JOBS_RUNNING.fetch_add(1, Ordering::Relaxed);

                let outcome: Result<JobResult, (&'static str, String)> = match kind {
                    JobKind::BackupCreate { collection_id } => {
                        match CollectionId::new(collection_id) {
                            Ok(collection_id) => BackupService::new(state)
                                .create(&collection_id)
                                .await
                                .map(|manifest| JobResult::BackupCreated {
                                    backup_id: manifest.backup_id,
                                })
                                .map_err(|error| ("backup_create_failed", error.to_string())),
                            Err(error) => Err(("backup_create_failed", error.to_string())),
                        }
                    }
                    JobKind::BackupRestore { backup_id } => BackupService::new(state)
                        .restore(&backup_id)
                        .await
                        .map(|result| JobResult::BackupRestored {
                            backup_id: result.backup_id,
                            collection_id: result.collection_id,
                        })
                        .map_err(|error| ("backup_restore_failed", error.to_string())),
                    JobKind::EmbeddingMigrationCatchUp { .. } => unreachable!(),
                };

                match outcome {
                    Ok(result) => {
                        let _ = runtime.store.update(id, |record| {
                            record.state = JobState::Completed;
                            record.progress.completed = 1;
                            record.progress.total = Some(1);
                            record.progress.message =
                                Some("backup operation completed".to_string());
                            record.error = None;
                            record.result = Some(result);
                        });
                        decrement(&JOBS_RUNNING);
                        JOBS_COMPLETED.fetch_add(1, Ordering::Relaxed);
                    }
                    Err((code, message)) => {
                        let _ = fail_job(&runtime, id, code, &message);
                    }
                }
                runtime.remove_cancellation(id);
                drop(permit);
            }
            .instrument(span),
        );

        Ok(record)
    }
}

fn fail_job(
    runtime: &JobRuntime,
    id: JobId,
    code: &str,
    message: &str,
) -> Result<JobRecord, JobServiceError> {
    let current = runtime.store.get(id)?;
    match current.state {
        JobState::Queued => decrement(&JOBS_QUEUED),
        JobState::Running => decrement(&JOBS_RUNNING),
        JobState::Completed | JobState::Failed | JobState::Cancelled => {}
    }
    let record = runtime.store.update(id, |record| {
        record.state = JobState::Failed;
        record.progress.message = Some("job failed".to_string());
        record.error = Some(JobFailure {
            code: code.to_string(),
            message: message.to_string(),
        });
    })?;
    JOBS_FAILED.fetch_add(1, Ordering::Relaxed);
    Ok(record)
}

fn decrement(value: &AtomicU64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
}

#[must_use]
pub fn job_prometheus_metrics() -> String {
    format!(
        concat!(
            "ketebe_jobs_queued {}\n",
            "ketebe_jobs_running {}\n",
            "ketebe_jobs_completed_total {}\n",
            "ketebe_jobs_failed_total {}\n",
            "ketebe_jobs_cancelled_total {}\n",
            "ketebe_jobs_interrupted_total {}\n"
        ),
        JOBS_QUEUED.load(Ordering::Relaxed),
        JOBS_RUNNING.load(Ordering::Relaxed),
        JOBS_COMPLETED.load(Ordering::Relaxed),
        JOBS_FAILED.load(Ordering::Relaxed),
        JOBS_CANCELLED.load(Ordering::Relaxed),
        JOBS_INTERRUPTED.load(Ordering::Relaxed),
    )
}
