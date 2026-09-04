use crate::embedding::{EMBEDDING_METADATA_KEY, EmbeddingProvider};
use crate::{AppState, CollectionService, PendingRecord, WriteError, WriteService};
use ketebe_core::{CollectionId, Metadata, MetadataValue, Record, RecordId, SequenceNumber};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

const MIGRATION_STATE_VERSION: u32 = 1;
const MIGRATION_STATE_FILE: &str = "embedding-migration.json";
const MIGRATION_SNAPSHOT_FILE: &str = "embedding-migration.snapshot.json";
const CUTOVER_JOURNAL_FILE: &str = "embedding-migration.cutover.json";
const CUTOVER_JOURNAL_VERSION: u32 = 1;

static MIGRATIONS_STARTED: AtomicU64 = AtomicU64::new(0);
static MIGRATION_CATCH_UP_RUNS: AtomicU64 = AtomicU64::new(0);
static MIGRATION_RECONCILED_RECORDS: AtomicU64 = AtomicU64::new(0);
static MIGRATION_ACTIVATIONS: AtomicU64 = AtomicU64::new(0);
static MIGRATION_FAILURES: AtomicU64 = AtomicU64::new(0);
static MIGRATION_RECOVERIES: AtomicU64 = AtomicU64::new(0);
static MIGRATION_LAST_TOTAL: AtomicU64 = AtomicU64::new(0);
static MIGRATION_LAST_COMPLETED: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn embedding_migration_prometheus_metrics() -> String {
    format!(
        concat!(
            "ketebe_embedding_migrations_started_total {}\n",
            "ketebe_embedding_migration_catch_up_runs_total {}\n",
            "ketebe_embedding_migration_reconciled_records_total {}\n",
            "ketebe_embedding_migration_activations_total {}\n",
            "ketebe_embedding_migration_failures_total {}\n",
            "ketebe_embedding_migration_recoveries_total {}\n",
            "ketebe_embedding_migration_last_total_records {}\n",
            "ketebe_embedding_migration_last_completed_records {}\n"
        ),
        MIGRATIONS_STARTED.load(Ordering::Relaxed),
        MIGRATION_CATCH_UP_RUNS.load(Ordering::Relaxed),
        MIGRATION_RECONCILED_RECORDS.load(Ordering::Relaxed),
        MIGRATION_ACTIVATIONS.load(Ordering::Relaxed),
        MIGRATION_FAILURES.load(Ordering::Relaxed),
        MIGRATION_RECOVERIES.load(Ordering::Relaxed),
        MIGRATION_LAST_TOTAL.load(Ordering::Relaxed),
        MIGRATION_LAST_COMPLETED.load(Ordering::Relaxed),
    )
}

static ACTIVE_MIGRATIONS: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();

fn active_migrations() -> &'static Mutex<BTreeSet<PathBuf>> {
    ACTIVE_MIGRATIONS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingMigrationStatus {
    Running,
    Ready,
    Activating,
    Activated,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingMigrationState {
    pub version: u32,
    pub source_profile: String,
    pub target_profile: String,
    pub target_provider: String,
    pub target_model: String,
    pub target_model_version: String,
    pub status: EmbeddingMigrationStatus,
    pub total_managed_records: usize,
    pub completed_records: usize,
    #[serde(default)]
    pub catch_up_runs: u64,
    #[serde(default)]
    pub reconciled_records: u64,
    #[serde(default)]
    pub last_frontier_sequence: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MigrationSnapshot {
    version: u32,
    source_profile: String,
    target_profile: String,
    target_provider: String,
    target_model: String,
    target_model_version: String,
    records: Vec<StagedRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StagedRecord {
    id: PersistedRecordId,
    source_sequence: u64,
    vector: Vec<f32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CutoverPhase {
    Prepared,
    WalPublished,
    Committed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CutoverJournal {
    version: u32,
    source_profile: String,
    target_profile: String,
    target_provider: String,
    target_model: String,
    target_model_version: String,
    phase: CutoverPhase,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum PersistedRecordId {
    String(String),
    U64(u64),
}

impl From<&RecordId> for PersistedRecordId {
    fn from(value: &RecordId) -> Self {
        match value {
            RecordId::String(value) => Self::String(value.clone()),
            RecordId::Unsigned(value) => Self::U64(*value),
        }
    }
}

impl PersistedRecordId {
    fn into_domain(self) -> Result<RecordId, EmbeddingMigrationError> {
        match self {
            Self::String(value) => RecordId::string(value)
                .map_err(|error| EmbeddingMigrationError::Corrupt(error.to_string())),
            Self::U64(value) => Ok(RecordId::unsigned(value)),
        }
    }
}

#[derive(Clone)]
pub struct EmbeddingMigrationService {
    state: AppState,
}

impl EmbeddingMigrationService {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    #[tracing::instrument(
        skip_all,
        name = "ketebe.embedding.migration.start",
        fields(component = "embedding_migration")
    )]
    pub async fn start(
        &self,
        collection_id: &CollectionId,
        target_profile: impl Into<String>,
    ) -> Result<EmbeddingMigrationState, EmbeddingMigrationError> {
        let target_profile = target_profile.into();
        let collection = CollectionService::new(self.state.clone())
            .get(collection_id)
            .await
            .map_err(EmbeddingMigrationError::Management)?;
        let ingestion = collection
            .ingestion
            .as_ref()
            .ok_or(EmbeddingMigrationError::NoIngestionSchema)?;
        let source_profile = ingestion.embedding_profile().to_string();
        if source_profile == target_profile {
            return Err(EmbeddingMigrationError::TargetAlreadyActive(target_profile));
        }
        let provider = self
            .state
            .embedding_provider_profile(&target_profile)
            .await
            .ok_or_else(|| {
                EmbeddingMigrationError::ProviderProfileUnavailable(target_profile.clone())
            })?;
        if let Some(dimension) = provider.fixed_dimension()
            && dimension != collection.dimension
        {
            return Err(EmbeddingMigrationError::DimensionMismatch {
                expected: collection.dimension,
                actual: dimension,
            });
        }

        let state_path = self.state_path(collection_id);
        if let Ok(existing) = read_state(&state_path)
            && matches!(
                existing.status,
                EmbeddingMigrationStatus::Running
                    | EmbeddingMigrationStatus::Ready
                    | EmbeddingMigrationStatus::Activating
            )
        {
            return Err(EmbeddingMigrationError::MigrationAlreadyExists(
                existing.status,
            ));
        }

        let records = self.managed_live_records(collection_id).await?;
        let model = provider.model();
        let migration = EmbeddingMigrationState {
            version: MIGRATION_STATE_VERSION,
            source_profile: source_profile.clone(),
            target_profile: target_profile.clone(),
            target_provider: provider.provider_name().to_string(),
            target_model: model.name.clone(),
            target_model_version: model.version.clone(),
            status: EmbeddingMigrationStatus::Running,
            total_managed_records: records.len(),
            completed_records: 0,
            catch_up_runs: 0,
            reconciled_records: 0,
            last_frontier_sequence: records
                .iter()
                .map(|r| r.sequence_number().get())
                .max()
                .unwrap_or(0),
            error: None,
        };
        persist_state(&state_path, &migration)?;
        MIGRATIONS_STARTED.fetch_add(1, Ordering::Relaxed);
        MIGRATION_LAST_TOTAL.store(records.len() as u64, Ordering::Relaxed);
        MIGRATION_LAST_COMPLETED.store(0, Ordering::Relaxed);

        let active_key = state_path.clone();
        {
            let mut active = active_migrations().lock().map_err(|_| {
                EmbeddingMigrationError::Corrupt("migration activity lock poisoned".to_string())
            })?;
            if !active.insert(active_key.clone()) {
                return Err(EmbeddingMigrationError::MigrationAlreadyExists(
                    EmbeddingMigrationStatus::Running,
                ));
            }
        }

        let snapshot_path = self.snapshot_path(collection_id);
        let dimension = collection.dimension;
        tokio::spawn(async move {
            let result = build_snapshot(
                provider,
                dimension,
                source_profile,
                target_profile,
                records,
                state_path.clone(),
                snapshot_path,
            )
            .await;
            if let Err(error) = result {
                let mut failed = read_state(&state_path).unwrap_or(EmbeddingMigrationState {
                    version: MIGRATION_STATE_VERSION,
                    source_profile: String::new(),
                    target_profile: String::new(),
                    target_provider: String::new(),
                    target_model: String::new(),
                    target_model_version: String::new(),
                    status: EmbeddingMigrationStatus::Failed,
                    total_managed_records: 0,
                    completed_records: 0,
                    catch_up_runs: 0,
                    reconciled_records: 0,
                    last_frontier_sequence: 0,
                    error: None,
                });
                failed.status = EmbeddingMigrationStatus::Failed;
                failed.error = Some(error.to_string());
                MIGRATION_FAILURES.fetch_add(1, Ordering::Relaxed);
                let _ = persist_state(&state_path, &failed);
            }
            if let Ok(mut active) = active_migrations().lock() {
                active.remove(&active_key);
            }
        });

        Ok(migration)
    }

    pub async fn status(
        &self,
        collection_id: &CollectionId,
    ) -> Result<EmbeddingMigrationState, EmbeddingMigrationError> {
        let state_path = self.state_path(collection_id);
        let mut state = read_state(&state_path)?;
        if state.status == EmbeddingMigrationStatus::Activating {
            return self.recover_cutover(collection_id, state).await;
        }
        if state.status == EmbeddingMigrationStatus::Running {
            let is_active = active_migrations()
                .lock()
                .map_err(|_| {
                    EmbeddingMigrationError::Corrupt("migration activity lock poisoned".to_string())
                })?
                .contains(&state_path);
            if !is_active {
                state.status = EmbeddingMigrationStatus::Failed;
                state.error = Some("migration was interrupted before completion".to_string());
                MIGRATION_FAILURES.fetch_add(1, Ordering::Relaxed);
                persist_state(&state_path, &state)?;
            }
        }
        MIGRATION_LAST_TOTAL.store(state.total_managed_records as u64, Ordering::Relaxed);
        MIGRATION_LAST_COMPLETED.store(state.completed_records as u64, Ordering::Relaxed);
        Ok(state)
    }

    #[tracing::instrument(
        skip_all,
        name = "ketebe.embedding.migration.catch_up",
        fields(component = "embedding_migration")
    )]
    pub async fn catch_up(
        &self,
        collection_id: &CollectionId,
    ) -> Result<EmbeddingMigrationState, EmbeddingMigrationError> {
        let state_path = self.state_path(collection_id);
        let mut migration = self.status(collection_id).await?;
        if migration.status != EmbeddingMigrationStatus::Ready {
            return Err(EmbeddingMigrationError::MigrationNotReady(migration.status));
        }
        let provider = self.validate_target_provider(&migration).await?;
        let mut snapshot = read_snapshot(&self.snapshot_path(collection_id))?;
        validate_snapshot_identity(&migration, &snapshot)?;
        let collection = CollectionService::new(self.state.clone())
            .get(collection_id)
            .await
            .map_err(EmbeddingMigrationError::Management)?;
        let current = self.managed_live_records(collection_id).await?;
        let previous = snapshot
            .records
            .into_iter()
            .map(|record| (record.id.clone(), record))
            .collect::<BTreeMap<_, _>>();
        let mut reconciled = 0_u64;
        let mut staged = Vec::with_capacity(current.len());
        for record in &current {
            let key = PersistedRecordId::from(record.id());
            if let Some(existing) = previous.get(&key)
                && existing.source_sequence == record.sequence_number().get()
            {
                staged.push(existing.clone());
                continue;
            }
            let text = source_text(record.metadata())
                .ok_or_else(|| EmbeddingMigrationError::MissingSourceText(record.id().clone()))?;
            let mut vectors = crate::embed_texts_cached(
                self.state.embedding_cache(),
                &migration.target_profile,
                provider.clone(),
                std::slice::from_ref(&text),
                collection.dimension,
            )
            .await
            .map_err(|error| EmbeddingMigrationError::Provider(error.to_string()))?;
            let vector = vectors.pop().ok_or_else(|| {
                EmbeddingMigrationError::Provider(
                    "embedding cache returned no migration vector".to_string(),
                )
            })?;
            validate_target_vector(&vector, collection.dimension)?;
            staged.push(StagedRecord {
                id: key,
                source_sequence: record.sequence_number().get(),
                vector,
            });
            reconciled = reconciled.saturating_add(1);
        }
        reconciled = reconciled.saturating_add(
            previous
                .keys()
                .filter(|id| !staged.iter().any(|record| &record.id == *id))
                .count() as u64,
        );
        staged.sort_by(|a, b| a.id.cmp(&b.id));
        snapshot.records = staged;
        persist_json(&self.snapshot_path(collection_id), &snapshot)?;
        migration.total_managed_records = current.len();
        migration.completed_records = current.len();
        migration.catch_up_runs = migration.catch_up_runs.saturating_add(1);
        migration.reconciled_records = migration.reconciled_records.saturating_add(reconciled);
        migration.last_frontier_sequence = current
            .iter()
            .map(|record| record.sequence_number().get())
            .max()
            .unwrap_or(0);
        migration.error = None;
        persist_state(&state_path, &migration)?;
        MIGRATION_CATCH_UP_RUNS.fetch_add(1, Ordering::Relaxed);
        MIGRATION_RECONCILED_RECORDS.fetch_add(reconciled, Ordering::Relaxed);
        MIGRATION_LAST_TOTAL.store(current.len() as u64, Ordering::Relaxed);
        MIGRATION_LAST_COMPLETED.store(current.len() as u64, Ordering::Relaxed);
        Ok(migration)
    }

    pub async fn recover_interrupted_cutovers(&self) -> Result<u64, EmbeddingMigrationError> {
        let ids = {
            let catalog = self.state.catalog.read().await;
            catalog.collections.keys().cloned().collect::<Vec<_>>()
        };
        let mut recovered = 0_u64;
        for id in ids {
            let path = self.state_path(&id);
            if !path.exists() {
                continue;
            }
            let state = read_state(&path)?;
            if state.status == EmbeddingMigrationStatus::Activating {
                self.recover_cutover(&id, state).await?;
                recovered = recovered.saturating_add(1);
            }
        }
        Ok(recovered)
    }

    #[tracing::instrument(
        skip_all,
        name = "ketebe.embedding.migration.activate",
        fields(component = "embedding_migration")
    )]
    pub async fn activate(
        &self,
        collection_id: &CollectionId,
    ) -> Result<EmbeddingMigrationState, EmbeddingMigrationError> {
        let state_path = self.state_path(collection_id);
        self.catch_up(collection_id).await?;
        let mut migration = self.status(collection_id).await?;
        let snapshot = read_snapshot(&self.snapshot_path(collection_id))?;
        validate_snapshot_identity(&migration, &snapshot)?;
        let provider = self.validate_target_provider(&migration).await?;
        let current = self.managed_live_records(collection_id).await?;
        if current.len() != snapshot.records.len() {
            return Err(EmbeddingMigrationError::SourceChanged);
        }
        let current_by_id = current
            .into_iter()
            .map(|record| (record.id().clone(), record))
            .collect::<BTreeMap<_, _>>();
        let mut pending = Vec::with_capacity(snapshot.records.len());
        for staged in snapshot.records {
            let id = staged.id.into_domain()?;
            let record = current_by_id
                .get(&id)
                .ok_or(EmbeddingMigrationError::SourceChanged)?;
            if record.sequence_number().get() != staged.source_sequence {
                return Err(EmbeddingMigrationError::SourceChanged);
            }
            let source_text = source_text(record.metadata())
                .ok_or_else(|| EmbeddingMigrationError::MissingSourceText(id.clone()))?;
            let mut metadata = record.metadata().clone();
            set_embedding_provenance(
                &mut metadata,
                &migration.target_profile,
                provider.as_ref(),
                record.vector().len(),
                &source_text,
            );
            pending.push(PendingRecord {
                id,
                vector: staged.vector,
                metadata,
            });
        }

        migration.status = EmbeddingMigrationStatus::Activating;
        migration.error = None;
        persist_state(&state_path, &migration)?;
        let journal_path = self.journal_path(collection_id);
        let mut journal = CutoverJournal {
            version: CUTOVER_JOURNAL_VERSION,
            source_profile: migration.source_profile.clone(),
            target_profile: migration.target_profile.clone(),
            target_provider: migration.target_provider.clone(),
            target_model: migration.target_model.clone(),
            target_model_version: migration.target_model_version.clone(),
            phase: CutoverPhase::Prepared,
        };
        persist_json(&journal_path, &journal)?;
        let writes = WriteService::new(self.state.clone());
        writes
            .publish_embedding_migration_vectors(
                collection_id,
                &migration.source_profile,
                &migration.target_profile,
                pending,
            )
            .await
            .map_err(EmbeddingMigrationError::Write)?;
        journal.phase = CutoverPhase::WalPublished;
        persist_json(&journal_path, &journal)?;
        writes
            .finalize_embedding_profile_cutover(
                collection_id,
                &migration.source_profile,
                &migration.target_profile,
            )
            .await
            .map_err(EmbeddingMigrationError::Write)?;
        journal.phase = CutoverPhase::Committed;
        persist_json(&journal_path, &journal)?;
        migration.status = EmbeddingMigrationStatus::Activated;
        migration.completed_records = migration.total_managed_records;
        persist_state(&state_path, &migration)?;
        MIGRATION_ACTIVATIONS.fetch_add(1, Ordering::Relaxed);
        Ok(migration)
    }

    async fn validate_target_provider(
        &self,
        migration: &EmbeddingMigrationState,
    ) -> Result<Arc<dyn EmbeddingProvider>, EmbeddingMigrationError> {
        let provider = self
            .state
            .embedding_provider_profile(&migration.target_profile)
            .await
            .ok_or_else(|| {
                EmbeddingMigrationError::ProviderProfileUnavailable(
                    migration.target_profile.clone(),
                )
            })?;
        let model = provider.model();
        if provider.provider_name() != migration.target_provider
            || model.name != migration.target_model
            || model.version != migration.target_model_version
        {
            return Err(EmbeddingMigrationError::ProviderChanged);
        }
        Ok(provider)
    }

    async fn recover_cutover(
        &self,
        collection_id: &CollectionId,
        mut migration: EmbeddingMigrationState,
    ) -> Result<EmbeddingMigrationState, EmbeddingMigrationError> {
        let journal = read_journal(&self.journal_path(collection_id))?;
        validate_journal_identity(&migration, &journal)?;
        let collection = CollectionService::new(self.state.clone())
            .get(collection_id)
            .await
            .map_err(EmbeddingMigrationError::Management)?;
        let active_profile = collection
            .ingestion
            .as_ref()
            .ok_or(EmbeddingMigrationError::NoIngestionSchema)?
            .embedding_profile()
            .to_string();
        if active_profile == migration.target_profile {
            migration.status = EmbeddingMigrationStatus::Activated;
            migration.error = None;
            persist_state(&self.state_path(collection_id), &migration)?;
            MIGRATION_RECOVERIES.fetch_add(1, Ordering::Relaxed);
            return Ok(migration);
        }
        if active_profile != migration.source_profile {
            return Err(EmbeddingMigrationError::SourceChanged);
        }
        let snapshot = read_snapshot(&self.snapshot_path(collection_id))?;
        let current = self.managed_live_records(collection_id).await?;
        let current_by_id = current
            .iter()
            .map(|record| (PersistedRecordId::from(record.id()), record))
            .collect::<BTreeMap<_, _>>();
        let mut target_matches = 0_usize;
        for staged in &snapshot.records {
            if let Some(record) = current_by_id.get(&staged.id)
                && embedding_profile(record.metadata()) == Some(migration.target_profile.as_str())
                && record.vector().as_slice() == staged.vector.as_slice()
            {
                target_matches += 1;
            }
        }
        if target_matches == snapshot.records.len() && !snapshot.records.is_empty() {
            WriteService::new(self.state.clone())
                .finalize_embedding_profile_cutover(
                    collection_id,
                    &migration.source_profile,
                    &migration.target_profile,
                )
                .await
                .map_err(EmbeddingMigrationError::Write)?;
            migration.status = EmbeddingMigrationStatus::Activated;
            migration.error = None;
            persist_state(&self.state_path(collection_id), &migration)?;
            MIGRATION_RECOVERIES.fetch_add(1, Ordering::Relaxed);
            return Ok(migration);
        }
        if target_matches == 0 && journal.phase == CutoverPhase::Prepared {
            migration.status = EmbeddingMigrationStatus::Ready;
            migration.error =
                Some("interrupted before WAL publication; activation can be retried".to_string());
            persist_state(&self.state_path(collection_id), &migration)?;
            MIGRATION_RECOVERIES.fetch_add(1, Ordering::Relaxed);
            return Ok(migration);
        }
        migration.status = EmbeddingMigrationStatus::Failed;
        migration.error = Some("cutover journal and replayed managed vectors disagree".to_string());
        persist_state(&self.state_path(collection_id), &migration)?;
        MIGRATION_FAILURES.fetch_add(1, Ordering::Relaxed);
        Err(EmbeddingMigrationError::Corrupt(
            "cutover journal and replayed managed vectors disagree".to_string(),
        ))
    }

    async fn managed_live_records(
        &self,
        collection_id: &CollectionId,
    ) -> Result<Vec<Record>, EmbeddingMigrationError> {
        let catalog = self.state.catalog.read().await;
        let runtime = catalog
            .collections
            .get(collection_id)
            .ok_or_else(|| EmbeddingMigrationError::CollectionNotFound(collection_id.clone()))?;
        let segments = runtime
            .query_segments()
            .map_err(|error| EmbeddingMigrationError::Corrupt(error.to_string()))?;
        let mut latest = BTreeMap::<RecordId, (SequenceNumber, Option<Record>)>::new();
        for segment in segments {
            if segment.collection_id() != collection_id {
                continue;
            }
            for record in segment.records() {
                apply_latest(
                    &mut latest,
                    record.id().clone(),
                    record.sequence_number(),
                    Some(record.clone()),
                );
            }
            for tombstone in segment.tombstones() {
                apply_latest(
                    &mut latest,
                    tombstone.record_id().clone(),
                    tombstone.sequence_number(),
                    None,
                );
            }
        }
        Ok(latest
            .into_values()
            .filter_map(|(_, record)| record)
            .filter(|record| record.metadata().contains_key(EMBEDDING_METADATA_KEY))
            .collect())
    }

    fn collection_dir(&self, id: &CollectionId) -> PathBuf {
        self.state.data_dir.join("collections").join(id.as_str())
    }

    fn state_path(&self, id: &CollectionId) -> PathBuf {
        self.collection_dir(id).join(MIGRATION_STATE_FILE)
    }

    fn snapshot_path(&self, id: &CollectionId) -> PathBuf {
        self.collection_dir(id).join(MIGRATION_SNAPSHOT_FILE)
    }

    fn journal_path(&self, id: &CollectionId) -> PathBuf {
        self.collection_dir(id).join(CUTOVER_JOURNAL_FILE)
    }
}

async fn build_snapshot(
    provider: Arc<dyn EmbeddingProvider>,
    dimension: usize,
    source_profile: String,
    target_profile: String,
    records: Vec<Record>,
    state_path: PathBuf,
    snapshot_path: PathBuf,
) -> Result<(), EmbeddingMigrationError> {
    let model = provider.model();
    let mut migration = read_state(&state_path)?;
    let mut staged = Vec::with_capacity(records.len());
    for record in records {
        let source_text = source_text(record.metadata())
            .ok_or_else(|| EmbeddingMigrationError::MissingSourceText(record.id().clone()))?;
        let vector = provider
            .embed(&source_text, dimension)
            .await
            .map_err(|error| EmbeddingMigrationError::Provider(error.to_string()))?;
        if vector.len() != dimension {
            return Err(EmbeddingMigrationError::DimensionMismatch {
                expected: dimension,
                actual: vector.len(),
            });
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(EmbeddingMigrationError::Provider(
                "provider returned non-finite embedding".to_string(),
            ));
        }
        staged.push(StagedRecord {
            id: PersistedRecordId::from(record.id()),
            source_sequence: record.sequence_number().get(),
            vector,
        });
        migration.completed_records = staged.len();
        MIGRATION_LAST_COMPLETED.store(staged.len() as u64, Ordering::Relaxed);
        persist_state(&state_path, &migration)?;
    }
    let snapshot = MigrationSnapshot {
        version: MIGRATION_STATE_VERSION,
        source_profile,
        target_profile,
        target_provider: provider.provider_name().to_string(),
        target_model: model.name,
        target_model_version: model.version,
        records: staged,
    };
    persist_json(&snapshot_path, &snapshot)?;
    migration.status = EmbeddingMigrationStatus::Ready;
    migration.error = None;
    persist_state(&state_path, &migration)?;
    Ok(())
}

fn validate_target_vector(vector: &[f32], dimension: usize) -> Result<(), EmbeddingMigrationError> {
    if vector.len() != dimension {
        return Err(EmbeddingMigrationError::DimensionMismatch {
            expected: dimension,
            actual: vector.len(),
        });
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(EmbeddingMigrationError::Provider(
            "provider returned non-finite embedding".to_string(),
        ));
    }
    Ok(())
}

fn validate_snapshot_identity(
    migration: &EmbeddingMigrationState,
    snapshot: &MigrationSnapshot,
) -> Result<(), EmbeddingMigrationError> {
    if snapshot.source_profile != migration.source_profile
        || snapshot.target_profile != migration.target_profile
        || snapshot.target_provider != migration.target_provider
        || snapshot.target_model != migration.target_model
        || snapshot.target_model_version != migration.target_model_version
    {
        return Err(EmbeddingMigrationError::Corrupt(
            "migration state and staged snapshot disagree".to_string(),
        ));
    }
    Ok(())
}

fn validate_journal_identity(
    migration: &EmbeddingMigrationState,
    journal: &CutoverJournal,
) -> Result<(), EmbeddingMigrationError> {
    if journal.source_profile != migration.source_profile
        || journal.target_profile != migration.target_profile
        || journal.target_provider != migration.target_provider
        || journal.target_model != migration.target_model
        || journal.target_model_version != migration.target_model_version
    {
        return Err(EmbeddingMigrationError::Corrupt(
            "migration state and cutover journal disagree".to_string(),
        ));
    }
    Ok(())
}

fn embedding_profile(metadata: &Metadata) -> Option<&str> {
    let MetadataValue::Object(embedding) = metadata.get(EMBEDDING_METADATA_KEY)? else {
        return None;
    };
    let MetadataValue::String(profile) = embedding.get("profile")? else {
        return None;
    };
    Some(profile.as_str())
}

fn source_text(metadata: &Metadata) -> Option<String> {
    if let Some(MetadataValue::Object(chunk)) = metadata.get(crate::CHUNK_METADATA_KEY)
        && let Some(MetadataValue::String(text)) = chunk.get("text")
    {
        return Some(text.clone());
    }
    if let Some(MetadataValue::Object(embedding)) = metadata.get(EMBEDDING_METADATA_KEY)
        && let Some(MetadataValue::String(text)) = embedding.get("source_text")
    {
        return Some(text.clone());
    }
    None
}

pub(crate) fn set_embedding_provenance(
    metadata: &mut Metadata,
    profile: &str,
    provider: &dyn EmbeddingProvider,
    dimension: usize,
    source_text: &str,
) {
    let model = provider.model();
    let mut provenance = BTreeMap::new();
    provenance.insert(
        "profile".to_string(),
        MetadataValue::String(profile.to_string()),
    );
    provenance.insert(
        "provider".to_string(),
        MetadataValue::String(provider.provider_name().to_string()),
    );
    provenance.insert("model".to_string(), MetadataValue::String(model.name));
    provenance.insert("version".to_string(), MetadataValue::String(model.version));
    provenance.insert(
        "dimension".to_string(),
        MetadataValue::Number(dimension as f64),
    );
    provenance.insert(
        "source_text".to_string(),
        MetadataValue::String(source_text.to_string()),
    );
    metadata.insert(
        EMBEDDING_METADATA_KEY.to_string(),
        MetadataValue::Object(provenance),
    );
}

fn apply_latest(
    latest: &mut BTreeMap<RecordId, (SequenceNumber, Option<Record>)>,
    id: RecordId,
    sequence: SequenceNumber,
    record: Option<Record>,
) {
    match latest.get(&id) {
        Some((existing, _)) if *existing >= sequence => {}
        _ => {
            latest.insert(id, (sequence, record));
        }
    }
}

fn read_state(path: &Path) -> Result<EmbeddingMigrationState, EmbeddingMigrationError> {
    if !path.exists() {
        return Err(EmbeddingMigrationError::MigrationNotFound);
    }
    let state: EmbeddingMigrationState = serde_json::from_slice(&fs::read(path)?)?;
    if state.version != MIGRATION_STATE_VERSION {
        return Err(EmbeddingMigrationError::Corrupt(format!(
            "unsupported migration state version {}",
            state.version
        )));
    }
    Ok(state)
}

fn read_snapshot(path: &Path) -> Result<MigrationSnapshot, EmbeddingMigrationError> {
    if !path.exists() {
        return Err(EmbeddingMigrationError::Corrupt(
            "migration snapshot is missing".to_string(),
        ));
    }
    let snapshot: MigrationSnapshot = serde_json::from_slice(&fs::read(path)?)?;
    if snapshot.version != MIGRATION_STATE_VERSION {
        return Err(EmbeddingMigrationError::Corrupt(format!(
            "unsupported migration snapshot version {}",
            snapshot.version
        )));
    }
    Ok(snapshot)
}

fn read_journal(path: &Path) -> Result<CutoverJournal, EmbeddingMigrationError> {
    if !path.exists() {
        return Err(EmbeddingMigrationError::Corrupt(
            "cutover journal is missing".to_string(),
        ));
    }
    let journal: CutoverJournal = serde_json::from_slice(&fs::read(path)?)?;
    if journal.version != CUTOVER_JOURNAL_VERSION {
        return Err(EmbeddingMigrationError::Corrupt(format!(
            "unsupported cutover journal version {}",
            journal.version
        )));
    }
    Ok(journal)
}

fn persist_state(
    path: &Path,
    state: &EmbeddingMigrationState,
) -> Result<(), EmbeddingMigrationError> {
    persist_json(path, state)
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<(), EmbeddingMigrationError> {
    let parent = path.parent().ok_or_else(|| {
        EmbeddingMigrationError::Corrupt("migration path has no parent".to_string())
    })?;
    fs::create_dir_all(parent)?;
    let temp = path.with_extension("tmp");
    if temp.exists() {
        fs::remove_file(&temp)?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.flush()?;
    file.sync_data()?;
    drop(file);
    fs::rename(&temp, path)?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[derive(Debug)]
pub enum EmbeddingMigrationError {
    MigrationNotFound,
    MigrationAlreadyExists(EmbeddingMigrationStatus),
    MigrationNotReady(EmbeddingMigrationStatus),
    NoIngestionSchema,
    TargetAlreadyActive(String),
    ProviderProfileUnavailable(String),
    ProviderChanged,
    DimensionMismatch { expected: usize, actual: usize },
    MissingSourceText(RecordId),
    SourceChanged,
    CollectionNotFound(CollectionId),
    Provider(String),
    Corrupt(String),
    Management(crate::ManagementError),
    Write(WriteError),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for EmbeddingMigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MigrationNotFound => f.write_str("embedding migration was not found"),
            Self::MigrationAlreadyExists(status) => {
                write!(f, "embedding migration already exists in state {status:?}")
            }
            Self::MigrationNotReady(status) => {
                write!(
                    f,
                    "embedding migration is not ready for activation: {status:?}"
                )
            }
            Self::NoIngestionSchema => {
                f.write_str("collection does not have a managed ingestion schema")
            }
            Self::TargetAlreadyActive(profile) => {
                write!(f, "embedding profile '{profile}' is already active")
            }
            Self::ProviderProfileUnavailable(profile) => {
                write!(f, "embedding profile '{profile}' is not available")
            }
            Self::ProviderChanged => {
                f.write_str("target provider model/version changed since migration build")
            }
            Self::DimensionMismatch { expected, actual } => write!(
                f,
                "target embedding dimension mismatch: expected {expected}, got {actual}"
            ),
            Self::MissingSourceText(id) => {
                write!(f, "managed record {id:?} does not retain source text")
            }
            Self::SourceChanged => {
                f.write_str("managed records changed after migration snapshot was built")
            }
            Self::CollectionNotFound(id) => write!(f, "collection not found: {id}"),
            Self::Provider(message) => write!(f, "embedding provider failed: {message}"),
            Self::Corrupt(message) => write!(f, "embedding migration state is corrupt: {message}"),
            Self::Management(error) => write!(f, "collection lookup failed: {error}"),
            Self::Write(error) => write!(f, "embedding migration write failed: {error}"),
            Self::Io(error) => write!(f, "embedding migration I/O failed: {error}"),
            Self::Json(error) => write!(f, "embedding migration JSON failed: {error}"),
        }
    }
}

impl std::error::Error for EmbeddingMigrationError {}

impl From<std::io::Error> for EmbeddingMigrationError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for EmbeddingMigrationError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
