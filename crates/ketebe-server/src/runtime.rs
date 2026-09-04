use ketebe_core::{
    ChunkingPolicy, ChunkingStructure, CollectionConfig, CollectionId, CollectionIngestionConfig,
    DataPlaneScope, DistanceMetric, FieldPath, LexicalAnalyzerConfig, SemanticChunkingPolicy,
    TokenChunkingPolicy, TokenizerKind,
};
use ketebe_storage::{
    Checkpoint, CheckpointStore, HnswConfig, HnswIndex, HnswIndexStore, HnswLoadResult,
    LexicalIndex, LexicalIndexError, LexicalIndexStore, LexicalLoadResult, ScopedStorageNamespace,
    ScopedWal, Segment, SegmentId, SegmentStore, Wal, WalMutation, lexical_checkpoint_fingerprint,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

use crate::embedding::{EmbeddingProfileInfo, EmbeddingProvider, EmbeddingProviderRegistry};
use crate::embedding_cache::EmbeddingCache;
use crate::jobs::{DEFAULT_JOB_CONCURRENCY, JobRuntime};
use crate::lexical_scheduler::LexicalBuildScheduler;
use crate::lifecycle::{Lifecycle, LifecyclePhase, LifecycleWriteGuard};
use crate::query_runtime::{QueryLimits, QueryRuntime};
use crate::reranking::{Reranker, RerankerProfileInfo, RerankerRegistry};

pub const DEFAULT_SEAL_THRESHOLD: usize = 1_000;

/// Runtime lifecycle for the collection's checkpoint-scoped persistent lexical index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexicalBuildState {
    Disabled,
    Missing {
        fingerprint: u64,
    },
    Queued {
        fingerprint: u64,
    },
    Building {
        fingerprint: u64,
        attempt: u32,
    },
    Retrying {
        fingerprint: u64,
        attempt: u32,
        delay_ms: u64,
    },
    Ready {
        fingerprint: u64,
    },
    Stale {
        fingerprint: u64,
    },
    Failed {
        fingerprint: u64,
        message: String,
    },
}

pub struct CollectionRuntime {
    /// Immutable ownership binding for production project-scoped collections.
    pub(crate) scope: Option<DataPlaneScope>,
    pub(crate) metric: DistanceMetric,
    pub(crate) config: Option<CollectionConfig>,
    pub(crate) segments: Vec<Segment>,
    pub(crate) mutable: Vec<WalMutation>,
    pub(crate) hnsw: Option<HnswIndex>,
    pub(crate) wal_path: Option<PathBuf>,
    pub(crate) wal: Option<Wal>,
    pub(crate) scoped_wal: Option<ScopedWal>,
    pub(crate) next_sequence: u64,
    pub(crate) checkpoint: Option<Checkpoint>,
    lexical_indexes: Mutex<BTreeMap<u64, Arc<LexicalIndex>>>,
    lexical_state: Mutex<LexicalBuildState>,
}

pub(crate) struct WritableCollectionRuntime {
    pub(crate) scope: Option<DataPlaneScope>,
    pub(crate) config: CollectionConfig,
    pub(crate) segments: Vec<Segment>,
    pub(crate) mutable: Vec<WalMutation>,
    pub(crate) wal_path: PathBuf,
    pub(crate) wal: Option<Wal>,
    pub(crate) scoped_wal: Option<ScopedWal>,
    pub(crate) checkpoint: Option<Checkpoint>,
}

impl CollectionRuntime {
    #[must_use]
    pub fn new(metric: DistanceMetric, segments: Vec<Segment>, hnsw: Option<HnswIndex>) -> Self {
        let next_sequence = segments
            .iter()
            .map(|segment| segment.max_sequence().get())
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        Self {
            scope: None,
            metric,
            config: None,
            segments,
            mutable: Vec::new(),
            hnsw,
            wal_path: None,
            wal: None,
            scoped_wal: None,
            next_sequence,
            checkpoint: None,
            lexical_indexes: Mutex::new(BTreeMap::new()),
            lexical_state: Mutex::new(LexicalBuildState::Disabled),
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn new_scoped(
        metric: DistanceMetric,
        segments: Vec<Segment>,
        hnsw: Option<HnswIndex>,
        scope: DataPlaneScope,
    ) -> Self {
        let mut runtime = Self::new(metric, segments, hnsw);
        runtime.scope = Some(scope);
        runtime
    }

    #[must_use]
    pub(crate) fn matches_scope(&self, scope: &DataPlaneScope) -> bool {
        self.scope.as_ref() == Some(scope)
    }

    pub(crate) fn writable(input: WritableCollectionRuntime) -> Self {
        let max_segment = input
            .segments
            .iter()
            .map(|segment| segment.max_sequence().get())
            .max()
            .unwrap_or(0);
        let max_wal = input
            .mutable
            .iter()
            .map(|mutation| mutation.sequence_number().get())
            .max()
            .unwrap_or(0);
        let max_checkpoint = input
            .checkpoint
            .as_ref()
            .map(|value| value.sequence_number().get())
            .unwrap_or(0);
        let lexical_state = if input.config.lexical_fields().is_empty() {
            LexicalBuildState::Disabled
        } else if let Some(value) = &input.checkpoint {
            LexicalBuildState::Missing {
                fingerprint: lexical_checkpoint_fingerprint(
                    value,
                    input.config.lexical_fields(),
                    input.config.lexical_analyzer(),
                ),
            }
        } else {
            LexicalBuildState::Disabled
        };
        Self {
            scope: input.scope,
            metric: input.config.distance_metric(),
            config: Some(input.config),
            segments: input.segments,
            mutable: input.mutable,
            hnsw: None,
            wal_path: Some(input.wal_path),
            wal: input.wal,
            scoped_wal: input.scoped_wal,
            next_sequence: max_segment
                .max(max_wal)
                .max(max_checkpoint)
                .saturating_add(1),
            checkpoint: input.checkpoint,
            lexical_indexes: Mutex::new(BTreeMap::new()),
            lexical_state: Mutex::new(lexical_state),
        }
    }

    pub(crate) fn query_segments(&self) -> Result<Vec<Segment>, ketebe_storage::SegmentError> {
        let mut segments = self.segments.clone();
        if !self.mutable.is_empty() {
            segments.push(Segment::from_mutations(
                SegmentId::new(u64::MAX),
                &self.mutable,
            )?);
        }
        Ok(segments)
    }

    pub(crate) fn query_hnsw(&self) -> Option<&HnswIndex> {
        if self.mutable.is_empty() {
            self.hnsw.as_ref()
        } else {
            None
        }
    }

    pub(crate) fn configured_lexical_analyzer(&self) -> LexicalAnalyzerConfig {
        self.config
            .as_ref()
            .map(CollectionConfig::lexical_analyzer)
            .unwrap_or_default()
    }

    pub(crate) fn configured_lexical_fields(&self) -> &[FieldPath] {
        self.config
            .as_ref()
            .map(CollectionConfig::lexical_fields)
            .unwrap_or(&[])
    }

    pub(crate) fn lexical_state(&self) -> LexicalBuildState {
        self.lexical_state
            .lock()
            .map(|state| state.clone())
            .unwrap_or_else(|_| LexicalBuildState::Failed {
                fingerprint: 0,
                message: "lexical lifecycle lock poisoned".to_string(),
            })
    }

    pub(crate) fn set_lexical_state(&self, state: LexicalBuildState) {
        if let Ok(mut current) = self.lexical_state.lock() {
            *current = state;
        }
    }

    pub(crate) fn invalidate_lexical_index(&self) {
        if let Ok(mut indexes) = self.lexical_indexes.lock() {
            indexes.clear();
        }
        match (
            &self.checkpoint,
            self.configured_lexical_fields().is_empty(),
        ) {
            (_, true) => self.set_lexical_state(LexicalBuildState::Disabled),
            (Some(checkpoint), false) => self.set_lexical_state(LexicalBuildState::Missing {
                fingerprint: lexical_checkpoint_fingerprint(
                    checkpoint,
                    self.configured_lexical_fields(),
                    self.configured_lexical_analyzer(),
                ),
            }),
            (None, false) => self.set_lexical_state(LexicalBuildState::Disabled),
        }
    }

    pub(crate) fn install_lexical_index(
        &self,
        fingerprint: u64,
        index: LexicalIndex,
    ) -> Result<Arc<LexicalIndex>, LexicalIndexError> {
        let index = Arc::new(index);
        let mut indexes = self
            .lexical_indexes
            .lock()
            .map_err(|_| LexicalIndexError::Corrupt("lexical index cache lock poisoned"))?;
        indexes.clear();
        indexes.insert(fingerprint, Arc::clone(&index));
        self.set_lexical_state(LexicalBuildState::Ready { fingerprint });
        Ok(index)
    }

    pub(crate) fn query_lexical_index(
        &self,
        collection_directory: &Path,
        fields: &[FieldPath],
    ) -> Result<Option<Arc<LexicalIndex>>, LexicalIndexError> {
        if !self.mutable.is_empty() {
            return Ok(None);
        }
        let Some(checkpoint) = &self.checkpoint else {
            return Ok(None);
        };
        let configured = self.configured_lexical_fields();
        if configured.is_empty() || configured != fields {
            return Ok(None);
        }
        let fingerprint = lexical_checkpoint_fingerprint(
            checkpoint,
            configured,
            self.configured_lexical_analyzer(),
        );
        if let Some(index) = self
            .lexical_indexes
            .lock()
            .map_err(|_| LexicalIndexError::Corrupt("lexical index cache lock poisoned"))?
            .get(&fingerprint)
            .cloned()
        {
            return Ok(Some(index));
        }
        let store = LexicalIndexStore::open(collection_directory)?;
        match store.load(
            checkpoint,
            configured,
            self.configured_lexical_analyzer(),
            &self.segments,
        )? {
            LexicalLoadResult::Loaded(index) => {
                self.install_lexical_index(fingerprint, index).map(Some)
            }
            LexicalLoadResult::Missing => {
                self.set_lexical_state(LexicalBuildState::Missing { fingerprint });
                Ok(None)
            }
            LexicalLoadResult::Stale => {
                self.set_lexical_state(LexicalBuildState::Stale { fingerprint });
                Ok(None)
            }
        }
    }
}

#[derive(Default)]
pub struct RuntimeCatalog {
    pub(crate) ready: bool,
    pub(crate) collections: BTreeMap<CollectionId, CollectionRuntime>,
}

impl RuntimeCatalog {
    #[must_use]
    pub fn empty_ready() -> Self {
        Self {
            ready: true,
            collections: BTreeMap::new(),
        }
    }

    pub fn set_ready(&mut self, ready: bool) {
        self.ready = ready;
    }

    pub fn insert_collection(&mut self, id: CollectionId, runtime: CollectionRuntime) {
        self.collections.insert(id, runtime);
    }
}

#[derive(Clone)]
pub struct AppState {
    pub(crate) catalog: Arc<RwLock<RuntimeCatalog>>,
    pub(crate) data_dir: Arc<PathBuf>,
    pub(crate) seal_threshold: usize,
    pub(crate) lexical_scheduler: Arc<LexicalBuildScheduler>,
    embedding_registry: Arc<RwLock<EmbeddingProviderRegistry>>,
    reranker_registry: Arc<RwLock<RerankerRegistry>>,
    embedding_cache: Arc<EmbeddingCache>,
    job_runtime: Arc<JobRuntime>,
    query_runtime: Arc<QueryRuntime>,
    lifecycle: Arc<Lifecycle>,
    authorization: Arc<crate::AuthorizationService>,
    governance: Arc<crate::GovernanceService>,
    audit: Arc<crate::AuditService>,
}

impl AppState {
    #[must_use]
    pub fn new(catalog: RuntimeCatalog) -> Self {
        Self::with_data_dir(catalog, std::env::temp_dir().join("ketebe-test-runtime"))
    }

    #[must_use]
    pub fn with_data_dir(catalog: RuntimeCatalog, data_dir: PathBuf) -> Self {
        Self::with_data_dir_and_threshold(catalog, data_dir, DEFAULT_SEAL_THRESHOLD)
    }

    #[must_use]
    pub fn with_data_dir_and_threshold(
        catalog: RuntimeCatalog,
        data_dir: PathBuf,
        seal_threshold: usize,
    ) -> Self {
        Self::with_data_dir_threshold_and_query_limits(
            catalog,
            data_dir,
            seal_threshold,
            QueryLimits::default(),
        )
    }

    #[must_use]
    pub fn with_data_dir_threshold_and_query_limits(
        catalog: RuntimeCatalog,
        data_dir: PathBuf,
        seal_threshold: usize,
        query_limits: QueryLimits,
    ) -> Self {
        let job_runtime = Arc::new(JobRuntime::new(&data_dir, DEFAULT_JOB_CONCURRENCY));
        let audit =
            crate::AuditService::durable(&data_dir).unwrap_or_else(|_| crate::AuditService::noop());
        let query_runtime = Arc::new(
            QueryRuntime::new(query_limits)
                .expect("query limits supplied to AppState must be valid"),
        );
        Self {
            catalog: Arc::new(RwLock::new(catalog)),
            data_dir: Arc::new(data_dir),
            seal_threshold: seal_threshold.max(1),
            lexical_scheduler: Arc::new(LexicalBuildScheduler::default()),
            embedding_registry: Arc::new(RwLock::new(EmbeddingProviderRegistry::new())),
            reranker_registry: Arc::new(RwLock::new(RerankerRegistry::new())),
            embedding_cache: Arc::new(EmbeddingCache::default()),
            job_runtime,
            query_runtime,
            lifecycle: Arc::new(Lifecycle::ready()),
            authorization: Arc::new(crate::AuthorizationService::development()),
            governance: Arc::new(crate::GovernanceService::new()),
            audit: Arc::new(audit),
        }
    }

    pub async fn set_embedding_provider(&self, provider: Arc<dyn EmbeddingProvider>) {
        self.embedding_cache.clear();
        let mut registry = EmbeddingProviderRegistry::new();
        registry
            .register("default", provider)
            .expect("default embedding profile is valid");
        registry
            .set_default("default")
            .expect("registered default embedding profile exists");
        *self.embedding_registry.write().await = registry;
    }

    pub async fn set_embedding_provider_registry(&self, registry: EmbeddingProviderRegistry) {
        self.embedding_cache.clear();
        *self.embedding_registry.write().await = registry;
    }

    pub async fn clear_embedding_provider(&self) {
        self.embedding_cache.clear();
        *self.embedding_registry.write().await = EmbeddingProviderRegistry::new();
    }

    pub(crate) async fn embedding_profiles(&self) -> Vec<EmbeddingProfileInfo> {
        self.embedding_registry.read().await.profiles()
    }

    pub async fn set_reranker(&self, reranker: Arc<dyn Reranker>) {
        let mut registry = RerankerRegistry::new();
        registry
            .register("default", reranker)
            .expect("default reranker profile is valid");
        registry
            .set_default("default")
            .expect("registered default reranker profile exists");
        *self.reranker_registry.write().await = registry;
    }

    pub async fn set_reranker_registry(&self, registry: RerankerRegistry) {
        *self.reranker_registry.write().await = registry;
    }

    pub async fn clear_reranker(&self) {
        *self.reranker_registry.write().await = RerankerRegistry::new();
    }

    pub(crate) async fn reranker_profiles(&self) -> Vec<RerankerProfileInfo> {
        self.reranker_registry.read().await.profiles()
    }

    pub(crate) async fn reranker_profile(&self, profile: &str) -> Option<Arc<dyn Reranker>> {
        self.reranker_registry.read().await.resolve(profile)
    }

    pub(crate) fn embedding_cache(&self) -> Arc<EmbeddingCache> {
        Arc::clone(&self.embedding_cache)
    }

    pub(crate) fn job_runtime(&self) -> Arc<JobRuntime> {
        Arc::clone(&self.job_runtime)
    }

    pub(crate) fn query_runtime(&self) -> Arc<QueryRuntime> {
        Arc::clone(&self.query_runtime)
    }

    #[must_use]
    pub fn query_prometheus_metrics(&self) -> String {
        self.query_runtime.prometheus_metrics()
    }

    #[must_use]
    pub fn lifecycle_phase(&self) -> LifecyclePhase {
        self.lifecycle.phase()
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.lifecycle.is_ready()
    }

    #[must_use]
    pub fn try_admit_foreground_write(&self) -> Option<LifecycleWriteGuard> {
        self.lifecycle.try_admit_foreground_write()
    }

    pub async fn wait_for_foreground_writes_drained(&self) {
        self.lifecycle.wait_for_foreground_writes_drained().await;
    }

    #[must_use]
    pub fn with_authorization(mut self, authorization: crate::AuthorizationService) -> Self {
        self.authorization = Arc::new(authorization);
        self
    }

    #[must_use]
    pub fn authorization(&self) -> Arc<crate::AuthorizationService> {
        Arc::clone(&self.authorization)
    }

    #[must_use]
    pub fn with_governance(mut self, governance: crate::GovernanceService) -> Self {
        self.governance = Arc::new(governance);
        self
    }

    #[must_use]
    pub fn governance(&self) -> Arc<crate::GovernanceService> {
        Arc::clone(&self.governance)
    }

    #[must_use]
    pub fn with_audit(mut self, audit: crate::AuditService) -> Self {
        self.audit = Arc::new(audit);
        self
    }

    #[must_use]
    pub fn audit(&self) -> Arc<crate::AuditService> {
        Arc::clone(&self.audit)
    }

    #[must_use]
    pub fn governance_prometheus_metrics(&self) -> String {
        self.governance.prometheus_metrics().unwrap_or_default()
    }

    pub fn begin_draining(&self) {
        self.lifecycle.begin_draining();
        if let Ok(mut catalog) = self.catalog.try_write() {
            catalog.ready = false;
        }
    }

    pub fn mark_stopped(&self) {
        self.lifecycle.mark_stopped();
    }

    #[must_use]
    pub fn lifecycle(&self) -> Arc<Lifecycle> {
        Arc::clone(&self.lifecycle)
    }

    pub(crate) async fn embedding_provider(&self) -> Option<Arc<dyn EmbeddingProvider>> {
        self.embedding_registry.read().await.default_provider()
    }

    pub(crate) async fn embedding_provider_profile(
        &self,
        profile: &str,
    ) -> Option<Arc<dyn EmbeddingProvider>> {
        self.embedding_registry.read().await.resolve(profile)
    }

    pub fn recover(data_dir: impl AsRef<Path>) -> Result<Self, RuntimeError> {
        Self::recover_with_threshold(data_dir, DEFAULT_SEAL_THRESHOLD)
    }

    pub fn recover_with_threshold(
        data_dir: impl AsRef<Path>,
        seal_threshold: usize,
    ) -> Result<Self, RuntimeError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let collections_dir = data_dir.join("collections");
        fs::create_dir_all(&collections_dir)?;
        let mut catalog = RuntimeCatalog::empty_ready();
        let namespace_catalog = crate::CollectionNamespaceCatalog::open(&data_dir)
            .map_err(|error| RuntimeError::InvalidCollectionMetadata(error.to_string()))?;
        let mut recovery_targets = Vec::new();
        for scope in namespace_catalog
            .list_all_scopes()
            .map_err(|error| RuntimeError::InvalidCollectionMetadata(error.to_string()))?
        {
            let namespace = match ScopedStorageNamespace::open_existing(&data_dir, scope.clone()) {
                Ok(namespace) => namespace,
                Err(ketebe_storage::NamespaceError::MissingNamespace(_))
                    if scope.project_id().as_str() == "default" =>
                {
                    ScopedStorageNamespace::migrate_legacy_default(&data_dir, scope.clone())
                        .map_err(|error| {
                            RuntimeError::InvalidCollectionMetadata(error.to_string())
                        })?
                }
                Err(error) => {
                    return Err(RuntimeError::InvalidCollectionMetadata(error.to_string()));
                }
            };
            recovery_targets.push((namespace.root().to_path_buf(), Some(scope)));
        }
        for entry in fs::read_dir(&collections_dir)? {
            let path = entry?.path();
            if path.is_dir() {
                recovery_targets.push((path, None));
            }
        }

        for (path, expected_scope) in recovery_targets {
            let config_path = path.join("collection.json");
            if !config_path.exists() {
                continue;
            }
            let persisted: PersistedCollection = serde_json::from_slice(&fs::read(&config_path)?)?;
            if !matches!(persisted.version, 1..=6) {
                return Err(RuntimeError::UnsupportedCollectionVersion(
                    persisted.version,
                ));
            }
            let id = CollectionId::new(persisted.id)
                .map_err(|error| RuntimeError::InvalidCollectionMetadata(error.to_string()))?;
            if let Some(scope) = &expected_scope
                && scope.collection_id() != &id
            {
                return Err(RuntimeError::InvalidCollectionMetadata(
                    "storage namespace collection identity does not match collection metadata"
                        .to_string(),
                ));
            }
            let metric = persisted.metric.into_domain();
            let lexical_fields = persisted
                .lexical_fields
                .into_iter()
                .map(|segments| {
                    FieldPath::new(segments)
                        .map_err(|error| RuntimeError::InvalidCollectionMetadata(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let ingestion = persisted
                .ingestion
                .map(PersistedIngestionConfig::into_domain)
                .transpose()?;
            let mut config = CollectionConfig::new(id.clone(), persisted.dimension, metric)
                .map_err(|error| RuntimeError::InvalidCollectionMetadata(error.to_string()))?
                .with_lexical_fields(lexical_fields)
                .with_lexical_analyzer(persisted.lexical_analyzer.into_domain());
            if let Some(ingestion) = ingestion {
                config = config.with_ingestion(ingestion);
            }

            let segment_store = SegmentStore::open(path.join("segments"))?;
            let checkpoint = CheckpointStore::open(&path)?.load()?;
            if let Some(value) = &checkpoint
                && value.collection_id() != &id
            {
                return Err(RuntimeError::InvalidCollectionMetadata(
                    "checkpoint belongs to a different collection".to_string(),
                ));
            }

            let segments = if let Some(value) = &checkpoint {
                let mut named = Vec::with_capacity(value.segments().len());
                for segment_id in value.segments() {
                    let segment = segment_store.open_segment(*segment_id)?;
                    if segment.collection_id() != &id {
                        return Err(RuntimeError::InvalidCollectionMetadata(
                            "checkpoint references a segment from a different collection"
                                .to_string(),
                        ));
                    }
                    if segment.max_sequence() > value.sequence_number() {
                        return Err(RuntimeError::InvalidCollectionMetadata(
                            "checkpoint sequence is older than a referenced segment".to_string(),
                        ));
                    }
                    named.push(segment);
                }
                named
            } else {
                segment_store.discover()?
            };

            let wal_path = path.join("wal.log");
            let (wal, scoped_wal, replay) = if let Some(scope) = &expected_scope {
                let wal = ScopedWal::open_existing(&data_dir, scope.clone())
                    .map_err(|error| RuntimeError::InvalidCollectionMetadata(error.to_string()))?;
                let replay = wal
                    .replay()
                    .map_err(|error| RuntimeError::InvalidCollectionMetadata(error.to_string()))?;
                (None, Some(wal), replay)
            } else {
                let wal = Wal::open(&wal_path)?;
                let replay = wal.replay()?;
                (Some(wal), None, replay)
            };
            let checkpoint_sequence = checkpoint
                .as_ref()
                .map(|value| value.sequence_number().get())
                .unwrap_or(0);
            let mut mutable = Vec::new();
            for mutation in replay.entries {
                if mutation_collection_id(&mutation) != &id {
                    return Err(RuntimeError::InvalidCollectionMetadata(
                        "WAL mutation belongs to a different collection".to_string(),
                    ));
                }
                if mutation.sequence_number().get() <= checkpoint_sequence {
                    continue;
                }
                if let WalMutation::Upsert { record, .. } = &mutation {
                    config.validate_record(record).map_err(|error| {
                        RuntimeError::InvalidCollectionMetadata(error.to_string())
                    })?;
                }
                mutable.push(mutation);
            }

            let mut runtime = CollectionRuntime::writable(WritableCollectionRuntime {
                scope: expected_scope.clone(),
                config,
                segments,
                mutable,
                wal_path,
                wal,
                scoped_wal,
                checkpoint: checkpoint.clone(),
            });
            if runtime.mutable.is_empty()
                && let Some(checkpoint) = &checkpoint
            {
                runtime.hnsw =
                    restore_or_rebuild_hnsw(&path, checkpoint, metric, &runtime.segments);
                if !runtime.configured_lexical_fields().is_empty() {
                    match restore_or_rebuild_lexical(
                        &path,
                        checkpoint,
                        runtime.configured_lexical_fields(),
                        runtime.configured_lexical_analyzer(),
                        &runtime.segments,
                    ) {
                        Ok((fingerprint, index)) => {
                            let _ = runtime.install_lexical_index(fingerprint, index);
                        }
                        Err(error) => runtime.set_lexical_state(LexicalBuildState::Failed {
                            fingerprint: lexical_checkpoint_fingerprint(
                                checkpoint,
                                runtime.configured_lexical_fields(),
                                runtime.configured_lexical_analyzer(),
                            ),
                            message: error.to_string(),
                        }),
                    }
                }
            }
            catalog.insert_collection(id, runtime);
        }

        Ok(Self::with_data_dir_and_threshold(
            catalog,
            data_dir,
            seal_threshold,
        ))
    }
}

fn restore_or_rebuild_hnsw(
    collection_dir: &Path,
    checkpoint: &Checkpoint,
    metric: DistanceMetric,
    segments: &[Segment],
) -> Option<HnswIndex> {
    let config = HnswConfig::default();
    let store = HnswIndexStore::open(collection_dir).ok()?;
    match store.load(checkpoint, metric, config) {
        Ok(HnswLoadResult::Loaded(index)) => Some(index),
        Ok(HnswLoadResult::Missing | HnswLoadResult::Stale) | Err(_) => store
            .rebuild_and_publish(checkpoint, metric, config, segments)
            .or_else(|_| HnswIndex::build(segments, checkpoint.collection_id(), metric, config))
            .ok(),
    }
}

fn restore_or_rebuild_lexical(
    collection_dir: &Path,
    checkpoint: &Checkpoint,
    fields: &[FieldPath],
    analyzer: LexicalAnalyzerConfig,
    segments: &[Segment],
) -> Result<(u64, LexicalIndex), LexicalIndexError> {
    let fingerprint = lexical_checkpoint_fingerprint(checkpoint, fields, analyzer);
    let store = LexicalIndexStore::open(collection_dir)?;
    let index = match store.load(checkpoint, fields, analyzer, segments)? {
        LexicalLoadResult::Loaded(index) => index,
        LexicalLoadResult::Missing | LexicalLoadResult::Stale => {
            store.rebuild_and_publish(checkpoint, fields.to_vec(), analyzer, segments)?
        }
    };
    store.garbage_collect(fingerprint)?;
    Ok((fingerprint, index))
}

fn mutation_collection_id(mutation: &WalMutation) -> &CollectionId {
    match mutation {
        WalMutation::Upsert { collection_id, .. } | WalMutation::Delete { collection_id, .. } => {
            collection_id
        }
    }
}

#[cfg(test)]
mod scope_tests {
    use super::*;
    use ketebe_core::{DataPlaneScope, ProjectId};

    #[test]
    fn runtime_rejects_a_resolver_scope_from_another_project() {
        let collection = CollectionId::new("c_docs").expect("collection id");
        let bound = DataPlaneScope::new(
            ProjectId::new("project-a").expect("project id"),
            collection.clone(),
        );
        let resolved =
            DataPlaneScope::new(ProjectId::new("project-b").expect("project id"), collection);
        let runtime = CollectionRuntime::new_scoped(DistanceMetric::L2, Vec::new(), None, bound);

        assert!(!runtime.matches_scope(&resolved));
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedCollection {
    pub(crate) version: u8,
    pub(crate) id: String,
    pub(crate) dimension: usize,
    pub(crate) metric: PersistedMetric,
    #[serde(default)]
    pub(crate) lexical_fields: Vec<Vec<String>>,
    #[serde(default)]
    pub(crate) lexical_analyzer: PersistedLexicalAnalyzer,
    #[serde(default)]
    pub(crate) ingestion: Option<PersistedIngestionConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistedIngestionConfig {
    pub(crate) embedding_profile: String,
    #[serde(default)]
    pub(crate) chunking: Option<PersistedChunkingPolicy>,
    #[serde(default)]
    pub(crate) token_chunking: Option<PersistedTokenChunkingPolicy>,
    #[serde(default)]
    pub(crate) semantic_chunking: Option<PersistedSemanticChunkingPolicy>,
    #[serde(default)]
    pub(crate) index_chunk_text: bool,
}

impl PersistedIngestionConfig {
    fn into_domain(self) -> Result<CollectionIngestionConfig, RuntimeError> {
        let configured_modes = usize::from(self.chunking.is_some())
            + usize::from(self.token_chunking.is_some())
            + usize::from(self.semantic_chunking.is_some());
        if configured_modes > 1 {
            return Err(RuntimeError::InvalidCollectionMetadata(
                "multiple chunking modes configured".to_string(),
            ));
        }
        if let Some(value) = self.semantic_chunking {
            return CollectionIngestionConfig::new_semantic(
                self.embedding_profile,
                value.into_domain()?,
                self.index_chunk_text,
            )
            .map_err(|e| RuntimeError::InvalidCollectionMetadata(e.to_string()));
        }
        if let Some(value) = self.token_chunking {
            let chunking = value.into_domain()?;
            return CollectionIngestionConfig::new_tokenized(
                self.embedding_profile,
                chunking,
                self.index_chunk_text,
            )
            .map_err(|error| RuntimeError::InvalidCollectionMetadata(error.to_string()));
        }
        let chunking = self
            .chunking
            .map(|value| ChunkingPolicy::new(value.max_chars, value.overlap_chars))
            .transpose()
            .map_err(|error| RuntimeError::InvalidCollectionMetadata(error.to_string()))?;
        CollectionIngestionConfig::new(self.embedding_profile, chunking, self.index_chunk_text)
            .map_err(|error| RuntimeError::InvalidCollectionMetadata(error.to_string()))
    }
}

impl From<&CollectionIngestionConfig> for PersistedIngestionConfig {
    fn from(value: &CollectionIngestionConfig) -> Self {
        Self {
            embedding_profile: value.embedding_profile().to_string(),
            chunking: value.chunking().map(PersistedChunkingPolicy::from),
            token_chunking: value
                .token_chunking()
                .map(PersistedTokenChunkingPolicy::from),
            semantic_chunking: value
                .semantic_chunking()
                .map(PersistedSemanticChunkingPolicy::from),
            index_chunk_text: value.index_chunk_text(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct PersistedChunkingPolicy {
    pub(crate) max_chars: usize,
    pub(crate) overlap_chars: usize,
}

impl From<ChunkingPolicy> for PersistedChunkingPolicy {
    fn from(value: ChunkingPolicy) -> Self {
        Self {
            max_chars: value.max_chars(),
            overlap_chars: value.overlap_chars(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct PersistedSemanticChunkingPolicy {
    pub(crate) max_tokens: usize,
    pub(crate) token_overlap: usize,
    pub(crate) min_tokens: usize,
    pub(crate) breakpoint_threshold_milli: u16,
    pub(crate) tokenizer: PersistedTokenizerKind,
}
impl PersistedSemanticChunkingPolicy {
    fn into_domain(self) -> Result<SemanticChunkingPolicy, RuntimeError> {
        SemanticChunkingPolicy::new(
            self.max_tokens,
            self.token_overlap,
            self.min_tokens,
            self.breakpoint_threshold_milli,
            self.tokenizer.into_domain(),
        )
        .map_err(|e| RuntimeError::InvalidCollectionMetadata(e.to_string()))
    }
}
impl From<SemanticChunkingPolicy> for PersistedSemanticChunkingPolicy {
    fn from(value: SemanticChunkingPolicy) -> Self {
        Self {
            max_tokens: value.max_tokens(),
            token_overlap: value.token_overlap(),
            min_tokens: value.min_tokens(),
            breakpoint_threshold_milli: value.breakpoint_threshold_milli(),
            tokenizer: PersistedTokenizerKind::from(value.tokenizer()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct PersistedTokenChunkingPolicy {
    pub(crate) structure: PersistedChunkingStructure,
    pub(crate) max_tokens: usize,
    pub(crate) token_overlap: usize,
    pub(crate) tokenizer: PersistedTokenizerKind,
}

impl PersistedTokenChunkingPolicy {
    fn into_domain(self) -> Result<TokenChunkingPolicy, RuntimeError> {
        TokenChunkingPolicy::new(
            self.structure.into_domain(),
            self.max_tokens,
            self.token_overlap,
            self.tokenizer.into_domain(),
        )
        .map_err(|error| RuntimeError::InvalidCollectionMetadata(error.to_string()))
    }
}

impl From<TokenChunkingPolicy> for PersistedTokenChunkingPolicy {
    fn from(value: TokenChunkingPolicy) -> Self {
        Self {
            structure: PersistedChunkingStructure::from(value.structure()),
            max_tokens: value.max_tokens(),
            token_overlap: value.token_overlap(),
            tokenizer: PersistedTokenizerKind::from(value.tokenizer()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PersistedChunkingStructure {
    Tokens,
    Sentences,
    Paragraphs,
    Markdown,
    Html,
}

impl PersistedChunkingStructure {
    fn into_domain(self) -> ChunkingStructure {
        match self {
            Self::Tokens => ChunkingStructure::Tokens,
            Self::Sentences => ChunkingStructure::Sentences,
            Self::Paragraphs => ChunkingStructure::Paragraphs,
            Self::Markdown => ChunkingStructure::Markdown,
            Self::Html => ChunkingStructure::Html,
        }
    }
}

impl From<ChunkingStructure> for PersistedChunkingStructure {
    fn from(value: ChunkingStructure) -> Self {
        match value {
            ChunkingStructure::Tokens => Self::Tokens,
            ChunkingStructure::Sentences => Self::Sentences,
            ChunkingStructure::Paragraphs => Self::Paragraphs,
            ChunkingStructure::Markdown => Self::Markdown,
            ChunkingStructure::Html => Self::Html,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PersistedTokenizerKind {
    UnicodeWordsV1,
}

impl PersistedTokenizerKind {
    fn into_domain(self) -> TokenizerKind {
        match self {
            Self::UnicodeWordsV1 => TokenizerKind::UnicodeWordsV1,
        }
    }
}

impl From<TokenizerKind> for PersistedTokenizerKind {
    fn from(value: TokenizerKind) -> Self {
        match value {
            TokenizerKind::UnicodeWordsV1 => Self::UnicodeWordsV1,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct PersistedLexicalAnalyzer {
    #[serde(default = "default_standard_analyzer")]
    pub(crate) kind: PersistedLexicalAnalyzerKind,
    #[serde(default = "default_true")]
    pub(crate) lowercase: bool,
}

impl Default for PersistedLexicalAnalyzer {
    fn default() -> Self {
        Self {
            kind: PersistedLexicalAnalyzerKind::Standard,
            lowercase: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum PersistedLexicalAnalyzerKind {
    #[default]
    Standard,
}

fn default_standard_analyzer() -> PersistedLexicalAnalyzerKind {
    PersistedLexicalAnalyzerKind::Standard
}
fn default_true() -> bool {
    true
}

impl PersistedLexicalAnalyzer {
    pub(crate) fn into_domain(self) -> LexicalAnalyzerConfig {
        match self.kind {
            PersistedLexicalAnalyzerKind::Standard => {
                LexicalAnalyzerConfig::standard(self.lowercase)
            }
        }
    }
}

impl From<LexicalAnalyzerConfig> for PersistedLexicalAnalyzer {
    fn from(value: LexicalAnalyzerConfig) -> Self {
        Self {
            kind: PersistedLexicalAnalyzerKind::Standard,
            lowercase: value.lowercase(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum PersistedMetric {
    Cosine,
    Dot,
    L2,
}

impl PersistedMetric {
    pub(crate) fn into_domain(self) -> DistanceMetric {
        match self {
            Self::Cosine => DistanceMetric::Cosine,
            Self::Dot => DistanceMetric::Dot,
            Self::L2 => DistanceMetric::L2,
        }
    }
}

impl From<DistanceMetric> for PersistedMetric {
    fn from(value: DistanceMetric) -> Self {
        match value {
            DistanceMetric::Cosine => Self::Cosine,
            DistanceMetric::Dot => Self::Dot,
            DistanceMetric::L2 => Self::L2,
        }
    }
}

#[derive(Debug)]
pub enum RuntimeError {
    Io(std::io::Error),
    Json(serde_json::Error),
    Wal(ketebe_storage::WalError),
    Segment(ketebe_storage::SegmentError),
    Checkpoint(ketebe_storage::CheckpointError),
    UnsupportedCollectionVersion(u8),
    InvalidCollectionMetadata(String),
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "runtime I/O error: {error}"),
            Self::Json(error) => write!(f, "runtime JSON error: {error}"),
            Self::Wal(error) => write!(f, "runtime WAL error: {error}"),
            Self::Segment(error) => write!(f, "runtime segment error: {error}"),
            Self::Checkpoint(error) => write!(f, "runtime checkpoint error: {error}"),
            Self::UnsupportedCollectionVersion(version) => {
                write!(f, "unsupported collection metadata version: {version}")
            }
            Self::InvalidCollectionMetadata(message) => {
                write!(f, "invalid collection metadata: {message}")
            }
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<std::io::Error> for RuntimeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for RuntimeError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<ketebe_storage::WalError> for RuntimeError {
    fn from(value: ketebe_storage::WalError) -> Self {
        Self::Wal(value)
    }
}

impl From<ketebe_storage::SegmentError> for RuntimeError {
    fn from(value: ketebe_storage::SegmentError) -> Self {
        Self::Segment(value)
    }
}

impl From<ketebe_storage::CheckpointError> for RuntimeError {
    fn from(value: ketebe_storage::CheckpointError) -> Self {
        Self::Checkpoint(value)
    }
}
