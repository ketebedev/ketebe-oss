use ketebe_core::{
    CollectionConfig, CollectionId, CollectionIngestionConfig, DataPlaneScope, DistanceMetric,
    FieldPath, LexicalAnalyzerConfig, Metadata, Record, RecordId, SequenceNumber, Vector,
};
use ketebe_storage::{
    Checkpoint, CheckpointStore, HnswConfig, HnswIndex, HnswIndexStore, LexicalIndexStore,
    ScopedCheckpointStore, ScopedSegmentStore, ScopedStorageNamespace, ScopedWal, Segment,
    SegmentId, SegmentStore, Wal, WalMutation, compact_scoped_segments, compact_segments,
    garbage_collect_segment_store, lexical_checkpoint_fingerprint, reclaim_wal,
};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use tracing::Instrument as _;

use crate::runtime::{
    AppState, CollectionRuntime, LexicalBuildState, PersistedCollection, PersistedIngestionConfig,
    PersistedLexicalAnalyzer, PersistedMetric, WritableCollectionRuntime,
};

#[derive(Debug, Clone)]
pub struct PendingRecord {
    pub id: RecordId,
    pub vector: Vec<f32>,
    pub metadata: Metadata,
}

struct CollectionCreateSpec {
    scope: Option<DataPlaneScope>,
    id: CollectionId,
    dimension: usize,
    metric: DistanceMetric,
    lexical_fields: Vec<FieldPath>,
    analyzer: LexicalAnalyzerConfig,
    ingestion: Option<CollectionIngestionConfig>,
}

#[derive(Clone)]
pub struct WriteService {
    state: AppState,
}

impl WriteService {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    async fn require_scope(&self, scope: &DataPlaneScope) -> Result<(), WriteError> {
        let actual =
            crate::data_plane_request::scope_for_collection_id(&self.state, scope.collection_id())
                .map_err(|error| WriteError::Scope(error.to_string()))?
                .ok_or_else(|| {
                    WriteError::Scope("collection has no durable project scope".to_string())
                })?;
        if &actual != scope {
            return Err(WriteError::Scope(format!(
                "data-plane scope mismatch: expected {}/{}, found {}/{}",
                scope.project_id().as_str(),
                scope.collection_id().as_str(),
                actual.project_id().as_str(),
                actual.collection_id().as_str(),
            )));
        }
        let catalog = self.state.catalog.read().await;
        if let Some(runtime) = catalog.collections.get(scope.collection_id())
            && !runtime.matches_scope(scope)
        {
            return Err(WriteError::Scope(
                "resolved data-plane scope does not match the collection runtime".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn create_collection_with_schema_scoped(
        &self,
        scope: &DataPlaneScope,
        dimension: usize,
        metric: DistanceMetric,
        lexical_fields: Vec<FieldPath>,
        analyzer: LexicalAnalyzerConfig,
        ingestion: Option<CollectionIngestionConfig>,
    ) -> Result<CollectionConfig, WriteError> {
        self.require_scope(scope).await?;
        self.create_collection_with_schema_in_scope(CollectionCreateSpec {
            scope: Some(scope.clone()),
            id: scope.collection_id().clone(),
            dimension,
            metric,
            lexical_fields,
            analyzer,
            ingestion,
        })
        .await
    }

    pub async fn update_lexical_schema_scoped(
        &self,
        scope: &DataPlaneScope,
        lexical_fields: Vec<FieldPath>,
        analyzer: LexicalAnalyzerConfig,
    ) -> Result<CollectionConfig, WriteError> {
        self.require_scope(scope).await?;
        self.update_lexical_schema(scope.collection_id(), lexical_fields, analyzer)
            .await
    }

    pub async fn upsert_scoped(
        &self,
        scope: &DataPlaneScope,
        pending: PendingRecord,
    ) -> Result<SequenceNumber, WriteError> {
        self.require_scope(scope).await?;
        self.upsert(scope.collection_id(), pending).await
    }

    pub async fn upsert_batch_scoped(
        &self,
        scope: &DataPlaneScope,
        records: Vec<PendingRecord>,
    ) -> Result<Vec<SequenceNumber>, WriteError> {
        self.require_scope(scope).await?;
        self.upsert_batch(scope.collection_id(), records).await
    }

    pub async fn delete_scoped(
        &self,
        scope: &DataPlaneScope,
        record_id: RecordId,
    ) -> Result<SequenceNumber, WriteError> {
        self.require_scope(scope).await?;
        self.delete(scope.collection_id(), record_id).await
    }

    pub async fn create_collection(
        &self,
        id: CollectionId,
        dimension: usize,
        metric: DistanceMetric,
        lexical_fields: Vec<FieldPath>,
    ) -> Result<CollectionConfig, WriteError> {
        self.create_collection_with_analyzer(
            id,
            dimension,
            metric,
            lexical_fields,
            LexicalAnalyzerConfig::default(),
        )
        .await
    }

    pub async fn create_collection_with_analyzer(
        &self,
        id: CollectionId,
        dimension: usize,
        metric: DistanceMetric,
        lexical_fields: Vec<FieldPath>,
        analyzer: LexicalAnalyzerConfig,
    ) -> Result<CollectionConfig, WriteError> {
        self.create_collection_with_schema(id, dimension, metric, lexical_fields, analyzer, None)
            .await
    }

    pub async fn create_collection_with_schema(
        &self,
        id: CollectionId,
        dimension: usize,
        metric: DistanceMetric,
        lexical_fields: Vec<FieldPath>,
        analyzer: LexicalAnalyzerConfig,
        ingestion: Option<CollectionIngestionConfig>,
    ) -> Result<CollectionConfig, WriteError> {
        self.create_collection_with_schema_in_scope(CollectionCreateSpec {
            scope: None,
            id,
            dimension,
            metric,
            lexical_fields,
            analyzer,
            ingestion,
        })
        .await
    }

    async fn create_collection_with_schema_in_scope(
        &self,
        spec: CollectionCreateSpec,
    ) -> Result<CollectionConfig, WriteError> {
        if let Some(ingestion) = spec.ingestion.as_ref() {
            let provider = self
                .state
                .embedding_provider_profile(ingestion.embedding_profile())
                .await
                .ok_or_else(|| {
                    WriteError::Validation(format!(
                        "embedding profile '{}' is not registered",
                        ingestion.embedding_profile()
                    ))
                })?;
            if let Some(provider_dimension) = provider.fixed_dimension()
                && provider_dimension != spec.dimension
            {
                return Err(WriteError::Validation(format!(
                    "embedding profile '{}' dimension {} does not match collection dimension {}",
                    ingestion.embedding_profile(),
                    provider_dimension,
                    spec.dimension,
                )));
            }
        }
        let mut config = CollectionConfig::new(spec.id.clone(), spec.dimension, spec.metric)
            .map_err(|error| WriteError::Validation(error.to_string()))?
            .with_lexical_fields(spec.lexical_fields)
            .with_lexical_analyzer(spec.analyzer);
        if let Some(ingestion) = spec.ingestion {
            config = config.with_ingestion(ingestion);
        }
        let mut catalog = self.state.catalog.write().await;
        if catalog.collections.contains_key(&spec.id) {
            return Err(WriteError::CollectionAlreadyExists(spec.id));
        }

        let collection_dir = self.collection_dir(config.id())?;
        fs::create_dir_all(collection_dir.join("segments"))?;
        persist_collection_config(&collection_dir, &config)?;
        let wal_path = collection_dir.join("wal.log");
        let (wal, scoped_wal) = if let Some(scope) = &spec.scope {
            (
                None,
                Some(
                    ScopedWal::open(&*self.state.data_dir, scope.clone())
                        .map_err(|error| WriteError::Scope(error.to_string()))?,
                ),
            )
        } else {
            (Some(Wal::open(&wal_path)?), None)
        };
        catalog.insert_collection(
            config.id().clone(),
            CollectionRuntime::writable(WritableCollectionRuntime {
                scope: spec.scope,
                config: config.clone(),
                segments: Vec::new(),
                mutable: Vec::new(),
                wal_path,
                wal,
                scoped_wal,
                checkpoint: None,
            }),
        );
        Ok(config)
    }

    pub async fn update_lexical_schema(
        &self,
        collection_id: &CollectionId,
        lexical_fields: Vec<FieldPath>,
        analyzer: LexicalAnalyzerConfig,
    ) -> Result<CollectionConfig, WriteError> {
        self.state.lexical_scheduler.cancel(collection_id);
        let config = {
            let mut catalog = self.state.catalog.write().await;
            let runtime = catalog
                .collections
                .get_mut(collection_id)
                .ok_or_else(|| WriteError::CollectionNotFound(collection_id.clone()))?;
            let existing = runtime
                .config
                .as_ref()
                .ok_or(WriteError::CollectionNotWritable)?;
            let ingestion = existing.ingestion().cloned();
            let mut updated = CollectionConfig::new(
                collection_id.clone(),
                existing.dimension(),
                existing.distance_metric(),
            )
            .map_err(|error| WriteError::Validation(error.to_string()))?
            .with_lexical_fields(lexical_fields)
            .with_lexical_analyzer(analyzer);
            if let Some(ingestion) = ingestion {
                updated = updated.with_ingestion(ingestion);
            }
            persist_collection_config(&self.collection_dir(collection_id)?, &updated)?;
            runtime.config = Some(updated.clone());
            runtime.invalidate_lexical_index();
            updated
        };
        self.schedule_lexical_build(collection_id.clone()).await;
        Ok(config)
    }

    pub(crate) async fn publish_embedding_migration_vectors(
        &self,
        collection_id: &CollectionId,
        expected_active_profile: &str,
        target_profile: &str,
        records: Vec<PendingRecord>,
    ) -> Result<Vec<SequenceNumber>, WriteError> {
        if records.is_empty() {
            return Err(WriteError::Validation(
                "embedding migration contains no managed records".to_string(),
            ));
        }
        let provider = self
            .state
            .embedding_provider_profile(target_profile)
            .await
            .ok_or_else(|| {
                WriteError::Validation(format!(
                    "embedding profile '{target_profile}' is not registered"
                ))
            })?;

        let (sequences, should_seal) = {
            let mut catalog = self.state.catalog.write().await;
            let runtime = catalog
                .collections
                .get_mut(collection_id)
                .ok_or_else(|| WriteError::CollectionNotFound(collection_id.clone()))?;
            let existing = runtime
                .config
                .as_ref()
                .ok_or(WriteError::CollectionNotWritable)?
                .clone();
            let ingestion = existing.ingestion().ok_or_else(|| {
                WriteError::Validation(
                    "collection does not have a managed ingestion schema".to_string(),
                )
            })?;
            if ingestion.embedding_profile() != expected_active_profile {
                return Err(WriteError::Validation(format!(
                    "active embedding profile changed from '{expected_active_profile}' to '{}'",
                    ingestion.embedding_profile()
                )));
            }
            if let Some(dimension) = provider.fixed_dimension()
                && dimension != existing.dimension()
            {
                return Err(WriteError::Validation(format!(
                    "embedding profile '{target_profile}' dimension {dimension} does not match collection dimension {}",
                    existing.dimension()
                )));
            }

            let mut vectors = Vec::with_capacity(records.len());
            for pending in &records {
                let vector = Vector::new(pending.vector.clone())
                    .map_err(|error| WriteError::Validation(error.to_string()))?;
                existing
                    .validate_vector(&vector)
                    .map_err(|error| WriteError::Validation(error.to_string()))?;
                vectors.push(vector);
            }

            let mut next_sequence = runtime.next_sequence;
            let mut sequences = Vec::with_capacity(records.len());
            let mut mutations = Vec::with_capacity(records.len());
            for (pending, vector) in records.into_iter().zip(vectors) {
                let sequence = SequenceNumber::new(next_sequence);
                let record = Record::new(pending.id, vector, pending.metadata, sequence);
                mutations.push(WalMutation::Upsert {
                    collection_id: collection_id.clone(),
                    record,
                });
                sequences.push(sequence);
                next_sequence = next_sequence.saturating_add(1);
            }
            if let Some(wal) = runtime.scoped_wal.as_mut() {
                wal.append_batch(&mutations)
                    .map_err(|error| WriteError::Scope(error.to_string()))?;
            } else {
                runtime
                    .wal
                    .as_mut()
                    .ok_or(WriteError::CollectionNotWritable)?
                    .append_batch(&mutations)?;
            }

            runtime.mutable.extend(mutations);
            runtime.next_sequence = next_sequence;
            (
                sequences,
                runtime.mutable.len() >= self.state.seal_threshold,
            )
        };
        if should_seal {
            self.seal_collection(collection_id).await?;
        }
        Ok(sequences)
    }

    pub(crate) async fn finalize_embedding_profile_cutover(
        &self,
        collection_id: &CollectionId,
        expected_active_profile: &str,
        target_profile: &str,
    ) -> Result<CollectionConfig, WriteError> {
        let provider = self
            .state
            .embedding_provider_profile(target_profile)
            .await
            .ok_or_else(|| {
                WriteError::Validation(format!(
                    "embedding profile '{target_profile}' is not registered"
                ))
            })?;
        let mut catalog = self.state.catalog.write().await;
        let runtime = catalog
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| WriteError::CollectionNotFound(collection_id.clone()))?;
        let existing = runtime
            .config
            .as_ref()
            .ok_or(WriteError::CollectionNotWritable)?
            .clone();
        let ingestion = existing.ingestion().ok_or_else(|| {
            WriteError::Validation(
                "collection does not have a managed ingestion schema".to_string(),
            )
        })?;
        if ingestion.embedding_profile() == target_profile {
            return Ok(existing);
        }
        if ingestion.embedding_profile() != expected_active_profile {
            return Err(WriteError::Validation(format!(
                "active embedding profile changed from '{expected_active_profile}' to '{}'",
                ingestion.embedding_profile()
            )));
        }
        if let Some(dimension) = provider.fixed_dimension()
            && dimension != existing.dimension()
        {
            return Err(WriteError::Validation(format!(
                "embedding profile '{target_profile}' dimension {dimension} does not match collection dimension {}",
                existing.dimension()
            )));
        }
        let updated_ingestion = CollectionIngestionConfig::new(
            target_profile,
            ingestion.chunking(),
            ingestion.index_chunk_text(),
        )
        .map_err(|error| WriteError::Validation(error.to_string()))?;
        let updated = CollectionConfig::new(
            collection_id.clone(),
            existing.dimension(),
            existing.distance_metric(),
        )
        .map_err(|error| WriteError::Validation(error.to_string()))?
        .with_lexical_fields(existing.lexical_fields().to_vec())
        .with_lexical_analyzer(existing.lexical_analyzer())
        .with_ingestion(updated_ingestion);
        persist_collection_config(&self.collection_dir(collection_id)?, &updated)?;
        runtime.config = Some(updated.clone());
        runtime.invalidate_lexical_index();
        Ok(updated)
    }

    pub async fn upsert(
        &self,
        collection_id: &CollectionId,
        pending: PendingRecord,
    ) -> Result<SequenceNumber, WriteError> {
        let mut sequences = self.upsert_batch(collection_id, vec![pending]).await?;
        Ok(sequences.remove(0))
    }

    #[tracing::instrument(
        skip_all,
        name = "ketebe.write.upsert_batch",
        fields(component = "write")
    )]
    pub async fn upsert_batch(
        &self,
        collection_id: &CollectionId,
        records: Vec<PendingRecord>,
    ) -> Result<Vec<SequenceNumber>, WriteError> {
        if records.is_empty() {
            return Err(WriteError::Validation(
                "batch must contain at least one record".to_string(),
            ));
        }

        let (sequences, should_seal) = {
            let mut catalog = self.state.catalog.write().await;
            let runtime = catalog
                .collections
                .get_mut(collection_id)
                .ok_or_else(|| WriteError::CollectionNotFound(collection_id.clone()))?;
            let config = runtime
                .config
                .as_ref()
                .ok_or(WriteError::CollectionNotWritable)?
                .clone();

            let mut vectors = Vec::with_capacity(records.len());
            for pending in &records {
                let vector = Vector::new(pending.vector.clone())
                    .map_err(|error| WriteError::Validation(error.to_string()))?;
                config
                    .validate_vector(&vector)
                    .map_err(|error| WriteError::Validation(error.to_string()))?;
                vectors.push(vector);
            }

            let mut next_sequence = runtime.next_sequence;
            let mut sequences = Vec::with_capacity(records.len());
            let mut mutations = Vec::with_capacity(records.len());
            for (pending, vector) in records.into_iter().zip(vectors) {
                let sequence = SequenceNumber::new(next_sequence);
                let record = Record::new(pending.id, vector, pending.metadata, sequence);
                mutations.push(WalMutation::Upsert {
                    collection_id: collection_id.clone(),
                    record,
                });
                sequences.push(sequence);
                next_sequence = next_sequence.saturating_add(1);
            }

            if let Some(wal) = runtime.scoped_wal.as_mut() {
                wal.append_batch(&mutations)
                    .map_err(|error| WriteError::Scope(error.to_string()))?;
            } else {
                runtime
                    .wal
                    .as_mut()
                    .ok_or(WriteError::CollectionNotWritable)?
                    .append_batch(&mutations)?;
            }

            runtime.mutable.extend(mutations);
            runtime.next_sequence = next_sequence;
            (
                sequences,
                runtime.mutable.len() >= self.state.seal_threshold,
            )
        };

        if should_seal {
            self.seal_collection(collection_id).await?;
        }
        Ok(sequences)
    }

    pub async fn delete(
        &self,
        collection_id: &CollectionId,
        record_id: RecordId,
    ) -> Result<SequenceNumber, WriteError> {
        let (sequence, should_seal) = {
            let mut catalog = self.state.catalog.write().await;
            let runtime = catalog
                .collections
                .get_mut(collection_id)
                .ok_or_else(|| WriteError::CollectionNotFound(collection_id.clone()))?;
            if runtime.config.is_none() {
                return Err(WriteError::CollectionNotWritable);
            }
            let sequence = SequenceNumber::new(runtime.next_sequence);
            let mutation = WalMutation::Delete {
                collection_id: collection_id.clone(),
                record_id,
                sequence_number: sequence,
            };
            if let Some(wal) = runtime.scoped_wal.as_mut() {
                wal.append(&mutation)
                    .map_err(|error| WriteError::Scope(error.to_string()))?;
            } else {
                runtime
                    .wal
                    .as_mut()
                    .ok_or(WriteError::CollectionNotWritable)?
                    .append(&mutation)?;
            }
            runtime.mutable.push(mutation);
            runtime.next_sequence = runtime.next_sequence.saturating_add(1);
            (sequence, runtime.mutable.len() >= self.state.seal_threshold)
        };

        if should_seal {
            self.seal_collection(collection_id).await?;
        }
        Ok(sequence)
    }

    pub async fn seal_collection(
        &self,
        collection_id: &CollectionId,
    ) -> Result<Option<Checkpoint>, WriteError> {
        let mut catalog = self.state.catalog.write().await;
        let runtime = catalog
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| WriteError::CollectionNotFound(collection_id.clone()))?;
        if runtime.config.is_none() {
            return Err(WriteError::CollectionNotWritable);
        }
        if runtime.mutable.is_empty() {
            return Ok(None);
        }

        let scope = runtime.scope.clone();
        if scope.is_none() && collection_id.as_str().starts_with("c_") {
            return Err(WriteError::Scope(
                "stable collection runtime has no project scope".to_string(),
            ));
        }
        let collection_dir = if let Some(scope) = &scope {
            ScopedStorageNamespace::open_existing(&*self.state.data_dir, scope.clone())
                .map_err(|error| WriteError::Scope(error.to_string()))?
                .root()
                .to_path_buf()
        } else {
            self.state
                .data_dir
                .join("collections")
                .join(collection_id.as_str())
        };
        let scoped_segment_store = scope
            .as_ref()
            .map(|scope| ScopedSegmentStore::open(&*self.state.data_dir, scope.clone()))
            .transpose()
            .map_err(|error| WriteError::Scope(error.to_string()))?;
        let legacy_segment_store = if scoped_segment_store.is_none() {
            Some(SegmentStore::open(collection_dir.join("segments"))?)
        } else {
            None
        };
        let discovered = if let Some(store) = &scoped_segment_store {
            store
                .discover()
                .map_err(|error| WriteError::Scope(error.to_string()))?
        } else {
            legacy_segment_store
                .as_ref()
                .expect("legacy segment store")
                .discover()?
        };
        let next_segment_id = discovered
            .iter()
            .map(|segment| segment.id().get())
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| WriteError::Io(std::io::Error::other("segment ID space exhausted")))?;

        let sealed_mutations = runtime.mutable.clone();
        let segment = Segment::from_mutations(SegmentId::new(next_segment_id), &sealed_mutations)?;
        if let Some(store) = &scoped_segment_store {
            store
                .publish(&segment)
                .map_err(|error| WriteError::Scope(error.to_string()))?;
        } else {
            legacy_segment_store
                .as_ref()
                .expect("legacy segment store")
                .publish(&segment)?;
        }

        let mut segment_ids = runtime
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.segments().to_vec())
            .unwrap_or_else(|| runtime.segments.iter().map(Segment::id).collect());
        segment_ids.push(segment.id());
        let checkpoint =
            Checkpoint::new(collection_id.clone(), segment_ids, segment.max_sequence());
        if let Some(scope) = &scope {
            ScopedCheckpointStore::open(&*self.state.data_dir, scope.clone())
                .map_err(|error| WriteError::Scope(error.to_string()))?
                .publish(&checkpoint)
                .map_err(|error| WriteError::Scope(error.to_string()))?;
        } else {
            CheckpointStore::open(&collection_dir)?.publish(&checkpoint)?;
        }

        runtime.segments.push(segment);
        runtime.mutable.clear();
        runtime.hnsw = None;
        runtime.checkpoint = Some(checkpoint.clone());

        let wal_path = runtime
            .wal_path
            .as_ref()
            .ok_or(WriteError::CollectionNotWritable)?
            .clone();
        let old_writer = runtime.wal.take();
        let old_scoped_writer = runtime.scoped_wal.take();
        if old_writer.is_none() && old_scoped_writer.is_none() {
            return Err(WriteError::CollectionNotWritable);
        }
        drop(old_writer);
        drop(old_scoped_writer);
        if let Err(error) = reclaim_wal(&wal_path, &runtime.mutable) {
            if let Some(scope) = &scope {
                runtime.scoped_wal =
                    ScopedWal::open_existing(&*self.state.data_dir, scope.clone()).ok();
            } else {
                runtime.wal = Wal::open(&wal_path).ok();
            }
            return Err(error.into());
        }
        if let Some(scope) = &scope {
            runtime.scoped_wal = Some(
                ScopedWal::open_existing(&*self.state.data_dir, scope.clone())
                    .map_err(|error| WriteError::Scope(error.to_string()))?,
            );
        } else {
            runtime.wal = Some(Wal::open(&wal_path)?);
        }
        runtime.hnsw = rebuild_hnsw(
            &collection_dir,
            &checkpoint,
            runtime.metric,
            &runtime.segments,
        );
        drop(catalog);
        self.schedule_lexical_build(collection_id.clone()).await;
        Ok(Some(checkpoint))
    }

    #[tracing::instrument(skip_all, name = "ketebe.compaction", fields(component = "compaction"))]
    pub async fn compact_collection(
        &self,
        collection_id: &CollectionId,
    ) -> Result<Option<Checkpoint>, WriteError> {
        let has_mutable = {
            let catalog = self.state.catalog.read().await;
            let runtime = catalog
                .collections
                .get(collection_id)
                .ok_or_else(|| WriteError::CollectionNotFound(collection_id.clone()))?;
            if runtime.config.is_none() {
                return Err(WriteError::CollectionNotWritable);
            }
            !runtime.mutable.is_empty()
        };
        if has_mutable {
            self.seal_collection(collection_id).await?;
        }

        let should_compact = {
            let catalog = self.state.catalog.read().await;
            let runtime = catalog
                .collections
                .get(collection_id)
                .ok_or_else(|| WriteError::CollectionNotFound(collection_id.clone()))?;
            runtime.segments.len() >= 2
        };
        if !should_compact {
            return Ok(None);
        }

        // Resource admission must happen without holding the catalog lock: slow or
        // saturated background work must not block foreground query/write access.
        let _resource_permit = crate::global_resource_scheduler()
            .acquire(crate::WorkKind::Compaction)
            .await
            .map_err(|error| {
                WriteError::Io(std::io::Error::other(format!(
                    "compaction resource admission failed: {error}"
                )))
            })?;

        // Revalidate after queueing. Another task may have compacted the collection
        // while this request waited for resource admission.
        let mut catalog = self.state.catalog.write().await;
        let runtime = catalog
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| WriteError::CollectionNotFound(collection_id.clone()))?;
        if runtime.segments.len() < 2 {
            return Ok(None);
        }

        let scope = runtime.scope.clone();
        if scope.is_none() && collection_id.as_str().starts_with("c_") {
            return Err(WriteError::Scope(
                "stable collection runtime has no project scope".to_string(),
            ));
        }
        let collection_dir = if let Some(scope) = &scope {
            ScopedStorageNamespace::open_existing(&*self.state.data_dir, scope.clone())
                .map_err(|error| WriteError::Scope(error.to_string()))?
                .root()
                .to_path_buf()
        } else {
            self.state
                .data_dir
                .join("collections")
                .join(collection_id.as_str())
        };
        let scoped_segment_store = scope
            .as_ref()
            .map(|scope| ScopedSegmentStore::open(&*self.state.data_dir, scope.clone()))
            .transpose()
            .map_err(|error| WriteError::Scope(error.to_string()))?;
        let legacy_segment_store = if scoped_segment_store.is_none() {
            Some(SegmentStore::open(collection_dir.join("segments"))?)
        } else {
            None
        };
        let discovered = if let Some(store) = &scoped_segment_store {
            store
                .discover()
                .map_err(|error| WriteError::Scope(error.to_string()))?
        } else {
            legacy_segment_store
                .as_ref()
                .expect("legacy segment store")
                .discover()?
        };
        let next_segment_id = discovered
            .iter()
            .map(|segment| segment.id().get())
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| WriteError::Io(std::io::Error::other("segment ID space exhausted")))?;

        let replacement = if let Some(scope) = &scope {
            compact_scoped_segments(scope, SegmentId::new(next_segment_id), &runtime.segments)
                .map_err(|error| WriteError::Scope(error.to_string()))?
        } else {
            compact_segments(SegmentId::new(next_segment_id), &runtime.segments)?
        };
        if let Some(store) = &scoped_segment_store {
            store
                .publish(&replacement)
                .map_err(|error| WriteError::Scope(error.to_string()))?;
        } else {
            legacy_segment_store
                .as_ref()
                .expect("legacy segment store")
                .publish(&replacement)?;
        }

        let represented_sequence = runtime
            .checkpoint
            .as_ref()
            .map(Checkpoint::sequence_number)
            .unwrap_or_else(|| {
                runtime
                    .segments
                    .iter()
                    .map(Segment::max_sequence)
                    .max()
                    .expect("at least two segments")
            });
        let checkpoint = Checkpoint::new(
            collection_id.clone(),
            vec![replacement.id()],
            represented_sequence,
        );
        if let Some(scope) = &scope {
            ScopedCheckpointStore::open(&*self.state.data_dir, scope.clone())
                .map_err(|error| WriteError::Scope(error.to_string()))?
                .publish(&checkpoint)
                .map_err(|error| WriteError::Scope(error.to_string()))?;
        } else {
            CheckpointStore::open(&collection_dir)?.publish(&checkpoint)?;
        }

        runtime.segments = vec![replacement];
        runtime.hnsw = None;
        runtime.checkpoint = Some(checkpoint.clone());

        if let Some(store) = &scoped_segment_store {
            store
                .garbage_collect(checkpoint.segments())
                .map_err(|error| WriteError::Scope(error.to_string()))?;
        } else {
            garbage_collect_segment_store(
                legacy_segment_store.as_ref().expect("legacy segment store"),
                checkpoint.segments(),
            )?;
        }
        runtime.hnsw = rebuild_hnsw(
            &collection_dir,
            &checkpoint,
            runtime.metric,
            &runtime.segments,
        );
        drop(catalog);
        self.schedule_lexical_build(collection_id.clone()).await;
        Ok(Some(checkpoint))
    }

    async fn schedule_lexical_build(&self, collection_id: CollectionId) {
        if self.state.lifecycle().is_draining() {
            return;
        }
        let state = self.state.clone();
        let collection_dir = match collection_dir_for_state(&state, &collection_id) {
            Ok(path) => path,
            Err(_) => return,
        };
        let job = {
            let catalog = state.catalog.read().await;
            let Some(runtime) = catalog.collections.get(&collection_id) else {
                return;
            };
            if !runtime.mutable.is_empty() {
                return;
            }
            let Some(checkpoint) = runtime.checkpoint.clone() else {
                return;
            };
            let fields = runtime.configured_lexical_fields().to_vec();
            if fields.is_empty() {
                runtime.set_lexical_state(LexicalBuildState::Disabled);
                return;
            }
            let analyzer = runtime.configured_lexical_analyzer();
            let fingerprint = lexical_checkpoint_fingerprint(&checkpoint, &fields, analyzer);
            (
                checkpoint,
                runtime.segments.clone(),
                fields,
                analyzer,
                collection_dir.clone(),
                fingerprint,
            )
        };

        let (checkpoint, segments, fields, analyzer, collection_dir, fingerprint) = job;
        if !state
            .lexical_scheduler
            .register(collection_id.clone(), fingerprint)
        {
            return;
        }
        {
            let catalog = state.catalog.read().await;
            if let Some(runtime) = catalog.collections.get(&collection_id) {
                runtime.set_lexical_state(LexicalBuildState::Queued { fingerprint });
            }
        }

        let span = tracing::info_span!(
            "ketebe.background.lexical_build",
            component = "lexical_index"
        );
        tokio::spawn(
            async move {
                let max_attempts = state.lexical_scheduler.max_attempts();
                for attempt in 1..=max_attempts {
                    if state.lifecycle().is_draining() {
                        state.lexical_scheduler.finish(&collection_id, fingerprint);
                        return;
                    }
                    if !state
                        .lexical_scheduler
                        .is_current(&collection_id, fingerprint)
                    {
                        return;
                    }
                    let Some(permit) = state.lexical_scheduler.acquire().await else {
                        state.lexical_scheduler.finish(&collection_id, fingerprint);
                        return;
                    };
                    if !state
                        .lexical_scheduler
                        .is_current(&collection_id, fingerprint)
                    {
                        drop(permit);
                        return;
                    }

                    let still_current = {
                        let catalog = state.catalog.read().await;
                        let Some(runtime) = catalog.collections.get(&collection_id) else {
                            drop(permit);
                            state.lexical_scheduler.finish(&collection_id, fingerprint);
                            return;
                        };
                        let current = runtime.checkpoint.as_ref().and_then(|value| {
                            let configured = runtime.configured_lexical_fields();
                            if runtime.mutable.is_empty() && !configured.is_empty() {
                                Some(lexical_checkpoint_fingerprint(
                                    value,
                                    configured,
                                    runtime.configured_lexical_analyzer(),
                                ))
                            } else {
                                None
                            }
                        });
                        if current == Some(fingerprint) {
                            runtime.set_lexical_state(LexicalBuildState::Building {
                                fingerprint,
                                attempt,
                            });
                            true
                        } else {
                            false
                        }
                    };
                    if !still_current {
                        drop(permit);
                        state.lexical_scheduler.finish(&collection_id, fingerprint);
                        return;
                    }

                    let build_checkpoint = checkpoint.clone();
                    let build_segments = segments.clone();
                    let build_fields = fields.clone();
                    let build_analyzer = analyzer;
                    let build_directory = collection_dir.clone();
                    let result = tokio::task::spawn_blocking(move || {
                        let store = LexicalIndexStore::open(&build_directory)?;
                        let index = store.rebuild_and_publish(
                            &build_checkpoint,
                            build_fields,
                            build_analyzer,
                            &build_segments,
                        )?;
                        store.garbage_collect(fingerprint)?;
                        Ok::<_, ketebe_storage::LexicalIndexError>(index)
                    })
                    .await;
                    drop(permit);

                    if state.lifecycle().is_draining() {
                        if let Ok(store) = LexicalIndexStore::open(&collection_dir) {
                            let _ = store.remove_snapshot(fingerprint);
                        }
                        state.lexical_scheduler.finish(&collection_id, fingerprint);
                        return;
                    }

                    if !state
                        .lexical_scheduler
                        .is_current(&collection_id, fingerprint)
                    {
                        if let Ok(store) = LexicalIndexStore::open(&collection_dir) {
                            let _ = store.remove_snapshot(fingerprint);
                        }
                        return;
                    }

                    let current_fingerprint = {
                        let catalog = state.catalog.read().await;
                        catalog.collections.get(&collection_id).and_then(|runtime| {
                            runtime.checkpoint.as_ref().and_then(|value| {
                                let configured = runtime.configured_lexical_fields();
                                if runtime.mutable.is_empty() && !configured.is_empty() {
                                    Some(lexical_checkpoint_fingerprint(
                                        value,
                                        configured,
                                        runtime.configured_lexical_analyzer(),
                                    ))
                                } else {
                                    None
                                }
                            })
                        })
                    };
                    if current_fingerprint != Some(fingerprint) {
                        if let Ok(store) = LexicalIndexStore::open(&collection_dir) {
                            let _ = store.remove_snapshot(fingerprint);
                        }
                        state.lexical_scheduler.finish(&collection_id, fingerprint);
                        return;
                    }

                    match result {
                        Ok(Ok(index)) => {
                            let catalog = state.catalog.read().await;
                            if let Some(runtime) = catalog.collections.get(&collection_id) {
                                let _ = runtime.install_lexical_index(fingerprint, index);
                            }
                            state.lexical_scheduler.finish(&collection_id, fingerprint);
                            return;
                        }
                        Ok(Err(_error)) if attempt < max_attempts => {
                            let delay = state.lexical_scheduler.retry_delay(attempt);
                            let catalog = state.catalog.read().await;
                            if let Some(runtime) = catalog.collections.get(&collection_id) {
                                runtime.set_lexical_state(LexicalBuildState::Retrying {
                                    fingerprint,
                                    attempt,
                                    delay_ms: u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                                });
                            }
                            drop(catalog);
                            tokio::time::sleep(delay).await;
                        }
                        Ok(Err(error)) => {
                            let catalog = state.catalog.read().await;
                            if let Some(runtime) = catalog.collections.get(&collection_id) {
                                runtime.set_lexical_state(LexicalBuildState::Failed {
                                    fingerprint,
                                    message: error.to_string(),
                                });
                            }
                            state.lexical_scheduler.finish(&collection_id, fingerprint);
                            return;
                        }
                        Err(_error) if attempt < max_attempts => {
                            let delay = state.lexical_scheduler.retry_delay(attempt);
                            let catalog = state.catalog.read().await;
                            if let Some(runtime) = catalog.collections.get(&collection_id) {
                                runtime.set_lexical_state(LexicalBuildState::Retrying {
                                    fingerprint,
                                    attempt,
                                    delay_ms: u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                                });
                            }
                            drop(catalog);
                            tokio::time::sleep(delay).await;
                        }
                        Err(error) => {
                            let catalog = state.catalog.read().await;
                            if let Some(runtime) = catalog.collections.get(&collection_id) {
                                runtime.set_lexical_state(LexicalBuildState::Failed {
                                    fingerprint,
                                    message: error.to_string(),
                                });
                            }
                            state.lexical_scheduler.finish(&collection_id, fingerprint);
                            return;
                        }
                    }
                }
            }
            .instrument(span),
        );
    }

    fn collection_dir(&self, id: &CollectionId) -> Result<std::path::PathBuf, WriteError> {
        collection_dir_for_state(&self.state, id)
    }
}

fn collection_dir_for_state(
    state: &AppState,
    id: &CollectionId,
) -> Result<std::path::PathBuf, WriteError> {
    match crate::data_plane_request::scope_for_collection_id(state, id)
        .map_err(|error| WriteError::Scope(error.to_string()))?
    {
        Some(scope) => {
            let namespace = ScopedStorageNamespace::open(&*state.data_dir, scope)
                .map_err(|error| WriteError::Scope(error.to_string()))?;
            Ok(namespace.root().to_path_buf())
        }
        None if id.as_str().starts_with("c_") => Err(WriteError::Scope(
            "stable collection identity has no project namespace binding".to_string(),
        )),
        None => Ok(state.data_dir.join("collections").join(id.as_str())),
    }
}

fn rebuild_hnsw(
    collection_dir: &Path,
    checkpoint: &Checkpoint,
    metric: DistanceMetric,
    segments: &[Segment],
) -> Option<HnswIndex> {
    let config = HnswConfig::default();
    match HnswIndexStore::open(collection_dir) {
        Ok(store) => store
            .rebuild_and_publish(checkpoint, metric, config, segments)
            .or_else(|_| HnswIndex::build(segments, checkpoint.collection_id(), metric, config))
            .ok(),
        Err(_) => HnswIndex::build(segments, checkpoint.collection_id(), metric, config).ok(),
    }
}

fn persist_collection_config(
    collection_dir: &Path,
    config: &CollectionConfig,
) -> Result<(), WriteError> {
    let persisted = PersistedCollection {
        version: 6,
        id: config.id().as_str().to_string(),
        dimension: config.dimension(),
        metric: PersistedMetric::from(config.distance_metric()),
        lexical_fields: config
            .lexical_fields()
            .iter()
            .map(|path| path.segments().to_vec())
            .collect(),
        lexical_analyzer: PersistedLexicalAnalyzer::from(config.lexical_analyzer()),
        ingestion: config.ingestion().map(PersistedIngestionConfig::from),
    };
    let bytes = serde_json::to_vec_pretty(&persisted)?;
    let final_path = collection_dir.join("collection.json");
    let temp_path = collection_dir.join("collection.json.tmp");
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)?;
    file.write_all(&bytes)?;
    file.flush()?;
    file.sync_data()?;
    drop(file);
    fs::rename(&temp_path, &final_path)?;
    sync_directory(collection_dir)?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), WriteError> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

#[derive(Debug)]
pub enum WriteError {
    CollectionAlreadyExists(CollectionId),
    CollectionNotFound(CollectionId),
    CollectionNotWritable,
    Validation(String),
    Scope(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Wal(ketebe_storage::WalError),
    Segment(ketebe_storage::SegmentError),
    Checkpoint(ketebe_storage::CheckpointError),
    Compaction(ketebe_storage::CompactionError),
    WalReclaim(ketebe_storage::WalReclaimError),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CollectionAlreadyExists(id) => write!(f, "collection already exists: {id}"),
            Self::CollectionNotFound(id) => write!(f, "collection not found: {id}"),
            Self::CollectionNotWritable => f.write_str("collection is not writable"),
            Self::Validation(message) => write!(f, "validation failed: {message}"),
            Self::Scope(message) => write!(f, "data-plane scope failure: {message}"),
            Self::Io(error) => write!(f, "write I/O error: {error}"),
            Self::Json(error) => write!(f, "write JSON error: {error}"),
            Self::Wal(error) => write!(f, "write WAL error: {error}"),
            Self::Segment(error) => write!(f, "write segment error: {error}"),
            Self::Checkpoint(error) => write!(f, "write checkpoint error: {error}"),
            Self::Compaction(error) => write!(f, "compaction error: {error}"),
            Self::WalReclaim(error) => write!(f, "WAL reclaim error: {error}"),
        }
    }
}

impl std::error::Error for WriteError {}

impl From<std::io::Error> for WriteError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for WriteError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<ketebe_storage::WalError> for WriteError {
    fn from(value: ketebe_storage::WalError) -> Self {
        Self::Wal(value)
    }
}

impl From<ketebe_storage::SegmentError> for WriteError {
    fn from(value: ketebe_storage::SegmentError) -> Self {
        Self::Segment(value)
    }
}

impl From<ketebe_storage::CheckpointError> for WriteError {
    fn from(value: ketebe_storage::CheckpointError) -> Self {
        Self::Checkpoint(value)
    }
}

impl From<ketebe_storage::CompactionError> for WriteError {
    fn from(value: ketebe_storage::CompactionError) -> Self {
        Self::Compaction(value)
    }
}

impl From<ketebe_storage::WalReclaimError> for WriteError {
    fn from(value: ketebe_storage::WalReclaimError) -> Self {
        Self::WalReclaim(value)
    }
}
