use crate::{AppState, CollectionService, EmbeddingError, PendingRecord, WriteError, WriteService};
use ketebe_core::{
    ChunkingPolicy, CollectionId, CollectionIngestionConfig, Metadata, MetadataValue, Record,
    RecordId, SequenceNumber, Vector,
};
use ketebe_storage::{Segment, WalMutation};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const CHUNK_METADATA_KEY: &str = "_ketebe_chunk";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkingConfig {
    pub max_chars: usize,
    #[serde(default)]
    pub overlap_chars: usize,
}

impl Default for ChunkingConfig {
    fn default() -> Self {
        Self {
            max_chars: 800,
            overlap_chars: 100,
        }
    }
}

impl ChunkingConfig {
    pub fn validate(self) -> Result<Self, ChunkingError> {
        if self.max_chars == 0 {
            return Err(ChunkingError::InvalidConfig(
                "max_chars must be greater than zero".to_string(),
            ));
        }
        if self.overlap_chars >= self.max_chars {
            return Err(ChunkingError::InvalidConfig(
                "overlap_chars must be smaller than max_chars".to_string(),
            ));
        }
        Ok(self)
    }
}

impl TryFrom<ChunkingConfig> for ChunkingPolicy {
    type Error = ChunkingError;

    fn try_from(value: ChunkingConfig) -> Result<Self, Self::Error> {
        value.validate()?;
        ChunkingPolicy::new(value.max_chars, value.overlap_chars)
            .map_err(|error| ChunkingError::InvalidConfig(error.to_string()))
    }
}

impl From<ChunkingPolicy> for ChunkingConfig {
    fn from(value: ChunkingPolicy) -> Self {
        Self {
            max_chars: value.max_chars(),
            overlap_chars: value.overlap_chars(),
        }
    }
}

pub fn resolve_effective_chunking(
    ingestion: Option<&CollectionIngestionConfig>,
    requested: Option<ChunkingConfig>,
) -> Result<Option<ChunkingConfig>, ChunkingError> {
    let Some(ingestion) = ingestion else {
        return requested.map(ChunkingConfig::validate).transpose();
    };
    let configured = ingestion.chunking().map(ChunkingConfig::from);
    match (configured, requested) {
        (None, None) => Ok(None),
        (Some(configured), None) => Ok(Some(configured)),
        (Some(configured), Some(requested)) if configured == requested.validate()? => {
            Ok(Some(configured))
        }
        _ => Err(ChunkingError::SchemaMismatch),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextChunk {
    pub ordinal: usize,
    pub start_char: usize,
    pub end_char: usize,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct ChunkedDocument {
    pub id: RecordId,
    pub text: String,
    pub metadata: Metadata,
    pub chunking: ChunkingConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkedDocumentResult {
    pub chunk_ids: Vec<RecordId>,
    pub sequence_numbers: Vec<SequenceNumber>,
    pub generation: SequenceNumber,
    pub reconciled_chunks: usize,
}

#[derive(Debug)]
pub enum ChunkingError {
    InvalidConfig(String),
    EmptyText,
    ReservedMetadata,
    SchemaMismatch,
    Embedding(EmbeddingError),
    Write(WriteError),
}

impl fmt::Display for ChunkingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid chunking config: {message}"),
            Self::EmptyText => write!(f, "document text must not be empty"),
            Self::ReservedMetadata => write!(
                f,
                "metadata keys '_ketebe_embedding' and '{CHUNK_METADATA_KEY}' are reserved by Ketebe"
            ),
            Self::SchemaMismatch => write!(
                f,
                "request chunking does not match the collection ingestion schema"
            ),
            Self::Embedding(error) => write!(f, "embedding failed: {error}"),
            Self::Write(error) => write!(f, "chunk write failed: {error}"),
        }
    }
}
impl std::error::Error for ChunkingError {}

#[must_use]
pub fn chunk_text(text: &str, config: ChunkingConfig) -> Vec<TextChunk> {
    let chars: Vec<char> = text.chars().collect();
    if chars.is_empty() {
        return Vec::new();
    }
    let step = config.max_chars - config.overlap_chars;
    let mut chunks = Vec::new();
    let mut start = 0_usize;
    while start < chars.len() {
        let end = (start + config.max_chars).min(chars.len());
        chunks.push(TextChunk {
            ordinal: chunks.len(),
            start_char: start,
            end_char: end,
            text: chars[start..end].iter().collect(),
        });
        if end == chars.len() {
            break;
        }
        start = start.saturating_add(step);
    }
    chunks
}

pub fn chunk_record_id(parent: &RecordId, ordinal: usize) -> RecordId {
    let encoded_parent = match parent {
        RecordId::String(value) => format!("s:{}", hex_encode(value.as_bytes())),
        RecordId::Unsigned(value) => format!("u:{value}"),
    };
    RecordId::string(format!("_ketebe_chunk:{encoded_parent}:{ordinal}"))
        .expect("derived chunk id is non-empty")
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[derive(Clone)]
pub struct ChunkingService {
    state: AppState,
    write: WriteService,
}

impl ChunkingService {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self {
            write: WriteService::new(state.clone()),
            state,
        }
    }

    pub async fn chunk_embed_and_upsert(
        &self,
        collection_id: &CollectionId,
        document: ChunkedDocument,
    ) -> Result<ChunkedDocumentResult, ChunkingError> {
        let config = document.chunking.validate()?;
        if document.text.trim().is_empty() {
            return Err(ChunkingError::EmptyText);
        }
        if document
            .metadata
            .contains_key(crate::embedding::EMBEDDING_METADATA_KEY)
            || document.metadata.contains_key(CHUNK_METADATA_KEY)
        {
            return Err(ChunkingError::ReservedMetadata);
        }

        let collection = CollectionService::new(self.state.clone())
            .get(collection_id)
            .await
            .map_err(|error| ChunkingError::Embedding(EmbeddingError::Management(error)))?;
        if let Some(ingestion) = collection.ingestion.as_ref() {
            let configured = ingestion.chunking().map(ChunkingConfig::from);
            if configured != Some(config) {
                return Err(ChunkingError::SchemaMismatch);
            }
        }
        let provider = if let Some(ingestion) = collection.ingestion.as_ref() {
            self.state
                .embedding_provider_profile(ingestion.embedding_profile())
                .await
                .ok_or_else(|| {
                    ChunkingError::Embedding(EmbeddingError::ProviderProfileUnavailable(
                        ingestion.embedding_profile().to_string(),
                    ))
                })?
        } else {
            self.state
                .embedding_provider()
                .await
                .ok_or(ChunkingError::Embedding(
                    EmbeddingError::ProviderUnavailable,
                ))?
        };
        let profile = collection
            .ingestion
            .as_ref()
            .map(|ingestion| ingestion.embedding_profile())
            .unwrap_or("default");
        let chunks = chunk_text(&document.text, config);
        let chunk_count = chunks.len();
        let texts = chunks
            .iter()
            .map(|chunk| chunk.text.clone())
            .collect::<Vec<_>>();
        let vectors = crate::embedding_cache::embed_texts_cached(
            self.state.embedding_cache(),
            profile,
            provider.clone(),
            &texts,
            collection.dimension,
        )
        .await
        .map_err(|error| ChunkingError::Embedding(EmbeddingError::Provider(error)))?;
        let mut records = Vec::with_capacity(chunk_count);
        let mut chunk_ids = Vec::with_capacity(chunk_count);
        let model = provider.model();

        for (chunk, vector) in chunks.into_iter().zip(vectors) {
            if vector.len() != collection.dimension {
                return Err(ChunkingError::Embedding(
                    EmbeddingError::DimensionMismatch {
                        expected: collection.dimension,
                        actual: vector.len(),
                    },
                ));
            }
            if let Some((index, _)) = vector
                .iter()
                .enumerate()
                .find(|(_, value)| !value.is_finite())
            {
                return Err(ChunkingError::Embedding(EmbeddingError::NonFiniteVector {
                    index,
                }));
            }

            let chunk_id = chunk_record_id(&document.id, chunk.ordinal);
            let mut metadata = document.metadata.clone();
            crate::provenance::apply_chunk_content_hash(&mut metadata, &chunk.text);
            let mut embedding = BTreeMap::new();
            embedding.insert(
                "profile".to_string(),
                MetadataValue::String(profile.to_string()),
            );
            embedding.insert(
                "provider".to_string(),
                MetadataValue::String(provider.provider_name().to_string()),
            );
            embedding.insert(
                "model".to_string(),
                MetadataValue::String(model.name.clone()),
            );
            embedding.insert(
                "version".to_string(),
                MetadataValue::String(model.version.clone()),
            );
            embedding.insert(
                "dimension".to_string(),
                MetadataValue::Number(collection.dimension as f64),
            );
            metadata.insert(
                crate::embedding::EMBEDDING_METADATA_KEY.to_string(),
                MetadataValue::Object(embedding),
            );

            let mut chunk_meta = BTreeMap::new();
            match &document.id {
                RecordId::String(value) => {
                    chunk_meta.insert(
                        "parent_type".to_string(),
                        MetadataValue::String("string".to_string()),
                    );
                    chunk_meta.insert(
                        "parent_id".to_string(),
                        MetadataValue::String(value.clone()),
                    );
                }
                RecordId::Unsigned(value) => {
                    chunk_meta.insert(
                        "parent_type".to_string(),
                        MetadataValue::String("u64".to_string()),
                    );
                    chunk_meta.insert(
                        "parent_id".to_string(),
                        MetadataValue::String(value.to_string()),
                    );
                }
            }
            chunk_meta.insert(
                "ordinal".to_string(),
                MetadataValue::Number(chunk.ordinal as f64),
            );
            chunk_meta.insert(
                "chunk_count".to_string(),
                MetadataValue::Number(chunk_count as f64),
            );
            chunk_meta.insert(
                "start_char".to_string(),
                MetadataValue::Number(chunk.start_char as f64),
            );
            chunk_meta.insert(
                "end_char".to_string(),
                MetadataValue::Number(chunk.end_char as f64),
            );
            chunk_meta.insert(
                "max_chars".to_string(),
                MetadataValue::Number(config.max_chars as f64),
            );
            chunk_meta.insert(
                "overlap_chars".to_string(),
                MetadataValue::Number(config.overlap_chars as f64),
            );
            chunk_meta.insert("text".to_string(), MetadataValue::String(chunk.text));
            metadata.insert(
                CHUNK_METADATA_KEY.to_string(),
                MetadataValue::Object(chunk_meta),
            );

            records.push(PendingRecord {
                id: chunk_id.clone(),
                vector,
                metadata,
            });
            chunk_ids.push(chunk_id);
        }

        self.replace_parent_chunks(collection_id, &document.id, records, chunk_ids)
            .await
    }

    pub async fn delete_parent_document(
        &self,
        collection_id: &CollectionId,
        parent_id: &RecordId,
    ) -> Result<Vec<SequenceNumber>, ChunkingError> {
        let (sequences, should_seal) = {
            let mut catalog = self.state.catalog.write().await;
            let runtime = catalog.collections.get_mut(collection_id).ok_or_else(|| {
                ChunkingError::Write(WriteError::CollectionNotFound(collection_id.clone()))
            })?;
            if runtime.config.is_none() {
                return Err(ChunkingError::Write(WriteError::CollectionNotWritable));
            }

            let visible = runtime
                .query_segments()
                .map_err(|error| ChunkingError::Write(WriteError::Segment(error)))?;
            let chunk_ids = visible_parent_chunk_ids(&visible, parent_id);
            if chunk_ids.is_empty() {
                return Ok(Vec::new());
            }

            let mut next_sequence = runtime.next_sequence;
            let mut sequences = Vec::with_capacity(chunk_ids.len());
            let mut mutations = Vec::with_capacity(chunk_ids.len());
            for record_id in chunk_ids {
                let sequence = SequenceNumber::new(next_sequence);
                mutations.push(WalMutation::Delete {
                    collection_id: collection_id.clone(),
                    record_id,
                    sequence_number: sequence,
                });
                sequences.push(sequence);
                next_sequence = next_sequence.saturating_add(1);
            }
            if let Some(wal) = runtime.scoped_wal.as_mut() {
                wal.append_batch(&mutations)
                    .map_err(|error| ChunkingError::Write(WriteError::Scope(error.to_string())))?;
            } else {
                runtime
                    .wal
                    .as_mut()
                    .ok_or(ChunkingError::Write(WriteError::CollectionNotWritable))?
                    .append_batch(&mutations)
                    .map_err(|error| ChunkingError::Write(WriteError::Wal(error)))?;
            }
            runtime.mutable.extend(mutations);
            runtime.next_sequence = next_sequence;
            (
                sequences,
                runtime.mutable.len() >= self.state.seal_threshold,
            )
        };

        if should_seal {
            self.write
                .seal_collection(collection_id)
                .await
                .map_err(ChunkingError::Write)?;
        }
        Ok(sequences)
    }

    pub(crate) async fn replace_parent_chunks(
        &self,
        collection_id: &CollectionId,
        parent_id: &RecordId,
        mut records: Vec<PendingRecord>,
        chunk_ids: Vec<RecordId>,
    ) -> Result<ChunkedDocumentResult, ChunkingError> {
        let (result, should_seal) = {
            let mut catalog = self.state.catalog.write().await;
            let runtime = catalog.collections.get_mut(collection_id).ok_or_else(|| {
                ChunkingError::Write(WriteError::CollectionNotFound(collection_id.clone()))
            })?;
            let config = runtime
                .config
                .as_ref()
                .ok_or(ChunkingError::Write(WriteError::CollectionNotWritable))?
                .clone();

            let visible = runtime
                .query_segments()
                .map_err(|error| ChunkingError::Write(WriteError::Segment(error)))?;
            let previous_chunks = visible_parent_chunk_ids(&visible, parent_id);
            let new_chunks = chunk_ids.iter().cloned().collect::<BTreeSet<_>>();
            let obsolete = previous_chunks
                .difference(&new_chunks)
                .cloned()
                .collect::<Vec<_>>();

            let generation = SequenceNumber::new(runtime.next_sequence);
            let mut vectors = Vec::with_capacity(records.len());
            for pending in &mut records {
                let chunk_meta = pending
                    .metadata
                    .get_mut(CHUNK_METADATA_KEY)
                    .and_then(|value| match value {
                        MetadataValue::Object(value) => Some(value),
                        _ => None,
                    })
                    .expect("Ketebe-created chunk metadata is an object");
                chunk_meta.insert(
                    "generation".to_string(),
                    MetadataValue::Number(generation.get() as f64),
                );

                let vector = Vector::new(pending.vector.clone()).map_err(|error| {
                    ChunkingError::Write(WriteError::Validation(error.to_string()))
                })?;
                config.validate_vector(&vector).map_err(|error| {
                    ChunkingError::Write(WriteError::Validation(error.to_string()))
                })?;
                vectors.push(vector);
            }

            let mut next_sequence = runtime.next_sequence;
            let mut sequences = Vec::with_capacity(records.len());
            let mut mutations = Vec::with_capacity(records.len() + obsolete.len());
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
            for record_id in &obsolete {
                let sequence = SequenceNumber::new(next_sequence);
                mutations.push(WalMutation::Delete {
                    collection_id: collection_id.clone(),
                    record_id: record_id.clone(),
                    sequence_number: sequence,
                });
                next_sequence = next_sequence.saturating_add(1);
            }

            if let Some(wal) = runtime.scoped_wal.as_mut() {
                wal.append_batch(&mutations)
                    .map_err(|error| ChunkingError::Write(WriteError::Scope(error.to_string())))?;
            } else {
                runtime
                    .wal
                    .as_mut()
                    .ok_or(ChunkingError::Write(WriteError::CollectionNotWritable))?
                    .append_batch(&mutations)
                    .map_err(|error| ChunkingError::Write(WriteError::Wal(error)))?;
            }
            runtime.mutable.extend(mutations);
            runtime.next_sequence = next_sequence;

            (
                ChunkedDocumentResult {
                    chunk_ids,
                    sequence_numbers: sequences,
                    generation,
                    reconciled_chunks: obsolete.len(),
                },
                runtime.mutable.len() >= self.state.seal_threshold,
            )
        };

        if should_seal {
            self.write
                .seal_collection(collection_id)
                .await
                .map_err(ChunkingError::Write)?;
        }
        Ok(result)
    }
}

fn visible_parent_chunk_ids(segments: &[Segment], parent_id: &RecordId) -> BTreeSet<RecordId> {
    let mut latest = BTreeMap::<RecordId, (SequenceNumber, Option<Record>)>::new();
    for segment in segments {
        for record in segment.records() {
            let replace = latest
                .get(record.id())
                .map(|(sequence, _)| record.sequence_number() > *sequence)
                .unwrap_or(true);
            if replace {
                latest.insert(
                    record.id().clone(),
                    (record.sequence_number(), Some(record.clone())),
                );
            }
        }
        for tombstone in segment.tombstones() {
            let replace = latest
                .get(tombstone.record_id())
                .map(|(sequence, _)| tombstone.sequence_number() > *sequence)
                .unwrap_or(true);
            if replace {
                latest.insert(
                    tombstone.record_id().clone(),
                    (tombstone.sequence_number(), None),
                );
            }
        }
    }

    latest
        .into_values()
        .filter_map(|(_, record)| record)
        .filter(|record| record_belongs_to_parent(record, parent_id))
        .map(|record| record.id().clone())
        .collect()
}

fn record_belongs_to_parent(record: &Record, parent_id: &RecordId) -> bool {
    let Some(MetadataValue::Object(chunk)) = record.metadata().get(CHUNK_METADATA_KEY) else {
        return false;
    };
    let expected_type = match parent_id {
        RecordId::String(_) => "string",
        RecordId::Unsigned(_) => "u64",
    };
    let expected_id = match parent_id {
        RecordId::String(value) => value.clone(),
        RecordId::Unsigned(value) => value.to_string(),
    };
    matches!(
        (chunk.get("parent_type"), chunk.get("parent_id")),
        (Some(MetadataValue::String(parent_type)), Some(MetadataValue::String(parent)))
            if parent_type == expected_type && parent == &expected_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_boundaries_are_deterministic_and_overlap() {
        let config = ChunkingConfig {
            max_chars: 5,
            overlap_chars: 2,
        };
        let chunks = chunk_text("abcdefghij", config);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.text.as_str())
                .collect::<Vec<_>>(),
            vec!["abcde", "defgh", "ghij"]
        );
        assert_eq!((chunks[1].start_char, chunks[1].end_char), (3, 8));
    }

    #[test]
    fn typed_parent_namespaces_derive_distinct_chunk_ids() {
        let string_id = chunk_record_id(&RecordId::string("42").unwrap(), 0);
        let numeric_id = chunk_record_id(&RecordId::unsigned(42), 0);
        assert_ne!(string_id, numeric_id);
        assert_eq!(
            string_id,
            chunk_record_id(&RecordId::string("42").unwrap(), 0)
        );
    }
}
