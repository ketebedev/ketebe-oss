use crate::TokenCounter;
use crate::chunking::{
    CHUNK_METADATA_KEY, ChunkedDocumentResult, ChunkingService, chunk_record_id,
};
use crate::semantic_chunking::{
    SEMANTIC_CHUNKER_VERSION, SEMANTIC_SCORER_ID, chunks_from_similarity_scores, cosine_similarity,
    semantic_boundary_candidates, semantic_chunker_fingerprint, token_fallback_policy,
};
use crate::token_chunking::chunk_text_token_aware;
use crate::{AppState, CollectionService, EmbeddingError, PendingRecord};
use ketebe_core::{CollectionId, Metadata, MetadataValue, RecordId, SemanticChunkingPolicy};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static REQUESTS: AtomicU64 = AtomicU64::new(0);
static FALLBACKS: AtomicU64 = AtomicU64::new(0);
static SCORER_INPUT_TOKENS: AtomicU64 = AtomicU64::new(0);
static LAST_SCORING_LATENCY_MICROS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct SemanticChunkedDocument {
    pub id: RecordId,
    pub text: String,
    pub metadata: Metadata,
    pub chunking: SemanticChunkingPolicy,
}

#[derive(Debug)]
pub enum SemanticChunkingError {
    EmptyText,
    ReservedMetadata,
    SchemaMismatch,
    Embedding(EmbeddingError),
    Lifecycle(crate::ChunkingError),
}
impl fmt::Display for SemanticChunkingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyText => write!(f, "document text must not be empty"),
            Self::ReservedMetadata => write!(
                f,
                "metadata keys '_ketebe_embedding' and '{CHUNK_METADATA_KEY}' are reserved by Ketebe"
            ),
            Self::SchemaMismatch => write!(
                f,
                "request semantic chunking does not match the collection ingestion schema"
            ),
            Self::Embedding(error) => write!(f, "embedding failed: {error}"),
            Self::Lifecycle(error) => write!(f, "parent document lifecycle failed: {error}"),
        }
    }
}
impl std::error::Error for SemanticChunkingError {}

#[derive(Clone)]
pub struct SemanticChunkingService {
    state: AppState,
    lifecycle: ChunkingService,
}

impl SemanticChunkingService {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self {
            lifecycle: ChunkingService::new(state.clone()),
            state,
        }
    }

    pub async fn chunk_embed_and_upsert(
        &self,
        collection_id: &CollectionId,
        document: SemanticChunkedDocument,
    ) -> Result<ChunkedDocumentResult, SemanticChunkingError> {
        REQUESTS.fetch_add(1, Ordering::Relaxed);
        if document.text.trim().is_empty() {
            return Err(SemanticChunkingError::EmptyText);
        }
        if document
            .metadata
            .contains_key(crate::embedding::EMBEDDING_METADATA_KEY)
            || document.metadata.contains_key(CHUNK_METADATA_KEY)
        {
            return Err(SemanticChunkingError::ReservedMetadata);
        }
        let collection = CollectionService::new(self.state.clone())
            .get(collection_id)
            .await
            .map_err(|e| SemanticChunkingError::Embedding(EmbeddingError::Management(e)))?;
        if let Some(ingestion) = collection.ingestion.as_ref()
            && ingestion.semantic_chunking() != Some(document.chunking)
        {
            return Err(SemanticChunkingError::SchemaMismatch);
        }
        let profile = collection
            .ingestion
            .as_ref()
            .map(|v| v.embedding_profile())
            .unwrap_or("default");
        let provider = if let Some(ingestion) = collection.ingestion.as_ref() {
            self.state
                .embedding_provider_profile(ingestion.embedding_profile())
                .await
                .ok_or_else(|| {
                    SemanticChunkingError::Embedding(EmbeddingError::ProviderProfileUnavailable(
                        ingestion.embedding_profile().to_string(),
                    ))
                })?
        } else {
            self.state
                .embedding_provider()
                .await
                .ok_or(SemanticChunkingError::Embedding(
                    EmbeddingError::ProviderUnavailable,
                ))?
        };

        let candidates = semantic_boundary_candidates(&document.text, document.chunking);
        let mut scorer_inputs = Vec::with_capacity(candidates.len() * 2);
        for candidate in &candidates {
            scorer_inputs.push(candidate.left_context.clone());
            scorer_inputs.push(candidate.right_context.clone());
        }
        let scorer_tokens: usize = scorer_inputs
            .iter()
            .map(|v| crate::UnicodeWordTokenCounter.count(v))
            .sum();
        SCORER_INPUT_TOKENS.fetch_add(scorer_tokens as u64, Ordering::Relaxed);
        let scoring_started = Instant::now();
        let scored_vectors = crate::embedding_cache::embed_texts_cached(
            self.state.embedding_cache(),
            profile,
            provider.clone(),
            &scorer_inputs,
            collection.dimension,
        )
        .await;
        LAST_SCORING_LATENCY_MICROS.store(
            scoring_started
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        let (chunks, fallback_used) = match scored_vectors {
            Ok(vectors) if vectors.len() == candidates.len() * 2 => {
                let mut scores = Vec::with_capacity(candidates.len());
                let mut valid = true;
                let (pairs, remainder) = vectors.as_chunks::<2>();
                debug_assert!(remainder.is_empty());
                for (candidate, pair) in candidates.iter().zip(pairs) {
                    if let Some(score) = cosine_similarity(&pair[0], &pair[1]) {
                        scores.push((candidate.token_index, score));
                    } else {
                        valid = false;
                        break;
                    }
                }
                if valid {
                    (
                        chunks_from_similarity_scores(&document.text, document.chunking, &scores),
                        false,
                    )
                } else {
                    FALLBACKS.fetch_add(1, Ordering::Relaxed);
                    (
                        chunk_text_token_aware(
                            &document.text,
                            token_fallback_policy(document.chunking),
                        ),
                        true,
                    )
                }
            }
            _ => {
                FALLBACKS.fetch_add(1, Ordering::Relaxed);
                (
                    chunk_text_token_aware(
                        &document.text,
                        token_fallback_policy(document.chunking),
                    ),
                    true,
                )
            }
        };

        let texts = chunks.iter().map(|c| c.text.clone()).collect::<Vec<_>>();
        let vectors = crate::embedding_cache::embed_texts_cached(
            self.state.embedding_cache(),
            profile,
            provider.clone(),
            &texts,
            collection.dimension,
        )
        .await
        .map_err(|e| SemanticChunkingError::Embedding(EmbeddingError::Provider(e)))?;
        let model = provider.model();
        let fingerprint = semantic_chunker_fingerprint(document.chunking, profile);
        let chunk_count = chunks.len();
        let mut records = Vec::with_capacity(chunk_count);
        let mut ids = Vec::with_capacity(chunk_count);
        for (chunk, vector) in chunks.into_iter().zip(vectors) {
            if vector.len() != collection.dimension {
                return Err(SemanticChunkingError::Embedding(
                    EmbeddingError::DimensionMismatch {
                        expected: collection.dimension,
                        actual: vector.len(),
                    },
                ));
            }
            if let Some((index, _)) = vector.iter().enumerate().find(|(_, v)| !v.is_finite()) {
                return Err(SemanticChunkingError::Embedding(
                    EmbeddingError::NonFiniteVector { index },
                ));
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
                "start_byte".to_string(),
                MetadataValue::Number(chunk.start_byte as f64),
            );
            chunk_meta.insert(
                "end_byte".to_string(),
                MetadataValue::Number(chunk.end_byte as f64),
            );
            chunk_meta.insert(
                "token_count".to_string(),
                MetadataValue::Number(chunk.token_count as f64),
            );
            chunk_meta.insert(
                "max_tokens".to_string(),
                MetadataValue::Number(document.chunking.max_tokens() as f64),
            );
            chunk_meta.insert(
                "token_overlap".to_string(),
                MetadataValue::Number(document.chunking.token_overlap() as f64),
            );
            chunk_meta.insert(
                "min_tokens".to_string(),
                MetadataValue::Number(document.chunking.min_tokens() as f64),
            );
            chunk_meta.insert(
                "breakpoint_threshold_milli".to_string(),
                MetadataValue::Number(document.chunking.breakpoint_threshold_milli() as f64),
            );
            chunk_meta.insert(
                "strategy".to_string(),
                MetadataValue::String("semantic".to_string()),
            );
            chunk_meta.insert(
                "tokenizer".to_string(),
                MetadataValue::String("unicode_words_v1".to_string()),
            );
            chunk_meta.insert(
                "chunker_version".to_string(),
                MetadataValue::String(SEMANTIC_CHUNKER_VERSION.to_string()),
            );
            chunk_meta.insert(
                "scorer".to_string(),
                MetadataValue::String(SEMANTIC_SCORER_ID.to_string()),
            );
            chunk_meta.insert(
                "scorer_profile".to_string(),
                MetadataValue::String(profile.to_string()),
            );
            chunk_meta.insert(
                "scorer_provider".to_string(),
                MetadataValue::String(provider.provider_name().to_string()),
            );
            chunk_meta.insert(
                "scorer_model".to_string(),
                MetadataValue::String(model.name.clone()),
            );
            chunk_meta.insert(
                "scorer_version".to_string(),
                MetadataValue::String(model.version.clone()),
            );
            chunk_meta.insert(
                "fallback_used".to_string(),
                MetadataValue::Bool(fallback_used),
            );
            chunk_meta.insert(
                "fingerprint".to_string(),
                MetadataValue::String(fingerprint.clone()),
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
            ids.push(chunk_id);
        }
        self.lifecycle
            .replace_parent_chunks(collection_id, &document.id, records, ids)
            .await
            .map_err(SemanticChunkingError::Lifecycle)
    }
}

#[must_use]
pub fn semantic_chunking_prometheus_metrics() -> String {
    format!(
        concat!(
            "ketebe_semantic_chunking_requests_total {}\n",
            "ketebe_semantic_chunking_fallbacks_total {}\n",
            "ketebe_semantic_chunking_scorer_input_tokens_total {}\n",
            "ketebe_semantic_chunking_last_scoring_latency_microseconds {}\n"
        ),
        REQUESTS.load(Ordering::Relaxed),
        FALLBACKS.load(Ordering::Relaxed),
        SCORER_INPUT_TOKENS.load(Ordering::Relaxed),
        LAST_SCORING_LATENCY_MICROS.load(Ordering::Relaxed)
    )
}
