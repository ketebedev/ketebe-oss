use crate::chunking::{
    CHUNK_METADATA_KEY, ChunkedDocumentResult, ChunkingService, chunk_record_id,
};
use crate::token_chunking::{chunk_text_token_aware, chunker_fingerprint};
use crate::{AppState, CollectionService, EmbeddingError, PendingRecord};
use ketebe_core::{CollectionId, Metadata, MetadataValue, RecordId, TokenChunkingPolicy};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone)]
pub struct TokenChunkedDocument {
    pub id: RecordId,
    pub text: String,
    pub metadata: Metadata,
    pub chunking: TokenChunkingPolicy,
}

#[derive(Debug)]
pub enum TokenChunkingError {
    EmptyText,
    ReservedMetadata,
    SchemaMismatch,
    Embedding(EmbeddingError),
    Lifecycle(crate::ChunkingError),
}

impl fmt::Display for TokenChunkingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyText => write!(f, "document text must not be empty"),
            Self::ReservedMetadata => write!(
                f,
                "metadata keys '_ketebe_embedding' and '{CHUNK_METADATA_KEY}' are reserved by Ketebe"
            ),
            Self::SchemaMismatch => write!(
                f,
                "request token chunking does not match the collection ingestion schema"
            ),
            Self::Embedding(error) => write!(f, "embedding failed: {error}"),
            Self::Lifecycle(error) => write!(f, "parent document lifecycle failed: {error}"),
        }
    }
}
impl std::error::Error for TokenChunkingError {}

#[derive(Clone)]
pub struct TokenChunkingService {
    state: AppState,
    lifecycle: ChunkingService,
}

impl TokenChunkingService {
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
        document: TokenChunkedDocument,
    ) -> Result<ChunkedDocumentResult, TokenChunkingError> {
        if document.text.trim().is_empty() {
            return Err(TokenChunkingError::EmptyText);
        }
        if document
            .metadata
            .contains_key(crate::embedding::EMBEDDING_METADATA_KEY)
            || document.metadata.contains_key(CHUNK_METADATA_KEY)
        {
            return Err(TokenChunkingError::ReservedMetadata);
        }

        let collection = CollectionService::new(self.state.clone())
            .get(collection_id)
            .await
            .map_err(|error| TokenChunkingError::Embedding(EmbeddingError::Management(error)))?;
        if let Some(ingestion) = collection.ingestion.as_ref()
            && ingestion.token_chunking() != Some(document.chunking)
        {
            return Err(TokenChunkingError::SchemaMismatch);
        }

        let provider = if let Some(ingestion) = collection.ingestion.as_ref() {
            self.state
                .embedding_provider_profile(ingestion.embedding_profile())
                .await
                .ok_or_else(|| {
                    TokenChunkingError::Embedding(EmbeddingError::ProviderProfileUnavailable(
                        ingestion.embedding_profile().to_string(),
                    ))
                })?
        } else {
            self.state
                .embedding_provider()
                .await
                .ok_or(TokenChunkingError::Embedding(
                    EmbeddingError::ProviderUnavailable,
                ))?
        };

        let profile = collection
            .ingestion
            .as_ref()
            .map(|ingestion| ingestion.embedding_profile())
            .unwrap_or("default");
        let chunks = chunk_text_token_aware(&document.text, document.chunking);
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
        .map_err(|error| TokenChunkingError::Embedding(EmbeddingError::Provider(error)))?;
        let chunk_count = chunks.len();
        let model = provider.model();
        let fingerprint = chunker_fingerprint(document.chunking);
        let mut records = Vec::with_capacity(chunk_count);
        let mut chunk_ids = Vec::with_capacity(chunk_count);

        for (chunk, vector) in chunks.into_iter().zip(vectors) {
            if vector.len() != collection.dimension {
                return Err(TokenChunkingError::Embedding(
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
                return Err(TokenChunkingError::Embedding(
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
                "tokenizer".to_string(),
                MetadataValue::String(tokenizer_name(document.chunking).to_string()),
            );
            chunk_meta.insert(
                "strategy".to_string(),
                MetadataValue::String(structure_name(document.chunking).to_string()),
            );
            chunk_meta.insert(
                "chunker_version".to_string(),
                MetadataValue::String("token-structural-v1".to_string()),
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
            chunk_ids.push(chunk_id);
        }

        self.lifecycle
            .replace_parent_chunks(collection_id, &document.id, records, chunk_ids)
            .await
            .map_err(TokenChunkingError::Lifecycle)
    }
}

fn tokenizer_name(policy: TokenChunkingPolicy) -> &'static str {
    match policy.tokenizer() {
        ketebe_core::TokenizerKind::UnicodeWordsV1 => "unicode_words_v1",
    }
}

fn structure_name(policy: TokenChunkingPolicy) -> &'static str {
    match policy.structure() {
        ketebe_core::ChunkingStructure::Tokens => "tokens",
        ketebe_core::ChunkingStructure::Sentences => "sentences",
        ketebe_core::ChunkingStructure::Paragraphs => "paragraphs",
        ketebe_core::ChunkingStructure::Markdown => "markdown",
        ketebe_core::ChunkingStructure::Html => "html",
    }
}
