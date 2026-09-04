use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::routing::put;
use axum::{Json, Router};
use ketebe_core::RecordId;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::chunking::{
    ChunkedDocument, ChunkingConfig, ChunkingError, ChunkingService, resolve_effective_chunking,
};
use crate::dto::json_object_to_metadata;
use crate::embedding::{DocumentRecord, EmbeddingError, EmbeddingService};
use crate::http::{ApiError, admit_foreground_write};
use crate::management::CollectionService;
use crate::provenance::{DocumentSourceDto, ProvenanceError, apply_document_provenance};
use crate::runtime::AppState;
use crate::semantic_chunking_service::{
    SemanticChunkedDocument, SemanticChunkingError, SemanticChunkingService,
};
use crate::token_chunking_service::{
    TokenChunkedDocument, TokenChunkingError, TokenChunkingService,
};
use crate::write::WriteError;

pub(crate) fn routes(state: AppState) -> Router {
    Router::new()
        .route(
            "/v0/collections/{collection_id}/documents/{record_id}",
            put(upsert_document).delete(delete_document),
        )
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct DocumentBody {
    text: String,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    chunking: Option<ChunkingConfig>,
    #[serde(default)]
    source: Option<DocumentSourceDto>,
}

async fn upsert_document(
    State(state): State<AppState>,
    Path((collection_id, record_id)): Path<(String, String)>,
    principal: Extension<crate::Principal>,
    payload: Result<Json<DocumentBody>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let Json(body) = payload.map_err(|error| {
        ApiError::new(StatusCode::BAD_REQUEST, "invalid_json", error.body_text())
    })?;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal.0, &collection_id)
            .await
            .map_err(map_data_plane_request_error)?;
    let collection_id = scope.collection_id().clone();
    let record_id = RecordId::string(record_id).map_err(|error| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_record_id",
            error.to_string(),
        )
    })?;
    let mut metadata = json_object_to_metadata(body.metadata)?;
    let source = body
        .source
        .map(DocumentSourceDto::into_domain)
        .transpose()
        .map_err(map_provenance_error)?;
    apply_document_provenance(&mut metadata, source.as_ref(), &body.text)
        .map_err(map_provenance_error)?;
    let collection = CollectionService::new(state.clone())
        .get(&collection_id)
        .await
        .map_err(|error| map_embedding_error(EmbeddingError::Management(error)))?;

    if let Some(semantic_chunking) = collection
        .ingestion
        .as_ref()
        .and_then(|v| v.semantic_chunking())
    {
        if body.chunking.is_some() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_chunking_request",
                "request character chunking cannot override collection semantic_chunking schema",
            ));
        }
        let result = SemanticChunkingService::new(state)
            .chunk_embed_and_upsert(
                &collection_id,
                SemanticChunkedDocument {
                    id: record_id,
                    text: body.text,
                    metadata,
                    chunking: semantic_chunking,
                },
            )
            .await
            .map_err(map_semantic_chunking_error)?;
        let chunk_ids = result
            .chunk_ids
            .iter()
            .map(record_id_json)
            .collect::<Vec<_>>();
        return Ok(Json(json!({
            "chunk_count": result.chunk_ids.len(), "chunk_ids": chunk_ids,
            "sequence_numbers": result.sequence_numbers.iter().map(|v| v.get()).collect::<Vec<_>>(),
            "generation": result.generation.get(), "reconciled_chunks": result.reconciled_chunks,
            "chunking": "semantic"
        })));
    }

    if let Some(token_chunking) = collection
        .ingestion
        .as_ref()
        .and_then(|ingestion| ingestion.token_chunking())
    {
        if body.chunking.is_some() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_chunking_request",
                "request character chunking cannot override collection token_chunking schema",
            ));
        }
        let result = TokenChunkingService::new(state)
            .chunk_embed_and_upsert(
                &collection_id,
                TokenChunkedDocument {
                    id: record_id,
                    text: body.text,
                    metadata,
                    chunking: token_chunking,
                },
            )
            .await
            .map_err(map_token_chunking_error)?;
        let chunk_ids = result
            .chunk_ids
            .iter()
            .map(record_id_json)
            .collect::<Vec<_>>();
        return Ok(Json(json!({
            "chunk_count": result.chunk_ids.len(),
            "chunk_ids": chunk_ids,
            "sequence_numbers": result.sequence_numbers.iter().map(|value| value.get()).collect::<Vec<_>>(),
            "generation": result.generation.get(),
            "reconciled_chunks": result.reconciled_chunks,
            "chunking": "token_aware"
        })));
    }

    let effective_chunking =
        resolve_effective_chunking(collection.ingestion.as_ref(), body.chunking)
            .map_err(map_chunking_error)?;

    if let Some(chunking) = effective_chunking {
        let result = ChunkingService::new(state)
            .chunk_embed_and_upsert(
                &collection_id,
                ChunkedDocument {
                    id: record_id,
                    text: body.text,
                    metadata,
                    chunking,
                },
            )
            .await
            .map_err(map_chunking_error)?;
        let chunk_ids = result
            .chunk_ids
            .iter()
            .map(record_id_json)
            .collect::<Vec<_>>();
        return Ok(Json(json!({
            "chunk_count": result.chunk_ids.len(),
            "chunk_ids": chunk_ids,
            "sequence_numbers": result.sequence_numbers.iter().map(|value| value.get()).collect::<Vec<_>>(),
            "generation": result.generation.get(),
            "reconciled_chunks": result.reconciled_chunks
        })));
    }

    let service = EmbeddingService::from_state_for_collection(state, &collection_id)
        .await
        .map_err(map_embedding_error)?;
    let sequence = service
        .embed_and_upsert(
            &collection_id,
            DocumentRecord {
                id: record_id,
                text: body.text,
                metadata,
            },
        )
        .await
        .map_err(map_embedding_error)?;
    Ok(Json(json!({"sequence_number": sequence.get()})))
}

async fn delete_document(
    State(state): State<AppState>,
    Path((collection_id, record_id)): Path<(String, String)>,
    principal: Extension<crate::Principal>,
) -> Result<Json<Value>, ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal.0, &collection_id)
            .await
            .map_err(map_data_plane_request_error)?;
    let collection_id = scope.collection_id().clone();
    let record_id = RecordId::string(record_id).map_err(|error| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_record_id",
            error.to_string(),
        )
    })?;
    let sequences = ChunkingService::new(state)
        .delete_parent_document(&collection_id, &record_id)
        .await
        .map_err(map_chunking_error)?;
    Ok(Json(json!({
        "deleted_chunks": sequences.len(),
        "sequence_numbers": sequences.iter().map(|value| value.get()).collect::<Vec<_>>()
    })))
}

fn record_id_json(id: &RecordId) -> Value {
    match id {
        RecordId::String(value) => json!({"type":"string","value":value}),
        RecordId::Unsigned(value) => json!({"type":"u64","value":value}),
    }
}

fn map_data_plane_request_error(
    error: crate::data_plane_request::DataPlaneRequestError,
) -> ApiError {
    match error {
        crate::data_plane_request::DataPlaneRequestError::InvalidCollectionName(message) => {
            ApiError::new(StatusCode::BAD_REQUEST, "invalid_collection_name", message)
        }
        crate::data_plane_request::DataPlaneRequestError::CollectionNotFound => ApiError::new(
            StatusCode::NOT_FOUND,
            "collection_not_found",
            "collection was not found",
        ),
        crate::data_plane_request::DataPlaneRequestError::Resolution(
            crate::DataPlaneResolutionError::MissingProjectScope
            | crate::DataPlaneResolutionError::InvalidProjectScope(_),
        ) => ApiError::new(
            StatusCode::FORBIDDEN,
            "project_scope_required",
            error.to_string(),
        ),
        _ => ApiError::internal(error.to_string()),
    }
}

fn map_provenance_error(error: ProvenanceError) -> ApiError {
    ApiError::new(
        StatusCode::BAD_REQUEST,
        "invalid_document_provenance",
        error.to_string(),
    )
}

fn map_semantic_chunking_error(error: SemanticChunkingError) -> ApiError {
    match error {
        SemanticChunkingError::EmptyText
        | SemanticChunkingError::ReservedMetadata
        | SemanticChunkingError::SchemaMismatch => ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_chunking_request",
            error.to_string(),
        ),
        SemanticChunkingError::Embedding(error) => map_embedding_error(error),
        SemanticChunkingError::Lifecycle(error) => map_chunking_error(error),
    }
}

fn map_token_chunking_error(error: TokenChunkingError) -> ApiError {
    match error {
        TokenChunkingError::EmptyText
        | TokenChunkingError::ReservedMetadata
        | TokenChunkingError::SchemaMismatch => ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_chunking_request",
            error.to_string(),
        ),
        TokenChunkingError::Embedding(error) => map_embedding_error(error),
        TokenChunkingError::Lifecycle(error) => map_chunking_error(error),
    }
}

fn map_chunking_error(error: ChunkingError) -> ApiError {
    match error {
        ChunkingError::InvalidConfig(_)
        | ChunkingError::EmptyText
        | ChunkingError::ReservedMetadata
        | ChunkingError::SchemaMismatch => ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_chunking_request",
            error.to_string(),
        ),
        ChunkingError::Embedding(error) => map_embedding_error(error),
        ChunkingError::Write(WriteError::CollectionNotFound(_)) => ApiError::new(
            StatusCode::NOT_FOUND,
            "collection_not_found",
            error.to_string(),
        ),
        ChunkingError::Write(error) => ApiError::internal(error.to_string()),
    }
}

fn map_embedding_error(error: EmbeddingError) -> ApiError {
    match error {
        EmbeddingError::ProviderUnavailable | EmbeddingError::ProviderProfileUnavailable(_) => {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "embedding_unavailable",
                error.to_string(),
            )
        }
        EmbeddingError::EmptyText
        | EmbeddingError::ReservedMetadata
        | EmbeddingError::DimensionMismatch { .. }
        | EmbeddingError::NonFiniteVector { .. }
        | EmbeddingError::InvalidProvider(_) => ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_embedding_request",
            error.to_string(),
        ),
        EmbeddingError::Management(crate::ManagementError::CollectionNotFound(_)) => ApiError::new(
            StatusCode::NOT_FOUND,
            "collection_not_found",
            error.to_string(),
        ),
        EmbeddingError::Provider(_) => ApiError::new(
            StatusCode::BAD_GATEWAY,
            "embedding_provider_error",
            error.to_string(),
        ),
        EmbeddingError::Management(_) | EmbeddingError::Write(_) => {
            ApiError::internal(error.to_string())
        }
    }
}
