use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::http::ApiError;
use crate::management::CollectionService;
use crate::runtime::AppState;

pub(crate) fn routes(state: AppState) -> Router {
    Router::new()
        .route(
            "/v1/collections/{collection_id}/query:embed",
            post(embed_query_text),
        )
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct QueryEmbeddingBody {
    text: String,
}

#[derive(Debug, Serialize)]
struct QueryEmbeddingResponse {
    vector: Vec<f32>,
}

async fn embed_query_text(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    principal: Extension<crate::Principal>,
    payload: Result<Json<QueryEmbeddingBody>, JsonRejection>,
) -> Result<Json<QueryEmbeddingResponse>, ApiError> {
    let Json(body) = payload.map_err(|error| {
        ApiError::new(StatusCode::BAD_REQUEST, "invalid_json", error.body_text())
    })?;
    if body.text.trim().is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_embedding_request",
            "query text must not be empty",
        ));
    }

    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal.0, &collection_id)
            .await
            .map_err(map_data_plane_request_error)?;
    let collection_id = scope.collection_id().clone();
    let collection = CollectionService::new(state.clone())
        .get(&collection_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let profile = collection
        .ingestion
        .as_ref()
        .map(|ingestion| ingestion.embedding_profile())
        .unwrap_or("default");
    let provider = if collection.ingestion.is_some() {
        state.embedding_provider_profile(profile).await
    } else {
        state.embedding_provider().await
    }
    .ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "embedding_unavailable",
            "embedding provider is not configured",
        )
    })?;

    let mut vectors = crate::embedding_cache::embed_texts_cached(
        state.embedding_cache(),
        profile,
        provider,
        &[body.text],
        collection.dimension,
    )
    .await
    .map_err(|_| {
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            "embedding_provider_error",
            "embedding provider failed",
        )
    })?;
    let vector = vectors.pop().ok_or_else(|| {
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            "embedding_provider_error",
            "embedding provider returned no vector",
        )
    })?;
    Ok(Json(QueryEmbeddingResponse { vector }))
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
