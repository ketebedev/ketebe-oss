use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::embedding::EmbeddingProfileInfo;
use crate::http::ApiError;
use crate::reranking::RerankerProfileInfo;
use crate::runtime::AppState;

pub(crate) fn routes(state: AppState) -> Router {
    Router::new()
        .route("/v0/embedding-profiles", get(list_embedding_profiles))
        .route(
            "/v0/embedding-profiles/{profile}",
            get(describe_embedding_profile),
        )
        .route("/v0/reranker-profiles", get(list_reranker_profiles))
        .route(
            "/v0/reranker-profiles/{profile}",
            get(describe_reranker_profile),
        )
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct EmbeddingProfileList {
    profiles: Vec<EmbeddingProfileInfo>,
}

#[derive(Debug, Serialize)]
struct RerankerProfileList {
    profiles: Vec<RerankerProfileInfo>,
}

async fn list_embedding_profiles(State(state): State<AppState>) -> Json<EmbeddingProfileList> {
    Json(EmbeddingProfileList {
        profiles: state.embedding_profiles().await,
    })
}

async fn describe_embedding_profile(
    State(state): State<AppState>,
    Path(profile): Path<String>,
) -> Result<Json<EmbeddingProfileInfo>, ApiError> {
    state
        .embedding_profiles()
        .await
        .into_iter()
        .find(|candidate| candidate.profile == profile)
        .map(Json)
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "embedding_profile_not_found",
                "embedding profile was not found",
            )
        })
}

async fn list_reranker_profiles(State(state): State<AppState>) -> Json<RerankerProfileList> {
    Json(RerankerProfileList {
        profiles: state.reranker_profiles().await,
    })
}

async fn describe_reranker_profile(
    State(state): State<AppState>,
    Path(profile): Path<String>,
) -> Result<Json<RerankerProfileInfo>, ApiError> {
    state
        .reranker_profiles()
        .await
        .into_iter()
        .find(|candidate| candidate.profile == profile)
        .map(Json)
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "reranker_profile_not_found",
                "reranker profile was not found",
            )
        })
}
