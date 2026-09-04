use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::http::{ApiError, admit_foreground_write};
use crate::runtime::AppState;
use crate::search_profiles::{
    SearchProfile, SearchProfileError, SearchProfileExecution, SearchProfileFailurePolicy,
    SearchProfileRerank, SearchProfileStore,
};

pub(crate) fn routes(state: AppState) -> Router {
    Router::new()
        .route(
            "/v1/collections/{collection_id}/search-profiles",
            post(create_profile).get(list_profiles),
        )
        .route(
            "/v1/collections/{collection_id}/search-profiles/{selector}",
            get(get_profile).delete(delete_profile),
        )
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct SearchProfileBody {
    name: String,
    version: u64,
    #[serde(default)]
    execution: ExecutionBody,
    #[serde(default)]
    dense_candidates: Option<usize>,
    #[serde(default)]
    lexical_candidates: Option<usize>,
    #[serde(default = "default_rrf_k")]
    rrf_k: u32,
    #[serde(default = "default_top_k")]
    final_top_k: usize,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    rerank: Option<RerankBody>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionBody {
    #[default]
    Auto,
    Exact,
    Hnsw,
}

#[derive(Debug, Deserialize)]
struct RerankBody {
    profile: String,
    top_n: usize,
    text_fields: Vec<Vec<String>>,
    #[serde(default)]
    include_metadata: bool,
    #[serde(default)]
    failure_policy: FailurePolicyBody,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FailurePolicyBody {
    #[default]
    Fail,
    PreserveCandidateOrder,
}

#[derive(Debug, Serialize)]
struct SearchProfileDto {
    name: String,
    version: u64,
    pinned_id: String,
    execution: &'static str,
    dense_candidates: Option<usize>,
    lexical_candidates: Option<usize>,
    rrf_k: u32,
    final_top_k: usize,
    timeout_ms: Option<u64>,
    rerank: Option<SearchProfileRerankDto>,
}

#[derive(Debug, Serialize)]
struct SearchProfileRerankDto {
    profile: String,
    top_n: usize,
    text_fields: Vec<Vec<String>>,
    include_metadata: bool,
    failure_policy: &'static str,
}

const fn default_rrf_k() -> u32 {
    ketebe_storage::DEFAULT_RRF_K
}

const fn default_top_k() -> usize {
    crate::search_profiles::DEFAULT_QUERY_TOP_K
}

async fn create_profile(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    principal: Extension<crate::Principal>,
    payload: Result<Json<SearchProfileBody>, JsonRejection>,
) -> Result<(StatusCode, Json<SearchProfileDto>), ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let collection_id = resolve_collection_id(&state, &principal.0, &collection_id).await?;
    let Json(body) = payload.map_err(|error| {
        ApiError::new(StatusCode::BAD_REQUEST, "invalid_json", error.body_text())
    })?;
    let profile = body.into_domain();
    let store = SearchProfileStore::new(state.data_dir.as_ref().clone());
    let profile = store
        .create(&collection_id, profile)
        .map_err(map_profile_error)?;
    Ok((StatusCode::CREATED, Json(profile.into())))
}

async fn list_profiles(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    principal: Extension<crate::Principal>,
) -> Result<Json<Vec<SearchProfileDto>>, ApiError> {
    let collection_id = resolve_collection_id(&state, &principal.0, &collection_id).await?;
    let store = SearchProfileStore::new(state.data_dir.as_ref().clone());
    let profiles = store
        .list(&collection_id)
        .map_err(map_profile_error)?
        .into_iter()
        .map(SearchProfileDto::from)
        .collect();
    Ok(Json(profiles))
}

async fn get_profile(
    State(state): State<AppState>,
    Path((collection_id, selector)): Path<(String, String)>,
    principal: Extension<crate::Principal>,
) -> Result<Json<SearchProfileDto>, ApiError> {
    let collection_id = resolve_collection_id(&state, &principal.0, &collection_id).await?;
    let store = SearchProfileStore::new(state.data_dir.as_ref().clone());
    let profile = store
        .get(&collection_id, &selector)
        .map_err(map_profile_error)?;
    Ok(Json(profile.into()))
}

async fn delete_profile(
    State(state): State<AppState>,
    Path((collection_id, selector)): Path<(String, String)>,
    principal: Extension<crate::Principal>,
) -> Result<Json<SearchProfileDto>, ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let collection_id = resolve_collection_id(&state, &principal.0, &collection_id).await?;
    let store = SearchProfileStore::new(state.data_dir.as_ref().clone());
    let profile = store
        .delete(&collection_id, &selector)
        .map_err(map_profile_error)?;
    Ok(Json(profile.into()))
}

async fn resolve_collection_id(
    state: &AppState,
    principal: &crate::Principal,
    collection_name: &str,
) -> Result<String, ApiError> {
    let scope =
        crate::data_plane_request::resolve_existing_scope(state, principal, collection_name)
            .await
            .map_err(map_data_plane_request_error)?;
    Ok(scope.collection_id().as_str().to_string())
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

impl SearchProfileBody {
    fn into_domain(self) -> SearchProfile {
        SearchProfile {
            name: self.name,
            version: self.version,
            execution: match self.execution {
                ExecutionBody::Auto => SearchProfileExecution::Auto,
                ExecutionBody::Exact => SearchProfileExecution::Exact,
                ExecutionBody::Hnsw => SearchProfileExecution::Hnsw,
            },
            dense_candidates: self.dense_candidates,
            lexical_candidates: self.lexical_candidates,
            rrf_k: self.rrf_k,
            final_top_k: self.final_top_k,
            timeout_ms: self.timeout_ms,
            rerank: self.rerank.map(|rerank| SearchProfileRerank {
                profile: rerank.profile,
                top_n: rerank.top_n,
                text_fields: rerank.text_fields,
                include_metadata: rerank.include_metadata,
                failure_policy: match rerank.failure_policy {
                    FailurePolicyBody::Fail => SearchProfileFailurePolicy::Fail,
                    FailurePolicyBody::PreserveCandidateOrder => {
                        SearchProfileFailurePolicy::PreserveCandidateOrder
                    }
                },
            }),
        }
    }
}

impl From<SearchProfile> for SearchProfileDto {
    fn from(profile: SearchProfile) -> Self {
        let pinned_id = profile.pinned_id();
        Self {
            name: profile.name,
            version: profile.version,
            pinned_id,
            execution: match profile.execution {
                SearchProfileExecution::Auto => "auto",
                SearchProfileExecution::Exact => "exact",
                SearchProfileExecution::Hnsw => "hnsw",
            },
            dense_candidates: profile.dense_candidates,
            lexical_candidates: profile.lexical_candidates,
            rrf_k: profile.rrf_k,
            final_top_k: profile.final_top_k,
            timeout_ms: profile.timeout_ms,
            rerank: profile.rerank.map(|rerank| SearchProfileRerankDto {
                profile: rerank.profile,
                top_n: rerank.top_n,
                text_fields: rerank.text_fields,
                include_metadata: rerank.include_metadata,
                failure_policy: match rerank.failure_policy {
                    SearchProfileFailurePolicy::Fail => "fail",
                    SearchProfileFailurePolicy::PreserveCandidateOrder => {
                        "preserve_candidate_order"
                    }
                },
            }),
        }
    }
}

fn map_profile_error(error: SearchProfileError) -> ApiError {
    match error {
        SearchProfileError::Invalid(message) => {
            ApiError::new(StatusCode::BAD_REQUEST, "invalid_search_profile", message)
        }
        SearchProfileError::AlreadyExists(selector) => ApiError::new(
            StatusCode::CONFLICT,
            "search_profile_exists",
            format!("search profile '{selector}' already exists"),
        ),
        SearchProfileError::NotFound(selector) => ApiError::new(
            StatusCode::NOT_FOUND,
            "search_profile_not_found",
            format!("search profile '{selector}' was not found"),
        ),
        SearchProfileError::Io(_) | SearchProfileError::Json(_) => {
            ApiError::internal(error.to_string())
        }
    }
}
