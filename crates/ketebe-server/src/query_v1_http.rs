use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use ketebe_core::FieldPath;
use ketebe_storage::{DEFAULT_RRF_K, ExecutionPreference};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cursor::CursorError;
use crate::dto::{ExecutionDto, PredicateDto, RecordIdDto, metadata_map_to_json};
use crate::http::ApiError;
use crate::query_v1::{
    QueryModeV1, QueryPaginationV1, QueryRerankV1, QueryV1Error, QueryV1Request,
    execute_query_v1_page,
};
use crate::reranking::RerankFailurePolicy;
use crate::runtime::AppState;

pub(crate) fn routes(state: AppState) -> Router {
    Router::new()
        .route("/v1/collections/{collection_id}/query", post(query))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct QueryV1Body {
    #[serde(default)]
    vector: Option<Vec<f32>>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default = "default_top_k")]
    top_k: usize,
    #[serde(default)]
    predicate: Option<PredicateDto>,
    #[serde(default)]
    execution: ExecutionDto,
    #[serde(default)]
    dense_candidates: Option<usize>,
    #[serde(default)]
    lexical_candidates: Option<usize>,
    #[serde(default = "default_rrf_k")]
    rrf_k: u32,
    #[serde(default)]
    search_profile: Option<String>,
    #[serde(default = "default_true")]
    include_metadata: bool,
    #[serde(default)]
    include_provenance: bool,
    #[serde(default)]
    explain: bool,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    rerank: Option<RerankV1Body>,
    #[serde(default)]
    paginate: bool,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RerankV1Body {
    #[serde(default = "default_profile")]
    profile: String,
    #[serde(default)]
    query: Option<String>,
    top_n: usize,
    text_fields: Vec<Vec<String>>,
    #[serde(default)]
    include_metadata: bool,
    #[serde(default)]
    failure_policy: RerankFailurePolicyDto,
}

impl RerankV1Body {
    fn into_domain(self) -> Result<QueryRerankV1, ApiError> {
        let text_fields = self
            .text_fields
            .into_iter()
            .map(|segments| {
                FieldPath::new(segments).map_err(|error| {
                    ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "invalid_rerank_text_field",
                        error.to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(QueryRerankV1 {
            profile: self.profile,
            query: self.query,
            top_n: self.top_n,
            text_fields,
            include_metadata: self.include_metadata,
            failure_policy: self.failure_policy.into(),
        })
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RerankFailurePolicyDto {
    #[default]
    Fail,
    PreserveCandidateOrder,
}

impl From<RerankFailurePolicyDto> for RerankFailurePolicy {
    fn from(value: RerankFailurePolicyDto) -> Self {
        match value {
            RerankFailurePolicyDto::Fail => Self::Fail,
            RerankFailurePolicyDto::PreserveCandidateOrder => Self::PreserveCandidateOrder,
        }
    }
}

const fn default_top_k() -> usize {
    10
}
const fn default_rrf_k() -> u32 {
    DEFAULT_RRF_K
}
const fn default_true() -> bool {
    true
}
fn default_profile() -> String {
    "default".into()
}

#[derive(Debug, Serialize)]
struct QueryV1ResponseDto {
    api_version: &'static str,
    hits: Vec<QueryV1HitDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    explain: Option<QueryV1ExplainDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct QueryV1HitDto {
    id: RecordIdDto,
    score: f32,
    sequence_number: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dense_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lexical_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dense_score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lexical_score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rerank_score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_rank: Option<usize>,
}

#[derive(Debug, Serialize)]
struct QueryV1ExplainDto {
    mode: &'static str,
    strategy: String,
    reason: String,
    top_k: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    dense_candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lexical_candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rrf_k: Option<u32>,
    has_predicate: bool,
    search_profile: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rerank: Option<QueryRerankExplainDto>,
}

#[derive(Debug, Serialize)]
struct QueryRerankExplainDto {
    profile: String,
    provider: String,
    input_candidates: usize,
    applied: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    fallback_reason: Option<String>,
}

async fn query(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    principal: Extension<crate::Principal>,
    payload: Result<Json<QueryV1Body>, JsonRejection>,
) -> Result<Json<QueryV1ResponseDto>, ApiError> {
    let Json(body) = payload.map_err(|error| {
        ApiError::new(StatusCode::BAD_REQUEST, "invalid_json", error.body_text())
    })?;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal.0, &collection_id)
            .await
            .map_err(map_data_plane_request_error)?;
    let collection_id = scope.collection_id().clone();
    let predicate = body.predicate.map(PredicateDto::into_domain).transpose()?;
    let rerank = body.rerank.map(RerankV1Body::into_domain).transpose()?;
    let request = QueryV1Request {
        collection_id,
        vector: body.vector,
        text: body.text,
        top_k: body.top_k,
        predicate,
        execution: ExecutionPreference::from(body.execution),
        dense_candidates: body.dense_candidates,
        lexical_candidates: body.lexical_candidates,
        rrf_k: body.rrf_k,
        search_profile: body.search_profile,
        include_metadata: body.include_metadata,
        include_provenance: body.include_provenance,
        explain: body.explain,
        timeout_ms: body.timeout_ms,
        rerank,
    };
    let include_metadata = request.include_metadata;
    let page = execute_query_v1_page(
        &state,
        request,
        QueryPaginationV1 {
            enabled: body.paginate,
            cursor: body.cursor,
        },
    )
    .await
    .map_err(map_query_v1_error)?;
    let response = page.response;
    let hits = response
        .hits
        .into_iter()
        .map(|hit| {
            Ok(QueryV1HitDto {
                id: RecordIdDto::from(&hit.id),
                score: hit.score,
                sequence_number: hit.sequence_number,
                metadata: include_metadata
                    .then(|| metadata_map_to_json(&hit.metadata))
                    .transpose()?,
                dense_rank: hit.dense_rank,
                lexical_rank: hit.lexical_rank,
                dense_score: hit.dense_score,
                lexical_score: hit.lexical_score,
                rerank_score: hit.rerank_score,
                original_rank: hit.original_rank,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let explain = response.explain.map(|explain| QueryV1ExplainDto {
        mode: mode_name(explain.mode),
        strategy: explain.strategy,
        reason: explain.reason,
        top_k: explain.top_k,
        dense_candidates: explain.dense_candidates,
        lexical_candidates: explain.lexical_candidates,
        rrf_k: explain.rrf_k,
        has_predicate: explain.has_predicate,
        search_profile: explain.search_profile,
        timeout_ms: explain.timeout_ms,
        rerank: explain.rerank.map(|rerank| QueryRerankExplainDto {
            profile: rerank.profile,
            provider: rerank.provider,
            input_candidates: rerank.input_candidates,
            applied: rerank.applied,
            fallback_reason: rerank.fallback_reason,
        }),
    });
    Ok(Json(QueryV1ResponseDto {
        api_version: "v1",
        hits,
        explain,
        next_cursor: page.next_cursor,
    }))
}

const fn mode_name(mode: QueryModeV1) -> &'static str {
    match mode {
        QueryModeV1::Dense => "dense",
        QueryModeV1::Lexical => "lexical",
        QueryModeV1::Hybrid => "hybrid",
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

fn map_query_v1_error(error: QueryV1Error) -> ApiError {
    match error {
        QueryV1Error::Invalid(message) => {
            ApiError::new(StatusCode::BAD_REQUEST, "invalid_query_v1", message)
        }
        QueryV1Error::CollectionNotFound(message) => {
            ApiError::new(StatusCode::NOT_FOUND, "collection_not_found", message)
        }
        QueryV1Error::LexicalNotConfigured => ApiError::new(
            StatusCode::BAD_REQUEST,
            "lexical_not_configured",
            error.to_string(),
        ),
        QueryV1Error::UnsupportedSearchProfile(_) => ApiError::new(
            StatusCode::BAD_REQUEST,
            "search_profile_not_found",
            error.to_string(),
        ),
        QueryV1Error::RerankerProfileNotFound(_) => ApiError::new(
            StatusCode::BAD_REQUEST,
            "reranker_profile_not_found",
            error.to_string(),
        ),
        QueryV1Error::Reranking(crate::reranking::RerankingError::Provider(_)) => ApiError::new(
            StatusCode::BAD_GATEWAY,
            "reranker_provider_failed",
            error.to_string(),
        ),
        QueryV1Error::Reranking(_) => ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_reranking",
            error.to_string(),
        ),
        QueryV1Error::ResourceLimit(message) => ApiError::new(
            StatusCode::BAD_REQUEST,
            "query_resource_limit_exceeded",
            message,
        ),
        QueryV1Error::Overloaded => ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "query_overloaded",
            error.to_string(),
        ),
        QueryV1Error::DeadlineExceeded => ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            "query_deadline_exceeded",
            error.to_string(),
        ),
        QueryV1Error::Cancelled => ApiError::new(
            StatusCode::REQUEST_TIMEOUT,
            "query_cancelled",
            error.to_string(),
        ),
        QueryV1Error::Cursor(CursorError::Expired) => {
            ApiError::new(StatusCode::GONE, "cursor_expired", error.to_string())
        }
        QueryV1Error::Cursor(CursorError::StaleSnapshot) => {
            ApiError::new(StatusCode::CONFLICT, "stale_cursor", error.to_string())
        }
        QueryV1Error::Cursor(CursorError::QueryMismatch) => ApiError::new(
            StatusCode::CONFLICT,
            "cursor_query_mismatch",
            error.to_string(),
        ),
        QueryV1Error::Cursor(_) => {
            ApiError::new(StatusCode::BAD_REQUEST, "invalid_cursor", error.to_string())
        }
        QueryV1Error::CursorUnsupported(_) => ApiError::new(
            StatusCode::BAD_REQUEST,
            "cursor_unsupported",
            error.to_string(),
        ),
        QueryV1Error::Search(_)
        | QueryV1Error::Planner(_)
        | QueryV1Error::Hybrid(_)
        | QueryV1Error::Internal(_) => ApiError::internal(error.to_string()),
    }
}
