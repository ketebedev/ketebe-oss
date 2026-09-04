use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
#[cfg(test)]
use ketebe_core::CollectionId;
use ketebe_core::{DistanceMetric, RecordId};
use ketebe_storage::{
    FilteredSearchError, HnswError, HybridError, LexicalQuery, PlannerError, QueryRequest,
    SearchError, execute_hybrid_query, execute_hybrid_query_with_index, execute_query,
};
use serde::Serialize;

use crate::dto::{
    BatchMutationDto, BatchUpsertBody, CollectionDto, CreateCollectionBody, ExplainDto, HitDto,
    IngestionSchemaDto, LexicalAnalyzerDto, MutationDto, QueryBody, QueryResponseDto, RecordIdDto,
    UpdateLexicalSchemaBody, UpsertBody, field_path, json_object_to_metadata, metadata_map_to_json,
    metric_name, reason_name, strategy_name,
};
use crate::runtime::AppState;
use crate::write::{PendingRecord, WriteError, WriteService};

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/v0/collections", post(create_collection))
        .route(
            "/v0/collections/{collection_id}/lexical-schema",
            put(update_lexical_schema),
        )
        .route(
            "/v0/collections/{collection_id}/query",
            post(query_collection),
        )
        .route(
            "/v0/collections/{collection_id}/records/{record_id}",
            put(upsert_record).delete(delete_record),
        )
        .route(
            "/v0/collections/{collection_id}/records:batchUpsert",
            post(batch_upsert),
        )
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct StatusDto {
    status: &'static str,
}
async fn health() -> Json<StatusDto> {
    Json(StatusDto { status: "ok" })
}
async fn ready(State(state): State<AppState>) -> Result<Json<StatusDto>, ApiError> {
    if state.is_ready() && state.catalog.read().await.ready {
        Ok(Json(StatusDto { status: "ready" }))
    } else {
        Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_ready",
            "Ketebe runtime is not ready",
        ))
    }
}

pub(crate) fn admit_foreground_write(
    state: &AppState,
) -> Result<crate::LifecycleWriteGuard, ApiError> {
    state.try_admit_foreground_write().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "draining",
            "Ketebe runtime is draining and no longer accepts new writes",
        )
    })
}

async fn create_collection(
    State(state): State<AppState>,
    principal: Option<Extension<crate::Principal>>,
    payload: Result<Json<CreateCollectionBody>, JsonRejection>,
) -> Result<(StatusCode, Json<CollectionDto>), ApiError> {
    let principal = request_principal(&state, principal)?;
    let _write_guard = admit_foreground_write(&state)?;
    let Json(body) = parse_json(payload)?;
    let collection_name = body.id;
    let metric = DistanceMetric::from(body.metric);
    let lexical_fields = body
        .lexical_fields
        .into_iter()
        .map(field_path)
        .collect::<Result<Vec<_>, _>>()?;
    let ingestion = body
        .ingestion
        .map(IngestionSchemaDto::into_domain)
        .transpose()?;
    let scope = crate::data_plane_request::create_scope(&state, &principal, &collection_name)
        .map_err(map_data_plane_request_error)?;
    let id = scope.collection_id().clone();
    let claim = match state
        .authorization()
        .claim_collection(&principal, &collection_name)
    {
        Ok(claim) => claim,
        Err(error) => {
            let _ =
                crate::data_plane_request::remove_scope(&state, &principal, &collection_name, &id);
            return Err(map_authorization_error(error));
        }
    };
    let result = WriteService::new(state.clone())
        .create_collection_with_schema_scoped(
            &scope,
            body.dimension,
            metric,
            lexical_fields,
            body.analyzer.into(),
            ingestion,
        )
        .await;
    let config = match result {
        Ok(config) => config,
        Err(error) => {
            let _ = state
                .authorization()
                .release_collection_claim_for_principal(&principal, &collection_name, claim);
            let _ =
                crate::data_plane_request::remove_scope(&state, &principal, &collection_name, &id);
            return Err(map_write_error(error));
        }
    };
    Ok((
        StatusCode::CREATED,
        Json(CollectionDto {
            id: collection_name,
            dimension: config.dimension(),
            metric: config.distance_metric().into(),
            lexical_fields: config
                .lexical_fields()
                .iter()
                .map(|path| path.segments().to_vec())
                .collect(),
            analyzer: LexicalAnalyzerDto::from(config.lexical_analyzer()),
            ingestion: config.ingestion().map(IngestionSchemaDto::from),
        }),
    ))
}

async fn update_lexical_schema(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    principal: Option<Extension<crate::Principal>>,
    payload: Result<Json<UpdateLexicalSchemaBody>, JsonRejection>,
) -> Result<Json<CollectionDto>, ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let Json(body) = parse_json(payload)?;
    let principal = request_principal(&state, principal)?;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &collection_id)
            .await
            .map_err(map_data_plane_request_error)?;
    let lexical_fields = body
        .lexical_fields
        .into_iter()
        .map(field_path)
        .collect::<Result<Vec<_>, _>>()?;
    let config = WriteService::new(state)
        .update_lexical_schema_scoped(&scope, lexical_fields, body.analyzer.into())
        .await
        .map_err(map_write_error)?;
    Ok(Json(CollectionDto {
        id: config.id().as_str().to_string(),
        dimension: config.dimension(),
        metric: config.distance_metric().into(),
        lexical_fields: config
            .lexical_fields()
            .iter()
            .map(|path| path.segments().to_vec())
            .collect(),
        analyzer: LexicalAnalyzerDto::from(config.lexical_analyzer()),
        ingestion: config.ingestion().map(IngestionSchemaDto::from),
    }))
}

async fn upsert_record(
    State(state): State<AppState>,
    Path((collection_id, record_id)): Path<(String, String)>,
    principal: Option<Extension<crate::Principal>>,
    payload: Result<Json<UpsertBody>, JsonRejection>,
) -> Result<Json<MutationDto>, ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let Json(body) = parse_json(payload)?;
    let principal = request_principal(&state, principal)?;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &collection_id)
            .await
            .map_err(map_data_plane_request_error)?;
    let record_id = RecordId::string(record_id).map_err(|error| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_record_id",
            error.to_string(),
        )
    })?;
    let sequence = WriteService::new(state)
        .upsert_scoped(
            &scope,
            PendingRecord {
                id: record_id,
                vector: body.vector,
                metadata: json_object_to_metadata(body.metadata)?,
            },
        )
        .await
        .map_err(map_write_error)?;
    Ok(Json(MutationDto {
        sequence_number: sequence.get(),
    }))
}

async fn batch_upsert(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    principal: Option<Extension<crate::Principal>>,
    payload: Result<Json<BatchUpsertBody>, JsonRejection>,
) -> Result<Json<BatchMutationDto>, ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let Json(body) = parse_json(payload)?;
    let principal = request_principal(&state, principal)?;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &collection_id)
            .await
            .map_err(map_data_plane_request_error)?;
    let records = body
        .records
        .into_iter()
        .map(|record| {
            Ok(PendingRecord {
                id: record.id.into_domain()?,
                vector: record.vector,
                metadata: json_object_to_metadata(record.metadata)?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let sequences = WriteService::new(state)
        .upsert_batch_scoped(&scope, records)
        .await
        .map_err(map_write_error)?;
    Ok(Json(BatchMutationDto {
        sequence_numbers: sequences.into_iter().map(|value| value.get()).collect(),
    }))
}

async fn delete_record(
    State(state): State<AppState>,
    Path((collection_id, record_id)): Path<(String, String)>,
    principal: Option<Extension<crate::Principal>>,
) -> Result<Json<MutationDto>, ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let principal = request_principal(&state, principal)?;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &collection_id)
            .await
            .map_err(map_data_plane_request_error)?;
    let record_id = RecordId::string(record_id).map_err(|error| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_record_id",
            error.to_string(),
        )
    })?;
    let sequence = WriteService::new(state)
        .delete_scoped(&scope, record_id)
        .await
        .map_err(map_write_error)?;
    Ok(Json(MutationDto {
        sequence_number: sequence.get(),
    }))
}

async fn query_collection(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    principal: Option<Extension<crate::Principal>>,
    payload: Result<Json<QueryBody>, JsonRejection>,
) -> Result<Json<QueryResponseDto>, ApiError> {
    let Json(body) = parse_json(payload)?;
    let principal = request_principal(&state, principal)?;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &collection_id)
            .await
            .map_err(map_data_plane_request_error)?;
    let collection_id = scope.collection_id().clone();
    let requested_metric = DistanceMetric::from(body.metric);
    let catalog = state.catalog.read().await;
    let runtime = catalog.collections.get(&collection_id).ok_or_else(|| {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "collection_not_found",
            format!("collection '{}' was not found", collection_id.as_str()),
        )
    })?;
    if requested_metric != runtime.metric {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "metric_mismatch",
            format!(
                "requested metric does not match collection metric: requested={}, collection={}",
                metric_name(requested_metric),
                metric_name(runtime.metric)
            ),
        ));
    }

    let mut request = QueryRequest::new(collection_id, body.vector, requested_metric, body.top_k)
        .with_preference(body.execution.into());
    if let Some(predicate) = body.predicate {
        request = request.with_predicate(predicate.into_domain()?);
    }
    let segments = runtime.query_segments().map_err(|error| {
        ApiError::internal(format!("failed to build mutable query overlay: {error}"))
    })?;

    if let Some(lexical) = body.lexical {
        let mut fields = lexical
            .fields
            .into_iter()
            .map(field_path)
            .collect::<Result<Vec<_>, _>>()?;
        let configured = runtime.configured_lexical_fields();
        if !configured.is_empty() {
            if fields.is_empty() {
                fields = configured.to_vec();
            } else {
                fields.sort();
                fields.dedup();
                if fields != configured {
                    return Err(ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "lexical_fields_mismatch",
                        "query lexical fields must match the collection lexical configuration",
                    ));
                }
                fields = configured.to_vec();
            }
        }
        let lexical_query = LexicalQuery::new(lexical.text, fields)
            .map_err(map_hybrid_error)?
            .with_analyzer(runtime.configured_lexical_analyzer());
        let collection_directory = match ketebe_storage::ScopedStorageNamespace::open_existing(
            &*state.data_dir,
            scope.clone(),
        ) {
            Ok(namespace) => namespace.root().to_path_buf(),
            Err(ketebe_storage::NamespaceError::MissingNamespace(_))
                if !scope.collection_id().as_str().starts_with("c_") =>
            {
                state
                    .data_dir
                    .join("collections")
                    .join(scope.collection_id().as_str())
            }
            Err(error) => {
                return Err(ApiError::internal(format!(
                    "storage scope validation failed: {error}"
                )));
            }
        };
        let persistent_index = runtime
            .query_lexical_index(&collection_directory, lexical_query.fields())
            .map_err(|error| {
                ApiError::internal(format!("lexical index lifecycle failure: {error}"))
            })?;
        let response = if let Some(index) = persistent_index {
            execute_hybrid_query_with_index(
                &request,
                &lexical_query,
                &index,
                &segments,
                runtime.query_hnsw(),
                lexical.rrf_k,
            )
        } else {
            execute_hybrid_query(
                &request,
                &lexical_query,
                &segments,
                runtime.query_hnsw(),
                lexical.rrf_k,
            )
        }
        .map_err(map_hybrid_error)?;
        let hits = response
            .hits()
            .iter()
            .map(|hit| {
                Ok(HitDto {
                    id: RecordIdDto::from(hit.record().id()),
                    score: hit.score(),
                    sequence_number: hit.record().sequence_number().get(),
                    metadata: metadata_map_to_json(hit.record().metadata())?,
                    dense_rank: hit.dense_rank(),
                    lexical_rank: hit.lexical_rank(),
                    dense_score: hit.dense_score(),
                    lexical_score: hit.lexical_score(),
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        let explain = response.explain();
        let dense = explain.dense();
        return Ok(Json(QueryResponseDto {
            hits,
            explain: ExplainDto {
                strategy: strategy_name(dense.strategy()),
                reason: reason_name(dense.reason()),
                collection_id: dense.collection_id().as_str().to_string(),
                metric: dense.metric().into(),
                top_k: dense.top_k(),
                has_predicate: dense.has_predicate(),
                candidate_limit: dense.candidate_limit(),
                fallback: dense.fallback(),
                hybrid: true,
                dense_candidates: Some(explain.dense_candidates()),
                lexical_candidates: Some(explain.lexical_candidates()),
                rrf_k: Some(explain.rrf_k()),
            },
        }));
    }

    let response =
        execute_query(&request, &segments, runtime.query_hnsw()).map_err(map_planner_error)?;
    let hits = response
        .hits()
        .iter()
        .map(|hit| {
            Ok(HitDto {
                id: RecordIdDto::from(hit.record().id()),
                score: hit.score(),
                sequence_number: hit.record().sequence_number().get(),
                metadata: metadata_map_to_json(hit.record().metadata())?,
                dense_rank: None,
                lexical_rank: None,
                dense_score: None,
                lexical_score: None,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let explain = response.explain();
    Ok(Json(QueryResponseDto {
        hits,
        explain: ExplainDto {
            strategy: strategy_name(explain.strategy()),
            reason: reason_name(explain.reason()),
            collection_id: explain.collection_id().as_str().to_string(),
            metric: explain.metric().into(),
            top_k: explain.top_k(),
            has_predicate: explain.has_predicate(),
            candidate_limit: explain.candidate_limit(),
            fallback: explain.fallback(),
            hybrid: false,
            dense_candidates: None,
            lexical_candidates: None,
            rrf_k: None,
        },
    }))
}

fn request_principal(
    state: &AppState,
    principal: Option<Extension<crate::Principal>>,
) -> Result<crate::Principal, ApiError> {
    match principal {
        Some(Extension(principal)) => Ok(principal),
        None if state.authorization().mode() == crate::AuthorizationMode::Development => {
            crate::Principal::for_project("development", "default").map_err(|error| {
                ApiError::internal(format!("failed to establish development scope: {error}"))
            })
        }
        None => Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "authentication required",
        )),
    }
}

fn map_data_plane_request_error(
    error: crate::data_plane_request::DataPlaneRequestError,
) -> ApiError {
    use crate::data_plane_request::DataPlaneRequestError;
    use crate::{CollectionNamespaceError, DataPlaneResolutionError};
    match error {
        DataPlaneRequestError::InvalidCollectionName(message) => {
            ApiError::new(StatusCode::BAD_REQUEST, "invalid_collection_name", message)
        }
        DataPlaneRequestError::CollectionNotFound
        | DataPlaneRequestError::Resolution(DataPlaneResolutionError::Catalog(
            CollectionNamespaceError::NameAlreadyExists,
        )) => ApiError::new(
            StatusCode::NOT_FOUND,
            "collection_not_found",
            "collection was not found",
        ),
        DataPlaneRequestError::Resolution(DataPlaneResolutionError::MissingProjectScope)
        | DataPlaneRequestError::Resolution(DataPlaneResolutionError::InvalidProjectScope(_)) => {
            ApiError::new(
                StatusCode::FORBIDDEN,
                "project_scope_required",
                error.to_string(),
            )
        }
        DataPlaneRequestError::Resolution(_)
        | DataPlaneRequestError::NamespaceCatalog(_)
        | DataPlaneRequestError::CatalogLockPoisoned => ApiError::internal(error.to_string()),
    }
}

fn parse_json<T>(payload: Result<Json<T>, JsonRejection>) -> Result<Json<T>, ApiError> {
    payload
        .map_err(|error| ApiError::new(StatusCode::BAD_REQUEST, "invalid_json", error.body_text()))
}
fn map_authorization_error(error: crate::AuthorizationError) -> ApiError {
    match error {
        crate::AuthorizationError::Undiscoverable
        | crate::AuthorizationError::OwnershipConflict => ApiError::new(
            StatusCode::NOT_FOUND,
            "collection_not_found",
            "collection was not found",
        ),
        crate::AuthorizationError::Denied | crate::AuthorizationError::MissingProject => {
            ApiError::new(StatusCode::FORBIDDEN, "forbidden", "authorization denied")
        }
        _ => ApiError::internal(error.to_string()),
    }
}

fn map_write_error(error: WriteError) -> ApiError {
    match error {
        WriteError::Validation(message) => {
            ApiError::new(StatusCode::BAD_REQUEST, "invalid_write", message)
        }
        WriteError::CollectionAlreadyExists(id) => ApiError::new(
            StatusCode::CONFLICT,
            "collection_already_exists",
            format!("collection '{}' already exists", id.as_str()),
        ),
        WriteError::CollectionNotFound(id) => ApiError::new(
            StatusCode::NOT_FOUND,
            "collection_not_found",
            format!("collection '{}' was not found", id.as_str()),
        ),
        WriteError::CollectionNotWritable => {
            ApiError::internal("collection runtime is not writable")
        }
        WriteError::Scope(message) => ApiError::internal(message),
        WriteError::Io(error) => ApiError::internal(format!("write storage failure: {error}")),
        WriteError::Json(error) => ApiError::internal(format!("write metadata failure: {error}")),
        WriteError::Wal(error) => ApiError::internal(format!("write WAL failure: {error}")),
        WriteError::Segment(error) => ApiError::internal(format!("write segment failure: {error}")),
        WriteError::Checkpoint(error) => {
            ApiError::internal(format!("write checkpoint failure: {error}"))
        }
        WriteError::Compaction(error) => {
            ApiError::internal(format!("write compaction failure: {error}"))
        }
        WriteError::WalReclaim(error) => {
            ApiError::internal(format!("write WAL reclaim failure: {error}"))
        }
    }
}
fn map_planner_error(error: PlannerError) -> ApiError {
    match error {
        PlannerError::MissingHnswIndex => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "hnsw_unavailable",
            "HNSW execution was requested but no index is available",
        ),
        PlannerError::HnswCollectionMismatch { .. } => {
            ApiError::internal("configured HNSW index does not match the collection")
        }
        PlannerError::Exact(error) => map_search_error(error),
        PlannerError::Hnsw(error) => map_hnsw_error(error),
        PlannerError::Filtered(error) => map_filtered_error(error),
    }
}
fn map_hybrid_error(error: HybridError) -> ApiError {
    match error {
        HybridError::EmptyLexicalQuery
        | HybridError::EmptyLexicalFields
        | HybridError::InvalidTopK
        | HybridError::InvalidRrfK
        | HybridError::CandidateDepthBelowTopK
        | HybridError::CandidateBudgetExceeded { .. }
        | HybridError::Predicate(_) => ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_hybrid_query",
            error.to_string(),
        ),
        HybridError::LexicalIndexMismatch | HybridError::Index(_) => {
            ApiError::internal(error.to_string())
        }
        HybridError::Dense(error) => map_planner_error(error),
    }
}
fn map_search_error(error: SearchError) -> ApiError {
    let message = error.to_string();
    match error {
        SearchError::InvalidTopK
        | SearchError::EmptyQueryVector
        | SearchError::NonFiniteQueryValue { .. }
        | SearchError::DimensionMismatch { .. }
        | SearchError::ZeroNormVector
        | SearchError::Predicate(_) => {
            ApiError::new(StatusCode::BAD_REQUEST, "invalid_query", message)
        }
        SearchError::Segment(_) | SearchError::Control(_) => ApiError::internal(message),
    }
}
fn map_hnsw_error(error: HnswError) -> ApiError {
    let message = error.to_string();
    match error {
        HnswError::InvalidTopK
        | HnswError::EfSearchTooSmall { .. }
        | HnswError::EmptyQueryVector
        | HnswError::NonFiniteQueryValue { .. }
        | HnswError::DimensionMismatch { .. }
        | HnswError::ZeroNormVector => {
            ApiError::new(StatusCode::BAD_REQUEST, "invalid_query", message)
        }
        HnswError::InvalidConfig(_)
        | HnswError::InvalidGraph(_)
        | HnswError::ExactSearch(_)
        | HnswError::Control(_) => ApiError::internal(message),
    }
}
fn map_filtered_error(error: FilteredSearchError) -> ApiError {
    match error {
        FilteredSearchError::Exact(error) => map_search_error(error),
        FilteredSearchError::Hnsw(error) => map_hnsw_error(error),
        FilteredSearchError::Predicate(error) => ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_predicate",
            error.to_string(),
        ),
    }
}

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}
impl ApiError {
    pub(crate) fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
    }
}
#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    error: ErrorDto,
}
#[derive(Debug, Serialize)]
struct ErrorDto {
    code: &'static str,
    message: String,
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorEnvelope {
                error: ErrorDto {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::PredicateDto;
    use crate::runtime::{AppState, CollectionRuntime, RuntimeCatalog};
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, header};
    use ketebe_core::{Metadata, MetadataValue, Record, SequenceNumber, Vector};
    use ketebe_storage::{Segment, SegmentId, WalMutation};
    use serde_json::Value;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tower::ServiceExt;

    fn record(id: RecordId, sequence: u64, value: f32, category: &str) -> Record {
        let mut metadata = Metadata::new();
        metadata.insert(
            "category".into(),
            MetadataValue::String(category.to_string()),
        );
        metadata.insert("price".into(), MetadataValue::Number(value as f64));
        Record::new(
            id,
            Vector::new(vec![value]).expect("vector"),
            metadata,
            SequenceNumber::new(sequence),
        )
    }
    fn test_state() -> AppState {
        let collection = CollectionId::new("docs").expect("collection");
        let mutations = vec![
            WalMutation::Upsert {
                collection_id: collection.clone(),
                record: record(RecordId::string("alpha").expect("id"), 1, 1.0, "book"),
            },
            WalMutation::Upsert {
                collection_id: collection.clone(),
                record: record(RecordId::unsigned(42), 2, 2.0, "game"),
            },
        ];
        let segment = Segment::from_mutations(SegmentId::new(1), &mutations).expect("segment");
        let mut catalog = RuntimeCatalog::empty_ready();
        catalog.insert_collection(
            collection,
            CollectionRuntime::new(DistanceMetric::L2, vec![segment], None),
        );
        AppState::new(catalog)
    }
    fn temp_data_dir() -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("ketebe-write-api-{nonce}"))
    }
    async fn body_json(response: Response<axum::body::Body>) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("json")
    }
    async fn request_json(router: Router, method: &str, uri: &str, body: &str) -> Response {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .expect("request");
        router.oneshot(request).await.expect("response")
    }

    #[tokio::test]
    async fn health_and_readiness_are_served() {
        let router = app(test_state());
        let health = router
            .clone()
            .oneshot(
                Request::get("/healthz")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(health.status(), StatusCode::OK);
        let ready = router
            .oneshot(
                Request::get("/readyz")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(ready.status(), StatusCode::OK);
    }
    #[tokio::test]
    async fn exact_query_returns_hits_explain_and_typed_ids() {
        let response = request_json(
            app(test_state()),
            "POST",
            "/v0/collections/docs/query",
            r#"{"vector":[1.0],"metric":"l2","top_k":2,"execution":"exact"}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["hits"][0]["id"]["type"], "string");
        assert_eq!(body["hits"][0]["id"]["value"], "alpha");
        assert_eq!(body["hits"][1]["id"]["type"], "u64");
        assert_eq!(body["explain"]["strategy"], "exact");
        assert_eq!(body["explain"]["hybrid"], false);
    }
    #[tokio::test]
    async fn hybrid_query_fuses_dense_and_lexical_results() {
        let response = request_json(app(test_state()), "POST", "/v0/collections/docs/query", r#"{"vector":[1.0],"metric":"l2","top_k":2,"execution":"exact","lexical":{"text":"book","fields":[["category"]]}}"#).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["explain"]["hybrid"], true);
        assert_eq!(body["explain"]["rrf_k"], 60);
        assert_eq!(body["hits"][0]["id"]["value"], "alpha");
        assert_eq!(body["hits"][0]["dense_rank"], 1);
        assert_eq!(body["hits"][0]["lexical_rank"], 1);
    }
    #[tokio::test]
    async fn predicate_query_maps_to_domain_predicate() {
        let response = request_json(app(test_state()), "POST", "/v0/collections/docs/query", r#"{"vector":[1.0],"metric":"l2","top_k":2,"execution":"exact","predicate":{"op":"eq","path":["category"],"value":"game"}}"#).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["hits"].as_array().expect("hits").len(), 1);
        assert_eq!(body["hits"][0]["id"]["type"], "u64");
    }
    #[test]
    fn all_predicate_shapes_deserialize() {
        let samples = [
            r#"{"op":"eq","path":["x"],"value":1}"#,
            r#"{"op":"ne","path":["x"],"value":1}"#,
            r#"{"op":"lt","path":["x"],"value":1}"#,
            r#"{"op":"lte","path":["x"],"value":1}"#,
            r#"{"op":"gt","path":["x"],"value":1}"#,
            r#"{"op":"gte","path":["x"],"value":1}"#,
            r#"{"op":"exists","path":["x"]}"#,
            r#"{"op":"in","path":["x"],"values":[1,2]}"#,
            r#"{"op":"contains","path":["x"],"value":1}"#,
            r#"{"op":"and","predicates":[{"op":"exists","path":["x"]}]}"#,
            r#"{"op":"or","predicates":[{"op":"exists","path":["x"]}]}"#,
            r#"{"op":"not","predicate":{"op":"exists","path":["x"]}}"#,
        ];
        for sample in samples {
            let dto: PredicateDto = serde_json::from_str(sample).expect("predicate dto");
            dto.into_domain().expect("domain predicate");
        }
    }
    #[tokio::test]
    async fn invalid_json_and_metric_mismatch_are_structured_4xx() {
        let invalid_json =
            request_json(app(test_state()), "POST", "/v0/collections/docs/query", "{").await;
        assert_eq!(invalid_json.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            body_json(invalid_json).await["error"]["code"],
            "invalid_json"
        );
        let mismatch = request_json(
            app(test_state()),
            "POST",
            "/v0/collections/docs/query",
            r#"{"vector":[1.0],"metric":"cosine","top_k":1}"#,
        )
        .await;
        assert_eq!(mismatch.status(), StatusCode::BAD_REQUEST);
    }
    #[tokio::test]
    async fn missing_collection_and_explicit_hnsw_are_mapped() {
        let missing = request_json(
            app(test_state()),
            "POST",
            "/v0/collections/missing/query",
            r#"{"vector":[1.0],"metric":"l2","top_k":1}"#,
        )
        .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let hnsw = request_json(
            app(test_state()),
            "POST",
            "/v0/collections/docs/query",
            r#"{"vector":[1.0],"metric":"l2","top_k":1,"execution":"hnsw"}"#,
        )
        .await;
        assert_eq!(hnsw.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    #[tokio::test]
    async fn write_api_is_visible_typed_and_recoverable() {
        let data_dir = temp_data_dir();
        let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), data_dir.clone());
        let router = app(state.clone());
        let created = request_json(
            router.clone(),
            "POST",
            "/v0/collections",
            r#"{"id":"products","dimension":2,"metric":"l2"}"#,
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let first = request_json(
            router.clone(),
            "PUT",
            "/v0/collections/products/records/42",
            r#"{"vector":[1.0,0.0],"metadata":{"kind":"string-id"}}"#,
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        let batch = request_json(router.clone(), "POST", "/v0/collections/products/records:batchUpsert", r#"{"records":[{"id":{"type":"u64","value":42},"vector":[2.0,0.0],"metadata":{"kind":"numeric-id"}},{"id":{"type":"string","value":"other"},"vector":[3.0,0.0]}]}"#).await;
        assert_eq!(batch.status(), StatusCode::OK);
        let query = request_json(
            router.clone(),
            "POST",
            "/v0/collections/products/query",
            r#"{"vector":[1.0,0.0],"metric":"l2","top_k":3,"execution":"exact"}"#,
        )
        .await;
        assert_eq!(query.status(), StatusCode::OK);
        let body = body_json(query).await;
        assert_eq!(body["hits"].as_array().expect("hits").len(), 3);
        let deleted = router
            .clone()
            .oneshot(
                Request::delete("/v0/collections/products/records/42")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(deleted.status(), StatusCode::OK);
        drop(state);
        let recovered = AppState::recover(&data_dir).expect("recover runtime");
        let recovered_query = request_json(
            app(recovered),
            "POST",
            "/v0/collections/products/query",
            r#"{"vector":[2.0,0.0],"metric":"l2","top_k":3,"execution":"exact"}"#,
        )
        .await;
        assert_eq!(recovered_query.status(), StatusCode::OK);
        std::fs::remove_dir_all(data_dir).expect("cleanup");
    }
}
