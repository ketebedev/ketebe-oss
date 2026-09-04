use crate::dto::IngestionSchemaDto;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use ketebe_core::{DistanceMetric, LexicalAnalyzerKind};
use serde::{Deserialize, Serialize};

use crate::backup::{BackupError, BackupService};
use crate::embedding::embedding_prometheus_metrics;
use crate::embedding_cache::embedding_cache_prometheus_metrics;
use crate::embedding_migration::{
    EmbeddingMigrationError, EmbeddingMigrationService, embedding_migration_prometheus_metrics,
};
use crate::http::{ApiError, admit_foreground_write};
use crate::integrity::{IntegrityError, IntegrityVerifier};
use crate::jobs::{JobId, JobService, JobServiceError, job_prometheus_metrics};
use crate::kafka_ingestion::kafka_prometheus_metrics;
use crate::management::{CollectionInfo, CollectionService, HnswState, ManagementError};
use crate::resource_scheduler::resource_scheduler_prometheus_metrics;
use crate::runtime::AppState;
use crate::semantic_chunking_service::semantic_chunking_prometheus_metrics;

pub(crate) fn routes(state: AppState) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .route("/v0/jobs/{job_id}", get(get_job))
        .route("/v0/jobs/{job_id}/cancel", post(cancel_job))
        .route("/v0/collections", get(list_collections))
        .route(
            "/v0/collections/{collection_id}",
            get(get_collection).delete(delete_collection),
        )
        .route(
            "/v0/collections/{collection_id}/integrity",
            get(get_collection_integrity),
        )
        .route(
            "/v0/collections/{collection_id}/backups",
            post(create_collection_backup),
        )
        .route(
            "/v0/collections/{collection_id}/backup-job",
            post(start_collection_backup_job),
        )
        .route("/v0/backups/{backup_id}/restore", post(restore_backup))
        .route(
            "/v0/backups/{backup_id}/restore-job",
            post(start_backup_restore_job),
        )
        .route(
            "/v0/collections/{collection_id}/embedding-migration",
            get(get_embedding_migration).post(start_embedding_migration),
        )
        .route(
            "/v0/collections/{collection_id}/embedding-migration/catch-up",
            post(catch_up_embedding_migration),
        )
        .route(
            "/v0/collections/{collection_id}/embedding-migration/catch-up-job",
            post(start_catch_up_job),
        )
        .route(
            "/v0/collections/{collection_id}/embedding-migration/activate",
            post(activate_embedding_migration),
        )
        .with_state(state)
}

async fn metrics(State(state): State<AppState>) -> String {
    format!(
        "{}{}{}{}{}{}{}{}",
        kafka_prometheus_metrics(),
        embedding_prometheus_metrics(),
        embedding_cache_prometheus_metrics(),
        embedding_migration_prometheus_metrics(),
        job_prometheus_metrics(),
        resource_scheduler_prometheus_metrics(),
        semantic_chunking_prometheus_metrics(),
        state.governance_prometheus_metrics()
    )
}

async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<crate::JobRecord>, ApiError> {
    let id = parse_job_id(job_id)?;
    JobService::new(state)
        .get(id)
        .map(Json)
        .map_err(map_job_error)
}

async fn cancel_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<crate::JobRecord>, ApiError> {
    let id = parse_job_id(job_id)?;
    JobService::new(state)
        .cancel(id)
        .map(Json)
        .map_err(map_job_error)
}

async fn list_collections(
    State(state): State<AppState>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<Json<CollectionListDto>, ApiError> {
    let authorization = state.authorization();
    let names = crate::data_plane_request::list_project_scopes(&state, &principal)
        .await
        .map_err(map_data_plane_request_error)?
        .into_iter()
        .map(|(name, scope)| (scope.collection_id().clone(), name))
        .collect::<std::collections::BTreeMap<_, _>>();
    let collections = CollectionService::new(state)
        .list()
        .await
        .map_err(map_management_error)?
        .into_iter()
        .filter_map(|collection| {
            let name = names.get(&collection.id)?;
            if !authorization.can_discover_collection(&principal, name) {
                return None;
            }
            let mut dto = CollectionManagementDto::from(collection);
            dto.id = name.clone();
            Some(dto)
        })
        .collect();
    Ok(Json(CollectionListDto { collections }))
}

async fn get_collection(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<Json<CollectionManagementDto>, ApiError> {
    let visible_name = collection_id;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &visible_name)
            .await
            .map_err(map_data_plane_request_error)?;
    let id = scope.collection_id().clone();
    let info = CollectionService::new(state)
        .get(&id)
        .await
        .map_err(map_management_error)?;
    let mut dto = CollectionManagementDto::from(info);
    dto.id = visible_name;
    Ok(Json(dto))
}

async fn start_collection_backup_job(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<(StatusCode, Json<crate::JobRecord>), ApiError> {
    let visible_name = collection_id;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &visible_name)
            .await
            .map_err(map_data_plane_request_error)?;
    let id = scope.collection_id().clone();
    let job = JobService::new(state)
        .submit_backup_create(id)
        .map_err(map_job_error)?;
    Ok((StatusCode::ACCEPTED, Json(job)))
}

async fn start_backup_restore_job(
    State(state): State<AppState>,
    Path(backup_id): Path<String>,
) -> Result<(StatusCode, Json<crate::JobRecord>), ApiError> {
    let job = JobService::new(state)
        .submit_backup_restore(backup_id)
        .map_err(map_job_error)?;
    Ok((StatusCode::ACCEPTED, Json(job)))
}

async fn restore_backup(
    State(state): State<AppState>,
    Path(backup_id): Path<String>,
) -> Result<(StatusCode, Json<crate::RestoreResult>), ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let result = BackupService::new(state)
        .restore(&backup_id)
        .await
        .map_err(map_backup_error)?;
    Ok((StatusCode::CREATED, Json(result)))
}

async fn create_collection_backup(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<(StatusCode, Json<crate::BackupManifest>), ApiError> {
    let visible_name = collection_id;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &visible_name)
            .await
            .map_err(map_data_plane_request_error)?;
    let id = scope.collection_id().clone();
    let manifest = BackupService::new(state)
        .create(&id)
        .await
        .map_err(map_backup_error)?;
    Ok((StatusCode::CREATED, Json(manifest)))
}

fn map_backup_error(error: BackupError) -> ApiError {
    match error {
        BackupError::CollectionNotFound(_) => ApiError::new(
            StatusCode::NOT_FOUND,
            "collection_not_found",
            error.to_string(),
        ),
        BackupError::AlreadyExists(_) => {
            ApiError::new(StatusCode::CONFLICT, "backup_exists", error.to_string())
        }
        BackupError::NotFound(_) => {
            ApiError::new(StatusCode::NOT_FOUND, "backup_not_found", error.to_string())
        }
        BackupError::CorruptSource(_) | BackupError::ChecksumMismatch(_) => ApiError::new(
            StatusCode::CONFLICT,
            "backup_integrity_failed",
            error.to_string(),
        ),
        BackupError::TargetNotEmpty(_) => ApiError::new(
            StatusCode::CONFLICT,
            "restore_target_not_empty",
            error.to_string(),
        ),
        BackupError::UnsupportedManifestVersion(_) | BackupError::InvalidManifest(_) => {
            ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_backup",
                error.to_string(),
            )
        }
        BackupError::Runtime(_)
        | BackupError::Io(_)
        | BackupError::Json(_)
        | BackupError::Integrity(_) => ApiError::internal(error.to_string()),
    }
}

async fn get_collection_integrity(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<Json<crate::IntegrityReport>, ApiError> {
    let visible_name = collection_id;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &visible_name)
            .await
            .map_err(map_data_plane_request_error)?;
    let id = scope.collection_id().clone();
    IntegrityVerifier::new(state.data_dir.as_ref().clone())
        .verify_collection(&id)
        .map(Json)
        .map_err(map_integrity_error)
}

fn map_integrity_error(error: IntegrityError) -> ApiError {
    match error {
        IntegrityError::CollectionNotFound(_) => ApiError::new(
            StatusCode::NOT_FOUND,
            "collection_not_found",
            error.to_string(),
        ),
        IntegrityError::Scope(_) | IntegrityError::Io(_) => ApiError::internal(error.to_string()),
    }
}

async fn delete_collection(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<StatusCode, ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let visible_name = collection_id;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &visible_name)
            .await
            .map_err(map_data_plane_request_error)?;
    let id = scope.collection_id().clone();
    CollectionService::new(state.clone())
        .delete(&id)
        .await
        .map_err(map_management_error)?;
    state
        .authorization()
        .remove_collection(&principal, &visible_name)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    crate::data_plane_request::remove_scope(&state, &principal, &visible_name, &id)
        .map_err(map_data_plane_request_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct StartEmbeddingMigrationRequest {
    target_profile: String,
}

async fn start_embedding_migration(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    Extension(principal): Extension<crate::Principal>,
    Json(request): Json<StartEmbeddingMigrationRequest>,
) -> Result<(StatusCode, Json<crate::EmbeddingMigrationState>), ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let visible_name = collection_id;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &visible_name)
            .await
            .map_err(map_data_plane_request_error)?;
    let id = scope.collection_id().clone();
    let migration = EmbeddingMigrationService::new(state)
        .start(&id, request.target_profile)
        .await
        .map_err(map_migration_error)?;
    Ok((StatusCode::ACCEPTED, Json(migration)))
}

async fn start_catch_up_job(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<(StatusCode, Json<crate::JobRecord>), ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let visible_name = collection_id;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &visible_name)
            .await
            .map_err(map_data_plane_request_error)?;
    let id = scope.collection_id().clone();
    let job = JobService::new(state)
        .submit_embedding_migration_catch_up(id)
        .map_err(map_job_error)?;
    Ok((StatusCode::ACCEPTED, Json(job)))
}

async fn get_embedding_migration(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<Json<crate::EmbeddingMigrationState>, ApiError> {
    let visible_name = collection_id;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &visible_name)
            .await
            .map_err(map_data_plane_request_error)?;
    let id = scope.collection_id().clone();
    let migration = EmbeddingMigrationService::new(state)
        .status(&id)
        .await
        .map_err(map_migration_error)?;
    Ok(Json(migration))
}

async fn catch_up_embedding_migration(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<Json<crate::EmbeddingMigrationState>, ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let visible_name = collection_id;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &visible_name)
            .await
            .map_err(map_data_plane_request_error)?;
    let id = scope.collection_id().clone();
    let migration = EmbeddingMigrationService::new(state)
        .catch_up(&id)
        .await
        .map_err(map_migration_error)?;
    Ok(Json(migration))
}

async fn activate_embedding_migration(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<Json<crate::EmbeddingMigrationState>, ApiError> {
    let _write_guard = admit_foreground_write(&state)?;
    let visible_name = collection_id;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &visible_name)
            .await
            .map_err(map_data_plane_request_error)?;
    let id = scope.collection_id().clone();
    let migration = EmbeddingMigrationService::new(state)
        .activate(&id)
        .await
        .map_err(map_migration_error)?;
    Ok(Json(migration))
}

fn map_migration_error(error: EmbeddingMigrationError) -> ApiError {
    match error {
        EmbeddingMigrationError::MigrationNotFound => ApiError::new(
            StatusCode::NOT_FOUND,
            "embedding_migration_not_found",
            error.to_string(),
        ),
        EmbeddingMigrationError::CollectionNotFound(_) => ApiError::new(
            StatusCode::NOT_FOUND,
            "collection_not_found",
            error.to_string(),
        ),
        EmbeddingMigrationError::MigrationAlreadyExists(_)
        | EmbeddingMigrationError::MigrationNotReady(_)
        | EmbeddingMigrationError::NoIngestionSchema
        | EmbeddingMigrationError::TargetAlreadyActive(_)
        | EmbeddingMigrationError::ProviderChanged
        | EmbeddingMigrationError::SourceChanged => ApiError::new(
            StatusCode::CONFLICT,
            "embedding_migration_conflict",
            error.to_string(),
        ),
        EmbeddingMigrationError::ProviderProfileUnavailable(_)
        | EmbeddingMigrationError::DimensionMismatch { .. } => ApiError::new(
            StatusCode::BAD_REQUEST,
            "embedding_migration_invalid_target",
            error.to_string(),
        ),
        EmbeddingMigrationError::MissingSourceText(_)
        | EmbeddingMigrationError::Provider(_)
        | EmbeddingMigrationError::Corrupt(_)
        | EmbeddingMigrationError::Management(_)
        | EmbeddingMigrationError::Write(_)
        | EmbeddingMigrationError::Io(_)
        | EmbeddingMigrationError::Json(_) => ApiError::internal(error.to_string()),
    }
}

fn parse_job_id(value: String) -> Result<JobId, ApiError> {
    let raw = value.parse::<u64>().map_err(|_| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "job id must be a positive integer",
        )
    })?;
    JobId::new(raw).map_err(|error| {
        ApiError::new(StatusCode::BAD_REQUEST, "invalid_job_id", error.to_string())
    })
}

fn map_job_error(error: JobServiceError) -> ApiError {
    match error {
        JobServiceError::JobNotFound(_) => {
            ApiError::new(StatusCode::NOT_FOUND, "job_not_found", error.to_string())
        }
        JobServiceError::InvalidJobId => {
            ApiError::new(StatusCode::BAD_REQUEST, "invalid_job_id", error.to_string())
        }
        JobServiceError::RuntimeDraining => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "draining",
            error.to_string(),
        ),
        JobServiceError::Io(_) | JobServiceError::Json(_) | JobServiceError::Corrupt(_) => {
            ApiError::internal(error.to_string())
        }
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

fn map_management_error(error: ManagementError) -> ApiError {
    match error {
        ManagementError::CollectionNotFound(id) => ApiError::new(
            StatusCode::NOT_FOUND,
            "collection_not_found",
            format!("collection '{}' was not found", id.as_str()),
        ),
        ManagementError::CollectionNotManageable => {
            ApiError::internal("collection runtime is not manageable")
        }
        ManagementError::Io(error) => {
            ApiError::internal(format!("collection management I/O failure: {error}"))
        }
        ManagementError::Segment(error) => {
            ApiError::internal(format!("collection management segment failure: {error}"))
        }
        ManagementError::Scope(message) => ApiError::internal(message),
    }
}

#[derive(Debug, Serialize)]
struct CollectionListDto {
    collections: Vec<CollectionManagementDto>,
}

#[derive(Debug, Serialize)]
struct CollectionManagementDto {
    id: String,
    dimension: usize,
    metric: &'static str,
    stats: CollectionStatsDto,
    index: IndexStateDto,
    lexical: LexicalSchemaDto,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingestion: Option<IngestionSchemaDto>,
}

#[derive(Debug, Serialize)]
struct LexicalSchemaDto {
    fields: Vec<Vec<String>>,
    analyzer: LexicalAnalyzerStateDto,
    state: String,
}

#[derive(Debug, Serialize)]
struct LexicalAnalyzerStateDto {
    kind: &'static str,
    lowercase: bool,
}

#[derive(Debug, Serialize)]
struct CollectionStatsDto {
    live_records: usize,
    tombstones: usize,
    immutable_segments: usize,
    mutable_mutations: usize,
    checkpoint_sequence: Option<u64>,
    next_sequence: u64,
}

#[derive(Debug, Serialize)]
struct IndexStateDto {
    state: &'static str,
    config: Option<HnswConfigDto>,
}

#[derive(Debug, Serialize)]
struct HnswConfigDto {
    m: usize,
    ef_construction: usize,
    ef_search: usize,
}

impl From<CollectionInfo> for CollectionManagementDto {
    fn from(info: CollectionInfo) -> Self {
        let hnsw_state = match info.hnsw_state {
            HnswState::Ready => "ready",
            HnswState::Unavailable => "unavailable",
        };
        let hnsw_config = info.hnsw_config.map(|config| HnswConfigDto {
            m: config.m,
            ef_construction: config.ef_construction,
            ef_search: config.ef_search,
        });
        Self {
            id: info.id.as_str().to_string(),
            dimension: info.dimension,
            metric: metric_name(info.metric),
            stats: CollectionStatsDto {
                live_records: info.live_records,
                tombstones: info.tombstones,
                immutable_segments: info.immutable_segments,
                mutable_mutations: info.mutable_mutations,
                checkpoint_sequence: info.checkpoint_sequence,
                next_sequence: info.next_sequence,
            },
            index: IndexStateDto {
                state: hnsw_state,
                config: hnsw_config,
            },
            ingestion: info.ingestion.as_ref().map(IngestionSchemaDto::from),
            lexical: LexicalSchemaDto {
                fields: info
                    .lexical_fields
                    .iter()
                    .map(|path| path.segments().to_vec())
                    .collect(),
                analyzer: LexicalAnalyzerStateDto {
                    kind: match info.lexical_analyzer.kind() {
                        LexicalAnalyzerKind::Standard => "standard",
                    },
                    lowercase: info.lexical_analyzer.lowercase(),
                },
                state: format!("{:?}", info.lexical_state).to_lowercase(),
            },
        }
    }
}

fn metric_name(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::Dot => "dot",
        DistanceMetric::L2 => "l2",
    }
}
