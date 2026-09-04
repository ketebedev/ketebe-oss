use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post},
};
use ketebe_core::CollectionId;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::task::JoinHandle;

use crate::{
    AppState, CollectionService, KafkaIngestionConfig, KafkaIngestionError, KafkaPoisonPolicy,
    KafkaSecurityConfig, SecretRef, SecretResolver, SystemSecretResolver, kafka_prometheus_metrics,
    run_kafka_ingestion,
};

static MANAGED_STREAM: OnceLock<Mutex<Option<ManagedStream>>> = OnceLock::new();

fn managed_stream() -> &'static Mutex<Option<ManagedStream>> {
    MANAGED_STREAM.get_or_init(|| Mutex::new(None))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StreamState {
    Running,
    Paused,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
struct StreamView {
    id: String,
    collection: String,
    topic: String,
    group_id: String,
    state: StreamState,
    consumer_lag_records: Option<u64>,
    failure_code: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateStreamRequest {
    brokers: String,
    topic: String,
    group_id: String,
    #[serde(default = "default_batch_max_records")]
    batch_max_records: usize,
    #[serde(default = "default_batch_linger_ms")]
    batch_linger_ms: u64,
    #[serde(default)]
    dlq_topic: Option<String>,
    #[serde(default)]
    security_protocol: Option<String>,
    #[serde(default)]
    sasl_mechanism: Option<String>,
    #[serde(default)]
    sasl_username_ref: Option<String>,
    #[serde(default)]
    sasl_password_ref: Option<String>,
}

const fn default_batch_max_records() -> usize {
    128
}

const fn default_batch_linger_ms() -> u64 {
    50
}

struct ManagedStream {
    id: String,
    collection: String,
    config: KafkaIngestionConfig,
    state: Arc<Mutex<ManagedState>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone, Debug)]
struct ManagedState {
    state: StreamState,
    failure_code: Option<String>,
}

pub(crate) fn routes(state: AppState) -> Router {
    Router::new()
        .route(
            "/v0/collections/{collection}/stream-ingestions",
            get(list_streams).post(create_stream),
        )
        .route(
            "/v0/collections/{collection}/stream-ingestions/{stream_id}",
            get(get_stream),
        )
        .route(
            "/v0/collections/{collection}/stream-ingestions/{stream_id}/pause",
            post(pause_stream),
        )
        .route(
            "/v0/collections/{collection}/stream-ingestions/{stream_id}/resume",
            post(resume_stream),
        )
        .with_state(state)
}

async fn list_streams(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<Json<Vec<StreamView>>, (StatusCode, Json<ErrorEnvelope>)> {
    let collection_id = resolve_collection_id(&state, &principal, &collection).await?;
    let guard = managed_stream().lock().map_err(lock_error)?;
    let streams = guard
        .as_ref()
        .filter(|stream| stream.config.collection_id == collection_id)
        .map(stream_view)
        .into_iter()
        .collect();
    Ok(Json(streams))
}

async fn create_stream(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Extension(principal): Extension<crate::Principal>,
    Json(request): Json<CreateStreamRequest>,
) -> Result<(StatusCode, Json<StreamView>), (StatusCode, Json<ErrorEnvelope>)> {
    let collection_id = resolve_collection_id(&state, &principal, &collection).await?;
    CollectionService::new(state.clone())
        .get(&collection_id)
        .await
        .map_err(|_| not_found())?;

    let mut guard = managed_stream().lock().map_err(lock_error)?;
    if guard.is_some() {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorEnvelope::new(
                "stream_ingestion_conflict",
                "stream ingestion v0 supports one managed ingestion at a time",
            )),
        ));
    }

    let mut config = KafkaIngestionConfig::new(
        request.brokers,
        request.topic,
        request.group_id,
        collection_id,
        request.batch_max_records,
        request.batch_linger_ms,
    )
    .map_err(config_error)?;

    if let Some(dlq_topic) = request.dlq_topic {
        if dlq_topic.trim().is_empty() {
            return Err(bad_request(
                "invalid_dlq_topic",
                "dlq_topic must not be empty",
            ));
        }
        config.poison_policy = KafkaPoisonPolicy::Dlq { topic: dlq_topic };
    }

    if request.security_protocol.is_some()
        || request.sasl_mechanism.is_some()
        || request.sasl_username_ref.is_some()
        || request.sasl_password_ref.is_some()
    {
        let resolver = SystemSecretResolver;
        let username = resolve_optional_secret(&resolver, request.sasl_username_ref.as_deref())?;
        let password = resolve_optional_secret(&resolver, request.sasl_password_ref.as_deref())?;
        config = config.with_security(KafkaSecurityConfig {
            security_protocol: request
                .security_protocol
                .unwrap_or_else(|| "SASL_SSL".to_string()),
            sasl_mechanism: request.sasl_mechanism,
            sasl_username: username,
            sasl_password: password,
        });
    }

    let id = format!("stream-{collection}");
    let lifecycle_state = Arc::new(Mutex::new(ManagedState {
        state: StreamState::Running,
        failure_code: None,
    }));
    let task = spawn_stream(state, config.clone(), lifecycle_state.clone());
    let stream = ManagedStream {
        id,
        collection,
        config,
        state: lifecycle_state,
        task: Some(task),
    };
    let view = stream_view(&stream);
    *guard = Some(stream);
    Ok((StatusCode::CREATED, Json(view)))
}

async fn get_stream(
    State(state): State<AppState>,
    Path((collection, stream_id)): Path<(String, String)>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<Json<StreamView>, (StatusCode, Json<ErrorEnvelope>)> {
    let collection_id = resolve_collection_id(&state, &principal, &collection).await?;
    let guard = managed_stream().lock().map_err(lock_error)?;
    let stream = matching_stream(guard.as_ref(), &collection_id, &stream_id)?;
    Ok(Json(stream_view(stream)))
}

async fn pause_stream(
    State(state): State<AppState>,
    Path((collection, stream_id)): Path<(String, String)>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<Json<StreamView>, (StatusCode, Json<ErrorEnvelope>)> {
    let collection_id = resolve_collection_id(&state, &principal, &collection).await?;
    let mut guard = managed_stream().lock().map_err(lock_error)?;
    let stream = matching_stream_mut(guard.as_mut(), &collection_id, &stream_id)?;
    let mut lifecycle = stream.state.lock().map_err(lock_error)?;
    if lifecycle.state != StreamState::Running {
        return Err(conflict(
            "stream_not_running",
            "stream ingestion is not running",
        ));
    }
    if let Some(task) = stream.task.take() {
        task.abort();
    }
    lifecycle.state = StreamState::Paused;
    lifecycle.failure_code = None;
    drop(lifecycle);
    Ok(Json(stream_view(stream)))
}

async fn resume_stream(
    State(state): State<AppState>,
    Path((collection, stream_id)): Path<(String, String)>,
    Extension(principal): Extension<crate::Principal>,
) -> Result<Json<StreamView>, (StatusCode, Json<ErrorEnvelope>)> {
    let collection_id = resolve_collection_id(&state, &principal, &collection).await?;
    let mut guard = managed_stream().lock().map_err(lock_error)?;
    let stream = matching_stream_mut(guard.as_mut(), &collection_id, &stream_id)?;
    {
        let mut lifecycle = stream.state.lock().map_err(lock_error)?;
        if lifecycle.state != StreamState::Paused && lifecycle.state != StreamState::Failed {
            return Err(conflict(
                "stream_not_paused",
                "stream ingestion is not paused or failed",
            ));
        }
        lifecycle.state = StreamState::Running;
        lifecycle.failure_code = None;
    }
    stream.task = Some(spawn_stream(
        state,
        stream.config.clone(),
        stream.state.clone(),
    ));
    Ok(Json(stream_view(stream)))
}

async fn resolve_collection_id(
    state: &AppState,
    principal: &crate::Principal,
    collection: &str,
) -> Result<CollectionId, (StatusCode, Json<ErrorEnvelope>)> {
    crate::data_plane_request::resolve_existing_scope(state, principal, collection)
        .await
        .map(|scope| scope.collection_id().clone())
        .map_err(|_| not_found())
}

fn spawn_stream(
    state: AppState,
    config: KafkaIngestionConfig,
    lifecycle: Arc<Mutex<ManagedState>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(error) = run_kafka_ingestion(state, config).await {
            let code = stable_failure_code(&error).to_string();
            if let Ok(mut state) = lifecycle.lock() {
                state.state = StreamState::Failed;
                state.failure_code = Some(code);
            }
        }
    })
}

fn matching_stream<'a>(
    stream: Option<&'a ManagedStream>,
    collection_id: &CollectionId,
    stream_id: &str,
) -> Result<&'a ManagedStream, (StatusCode, Json<ErrorEnvelope>)> {
    stream
        .filter(|stream| stream.id == stream_id && stream.config.collection_id == *collection_id)
        .ok_or_else(not_found)
}

fn matching_stream_mut<'a>(
    stream: Option<&'a mut ManagedStream>,
    collection_id: &CollectionId,
    stream_id: &str,
) -> Result<&'a mut ManagedStream, (StatusCode, Json<ErrorEnvelope>)> {
    stream
        .filter(|stream| stream.id == stream_id && stream.config.collection_id == *collection_id)
        .ok_or_else(not_found)
}

fn stream_view(stream: &ManagedStream) -> StreamView {
    let lifecycle = stream.state.lock().ok();
    StreamView {
        id: stream.id.clone(),
        collection: stream.collection.clone(),
        topic: stream.config.topic.clone(),
        group_id: stream.config.group_id.clone(),
        state: lifecycle
            .as_ref()
            .map_or(StreamState::Failed, |value| value.state),
        consumer_lag_records: prometheus_value("ketebe_kafka_consumer_lag_records"),
        failure_code: lifecycle.and_then(|value| value.failure_code.clone()),
    }
}

fn prometheus_value(name: &str) -> Option<u64> {
    kafka_prometheus_metrics().lines().find_map(|line| {
        let (metric, value) = line.split_once(' ')?;
        (metric == name)
            .then(|| value.parse::<u64>().ok())
            .flatten()
    })
}

fn resolve_optional_secret(
    resolver: &SystemSecretResolver,
    reference: Option<&str>,
) -> Result<Option<String>, (StatusCode, Json<ErrorEnvelope>)> {
    let Some(reference) = reference else {
        return Ok(None);
    };
    let reference = SecretRef::new(reference.to_string())
        .map_err(|_| bad_request("invalid_secret_ref", "invalid secret reference"))?;
    resolver
        .resolve(&reference)
        .map(|value| Some(value.expose_secret().to_string()))
        .map_err(|_| bad_request("secret_resolution_failed", "secret could not be resolved"))
}

fn stable_failure_code(error: &KafkaIngestionError) -> &'static str {
    match error {
        KafkaIngestionError::InvalidConfig(_) => "invalid_config",
        KafkaIngestionError::Kafka(_) => "kafka_error",
        KafkaIngestionError::Dlq(_) => "dlq_error",
        KafkaIngestionError::Decode(_)
        | KafkaIngestionError::InvalidEnvelope(_)
        | KafkaIngestionError::MissingPayload
        | KafkaIngestionError::UnsupportedEnvelopeVersion(_)
        | KafkaIngestionError::EmptyBatch
        | KafkaIngestionError::MixedPartitions
        | KafkaIngestionError::NonMonotonicOffsets => "source_record_error",
        KafkaIngestionError::Write(_)
        | KafkaIngestionError::Embedding(_)
        | KafkaIngestionError::Chunking(_)
        | KafkaIngestionError::TokenChunking(_)
        | KafkaIngestionError::SemanticChunking(_) => "ingestion_apply_error",
    }
}

fn config_error(error: KafkaIngestionError) -> (StatusCode, Json<ErrorEnvelope>) {
    bad_request("invalid_stream_ingestion", &error.to_string())
}

fn bad_request(code: &str, message: &str) -> (StatusCode, Json<ErrorEnvelope>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorEnvelope::new(code, message)),
    )
}

fn conflict(code: &str, message: &str) -> (StatusCode, Json<ErrorEnvelope>) {
    (
        StatusCode::CONFLICT,
        Json(ErrorEnvelope::new(code, message)),
    )
}

fn not_found() -> (StatusCode, Json<ErrorEnvelope>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorEnvelope::new(
            "stream_ingestion_not_found",
            "stream ingestion was not found",
        )),
    )
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> (StatusCode, Json<ErrorEnvelope>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorEnvelope::new(
            "stream_ingestion_unavailable",
            "stream ingestion service unavailable",
        )),
    )
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

impl ErrorEnvelope {
    fn new(code: &str, message: &str) -> Self {
        Self {
            error: ErrorBody {
                code: code.to_string(),
                message: message.to_string(),
            },
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    code: String,
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_view_does_not_serialize_brokers_or_secret_fields() {
        let view = StreamView {
            id: "stream-docs".to_string(),
            collection: "docs".to_string(),
            topic: "documents".to_string(),
            group_id: "ketebe-docs".to_string(),
            state: StreamState::Running,
            consumer_lag_records: Some(2),
            failure_code: None,
        };
        let json = serde_json::to_string(&view).unwrap();
        assert!(!json.contains("brokers"));
        assert!(!json.contains("password"));
        assert!(!json.contains("username_ref"));
        assert!(!json.contains("password_ref"));
    }

    #[test]
    fn failure_codes_are_stable_and_secret_free() {
        let error = KafkaIngestionError::InvalidConfig("secret material".to_string());
        assert_eq!(stable_failure_code(&error), "invalid_config");
        assert!(!stable_failure_code(&error).contains("secret"));
    }
}
