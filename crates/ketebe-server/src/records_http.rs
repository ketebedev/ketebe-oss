use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use ketebe_core::{Metadata, Record, RecordId};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

use crate::dto::{RecordIdDto, metadata_map_to_json};
use crate::runtime::AppState;

pub(crate) fn routes(state: AppState) -> Router {
    Router::new()
        .route(
            "/v0/collections/{collection_id}/records:fetch",
            post(fetch_records),
        )
        .with_state(state)
}

#[derive(Debug, Deserialize)]
pub(crate) struct FetchRecordsBody {
    pub(crate) ids: Vec<RecordIdDto>,
    #[serde(default)]
    pub(crate) fields: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct FetchRecordsResponse {
    pub(crate) records: Vec<FetchedRecordDto>,
    pub(crate) missing: Vec<RecordIdDto>,
}

#[derive(Debug, Serialize)]
pub(crate) struct FetchedRecordDto {
    pub(crate) id: RecordIdDto,
    pub(crate) sequence_number: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) vector: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<Value>,
}

async fn fetch_records(
    State(state): State<AppState>,
    Path(collection_id): Path<String>,
    principal: Option<Extension<crate::Principal>>,
    Json(body): Json<FetchRecordsBody>,
) -> Result<Json<FetchRecordsResponse>, crate::http::ApiError> {
    let principal = request_principal(&state, principal)?;
    let scope =
        crate::data_plane_request::resolve_existing_scope(&state, &principal, &collection_id)
            .await
            .map_err(map_scope_error)?;

    let ids = body
        .ids
        .into_iter()
        .map(RecordIdDto::into_domain)
        .collect::<Result<Vec<_>, _>>()?;
    let projection = Projection::parse(&body.fields)?;

    let catalog = state.catalog.read().await;
    let runtime = catalog
        .collections
        .get(scope.collection_id())
        .ok_or_else(|| {
            crate::http::ApiError::new(
                StatusCode::NOT_FOUND,
                "collection_not_found",
                format!("collection '{}' was not found", collection_id),
            )
        })?;
    let segments = runtime.query_segments().map_err(|error| {
        crate::http::ApiError::internal(format!("failed to build record read view: {error}"))
    })?;

    let mut records = Vec::new();
    let mut missing = Vec::new();
    for id in ids {
        match latest_record(&segments, &id) {
            Some(record) => records.push(project_record(record, &projection)?),
            None => missing.push(RecordIdDto::from(&id)),
        }
    }

    Ok(Json(FetchRecordsResponse { records, missing }))
}

fn latest_record<'a>(segments: &'a [ketebe_storage::Segment], id: &RecordId) -> Option<&'a Record> {
    for segment in segments.iter().rev() {
        if segment
            .tombstones()
            .iter()
            .any(|tombstone| tombstone.record_id() == id)
        {
            return None;
        }
        if let Some(record) = segment.records().iter().find(|record| record.id() == id) {
            return Some(record);
        }
    }
    None
}

#[derive(Debug)]
struct Projection {
    vector: bool,
    full_metadata: bool,
    metadata_keys: BTreeSet<String>,
}

impl Projection {
    fn parse(fields: &[String]) -> Result<Self, crate::http::ApiError> {
        if fields.is_empty() {
            return Ok(Self {
                vector: true,
                full_metadata: true,
                metadata_keys: BTreeSet::new(),
            });
        }
        let mut projection = Self {
            vector: false,
            full_metadata: false,
            metadata_keys: BTreeSet::new(),
        };
        for field in fields {
            match field.as_str() {
                "vector" => projection.vector = true,
                "metadata" => projection.full_metadata = true,
                value if value.starts_with("metadata.") && value.len() > "metadata.".len() => {
                    projection
                        .metadata_keys
                        .insert(value["metadata.".len()..].to_string());
                }
                _ => {
                    return Err(crate::http::ApiError::new(
                        StatusCode::BAD_REQUEST,
                        "invalid_record_field",
                        format!("unsupported record field projection '{field}'"),
                    ));
                }
            }
        }
        Ok(projection)
    }
}

fn project_record(
    record: &Record,
    projection: &Projection,
) -> Result<FetchedRecordDto, crate::http::ApiError> {
    let metadata = if projection.full_metadata {
        Some(metadata_map_to_json(record.metadata())?)
    } else if projection.metadata_keys.is_empty() {
        None
    } else {
        Some(project_metadata(
            record.metadata(),
            &projection.metadata_keys,
        )?)
    };
    Ok(FetchedRecordDto {
        id: RecordIdDto::from(record.id()),
        sequence_number: record.sequence_number().get(),
        vector: projection
            .vector
            .then(|| record.vector().as_slice().to_vec()),
        metadata,
    })
}

fn project_metadata(
    metadata: &Metadata,
    keys: &BTreeSet<String>,
) -> Result<Value, crate::http::ApiError> {
    let full = metadata_map_to_json(metadata)?;
    let object = full.as_object().ok_or_else(|| {
        crate::http::ApiError::internal("record metadata projection was not an object")
    })?;
    let mut selected = Map::new();
    for key in keys {
        if let Some(value) = object.get(key) {
            selected.insert(key.clone(), value.clone());
        }
    }
    Ok(Value::Object(selected))
}

fn request_principal(
    state: &AppState,
    principal: Option<Extension<crate::Principal>>,
) -> Result<crate::Principal, crate::http::ApiError> {
    if let Some(Extension(principal)) = principal {
        return Ok(principal);
    }
    if state.authorization().mode() == crate::AuthorizationMode::Development {
        return Ok(crate::Principal::development());
    }
    Err(crate::http::ApiError::new(
        StatusCode::UNAUTHORIZED,
        "unauthenticated",
        "request principal is missing",
    ))
}

fn map_scope_error(
    error: crate::data_plane_request::DataPlaneRequestError,
) -> crate::http::ApiError {
    match error {
        crate::data_plane_request::DataPlaneRequestError::CollectionNotFound => {
            crate::http::ApiError::new(
                StatusCode::NOT_FOUND,
                "collection_not_found",
                "collection was not found",
            )
        }
        crate::data_plane_request::DataPlaneRequestError::InvalidCollectionName(message) => {
            crate::http::ApiError::new(StatusCode::BAD_REQUEST, "invalid_collection_name", message)
        }
        other => crate::http::ApiError::internal(other.to_string()),
    }
}
