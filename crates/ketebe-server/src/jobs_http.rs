use axum::extract::{Extension, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::http::ApiError;
use crate::{AppState, JobRecord, Principal};

pub(crate) fn routes(state: AppState) -> Router {
    Router::new()
        .route("/v0/jobs", get(list_jobs))
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct JobListResponse {
    jobs: Vec<JobRecord>,
}

async fn list_jobs(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<JobListResponse>, ApiError> {
    crate::job_access::list_jobs_for_principal(&state, &principal)
        .map(|jobs| Json(JobListResponse { jobs }))
        .map_err(|error| ApiError::internal(error.to_string()))
}
