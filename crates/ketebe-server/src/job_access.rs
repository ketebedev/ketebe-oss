use crate::{AppState, BackupManifest, JobKind, JobRecord, JobService, JobServiceError, Principal};
use ketebe_core::{CollectionId, ProjectId};
use std::fs;

#[derive(Debug)]
pub(crate) enum JobAccessError {
    Job(JobServiceError),
    InvalidCollection(String),
    Namespace(String),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for JobAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Job(error) => write!(f, "job access failed: {error}"),
            Self::InvalidCollection(error) => {
                write!(f, "job collection identity is invalid: {error}")
            }
            Self::Namespace(error) => write!(f, "job project scope resolution failed: {error}"),
            Self::Io(error) => write!(f, "job access I/O failure: {error}"),
            Self::Json(error) => write!(f, "job access JSON failure: {error}"),
        }
    }
}

impl From<JobServiceError> for JobAccessError {
    fn from(value: JobServiceError) -> Self {
        Self::Job(value)
    }
}

impl From<std::io::Error> for JobAccessError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for JobAccessError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

pub(crate) fn principal_project(principal: &Principal) -> String {
    principal
        .project_id()
        .map(str::to_string)
        .unwrap_or_else(|| ProjectId::default_project().as_str().to_string())
}

pub(crate) fn can_access_job(
    state: &AppState,
    principal: &Principal,
    job: &JobRecord,
) -> Result<bool, JobAccessError> {
    Ok(job_project(state, job)? == principal_project(principal))
}

pub(crate) fn list_jobs_for_principal(
    state: &AppState,
    principal: &Principal,
) -> Result<Vec<JobRecord>, JobAccessError> {
    let directory = state.data_dir.join("jobs");
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let service = JobService::new(state.clone());
    let mut jobs = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(raw) = name
            .strip_prefix("job-")
            .and_then(|value| value.strip_suffix(".json"))
        else {
            continue;
        };
        let Ok(raw_id) = raw.parse::<u64>() else {
            continue;
        };
        let id = crate::JobId::new(raw_id)?;
        let job = service.get(id)?;
        if can_access_job(state, principal, &job)? {
            jobs.push(job);
        }
    }
    jobs.sort_by_key(|job| job.id.get());
    Ok(jobs)
}

fn job_project(state: &AppState, job: &JobRecord) -> Result<String, JobAccessError> {
    match &job.kind {
        JobKind::EmbeddingMigrationCatchUp { collection_id }
        | JobKind::BackupCreate { collection_id } => project_for_collection(state, collection_id),
        JobKind::BackupRestore { backup_id } => {
            let manifest_path = state
                .data_dir
                .join("backups")
                .join(backup_id)
                .join("manifest.json");
            let manifest: BackupManifest = serde_json::from_slice(&fs::read(manifest_path)?)?;
            project_for_collection(state, &manifest.collection_id)
        }
    }
}

fn project_for_collection(state: &AppState, collection_id: &str) -> Result<String, JobAccessError> {
    let id = CollectionId::new(collection_id.to_string())
        .map_err(|error| JobAccessError::InvalidCollection(error.to_string()))?;
    let catalog = crate::CollectionNamespaceCatalog::open(state.data_dir.as_ref())
        .map_err(|error| JobAccessError::Namespace(error.to_string()))?;
    catalog
        .find_scope_by_collection_id(&id)
        .map_err(|error| JobAccessError::Namespace(error.to_string()))
        .map(|scope| {
            scope
                .map(|value| value.project_id().as_str().to_string())
                .unwrap_or_else(|| ProjectId::default_project().as_str().to_string())
        })
}
