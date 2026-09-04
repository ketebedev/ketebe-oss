use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use ketebe_core::{
    ChunkingPolicy, CollectionId, CollectionIngestionConfig, DistanceMetric, Metadata, RecordId,
};
use ketebe_server::{
    AppState, ChunkedDocument, ChunkingConfig, ChunkingService, DeterministicEmbeddingProvider,
    EmbeddingMigrationService, EmbeddingMigrationStatus, EmbeddingProviderRegistry, JobFailure,
    JobId, JobKind, JobProgress, JobRecord, JobService, JobState, RuntimeCatalog, WriteService,
    app,
};
use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-jobs-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

async fn install_profiles(state: &AppState) {
    let mut registry = EmbeddingProviderRegistry::new();
    registry
        .register(
            "model-v1",
            Arc::new(DeterministicEmbeddingProvider::new("docs", "v1").unwrap()),
        )
        .unwrap();
    registry
        .register(
            "model-v2",
            Arc::new(DeterministicEmbeddingProvider::new("docs", "v2").unwrap()),
        )
        .unwrap();
    registry.set_default("model-v1").unwrap();
    state.set_embedding_provider_registry(registry).await;
}

async fn prepare_ready_migration(state: &AppState, id: &CollectionId) -> EmbeddingMigrationService {
    install_profiles(state).await;
    let ingestion =
        CollectionIngestionConfig::new("model-v1", Some(ChunkingPolicy::new(5, 2).unwrap()), true)
            .unwrap();
    WriteService::new(state.clone())
        .create_collection_with_schema(
            id.clone(),
            4,
            DistanceMetric::Cosine,
            Vec::new(),
            Default::default(),
            Some(ingestion),
        )
        .await
        .unwrap();
    ChunkingService::new(state.clone())
        .chunk_embed_and_upsert(
            id,
            ChunkedDocument {
                id: RecordId::string("parent").unwrap(),
                text: "abcdefghij".to_string(),
                metadata: Metadata::new(),
                chunking: ChunkingConfig {
                    max_chars: 5,
                    overlap_chars: 2,
                },
            },
        )
        .await
        .unwrap();

    let migrations = EmbeddingMigrationService::new(state.clone());
    migrations.start(id, "model-v2").await.unwrap();
    for _ in 0..200 {
        let current = migrations.status(id).await.unwrap();
        match current.status {
            EmbeddingMigrationStatus::Ready => return migrations,
            EmbeddingMigrationStatus::Failed => panic!("migration failed: {:?}", current.error),
            _ => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
    panic!("migration did not become ready");
}

fn persist_job(dir: &std::path::Path, record: &JobRecord) {
    let jobs = dir.join("jobs");
    fs::create_dir_all(&jobs).unwrap();
    fs::write(
        jobs.join(format!("job-{}.json", record.id)),
        serde_json::to_vec_pretty(record).unwrap(),
    )
    .unwrap();
}

fn synthetic_job(id: u64, state: JobState) -> JobRecord {
    JobRecord {
        version: 1,
        id: JobId::new(id).unwrap(),
        kind: JobKind::EmbeddingMigrationCatchUp {
            collection_id: "docs".to_string(),
        },
        state,
        progress: JobProgress {
            completed: 0,
            total: None,
            message: Some("synthetic".to_string()),
        },
        error: None,
        result: None,
        cancel_requested: false,
        created_at_unix_ms: 1,
        updated_at_unix_ms: 1,
    }
}

#[tokio::test]
async fn restart_recovery_marks_interrupted_running_job_failed() {
    let dir = temp_dir("restart");
    persist_job(&dir, &synthetic_job(1, JobState::Running));
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let jobs = JobService::new(state);

    assert_eq!(jobs.recover_interrupted_jobs().unwrap(), 1);
    let recovered = jobs.get(JobId::new(1).unwrap()).unwrap();
    assert_eq!(recovered.state, JobState::Failed);
    assert_eq!(
        recovered.error,
        Some(JobFailure {
            code: "interrupted_by_restart".to_string(),
            message: "job did not complete before the previous process stopped".to_string(),
        })
    );

    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn queued_job_can_be_cancelled_through_rest_api() {
    let dir = temp_dir("cancel");
    persist_job(&dir, &synthetic_job(1, JobState::Queued));
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());

    let response = app(state.clone())
        .oneshot(
            Request::post("/v0/jobs/1/cancel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let cancelled: JobRecord = serde_json::from_slice(&body).unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
    assert!(cancelled.cancel_requested);

    let response = app(state)
        .oneshot(Request::get("/v0/jobs/1").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let fetched: JobRecord = serde_json::from_slice(&body).unwrap();
    assert_eq!(fetched.state, JobState::Cancelled);

    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn migration_catch_up_job_completes_is_observable_and_survives_restart() {
    let dir = temp_dir("migration");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let id = CollectionId::new("docs").unwrap();
    let migrations = prepare_ready_migration(&state, &id).await;

    ChunkingService::new(state.clone())
        .chunk_embed_and_upsert(
            &id,
            ChunkedDocument {
                id: RecordId::string("parent").unwrap(),
                text: "abcde".to_string(),
                metadata: Metadata::new(),
                chunking: ChunkingConfig {
                    max_chars: 5,
                    overlap_chars: 2,
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(
        migrations.status(&id).await.unwrap().status,
        EmbeddingMigrationStatus::Ready
    );

    let response = app(state.clone())
        .oneshot(
            Request::post("/v0/collections/docs/embedding-migration/catch-up-job")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let submitted: JobRecord = serde_json::from_slice(&body).unwrap();
    assert_eq!(submitted.state, JobState::Queued);

    let jobs = JobService::new(state.clone());
    let completed = loop {
        let current = jobs.get(submitted.id).unwrap();
        match current.state {
            JobState::Completed => break current,
            JobState::Failed | JobState::Cancelled => {
                panic!("catch-up job terminated unexpectedly: {current:?}")
            }
            JobState::Queued | JobState::Running => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    };
    assert_eq!(
        completed.progress.completed,
        completed.progress.total.unwrap()
    );
    assert!(completed.progress.completed > 0);

    let response = app(state.clone())
        .oneshot(
            Request::get(format!("/v0/jobs/{}", submitted.id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app(state.clone())
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let metrics = String::from_utf8(body.to_vec()).unwrap();
    assert!(metrics.contains("ketebe_jobs_queued"));
    assert!(metrics.contains("ketebe_jobs_running"));
    assert!(metrics.contains("ketebe_jobs_completed_total"));
    assert!(metrics.contains("ketebe_jobs_failed_total"));

    drop(state);
    let recovered = AppState::recover(&dir).unwrap();
    let recovered_job = JobService::new(recovered).get(submitted.id).unwrap();
    assert_eq!(recovered_job.state, JobState::Completed);
    assert_eq!(recovered_job.progress, completed.progress);

    fs::remove_dir_all(dir).unwrap();
}
