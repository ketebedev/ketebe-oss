use ketebe_mcp::{jobs::JobStateView, ketebe::KetebeApi};
use ketebe_server::{
    AppState, JobId, JobKind, JobProgress, JobRecord, JobState, RuntimeCatalog, app,
};
use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-jobs-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

fn persist_queued_job(dir: &std::path::Path) {
    let jobs = dir.join("jobs");
    fs::create_dir_all(&jobs).expect("jobs directory");
    let record = JobRecord {
        version: 1,
        id: JobId::new(1).expect("job id"),
        kind: JobKind::EmbeddingMigrationCatchUp {
            collection_id: "docs".to_string(),
        },
        state: JobState::Queued,
        progress: JobProgress {
            completed: 0,
            total: None,
            message: Some("queued".to_string()),
        },
        error: None,
        result: None,
        cancel_requested: false,
        created_at_unix_ms: 1,
        updated_at_unix_ms: 1,
    };
    fs::write(
        jobs.join("job-1.json"),
        serde_json::to_vec_pretty(&record).expect("serialize job"),
    )
    .expect("persist job");
}

#[tokio::test]
async fn mcp_jobs_use_public_scoped_lifecycle_contract() {
    let dir = temp_dir();
    persist_queued_job(&dir);
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app(state)).await.expect("server");
    });

    let api = KetebeApi::new(format!("http://{address}")).expect("MCP API adapter");
    let listed = api.list_jobs(None).await.expect("list jobs");
    assert_eq!(listed.jobs.len(), 1);
    assert_eq!(listed.jobs[0].id, 1);
    assert_eq!(listed.jobs[0].state, JobStateView::Queued);
    assert_eq!(listed.jobs[0].progress.completed, 0);
    assert!(listed.jobs[0].error.is_none());

    let fetched = api.get_job(1, None).await.expect("get job");
    assert_eq!(fetched.state, JobStateView::Queued);

    let cancelled = api.cancel_job(1, None).await.expect("cancel job");
    assert_eq!(cancelled.state, JobStateView::Cancelled);
    assert!(cancelled.cancel_requested);

    let fetched = api.get_job(1, None).await.expect("get cancelled job");
    assert_eq!(fetched.state, JobStateView::Cancelled);
    assert!(fetched.cancel_requested);

    server.abort();
    let _ = fs::remove_dir_all(dir);
}
