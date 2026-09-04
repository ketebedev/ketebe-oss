use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use ketebe_core::{CollectionId, DistanceMetric, RecordId};
use ketebe_server::{
    AppState, JobService, JobServiceError, PendingRecord, RuntimeCatalog, WriteService, app,
};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ketebe-lifecycle-{label}-{nonce}"))
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("json")
}

#[tokio::test]
async fn draining_runtime_rejects_new_rest_writes_and_background_jobs() {
    let dir = temp_dir("reject");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let collection = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .unwrap();

    state.begin_draining();

    let response = app(state.clone())
        .oneshot(
            Request::put("/v0/collections/docs/records/new")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"vector":[1.0],"metadata":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let ready = app(state.clone())
        .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);

    assert!(matches!(
        JobService::new(state).submit_embedding_migration_catch_up(collection),
        Err(JobServiceError::RuntimeDraining)
    ));

    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn forced_restart_replays_acknowledged_wal_write() {
    let dir = temp_dir("forced-restart");
    let collection = CollectionId::new("docs").unwrap();
    {
        let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
        let writes = WriteService::new(state);
        writes
            .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
            .await
            .unwrap();
        let sequence = writes
            .upsert(
                &collection,
                PendingRecord {
                    id: RecordId::string("acknowledged").unwrap(),
                    vector: vec![1.0],
                    metadata: Default::default(),
                },
            )
            .await
            .unwrap();
        assert_eq!(sequence.get(), 1);
        // Drop the runtime without sealing/checkpointing to model abrupt termination.
    }

    let recovered = AppState::recover(&dir).unwrap();
    let query = Request::post("/v0/collections/docs/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"vector":[1.0],"metric":"l2","top_k":1,"execution":"exact"}"#,
        ))
        .unwrap();
    let response = app(recovered).oneshot(query).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["hits"][0]["sequence_number"], 1);

    std::fs::remove_dir_all(dir).unwrap();
}
