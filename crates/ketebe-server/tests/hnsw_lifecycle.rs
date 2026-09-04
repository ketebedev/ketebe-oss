use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use ketebe_core::{CollectionId, DistanceMetric, Metadata, RecordId};
use ketebe_server::{AppState, PendingRecord, RuntimeCatalog, WriteService, app};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-hnsw-lifecycle-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ))
}

fn pending(id: &str, value: f32) -> PendingRecord {
    PendingRecord {
        id: RecordId::string(id).expect("record ID"),
        vector: vec![value],
        metadata: Metadata::new(),
    }
}

async fn query(state: AppState, execution: &str) -> (StatusCode, Value) {
    let request = Request::post("/v0/collections/docs/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(format!(
            r#"{{"vector":[2.0],"metric":"l2","top_k":2,"execution":"{execution}"}}"#
        )))
        .expect("request");
    let response = app(state).oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = serde_json::from_slice(&bytes).expect("JSON");
    (status, body)
}

async fn assert_hnsw_available(state: AppState) {
    let (status, body) = query(state, "hnsw").await;
    assert!(status.is_success(), "unexpected response: {body}");
    assert_eq!(body["explain"]["strategy"], "hnsw");
}

async fn create_sealed_collection(dir: &Path) -> (AppState, WriteService, CollectionId) {
    let state = AppState::with_data_dir_and_threshold(
        RuntimeCatalog::empty_ready(),
        dir.to_path_buf(),
        100,
    );
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create");
    service
        .upsert_batch(
            &collection,
            vec![pending("a", 1.0), pending("b", 2.0), pending("c", 3.0)],
        )
        .await
        .expect("upsert");
    service
        .seal_collection(&collection)
        .await
        .expect("seal")
        .expect("checkpoint");
    (state, service, collection)
}

#[tokio::test]
async fn seal_persists_hnsw_and_restart_restores_ann() {
    let dir = temp_dir("restart");
    let (state, service, _) = create_sealed_collection(&dir).await;
    let index_path = dir.join("collections/docs/indexes/hnsw.kthi");
    assert!(index_path.exists());
    assert_hnsw_available(state.clone()).await;
    let persisted = fs::read(&index_path).expect("snapshot");

    drop(service);
    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 100).expect("recover");
    assert_hnsw_available(recovered).await;
    let recovered_index_path = if index_path.exists() {
        index_path
    } else {
        dir.join("projects/default/collections/docs/indexes/hnsw.kthi")
    };
    assert_eq!(
        fs::read(&recovered_index_path).expect("snapshot after restart"),
        persisted
    );
    fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn mutable_write_disables_ann_until_next_seal() {
    let dir = temp_dir("mutable");
    let (state, service, collection) = create_sealed_collection(&dir).await;
    assert_hnsw_available(state.clone()).await;

    service
        .upsert(&collection, pending("d", 4.0))
        .await
        .expect("mutable write");
    let (hnsw_status, _) = query(state.clone(), "hnsw").await;
    assert_eq!(hnsw_status, StatusCode::SERVICE_UNAVAILABLE);
    let (auto_status, auto_body) = query(state.clone(), "auto").await;
    assert!(auto_status.is_success());
    assert_eq!(auto_body["explain"]["strategy"], "exact");

    service
        .seal_collection(&collection)
        .await
        .expect("seal")
        .expect("checkpoint");
    assert_hnsw_available(state).await;
    fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn missing_or_corrupt_snapshot_is_rebuilt_on_restart() {
    for corrupt in [false, true] {
        let dir = temp_dir(if corrupt { "corrupt" } else { "missing" });
        let (state, service, _) = create_sealed_collection(&dir).await;
        let index_path = dir.join("collections/docs/indexes/hnsw.kthi");
        drop(service);
        drop(state);

        if corrupt {
            fs::write(&index_path, b"corrupt-index").expect("corrupt snapshot");
        } else {
            fs::remove_file(&index_path).expect("remove snapshot");
        }

        let recovered = AppState::recover_with_threshold(&dir, 100).expect("recover");
        assert_hnsw_available(recovered).await;
        let rebuilt = fs::read(&index_path).expect("rebuilt snapshot");
        assert!(rebuilt.starts_with(b"KTHI"));
        fs::remove_dir_all(dir).expect("cleanup");
    }
}

#[tokio::test]
async fn stale_snapshot_after_checkpoint_change_is_rebuilt() {
    let dir = temp_dir("stale");
    let (state, service, collection) = create_sealed_collection(&dir).await;
    let index_path = dir.join("collections/docs/indexes/hnsw.kthi");
    let old_snapshot = fs::read(&index_path).expect("old snapshot");

    service
        .upsert(&collection, pending("d", 4.0))
        .await
        .expect("write");
    service
        .seal_collection(&collection)
        .await
        .expect("second seal")
        .expect("checkpoint");
    let current_snapshot = fs::read(&index_path).expect("new snapshot");
    assert_ne!(current_snapshot, old_snapshot);

    drop(service);
    drop(state);
    fs::write(&index_path, old_snapshot).expect("restore stale snapshot");
    let recovered = AppState::recover_with_threshold(&dir, 100).expect("recover");
    assert_hnsw_available(recovered).await;
    assert_eq!(fs::read(&index_path).expect("rebuilt"), current_snapshot);
    fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn compaction_republishes_index_for_replacement_checkpoint() {
    let dir = temp_dir("compaction");
    let (state, service, collection) = create_sealed_collection(&dir).await;
    let index_path = dir.join("collections/docs/indexes/hnsw.kthi");
    let before = fs::read(&index_path).expect("before");

    service
        .upsert(&collection, pending("a", 10.0))
        .await
        .expect("overwrite");
    service
        .seal_collection(&collection)
        .await
        .expect("second seal")
        .expect("checkpoint");
    service
        .compact_collection(&collection)
        .await
        .expect("compact")
        .expect("replacement checkpoint");

    let after = fs::read(&index_path).expect("after");
    assert_ne!(after, before);
    assert_hnsw_available(state.clone()).await;

    drop(service);
    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 100).expect("recover");
    assert_hnsw_available(recovered).await;
    fs::remove_dir_all(dir).expect("cleanup");
}
