use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use ketebe_core::{CollectionId, DistanceMetric, Metadata, RecordId};
use ketebe_server::{AppState, PendingRecord, RuntimeCatalog, WriteService, app};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-management-api-{}",
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

async fn request(state: AppState, method: &str, uri: &str) -> (StatusCode, Option<Value>) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("request");
    let response = app(state).oneshot(request).await.expect("response");
    let status = response.status();
    if status == StatusCode::NO_CONTENT {
        return (status, None);
    }
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, Some(serde_json::from_slice(&bytes).expect("JSON")))
}

#[tokio::test]
async fn collection_management_reports_stats_and_delete_survives_restart() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let writes = WriteService::new(state.clone());
    let alpha = CollectionId::new("alpha").expect("alpha");
    let beta = CollectionId::new("beta").expect("beta");

    writes
        .create_collection(beta.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create beta");
    writes
        .create_collection(alpha.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create alpha");
    writes
        .upsert_batch(&alpha, vec![pending("a", 1.0), pending("b", 2.0)])
        .await
        .expect("upsert");
    writes
        .delete(&alpha, RecordId::string("a").expect("id"))
        .await
        .expect("delete record");

    let (status, body) = request(state.clone(), "GET", "/v0/collections").await;
    assert_eq!(status, StatusCode::OK);
    let body = body.expect("list body");
    assert_eq!(body["collections"][0]["id"], "alpha");
    assert_eq!(body["collections"][1]["id"], "beta");

    let (status, body) = request(state.clone(), "GET", "/v0/collections/alpha").await;
    assert_eq!(status, StatusCode::OK);
    let body = body.expect("get body");
    assert_eq!(body["stats"]["live_records"], 1);
    assert_eq!(body["stats"]["tombstones"], 1);
    assert_eq!(body["stats"]["immutable_segments"], 0);
    assert_eq!(body["stats"]["mutable_mutations"], 3);
    assert!(body["stats"]["checkpoint_sequence"].is_null());
    assert_eq!(body["stats"]["next_sequence"], 4);
    assert_eq!(body["index"]["state"], "unavailable");

    writes
        .seal_collection(&alpha)
        .await
        .expect("seal")
        .expect("checkpoint");
    let (status, body) = request(state.clone(), "GET", "/v0/collections/alpha").await;
    assert_eq!(status, StatusCode::OK);
    let body = body.expect("sealed body");
    assert_eq!(body["stats"]["live_records"], 1);
    assert_eq!(body["stats"]["tombstones"], 1);
    assert_eq!(body["stats"]["immutable_segments"], 1);
    assert_eq!(body["stats"]["mutable_mutations"], 0);
    assert_eq!(body["stats"]["checkpoint_sequence"], 3);
    assert_eq!(body["index"]["state"], "ready");
    assert_eq!(body["index"]["config"]["m"], 16);

    let (status, body) = request(state.clone(), "GET", "/v0/collections/missing").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body.expect("error")["error"]["code"],
        "collection_not_found"
    );

    let (status, body) = request(state.clone(), "DELETE", "/v0/collections/alpha").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_none());
    assert!(!dir.join("collections/alpha").exists());

    drop(writes);
    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 100).expect("recover");
    let (status, _) = request(recovered.clone(), "GET", "/v0/collections/alpha").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = request(recovered, "GET", "/v0/collections").await;
    assert_eq!(status, StatusCode::OK);
    let body = body.expect("recovered list");
    assert_eq!(body["collections"].as_array().expect("array").len(), 1);
    assert_eq!(body["collections"][0]["id"], "beta");

    fs::remove_dir_all(dir).expect("cleanup");
}
