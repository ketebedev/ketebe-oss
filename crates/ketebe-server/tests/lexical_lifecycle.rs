use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use ketebe_core::{CollectionId, DistanceMetric, FieldPath, Metadata, MetadataValue, RecordId};
use ketebe_server::{AppState, PendingRecord, RuntimeCatalog, WriteService, app};
use serde_json::Value;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn data_dir(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ketebe-lexical-{label}-{nonce}"))
}

fn title_path() -> FieldPath {
    FieldPath::new(["title"]).expect("field path")
}

fn record(id: u64, title: &str) -> PendingRecord {
    let mut metadata = Metadata::new();
    metadata.insert(
        "title".to_string(),
        MetadataValue::String(title.to_string()),
    );
    PendingRecord {
        id: RecordId::unsigned(id),
        vector: vec![1.0],
        metadata,
    }
}

fn lexical_snapshot_exists(collection_dir: &std::path::Path) -> bool {
    let directory = collection_dir.join("indexes/lexical");
    fs::read_dir(directory).is_ok_and(|entries| {
        entries.filter_map(Result::ok).any(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "ktli")
        })
    })
}

async fn wait_for_lexical_snapshot(collection_dir: &std::path::Path) {
    for _ in 0..200 {
        if lexical_snapshot_exists(collection_dir) {
            return;
        }
        tokio::task::yield_now().await;
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("checkpoint-triggered lexical snapshot was not published");
}

#[tokio::test]
async fn checkpoint_prebuilds_configured_lexical_index_and_recovery_uses_config() {
    let dir = data_dir("prebuild");
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 1);
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");

    let config = service
        .create_collection(
            collection.clone(),
            1,
            DistanceMetric::Dot,
            vec![title_path()],
        )
        .await
        .expect("create");
    assert_eq!(config.lexical_fields(), &[title_path()]);

    service
        .upsert(&collection, record(1, "rust database"))
        .await
        .expect("upsert and seal");

    let collection_dir = dir.join("collections/docs");
    wait_for_lexical_snapshot(&collection_dir).await;

    let persisted: Value = serde_json::from_slice(
        &fs::read(collection_dir.join("collection.json")).expect("collection metadata"),
    )
    .expect("collection json");
    assert_eq!(persisted["version"], 6);
    assert_eq!(persisted["lexical_fields"], serde_json::json!([["title"]]));
    assert_eq!(persisted["lexical_analyzer"]["kind"], "standard");
    assert_eq!(persisted["lexical_analyzer"]["lowercase"], true);

    drop(service);
    drop(state);

    let recovered = AppState::recover_with_threshold(&dir, 1).expect("recover");
    let request = Request::post("/v0/collections/docs/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"vector":[1.0],"metric":"dot","top_k":1,"execution":"exact","lexical":{"text":"rust","fields":[]}}"#,
        ))
        .expect("request");
    let response = app(recovered).oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(value["hits"].as_array().expect("hits").len(), 1);

    fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn configured_collection_rejects_conflicting_query_fields() {
    let dir = data_dir("mismatch");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection, 1, DistanceMetric::Dot, vec![title_path()])
        .await
        .expect("create");

    let request = Request::post("/v0/collections/docs/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"vector":[1.0],"metric":"dot","top_k":1,"execution":"exact","lexical":{"text":"rust","fields":[["body"]]}}"#,
        ))
        .expect("request");
    let response = app(state).oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(value["error"]["code"], "lexical_fields_mismatch");

    fs::remove_dir_all(dir).expect("cleanup");
}
