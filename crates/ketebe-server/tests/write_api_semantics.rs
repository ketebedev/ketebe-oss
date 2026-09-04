use axum::body::{Body, to_bytes};
use axum::http::{Request, header};
use ketebe_core::{CollectionId, DistanceMetric, RecordId};
use ketebe_server::{AppState, PendingRecord, RuntimeCatalog, WriteService, app};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn data_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ketebe-write-semantics-{nonce}"))
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("json")
}

#[tokio::test]
async fn overwrite_and_repeated_delete_preserve_latest_visibility() {
    let data_dir = data_dir();
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), data_dir.clone());
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create");

    let id = RecordId::string("same").expect("record id");
    let first = service
        .upsert(
            &collection,
            PendingRecord {
                id: id.clone(),
                vector: vec![5.0],
                metadata: Default::default(),
            },
        )
        .await
        .expect("first upsert");
    let second = service
        .upsert(
            &collection,
            PendingRecord {
                id: id.clone(),
                vector: vec![1.0],
                metadata: Default::default(),
            },
        )
        .await
        .expect("overwrite");
    assert_eq!(first.get(), 1);
    assert_eq!(second.get(), 2);

    let query = Request::post("/v0/collections/docs/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"vector":[1.0],"metric":"l2","top_k":1,"execution":"exact"}"#,
        ))
        .expect("request");
    let response = app(state.clone()).oneshot(query).await.expect("response");
    let body = response_json(response).await;
    assert_eq!(body["hits"][0]["sequence_number"], 2);
    assert_eq!(body["hits"][0]["score"], 0.0);

    let first_delete = service
        .delete(&collection, id.clone())
        .await
        .expect("first delete");
    let second_delete = service
        .delete(&collection, id)
        .await
        .expect("repeated delete");
    assert_eq!(first_delete.get(), 3);
    assert_eq!(second_delete.get(), 4);

    let query = Request::post("/v0/collections/docs/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"vector":[1.0],"metric":"l2","top_k":1,"execution":"exact"}"#,
        ))
        .expect("request");
    let response = app(state).oneshot(query).await.expect("response");
    let body = response_json(response).await;
    assert_eq!(body["hits"].as_array().expect("hits").len(), 0);

    std::fs::remove_dir_all(data_dir).expect("cleanup");
}
