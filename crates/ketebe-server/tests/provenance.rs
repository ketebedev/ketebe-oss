use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use ketebe_core::{CollectionId, DistanceMetric};
use ketebe_server::{
    AppState, DeterministicEmbeddingProvider, KafkaIngestionMessage, KafkaIngestionService,
    RuntimeCatalog, WriteService, app, canonical_content_hash,
};
use serde_json::{Value, json};
use std::fs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-provenance-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

async fn state_with_collection(dir: &std::path::Path) -> (AppState, CollectionId) {
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.to_path_buf());
    state
        .set_embedding_provider(Arc::new(
            DeterministicEmbeddingProvider::new("provenance-model", "v1").unwrap(),
        ))
        .await;
    let collection = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection(collection.clone(), 4, DistanceMetric::L2, Vec::new())
        .await
        .unwrap();
    (state, collection)
}

async fn query_all(state: AppState) -> Value {
    let request = Request::post("/v0/collections/docs/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"vector":[0.0,0.0,0.0,0.0],"metric":"l2","top_k":10,"execution":"exact"}"#,
        ))
        .unwrap();
    let response = app(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn rest_provenance_chunk_hashes_replacement_and_restart_are_durable() {
    let dir = temp_dir("rest");
    let (state, _) = state_with_collection(&dir).await;
    let first_text = "alpha\r\nbeta gamma";

    let request = Request::put("/v0/collections/docs/documents/parent-1")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "text": first_text,
                "chunking": {"max_chars": 8, "overlap_chars": 0},
                "source": {
                    "kind": "http",
                    "uri": "https://example.test/docs/1",
                    "external_id": "external-1",
                    "version": "v1",
                    "etag": "etag-1",
                    "observed_at_unix_ms": 42
                }
            })
            .to_string(),
        ))
        .unwrap();
    let response = app(state.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let first: Value = serde_json::from_slice(&body).unwrap();
    assert!(first["chunk_count"].as_u64().unwrap() >= 2);
    assert_eq!(first["generation"], 1);

    let visible = query_all(state.clone()).await;
    let hits = visible["hits"].as_array().unwrap();
    assert!(hits.len() >= 2);
    for hit in hits {
        let metadata = &hit["metadata"];
        assert_eq!(metadata["_ketebe_source"]["kind"], "http");
        assert_eq!(
            metadata["_ketebe_source"]["uri"],
            "https://example.test/docs/1"
        );
        assert_eq!(metadata["_ketebe_source"]["external_id"], "external-1");
        assert_eq!(metadata["_ketebe_source"]["version"], "v1");
        assert_eq!(metadata["_ketebe_source"]["etag"], "etag-1");
        assert_eq!(
            metadata["_ketebe_source"]["observed_at_unix_ms"]
                .as_f64()
                .unwrap(),
            42.0
        );
        assert_eq!(
            metadata["_ketebe_content"]["document_sha256"],
            canonical_content_hash(first_text)
        );
        assert_eq!(
            metadata["_ketebe_content"]["normalization"],
            "line_endings_v1"
        );
        assert!(
            metadata["_ketebe_content"]["chunk_sha256"]
                .as_str()
                .is_some_and(|value| value.len() == 64)
        );
        assert_eq!(
            metadata["_ketebe_chunk"]["generation"].as_f64().unwrap(),
            1.0
        );
    }

    let request = Request::put("/v0/collections/docs/documents/parent-1")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "text": "alpha",
                "chunking": {"max_chars": 8, "overlap_chars": 0},
                "source": {
                    "kind": "http",
                    "uri": "https://example.test/docs/1",
                    "external_id": "external-1",
                    "version": "v2",
                    "etag": "etag-2",
                    "observed_at_unix_ms": 84
                }
            })
            .to_string(),
        ))
        .unwrap();
    let response = app(state.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let second: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(second["chunk_count"], 1);
    assert!(second["reconciled_chunks"].as_u64().unwrap() >= 1);
    let replacement_generation = second["generation"].as_u64().unwrap();
    assert!(replacement_generation > 1);

    drop(state);
    let recovered = AppState::recover(&dir).unwrap();
    let visible = query_all(recovered).await;
    let hits = visible["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    let metadata = &hits[0]["metadata"];
    assert_eq!(metadata["_ketebe_source"]["version"], "v2");
    assert_eq!(metadata["_ketebe_source"]["etag"], "etag-2");
    assert_eq!(
        metadata["_ketebe_source"]["observed_at_unix_ms"]
            .as_f64()
            .unwrap(),
        84.0
    );
    assert_eq!(
        metadata["_ketebe_content"]["document_sha256"],
        canonical_content_hash("alpha")
    );
    assert_eq!(
        metadata["_ketebe_chunk"]["generation"].as_f64().unwrap(),
        replacement_generation as f64
    );

    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn kafka_document_envelope_maps_source_provenance() {
    let dir = temp_dir("kafka");
    let (state, collection) = state_with_collection(&dir).await;
    let payload = json!({
        "version": 1,
        "op": "document",
        "id": {"type": "string", "value": "kafka-doc"},
        "text": "streamed document",
        "source": {
            "kind": "kafka",
            "uri": "kafka://documents/topic/0/17",
            "external_id": "event-17",
            "revision": "17",
            "observed_at_unix_ms": 1234
        }
    })
    .to_string();

    let ack = KafkaIngestionService::new(state.clone())
        .apply_partition_batch(
            &collection,
            &[KafkaIngestionMessage {
                partition: 0,
                offset: 17,
                payload: payload.into_bytes(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(ack.next_offset, 18);

    let visible = query_all(state).await;
    let hits = visible["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    let metadata = &hits[0]["metadata"];
    assert_eq!(metadata["_ketebe_source"]["kind"], "kafka");
    assert_eq!(
        metadata["_ketebe_source"]["uri"],
        "kafka://documents/topic/0/17"
    );
    assert_eq!(metadata["_ketebe_source"]["external_id"], "event-17");
    assert_eq!(metadata["_ketebe_source"]["revision"], "17");
    assert_eq!(
        metadata["_ketebe_content"]["document_sha256"],
        canonical_content_hash("streamed document")
    );

    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn rest_rejects_user_owned_reserved_provenance_metadata() {
    let dir = temp_dir("reserved");
    let (state, _) = state_with_collection(&dir).await;
    let request = Request::put("/v0/collections/docs/documents/doc")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "text": "hello",
                "metadata": {"_ketebe_source": {"uri": "spoofed"}}
            })
            .to_string(),
        ))
        .unwrap();
    let response = app(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["error"]["code"], "invalid_document_provenance");
    fs::remove_dir_all(dir).unwrap();
}
