use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use ketebe_core::{CollectionId, DistanceMetric, Metadata, RecordId};
use ketebe_server::{
    AppState, CollectionService, DeterministicEmbeddingProvider, DocumentRecord, EmbeddingError,
    EmbeddingFuture, EmbeddingModel, EmbeddingProvider, EmbeddingProviderError, EmbeddingService,
    KafkaIngestionMessage, KafkaIngestionService, RuntimeCatalog, WriteService, app,
};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

static TEMP_DIR_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    let sequence = TEMP_DIR_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ketebe-embedding-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
        sequence,
    ))
}

#[tokio::test]
async fn deterministic_embedding_is_durable() {
    let dir = temp_dir();
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let collection = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection(collection.clone(), 4, DistanceMetric::L2, Vec::new())
        .await
        .unwrap();
    state
        .set_embedding_provider(Arc::new(
            DeterministicEmbeddingProvider::new("test-model", "2026-08").unwrap(),
        ))
        .await;

    let sequence = EmbeddingService::from_state(state.clone())
        .await
        .unwrap()
        .embed_and_upsert(
            &collection,
            DocumentRecord {
                id: RecordId::string("doc-1").unwrap(),
                text: "vector databases are useful".to_string(),
                metadata: Metadata::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(sequence.get(), 1);

    drop(state);
    let recovered = AppState::recover(&dir).unwrap();
    assert_eq!(
        CollectionService::new(recovered)
            .get(&collection)
            .await
            .unwrap()
            .live_records,
        1
    );
    fs::remove_dir_all(dir).unwrap();
}

#[derive(Clone)]
struct WrongDimensionProvider;
impl EmbeddingProvider for WrongDimensionProvider {
    fn provider_name(&self) -> &str {
        "wrong-dimension"
    }

    fn model(&self) -> EmbeddingModel {
        EmbeddingModel::new("wrong", "v1").unwrap()
    }

    fn embed<'a>(&'a self, _text: &'a str, _expected_dimension: usize) -> EmbeddingFuture<'a> {
        Box::pin(async { Ok(vec![1.0]) })
    }
}

#[tokio::test]
async fn provider_dimension_is_checked_before_write() {
    let dir = temp_dir();
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let collection = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection(collection.clone(), 3, DistanceMetric::Dot, Vec::new())
        .await
        .unwrap();

    let error = EmbeddingService::new(state.clone(), Arc::new(WrongDimensionProvider))
        .embed_and_upsert(
            &collection,
            DocumentRecord {
                id: RecordId::unsigned(7),
                text: "hello".to_string(),
                metadata: Metadata::new(),
            },
        )
        .await
        .expect_err("dimension mismatch");
    assert!(matches!(
        error,
        EmbeddingError::DimensionMismatch {
            expected: 3,
            actual: 1
        }
    ));
    assert_eq!(
        CollectionService::new(state)
            .get(&collection)
            .await
            .unwrap()
            .live_records,
        0
    );
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn rest_document_path_requires_provider_and_then_writes() {
    let dir = temp_dir();
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let collection = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection(collection, 2, DistanceMetric::L2, Vec::new())
        .await
        .unwrap();

    let request = || {
        Request::builder()
            .method("PUT")
            .uri("/v0/collections/docs/documents/a")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"text":"hello","metadata":{"kind":"note"}}"#))
            .unwrap()
    };

    let unavailable = app(state.clone()).oneshot(request()).await.unwrap();
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);

    state
        .set_embedding_provider(Arc::new(
            DeterministicEmbeddingProvider::new("test", "v1").unwrap(),
        ))
        .await;
    let response = app(state.clone()).oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["sequence_number"],
        1
    );
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn kafka_document_envelope_embeds_before_batch_ack() {
    let dir = temp_dir();
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let collection = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection(collection.clone(), 3, DistanceMetric::L2, Vec::new())
        .await
        .unwrap();
    state
        .set_embedding_provider(Arc::new(
            DeterministicEmbeddingProvider::new("test", "v1").unwrap(),
        ))
        .await;

    let service = KafkaIngestionService::new(state.clone());
    let ack = service
        .apply_partition_batch(
            &collection,
            &[KafkaIngestionMessage {
                partition: 0,
                offset: 12,
                payload: br#"{"version":1,"op":"document","id":{"type":"string","value":"k1"},"text":"hello kafka","metadata":{"source":"kafka"}}"#.to_vec(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(ack.next_offset, 13);
    assert_eq!(
        CollectionService::new(state)
            .get(&collection)
            .await
            .unwrap()
            .live_records,
        1
    );
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn provider_error_type_is_stable_for_integrators() {
    let error = EmbeddingProviderError::new("boom");
    assert_eq!(error.to_string(), "boom");
}
