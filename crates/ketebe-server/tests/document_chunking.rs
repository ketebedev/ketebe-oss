use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use ketebe_core::{CollectionId, DistanceMetric, Metadata, RecordId};
use ketebe_server::{
    AppState, ChunkedDocument, ChunkingConfig, ChunkingService, CollectionService,
    DeterministicEmbeddingProvider, EmbeddingFuture, EmbeddingModel, EmbeddingProvider,
    EmbeddingProviderError, KafkaIngestionMessage, KafkaIngestionService, RuntimeCatalog,
    WriteService, app,
};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-chunk-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

async fn state_with_collection(dir: &std::path::Path) -> (AppState, CollectionId) {
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.to_path_buf());
    let collection = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection(collection.clone(), 4, DistanceMetric::L2, Vec::new())
        .await
        .unwrap();
    (state, collection)
}

async fn install_provider(state: &AppState) {
    state
        .set_embedding_provider(Arc::new(
            DeterministicEmbeddingProvider::new("chunk-model", "v1").unwrap(),
        ))
        .await;
}

#[tokio::test]
async fn chunk_set_is_durable_as_one_validated_batch() {
    let dir = temp_dir("durable");
    let (state, collection) = state_with_collection(&dir).await;
    install_provider(&state).await;

    let result = ChunkingService::new(state.clone())
        .chunk_embed_and_upsert(
            &collection,
            ChunkedDocument {
                id: RecordId::string("parent-1").unwrap(),
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

    assert_eq!(result.chunk_ids.len(), 3);
    assert_eq!(result.generation.get(), 1);
    assert_eq!(result.reconciled_chunks, 0);
    assert_eq!(
        result
            .sequence_numbers
            .iter()
            .map(|value| value.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        CollectionService::new(state.clone())
            .get(&collection)
            .await
            .unwrap()
            .live_records,
        3
    );

    drop(state);
    let recovered = AppState::recover(&dir).unwrap();
    assert_eq!(
        CollectionService::new(recovered)
            .get(&collection)
            .await
            .unwrap()
            .live_records,
        3
    );
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn reingest_with_fewer_chunks_tombstones_obsolete_tail_in_same_logical_replace() {
    let dir = temp_dir("reconcile");
    let (state, collection) = state_with_collection(&dir).await;
    install_provider(&state).await;
    let service = ChunkingService::new(state.clone());
    let parent = RecordId::string("parent-replace").unwrap();

    let first = service
        .chunk_embed_and_upsert(
            &collection,
            ChunkedDocument {
                id: parent.clone(),
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
    assert_eq!(first.chunk_ids.len(), 3);

    let second = service
        .chunk_embed_and_upsert(
            &collection,
            ChunkedDocument {
                id: parent,
                text: "abcdefg".to_string(),
                metadata: Metadata::new(),
                chunking: ChunkingConfig {
                    max_chars: 5,
                    overlap_chars: 2,
                },
            },
        )
        .await
        .unwrap();

    assert_eq!(second.chunk_ids.len(), 2);
    assert_eq!(second.reconciled_chunks, 1);
    assert_eq!(second.generation.get(), 4);
    assert_eq!(
        CollectionService::new(state.clone())
            .get(&collection)
            .await
            .unwrap()
            .live_records,
        2
    );

    drop(state);
    let recovered = AppState::recover(&dir).unwrap();
    assert_eq!(
        CollectionService::new(recovered)
            .get(&collection)
            .await
            .unwrap()
            .live_records,
        2
    );
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn deleting_parent_tombstones_current_chunks_and_is_restart_safe() {
    let dir = temp_dir("delete-parent");
    let (state, collection) = state_with_collection(&dir).await;
    install_provider(&state).await;
    let service = ChunkingService::new(state.clone());
    let parent = RecordId::string("parent-delete").unwrap();

    service
        .chunk_embed_and_upsert(
            &collection,
            ChunkedDocument {
                id: parent.clone(),
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

    let deleted = service
        .delete_parent_document(&collection, &parent)
        .await
        .unwrap();
    assert_eq!(deleted.len(), 3);
    assert_eq!(
        CollectionService::new(state.clone())
            .get(&collection)
            .await
            .unwrap()
            .live_records,
        0
    );
    assert!(
        service
            .delete_parent_document(&collection, &parent)
            .await
            .unwrap()
            .is_empty()
    );

    drop(state);
    let recovered = AppState::recover(&dir).unwrap();
    assert_eq!(
        CollectionService::new(recovered)
            .get(&collection)
            .await
            .unwrap()
            .live_records,
        0
    );
    fs::remove_dir_all(dir).unwrap();
}

#[derive(Clone)]
struct FailSecondProvider {
    calls: Arc<AtomicUsize>,
}

impl EmbeddingProvider for FailSecondProvider {
    fn provider_name(&self) -> &str {
        "fail-second"
    }

    fn model(&self) -> EmbeddingModel {
        EmbeddingModel::new("failure-test", "v1").unwrap()
    }

    fn embed<'a>(&'a self, _text: &'a str, expected_dimension: usize) -> EmbeddingFuture<'a> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if call == 1 {
                return Err(EmbeddingProviderError::new("planned second chunk failure"));
            }
            Ok(vec![0.25; expected_dimension])
        })
    }
}

#[tokio::test]
async fn provider_failure_before_batch_write_leaves_no_partial_chunks() {
    let dir = temp_dir("failure");
    let (state, collection) = state_with_collection(&dir).await;
    state
        .set_embedding_provider(Arc::new(FailSecondProvider {
            calls: Arc::new(AtomicUsize::new(0)),
        }))
        .await;

    ChunkingService::new(state.clone())
        .chunk_embed_and_upsert(
            &collection,
            ChunkedDocument {
                id: RecordId::unsigned(42),
                text: "abcdefghij".to_string(),
                metadata: Metadata::new(),
                chunking: ChunkingConfig {
                    max_chars: 5,
                    overlap_chars: 2,
                },
            },
        )
        .await
        .expect_err("provider must fail");

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
async fn rest_and_kafka_accept_chunking_config() {
    let dir = temp_dir("transport");
    let (state, collection) = state_with_collection(&dir).await;
    install_provider(&state).await;

    let request = Request::builder()
        .method("PUT")
        .uri("/v0/collections/docs/documents/rest-parent")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"text":"abcdefghij","chunking":{"max_chars":5,"overlap_chars":2}}"#,
        ))
        .unwrap();
    let response = app(state.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["chunk_count"], 3);
    assert_eq!(json["generation"], 1);
    assert_eq!(json["reconciled_chunks"], 0);
    assert_eq!(json["sequence_numbers"], serde_json::json!([1, 2, 3]));

    let ack = KafkaIngestionService::new(state.clone())
        .apply_partition_batch(
            &collection,
            &[KafkaIngestionMessage {
                partition: 0,
                offset: 9,
                payload: br#"{"version":1,"op":"document","id":{"type":"u64","value":77},"text":"abcdefghij","chunking":{"max_chars":5,"overlap_chars":2}}"#.to_vec(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(ack.next_offset, 10);
    assert_eq!(
        CollectionService::new(state)
            .get(&collection)
            .await
            .unwrap()
            .live_records,
        6
    );
    fs::remove_dir_all(dir).unwrap();
}
