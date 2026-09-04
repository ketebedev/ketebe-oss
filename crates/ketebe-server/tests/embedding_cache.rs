use axum::body::{Body, to_bytes};
use axum::http::Request;
use ketebe_core::{CollectionId, DistanceMetric, Metadata, RecordId};
use ketebe_server::{
    AppState, DocumentRecord, EmbeddingBatchFuture, EmbeddingCache, EmbeddingFuture,
    EmbeddingModel, EmbeddingProvider, EmbeddingService, RuntimeCatalog, WriteService, app,
    embed_texts_cached,
};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

#[derive(Clone)]
struct CountingProvider {
    model: EmbeddingModel,
    calls: Arc<AtomicU64>,
}

impl CountingProvider {
    fn new(version: &str, calls: Arc<AtomicU64>) -> Self {
        Self {
            model: EmbeddingModel::new("counting-model", version).unwrap(),
            calls,
        }
    }
}

impl EmbeddingProvider for CountingProvider {
    fn provider_name(&self) -> &str {
        "counting"
    }

    fn model(&self) -> EmbeddingModel {
        self.model.clone()
    }

    fn max_batch_inputs(&self) -> usize {
        128
    }

    fn embed<'a>(&'a self, text: &'a str, expected_dimension: usize) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let seed = text
                .bytes()
                .fold(0_u32, |acc, byte| acc.wrapping_add(byte as u32));
            Ok((0..expected_dimension)
                .map(|index| (seed.wrapping_add(index as u32) as f32) / 1000.0)
                .collect())
        })
    }

    fn embed_batch<'a>(
        &'a self,
        texts: &'a [String],
        expected_dimension: usize,
    ) -> EmbeddingBatchFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(texts
                .iter()
                .map(|text| {
                    let seed = text
                        .bytes()
                        .fold(0_u32, |acc, byte| acc.wrapping_add(byte as u32));
                    (0..expected_dimension)
                        .map(|index| (seed.wrapping_add(index as u32) as f32) / 1000.0)
                        .collect::<Vec<_>>()
                })
                .collect())
        })
    }
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-embedding-cache-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

async fn create_collection(state: &AppState) -> CollectionId {
    let collection = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection(collection.clone(), 4, DistanceMetric::L2, Vec::new())
        .await
        .unwrap();
    collection
}

fn document(id: &str, text: &str) -> DocumentRecord {
    DocumentRecord {
        id: RecordId::string(id).unwrap(),
        text: text.to_string(),
        metadata: Metadata::new(),
    }
}

#[tokio::test]
async fn cache_executor_deduplicates_identical_batch_inputs() {
    let calls = Arc::new(AtomicU64::new(0));
    let provider: Arc<dyn EmbeddingProvider> = Arc::new(CountingProvider::new("v1", calls.clone()));
    let cache = Arc::new(EmbeddingCache::new(16));
    let texts = vec!["same".to_string(), "same".to_string(), "other".to_string()];

    let first = embed_texts_cached(cache.clone(), "profile-a", provider.clone(), &texts, 4)
        .await
        .unwrap();
    assert_eq!(first.len(), 3);
    assert_eq!(first[0], first[1]);
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    let second = embed_texts_cached(cache, "profile-a", provider, &texts, 4)
        .await
        .unwrap();
    assert_eq!(second, first);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn document_pipeline_reuses_cache_and_provider_identity_change_misses() {
    let dir = temp_dir("document");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let collection = create_collection(&state).await;

    let v1_calls = Arc::new(AtomicU64::new(0));
    state
        .set_embedding_provider(Arc::new(CountingProvider::new("v1", v1_calls.clone())))
        .await;
    let service = EmbeddingService::from_state_for_collection(state.clone(), &collection)
        .await
        .unwrap();
    service
        .embed_and_upsert(&collection, document("a", "identical content"))
        .await
        .unwrap();
    service
        .embed_and_upsert(&collection, document("b", "identical content"))
        .await
        .unwrap();
    assert_eq!(v1_calls.load(Ordering::Relaxed), 1);

    let v2_calls = Arc::new(AtomicU64::new(0));
    state
        .set_embedding_provider(Arc::new(CountingProvider::new("v2", v2_calls.clone())))
        .await;
    let service = EmbeddingService::from_state_for_collection(state.clone(), &collection)
        .await
        .unwrap();
    service
        .embed_and_upsert(&collection, document("c", "identical content"))
        .await
        .unwrap();
    assert_eq!(v2_calls.load(Ordering::Relaxed), 1);

    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn restart_starts_with_empty_derived_cache_without_losing_records() {
    let dir = temp_dir("restart");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let collection = create_collection(&state).await;
    let calls = Arc::new(AtomicU64::new(0));
    state
        .set_embedding_provider(Arc::new(CountingProvider::new("v1", calls.clone())))
        .await;
    EmbeddingService::from_state_for_collection(state.clone(), &collection)
        .await
        .unwrap()
        .embed_and_upsert(&collection, document("before", "restart content"))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    drop(state);

    let recovered = AppState::recover(&dir).unwrap();
    recovered
        .set_embedding_provider(Arc::new(CountingProvider::new("v1", calls.clone())))
        .await;
    EmbeddingService::from_state_for_collection(recovered.clone(), &collection)
        .await
        .unwrap()
        .embed_and_upsert(&collection, document("after", "restart content"))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 2);

    let response = app(recovered)
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("ketebe_embedding_cache_hits_total"));
    assert!(body.contains("ketebe_embedding_cache_misses_total"));
    assert!(body.contains("ketebe_embedding_cache_evictions_total"));

    fs::remove_dir_all(dir).unwrap();
}
