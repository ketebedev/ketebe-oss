use axum::Json;
use axum::Router;
use axum::http::HeaderMap;
use axum::routing::post;
use ketebe_server::{
    DeterministicEmbeddingProvider, EmbeddingBatchFuture, EmbeddingFuture, EmbeddingModel,
    EmbeddingProvider, EmbeddingProviderError, EmbeddingProviderRegistry,
    OpenAiCompatibleEmbeddingConfig, OpenAiCompatibleEmbeddingProvider, SecretRef,
    embed_texts_batched, embedding_prometheus_metrics,
};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::test]
async fn registry_resolves_default_profile() {
    let mut registry = EmbeddingProviderRegistry::new();
    registry
        .register(
            "local",
            Arc::new(DeterministicEmbeddingProvider::new("test", "v1").unwrap()),
        )
        .unwrap();
    registry.set_default("local").unwrap();
    let provider = registry.default_provider().expect("default provider");
    assert_eq!(provider.provider_name(), "deterministic");
    assert_eq!(provider.model().name, "test");
}

#[tokio::test]
async fn openai_compatible_provider_uses_bearer_auth_and_validates_dimension() {
    async fn embeddings(headers: HeaderMap, Json(body): Json<Value>) -> Json<Value> {
        assert_eq!(
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer secret-token")
        );
        assert_eq!(body["model"], "embedding-model");
        assert_eq!(body["dimensions"], 3);
        assert_eq!(body["input"], json!(["hello"]));
        Json(json!({
            "data": [{"index": 0, "embedding": [0.1, 0.2, 0.3]}]
        }))
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/v1/embeddings", post(embeddings)),
        )
        .await
        .unwrap();
    });

    let secret_dir = std::env::temp_dir().join(format!(
        "ketebe-embedding-provider-secret-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&secret_dir);
    std::fs::create_dir_all(&secret_dir).unwrap();
    let secret_path = secret_dir.join("api-key");
    std::fs::write(&secret_path, "secret-token\n").unwrap();

    let provider = OpenAiCompatibleEmbeddingProvider::new(OpenAiCompatibleEmbeddingConfig {
        endpoint: format!("http://{address}/v1/embeddings"),
        model: "embedding-model".to_string(),
        model_version: "2026-08".to_string(),
        dimension: 3,
        api_key_ref: Some(SecretRef::file(&secret_path).unwrap()),
        timeout: Duration::from_secs(2),
        max_retries: 1,
        retry_backoff: Duration::from_millis(1),
        max_concurrency: 2,
    })
    .unwrap();

    let vector = EmbeddingProvider::embed(&provider, "hello", 3)
        .await
        .unwrap();
    assert_eq!(vector, vec![0.1, 0.2, 0.3]);
    assert_eq!(
        EmbeddingProvider::provider_name(&provider),
        "openai-compatible"
    );

    let error = EmbeddingProvider::embed(&provider, "hello", 2)
        .await
        .expect_err("dimension mismatch");
    assert!(
        error
            .to_string()
            .contains("does not match collection dimension")
    );

    server.abort();
    let _ = std::fs::remove_dir_all(secret_dir);
}

#[tokio::test]
async fn openai_compatible_provider_sends_native_batch_and_restores_input_order() {
    async fn embeddings(Json(body): Json<Value>) -> Json<Value> {
        assert_eq!(body["input"], json!(["a", "b", "c"]));
        Json(json!({
            "data": [
                {"index": 2, "embedding": [2.0, 2.0, 2.0]},
                {"index": 0, "embedding": [0.0, 0.0, 0.0]},
                {"index": 1, "embedding": [1.0, 1.0, 1.0]}
            ]
        }))
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/v1/embeddings", post(embeddings)),
        )
        .await
        .unwrap();
    });
    let provider = OpenAiCompatibleEmbeddingProvider::new(OpenAiCompatibleEmbeddingConfig {
        endpoint: format!("http://{address}/v1/embeddings"),
        model: "embedding-model".to_string(),
        model_version: "2026-08".to_string(),
        dimension: 3,
        api_key_ref: None,
        timeout: Duration::from_secs(2),
        max_retries: 0,
        retry_backoff: Duration::from_millis(1),
        max_concurrency: 2,
    })
    .unwrap();

    let inputs = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let vectors = EmbeddingProvider::embed_batch(&provider, &inputs, 3)
        .await
        .unwrap();
    assert_eq!(vectors[0], vec![0.0, 0.0, 0.0]);
    assert_eq!(vectors[1], vec![1.0, 1.0, 1.0]);
    assert_eq!(vectors[2], vec![2.0, 2.0, 2.0]);
    assert_eq!(EmbeddingProvider::max_batch_inputs(&provider), 128);
    server.abort();
}

#[derive(Clone)]
struct BatchProbeProvider {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl BatchProbeProvider {
    fn record_peak(&self, value: usize) {
        let mut current = self.peak.load(Ordering::SeqCst);
        while value > current {
            match self
                .peak
                .compare_exchange(current, value, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
}

impl EmbeddingProvider for BatchProbeProvider {
    fn provider_name(&self) -> &str {
        "batch-probe"
    }

    fn model(&self) -> EmbeddingModel {
        EmbeddingModel::new("batch-probe", "v1").unwrap()
    }

    fn max_batch_inputs(&self) -> usize {
        2
    }

    fn max_concurrent_batches(&self) -> usize {
        2
    }

    fn embed<'a>(&'a self, _text: &'a str, expected_dimension: usize) -> EmbeddingFuture<'a> {
        Box::pin(async move { Ok(vec![0.0; expected_dimension]) })
    }

    fn embed_batch<'a>(
        &'a self,
        texts: &'a [String],
        expected_dimension: usize,
    ) -> EmbeddingBatchFuture<'a> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.record_peak(active);
            tokio::time::sleep(Duration::from_millis(15)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(texts
                .iter()
                .map(|text| vec![text.len() as f32; expected_dimension])
                .collect())
        })
    }
}

#[tokio::test]
async fn batch_executor_preserves_order_and_bounds_parallel_batches() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let provider: Arc<dyn EmbeddingProvider> = Arc::new(BatchProbeProvider {
        active,
        peak: peak.clone(),
    });
    let inputs = ["a", "bb", "ccc", "dddd", "eeeee", "ffffff"]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();

    let vectors = embed_texts_batched(provider, &inputs, 2).await.unwrap();
    assert_eq!(vectors.len(), inputs.len());
    assert_eq!(vectors[0], vec![1.0, 1.0]);
    assert_eq!(vectors[5], vec![6.0, 6.0]);
    assert_eq!(peak.load(Ordering::SeqCst), 2);

    let metrics = embedding_prometheus_metrics();
    assert!(metrics.contains("ketebe_embedding_provider_batch_requests_total"));
    assert!(metrics.contains("ketebe_embedding_provider_batch_inputs_total"));
}

#[tokio::test]
async fn providers_without_native_batching_use_safe_sequential_fallback() {
    let provider: Arc<dyn EmbeddingProvider> =
        Arc::new(DeterministicEmbeddingProvider::new("fallback", "v1").unwrap());
    assert_eq!(provider.max_batch_inputs(), 1);
    let inputs = vec!["one".to_string(), "two".to_string(), "three".to_string()];
    let vectors = embed_texts_batched(provider, &inputs, 4).await.unwrap();
    assert_eq!(vectors.len(), 3);
    assert!(vectors.iter().all(|vector| vector.len() == 4));
}

#[allow(dead_code)]
fn _provider_error_type_is_public(error: EmbeddingProviderError) -> String {
    error.to_string()
}
