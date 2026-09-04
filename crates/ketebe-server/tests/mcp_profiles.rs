use ketebe_mcp::{ketebe::KetebeApi, profiles::ProfileParams};
use ketebe_server::{
    AppState, DeterministicEmbeddingProvider, EmbeddingProviderRegistry, RerankCandidate,
    RerankFuture, RerankScore, Reranker, RerankerRegistry, RuntimeCatalog, app,
};
use std::{
    fs,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;

struct TestReranker;

impl Reranker for TestReranker {
    fn name(&self) -> &str {
        "test-reranker"
    }

    fn rerank<'a>(
        &'a self,
        _query: &'a str,
        _candidates: &'a [RerankCandidate],
    ) -> RerankFuture<'a> {
        Box::pin(async { Ok(Vec::<RerankScore>::new()) })
    }
}

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-profiles-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

#[tokio::test]
async fn mcp_profile_discovery_uses_safe_public_metadata() {
    let dir = temp_dir();
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());

    let mut embeddings = EmbeddingProviderRegistry::new();
    embeddings
        .register(
            "default",
            Arc::new(
                DeterministicEmbeddingProvider::new("embed-default", "v1")
                    .expect("embedding provider"),
            ),
        )
        .expect("register default embedding profile");
    embeddings
        .register(
            "next",
            Arc::new(
                DeterministicEmbeddingProvider::new("embed-next", "v2")
                    .expect("embedding provider"),
            ),
        )
        .expect("register next embedding profile");
    embeddings
        .set_default("default")
        .expect("set default embedding profile");
    state.set_embedding_provider_registry(embeddings).await;

    let mut rerankers = RerankerRegistry::new();
    rerankers
        .register("default", Arc::new(TestReranker))
        .expect("register reranker profile");
    rerankers
        .set_default("default")
        .expect("set default reranker profile");
    state.set_reranker_registry(rerankers).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app(state)).await.expect("server");
    });

    let api = KetebeApi::new(format!("http://{address}")).expect("MCP API adapter");

    let embeddings = api
        .list_embedding_profiles(None)
        .await
        .expect("list embedding profiles");
    assert_eq!(embeddings.profiles.len(), 2);
    assert_eq!(embeddings.profiles[0].profile, "default");
    assert!(embeddings.profiles[0].is_default);
    assert_eq!(embeddings.profiles[1].profile, "next");

    let next = api
        .describe_embedding_profile(
            ProfileParams {
                profile: "next".to_string(),
            },
            None,
        )
        .await
        .expect("describe embedding profile");
    assert_eq!(next.model, "embed-next");
    assert_eq!(next.model_version, "v2");

    let rerankers = api
        .list_reranker_profiles(None)
        .await
        .expect("list reranker profiles");
    assert_eq!(rerankers.profiles.len(), 1);
    assert_eq!(rerankers.profiles[0].provider, "test-reranker");
    assert!(rerankers.profiles[0].is_default);

    let safe = serde_json::to_value(&next).expect("serialize profile");
    let object = safe.as_object().expect("profile object");
    for forbidden in ["endpoint", "api_key", "api_key_ref", "credentials"] {
        assert!(!object.contains_key(forbidden), "{forbidden}");
    }

    server.abort();
    let _ = fs::remove_dir_all(dir);
}
