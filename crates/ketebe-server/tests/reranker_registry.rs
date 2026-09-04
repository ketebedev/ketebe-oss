use ketebe_server::{
    AppState, RerankCandidate, RerankFuture, RerankScore, Reranker, RerankerRegistry,
    RuntimeCatalog,
};
use std::sync::Arc;

struct NoopReranker(&'static str);

impl Reranker for NoopReranker {
    fn name(&self) -> &str {
        self.0
    }

    fn rerank<'a>(
        &'a self,
        _query: &'a str,
        candidates: &'a [RerankCandidate],
    ) -> RerankFuture<'a> {
        Box::pin(async move {
            Ok(candidates
                .iter()
                .enumerate()
                .map(|(index, _)| RerankScore { index, score: 0.0 })
                .collect())
        })
    }
}

#[tokio::test]
async fn registry_is_runtime_only_and_profiles_are_replaceable() {
    let state = AppState::new(RuntimeCatalog::empty_ready());
    state
        .set_reranker(Arc::new(NoopReranker("default-a")))
        .await;

    let mut registry = RerankerRegistry::new();
    registry
        .register("quality", Arc::new(NoopReranker("quality-b")))
        .unwrap();
    registry.set_default("quality").unwrap();
    state.set_reranker_registry(registry).await;

    state.clear_reranker().await;

    let mut duplicate = RerankerRegistry::new();
    duplicate
        .register("quality", Arc::new(NoopReranker("one")))
        .unwrap();
    assert!(
        duplicate
            .register("quality", Arc::new(NoopReranker("two")))
            .is_err()
    );
}
