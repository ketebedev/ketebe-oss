use axum::body::Body;
use ketebe_core::{
    CollectionId, CollectionIngestionConfig, DistanceMetric, SemanticChunkingPolicy, TokenizerKind,
};
use ketebe_server::{
    AppState, CollectionService, DeterministicEmbeddingProvider, EmbeddingProviderRegistry,
    RuntimeCatalog, WriteService, app,
};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-semantic-chunking-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
async fn install(state: &AppState) {
    let mut registry = EmbeddingProviderRegistry::new();
    registry
        .register(
            "semantic",
            Arc::new(DeterministicEmbeddingProvider::new("semantic-model", "v1").unwrap()),
        )
        .unwrap();
    registry.set_default("semantic").unwrap();
    state.set_embedding_provider_registry(registry).await;
}
fn policy() -> SemanticChunkingPolicy {
    SemanticChunkingPolicy::new(8, 1, 3, 700, TokenizerKind::UnicodeWordsV1).unwrap()
}

#[tokio::test]
async fn semantic_schema_persists_rest_reconciles_and_metrics_are_visible() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 1000);
    install(&state).await;
    let ingestion = CollectionIngestionConfig::new_semantic("semantic", policy(), true).unwrap();
    let id = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection_with_schema(
            id.clone(),
            4,
            DistanceMetric::Cosine,
            Vec::new(),
            Default::default(),
            Some(ingestion.clone()),
        )
        .await
        .unwrap();
    let router = app(state.clone());
    let first = axum::http::Request::builder().method("PUT").uri("/v0/collections/docs/documents/doc-1")
        .header("content-type", "application/json")
        .body(Body::from(json!({"text":"vector database retrieval search indexing. cooking banana recipe kitchen. distributed systems consensus replication storage."}).to_string())).unwrap();
    let response = router.clone().oneshot(first).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["chunking"], "semantic");
    assert!(body["chunk_count"].as_u64().unwrap() >= 2);

    let second = axum::http::Request::builder()
        .method("PUT")
        .uri("/v0/collections/docs/documents/doc-1")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"text":"short replacement document"}).to_string(),
        ))
        .unwrap();
    let response = router.clone().oneshot(second).await.unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(body["reconciled_chunks"].as_u64().unwrap() >= 1);

    let metrics = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let metrics = String::from_utf8(
        axum::body::to_bytes(metrics.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(metrics.contains("ketebe_semantic_chunking_requests_total"));
    assert!(metrics.contains("ketebe_semantic_chunking_scorer_input_tokens_total"));

    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 1000).unwrap();
    let info = CollectionService::new(recovered).get(&id).await.unwrap();
    assert_eq!(info.ingestion.as_ref(), Some(&ingestion));
    assert_eq!(info.ingestion.unwrap().semantic_chunking(), Some(policy()));
    if dir.exists() {
        fs::remove_dir_all(dir).unwrap();
    }
}
