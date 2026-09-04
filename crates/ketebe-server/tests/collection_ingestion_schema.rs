use axum::body::Body;
use ketebe_core::{
    ChunkingPolicy, CollectionId, CollectionIngestionConfig, DistanceMetric, FieldPath,
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
        "ketebe-ingestion-schema-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ))
}

async fn install_profile(state: &AppState, profile: &str) {
    let mut registry = EmbeddingProviderRegistry::new();
    registry
        .register(
            profile,
            Arc::new(DeterministicEmbeddingProvider::new("schema-model", "v1").expect("provider")),
        )
        .expect("register");
    registry.set_default(profile).expect("default");
    state.set_embedding_provider_registry(registry).await;
}

#[tokio::test]
async fn collection_ingestion_schema_is_persisted_recovered_and_applied_by_rest() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 1000);
    install_profile(&state, "docs-profile").await;

    let ingestion = CollectionIngestionConfig::new(
        "docs-profile",
        Some(ChunkingPolicy::new(5, 2).expect("chunking")),
        true,
    )
    .expect("schema");
    let id = CollectionId::new("docs").expect("id");
    let config = WriteService::new(state.clone())
        .create_collection_with_schema(
            id.clone(),
            4,
            DistanceMetric::Cosine,
            Vec::new(),
            Default::default(),
            Some(ingestion.clone()),
        )
        .await
        .expect("create");

    assert_eq!(config.ingestion(), Some(&ingestion));
    assert_eq!(
        config.lexical_fields(),
        &[FieldPath::new(["_ketebe_chunk", "text"]).expect("field")]
    );

    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 1000).expect("recover");
    let info = CollectionService::new(recovered.clone())
        .get(&id)
        .await
        .expect("info");
    assert_eq!(info.ingestion.as_ref(), Some(&ingestion));
    assert!(
        info.lexical_fields
            .contains(&FieldPath::new(["_ketebe_chunk", "text"]).expect("field"))
    );

    install_profile(&recovered, "docs-profile").await;
    let router = app(recovered.clone());
    let request = axum::http::Request::builder()
        .method("PUT")
        .uri("/v0/collections/docs/documents/doc-1")
        .header("content-type", "application/json")
        .body(Body::from(json!({"text":"abcdefghij"}).to_string()))
        .expect("request");
    let response = router.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(body["chunk_count"], 3);
    assert_eq!(body["sequence_numbers"], json!([1, 2, 3]));

    let conflicting = axum::http::Request::builder()
        .method("PUT")
        .uri("/v0/collections/docs/documents/doc-2")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "text":"abcdefghij",
                "chunking":{"max_chars":4,"overlap_chars":1}
            })
            .to_string(),
        ))
        .expect("request");
    let response = router.oneshot(conflicting).await.expect("response");
    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);

    if dir.exists() {
        fs::remove_dir_all(dir).expect("cleanup");
    }
}

#[tokio::test]
async fn collection_creation_rejects_unknown_profile() {
    let dir = temp_dir();
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let ingestion = CollectionIngestionConfig::new("missing", None, false).expect("schema");
    let error = WriteService::new(state)
        .create_collection_with_schema(
            CollectionId::new("docs").expect("id"),
            4,
            DistanceMetric::Cosine,
            Vec::new(),
            Default::default(),
            Some(ingestion),
        )
        .await
        .expect_err("unknown profile must fail");
    assert!(error.to_string().contains("not registered"));
    if dir.exists() {
        fs::remove_dir_all(dir).expect("cleanup");
    }
}
