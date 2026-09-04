use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use ketebe_core::{
    ChunkingPolicy, CollectionId, CollectionIngestionConfig, DistanceMetric, Metadata, RecordId,
};
use ketebe_server::{
    AppState, ChunkedDocument, ChunkingConfig, ChunkingService, CollectionService,
    DeterministicEmbeddingProvider, DocumentRecord, EmbeddingFuture, EmbeddingMigrationService,
    EmbeddingMigrationStatus, EmbeddingProvider, EmbeddingProviderError, EmbeddingProviderRegistry,
    EmbeddingService, RuntimeCatalog, WriteService, app,
};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

static NONCE: AtomicU64 = AtomicU64::new(0);

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-migration-{label}-{}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos(),
        NONCE.fetch_add(1, Ordering::Relaxed),
    ))
}

async fn install_profiles(state: &AppState) {
    let mut registry = EmbeddingProviderRegistry::new();
    registry
        .register(
            "model-v1",
            Arc::new(DeterministicEmbeddingProvider::new("docs", "v1").unwrap()),
        )
        .unwrap();
    registry
        .register(
            "model-v2",
            Arc::new(DeterministicEmbeddingProvider::new("docs", "v2").unwrap()),
        )
        .unwrap();
    registry.set_default("model-v1").unwrap();
    state.set_embedding_provider_registry(registry).await;
}

async fn wait_ready(service: &EmbeddingMigrationService, id: &CollectionId) {
    for _ in 0..100 {
        let state = service.status(id).await.unwrap();
        match state.status {
            EmbeddingMigrationStatus::Ready => return,
            EmbeddingMigrationStatus::Failed => panic!("migration failed: {:?}", state.error),
            _ => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
    panic!("migration did not become ready");
}

#[tokio::test]
async fn migration_stages_then_activates_without_changing_active_profile_early() {
    let dir = temp_dir("activate");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    install_profiles(&state).await;
    let id = CollectionId::new("docs").unwrap();
    let ingestion =
        CollectionIngestionConfig::new("model-v1", Some(ChunkingPolicy::new(5, 2).unwrap()), true)
            .unwrap();
    WriteService::new(state.clone())
        .create_collection_with_schema(
            id.clone(),
            4,
            DistanceMetric::Cosine,
            Vec::new(),
            Default::default(),
            Some(ingestion),
        )
        .await
        .unwrap();
    ChunkingService::new(state.clone())
        .chunk_embed_and_upsert(
            &id,
            ChunkedDocument {
                id: RecordId::string("parent").unwrap(),
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

    let migrations = EmbeddingMigrationService::new(state.clone());
    let started = migrations.start(&id, "model-v2").await.unwrap();
    assert_eq!(started.status, EmbeddingMigrationStatus::Running);
    wait_ready(&migrations, &id).await;

    let before = CollectionService::new(state.clone())
        .get(&id)
        .await
        .unwrap();
    assert_eq!(
        before.ingestion.unwrap().embedding_profile(),
        "model-v1",
        "staged vectors must not change active ingestion profile"
    );

    let activated = migrations.activate(&id).await.unwrap();
    assert_eq!(activated.status, EmbeddingMigrationStatus::Activated);
    assert_eq!(activated.completed_records, 3);
    let after = CollectionService::new(state.clone())
        .get(&id)
        .await
        .unwrap();
    assert_eq!(after.ingestion.unwrap().embedding_profile(), "model-v2");

    drop(state);
    let recovered = AppState::recover(&dir).unwrap();
    let recovered_status = EmbeddingMigrationService::new(recovered.clone())
        .status(&id)
        .await
        .unwrap();
    assert_eq!(recovered_status.status, EmbeddingMigrationStatus::Activated);
    assert_eq!(
        CollectionService::new(recovered)
            .get(&id)
            .await
            .unwrap()
            .ingestion
            .unwrap()
            .embedding_profile(),
        "model-v2"
    );
    fs::remove_dir_all(dir).unwrap();
}

#[derive(Clone)]
struct FixedThree;
impl EmbeddingProvider for FixedThree {
    fn provider_name(&self) -> &str {
        "fixed-three"
    }
    fn model(&self) -> ketebe_server::EmbeddingModel {
        ketebe_server::EmbeddingModel::new("bad", "v1").unwrap()
    }
    fn fixed_dimension(&self) -> Option<usize> {
        Some(3)
    }
    fn embed<'a>(&'a self, _text: &'a str, _expected_dimension: usize) -> EmbeddingFuture<'a> {
        Box::pin(async { Ok(vec![1.0; 3]) })
    }
}

#[tokio::test]
async fn incompatible_target_dimension_is_rejected_before_migration_state_is_created() {
    let dir = temp_dir("dimension");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let mut registry = EmbeddingProviderRegistry::new();
    registry
        .register(
            "model-v1",
            Arc::new(DeterministicEmbeddingProvider::new("docs", "v1").unwrap()),
        )
        .unwrap();
    registry.register("bad", Arc::new(FixedThree)).unwrap();
    registry.set_default("model-v1").unwrap();
    state.set_embedding_provider_registry(registry).await;
    let id = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection_with_schema(
            id.clone(),
            4,
            DistanceMetric::Cosine,
            Vec::new(),
            Default::default(),
            Some(CollectionIngestionConfig::new("model-v1", None, false).unwrap()),
        )
        .await
        .unwrap();
    let error = EmbeddingMigrationService::new(state)
        .start(&id, "bad")
        .await
        .expect_err("dimension mismatch");
    assert!(error.to_string().contains("expected 4, got 3"));
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn rest_management_surface_exposes_migration_status() {
    let dir = temp_dir("rest");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    install_profiles(&state).await;
    let id = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection_with_schema(
            id,
            4,
            DistanceMetric::Cosine,
            Vec::new(),
            Default::default(),
            Some(CollectionIngestionConfig::new("model-v1", None, false).unwrap()),
        )
        .await
        .unwrap();

    let request = Request::post("/v0/collections/docs/embedding-migration")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"target_profile":"model-v2"}"#))
        .unwrap();
    let response = app(state.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["target_profile"], "model-v2");

    let migrations = EmbeddingMigrationService::new(state.clone());
    wait_ready(&migrations, &CollectionId::new("docs").unwrap()).await;
    let request = Request::get("/v0/collections/docs/embedding-migration")
        .body(Body::empty())
        .unwrap();
    let response = app(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["status"], "ready");
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn provider_error_type_stays_constructible() {
    let error = EmbeddingProviderError::new("provider error");
    assert_eq!(error.to_string(), "provider error");
}

#[tokio::test]
async fn catch_up_reconciles_updates_additions_and_deletes_before_activation() {
    let dir = temp_dir("catch-up");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    install_profiles(&state).await;
    let id = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection_with_schema(
            id.clone(),
            4,
            DistanceMetric::Cosine,
            Vec::new(),
            Default::default(),
            Some(CollectionIngestionConfig::new("model-v1", None, false).unwrap()),
        )
        .await
        .unwrap();
    let embedding = EmbeddingService::from_state_for_collection(state.clone(), &id)
        .await
        .unwrap();
    embedding
        .embed_and_upsert(
            &id,
            DocumentRecord {
                id: RecordId::string("a").unwrap(),
                text: "old-a".to_string(),
                metadata: Metadata::new(),
            },
        )
        .await
        .unwrap();
    embedding
        .embed_and_upsert(
            &id,
            DocumentRecord {
                id: RecordId::string("delete-me").unwrap(),
                text: "delete-me".to_string(),
                metadata: Metadata::new(),
            },
        )
        .await
        .unwrap();

    let migrations = EmbeddingMigrationService::new(state.clone());
    migrations.start(&id, "model-v2").await.unwrap();
    wait_ready(&migrations, &id).await;

    embedding
        .embed_and_upsert(
            &id,
            DocumentRecord {
                id: RecordId::string("a").unwrap(),
                text: "new-a".to_string(),
                metadata: Metadata::new(),
            },
        )
        .await
        .unwrap();
    embedding
        .embed_and_upsert(
            &id,
            DocumentRecord {
                id: RecordId::string("b").unwrap(),
                text: "new-b".to_string(),
                metadata: Metadata::new(),
            },
        )
        .await
        .unwrap();
    WriteService::new(state.clone())
        .delete(&id, RecordId::string("delete-me").unwrap())
        .await
        .unwrap();

    let caught = migrations.catch_up(&id).await.unwrap();
    assert_eq!(caught.status, EmbeddingMigrationStatus::Ready);
    assert_eq!(caught.total_managed_records, 2);
    assert_eq!(caught.completed_records, 2);
    assert_eq!(caught.catch_up_runs, 1);
    assert!(caught.reconciled_records >= 3);

    let activated = migrations.activate(&id).await.unwrap();
    assert_eq!(activated.status, EmbeddingMigrationStatus::Activated);
    assert_eq!(
        CollectionService::new(state.clone())
            .get(&id)
            .await
            .unwrap()
            .ingestion
            .unwrap()
            .embedding_profile(),
        "model-v2"
    );

    let response = app(state)
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let metrics = String::from_utf8(body.to_vec()).unwrap();
    assert!(metrics.contains("ketebe_embedding_migration_catch_up_runs_total"));
    assert!(metrics.contains("ketebe_embedding_migration_reconciled_records_total"));
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn rest_catch_up_endpoint_is_available() {
    let dir = temp_dir("catch-up-rest");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    install_profiles(&state).await;
    let id = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection_with_schema(
            id.clone(),
            4,
            DistanceMetric::Cosine,
            Vec::new(),
            Default::default(),
            Some(CollectionIngestionConfig::new("model-v1", None, false).unwrap()),
        )
        .await
        .unwrap();
    EmbeddingMigrationService::new(state.clone())
        .start(&id, "model-v2")
        .await
        .unwrap();
    wait_ready(&EmbeddingMigrationService::new(state.clone()), &id).await;
    let response = app(state)
        .oneshot(
            Request::post("/v0/collections/docs/embedding-migration/catch-up")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    fs::remove_dir_all(dir).unwrap();
}
