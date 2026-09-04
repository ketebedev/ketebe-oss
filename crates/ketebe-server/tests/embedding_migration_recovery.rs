use ketebe_core::{
    CollectionId, CollectionIngestionConfig, DistanceMetric, Metadata, MetadataValue, RecordId,
};
use ketebe_server::{
    AppState, CollectionService, DeterministicEmbeddingProvider, DocumentRecord, EmbeddingFuture,
    EmbeddingMigrationService, EmbeddingMigrationStatus, EmbeddingProvider,
    EmbeddingProviderRegistry, EmbeddingService, PendingRecord, RuntimeCatalog, WriteService,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static NONCE: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-migration-recovery-{}-{}",
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
        let migration = service.status(id).await.unwrap();
        match migration.status {
            EmbeddingMigrationStatus::Ready => return,
            EmbeddingMigrationStatus::Failed => panic!("migration failed: {:?}", migration.error),
            _ => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
    panic!("migration did not become ready");
}

#[tokio::test]
async fn wal_published_cutover_is_finalized_after_restart() {
    let dir = temp_dir();
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
    EmbeddingService::from_state_for_collection(state.clone(), &id)
        .await
        .unwrap()
        .embed_and_upsert(
            &id,
            DocumentRecord {
                id: RecordId::string("doc-1").unwrap(),
                text: "restart-safe cutover".to_string(),
                metadata: Metadata::new(),
            },
        )
        .await
        .unwrap();

    let migrations = EmbeddingMigrationService::new(state.clone());
    migrations.start(&id, "model-v2").await.unwrap();
    wait_ready(&migrations, &id).await;

    let target = DeterministicEmbeddingProvider::new("docs", "v2").unwrap();
    let target_vector = target.embed("restart-safe cutover", 4).await.unwrap();
    let mut provenance = BTreeMap::new();
    provenance.insert(
        "profile".to_string(),
        MetadataValue::String("model-v2".to_string()),
    );
    provenance.insert(
        "provider".to_string(),
        MetadataValue::String(target.provider_name().to_string()),
    );
    provenance.insert(
        "model".to_string(),
        MetadataValue::String(target.model().name),
    );
    provenance.insert(
        "version".to_string(),
        MetadataValue::String(target.model().version),
    );
    provenance.insert("dimension".to_string(), MetadataValue::Number(4.0));
    provenance.insert(
        "source_text".to_string(),
        MetadataValue::String("restart-safe cutover".to_string()),
    );
    let mut metadata = Metadata::new();
    metadata.insert(
        "_ketebe_embedding".to_string(),
        MetadataValue::Object(provenance),
    );
    WriteService::new(state.clone())
        .upsert(
            &id,
            PendingRecord {
                id: RecordId::string("doc-1").unwrap(),
                vector: target_vector,
                metadata,
            },
        )
        .await
        .unwrap();

    let collection_dir = dir.join("collections").join("docs");
    let state_path = collection_dir.join("embedding-migration.json");
    let mut migration: Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    migration["status"] = Value::String("activating".to_string());
    fs::write(&state_path, serde_json::to_vec_pretty(&migration).unwrap()).unwrap();
    let journal = serde_json::json!({
        "version": 1,
        "source_profile": migration["source_profile"],
        "target_profile": migration["target_profile"],
        "target_provider": migration["target_provider"],
        "target_model": migration["target_model"],
        "target_model_version": migration["target_model_version"],
        "phase": "wal_published"
    });
    fs::write(
        collection_dir.join("embedding-migration.cutover.json"),
        serde_json::to_vec_pretty(&journal).unwrap(),
    )
    .unwrap();

    drop(state);
    let recovered = AppState::recover(&dir).unwrap();
    install_profiles(&recovered).await;
    let recovered_count = EmbeddingMigrationService::new(recovered.clone())
        .recover_interrupted_cutovers()
        .await
        .unwrap();
    assert_eq!(recovered_count, 1);
    assert_eq!(
        EmbeddingMigrationService::new(recovered.clone())
            .status(&id)
            .await
            .unwrap()
            .status,
        EmbeddingMigrationStatus::Activated
    );
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

#[test]
fn embedding_future_type_remains_public() {
    let _value: Option<EmbeddingFuture<'static>> = None;
}
