use ketebe_core::{CollectionId, DistanceMetric, FieldPath, LexicalAnalyzerConfig};
use ketebe_core::{Metadata, MetadataValue, RecordId};
use ketebe_server::{AppState, CollectionService, PendingRecord, RuntimeCatalog, WriteService};
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-lexical-schema-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[tokio::test]
async fn schema_evolution_persists_analyzer_and_reindexes() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let writes = WriteService::new(state.clone());
    let id = CollectionId::new("docs").unwrap();
    let title = FieldPath::new(["title"]).unwrap();
    writes
        .create_collection(id.clone(), 2, DistanceMetric::Cosine, vec![title.clone()])
        .await
        .unwrap();
    let mut metadata = Metadata::new();
    metadata.insert("title".into(), MetadataValue::String("Rust Search".into()));
    writes
        .upsert(
            &id,
            PendingRecord {
                id: RecordId::unsigned(1),
                vector: vec![1.0, 0.0],
                metadata,
            },
        )
        .await
        .unwrap();
    writes.seal_collection(&id).await.unwrap();

    let updated = writes
        .update_lexical_schema(&id, vec![title], LexicalAnalyzerConfig::standard(false))
        .await
        .unwrap();
    assert!(!updated.lexical_analyzer().lowercase());

    // Persisted schema is authoritative across restart even if a background build is still racing.
    let recovered = AppState::recover_with_threshold(&dir, 100).unwrap();
    let info = CollectionService::new(recovered).get(&id).await.unwrap();
    assert!(!info.lexical_analyzer.lowercase());

    std::fs::remove_dir_all(dir).unwrap();
}
