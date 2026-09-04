use ketebe_core::{CollectionId, DistanceMetric, Metadata, RecordId};
use ketebe_server::{
    AppState, BackupService, DerivedIndexBackupPolicy, PendingRecord, RuntimeCatalog, WriteService,
};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-backup-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

async fn collection(label: &str) -> (std::path::PathBuf, AppState, CollectionId) {
    let dir = temp_dir(label);
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 1);
    let id = CollectionId::new("docs").unwrap();
    let writes = WriteService::new(state.clone());
    writes
        .create_collection(id.clone(), 2, DistanceMetric::Cosine, Vec::new())
        .await
        .unwrap();
    writes
        .upsert(
            &id,
            PendingRecord {
                id: RecordId::string("a").unwrap(),
                vector: vec![1.0, 0.0],
                metadata: Metadata::new(),
            },
        )
        .await
        .unwrap();
    (dir, state, id)
}

#[tokio::test]
async fn backup_manifest_is_versioned_verified_and_excludes_derived_indexes() {
    let (dir, state, id) = collection("manifest").await;
    // Recovery materializes the rebuildable HNSW snapshot; it must not enter backup contents.
    drop(AppState::recover(&dir).unwrap());
    assert!(dir.join("collections/docs/indexes/hnsw.kthi").exists());

    let manifest = BackupService::new(state).create(&id).await.unwrap();
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.collection_id, "docs");
    assert_eq!(
        manifest.derived_index_policy,
        DerivedIndexBackupPolicy::Rebuild
    );
    assert!(manifest.snapshot_sequence >= 1);
    assert!(
        manifest
            .files
            .iter()
            .any(|file| file.path.ends_with("collection.json"))
    );
    assert!(
        manifest
            .files
            .iter()
            .any(|file| file.path.ends_with("wal.log"))
    );
    assert!(
        !manifest
            .files
            .iter()
            .any(|file| file.path.contains("/indexes/"))
    );

    let root = dir.join("backups").join(&manifest.backup_id);
    assert!(root.join("manifest.json").exists());
    assert!(!root.join("collections/docs/indexes").exists());
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn completed_backup_is_immutable_after_later_collection_writes() {
    let (dir, state, id) = collection("boundary").await;
    let manifest = BackupService::new(state.clone()).create(&id).await.unwrap();
    let backup_wal = dir
        .join("backups")
        .join(&manifest.backup_id)
        .join("collections/docs/wal.log");
    let before = fs::read(&backup_wal).unwrap();

    WriteService::new(state)
        .upsert(
            &id,
            PendingRecord {
                id: RecordId::string("b").unwrap(),
                vector: vec![0.0, 1.0],
                metadata: Metadata::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(fs::read(&backup_wal).unwrap(), before);
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn unknown_collection_lifecycle_metadata_is_preserved_but_indexes_are_not() {
    let (dir, state, id) = collection("metadata").await;
    let collection_dir = dir.join("collections/docs");
    fs::write(
        collection_dir.join("future-lifecycle.json"),
        b"{\"generation\":7}",
    )
    .unwrap();
    fs::create_dir_all(collection_dir.join("indexes/custom")).unwrap();
    fs::write(
        collection_dir.join("indexes/custom/derived.bin"),
        b"derived",
    )
    .unwrap();

    let manifest = BackupService::new(state).create(&id).await.unwrap();
    let root = dir.join("backups").join(&manifest.backup_id);
    assert_eq!(
        fs::read(root.join("collections/docs/future-lifecycle.json")).unwrap(),
        b"{\"generation\":7}"
    );
    assert!(
        !root
            .join("collections/docs/indexes/custom/derived.bin")
            .exists()
    );
    fs::remove_dir_all(dir).unwrap();
}
