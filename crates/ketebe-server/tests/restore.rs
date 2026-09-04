use ketebe_core::{CollectionId, DistanceMetric, Metadata, RecordId};
use ketebe_server::{
    AppState, BackupError, BackupService, IntegrityVerifier, LocalBackupRepository, PendingRecord,
    RuntimeCatalog, WriteService,
};
use std::fs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-restore-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

async fn source_backup(label: &str) -> (std::path::PathBuf, String) {
    let source = temp_dir(&format!("source-{label}"));
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), source.clone(), 1);
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
    let manifest = BackupService::new(state).create(&id).await.unwrap();
    (source, manifest.backup_id)
}

#[tokio::test]
async fn verified_backup_restores_into_empty_runtime_and_becomes_queryable_state() {
    let (source, backup_id) = source_backup("roundtrip").await;
    let target = temp_dir("target-roundtrip");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), target.clone());
    let repository = Arc::new(LocalBackupRepository::new(source.join("backups")));
    let result = BackupService::with_repository(state.clone(), repository)
        .restore(&backup_id)
        .await
        .unwrap();

    assert_eq!(result.collection_id, "docs");
    assert!(target.join("collections/docs/collection.json").exists());
    let id = CollectionId::new("docs").unwrap();
    let report = IntegrityVerifier::new(&target)
        .verify_collection(&id)
        .unwrap();
    assert!(report.authoritative_ok);
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
    fs::remove_dir_all(source).unwrap();
    fs::remove_dir_all(target).unwrap();
}

#[tokio::test]
async fn checksum_failure_never_publishes_restore_target() {
    let (source, backup_id) = source_backup("checksum").await;
    let backup_root = source.join("backups").join(&backup_id);
    let metadata = backup_root.join("collections/docs/collection.json");
    let mut bytes = fs::read(&metadata).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    fs::write(&metadata, bytes).unwrap();

    let target = temp_dir("target-checksum");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), target.clone());
    let repository = Arc::new(LocalBackupRepository::new(source.join("backups")));
    let error = BackupService::with_repository(state, repository)
        .restore(&backup_id)
        .await
        .unwrap_err();
    assert!(matches!(error, BackupError::ChecksumMismatch(_)));
    assert!(!target.join("collections/docs").exists());
    fs::remove_dir_all(source).unwrap();
    if target.exists() {
        fs::remove_dir_all(target).unwrap();
    }
}

#[tokio::test]
async fn restore_rejects_non_empty_target_without_replacing_existing_collection() {
    let (source, backup_id) = source_backup("occupied").await;
    let target = temp_dir("target-occupied");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), target.clone());
    let id = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection(id, 3, DistanceMetric::Dot, Vec::new())
        .await
        .unwrap();
    let before = fs::read(target.join("collections/docs/collection.json")).unwrap();

    let repository = Arc::new(LocalBackupRepository::new(source.join("backups")));
    let error = BackupService::with_repository(state, repository)
        .restore(&backup_id)
        .await
        .unwrap_err();
    assert!(matches!(error, BackupError::TargetNotEmpty(_)));
    assert_eq!(
        fs::read(target.join("collections/docs/collection.json")).unwrap(),
        before
    );
    fs::remove_dir_all(source).unwrap();
    fs::remove_dir_all(target).unwrap();
}

#[tokio::test]
async fn unsupported_manifest_version_is_rejected_before_target_publication() {
    let (source, backup_id) = source_backup("version").await;
    let manifest = source
        .join("backups")
        .join(&backup_id)
        .join("manifest.json");
    let mut json: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    json["version"] = serde_json::json!(99);
    fs::write(&manifest, serde_json::to_vec_pretty(&json).unwrap()).unwrap();

    let target = temp_dir("target-version");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), target.clone());
    let repository = Arc::new(LocalBackupRepository::new(source.join("backups")));
    let error = BackupService::with_repository(state, repository)
        .restore(&backup_id)
        .await
        .unwrap_err();
    assert!(matches!(error, BackupError::UnsupportedManifestVersion(99)));
    assert!(!target.join("collections/docs").exists());
    fs::remove_dir_all(source).unwrap();
    if target.exists() {
        fs::remove_dir_all(target).unwrap();
    }
}

#[tokio::test]
async fn interrupted_staging_namespaces_are_ignored_by_restart_recovery() {
    let dir = temp_dir("interrupted-staging");
    let restore_ghost = dir.join(".restore-staging/interrupted/collections/ghost");
    let backup_ghost = dir.join(".backup-staging/interrupted/collections/ghost");
    fs::create_dir_all(&restore_ghost).unwrap();
    fs::create_dir_all(&backup_ghost).unwrap();
    fs::write(restore_ghost.join("collection.json"), b"not-valid-json").unwrap();
    fs::write(backup_ghost.join("collection.json"), b"not-valid-json").unwrap();

    let state = AppState::recover(&dir).unwrap();
    let id = CollectionId::new("ghost").unwrap();
    WriteService::new(state)
        .create_collection(id, 2, DistanceMetric::Cosine, Vec::new())
        .await
        .unwrap();

    assert!(dir.join("collections/ghost/collection.json").exists());
    assert!(restore_ghost.join("collection.json").exists());
    assert!(backup_ghost.join("collection.json").exists());
    fs::remove_dir_all(dir).unwrap();
}
