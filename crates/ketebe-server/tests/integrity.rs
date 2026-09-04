use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use ketebe_core::{CollectionId, DistanceMetric, Metadata, RecordId};
use ketebe_server::{
    AppState, IntegrityClass, IntegrityOutcome, IntegrityStatus, IntegrityVerifier, PendingRecord,
    RuntimeCatalog, WriteService, app,
};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-integrity-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

async fn sealed_collection(label: &str) -> (std::path::PathBuf, CollectionId) {
    let dir = temp_dir(label);
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 1);
    let id = CollectionId::new("docs").unwrap();
    let writes = WriteService::new(state);
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
    (dir, id)
}

#[tokio::test]
async fn healthy_collection_has_no_authoritative_errors_and_json_endpoint_matches() {
    let (dir, id) = sealed_collection("healthy").await;
    let report = IntegrityVerifier::new(dir.clone())
        .verify_collection(&id)
        .unwrap();
    assert!(report.authoritative_ok);
    assert_ne!(report.status, IntegrityStatus::Corrupt);
    assert!(!report.checks.iter().any(|check| {
        check.class == IntegrityClass::Authoritative && check.outcome == IntegrityOutcome::Error
    }));

    let state = AppState::recover(&dir).unwrap();
    let response = app(state)
        .oneshot(
            Request::get("/v0/collections/docs/integrity")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["collection_id"], "docs");
    assert_eq!(json["authoritative_ok"], true);
    assert!(json["checks"].is_array());
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn wal_checksum_corruption_is_authoritative_corruption() {
    let (dir, id) = sealed_collection("wal").await;
    let wal = dir.join("collections/docs/wal.log");
    // Add an uncheckpointed durable write so the WAL contains a complete frame.
    let state = AppState::recover_with_threshold(&dir, 1000).unwrap();
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
    let mut bytes = fs::read(&wal).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x7f;
    fs::write(&wal, bytes).unwrap();

    let report = IntegrityVerifier::new(dir.clone())
        .verify_collection(&id)
        .unwrap();
    assert_eq!(report.status, IntegrityStatus::Corrupt);
    assert!(!report.authoritative_ok);
    assert!(report.checks.iter().any(|check| {
        check.code == "wal_decode"
            && check.class == IntegrityClass::Authoritative
            && check.outcome == IntegrityOutcome::Error
    }));
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn missing_checkpoint_segment_is_detected() {
    let (dir, id) = sealed_collection("missing-segment").await;
    let segment_dir = dir.join("collections/docs/segments");
    let segment = fs::read_dir(&segment_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().and_then(|value| value.to_str()) == Some("kseg"))
        .unwrap();
    fs::remove_file(segment).unwrap();

    let report = IntegrityVerifier::new(dir.clone())
        .verify_collection(&id)
        .unwrap();
    assert_eq!(report.status, IntegrityStatus::Corrupt);
    assert!(
        report
            .checks
            .iter()
            .any(|check| check.code == "missing_segment_reference")
    );
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn corrupt_hnsw_is_degraded_not_authoritative_corruption() {
    let (dir, id) = sealed_collection("derived").await;
    // Recovery materializes/rebuilds the checkpoint-scoped HNSW snapshot.
    drop(AppState::recover(&dir).unwrap());
    let hnsw = dir.join("collections/docs/indexes/hnsw.kthi");
    let mut bytes = fs::read(&hnsw).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x55;
    fs::write(&hnsw, bytes).unwrap();

    let report = IntegrityVerifier::new(dir.clone())
        .verify_collection(&id)
        .unwrap();
    assert_eq!(report.status, IntegrityStatus::Degraded);
    assert!(report.authoritative_ok);
    assert!(!report.derived_ok);
    assert!(report.checks.iter().any(|check| {
        check.code == "hnsw_compatibility"
            && check.class == IntegrityClass::Derived
            && check.outcome == IntegrityOutcome::Error
    }));
    fs::remove_dir_all(dir).unwrap();
}
