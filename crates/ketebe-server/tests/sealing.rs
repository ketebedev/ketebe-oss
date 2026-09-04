use axum::body::{Body, to_bytes};
use axum::http::{Request, header};
use ketebe_core::{CollectionId, DistanceMetric, Metadata, RecordId};
use ketebe_server::{AppState, PendingRecord, RuntimeCatalog, WriteService, app};
use ketebe_storage::{Checkpoint, CheckpointStore, Segment, SegmentId, SegmentStore, Wal};
use serde_json::Value;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ))
}

fn pending(id: &str, value: f32) -> PendingRecord {
    PendingRecord {
        id: RecordId::string(id).expect("record ID"),
        vector: vec![value],
        metadata: Metadata::new(),
    }
}

async fn query(state: AppState, collection: &str) -> Value {
    let request = Request::post(format!("/v0/collections/{collection}/query"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"vector":[0.0],"metric":"l2","top_k":10,"execution":"exact"}"#,
        ))
        .expect("request");
    let response = app(state).oneshot(request).await.expect("response");
    assert!(response.status().is_success());
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("JSON")
}

#[tokio::test]
async fn explicit_seal_preserves_query_semantics_and_restart_recovery() {
    let dir = temp_dir("explicit-seal");
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create");

    service
        .upsert(&collection, pending("a", 1.0))
        .await
        .expect("upsert a1");
    service
        .upsert(&collection, pending("a", 2.0))
        .await
        .expect("upsert a2");
    service
        .upsert(&collection, pending("b", 3.0))
        .await
        .expect("upsert b");
    service
        .delete(&collection, RecordId::string("b").expect("ID"))
        .await
        .expect("delete b");

    let before = query(state.clone(), "docs").await;
    assert_eq!(before["hits"].as_array().expect("hits").len(), 1);
    assert_eq!(before["hits"][0]["id"]["value"], "a");
    assert_eq!(before["hits"][0]["sequence_number"], 2);

    let checkpoint = service
        .seal_collection(&collection)
        .await
        .expect("seal")
        .expect("checkpoint");
    assert_eq!(checkpoint.sequence_number().get(), 4);
    assert_eq!(checkpoint.segments().len(), 1);

    let collection_dir = dir.join("collections/docs");
    assert!(collection_dir.join("checkpoint.ktcp").exists());
    assert!(
        Wal::open(collection_dir.join("wal.log"))
            .expect("wal")
            .replay()
            .expect("replay")
            .entries
            .is_empty()
    );

    let after = query(state.clone(), "docs").await;
    assert_eq!(after["hits"], before["hits"]);
    assert!(
        service
            .seal_collection(&collection)
            .await
            .expect("empty seal")
            .is_none()
    );

    drop(service);
    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 100).expect("recover");
    let after_restart = query(recovered, "docs").await;
    assert_eq!(after_restart["hits"], before["hits"]);

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn threshold_sealing_is_automatic_and_reclaims_wal() {
    let dir = temp_dir("auto-seal");
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 2);
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create");

    service
        .upsert(&collection, pending("a", 1.0))
        .await
        .expect("first");
    assert!(!dir.join("collections/docs/checkpoint.ktcp").exists());
    service
        .upsert(&collection, pending("b", 2.0))
        .await
        .expect("second triggers seal");

    assert!(dir.join("collections/docs/checkpoint.ktcp").exists());
    assert!(
        Wal::open(dir.join("collections/docs/wal.log"))
            .expect("wal")
            .replay()
            .expect("replay")
            .entries
            .is_empty()
    );
    assert_eq!(
        query(state, "docs").await["hits"]
            .as_array()
            .expect("hits")
            .len(),
        2
    );
    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn recovery_survives_segment_publish_before_checkpoint_publish() {
    let dir = temp_dir("orphan-segment");
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create");
    service
        .upsert_batch(&collection, vec![pending("a", 1.0), pending("b", 2.0)])
        .await
        .expect("batch");

    let collection_dir = dir.join("collections/docs");
    let entries = Wal::open(collection_dir.join("wal.log"))
        .expect("wal")
        .replay()
        .expect("replay")
        .entries;
    let segment = Segment::from_mutations(SegmentId::new(1), &entries).expect("segment");
    SegmentStore::open(collection_dir.join("segments"))
        .expect("store")
        .publish(&segment)
        .expect("publish segment");
    assert!(!collection_dir.join("checkpoint.ktcp").exists());

    drop(service);
    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 100).expect("recover");
    assert_eq!(
        query(recovered, "docs").await["hits"]
            .as_array()
            .expect("hits")
            .len(),
        2
    );
    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn recovery_ignores_checkpointed_entries_left_in_unreclaimed_wal() {
    let dir = temp_dir("checkpoint-before-reclaim");
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create");
    service
        .upsert_batch(&collection, vec![pending("a", 1.0), pending("b", 2.0)])
        .await
        .expect("batch");

    let collection_dir = dir.join("collections/docs");
    let entries = Wal::open(collection_dir.join("wal.log"))
        .expect("wal")
        .replay()
        .expect("replay")
        .entries;
    let segment = Segment::from_mutations(SegmentId::new(1), &entries).expect("segment");
    SegmentStore::open(collection_dir.join("segments"))
        .expect("segment store")
        .publish(&segment)
        .expect("publish segment");
    CheckpointStore::open(&collection_dir)
        .expect("checkpoint store")
        .publish(&Checkpoint::new(
            collection.clone(),
            vec![segment.id()],
            segment.max_sequence(),
        ))
        .expect("publish checkpoint");

    assert_eq!(entries.len(), 2);
    drop(service);
    drop(state);

    let recovered = AppState::recover_with_threshold(&dir, 100).expect("recover");
    let recovered_service = WriteService::new(recovered.clone());
    assert!(
        recovered_service
            .seal_collection(&collection)
            .await
            .expect("no mutable seal")
            .is_none()
    );
    assert_eq!(
        query(recovered, "docs").await["hits"]
            .as_array()
            .expect("hits")
            .len(),
        2
    );
    std::fs::remove_dir_all(dir).expect("cleanup");
}
