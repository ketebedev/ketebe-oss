use axum::body::{Body, to_bytes};
use axum::http::{Request, header};
use ketebe_core::{CollectionId, DistanceMetric, Metadata, RecordId};
use ketebe_server::{AppState, PendingRecord, RuntimeCatalog, WriteService, app};
use ketebe_storage::{Checkpoint, CheckpointStore, SegmentId, SegmentStore, compact_segments};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-compaction-{label}-{}",
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

fn segment_count(directory: &Path) -> usize {
    std::fs::read_dir(directory)
        .expect("read segments")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|value| value.to_str()) == Some("kseg"))
        .count()
}

async fn build_three_segments(dir: &Path) -> (AppState, WriteService, CollectionId) {
    let state = AppState::with_data_dir_and_threshold(
        RuntimeCatalog::empty_ready(),
        dir.to_path_buf(),
        100,
    );
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create");

    service
        .upsert_batch(
            &collection,
            vec![pending("a", 1.0), pending("b", 2.0), pending("d", 4.0)],
        )
        .await
        .expect("first batch");
    service
        .seal_collection(&collection)
        .await
        .expect("seal one")
        .expect("checkpoint one");

    service
        .upsert(&collection, pending("a", 3.0))
        .await
        .expect("overwrite a");
    service
        .delete(&collection, RecordId::string("b").expect("b"))
        .await
        .expect("delete b");
    service
        .delete(&collection, RecordId::string("d").expect("d"))
        .await
        .expect("delete d");
    service
        .seal_collection(&collection)
        .await
        .expect("seal two")
        .expect("checkpoint two");

    service
        .upsert(&collection, pending("b", 5.0))
        .await
        .expect("resurrect b");
    service
        .upsert(&collection, pending("c", 6.0))
        .await
        .expect("insert c");
    service
        .seal_collection(&collection)
        .await
        .expect("seal three")
        .expect("checkpoint three");

    (state, service, collection)
}

#[tokio::test]
async fn compaction_preserves_query_state_and_garbage_collects_old_segments() {
    let dir = temp_dir("normal");
    let (state, service, collection) = build_three_segments(&dir).await;
    let segment_dir = dir.join("collections/docs/segments");
    assert_eq!(segment_count(&segment_dir), 3);

    let before = query(state.clone(), "docs").await;
    assert_eq!(before["hits"].as_array().expect("hits").len(), 3);

    let checkpoint = service
        .compact_collection(&collection)
        .await
        .expect("compact")
        .expect("replacement checkpoint");
    assert_eq!(checkpoint.segments().len(), 1);
    assert_eq!(segment_count(&segment_dir), 1);

    let after = query(state.clone(), "docs").await;
    assert_eq!(after["hits"], before["hits"]);

    let replacement = SegmentStore::open(&segment_dir)
        .expect("store")
        .open_segment(checkpoint.segments()[0])
        .expect("replacement");
    assert_eq!(replacement.tombstones().len(), 1);
    assert_eq!(
        replacement.tombstones()[0].record_id(),
        &RecordId::string("d").expect("d")
    );

    drop(service);
    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 100).expect("recover");
    assert_eq!(query(recovered, "docs").await["hits"], before["hits"]);

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn compaction_is_noop_with_fewer_than_two_authoritative_segments() {
    let dir = temp_dir("noop");
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let service = WriteService::new(state);
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create");
    service
        .upsert(&collection, pending("a", 1.0))
        .await
        .expect("upsert");
    service
        .seal_collection(&collection)
        .await
        .expect("seal")
        .expect("checkpoint");

    assert!(
        service
            .compact_collection(&collection)
            .await
            .expect("compact")
            .is_none()
    );
    assert_eq!(segment_count(&dir.join("collections/docs/segments")), 1);
    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn recovery_uses_old_checkpoint_if_replacement_segment_was_not_checkpointed() {
    let dir = temp_dir("replacement-before-checkpoint");
    let (state, service, _collection) = build_three_segments(&dir).await;
    let before = query(state.clone(), "docs").await;
    let collection_dir = dir.join("collections/docs");
    let segment_dir = collection_dir.join("segments");
    let store = SegmentStore::open(&segment_dir).expect("store");
    let checkpoint = CheckpointStore::open(&collection_dir)
        .expect("checkpoint store")
        .load()
        .expect("load")
        .expect("checkpoint");
    let authoritative = checkpoint
        .segments()
        .iter()
        .map(|id| store.open_segment(*id).expect("segment"))
        .collect::<Vec<_>>();
    let orphan = compact_segments(SegmentId::new(4), &authoritative).expect("compact");
    store.publish(&orphan).expect("publish replacement only");
    assert_eq!(segment_count(&segment_dir), 4);

    drop(service);
    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 100).expect("recover");
    assert_eq!(query(recovered, "docs").await["hits"], before["hits"]);

    std::fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn recovery_uses_new_checkpoint_even_if_old_segments_were_not_garbage_collected() {
    let dir = temp_dir("checkpoint-before-gc");
    let (state, service, collection) = build_three_segments(&dir).await;
    let before = query(state.clone(), "docs").await;
    let collection_dir = dir.join("collections/docs");
    let segment_dir = collection_dir.join("segments");
    let store = SegmentStore::open(&segment_dir).expect("store");
    let old_checkpoint = CheckpointStore::open(&collection_dir)
        .expect("checkpoint store")
        .load()
        .expect("load")
        .expect("checkpoint");
    let authoritative = old_checkpoint
        .segments()
        .iter()
        .map(|id| store.open_segment(*id).expect("segment"))
        .collect::<Vec<_>>();
    let replacement = compact_segments(SegmentId::new(4), &authoritative).expect("compact");
    store.publish(&replacement).expect("publish replacement");
    let new_checkpoint = Checkpoint::new(
        collection,
        vec![replacement.id()],
        old_checkpoint.sequence_number(),
    );
    CheckpointStore::open(&collection_dir)
        .expect("checkpoint store")
        .publish(&new_checkpoint)
        .expect("publish new checkpoint");
    assert_eq!(segment_count(&segment_dir), 4);

    drop(service);
    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 100).expect("recover");
    assert_eq!(query(recovered, "docs").await["hits"], before["hits"]);

    std::fs::remove_dir_all(dir).expect("cleanup");
}
