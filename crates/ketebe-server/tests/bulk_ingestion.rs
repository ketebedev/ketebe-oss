use axum::body::{Body, to_bytes};
use axum::http::{Request, header};
use ketebe_core::{CollectionId, DistanceMetric, RecordId};
use ketebe_server::{AppState, PendingRecord, RuntimeCatalog, WriteService, app};
use ketebe_storage::Wal;
use serde_json::Value;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn data_dir(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ketebe-bulk-{label}-{nonce}"))
}

fn pending(id: u64, value: f32) -> PendingRecord {
    PendingRecord {
        id: RecordId::unsigned(id),
        vector: vec![value],
        metadata: Default::default(),
    }
}

async fn exact_query(state: AppState, top_k: usize) -> Value {
    let request = Request::post("/v0/collections/docs/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(format!(
            r#"{{"vector":[2.0],"metric":"l2","top_k":{top_k},"execution":"exact"}}"#
        )))
        .expect("request");
    let response = app(state).oneshot(request).await.expect("response");
    assert!(response.status().is_success());
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("json")
}

#[tokio::test]
async fn acknowledged_batch_is_visible_and_recovered_in_sequence_order() {
    let dir = data_dir("recover");
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 1_000);
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create");

    let sequences = service
        .upsert_batch(
            &collection,
            vec![pending(1, 1.0), pending(2, 2.0), pending(3, 3.0)],
        )
        .await
        .expect("batch");
    assert_eq!(
        sequences
            .iter()
            .map(|sequence| sequence.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    let body = exact_query(state.clone(), 3).await;
    assert_eq!(body["hits"].as_array().expect("hits").len(), 3);

    drop(service);
    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 1_000).expect("recover");
    let body = exact_query(recovered, 3).await;
    let hits = body["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 3);
    let mut recovered_sequences = hits
        .iter()
        .map(|hit| hit["sequence_number"].as_u64().expect("sequence"))
        .collect::<Vec<_>>();
    recovered_sequences.sort_unstable();
    assert_eq!(recovered_sequences, vec![1, 2, 3]);

    fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn invalid_batch_is_rejected_without_partial_wal_or_visibility() {
    let dir = data_dir("validation");
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 1_000);
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create");

    let invalid = PendingRecord {
        id: RecordId::unsigned(2),
        vector: vec![2.0, 3.0],
        metadata: Default::default(),
    };
    assert!(
        service
            .upsert_batch(&collection, vec![pending(1, 1.0), invalid])
            .await
            .is_err()
    );

    let wal_path = dir.join("collections/docs/wal.log");
    assert_eq!(fs::metadata(&wal_path).expect("wal metadata").len(), 0);
    let body = exact_query(state, 10).await;
    assert!(body["hits"].as_array().expect("hits").is_empty());

    fs::remove_dir_all(dir).expect("cleanup");
}

#[tokio::test]
async fn writer_rebinds_to_reclaimed_wal_and_post_seal_write_survives_restart() {
    let dir = data_dir("rebind");
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 2);
    let service = WriteService::new(state.clone());
    let collection = CollectionId::new("docs").expect("collection");
    service
        .create_collection(collection.clone(), 1, DistanceMetric::L2, Vec::new())
        .await
        .expect("create");

    service
        .upsert_batch(&collection, vec![pending(1, 1.0), pending(2, 2.0)])
        .await
        .expect("batch triggers seal");

    let wal_path = dir.join("collections/docs/wal.log");
    assert_eq!(fs::metadata(&wal_path).expect("reclaimed WAL").len(), 0);

    let sequence = service
        .upsert(&collection, pending(3, 3.0))
        .await
        .expect("post-seal upsert");
    assert_eq!(sequence.get(), 3);

    let replay = Wal::open(&wal_path)
        .expect("open WAL")
        .replay()
        .expect("replay");
    assert_eq!(replay.entries.len(), 1);
    assert_eq!(replay.entries[0].sequence_number().get(), 3);

    let body = exact_query(state.clone(), 3).await;
    assert_eq!(body["hits"].as_array().expect("hits").len(), 3);

    drop(service);
    drop(state);
    let recovered = AppState::recover_with_threshold(&dir, 2).expect("recover");
    let body = exact_query(recovered, 3).await;
    let hits = body["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 3);
    assert!(hits.iter().any(|hit| hit["sequence_number"] == 3));

    fs::remove_dir_all(dir).expect("cleanup");
}
