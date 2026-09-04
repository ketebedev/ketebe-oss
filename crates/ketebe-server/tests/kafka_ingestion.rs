use axum::body::Body;
use axum::http::Request;
use ketebe_core::{CollectionId, DistanceMetric};
use ketebe_server::{
    AppState, CollectionService, KafkaDlqEnvelope, KafkaIngestionConfig, KafkaIngestionMessage,
    KafkaIngestionService, KafkaPoisonPolicy, RuntimeCatalog, WriteService, app,
};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-kafka-ingestion-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

fn message(partition: i32, offset: i64, payload: &str) -> KafkaIngestionMessage {
    KafkaIngestionMessage {
        partition,
        offset,
        payload: payload.as_bytes().to_vec(),
    }
}

#[tokio::test]
async fn partition_batch_is_durable_before_ack_and_restart_safe() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let collection = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection(collection.clone(), 2, DistanceMetric::L2, Vec::new())
        .await
        .unwrap();

    let service = KafkaIngestionService::new(state.clone());
    let ack = service
        .apply_partition_batch(
            &collection,
            &[
                message(
                    3,
                    41,
                    r#"{"version":1,"op":"upsert","id":{"type":"string","value":"doc-1"},"vector":[1.0,2.0],"metadata":{"title":"Rust"}}"#,
                ),
                message(
                    3,
                    42,
                    r#"{"version":1,"op":"upsert","id":{"type":"u64","value":1},"vector":[2.0,3.0],"metadata":{"title":"Kafka"}}"#,
                ),
                message(
                    3,
                    43,
                    r#"{"version":1,"op":"delete","id":{"type":"u64","value":1}}"#,
                ),
            ],
        )
        .await
        .unwrap();
    assert_eq!(ack.partition, 3);
    assert_eq!(ack.next_offset, 44);
    assert_eq!(ack.applied_records, 3);

    let stats = service.stats();
    assert_eq!(stats.received_records, 3);
    assert_eq!(stats.applied_records, 3);
    assert_eq!(stats.decode_failures, 0);
    assert_eq!(stats.write_failures, 0);
    assert_eq!(stats.dlq_records, 0);
    assert_eq!(stats.rebalance_count, 0);

    drop(service);
    drop(state);

    let recovered = AppState::recover_with_threshold(&dir, 100).unwrap();
    let info = CollectionService::new(recovered)
        .get(&collection)
        .await
        .unwrap();
    assert_eq!(info.live_records, 1);
    assert_eq!(info.tombstones, 1);
    assert_eq!(info.next_sequence, 4);

    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn malformed_batch_fails_before_any_write_and_cannot_produce_commit_ack() {
    let dir = temp_dir();
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let collection = CollectionId::new("docs").unwrap();
    WriteService::new(state.clone())
        .create_collection(collection.clone(), 1, DistanceMetric::Dot, Vec::new())
        .await
        .unwrap();

    let service = KafkaIngestionService::new(state.clone());
    let error = service
        .apply_partition_batch(
            &collection,
            &[
                message(
                    0,
                    7,
                    r#"{"version":1,"op":"upsert","id":{"type":"string","value":"safe"},"vector":[1.0]}"#,
                ),
                message(
                    0,
                    8,
                    r#"{"version":99,"op":"delete","id":{"type":"u64","value":7}}"#,
                ),
            ],
        )
        .await
        .expect_err("unsupported message must stop the batch before writes");
    assert!(
        error
            .to_string()
            .contains("unsupported Kafka envelope version")
    );

    let info = CollectionService::new(state)
        .get(&collection)
        .await
        .unwrap();
    assert_eq!(info.live_records, 0);
    assert_eq!(info.mutable_mutations, 0);
    assert_eq!(info.next_sequence, 1);
    assert_eq!(service.stats().decode_failures, 1);

    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn batch_rejects_partition_mixing_and_non_monotonic_offsets() {
    let state = AppState::new(RuntimeCatalog::empty_ready());
    let service = KafkaIngestionService::new(state);
    let collection = CollectionId::new("docs").unwrap();
    let payload = r#"{"version":1,"op":"delete","id":{"type":"u64","value":1}}"#;

    let mixed = service
        .apply_partition_batch(
            &collection,
            &[message(0, 1, payload), message(1, 2, payload)],
        )
        .await
        .expect_err("mixed partitions must fail");
    assert!(mixed.to_string().contains("mixes partitions"));

    let unordered = service
        .apply_partition_batch(
            &collection,
            &[message(0, 2, payload), message(0, 2, payload)],
        )
        .await
        .expect_err("offsets must increase");
    assert!(unordered.to_string().contains("not strictly increasing"));
}

#[test]
fn dlq_policy_is_explicit_and_default_remains_blocking() {
    let collection = CollectionId::new("docs").unwrap();
    let config =
        KafkaIngestionConfig::new("localhost:9092", "source", "group", collection, 128, 50)
            .unwrap();
    assert_eq!(config.poison_policy, KafkaPoisonPolicy::Block);

    let config = config.with_dlq_topic("source.dlq");
    assert_eq!(
        config.poison_policy,
        KafkaPoisonPolicy::Dlq {
            topic: "source.dlq".to_string()
        }
    );
}

#[test]
fn dlq_envelope_preserves_source_coordinates_and_payload() {
    let envelope = KafkaDlqEnvelope {
        version: 1,
        source_topic: "source".to_string(),
        source_partition: 4,
        source_offset: 42,
        target_collection: "docs".to_string(),
        error_class: "invalid_json".to_string(),
        error_message: "bad json".to_string(),
        payload: vec![1, 2, 3],
    };
    let json = serde_json::to_value(&envelope).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["source_partition"], 4);
    assert_eq!(json["source_offset"], 42);
    assert_eq!(json["target_collection"], "docs");
    assert_eq!(json["payload"], serde_json::json!([1, 2, 3]));
}

#[tokio::test]
async fn metrics_endpoint_is_available_when_kafka_runtime_is_disabled() {
    let response = app(AppState::new(RuntimeCatalog::empty_ready()))
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("ketebe_kafka_ingestion_enabled 0"));
}
