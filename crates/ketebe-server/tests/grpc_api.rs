use ketebe_server::{AppState, RuntimeCatalog, proto, serve_grpc_listener};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Code;

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-grpc-api-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ))
}

fn string_value(value: &str) -> proto::MetadataValue {
    proto::MetadataValue {
        kind: Some(proto::metadata_value::Kind::StringValue(value.to_string())),
    }
}

fn metadata(category: &str) -> proto::MetadataObject {
    proto::MetadataObject {
        fields: HashMap::from([("category".to_string(), string_value(category))]),
    }
}

fn string_id(value: &str) -> proto::RecordId {
    proto::RecordId {
        value: Some(proto::record_id::Value::StringValue(value.to_string())),
    }
}

fn numeric_id(value: u64) -> proto::RecordId {
    proto::RecordId {
        value: Some(proto::record_id::Value::U64Value(value)),
    }
}

#[tokio::test]
async fn grpc_v0_shares_collection_write_query_hybrid_and_delete_semantics() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gRPC");
    let address = listener.local_addr().expect("local addr");
    let server = tokio::spawn(serve_grpc_listener(state, listener));
    let endpoint = format!("http://{address}");

    let mut collections = proto::collections_client::CollectionsClient::connect(endpoint.clone())
        .await
        .expect("collections client");
    let mut records = proto::records_client::RecordsClient::connect(endpoint.clone())
        .await
        .expect("records client");
    let mut query = proto::query_client::QueryClient::connect(endpoint)
        .await
        .expect("query client");

    let created = collections
        .create_collection(proto::CreateCollectionRequest {
            id: "docs".to_string(),
            dimension: 2,
            metric: proto::DistanceMetric::L2 as i32,
            lexical_fields: Vec::new(),
            lexical_analyzer: None,
            ingestion: None,
        })
        .await
        .expect("create")
        .into_inner();
    assert_eq!(created.id, "docs");
    assert_eq!(created.dimension, 2);
    assert!(!created.hnsw_ready);

    let duplicate = collections
        .create_collection(proto::CreateCollectionRequest {
            id: "docs".to_string(),
            dimension: 2,
            metric: proto::DistanceMetric::L2 as i32,
            lexical_fields: Vec::new(),
            lexical_analyzer: None,
            ingestion: None,
        })
        .await
        .expect_err("duplicate should fail");
    assert_eq!(duplicate.code(), Code::AlreadyExists);

    let batch = records
        .batch_upsert(proto::BatchUpsertRequest {
            collection_id: "docs".to_string(),
            records: vec![
                proto::RecordInput {
                    id: Some(string_id("42")),
                    vector: vec![1.0, 0.0],
                    metadata: Some(metadata("book")),
                },
                proto::RecordInput {
                    id: Some(numeric_id(42)),
                    vector: vec![2.0, 0.0],
                    metadata: Some(metadata("game")),
                },
            ],
        })
        .await
        .expect("batch upsert")
        .into_inner();
    assert_eq!(batch.sequence_numbers, vec![1, 2]);

    let updated_schema = collections
        .update_lexical_schema(proto::UpdateLexicalSchemaRequest {
            id: "docs".to_string(),
            lexical_fields: vec![proto::FieldPath {
                segments: vec!["category".to_string()],
            }],
            lexical_analyzer: Some(proto::LexicalAnalyzerConfig {
                kind: proto::LexicalAnalyzerKind::Standard as i32,
                lowercase: false,
            }),
        })
        .await
        .expect("update lexical schema")
        .into_inner();
    assert_eq!(updated_schema.lexical_fields.len(), 1);
    assert!(!updated_schema.lexical_analyzer.expect("analyzer").lowercase);

    let predicate = proto::Predicate {
        kind: Some(proto::predicate::Kind::Eq(proto::ComparisonPredicate {
            path: vec!["category".to_string()],
            value: Some(string_value("book")),
        })),
    };
    let response = query
        .query(proto::QueryRequest {
            collection_id: "docs".to_string(),
            vector: vec![1.0, 0.0],
            metric: proto::DistanceMetric::L2 as i32,
            top_k: 10,
            execution: proto::ExecutionPreference::Auto as i32,
            predicate: Some(predicate),
            lexical: None,
        })
        .await
        .expect("query")
        .into_inner();
    assert_eq!(response.hits.len(), 1);
    assert!(matches!(
        response.hits[0].id.as_ref().and_then(|id| id.value.as_ref()),
        Some(proto::record_id::Value::StringValue(value)) if value == "42"
    ));
    let explain = response.explain.expect("explain");
    assert_eq!(explain.strategy, "exact");
    assert!(explain.has_predicate);
    assert!(!explain.hybrid);

    let hybrid = query
        .query(proto::QueryRequest {
            collection_id: "docs".to_string(),
            vector: vec![1.0, 0.0],
            metric: proto::DistanceMetric::L2 as i32,
            top_k: 2,
            execution: proto::ExecutionPreference::Exact as i32,
            predicate: None,
            lexical: Some(proto::LexicalQuery {
                text: "book".to_string(),
                fields: vec![proto::FieldPath {
                    segments: vec!["category".to_string()],
                }],
                rrf_k: Some(60),
            }),
        })
        .await
        .expect("hybrid query")
        .into_inner();
    assert_eq!(hybrid.hits.len(), 2);
    assert!(matches!(
        hybrid.hits[0].id.as_ref().and_then(|id| id.value.as_ref()),
        Some(proto::record_id::Value::StringValue(value)) if value == "42"
    ));
    assert_eq!(hybrid.hits[0].dense_rank, Some(1));
    assert_eq!(hybrid.hits[0].lexical_rank, Some(1));
    let hybrid_explain = hybrid.explain.expect("hybrid explain");
    assert!(hybrid_explain.hybrid);
    assert_eq!(hybrid_explain.rrf_k, Some(60));
    assert_eq!(hybrid_explain.dense_candidates, Some(2));
    assert_eq!(hybrid_explain.lexical_candidates, Some(1));

    let unavailable = query
        .query(proto::QueryRequest {
            collection_id: "docs".to_string(),
            vector: vec![1.0, 0.0],
            metric: proto::DistanceMetric::L2 as i32,
            top_k: 1,
            execution: proto::ExecutionPreference::Hnsw as i32,
            predicate: None,
            lexical: None,
        })
        .await
        .expect_err("HNSW is unavailable while mutable data exists");
    assert_eq!(unavailable.code(), Code::Unavailable);

    let info = collections
        .get_collection(proto::GetCollectionRequest {
            id: "docs".to_string(),
        })
        .await
        .expect("get collection")
        .into_inner();
    let stats = info.stats.expect("stats");
    assert_eq!(stats.live_records, 2);
    assert_eq!(stats.mutable_mutations, 2);

    let listed = collections
        .list_collections(proto::ListCollectionsRequest {})
        .await
        .expect("list")
        .into_inner();
    assert_eq!(listed.collections.len(), 1);
    assert_eq!(listed.collections[0].id, "docs");

    let deleted_record = records
        .delete(proto::DeleteRecordRequest {
            collection_id: "docs".to_string(),
            id: Some(numeric_id(42)),
        })
        .await
        .expect("delete record")
        .into_inner();
    assert_eq!(deleted_record.sequence_number, 3);

    collections
        .delete_collection(proto::DeleteCollectionRequest {
            id: "docs".to_string(),
        })
        .await
        .expect("delete collection");
    let missing = collections
        .get_collection(proto::GetCollectionRequest {
            id: "docs".to_string(),
        })
        .await
        .expect_err("deleted collection should be missing");
    assert_eq!(missing.code(), Code::NotFound);

    server.abort();
    let _ = server.await;
    fs::remove_dir_all(dir).expect("cleanup");
}
