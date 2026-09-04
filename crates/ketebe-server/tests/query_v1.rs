use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use ketebe_server::{AppState, RuntimeCatalog, app, proto, proto_v1, serve_grpc_listener};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Code;
use tower::ServiceExt;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-query-v1-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

fn v0_string_value(value: &str) -> proto::MetadataValue {
    proto::MetadataValue {
        kind: Some(proto::metadata_value::Kind::StringValue(value.to_string())),
    }
}

fn v0_metadata(title: &str, category: &str) -> proto::MetadataObject {
    proto::MetadataObject {
        fields: HashMap::from([
            ("title".to_string(), v0_string_value(title)),
            ("category".to_string(), v0_string_value(category)),
        ]),
    }
}

fn v0_string_id(value: &str) -> proto::RecordId {
    proto::RecordId {
        value: Some(proto::record_id::Value::StringValue(value.to_string())),
    }
}

async fn rest_query(state: AppState, body: Value) -> (StatusCode, Value) {
    let response = app(state)
        .oneshot(
            Request::post("/v1/collections/docs/query")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

fn grpc_hit_id(hit: &proto_v1::SearchHit) -> &str {
    match hit.id.as_ref().and_then(|id| id.value.as_ref()) {
        Some(proto_v1::record_id::Value::StringValue(value)) => value,
        other => panic!("expected string id, got {other:?}"),
    }
}

#[tokio::test]
async fn query_v1_rest_and_grpc_share_dense_lexical_hybrid_semantics() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_grpc_listener(state.clone(), listener));
    let endpoint = format!("http://{address}");

    let mut collections = proto::collections_client::CollectionsClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut records = proto::records_client::RecordsClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut v0_query = proto::query_client::QueryClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut v1_query = proto_v1::query_client::QueryClient::connect(endpoint)
        .await
        .unwrap();

    collections
        .create_collection(proto::CreateCollectionRequest {
            id: "docs".into(),
            dimension: 2,
            metric: proto::DistanceMetric::L2 as i32,
            lexical_fields: vec![proto::FieldPath {
                segments: vec!["title".into()],
            }],
            lexical_analyzer: None,
            ingestion: None,
        })
        .await
        .unwrap();
    records
        .batch_upsert(proto::BatchUpsertRequest {
            collection_id: "docs".into(),
            records: vec![
                proto::RecordInput {
                    id: Some(v0_string_id("alpha")),
                    vector: vec![1.0, 0.0],
                    metadata: Some(v0_metadata("alpha guide", "book")),
                },
                proto::RecordInput {
                    id: Some(v0_string_id("beta")),
                    vector: vec![3.0, 0.0],
                    metadata: Some(v0_metadata("beta notes", "game")),
                },
            ],
        })
        .await
        .unwrap();

    let (status, dense_rest) = rest_query(
        state.clone(),
        json!({
            "vector": [1.0, 0.0],
            "top_k": 2,
            "execution": "exact",
            "explain": true,
            "timeout_ms": 250,
            "dense_candidates": 2
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dense_rest["api_version"], "v1");
    assert_eq!(dense_rest["explain"]["mode"], "dense");
    assert_eq!(dense_rest["explain"]["timeout_ms"], 250);
    assert_eq!(dense_rest["hits"][0]["id"]["type"], "string");
    assert_eq!(dense_rest["hits"][0]["id"]["value"], "alpha");

    let dense_grpc = v1_query
        .query(proto_v1::QueryRequest {
            collection_id: "docs".into(),
            dense: Some(proto_v1::DenseQuery {
                vector: vec![1.0, 0.0],
                candidates: Some(2),
            }),
            lexical: None,
            top_k: 2,
            predicate: None,
            execution: proto_v1::ExecutionPreference::Exact as i32,
            search_profile: None,
            include_metadata: Some(true),
            include_provenance: false,
            explain: true,
            timeout_ms: Some(250),
            rerank: None,
            paginate: false,
            cursor: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(dense_grpc.api_version, "v1");
    assert_eq!(grpc_hit_id(&dense_grpc.hits[0]), "alpha");
    let dense_explain = dense_grpc.explain.unwrap();
    assert_eq!(dense_explain.mode, "dense");
    assert_eq!(dense_explain.timeout_ms, Some(250));

    let (status, lexical_rest) = rest_query(
        state.clone(),
        json!({"text": "alpha", "top_k": 2, "explain": true, "lexical_candidates": 2}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(lexical_rest["explain"]["mode"], "lexical");
    assert_eq!(lexical_rest["hits"][0]["id"]["value"], "alpha");

    let lexical_grpc = v1_query
        .query(proto_v1::QueryRequest {
            collection_id: "docs".into(),
            dense: None,
            lexical: Some(proto_v1::LexicalQuery {
                text: "alpha".into(),
                candidates: Some(2),
                rrf_k: None,
            }),
            top_k: 2,
            predicate: None,
            execution: proto_v1::ExecutionPreference::Auto as i32,
            search_profile: None,
            include_metadata: Some(true),
            include_provenance: false,
            explain: true,
            timeout_ms: None,
            rerank: None,
            paginate: false,
            cursor: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(grpc_hit_id(&lexical_grpc.hits[0]), "alpha");
    assert_eq!(lexical_grpc.explain.unwrap().mode, "lexical");

    let predicate_json = json!({"op": "eq", "path": ["category"], "value": "book"});
    let (status, hybrid_rest) = rest_query(
        state.clone(),
        json!({
            "vector": [1.0, 0.0],
            "text": "alpha",
            "top_k": 1,
            "execution": "exact",
            "predicate": predicate_json,
            "dense_candidates": 2,
            "lexical_candidates": 1,
            "rrf_k": 60,
            "explain": true,
            "include_metadata": false
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(hybrid_rest["explain"]["mode"], "hybrid");
    assert_eq!(hybrid_rest["hits"].as_array().unwrap().len(), 1);
    assert_eq!(hybrid_rest["explain"]["dense_candidates"], 2);
    assert_eq!(hybrid_rest["explain"]["lexical_candidates"], 1);
    assert!(hybrid_rest["hits"][0].get("metadata").is_none());

    let grpc_predicate = proto_v1::Predicate {
        kind: Some(proto_v1::predicate::Kind::Eq(
            proto_v1::ComparisonPredicate {
                path: vec!["category".into()],
                value: Some(proto_v1::MetadataValue {
                    kind: Some(proto_v1::metadata_value::Kind::StringValue("book".into())),
                }),
            },
        )),
    };
    let hybrid_grpc = v1_query
        .query(proto_v1::QueryRequest {
            collection_id: "docs".into(),
            dense: Some(proto_v1::DenseQuery {
                vector: vec![1.0, 0.0],
                candidates: Some(2),
            }),
            lexical: Some(proto_v1::LexicalQuery {
                text: "alpha".into(),
                candidates: Some(1),
                rrf_k: Some(60),
            }),
            top_k: 1,
            predicate: Some(grpc_predicate),
            execution: proto_v1::ExecutionPreference::Exact as i32,
            search_profile: None,
            include_metadata: Some(false),
            include_provenance: false,
            explain: true,
            timeout_ms: None,
            rerank: None,
            paginate: false,
            cursor: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(hybrid_grpc.hits.len(), 1);
    assert_eq!(grpc_hit_id(&hybrid_grpc.hits[0]), "alpha");
    assert!(hybrid_grpc.hits[0].metadata.is_none());
    let hybrid_explain = hybrid_grpc.explain.unwrap();
    assert_eq!(hybrid_explain.mode, "hybrid");
    assert_eq!(hybrid_explain.dense_candidates, Some(2));
    assert_eq!(hybrid_explain.lexical_candidates, Some(1));

    // v0 dense-only remains valid during migration to v1.
    let v0 = v0_query
        .query(proto::QueryRequest {
            collection_id: "docs".into(),
            vector: vec![1.0, 0.0],
            metric: proto::DistanceMetric::L2 as i32,
            top_k: 1,
            execution: proto::ExecutionPreference::Exact as i32,
            predicate: None,
            lexical: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(v0.hits.len(), 1);

    let (status, invalid_rest) = rest_query(
        state.clone(),
        json!({"vector": [1.0, 0.0], "top_k": 10, "search_profile": "does-not-exist"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid_rest["error"]["code"], "search_profile_not_found");

    let invalid_grpc = v1_query
        .query(proto_v1::QueryRequest {
            collection_id: "docs".into(),
            dense: None,
            lexical: None,
            top_k: 10,
            predicate: None,
            execution: proto_v1::ExecutionPreference::Auto as i32,
            search_profile: Some("does-not-exist".into()),
            include_metadata: Some(true),
            include_provenance: false,
            explain: false,
            timeout_ms: None,
            rerank: None,
            paginate: false,
            cursor: None,
        })
        .await
        .expect_err("invalid v1 request should fail");
    assert_eq!(invalid_grpc.code(), Code::InvalidArgument);

    server.abort();
    fs::remove_dir_all(dir).unwrap();
}
