use ketebe_sdk::{
    BatchRecordUpsert, BatchUpsert, Client, ClientConfig, CreateCollection, QueryRequest, RecordId,
    RecordUpsert,
};
use ketebe_server::{AppState, RuntimeCatalog, app};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-sdk-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

#[tokio::test]
async fn sdk_round_trips_against_real_server() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app(state)).await.expect("server");
    });

    let client = Client::new(ClientConfig::new(format!("http://{address}"))).expect("client");
    let collection = client
        .create_collection(&CreateCollection {
            id: "docs".into(),
            dimension: 2,
            metric: "l2".into(),
            lexical_fields: None,
        })
        .await
        .expect("create collection");
    assert_eq!(collection.id, "docs");

    let mutation = client
        .upsert_record(
            "docs",
            &RecordId::String("one".into()),
            &RecordUpsert {
                vector: vec![1.0, 0.0],
                metadata: Some(serde_json::json!({"title": "one"})),
            },
        )
        .await
        .expect("upsert");
    assert!(mutation.sequence_number > 0);

    let batch = client
        .batch_upsert_records(
            "docs",
            &BatchUpsert {
                records: vec![
                    BatchRecordUpsert {
                        id: RecordId::String("two".into()),
                        vector: vec![0.0, 1.0],
                        metadata: Some(serde_json::json!({"title": "two"})),
                    },
                    BatchRecordUpsert {
                        id: RecordId::String("three".into()),
                        vector: vec![0.5, 0.5],
                        metadata: Some(serde_json::json!({"title": "three"})),
                    },
                ],
            },
        )
        .await
        .expect("batch upsert");
    assert!(batch.is_object());

    let result = client
        .query(
            "docs",
            &QueryRequest {
                vector: Some(vec![1.0, 0.0]),
                top_k: Some(3),
                execution: Some("exact".into()),
                explain: true,
                ..QueryRequest::default()
            },
        )
        .await
        .expect("query");
    assert_eq!(result.api_version, "v1");
    assert_eq!(result.hits.len(), 3);
    assert_eq!(result.hits[0].id, RecordId::String("one".into()));

    let missing = client.get_collection("missing").await.expect_err("404");
    assert_not_found(missing);

    let no_migration = client
        .get_embedding_migration("docs")
        .await
        .expect_err("no active migration");
    assert_not_found(no_migration);

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}

fn assert_not_found(error: ketebe_sdk::Error) {
    match error {
        ketebe_sdk::Error::Api { status, code, .. } => {
            assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
            assert!(!code.is_empty());
        }
        other => panic!("unexpected error: {other}"),
    }
}
