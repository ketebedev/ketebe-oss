use ketebe_mcp::{ketebe::KetebeApi, retrieval::AgentRecordId};
use ketebe_server::{AppState, RuntimeCatalog, app};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-records-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

#[tokio::test]
async fn mcp_record_retrieval_preserves_typed_ids_and_projection() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app(state)).await.expect("server");
    });

    let base_url = format!("http://{address}");
    let http = reqwest::Client::new();
    let response = http
        .post(format!("{base_url}/v0/collections"))
        .json(&json!({
            "id": "docs",
            "dimension": 3,
            "metric": "cosine"
        }))
        .send()
        .await
        .expect("create collection");
    assert!(response.status().is_success());

    let response = http
        .post(format!(
            "{base_url}/v0/collections/docs/records:batchUpsert"
        ))
        .json(&json!({
            "records": [
                {
                    "id": {"type": "string", "value": "42"},
                    "vector": [1.0, 0.0, 0.0],
                    "metadata": {"kind": "string", "hidden": "a"}
                },
                {
                    "id": {"type": "u64", "value": 42},
                    "vector": [0.0, 1.0, 0.0],
                    "metadata": {"kind": "u64", "hidden": "b"}
                }
            ]
        }))
        .send()
        .await
        .expect("batch upsert");
    assert!(response.status().is_success());

    let api = KetebeApi::new(base_url).expect("MCP API adapter");
    let result = api
        .fetch_records(
            "docs",
            vec![
                AgentRecordId::String("42".into()),
                AgentRecordId::U64(42),
                AgentRecordId::U64(99),
            ],
            vec!["metadata.kind".into()],
            None,
        )
        .await
        .expect("fetch records");

    assert_eq!(result.records.len(), 2);
    assert_eq!(result.records[0].id, AgentRecordId::String("42".into()));
    assert_eq!(result.records[1].id, AgentRecordId::U64(42));
    assert_eq!(result.missing, vec![AgentRecordId::U64(99)]);
    assert!(result.records.iter().all(|record| record.vector.is_none()));
    assert_eq!(
        result.records[0].metadata.as_ref().expect("metadata"),
        &json!({"kind": "string"})
    );
    assert_eq!(
        result.records[1].metadata.as_ref().expect("metadata"),
        &json!({"kind": "u64"})
    );

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}
