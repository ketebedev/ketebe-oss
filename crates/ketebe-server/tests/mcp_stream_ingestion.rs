use ketebe_mcp::{
    ketebe::KetebeApi,
    stream_ingestion::{CreateStreamIngestionParams, StreamIngestionParams},
};
use ketebe_server::{AppState, RuntimeCatalog, app};
use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-stream-ingestion-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

#[tokio::test]
async fn mcp_stream_ingestion_uses_public_api_and_safe_status_projection() {
    let dir = temp_dir();
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app(state)).await.expect("server");
    });

    let base_url = format!("http://{address}");
    let response = reqwest::Client::new()
        .post(format!("{base_url}/v0/collections"))
        .json(&serde_json::json!({
            "id": "docs",
            "dimension": 2,
            "metric": "cosine"
        }))
        .send()
        .await
        .expect("create collection request");
    assert!(response.status().is_success(), "create collection failed");

    let api = KetebeApi::new(base_url).expect("MCP API adapter");
    let created = api
        .create_stream_ingestion(
            CreateStreamIngestionParams {
                collection: "docs".to_string(),
                brokers: "127.0.0.1:1".to_string(),
                topic: "documents".to_string(),
                group_id: "ketebe-docs".to_string(),
                batch_max_records: Some(8),
                batch_linger_ms: Some(10),
                dlq_topic: None,
                security_protocol: None,
                sasl_mechanism: None,
                sasl_username_ref: None,
                sasl_password_ref: None,
            },
            None,
        )
        .await
        .expect("create stream ingestion");
    assert_eq!(created.id, "stream-docs");
    assert_eq!(created.collection, "docs");
    assert_eq!(created.topic, "documents");
    assert_eq!(created.group_id, "ketebe-docs");

    let listed = api
        .list_stream_ingestions("docs", None)
        .await
        .expect("list stream ingestions");
    assert_eq!(listed.streams.len(), 1);
    assert_eq!(listed.streams[0].id, created.id);

    let inspected = api
        .get_stream_ingestion(
            StreamIngestionParams {
                collection: "docs".to_string(),
                stream_id: created.id.clone(),
            },
            None,
        )
        .await
        .expect("get stream ingestion");
    assert!(matches!(inspected.state.as_str(), "running" | "failed"));
    if inspected.state == "failed" {
        assert_eq!(inspected.failure_code.as_deref(), Some("kafka_error"));
    }

    let safe = serde_json::to_value(&inspected).expect("serialize stream view");
    let object = safe.as_object().expect("stream object");
    for forbidden in [
        "brokers",
        "security_protocol",
        "sasl_mechanism",
        "sasl_username_ref",
        "sasl_password_ref",
        "credentials",
    ] {
        assert!(!object.contains_key(forbidden), "{forbidden}");
    }

    server.abort();
    let _ = fs::remove_dir_all(dir);
}
