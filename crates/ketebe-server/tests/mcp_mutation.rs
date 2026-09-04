use ketebe_mcp::{
    ketebe::KetebeApi,
    mutation::{
        IngestDocumentInput, IngestDocumentsParams, UpsertRecordInput, UpsertRecordsParams,
    },
    retrieval::AgentRecordId,
};
use ketebe_server::{AppState, DeterministicEmbeddingProvider, RuntimeCatalog, app};
use serde_json::json;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-mutation-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

#[tokio::test]
async fn mcp_mutations_reuse_idempotent_public_write_contracts() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    state
        .set_embedding_provider(Arc::new(
            DeterministicEmbeddingProvider::new("mcp-mutation-model", "v1").expect("provider"),
        ))
        .await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let server_state = state.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app(server_state))
            .await
            .expect("server");
    });

    let base_url = format!("http://{address}");
    let response = reqwest::Client::new()
        .post(format!("{base_url}/v0/collections"))
        .json(&json!({
            "id": "docs",
            "dimension": 4,
            "metric": "l2"
        }))
        .send()
        .await
        .expect("create collection request");
    assert!(response.status().is_success());

    let api = KetebeApi::new(base_url).expect("MCP API adapter");
    let record_id = AgentRecordId::String("stable-record".into());
    for version in [1, 2] {
        let output = api
            .upsert_records(
                UpsertRecordsParams {
                    collection: "docs".into(),
                    records: vec![UpsertRecordInput {
                        id: record_id.clone(),
                        vector: vec![1.0, 0.0, 0.0, 0.0],
                        metadata: Some(json!({"version": version})),
                    }],
                },
                None,
            )
            .await
            .expect("record upsert");
        assert_eq!(output.accepted_records, 1);
    }

    let fetched = api
        .fetch_records("docs", vec![record_id], vec![], None)
        .await
        .expect("fetch record");
    assert_eq!(fetched.records.len(), 1);
    assert_eq!(
        fetched.records[0].metadata.as_ref().unwrap()["version"].as_f64(),
        Some(2.0)
    );

    let document_id = AgentRecordId::String("stable-document".into());
    for _ in 0..2 {
        let output = api
            .ingest_documents(
                IngestDocumentsParams {
                    collection: "docs".into(),
                    documents: vec![IngestDocumentInput {
                        id: document_id.clone(),
                        text: "abcdefghij".into(),
                        metadata: Some(json!({"kind":"rfc"})),
                        source: None,
                        chunking: Some(json!({"max_chars":5,"overlap_chars":2})),
                    }],
                },
                None,
            )
            .await
            .expect("document ingestion");
        assert_eq!(output.accepted_documents, vec![document_id.clone()]);
    }

    let chunk_ids = (0..4)
        .map(|ordinal| {
            AgentRecordId::String(format!(
                "_ketebe_chunk:s:737461626c652d646f63756d656e74:{ordinal}"
            ))
        })
        .collect::<Vec<_>>();
    let chunks = api
        .fetch_records("docs", chunk_ids.clone(), vec![], None)
        .await
        .expect("fetch document chunks");
    assert_eq!(chunks.records.len(), 3);
    assert_eq!(chunks.missing, vec![chunk_ids[3].clone()]);
    assert!(chunks.records.iter().all(|record| {
        record
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("kind"))
            == Some(&json!("rfc"))
    }));

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}
