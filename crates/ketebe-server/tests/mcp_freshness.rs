use ketebe_mcp::{
    ketebe::KetebeApi,
    search::{SearchMode, SearchParams},
};
use ketebe_server::{AppState, DeterministicEmbeddingProvider, RuntimeCatalog, app};
use serde_json::json;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-freshness-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

fn sparse_params() -> SearchParams {
    SearchParams {
        collection: "docs".into(),
        mode: SearchMode::Sparse,
        text: Some("alpha".into()),
        vector: None,
        filter: None,
        after: None,
        before: None,
        prefer_recent: false,
        limit: 10,
        fields: vec!["metadata.title".into(), "source_timestamp_unix_ms".into()],
        execution: None,
        dense_candidates: None,
        sparse_candidates: Some(10),
        search_profile: None,
        timeout_ms: Some(500),
        explain: true,
    }
}

async fn put_document(http: &reqwest::Client, base_url: &str, id: &str, observed_at_unix_ms: u64) {
    let response = http
        .put(format!("{base_url}/v0/collections/docs/documents/{id}"))
        .json(&json!({
            "text": "alpha",
            "metadata": {"title": "alpha"},
            "chunking": {"max_chars": 64, "overlap_chars": 0},
            "source": {
                "kind": "http",
                "uri": format!("https://example.test/{id}"),
                "external_id": id,
                "observed_at_unix_ms": observed_at_unix_ms
            }
        }))
        .send()
        .await
        .expect("ingest document");
    assert!(response.status().is_success());
}

#[tokio::test]
async fn mcp_freshness_filters_and_breaks_equal_relevance_ties() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    state
        .set_embedding_provider(Arc::new(
            DeterministicEmbeddingProvider::new("freshness-model", "v1").expect("provider"),
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
    let http = reqwest::Client::new();
    let response = http
        .post(format!("{base_url}/v0/collections"))
        .json(&json!({
            "id": "docs",
            "dimension": 2,
            "metric": "l2",
            "lexical_fields": [["title"]]
        }))
        .send()
        .await
        .expect("create collection");
    assert!(response.status().is_success());

    put_document(&http, &base_url, "older", 100).await;
    put_document(&http, &base_url, "newer", 200).await;

    let api = KetebeApi::new(base_url).expect("MCP API adapter");

    let mut filtered = sparse_params();
    filtered.after = Some(120);
    filtered.before = Some(250);
    let filtered = api
        .search_params(filtered, None)
        .await
        .expect("freshness-filtered search");
    assert_eq!(filtered.hits.len(), 1);
    assert_eq!(filtered.hits[0].source_timestamp_unix_ms, Some(200));
    assert_eq!(
        filtered.hits[0].metadata.as_ref().expect("metadata"),
        &json!({"title": "alpha"})
    );

    let mut preferred = sparse_params();
    preferred.prefer_recent = true;
    let preferred = api
        .search_params(preferred, None)
        .await
        .expect("freshness-preferred search");
    assert_eq!(preferred.hits.len(), 2);
    assert_eq!(preferred.hits[0].score, preferred.hits[1].score);
    assert_eq!(preferred.hits[0].source_timestamp_unix_ms, Some(200));
    assert_eq!(preferred.hits[1].source_timestamp_unix_ms, Some(100));

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}
