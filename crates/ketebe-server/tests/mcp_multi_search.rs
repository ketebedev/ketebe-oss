use ketebe_mcp::{
    error::McpErrorCategory,
    ketebe::KetebeApi,
    multi_search::{CollectionSearchStatus, SearchManyParams, SearchManyTarget},
    retrieval::AgentRecordId,
    search::SearchMode,
};
use ketebe_server::{AppState, RuntimeCatalog, app};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-multi-search-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

async fn create_collection(http: &reqwest::Client, base_url: &str, collection: &str) {
    let response = http
        .post(format!("{base_url}/v0/collections"))
        .json(&json!({"id":collection, "dimension":2, "metric":"l2"}))
        .send()
        .await
        .expect("create collection");
    assert!(response.status().is_success());
}

async fn upsert_record(
    http: &reqwest::Client,
    base_url: &str,
    collection: &str,
    id: &str,
    vector: [f32; 2],
) {
    let response = http
        .post(format!(
            "{base_url}/v0/collections/{collection}/records:batchUpsert"
        ))
        .json(&json!({
            "records": [{
                "id":{"type":"string","value":id},
                "vector":vector,
                "metadata":{"source":collection}
            }]
        }))
        .send()
        .await
        .expect("upsert record");
    assert!(response.status().is_success());
}

#[tokio::test]
async fn mcp_multi_search_preserves_provenance_order_and_partial_failures() {
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
    create_collection(&http, &base_url, "docs-a").await;
    create_collection(&http, &base_url, "docs-b").await;
    upsert_record(&http, &base_url, "docs-a", "a", [1.0, 0.0]).await;
    upsert_record(&http, &base_url, "docs-b", "b", [2.0, 0.0]).await;

    let api = KetebeApi::new(base_url).expect("MCP API adapter");
    let output = api
        .search_many_params(
            SearchManyParams {
                collections: vec![
                    SearchManyTarget {
                        collection: "docs-b".into(),
                        search_profile: None,
                    },
                    SearchManyTarget {
                        collection: "hidden-or-missing".into(),
                        search_profile: None,
                    },
                    SearchManyTarget {
                        collection: "docs-a".into(),
                        search_profile: None,
                    },
                ],
                mode: SearchMode::Dense,
                text: None,
                vector: Some(vec![1.0, 0.0]),
                filter: None,
                after: None,
                before: None,
                prefer_recent: false,
                limit: 1,
                fields: vec!["metadata.source".into()],
                execution: Some("exact".into()),
                dense_candidates: None,
                sparse_candidates: None,
                timeout_ms: None,
                explain: false,
            },
            None,
        )
        .await
        .expect("multi-search envelope");

    assert_eq!(output.results.len(), 3);
    assert_eq!(output.results[0].collection, "docs-b");
    assert_eq!(output.results[0].status, CollectionSearchStatus::Ok);
    assert_eq!(output.results[0].hits.len(), 1);
    assert_eq!(
        output.results[0].hits[0].id,
        AgentRecordId::String("b".into())
    );

    assert_eq!(output.results[1].collection, "hidden-or-missing");
    assert_eq!(output.results[1].status, CollectionSearchStatus::Error);
    let error = output.results[1]
        .error
        .as_ref()
        .expect("typed multi-search failure");
    assert_eq!(error.code, "collection_not_found");
    assert_eq!(error.category, McpErrorCategory::NotFound);
    assert!(!error.retryable);

    assert_eq!(output.results[2].collection, "docs-a");
    assert_eq!(output.results[2].status, CollectionSearchStatus::Ok);
    assert_eq!(output.results[2].hits.len(), 1);
    assert_eq!(
        output.results[2].hits[0].id,
        AgentRecordId::String("a".into())
    );

    assert_eq!(output.merge_input.len(), 2);
    assert_eq!(output.merge_input[0].source_collection, "docs-b");
    assert_eq!(output.merge_input[0].source_rank, 1);
    assert_eq!(output.merge_input[1].source_collection, "docs-a");
    assert_eq!(output.merge_input[1].source_rank, 1);

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}
