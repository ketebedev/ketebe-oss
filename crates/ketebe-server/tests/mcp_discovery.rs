use ketebe_mcp::{
    discovery::{CollectionStatsOutput, CollectionView},
    ketebe::KetebeApi,
};
use ketebe_server::{AppState, RuntimeCatalog, app};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-discovery-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

#[tokio::test]
async fn mcp_discovery_round_trips_through_public_api_against_real_server() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app(state)).await.expect("server");
    });

    let base_url = format!("http://{address}");
    let response = reqwest::Client::new()
        .post(format!("{base_url}/v0/collections"))
        .json(&json!({
            "id": "docs",
            "dimension": 3,
            "metric": "cosine",
            "lexical_fields": [["title"]]
        }))
        .send()
        .await
        .expect("create collection request");
    assert!(response.status().is_success());

    let api = KetebeApi::new(base_url).expect("MCP API adapter");
    let collections = api.list_collections(None).await.expect("list collections");
    assert_eq!(collections.len(), 1);

    let list_view = CollectionView::from(collections.into_iter().next().expect("collection"));
    assert_eq!(list_view.id, "docs");
    assert_eq!(list_view.dimension, 3);
    assert_eq!(list_view.metric, "cosine");
    assert!(!list_view.metadata.contains_key("shard_id"));
    assert!(!list_view.metadata.contains_key("node"));

    let described = api
        .get_collection("docs", None)
        .await
        .expect("describe collection");
    let described_view = CollectionView::from(described.clone());
    assert_eq!(described_view.id, "docs");
    assert!(!described_view.metadata.contains_key("shard_id"));
    assert!(!described_view.metadata.contains_key("node"));

    let stats = CollectionStatsOutput::from(described);
    assert_eq!(stats.id, "docs");
    assert_eq!(stats.dimension, 3);
    assert_eq!(stats.metric, "cosine");

    assert!(
        api.get_collection("missing", None).await.is_err(),
        "missing collection must fail closed"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}
