use ketebe_mcp::{
    ketebe::KetebeApi,
    retrieval::AgentRecordId,
    search::{SearchMode, SearchParams},
};
use ketebe_server::{AppState, RuntimeCatalog, app};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-search-profiles-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

fn dense_params(profile: Option<&str>) -> SearchParams {
    SearchParams {
        collection: "docs".into(),
        mode: SearchMode::Dense,
        text: None,
        vector: Some(vec![1.0, 0.0]),
        filter: None,
        after: None,
        before: None,
        prefer_recent: false,
        limit: 10,
        fields: Vec::new(),
        execution: None,
        dense_candidates: None,
        sparse_candidates: None,
        search_profile: profile.map(str::to_string),
        timeout_ms: None,
        explain: true,
    }
}

#[tokio::test]
async fn mcp_search_profiles_are_discoverable_selectable_and_stable() {
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
        .json(&json!({"id":"docs", "dimension":2, "metric":"l2"}))
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
                {"id":{"type":"string","value":"a"}, "vector":[1.0,0.0], "metadata":{"title":"a"}},
                {"id":{"type":"string","value":"b"}, "vector":[2.0,0.0], "metadata":{"title":"b"}}
            ]
        }))
        .send()
        .await
        .expect("upsert records");
    assert!(response.status().is_success());

    let response = http
        .post(format!("{base_url}/v1/collections/docs/search-profiles"))
        .json(&json!({
            "name":"agent-default",
            "version":1,
            "execution":"exact",
            "dense_candidates":2,
            "rrf_k":60,
            "final_top_k":1,
            "timeout_ms":500
        }))
        .send()
        .await
        .expect("create profile");
    assert!(response.status().is_success());

    let api = KetebeApi::new(base_url).expect("MCP API adapter");
    let profiles = api
        .list_search_profiles("docs", None)
        .await
        .expect("list profiles");
    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0].pinned_id, "agent-default@1");
    assert_eq!(profiles[0].execution, "exact");
    assert_eq!(profiles[0].final_top_k, 1);

    let profile = api
        .get_search_profile("docs", "agent-default", None)
        .await
        .expect("describe latest profile");
    assert_eq!(profile.pinned_id, "agent-default@1");

    let output = api
        .search_params(dense_params(Some("agent-default")), None)
        .await
        .expect("profile search");
    assert_eq!(output.hits.len(), 1);
    assert_eq!(output.hits[0].id, AgentRecordId::String("a".into()));

    let missing = api
        .get_search_profile("docs", "missing", None)
        .await
        .expect_err("missing profile must be stable");
    assert_eq!(
        missing,
        "Ketebe search profile request failed: 404 search_profile_not_found"
    );

    let missing_search = api
        .search_params(dense_params(Some("missing")), None)
        .await
        .expect_err("missing selected profile must fail");
    assert!(missing_search.contains("search_profile_not_found"));

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}
