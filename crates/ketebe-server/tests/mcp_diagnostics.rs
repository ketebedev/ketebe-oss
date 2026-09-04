use ketebe_mcp::{
    diagnostics::ExplainSearchOutput,
    fusion::{DedupStrategy, FusedSearchParams, FusionStrategy},
    ketebe::KetebeApi,
    multi_search::SearchManyTarget,
    search::SearchMode,
};
use ketebe_server::{AppState, RuntimeCatalog, app};
use serde_json::json;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-diagnostics-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

#[tokio::test]
async fn mcp_diagnostics_preserve_safe_public_explain_and_latency() {
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
    assert!(
        http.post(format!("{base_url}/v0/collections"))
            .json(&json!({"id":"docs", "dimension":2, "metric":"l2"}))
            .send()
            .await
            .expect("create")
            .status()
            .is_success()
    );
    assert!(
        http.post(format!(
            "{base_url}/v0/collections/docs/records:batchUpsert"
        ))
        .json(&json!({"records":[{
            "id":{"type":"string","value":"a"},
            "vector":[1.0,0.0],
            "metadata":{"kind":"doc","title":"safe"}
        }]}))
        .send()
        .await
        .expect("upsert")
        .status()
        .is_success()
    );

    let params = FusedSearchParams {
        collections: vec![SearchManyTarget {
            collection: "docs".into(),
            search_profile: None,
        }],
        mode: SearchMode::Dense,
        text: None,
        vector: Some(vec![1.0, 0.0]),
        filter: Some(json!({"op":"eq","path":["kind"],"value":"doc"})),
        after: None,
        before: None,
        prefer_recent: false,
        candidate_limit: 5,
        final_limit: 5,
        fields: vec!["metadata.title".into()],
        execution: Some("exact".into()),
        dense_candidates: None,
        sparse_candidates: None,
        timeout_ms: None,
        fusion: FusionStrategy::Rrf,
        rrf_k: 60,
        dedup: DedupStrategy::RecordId,
        rerank: None,
        explain: true,
    };
    let api = KetebeApi::new(base_url).expect("MCP API adapter");
    let started = Instant::now();
    let result = api
        .search_fused_params(params.clone(), None)
        .await
        .expect("search");
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let output = ExplainSearchOutput::from_execution(&params, elapsed_ms, result);

    assert_eq!(output.plan.collections, vec!["docs"]);
    assert!(output.plan.has_filter);
    assert_eq!(output.diagnostics.successful_collections, 1);
    assert_eq!(output.diagnostics.failed_collections, 0);
    assert_eq!(output.diagnostics.returned_hits, 1);
    let diagnostic = &output.diagnostics.collection_diagnostics[0];
    assert_eq!(diagnostic.collection, "docs");
    assert_eq!(diagnostic.filtered_returned_hits, Some(1));
    assert_eq!(diagnostic.dense_candidates, Some(5));
    assert_eq!(diagnostic.search_profile.as_deref(), Some("default@1"));
    assert!(
        !serde_json::to_string(&output)
            .expect("json")
            .contains("internal_node")
    );

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}
