use ketebe_mcp::{
    fusion::{
        DedupStrategy, FusedSearchParams, FusionStrategy, RerankFailurePolicy, ServerRerankParams,
    },
    ketebe::KetebeApi,
    multi_search::SearchManyTarget,
    retrieval::AgentRecordId,
    search::SearchMode,
};
use ketebe_server::{
    AppState, RerankCandidate, RerankFuture, RerankScore, Reranker, RuntimeCatalog, app,
};
use serde_json::json;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

struct ReverseReranker;

impl Reranker for ReverseReranker {
    fn name(&self) -> &str {
        "reverse-test"
    }

    fn rerank<'a>(
        &'a self,
        _query: &'a str,
        candidates: &'a [RerankCandidate],
    ) -> RerankFuture<'a> {
        Box::pin(async move {
            Ok(candidates
                .iter()
                .enumerate()
                .map(|(index, _)| RerankScore {
                    index,
                    score: index as f32,
                })
                .collect())
        })
    }
}

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-fusion-{}",
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

async fn upsert_records(http: &reqwest::Client, base_url: &str, collection: &str, unique: &str) {
    let response = http
        .post(format!(
            "{base_url}/v0/collections/{collection}/records:batchUpsert"
        ))
        .json(&json!({
            "records": [
                {
                    "id":{"type":"string","value":"shared"},
                    "vector":[1.0,0.0],
                    "metadata":{"title":"shared candidate"}
                },
                {
                    "id":{"type":"string","value":unique},
                    "vector":[1.1,0.0],
                    "metadata":{"title":format!("{unique} candidate")}
                }
            ]
        }))
        .send()
        .await
        .expect("upsert records");
    assert!(response.status().is_success());
}

#[tokio::test]
async fn mcp_fusion_deduplicates_deterministically_and_uses_server_reranker() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    state.set_reranker(Arc::new(ReverseReranker)).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app(state)).await.expect("server");
    });

    let base_url = format!("http://{address}");
    let http = reqwest::Client::new();
    create_collection(&http, &base_url, "docs-a").await;
    create_collection(&http, &base_url, "docs-b").await;
    upsert_records(&http, &base_url, "docs-a", "a").await;
    upsert_records(&http, &base_url, "docs-b", "b").await;

    let api = KetebeApi::new(base_url).expect("MCP API adapter");
    let output = api
        .search_fused_params(
            FusedSearchParams {
                collections: vec![
                    SearchManyTarget {
                        collection: "docs-a".into(),
                        search_profile: None,
                    },
                    SearchManyTarget {
                        collection: "docs-b".into(),
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
                candidate_limit: 2,
                final_limit: 3,
                fields: vec!["metadata.title".into()],
                execution: Some("exact".into()),
                dense_candidates: None,
                sparse_candidates: None,
                timeout_ms: None,
                fusion: FusionStrategy::Rrf,
                rrf_k: 60,
                dedup: DedupStrategy::RecordId,
                rerank: Some(ServerRerankParams {
                    profile: "default".into(),
                    query: Some("agent query".into()),
                    top_n: 2,
                    text_fields: vec![vec!["title".into()]],
                    include_metadata: false,
                    failure_policy: RerankFailurePolicy::Fail,
                }),
                explain: true,
            },
            None,
        )
        .await
        .expect("fused search");

    assert_eq!(output.results.len(), 2);
    assert_eq!(output.hits.len(), 3);
    assert_eq!(output.hits[0].fusion_rank, 1);
    assert_eq!(output.hits[0].id, AgentRecordId::String("shared".into()));
    assert_eq!(output.hits[0].provenance.len(), 2);
    assert_eq!(output.hits[0].provenance[0].source_collection, "docs-a");
    assert_eq!(output.hits[0].provenance[0].source_rank, 2);
    assert_eq!(output.hits[0].provenance[1].source_collection, "docs-b");
    assert_eq!(output.hits[0].provenance[1].source_rank, 2);
    assert_eq!(output.hits[0].representative.rerank_score, Some(0.0));
    assert_eq!(output.hits[0].representative.original_rank, Some(1));

    let first_explain = output.results[0]
        .explain
        .as_ref()
        .expect("query explain must be preserved");
    assert_eq!(first_explain["rerank"]["provider"], "reverse-test");
    assert_eq!(first_explain["rerank"]["applied"], true);

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}
