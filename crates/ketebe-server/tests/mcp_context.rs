use ketebe_mcp::{
    context::{RetrieveContextParams, assemble_context},
    fusion::{DedupStrategy, FusedSearchParams, FusionStrategy},
    ketebe::KetebeApi,
    multi_search::SearchManyTarget,
    search::SearchMode,
};
use ketebe_server::{AppState, RuntimeCatalog, app};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-context-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

#[tokio::test]
async fn mcp_context_preserves_citations_and_applies_deterministic_budgets() {
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
    for collection in ["docs-a", "docs-b"] {
        assert!(
            http.post(format!("{base_url}/v0/collections"))
                .json(&json!({"id":collection, "dimension":2, "metric":"l2"}))
                .send()
                .await
                .expect("create")
                .status()
                .is_success()
        );
    }

    assert!(
        http.post(format!(
            "{base_url}/v0/collections/docs-a/records:batchUpsert"
        ))
        .json(&json!({"records":[{
            "id":{"type":"string","value":"shared"},
            "vector":[1.0,0.0],
            "metadata":{
                "content":"alpha beta gamma delta",
                "source_uri":"file:///docs/source-a.md",
                "document_id":"document-a",
                "chunk_id":"chunk-a1"
            }
        }]}))
        .send()
        .await
        .expect("upsert a")
        .status()
        .is_success()
    );
    assert!(
        http.post(format!(
            "{base_url}/v0/collections/docs-b/records:batchUpsert"
        ))
        .json(&json!({"records":[{
            "id":{"type":"string","value":"shared"},
            "vector":[1.0,0.0],
            "metadata":{
                "content":"alternate source content",
                "source_uri":"file:///docs/source-b.md",
                "document_id":"document-b",
                "chunk_id":"chunk-b1"
            }
        }]}))
        .send()
        .await
        .expect("upsert b")
        .status()
        .is_success()
    );

    let search = FusedSearchParams {
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
        candidate_limit: 5,
        final_limit: 5,
        fields: Vec::new(),
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
    let params = RetrieveContextParams {
        search: search.clone(),
        content_field: "content".into(),
        max_tokens: 3,
        max_bytes: 64,
        max_documents: 1,
    };

    let api = KetebeApi::new(base_url).expect("MCP API adapter");
    let result = api
        .search_fused_params(search, None)
        .await
        .expect("fused search");
    let output = assemble_context(&params, result);

    assert_eq!(output.blocks.len(), 1);
    assert_eq!(output.blocks[0].citation_id, "ctx-1");
    assert_eq!(output.blocks[0].text, "alpha beta gamma");
    assert!(output.blocks[0].truncated);
    assert_eq!(output.citations.len(), 1);
    assert_eq!(output.citations[0].collection, "docs-a");
    assert_eq!(
        output.citations[0].source_uri.as_deref(),
        Some("file:///docs/source-a.md")
    );
    assert_eq!(
        output.citations[0].document_id.as_deref(),
        Some("document-a")
    );
    assert_eq!(output.citations[0].chunk_id.as_deref(), Some("chunk-a1"));
    assert_eq!(output.citations[0].provenance.len(), 2);
    assert_eq!(
        output.citations[0].provenance[0].source_collection,
        "docs-a"
    );
    assert_eq!(
        output.citations[0].provenance[1].source_collection,
        "docs-b"
    );
    assert_eq!(output.budget.tokenizer, "unicode_whitespace_v0");
    assert_eq!(output.budget.used_tokens, 3);
    assert!(output.budget.used_bytes <= 64);
    assert_eq!(output.budget.used_documents, 1);
    assert_eq!(output.context_text, "[ctx-1]\nalpha beta gamma");

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}
