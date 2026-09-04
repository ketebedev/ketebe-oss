use ketebe_mcp::{
    ketebe::KetebeApi,
    retrieval::AgentRecordId,
    search::{SearchMode, SearchParams},
};
use ketebe_server::{
    AppState, AuthenticationError, AuthenticationService, AuthorizationService, Credential,
    CredentialAuthenticator, DeterministicEmbeddingProvider, EmbeddingProvider, Principal,
    ProjectRole, RuntimeCatalog, app, app_with_authentication,
};
use serde_json::json;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-mcp-search-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

fn params(mode: SearchMode) -> SearchParams {
    SearchParams {
        collection: "docs".into(),
        mode,
        text: None,
        vector: None,
        filter: None,
        after: None,
        before: None,
        prefer_recent: false,
        limit: 10,
        fields: Vec::new(),
        execution: Some("exact".into()),
        dense_candidates: None,
        sparse_candidates: None,
        search_profile: None,
        timeout_ms: Some(500),
        explain: true,
    }
}

struct ProjectAuthenticator;

impl CredentialAuthenticator for ProjectAuthenticator {
    fn authenticate(&self, credential: &Credential) -> Result<Principal, AuthenticationError> {
        match credential.expose_secret() {
            "project-a-token" => Principal::for_project("subject-a", "project-a"),
            "project-a-reader-token" => Principal::for_project("reader-a", "project-a"),
            "project-b-token" => Principal::for_project("subject-b", "project-b"),
            _ => Err(AuthenticationError::InvalidCredential),
        }
    }
}

#[tokio::test]
async fn mcp_search_matches_public_query_dense_sparse_hybrid_and_filtering() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let provider =
        Arc::new(DeterministicEmbeddingProvider::new("test-model", "v1").expect("provider"));
    let embedded_vector = provider
        .embed("embedded alpha", 2)
        .await
        .expect("embedded query vector");
    state.set_embedding_provider(provider).await;
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

    let response = http
        .post(format!(
            "{base_url}/v0/collections/docs/records:batchUpsert"
        ))
        .json(&json!({
            "records": [
                {
                    "id": {"type": "string", "value": "alpha"},
                    "vector": [1.0, 0.0],
                    "metadata": {"title": "alpha guide", "category": "book", "hidden": "a"}
                },
                {
                    "id": {"type": "string", "value": "beta"},
                    "vector": [3.0, 0.0],
                    "metadata": {"title": "beta notes", "category": "game", "hidden": "b"}
                },
                {
                    "id": {"type": "string", "value": "embedded"},
                    "vector": embedded_vector,
                    "metadata": {"title": "embedded alpha", "category": "book"}
                }
            ]
        }))
        .send()
        .await
        .expect("batch upsert");
    assert!(response.status().is_success());

    let api = KetebeApi::new(base_url).expect("MCP API adapter");

    let mut dense = params(SearchMode::Dense);
    dense.vector = Some(vec![1.0, 0.0]);
    dense.limit = 1;
    dense.fields = vec!["metadata.title".into()];
    let dense = api.search_params(dense, None).await.expect("dense search");
    assert_eq!(dense.mode, SearchMode::Dense);
    assert_eq!(dense.hits.len(), 1);
    assert_eq!(dense.hits[0].id, AgentRecordId::String("alpha".into()));
    assert_eq!(
        dense.hits[0].metadata.as_ref().expect("metadata"),
        &json!({"title": "alpha guide"})
    );

    let mut embedded_dense = params(SearchMode::Dense);
    embedded_dense.text = Some("embedded alpha".into());
    embedded_dense.limit = 1;
    let embedded_dense = api
        .search_params(embedded_dense, None)
        .await
        .expect("server-embedded dense search");
    assert_eq!(embedded_dense.mode, SearchMode::Dense);
    assert_eq!(
        embedded_dense.hits[0].id,
        AgentRecordId::String("embedded".into())
    );
    assert_eq!(
        embedded_dense.explain.as_ref().expect("explain")["mode"],
        "dense"
    );

    let mut sparse = params(SearchMode::Sparse);
    sparse.execution = None;
    sparse.text = Some("alpha".into());
    sparse.sparse_candidates = Some(3);
    let sparse = api
        .search_params(sparse, None)
        .await
        .expect("sparse search");
    assert_eq!(sparse.mode, SearchMode::Sparse);
    assert_eq!(sparse.hits[0].id, AgentRecordId::String("alpha".into()));
    assert_eq!(sparse.explain.as_ref().expect("explain")["mode"], "lexical");

    let mut hybrid = params(SearchMode::Hybrid);
    hybrid.vector = Some(vec![1.0, 0.0]);
    hybrid.text = Some("alpha".into());
    hybrid.dense_candidates = Some(3);
    hybrid.sparse_candidates = Some(3);
    hybrid.limit = 1;
    hybrid.filter = Some(json!({
        "op": "and",
        "predicates": [
            {"op": "eq", "path": ["category"], "value": "book"},
            {"op": "exists", "path": ["title"]}
        ]
    }));
    let hybrid = api
        .search_params(hybrid, None)
        .await
        .expect("hybrid search");
    assert_eq!(hybrid.mode, SearchMode::Hybrid);
    assert_eq!(hybrid.hits.len(), 1);
    assert_eq!(hybrid.hits[0].id, AgentRecordId::String("alpha".into()));
    assert_eq!(hybrid.explain.as_ref().expect("explain")["mode"], "hybrid");

    let mut embedded_hybrid = params(SearchMode::Hybrid);
    embedded_hybrid.text = Some("embedded alpha".into());
    embedded_hybrid.dense_candidates = Some(3);
    embedded_hybrid.sparse_candidates = Some(3);
    embedded_hybrid.limit = 1;
    let embedded_hybrid = api
        .search_params(embedded_hybrid, None)
        .await
        .expect("server-embedded hybrid search");
    assert_eq!(embedded_hybrid.mode, SearchMode::Hybrid);
    assert_eq!(
        embedded_hybrid.explain.as_ref().expect("explain")["mode"],
        "hybrid"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn mcp_search_is_project_scoped_and_undiscoverable_cross_project() {
    let dir = temp_dir();
    let authorization = AuthorizationService::required(&dir).expect("authorization");
    authorization
        .set_project_role("project-a", "reader-a", ProjectRole::Reader)
        .expect("reader role");
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100)
            .with_authorization(authorization);
    state
        .set_embedding_provider(Arc::new(
            DeterministicEmbeddingProvider::new("test-model", "v1").expect("provider"),
        ))
        .await;
    let authentication = AuthenticationService::required(Arc::new(ProjectAuthenticator));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app_with_authentication(state, authentication))
            .await
            .expect("server");
    });

    let base_url = format!("http://{address}");
    let http = reqwest::Client::new();
    let response = http
        .post(format!("{base_url}/v0/collections"))
        .bearer_auth("project-a-token")
        .json(&json!({"id":"docs", "dimension":2, "metric":"l2"}))
        .send()
        .await
        .expect("create collection");
    assert!(response.status().is_success());

    let response = http
        .post(format!(
            "{base_url}/v0/collections/docs/records:batchUpsert"
        ))
        .bearer_auth("project-a-token")
        .json(&json!({
            "records": [{
                "id": {"type":"string", "value":"owned"},
                "vector": [1.0, 0.0],
                "metadata": {"title":"owned"}
            }]
        }))
        .send()
        .await
        .expect("upsert record");
    assert!(response.status().is_success());

    let api = KetebeApi::new(base_url).expect("MCP API adapter");
    let mut query = params(SearchMode::Dense);
    query.vector = Some(vec![1.0, 0.0]);
    query.limit = 1;
    let allowed = api
        .search_params(query.clone(), Some("project-a-token"))
        .await
        .expect("project owner search");
    assert_eq!(allowed.hits[0].id, AgentRecordId::String("owned".into()));

    let mut reader_query = params(SearchMode::Dense);
    reader_query.text = Some("reader query".into());
    reader_query.limit = 1;
    let reader = api
        .search_params(reader_query, Some("project-a-reader-token"))
        .await
        .expect("project reader search");
    assert_eq!(reader.hits[0].id, AgentRecordId::String("owned".into()));

    let denied = api
        .search_params(query, Some("project-b-token"))
        .await
        .expect_err("cross-project search must fail closed");
    assert!(denied.contains("404 collection_not_found"));

    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}
