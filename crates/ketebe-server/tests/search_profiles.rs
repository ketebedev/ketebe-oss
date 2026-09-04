use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use ketebe_core::CollectionId;
use ketebe_server::{
    AppState, PendingRecord, RuntimeCatalog, SearchProfile, SearchProfileExecution,
    SearchProfileStore, WriteService, app,
};
use serde_json::{Value, json};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-search-profile-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

#[test]
fn persisted_profiles_are_immutable_versioned_and_restart_safe() {
    let dir = temp_dir();
    let store = SearchProfileStore::new(dir.clone());
    let mut v1 = SearchProfile {
        name: "balanced".into(),
        version: 1,
        execution: SearchProfileExecution::Exact,
        dense_candidates: Some(20),
        lexical_candidates: Some(30),
        final_top_k: 5,
        timeout_ms: Some(250),
        ..SearchProfile::default()
    };
    store.create("docs", v1.clone()).unwrap();
    assert!(store.create("docs", v1.clone()).is_err());

    v1.version = 2;
    v1.final_top_k = 7;
    store.create("docs", v1.clone()).unwrap();
    assert_eq!(store.get("docs", "balanced").unwrap().version, 2);
    assert_eq!(store.get("docs", "balanced@1").unwrap().final_top_k, 5);

    let reopened = SearchProfileStore::new(dir.clone());
    assert_eq!(reopened.get("docs", "balanced@2").unwrap().final_top_k, 7);
    assert_eq!(reopened.list("docs").unwrap().len(), 2);
    reopened.delete("docs", "balanced@1").unwrap();
    assert!(reopened.get("docs", "balanced@1").is_err());
    let _ = fs::remove_dir_all(dir);
}

async fn request_json(
    state: AppState,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = if let Some(body) = body {
        builder = builder.header("content-type", "application/json");
        Body::from(body.to_string())
    } else {
        Body::empty()
    };
    let response = app(state)
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn query(state: AppState, body: Value) -> (StatusCode, Value) {
    request_json(
        state,
        Method::POST,
        "/v1/collections/docs/query",
        Some(body),
    )
    .await
}

async fn create_docs_collection(state: AppState) -> CollectionId {
    let collection = CollectionId::new("docs").unwrap();
    WriteService::new(state)
        .create_collection(
            collection.clone(),
            2,
            ketebe_core::DistanceMetric::L2,
            vec![],
        )
        .await
        .unwrap();
    collection
}

#[tokio::test]
async fn management_api_creates_lists_resolves_and_deletes_versioned_profiles() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    create_docs_collection(state.clone()).await;

    let body_v1 = json!({
        "name": "balanced",
        "version": 1,
        "execution": "exact",
        "dense_candidates": 20,
        "lexical_candidates": 30,
        "rrf_k": 60,
        "final_top_k": 5,
        "timeout_ms": 250
    });
    let (status, created) = request_json(
        state.clone(),
        Method::POST,
        "/v1/collections/docs/search-profiles",
        Some(body_v1.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["pinned_id"], "balanced@1");
    assert_eq!(created["execution"], "exact");

    let (status, _) = request_json(
        state.clone(),
        Method::POST,
        "/v1/collections/docs/search-profiles",
        Some(body_v1),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (status, created_v2) = request_json(
        state.clone(),
        Method::POST,
        "/v1/collections/docs/search-profiles",
        Some(json!({
            "name": "balanced",
            "version": 2,
            "execution": "auto",
            "dense_candidates": 40,
            "lexical_candidates": 50,
            "rrf_k": 75,
            "final_top_k": 7,
            "timeout_ms": 500
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created_v2["pinned_id"], "balanced@2");

    let (status, listed) = request_json(
        state.clone(),
        Method::GET,
        "/v1/collections/docs/search-profiles",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(listed.as_array().unwrap().len(), 2);

    let (status, latest) = request_json(
        state.clone(),
        Method::GET,
        "/v1/collections/docs/search-profiles/balanced",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(latest["pinned_id"], "balanced@2");
    assert_eq!(latest["final_top_k"], 7);

    let (status, pinned) = request_json(
        state.clone(),
        Method::GET,
        "/v1/collections/docs/search-profiles/balanced@1",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pinned["pinned_id"], "balanced@1");

    let (status, deleted) = request_json(
        state.clone(),
        Method::DELETE,
        "/v1/collections/docs/search-profiles/balanced@1",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(deleted["pinned_id"], "balanced@1");

    let (status, _) = request_json(
        state,
        Method::GET,
        "/v1/collections/docs/search-profiles/balanced@1",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test]
async fn query_resolves_latest_or_pinned_profile_and_request_overrides_win() {
    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let collection = create_docs_collection(state.clone()).await;
    WriteService::new(state.clone())
        .upsert(
            &collection,
            PendingRecord {
                id: ketebe_core::RecordId::string("one").unwrap(),
                vector: vec![1.0, 0.0],
                metadata: Default::default(),
            },
        )
        .await
        .unwrap();

    let store = SearchProfileStore::new(dir.clone());
    store
        .create(
            "docs",
            SearchProfile {
                name: "fast".into(),
                version: 1,
                execution: SearchProfileExecution::Exact,
                dense_candidates: Some(7),
                final_top_k: 3,
                timeout_ms: Some(125),
                ..SearchProfile::default()
            },
        )
        .unwrap();
    store
        .create(
            "docs",
            SearchProfile {
                name: "fast".into(),
                version: 2,
                execution: SearchProfileExecution::Exact,
                dense_candidates: Some(9),
                final_top_k: 4,
                timeout_ms: Some(250),
                ..SearchProfile::default()
            },
        )
        .unwrap();

    let (status, latest) = query(
        state.clone(),
        json!({"vector": [1.0, 0.0], "search_profile": "fast", "explain": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(latest["explain"]["search_profile"], "fast@2");
    assert_eq!(latest["explain"]["top_k"], 4);
    assert_eq!(latest["explain"]["dense_candidates"], 9);
    assert_eq!(latest["explain"]["timeout_ms"], 250);

    let (status, pinned) = query(
        state.clone(),
        json!({
            "vector": [1.0, 0.0],
            "search_profile": "fast@1",
            "top_k": 1,
            "dense_candidates": 2,
            "timeout_ms": 50,
            "explain": true
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pinned["explain"]["search_profile"], "fast@1");
    assert_eq!(pinned["explain"]["top_k"], 1);
    assert_eq!(pinned["explain"]["dense_candidates"], 2);
    assert_eq!(pinned["explain"]["timeout_ms"], 50);

    let (status, builtin) = query(state, json!({"vector": [1.0, 0.0], "explain": true})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(builtin["explain"]["search_profile"], "default@1");
    let _ = fs::remove_dir_all(dir);
}
