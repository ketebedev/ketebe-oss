use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use ketebe_server::{AppState, RuntimeCatalog, app};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root")
}

fn read_json(path: &str) -> Value {
    serde_json::from_slice(&fs::read(repo_root().join(path)).expect("contract file"))
        .expect("valid JSON contract")
}

fn operation<'a>(spec: &'a Value, method: &str, path: &str) -> &'a Value {
    spec["paths"][path][method]
        .as_object()
        .map(|_| &spec["paths"][path][method])
        .unwrap_or_else(|| panic!("missing OpenAPI operation {method} {path}"))
}

#[test]
fn openapi_v1_is_a_stable_additive_client_contract() {
    let spec = read_json("api/openapi/v1.json");
    let baseline = read_json("api/openapi/v1.compatibility.json");

    assert_eq!(spec["openapi"], "3.1.0");
    assert_eq!(spec["x-ketebe-contract-version"], 1);
    assert_eq!(
        spec["x-ketebe-generated-client-boundary"],
        "api/openapi/v1.json"
    );
    assert!(
        spec["info"]["version"]
            .as_str()
            .is_some_and(|version| version.starts_with("1."))
    );

    let mut operation_ids = BTreeSet::new();
    for path_item in spec["paths"].as_object().expect("paths object").values() {
        for method in ["get", "post", "put", "patch", "delete"] {
            if let Some(operation) = path_item.get(method) {
                let id = operation["operationId"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{method} operation is missing operationId"));
                assert!(
                    operation_ids.insert(id.to_string()),
                    "duplicate operationId {id}"
                );
            }
        }
    }

    for baseline_operation in baseline["operations"]
        .as_array()
        .expect("compatibility operations")
    {
        let method = baseline_operation["method"].as_str().expect("method");
        let path = baseline_operation["path"].as_str().expect("path");
        let expected_id = baseline_operation["operation_id"]
            .as_str()
            .expect("operation id");
        assert_eq!(operation(&spec, method, path)["operationId"], expected_id);
    }

    for component in baseline["required_components"]
        .as_array()
        .expect("required components")
    {
        let component = component.as_str().expect("component name");
        assert!(
            spec["components"]["schemas"].get(component).is_some(),
            "required OpenAPI component {component} was removed"
        );
    }

    let envelope = &spec["components"]["schemas"]["ErrorEnvelope"];
    assert_eq!(envelope["additionalProperties"], false);
    assert!(
        envelope["required"]
            .as_array()
            .expect("ErrorEnvelope required")
            .iter()
            .any(|field| field == "error")
    );
    let error = &spec["components"]["schemas"]["Error"];
    let required = error["required"].as_array().expect("Error required");
    assert!(required.iter().any(|field| field == "code"));
    assert!(required.iter().any(|field| field == "message"));
}

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-openapi-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
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
        .oneshot(builder.body(body).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn assert_error_envelope(body: &Value) {
    assert!(
        body["error"]["code"].as_str().is_some(),
        "missing error.code: {body}"
    );
    assert!(
        body["error"]["message"].as_str().is_some(),
        "missing error.message: {body}"
    );
}

#[tokio::test]
async fn openapi_representative_operations_conform_to_the_running_server() {
    let spec = read_json("api/openapi/v1.json");
    for (method, path) in [
        ("post", "/v0/collections"),
        ("put", "/v0/collections/{collection_id}/records/{record_id}"),
        (
            "put",
            "/v0/collections/{collection_id}/documents/{record_id}",
        ),
        ("post", "/v1/collections/{collection_id}/query"),
        ("post", "/v1/collections/{collection_id}/search-profiles"),
        ("get", "/v0/jobs/{job_id}"),
        ("get", "/v0/collections/{collection_id}/embedding-migration"),
    ] {
        let _ = operation(&spec, method, path);
    }

    let dir = temp_dir();
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);

    let (status, health) = request_json(state.clone(), Method::GET, "/healthz", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health["status"], "ok");

    let (status, collection) = request_json(
        state.clone(),
        Method::POST,
        "/v0/collections",
        Some(json!({"id": "docs", "dimension": 2, "metric": "l2"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(collection["id"], "docs");

    let (status, mutation) = request_json(
        state.clone(),
        Method::PUT,
        "/v0/collections/docs/records/one",
        Some(json!({"vector": [1.0, 0.0], "metadata": {"title": "one"}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(mutation["sequence_number"].as_u64().is_some());

    let (status, query) = request_json(
        state.clone(),
        Method::POST,
        "/v1/collections/docs/query",
        Some(json!({"vector": [1.0, 0.0], "top_k": 1, "execution": "exact", "explain": true})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(query["api_version"], "v1");
    assert_eq!(query["hits"][0]["id"]["value"], "one");

    let (status, profile) = request_json(
        state.clone(),
        Method::POST,
        "/v1/collections/docs/search-profiles",
        Some(json!({"name": "balanced", "version": 1, "final_top_k": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(profile["pinned_id"], "balanced@1");

    let (status, document_error) = request_json(
        state.clone(),
        Method::PUT,
        "/v0/collections/missing/documents/doc-1",
        Some(json!({"text": "document route conformance"})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error_envelope(&document_error);

    let (status, job_error) =
        request_json(state.clone(), Method::GET, "/v0/jobs/not-a-job", None).await;
    assert!(status.is_client_error());
    assert_error_envelope(&job_error);

    let (status, migration_error) = request_json(
        state,
        Method::GET,
        "/v0/collections/docs/embedding-migration",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error_envelope(&migration_error);

    let _ = fs::remove_dir_all(dir);
}
