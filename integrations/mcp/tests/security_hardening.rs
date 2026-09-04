use std::{
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    extract::Request,
    http::{StatusCode, header},
    middleware,
    response::IntoResponse,
    routing::{get, post},
};
use ketebe_mcp::{
    auth::{AuthMode, RemoteAuthState, RequestCredential, authenticate_remote},
    ketebe::KetebeApi,
    rate_limit::{RateLimitState, enforce_rate_limit},
};
use tower::ServiceExt;
use tower_http::{limit::RequestBodyLimitLayer, timeout::TimeoutLayer};

fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn auth_probe(request: Request) -> impl IntoResponse {
    match request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        Some("Bearer tenant-a") | Some("Bearer tenant-b") => (
            StatusCode::OK,
            axum::Json(serde_json::json!({"collections": []})),
        ),
        _ => (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({
                "error": {"code": "unauthenticated", "message": "authentication required"}
            })),
        ),
    }
}

async fn tenant_resource(request: Request) -> StatusCode {
    let tenant = request
        .uri()
        .path()
        .strip_prefix("/tenant/")
        .unwrap_or_default();
    let credential = request.extensions().get::<RequestCredential>().unwrap();
    if credential.expose_secret() == tenant {
        StatusCode::OK
    } else {
        StatusCode::FORBIDDEN
    }
}

#[tokio::test]
async fn authenticated_principal_cannot_cross_tenant_boundary() {
    let upstream_addr = free_addr();
    let upstream = Router::new().route("/v0/collections", get(auth_probe));
    let upstream_task = tokio::spawn(async move {
        axum_server::bind(upstream_addr)
            .serve(upstream.into_make_service())
            .await
            .unwrap();
    });

    let protected = Router::new()
        .route("/tenant/{name}", get(tenant_resource))
        .layer(middleware::from_fn_with_state(
            RemoteAuthState {
                mode: AuthMode::Required,
                api: KetebeApi::new(format!("http://{upstream_addr}")).unwrap(),
            },
            authenticate_remote,
        ));

    tokio::time::sleep(Duration::from_millis(50)).await;
    let allowed = protected
        .clone()
        .oneshot(
            Request::builder()
                .uri("/tenant/tenant-a")
                .header(header::AUTHORIZATION, "Bearer tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::OK);

    let denied = protected
        .oneshot(
            Request::builder()
                .uri("/tenant/tenant-b")
                .header(header::AUTHORIZATION, "Bearer tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    upstream_task.abort();
}

#[test]
fn credentials_are_redacted_from_debug_output() {
    let secret = "tenant-super-secret-token";
    let credential = RequestCredential::from_token(secret).unwrap();
    let rendered = format!("{credential:?}");
    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains(secret));
}

#[tokio::test]
async fn abuse_guards_enforce_rate_size_and_timeout_limits() {
    let rate_app = Router::new()
        .route("/mcp", get(|| async { StatusCode::OK }))
        .layer(middleware::from_fn_with_state(
            RateLimitState::per_second(1),
            enforce_rate_limit,
        ));
    let first = rate_app
        .clone()
        .oneshot(Request::builder().uri("/mcp").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let limited = rate_app
        .oneshot(Request::builder().uri("/mcp").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);

    let size_app = Router::new()
        .route("/mcp", post(|| async { StatusCode::OK }))
        .layer(RequestBodyLimitLayer::new(4));
    let oversized = size_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_LENGTH, "5")
                .body(Body::from("12345"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let timeout_app = Router::new()
        .route(
            "/mcp",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                StatusCode::OK
            }),
        )
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_millis(5),
        ));
    let timed_out = timeout_app
        .oneshot(Request::builder().uri("/mcp").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(timed_out.status(), StatusCode::REQUEST_TIMEOUT);
}
