use axum::{
    Router,
    extract::Request,
    http::{StatusCode, header},
    middleware,
    response::IntoResponse,
    routing::get,
};
use ketebe_mcp::{
    auth::{AuthMode, RemoteAuthState, authenticate_remote},
    ketebe::KetebeApi,
};
use std::net::{SocketAddr, TcpListener};

fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn upstream_collections(request: Request) -> impl IntoResponse {
    match request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        Some("Bearer allow-token") => (
            StatusCode::OK,
            axum::Json(serde_json::json!({"collections": []})),
        ),
        Some("Bearer forbidden-token") => (
            StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({
                "error": {"code": "forbidden", "message": "authorization denied"}
            })),
        ),
        _ => (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({
                "error": {"code": "unauthenticated", "message": "authentication required"}
            })),
        ),
    }
}

#[tokio::test]
async fn required_remote_auth_forwards_bearer_and_preserves_upstream_allow_deny() {
    let upstream_addr = free_addr();
    let upstream = Router::new().route("/v0/collections", get(upstream_collections));
    let upstream_task = tokio::spawn(async move {
        axum_server::bind(upstream_addr)
            .serve(upstream.into_make_service())
            .await
            .unwrap();
    });

    let mcp_addr = free_addr();
    let auth_state = RemoteAuthState {
        mode: AuthMode::Required,
        api: KetebeApi::new(format!("http://{upstream_addr}")).unwrap(),
    };
    let protected = Router::new()
        .route("/protected", get(|| async { StatusCode::OK }))
        .layer(middleware::from_fn_with_state(
            auth_state,
            authenticate_remote,
        ));
    let mcp_task = tokio::spawn(async move {
        axum_server::bind(mcp_addr)
            .serve(protected.into_make_service())
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let client = reqwest::Client::new();
    let url = format!("http://{mcp_addr}/protected");

    let missing = client.get(&url).send().await.unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::UNAUTHORIZED);

    let invalid = client
        .get(&url)
        .bearer_auth("invalid-token")
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), reqwest::StatusCode::UNAUTHORIZED);

    let forbidden = client
        .get(&url)
        .bearer_auth("forbidden-token")
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);

    let allowed = client
        .get(&url)
        .bearer_auth("allow-token")
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), reqwest::StatusCode::OK);

    mcp_task.abort();
    upstream_task.abort();
}
