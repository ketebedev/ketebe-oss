use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tokio::sync::Mutex;

pub const DEFAULT_REQUESTS_PER_SECOND: usize = 100;

#[derive(Clone, Debug)]
pub struct RateLimitState {
    limit: usize,
    window: Duration,
    requests: Arc<Mutex<VecDeque<Instant>>>,
}

impl RateLimitState {
    #[must_use]
    pub fn per_second(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            window: Duration::from_secs(1),
            requests: Arc::new(Mutex::new(VecDeque::with_capacity(limit.max(1)))),
        }
    }

    async fn allow(&self, now: Instant) -> bool {
        let mut requests = self.requests.lock().await;
        while requests
            .front()
            .is_some_and(|seen| now.duration_since(*seen) >= self.window)
        {
            requests.pop_front();
        }
        if requests.len() >= self.limit {
            return false;
        }
        requests.push_back(now);
        true
    }
}

pub async fn enforce_rate_limit(
    State(state): State<RateLimitState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if state.allow(Instant::now()).await {
        return next.run(request).await;
    }

    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        axum::Json(serde_json::json!({
            "error": {
                "code": "rate_limited",
                "message": "MCP request rate limit exceeded"
            }
        })),
    )
        .into_response();
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, middleware, routing::get};
    use tower::ServiceExt;

    #[tokio::test]
    async fn rejects_requests_over_window_limit_and_recovers() {
        let state = RateLimitState {
            limit: 2,
            window: Duration::from_millis(25),
            requests: Arc::new(Mutex::new(VecDeque::new())),
        };
        let app = Router::new()
            .route("/mcp", get(|| async { StatusCode::OK }))
            .layer(middleware::from_fn_with_state(state, enforce_rate_limit));

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(Request::builder().uri("/mcp").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let limited = app
            .clone()
            .oneshot(Request::builder().uri("/mcp").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(limited.headers().get(header::RETRY_AFTER).unwrap(), "1");

        tokio::time::sleep(Duration::from_millis(30)).await;
        let recovered = app
            .oneshot(Request::builder().uri("/mcp").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(recovered.status(), StatusCode::OK);
    }
}
