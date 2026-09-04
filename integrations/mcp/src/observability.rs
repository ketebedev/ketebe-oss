use std::{
    collections::BTreeMap,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use reqwest::Method;
use serde_json::Value;

const REQUEST_ID_HEADER: &str = "x-request-id";
const ORIGIN_HEADER: &str = "x-ketebe-origin";
const ORIGIN_MCP: &str = "mcp";

static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static METRICS: OnceLock<Metrics> = OnceLock::new();

tokio::task_local! {
    static CURRENT_CORRELATION_ID: CorrelationId;
}

#[derive(Debug, Default)]
struct Metrics {
    requests_total: AtomicU64,
    request_errors_total: AtomicU64,
    request_latency_micros_total: AtomicU64,
    auth_denied_total: AtomicU64,
    rbac_denied_total: AtomicU64,
    tool_calls: Mutex<BTreeMap<String, u64>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorrelationId(String);

impl CorrelationId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct ObservedHttpClient {
    inner: reqwest::Client,
}

impl Default for ObservedHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ObservedHttpClient {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: reqwest::Client::new(),
        }
    }

    pub fn get(&self, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        self.request(Method::GET, url)
    }

    pub fn post(&self, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        self.request(Method::POST, url)
    }

    pub fn put(&self, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        self.request(Method::PUT, url)
    }

    pub fn delete(&self, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        self.request(Method::DELETE, url)
    }

    pub fn patch(&self, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        self.request(Method::PATCH, url)
    }

    pub fn request(&self, method: Method, url: impl reqwest::IntoUrl) -> reqwest::RequestBuilder {
        decorate_downstream_request(self.inner.request(method, url))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RequestMetadata {
    method: Option<String>,
    tool: Option<String>,
}

fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::default)
}

fn valid_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn request_id(request: &Request<Body>) -> CorrelationId {
    if let Some(value) = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| valid_request_id(value))
    {
        return CorrelationId(value.to_string());
    }
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let sequence = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    CorrelationId(format!("mcp-{millis}-{sequence}"))
}

fn current_correlation_id() -> Option<CorrelationId> {
    CURRENT_CORRELATION_ID.try_with(Clone::clone).ok()
}

fn decorate_downstream_request(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let builder = builder.header(ORIGIN_HEADER, ORIGIN_MCP);
    match current_correlation_id() {
        Some(correlation_id) => builder.header(REQUEST_ID_HEADER, correlation_id.as_str()),
        None => builder,
    }
}

fn request_metadata(bytes: &[u8]) -> RequestMetadata {
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
        return RequestMetadata::default();
    };
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_string);
    let tool = if method.as_deref() == Some("tools/call") {
        value
            .get("params")
            .and_then(|params| params.get("name"))
            .and_then(Value::as_str)
            .map(str::to_string)
    } else {
        None
    };
    RequestMetadata { method, tool }
}

pub async fn observe_http_request(
    State(max_request_bytes): State<usize>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let correlation_id = request_id(&request);
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, max_request_bytes).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let metadata = request_metadata(&bytes);
    request = Request::from_parts(parts, Body::from(bytes));
    request.extensions_mut().insert(correlation_id.clone());
    request.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(correlation_id.as_str())
            .expect("generated or validated request id must be a valid header value"),
    );
    request
        .headers_mut()
        .insert(ORIGIN_HEADER, HeaderValue::from_static(ORIGIN_MCP));

    let started = std::time::Instant::now();
    let response = CURRENT_CORRELATION_ID
        .scope(correlation_id.clone(), next.run(request))
        .await;
    let elapsed = started.elapsed();
    observe_request(&metadata, response.status(), elapsed);

    tracing::info!(
        event = "mcp_request",
        origin = ORIGIN_MCP,
        request_id = %correlation_id.as_str(),
        rpc_method = metadata.method.as_deref().unwrap_or("unknown"),
        tool = metadata.tool.as_deref().unwrap_or("none"),
        status = response.status().as_u16(),
        latency_ms = elapsed.as_millis(),
        "MCP request completed"
    );

    let mut response = response;
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(correlation_id.as_str())
            .expect("generated or validated request id must be a valid header value"),
    );
    response
}

fn observe_request(metadata: &RequestMetadata, status: StatusCode, elapsed: Duration) {
    let metrics = metrics();
    metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    metrics.request_latency_micros_total.fetch_add(
        u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    if status.is_client_error() || status.is_server_error() {
        metrics.request_errors_total.fetch_add(1, Ordering::Relaxed);
    }
    if let Some(tool) = metadata.tool.as_ref() {
        let mut calls = metrics
            .tool_calls
            .lock()
            .expect("MCP tool metrics lock poisoned");
        *calls.entry(tool.clone()).or_default() += 1;
    }
}

pub fn observe_auth_denial(forbidden: bool) {
    let metrics = metrics();
    metrics.auth_denied_total.fetch_add(1, Ordering::Relaxed);
    if forbidden {
        metrics.rbac_denied_total.fetch_add(1, Ordering::Relaxed);
    }
}

#[must_use]
pub fn prometheus_metrics() -> String {
    let metrics = metrics();
    let requests = metrics.requests_total.load(Ordering::Relaxed);
    let latency_micros = metrics.request_latency_micros_total.load(Ordering::Relaxed);
    let mut output = format!(
        "# TYPE ketebe_mcp_requests_total counter\nketebe_mcp_requests_total {requests}\n\
# TYPE ketebe_mcp_request_errors_total counter\nketebe_mcp_request_errors_total {}\n\
# TYPE ketebe_mcp_request_latency_seconds_sum counter\nketebe_mcp_request_latency_seconds_sum {:.6}\n\
# TYPE ketebe_mcp_auth_denied_total counter\nketebe_mcp_auth_denied_total {}\n\
# TYPE ketebe_mcp_rbac_denied_total counter\nketebe_mcp_rbac_denied_total {}\n",
        metrics.request_errors_total.load(Ordering::Relaxed),
        latency_micros as f64 / 1_000_000.0,
        metrics.auth_denied_total.load(Ordering::Relaxed),
        metrics.rbac_denied_total.load(Ordering::Relaxed),
    );
    for (tool, count) in metrics
        .tool_calls
        .lock()
        .expect("MCP tool metrics lock poisoned")
        .iter()
    {
        let safe_tool = tool.replace(['\\', '"'], "_");
        output.push_str(&format!(
            "ketebe_mcp_tool_calls_total{{tool=\"{safe_tool}\"}} {count}\n"
        ));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_extracts_only_method_and_tool_name() {
        let metadata = request_metadata(
            br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"search","arguments":{"secret":"do-not-log"}}}"#,
        );
        assert_eq!(metadata.method.as_deref(), Some("tools/call"));
        assert_eq!(metadata.tool.as_deref(), Some("search"));
        assert!(!format!("{metadata:?}").contains("do-not-log"));
    }

    #[test]
    fn request_ids_are_strictly_bounded() {
        assert!(valid_request_id("client-123:abc"));
        assert!(!valid_request_id("contains space"));
        assert!(!valid_request_id(&"x".repeat(129)));
    }

    #[tokio::test]
    async fn downstream_requests_receive_origin_and_correlation_headers() {
        let client = ObservedHttpClient::new();
        let correlation_id = CorrelationId("test-correlation-1".into());
        let request = CURRENT_CORRELATION_ID
            .scope(correlation_id, async {
                client
                    .get("http://127.0.0.1/example")
                    .build()
                    .expect("request")
            })
            .await;
        assert_eq!(
            request
                .headers()
                .get(ORIGIN_HEADER)
                .and_then(|v| v.to_str().ok()),
            Some(ORIGIN_MCP)
        );
        assert_eq!(
            request
                .headers()
                .get(REQUEST_ID_HEADER)
                .and_then(|v| v.to_str().ok()),
            Some("test-correlation-1")
        );
    }
}
