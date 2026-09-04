use axum::body::Body;
use axum::http::{HeaderMap, Request};
use axum::middleware::Next;
use axum::response::Response;
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::{Instrument as _, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

pub const TRACEPARENT_HEADER: &str = "traceparent";
pub const TRACESTATE_HEADER: &str = "tracestate";

pub struct ObservabilityGuard {
    provider: SdkTracerProvider,
}

impl Drop for ObservabilityGuard {
    fn drop(&mut self) {
        let _ = self.provider.shutdown();
    }
}

#[must_use]
pub fn init_observability() -> ObservabilityGuard {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let provider = match build_otlp_provider() {
        Ok(Some(provider)) => provider,
        Ok(None) => SdkTracerProvider::builder().build(),
        Err(error) => {
            eprintln!("Ketebe telemetry exporter disabled after configuration error: {error}");
            SdkTracerProvider::builder().build()
        }
    };
    global::set_tracer_provider(provider.clone());
    let tracer = provider.tracer("ketebe-server");

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,ketebe_server=info"));
    let formatting = tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .with_target(true);
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(formatting)
        .with(telemetry)
        .try_init();

    ObservabilityGuard { provider }
}

fn build_otlp_provider() -> Result<Option<SdkTracerProvider>, opentelemetry_otlp::ExporterBuildError>
{
    let enabled = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
        || std::env::var_os("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some();
    if !enabled {
        return Ok(None);
    }
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()?;
    Ok(Some(
        SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .build(),
    ))
}

pub async fn http_trace(request: Request<Body>, next: Next) -> Response {
    let span = inbound_span(request.headers(), "http", request.method().as_str());
    let response = next.run(request).instrument(span.clone()).await;
    span.record("http.status_code", response.status().as_u16());
    tracing::info!(parent: &span, event = "request.completed", status_code = response.status().as_u16());
    response
}

pub fn grpc_span<B>(request: &Request<B>) -> Span {
    inbound_span(request.headers(), "grpc", "POST")
}

pub fn kafka_span(headers: Option<&HeaderMap>, partition: i32, records: usize) -> Span {
    let span = tracing::info_span!(
        "ketebe.kafka.ingest",
        otel.kind = "consumer",
        messaging.system = "kafka",
        kafka.partition = partition,
        messaging.batch.message_count = records,
        trace_id = tracing::field::Empty,
    );
    if let Some(headers) = headers {
        set_parent_and_trace_id(&span, headers);
    } else {
        record_trace_id(&span);
    }
    span
}

fn inbound_span(headers: &HeaderMap, transport: &'static str, method: &str) -> Span {
    let span = tracing::info_span!(
        "ketebe.request",
        otel.kind = "server",
        transport = transport,
        http.request.method = method,
        http.status_code = tracing::field::Empty,
        trace_id = tracing::field::Empty,
    );
    set_parent_and_trace_id(&span, headers);
    span
}

fn set_parent_and_trace_id(span: &Span, headers: &HeaderMap) {
    let parent = global::get_text_map_propagator(|propagator| {
        propagator.extract(&SafeHeaderExtractor(headers))
    });
    let _ = span.set_parent(parent);
    record_trace_id(span);
}

fn record_trace_id(span: &Span) {
    let context = span.context();
    let trace_id = context.span().span_context().trace_id().to_string();
    span.record("trace_id", trace_id.as_str());
}

struct SafeHeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for SafeHeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        if !is_trace_context_header(key) {
            return None;
        }
        self.0.get(key)?.to_str().ok()
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(axum::http::HeaderName::as_str)
            .filter(|name| is_trace_context_header(name))
            .collect()
    }
}

fn is_trace_context_header(name: &str) -> bool {
    name.eq_ignore_ascii_case(TRACEPARENT_HEADER) || name.eq_ignore_ascii_case(TRACESTATE_HEADER)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn propagation_extractor_redacts_credentials_and_payload_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            TRACEPARENT_HEADER,
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
        headers.insert("cookie", HeaderValue::from_static("session=secret"));
        headers.insert("x-api-key", HeaderValue::from_static("secret"));
        headers.insert(
            "x-document-text",
            HeaderValue::from_static("private payload"),
        );
        let extractor = SafeHeaderExtractor(&headers);

        assert!(extractor.get(TRACEPARENT_HEADER).is_some());
        assert_eq!(extractor.get("authorization"), None);
        assert_eq!(extractor.get("cookie"), None);
        assert_eq!(extractor.get("x-api-key"), None);
        assert_eq!(extractor.get("x-document-text"), None);
        assert_eq!(extractor.keys(), vec![TRACEPARENT_HEADER]);
    }

    #[test]
    fn only_w3c_trace_context_headers_are_admitted() {
        assert!(is_trace_context_header("traceparent"));
        assert!(is_trace_context_header("TraceState"));
        assert!(!is_trace_context_header("baggage"));
        assert!(!is_trace_context_header("authorization"));
    }
}
