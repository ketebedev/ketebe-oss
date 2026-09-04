use crate::{
    auth::{RemoteAuthState, RequestCredential, authenticate_remote},
    config::{Config, HttpProtocol},
    ketebe::KetebeApi,
    observability::{observe_http_request, prometheus_metrics},
    rate_limit::{DEFAULT_REQUESTS_PER_SECOND, RateLimitState, enforce_rate_limit},
    readiness::Readiness,
    server::KetebeMcpServer,
};
use axum::{
    Router,
    http::{StatusCode, header},
    middleware,
    response::IntoResponse,
    routing::get,
};
use axum_server::{Handle, tls_rustls::RustlsConfig};
use rmcp::{
    ServiceExt,
    transport::{
        StreamableHttpServerConfig, stdio,
        streamable_http_server::{
            session::local::LocalSessionManager, tower::StreamableHttpService,
        },
    },
};
use std::{error::Error, io, net::SocketAddr, time::Duration};
use tokio_util::sync::CancellationToken;
use tower_http::{limit::RequestBodyLimitLayer, timeout::TimeoutLayer};

pub async fn run_stdio(
    config: &Config,
    readiness: Readiness,
    ct: CancellationToken,
) -> Result<(), Box<dyn Error>> {
    let static_credential = config
        .ketebe_token
        .clone()
        .map(RequestCredential::from_token)
        .transpose()?;
    let api = KetebeApi::new(config.ketebe_url.clone())?;
    let service = KetebeMcpServer::new(readiness, config.auth_mode, static_credential, api)
        .serve_with_ct(stdio(), ct.clone())
        .await?;
    let reason = service.waiting().await?;
    tracing::info!(?reason, "MCP stdio service stopped");
    Ok(())
}

fn app(config: &Config, readiness: Readiness, ct: CancellationToken) -> Router {
    let api = KetebeApi::new(config.ketebe_url.clone())
        .expect("validated Ketebe URL must build an MCP API client");
    let service = StreamableHttpService::new(
        {
            let readiness = readiness.clone();
            let auth_mode = config.auth_mode;
            let api = api.clone();
            move || {
                Ok(KetebeMcpServer::new(
                    readiness.clone(),
                    auth_mode,
                    None,
                    api.clone(),
                ))
            }
        },
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default().with_cancellation_token(ct.child_token()),
    );

    let mcp = Router::new()
        .nest_service(&config.path, service)
        .layer(middleware::from_fn_with_state(
            RemoteAuthState {
                mode: config.auth_mode,
                api,
            },
            authenticate_remote,
        ))
        .layer(middleware::from_fn_with_state(
            RateLimitState::per_second(DEFAULT_REQUESTS_PER_SECOND),
            enforce_rate_limit,
        ))
        .layer(RequestBodyLimitLayer::new(config.max_request_bytes))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            config.request_timeout,
        ))
        .layer(middleware::from_fn_with_state(
            config.max_request_bytes,
            observe_http_request,
        ));

    let ready = readiness.clone();
    let operations = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route(
            "/readyz",
            get(move || {
                let readiness = ready.clone();
                async move {
                    if readiness.is_ready() {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        )
        .route(
            "/metrics",
            get(|| async {
                (
                    [(
                        header::CONTENT_TYPE,
                        "text/plain; version=0.0.4; charset=utf-8",
                    )],
                    prometheus_metrics(),
                )
                    .into_response()
            }),
        );

    mcp.merge(operations)
}

pub async fn run_streamable_http(
    config: &Config,
    readiness: Readiness,
    ct: CancellationToken,
) -> Result<(), Box<dyn Error>> {
    let app = app(config, readiness, ct.clone());
    let handle: Handle<SocketAddr> = Handle::new();
    let shutdown_handle = handle.clone();
    let shutdown = tokio::spawn(async move {
        ct.cancelled().await;
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(5)));
    });
    tracing::info!(bind_addr=%config.bind_addr,path=%config.path,protocol=?config.protocol,max_request_bytes=config.max_request_bytes,request_timeout_ms=config.request_timeout.as_millis(),rate_limit_rps=DEFAULT_REQUESTS_PER_SECOND,"Streamable HTTP MCP listening");
    let result = match config.protocol {
        HttpProtocol::Http => {
            axum_server::bind(config.bind_addr)
                .handle(handle)
                .serve(app.into_make_service())
                .await
        }
        HttpProtocol::Https => {
            let tls = config.tls.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "https requires certificate and private key",
                )
            })?;
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            let rustls = RustlsConfig::from_pem_file(&tls.certificate, &tls.private_key)
                .await
                .map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!(
                            "failed to load TLS certificate {:?} or private key {:?}: {e}",
                            tls.certificate, tls.private_key
                        ),
                    )
                })?;
            axum_server::bind_rustls(config.bind_addr, rustls)
                .handle(handle)
                .serve(app.into_make_service())
                .await
        }
    };
    shutdown.abort();
    result.map_err(Into::into)
}
