use crate::{
    ApiKeyStore, AppState, AuthenticationService, AuthorizationService,
    DeterministicEmbeddingProvider, EmbeddingMigrationService, EmbeddingProviderRegistry,
    JobService, KafkaIngestionConfig, KafkaSecurityConfig, OpenAiCompatibleEmbeddingConfig,
    OpenAiCompatibleEmbeddingProvider, SecretRef, app_with_authentication, init_observability,
    run_kafka_ingestion, serve_grpc_transport_until_shutdown, transport_tls_from_env,
};
use ketebe_core::CollectionId;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Explicit OSS composition root for the standalone Ketebe server.
///
/// Commercial editions may compose additional implementations around public/shared
/// contracts, but the standalone runtime must remain independently buildable and
/// runnable with no commercial source present.
pub async fn run_standalone_from_env() {
    let _observability = init_observability();
    let info = ketebe_core::build_info();
    let storage_info = ketebe_storage::build_info();
    debug_assert_eq!(info, storage_info);

    let http_address: SocketAddr = std::env::var("KETEBE_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:7610".to_string())
        .parse()
        .unwrap_or_else(|error| panic!("invalid KETEBE_HTTP_ADDR: {error}"));
    let grpc_address: SocketAddr = std::env::var("KETEBE_GRPC_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:7611".to_string())
        .parse()
        .unwrap_or_else(|error| panic!("invalid KETEBE_GRPC_ADDR: {error}"));
    let data_dir =
        PathBuf::from(std::env::var("KETEBE_DATA_DIR").unwrap_or_else(|_| "./data".to_string()));
    let authorization = authorization_from_env(&data_dir);
    let state = AppState::recover(&data_dir)
        .unwrap_or_else(|error| panic!("failed to recover {}: {error}", data_dir.display()))
        .with_authorization(authorization);
    JobService::new(state.clone())
        .recover_interrupted_jobs()
        .unwrap_or_else(|error| panic!("failed to recover background jobs: {error}"));
    if let Some(registry) = embedding_registry_from_env() {
        state.set_embedding_provider_registry(registry).await;
    }
    EmbeddingMigrationService::new(state.clone())
        .recover_interrupted_cutovers()
        .await
        .unwrap_or_else(|error| panic!("failed to recover embedding cutover: {error}"));
    let kafka_config = kafka_config_from_env();
    let authentication = authentication_from_env(&data_dir);
    let tls = transport_tls_from_env()
        .unwrap_or_else(|error| panic!("invalid transport TLS configuration: {error}"));

    let http_listener = if tls.is_none() {
        Some(
            tokio::net::TcpListener::bind(http_address)
                .await
                .unwrap_or_else(|error| panic!("failed to bind HTTP {http_address}: {error}")),
        )
    } else {
        None
    };
    let grpc_listener = tokio::net::TcpListener::bind(grpc_address)
        .await
        .unwrap_or_else(|error| panic!("failed to bind gRPC {grpc_address}: {error}"));

    let tls_enabled = tls.is_some();
    let mtls_enabled = tls
        .as_ref()
        .is_some_and(|config| config.client_ca_path.is_some());
    tracing::info!(
        service.name = info.name,
        service.version = info.version,
        http.address = %http_address,
        grpc.address = %grpc_address,
        kafka.enabled = kafka_config.is_some(),
        auth.mode = ?authentication.mode(),
        tls.enabled = tls_enabled,
        mtls.enabled = mtls_enabled,
        edition = "oss",
        "Ketebe server started"
    );

    let shutdown_timeout =
        Duration::from_millis(optional_u64("KETEBE_SHUTDOWN_TIMEOUT_MS", 30_000));
    let http_state = state.clone();
    let http_lifecycle = http_state.lifecycle();
    let grpc_state = state.clone();
    let http_authentication = authentication.clone();
    let grpc_authentication = authentication.clone();
    let http_tls = tls.clone();
    let grpc_tls = tls;
    let mut http = tokio::spawn(async move {
        let app = app_with_authentication(http_state, http_authentication);
        if let Some(tls) = http_tls {
            let rustls = tls.rest_rustls_config().map_err(std::io::Error::other)?;
            let listener = std::net::TcpListener::bind(http_address)?;
            listener.set_nonblocking(true)?;
            let handle = axum_server::Handle::new();
            let shutdown_handle = handle.clone();
            tokio::spawn(async move {
                http_lifecycle.wait_for_draining().await;
                shutdown_handle.graceful_shutdown(None);
            });
            axum_server::from_tcp_rustls(listener, rustls)?
                .handle(handle)
                .serve(app.into_make_service())
                .await
        } else {
            axum::serve(http_listener.expect("plaintext HTTP listener"), app)
                .with_graceful_shutdown(async move {
                    http_lifecycle.wait_for_draining().await;
                })
                .await
        }
    });
    let mut grpc = tokio::spawn(async move {
        serve_grpc_transport_until_shutdown(
            grpc_state,
            grpc_listener,
            grpc_authentication,
            grpc_tls,
        )
        .await
    });

    if let Some(config) = kafka_config {
        let kafka_state = state.clone();
        let mut kafka = tokio::spawn(run_kafka_ingestion(kafka_state, config));
        tokio::select! {
            () = shutdown_signal() => {
                tracing::info!("shutdown signal received");
            }
            result = &mut http => finish_http(result),
            result = &mut grpc => finish_grpc(result),
            result = &mut kafka => finish_kafka(result),
        }
        state.begin_draining();
        let drained = tokio::time::timeout(shutdown_timeout, async {
            state.wait_for_foreground_writes_drained().await;
            if !http.is_finished() {
                finish_http((&mut http).await);
            }
            if !grpc.is_finished() {
                finish_grpc((&mut grpc).await);
            }
            if !kafka.is_finished() {
                finish_kafka((&mut kafka).await);
            }
        })
        .await;
        if drained.is_err() {
            tracing::warn!(
                shutdown.timeout_ms = shutdown_timeout.as_millis(),
                "graceful shutdown deadline exceeded; aborting remaining tasks"
            );
            http.abort();
            grpc.abort();
            kafka.abort();
        }
    } else {
        tokio::select! {
            () = shutdown_signal() => {
                tracing::info!("shutdown signal received");
            }
            result = &mut http => finish_http(result),
            result = &mut grpc => finish_grpc(result),
        }
        state.begin_draining();
        let drained = tokio::time::timeout(shutdown_timeout, async {
            state.wait_for_foreground_writes_drained().await;
            if !http.is_finished() {
                finish_http((&mut http).await);
            }
            if !grpc.is_finished() {
                finish_grpc((&mut grpc).await);
            }
        })
        .await;
        if drained.is_err() {
            tracing::warn!(
                shutdown.timeout_ms = shutdown_timeout.as_millis(),
                "graceful shutdown deadline exceeded; aborting remaining tasks"
            );
            http.abort();
            grpc.abort();
        }
    }
    state.mark_stopped();
    tracing::info!("Ketebe server stopped");
}

fn authorization_from_env(data_dir: &Path) -> AuthorizationService {
    match std::env::var("KETEBE_AUTH_MODE")
        .unwrap_or_else(|_| "development".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "development" => AuthorizationService::development(),
        "required" => AuthorizationService::required(data_dir)
            .unwrap_or_else(|error| panic!("failed to open authorization store: {error}")),
        other => panic!("unsupported KETEBE_AUTH_MODE '{other}'; expected development or required"),
    }
}

fn authentication_from_env(data_dir: &Path) -> AuthenticationService {
    match std::env::var("KETEBE_AUTH_MODE")
        .unwrap_or_else(|_| "development".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "development" => AuthenticationService::development(),
        "required" => {
            let store = ApiKeyStore::open(data_dir)
                .unwrap_or_else(|error| panic!("failed to open API key store: {error}"));
            AuthenticationService::required(Arc::new(store))
        }
        other => panic!("unsupported KETEBE_AUTH_MODE '{other}'; expected development or required"),
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.expect("failed to install Ctrl-C handler");
            }
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    }
}

fn embedding_registry_from_env() -> Option<EmbeddingProviderRegistry> {
    let provider_kind = std::env::var("KETEBE_EMBEDDING_PROVIDER").ok()?;
    let profile =
        std::env::var("KETEBE_EMBEDDING_PROFILE").unwrap_or_else(|_| "default".to_string());
    let mut registry = EmbeddingProviderRegistry::new();

    match provider_kind.trim().to_ascii_lowercase().as_str() {
        "deterministic" => {
            let model = std::env::var("KETEBE_EMBEDDING_MODEL")
                .unwrap_or_else(|_| "ketebe-deterministic".to_string());
            let version = std::env::var("KETEBE_EMBEDDING_MODEL_VERSION")
                .unwrap_or_else(|_| "v0".to_string());
            registry
                .register(
                    profile.clone(),
                    Arc::new(
                        DeterministicEmbeddingProvider::new(model, version).unwrap_or_else(
                            |error| panic!("invalid embedding provider configuration: {error}"),
                        ),
                    ),
                )
                .unwrap_or_else(|error| panic!("invalid embedding registry: {error}"));
        }
        "openai-compatible" => {
            let endpoint = required_env("KETEBE_EMBEDDING_ENDPOINT");
            let model = required_env("KETEBE_EMBEDDING_MODEL");
            let model_version =
                std::env::var("KETEBE_EMBEDDING_MODEL_VERSION").unwrap_or_else(|_| model.clone());
            let dimension = optional_usize("KETEBE_EMBEDDING_DIMENSION", 0);
            let timeout_ms = optional_u64("KETEBE_EMBEDDING_TIMEOUT_MS", 10_000);
            let max_retries = optional_u64("KETEBE_EMBEDDING_MAX_RETRIES", 2) as u32;
            let retry_backoff_ms = optional_u64("KETEBE_EMBEDDING_RETRY_BACKOFF_MS", 100);
            let max_concurrency = optional_usize("KETEBE_EMBEDDING_MAX_CONCURRENCY", 16);
            let api_key_ref = std::env::var("KETEBE_EMBEDDING_API_KEY_REF")
                .ok()
                .map(SecretRef::new)
                .transpose()
                .unwrap_or_else(|error| panic!("invalid KETEBE_EMBEDDING_API_KEY_REF: {error}"));
            if std::env::var_os("KETEBE_EMBEDDING_API_KEY").is_some() {
                panic!(
                    "KETEBE_EMBEDDING_API_KEY is not accepted; configure KETEBE_EMBEDDING_API_KEY_REF=env://KETEBE_EMBEDDING_API_KEY instead"
                );
            }
            let provider =
                OpenAiCompatibleEmbeddingProvider::new(OpenAiCompatibleEmbeddingConfig {
                    endpoint,
                    model,
                    model_version,
                    dimension,
                    api_key_ref,
                    timeout: Duration::from_millis(timeout_ms),
                    max_retries,
                    retry_backoff: Duration::from_millis(retry_backoff_ms),
                    max_concurrency,
                })
                .unwrap_or_else(|error| {
                    panic!("invalid embedding provider configuration: {error}")
                });
            registry
                .register(profile.clone(), Arc::new(provider))
                .unwrap_or_else(|error| panic!("invalid embedding registry: {error}"));
        }
        other => panic!("unsupported KETEBE_EMBEDDING_PROVIDER '{other}'"),
    }

    registry
        .set_default(profile)
        .unwrap_or_else(|error| panic!("invalid embedding registry default: {error}"));
    Some(registry)
}

fn kafka_config_from_env() -> Option<KafkaIngestionConfig> {
    let brokers = std::env::var("KETEBE_KAFKA_BROKERS").ok()?;
    let topic = required_env("KETEBE_KAFKA_TOPIC");
    let group_id = required_env("KETEBE_KAFKA_GROUP_ID");
    let collection_id = CollectionId::new(required_env("KETEBE_KAFKA_COLLECTION"))
        .unwrap_or_else(|error| panic!("invalid KETEBE_KAFKA_COLLECTION: {error}"));
    let batch_max_records = optional_usize("KETEBE_KAFKA_BATCH_MAX_RECORDS", 128);
    let batch_linger_ms = optional_u64("KETEBE_KAFKA_BATCH_LINGER_MS", 50);
    let mut config = KafkaIngestionConfig::new(
        brokers,
        topic,
        group_id,
        collection_id,
        batch_max_records,
        batch_linger_ms,
    )
    .unwrap_or_else(|error| panic!("invalid Kafka ingestion configuration: {error}"));

    if let Ok(dlq_topic) = std::env::var("KETEBE_KAFKA_DLQ_TOPIC")
        && !dlq_topic.trim().is_empty()
    {
        config = config.with_dlq_topic(dlq_topic);
    }

    if let Ok(security_protocol) = std::env::var("KETEBE_KAFKA_SECURITY_PROTOCOL") {
        config = config.with_security(KafkaSecurityConfig {
            security_protocol,
            sasl_mechanism: std::env::var("KETEBE_KAFKA_SASL_MECHANISM").ok(),
            sasl_username: std::env::var("KETEBE_KAFKA_SASL_USERNAME").ok(),
            sasl_password: std::env::var("KETEBE_KAFKA_SASL_PASSWORD").ok(),
        });
    }

    Some(config)
}

fn required_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} is required when the corresponding feature is enabled"))
}

fn optional_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|error| panic!("invalid {name}: {error}"))
        })
        .unwrap_or(default)
}

fn optional_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|error| panic!("invalid {name}: {error}"))
        })
        .unwrap_or(default)
}

fn finish_http(result: Result<Result<(), std::io::Error>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => panic!("HTTP server stopped unexpectedly"),
        Ok(Err(error)) => panic!("HTTP server failed: {error}"),
        Err(error) => panic!("HTTP task failed: {error}"),
    }
}

fn finish_grpc(result: Result<Result<(), crate::GrpcTransportError>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => panic!("gRPC server stopped unexpectedly"),
        Ok(Err(error)) => panic!("gRPC server failed: {error}"),
        Err(error) => panic!("gRPC task failed: {error}"),
    }
}

fn finish_kafka(result: Result<Result<(), crate::KafkaIngestionError>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => panic!("Kafka ingestion stopped unexpectedly"),
        Ok(Err(error)) => panic!("Kafka ingestion failed: {error}"),
        Err(error) => panic!("Kafka ingestion task failed: {error}"),
    }
}
