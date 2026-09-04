use ketebe_mcp::{
    config::{Config, Transport},
    ketebe::KetebeApi,
    policy::ToolPolicy,
    readiness::{Readiness, spawn_probe},
    transport::{run_stdio, run_streamable_http},
};
use std::{error::Error, io};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_logging();
    let policy = ToolPolicy::from_env()?;
    if !policy.enabled() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Ketebe MCP is disabled; set KETEBE_MCP_ENABLED=true to start the server",
        )
        .into());
    }
    let config = Config::from_env()?;
    let api = KetebeApi::new(config.ketebe_url.clone())?;
    let readiness = Readiness::default();
    let ct = CancellationToken::new();
    let probe = spawn_probe(api, readiness.clone(), config.probe_interval, ct.clone());
    let signal_ct = ct.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("shutdown signal received");
            signal_ct.cancel();
        }
    });
    tracing::info!(transport=?config.transport,auth_mode=?config.auth_mode,ketebe_url=%config.ketebe_url,"starting Ketebe MCP server");
    let result = match config.transport {
        Transport::Stdio => run_stdio(&config, readiness, ct.clone()).await,
        Transport::StreamableHttp => run_streamable_http(&config, readiness, ct.clone()).await,
    };
    ct.cancel();
    signal.abort();
    probe.await?;
    result
}

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .with_ansi(false)
        .json()
        .init();
}
