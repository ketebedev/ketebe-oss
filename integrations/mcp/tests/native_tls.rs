use ketebe_mcp::{
    auth::AuthMode,
    config::{Config, HttpProtocol, TlsConfig, Transport},
    readiness::Readiness,
    transport::run_streamable_http,
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use std::{
    net::{SocketAddr, TcpListener},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}
fn base(addr: SocketAddr, protocol: HttpProtocol, tls: Option<TlsConfig>) -> Config {
    Config {
        ketebe_url: "http://127.0.0.1:9".into(),
        transport: Transport::StreamableHttp,
        protocol,
        probe_interval: Duration::from_secs(5),
        bind_addr: addr,
        path: "/mcp".into(),
        request_timeout: Duration::from_secs(5),
        max_request_bytes: 1024 * 1024,
        tls,
        auth_mode: AuthMode::Development,
        ketebe_token: None,
    }
}

#[tokio::test]
async fn http_mode_regression_serves_streamable_http() {
    let addr = free_addr();
    let ct = CancellationToken::new();
    let child = ct.clone();
    let task = tokio::spawn(async move {
        run_streamable_http(
            &base(addr, HttpProtocol::Http, None),
            Readiness::default(),
            child,
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let response=reqwest::Client::new().post(format!("http://{addr}/mcp")).header("accept","application/json, text/event-stream").json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"tls-test","version":"1"}}})).send().await.unwrap();
    assert!(response.status().is_success());
    ct.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn native_tls_serves_streamable_http_over_https() {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, signing_key.serialize_pem()).unwrap();
    let addr = free_addr();
    let ct = CancellationToken::new();
    let child = ct.clone();
    let tls = TlsConfig {
        certificate: cert_path.clone(),
        private_key: key_path,
    };
    let task = tokio::spawn(async move {
        run_streamable_http(
            &base(addr, HttpProtocol::Https, Some(tls)),
            Readiness::default(),
            child,
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let root = reqwest::Certificate::from_pem(cert.pem().as_bytes()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(root)
        .build()
        .unwrap();
    let response=client.post(format!("https://localhost:{}/mcp",addr.port())).header("accept","application/json, text/event-stream").json(&serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28","capabilities":{},"clientInfo":{"name":"tls-test","version":"1"}}})).send().await.unwrap();
    assert!(response.status().is_success());
    ct.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn malformed_tls_identity_fails_before_serving() {
    let dir = tempfile::tempdir().unwrap();
    let cert = dir.path().join("bad.crt");
    let key = dir.path().join("bad.key");
    std::fs::write(&cert, "bad").unwrap();
    std::fs::write(&key, "bad").unwrap();
    let err = run_streamable_http(
        &base(
            free_addr(),
            HttpProtocol::Https,
            Some(TlsConfig {
                certificate: cert,
                private_key: key,
            }),
        ),
        Readiness::default(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("failed to load TLS certificate"));
}
