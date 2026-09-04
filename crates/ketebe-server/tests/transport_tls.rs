use ketebe_server::TransportTlsConfig;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use std::io::BufReader;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

struct TestCertificates {
    _directory: TempDir,
    ca_pem: String,
    server_certificate: std::path::PathBuf,
    server_key: std::path::PathBuf,
    client_certificate_pem: String,
    client_key_pem: String,
}

fn certificates() -> TestCertificates {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().unwrap();
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();

    let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let server_key = KeyPair::generate().unwrap();
    let server_certificate = server_params.signed_by(&server_key, &ca).unwrap();

    let mut client_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    client_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    let client_key = KeyPair::generate().unwrap();
    let client_certificate = client_params.signed_by(&client_key, &ca).unwrap();

    let directory = tempfile::tempdir().unwrap();
    let server_certificate_path = directory.path().join("server-cert.pem");
    let server_key_path = directory.path().join("server-key.pem");
    std::fs::write(&server_certificate_path, server_certificate.pem()).unwrap();
    std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();

    TestCertificates {
        _directory: directory,
        ca_pem: ca.pem(),
        server_certificate: server_certificate_path,
        server_key: server_key_path,
        client_certificate_pem: client_certificate.pem(),
        client_key_pem: client_key.serialize_pem(),
    }
}

fn roots(ca_pem: &str) -> RootCertStore {
    let mut store = RootCertStore::empty();
    let mut reader = BufReader::new(ca_pem.as_bytes());
    for certificate in rustls_pemfile::certs(&mut reader) {
        store.add(certificate.unwrap()).unwrap();
    }
    store
}

fn client_identity(
    certificate_pem: &str,
    key_pem: &str,
) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let mut certificate_reader = BufReader::new(certificate_pem.as_bytes());
    let certificates = rustls_pemfile::certs(&mut certificate_reader)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut key_reader = BufReader::new(key_pem.as_bytes());
    let key = rustls_pemfile::private_key(&mut key_reader)
        .unwrap()
        .expect("client private key");
    (certificates, key)
}

async fn handshake(
    server: Arc<rustls::ServerConfig>,
    client: Arc<ClientConfig>,
) -> (Result<(), String>, Result<(), String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        TlsAcceptor::from(server)
            .accept(stream)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    });

    let stream = TcpStream::connect(address).await.unwrap();
    let client_result = TlsConnector::from(client)
        .connect(
            ServerName::try_from("localhost".to_string()).unwrap(),
            stream,
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string());
    let server_result = server_task.await.unwrap();
    (server_result, client_result)
}

#[tokio::test]
async fn server_tls_accepts_a_client_that_trusts_the_runtime_generated_ca() {
    let materials = certificates();
    let server = TransportTlsConfig::new(
        &materials.server_certificate,
        &materials.server_key,
        None::<std::path::PathBuf>,
    )
    .rustls_server_config()
    .unwrap();
    let client = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots(&materials.ca_pem))
            .with_no_client_auth(),
    );

    let (server_result, client_result) = handshake(server, client).await;
    assert!(server_result.is_ok(), "server handshake: {server_result:?}");
    assert!(client_result.is_ok(), "client handshake: {client_result:?}");
}

#[tokio::test]
async fn mtls_rejects_missing_client_identity_and_accepts_a_trusted_identity() {
    let materials = certificates();
    let ca_path = materials._directory.path().join("client-ca.pem");
    std::fs::write(&ca_path, &materials.ca_pem).unwrap();
    let server_tls = TransportTlsConfig::new(
        &materials.server_certificate,
        &materials.server_key,
        Some(&ca_path),
    );
    server_tls.validate().unwrap();

    let anonymous_client = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots(&materials.ca_pem))
            .with_no_client_auth(),
    );
    let (server_result, _) =
        handshake(server_tls.rustls_server_config().unwrap(), anonymous_client).await;
    assert!(
        server_result.is_err(),
        "mTLS must reject a client without a certificate"
    );

    let (client_certificates, client_key) =
        client_identity(&materials.client_certificate_pem, &materials.client_key_pem);
    let authenticated_client = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots(&materials.ca_pem))
            .with_client_auth_cert(client_certificates, client_key)
            .unwrap(),
    );
    let (server_result, client_result) = handshake(
        server_tls.rustls_server_config().unwrap(),
        authenticated_client,
    )
    .await;
    assert!(
        server_result.is_ok(),
        "server mTLS handshake: {server_result:?}"
    );
    assert!(
        client_result.is_ok(),
        "client mTLS handshake: {client_result:?}"
    );
}
