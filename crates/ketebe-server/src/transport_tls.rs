use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::fmt;
use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportTlsConfig {
    pub certificate_path: PathBuf,
    pub private_key_path: PathBuf,
    pub client_ca_path: Option<PathBuf>,
}

#[derive(Debug)]
pub enum TransportTlsError {
    MissingPair(&'static str),
    Read {
        kind: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    InvalidCertificate(String),
    InvalidPrivateKey(String),
    InvalidClientCa(String),
    Rustls(String),
}

impl fmt::Display for TransportTlsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPair(message) => formatter.write_str(message),
            Self::Read { kind, path, source } => {
                write!(
                    formatter,
                    "failed to read {kind} '{}': {source}",
                    path.display()
                )
            }
            Self::InvalidCertificate(message) => {
                write!(formatter, "invalid TLS certificate: {message}")
            }
            Self::InvalidPrivateKey(message) => {
                write!(formatter, "invalid TLS private key: {message}")
            }
            Self::InvalidClientCa(message) => write!(formatter, "invalid TLS client CA: {message}"),
            Self::Rustls(message) => write!(formatter, "invalid TLS configuration: {message}"),
        }
    }
}

impl std::error::Error for TransportTlsError {}

fn ensure_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

impl TransportTlsConfig {
    pub fn new(
        certificate_path: impl Into<PathBuf>,
        private_key_path: impl Into<PathBuf>,
        client_ca_path: Option<impl Into<PathBuf>>,
    ) -> Self {
        Self {
            certificate_path: certificate_path.into(),
            private_key_path: private_key_path.into(),
            client_ca_path: client_ca_path.map(Into::into),
        }
    }

    pub fn validate(&self) -> Result<(), TransportTlsError> {
        self.rustls_server_config().map(|_| ())
    }

    pub fn rustls_server_config(&self) -> Result<Arc<ServerConfig>, TransportTlsError> {
        ensure_crypto_provider();
        let certificate = read_required("TLS certificate", &self.certificate_path)?;
        let private_key = read_required("TLS private key", &self.private_key_path)?;
        let certificates = parse_certificates(&certificate)?;
        let private_key = parse_private_key(&private_key)?;
        let mut config = if let Some(client_ca_path) = &self.client_ca_path {
            let client_ca = read_required("TLS client CA", client_ca_path)?;
            let roots = parse_client_ca_store(&client_ca)?;
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|error| TransportTlsError::InvalidClientCa(error.to_string()))?;
            ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(certificates, private_key)
                .map_err(|error| TransportTlsError::Rustls(error.to_string()))?
        } else {
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certificates, private_key)
                .map_err(|error| TransportTlsError::Rustls(error.to_string()))?
        };
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Arc::new(config))
    }

    pub fn rest_rustls_config(
        &self,
    ) -> Result<axum_server::tls_rustls::RustlsConfig, TransportTlsError> {
        Ok(axum_server::tls_rustls::RustlsConfig::from_config(
            self.rustls_server_config()?,
        ))
    }
}

fn read_required(kind: &'static str, path: &Path) -> Result<Vec<u8>, TransportTlsError> {
    fs::read(path).map_err(|source| TransportTlsError::Read {
        kind,
        path: path.to_path_buf(),
        source,
    })
}

fn parse_certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, TransportTlsError> {
    let mut reader = BufReader::new(pem);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| TransportTlsError::InvalidCertificate(error.to_string()))?;
    if certificates.is_empty() {
        return Err(TransportTlsError::InvalidCertificate(
            "certificate file contains no certificates".to_string(),
        ));
    }
    Ok(certificates)
}

fn parse_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, TransportTlsError> {
    let mut reader = BufReader::new(pem);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|error| TransportTlsError::InvalidPrivateKey(error.to_string()))?
        .ok_or_else(|| {
            TransportTlsError::InvalidPrivateKey(
                "private-key file contains no supported private key".to_string(),
            )
        })
}

fn parse_client_ca_store(pem: &[u8]) -> Result<RootCertStore, TransportTlsError> {
    let mut roots = RootCertStore::empty();
    let certificates = parse_certificates(pem)
        .map_err(|error| TransportTlsError::InvalidClientCa(error.to_string()))?;
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|error| TransportTlsError::InvalidClientCa(error.to_string()))?;
    }
    if roots.is_empty() {
        return Err(TransportTlsError::InvalidClientCa(
            "client CA file contains no trusted certificates".to_string(),
        ));
    }
    Ok(roots)
}

pub fn transport_tls_from_env() -> Result<Option<TransportTlsConfig>, TransportTlsError> {
    let certificate = std::env::var_os("KETEBE_TLS_CERT_PATH").map(PathBuf::from);
    let private_key = std::env::var_os("KETEBE_TLS_KEY_PATH").map(PathBuf::from);
    let client_ca = std::env::var_os("KETEBE_TLS_CLIENT_CA_PATH").map(PathBuf::from);
    match (certificate, private_key) {
        (None, None) => {
            if client_ca.is_some() {
                Err(TransportTlsError::MissingPair(
                    "KETEBE_TLS_CLIENT_CA_PATH requires KETEBE_TLS_CERT_PATH and KETEBE_TLS_KEY_PATH",
                ))
            } else {
                Ok(None)
            }
        }
        (Some(certificate_path), Some(private_key_path)) => {
            let config = TransportTlsConfig {
                certificate_path,
                private_key_path,
                client_ca_path: client_ca,
            };
            config.validate()?;
            Ok(Some(config))
        }
        _ => Err(TransportTlsError::MissingPair(
            "KETEBE_TLS_CERT_PATH and KETEBE_TLS_KEY_PATH must be configured together",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_material_fails_with_actionable_path() {
        let config = TransportTlsConfig::new(
            "/definitely/missing/ketebe-cert.pem",
            "/definitely/missing/ketebe-key.pem",
            None::<PathBuf>,
        );
        let error = config
            .validate()
            .expect_err("missing certificate must fail");
        assert!(error.to_string().contains("ketebe-cert.pem"));
    }

    #[test]
    fn key_material_is_never_part_of_config_debug() {
        let config = TransportTlsConfig::new("cert.pem", "key.pem", Some("ca.pem"));
        let debug = format!("{config:?}");
        assert!(debug.contains("cert.pem"));
        assert!(debug.contains("key.pem"));
        assert!(!debug.contains("PRIVATE KEY"));
    }
}
