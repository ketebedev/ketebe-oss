use crate::auth::AuthMode;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Stdio,
    StreamableHttp,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HttpProtocol {
    #[default]
    Http,
    Https,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsConfig {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    pub ketebe_url: String,
    pub transport: Transport,
    pub protocol: HttpProtocol,
    pub probe_interval: Duration,
    pub bind_addr: SocketAddr,
    pub path: String,
    pub request_timeout: Duration,
    pub max_request_bytes: usize,
    pub tls: Option<TlsConfig>,
    pub auth_mode: AuthMode,
    pub ketebe_token: Option<String>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("ketebe_url", &self.ketebe_url)
            .field("transport", &self.transport)
            .field("protocol", &self.protocol)
            .field("probe_interval", &self.probe_interval)
            .field("bind_addr", &self.bind_addr)
            .field("path", &self.path)
            .field("request_timeout", &self.request_timeout)
            .field("max_request_bytes", &self.max_request_bytes)
            .field("tls", &self.tls)
            .field("auth_mode", &self.auth_mode)
            .field(
                "ketebe_token",
                &self.ketebe_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Missing(&'static str),
    InvalidTransport(String),
    InvalidProtocol(String),
    InvalidProbeInterval(String),
    InvalidBind(String),
    InvalidRequestTimeout(String),
    InvalidRequestBytes(String),
    InvalidPath(String),
    ReadConfig(std::io::Error),
    ParseConfig(toml::de::Error),
    HttpsRequiresStreamableHttp,
    MissingTlsCertificate,
    MissingTlsPrivateKey,
    InvalidAuthMode(String),
    MissingStdioCredential,
}
impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing(v) => write!(f, "missing required configuration {v}"),
            Self::InvalidTransport(v) => write!(f, "invalid MCP transport {v:?}"),
            Self::InvalidProtocol(v) => {
                write!(f, "invalid MCP HTTP protocol {v:?}; expected http or https")
            }
            Self::InvalidProbeInterval(v) => write!(f, "invalid probe interval {v:?}"),
            Self::InvalidBind(v) => write!(f, "invalid bind address {v:?}"),
            Self::InvalidRequestTimeout(v) => write!(f, "invalid request timeout {v:?}"),
            Self::InvalidRequestBytes(v) => write!(f, "invalid max request bytes {v:?}"),
            Self::InvalidPath(v) => write!(f, "invalid MCP path {v:?}; expected an absolute path"),
            Self::ReadConfig(e) => write!(f, "cannot read MCP config file: {e}"),
            Self::ParseConfig(e) => write!(f, "cannot parse MCP config file: {e}"),
            Self::HttpsRequiresStreamableHttp => {
                write!(f, "https is valid only with streamable_http transport")
            }
            Self::MissingTlsCertificate => write!(f, "https requires tls.certificate"),
            Self::MissingTlsPrivateKey => write!(f, "https requires tls.private_key"),
            Self::InvalidAuthMode(v) => write!(
                f,
                "invalid MCP auth mode {v:?}; expected development or required"
            ),
            Self::MissingStdioCredential => {
                write!(f, "required stdio mode needs KETEBE_MCP_KETEBE_TOKEN")
            }
        }
    }
}
impl std::error::Error for ConfigError {}

#[derive(Deserialize)]
struct FileConfig {
    ketebe: KetebeSection,
    transport: TransportSection,
    tls: Option<TlsSection>,
}
#[derive(Deserialize)]
struct KetebeSection {
    url: String,
}
#[derive(Deserialize)]
struct TransportSection {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    protocol: ProtocolValue,
    bind: Option<String>,
    path: Option<String>,
    probe_interval_ms: Option<u64>,
    request_timeout_ms: Option<u64>,
    max_request_bytes: Option<usize>,
}
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ProtocolValue {
    #[default]
    Http,
    Https,
}
#[derive(Deserialize)]
struct TlsSection {
    certificate: Option<PathBuf>,
    private_key: Option<PathBuf>,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        if let Ok(path) = std::env::var("KETEBE_MCP_CONFIG")
            && !path.trim().is_empty()
        {
            return Self::from_file(path);
        }
        Self::from_map(&std::env::vars().collect())
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path).map_err(ConfigError::ReadConfig)?;
        let file: FileConfig = toml::from_str(&raw).map_err(ConfigError::ParseConfig)?;
        let transport = parse_transport(&file.transport.kind)?;
        let protocol = match file.transport.protocol {
            ProtocolValue::Http => HttpProtocol::Http,
            ProtocolValue::Https => HttpProtocol::Https,
        };
        let bind_addr = file
            .transport
            .bind
            .as_deref()
            .unwrap_or("127.0.0.1:8000")
            .parse()
            .map_err(|_| {
                ConfigError::InvalidBind(file.transport.bind.clone().unwrap_or_default())
            })?;
        let path = file.transport.path.unwrap_or_else(|| "/mcp".into());
        validate_path(&path)?;
        let probe_interval = positive_duration(
            file.transport.probe_interval_ms.unwrap_or(5000),
            ConfigError::InvalidProbeInterval,
        )?;
        let request_timeout = positive_duration(
            file.transport.request_timeout_ms.unwrap_or(30000),
            ConfigError::InvalidRequestTimeout,
        )?;
        let max_request_bytes = file.transport.max_request_bytes.unwrap_or(1024 * 1024);
        if max_request_bytes == 0 {
            return Err(ConfigError::InvalidRequestBytes("0".into()));
        }
        let tls = file
            .tls
            .map(|t| -> Result<TlsConfig, ConfigError> {
                Ok(TlsConfig {
                    certificate: t.certificate.ok_or(ConfigError::MissingTlsCertificate)?,
                    private_key: t.private_key.ok_or(ConfigError::MissingTlsPrivateKey)?,
                })
            })
            .transpose()?;
        validate_tls(transport, protocol, &tls)?;
        let auth_mode = parse_auth_mode(
            &std::env::var("KETEBE_MCP_AUTH_MODE").unwrap_or_else(|_| "development".to_string()),
        )?;
        let ketebe_token = std::env::var("KETEBE_MCP_KETEBE_TOKEN")
            .ok()
            .filter(|value| !value.is_empty());
        validate_auth(transport, auth_mode, &ketebe_token)?;
        Ok(Self {
            ketebe_url: file.ketebe.url,
            transport,
            protocol,
            probe_interval,
            bind_addr,
            path,
            request_timeout,
            max_request_bytes,
            tls,
            auth_mode,
            ketebe_token,
        })
    }

    pub fn from_map(v: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let ketebe_url = required(v, "KETEBE_MCP_KETEBE_URL")?;
        let transport = parse_transport(&required(v, "KETEBE_MCP_TRANSPORT")?)?;
        let protocol = match v
            .get("KETEBE_MCP_PROTOCOL")
            .map(String::as_str)
            .unwrap_or("http")
        {
            "http" => HttpProtocol::Http,
            "https" => HttpProtocol::Https,
            other => return Err(ConfigError::InvalidProtocol(other.into())),
        };
        let probe_interval = duration(
            v,
            "KETEBE_MCP_PROBE_INTERVAL_MS",
            5000,
            ConfigError::InvalidProbeInterval,
        )?;
        let request_timeout = duration(
            v,
            "KETEBE_MCP_REQUEST_TIMEOUT_MS",
            30000,
            ConfigError::InvalidRequestTimeout,
        )?;
        let max_request_bytes =
            v.get("KETEBE_MCP_MAX_REQUEST_BYTES")
                .map_or(Ok(1024 * 1024), |s| {
                    s.parse::<usize>()
                        .ok()
                        .filter(|n| *n > 0)
                        .ok_or_else(|| ConfigError::InvalidRequestBytes(s.clone()))
                })?;
        let bind_addr = v.get("KETEBE_MCP_BIND_ADDR").map_or_else(
            || "127.0.0.1:8000".parse().map_err(|_| unreachable!()),
            |s| s.parse().map_err(|_| ConfigError::InvalidBind(s.clone())),
        )?;
        let path = v
            .get("KETEBE_MCP_PATH")
            .cloned()
            .unwrap_or_else(|| "/mcp".into());
        validate_path(&path)?;
        let tls = match (
            v.get("KETEBE_MCP_TLS_CERTIFICATE"),
            v.get("KETEBE_MCP_TLS_PRIVATE_KEY"),
        ) {
            (None, None) => None,
            (cert, key) => Some(TlsConfig {
                certificate: cert.ok_or(ConfigError::MissingTlsCertificate)?.into(),
                private_key: key.ok_or(ConfigError::MissingTlsPrivateKey)?.into(),
            }),
        };
        validate_tls(transport, protocol, &tls)?;
        let auth_mode = parse_auth_mode(
            v.get("KETEBE_MCP_AUTH_MODE")
                .map(String::as_str)
                .unwrap_or("development"),
        )?;
        let ketebe_token = v
            .get("KETEBE_MCP_KETEBE_TOKEN")
            .cloned()
            .filter(|value| !value.is_empty());
        validate_auth(transport, auth_mode, &ketebe_token)?;
        Ok(Self {
            ketebe_url,
            transport,
            protocol,
            probe_interval,
            bind_addr,
            path,
            request_timeout,
            max_request_bytes,
            tls,
            auth_mode,
            ketebe_token,
        })
    }
}

fn parse_auth_mode(value: &str) -> Result<AuthMode, ConfigError> {
    match value {
        "development" => Ok(AuthMode::Development),
        "required" => Ok(AuthMode::Required),
        other => Err(ConfigError::InvalidAuthMode(other.to_string())),
    }
}

fn validate_auth(
    transport: Transport,
    mode: AuthMode,
    token: &Option<String>,
) -> Result<(), ConfigError> {
    if mode == AuthMode::Required && transport == Transport::Stdio && token.is_none() {
        return Err(ConfigError::MissingStdioCredential);
    }
    Ok(())
}

fn parse_transport(v: &str) -> Result<Transport, ConfigError> {
    match v {
        "stdio" => Ok(Transport::Stdio),
        "streamable-http" | "streamable_http" => Ok(Transport::StreamableHttp),
        other => Err(ConfigError::InvalidTransport(other.into())),
    }
}
fn validate_tls(
    transport: Transport,
    protocol: HttpProtocol,
    tls: &Option<TlsConfig>,
) -> Result<(), ConfigError> {
    if protocol == HttpProtocol::Https {
        if transport != Transport::StreamableHttp {
            return Err(ConfigError::HttpsRequiresStreamableHttp);
        }
        if tls.is_none() {
            return Err(ConfigError::MissingTlsCertificate);
        }
    }
    Ok(())
}
fn validate_path(path: &str) -> Result<(), ConfigError> {
    if path.starts_with('/') && path.len() > 1 {
        Ok(())
    } else {
        Err(ConfigError::InvalidPath(path.into()))
    }
}
fn required(v: &HashMap<String, String>, n: &'static str) -> Result<String, ConfigError> {
    v.get(n)
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .ok_or(ConfigError::Missing(n))
}
fn duration(
    v: &HashMap<String, String>,
    n: &'static str,
    default: u64,
    err: fn(String) -> ConfigError,
) -> Result<Duration, ConfigError> {
    positive_duration(
        v.get(n)
            .map_or(Ok(default), |s| s.parse().map_err(|_| err(s.clone())))?,
        err,
    )
}
fn positive_duration(ms: u64, err: fn(String) -> ConfigError) -> Result<Duration, ConfigError> {
    if ms == 0 {
        Err(err("0".into()))
    } else {
        Ok(Duration::from_millis(ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_env() -> HashMap<String, String> {
        HashMap::from([
            (
                "KETEBE_MCP_KETEBE_URL".to_string(),
                "http://127.0.0.1:7610".to_string(),
            ),
            (
                "KETEBE_MCP_TRANSPORT".to_string(),
                "streamable-http".to_string(),
            ),
        ])
    }
    use std::io::Write;
    fn base(t: &str) -> HashMap<String, String> {
        HashMap::from([
            (
                "KETEBE_MCP_KETEBE_URL".into(),
                "http://127.0.0.1:17610".into(),
            ),
            ("KETEBE_MCP_TRANSPORT".into(), t.into()),
        ])
    }
    #[test]
    fn legacy_env_defaults_to_http() {
        let c = Config::from_map(&base("streamable-http")).unwrap();
        assert_eq!(c.protocol, HttpProtocol::Http);
        assert!(c.tls.is_none());
    }
    #[test]
    fn required_stdio_requires_static_credential() {
        let mut map = base_env();
        map.insert("KETEBE_MCP_TRANSPORT".into(), "stdio".into());
        map.insert("KETEBE_MCP_AUTH_MODE".into(), "required".into());
        assert!(matches!(
            Config::from_map(&map),
            Err(ConfigError::MissingStdioCredential)
        ));
        map.insert("KETEBE_MCP_KETEBE_TOKEN".into(), "secret".into());
        let config = Config::from_map(&map).unwrap();
        assert_eq!(config.auth_mode, AuthMode::Required);
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn required_http_accepts_per_request_credentials_without_static_secret() {
        let mut map = base_env();
        map.insert("KETEBE_MCP_TRANSPORT".into(), "streamable-http".into());
        map.insert("KETEBE_MCP_AUTH_MODE".into(), "required".into());
        let config = Config::from_map(&map).unwrap();
        assert!(config.ketebe_token.is_none());
    }

    #[test]
    fn https_requires_both_paths() {
        let mut v = base("streamable-http");
        v.insert("KETEBE_MCP_PROTOCOL".into(), "https".into());
        assert!(matches!(
            Config::from_map(&v),
            Err(ConfigError::MissingTlsCertificate)
        ));
        v.insert("KETEBE_MCP_TLS_CERTIFICATE".into(), "cert.pem".into());
        assert!(matches!(
            Config::from_map(&v),
            Err(ConfigError::MissingTlsPrivateKey)
        ));
    }
    #[test]
    fn config_file_http_needs_no_tls_block() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            "[ketebe]\nurl='http://127.0.0.1:17610'\n[transport]\ntype='streamable_http'\n"
        )
        .unwrap();
        let c = Config::from_file(f.path()).unwrap();
        assert_eq!(c.protocol, HttpProtocol::Http);
        assert!(c.tls.is_none());
    }
    #[test]
    fn config_file_https_loads_paths() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f,"[ketebe]\nurl='http://127.0.0.1:17610'\n[transport]\ntype='streamable_http'\nprotocol='https'\n[tls]\ncertificate='/tmp/cert.pem'\nprivate_key='/tmp/key.pem'\n").unwrap();
        let c = Config::from_file(f.path()).unwrap();
        assert_eq!(c.protocol, HttpProtocol::Https);
        assert_eq!(c.tls.unwrap().certificate, PathBuf::from("/tmp/cert.pem"));
    }
}
