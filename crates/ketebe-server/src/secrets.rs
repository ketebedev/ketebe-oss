use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretRef(String);

impl SecretRef {
    pub fn new(value: impl Into<String>) -> Result<Self, SecretError> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(SecretError::InvalidReference);
        }
        if !(trimmed.starts_with("env://") || trimmed.starts_with("file://")) {
            return Err(SecretError::UnsupportedScheme);
        }
        Ok(Self(value))
    }

    pub fn env(name: impl Into<String>) -> Result<Self, SecretError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(SecretError::InvalidReference);
        }
        Self::new(format!("env://{name}"))
    }

    pub fn file(path: impl Into<PathBuf>) -> Result<Self, SecretError> {
        let path = path.into();
        if path.as_os_str().is_empty() {
            return Err(SecretError::InvalidReference);
        }
        Self::new(format!("file://{}", path.display()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SecretRef").field(&self.0).finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SecretValue(String);

impl SecretValue {
    fn new(value: String) -> Result<Self, SecretError> {
        if value.is_empty() {
            return Err(SecretError::EmptySecret);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretValue([REDACTED])")
    }
}

#[derive(Debug)]
pub enum SecretError {
    InvalidReference,
    UnsupportedScheme,
    MissingEnvironmentVariable(String),
    FileRead {
        path: PathBuf,
        source: std::io::Error,
    },
    EmptySecret,
}

impl fmt::Display for SecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReference => f.write_str("secret reference is invalid"),
            Self::UnsupportedScheme => f.write_str("secret reference scheme is unsupported"),
            Self::MissingEnvironmentVariable(name) => {
                write!(f, "secret environment variable '{name}' is not set")
            }
            Self::FileRead { path, source } => {
                write!(
                    f,
                    "failed to read secret file '{}': {source}",
                    path.display()
                )
            }
            Self::EmptySecret => f.write_str("resolved secret is empty"),
        }
    }
}

impl std::error::Error for SecretError {}

pub trait SecretResolver: Send + Sync + 'static {
    fn resolve(&self, reference: &SecretRef) -> Result<SecretValue, SecretError>;
}

#[derive(Clone, Default)]
pub struct SystemSecretResolver;

impl SecretResolver for SystemSecretResolver {
    fn resolve(&self, reference: &SecretRef) -> Result<SecretValue, SecretError> {
        if let Some(name) = reference.as_str().strip_prefix("env://") {
            if name.is_empty() {
                return Err(SecretError::InvalidReference);
            }
            let value = std::env::var(name)
                .map_err(|_| SecretError::MissingEnvironmentVariable(name.to_string()))?;
            return SecretValue::new(value);
        }
        if let Some(path) = reference.as_str().strip_prefix("file://") {
            if path.is_empty() {
                return Err(SecretError::InvalidReference);
            }
            let path = PathBuf::from(path);
            let value = fs::read_to_string(&path).map_err(|source| SecretError::FileRead {
                path: path.clone(),
                source,
            })?;
            return SecretValue::new(value.trim_end_matches(['\r', '\n']).to_string());
        }
        Err(SecretError::UnsupportedScheme)
    }
}

#[derive(Clone)]
pub struct SecretResolverHandle(Arc<dyn SecretResolver>);

impl Default for SecretResolverHandle {
    fn default() -> Self {
        Self(Arc::new(SystemSecretResolver))
    }
}

impl SecretResolverHandle {
    #[must_use]
    pub fn new(resolver: Arc<dyn SecretResolver>) -> Self {
        Self(resolver)
    }

    pub fn resolve(&self, reference: &SecretRef) -> Result<SecretValue, SecretError> {
        self.0.resolve(reference)
    }
}

impl fmt::Debug for SecretResolverHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretResolverHandle")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_value_debug_is_redacted() {
        let value = SecretValue::new("do-not-log".to_string()).unwrap();
        let debug = format!("{value:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("do-not-log"));
    }

    #[test]
    fn secret_reference_serializes_reference_only() {
        let reference = SecretRef::env("KETEBE_PROVIDER_TOKEN").unwrap();
        let json = serde_json::to_string(&reference).unwrap();
        assert_eq!(json, "\"env://KETEBE_PROVIDER_TOKEN\"");
    }

    #[test]
    fn file_secret_is_resolved_on_each_call_for_rotation() {
        let dir = std::env::temp_dir().join(format!("ketebe-secret-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("provider.token");
        let reference = SecretRef::file(&path).unwrap();
        let resolver = SystemSecretResolver;

        fs::write(&path, "first\n").unwrap();
        assert_eq!(
            resolver.resolve(&reference).unwrap().expose_secret(),
            "first"
        );
        fs::write(&path, "second\n").unwrap();
        assert_eq!(
            resolver.resolve(&reference).unwrap().expose_secret(),
            "second"
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn missing_secret_fails_without_exposing_secret_material() {
        let reference = SecretRef::env("KETEBE_TEST_SECRET_MUST_NOT_EXIST_106").unwrap();
        let error = SystemSecretResolver.resolve(&reference).unwrap_err();
        assert!(matches!(error, SecretError::MissingEnvironmentVariable(_)));
        assert!(!error.to_string().contains("Bearer"));
    }
}
