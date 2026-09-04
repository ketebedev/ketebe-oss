use crate::DomainError;

/// Durable source family for AI-native document provenance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SourceKind {
    Http,
    ObjectStore,
    Kafka,
    File,
    Connector(String),
}

/// Stable source identity. It is deliberately independent from `RecordId`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceIdentity {
    kind: SourceKind,
    uri: String,
    external_id: Option<String>,
}

impl SourceIdentity {
    pub fn new(
        kind: SourceKind,
        uri: impl Into<String>,
        external_id: Option<String>,
    ) -> Result<Self, DomainError> {
        let uri = uri.into();
        if uri.trim().is_empty() || uri.trim() != uri {
            return Err(DomainError::InvalidIngestionConfig {
                message: "source uri must be non-empty and trimmed".to_string(),
            });
        }
        if let SourceKind::Connector(name) = &kind
            && (name.trim().is_empty() || name.trim() != name)
        {
            return Err(DomainError::InvalidIngestionConfig {
                message: "source connector name must be non-empty and trimmed".to_string(),
            });
        }
        if external_id
            .as_ref()
            .is_some_and(|value| value.trim().is_empty() || value.trim() != value)
        {
            return Err(DomainError::InvalidIngestionConfig {
                message: "source external_id must be non-empty and trimmed when provided"
                    .to_string(),
            });
        }
        Ok(Self {
            kind,
            uri,
            external_id,
        })
    }

    #[must_use]
    pub fn kind(&self) -> &SourceKind {
        &self.kind
    }

    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    #[must_use]
    pub fn external_id(&self) -> Option<&str> {
        self.external_id.as_deref()
    }
}

/// Optional connector/source revision signals. At least one signal may be supplied.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct SourceRevision {
    version: Option<String>,
    etag: Option<String>,
    revision: Option<String>,
}

impl SourceRevision {
    pub fn new(
        version: Option<String>,
        etag: Option<String>,
        revision: Option<String>,
    ) -> Result<Self, DomainError> {
        for (name, value) in [
            ("version", version.as_ref()),
            ("etag", etag.as_ref()),
            ("revision", revision.as_ref()),
        ] {
            if value.is_some_and(|value| value.trim().is_empty() || value.trim() != value) {
                return Err(DomainError::InvalidIngestionConfig {
                    message: format!("source {name} must be non-empty and trimmed when provided"),
                });
            }
        }
        Ok(Self {
            version,
            etag,
            revision,
        })
    }

    #[must_use]
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    #[must_use]
    pub fn revision(&self) -> Option<&str> {
        self.revision.as_deref()
    }
}

/// Typed source provenance supplied by a document ingestion caller.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DocumentSource {
    identity: SourceIdentity,
    revision: SourceRevision,
    observed_at_unix_ms: Option<u64>,
}

impl DocumentSource {
    #[must_use]
    pub fn new(
        identity: SourceIdentity,
        revision: SourceRevision,
        observed_at_unix_ms: Option<u64>,
    ) -> Self {
        Self {
            identity,
            revision,
            observed_at_unix_ms,
        }
    }

    #[must_use]
    pub fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    #[must_use]
    pub fn revision(&self) -> &SourceRevision {
        &self.revision
    }

    #[must_use]
    pub const fn observed_at_unix_ms(&self) -> Option<u64> {
        self.observed_at_unix_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_identity_is_typed_and_independent() {
        let source = SourceIdentity::new(
            SourceKind::Http,
            "https://example.test/docs/1",
            Some("external-1".to_string()),
        )
        .unwrap();
        assert_eq!(source.uri(), "https://example.test/docs/1");
        assert_eq!(source.external_id(), Some("external-1"));
        assert_eq!(source.kind(), &SourceKind::Http);
    }

    #[test]
    fn empty_source_fields_are_rejected() {
        assert!(SourceIdentity::new(SourceKind::File, "", None).is_err());
        assert!(SourceIdentity::new(SourceKind::Connector(" ".to_string()), "x", None).is_err());
        assert!(SourceRevision::new(Some(" ".to_string()), None, None).is_err());
    }
}
