use ketebe_core::{
    DocumentSource, DomainError, Metadata, MetadataValue, SourceIdentity, SourceKind,
    SourceRevision,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

pub const SOURCE_METADATA_KEY: &str = "_ketebe_source";
pub const CONTENT_METADATA_KEY: &str = "_ketebe_content";
const CONTENT_NORMALIZATION: &str = "line_endings_v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceChange {
    Unchanged,
    Changed,
}

#[derive(Debug)]
pub enum ProvenanceError {
    ReservedMetadata,
    InvalidSource(DomainError),
}

impl fmt::Display for ProvenanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservedMetadata => write!(
                f,
                "metadata keys '{SOURCE_METADATA_KEY}' and '{CONTENT_METADATA_KEY}' are reserved by Ketebe"
            ),
            Self::InvalidSource(error) => write!(f, "invalid document source: {error}"),
        }
    }
}
impl std::error::Error for ProvenanceError {}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceKindDto {
    Http,
    ObjectStore,
    Kafka,
    File,
    Connector,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DocumentSourceDto {
    pub(crate) kind: SourceKindDto,
    pub(crate) uri: String,
    #[serde(default)]
    pub(crate) connector: Option<String>,
    #[serde(default)]
    pub(crate) external_id: Option<String>,
    #[serde(default)]
    pub(crate) version: Option<String>,
    #[serde(default)]
    pub(crate) etag: Option<String>,
    #[serde(default)]
    pub(crate) revision: Option<String>,
    #[serde(default)]
    pub(crate) observed_at_unix_ms: Option<u64>,
}

impl DocumentSourceDto {
    pub(crate) fn into_domain(self) -> Result<DocumentSource, ProvenanceError> {
        let kind = match self.kind {
            SourceKindDto::Http => {
                reject_connector_name(self.connector.as_deref())?;
                SourceKind::Http
            }
            SourceKindDto::ObjectStore => {
                reject_connector_name(self.connector.as_deref())?;
                SourceKind::ObjectStore
            }
            SourceKindDto::Kafka => {
                reject_connector_name(self.connector.as_deref())?;
                SourceKind::Kafka
            }
            SourceKindDto::File => {
                reject_connector_name(self.connector.as_deref())?;
                SourceKind::File
            }
            SourceKindDto::Connector => {
                let name = self.connector.ok_or_else(|| {
                    ProvenanceError::InvalidSource(DomainError::InvalidIngestionConfig {
                        message: "source connector must be provided for connector kind".to_string(),
                    })
                })?;
                SourceKind::Connector(name)
            }
        };
        let identity = SourceIdentity::new(kind, self.uri, self.external_id)
            .map_err(ProvenanceError::InvalidSource)?;
        let revision = SourceRevision::new(self.version, self.etag, self.revision)
            .map_err(ProvenanceError::InvalidSource)?;
        Ok(DocumentSource::new(
            identity,
            revision,
            self.observed_at_unix_ms,
        ))
    }
}

fn reject_connector_name(value: Option<&str>) -> Result<(), ProvenanceError> {
    if value.is_some() {
        return Err(ProvenanceError::InvalidSource(
            DomainError::InvalidIngestionConfig {
                message: "source connector is only valid for connector kind".to_string(),
            },
        ));
    }
    Ok(())
}

#[must_use]
pub fn canonical_content_hash(text: &str) -> String {
    let normalized = normalize_content(text);
    let digest = Sha256::digest(normalized.as_bytes());
    hex_encode(&digest)
}

#[must_use]
pub fn normalize_content(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

pub fn apply_document_provenance(
    metadata: &mut Metadata,
    source: Option<&DocumentSource>,
    text: &str,
) -> Result<String, ProvenanceError> {
    if metadata.contains_key(SOURCE_METADATA_KEY) || metadata.contains_key(CONTENT_METADATA_KEY) {
        return Err(ProvenanceError::ReservedMetadata);
    }

    let document_hash = canonical_content_hash(text);
    let ingested_at = now_unix_ms();

    let mut content = BTreeMap::new();
    content.insert(
        "normalization".to_string(),
        MetadataValue::String(CONTENT_NORMALIZATION.to_string()),
    );
    content.insert(
        "document_sha256".to_string(),
        MetadataValue::String(document_hash.clone()),
    );
    content.insert(
        "ingested_at_unix_ms".to_string(),
        MetadataValue::Number(ingested_at as f64),
    );
    metadata.insert(
        CONTENT_METADATA_KEY.to_string(),
        MetadataValue::Object(content),
    );

    if let Some(source) = source {
        metadata.insert(
            SOURCE_METADATA_KEY.to_string(),
            MetadataValue::Object(source_metadata(source, ingested_at)),
        );
    }
    Ok(document_hash)
}

pub fn apply_chunk_content_hash(metadata: &mut Metadata, chunk_text: &str) {
    let hash = canonical_content_hash(chunk_text);
    let content = metadata
        .entry(CONTENT_METADATA_KEY.to_string())
        .or_insert_with(|| MetadataValue::Object(BTreeMap::new()));
    if let MetadataValue::Object(content) = content {
        content.insert("chunk_sha256".to_string(), MetadataValue::String(hash));
    }
}

#[must_use]
pub fn detect_source_change(
    existing: &Metadata,
    source: Option<&DocumentSource>,
    text: &str,
) -> SourceChange {
    let Some(MetadataValue::Object(content)) = existing.get(CONTENT_METADATA_KEY) else {
        return SourceChange::Changed;
    };
    let expected_hash = canonical_content_hash(text);
    if !matches!(
        content.get("document_sha256"),
        Some(MetadataValue::String(value)) if value == &expected_hash
    ) {
        return SourceChange::Changed;
    }

    match (existing.get(SOURCE_METADATA_KEY), source) {
        (None, None) => SourceChange::Unchanged,
        (Some(MetadataValue::Object(current)), Some(source)) => {
            let candidate = source_metadata(source, 0);
            for key in [
                "kind",
                "connector",
                "uri",
                "external_id",
                "version",
                "etag",
                "revision",
                "observed_at_unix_ms",
            ] {
                if current.get(key) != candidate.get(key) {
                    return SourceChange::Changed;
                }
            }
            SourceChange::Unchanged
        }
        _ => SourceChange::Changed,
    }
}

fn source_metadata(source: &DocumentSource, ingested_at: u64) -> BTreeMap<String, MetadataValue> {
    let mut output = BTreeMap::new();
    match source.identity().kind() {
        SourceKind::Http => {
            output.insert(
                "kind".to_string(),
                MetadataValue::String("http".to_string()),
            );
        }
        SourceKind::ObjectStore => {
            output.insert(
                "kind".to_string(),
                MetadataValue::String("object_store".to_string()),
            );
        }
        SourceKind::Kafka => {
            output.insert(
                "kind".to_string(),
                MetadataValue::String("kafka".to_string()),
            );
        }
        SourceKind::File => {
            output.insert(
                "kind".to_string(),
                MetadataValue::String("file".to_string()),
            );
        }
        SourceKind::Connector(name) => {
            output.insert(
                "kind".to_string(),
                MetadataValue::String("connector".to_string()),
            );
            output.insert("connector".to_string(), MetadataValue::String(name.clone()));
        }
    }
    output.insert(
        "uri".to_string(),
        MetadataValue::String(source.identity().uri().to_string()),
    );
    if let Some(value) = source.identity().external_id() {
        output.insert(
            "external_id".to_string(),
            MetadataValue::String(value.to_string()),
        );
    }
    if let Some(value) = source.revision().version() {
        output.insert(
            "version".to_string(),
            MetadataValue::String(value.to_string()),
        );
    }
    if let Some(value) = source.revision().etag() {
        output.insert("etag".to_string(), MetadataValue::String(value.to_string()));
    }
    if let Some(value) = source.revision().revision() {
        output.insert(
            "revision".to_string(),
            MetadataValue::String(value.to_string()),
        );
    }
    if let Some(value) = source.observed_at_unix_ms() {
        output.insert(
            "observed_at_unix_ms".to_string(),
            MetadataValue::Number(value as f64),
        );
    }
    output.insert(
        "ingested_at_unix_ms".to_string(),
        MetadataValue::Number(ingested_at as f64),
    );
    output
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(version: &str) -> DocumentSource {
        DocumentSource::new(
            SourceIdentity::new(
                SourceKind::Http,
                "https://example.test/doc",
                Some("doc-1".to_string()),
            )
            .unwrap(),
            SourceRevision::new(Some(version.to_string()), None, None).unwrap(),
            Some(42),
        )
    }

    #[test]
    fn canonical_hash_normalizes_line_endings() {
        assert_eq!(
            canonical_content_hash("a\r\nb\r"),
            canonical_content_hash("a\nb\n")
        );
        assert_ne!(
            canonical_content_hash("a\nb"),
            canonical_content_hash("a\nc")
        );
    }

    #[test]
    fn source_change_detection_uses_content_and_revision() {
        let mut metadata = Metadata::new();
        apply_document_provenance(&mut metadata, Some(&source("v1")), "hello").unwrap();
        assert_eq!(
            detect_source_change(&metadata, Some(&source("v1")), "hello"),
            SourceChange::Unchanged
        );
        assert_eq!(
            detect_source_change(&metadata, Some(&source("v2")), "hello"),
            SourceChange::Changed
        );
        assert_eq!(
            detect_source_change(&metadata, Some(&source("v1")), "changed"),
            SourceChange::Changed
        );
    }
}
