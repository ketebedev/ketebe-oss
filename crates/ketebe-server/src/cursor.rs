use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ketebe_core::{CollectionId, DistanceMetric, MetadataValue, Predicate, RecordId};
use ketebe_storage::ExecutionPreference;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const CURSOR_VERSION: u8 = 1;
pub(crate) const CURSOR_MAX_AGE_MS: u64 = 15 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(crate) enum CursorRecordId {
    String(String),
    Unsigned(u64),
}

impl From<&RecordId> for CursorRecordId {
    fn from(value: &RecordId) -> Self {
        match value {
            RecordId::String(v) => Self::String(v.clone()),
            RecordId::Unsigned(v) => Self::Unsigned(*v),
        }
    }
}
impl CursorRecordId {
    pub(crate) fn to_record_id(&self) -> Result<RecordId, CursorError> {
        match self {
            Self::String(v) => {
                RecordId::string(v.clone()).map_err(|e| CursorError::Invalid(e.to_string()))
            }
            Self::Unsigned(v) => Ok(RecordId::unsigned(*v)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CursorPayload {
    pub version: u8,
    pub collection_id: String,
    pub query_hash: String,
    pub snapshot_sequence: u64,
    pub checkpoint_sequence: Option<u64>,
    pub score: f32,
    pub record_id: CursorRecordId,
    pub issued_at_unix_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CursorEnvelope {
    payload: CursorPayload,
    checksum: String,
}

#[derive(Debug)]
pub enum CursorError {
    Invalid(String),
    UnsupportedVersion(u8),
    Expired,
    StaleSnapshot,
    QueryMismatch,
}
impl fmt::Display for CursorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(m) => write!(f, "invalid cursor: {m}"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported cursor version: {v}"),
            Self::Expired => f.write_str("cursor has expired"),
            Self::StaleSnapshot => f.write_str("cursor snapshot is stale"),
            Self::QueryMismatch => f.write_str("cursor does not match this query or collection"),
        }
    }
}
impl std::error::Error for CursorError {}

pub(crate) fn encode_cursor(
    collection_id: &CollectionId,
    query_hash: String,
    snapshot_sequence: u64,
    checkpoint_sequence: Option<u64>,
    score: f32,
    record_id: &RecordId,
) -> Result<String, CursorError> {
    if !score.is_finite() {
        return Err(CursorError::Invalid("non-finite score".into()));
    }
    let payload = CursorPayload {
        version: CURSOR_VERSION,
        collection_id: collection_id.as_str().to_string(),
        query_hash,
        snapshot_sequence,
        checkpoint_sequence,
        score,
        record_id: CursorRecordId::from(record_id),
        issued_at_unix_ms: unix_ms(),
    };
    let payload_bytes =
        serde_json::to_vec(&payload).map_err(|e| CursorError::Invalid(e.to_string()))?;
    let envelope = CursorEnvelope {
        payload,
        checksum: sha256_hex(&payload_bytes),
    };
    let bytes = serde_json::to_vec(&envelope).map_err(|e| CursorError::Invalid(e.to_string()))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub(crate) fn decode_cursor(token: &str) -> Result<CursorPayload, CursorError> {
    if token.is_empty() || token.len() > 8192 {
        return Err(CursorError::Invalid("token length is invalid".into()));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| CursorError::Invalid("base64 decoding failed".into()))?;
    let envelope: CursorEnvelope = serde_json::from_slice(&bytes)
        .map_err(|_| CursorError::Invalid("payload decoding failed".into()))?;
    let payload_bytes =
        serde_json::to_vec(&envelope.payload).map_err(|e| CursorError::Invalid(e.to_string()))?;
    if sha256_hex(&payload_bytes) != envelope.checksum {
        return Err(CursorError::Invalid("checksum mismatch".into()));
    }
    if envelope.payload.version != CURSOR_VERSION {
        return Err(CursorError::UnsupportedVersion(envelope.payload.version));
    }
    let now = unix_ms();
    if envelope.payload.issued_at_unix_ms > now.saturating_add(60_000) {
        return Err(CursorError::Invalid("issued-at is in the future".into()));
    }
    if now.saturating_sub(envelope.payload.issued_at_unix_ms) > CURSOR_MAX_AGE_MS {
        return Err(CursorError::Expired);
    }
    Ok(envelope.payload)
}

pub(crate) fn validate_cursor(
    payload: &CursorPayload,
    collection_id: &CollectionId,
    query_hash: &str,
    snapshot_sequence: u64,
) -> Result<(), CursorError> {
    if payload.collection_id != collection_id.as_str() || payload.query_hash != query_hash {
        return Err(CursorError::QueryMismatch);
    }
    if payload.snapshot_sequence != snapshot_sequence {
        return Err(CursorError::StaleSnapshot);
    }
    Ok(())
}

pub(crate) struct CursorQueryBinding<'a> {
    pub collection_id: &'a CollectionId,
    pub vector: &'a [f32],
    pub predicate: Option<&'a Predicate>,
    pub execution: ExecutionPreference,
    pub search_profile: Option<&'a str>,
    pub metric: DistanceMetric,
}

pub(crate) fn cursor_query_hash(binding: &CursorQueryBinding<'_>) -> String {
    let mut canonical = String::new();
    canonical.push_str("cursor-query-v1|");
    push_string(&mut canonical, binding.collection_id.as_str());
    canonical.push('|');
    canonical.push_str(match binding.metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::Dot => "dot",
        DistanceMetric::L2 => "l2",
    });
    canonical.push('|');
    canonical.push_str(match binding.execution {
        ExecutionPreference::Auto => "auto",
        ExecutionPreference::Exact => "exact",
        ExecutionPreference::Hnsw => "hnsw",
    });
    canonical.push('|');
    push_string(
        &mut canonical,
        binding.search_profile.unwrap_or("default@1"),
    );
    canonical.push('|');
    for value in binding.vector {
        canonical.push_str(&format!("{:08x},", value.to_bits()));
    }
    canonical.push('|');
    if let Some(predicate) = binding.predicate {
        push_predicate(&mut canonical, predicate);
    } else {
        canonical.push('-');
    }
    sha256_hex(canonical.as_bytes())
}

fn push_predicate(out: &mut String, p: &Predicate) {
    match p {
        Predicate::Eq(path, v) => push_cmp(out, "eq", path.segments(), v),
        Predicate::Ne(path, v) => push_cmp(out, "ne", path.segments(), v),
        Predicate::Lt(path, v) => push_cmp(out, "lt", path.segments(), v),
        Predicate::Lte(path, v) => push_cmp(out, "lte", path.segments(), v),
        Predicate::Gt(path, v) => push_cmp(out, "gt", path.segments(), v),
        Predicate::Gte(path, v) => push_cmp(out, "gte", path.segments(), v),
        Predicate::Contains(path, v) => push_cmp(out, "contains", path.segments(), v),
        Predicate::Exists(path) => {
            out.push_str("exists[");
            push_path(out, path.segments());
            out.push(']');
        }
        Predicate::In(path, values) => {
            out.push_str("in[");
            push_path(out, path.segments());
            out.push(':');
            for v in values {
                push_metadata(out, v);
                out.push(',');
            }
            out.push(']');
        }
        Predicate::And(values) => push_list(out, "and", values),
        Predicate::Or(values) => push_list(out, "or", values),
        Predicate::Not(value) => {
            out.push_str("not(");
            push_predicate(out, value);
            out.push(')');
        }
    }
}
fn push_cmp(out: &mut String, name: &str, path: &[String], value: &MetadataValue) {
    out.push_str(name);
    out.push('[');
    push_path(out, path);
    out.push(':');
    push_metadata(out, value);
    out.push(']');
}
fn push_list(out: &mut String, name: &str, values: &[Predicate]) {
    out.push_str(name);
    out.push('(');
    for v in values {
        push_predicate(out, v);
        out.push(';');
    }
    out.push(')');
}
fn push_path(out: &mut String, path: &[String]) {
    for p in path {
        push_string(out, p);
        out.push('/');
    }
}
fn push_metadata(out: &mut String, v: &MetadataValue) {
    match v {
        MetadataValue::Null => out.push('n'),
        MetadataValue::Bool(v) => out.push_str(if *v { "t" } else { "f" }),
        MetadataValue::Number(v) => out.push_str(&format!("d{:016x}", v.to_bits())),
        MetadataValue::String(v) => {
            out.push('s');
            push_string(out, v);
        }
        MetadataValue::Array(values) => {
            out.push('[');
            for v in values {
                push_metadata(out, v);
                out.push(',');
            }
            out.push(']');
        }
        MetadataValue::Object(values) => {
            out.push('{');
            for (k, v) in values {
                push_string(out, k);
                out.push(':');
                push_metadata(out, v);
                out.push(',');
            }
            out.push('}');
        }
    }
}
fn push_string(out: &mut String, v: &str) {
    out.push_str(&v.len().to_string());
    out.push(':');
    out.push_str(v);
}
fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursor_round_trip_preserves_typed_record_id() {
        let c = CollectionId::new("docs").unwrap();
        let token =
            encode_cursor(&c, "abc".into(), 7, Some(5), 1.25, &RecordId::unsigned(42)).unwrap();
        let p = decode_cursor(&token).unwrap();
        validate_cursor(&p, &c, "abc", 7).unwrap();
        assert_eq!(p.record_id.to_record_id().unwrap(), RecordId::unsigned(42));
    }
}
