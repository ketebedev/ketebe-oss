use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", content = "value", rename_all = "lowercase")]
pub enum AgentRecordId {
    String(String),
    U64(u64),
}

impl From<AgentRecordId> for ketebe_sdk::RecordId {
    fn from(value: AgentRecordId) -> Self {
        match value {
            AgentRecordId::String(value) => Self::String(value),
            AgentRecordId::U64(value) => Self::U64(value),
        }
    }
}

impl From<ketebe_sdk::RecordId> for AgentRecordId {
    fn from(value: ketebe_sdk::RecordId) -> Self {
        match value {
            ketebe_sdk::RecordId::String(value) => Self::String(value),
            ketebe_sdk::RecordId::U64(value) => Self::U64(value),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct GetRecordParams {
    pub collection: String,
    pub id: AgentRecordId,
    #[serde(default)]
    pub fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct GetRecordsParams {
    pub collection: String,
    pub ids: Vec<AgentRecordId>,
    #[serde(default)]
    pub fields: Vec<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct RecordView {
    pub id: AgentRecordId,
    pub sequence_number: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct GetRecordsOutput {
    pub records: Vec<RecordView>,
    pub missing: Vec<AgentRecordId>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FetchRecordsRequest {
    pub(crate) ids: Vec<ketebe_sdk::RecordId>,
    pub(crate) fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct FetchRecordsResponse {
    pub(crate) records: Vec<FetchedRecord>,
    pub(crate) missing: Vec<ketebe_sdk::RecordId>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct FetchedRecord {
    pub(crate) id: ketebe_sdk::RecordId,
    pub(crate) sequence_number: u64,
    pub(crate) vector: Option<Vec<f32>>,
    pub(crate) metadata: Option<Value>,
}

impl From<FetchedRecord> for RecordView {
    fn from(value: FetchedRecord) -> Self {
        Self {
            id: value.id.into(),
            sequence_number: value.sequence_number,
            vector: value.vector,
            metadata: value.metadata,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordFetchError {
    Api { status: u16, code: String },
    Transport,
}

impl RecordFetchError {
    #[must_use]
    pub fn stable_message(&self) -> String {
        match self {
            Self::Api { status, code } => format!("Ketebe record request failed: {status} {code}"),
            Self::Transport => "Ketebe record service unavailable".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_id_variants_remain_distinct() {
        let string = serde_json::to_value(AgentRecordId::String("42".into())).unwrap();
        let numeric = serde_json::to_value(AgentRecordId::U64(42)).unwrap();
        assert_ne!(string, numeric);
        assert_eq!(
            serde_json::from_value::<AgentRecordId>(string).unwrap(),
            AgentRecordId::String("42".into())
        );
        assert_eq!(
            serde_json::from_value::<AgentRecordId>(numeric).unwrap(),
            AgentRecordId::U64(42)
        );
    }
}
