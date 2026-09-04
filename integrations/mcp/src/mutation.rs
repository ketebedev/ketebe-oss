use crate::{ketebe::KetebeApi, retrieval::AgentRecordId};
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct UpsertRecordInput {
    pub id: AgentRecordId,
    pub vector: Vec<f32>,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct UpsertRecordsParams {
    pub collection: String,
    pub records: Vec<UpsertRecordInput>,
}

impl UpsertRecordsParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.collection.trim().is_empty() {
            return Err("Ketebe mutation request failed: collection_required".to_string());
        }
        if self.records.is_empty() {
            return Err("Ketebe mutation request failed: records_required".to_string());
        }
        if self.records.iter().any(|record| record.vector.is_empty()) {
            return Err("Ketebe mutation request failed: vector_required".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
pub struct UpsertRecordsOutput {
    pub accepted_records: usize,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct IngestDocumentInput {
    pub id: AgentRecordId,
    pub text: String,
    #[serde(default)]
    pub metadata: Option<Value>,
    #[serde(default)]
    pub source: Option<Value>,
    #[serde(default)]
    pub chunking: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct IngestDocumentsParams {
    pub collection: String,
    pub documents: Vec<IngestDocumentInput>,
}

impl IngestDocumentsParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.collection.trim().is_empty() {
            return Err("Ketebe document ingestion failed: collection_required".to_string());
        }
        if self.documents.is_empty() {
            return Err("Ketebe document ingestion failed: documents_required".to_string());
        }
        if self
            .documents
            .iter()
            .any(|document| document.text.trim().is_empty())
        {
            return Err("Ketebe document ingestion failed: text_required".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
pub struct IngestDocumentsOutput {
    pub accepted_documents: Vec<AgentRecordId>,
}

impl KetebeApi {
    pub async fn upsert_records(
        &self,
        params: UpsertRecordsParams,
        bearer_token: Option<&str>,
    ) -> Result<UpsertRecordsOutput, String> {
        params.validate()?;
        let accepted_records = params.records.len();
        let request = ketebe_sdk::BatchUpsert {
            records: params
                .records
                .into_iter()
                .map(|record| ketebe_sdk::BatchRecordUpsert {
                    id: record.id.into(),
                    vector: record.vector,
                    metadata: record.metadata,
                })
                .collect(),
        };
        self.client_for(bearer_token)
            .map_err(mutation_error)?
            .batch_upsert_records(&params.collection, &request)
            .await
            .map_err(mutation_error)?;
        Ok(UpsertRecordsOutput { accepted_records })
    }

    pub async fn ingest_documents(
        &self,
        params: IngestDocumentsParams,
        bearer_token: Option<&str>,
    ) -> Result<IngestDocumentsOutput, String> {
        params.validate()?;
        let client = self.client_for(bearer_token).map_err(mutation_error)?;
        let mut accepted_documents = Vec::with_capacity(params.documents.len());
        for document in params.documents {
            let id = document.id.clone();
            client
                .upsert_document(
                    &params.collection,
                    &document.id.into(),
                    &ketebe_sdk::DocumentUpsert {
                        text: document.text,
                        metadata: document.metadata,
                        source: document.source,
                        chunking: document.chunking,
                    },
                )
                .await
                .map_err(mutation_error)?;
            accepted_documents.push(id);
        }
        Ok(IngestDocumentsOutput { accepted_documents })
    }
}

fn mutation_error(error: ketebe_sdk::Error) -> String {
    match error {
        ketebe_sdk::Error::Api { status, code, .. } => {
            format!("Ketebe mutation request failed: {status} {code}")
        }
        ketebe_sdk::Error::Transport(_) => "Ketebe mutation service unavailable".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_inputs_reject_empty_batches_and_payloads() {
        assert!(
            UpsertRecordsParams {
                collection: "docs".into(),
                records: Vec::new(),
            }
            .validate()
            .is_err()
        );
        assert!(
            IngestDocumentsParams {
                collection: "docs".into(),
                documents: vec![IngestDocumentInput {
                    id: AgentRecordId::String("empty".into()),
                    text: "   ".into(),
                    metadata: None,
                    source: None,
                    chunking: None,
                }],
            }
            .validate()
            .is_err()
        );
    }
}
