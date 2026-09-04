use crate::ketebe::KetebeApi;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct ReembeddingParams {
    pub collection: String,
    pub target_profile: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct ReembeddingStatusParams {
    pub collection: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct ReembeddingView {
    pub version: u32,
    pub source_profile: String,
    pub target_profile: String,
    pub target_provider: String,
    pub target_model: String,
    pub target_model_version: String,
    pub status: String,
    pub total_managed_records: usize,
    pub completed_records: usize,
    pub catch_up_runs: u64,
    pub reconciled_records: u64,
    pub last_frontier_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReembeddingResponse {
    version: u32,
    source_profile: String,
    target_profile: String,
    target_provider: String,
    target_model: String,
    target_model_version: String,
    status: String,
    total_managed_records: usize,
    completed_records: usize,
    #[serde(default)]
    catch_up_runs: u64,
    #[serde(default)]
    reconciled_records: u64,
    #[serde(default)]
    last_frontier_sequence: u64,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct StartReembeddingRequest<'a> {
    target_profile: &'a str,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
}

impl ReembeddingParams {
    pub fn validate(&self) -> Result<(), String> {
        validate_collection(&self.collection)?;
        if self.target_profile.trim().is_empty() {
            return Err("Ketebe re-embedding request failed: invalid_target_profile".to_string());
        }
        Ok(())
    }
}

impl ReembeddingStatusParams {
    pub fn validate(&self) -> Result<(), String> {
        validate_collection(&self.collection)
    }
}

fn validate_collection(collection: &str) -> Result<(), String> {
    if collection.trim().is_empty() {
        return Err("Ketebe re-embedding request failed: invalid_collection".to_string());
    }
    Ok(())
}

impl From<ReembeddingResponse> for ReembeddingView {
    fn from(value: ReembeddingResponse) -> Self {
        Self {
            version: value.version,
            source_profile: value.source_profile,
            target_profile: value.target_profile,
            target_provider: value.target_provider,
            target_model: value.target_model,
            target_model_version: value.target_model_version,
            status: value.status,
            total_managed_records: value.total_managed_records,
            completed_records: value.completed_records,
            catch_up_runs: value.catch_up_runs,
            reconciled_records: value.reconciled_records,
            last_frontier_sequence: value.last_frontier_sequence,
            error: value.error,
        }
    }
}

impl KetebeApi {
    pub async fn start_reembedding(
        &self,
        params: ReembeddingParams,
        bearer_token: Option<&str>,
    ) -> Result<ReembeddingView, String> {
        params.validate()?;
        let url = format!(
            "{}/v0/collections/{}/embedding-migration",
            self.base_url.trim_end_matches('/'),
            params.collection
        );
        let request = StartReembeddingRequest {
            target_profile: &params.target_profile,
        };
        self.reembedding_request(self.http.post(url).json(&request), bearer_token)
            .await
    }

    pub async fn get_reembedding_status(
        &self,
        params: ReembeddingStatusParams,
        bearer_token: Option<&str>,
    ) -> Result<ReembeddingView, String> {
        params.validate()?;
        let url = format!(
            "{}/v0/collections/{}/embedding-migration",
            self.base_url.trim_end_matches('/'),
            params.collection
        );
        self.reembedding_request(self.http.get(url), bearer_token)
            .await
    }

    async fn reembedding_request(
        &self,
        mut builder: reqwest::RequestBuilder,
        bearer_token: Option<&str>,
    ) -> Result<ReembeddingView, String> {
        if let Some(token) = bearer_token {
            builder = builder.bearer_auth(token);
        }
        let response = builder
            .send()
            .await
            .map_err(|_| "Ketebe re-embedding service unavailable".to_string())?;
        let status = response.status();
        if status.is_success() {
            return response
                .json::<ReembeddingResponse>()
                .await
                .map(ReembeddingView::from)
                .map_err(|_| "Ketebe re-embedding service unavailable".to_string());
        }
        let code = response
            .json::<ErrorEnvelope>()
            .await
            .map(|value| value.error.code)
            .unwrap_or_else(|_| "http_error".to_string());
        Err(format!(
            "Ketebe re-embedding request failed: {} {code}",
            status.as_u16()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_blank_collection_and_target_profile() {
        assert!(
            ReembeddingParams {
                collection: " ".into(),
                target_profile: "next".into(),
            }
            .validate()
            .is_err()
        );
        assert!(
            ReembeddingParams {
                collection: "docs".into(),
                target_profile: " ".into(),
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn public_view_contains_no_provider_secret_fields() {
        let value = serde_json::to_value(ReembeddingView {
            version: 1,
            source_profile: "old".into(),
            target_profile: "new".into(),
            target_provider: "openai-compatible".into(),
            target_model: "embedding-model".into(),
            target_model_version: "v2".into(),
            status: "running".into(),
            total_managed_records: 10,
            completed_records: 3,
            catch_up_runs: 0,
            reconciled_records: 0,
            last_frontier_sequence: 7,
            error: None,
        })
        .unwrap();
        let serde_json::Value::Object(fields) = value else {
            panic!("re-embedding view must serialize as an object");
        };
        assert!(!fields.contains_key("endpoint"));
        assert!(!fields.contains_key("api_key"));
        assert!(!fields.contains_key("api_key_ref"));
        assert!(!fields.contains_key("credentials"));
    }
}
