use crate::ketebe::KetebeApi;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobStateView {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct JobProgressView {
    pub completed: u64,
    pub total: Option<u64>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct JobFailureView {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct JobView {
    pub version: u32,
    pub id: u64,
    pub kind: Value,
    pub state: JobStateView,
    pub progress: JobProgressView,
    pub error: Option<JobFailureView>,
    pub result: Option<Value>,
    pub cancel_requested: bool,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct JobParams {
    pub job_id: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ListJobsOutput {
    pub jobs: Vec<JobView>,
}

#[derive(Debug, Deserialize)]
struct JobListResponse {
    jobs: Vec<JobView>,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
}

impl KetebeApi {
    pub async fn get_job(
        &self,
        job_id: u64,
        bearer_token: Option<&str>,
    ) -> Result<JobView, String> {
        if job_id == 0 {
            return Err("Ketebe job request failed: invalid_job_id".to_string());
        }
        let url = format!("{}/v0/jobs/{job_id}", self.base_url.trim_end_matches('/'));
        self.job_request(self.http.get(url), bearer_token).await
    }

    pub async fn list_jobs(&self, bearer_token: Option<&str>) -> Result<ListJobsOutput, String> {
        let url = format!("{}/v0/jobs", self.base_url.trim_end_matches('/'));
        let mut builder = self.http.get(url);
        if let Some(token) = bearer_token {
            builder = builder.bearer_auth(token);
        }
        let response = builder
            .send()
            .await
            .map_err(|_| "Ketebe job service unavailable".to_string())?;
        let status = response.status();
        if status.is_success() {
            return response
                .json::<JobListResponse>()
                .await
                .map(|value| ListJobsOutput { jobs: value.jobs })
                .map_err(|_| "Ketebe job service unavailable".to_string());
        }
        let code = response
            .json::<ErrorEnvelope>()
            .await
            .map(|value| value.error.code)
            .unwrap_or_else(|_| "http_error".to_string());
        Err(format!(
            "Ketebe job request failed: {} {code}",
            status.as_u16()
        ))
    }

    pub async fn cancel_job(
        &self,
        job_id: u64,
        bearer_token: Option<&str>,
    ) -> Result<JobView, String> {
        if job_id == 0 {
            return Err("Ketebe job request failed: invalid_job_id".to_string());
        }
        let url = format!(
            "{}/v0/jobs/{job_id}/cancel",
            self.base_url.trim_end_matches('/')
        );
        self.job_request(self.http.post(url), bearer_token).await
    }

    async fn job_request(
        &self,
        mut builder: reqwest::RequestBuilder,
        bearer_token: Option<&str>,
    ) -> Result<JobView, String> {
        if let Some(token) = bearer_token {
            builder = builder.bearer_auth(token);
        }
        let response = builder
            .send()
            .await
            .map_err(|_| "Ketebe job service unavailable".to_string())?;
        let status = response.status();
        if status.is_success() {
            return response
                .json::<JobView>()
                .await
                .map_err(|_| "Ketebe job service unavailable".to_string());
        }
        let code = response
            .json::<ErrorEnvelope>()
            .await
            .map(|value| value.error.code)
            .unwrap_or_else(|_| "http_error".to_string());
        Err(format!(
            "Ketebe job request failed: {} {code}",
            status.as_u16()
        ))
    }
}
