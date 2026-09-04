use crate::ketebe::KetebeApi;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProfileParams {
    pub profile: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EmbeddingProfileView {
    pub profile: String,
    pub provider: String,
    pub model: String,
    pub model_version: String,
    pub dimension: Option<usize>,
    pub is_default: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ListEmbeddingProfilesOutput {
    pub profiles: Vec<EmbeddingProfileView>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RerankerProfileView {
    pub profile: String,
    pub provider: String,
    pub is_default: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ListRerankerProfilesOutput {
    pub profiles: Vec<RerankerProfileView>,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
}

impl ProfileParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.profile.trim().is_empty() {
            return Err("Ketebe profile request failed: invalid_profile".to_string());
        }
        Ok(())
    }
}

impl KetebeApi {
    pub async fn list_embedding_profiles(
        &self,
        bearer_token: Option<&str>,
    ) -> Result<ListEmbeddingProfilesOutput, String> {
        let url = format!(
            "{}/v0/embedding-profiles",
            self.base_url.trim_end_matches('/')
        );
        self.profile_discovery_request(self.http.get(url), bearer_token)
            .await
    }

    pub async fn describe_embedding_profile(
        &self,
        params: ProfileParams,
        bearer_token: Option<&str>,
    ) -> Result<EmbeddingProfileView, String> {
        params.validate()?;
        let url = format!(
            "{}/v0/embedding-profiles/{}",
            self.base_url.trim_end_matches('/'),
            params.profile
        );
        self.profile_discovery_request(self.http.get(url), bearer_token)
            .await
    }

    pub async fn list_reranker_profiles(
        &self,
        bearer_token: Option<&str>,
    ) -> Result<ListRerankerProfilesOutput, String> {
        let url = format!(
            "{}/v0/reranker-profiles",
            self.base_url.trim_end_matches('/')
        );
        self.profile_discovery_request(self.http.get(url), bearer_token)
            .await
    }

    pub async fn describe_reranker_profile(
        &self,
        params: ProfileParams,
        bearer_token: Option<&str>,
    ) -> Result<RerankerProfileView, String> {
        params.validate()?;
        let url = format!(
            "{}/v0/reranker-profiles/{}",
            self.base_url.trim_end_matches('/'),
            params.profile
        );
        self.profile_discovery_request(self.http.get(url), bearer_token)
            .await
    }

    async fn profile_discovery_request<T>(
        &self,
        mut builder: reqwest::RequestBuilder,
        bearer_token: Option<&str>,
    ) -> Result<T, String>
    where
        T: for<'de> Deserialize<'de>,
    {
        if let Some(token) = bearer_token {
            builder = builder.bearer_auth(token);
        }
        let response = builder
            .send()
            .await
            .map_err(|_| "Ketebe profile discovery unavailable".to_string())?;
        let status = response.status();
        if status.is_success() {
            return response
                .json::<T>()
                .await
                .map_err(|_| "Ketebe profile discovery unavailable".to_string());
        }
        let code = response
            .json::<ErrorEnvelope>()
            .await
            .map(|value| value.error.code)
            .unwrap_or_else(|_| "http_error".to_string());
        Err(format!(
            "Ketebe profile request failed: {} {code}",
            status.as_u16()
        ))
    }
}
