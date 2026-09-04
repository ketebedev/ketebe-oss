use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::fmt;
use std::time::Duration;

#[derive(Clone)]
pub struct ClientConfig {
    pub base_url: String,
    pub timeout: Duration,
    pub max_retries: usize,
    pub retry_backoff: Duration,
    bearer_token: Option<String>,
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("base_url", &self.base_url)
            .field("timeout", &self.timeout)
            .field("max_retries", &self.max_retries)
            .field("retry_backoff", &self.retry_backoff)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

impl ClientConfig {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            timeout: Duration::from_secs(10),
            max_retries: 2,
            retry_backoff: Duration::from_millis(50),
            bearer_token: None,
        }
    }

    #[must_use]
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    config: ClientConfig,
    http: reqwest::Client,
}

impl Client {
    pub fn new(config: ClientConfig) -> Result<Self, Error> {
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(Error::Transport)?;
        Ok(Self { config, http })
    }

    pub async fn health(&self) -> Result<(), Error> {
        self.send_json::<(), Value>(Method::GET, "/healthz", None, true)
            .await
            .map(|_| ())
    }

    pub async fn list_collections(&self) -> Result<Vec<Collection>, Error> {
        #[derive(Deserialize)]
        struct Response {
            collections: Vec<Collection>,
        }
        Ok(self
            .send_json::<(), Response>(Method::GET, "/v0/collections", None, true)
            .await?
            .collections)
    }

    pub async fn create_collection(&self, request: &CreateCollection) -> Result<Collection, Error> {
        self.send_json(Method::POST, "/v0/collections", Some(request), false)
            .await
    }

    pub async fn get_collection(&self, id: &str) -> Result<Collection, Error> {
        self.send_json::<(), Collection>(Method::GET, &format!("/v0/collections/{id}"), None, true)
            .await
    }

    pub async fn delete_collection(&self, id: &str) -> Result<(), Error> {
        self.send_empty(Method::DELETE, &format!("/v0/collections/{id}"), true)
            .await
    }

    pub async fn upsert_record(
        &self,
        collection: &str,
        id: &RecordId,
        request: &RecordUpsert,
    ) -> Result<Mutation, Error> {
        self.send_json(
            Method::PUT,
            &format!(
                "/v0/collections/{collection}/records/{}",
                id.path_component()
            ),
            Some(request),
            true,
        )
        .await
    }

    pub async fn batch_upsert_records(
        &self,
        collection: &str,
        request: &BatchUpsert,
    ) -> Result<Value, Error> {
        self.send_json(
            Method::POST,
            &format!("/v0/collections/{collection}/records:batchUpsert"),
            Some(request),
            true,
        )
        .await
    }

    pub async fn delete_record(&self, collection: &str, id: &RecordId) -> Result<Mutation, Error> {
        self.send_json::<(), Mutation>(
            Method::DELETE,
            &format!(
                "/v0/collections/{collection}/records/{}",
                id.path_component()
            ),
            None,
            true,
        )
        .await
    }

    pub async fn upsert_document(
        &self,
        collection: &str,
        id: &RecordId,
        request: &DocumentUpsert,
    ) -> Result<Value, Error> {
        self.send_json(
            Method::PUT,
            &format!(
                "/v0/collections/{collection}/documents/{}",
                id.path_component()
            ),
            Some(request),
            true,
        )
        .await
    }

    pub async fn query(
        &self,
        collection: &str,
        request: &QueryRequest,
    ) -> Result<QueryResponse, Error> {
        self.send_json(
            Method::POST,
            &format!("/v1/collections/{collection}/query"),
            Some(request),
            true,
        )
        .await
    }

    pub async fn get_job(&self, job_id: &str) -> Result<Job, Error> {
        self.send_json::<(), Job>(Method::GET, &format!("/v0/jobs/{job_id}"), None, true)
            .await
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<Job, Error> {
        self.send_json::<(), Job>(
            Method::POST,
            &format!("/v0/jobs/{job_id}/cancel"),
            None,
            false,
        )
        .await
    }

    pub async fn get_embedding_migration(
        &self,
        collection: &str,
    ) -> Result<EmbeddingMigration, Error> {
        self.send_json::<(), EmbeddingMigration>(
            Method::GET,
            &format!("/v0/collections/{collection}/embedding-migration"),
            None,
            true,
        )
        .await
    }

    pub async fn start_embedding_migration(
        &self,
        collection: &str,
        request: &StartEmbeddingMigration,
    ) -> Result<EmbeddingMigration, Error> {
        self.send_json(
            Method::POST,
            &format!("/v0/collections/{collection}/embedding-migration"),
            Some(request),
            false,
        )
        .await
    }

    pub async fn catch_up_embedding_migration(
        &self,
        collection: &str,
    ) -> Result<EmbeddingMigration, Error> {
        self.send_json::<(), EmbeddingMigration>(
            Method::POST,
            &format!("/v0/collections/{collection}/embedding-migration/catch-up"),
            None,
            false,
        )
        .await
    }

    pub async fn start_embedding_migration_catch_up_job(
        &self,
        collection: &str,
    ) -> Result<Job, Error> {
        self.send_json::<(), Job>(
            Method::POST,
            &format!("/v0/collections/{collection}/embedding-migration/catch-up-job"),
            None,
            false,
        )
        .await
    }

    pub async fn activate_embedding_migration(
        &self,
        collection: &str,
    ) -> Result<EmbeddingMigration, Error> {
        self.send_json::<(), EmbeddingMigration>(
            Method::POST,
            &format!("/v0/collections/{collection}/embedding-migration/activate"),
            None,
            false,
        )
        .await
    }

    async fn send_empty(&self, method: Method, path: &str, idempotent: bool) -> Result<(), Error> {
        let response = self.execute(method, path, None::<&()>, idempotent).await?;
        if response.status().is_success() {
            return Ok(());
        }
        Err(parse_error(response).await)
    }

    async fn send_json<B, R>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        idempotent: bool,
    ) -> Result<R, Error>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let response = self.execute(method, path, body, idempotent).await?;
        if !response.status().is_success() {
            return Err(parse_error(response).await);
        }
        response.json().await.map_err(Error::Transport)
    }

    async fn execute<B: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        idempotent: bool,
    ) -> Result<reqwest::Response, Error> {
        let url = format!("{}{}", self.config.base_url, path);
        let attempts = if idempotent {
            self.config.max_retries.saturating_add(1)
        } else {
            1
        };

        for attempt in 0..attempts {
            let mut request = self.http.request(method.clone(), &url);
            if let Some(token) = self.config.bearer_token.as_deref() {
                request = request.bearer_auth(token);
            }
            if let Some(body) = body {
                request = request.json(body);
            }
            match request.send().await {
                Ok(response)
                    if idempotent
                        && attempt + 1 < attempts
                        && is_retryable_status(response.status()) =>
                {
                    tokio::time::sleep(self.config.retry_backoff).await;
                }
                Ok(response) => return Ok(response),
                Err(error) if idempotent && attempt + 1 < attempts => {
                    tokio::time::sleep(self.config.retry_backoff).await;
                    if !error.is_timeout() && !error.is_connect() {
                        return Err(Error::Transport(error));
                    }
                }
                Err(error) => return Err(Error::Transport(error)),
            }
        }
        unreachable!("at least one HTTP attempt is always executed")
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

async fn parse_error(response: reqwest::Response) -> Error {
    let status = response.status();
    match response.json::<ErrorEnvelope>().await {
        Ok(envelope) => Error::Api {
            status,
            code: envelope.error.code,
            message: envelope.error.message,
        },
        Err(_) => Error::Api {
            status,
            code: "http_error".to_string(),
            message: status.to_string(),
        },
    }
}

#[derive(Debug)]
pub enum Error {
    Transport(reqwest::Error),
    Api {
        status: StatusCode,
        code: String,
        message: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "transport error: {error}"),
            Self::Api {
                status,
                code,
                message,
            } => write!(f, "Ketebe API error {status} {code}: {message}"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "value", rename_all = "lowercase")]
pub enum RecordId {
    String(String),
    U64(u64),
}

impl RecordId {
    fn path_component(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::U64(value) => value.to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateCollection {
    pub id: String,
    pub dimension: usize,
    pub metric: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lexical_fields: Option<Vec<Vec<String>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Collection {
    pub id: String,
    pub dimension: usize,
    pub metric: String,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RecordUpsert {
    pub vector: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BatchRecordUpsert {
    pub id: RecordId,
    pub vector: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BatchUpsert {
    pub records: Vec<BatchRecordUpsert>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Mutation {
    pub sequence_number: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DocumentUpsert {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunking: Option<Value>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct QueryRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_k: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dense_candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lexical_candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub explain: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QueryResponse {
    pub api_version: String,
    pub hits: Vec<QueryHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QueryHit {
    pub id: RecordId,
    pub score: f32,
    pub sequence_number: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Job {
    pub id: String,
    pub kind: String,
    pub state: String,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StartEmbeddingMigration {
    pub target_profile: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingMigration {
    #[serde(flatten)]
    pub fields: serde_json::Map<String, Value>,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ApiError,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    code: String,
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_config_debug_redacts_bearer_token() {
        let config =
            ClientConfig::new("http://127.0.0.1:7610").with_bearer_token("super-secret-token");
        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("super-secret-token"));
    }

    #[test]
    fn typed_record_ids_remain_distinct_on_the_wire() {
        let string = serde_json::to_value(RecordId::String("42".into())).unwrap();
        let numeric = serde_json::to_value(RecordId::U64(42)).unwrap();
        assert_ne!(string, numeric);
        assert_eq!(string["type"], "string");
        assert_eq!(numeric["type"], "u64");
    }

    #[test]
    fn openapi_contract_contains_sdk_operations() {
        let spec: Value =
            serde_json::from_str(include_str!("../../../api/openapi/v1.json")).unwrap();
        for (method, path) in [
            ("get", "/v0/collections"),
            ("post", "/v0/collections"),
            ("put", "/v0/collections/{collection_id}/records/{record_id}"),
            (
                "post",
                "/v0/collections/{collection_id}/records:batchUpsert",
            ),
            (
                "put",
                "/v0/collections/{collection_id}/documents/{record_id}",
            ),
            ("post", "/v1/collections/{collection_id}/query"),
            ("get", "/v0/jobs/{job_id}"),
            ("post", "/v0/jobs/{job_id}/cancel"),
            ("get", "/v0/collections/{collection_id}/embedding-migration"),
            (
                "post",
                "/v0/collections/{collection_id}/embedding-migration",
            ),
            (
                "post",
                "/v0/collections/{collection_id}/embedding-migration/catch-up",
            ),
            (
                "post",
                "/v0/collections/{collection_id}/embedding-migration/catch-up-job",
            ),
            (
                "post",
                "/v0/collections/{collection_id}/embedding-migration/activate",
            ),
        ] {
            assert!(
                spec["paths"][path][method].is_object(),
                "missing {method} {path}"
            );
        }
    }
}
