use crate::{SecretRef, SecretResolver, SecretResolverHandle};
use ketebe_core::{FieldPath, Metadata, MetadataValue, RecordId};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

pub type RerankFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<RerankScore>, RerankerError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq)]
pub struct RerankCandidate {
    pub id: RecordId,
    pub text: String,
    pub metadata: Metadata,
    pub retrieval_score: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RerankScore {
    pub index: usize,
    pub score: f32,
}

pub trait Reranker: Send + Sync {
    fn name(&self) -> &str;
    fn rerank<'a>(&'a self, query: &'a str, candidates: &'a [RerankCandidate]) -> RerankFuture<'a>;
}

#[derive(Clone, Default)]
pub struct RerankerRegistry {
    rerankers: HashMap<String, Arc<dyn Reranker>>,
    default_profile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RerankerProfileInfo {
    pub profile: String,
    pub provider: String,
    pub is_default: bool,
}

impl RerankerRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        profile: impl Into<String>,
        reranker: Arc<dyn Reranker>,
    ) -> Result<(), RerankingError> {
        let profile = profile.into();
        if profile.trim().is_empty() {
            return Err(RerankingError::InvalidConfig(
                "reranker profile name must not be empty".into(),
            ));
        }
        if self.rerankers.contains_key(&profile) {
            return Err(RerankingError::InvalidConfig(format!(
                "reranker profile '{profile}' is already registered"
            )));
        }
        self.rerankers.insert(profile, reranker);
        Ok(())
    }

    pub fn set_default(&mut self, profile: impl Into<String>) -> Result<(), RerankingError> {
        let profile = profile.into();
        if !self.rerankers.contains_key(&profile) {
            return Err(RerankingError::InvalidConfig(format!(
                "reranker profile '{profile}' is not registered"
            )));
        }
        self.default_profile = Some(profile);
        Ok(())
    }

    #[must_use]
    pub fn resolve(&self, profile: &str) -> Option<Arc<dyn Reranker>> {
        self.rerankers.get(profile).cloned()
    }

    #[must_use]
    pub fn default_reranker(&self) -> Option<Arc<dyn Reranker>> {
        self.default_profile
            .as_deref()
            .and_then(|profile| self.resolve(profile))
    }

    #[must_use]
    pub fn profiles(&self) -> Vec<RerankerProfileInfo> {
        let mut profiles = self
            .rerankers
            .iter()
            .map(|(profile, reranker)| RerankerProfileInfo {
                profile: profile.clone(),
                provider: reranker.name().to_string(),
                is_default: self.default_profile.as_deref() == Some(profile.as_str()),
            })
            .collect::<Vec<_>>();
        profiles.sort_by(|left, right| left.profile.cmp(&right.profile));
        profiles
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RerankFailurePolicy {
    Fail,
    PreserveCandidateOrder,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RerankedCandidate {
    pub candidate: RerankCandidate,
    pub rerank_score: Option<f32>,
    pub original_rank: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerankExplain {
    pub provider: String,
    pub input_candidates: usize,
    pub output_candidates: usize,
    pub applied: bool,
    pub fallback_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RerankResult {
    pub candidates: Vec<RerankedCandidate>,
    pub explain: RerankExplain,
}

#[derive(Debug)]
pub enum RerankingError {
    InvalidConfig(String),
    InvalidRequest(String),
    Provider(RerankerError),
}

impl fmt::Display for RerankingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid reranking config: {message}"),
            Self::InvalidRequest(message) => write!(f, "invalid reranking request: {message}"),
            Self::Provider(error) => write!(f, "reranker failed: {error}"),
        }
    }
}
impl std::error::Error for RerankingError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerankerError {
    message: String,
    retryable: bool,
}

impl RerankerError {
    pub fn new(message: impl Into<String>, retryable: bool) -> Self {
        Self {
            message: message.into(),
            retryable,
        }
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }
}

impl fmt::Display for RerankerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for RerankerError {}

#[derive(Clone)]
pub struct RerankingService {
    reranker: Arc<dyn Reranker>,
    failure_policy: RerankFailurePolicy,
}

impl RerankingService {
    #[must_use]
    pub fn new(reranker: Arc<dyn Reranker>, failure_policy: RerankFailurePolicy) -> Self {
        Self {
            reranker,
            failure_policy,
        }
    }

    pub async fn rerank(
        &self,
        query: &str,
        candidates: Vec<RerankCandidate>,
        final_top_k: usize,
    ) -> Result<RerankResult, RerankingError> {
        if query.trim().is_empty() {
            return Err(RerankingError::InvalidRequest(
                "reranking query must not be empty".into(),
            ));
        }
        if final_top_k == 0 {
            return Err(RerankingError::InvalidRequest(
                "final_top_k must be greater than zero".into(),
            ));
        }
        if candidates.is_empty() {
            return Ok(RerankResult {
                candidates: Vec::new(),
                explain: RerankExplain {
                    provider: self.reranker.name().to_string(),
                    input_candidates: 0,
                    output_candidates: 0,
                    applied: false,
                    fallback_reason: None,
                },
            });
        }
        if candidates
            .iter()
            .any(|candidate| candidate.text.trim().is_empty())
        {
            return Err(RerankingError::InvalidRequest(
                "reranking candidates must have non-empty projected text".into(),
            ));
        }

        let scores = match self.reranker.rerank(query, &candidates).await {
            Ok(scores) => scores,
            Err(error) => match self.failure_policy {
                RerankFailurePolicy::Fail => return Err(RerankingError::Provider(error)),
                RerankFailurePolicy::PreserveCandidateOrder => {
                    let input_candidates = candidates.len();
                    let output_candidates = input_candidates.min(final_top_k);
                    return Ok(RerankResult {
                        candidates: candidates
                            .into_iter()
                            .take(final_top_k)
                            .enumerate()
                            .map(|(index, candidate)| RerankedCandidate {
                                candidate,
                                rerank_score: None,
                                original_rank: index + 1,
                            })
                            .collect(),
                        explain: RerankExplain {
                            provider: self.reranker.name().to_string(),
                            input_candidates,
                            output_candidates,
                            applied: false,
                            fallback_reason: Some(error.to_string()),
                        },
                    });
                }
            },
        };

        validate_scores(&scores, candidates.len())?;
        let input_candidates = candidates.len();
        let mut scored = scores
            .into_iter()
            .map(|score| {
                let original_rank = score.index + 1;
                RerankedCandidate {
                    candidate: candidates[score.index].clone(),
                    rerank_score: Some(score.score),
                    original_rank,
                }
            })
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            right
                .rerank_score
                .partial_cmp(&left.rerank_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.original_rank.cmp(&right.original_rank))
        });
        scored.truncate(final_top_k);
        Ok(RerankResult {
            explain: RerankExplain {
                provider: self.reranker.name().to_string(),
                input_candidates,
                output_candidates: scored.len(),
                applied: true,
                fallback_reason: None,
            },
            candidates: scored,
        })
    }
}

fn validate_scores(scores: &[RerankScore], candidate_count: usize) -> Result<(), RerankingError> {
    if scores.len() != candidate_count {
        return Err(RerankingError::Provider(RerankerError::new(
            format!(
                "reranker returned {} scores for {candidate_count} candidates",
                scores.len()
            ),
            false,
        )));
    }
    let mut seen = vec![false; candidate_count];
    for score in scores {
        if score.index >= candidate_count {
            return Err(RerankingError::Provider(RerankerError::new(
                format!(
                    "reranker returned out-of-range candidate index {}",
                    score.index
                ),
                false,
            )));
        }
        if seen[score.index] {
            return Err(RerankingError::Provider(RerankerError::new(
                format!(
                    "reranker returned duplicate candidate index {}",
                    score.index
                ),
                false,
            )));
        }
        if !score.score.is_finite() {
            return Err(RerankingError::Provider(RerankerError::new(
                format!(
                    "reranker returned non-finite score for candidate {}",
                    score.index
                ),
                false,
            )));
        }
        seen[score.index] = true;
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct CandidateProjection {
    pub text_fields: Vec<FieldPath>,
    pub include_metadata: bool,
}

impl CandidateProjection {
    pub fn project(
        &self,
        id: RecordId,
        metadata: &Metadata,
        retrieval_score: f32,
    ) -> Result<RerankCandidate, RerankingError> {
        if self.text_fields.is_empty() {
            return Err(RerankingError::InvalidConfig(
                "candidate projection requires at least one text field".into(),
            ));
        }
        let mut parts = Vec::new();
        for field in &self.text_fields {
            if let Some(value) = metadata_value_at_path(metadata, field)
                && let Some(text) = metadata_value_text(value)
                && !text.trim().is_empty()
            {
                parts.push(text);
            }
        }
        if parts.is_empty() {
            return Err(RerankingError::InvalidRequest(
                "candidate projection produced empty text".into(),
            ));
        }
        Ok(RerankCandidate {
            id,
            text: parts.join("\n"),
            metadata: if self.include_metadata {
                metadata.clone()
            } else {
                Metadata::new()
            },
            retrieval_score,
        })
    }
}

fn metadata_value_at_path<'a>(
    metadata: &'a Metadata,
    field: &FieldPath,
) -> Option<&'a MetadataValue> {
    let mut segments = field.segments().iter();
    let first = segments.next()?;
    let mut current = metadata.get(first)?;
    for segment in segments {
        match current {
            MetadataValue::Object(object) => current = object.get(segment)?,
            _ => return None,
        }
    }
    Some(current)
}

fn metadata_value_text(value: &MetadataValue) -> Option<String> {
    match value {
        MetadataValue::String(value) => Some(value.clone()),
        MetadataValue::Bool(value) => Some(value.to_string()),
        MetadataValue::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct HttpRerankerConfig {
    pub endpoint: String,
    pub model: Option<String>,
    pub api_key_ref: Option<SecretRef>,
    pub timeout: Duration,
    pub max_retries: u32,
    pub retry_backoff: Duration,
    pub max_concurrency: usize,
}

impl HttpRerankerConfig {
    pub fn validate(&self) -> Result<(), RerankingError> {
        if self.endpoint.trim().is_empty() {
            return Err(RerankingError::InvalidConfig(
                "reranker endpoint must not be empty".into(),
            ));
        }
        if self.max_concurrency == 0 {
            return Err(RerankingError::InvalidConfig(
                "reranker max_concurrency must be greater than zero".into(),
            ));
        }
        if self.timeout.is_zero() {
            return Err(RerankingError::InvalidConfig(
                "reranker timeout must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct HttpReranker {
    config: Arc<HttpRerankerConfig>,
    secret_resolver: SecretResolverHandle,
    client: reqwest::Client,
    concurrency: Arc<Semaphore>,
}

impl HttpReranker {
    pub fn new(config: HttpRerankerConfig) -> Result<Self, RerankingError> {
        Self::new_with_secret_resolver(config, Arc::new(crate::SystemSecretResolver))
    }

    pub fn new_with_secret_resolver(
        config: HttpRerankerConfig,
        secret_resolver: Arc<dyn SecretResolver>,
    ) -> Result<Self, RerankingError> {
        config.validate()?;
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|error| RerankingError::InvalidConfig(error.to_string()))?;
        let concurrency = Arc::new(Semaphore::new(config.max_concurrency));
        Ok(Self {
            config: Arc::new(config),
            secret_resolver: SecretResolverHandle::new(secret_resolver),
            client,
            concurrency,
        })
    }

    async fn request(
        &self,
        query: &str,
        candidates: &[RerankCandidate],
    ) -> Result<Vec<RerankScore>, RerankerError> {
        let _permit = self
            .concurrency
            .acquire()
            .await
            .map_err(|_| RerankerError::new("reranker is shutting down", true))?;
        let mut attempt = 0_u32;
        loop {
            match self.request_once(query, candidates).await {
                Ok(scores) => return Ok(scores),
                Err(error) if error.retryable() && attempt < self.config.max_retries => {
                    attempt += 1;
                    let multiplier = 1_u32 << attempt.saturating_sub(1).min(10);
                    tokio::time::sleep(self.config.retry_backoff.saturating_mul(multiplier)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn request_once(
        &self,
        query: &str,
        candidates: &[RerankCandidate],
    ) -> Result<Vec<RerankScore>, RerankerError> {
        let body = HttpRerankRequest {
            model: self.config.model.as_deref(),
            query,
            candidates: candidates
                .iter()
                .enumerate()
                .map(|(index, candidate)| HttpRerankCandidate {
                    index,
                    text: &candidate.text,
                    metadata: metadata_json(&candidate.metadata),
                })
                .collect(),
        };
        let mut request = self.client.post(&self.config.endpoint).json(&body);
        let api_key = self
            .config
            .api_key_ref
            .as_ref()
            .map(|reference| self.secret_resolver.resolve(reference))
            .transpose()
            .map_err(|error| {
                RerankerError::new(
                    format!("provider credential resolution failed: {error}"),
                    false,
                )
            })?;
        if let Some(api_key) = api_key.as_ref() {
            request = request.bearer_auth(api_key.expose_secret());
        }
        let response = request
            .send()
            .await
            .map_err(|error| RerankerError::new(format!("HTTP request failed: {error}"), true))?;
        let status = response.status();
        if !status.is_success() {
            let retryable = status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error();
            return Err(RerankerError::new(
                format!("reranker HTTP status {}", status.as_u16()),
                retryable,
            ));
        }
        let response: HttpRerankResponse = response.json().await.map_err(|error| {
            RerankerError::new(format!("invalid reranker JSON: {error}"), false)
        })?;
        Ok(response.into_scores())
    }
}

impl Reranker for HttpReranker {
    fn name(&self) -> &str {
        "http"
    }

    fn rerank<'a>(&'a self, query: &'a str, candidates: &'a [RerankCandidate]) -> RerankFuture<'a> {
        Box::pin(async move { self.request(query, candidates).await })
    }
}

#[derive(Serialize)]
struct HttpRerankRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<&'a str>,
    query: &'a str,
    candidates: Vec<HttpRerankCandidate<'a>>,
}

#[derive(Serialize)]
struct HttpRerankCandidate<'a> {
    index: usize,
    text: &'a str,
    metadata: serde_json::Value,
}

fn metadata_json(metadata: &Metadata) -> serde_json::Value {
    serde_json::Value::Object(
        metadata
            .iter()
            .map(|(key, value)| (key.clone(), metadata_value_json(value)))
            .collect(),
    )
}

fn metadata_value_json(value: &MetadataValue) -> serde_json::Value {
    match value {
        MetadataValue::Null => serde_json::Value::Null,
        MetadataValue::Bool(value) => serde_json::Value::Bool(*value),
        MetadataValue::Number(value) => serde_json::Number::from_f64(*value)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        MetadataValue::String(value) => serde_json::Value::String(value.clone()),
        MetadataValue::Array(values) => {
            serde_json::Value::Array(values.iter().map(metadata_value_json).collect())
        }
        MetadataValue::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), metadata_value_json(value)))
                .collect(),
        ),
    }
}

#[derive(Deserialize)]
struct HttpRerankResponse {
    results: Vec<RerankScoreDto>,
}

#[derive(Deserialize)]
struct RerankScoreDto {
    index: usize,
    score: f32,
}

impl From<RerankScoreDto> for RerankScore {
    fn from(value: RerankScoreDto) -> Self {
        Self {
            index: value.index,
            score: value.score,
        }
    }
}

impl HttpRerankResponse {
    fn into_scores(self) -> Vec<RerankScore> {
        self.results.into_iter().map(Into::into).collect()
    }
}
