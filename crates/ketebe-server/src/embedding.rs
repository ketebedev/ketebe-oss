use crate::{
    AppState, CollectionService, ManagementError, PendingRecord, SecretRef, SecretResolver,
    SecretResolverHandle, WriteError, WriteService,
};
use ketebe_core::{CollectionId, Metadata, RecordId, SequenceNumber};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

pub const EMBEDDING_METADATA_KEY: &str = "_ketebe_embedding";
const OPENAI_COMPATIBLE_MAX_BATCH_INPUTS: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingModel {
    pub name: String,
    pub version: String,
}

impl EmbeddingModel {
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self, EmbeddingError> {
        let name = name.into();
        let version = version.into();
        if name.trim().is_empty() {
            return Err(EmbeddingError::InvalidProvider(
                "model name must not be empty".to_string(),
            ));
        }
        if version.trim().is_empty() {
            return Err(EmbeddingError::InvalidProvider(
                "model version must not be empty".to_string(),
            ));
        }
        Ok(Self { name, version })
    }
}

pub type EmbeddingFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<f32>, EmbeddingProviderError>> + Send + 'a>>;
pub type EmbeddingBatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>, EmbeddingProviderError>> + Send + 'a>>;

pub trait EmbeddingProvider: Send + Sync {
    fn provider_name(&self) -> &str;
    fn model(&self) -> EmbeddingModel;
    fn fixed_dimension(&self) -> Option<usize> {
        None
    }
    fn max_batch_inputs(&self) -> usize {
        1
    }
    fn max_concurrent_batches(&self) -> usize {
        1
    }
    fn embed<'a>(&'a self, text: &'a str, expected_dimension: usize) -> EmbeddingFuture<'a>;
    fn embed_batch<'a>(
        &'a self,
        texts: &'a [String],
        expected_dimension: usize,
    ) -> EmbeddingBatchFuture<'a> {
        Box::pin(async move {
            let mut vectors = Vec::with_capacity(texts.len());
            for text in texts {
                vectors.push(self.embed(text, expected_dimension).await?);
            }
            Ok(vectors)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingProviderError {
    message: String,
}

impl EmbeddingProviderError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for EmbeddingProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for EmbeddingProviderError {}

pub async fn embed_texts_batched(
    provider: Arc<dyn EmbeddingProvider>,
    texts: &[String],
    expected_dimension: usize,
) -> Result<Vec<Vec<f32>>, EmbeddingProviderError> {
    if texts.is_empty() {
        return Ok(Vec::new());
    }
    let batch_size = provider.max_batch_inputs().max(1);
    let max_parallel = provider.max_concurrent_batches().max(1);
    let batch_count = texts.len().div_ceil(batch_size);
    let limiter = Arc::new(Semaphore::new(max_parallel));
    let mut tasks = JoinSet::new();

    for (batch_index, batch) in texts.chunks(batch_size).enumerate() {
        let provider = provider.clone();
        let batch = batch.to_vec();
        let limiter = limiter.clone();
        tasks.spawn(async move {
            let _permit = limiter.acquire_owned().await.map_err(|_| {
                EmbeddingProviderError::new("embedding batch executor is shutting down")
            })?;
            let started = Instant::now();
            let vectors = provider.embed_batch(&batch, expected_dimension).await?;
            if vectors.len() != batch.len() {
                return Err(EmbeddingProviderError::new(format!(
                    "provider returned {} vectors for {} batch inputs",
                    vectors.len(),
                    batch.len()
                )));
            }
            PROVIDER_BATCH_REQUESTS.fetch_add(1, Ordering::Relaxed);
            PROVIDER_BATCH_INPUTS.fetch_add(batch.len() as u64, Ordering::Relaxed);
            PROVIDER_LAST_BATCH_SIZE.store(batch.len() as u64, Ordering::Relaxed);
            PROVIDER_LAST_BATCH_LATENCY_MICROS.store(
                started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed,
            );
            Ok::<_, EmbeddingProviderError>((batch_index, vectors))
        });
    }

    let mut batches = vec![None; batch_count];
    while let Some(result) = tasks.join_next().await {
        let (batch_index, vectors) = result.map_err(|error| {
            EmbeddingProviderError::new(format!("embedding batch task failed: {error}"))
        })??;
        batches[batch_index] = Some(vectors);
    }

    let mut output = Vec::with_capacity(texts.len());
    for batch in batches {
        output.extend(batch.ok_or_else(|| {
            EmbeddingProviderError::new("embedding batch executor lost a completed batch")
        })?);
    }
    Ok(output)
}

#[derive(Debug, Clone)]
pub struct DeterministicEmbeddingProvider {
    model: EmbeddingModel,
}

impl DeterministicEmbeddingProvider {
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self, EmbeddingError> {
        Ok(Self {
            model: EmbeddingModel::new(name, version)?,
        })
    }
}

impl EmbeddingProvider for DeterministicEmbeddingProvider {
    fn provider_name(&self) -> &str {
        "deterministic"
    }

    fn model(&self) -> EmbeddingModel {
        self.model.clone()
    }

    fn embed<'a>(&'a self, text: &'a str, expected_dimension: usize) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            if expected_dimension == 0 {
                return Err(EmbeddingProviderError::new(
                    "expected dimension must be greater than zero",
                ));
            }
            let bytes = text.as_bytes();
            let mut vector = Vec::with_capacity(expected_dimension);
            for index in 0..expected_dimension {
                let mut hash =
                    0xcbf2_9ce4_8422_2325_u64 ^ (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
                for byte in bytes {
                    hash ^= u64::from(*byte);
                    hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
                }
                let mantissa = ((hash >> 40) & 0x00ff_ffff) as f32 / 16_777_215.0;
                vector.push((mantissa * 2.0) - 1.0);
            }
            if vector.iter().all(|value| *value == 0.0) {
                vector[0] = 1.0;
            }
            Ok(vector)
        })
    }
}

#[derive(Clone, Default)]
pub struct EmbeddingProviderRegistry {
    providers: HashMap<String, Arc<dyn EmbeddingProvider>>,
    default_profile: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EmbeddingProfileInfo {
    pub profile: String,
    pub provider: String,
    pub model: String,
    pub model_version: String,
    pub dimension: Option<usize>,
    pub is_default: bool,
}

impl EmbeddingProviderRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        profile: impl Into<String>,
        provider: Arc<dyn EmbeddingProvider>,
    ) -> Result<(), EmbeddingError> {
        let profile = profile.into();
        if profile.trim().is_empty() {
            return Err(EmbeddingError::InvalidProvider(
                "provider profile name must not be empty".to_string(),
            ));
        }
        if self.providers.contains_key(&profile) {
            return Err(EmbeddingError::InvalidProvider(format!(
                "provider profile '{profile}' is already registered"
            )));
        }
        self.providers.insert(profile, provider);
        Ok(())
    }

    pub fn set_default(&mut self, profile: impl Into<String>) -> Result<(), EmbeddingError> {
        let profile = profile.into();
        if !self.providers.contains_key(&profile) {
            return Err(EmbeddingError::InvalidProvider(format!(
                "provider profile '{profile}' is not registered"
            )));
        }
        self.default_profile = Some(profile);
        Ok(())
    }

    #[must_use]
    pub fn resolve(&self, profile: &str) -> Option<Arc<dyn EmbeddingProvider>> {
        self.providers.get(profile).cloned()
    }

    #[must_use]
    pub fn default_provider(&self) -> Option<Arc<dyn EmbeddingProvider>> {
        self.default_profile
            .as_deref()
            .and_then(|profile| self.resolve(profile))
    }

    #[must_use]
    pub fn profiles(&self) -> Vec<EmbeddingProfileInfo> {
        let mut profiles = self
            .providers
            .iter()
            .map(|(profile, provider)| {
                let model = provider.model();
                EmbeddingProfileInfo {
                    profile: profile.clone(),
                    provider: provider.provider_name().to_string(),
                    model: model.name,
                    model_version: model.version,
                    dimension: provider.fixed_dimension(),
                    is_default: self.default_profile.as_deref() == Some(profile.as_str()),
                }
            })
            .collect::<Vec<_>>();
        profiles.sort_by(|left, right| left.profile.cmp(&right.profile));
        profiles
    }
}

#[derive(Debug, Clone)]
pub struct OpenAiCompatibleEmbeddingConfig {
    pub endpoint: String,
    pub model: String,
    pub model_version: String,
    pub dimension: usize,
    pub api_key_ref: Option<SecretRef>,
    pub timeout: Duration,
    pub max_retries: u32,
    pub retry_backoff: Duration,
    pub max_concurrency: usize,
}

impl OpenAiCompatibleEmbeddingConfig {
    pub fn validate(&self) -> Result<(), EmbeddingError> {
        if self.endpoint.trim().is_empty() {
            return Err(EmbeddingError::InvalidProvider(
                "embedding endpoint must not be empty".to_string(),
            ));
        }
        EmbeddingModel::new(self.model.clone(), self.model_version.clone())?;
        if self.dimension == 0 {
            return Err(EmbeddingError::InvalidProvider(
                "embedding dimension must be greater than zero".to_string(),
            ));
        }
        if self.max_concurrency == 0 {
            return Err(EmbeddingError::InvalidProvider(
                "embedding max_concurrency must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct OpenAiCompatibleEmbeddingProvider {
    config: Arc<OpenAiCompatibleEmbeddingConfig>,
    secret_resolver: SecretResolverHandle,
    client: reqwest::Client,
    concurrency: Arc<Semaphore>,
}

impl OpenAiCompatibleEmbeddingProvider {
    pub fn new(config: OpenAiCompatibleEmbeddingConfig) -> Result<Self, EmbeddingError> {
        Self::new_with_secret_resolver(config, Arc::new(crate::SystemSecretResolver))
    }

    pub fn new_with_secret_resolver(
        config: OpenAiCompatibleEmbeddingConfig,
        secret_resolver: Arc<dyn SecretResolver>,
    ) -> Result<Self, EmbeddingError> {
        config.validate()?;
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|error| EmbeddingError::InvalidProvider(error.to_string()))?;
        let max_concurrency = config.max_concurrency;
        Ok(Self {
            config: Arc::new(config),
            secret_resolver: SecretResolverHandle::new(secret_resolver),
            client,
            concurrency: Arc::new(Semaphore::new(max_concurrency)),
        })
    }

    #[tracing::instrument(
        skip_all,
        name = "ketebe.embedding.provider",
        fields(component = "embedding", provider = "openai-compatible")
    )]
    async fn request_batch(
        &self,
        texts: &[String],
    ) -> Result<Vec<Vec<f32>>, EmbeddingProviderError> {
        let _permit = self
            .concurrency
            .acquire()
            .await
            .map_err(|_| EmbeddingProviderError::new("embedding provider is shutting down"))?;
        let mut attempt = 0_u32;
        loop {
            match self.request_batch_once(texts).await {
                Ok(vectors) => return Ok(vectors),
                Err(error) if attempt < self.config.max_retries => {
                    attempt += 1;
                    PROVIDER_RETRIES.fetch_add(1, Ordering::Relaxed);
                    let multiplier = 1_u32 << attempt.saturating_sub(1).min(10);
                    tokio::time::sleep(self.config.retry_backoff.saturating_mul(multiplier)).await;
                    if error.to_string().contains("non-retryable") {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn request_batch_once(
        &self,
        texts: &[String],
    ) -> Result<Vec<Vec<f32>>, EmbeddingProviderError> {
        let body = OpenAiEmbeddingRequest {
            model: &self.config.model,
            input: texts,
            dimensions: self.config.dimension,
        };
        let mut request = self.client.post(&self.config.endpoint).json(&body);
        let api_key = self
            .config
            .api_key_ref
            .as_ref()
            .map(|reference| self.secret_resolver.resolve(reference))
            .transpose()
            .map_err(|error| {
                EmbeddingProviderError::new(format!(
                    "non-retryable provider credential resolution failed: {error}"
                ))
            })?;
        if let Some(api_key) = api_key.as_ref() {
            request = request.bearer_auth(api_key.expose_secret());
        }
        let response = request.send().await.map_err(|error| {
            EmbeddingProviderError::new(format!("HTTP request failed: {error}"))
        })?;
        let status = response.status();
        if !status.is_success() {
            let retryable = status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error();
            let prefix = if retryable {
                "retryable"
            } else {
                "non-retryable"
            };
            return Err(EmbeddingProviderError::new(format!(
                "{prefix} embedding HTTP status {}",
                status.as_u16()
            )));
        }
        let body: OpenAiEmbeddingResponse = response.json().await.map_err(|error| {
            EmbeddingProviderError::new(format!("invalid embedding JSON: {error}"))
        })?;
        if body.data.len() != texts.len() {
            return Err(EmbeddingProviderError::new(format!(
                "embedding response contains {} vectors for {} inputs",
                body.data.len(),
                texts.len()
            )));
        }
        let mut data = body.data;
        data.sort_by_key(|item| item.index);
        let mut vectors = Vec::with_capacity(data.len());
        for (expected_index, item) in data.into_iter().enumerate() {
            if item.index != expected_index {
                return Err(EmbeddingProviderError::new(format!(
                    "embedding response index mismatch: expected {expected_index}, got {}",
                    item.index
                )));
            }
            vectors.push(item.embedding);
        }
        Ok(vectors)
    }

    fn validate_vectors(
        &self,
        vectors: Vec<Vec<f32>>,
    ) -> Result<Vec<Vec<f32>>, EmbeddingProviderError> {
        for (vector_index, vector) in vectors.iter().enumerate() {
            if vector.len() != self.config.dimension {
                return Err(EmbeddingProviderError::new(format!(
                    "provider returned dimension {} for input {vector_index}, expected {}",
                    vector.len(),
                    self.config.dimension
                )));
            }
            if vector.iter().any(|value| !value.is_finite()) {
                return Err(EmbeddingProviderError::new(format!(
                    "provider returned a non-finite embedding value for input {vector_index}"
                )));
            }
        }
        Ok(vectors)
    }
}

impl EmbeddingProvider for OpenAiCompatibleEmbeddingProvider {
    fn provider_name(&self) -> &str {
        "openai-compatible"
    }

    fn model(&self) -> EmbeddingModel {
        EmbeddingModel {
            name: self.config.model.clone(),
            version: self.config.model_version.clone(),
        }
    }

    fn fixed_dimension(&self) -> Option<usize> {
        Some(self.config.dimension)
    }

    fn max_batch_inputs(&self) -> usize {
        OPENAI_COMPATIBLE_MAX_BATCH_INPUTS
    }

    fn max_concurrent_batches(&self) -> usize {
        self.config.max_concurrency
    }

    fn embed<'a>(&'a self, text: &'a str, expected_dimension: usize) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            if expected_dimension != self.config.dimension {
                return Err(EmbeddingProviderError::new(format!(
                    "configured provider dimension {} does not match collection dimension {expected_dimension}",
                    self.config.dimension
                )));
            }
            let input = vec![text.to_string()];
            let mut vectors = self.validate_vectors(self.request_batch(&input).await?)?;
            vectors
                .pop()
                .ok_or_else(|| EmbeddingProviderError::new("embedding response contains no data"))
        })
    }

    fn embed_batch<'a>(
        &'a self,
        texts: &'a [String],
        expected_dimension: usize,
    ) -> EmbeddingBatchFuture<'a> {
        Box::pin(async move {
            if texts.is_empty() {
                return Ok(Vec::new());
            }
            if expected_dimension != self.config.dimension {
                return Err(EmbeddingProviderError::new(format!(
                    "configured provider dimension {} does not match collection dimension {expected_dimension}",
                    self.config.dimension
                )));
            }
            if texts.len() > OPENAI_COMPATIBLE_MAX_BATCH_INPUTS {
                return Err(EmbeddingProviderError::new(format!(
                    "embedding batch contains {} inputs, maximum is {OPENAI_COMPATIBLE_MAX_BATCH_INPUTS}",
                    texts.len()
                )));
            }
            self.validate_vectors(self.request_batch(texts).await?)
        })
    }
}

#[derive(Serialize)]
struct OpenAiEmbeddingRequest<'a> {
    model: &'a str,
    input: &'a [String],
    dimensions: usize,
}

#[derive(Deserialize)]
struct OpenAiEmbeddingResponse {
    data: Vec<OpenAiEmbeddingData>,
}

#[derive(Deserialize)]
struct OpenAiEmbeddingData {
    index: usize,
    embedding: Vec<f32>,
}

#[derive(Debug, Clone)]
pub struct DocumentRecord {
    pub id: RecordId,
    pub text: String,
    pub metadata: Metadata,
}

#[derive(Debug)]
pub enum EmbeddingError {
    ProviderUnavailable,
    ProviderProfileUnavailable(String),
    InvalidProvider(String),
    EmptyText,
    ReservedMetadata,
    DimensionMismatch { expected: usize, actual: usize },
    NonFiniteVector { index: usize },
    Provider(EmbeddingProviderError),
    Management(ManagementError),
    Write(WriteError),
}

impl fmt::Display for EmbeddingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProviderUnavailable => write!(f, "embedding provider is not configured"),
            Self::ProviderProfileUnavailable(profile) => {
                write!(
                    f,
                    "embedding profile '{profile}' is not available in the runtime registry"
                )
            }
            Self::InvalidProvider(message) => write!(f, "invalid embedding provider: {message}"),
            Self::EmptyText => write!(f, "document text must not be empty"),
            Self::ReservedMetadata => write!(
                f,
                "metadata key '{EMBEDDING_METADATA_KEY}' is reserved by Ketebe"
            ),
            Self::DimensionMismatch { expected, actual } => write!(
                f,
                "embedding dimension mismatch: expected {expected}, got {actual}"
            ),
            Self::NonFiniteVector { index } => {
                write!(f, "embedding contains a non-finite value at index {index}")
            }
            Self::Provider(error) => write!(f, "embedding provider failed: {error}"),
            Self::Management(error) => write!(f, "collection lookup failed: {error}"),
            Self::Write(error) => write!(f, "embedded write failed: {error}"),
        }
    }
}
impl std::error::Error for EmbeddingError {}

#[derive(Default)]
struct EmbeddingCounters {
    requests: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    provider_latency_micros: AtomicU64,
}
static EMBEDDING_COUNTERS: OnceLock<EmbeddingCounters> = OnceLock::new();
static PROVIDER_RETRIES: AtomicU64 = AtomicU64::new(0);
static PROVIDER_BATCH_REQUESTS: AtomicU64 = AtomicU64::new(0);
static PROVIDER_BATCH_INPUTS: AtomicU64 = AtomicU64::new(0);
static PROVIDER_LAST_BATCH_SIZE: AtomicU64 = AtomicU64::new(0);
static PROVIDER_LAST_BATCH_LATENCY_MICROS: AtomicU64 = AtomicU64::new(0);
fn counters() -> &'static EmbeddingCounters {
    EMBEDDING_COUNTERS.get_or_init(EmbeddingCounters::default)
}

#[must_use]
pub fn embedding_prometheus_metrics() -> String {
    let counters = counters();
    format!(
        concat!(
            "ketebe_embedding_requests_total {}\n",
            "ketebe_embedding_successes_total {}\n",
            "ketebe_embedding_failures_total {}\n",
            "ketebe_embedding_provider_retries_total {}\n",
            "ketebe_embedding_provider_batch_requests_total {}\n",
            "ketebe_embedding_provider_batch_inputs_total {}\n",
            "ketebe_embedding_provider_last_batch_size {}\n",
            "ketebe_embedding_provider_last_batch_latency_microseconds {}\n",
            "ketebe_embedding_last_provider_latency_microseconds {}\n"
        ),
        counters.requests.load(Ordering::Relaxed),
        counters.successes.load(Ordering::Relaxed),
        counters.failures.load(Ordering::Relaxed),
        PROVIDER_RETRIES.load(Ordering::Relaxed),
        PROVIDER_BATCH_REQUESTS.load(Ordering::Relaxed),
        PROVIDER_BATCH_INPUTS.load(Ordering::Relaxed),
        PROVIDER_LAST_BATCH_SIZE.load(Ordering::Relaxed),
        PROVIDER_LAST_BATCH_LATENCY_MICROS.load(Ordering::Relaxed),
        counters.provider_latency_micros.load(Ordering::Relaxed),
    )
}

#[derive(Clone)]
pub struct EmbeddingService {
    state: AppState,
    write: WriteService,
    provider: Arc<dyn EmbeddingProvider>,
}

impl EmbeddingService {
    #[must_use]
    pub fn new(state: AppState, provider: Arc<dyn EmbeddingProvider>) -> Self {
        Self {
            write: WriteService::new(state.clone()),
            state,
            provider,
        }
    }

    pub async fn from_state(state: AppState) -> Result<Self, EmbeddingError> {
        let provider = state
            .embedding_provider()
            .await
            .ok_or(EmbeddingError::ProviderUnavailable)?;
        Ok(Self::new(state, provider))
    }

    pub async fn from_state_for_collection(
        state: AppState,
        collection_id: &CollectionId,
    ) -> Result<Self, EmbeddingError> {
        let collection = CollectionService::new(state.clone())
            .get(collection_id)
            .await
            .map_err(EmbeddingError::Management)?;
        let provider = if let Some(ingestion) = collection.ingestion.as_ref() {
            state
                .embedding_provider_profile(ingestion.embedding_profile())
                .await
                .ok_or_else(|| {
                    EmbeddingError::ProviderProfileUnavailable(
                        ingestion.embedding_profile().to_string(),
                    )
                })?
        } else {
            state
                .embedding_provider()
                .await
                .ok_or(EmbeddingError::ProviderUnavailable)?
        };
        Ok(Self::new(state, provider))
    }

    pub async fn embed_and_upsert(
        &self,
        collection_id: &CollectionId,
        mut document: DocumentRecord,
    ) -> Result<SequenceNumber, EmbeddingError> {
        counters().requests.fetch_add(1, Ordering::Relaxed);
        let result = self
            .embed_and_upsert_inner(collection_id, &mut document)
            .await;
        match &result {
            Ok(_) => {
                counters().successes.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                counters().failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    async fn embed_and_upsert_inner(
        &self,
        collection_id: &CollectionId,
        document: &mut DocumentRecord,
    ) -> Result<SequenceNumber, EmbeddingError> {
        if document.text.trim().is_empty() {
            return Err(EmbeddingError::EmptyText);
        }
        if document.metadata.contains_key(EMBEDDING_METADATA_KEY) {
            return Err(EmbeddingError::ReservedMetadata);
        }
        let collection = CollectionService::new(self.state.clone())
            .get(collection_id)
            .await
            .map_err(EmbeddingError::Management)?;
        let profile = collection
            .ingestion
            .as_ref()
            .map(|ingestion| ingestion.embedding_profile())
            .unwrap_or("default");
        let started = Instant::now();
        let mut vectors = crate::embedding_cache::embed_texts_cached(
            self.state.embedding_cache(),
            profile,
            self.provider.clone(),
            std::slice::from_ref(&document.text),
            collection.dimension,
        )
        .await
        .map_err(EmbeddingError::Provider)?;
        let vector = vectors.pop().ok_or_else(|| {
            EmbeddingError::Provider(EmbeddingProviderError::new(
                "embedding cache returned no vector",
            ))
        })?;
        counters().provider_latency_micros.store(
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        if vector.len() != collection.dimension {
            return Err(EmbeddingError::DimensionMismatch {
                expected: collection.dimension,
                actual: vector.len(),
            });
        }
        if let Some((index, _)) = vector
            .iter()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(EmbeddingError::NonFiniteVector { index });
        }
        crate::embedding_migration::set_embedding_provenance(
            &mut document.metadata,
            profile,
            self.provider.as_ref(),
            collection.dimension,
            &document.text,
        );
        self.write
            .upsert(
                collection_id,
                PendingRecord {
                    id: document.id.clone(),
                    vector,
                    metadata: document.metadata.clone(),
                },
            )
            .await
            .map_err(EmbeddingError::Write)
    }
}
