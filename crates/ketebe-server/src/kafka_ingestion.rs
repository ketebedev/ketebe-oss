use crate::provenance::{DocumentSourceDto, apply_document_provenance};
use crate::{
    AppState, ChunkedDocument, ChunkingConfig, ChunkingError, ChunkingService, CollectionService,
    DocumentRecord, EmbeddingError, EmbeddingService, PendingRecord, SemanticChunkedDocument,
    SemanticChunkingError, SemanticChunkingService, TokenChunkedDocument, TokenChunkingError,
    TokenChunkingService, WriteError, WriteService,
};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use ketebe_core::{CollectionId, Metadata, MetadataValue, RecordId};
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::message::{Headers as _, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, Message};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use tracing::Instrument as _;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KafkaPoisonPolicy {
    Block,
    Dlq { topic: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaSecurityConfig {
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaIngestionConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub collection_id: CollectionId,
    pub batch_max_records: usize,
    pub batch_linger_ms: u64,
    pub poison_policy: KafkaPoisonPolicy,
    pub security: Option<KafkaSecurityConfig>,
}

impl KafkaIngestionConfig {
    pub fn new(
        brokers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
        collection_id: CollectionId,
        batch_max_records: usize,
        batch_linger_ms: u64,
    ) -> Result<Self, KafkaIngestionError> {
        let brokers = brokers.into();
        let topic = topic.into();
        let group_id = group_id.into();
        if brokers.trim().is_empty() {
            return Err(KafkaIngestionError::InvalidConfig(
                "brokers must not be empty".to_string(),
            ));
        }
        if topic.trim().is_empty() {
            return Err(KafkaIngestionError::InvalidConfig(
                "topic must not be empty".to_string(),
            ));
        }
        if group_id.trim().is_empty() {
            return Err(KafkaIngestionError::InvalidConfig(
                "group_id must not be empty".to_string(),
            ));
        }
        if batch_max_records == 0 {
            return Err(KafkaIngestionError::InvalidConfig(
                "batch_max_records must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            brokers,
            topic,
            group_id,
            collection_id,
            batch_max_records,
            batch_linger_ms,
            poison_policy: KafkaPoisonPolicy::Block,
            security: None,
        })
    }

    #[must_use]
    pub fn with_dlq_topic(mut self, topic: impl Into<String>) -> Self {
        self.poison_policy = KafkaPoisonPolicy::Dlq {
            topic: topic.into(),
        };
        self
    }

    #[must_use]
    pub fn with_security(mut self, security: KafkaSecurityConfig) -> Self {
        self.security = Some(security);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaIngestionState {
    Starting,
    Running,
    Rebalancing,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaIngestionStats {
    pub state: KafkaIngestionState,
    pub received_records: u64,
    pub applied_records: u64,
    pub committed_batches: u64,
    pub decode_failures: u64,
    pub write_failures: u64,
    pub commit_failures: u64,
    pub dlq_records: u64,
    pub dlq_failures: u64,
    pub rebalance_count: u64,
    pub assigned_partitions: u64,
    pub buffered_records: u64,
    pub last_commit_latency_micros: u64,
    pub consumer_lag_records: u64,
}

#[derive(Default)]
struct Counters {
    received_records: AtomicU64,
    applied_records: AtomicU64,
    committed_batches: AtomicU64,
    decode_failures: AtomicU64,
    write_failures: AtomicU64,
    commit_failures: AtomicU64,
    dlq_records: AtomicU64,
    dlq_failures: AtomicU64,
    rebalance_count: AtomicU64,
    assigned_partitions: AtomicU64,
    buffered_records: AtomicU64,
    last_commit_latency_micros: AtomicU64,
    consumer_lag_records: AtomicU64,
}

struct GlobalObservability {
    counters: Weak<Counters>,
    state: Weak<Mutex<KafkaIngestionState>>,
}

static GLOBAL_OBSERVABILITY: OnceLock<Mutex<Option<GlobalObservability>>> = OnceLock::new();

#[derive(Clone)]
pub struct KafkaIngestionService {
    app_state: AppState,
    write: WriteService,
    counters: Arc<Counters>,
    state: Arc<Mutex<KafkaIngestionState>>,
}

impl KafkaIngestionService {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self {
            app_state: state.clone(),
            write: WriteService::new(state),
            counters: Arc::new(Counters::default()),
            state: Arc::new(Mutex::new(KafkaIngestionState::Starting)),
        }
    }

    #[must_use]
    pub fn stats(&self) -> KafkaIngestionStats {
        stats_from(&self.counters, &self.state)
    }

    pub async fn apply_partition_batch(
        &self,
        collection_id: &CollectionId,
        messages: &[KafkaIngestionMessage],
    ) -> Result<KafkaBatchAck, KafkaIngestionError> {
        validate_partition_batch(messages)?;
        self.counters
            .received_records
            .fetch_add(messages.len() as u64, Ordering::Relaxed);

        let mut decoded = Vec::with_capacity(messages.len());
        for message in messages {
            match decode_envelope(&message.payload) {
                Ok(value) => decoded.push(value),
                Err(error) => {
                    self.counters
                        .decode_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(error);
                }
            }
        }

        for mutation in decoded {
            let result: Result<(), KafkaIngestionError> = match mutation {
                KafkaMutation::Upsert(record) => self
                    .write
                    .upsert(collection_id, record)
                    .await
                    .map(|_| ())
                    .map_err(KafkaIngestionError::Write),
                KafkaMutation::Delete(record_id) => self
                    .write
                    .delete(collection_id, record_id)
                    .await
                    .map(|_| ())
                    .map_err(KafkaIngestionError::Write),
                KafkaMutation::Document(document) => {
                    let collection = CollectionService::new(self.app_state.clone())
                        .get(collection_id)
                        .await
                        .map_err(|error| {
                            KafkaIngestionError::Embedding(EmbeddingError::Management(error))
                        });
                    match collection {
                        Ok(collection) => {
                            if let Some(chunking) = collection
                                .ingestion
                                .as_ref()
                                .and_then(|v| v.semantic_chunking())
                            {
                                SemanticChunkingService::new(self.app_state.clone())
                                    .chunk_embed_and_upsert(
                                        collection_id,
                                        SemanticChunkedDocument {
                                            id: document.id,
                                            text: document.text,
                                            metadata: document.metadata,
                                            chunking,
                                        },
                                    )
                                    .await
                                    .map(|_| ())
                                    .map_err(KafkaIngestionError::SemanticChunking)
                            } else if let Some(chunking) = collection
                                .ingestion
                                .as_ref()
                                .and_then(|v| v.token_chunking())
                            {
                                TokenChunkingService::new(self.app_state.clone())
                                    .chunk_embed_and_upsert(
                                        collection_id,
                                        TokenChunkedDocument {
                                            id: document.id,
                                            text: document.text,
                                            metadata: document.metadata,
                                            chunking,
                                        },
                                    )
                                    .await
                                    .map(|_| ())
                                    .map_err(KafkaIngestionError::TokenChunking)
                            } else if let Some(chunking) = collection
                                .ingestion
                                .as_ref()
                                .and_then(|v| v.chunking())
                                .map(ChunkingConfig::from)
                            {
                                ChunkingService::new(self.app_state.clone())
                                    .chunk_embed_and_upsert(
                                        collection_id,
                                        ChunkedDocument {
                                            id: document.id,
                                            text: document.text,
                                            metadata: document.metadata,
                                            chunking,
                                        },
                                    )
                                    .await
                                    .map(|_| ())
                                    .map_err(KafkaIngestionError::Chunking)
                            } else {
                                match EmbeddingService::from_state_for_collection(
                                    self.app_state.clone(),
                                    collection_id,
                                )
                                .await
                                {
                                    Ok(service) => service
                                        .embed_and_upsert(collection_id, document)
                                        .await
                                        .map(|_| ())
                                        .map_err(KafkaIngestionError::Embedding),
                                    Err(error) => Err(KafkaIngestionError::Embedding(error)),
                                }
                            }
                        }
                        Err(error) => Err(error),
                    }
                }
                KafkaMutation::ChunkedDocument(document) => {
                    ChunkingService::new(self.app_state.clone())
                        .chunk_embed_and_upsert(collection_id, document)
                        .await
                        .map(|_| ())
                        .map_err(KafkaIngestionError::Chunking)
                }
            };
            if let Err(error) = result {
                self.counters.write_failures.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
            self.counters
                .applied_records
                .fetch_add(1, Ordering::Relaxed);
        }

        Ok(KafkaBatchAck {
            partition: messages[0].partition,
            next_offset: messages.last().expect("non-empty batch").offset + 1,
            applied_records: messages.len(),
        })
    }

    fn mark_state(&self, state: KafkaIngestionState) {
        if let Ok(mut current) = self.state.lock() {
            *current = state;
        }
    }
}

fn stats_from(counters: &Counters, state: &Mutex<KafkaIngestionState>) -> KafkaIngestionStats {
    KafkaIngestionStats {
        state: state
            .lock()
            .map(|value| *value)
            .unwrap_or(KafkaIngestionState::Failed),
        received_records: counters.received_records.load(Ordering::Relaxed),
        applied_records: counters.applied_records.load(Ordering::Relaxed),
        committed_batches: counters.committed_batches.load(Ordering::Relaxed),
        decode_failures: counters.decode_failures.load(Ordering::Relaxed),
        write_failures: counters.write_failures.load(Ordering::Relaxed),
        commit_failures: counters.commit_failures.load(Ordering::Relaxed),
        dlq_records: counters.dlq_records.load(Ordering::Relaxed),
        dlq_failures: counters.dlq_failures.load(Ordering::Relaxed),
        rebalance_count: counters.rebalance_count.load(Ordering::Relaxed),
        assigned_partitions: counters.assigned_partitions.load(Ordering::Relaxed),
        buffered_records: counters.buffered_records.load(Ordering::Relaxed),
        last_commit_latency_micros: counters.last_commit_latency_micros.load(Ordering::Relaxed),
        consumer_lag_records: counters.consumer_lag_records.load(Ordering::Relaxed),
    }
}

fn install_global_observability(service: &KafkaIngestionService) {
    let registry = GLOBAL_OBSERVABILITY.get_or_init(|| Mutex::new(None));
    if let Ok(mut current) = registry.lock() {
        *current = Some(GlobalObservability {
            counters: Arc::downgrade(&service.counters),
            state: Arc::downgrade(&service.state),
        });
    }
}

#[must_use]
pub fn kafka_prometheus_metrics() -> String {
    let Some(registry) = GLOBAL_OBSERVABILITY.get() else {
        return "ketebe_kafka_ingestion_enabled 0\n".to_string();
    };
    let Ok(current) = registry.lock() else {
        return "ketebe_kafka_ingestion_enabled 0\n".to_string();
    };
    let Some(current) = current.as_ref() else {
        return "ketebe_kafka_ingestion_enabled 0\n".to_string();
    };
    let (Some(counters), Some(state)) = (current.counters.upgrade(), current.state.upgrade())
    else {
        return "ketebe_kafka_ingestion_enabled 0\n".to_string();
    };
    let stats = stats_from(&counters, &state);
    let state_value = match stats.state {
        KafkaIngestionState::Starting => 1,
        KafkaIngestionState::Running => 2,
        KafkaIngestionState::Rebalancing => 3,
        KafkaIngestionState::Failed => 4,
        KafkaIngestionState::Stopped => 5,
    };
    format!(
        concat!(
            "ketebe_kafka_ingestion_enabled 1\n",
            "ketebe_kafka_ingestion_state {}\n",
            "ketebe_kafka_received_records_total {}\n",
            "ketebe_kafka_applied_records_total {}\n",
            "ketebe_kafka_committed_batches_total {}\n",
            "ketebe_kafka_decode_failures_total {}\n",
            "ketebe_kafka_write_failures_total {}\n",
            "ketebe_kafka_commit_failures_total {}\n",
            "ketebe_kafka_dlq_records_total {}\n",
            "ketebe_kafka_dlq_failures_total {}\n",
            "ketebe_kafka_rebalances_total {}\n",
            "ketebe_kafka_assigned_partitions {}\n",
            "ketebe_kafka_buffered_records {}\n",
            "ketebe_kafka_last_commit_latency_microseconds {}\n",
            "ketebe_kafka_consumer_lag_records {}\n"
        ),
        state_value,
        stats.received_records,
        stats.applied_records,
        stats.committed_batches,
        stats.decode_failures,
        stats.write_failures,
        stats.commit_failures,
        stats.dlq_records,
        stats.dlq_failures,
        stats.rebalance_count,
        stats.assigned_partitions,
        stats.buffered_records,
        stats.last_commit_latency_micros,
        stats.consumer_lag_records,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaIngestionMessage {
    pub partition: i32,
    pub offset: i64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaBatchAck {
    pub partition: i32,
    pub next_offset: i64,
    pub applied_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaDlqEnvelope {
    pub version: u32,
    pub source_topic: String,
    pub source_partition: i32,
    pub source_offset: i64,
    pub target_collection: String,
    pub error_class: String,
    pub error_message: String,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum KafkaIngestionError {
    InvalidConfig(String),
    EmptyBatch,
    MixedPartitions,
    NonMonotonicOffsets,
    MissingPayload,
    UnsupportedEnvelopeVersion(u32),
    Decode(serde_json::Error),
    InvalidEnvelope(String),
    Write(WriteError),
    Embedding(EmbeddingError),
    Chunking(ChunkingError),
    TokenChunking(TokenChunkingError),
    SemanticChunking(SemanticChunkingError),
    Dlq(String),
    Kafka(rdkafka::error::KafkaError),
}

impl fmt::Display for KafkaIngestionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid Kafka ingestion config: {message}"),
            Self::EmptyBatch => write!(f, "Kafka ingestion batch must not be empty"),
            Self::MixedPartitions => write!(f, "Kafka ingestion batch mixes partitions"),
            Self::NonMonotonicOffsets => {
                write!(f, "Kafka ingestion offsets are not strictly increasing")
            }
            Self::MissingPayload => write!(f, "Kafka record payload is missing"),
            Self::UnsupportedEnvelopeVersion(version) => {
                write!(f, "unsupported Kafka envelope version {version}")
            }
            Self::Decode(error) => write!(f, "invalid Kafka envelope JSON: {error}"),
            Self::InvalidEnvelope(message) => write!(f, "invalid Kafka envelope: {message}"),
            Self::Write(error) => write!(f, "Ketebe write failed: {error}"),
            Self::Embedding(error) => write!(f, "Ketebe embedding failed: {error}"),
            Self::Chunking(error) => write!(f, "Ketebe chunking failed: {error}"),
            Self::TokenChunking(error) => write!(f, "Ketebe token chunking failed: {error}"),
            Self::SemanticChunking(error) => write!(f, "Ketebe semantic chunking failed: {error}"),
            Self::Dlq(message) => write!(f, "Kafka DLQ publish failed: {message}"),
            Self::Kafka(error) => write!(f, "Kafka error: {error}"),
        }
    }
}

impl std::error::Error for KafkaIngestionError {}

impl From<rdkafka::error::KafkaError> for KafkaIngestionError {
    fn from(value: rdkafka::error::KafkaError) -> Self {
        Self::Kafka(value)
    }
}

#[derive(Deserialize)]
struct WireEnvelope {
    version: u32,
    #[serde(flatten)]
    mutation: WireMutation,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum WireMutation {
    Upsert {
        id: WireRecordId,
        vector: Vec<f32>,
        #[serde(default)]
        metadata: serde_json::Map<String, serde_json::Value>,
    },
    Delete {
        id: WireRecordId,
    },
    Document {
        id: WireRecordId,
        text: String,
        #[serde(default)]
        metadata: serde_json::Map<String, serde_json::Value>,
        #[serde(default)]
        chunking: Option<ChunkingConfig>,
        #[serde(default)]
        source: Option<DocumentSourceDto>,
    },
}

#[derive(Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum WireRecordId {
    String(String),
    U64(u64),
}

enum KafkaMutation {
    Upsert(PendingRecord),
    Delete(RecordId),
    Document(DocumentRecord),
    ChunkedDocument(ChunkedDocument),
}

fn validate_partition_batch(messages: &[KafkaIngestionMessage]) -> Result<(), KafkaIngestionError> {
    if messages.is_empty() {
        return Err(KafkaIngestionError::EmptyBatch);
    }
    let partition = messages[0].partition;
    let mut previous_offset = None;
    for message in messages {
        if message.partition != partition {
            return Err(KafkaIngestionError::MixedPartitions);
        }
        if let Some(previous) = previous_offset
            && message.offset <= previous
        {
            return Err(KafkaIngestionError::NonMonotonicOffsets);
        }
        previous_offset = Some(message.offset);
    }
    Ok(())
}

fn decode_envelope(payload: &[u8]) -> Result<KafkaMutation, KafkaIngestionError> {
    let envelope: WireEnvelope =
        serde_json::from_slice(payload).map_err(KafkaIngestionError::Decode)?;
    if envelope.version != 1 {
        return Err(KafkaIngestionError::UnsupportedEnvelopeVersion(
            envelope.version,
        ));
    }
    match envelope.mutation {
        WireMutation::Upsert {
            id,
            vector,
            metadata,
        } => Ok(KafkaMutation::Upsert(PendingRecord {
            id: record_id(id)?,
            vector,
            metadata: json_metadata(metadata)?,
        })),
        WireMutation::Delete { id } => Ok(KafkaMutation::Delete(record_id(id)?)),
        WireMutation::Document {
            id,
            text,
            metadata,
            chunking,
            source,
        } => {
            let id = record_id(id)?;
            let mut metadata = json_metadata(metadata)?;
            let source = source
                .map(DocumentSourceDto::into_domain)
                .transpose()
                .map_err(|error| KafkaIngestionError::InvalidEnvelope(error.to_string()))?;
            apply_document_provenance(&mut metadata, source.as_ref(), &text)
                .map_err(|error| KafkaIngestionError::InvalidEnvelope(error.to_string()))?;
            if let Some(chunking) = chunking {
                Ok(KafkaMutation::ChunkedDocument(ChunkedDocument {
                    id,
                    text,
                    metadata,
                    chunking,
                }))
            } else {
                Ok(KafkaMutation::Document(DocumentRecord {
                    id,
                    text,
                    metadata,
                }))
            }
        }
    }
}

fn record_id(value: WireRecordId) -> Result<RecordId, KafkaIngestionError> {
    match value {
        WireRecordId::String(value) => RecordId::string(value)
            .map_err(|error| KafkaIngestionError::InvalidEnvelope(error.to_string())),
        WireRecordId::U64(value) => Ok(RecordId::unsigned(value)),
    }
}

fn json_metadata(
    fields: serde_json::Map<String, serde_json::Value>,
) -> Result<Metadata, KafkaIngestionError> {
    fields
        .into_iter()
        .map(|(key, value)| Ok((key, json_value(value)?)))
        .collect()
}

fn json_value(value: serde_json::Value) -> Result<MetadataValue, KafkaIngestionError> {
    match value {
        serde_json::Value::Null => Ok(MetadataValue::Null),
        serde_json::Value::Bool(value) => Ok(MetadataValue::Bool(value)),
        serde_json::Value::Number(value) => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(MetadataValue::Number)
            .ok_or_else(|| {
                KafkaIngestionError::InvalidEnvelope("metadata number must be finite".to_string())
            }),
        serde_json::Value::String(value) => Ok(MetadataValue::String(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(json_value)
            .collect::<Result<Vec<_>, _>>()
            .map(MetadataValue::Array),
        serde_json::Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, json_value(value)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(MetadataValue::Object),
    }
}

#[derive(Debug, Clone)]
enum RebalanceEvent {
    Assigned(Vec<i32>),
    Revoked(Vec<i32>),
}

#[derive(Clone)]
struct RebalanceContext {
    events: Arc<Mutex<Vec<RebalanceEvent>>>,
    counters: Arc<Counters>,
}

impl ClientContext for RebalanceContext {}

impl ConsumerContext for RebalanceContext {
    fn pre_rebalance<'a>(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'a>) {
        self.counters
            .rebalance_count
            .fetch_add(1, Ordering::Relaxed);
        let event = match rebalance {
            Rebalance::Assign(partitions) => Some(RebalanceEvent::Assigned(
                partitions
                    .elements()
                    .iter()
                    .map(|element| element.partition())
                    .collect(),
            )),
            Rebalance::Revoke(partitions) => Some(RebalanceEvent::Revoked(
                partitions
                    .elements()
                    .iter()
                    .map(|element| element.partition())
                    .collect(),
            )),
            Rebalance::Error(_) => None,
        };
        if let Some(event) = event
            && let Ok(mut events) = self.events.lock()
        {
            events.push(event);
        }
    }
}

type KetebeConsumer = StreamConsumer<RebalanceContext>;

pub async fn run_kafka_ingestion(
    state: AppState,
    config: KafkaIngestionConfig,
) -> Result<(), KafkaIngestionError> {
    let lifecycle = state.lifecycle();
    let service = KafkaIngestionService::new(state);
    install_global_observability(&service);
    let rebalance_events = Arc::new(Mutex::new(Vec::new()));
    let context = RebalanceContext {
        events: rebalance_events.clone(),
        counters: service.counters.clone(),
    };
    let consumer: KetebeConsumer = consumer_config(&config).create_with_context(context)?;
    consumer.subscribe(&[&config.topic])?;
    let dlq_producer = match &config.poison_policy {
        KafkaPoisonPolicy::Block => None,
        KafkaPoisonPolicy::Dlq { .. } => Some(producer_config(&config).create()?),
    };

    service.mark_state(KafkaIngestionState::Running);
    let mut buffers: BTreeMap<i32, Vec<OwnedMessage>> = BTreeMap::new();
    let linger = Duration::from_millis(config.batch_linger_ms.max(1));

    loop {
        if lifecycle.is_draining() {
            let assignment = consumer.assignment()?;
            consumer.pause(&assignment)?;
            drain_kafka_buffers(
                &consumer,
                &service,
                &config,
                dlq_producer.as_ref(),
                &mut buffers,
            )
            .await?;
            service.mark_state(KafkaIngestionState::Stopped);
            return Ok(());
        }
        handle_rebalance_events(
            &consumer,
            &service,
            &config,
            dlq_producer.as_ref(),
            &rebalance_events,
            &mut buffers,
        )
        .await?;
        update_buffer_gauge(&service, &buffers);
        update_lag(&consumer, &service, &config);

        match tokio::time::timeout(linger, consumer.recv()).await {
            Ok(Ok(message)) => {
                let partition = message.partition();
                let should_flush = {
                    let buffer = buffers.entry(partition).or_default();
                    buffer.push(message.detach());
                    buffer.len() >= config.batch_max_records
                };
                update_buffer_gauge(&service, &buffers);
                if should_flush && let Some(buffer) = buffers.get_mut(&partition) {
                    flush_partition(
                        &consumer,
                        &service,
                        &config,
                        dlq_producer.as_ref(),
                        partition,
                        buffer,
                    )
                    .await?;
                }
            }
            Ok(Err(error)) => {
                service.mark_state(KafkaIngestionState::Failed);
                return Err(KafkaIngestionError::Kafka(error));
            }
            Err(_) => {
                let partitions: Vec<i32> = buffers
                    .iter()
                    .filter_map(|(partition, messages)| {
                        (!messages.is_empty()).then_some(*partition)
                    })
                    .collect();
                for partition in partitions {
                    if let Some(buffer) = buffers.get_mut(&partition) {
                        flush_partition(
                            &consumer,
                            &service,
                            &config,
                            dlq_producer.as_ref(),
                            partition,
                            buffer,
                        )
                        .await?;
                    }
                }
            }
        }
    }
}

async fn drain_kafka_buffers(
    consumer: &KetebeConsumer,
    service: &KafkaIngestionService,
    config: &KafkaIngestionConfig,
    dlq_producer: Option<&FutureProducer>,
    buffers: &mut BTreeMap<i32, Vec<OwnedMessage>>,
) -> Result<(), KafkaIngestionError> {
    let partitions: Vec<i32> = buffers
        .iter()
        .filter_map(|(partition, messages)| (!messages.is_empty()).then_some(*partition))
        .collect();
    for partition in partitions {
        if let Some(buffer) = buffers.get_mut(&partition) {
            flush_partition(consumer, service, config, dlq_producer, partition, buffer).await?;
        }
    }
    update_buffer_gauge(service, buffers);
    Ok(())
}

fn consumer_config(config: &KafkaIngestionConfig) -> ClientConfig {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest");
    apply_security(&mut client, config.security.as_ref());
    client
}

fn producer_config(config: &KafkaIngestionConfig) -> ClientConfig {
    let mut client = ClientConfig::new();
    client.set("bootstrap.servers", &config.brokers);
    apply_security(&mut client, config.security.as_ref());
    client
}

fn apply_security(client: &mut ClientConfig, security: Option<&KafkaSecurityConfig>) {
    if let Some(security) = security {
        client.set("security.protocol", &security.security_protocol);
        if let Some(value) = &security.sasl_mechanism {
            client.set("sasl.mechanism", value);
        }
        if let Some(value) = &security.sasl_username {
            client.set("sasl.username", value);
        }
        if let Some(value) = &security.sasl_password {
            client.set("sasl.password", value);
        }
    }
}

async fn handle_rebalance_events(
    consumer: &KetebeConsumer,
    service: &KafkaIngestionService,
    config: &KafkaIngestionConfig,
    dlq_producer: Option<&FutureProducer>,
    events: &Arc<Mutex<Vec<RebalanceEvent>>>,
    buffers: &mut BTreeMap<i32, Vec<OwnedMessage>>,
) -> Result<(), KafkaIngestionError> {
    let pending = if let Ok(mut events) = events.lock() {
        std::mem::take(&mut *events)
    } else {
        Vec::new()
    };
    for event in pending {
        match event {
            RebalanceEvent::Assigned(partitions) => {
                service
                    .counters
                    .assigned_partitions
                    .store(partitions.len() as u64, Ordering::Relaxed);
                service.mark_state(KafkaIngestionState::Running);
            }
            RebalanceEvent::Revoked(partitions) => {
                service.mark_state(KafkaIngestionState::Rebalancing);
                for partition in partitions {
                    if let Some(buffer) = buffers.get_mut(&partition) {
                        flush_partition(consumer, service, config, dlq_producer, partition, buffer)
                            .await?;
                    }
                    buffers.remove(&partition);
                }
                service
                    .counters
                    .assigned_partitions
                    .store(0, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

async fn flush_partition(
    consumer: &KetebeConsumer,
    service: &KafkaIngestionService,
    config: &KafkaIngestionConfig,
    dlq_producer: Option<&FutureProducer>,
    partition: i32,
    buffer: &mut Vec<OwnedMessage>,
) -> Result<(), KafkaIngestionError> {
    if buffer.is_empty() {
        return Ok(());
    }
    let trace_headers = buffer.first().map(kafka_trace_headers);
    let span = crate::observability::kafka_span(trace_headers.as_ref(), partition, buffer.len());
    async {
        match &config.poison_policy {
            KafkaPoisonPolicy::Block => {
                let messages = owned_messages(buffer)?;
                let ack = service
                    .apply_partition_batch(&config.collection_id, &messages)
                    .await?;
                commit_ack(consumer, service, config, &ack)?;
                buffer.clear();
            }
            KafkaPoisonPolicy::Dlq { topic } => {
                let producer = dlq_producer.ok_or_else(|| {
                    KafkaIngestionError::InvalidConfig("DLQ producer is not configured".to_string())
                })?;
                flush_partition_with_dlq(
                    consumer, producer, service, config, topic, partition, buffer,
                )
                .await?;
            }
        }
        Ok::<(), KafkaIngestionError>(())
    }
    .instrument(span)
    .await?;
    update_buffer_gauge_single(service, buffer.len());
    Ok(())
}

async fn flush_partition_with_dlq(
    consumer: &KetebeConsumer,
    producer: &FutureProducer,
    service: &KafkaIngestionService,
    config: &KafkaIngestionConfig,
    dlq_topic: &str,
    partition: i32,
    buffer: &mut Vec<OwnedMessage>,
) -> Result<(), KafkaIngestionError> {
    while !buffer.is_empty() {
        let messages = owned_messages(buffer)?;
        let poison = messages.iter().enumerate().find_map(|(index, message)| {
            decode_envelope(&message.payload)
                .err()
                .map(|error| (index, error))
        });
        let Some((poison_index, poison_error)) = poison else {
            let ack = service
                .apply_partition_batch(&config.collection_id, &messages)
                .await?;
            commit_ack(consumer, service, config, &ack)?;
            buffer.clear();
            return Ok(());
        };

        if poison_index > 0 {
            let prefix = &messages[..poison_index];
            let ack = service
                .apply_partition_batch(&config.collection_id, prefix)
                .await?;
            commit_ack(consumer, service, config, &ack)?;
            buffer.drain(..poison_index);
            continue;
        }

        let poison_message = &messages[0];
        service
            .counters
            .received_records
            .fetch_add(1, Ordering::Relaxed);
        service
            .counters
            .decode_failures
            .fetch_add(1, Ordering::Relaxed);
        let envelope = KafkaDlqEnvelope {
            version: 1,
            source_topic: config.topic.clone(),
            source_partition: partition,
            source_offset: poison_message.offset,
            target_collection: config.collection_id.as_str().to_string(),
            error_class: poison_error_class(&poison_error).to_string(),
            error_message: poison_error.to_string(),
            payload: poison_message.payload.clone(),
        };
        publish_dlq(producer, service, dlq_topic, &envelope).await?;
        let ack = KafkaBatchAck {
            partition,
            next_offset: poison_message.offset + 1,
            applied_records: 0,
        };
        commit_ack(consumer, service, config, &ack)?;
        buffer.remove(0);
    }
    Ok(())
}

fn kafka_trace_headers(message: &OwnedMessage) -> HeaderMap {
    let mut extracted = HeaderMap::new();
    let Some(headers) = message.headers() else {
        return extracted;
    };
    for header in headers.iter() {
        if !(header
            .key
            .eq_ignore_ascii_case(crate::observability::TRACEPARENT_HEADER)
            || header
                .key
                .eq_ignore_ascii_case(crate::observability::TRACESTATE_HEADER))
        {
            continue;
        }
        let Some(value) = header.value else {
            continue;
        };
        let Ok(name) = HeaderName::from_bytes(header.key.as_bytes()) else {
            continue;
        };
        let Ok(value) = HeaderValue::from_bytes(value) else {
            continue;
        };
        extracted.insert(name, value);
    }
    extracted
}

fn owned_messages(
    buffer: &[OwnedMessage],
) -> Result<Vec<KafkaIngestionMessage>, KafkaIngestionError> {
    buffer
        .iter()
        .map(|message| {
            let payload = message
                .payload()
                .ok_or(KafkaIngestionError::MissingPayload)?
                .to_vec();
            Ok(KafkaIngestionMessage {
                partition: message.partition(),
                offset: message.offset(),
                payload,
            })
        })
        .collect()
}

async fn publish_dlq(
    producer: &FutureProducer,
    service: &KafkaIngestionService,
    topic: &str,
    envelope: &KafkaDlqEnvelope,
) -> Result<(), KafkaIngestionError> {
    let payload = serde_json::to_vec(envelope)
        .map_err(|error| KafkaIngestionError::Dlq(error.to_string()))?;
    let key = format!(
        "{}:{}:{}",
        envelope.source_topic, envelope.source_partition, envelope.source_offset
    );
    let record = FutureRecord::to(topic).payload(&payload).key(&key);
    match producer
        .send(record, Timeout::After(Duration::from_secs(5)))
        .await
    {
        Ok(_) => {
            service.counters.dlq_records.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err((error, _)) => {
            service
                .counters
                .dlq_failures
                .fetch_add(1, Ordering::Relaxed);
            Err(KafkaIngestionError::Dlq(error.to_string()))
        }
    }
}

fn poison_error_class(error: &KafkaIngestionError) -> &'static str {
    match error {
        KafkaIngestionError::MissingPayload => "missing_payload",
        KafkaIngestionError::UnsupportedEnvelopeVersion(_) => "unsupported_version",
        KafkaIngestionError::Decode(_) => "invalid_json",
        KafkaIngestionError::InvalidEnvelope(_) => "invalid_envelope",
        _ => "ingestion_error",
    }
}

fn commit_ack(
    consumer: &KetebeConsumer,
    service: &KafkaIngestionService,
    config: &KafkaIngestionConfig,
    ack: &KafkaBatchAck,
) -> Result<(), KafkaIngestionError> {
    let mut offsets = TopicPartitionList::new();
    offsets.add_partition_offset(
        &config.topic,
        ack.partition,
        Offset::Offset(ack.next_offset),
    )?;
    let started = Instant::now();
    if let Err(error) = consumer.commit(&offsets, CommitMode::Sync) {
        service
            .counters
            .commit_failures
            .fetch_add(1, Ordering::Relaxed);
        return Err(KafkaIngestionError::Kafka(error));
    }
    service
        .counters
        .last_commit_latency_micros
        .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
    service
        .counters
        .committed_batches
        .fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn update_buffer_gauge(
    service: &KafkaIngestionService,
    buffers: &BTreeMap<i32, Vec<OwnedMessage>>,
) {
    let count = buffers.values().map(Vec::len).sum::<usize>();
    service
        .counters
        .buffered_records
        .store(count as u64, Ordering::Relaxed);
}

fn update_buffer_gauge_single(service: &KafkaIngestionService, count: usize) {
    service
        .counters
        .buffered_records
        .store(count as u64, Ordering::Relaxed);
}

fn update_lag(
    consumer: &KetebeConsumer,
    service: &KafkaIngestionService,
    config: &KafkaIngestionConfig,
) {
    let Ok(assignment) = consumer.assignment() else {
        return;
    };
    let Ok(position) = consumer.position() else {
        return;
    };
    let mut lag = 0_u64;
    for element in assignment.elements() {
        if element.topic() != config.topic {
            continue;
        }
        let partition = element.partition();
        let Ok((_, high)) =
            consumer.fetch_watermarks(&config.topic, partition, Duration::from_millis(20))
        else {
            continue;
        };
        let current = position
            .find_partition(&config.topic, partition)
            .and_then(|value| value.offset().to_raw())
            .unwrap_or(high);
        lag = lag.saturating_add(high.saturating_sub(current).max(0) as u64);
    }
    service
        .counters
        .consumer_lag_records
        .store(lag, Ordering::Relaxed);
}
