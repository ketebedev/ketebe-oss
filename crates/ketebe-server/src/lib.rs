#![forbid(unsafe_code)]

mod api_keys;
mod audit;
mod authentication;
mod authorization;
mod authorization_http;
mod backup;
mod chunking;
mod cursor;
mod data_plane_catalog;
mod data_plane_request;
mod data_plane_resolver;
mod dto;
mod embedding;
mod embedding_cache;
mod embedding_http;
mod embedding_migration;
mod governance;
mod grpc;
mod grpc_transport;
mod grpc_v1;
mod http;
mod integrity;
mod job_access;
mod jobs;
mod jobs_http;
mod kafka_ingestion;
mod lexical_scheduler;
mod lifecycle;
mod management;
mod management_http;
mod observability;
mod profiles_http;
mod provenance;
mod query_embedding_http;
mod query_runtime;
mod query_v1;
mod query_v1_http;
mod records_http;
mod reranking;
mod resource_governance;
mod resource_scheduler;
mod runtime;
mod search_profiles;
mod search_profiles_http;
mod secrets;
mod semantic_chunking;
mod semantic_chunking_service;
mod standalone;
mod stream_ingestion_http;
mod token_chunking;
mod token_chunking_service;
mod transport_tls;
mod write;

pub fn app(state: AppState) -> axum::Router {
    app_with_authentication(state, AuthenticationService::development())
}

pub fn app_with_authentication(
    state: AppState,
    authentication: AuthenticationService,
) -> axum::Router {
    let authorization_state = state.clone();
    let authentication_state =
        authentication::HttpAuthenticationState::new(authentication, state.audit());
    http::app(state.clone())
        .merge(embedding_http::routes(state.clone()))
        .merge(query_embedding_http::routes(state.clone()))
        .merge(query_v1_http::routes(state.clone()))
        .merge(records_http::routes(state.clone()))
        .merge(search_profiles_http::routes(state.clone()))
        .merge(profiles_http::routes(state.clone()))
        .merge(jobs_http::routes(state.clone()))
        .merge(stream_ingestion_http::routes(state.clone()))
        .merge(management_http::routes(state))
        .layer(axum::middleware::from_fn_with_state(
            authorization_state,
            authorization_http::http_authorize,
        ))
        .layer(axum::middleware::from_fn_with_state(
            authentication_state,
            authentication::http_authenticate,
        ))
        .layer(axum::middleware::from_fn(observability::http_trace))
}

pub use api_keys::{ApiKeyError, ApiKeyId, ApiKeyMetadata, ApiKeyStore, IssuedApiKey};
pub use audit::{
    AuditCategory, AuditError, AuditEvent, AuditOrigin, AuditResult, AuditService, AuditSink,
    JsonlAuditSink, NoopAuditSink,
};
pub use authentication::{
    AuthenticationError, AuthenticationMode, AuthenticationService, Credential,
    CredentialAuthenticator, Principal, PrincipalKind,
};
pub use authorization::{
    AuthorizationAction, AuthorizationError, AuthorizationMode, AuthorizationResource,
    AuthorizationService, ClaimOutcome, CollectionPermission, ProjectRole,
};
pub use backup::{
    BACKUP_MANIFEST_VERSION, BackupError, BackupFileEntry, BackupManifest, BackupRepository,
    BackupService, DerivedIndexBackupPolicy, LocalBackupRepository, RestoreResult,
};
pub use chunking::{
    CHUNK_METADATA_KEY, ChunkedDocument, ChunkedDocumentResult, ChunkingConfig, ChunkingError,
    ChunkingService, TextChunk, chunk_record_id, chunk_text,
};
pub use cursor::CursorError;
pub use data_plane_catalog::{CollectionNamespaceCatalog, CollectionNamespaceError};
pub use data_plane_resolver::{DataPlaneResolutionError, DataPlaneResolver};
pub use embedding::{
    DeterministicEmbeddingProvider, DocumentRecord, EmbeddingBatchFuture, EmbeddingError,
    EmbeddingFuture, EmbeddingModel, EmbeddingProfileInfo, EmbeddingProvider,
    EmbeddingProviderError, EmbeddingProviderRegistry, EmbeddingService,
    OpenAiCompatibleEmbeddingConfig, OpenAiCompatibleEmbeddingProvider, embed_texts_batched,
    embedding_prometheus_metrics,
};
pub use embedding_cache::{
    DEFAULT_EMBEDDING_CACHE_CAPACITY, EmbeddingCache, EmbeddingCacheKey, embed_texts_cached,
    embedding_cache_key, embedding_cache_prometheus_metrics,
};
pub use embedding_migration::{
    EmbeddingMigrationError, EmbeddingMigrationService, EmbeddingMigrationState,
    EmbeddingMigrationStatus, embedding_migration_prometheus_metrics,
};
pub use governance::{
    AdmissionClass, AdmissionDecision, GovernanceError, GovernancePolicy, GovernanceService,
    ProjectQuota, RateLimit,
};
pub use grpc::{
    proto, serve_grpc, serve_grpc_listener, serve_grpc_listener_until_shutdown,
    serve_grpc_listener_until_shutdown_with_authentication,
    serve_grpc_listener_with_authentication,
};
pub use grpc_transport::{GrpcTransportError, serve_grpc_transport_until_shutdown};
pub use grpc_v1::proto as proto_v1;
pub use integrity::{
    IntegrityCheck, IntegrityClass, IntegrityError, IntegrityOutcome, IntegrityReport,
    IntegrityStatus, IntegrityVerifier,
};
pub use jobs::{
    DEFAULT_JOB_CONCURRENCY, JobFailure, JobId, JobKind, JobProgress, JobRecord, JobResult,
    JobService, JobServiceError, JobState, job_prometheus_metrics,
};
pub use kafka_ingestion::{
    KafkaBatchAck, KafkaDlqEnvelope, KafkaIngestionConfig, KafkaIngestionError,
    KafkaIngestionMessage, KafkaIngestionService, KafkaIngestionState, KafkaIngestionStats,
    KafkaPoisonPolicy, KafkaSecurityConfig, kafka_prometheus_metrics, run_kafka_ingestion,
};
pub use lifecycle::{Lifecycle, LifecyclePhase, LifecycleWriteGuard};
pub use management::{CollectionInfo, CollectionService, HnswState, ManagementError};
pub use observability::{ObservabilityGuard, init_observability};
pub use provenance::{
    CONTENT_METADATA_KEY, ProvenanceError, SOURCE_METADATA_KEY, SourceChange,
    apply_chunk_content_hash, apply_document_provenance, canonical_content_hash,
    detect_source_change, normalize_content,
};
pub use query_runtime::{
    DEFAULT_MAX_QUERY_CANDIDATES, DEFAULT_MAX_QUERY_TIMEOUT_MS, DEFAULT_MAX_QUERY_TOP_K,
    DEFAULT_QUERY_CONCURRENCY, DEFAULT_QUERY_TIMEOUT_MS, QueryAdmissionError,
    QueryAdmissionRequest, QueryLimits, QueryRuntime,
};
pub use query_v1::{
    QueryModeV1, QueryPaginationV1, QueryRerankExplainV1, QueryRerankV1, QueryV1Error,
    QueryV1Explain, QueryV1Hit, QueryV1Page, QueryV1Request, QueryV1Response, execute_query_v1,
    execute_query_v1_page,
};
pub use reranking::{
    CandidateProjection, HttpReranker, HttpRerankerConfig, RerankCandidate, RerankExplain,
    RerankFailurePolicy, RerankFuture, RerankResult, RerankScore, RerankedCandidate, Reranker,
    RerankerError, RerankerProfileInfo, RerankerRegistry, RerankingError, RerankingService,
};
pub use resource_governance::{
    InMemoryResourceGovernor, ProjectResourceBudget, ResourceAdmission, ResourceGovernanceError,
    ResourceGovernor, ResourcePermit as ProjectResourcePermit, ResourceWorkClass, ThroughputBudget,
};
pub use resource_scheduler::{
    ResourceBudget, ResourcePermit, ResourceRequest, ResourceScheduler, ResourceSchedulerError,
    ResourceSchedulerSnapshot, WorkKind, WorkPriority, global_resource_scheduler,
    resource_scheduler_prometheus_metrics,
};
pub use runtime::{AppState, CollectionRuntime, LexicalBuildState, RuntimeCatalog, RuntimeError};
pub use search_profiles::{
    DEFAULT_QUERY_TOP_K, SearchProfile, SearchProfileError, SearchProfileExecution,
    SearchProfileFailurePolicy, SearchProfileRerank, SearchProfileStore, profiles_by_name,
};
pub use secrets::{
    SecretError, SecretRef, SecretResolver, SecretResolverHandle, SecretValue, SystemSecretResolver,
};
pub use semantic_chunking::{
    ReferenceLexicalBoundaryScorer, SEMANTIC_CHUNKER_VERSION, SEMANTIC_SCORER_ID,
    SemanticBoundaryCandidate, SemanticBoundaryScorer, chunks_from_similarity_scores,
    cosine_similarity, semantic_boundary_candidates, semantic_chunker_fingerprint,
    token_fallback_policy,
};
pub use semantic_chunking_service::{
    SemanticChunkedDocument, SemanticChunkingError, SemanticChunkingService,
    semantic_chunking_prometheus_metrics,
};
pub use standalone::run_standalone_from_env;
pub use token_chunking::{
    StructuredChunk, TokenCounter, TokenSpan, UnicodeWordTokenCounter, chunk_text_token_aware,
    chunker_fingerprint,
};
pub use token_chunking_service::{TokenChunkedDocument, TokenChunkingError, TokenChunkingService};
pub use transport_tls::{TransportTlsConfig, TransportTlsError, transport_tls_from_env};
pub use write::{PendingRecord, WriteError, WriteService};
