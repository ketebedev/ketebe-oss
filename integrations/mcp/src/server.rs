use std::time::Instant;

use axum::http::{header, request::Parts};
use rmcp::{
    RoleServer, ServerHandler,
    handler::server::wrapper::{Json, Parameters},
    service::RequestContext,
    tool, tool_handler, tool_router,
};

use crate::{
    auth::{AuthMode, RequestCredential},
    context::{RetrieveContextOutput, RetrieveContextParams, assemble_context},
    diagnostics::ExplainSearchOutput,
    discovery::{CollectionParams, CollectionStatsOutput, CollectionView, ListCollectionsOutput},
    embedding_lifecycle::{ReembeddingParams, ReembeddingStatusParams, ReembeddingView},
    fusion::{FusedSearchOutput, FusedSearchParams},
    jobs::{JobParams, JobView, ListJobsOutput},
    ketebe::KetebeApi,
    multi_search::{SearchManyOutput, SearchManyParams},
    mutation::{
        IngestDocumentsOutput, IngestDocumentsParams, UpsertRecordsOutput, UpsertRecordsParams,
    },
    policy::{ToolClass, ToolPolicy},
    profiles::{
        EmbeddingProfileView, ListEmbeddingProfilesOutput, ListRerankerProfilesOutput,
        ProfileParams, RerankerProfileView,
    },
    readiness::Readiness,
    retrieval::{GetRecordParams, GetRecordsOutput, GetRecordsParams, RecordView},
    search::{SearchOutput, SearchParams},
    search_profiles::{
        DescribeSearchProfileParams, ListSearchProfilesOutput, ListSearchProfilesParams,
        SearchProfileView,
    },
    stream_ingestion::{
        CreateStreamIngestionParams, ListStreamIngestionsOutput, StreamCollectionParams,
        StreamIngestionParams, StreamIngestionView,
    },
};

#[derive(Clone, Debug)]
pub struct KetebeMcpServer {
    readiness: Readiness,
    auth_mode: AuthMode,
    static_credential: Option<RequestCredential>,
    api: KetebeApi,
}

impl KetebeMcpServer {
    pub fn new(
        readiness: Readiness,
        auth_mode: AuthMode,
        static_credential: Option<RequestCredential>,
        api: KetebeApi,
    ) -> Self {
        Self {
            readiness,
            auth_mode,
            static_credential,
            api,
        }
    }

    #[must_use]
    pub fn auth_mode(&self) -> AuthMode {
        self.auth_mode
    }

    #[must_use]
    pub fn static_credential(&self) -> Option<&RequestCredential> {
        self.static_credential.as_ref()
    }

    fn bearer_token(&self, context: &RequestContext<RoleServer>) -> Option<String> {
        if let Some(credential) = self.static_credential.as_ref() {
            return Some(credential.expose_secret().to_string());
        }
        let parts = context.extensions.get::<Parts>()?;
        let value = parts.headers.get(header::AUTHORIZATION)?.to_str().ok()?;
        RequestCredential::from_authorization(value)
            .ok()
            .map(|credential| credential.expose_secret().to_string())
    }

    fn discovery_error(error: ketebe_sdk::Error) -> String {
        match error {
            ketebe_sdk::Error::Api { status, code, .. } => {
                format!("Ketebe discovery request failed: {status} {code}")
            }
            ketebe_sdk::Error::Transport(_) => "Ketebe discovery service unavailable".to_string(),
        }
    }

    fn require_write_tool(name: &str) -> Result<(), String> {
        let policy = ToolPolicy::from_env()
            .map_err(|_| "Ketebe MCP policy configuration invalid".to_string())?;
        if policy.tool_visible(name, ToolClass::Write) {
            Ok(())
        } else {
            Err(format!("Ketebe MCP tool {name} is disabled by policy"))
        }
    }
}

#[tool_router]
impl KetebeMcpServer {
    #[tool(
        description = "Return whether the configured Ketebe public API is currently reachable",
        annotations(read_only_hint = true)
    )]
    async fn ketebe_status(&self) -> String {
        if self.readiness.is_ready() {
            "ready".to_string()
        } else {
            "not_ready".to_string()
        }
    }

    #[tool(
        description = "List collections visible to the authenticated Ketebe principal",
        annotations(read_only_hint = true)
    )]
    async fn list_collections(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<ListCollectionsOutput>, String> {
        let token = self.bearer_token(&context);
        let collections = self
            .api
            .list_collections(token.as_deref())
            .await
            .map_err(Self::discovery_error)?;
        Ok(Json(ListCollectionsOutput {
            collections: collections.into_iter().map(CollectionView::from).collect(),
        }))
    }

    #[tool(
        description = "Describe an authorized Ketebe collection without exposing shard or node topology",
        annotations(read_only_hint = true)
    )]
    async fn describe_collection(
        &self,
        Parameters(CollectionParams { collection }): Parameters<CollectionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<CollectionView>, String> {
        let token = self.bearer_token(&context);
        let collection = self
            .api
            .get_collection(&collection, token.as_deref())
            .await
            .map_err(Self::discovery_error)?;
        Ok(Json(CollectionView::from(collection)))
    }

    #[tool(
        description = "Return safe public statistics for an authorized Ketebe collection",
        annotations(read_only_hint = true)
    )]
    async fn collection_stats(
        &self,
        Parameters(CollectionParams { collection }): Parameters<CollectionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<CollectionStatsOutput>, String> {
        let token = self.bearer_token(&context);
        let collection = self
            .api
            .get_collection(&collection, token.as_deref())
            .await
            .map_err(Self::discovery_error)?;
        Ok(Json(CollectionStatsOutput::from(collection)))
    }

    #[tool(
        description = "List authorized Ketebe search profiles for a collection so agents can select stable retrieval policy instead of low-level tuning",
        annotations(read_only_hint = true)
    )]
    async fn list_search_profiles(
        &self,
        Parameters(ListSearchProfilesParams { collection }): Parameters<ListSearchProfilesParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<ListSearchProfilesOutput>, String> {
        let token = self.bearer_token(&context);
        let profiles = self
            .api
            .list_search_profiles(&collection, token.as_deref())
            .await?;
        Ok(Json(ListSearchProfilesOutput { profiles }))
    }

    #[tool(
        description = "Describe one authorized Ketebe search profile by name or pinned name@version selector",
        annotations(read_only_hint = true)
    )]
    async fn describe_search_profile(
        &self,
        Parameters(DescribeSearchProfileParams {
            collection,
            profile,
        }): Parameters<DescribeSearchProfileParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SearchProfileView>, String> {
        let token = self.bearer_token(&context);
        self.api
            .get_search_profile(&collection, &profile, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "List configured Ketebe embedding profiles using safe provider/model metadata only. Provider endpoints, secret references, and credentials are never returned.",
        annotations(read_only_hint = true)
    )]
    async fn list_embedding_profiles(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<ListEmbeddingProfilesOutput>, String> {
        let token = self.bearer_token(&context);
        self.api
            .list_embedding_profiles(token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Describe one Ketebe embedding profile using safe provider/model metadata only, without provider endpoints, secret references, or credentials.",
        annotations(read_only_hint = true)
    )]
    async fn describe_embedding_profile(
        &self,
        Parameters(params): Parameters<ProfileParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<EmbeddingProfileView>, String> {
        let token = self.bearer_token(&context);
        self.api
            .describe_embedding_profile(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "List configured Ketebe reranker profiles using safe provider metadata only. Provider endpoints, secret references, and credentials are never returned.",
        annotations(read_only_hint = true)
    )]
    async fn list_reranker_profiles(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<ListRerankerProfilesOutput>, String> {
        let token = self.bearer_token(&context);
        self.api
            .list_reranker_profiles(token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Describe one Ketebe reranker profile using safe provider metadata only, without provider endpoints, secret references, or credentials.",
        annotations(read_only_hint = true)
    )]
    async fn describe_reranker_profile(
        &self,
        Parameters(params): Parameters<ProfileParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<RerankerProfileView>, String> {
        let token = self.bearer_token(&context);
        self.api
            .describe_reranker_profile(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Start a controlled Ketebe embedding migration for an authorized collection. This write tool is disabled by default and requires KETEBE_MCP_ALLOW_WRITE=true in addition to normal Ketebe authorization. Long-running catch-up remains server-side and is observed through Ketebe job tools.",
        annotations(read_only_hint = false)
    )]
    async fn start_reembedding(
        &self,
        Parameters(params): Parameters<ReembeddingParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<ReembeddingView>, String> {
        Self::require_write_tool("start_reembedding")?;
        let token = self.bearer_token(&context);
        self.api
            .start_reembedding(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Inspect the current Ketebe embedding migration state for an authorized collection, including progress and safe provider/model identity without credentials.",
        annotations(read_only_hint = true)
    )]
    async fn get_reembedding_status(
        &self,
        Parameters(params): Parameters<ReembeddingStatusParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<ReembeddingView>, String> {
        let token = self.bearer_token(&context);
        self.api
            .get_reembedding_status(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "List Kafka-native stream ingestions for an authorized Ketebe collection. Returns safe lifecycle, lag, topic, and group metadata only; broker endpoints and credentials are never exposed.",
        annotations(read_only_hint = true)
    )]
    async fn list_stream_ingestions(
        &self,
        Parameters(StreamCollectionParams { collection }): Parameters<StreamCollectionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<ListStreamIngestionsOutput>, String> {
        let token = self.bearer_token(&context);
        self.api
            .list_stream_ingestions(&collection, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Create a Kafka-native stream ingestion through Ketebe's public collection-scoped management API. MCP does not manage offsets or delivery correctness. This write tool is disabled by default and requires KETEBE_MCP_ALLOW_WRITE=true plus normal Ketebe authorization.",
        annotations(read_only_hint = false)
    )]
    async fn create_stream_ingestion(
        &self,
        Parameters(params): Parameters<CreateStreamIngestionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<StreamIngestionView>, String> {
        Self::require_write_tool("create_stream_ingestion")?;
        let token = self.bearer_token(&context);
        self.api
            .create_stream_ingestion(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Inspect one authorized Kafka-native Ketebe stream ingestion, including stable state, consumer lag, and failure code without broker secrets or credentials.",
        annotations(read_only_hint = true)
    )]
    async fn get_stream_ingestion(
        &self,
        Parameters(params): Parameters<StreamIngestionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<StreamIngestionView>, String> {
        let token = self.bearer_token(&context);
        self.api
            .get_stream_ingestion(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Pause an authorized Kafka-native Ketebe stream ingestion. Ketebe remains owner of offset and WAL correctness. This write tool is disabled by default and requires KETEBE_MCP_ALLOW_WRITE=true plus normal Ketebe authorization.",
        annotations(read_only_hint = false)
    )]
    async fn pause_stream_ingestion(
        &self,
        Parameters(params): Parameters<StreamIngestionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<StreamIngestionView>, String> {
        Self::require_write_tool("pause_stream_ingestion")?;
        let token = self.bearer_token(&context);
        self.api
            .pause_stream_ingestion(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Resume an authorized Kafka-native Ketebe stream ingestion using the existing Kafka consumer-group and WAL durability semantics. This write tool is disabled by default and requires KETEBE_MCP_ALLOW_WRITE=true plus normal Ketebe authorization.",
        annotations(read_only_hint = false)
    )]
    async fn resume_stream_ingestion(
        &self,
        Parameters(params): Parameters<StreamIngestionParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<StreamIngestionView>, String> {
        Self::require_write_tool("resume_stream_ingestion")?;
        let token = self.bearer_token(&context);
        self.api
            .resume_stream_ingestion(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Fetch one record by its typed string or u64 identifier from an authorized collection",
        annotations(read_only_hint = true)
    )]
    async fn get_record(
        &self,
        Parameters(params): Parameters<GetRecordParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<RecordView>, String> {
        let token = self.bearer_token(&context);
        let response = self
            .api
            .fetch_records(
                &params.collection,
                vec![params.id],
                params.fields,
                token.as_deref(),
            )
            .await
            .map_err(|error| error.stable_message())?;
        response
            .records
            .into_iter()
            .next()
            .map(Json)
            .ok_or_else(|| "Ketebe record request failed: 404 record_not_found".to_string())
    }

    #[tool(
        description = "Fetch multiple records by typed string or u64 identifiers from an authorized collection",
        annotations(read_only_hint = true)
    )]
    async fn get_records(
        &self,
        Parameters(params): Parameters<GetRecordsParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<GetRecordsOutput>, String> {
        let token = self.bearer_token(&context);
        self.api
            .fetch_records(
                &params.collection,
                params.ids,
                params.fields,
                token.as_deref(),
            )
            .await
            .map(Json)
            .map_err(|error| error.stable_message())
    }

    #[tool(
        description = "Inspect one asynchronous Ketebe job visible to the authenticated principal. Returns stable queued/running/completed/failed/cancelled state, progress, result, and safe error details without holding the MCP request open for the underlying work.",
        annotations(read_only_hint = true)
    )]
    async fn get_job(
        &self,
        Parameters(JobParams { job_id }): Parameters<JobParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<JobView>, String> {
        let token = self.bearer_token(&context);
        self.api.get_job(job_id, token.as_deref()).await.map(Json)
    }

    #[tool(
        description = "List asynchronous Ketebe jobs visible to the authenticated principal. Long-running work remains server-side; this tool returns current job lifecycle snapshots only.",
        annotations(read_only_hint = true)
    )]
    async fn list_jobs(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<ListJobsOutput>, String> {
        let token = self.bearer_token(&context);
        self.api.list_jobs(token.as_deref()).await.map(Json)
    }

    #[tool(
        description = "Request cancellation of an asynchronous Ketebe job visible to the authenticated principal. This mutation is disabled by default and requires KETEBE_MCP_ALLOW_WRITE=true in addition to normal Ketebe authorization and job ownership checks.",
        annotations(read_only_hint = false)
    )]
    async fn cancel_job(
        &self,
        Parameters(JobParams { job_id }): Parameters<JobParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<JobView>, String> {
        Self::require_write_tool("cancel_job")?;
        let token = self.bearer_token(&context);
        self.api
            .cancel_job(job_id, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Upsert records into an authorized Ketebe collection through the public batch write API. This write tool is disabled by default and requires KETEBE_MCP_ALLOW_WRITE=true in addition to normal Ketebe authorization.",
        annotations(read_only_hint = false)
    )]
    async fn upsert_records(
        &self,
        Parameters(params): Parameters<UpsertRecordsParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<UpsertRecordsOutput>, String> {
        Self::require_write_tool("upsert_records")?;
        let token = self.bearer_token(&context);
        self.api
            .upsert_records(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Ingest documents into an authorized Ketebe collection using stable parent document IDs. Ketebe performs chunking and embedding server-side; provider credentials are never exposed to MCP. This write tool is disabled by default and requires KETEBE_MCP_ALLOW_WRITE=true.",
        annotations(read_only_hint = false)
    )]
    async fn ingest_documents(
        &self,
        Parameters(params): Parameters<IngestDocumentsParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<IngestDocumentsOutput>, String> {
        Self::require_write_tool("ingest_documents")?;
        let token = self.bearer_token(&context);
        self.api
            .ingest_documents(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Search an authorized Ketebe collection using dense, sparse, or hybrid retrieval. Prefer search_profile for stable server-managed retrieval policy; low-level tuning remains optional.",
        annotations(read_only_hint = true)
    )]
    async fn search(
        &self,
        Parameters(params): Parameters<SearchParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SearchOutput>, String> {
        let token = self.bearer_token(&context);
        self.api
            .search_params(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Search multiple Ketebe collections in one read-only operation. Results preserve request order and source collection provenance; per-collection failures are explicit and do not cancel successful searches.",
        annotations(read_only_hint = true)
    )]
    async fn search_many(
        &self,
        Parameters(params): Parameters<SearchManyParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<SearchManyOutput>, String> {
        let token = self.bearer_token(&context);
        self.api
            .search_many_params(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Search multiple authorized Ketebe collections and return a deterministic fused result set with RRF or score-sum fusion, optional typed RecordId deduplication, and optional server-side reranking through the public Query v1 contract.",
        annotations(read_only_hint = true)
    )]
    async fn search_fused(
        &self,
        Parameters(params): Parameters<FusedSearchParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<FusedSearchOutput>, String> {
        let token = self.bearer_token(&context);
        self.api
            .search_fused_params(params, token.as_deref())
            .await
            .map(Json)
    }

    #[tool(
        description = "Retrieve LLM-ready context from authorized Ketebe collections by composing multi-collection search, fusion, deduplication, optional server-side reranking, source citations, and deterministic token/byte/document budgets.",
        annotations(read_only_hint = true)
    )]
    async fn retrieve_context(
        &self,
        Parameters(params): Parameters<RetrieveContextParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<RetrieveContextOutput>, String> {
        params.validate()?;
        let token = self.bearer_token(&context);
        let search = params.prepare_search();
        let result = self
            .api
            .search_fused_params(search, token.as_deref())
            .await?;
        Ok(Json(assemble_context(&params, result)))
    }

    #[tool(
        description = "Explain an authorized retrieval using only public Query v1 planner metadata and stable MCP fusion provenance. Returns dense/sparse/fusion/rerank score breakdowns, candidate and filter diagnostics, and observed end-to-end search latency without exposing shard, node, credential, or provider topology.",
        annotations(read_only_hint = true)
    )]
    async fn explain_search(
        &self,
        Parameters(mut params): Parameters<FusedSearchParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<Json<ExplainSearchOutput>, String> {
        params.explain = true;
        let token = self.bearer_token(&context);
        let started = Instant::now();
        let result = self
            .api
            .search_fused_params(params.clone(), token.as_deref())
            .await?;
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok(Json(ExplainSearchOutput::from_execution(
            &params, elapsed_ms, result,
        )))
    }
}

#[tool_handler(
    name = "ketebe-mcp",
    instructions = "Ketebe MCP integration. Discovery, retrieval, profile discovery, embedding migration status, Kafka-native stream ingestion lifecycle, and asynchronous job inspection reuse Ketebe public API authorization and never expose storage topology, WAL access, broker/provider endpoints, secret references, or credentials. Write tools including create/pause/resume stream ingestion, start_reembedding, and job cancellation are disabled by default and require both MCP write policy enablement and normal Ketebe authorization. MCP never owns Kafka offsets or ingestion correctness; Kafka delivery and WAL durability remain server-side Ketebe responsibilities. Prefer search profiles for stable server-managed retrieval policy; use embedding/reranker profile discovery to select safe configured capabilities, search_fused for multi-source fusion/deduplication and server-side reranking, retrieve_context for LLM-ready cited context with deterministic budgets, explain_search for safe public retrieval diagnostics, and ingest_documents for server-side chunking/embedding when writes are explicitly enabled."
)]
impl ServerHandler for KetebeMcpServer {}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::ServerHandler;

    fn test_api() -> KetebeApi {
        KetebeApi::new("http://127.0.0.1:18080").unwrap()
    }

    #[test]
    fn static_credential_debug_is_redacted() {
        let server = KetebeMcpServer::new(
            Readiness::default(),
            AuthMode::Required,
            Some(RequestCredential::from_token("hidden-token").unwrap()),
            test_api(),
        );
        let debug = format!("{server:?}");
        assert!(!debug.contains("hidden-token"));
    }

    #[test]
    fn foundation_advertises_tools_capability() {
        let info = KetebeMcpServer::new(
            Readiness::default(),
            AuthMode::Development,
            None,
            test_api(),
        )
        .get_info();
        assert!(info.capabilities.tools.is_some());
    }
}
