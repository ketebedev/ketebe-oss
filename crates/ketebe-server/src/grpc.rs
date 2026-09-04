use ketebe_core::{
    ChunkingPolicy, CollectionIngestionConfig, DistanceMetric, FieldPath, LexicalAnalyzerConfig,
    Metadata, MetadataValue, Predicate, RecordId,
};
use ketebe_storage::{
    DEFAULT_RRF_K, ExecutionPreference, ExecutionStrategy, FilteredSearchError, HnswError,
    HybridError, LexicalQuery, PlanReason, PlannerError, QueryRequest as StorageQueryRequest,
    SearchError, execute_hybrid_query, execute_hybrid_query_with_index, execute_query,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, service::InterceptorLayer};

use crate::management::{CollectionInfo, CollectionService, HnswState, ManagementError};
use crate::runtime::{AppState, LexicalBuildState};
use crate::write::{PendingRecord, WriteError, WriteService};

pub mod proto {
    tonic::include_proto!("ketebe.v0");
}
use proto::collections_server::{Collections, CollectionsServer};
use proto::query_server::{Query, QueryServer};
use proto::records_server::{Records, RecordsServer};

#[derive(Clone)]
struct GrpcApi {
    state: AppState,
}
impl GrpcApi {
    fn new(state: AppState) -> Self {
        Self { state }
    }

    fn admit_foreground_write(&self) -> Result<crate::LifecycleWriteGuard, Status> {
        self.state.try_admit_foreground_write().ok_or_else(|| {
            Status::unavailable("Ketebe runtime is draining and no longer accepts new writes")
        })
    }
}

pub async fn serve_grpc(state: AppState, address: SocketAddr) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(address).await?;
    serve_grpc_listener(state, listener)
        .await
        .map_err(std::io::Error::other)
}
pub async fn serve_grpc_listener(
    state: AppState,
    listener: TcpListener,
) -> Result<(), tonic::transport::Error> {
    serve_grpc_listener_with_authentication(
        state,
        listener,
        crate::AuthenticationService::development(),
    )
    .await
}

pub async fn serve_grpc_listener_with_authentication(
    state: AppState,
    listener: TcpListener,
    authentication: crate::AuthenticationService,
) -> Result<(), tonic::transport::Error> {
    serve_grpc_listener_inner(
        state,
        listener,
        authentication,
        std::future::pending::<()>(),
    )
    .await
}

pub async fn serve_grpc_listener_until_shutdown(
    state: AppState,
    listener: TcpListener,
) -> Result<(), tonic::transport::Error> {
    serve_grpc_listener_until_shutdown_with_authentication(
        state,
        listener,
        crate::AuthenticationService::development(),
    )
    .await
}

pub async fn serve_grpc_listener_until_shutdown_with_authentication(
    state: AppState,
    listener: TcpListener,
    authentication: crate::AuthenticationService,
) -> Result<(), tonic::transport::Error> {
    let lifecycle = state.lifecycle();
    serve_grpc_listener_inner(state, listener, authentication, async move {
        lifecycle.wait_for_draining().await;
    })
    .await
}

async fn serve_grpc_listener_inner<F>(
    state: AppState,
    listener: TcpListener,
    authentication: crate::AuthenticationService,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: std::future::Future<Output = ()>,
{
    let query_v1 = crate::grpc_v1::server(state.clone());
    let audit = state.audit();
    let api = GrpcApi::new(state);
    let trace_layer = tower_http::trace::TraceLayer::new_for_grpc().make_span_with(
        |request: &tonic::codegen::http::Request<tonic::body::Body>| {
            crate::observability::grpc_span(request)
        },
    );
    let auth_layer = InterceptorLayer::new(move |request| {
        crate::authentication::grpc_authenticate(&authentication, &audit, request)
    });
    tonic::transport::Server::builder()
        .layer(trace_layer)
        .layer(auth_layer)
        .add_service(CollectionsServer::new(api.clone()))
        .add_service(RecordsServer::new(api.clone()))
        .add_service(QueryServer::new(api))
        .add_service(query_v1)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}

#[tonic::async_trait]
impl Collections for GrpcApi {
    async fn create_collection(
        &self,
        request: Request<proto::CreateCollectionRequest>,
    ) -> Result<Response<proto::Collection>, Status> {
        let principal = crate::authorization::grpc_principal(&request)?;
        crate::authorization::grpc_authorize_create(&self.state, &principal)?;
        let _write_guard = self.admit_foreground_write()?;
        let request = request.into_inner();
        let collection_name = request.id;
        let metric = metric_from_proto(request.metric)?;
        let dimension = usize::try_from(request.dimension)
            .map_err(|_| Status::invalid_argument("dimension is too large"))?;
        let lexical_fields = request
            .lexical_fields
            .into_iter()
            .map(|path| field_path(path.segments))
            .collect::<Result<Vec<_>, _>>()?;
        let ingestion = ingestion_from_proto(request.ingestion)?;
        let analyzer = analyzer_from_proto(request.lexical_analyzer)?;
        let scope =
            crate::data_plane_request::create_scope(&self.state, &principal, &collection_name)
                .map_err(map_data_plane_request_error)?;
        let id = scope.collection_id().clone();
        let claim = match self
            .state
            .authorization()
            .claim_collection(&principal, &collection_name)
        {
            Ok(claim) => claim,
            Err(_) => {
                let _ = crate::data_plane_request::remove_scope(
                    &self.state,
                    &principal,
                    &collection_name,
                    &id,
                );
                return Err(Status::permission_denied("authorization denied"));
            }
        };
        let create_result = WriteService::new(self.state.clone())
            .create_collection_with_schema_scoped(
                &scope,
                dimension,
                metric,
                lexical_fields,
                analyzer,
                ingestion,
            )
            .await;
        if let Err(error) = create_result {
            let _ = self
                .state
                .authorization()
                .release_collection_claim_for_principal(&principal, &collection_name, claim);
            let _ = crate::data_plane_request::remove_scope(
                &self.state,
                &principal,
                &collection_name,
                &id,
            );
            return Err(map_write_error(error));
        }
        let info = CollectionService::new(self.state.clone())
            .get(&id)
            .await
            .map_err(map_management_error)?;
        Ok(Response::new(collection_to_proto_named(
            info,
            collection_name,
        )?))
    }
    async fn update_lexical_schema(
        &self,
        request: Request<proto::UpdateLexicalSchemaRequest>,
    ) -> Result<Response<proto::Collection>, Status> {
        let principal = crate::authorization::grpc_principal(&request)?;
        let _write_guard = self.admit_foreground_write()?;
        let request = request.into_inner();
        let collection_name = request.id;
        let scope = crate::data_plane_request::resolve_existing_scope(
            &self.state,
            &principal,
            &collection_name,
        )
        .await
        .map_err(map_data_plane_request_error)?;
        let id = scope.collection_id().clone();
        crate::authorization::grpc_authorize_collection(
            &self.state,
            &principal,
            crate::AuthorizationAction::CollectionWrite,
            &collection_name,
        )?;
        let fields = request
            .lexical_fields
            .into_iter()
            .map(|path| field_path(path.segments))
            .collect::<Result<Vec<_>, _>>()?;
        WriteService::new(self.state.clone())
            .update_lexical_schema_scoped(
                &scope,
                fields,
                analyzer_from_proto(request.lexical_analyzer)?,
            )
            .await
            .map_err(map_write_error)?;
        let info = CollectionService::new(self.state.clone())
            .get(&id)
            .await
            .map_err(map_management_error)?;
        Ok(Response::new(collection_to_proto_named(
            info,
            collection_name,
        )?))
    }

    async fn list_collections(
        &self,
        request: Request<proto::ListCollectionsRequest>,
    ) -> Result<Response<proto::ListCollectionsResponse>, Status> {
        let principal = crate::authorization::grpc_principal(&request)?;
        crate::authorization::grpc_authorize_discover(&self.state, &principal)?;
        let authorization = self.state.authorization();
        let names = crate::data_plane_request::list_project_scopes(&self.state, &principal)
            .await
            .map_err(map_data_plane_request_error)?
            .into_iter()
            .map(|(name, scope)| (scope.collection_id().clone(), name))
            .collect::<std::collections::BTreeMap<_, _>>();
        let collections = CollectionService::new(self.state.clone())
            .list()
            .await
            .map_err(map_management_error)?
            .into_iter()
            .filter_map(|collection| {
                let name = names.get(&collection.id)?;
                authorization
                    .can_discover_collection(&principal, name)
                    .then_some((collection, name.clone()))
            })
            .map(|(collection, name)| collection_to_proto_named(collection, name))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(proto::ListCollectionsResponse {
            collections,
        }))
    }
    async fn get_collection(
        &self,
        request: Request<proto::GetCollectionRequest>,
    ) -> Result<Response<proto::Collection>, Status> {
        let principal = crate::authorization::grpc_principal(&request)?;
        let collection_name = request.into_inner().id;
        let scope = crate::data_plane_request::resolve_existing_scope(
            &self.state,
            &principal,
            &collection_name,
        )
        .await
        .map_err(map_data_plane_request_error)?;
        let id = scope.collection_id().clone();
        crate::authorization::grpc_authorize_collection(
            &self.state,
            &principal,
            crate::AuthorizationAction::CollectionRead,
            &collection_name,
        )?;
        let info = CollectionService::new(self.state.clone())
            .get(&id)
            .await
            .map_err(map_management_error)?;
        Ok(Response::new(collection_to_proto_named(
            info,
            collection_name,
        )?))
    }
    async fn delete_collection(
        &self,
        request: Request<proto::DeleteCollectionRequest>,
    ) -> Result<Response<proto::DeleteCollectionResponse>, Status> {
        let principal = crate::authorization::grpc_principal(&request)?;
        let _write_guard = self.admit_foreground_write()?;
        let collection_name = request.into_inner().id;
        let scope = crate::data_plane_request::resolve_existing_scope(
            &self.state,
            &principal,
            &collection_name,
        )
        .await
        .map_err(map_data_plane_request_error)?;
        let id = scope.collection_id().clone();
        crate::authorization::grpc_authorize_collection(
            &self.state,
            &principal,
            crate::AuthorizationAction::CollectionDelete,
            &collection_name,
        )?;
        CollectionService::new(self.state.clone())
            .delete(&id)
            .await
            .map_err(map_management_error)?;
        self.state
            .authorization()
            .remove_collection(&principal, &collection_name)
            .map_err(|_| Status::permission_denied("authorization denied"))?;
        crate::data_plane_request::remove_scope(&self.state, &principal, &collection_name, &id)
            .map_err(map_data_plane_request_error)?;
        Ok(Response::new(proto::DeleteCollectionResponse {}))
    }
}

#[tonic::async_trait]
impl Records for GrpcApi {
    async fn upsert(
        &self,
        request: Request<proto::UpsertRequest>,
    ) -> Result<Response<proto::UpsertResponse>, Status> {
        let principal = crate::authorization::grpc_principal(&request)?;
        let _write_guard = self.admit_foreground_write()?;
        let request = request.into_inner();
        let collection_name = request.collection_id;
        let scope = crate::data_plane_request::resolve_existing_scope(
            &self.state,
            &principal,
            &collection_name,
        )
        .await
        .map_err(map_data_plane_request_error)?;
        crate::authorization::grpc_authorize_collection(
            &self.state,
            &principal,
            crate::AuthorizationAction::CollectionWrite,
            &collection_name,
        )?;
        let record = pending_record(
            request
                .record
                .ok_or_else(|| Status::invalid_argument("record is required"))?,
        )?;
        let sequence = WriteService::new(self.state.clone())
            .upsert_scoped(&scope, record)
            .await
            .map_err(map_write_error)?;
        Ok(Response::new(proto::UpsertResponse {
            sequence_number: sequence.get(),
        }))
    }
    async fn batch_upsert(
        &self,
        request: Request<proto::BatchUpsertRequest>,
    ) -> Result<Response<proto::BatchUpsertResponse>, Status> {
        let principal = crate::authorization::grpc_principal(&request)?;
        let _write_guard = self.admit_foreground_write()?;
        let request = request.into_inner();
        let collection_name = request.collection_id;
        let scope = crate::data_plane_request::resolve_existing_scope(
            &self.state,
            &principal,
            &collection_name,
        )
        .await
        .map_err(map_data_plane_request_error)?;
        crate::authorization::grpc_authorize_collection(
            &self.state,
            &principal,
            crate::AuthorizationAction::CollectionWrite,
            &collection_name,
        )?;
        let records = request
            .records
            .into_iter()
            .map(pending_record)
            .collect::<Result<Vec<_>, _>>()?;
        let sequences = WriteService::new(self.state.clone())
            .upsert_batch_scoped(&scope, records)
            .await
            .map_err(map_write_error)?;
        Ok(Response::new(proto::BatchUpsertResponse {
            sequence_numbers: sequences.into_iter().map(|v| v.get()).collect(),
        }))
    }
    async fn delete(
        &self,
        request: Request<proto::DeleteRecordRequest>,
    ) -> Result<Response<proto::DeleteRecordResponse>, Status> {
        let principal = crate::authorization::grpc_principal(&request)?;
        let _write_guard = self.admit_foreground_write()?;
        let request = request.into_inner();
        let collection_name = request.collection_id;
        let scope = crate::data_plane_request::resolve_existing_scope(
            &self.state,
            &principal,
            &collection_name,
        )
        .await
        .map_err(map_data_plane_request_error)?;
        crate::authorization::grpc_authorize_collection(
            &self.state,
            &principal,
            crate::AuthorizationAction::CollectionWrite,
            &collection_name,
        )?;
        let id = record_id_from_proto(
            request
                .id
                .ok_or_else(|| Status::invalid_argument("record id is required"))?,
        )?;
        let sequence = WriteService::new(self.state.clone())
            .delete_scoped(&scope, id)
            .await
            .map_err(map_write_error)?;
        Ok(Response::new(proto::DeleteRecordResponse {
            sequence_number: sequence.get(),
        }))
    }
}

#[tonic::async_trait]
impl Query for GrpcApi {
    async fn query(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<proto::QueryResponse>, Status> {
        let principal = crate::authorization::grpc_principal(&request)?;
        let request = request.into_inner();
        let collection_name = request.collection_id;
        let scope = crate::data_plane_request::resolve_existing_scope(
            &self.state,
            &principal,
            &collection_name,
        )
        .await
        .map_err(map_data_plane_request_error)?;
        let collection_id = scope.collection_id().clone();
        crate::authorization::grpc_authorize_collection(
            &self.state,
            &principal,
            crate::AuthorizationAction::CollectionRead,
            &collection_name,
        )?;
        let metric = metric_from_proto(request.metric)?;
        let top_k = usize::try_from(request.top_k)
            .map_err(|_| Status::invalid_argument("top_k is too large"))?;
        let preference = execution_from_proto(request.execution)?;
        let mut query =
            StorageQueryRequest::new(collection_id.clone(), request.vector, metric, top_k)
                .with_preference(preference);
        if let Some(predicate) = request.predicate {
            query = query.with_predicate(predicate_from_proto(predicate)?);
        }
        let catalog = self.state.catalog.read().await;
        let runtime = catalog.collections.get(&collection_id).ok_or_else(|| {
            Status::not_found(format!("collection '{}' was not found", collection_id))
        })?;
        if runtime.metric != metric {
            return Err(Status::invalid_argument(
                "requested metric does not match collection metric",
            ));
        }
        let segments = runtime
            .query_segments()
            .map_err(|e| Status::internal(format!("failed to build query overlay: {e}")))?;
        if let Some(lexical) = request.lexical {
            let mut fields = lexical
                .fields
                .into_iter()
                .map(|path| field_path(path.segments))
                .collect::<Result<Vec<_>, _>>()?;
            let configured = runtime.configured_lexical_fields();
            if !configured.is_empty() {
                if fields.is_empty() {
                    fields = configured.to_vec();
                } else {
                    fields.sort();
                    fields.dedup();
                    if fields != configured {
                        return Err(Status::invalid_argument(
                            "query lexical fields must match the collection lexical configuration",
                        ));
                    }
                    fields = configured.to_vec();
                }
            }
            let lexical_query = LexicalQuery::new(lexical.text, fields)
                .map_err(map_hybrid_error)?
                .with_analyzer(runtime.configured_lexical_analyzer());
            let rrf_k = lexical.rrf_k.unwrap_or(DEFAULT_RRF_K);
            let collection_directory = match ketebe_storage::ScopedStorageNamespace::open_existing(
                &*self.state.data_dir,
                scope.clone(),
            ) {
                Ok(namespace) => namespace.root().to_path_buf(),
                Err(ketebe_storage::NamespaceError::MissingNamespace(_))
                    if !scope.collection_id().as_str().starts_with("c_") =>
                {
                    self.state
                        .data_dir
                        .join("collections")
                        .join(scope.collection_id().as_str())
                }
                Err(error) => {
                    return Err(Status::internal(format!(
                        "storage scope validation failed: {error}"
                    )));
                }
            };
            let persistent_index = runtime
                .query_lexical_index(&collection_directory, lexical_query.fields())
                .map_err(|error| {
                    Status::internal(format!("lexical index lifecycle failure: {error}"))
                })?;
            let response = if let Some(index) = persistent_index {
                execute_hybrid_query_with_index(
                    &query,
                    &lexical_query,
                    &index,
                    &segments,
                    runtime.query_hnsw(),
                    rrf_k,
                )
            } else {
                execute_hybrid_query(
                    &query,
                    &lexical_query,
                    &segments,
                    runtime.query_hnsw(),
                    rrf_k,
                )
            }
            .map_err(map_hybrid_error)?;
            let hits = response
                .hits()
                .iter()
                .map(|hit| {
                    Ok(proto::SearchHit {
                        id: Some(record_id_to_proto(hit.record().id())),
                        score: hit.score(),
                        sequence_number: hit.record().sequence_number().get(),
                        metadata: Some(metadata_to_proto(hit.record().metadata())?),
                        dense_rank: opt_usize_to_u32(hit.dense_rank(), "dense_rank")?,
                        lexical_rank: opt_usize_to_u32(hit.lexical_rank(), "lexical_rank")?,
                        dense_score: hit.dense_score(),
                        lexical_score: hit.lexical_score(),
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;
            let hybrid = response.explain();
            let dense = hybrid.dense();
            return Ok(Response::new(proto::QueryResponse {
                hits,
                explain: Some(explain_to_proto(
                    dense,
                    true,
                    Some(hybrid.dense_candidates()),
                    Some(hybrid.lexical_candidates()),
                    Some(hybrid.rrf_k()),
                )?),
            }));
        }
        let response =
            execute_query(&query, &segments, runtime.query_hnsw()).map_err(map_planner_error)?;
        let hits = response
            .hits()
            .iter()
            .map(|hit| {
                Ok(proto::SearchHit {
                    id: Some(record_id_to_proto(hit.record().id())),
                    score: hit.score(),
                    sequence_number: hit.record().sequence_number().get(),
                    metadata: Some(metadata_to_proto(hit.record().metadata())?),
                    dense_rank: None,
                    lexical_rank: None,
                    dense_score: None,
                    lexical_score: None,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(Response::new(proto::QueryResponse {
            hits,
            explain: Some(explain_to_proto(
                response.explain(),
                false,
                None,
                None,
                None,
            )?),
        }))
    }
}

fn explain_to_proto(
    explain: &ketebe_storage::SearchExplain,
    hybrid: bool,
    dense_candidates: Option<usize>,
    lexical_candidates: Option<usize>,
    rrf_k: Option<u32>,
) -> Result<proto::SearchExplain, Status> {
    Ok(proto::SearchExplain {
        strategy: strategy_name(explain.strategy()).to_string(),
        reason: reason_name(explain.reason()).to_string(),
        collection_id: explain.collection_id().as_str().to_string(),
        metric: metric_to_proto(explain.metric()) as i32,
        top_k: usize_to_u32(explain.top_k(), "top_k")?,
        has_predicate: explain.has_predicate(),
        candidate_limit: opt_usize_to_u32(explain.candidate_limit(), "candidate_limit")?,
        fallback: explain.fallback(),
        hybrid,
        dense_candidates: opt_usize_to_u32(dense_candidates, "dense_candidates")?,
        lexical_candidates: opt_usize_to_u32(lexical_candidates, "lexical_candidates")?,
        rrf_k,
    })
}
fn opt_usize_to_u32(value: Option<usize>, field: &'static str) -> Result<Option<u32>, Status> {
    value.map(|v| usize_to_u32(v, field)).transpose()
}
fn metric_from_proto(value: i32) -> Result<DistanceMetric, Status> {
    match proto::DistanceMetric::try_from(value)
        .map_err(|_| Status::invalid_argument("unknown distance metric"))?
    {
        proto::DistanceMetric::Unspecified => {
            Err(Status::invalid_argument("distance metric is required"))
        }
        proto::DistanceMetric::Cosine => Ok(DistanceMetric::Cosine),
        proto::DistanceMetric::Dot => Ok(DistanceMetric::Dot),
        proto::DistanceMetric::L2 => Ok(DistanceMetric::L2),
    }
}
const fn metric_to_proto(value: DistanceMetric) -> proto::DistanceMetric {
    match value {
        DistanceMetric::Cosine => proto::DistanceMetric::Cosine,
        DistanceMetric::Dot => proto::DistanceMetric::Dot,
        DistanceMetric::L2 => proto::DistanceMetric::L2,
    }
}
fn execution_from_proto(value: i32) -> Result<ExecutionPreference, Status> {
    match proto::ExecutionPreference::try_from(value)
        .map_err(|_| Status::invalid_argument("unknown execution preference"))?
    {
        proto::ExecutionPreference::Auto => Ok(ExecutionPreference::Auto),
        proto::ExecutionPreference::Exact => Ok(ExecutionPreference::Exact),
        proto::ExecutionPreference::Hnsw => Ok(ExecutionPreference::Hnsw),
    }
}
fn pending_record(record: proto::RecordInput) -> Result<PendingRecord, Status> {
    let id = record_id_from_proto(
        record
            .id
            .ok_or_else(|| Status::invalid_argument("record id is required"))?,
    )?;
    let metadata = record
        .metadata
        .map(metadata_from_proto)
        .transpose()?
        .unwrap_or_default();
    Ok(PendingRecord {
        id,
        vector: record.vector,
        metadata,
    })
}
fn record_id_from_proto(value: proto::RecordId) -> Result<RecordId, Status> {
    match value.value {
        Some(proto::record_id::Value::StringValue(v)) => {
            RecordId::string(v).map_err(|e| Status::invalid_argument(e.to_string()))
        }
        Some(proto::record_id::Value::U64Value(v)) => Ok(RecordId::unsigned(v)),
        None => Err(Status::invalid_argument("record id value is required")),
    }
}
fn record_id_to_proto(value: &RecordId) -> proto::RecordId {
    let value = match value {
        RecordId::String(v) => proto::record_id::Value::StringValue(v.clone()),
        RecordId::Unsigned(v) => proto::record_id::Value::U64Value(*v),
    };
    proto::RecordId { value: Some(value) }
}
fn metadata_from_proto(value: proto::MetadataObject) -> Result<Metadata, Status> {
    value
        .fields
        .into_iter()
        .map(|(k, v)| Ok((k, metadata_value_from_proto(v)?)))
        .collect()
}
fn metadata_value_from_proto(value: proto::MetadataValue) -> Result<MetadataValue, Status> {
    match value.kind {
        Some(proto::metadata_value::Kind::NullValue(_)) => Ok(MetadataValue::Null),
        Some(proto::metadata_value::Kind::BoolValue(v)) => Ok(MetadataValue::Bool(v)),
        Some(proto::metadata_value::Kind::NumberValue(v)) if v.is_finite() => {
            Ok(MetadataValue::Number(v))
        }
        Some(proto::metadata_value::Kind::NumberValue(_)) => {
            Err(Status::invalid_argument("metadata numbers must be finite"))
        }
        Some(proto::metadata_value::Kind::StringValue(v)) => Ok(MetadataValue::String(v)),
        Some(proto::metadata_value::Kind::ArrayValue(v)) => Ok(MetadataValue::Array(
            v.values
                .into_iter()
                .map(metadata_value_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Some(proto::metadata_value::Kind::ObjectValue(v)) => {
            Ok(MetadataValue::Object(metadata_from_proto(v)?))
        }
        None => Err(Status::invalid_argument("metadata value kind is required")),
    }
}
fn metadata_to_proto(value: &Metadata) -> Result<proto::MetadataObject, Status> {
    Ok(proto::MetadataObject {
        fields: value
            .iter()
            .map(|(k, v)| Ok((k.clone(), metadata_value_to_proto(v)?)))
            .collect::<Result<_, Status>>()?,
    })
}
fn metadata_value_to_proto(value: &MetadataValue) -> Result<proto::MetadataValue, Status> {
    let kind = match value {
        MetadataValue::Null => proto::metadata_value::Kind::NullValue(0),
        MetadataValue::Bool(v) => proto::metadata_value::Kind::BoolValue(*v),
        MetadataValue::Number(v) if v.is_finite() => proto::metadata_value::Kind::NumberValue(*v),
        MetadataValue::Number(_) => {
            return Err(Status::internal(
                "stored metadata contains a non-finite number",
            ));
        }
        MetadataValue::String(v) => proto::metadata_value::Kind::StringValue(v.clone()),
        MetadataValue::Array(values) => {
            proto::metadata_value::Kind::ArrayValue(proto::MetadataArray {
                values: values
                    .iter()
                    .map(metadata_value_to_proto)
                    .collect::<Result<Vec<_>, _>>()?,
            })
        }
        MetadataValue::Object(values) => {
            proto::metadata_value::Kind::ObjectValue(metadata_to_proto(values)?)
        }
    };
    Ok(proto::MetadataValue { kind: Some(kind) })
}
fn predicate_from_proto(value: proto::Predicate) -> Result<Predicate, Status> {
    use proto::predicate::Kind;
    match value.kind {
        Some(Kind::Eq(v)) => comparison(v, Predicate::Eq),
        Some(Kind::Ne(v)) => comparison(v, Predicate::Ne),
        Some(Kind::Lt(v)) => comparison(v, Predicate::Lt),
        Some(Kind::Lte(v)) => comparison(v, Predicate::Lte),
        Some(Kind::Gt(v)) => comparison(v, Predicate::Gt),
        Some(Kind::Gte(v)) => comparison(v, Predicate::Gte),
        Some(Kind::Exists(v)) => Ok(Predicate::Exists(field_path(v.path)?)),
        Some(Kind::InValues(v)) => Ok(Predicate::In(
            field_path(v.path)?,
            v.values
                .into_iter()
                .map(metadata_value_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Some(Kind::Contains(v)) => comparison(v, Predicate::Contains),
        Some(Kind::And(v)) => Ok(Predicate::And(
            v.predicates
                .into_iter()
                .map(predicate_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Some(Kind::Or(v)) => Ok(Predicate::Or(
            v.predicates
                .into_iter()
                .map(predicate_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Some(Kind::Not(v)) => Ok(Predicate::Not(Box::new(predicate_from_proto(*v)?))),
        None => Err(Status::invalid_argument("predicate kind is required")),
    }
}
fn comparison(
    value: proto::ComparisonPredicate,
    constructor: fn(FieldPath, MetadataValue) -> Predicate,
) -> Result<Predicate, Status> {
    let path = field_path(value.path)?;
    let value = value
        .value
        .ok_or_else(|| Status::invalid_argument("predicate value is required"))?;
    Ok(constructor(path, metadata_value_from_proto(value)?))
}
fn field_path(path: Vec<String>) -> Result<FieldPath, Status> {
    FieldPath::new(path).map_err(|e| Status::invalid_argument(e.to_string()))
}
fn analyzer_from_proto(
    value: Option<proto::LexicalAnalyzerConfig>,
) -> Result<LexicalAnalyzerConfig, Status> {
    let Some(value) = value else {
        return Ok(LexicalAnalyzerConfig::default());
    };
    match proto::LexicalAnalyzerKind::try_from(value.kind)
        .unwrap_or(proto::LexicalAnalyzerKind::LexicalAnalyzerUnspecified)
    {
        proto::LexicalAnalyzerKind::LexicalAnalyzerUnspecified
        | proto::LexicalAnalyzerKind::Standard => {
            Ok(LexicalAnalyzerConfig::standard(value.lowercase))
        }
    }
}

fn lexical_state_name(state: &LexicalBuildState) -> &'static str {
    match state {
        LexicalBuildState::Disabled => "disabled",
        LexicalBuildState::Missing { .. } => "missing",
        LexicalBuildState::Queued { .. } => "queued",
        LexicalBuildState::Building { .. } => "building",
        LexicalBuildState::Retrying { .. } => "retrying",
        LexicalBuildState::Ready { .. } => "ready",
        LexicalBuildState::Stale { .. } => "stale",
        LexicalBuildState::Failed { .. } => "failed",
    }
}

fn ingestion_from_proto(
    value: Option<proto::CollectionIngestionSchema>,
) -> Result<Option<CollectionIngestionConfig>, Status> {
    let Some(value) = value else {
        return Ok(None);
    };
    let chunking = value
        .chunking
        .map(|chunking| {
            let max_chars = usize::try_from(chunking.max_chars)
                .map_err(|_| Status::invalid_argument("chunking max_chars is too large"))?;
            let overlap_chars = usize::try_from(chunking.overlap_chars)
                .map_err(|_| Status::invalid_argument("chunking overlap_chars is too large"))?;
            ChunkingPolicy::new(max_chars, overlap_chars)
                .map_err(|error| Status::invalid_argument(error.to_string()))
        })
        .transpose()?;
    CollectionIngestionConfig::new(value.embedding_profile, chunking, value.index_chunk_text)
        .map(Some)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn ingestion_to_proto(
    value: &CollectionIngestionConfig,
) -> Result<proto::CollectionIngestionSchema, Status> {
    Ok(proto::CollectionIngestionSchema {
        embedding_profile: value.embedding_profile().to_string(),
        chunking: value
            .chunking()
            .map(|chunking| {
                Ok::<proto::ChunkingPolicy, Status>(proto::ChunkingPolicy {
                    max_chars: usize_to_u32(chunking.max_chars(), "chunking.max_chars")?,
                    overlap_chars: usize_to_u32(
                        chunking.overlap_chars(),
                        "chunking.overlap_chars",
                    )?,
                })
            })
            .transpose()?,
        index_chunk_text: value.index_chunk_text(),
    })
}

fn collection_to_proto_named(
    info: CollectionInfo,
    name: String,
) -> Result<proto::Collection, Status> {
    let mut collection = collection_to_proto(info)?;
    collection.id = name;
    Ok(collection)
}

fn collection_to_proto(info: CollectionInfo) -> Result<proto::Collection, Status> {
    let stats = proto::CollectionStats {
        live_records: usize_to_u64(info.live_records, "live_records")?,
        tombstones: usize_to_u64(info.tombstones, "tombstones")?,
        immutable_segments: usize_to_u64(info.immutable_segments, "immutable_segments")?,
        mutable_mutations: usize_to_u64(info.mutable_mutations, "mutable_mutations")?,
        checkpoint_sequence: info.checkpoint_sequence,
        next_sequence: info.next_sequence,
    };
    let hnsw_config = match info.hnsw_config {
        Some(c) => Some(proto::HnswConfig {
            m: usize_to_u32(c.m, "hnsw.m")?,
            ef_construction: usize_to_u32(c.ef_construction, "hnsw.ef_construction")?,
            ef_search: usize_to_u32(c.ef_search, "hnsw.ef_search")?,
        }),
        None => None,
    };
    Ok(proto::Collection {
        id: info.id.as_str().to_string(),
        dimension: usize_to_u32(info.dimension, "dimension")?,
        metric: metric_to_proto(info.metric) as i32,
        stats: Some(stats),
        hnsw_ready: matches!(info.hnsw_state, HnswState::Ready),
        hnsw_config,
        lexical_fields: info
            .lexical_fields
            .into_iter()
            .map(|path| proto::FieldPath {
                segments: path.segments().to_vec(),
            })
            .collect(),
        lexical_index_state: lexical_state_name(&info.lexical_state).to_string(),
        lexical_analyzer: Some(proto::LexicalAnalyzerConfig {
            kind: proto::LexicalAnalyzerKind::Standard as i32,
            lowercase: info.lexical_analyzer.lowercase(),
        }),
        ingestion: info
            .ingestion
            .as_ref()
            .map(ingestion_to_proto)
            .transpose()?,
    })
}
fn usize_to_u32(value: usize, field: &'static str) -> Result<u32, Status> {
    u32::try_from(value)
        .map_err(|_| Status::internal(format!("{field} cannot be represented in protobuf")))
}
fn usize_to_u64(value: usize, field: &'static str) -> Result<u64, Status> {
    u64::try_from(value)
        .map_err(|_| Status::internal(format!("{field} cannot be represented in protobuf")))
}
fn strategy_name(value: ExecutionStrategy) -> &'static str {
    match value {
        ExecutionStrategy::Exact => "exact",
        ExecutionStrategy::Hnsw => "hnsw",
        ExecutionStrategy::HnswPostFilter => "hnsw_post_filter",
    }
}
fn reason_name(value: PlanReason) -> &'static str {
    match value {
        PlanReason::ExplicitExact => "explicit_exact",
        PlanReason::ExplicitHnsw => "explicit_hnsw",
        PlanReason::ExplicitHnswWithPredicate => "explicit_hnsw_with_predicate",
        PlanReason::AutoHnswAvailable => "auto_hnsw_available",
        PlanReason::AutoHnswWithPredicate => "auto_hnsw_with_predicate",
        PlanReason::AutoExactFallbackNoHnsw => "auto_exact_fallback_no_hnsw",
    }
}
fn map_data_plane_request_error(error: crate::data_plane_request::DataPlaneRequestError) -> Status {
    match error {
        crate::data_plane_request::DataPlaneRequestError::InvalidCollectionName(message) => {
            Status::invalid_argument(message)
        }
        crate::data_plane_request::DataPlaneRequestError::CollectionNotFound => {
            Status::not_found("collection was not found")
        }
        crate::data_plane_request::DataPlaneRequestError::Resolution(
            crate::DataPlaneResolutionError::Catalog(
                crate::CollectionNamespaceError::NameAlreadyExists,
            ),
        )
        | crate::data_plane_request::DataPlaneRequestError::NamespaceCatalog(
            crate::CollectionNamespaceError::NameAlreadyExists,
        ) => Status::already_exists("collection already exists in project"),
        crate::data_plane_request::DataPlaneRequestError::Resolution(
            crate::DataPlaneResolutionError::MissingProjectScope
            | crate::DataPlaneResolutionError::InvalidProjectScope(_),
        ) => Status::permission_denied(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

fn map_write_error(error: WriteError) -> Status {
    match error {
        WriteError::Validation(m) => Status::invalid_argument(m),
        WriteError::CollectionAlreadyExists(id) => {
            Status::already_exists(format!("collection '{}' already exists", id.as_str()))
        }
        WriteError::CollectionNotFound(id) => {
            Status::not_found(format!("collection '{}' was not found", id.as_str()))
        }
        WriteError::CollectionNotWritable => Status::internal("collection runtime is not writable"),
        WriteError::Scope(message) => Status::internal(message),
        WriteError::Io(e) => Status::internal(format!("write storage failure: {e}")),
        WriteError::Json(e) => Status::internal(format!("write metadata failure: {e}")),
        WriteError::Wal(e) => Status::internal(format!("write WAL failure: {e}")),
        WriteError::Segment(e) => Status::internal(format!("write segment failure: {e}")),
        WriteError::Checkpoint(e) => Status::internal(format!("write checkpoint failure: {e}")),
        WriteError::Compaction(e) => Status::internal(format!("write compaction failure: {e}")),
        WriteError::WalReclaim(e) => Status::internal(format!("write WAL reclaim failure: {e}")),
    }
}
fn map_management_error(error: ManagementError) -> Status {
    match error {
        ManagementError::CollectionNotFound(id) => {
            Status::not_found(format!("collection '{}' was not found", id.as_str()))
        }
        ManagementError::CollectionNotManageable => {
            Status::internal("collection runtime is not manageable")
        }
        ManagementError::Io(e) => {
            Status::internal(format!("collection management I/O failure: {e}"))
        }
        ManagementError::Segment(e) => {
            Status::internal(format!("collection management segment failure: {e}"))
        }
        ManagementError::Scope(message) => Status::internal(message),
    }
}
fn map_planner_error(error: PlannerError) -> Status {
    match error {
        PlannerError::MissingHnswIndex => {
            Status::unavailable("HNSW execution was requested but no current index is available")
        }
        PlannerError::HnswCollectionMismatch { .. } => {
            Status::internal("configured HNSW index does not match collection")
        }
        PlannerError::Exact(e) => map_search_error(e),
        PlannerError::Hnsw(e) => map_hnsw_error(e),
        PlannerError::Filtered(e) => map_filtered_error(e),
    }
}
fn map_hybrid_error(error: HybridError) -> Status {
    match error {
        HybridError::Dense(e) => map_planner_error(e),
        HybridError::EmptyLexicalQuery
        | HybridError::EmptyLexicalFields
        | HybridError::InvalidTopK
        | HybridError::InvalidRrfK
        | HybridError::CandidateDepthBelowTopK
        | HybridError::CandidateBudgetExceeded { .. }
        | HybridError::Predicate(_) => Status::invalid_argument(error.to_string()),
        HybridError::LexicalIndexMismatch | HybridError::Index(_) => {
            Status::internal(error.to_string())
        }
    }
}
fn map_search_error(error: SearchError) -> Status {
    let message = error.to_string();
    match error {
        SearchError::InvalidTopK
        | SearchError::EmptyQueryVector
        | SearchError::NonFiniteQueryValue { .. }
        | SearchError::DimensionMismatch { .. }
        | SearchError::ZeroNormVector
        | SearchError::Predicate(_) => Status::invalid_argument(message),
        SearchError::Segment(_) | SearchError::Control(_) => Status::internal(message),
    }
}
fn map_hnsw_error(error: HnswError) -> Status {
    let message = error.to_string();
    match error {
        HnswError::InvalidTopK
        | HnswError::EfSearchTooSmall { .. }
        | HnswError::EmptyQueryVector
        | HnswError::NonFiniteQueryValue { .. }
        | HnswError::DimensionMismatch { .. }
        | HnswError::ZeroNormVector => Status::invalid_argument(message),
        HnswError::InvalidConfig(_)
        | HnswError::InvalidGraph(_)
        | HnswError::ExactSearch(_)
        | HnswError::Control(_) => Status::internal(message),
    }
}
fn map_filtered_error(error: FilteredSearchError) -> Status {
    match error {
        FilteredSearchError::Exact(e) => map_search_error(e),
        FilteredSearchError::Hnsw(e) => map_hnsw_error(e),
        FilteredSearchError::Predicate(e) => Status::invalid_argument(e.to_string()),
    }
}
