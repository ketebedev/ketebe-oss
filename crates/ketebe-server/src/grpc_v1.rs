use ketebe_core::{FieldPath, Metadata, MetadataValue, Predicate, RecordId};
use ketebe_storage::{DEFAULT_RRF_K, ExecutionPreference};
use tonic::{Request, Response, Status};

use crate::cursor::CursorError;
use crate::query_v1::{
    QueryModeV1, QueryPaginationV1, QueryRerankV1, QueryV1Error, QueryV1Request,
    execute_query_v1_page,
};
use crate::reranking::{RerankFailurePolicy, RerankingError};
use crate::runtime::AppState;

pub mod proto {
    tonic::include_proto!("ketebe.v1");
}

#[derive(Clone)]
pub(crate) struct GrpcQueryV1 {
    state: AppState,
}

pub(crate) fn server(state: AppState) -> proto::query_server::QueryServer<GrpcQueryV1> {
    proto::query_server::QueryServer::new(GrpcQueryV1 { state })
}

#[tonic::async_trait]
impl proto::query_server::Query for GrpcQueryV1 {
    async fn query(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<proto::QueryResponse>, Status> {
        let principal = crate::authorization::grpc_principal(&request)?;
        let request = request.into_inner();
        let collection_name = request.collection_id.clone();
        let scope = crate::data_plane_request::resolve_existing_scope(
            &self.state,
            &principal,
            &collection_name,
        )
        .await
        .map_err(map_data_plane_request_error)?;
        let collection_id = scope.collection_id().clone();
        crate::data_plane_request::scope_for_collection_id(&self.state, &collection_id)
            .map_err(|error| Status::internal(error.to_string()))?
            .filter(|resolved| resolved == &scope)
            .ok_or_else(|| Status::permission_denied("data-plane scope mismatch"))?;
        crate::authorization::grpc_authorize_collection(
            &self.state,
            &principal,
            crate::AuthorizationAction::CollectionRead,
            &collection_name,
        )?;
        let dense = request.dense;
        let lexical = request.lexical;
        let top_k = if request.top_k == 0 {
            10
        } else {
            usize::try_from(request.top_k)
                .map_err(|_| Status::invalid_argument("top_k is too large"))?
        };
        let predicate = request.predicate.map(predicate_from_proto).transpose()?;
        let execution = execution_from_proto(request.execution)?;
        let include_metadata = request.include_metadata.unwrap_or(true);
        let rerank = request.rerank.map(rerank_from_proto).transpose()?;
        let domain = QueryV1Request {
            collection_id,
            vector: dense.as_ref().map(|value| value.vector.clone()),
            text: lexical.as_ref().map(|value| value.text.clone()),
            top_k,
            predicate,
            execution,
            dense_candidates: dense
                .as_ref()
                .and_then(|value| value.candidates)
                .map(|value| usize::try_from(value).expect("u32 fits usize")),
            lexical_candidates: lexical
                .as_ref()
                .and_then(|value| value.candidates)
                .map(|value| usize::try_from(value).expect("u32 fits usize")),
            rrf_k: lexical
                .as_ref()
                .and_then(|value| value.rrf_k)
                .unwrap_or(DEFAULT_RRF_K),
            search_profile: request.search_profile,
            include_metadata,
            include_provenance: request.include_provenance,
            explain: request.explain,
            timeout_ms: request.timeout_ms,
            rerank,
        };
        let page = execute_query_v1_page(
            &self.state,
            domain,
            QueryPaginationV1 {
                enabled: request.paginate,
                cursor: request.cursor,
            },
        )
        .await
        .map_err(map_query_error)?;
        let response = page.response;
        let hits = response
            .hits
            .into_iter()
            .map(|hit| {
                Ok(proto::SearchHit {
                    id: Some(record_id_to_proto(&hit.id)),
                    score: hit.score,
                    sequence_number: hit.sequence_number,
                    metadata: include_metadata
                        .then(|| metadata_to_proto(&hit.metadata))
                        .transpose()?,
                    dense_rank: usize_to_u32_option(hit.dense_rank, "dense_rank")?,
                    lexical_rank: usize_to_u32_option(hit.lexical_rank, "lexical_rank")?,
                    dense_score: hit.dense_score,
                    lexical_score: hit.lexical_score,
                    rerank_score: hit.rerank_score,
                    original_rank: usize_to_u32_option(hit.original_rank, "original_rank")?,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let explain = response
            .explain
            .map(|explain| {
                Ok::<proto::SearchExplain, Status>(proto::SearchExplain {
                    mode: mode_name(explain.mode).to_string(),
                    strategy: explain.strategy,
                    reason: explain.reason,
                    top_k: usize_to_u32(explain.top_k, "top_k")?,
                    dense_candidates: usize_to_u32_option(
                        explain.dense_candidates,
                        "dense_candidates",
                    )?,
                    lexical_candidates: usize_to_u32_option(
                        explain.lexical_candidates,
                        "lexical_candidates",
                    )?,
                    rrf_k: explain.rrf_k,
                    has_predicate: explain.has_predicate,
                    search_profile: explain.search_profile,
                    timeout_ms: explain.timeout_ms,
                    rerank: explain
                        .rerank
                        .map(|rerank| {
                            Ok::<proto::RerankExplain, Status>(proto::RerankExplain {
                                profile: rerank.profile,
                                provider: rerank.provider,
                                input_candidates: usize_to_u32(
                                    rerank.input_candidates,
                                    "rerank_input_candidates",
                                )?,
                                applied: rerank.applied,
                                fallback_reason: rerank.fallback_reason,
                            })
                        })
                        .transpose()?,
                })
            })
            .transpose()?;
        Ok(Response::new(proto::QueryResponse {
            api_version: "v1".to_string(),
            hits,
            explain,
            next_cursor: page.next_cursor,
        }))
    }
}

const fn mode_name(mode: QueryModeV1) -> &'static str {
    match mode {
        QueryModeV1::Dense => "dense",
        QueryModeV1::Lexical => "lexical",
        QueryModeV1::Hybrid => "hybrid",
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

fn map_data_plane_request_error(error: crate::data_plane_request::DataPlaneRequestError) -> Status {
    match error {
        crate::data_plane_request::DataPlaneRequestError::InvalidCollectionName(message) => {
            Status::invalid_argument(message)
        }
        crate::data_plane_request::DataPlaneRequestError::CollectionNotFound => {
            Status::not_found("collection was not found")
        }
        crate::data_plane_request::DataPlaneRequestError::Resolution(
            crate::DataPlaneResolutionError::MissingProjectScope
            | crate::DataPlaneResolutionError::InvalidProjectScope(_),
        ) => Status::permission_denied(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

fn map_query_error(error: QueryV1Error) -> Status {
    match error {
        QueryV1Error::Invalid(message) => Status::invalid_argument(message),
        QueryV1Error::CollectionNotFound(message) => Status::not_found(message),
        QueryV1Error::LexicalNotConfigured => Status::failed_precondition(error.to_string()),
        QueryV1Error::UnsupportedSearchProfile(_) | QueryV1Error::RerankerProfileNotFound(_) => {
            Status::invalid_argument(error.to_string())
        }
        QueryV1Error::Reranking(RerankingError::Provider(_)) => {
            Status::unavailable(error.to_string())
        }
        QueryV1Error::Reranking(_) => Status::invalid_argument(error.to_string()),
        QueryV1Error::ResourceLimit(_) => Status::invalid_argument(error.to_string()),
        QueryV1Error::Overloaded => Status::resource_exhausted(error.to_string()),
        QueryV1Error::DeadlineExceeded => Status::deadline_exceeded(error.to_string()),
        QueryV1Error::Cancelled => Status::cancelled(error.to_string()),
        QueryV1Error::Cursor(
            CursorError::Expired | CursorError::StaleSnapshot | CursorError::QueryMismatch,
        ) => Status::failed_precondition(error.to_string()),
        QueryV1Error::Cursor(_) | QueryV1Error::CursorUnsupported(_) => {
            Status::invalid_argument(error.to_string())
        }
        QueryV1Error::Search(_)
        | QueryV1Error::Planner(_)
        | QueryV1Error::Hybrid(_)
        | QueryV1Error::Internal(_) => Status::internal(error.to_string()),
    }
}

fn rerank_from_proto(value: proto::RerankQuery) -> Result<QueryRerankV1, Status> {
    let profile = if value.profile.trim().is_empty() {
        "default".to_string()
    } else {
        value.profile
    };
    let top_n = usize::try_from(value.top_n)
        .map_err(|_| Status::invalid_argument("rerank top_n is too large"))?;
    let text_fields = value
        .text_fields
        .into_iter()
        .map(|field| {
            FieldPath::new(field.segments)
                .map_err(|error| Status::invalid_argument(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let failure_policy = match proto::RerankFailurePolicy::try_from(value.failure_policy)
        .map_err(|_| Status::invalid_argument("unknown rerank failure policy"))?
    {
        proto::RerankFailurePolicy::RerankFail => RerankFailurePolicy::Fail,
        proto::RerankFailurePolicy::RerankPreserveCandidateOrder => {
            RerankFailurePolicy::PreserveCandidateOrder
        }
    };
    Ok(QueryRerankV1 {
        profile,
        query: value.query,
        top_n,
        text_fields,
        include_metadata: value.include_metadata,
        failure_policy,
    })
}

fn predicate_from_proto(value: proto::Predicate) -> Result<Predicate, Status> {
    use proto::predicate::Kind;
    match value.kind {
        Some(Kind::Eq(value)) => comparison(value, Predicate::Eq),
        Some(Kind::Ne(value)) => comparison(value, Predicate::Ne),
        Some(Kind::Lt(value)) => comparison(value, Predicate::Lt),
        Some(Kind::Lte(value)) => comparison(value, Predicate::Lte),
        Some(Kind::Gt(value)) => comparison(value, Predicate::Gt),
        Some(Kind::Gte(value)) => comparison(value, Predicate::Gte),
        Some(Kind::Exists(value)) => Ok(Predicate::Exists(field_path(value.path)?)),
        Some(Kind::InValues(value)) => Ok(Predicate::In(
            field_path(value.path)?,
            value
                .values
                .into_iter()
                .map(metadata_value_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Some(Kind::Contains(value)) => comparison(value, Predicate::Contains),
        Some(Kind::And(value)) => Ok(Predicate::And(
            value
                .predicates
                .into_iter()
                .map(predicate_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Some(Kind::Or(value)) => Ok(Predicate::Or(
            value
                .predicates
                .into_iter()
                .map(predicate_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Some(Kind::Not(value)) => Ok(Predicate::Not(Box::new(predicate_from_proto(*value)?))),
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
    FieldPath::new(path).map_err(|error| Status::invalid_argument(error.to_string()))
}

fn metadata_value_from_proto(value: proto::MetadataValue) -> Result<MetadataValue, Status> {
    match value.kind {
        Some(proto::metadata_value::Kind::NullValue(_)) => Ok(MetadataValue::Null),
        Some(proto::metadata_value::Kind::BoolValue(value)) => Ok(MetadataValue::Bool(value)),
        Some(proto::metadata_value::Kind::NumberValue(value)) if value.is_finite() => {
            Ok(MetadataValue::Number(value))
        }
        Some(proto::metadata_value::Kind::NumberValue(_)) => {
            Err(Status::invalid_argument("metadata numbers must be finite"))
        }
        Some(proto::metadata_value::Kind::StringValue(value)) => Ok(MetadataValue::String(value)),
        Some(proto::metadata_value::Kind::ArrayValue(value)) => Ok(MetadataValue::Array(
            value
                .values
                .into_iter()
                .map(metadata_value_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Some(proto::metadata_value::Kind::ObjectValue(value)) => Ok(MetadataValue::Object(
            value
                .fields
                .into_iter()
                .map(|(key, value)| Ok((key, metadata_value_from_proto(value)?)))
                .collect::<Result<Metadata, Status>>()?,
        )),
        None => Err(Status::invalid_argument("metadata value kind is required")),
    }
}

fn metadata_to_proto(value: &Metadata) -> Result<proto::MetadataObject, Status> {
    Ok(proto::MetadataObject {
        fields: value
            .iter()
            .map(|(key, value)| Ok((key.clone(), metadata_value_to_proto(value)?)))
            .collect::<Result<_, Status>>()?,
    })
}

fn metadata_value_to_proto(value: &MetadataValue) -> Result<proto::MetadataValue, Status> {
    let kind = match value {
        MetadataValue::Null => proto::metadata_value::Kind::NullValue(0),
        MetadataValue::Bool(value) => proto::metadata_value::Kind::BoolValue(*value),
        MetadataValue::Number(value) if value.is_finite() => {
            proto::metadata_value::Kind::NumberValue(*value)
        }
        MetadataValue::Number(_) => {
            return Err(Status::internal(
                "stored metadata contains a non-finite number",
            ));
        }
        MetadataValue::String(value) => proto::metadata_value::Kind::StringValue(value.clone()),
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

fn record_id_to_proto(value: &RecordId) -> proto::RecordId {
    let value = match value {
        RecordId::String(value) => proto::record_id::Value::StringValue(value.clone()),
        RecordId::Unsigned(value) => proto::record_id::Value::U64Value(*value),
    };
    proto::RecordId { value: Some(value) }
}

fn usize_to_u32(value: usize, field: &'static str) -> Result<u32, Status> {
    u32::try_from(value)
        .map_err(|_| Status::internal(format!("{field} cannot be represented in protobuf")))
}
fn usize_to_u32_option(value: Option<usize>, field: &'static str) -> Result<Option<u32>, Status> {
    value.map(|value| usize_to_u32(value, field)).transpose()
}
