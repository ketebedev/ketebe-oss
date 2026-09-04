use rmcp::schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

use crate::{
    fusion::{DedupStrategy, FusedSearchOutput, FusedSearchParams, FusionStrategy},
    multi_search::CollectionSearchStatus,
    search::SearchMode,
};

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct ExplainSearchOutput {
    pub plan: ExplainSearchPlan,
    pub diagnostics: ExplainSearchDiagnostics,
    pub result: FusedSearchOutput,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct ExplainSearchPlan {
    pub mode: SearchMode,
    pub collections: Vec<String>,
    pub fusion: FusionStrategy,
    pub dedup: DedupStrategy,
    pub candidate_limit: usize,
    pub final_limit: usize,
    pub rrf_k: u32,
    pub has_filter: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rerank_profile: Option<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct ExplainSearchDiagnostics {
    pub search_latency_ms: u64,
    pub successful_collections: usize,
    pub failed_collections: usize,
    pub returned_hits: usize,
    pub collection_diagnostics: Vec<CollectionDiagnostics>,
    pub stage_latency_semantics: StageLatencySemantics,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
pub struct StageLatencySemantics {
    pub embedding: String,
    pub retrieval: String,
    pub rerank: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct CollectionDiagnostics {
    pub collection: String,
    pub status: CollectionSearchStatus,
    pub returned_hits: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filtered_returned_hits: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dense_candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rerank: Option<RerankDiagnostics>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct RerankDiagnostics {
    pub profile: Option<String>,
    pub provider: Option<String>,
    pub input_candidates: Option<usize>,
    pub applied: Option<bool>,
    pub fallback_reason: Option<String>,
}

impl ExplainSearchOutput {
    #[must_use]
    pub fn from_execution(
        params: &FusedSearchParams,
        search_latency_ms: u64,
        result: FusedSearchOutput,
    ) -> Self {
        let has_filter = params.filter.is_some();
        let collection_diagnostics = result
            .results
            .iter()
            .map(|value| CollectionDiagnostics::from_result(value, has_filter))
            .collect::<Vec<_>>();
        let successful_collections = result
            .results
            .iter()
            .filter(|value| value.status == CollectionSearchStatus::Ok)
            .count();
        let failed_collections = result.results.len().saturating_sub(successful_collections);
        let returned_hits = result.hits.len();
        Self {
            plan: ExplainSearchPlan {
                mode: params.mode,
                collections: params
                    .collections
                    .iter()
                    .map(|value| value.collection.clone())
                    .collect(),
                fusion: params.fusion,
                dedup: params.dedup,
                candidate_limit: params.candidate_limit,
                final_limit: params.final_limit,
                rrf_k: params.rrf_k,
                has_filter,
                rerank_profile: params.rerank.as_ref().map(|value| value.profile.clone()),
            },
            diagnostics: ExplainSearchDiagnostics {
                search_latency_ms,
                successful_collections,
                failed_collections,
                returned_hits,
                collection_diagnostics,
                stage_latency_semantics: StageLatencySemantics {
                    embedding: "included_in_search_latency".into(),
                    retrieval: "included_in_search_latency".into(),
                    rerank: "included_in_search_latency".into(),
                },
            },
            result,
        }
    }
}

impl CollectionDiagnostics {
    fn from_result(value: &crate::multi_search::CollectionSearchResult, has_filter: bool) -> Self {
        let explain = value.explain.as_ref();
        Self {
            collection: value.collection.clone(),
            status: value.status,
            returned_hits: value.hits.len(),
            filtered_returned_hits: has_filter.then_some(value.hits.len()),
            strategy: string_field(explain, "strategy"),
            reason: string_field(explain, "reason"),
            dense_candidates: usize_field(explain, "dense_candidates"),
            sparse_candidates: usize_field(explain, "lexical_candidates"),
            search_profile: string_field(explain, "search_profile"),
            timeout_ms: u64_field(explain, "timeout_ms"),
            rerank: explain
                .and_then(|value| value.get("rerank"))
                .filter(|value| !value.is_null())
                .map(|rerank| RerankDiagnostics {
                    profile: rerank
                        .get("profile")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    provider: rerank
                        .get("provider")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    input_candidates: rerank
                        .get("input_candidates")
                        .and_then(Value::as_u64)
                        .and_then(|value| usize::try_from(value).ok()),
                    applied: rerank.get("applied").and_then(Value::as_bool),
                    fallback_reason: rerank
                        .get("fallback_reason")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                }),
        }
    }
}

fn string_field(value: Option<&Value>, key: &str) -> Option<String> {
    value?.get(key)?.as_str().map(str::to_string)
}

fn usize_field(value: Option<&Value>, key: &str) -> Option<usize> {
    value?
        .get(key)?
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
}

fn u64_field(value: Option<&Value>, key: &str) -> Option<u64> {
    value?.get(key)?.as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fusion::{DedupStrategy, FusionStrategy},
        multi_search::{CollectionSearchResult, SearchManyTarget},
    };
    use serde_json::json;

    #[test]
    fn diagnostics_project_only_safe_public_explain_fields() {
        let params = FusedSearchParams {
            collections: vec![SearchManyTarget {
                collection: "docs".into(),
                search_profile: None,
            }],
            mode: SearchMode::Dense,
            text: None,
            vector: Some(vec![1.0]),
            filter: Some(json!({"op":"eq","path":["kind"],"value":"doc"})),
            after: None,
            before: None,
            prefer_recent: false,
            candidate_limit: 20,
            final_limit: 10,
            fields: Vec::new(),
            execution: None,
            dense_candidates: None,
            sparse_candidates: None,
            timeout_ms: None,
            fusion: FusionStrategy::Rrf,
            rrf_k: 60,
            dedup: DedupStrategy::RecordId,
            rerank: None,
            explain: true,
        };
        let result = FusedSearchOutput {
            fusion: FusionStrategy::Rrf,
            dedup: DedupStrategy::RecordId,
            results: vec![CollectionSearchResult {
                collection: "docs".into(),
                status: CollectionSearchStatus::Ok,
                mode: Some(SearchMode::Dense),
                hits: Vec::new(),
                explain: Some(json!({
                    "strategy":"exact",
                    "reason":"small_collection",
                    "dense_candidates":20,
                    "search_profile":"default@1",
                    "internal_node":"must_not_be_projected"
                })),
                error: None,
            }],
            hits: Vec::new(),
        };
        let output = ExplainSearchOutput::from_execution(&params, 7, result);
        assert_eq!(output.diagnostics.search_latency_ms, 7);
        assert_eq!(
            output.diagnostics.collection_diagnostics[0]
                .strategy
                .as_deref(),
            Some("exact")
        );
        assert_eq!(
            output.diagnostics.collection_diagnostics[0].dense_candidates,
            Some(20)
        );
        assert_eq!(
            output.diagnostics.collection_diagnostics[0].filtered_returned_hits,
            Some(0)
        );
    }
}
