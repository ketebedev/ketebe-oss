use std::collections::BTreeMap;

use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    multi_search::{CollectionSearchResult, CollectionSearchStatus, SearchManyTarget},
    retrieval::AgentRecordId,
    search::{SearchHit, SearchMode, SearchParams},
};

const DEFAULT_CANDIDATE_LIMIT: usize = 20;
const DEFAULT_FINAL_LIMIT: usize = 10;
const DEFAULT_RRF_K: u32 = 60;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FusionStrategy {
    #[default]
    Rrf,
    ScoreSum,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DedupStrategy {
    None,
    #[default]
    RecordId,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RerankFailurePolicy {
    #[default]
    Fail,
    PreserveCandidateOrder,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ServerRerankParams {
    #[serde(default = "default_rerank_profile")]
    pub profile: String,
    #[serde(default)]
    pub query: Option<String>,
    pub top_n: usize,
    pub text_fields: Vec<Vec<String>>,
    #[serde(default)]
    pub include_metadata: bool,
    #[serde(default)]
    pub failure_policy: RerankFailurePolicy,
}

fn default_rerank_profile() -> String {
    "default".into()
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct FusedSearchParams {
    pub collections: Vec<SearchManyTarget>,
    pub mode: SearchMode,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
    #[serde(default)]
    pub filter: Option<Value>,
    #[serde(default)]
    pub after: Option<u64>,
    #[serde(default)]
    pub before: Option<u64>,
    #[serde(default)]
    pub prefer_recent: bool,
    #[serde(default = "default_candidate_limit")]
    pub candidate_limit: usize,
    #[serde(default = "default_final_limit")]
    pub final_limit: usize,
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub execution: Option<String>,
    #[serde(default)]
    pub dense_candidates: Option<usize>,
    #[serde(default)]
    pub sparse_candidates: Option<usize>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub fusion: FusionStrategy,
    #[serde(default = "default_rrf_k")]
    pub rrf_k: u32,
    #[serde(default)]
    pub dedup: DedupStrategy,
    #[serde(default)]
    pub rerank: Option<ServerRerankParams>,
    #[serde(default)]
    pub explain: bool,
}

const fn default_candidate_limit() -> usize {
    DEFAULT_CANDIDATE_LIMIT
}

const fn default_final_limit() -> usize {
    DEFAULT_FINAL_LIMIT
}

const fn default_rrf_k() -> u32 {
    DEFAULT_RRF_K
}

impl FusedSearchParams {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.collections.is_empty() {
            return Err("Ketebe fused search request failed: collections_required".to_string());
        }
        if self
            .collections
            .iter()
            .any(|target| target.collection.trim().is_empty())
        {
            return Err("Ketebe fused search request failed: invalid_collection".to_string());
        }
        if self
            .after
            .zip(self.before)
            .is_some_and(|(after, before)| after >= before)
        {
            return Err("Ketebe fused search request failed: invalid_time_range".to_string());
        }
        if self.candidate_limit == 0 {
            return Err("Ketebe fused search request failed: invalid_candidate_limit".to_string());
        }
        if self.final_limit == 0 {
            return Err("Ketebe fused search request failed: invalid_final_limit".to_string());
        }
        if self.rrf_k == 0 {
            return Err("Ketebe fused search request failed: invalid_rrf_k".to_string());
        }
        if let Some(rerank) = &self.rerank {
            if rerank.profile.trim().is_empty() {
                return Err(
                    "Ketebe fused search request failed: invalid_rerank_profile".to_string()
                );
            }
            if rerank.top_n == 0 || rerank.top_n > self.candidate_limit {
                return Err("Ketebe fused search request failed: invalid_rerank_top_n".to_string());
            }
            if rerank.text_fields.is_empty()
                || rerank.text_fields.iter().any(|path| {
                    path.is_empty() || path.iter().any(|segment| segment.trim().is_empty())
                })
            {
                return Err(
                    "Ketebe fused search request failed: invalid_rerank_text_fields".to_string(),
                );
            }
        }
        Ok(())
    }

    pub(crate) fn search_params_for(&self, target: &SearchManyTarget) -> SearchParams {
        SearchParams {
            collection: target.collection.clone(),
            mode: self.mode,
            text: self.text.clone(),
            vector: self.vector.clone(),
            filter: self.filter.clone(),
            after: self.after,
            before: self.before,
            prefer_recent: self.prefer_recent,
            limit: self.candidate_limit,
            fields: self.fields.clone(),
            execution: self.execution.clone(),
            dense_candidates: self.dense_candidates,
            sparse_candidates: self.sparse_candidates,
            search_profile: target.search_profile.clone(),
            timeout_ms: self.timeout_ms,
            explain: self.explain,
        }
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct FusedSearchOutput {
    pub fusion: FusionStrategy,
    pub dedup: DedupStrategy,
    pub results: Vec<CollectionSearchResult>,
    pub hits: Vec<FusedSearchHit>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct FusedSearchHit {
    pub fusion_rank: usize,
    pub fusion_score: f64,
    pub id: AgentRecordId,
    pub representative: SearchHit,
    pub provenance: Vec<FusionContribution>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct FusionContribution {
    pub source_collection: String,
    pub source_rank: usize,
    pub retrieval_score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rerank_score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_rank: Option<usize>,
    pub fusion_contribution: f64,
}

#[derive(Clone)]
struct Accumulator {
    id: AgentRecordId,
    representative: SearchHit,
    representative_order: (usize, usize),
    score: f64,
    provenance: Vec<FusionContribution>,
}

pub(crate) fn fuse_results(
    params: &FusedSearchParams,
    results: Vec<CollectionSearchResult>,
) -> FusedSearchOutput {
    let mut accumulators = BTreeMap::<String, Accumulator>::new();

    for (collection_order, result) in results.iter().enumerate() {
        if result.status != CollectionSearchStatus::Ok {
            continue;
        }
        for (index, hit) in result.hits.iter().enumerate() {
            let source_rank = index + 1;
            let base_key = record_key(&hit.id);
            let key = match params.dedup {
                DedupStrategy::RecordId => base_key.clone(),
                DedupStrategy::None => format!("{collection_order}:{base_key}"),
            };
            let contribution = match params.fusion {
                FusionStrategy::Rrf => {
                    1.0 / f64::from(params.rrf_k.saturating_add(source_rank as u32))
                }
                FusionStrategy::ScoreSum => f64::from(hit.rerank_score.unwrap_or(hit.score)),
            };
            let provenance = FusionContribution {
                source_collection: result.collection.clone(),
                source_rank,
                retrieval_score: hit.score,
                rerank_score: hit.rerank_score,
                original_rank: hit.original_rank,
                fusion_contribution: contribution,
            };
            match accumulators.get_mut(&key) {
                Some(current) => {
                    current.score += contribution;
                    current.provenance.push(provenance);
                    let newer_representative = params.prefer_recent
                        && hit.source_timestamp_unix_ms
                            > current.representative.source_timestamp_unix_ms;
                    if newer_representative
                        || (!params.prefer_recent
                            && (collection_order, source_rank) < current.representative_order)
                    {
                        current.representative = hit.clone();
                        current.representative_order = (collection_order, source_rank);
                    }
                }
                None => {
                    accumulators.insert(
                        key,
                        Accumulator {
                            id: hit.id.clone(),
                            representative: hit.clone(),
                            representative_order: (collection_order, source_rank),
                            score: contribution,
                            provenance: vec![provenance],
                        },
                    );
                }
            }
        }
    }

    let mut fused = accumulators.into_values().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                if params.prefer_recent {
                    right
                        .representative
                        .source_timestamp_unix_ms
                        .cmp(&left.representative.source_timestamp_unix_ms)
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .then_with(|| left.representative_order.cmp(&right.representative_order))
            .then_with(|| record_key(&left.id).cmp(&record_key(&right.id)))
    });
    fused.truncate(params.final_limit);

    FusedSearchOutput {
        fusion: params.fusion,
        dedup: params.dedup,
        results,
        hits: fused
            .into_iter()
            .enumerate()
            .map(|(index, value)| FusedSearchHit {
                fusion_rank: index + 1,
                fusion_score: value.score,
                id: value.id,
                representative: value.representative,
                provenance: value.provenance,
            })
            .collect(),
    }
}

fn record_key(id: &AgentRecordId) -> String {
    match id {
        AgentRecordId::String(value) => format!("string:{value}"),
        AgentRecordId::U64(value) => format!("u64:{value}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi_search::CollectionSearchResult;
    use serde_json::json;

    fn hit(id: &str, score: f32, timestamp: Option<u64>) -> SearchHit {
        SearchHit {
            id: AgentRecordId::String(id.into()),
            score,
            sequence_number: 1,
            source_timestamp_unix_ms: timestamp,
            metadata: Some(json!({"title":id})),
            dense_rank: Some(1),
            sparse_rank: None,
            dense_score: Some(score),
            sparse_score: None,
            rerank_score: None,
            original_rank: None,
        }
    }

    fn params() -> FusedSearchParams {
        FusedSearchParams {
            collections: vec![
                SearchManyTarget {
                    collection: "a".into(),
                    search_profile: None,
                },
                SearchManyTarget {
                    collection: "b".into(),
                    search_profile: None,
                },
            ],
            mode: SearchMode::Dense,
            text: None,
            vector: Some(vec![1.0]),
            filter: None,
            after: None,
            before: None,
            prefer_recent: false,
            candidate_limit: 10,
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
            explain: false,
        }
    }

    #[test]
    fn rrf_dedup_is_deterministic_and_preserves_provenance() {
        let results = vec![
            CollectionSearchResult {
                collection: "a".into(),
                status: CollectionSearchStatus::Ok,
                mode: Some(SearchMode::Dense),
                hits: vec![hit("shared", 0.9, Some(100)), hit("a", 0.8, Some(100))],
                explain: None,
                error: None,
            },
            CollectionSearchResult {
                collection: "b".into(),
                status: CollectionSearchStatus::Ok,
                mode: Some(SearchMode::Dense),
                hits: vec![hit("shared", 0.7, Some(200)), hit("b", 0.6, Some(200))],
                explain: None,
                error: None,
            },
        ];
        let output = fuse_results(&params(), results);
        assert_eq!(output.hits[0].id, AgentRecordId::String("shared".into()));
        assert_eq!(output.hits[0].provenance.len(), 2);
        assert_eq!(output.hits[0].provenance[0].source_collection, "a");
        assert_eq!(output.hits[0].provenance[1].source_collection, "b");
    }

    #[test]
    fn freshness_only_breaks_equal_fusion_scores() {
        let mut params = params();
        params.fusion = FusionStrategy::ScoreSum;
        params.dedup = DedupStrategy::None;
        params.prefer_recent = true;
        let results = vec![CollectionSearchResult {
            collection: "a".into(),
            status: CollectionSearchStatus::Ok,
            mode: Some(SearchMode::Dense),
            hits: vec![hit("older", 1.0, Some(100)), hit("newer", 1.0, Some(200))],
            explain: None,
            error: None,
        }];
        let output = fuse_results(&params, results);
        assert_eq!(output.hits[0].id, AgentRecordId::String("newer".into()));
        assert_eq!(output.hits[1].id, AgentRecordId::String("older".into()));
    }
}
