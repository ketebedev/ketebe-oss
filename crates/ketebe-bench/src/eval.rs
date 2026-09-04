use ketebe_core::{CollectionId, DistanceMetric, FieldPath, LexicalAnalyzerConfig, RecordId};
use ketebe_storage::{
    ExecutionPreference, HnswIndex, HybridOptions, LexicalIndex, LexicalQuery, QueryRequest,
    Segment, execute_hybrid_query_with_index_and_options, execute_query, lexical_search_index,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const EVALUATION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationDataset {
    pub schema_version: u32,
    pub name: String,
    pub version: String,
    pub queries: Vec<EvaluationQuery>,
}

impl EvaluationDataset {
    pub fn validate(&self) -> Result<(), EvaluationError> {
        if self.schema_version != EVALUATION_SCHEMA_VERSION {
            return Err(EvaluationError::UnsupportedSchemaVersion(
                self.schema_version,
            ));
        }
        if self.name.trim().is_empty() || self.version.trim().is_empty() {
            return Err(EvaluationError::InvalidDatasetIdentity);
        }
        if self.queries.is_empty() {
            return Err(EvaluationError::EmptyDataset);
        }
        let mut query_ids = BTreeSet::new();
        for query in &self.queries {
            if query.id.trim().is_empty() {
                return Err(EvaluationError::EmptyQueryId);
            }
            if !query_ids.insert(query.id.clone()) {
                return Err(EvaluationError::DuplicateQueryId(query.id.clone()));
            }
            if query
                .text
                .as_ref()
                .is_some_and(|text| text.trim().is_empty())
            {
                return Err(EvaluationError::EmptyQueryText(query.id.clone()));
            }
            if query.vector.as_ref().is_some_and(Vec::is_empty) {
                return Err(EvaluationError::EmptyQueryVector(query.id.clone()));
            }
            if query.judgments.is_empty() {
                return Err(EvaluationError::EmptyJudgments(query.id.clone()));
            }
            let mut judged = BTreeSet::new();
            for judgment in &query.judgments {
                if !judged.insert(judgment.record_id.clone()) {
                    return Err(EvaluationError::DuplicateJudgment {
                        query_id: query.id.clone(),
                        record_id: judgment.record_id.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationQuery {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    pub judgments: Vec<RelevanceJudgment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelevanceJudgment {
    pub record_id: EvaluationRecordId,
    pub relevance: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum EvaluationRecordId {
    String(String),
    U64(u64),
}

impl EvaluationRecordId {
    #[must_use]
    pub fn from_record_id(value: &RecordId) -> Self {
        match value {
            RecordId::String(value) => Self::String(value.clone()),
            RecordId::Unsigned(value) => Self::U64(*value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationRunKind {
    Dense,
    Lexical,
    Hybrid,
    Reranked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationConfig {
    pub name: String,
    pub kind: EvaluationRunKind,
    pub top_k: usize,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

impl EvaluationConfig {
    pub fn new(name: impl Into<String>, kind: EvaluationRunKind, top_k: usize) -> Self {
        Self {
            name: name.into(),
            kind,
            top_k,
            parameters: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_parameter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(key.into(), value.into());
        self
    }

    fn validate(&self) -> Result<(), EvaluationError> {
        if self.name.trim().is_empty() {
            return Err(EvaluationError::InvalidRunName);
        }
        if self.top_k == 0 {
            return Err(EvaluationError::InvalidTopK);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct RelevanceMetrics {
    pub recall_at_k: f64,
    pub precision_at_k: f64,
    pub hit_rate_at_k: f64,
    pub mrr: f64,
    pub ndcg_at_k: f64,
}

impl RelevanceMetrics {
    fn add(self, other: Self) -> Self {
        Self {
            recall_at_k: self.recall_at_k + other.recall_at_k,
            precision_at_k: self.precision_at_k + other.precision_at_k,
            hit_rate_at_k: self.hit_rate_at_k + other.hit_rate_at_k,
            mrr: self.mrr + other.mrr,
            ndcg_at_k: self.ndcg_at_k + other.ndcg_at_k,
        }
    }

    fn divide(self, denominator: f64) -> Self {
        Self {
            recall_at_k: self.recall_at_k / denominator,
            precision_at_k: self.precision_at_k / denominator,
            hit_rate_at_k: self.hit_rate_at_k / denominator,
            mrr: self.mrr / denominator,
            ndcg_at_k: self.ndcg_at_k / denominator,
        }
    }

    fn subtract(self, baseline: Self) -> Self {
        Self {
            recall_at_k: self.recall_at_k - baseline.recall_at_k,
            precision_at_k: self.precision_at_k - baseline.precision_at_k,
            hit_rate_at_k: self.hit_rate_at_k - baseline.hit_rate_at_k,
            mrr: self.mrr - baseline.mrr,
            ndcg_at_k: self.ndcg_at_k - baseline.ndcg_at_k,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryEvaluation {
    pub query_id: String,
    pub returned: usize,
    pub metrics: RelevanceMetrics,
    pub ranking: Vec<EvaluationRecordId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationReport {
    pub report_schema_version: u32,
    pub dataset_name: String,
    pub dataset_version: String,
    pub dataset_schema_version: u32,
    pub config: EvaluationConfig,
    pub aggregate: RelevanceMetrics,
    pub queries: Vec<QueryEvaluation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationComparison {
    pub dataset_name: String,
    pub dataset_version: String,
    pub baseline: String,
    pub candidate: String,
    pub aggregate_delta: RelevanceMetrics,
    pub improved_queries: usize,
    pub regressed_queries: usize,
    pub unchanged_queries: usize,
}

#[derive(Debug)]
pub enum EvaluationError {
    UnsupportedSchemaVersion(u32),
    InvalidDatasetIdentity,
    EmptyDataset,
    EmptyQueryId,
    DuplicateQueryId(String),
    EmptyQueryText(String),
    EmptyQueryVector(String),
    EmptyJudgments(String),
    DuplicateJudgment {
        query_id: String,
        record_id: EvaluationRecordId,
    },
    InvalidRunName,
    InvalidTopK,
    MissingVector(String),
    MissingText(String),
    MissingRanking(String),
    RankingForUnknownQuery(String),
    DatasetMismatch,
    QuerySetMismatch,
    Execution(String),
}

impl fmt::Display for EvaluationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchemaVersion(value) => {
                write!(f, "unsupported evaluation schema version {value}")
            }
            Self::InvalidDatasetIdentity => {
                f.write_str("dataset name and version must be non-empty")
            }
            Self::EmptyDataset => f.write_str("evaluation dataset must contain at least one query"),
            Self::EmptyQueryId => f.write_str("evaluation query id must be non-empty"),
            Self::DuplicateQueryId(value) => write!(f, "duplicate evaluation query id {value}"),
            Self::EmptyQueryText(value) => write!(f, "query {value} has empty text"),
            Self::EmptyQueryVector(value) => write!(f, "query {value} has empty vector"),
            Self::EmptyJudgments(value) => write!(f, "query {value} has no relevance judgments"),
            Self::DuplicateJudgment {
                query_id,
                record_id,
            } => {
                write!(
                    f,
                    "query {query_id} contains duplicate judgment for {record_id:?}"
                )
            }
            Self::InvalidRunName => f.write_str("evaluation run name must be non-empty"),
            Self::InvalidTopK => f.write_str("evaluation top_k must be greater than zero"),
            Self::MissingVector(value) => write!(f, "query {value} does not contain a vector"),
            Self::MissingText(value) => write!(f, "query {value} does not contain text"),
            Self::MissingRanking(value) => write!(f, "ranking is missing for query {value}"),
            Self::RankingForUnknownQuery(value) => {
                write!(f, "ranking supplied for unknown query {value}")
            }
            Self::DatasetMismatch => f.write_str("evaluation reports refer to different datasets"),
            Self::QuerySetMismatch => {
                f.write_str("evaluation reports contain different query sets")
            }
            Self::Execution(value) => write!(f, "evaluation execution failed: {value}"),
        }
    }
}

impl std::error::Error for EvaluationError {}

pub fn metrics_at_k(
    query: &EvaluationQuery,
    ranking: &[EvaluationRecordId],
    top_k: usize,
) -> Result<RelevanceMetrics, EvaluationError> {
    if top_k == 0 {
        return Err(EvaluationError::InvalidTopK);
    }
    let relevance = query
        .judgments
        .iter()
        .map(|judgment| (judgment.record_id.clone(), judgment.relevance))
        .collect::<BTreeMap<_, _>>();
    let relevant_total = relevance.values().filter(|value| **value > 0).count();
    let ranked = ranking.iter().take(top_k).collect::<Vec<_>>();
    let relevant_retrieved = ranked
        .iter()
        .filter(|id| relevance.get(**id).copied().unwrap_or(0) > 0)
        .count();
    let recall = if relevant_total == 0 {
        1.0
    } else {
        relevant_retrieved as f64 / relevant_total as f64
    };
    let precision = relevant_retrieved as f64 / top_k as f64;
    let hit_rate = f64::from(relevant_retrieved > 0);
    let mrr = ranked
        .iter()
        .position(|id| relevance.get(*id).copied().unwrap_or(0) > 0)
        .map_or(0.0, |index| 1.0 / (index + 1) as f64);
    let dcg = ranked
        .iter()
        .enumerate()
        .map(|(index, id)| discounted_gain(relevance.get(*id).copied().unwrap_or(0), index + 1))
        .sum::<f64>();
    let mut ideal = query
        .judgments
        .iter()
        .map(|judgment| judgment.relevance)
        .collect::<Vec<_>>();
    ideal.sort_unstable_by(|left, right| right.cmp(left));
    let idcg = ideal
        .into_iter()
        .take(top_k)
        .enumerate()
        .map(|(index, grade)| discounted_gain(grade, index + 1))
        .sum::<f64>();
    Ok(RelevanceMetrics {
        recall_at_k: recall,
        precision_at_k: precision,
        hit_rate_at_k: hit_rate,
        mrr,
        ndcg_at_k: if idcg > 0.0 { dcg / idcg } else { 1.0 },
    })
}

fn discounted_gain(relevance: u32, rank: usize) -> f64 {
    if relevance == 0 {
        return 0.0;
    }
    let gain = 2_f64.powf(f64::from(relevance)) - 1.0;
    gain / (rank as f64 + 1.0).log2()
}

pub fn evaluate_rankings(
    dataset: &EvaluationDataset,
    config: EvaluationConfig,
    rankings: &BTreeMap<String, Vec<EvaluationRecordId>>,
) -> Result<EvaluationReport, EvaluationError> {
    dataset.validate()?;
    config.validate()?;
    let known = dataset
        .queries
        .iter()
        .map(|query| query.id.as_str())
        .collect::<BTreeSet<_>>();
    if let Some(unknown) = rankings.keys().find(|id| !known.contains(id.as_str())) {
        return Err(EvaluationError::RankingForUnknownQuery(unknown.clone()));
    }
    let mut query_reports = Vec::with_capacity(dataset.queries.len());
    let mut aggregate = RelevanceMetrics::default();
    for query in &dataset.queries {
        let ranking = rankings
            .get(&query.id)
            .ok_or_else(|| EvaluationError::MissingRanking(query.id.clone()))?;
        let metrics = metrics_at_k(query, ranking, config.top_k)?;
        aggregate = aggregate.add(metrics);
        query_reports.push(QueryEvaluation {
            query_id: query.id.clone(),
            returned: ranking.len().min(config.top_k),
            metrics,
            ranking: ranking.iter().take(config.top_k).cloned().collect(),
        });
    }
    Ok(EvaluationReport {
        report_schema_version: EVALUATION_SCHEMA_VERSION,
        dataset_name: dataset.name.clone(),
        dataset_version: dataset.version.clone(),
        dataset_schema_version: dataset.schema_version,
        config,
        aggregate: aggregate.divide(dataset.queries.len() as f64),
        queries: query_reports,
    })
}

pub fn evaluate_reranked_rankings(
    dataset: &EvaluationDataset,
    name: impl Into<String>,
    top_k: usize,
    rankings: &BTreeMap<String, Vec<EvaluationRecordId>>,
    parameters: BTreeMap<String, String>,
) -> Result<EvaluationReport, EvaluationError> {
    let mut config = EvaluationConfig::new(name, EvaluationRunKind::Reranked, top_k);
    config.parameters = parameters;
    evaluate_rankings(dataset, config, rankings)
}

#[derive(Debug, Clone, Copy)]
pub struct DenseEvaluationExecution<'a> {
    pub collection_id: &'a CollectionId,
    pub segments: &'a [Segment],
    pub hnsw: Option<&'a HnswIndex>,
    pub metric: DistanceMetric,
    pub preference: ExecutionPreference,
    pub top_k: usize,
}

pub fn evaluate_dense(
    dataset: &EvaluationDataset,
    name: impl Into<String>,
    execution: DenseEvaluationExecution<'_>,
) -> Result<EvaluationReport, EvaluationError> {
    let DenseEvaluationExecution {
        collection_id,
        segments,
        hnsw,
        metric,
        preference,
        top_k,
    } = execution;
    dataset.validate()?;
    let mut rankings = BTreeMap::new();
    for query in &dataset.queries {
        let vector = query
            .vector
            .clone()
            .ok_or_else(|| EvaluationError::MissingVector(query.id.clone()))?;
        let response = execute_query(
            &QueryRequest::new(collection_id.clone(), vector, metric, top_k)
                .with_preference(preference),
            segments,
            hnsw,
        )
        .map_err(|error| EvaluationError::Execution(error.to_string()))?;
        rankings.insert(
            query.id.clone(),
            response
                .hits()
                .iter()
                .map(|hit| EvaluationRecordId::from_record_id(hit.record().id()))
                .collect(),
        );
    }
    let config = EvaluationConfig::new(name, EvaluationRunKind::Dense, top_k)
        .with_parameter("metric", format!("{metric:?}").to_lowercase())
        .with_parameter("execution", format!("{preference:?}").to_lowercase());
    evaluate_rankings(dataset, config, &rankings)
}

pub fn evaluate_lexical(
    dataset: &EvaluationDataset,
    name: impl Into<String>,
    lexical_index: &LexicalIndex,
    fields: Vec<FieldPath>,
    analyzer: LexicalAnalyzerConfig,
    top_k: usize,
) -> Result<EvaluationReport, EvaluationError> {
    dataset.validate()?;
    let mut rankings = BTreeMap::new();
    for query in &dataset.queries {
        let text = query
            .text
            .clone()
            .ok_or_else(|| EvaluationError::MissingText(query.id.clone()))?;
        let lexical_query = LexicalQuery::new(text, fields.clone())?.with_analyzer(analyzer);
        let hits = lexical_search_index(lexical_index, &lexical_query, top_k, None)
            .map_err(|error| EvaluationError::Execution(error.to_string()))?;
        rankings.insert(
            query.id.clone(),
            hits.iter()
                .map(|hit| EvaluationRecordId::from_record_id(hit.record().id()))
                .collect(),
        );
    }
    let config = EvaluationConfig::new(name, EvaluationRunKind::Lexical, top_k)
        .with_parameter("fields", fields.len().to_string())
        .with_parameter("analyzer", format!("{analyzer:?}"));
    evaluate_rankings(dataset, config, &rankings)
}

pub struct HybridEvaluationExecution<'a> {
    pub collection_id: &'a CollectionId,
    pub segments: &'a [Segment],
    pub hnsw: Option<&'a HnswIndex>,
    pub lexical_index: &'a LexicalIndex,
    pub fields: Vec<FieldPath>,
    pub analyzer: LexicalAnalyzerConfig,
    pub metric: DistanceMetric,
    pub preference: ExecutionPreference,
    pub options: HybridOptions,
}

pub fn evaluate_hybrid(
    dataset: &EvaluationDataset,
    name: impl Into<String>,
    execution: HybridEvaluationExecution<'_>,
) -> Result<EvaluationReport, EvaluationError> {
    let HybridEvaluationExecution {
        collection_id,
        segments,
        hnsw,
        lexical_index,
        fields,
        analyzer,
        metric,
        preference,
        options,
    } = execution;
    dataset.validate()?;
    let mut rankings = BTreeMap::new();
    for query in &dataset.queries {
        let vector = query
            .vector
            .clone()
            .ok_or_else(|| EvaluationError::MissingVector(query.id.clone()))?;
        let text = query
            .text
            .clone()
            .ok_or_else(|| EvaluationError::MissingText(query.id.clone()))?;
        let dense = QueryRequest::new(collection_id.clone(), vector, metric, options.top_k)
            .with_preference(preference);
        let lexical_query = LexicalQuery::new(text, fields.clone())?.with_analyzer(analyzer);
        let response = execute_hybrid_query_with_index_and_options(
            &dense,
            &lexical_query,
            lexical_index,
            segments,
            hnsw,
            options,
        )
        .map_err(|error| EvaluationError::Execution(error.to_string()))?;
        rankings.insert(
            query.id.clone(),
            response
                .hits()
                .iter()
                .map(|hit| EvaluationRecordId::from_record_id(hit.record().id()))
                .collect(),
        );
    }
    let config = EvaluationConfig::new(name, EvaluationRunKind::Hybrid, options.top_k)
        .with_parameter("metric", format!("{metric:?}").to_lowercase())
        .with_parameter("execution", format!("{preference:?}").to_lowercase())
        .with_parameter("dense_k", options.dense_k.to_string())
        .with_parameter("lexical_k", options.lexical_k.to_string())
        .with_parameter("rrf_k", options.rrf_k.to_string());
    evaluate_rankings(dataset, config, &rankings)
}

pub fn compare_reports(
    baseline: &EvaluationReport,
    candidate: &EvaluationReport,
) -> Result<EvaluationComparison, EvaluationError> {
    if baseline.dataset_name != candidate.dataset_name
        || baseline.dataset_version != candidate.dataset_version
        || baseline.dataset_schema_version != candidate.dataset_schema_version
    {
        return Err(EvaluationError::DatasetMismatch);
    }
    let baseline_queries = baseline
        .queries
        .iter()
        .map(|query| (query.query_id.as_str(), query.metrics))
        .collect::<BTreeMap<_, _>>();
    let candidate_queries = candidate
        .queries
        .iter()
        .map(|query| (query.query_id.as_str(), query.metrics))
        .collect::<BTreeMap<_, _>>();
    if baseline_queries.keys().ne(candidate_queries.keys()) {
        return Err(EvaluationError::QuerySetMismatch);
    }
    let mut improved = 0;
    let mut regressed = 0;
    let mut unchanged = 0;
    for (id, baseline_metrics) in baseline_queries {
        let delta = candidate_queries[&id].ndcg_at_k - baseline_metrics.ndcg_at_k;
        if delta > 1e-12 {
            improved += 1;
        } else if delta < -1e-12 {
            regressed += 1;
        } else {
            unchanged += 1;
        }
    }
    Ok(EvaluationComparison {
        dataset_name: baseline.dataset_name.clone(),
        dataset_version: baseline.dataset_version.clone(),
        baseline: baseline.config.name.clone(),
        candidate: candidate.config.name.clone(),
        aggregate_delta: candidate.aggregate.subtract(baseline.aggregate),
        improved_queries: improved,
        regressed_queries: regressed,
        unchanged_queries: unchanged,
    })
}

impl From<ketebe_storage::HybridError> for EvaluationError {
    fn from(value: ketebe_storage::HybridError) -> Self {
        Self::Execution(value.to_string())
    }
}
