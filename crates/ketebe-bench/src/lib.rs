#![forbid(unsafe_code)]

mod eval;

pub use eval::{
    DenseEvaluationExecution, EVALUATION_SCHEMA_VERSION, EvaluationComparison, EvaluationConfig,
    EvaluationDataset, EvaluationError, EvaluationQuery, EvaluationRecordId, EvaluationReport,
    EvaluationRunKind, HybridEvaluationExecution, QueryEvaluation, RelevanceJudgment,
    RelevanceMetrics, compare_reports, evaluate_dense, evaluate_hybrid, evaluate_lexical,
    evaluate_rankings, evaluate_reranked_rankings, metrics_at_k,
};
