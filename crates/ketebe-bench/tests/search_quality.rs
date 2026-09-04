use ketebe_bench::{
    DenseEvaluationExecution, EvaluationDataset, EvaluationRecordId, EvaluationRunKind,
    HybridEvaluationExecution, compare_reports, evaluate_dense, evaluate_hybrid, evaluate_lexical,
    evaluate_reranked_rankings, metrics_at_k,
};
use ketebe_core::{
    CollectionId, DistanceMetric, FieldPath, LexicalAnalyzerConfig, Metadata, MetadataValue,
    Record, RecordId, SequenceNumber, Vector,
};
use ketebe_storage::{
    DEFAULT_RRF_K, ExecutionPreference, HybridOptions, LexicalIndex, Segment, SegmentId,
    WalMutation,
};
use std::collections::BTreeMap;

fn record(id: u64, sequence: u64, vector: [f32; 2], title: &str) -> WalMutation {
    let mut metadata = Metadata::new();
    metadata.insert("title".into(), MetadataValue::String(title.into()));
    WalMutation::Upsert {
        collection_id: CollectionId::new("quality").unwrap(),
        record: Record::new(
            RecordId::unsigned(id),
            Vector::new(vector.to_vec()).unwrap(),
            metadata,
            SequenceNumber::new(sequence),
        ),
    }
}

fn fixture() -> EvaluationDataset {
    serde_json::from_str(include_str!("../fixtures/search-quality-v0.json")).unwrap()
}

fn segment() -> Segment {
    Segment::from_mutations(
        SegmentId::new(1),
        &[
            record(1, 1, [1.0, 0.0], "alpha primary"),
            record(2, 2, [0.9, 0.0], "alpha secondary"),
            record(3, 3, [0.0, 1.0], "beta primary"),
            record(4, 4, [0.0, 0.9], "beta secondary"),
        ],
    )
    .unwrap()
}

#[test]
fn fixture_schema_and_metrics_are_reproducible() {
    let dataset = fixture();
    dataset.validate().unwrap();
    assert_eq!(dataset.schema_version, 1);
    assert_eq!(dataset.version, "2026-08-27.v1");

    let ranking = vec![EvaluationRecordId::U64(1), EvaluationRecordId::U64(2)];
    let metrics = metrics_at_k(&dataset.queries[0], &ranking, 2).unwrap();
    assert_eq!(metrics.recall_at_k, 1.0);
    assert_eq!(metrics.precision_at_k, 1.0);
    assert_eq!(metrics.hit_rate_at_k, 1.0);
    assert_eq!(metrics.mrr, 1.0);
    assert!((metrics.ndcg_at_k - 1.0).abs() < 1e-12);
}

#[test]
fn dense_lexical_hybrid_and_reranked_runs_share_one_report_contract() {
    let dataset = fixture();
    let collection = CollectionId::new("quality").unwrap();
    let segment = segment();
    let segments = vec![segment];
    let field = FieldPath::new(["title"]).unwrap();
    let analyzer = LexicalAnalyzerConfig::default();
    let lexical_index =
        LexicalIndex::build(&segments, &collection, vec![field.clone()], analyzer, 1).unwrap();

    let dense = evaluate_dense(
        &dataset,
        "dense-exact",
        DenseEvaluationExecution {
            collection_id: &collection,
            segments: &segments,
            hnsw: None,
            metric: DistanceMetric::L2,
            preference: ExecutionPreference::Exact,
            top_k: 2,
        },
    )
    .unwrap();
    assert_eq!(dense.config.kind, EvaluationRunKind::Dense);
    assert!((dense.aggregate.ndcg_at_k - 1.0).abs() < 1e-12);

    let lexical = evaluate_lexical(
        &dataset,
        "lexical-standard",
        &lexical_index,
        vec![field.clone()],
        analyzer,
        2,
    )
    .unwrap();
    assert_eq!(lexical.config.kind, EvaluationRunKind::Lexical);
    assert!((lexical.aggregate.recall_at_k - 1.0).abs() < 1e-12);

    let hybrid = evaluate_hybrid(
        &dataset,
        "hybrid-expanded",
        HybridEvaluationExecution {
            collection_id: &collection,
            segments: &segments,
            hnsw: None,
            lexical_index: &lexical_index,
            fields: vec![field],
            analyzer,
            metric: DistanceMetric::L2,
            preference: ExecutionPreference::Exact,
            options: HybridOptions::new(2, 4, 4, DEFAULT_RRF_K).unwrap(),
        },
    )
    .unwrap();
    assert_eq!(hybrid.config.kind, EvaluationRunKind::Hybrid);
    assert_eq!(hybrid.config.parameters["dense_k"], "4");
    assert_eq!(hybrid.config.parameters["lexical_k"], "4");

    let rankings = BTreeMap::from([
        (
            "alpha-query".to_string(),
            vec![EvaluationRecordId::U64(2), EvaluationRecordId::U64(1)],
        ),
        (
            "beta-query".to_string(),
            vec![EvaluationRecordId::U64(4), EvaluationRecordId::U64(3)],
        ),
    ]);
    let reranked = evaluate_reranked_rankings(
        &dataset,
        "reranker-fixture",
        2,
        &rankings,
        BTreeMap::from([("model".to_string(), "fixture-v1".to_string())]),
    )
    .unwrap();
    assert_eq!(reranked.config.kind, EvaluationRunKind::Reranked);
    assert!(reranked.aggregate.ndcg_at_k < dense.aggregate.ndcg_at_k);

    let comparison = compare_reports(&dense, &reranked).unwrap();
    assert_eq!(comparison.baseline, "dense-exact");
    assert_eq!(comparison.candidate, "reranker-fixture");
    assert_eq!(comparison.regressed_queries, 2);
    assert!(comparison.aggregate_delta.ndcg_at_k < 0.0);

    let json = serde_json::to_value(&hybrid).unwrap();
    assert_eq!(json["dataset_name"], "ketebe-search-quality-smoke");
    assert_eq!(json["dataset_version"], "2026-08-27.v1");
    assert_eq!(json["report_schema_version"], 1);
    assert_eq!(json["config"]["kind"], "hybrid");
}
