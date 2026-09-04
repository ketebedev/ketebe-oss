use ketebe_core::{FieldPath, Metadata, MetadataValue, RecordId};
use ketebe_server::{
    CandidateProjection, RerankCandidate, RerankFailurePolicy, RerankFuture, RerankScore, Reranker,
    RerankerError, RerankingError, RerankingService,
};
use std::sync::Arc;

#[derive(Clone)]
struct FakeReranker {
    fail: bool,
}

impl Reranker for FakeReranker {
    fn name(&self) -> &str {
        "fake"
    }

    fn rerank<'a>(
        &'a self,
        _query: &'a str,
        candidates: &'a [RerankCandidate],
    ) -> RerankFuture<'a> {
        Box::pin(async move {
            if self.fail {
                return Err(RerankerError::new("fake unavailable", true));
            }
            Ok(candidates
                .iter()
                .enumerate()
                .map(|(index, _)| RerankScore {
                    index,
                    score: index as f32,
                })
                .collect())
        })
    }
}

fn candidate(id: &str, retrieval_score: f32) -> RerankCandidate {
    RerankCandidate {
        id: RecordId::string(id).unwrap(),
        text: format!("document {id}"),
        metadata: Metadata::new(),
        retrieval_score,
    }
}

#[tokio::test]
async fn reranking_reorders_explicit_candidates_and_truncates_final_top_k() {
    let service = RerankingService::new(
        Arc::new(FakeReranker { fail: false }),
        RerankFailurePolicy::Fail,
    );
    let result = service
        .rerank(
            "best document",
            vec![
                candidate("a", 0.9),
                candidate("b", 0.8),
                candidate("c", 0.7),
            ],
            2,
        )
        .await
        .unwrap();

    assert_eq!(result.candidates.len(), 2);
    assert_eq!(
        result.candidates[0].candidate.id,
        RecordId::string("c").unwrap()
    );
    assert_eq!(result.candidates[0].original_rank, 3);
    assert_eq!(result.candidates[0].rerank_score, Some(2.0));
    assert_eq!(result.explain.input_candidates, 3);
    assert_eq!(result.explain.output_candidates, 2);
    assert!(result.explain.applied);
}

#[tokio::test]
async fn fallback_preserves_candidate_order_and_is_observable() {
    let service = RerankingService::new(
        Arc::new(FakeReranker { fail: true }),
        RerankFailurePolicy::PreserveCandidateOrder,
    );
    let result = service
        .rerank(
            "query",
            vec![
                candidate("a", 0.9),
                candidate("b", 0.8),
                candidate("c", 0.7),
            ],
            2,
        )
        .await
        .unwrap();

    assert_eq!(
        result.candidates[0].candidate.id,
        RecordId::string("a").unwrap()
    );
    assert_eq!(
        result.candidates[1].candidate.id,
        RecordId::string("b").unwrap()
    );
    assert_eq!(result.explain.input_candidates, 3);
    assert_eq!(result.explain.output_candidates, 2);
    assert!(!result.explain.applied);
    assert!(
        result
            .explain
            .fallback_reason
            .as_deref()
            .unwrap()
            .contains("fake unavailable")
    );
}

#[tokio::test]
async fn fail_policy_surfaces_provider_failure() {
    let service = RerankingService::new(
        Arc::new(FakeReranker { fail: true }),
        RerankFailurePolicy::Fail,
    );
    let error = service
        .rerank("query", vec![candidate("a", 1.0)], 1)
        .await
        .unwrap_err();
    assert!(matches!(error, RerankingError::Provider(_)));
}

#[test]
fn projection_builds_candidate_text_without_mutating_metadata() {
    let mut metadata = Metadata::new();
    metadata.insert("title".into(), MetadataValue::String("Ketebe guide".into()));
    metadata.insert(
        "summary".into(),
        MetadataValue::String("Hybrid retrieval".into()),
    );
    let projection = CandidateProjection {
        text_fields: vec![
            FieldPath::new(["title"]).unwrap(),
            FieldPath::new(["summary"]).unwrap(),
        ],
        include_metadata: false,
    };

    let projected = projection
        .project(RecordId::string("doc-1").unwrap(), &metadata, 0.75)
        .unwrap();
    assert_eq!(projected.text, "Ketebe guide\nHybrid retrieval");
    assert!(projected.metadata.is_empty());
    assert_eq!(metadata.len(), 2);
}
