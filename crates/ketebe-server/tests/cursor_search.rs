use ketebe_core::{
    CollectionId, DistanceMetric, FieldPath, Metadata, MetadataValue, Predicate, RecordId,
};
use ketebe_server::{
    AppState, PendingRecord, QueryPaginationV1, QueryV1Error, QueryV1Request, RuntimeCatalog,
    WriteService, execute_query_v1_page,
};
use ketebe_storage::ExecutionPreference;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-cursor-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
async fn fixture(label: &str) -> (std::path::PathBuf, AppState, CollectionId) {
    let dir = temp_dir(label);
    let state =
        AppState::with_data_dir_and_threshold(RuntimeCatalog::empty_ready(), dir.clone(), 100);
    let id = CollectionId::new("docs").unwrap();
    let writes = WriteService::new(state.clone());
    writes
        .create_collection(id.clone(), 1, DistanceMetric::Dot, Vec::new())
        .await
        .unwrap();
    for (rid, score, group) in [
        (RecordId::string("1").unwrap(), 5.0, "a"),
        (RecordId::unsigned(1), 5.0, "a"),
        (RecordId::unsigned(2), 4.0, "a"),
        (RecordId::unsigned(3), 3.0, "b"),
        (RecordId::unsigned(4), 2.0, "a"),
    ] {
        let mut metadata = Metadata::new();
        metadata.insert("group".into(), MetadataValue::String(group.into()));
        writes
            .upsert(
                &id,
                PendingRecord {
                    id: rid,
                    vector: vec![score],
                    metadata,
                },
            )
            .await
            .unwrap();
    }
    (dir, state, id)
}
fn request(id: &CollectionId) -> QueryV1Request {
    QueryV1Request {
        collection_id: id.clone(),
        vector: Some(vec![1.0]),
        text: None,
        top_k: 2,
        predicate: None,
        execution: ExecutionPreference::Exact,
        dense_candidates: None,
        lexical_candidates: None,
        rrf_k: 60,
        search_profile: None,
        include_metadata: true,
        include_provenance: false,
        explain: true,
        timeout_ms: None,
        rerank: None,
    }
}

#[tokio::test]
async fn dense_cursor_continues_deterministically_across_typed_ids_and_restart() {
    let (dir, state, id) = fixture("restart").await;
    let first = execute_query_v1_page(
        &state,
        request(&id),
        QueryPaginationV1 {
            enabled: true,
            cursor: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(first.response.hits.len(), 2);
    let cursor = first.next_cursor.clone().expect("next cursor");
    let first_ids = first
        .response
        .hits
        .iter()
        .map(|h| h.id.clone())
        .collect::<Vec<_>>();
    drop(state);
    let recovered = AppState::recover(&dir).unwrap();
    let second = execute_query_v1_page(
        &recovered,
        request(&id),
        QueryPaginationV1 {
            enabled: true,
            cursor: Some(cursor),
        },
    )
    .await
    .unwrap();
    assert!(
        second
            .response
            .hits
            .iter()
            .all(|h| !first_ids.contains(&h.id))
    );
    assert_eq!(
        second.response.explain.as_ref().unwrap().strategy,
        "exact_cursor"
    );
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn filtered_cursor_and_stale_query_errors_are_explicit() {
    let (dir, state, id) = fixture("filtered").await;
    let mut req = request(&id);
    req.predicate = Some(Predicate::Eq(
        FieldPath::new(["group"]).unwrap(),
        MetadataValue::String("a".into()),
    ));
    let first = execute_query_v1_page(
        &state,
        req.clone(),
        QueryPaginationV1 {
            enabled: true,
            cursor: None,
        },
    )
    .await
    .unwrap();
    let cursor = first.next_cursor.clone().unwrap();
    let second = execute_query_v1_page(
        &state,
        req.clone(),
        QueryPaginationV1 {
            enabled: true,
            cursor: Some(cursor.clone()),
        },
    )
    .await
    .unwrap();
    assert!(
        second
            .response
            .hits
            .iter()
            .all(|h| h.metadata.get("group") == Some(&MetadataValue::String("a".into())))
    );
    let mut changed = req.clone();
    changed.vector = Some(vec![2.0]);
    assert!(matches!(
        execute_query_v1_page(
            &state,
            changed,
            QueryPaginationV1 {
                enabled: true,
                cursor: Some(cursor.clone())
            }
        )
        .await,
        Err(QueryV1Error::Cursor(_))
    ));
    WriteService::new(state.clone())
        .upsert(
            &id,
            PendingRecord {
                id: RecordId::unsigned(9),
                vector: vec![9.0],
                metadata: Metadata::new(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        execute_query_v1_page(
            &state,
            req,
            QueryPaginationV1 {
                enabled: true,
                cursor: Some(cursor)
            }
        )
        .await,
        Err(QueryV1Error::Cursor(_))
    ));
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn hybrid_pagination_is_explicitly_unsupported() {
    let (dir, state, id) = fixture("hybrid").await;
    let mut req = request(&id);
    req.text = Some("hello".into());
    assert!(matches!(
        execute_query_v1_page(
            &state,
            req,
            QueryPaginationV1 {
                enabled: true,
                cursor: None
            }
        )
        .await,
        Err(QueryV1Error::CursorUnsupported(_))
    ));
    fs::remove_dir_all(dir).unwrap();
}
