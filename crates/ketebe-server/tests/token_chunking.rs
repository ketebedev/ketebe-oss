use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use ketebe_core::{
    ChunkingStructure, CollectionId, CollectionIngestionConfig, DistanceMetric,
    LexicalAnalyzerConfig, Metadata, RecordId, TokenChunkingPolicy, TokenizerKind,
};
use ketebe_server::{
    AppState, CollectionService, DeterministicEmbeddingProvider, RuntimeCatalog,
    TokenChunkedDocument, TokenChunkingService, WriteService, app,
};
use std::fs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tower::ServiceExt;

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "ketebe-token-chunk-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

fn policy(structure: ChunkingStructure, max_tokens: usize) -> TokenChunkingPolicy {
    TokenChunkingPolicy::new(structure, max_tokens, 0, TokenizerKind::UnicodeWordsV1).unwrap()
}

async fn state_with_tokenized_collection(
    dir: &std::path::Path,
    chunking: TokenChunkingPolicy,
) -> (AppState, CollectionId) {
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.to_path_buf());
    state
        .set_embedding_provider(Arc::new(
            DeterministicEmbeddingProvider::new("token-test", "v1").unwrap(),
        ))
        .await;
    let collection = CollectionId::new("docs").unwrap();
    let ingestion = CollectionIngestionConfig::new_tokenized("default", chunking, true).unwrap();
    WriteService::new(state.clone())
        .create_collection_with_schema(
            collection.clone(),
            4,
            DistanceMetric::L2,
            Vec::new(),
            LexicalAnalyzerConfig::default(),
            Some(ingestion),
        )
        .await
        .unwrap();
    (state, collection)
}

#[tokio::test]
async fn tokenized_reingest_reconciles_children_and_recovers_schema() {
    let dir = temp_dir("recovery");
    let chunking = policy(ChunkingStructure::Sentences, 5);
    let (state, collection) = state_with_tokenized_collection(&dir, chunking).await;
    let service = TokenChunkingService::new(state.clone());
    let parent = RecordId::string("parent-1").unwrap();

    let first = service
        .chunk_embed_and_upsert(
            &collection,
            TokenChunkedDocument {
                id: parent.clone(),
                text: "One two three. Four five six. Seven eight nine.".to_string(),
                metadata: Metadata::new(),
                chunking,
            },
        )
        .await
        .unwrap();
    assert_eq!(first.chunk_ids.len(), 3);

    let second = service
        .chunk_embed_and_upsert(
            &collection,
            TokenChunkedDocument {
                id: parent,
                text: "One two three.".to_string(),
                metadata: Metadata::new(),
                chunking,
            },
        )
        .await
        .unwrap();
    assert_eq!(second.chunk_ids.len(), 1);
    assert_eq!(second.reconciled_chunks, 2);
    assert_eq!(
        CollectionService::new(state.clone())
            .get(&collection)
            .await
            .unwrap()
            .live_records,
        1
    );

    drop(state);
    let recovered = AppState::recover(&dir).unwrap();
    let info = CollectionService::new(recovered)
        .get(&collection)
        .await
        .unwrap();
    assert_eq!(info.live_records, 1);
    assert_eq!(
        info.ingestion
            .as_ref()
            .and_then(CollectionIngestionConfig::token_chunking),
        Some(chunking)
    );
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn rest_uses_token_schema_and_rejects_character_override() {
    let dir = temp_dir("rest");
    let chunking = policy(ChunkingStructure::Markdown, 5);
    let (state, _) = state_with_tokenized_collection(&dir, chunking).await;

    let request = Request::builder()
        .method("PUT")
        .uri("/v0/collections/docs/documents/rest-parent")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r##"{"text":"intro words\n# Heading\nnext words here"}"##,
        ))
        .unwrap();
    let response = app(state.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["chunking"], "token_aware");
    assert!(json["chunk_count"].as_u64().unwrap() >= 2);

    let request = Request::builder()
        .method("GET")
        .uri("/v0/collections/docs")
        .body(Body::empty())
        .unwrap();
    let response = app(state.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["ingestion"]["token_chunking"]["strategy"], "markdown");
    assert_eq!(json["ingestion"]["token_chunking"]["max_tokens"], 5);
    assert_eq!(
        json["ingestion"]["token_chunking"]["tokenizer"],
        "unicode_words_v1"
    );

    let request = Request::builder()
        .method("PUT")
        .uri("/v0/collections/docs/documents/rest-parent")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"text":"override attempt","chunking":{"max_chars":8,"overlap_chars":1}}"#,
        ))
        .unwrap();
    let response = app(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    fs::remove_dir_all(dir).unwrap();
}
