use ketebe_core::{
    CollectionName, DistanceMetric, LexicalAnalyzerConfig, Metadata, ProjectId, RecordId,
};
use ketebe_server::{
    AppState, CollectionNamespaceCatalog, CollectionService, PendingRecord, RuntimeCatalog,
    WriteService,
};
use ketebe_storage::ScopedStorageNamespace;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ketebe-{label}-{}-{nonce}", std::process::id()))
}

#[tokio::test]
async fn same_name_and_record_id_are_isolated_and_recover_after_restart() {
    let root = temp_root("project-scope-restart");
    let namespaces = CollectionNamespaceCatalog::open(&root).expect("namespace catalog");
    let name = CollectionName::new("documents").expect("name");
    let scope_a = namespaces
        .create(&ProjectId::new("project-a").expect("project"), &name)
        .expect("scope a");
    let scope_b = namespaces
        .create(&ProjectId::new("project-b").expect("project"), &name)
        .expect("scope b");
    assert_ne!(scope_a.collection_id(), scope_b.collection_id());

    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), root.clone());
    let writes = WriteService::new(state.clone());
    writes
        .create_collection_with_schema_scoped(
            &scope_a,
            2,
            DistanceMetric::L2,
            vec![],
            LexicalAnalyzerConfig::default(),
            None,
        )
        .await
        .expect("create a");
    writes
        .create_collection_with_schema_scoped(
            &scope_b,
            2,
            DistanceMetric::L2,
            vec![],
            LexicalAnalyzerConfig::default(),
            None,
        )
        .await
        .expect("create b");

    let same_id = RecordId::string("same-record").expect("record id");
    writes
        .upsert_scoped(
            &scope_a,
            PendingRecord {
                id: same_id.clone(),
                vector: vec![1.0, 0.0],
                metadata: Metadata::new(),
            },
        )
        .await
        .expect("upsert a");
    writes
        .upsert_scoped(
            &scope_b,
            PendingRecord {
                id: same_id,
                vector: vec![0.0, 1.0],
                metadata: Metadata::new(),
            },
        )
        .await
        .expect("upsert b");

    // Exercise the production sealed-segment and compaction paths before a
    // restart, not only replay from the scoped WAL.
    writes
        .seal_collection(scope_a.collection_id())
        .await
        .expect("seal a first segment");
    writes
        .seal_collection(scope_b.collection_id())
        .await
        .expect("seal b first segment");
    writes
        .upsert_scoped(
            &scope_a,
            PendingRecord {
                id: RecordId::string("a-second").expect("record id"),
                vector: vec![2.0, 0.0],
                metadata: Metadata::new(),
            },
        )
        .await
        .expect("upsert a second record");
    writes
        .upsert_scoped(
            &scope_b,
            PendingRecord {
                id: RecordId::string("b-second").expect("record id"),
                vector: vec![0.0, 2.0],
                metadata: Metadata::new(),
            },
        )
        .await
        .expect("upsert b second record");
    writes
        .seal_collection(scope_a.collection_id())
        .await
        .expect("seal a second segment");
    writes
        .seal_collection(scope_b.collection_id())
        .await
        .expect("seal b second segment");
    writes
        .compact_collection(scope_a.collection_id())
        .await
        .expect("compact a")
        .expect("a compacted checkpoint");
    writes
        .compact_collection(scope_b.collection_id())
        .await
        .expect("compact b")
        .expect("b compacted checkpoint");

    let namespace_a =
        ScopedStorageNamespace::open_existing(&root, scope_a.clone()).expect("namespace a");
    let namespace_b =
        ScopedStorageNamespace::open_existing(&root, scope_b.clone()).expect("namespace b");
    assert_ne!(namespace_a.root(), namespace_b.root());
    assert!(
        !root
            .join("collections")
            .join(scope_a.collection_id().as_str())
            .exists()
    );
    assert!(
        !root
            .join("collections")
            .join(scope_b.collection_id().as_str())
            .exists()
    );

    drop(writes);
    drop(state);
    let recovered = AppState::recover(&root).expect("recover scoped runtime");
    let collections = CollectionService::new(recovered);
    let a = collections
        .get(scope_a.collection_id())
        .await
        .expect("recovered a");
    let b = collections
        .get(scope_b.collection_id())
        .await
        .expect("recovered b");
    assert_eq!(a.live_records, 2);
    assert_eq!(b.live_records, 2);

    fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn corrupt_scope_manifest_fails_runtime_recovery_closed() {
    let root = temp_root("project-scope-corrupt");
    let namespaces = CollectionNamespaceCatalog::open(&root).expect("namespace catalog");
    let scope = namespaces
        .create(
            &ProjectId::new("project-a").expect("project"),
            &CollectionName::new("documents").expect("name"),
        )
        .expect("scope");
    let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), root.clone());
    WriteService::new(state.clone())
        .create_collection_with_schema_scoped(
            &scope,
            2,
            DistanceMetric::L2,
            vec![],
            LexicalAnalyzerConfig::default(),
            None,
        )
        .await
        .expect("create");
    let namespace = ScopedStorageNamespace::open_existing(&root, scope.clone()).expect("namespace");
    fs::write(
        namespace.root().join("scope.meta"),
        format!(
            "version=1\nproject_id=project-b\ncollection_id={}\n",
            scope.collection_id().as_str()
        ),
    )
    .expect("forge scope");
    drop(state);

    assert!(AppState::recover(&root).is_err());
    fs::remove_dir_all(root).expect("cleanup");
}
