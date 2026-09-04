use ketebe_core::{
    CollectionId, DataEncryptionKeyRef, DataEncryptionKeyResolver, DataEncryptionKeyVersion,
    DataEncryptionOwnership, DataEncryptionPolicy, DataPlaneScope, ProjectId,
};
use ketebe_server::{
    ApiKeyStore, AuthenticationService, AuthorizationAction, AuthorizationError,
    AuthorizationService, ClaimOutcome, InMemoryResourceGovernor, ProjectResourceBudget,
    ResourceGovernanceError, ResourceGovernor, ResourceWorkClass,
};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("ketebe-{label}-{}-{nonce}", std::process::id()))
}

fn scope(project: &str, collection: &str) -> DataPlaneScope {
    DataPlaneScope::new(
        ProjectId::new(project).expect("project"),
        CollectionId::new(collection).expect("collection"),
    )
}

#[test]
fn api_key_rotation_revocation_and_restart_fail_closed() {
    let root = temp_root("security-api-keys");
    let store = ApiKeyStore::open(&root).expect("store");
    let issued = store.create("project-a", None).expect("create");
    let old = issued.credential.clone();
    let auth = AuthenticationService::required(Arc::new(store.clone()));
    assert_eq!(
        auth.authenticate_authorization_value(Some(&format!("Bearer {}", old.expose_secret())))
            .expect("old credential active")
            .project_id(),
        Some("project-a")
    );

    let rotated = store.rotate(&issued.metadata.id).expect("rotate");
    assert!(
        auth.authenticate_authorization_value(Some(&format!("Bearer {}", old.expose_secret())))
            .is_err()
    );
    assert!(
        auth.authenticate_authorization_value(Some(&format!(
            "Bearer {}",
            rotated.credential.expose_secret()
        )))
        .is_ok()
    );

    store.revoke(&issued.metadata.id).expect("revoke");
    assert!(
        auth.authenticate_authorization_value(Some(&format!(
            "Bearer {}",
            rotated.credential.expose_secret()
        )))
        .is_err()
    );
    drop(auth);
    drop(store);

    let reopened = ApiKeyStore::open(&root).expect("reopen");
    let auth = AuthenticationService::required(Arc::new(reopened));
    assert!(
        auth.authenticate_authorization_value(Some(&format!(
            "Bearer {}",
            rotated.credential.expose_secret()
        )))
        .is_err()
    );

    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn authorization_prevents_cross_project_discovery_and_preserves_same_name_namespace() {
    let root = temp_root("security-authz");
    let key_store = ApiKeyStore::open(&root).expect("key store");
    let key_a = key_store.create("project-a", None).expect("key a");
    let key_b = key_store.create("project-b", None).expect("key b");
    let auth = AuthenticationService::required(Arc::new(key_store));
    let principal_a = auth
        .authenticate_authorization_value(Some(&format!(
            "Bearer {}",
            key_a.credential.expose_secret()
        )))
        .expect("principal a");
    let principal_b = auth
        .authenticate_authorization_value(Some(&format!(
            "Bearer {}",
            key_b.credential.expose_secret()
        )))
        .expect("principal b");

    let authorization = AuthorizationService::required(&root).expect("authorization");
    assert_eq!(
        authorization
            .claim_collection(&principal_a, "documents")
            .expect("claim a"),
        ClaimOutcome::Inserted
    );
    assert_eq!(
        authorization
            .claim_collection(&principal_b, "documents")
            .expect("claim b"),
        ClaimOutcome::Inserted
    );
    authorization
        .claim_collection(&principal_b, "private-b")
        .expect("claim private b");

    assert!(authorization.can_discover_collection(&principal_a, "documents"));
    assert!(authorization.can_discover_collection(&principal_b, "documents"));
    assert!(!authorization.can_discover_collection(&principal_a, "private-b"));
    assert!(matches!(
        authorization.authorize_collection(
            &principal_a,
            AuthorizationAction::CollectionRead,
            "private-b"
        ),
        Err(AuthorizationError::Undiscoverable)
    ));
    assert!(matches!(
        authorization.authorize_project(
            &principal_a,
            AuthorizationAction::ProjectAdmin,
            "project-b"
        ),
        Err(AuthorizationError::Denied)
    ));

    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn noisy_neighbor_budget_is_project_scoped_and_retry_semantics_are_stable() {
    let budget = ProjectResourceBudget {
        max_concurrent_queries: 1,
        max_concurrent_writes: 1,
        max_concurrent_ingestion: 1,
        max_concurrent_background: 1,
        ingestion_throughput: None,
    };
    let governor = InMemoryResourceGovernor::new(budget).expect("governor");
    let scope_a = scope("project-a", "collection-a");
    let scope_b = scope("project-b", "collection-b");

    let _held = governor
        .admit(&scope_a, ResourceWorkClass::Query, 1)
        .expect("first project-a query");
    let denied = match governor.admit(&scope_a, ResourceWorkClass::Query, 1) {
        Ok(_) => panic!("project-a second query must be denied"),
        Err(error) => error,
    };
    assert!(matches!(
        denied,
        ResourceGovernanceError::ConcurrencyExceeded {
            class: ResourceWorkClass::Query,
            limit: 1,
            ..
        }
    ));
    assert!(denied.retryable());
    assert!(denied.retry_after().is_some());

    let _other_project = governor
        .admit(&scope_b, ResourceWorkClass::Query, 1)
        .expect("project-b remains independent");
    let metrics = governor.prometheus_metrics().expect("metrics");
    assert!(!metrics.contains("project-a"));
    assert!(!metrics.contains("project-b"));
}

#[test]
fn encryption_key_ownership_rejects_cross_project_resolution_and_redacts_material() {
    let resolver = ketebe_core::LocalDataEncryptionKeyResolver::new();
    let owner_a = DataEncryptionOwnership::new(
        scope("project-a", "collection-a"),
        DataEncryptionPolicy::new(
            DataEncryptionKeyRef::new("local/key-a").expect("key ref"),
            DataEncryptionKeyVersion::new(1).expect("version"),
        ),
    );
    resolver
        .insert(owner_a.clone(), b"plaintext-must-not-leak".to_vec())
        .expect("insert");

    let resolved = resolver.resolve(&owner_a).expect("resolve owner a");
    let debug = format!("{resolved:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("plaintext-must-not-leak"));

    let owner_b =
        DataEncryptionOwnership::new(scope("project-b", "collection-a"), owner_a.policy().clone());
    assert!(resolver.resolve(&owner_b).is_err());
}
