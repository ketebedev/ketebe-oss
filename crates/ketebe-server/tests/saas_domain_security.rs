use std::sync::Arc;

use ketebe_core::{CollectionName, OrganizationId, ProjectId, ResourceLifecycleState};
use ketebe_server::{
    ApiKeyStore, AuditService, AuthorizationAction, AuthorizationService,
    CollectionNamespaceCatalog, ControlPlaneApiService, CreateOrganizationInput,
    CreateProjectInput, CredentialAuthenticator, OrganizationMembershipService, OrganizationRole,
    Principal, PrincipalKind, ProjectMembershipService, ProjectRole, ServiceAccountId,
    ServiceAccountLifecycle, ServiceAccountStore, UpdateProjectInput,
    bootstrap_default_control_plane,
};
use ketebe_storage::ControlPlaneStore;
use tempfile::TempDir;

struct DomainFixture {
    dir: TempDir,
    owner_a: Principal,
    org_a: OrganizationId,
    project_a1: ProjectId,
    project_a2: ProjectId,
    project_b1: ProjectId,
}

fn human(subject: &str) -> Principal {
    Principal::new(subject, PrincipalKind::Credential).expect("human principal")
}

fn fixture() -> DomainFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let owner_a = human("owner-a");
    let owner_b = human("owner-b");
    let api = ControlPlaneApiService::new(dir.path(), Arc::new(AuditService::noop()));

    let org_a = api
        .create_organization(
            &owner_a,
            CreateOrganizationInput {
                id: "org-a".into(),
                name: "Organization A".into(),
                slug: "org-a".into(),
            },
        )
        .expect("org a");
    let org_b = api
        .create_organization(
            &owner_b,
            CreateOrganizationInput {
                id: "org-b".into(),
                name: "Organization B".into(),
                slug: "org-b".into(),
            },
        )
        .expect("org b");

    let project_a1 = api
        .create_project(
            &owner_a,
            org_a.id(),
            CreateProjectInput {
                id: "project-a1".into(),
                name: "Project A1".into(),
                slug: "project-a1".into(),
            },
        )
        .expect("project a1");
    let project_a2 = api
        .create_project(
            &owner_a,
            org_a.id(),
            CreateProjectInput {
                id: "project-a2".into(),
                name: "Project A2".into(),
                slug: "project-a2".into(),
            },
        )
        .expect("project a2");
    let project_b1 = api
        .create_project(
            &owner_b,
            org_b.id(),
            CreateProjectInput {
                id: "project-b1".into(),
                name: "Project B1".into(),
                slug: "project-b1".into(),
            },
        )
        .expect("project b1");

    DomainFixture {
        dir,
        owner_a,
        org_a: org_a.id().clone(),
        project_a1: project_a1.id().clone(),
        project_a2: project_a2.id().clone(),
        project_b1: project_b1.id().clone(),
    }
}

fn membership_services(
    fixture: &DomainFixture,
) -> (OrganizationMembershipService, ProjectMembershipService) {
    let store =
        ControlPlaneStore::open(fixture.dir.path().join("control-plane")).expect("control plane");
    let audit = Arc::new(AuditService::noop());
    let organizations =
        OrganizationMembershipService::open(fixture.dir.path(), store.clone(), Arc::clone(&audit))
            .expect("organization memberships");
    let projects = ProjectMembershipService::open(fixture.dir.path(), store, audit)
        .expect("project memberships");
    (organizations, projects)
}

#[test]
fn cross_organization_and_multi_project_human_access_fail_closed() {
    let fixture = fixture();
    let subject = human("shared-human");
    let org_only = human("org-only-human");
    let (organizations, projects) = membership_services(&fixture);

    organizations
        .create_membership(
            &fixture.owner_a,
            &fixture.org_a,
            subject.subject(),
            OrganizationRole::Member,
        )
        .expect("organization membership");
    organizations
        .create_membership(
            &fixture.owner_a,
            &fixture.org_a,
            org_only.subject(),
            OrganizationRole::Member,
        )
        .expect("organization-only membership");

    projects
        .create_membership(
            &fixture.owner_a,
            &fixture.project_a1,
            subject.subject(),
            ProjectRole::Reader,
        )
        .expect("a1 reader");
    projects
        .create_membership(
            &fixture.owner_a,
            &fixture.project_a2,
            subject.subject(),
            ProjectRole::Editor,
        )
        .expect("a2 editor");

    let authorization = AuthorizationService::required(fixture.dir.path()).expect("authorization");

    assert!(
        authorization
            .authorize_project(
                &subject,
                AuthorizationAction::CollectionRead,
                fixture.project_a1.as_str(),
            )
            .is_ok()
    );
    assert!(
        authorization
            .authorize_project(
                &subject,
                AuthorizationAction::CollectionWrite,
                fixture.project_a1.as_str(),
            )
            .is_err()
    );
    assert!(
        authorization
            .authorize_project(
                &subject,
                AuthorizationAction::CollectionWrite,
                fixture.project_a2.as_str(),
            )
            .is_ok()
    );
    assert!(
        authorization
            .authorize_project(
                &subject,
                AuthorizationAction::CollectionRead,
                fixture.project_b1.as_str(),
            )
            .is_err()
    );
    assert!(
        authorization
            .authorize_project(
                &org_only,
                AuthorizationAction::CollectionRead,
                fixture.project_a1.as_str(),
            )
            .is_err(),
        "Organization membership alone must not grant Project data access"
    );
}

#[test]
fn membership_role_changes_and_revocation_are_immediate_and_survive_restart() {
    let fixture = fixture();
    let subject = human("mutable-human");
    let (_, projects) = membership_services(&fixture);

    projects
        .create_membership(
            &fixture.owner_a,
            &fixture.project_a1,
            subject.subject(),
            ProjectRole::Reader,
        )
        .expect("reader membership");

    let authorization = AuthorizationService::required(fixture.dir.path()).expect("authorization");
    assert!(
        authorization
            .authorize_project(
                &subject,
                AuthorizationAction::CollectionWrite,
                fixture.project_a1.as_str(),
            )
            .is_err()
    );

    projects
        .update_role(
            &fixture.owner_a,
            &fixture.project_a1,
            subject.subject(),
            ProjectRole::Editor,
        )
        .expect("role update");
    assert!(
        authorization
            .authorize_project(
                &subject,
                AuthorizationAction::CollectionWrite,
                fixture.project_a1.as_str(),
            )
            .is_ok(),
        "role update must affect an already-open AuthorizationService"
    );

    projects
        .remove_membership(&fixture.owner_a, &fixture.project_a1, subject.subject())
        .expect("membership revoke");
    assert!(
        authorization
            .authorize_project(
                &subject,
                AuthorizationAction::CollectionRead,
                fixture.project_a1.as_str(),
            )
            .is_err(),
        "membership revoke must affect an already-open AuthorizationService"
    );

    let reopened = AuthorizationService::required(fixture.dir.path()).expect("reopened auth");
    assert!(
        reopened
            .authorize_project(
                &subject,
                AuthorizationAction::CollectionRead,
                fixture.project_a1.as_str(),
            )
            .is_err(),
        "restart must not resurrect revoked authority"
    );
}

#[test]
fn inactive_project_denies_human_and_workload_authorization() {
    let fixture = fixture();
    let subject = human("project-user");
    let (_, projects) = membership_services(&fixture);

    projects
        .create_membership(
            &fixture.owner_a,
            &fixture.project_a2,
            subject.subject(),
            ProjectRole::Editor,
        )
        .expect("project membership");

    let authorization = AuthorizationService::required(fixture.dir.path()).expect("authorization");
    assert!(
        authorization
            .authorize_project(
                &subject,
                AuthorizationAction::CollectionRead,
                fixture.project_a2.as_str(),
            )
            .is_ok()
    );

    let api = ControlPlaneApiService::new(fixture.dir.path(), Arc::new(AuditService::noop()));
    api.update_project(
        &fixture.owner_a,
        &fixture.project_a2,
        UpdateProjectInput {
            name: "Project A2".into(),
            slug: "project-a2".into(),
            lifecycle: ResourceLifecycleState::Suspended,
        },
    )
    .expect("suspend project");

    assert!(
        authorization
            .authorize_project(
                &subject,
                AuthorizationAction::CollectionRead,
                fixture.project_a2.as_str(),
            )
            .is_err()
    );

    let workload = Principal::for_workload_project("workload-a2", fixture.project_a2.as_str())
        .expect("workload principal");
    assert!(
        authorization
            .authorize_project(
                &workload,
                AuthorizationAction::CollectionRead,
                fixture.project_a2.as_str(),
            )
            .is_err()
    );
}

#[test]
fn service_account_and_api_key_revocation_survive_restart() {
    let fixture = fixture();
    let service_accounts =
        ServiceAccountStore::open(fixture.dir.path()).expect("service account store");
    let service_account_id = ServiceAccountId::new("search-service").expect("service account id");
    service_accounts
        .create(service_account_id.clone(), "Search Service")
        .expect("service account");
    service_accounts
        .assign_project(
            &service_account_id,
            &fixture.project_a1,
            ProjectRole::Editor,
        )
        .expect("project assignment");

    let keys = ApiKeyStore::open(fixture.dir.path()).expect("api key store");
    let issued = keys
        .create_for_service_account(&service_account_id, fixture.project_a1.as_str(), None)
        .expect("service account key");
    let principal = keys
        .authenticate(&issued.credential)
        .expect("authenticate key");
    assert_eq!(
        principal.workload_project_id(),
        Some(fixture.project_a1.as_str())
    );

    keys.revoke(&issued.metadata.id).expect("revoke key");
    assert!(keys.authenticate(&issued.credential).is_err());
    let reopened_keys = ApiKeyStore::open(fixture.dir.path()).expect("reopened keys");
    assert!(reopened_keys.authenticate(&issued.credential).is_err());

    let active_key = reopened_keys
        .create_for_service_account(&service_account_id, fixture.project_a1.as_str(), None)
        .expect("second key");
    service_accounts
        .set_lifecycle(&service_account_id, ServiceAccountLifecycle::Disabled)
        .expect("disable service account");
    assert!(
        reopened_keys.authenticate(&active_key.credential).is_err(),
        "disabled Service Account must invalidate its API keys immediately"
    );
    let reopened_again = ApiKeyStore::open(fixture.dir.path()).expect("reopened keys again");
    assert!(reopened_again.authenticate(&active_key.credential).is_err());
}

#[test]
fn identical_collection_names_remain_project_scoped_across_restart() {
    let fixture = fixture();
    let catalog = CollectionNamespaceCatalog::open(fixture.dir.path()).expect("catalog");
    let name = CollectionName::new("docs").expect("collection name");

    let a1 = catalog
        .create(&fixture.project_a1, &name)
        .expect("a1 collection");
    let a2 = catalog
        .create(&fixture.project_a2, &name)
        .expect("a2 collection");
    let b1 = catalog
        .create(&fixture.project_b1, &name)
        .expect("b1 collection");

    assert_ne!(a1.collection_id(), a2.collection_id());
    assert_ne!(a1.collection_id(), b1.collection_id());
    assert_eq!(a1.project_id(), &fixture.project_a1);
    assert_eq!(a2.project_id(), &fixture.project_a2);
    assert_eq!(b1.project_id(), &fixture.project_b1);

    let reopened = CollectionNamespaceCatalog::open(fixture.dir.path()).expect("reopened catalog");
    assert_eq!(
        reopened
            .resolve(&fixture.project_a1, &name)
            .expect("resolve a1"),
        Some(a1)
    );
    assert_eq!(
        reopened
            .resolve(&fixture.project_a2, &name)
            .expect("resolve a2"),
        Some(a2)
    );
    assert_eq!(
        reopened
            .resolve(&fixture.project_b1, &name)
            .expect("resolve b1"),
        Some(b1)
    );
}

#[test]
fn default_control_plane_migration_is_idempotent_and_restart_safe() {
    let dir = tempfile::tempdir().expect("tempdir");

    let first = bootstrap_default_control_plane(dir.path()).expect("first migration");
    assert!(first.default_resources_created);

    let second = bootstrap_default_control_plane(dir.path()).expect("second migration");
    assert!(!second.default_resources_created);

    let store = ControlPlaneStore::open(dir.path().join("control-plane")).expect("control plane");
    let default_org = OrganizationId::default_organization();
    let default_project = ProjectId::default_project();
    let project = store
        .project(&default_project)
        .expect("read project")
        .expect("default project");
    assert_eq!(project.organization_id(), &default_org);
    assert_eq!(project.lifecycle(), ResourceLifecycleState::Active);
}
