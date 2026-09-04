use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::Principal;

const STORE_VERSION: u32 = 1;

fn collection_policy_key(project_id: &str, collection_id: &str) -> String {
    format!("{project_id}\u{1f}{collection_id}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectRole {
    Reader,
    Editor,
    Owner,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionPermission {
    Read,
    Write,
    Admin,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizationAction {
    CollectionDiscover,
    CollectionRead,
    CollectionCreate,
    CollectionWrite,
    CollectionDelete,
    ProjectAdmin,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorizationResource {
    Project {
        project_id: String,
    },
    Collection {
        project_id: String,
        collection_id: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizationMode {
    Development,
    Required,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimOutcome {
    Inserted,
    Existing,
}

#[derive(Debug)]
pub enum AuthorizationError {
    Denied,
    Undiscoverable,
    MissingProject,
    OwnershipConflict,
    Io(std::io::Error),
    Json(serde_json::Error),
    UnsupportedVersion(u32),
    LockPoisoned,
}

impl fmt::Display for AuthorizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Denied => f.write_str("authorization denied"),
            Self::Undiscoverable => f.write_str("resource is not discoverable"),
            Self::MissingProject => f.write_str("authenticated principal has no project scope"),
            Self::OwnershipConflict => f.write_str("resource belongs to another project"),
            Self::Io(error) => write!(f, "authorization store I/O error: {error}"),
            Self::Json(error) => write!(f, "authorization store JSON error: {error}"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported authorization store version {version}")
            }
            Self::LockPoisoned => f.write_str("authorization store lock poisoned"),
        }
    }
}
impl std::error::Error for AuthorizationError {}
impl From<std::io::Error> for AuthorizationError {
    fn from(v: std::io::Error) -> Self {
        Self::Io(v)
    }
}
impl From<serde_json::Error> for AuthorizationError {
    fn from(v: serde_json::Error) -> Self {
        Self::Json(v)
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct ProjectPolicy {
    roles: BTreeMap<String, ProjectRole>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct CollectionPolicy {
    project_id: String,
    permissions: BTreeMap<String, CollectionPermission>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PolicyFile {
    version: u32,
    projects: BTreeMap<String, ProjectPolicy>,
    collections: BTreeMap<String, CollectionPolicy>,
}
impl Default for PolicyFile {
    fn default() -> Self {
        Self {
            version: STORE_VERSION,
            projects: BTreeMap::new(),
            collections: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct AuthorizationService {
    mode: AuthorizationMode,
    path: Option<Arc<PathBuf>>,
    state: Arc<Mutex<PolicyFile>>,
}

impl fmt::Debug for AuthorizationService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthorizationService")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl AuthorizationService {
    #[must_use]
    pub fn development() -> Self {
        Self {
            mode: AuthorizationMode::Development,
            path: None,
            state: Arc::new(Mutex::new(PolicyFile::default())),
        }
    }

    pub fn required(data_dir: impl AsRef<Path>) -> Result<Self, AuthorizationError> {
        let path = data_dir
            .as_ref()
            .join("security")
            .join("authorization.json");
        let state = if path.exists() {
            let decoded: PolicyFile = serde_json::from_slice(&fs::read(&path)?)?;
            if decoded.version != STORE_VERSION {
                return Err(AuthorizationError::UnsupportedVersion(decoded.version));
            }
            decoded
        } else {
            PolicyFile::default()
        };
        Ok(Self {
            mode: AuthorizationMode::Required,
            path: Some(Arc::new(path)),
            state: Arc::new(Mutex::new(state)),
        })
    }

    #[must_use]
    pub fn mode(&self) -> AuthorizationMode {
        self.mode
    }

    pub fn set_project_role(
        &self,
        project_id: &str,
        subject: &str,
        role: ProjectRole,
    ) -> Result<(), AuthorizationError> {
        if self.mode == AuthorizationMode::Development {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| AuthorizationError::LockPoisoned)?;
        state
            .projects
            .entry(project_id.to_string())
            .or_default()
            .roles
            .insert(subject.to_string(), role);
        self.persist(&state)
    }

    pub fn set_collection_permission(
        &self,
        project_id: &str,
        collection_id: &str,
        subject: &str,
        permission: CollectionPermission,
    ) -> Result<(), AuthorizationError> {
        if self.mode == AuthorizationMode::Development {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| AuthorizationError::LockPoisoned)?;
        let key = collection_policy_key(project_id, collection_id);
        let collection = if state.collections.contains_key(&key) {
            state.collections.get_mut(&key)
        } else {
            state
                .collections
                .get_mut(collection_id)
                .filter(|legacy| legacy.project_id == project_id)
        }
        .ok_or(AuthorizationError::Undiscoverable)?;
        collection
            .permissions
            .insert(subject.to_string(), permission);
        self.persist(&state)
    }

    pub fn authorize_project(
        &self,
        principal: &Principal,
        action: AuthorizationAction,
        project_id: &str,
    ) -> Result<(), AuthorizationError> {
        if self.mode == AuthorizationMode::Development {
            return Ok(());
        }
        if principal.project_id() != Some(project_id) {
            return Err(AuthorizationError::Denied);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| AuthorizationError::LockPoisoned)?;
        let role = state
            .projects
            .get(project_id)
            .and_then(|p| p.roles.get(principal.subject()).copied())
            .unwrap_or(ProjectRole::Owner);
        if project_role_allows(role, action) {
            Ok(())
        } else {
            Err(AuthorizationError::Denied)
        }
    }

    pub fn authorize_principal_project(
        &self,
        principal: &Principal,
        action: AuthorizationAction,
    ) -> Result<String, AuthorizationError> {
        if self.mode == AuthorizationMode::Development {
            return Ok("development".to_string());
        }
        let project = principal
            .project_id()
            .ok_or(AuthorizationError::MissingProject)?
            .to_string();
        self.authorize_project(principal, action, &project)?;
        Ok(project)
    }

    pub fn authorize_collection(
        &self,
        principal: &Principal,
        action: AuthorizationAction,
        collection_id: &str,
    ) -> Result<AuthorizationResource, AuthorizationError> {
        if self.mode == AuthorizationMode::Development {
            return Ok(AuthorizationResource::Collection {
                project_id: "development".to_string(),
                collection_id: collection_id.to_string(),
            });
        }
        let project_id = principal
            .project_id()
            .ok_or(AuthorizationError::MissingProject)?;
        let state = self
            .state
            .lock()
            .map_err(|_| AuthorizationError::LockPoisoned)?;
        let key = collection_policy_key(project_id, collection_id);
        let collection = state
            .collections
            .get(&key)
            .or_else(|| {
                state
                    .collections
                    .get(collection_id)
                    .filter(|legacy| legacy.project_id == project_id)
            })
            .ok_or(AuthorizationError::Undiscoverable)?;
        let project_role = state
            .projects
            .get(&collection.project_id)
            .and_then(|p| p.roles.get(principal.subject()).copied())
            .unwrap_or(ProjectRole::Owner);
        let allowed =
            if let Some(permission) = collection.permissions.get(principal.subject()).copied() {
                collection_permission_allows(permission, action)
            } else {
                project_role_allows(project_role, action)
            };
        if !allowed {
            return Err(AuthorizationError::Undiscoverable);
        }
        Ok(AuthorizationResource::Collection {
            project_id: collection.project_id.clone(),
            collection_id: collection_id.to_string(),
        })
    }

    pub fn project_collection_count(&self, project_id: &str) -> Result<u64, AuthorizationError> {
        if self.mode == AuthorizationMode::Development {
            return Ok(0);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| AuthorizationError::LockPoisoned)?;
        Ok(state
            .collections
            .values()
            .filter(|collection| collection.project_id == project_id)
            .count() as u64)
    }

    pub fn can_discover_collection(&self, principal: &Principal, collection_id: &str) -> bool {
        self.authorize_collection(
            principal,
            AuthorizationAction::CollectionDiscover,
            collection_id,
        )
        .is_ok()
    }

    pub fn claim_collection(
        &self,
        principal: &Principal,
        collection_id: &str,
    ) -> Result<ClaimOutcome, AuthorizationError> {
        if self.mode == AuthorizationMode::Development {
            return Ok(ClaimOutcome::Existing);
        }
        let project_id =
            self.authorize_principal_project(principal, AuthorizationAction::CollectionCreate)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| AuthorizationError::LockPoisoned)?;
        let key = collection_policy_key(&project_id, collection_id);
        match state.collections.get(&key) {
            Some(existing) if existing.project_id == project_id => Ok(ClaimOutcome::Existing),
            Some(_) => Err(AuthorizationError::OwnershipConflict),
            None => {
                state.collections.insert(
                    key,
                    CollectionPolicy {
                        project_id,
                        permissions: BTreeMap::new(),
                    },
                );
                self.persist(&state)?;
                Ok(ClaimOutcome::Inserted)
            }
        }
    }

    pub fn release_collection_claim_for_principal(
        &self,
        principal: &Principal,
        collection_id: &str,
        outcome: ClaimOutcome,
    ) -> Result<(), AuthorizationError> {
        if self.mode == AuthorizationMode::Development || outcome != ClaimOutcome::Inserted {
            return Ok(());
        }
        let project_id = principal
            .project_id()
            .ok_or(AuthorizationError::MissingProject)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| AuthorizationError::LockPoisoned)?;
        state
            .collections
            .remove(&collection_policy_key(project_id, collection_id));
        self.persist(&state)
    }

    pub fn release_collection_claim(
        &self,
        collection_id: &str,
        outcome: ClaimOutcome,
    ) -> Result<(), AuthorizationError> {
        if self.mode == AuthorizationMode::Development || outcome != ClaimOutcome::Inserted {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| AuthorizationError::LockPoisoned)?;
        state.collections.remove(collection_id);
        self.persist(&state)
    }

    pub fn remove_collection(
        &self,
        principal: &Principal,
        collection_id: &str,
    ) -> Result<(), AuthorizationError> {
        if self.mode == AuthorizationMode::Development {
            return Ok(());
        }
        self.authorize_collection(
            principal,
            AuthorizationAction::CollectionDelete,
            collection_id,
        )?;
        let project_id = principal
            .project_id()
            .ok_or(AuthorizationError::MissingProject)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| AuthorizationError::LockPoisoned)?;
        state
            .collections
            .remove(&collection_policy_key(project_id, collection_id));
        if state
            .collections
            .get(collection_id)
            .is_some_and(|legacy| legacy.project_id == project_id)
        {
            state.collections.remove(collection_id);
        }
        self.persist(&state)
    }

    fn persist(&self, state: &PolicyFile) -> Result<(), AuthorizationError> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        let parent = path.parent().expect("authorization path has parent");
        fs::create_dir_all(parent)?;
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(state)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
        }
        fs::rename(tmp, path.as_ref())?;
        Ok(())
    }
}

fn project_role_allows(role: ProjectRole, action: AuthorizationAction) -> bool {
    match role {
        ProjectRole::Owner => true,
        ProjectRole::Editor => matches!(
            action,
            AuthorizationAction::CollectionDiscover
                | AuthorizationAction::CollectionRead
                | AuthorizationAction::CollectionCreate
                | AuthorizationAction::CollectionWrite
        ),
        ProjectRole::Reader => matches!(
            action,
            AuthorizationAction::CollectionDiscover | AuthorizationAction::CollectionRead
        ),
    }
}
fn collection_permission_allows(
    permission: CollectionPermission,
    action: AuthorizationAction,
) -> bool {
    match permission {
        CollectionPermission::Admin => true,
        CollectionPermission::Write => matches!(
            action,
            AuthorizationAction::CollectionDiscover
                | AuthorizationAction::CollectionRead
                | AuthorizationAction::CollectionWrite
        ),
        CollectionPermission::Read => matches!(
            action,
            AuthorizationAction::CollectionDiscover | AuthorizationAction::CollectionRead
        ),
    }
}

fn audit_http_decision(
    state: &crate::AppState,
    principal: &Principal,
    category: crate::AuditCategory,
    action: &str,
    result: crate::AuditResult,
    resource_id: Option<&str>,
    correlation_id: Option<&str>,
) {
    let mut event = crate::AuditEvent::new(category, action, result, crate::AuditOrigin::Http)
        .with_actor(principal.subject());
    if let Some(project_id) = principal.project_id() {
        event = event.with_project(project_id);
    }
    if let Some(resource_id) = resource_id {
        event = event.with_resource("collection", resource_id);
    }
    if let Some(correlation_id) = correlation_id {
        event = event.with_correlation_id(correlation_id);
    }
    let _ = state.audit().record(&event);
}

pub(crate) async fn http_authorize(
    State(state): State<crate::AppState>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let correlation_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if matches!(path, "/healthz" | "/readyz" | "/metrics") {
        return next.run(request).await;
    }
    let Some(principal) = request.extensions().get::<Principal>() else {
        return forbidden_response();
    };
    if request.method() == Method::POST && path == "/v0/collections" {
        let project_id = match state
            .authorization()
            .authorize_principal_project(principal, AuthorizationAction::CollectionCreate)
        {
            Ok(project_id) => project_id,
            Err(_) => {
                audit_http_decision(
                    &state,
                    principal,
                    crate::AuditCategory::Authorization,
                    "collection_create",
                    crate::AuditResult::Denied,
                    None,
                    correlation_id.as_deref(),
                );
                return forbidden_response();
            }
        };
        audit_http_decision(
            &state,
            principal,
            crate::AuditCategory::Authorization,
            "collection_create",
            crate::AuditResult::Allowed,
            None,
            correlation_id.as_deref(),
        );
        if let Err(error) = state
            .governance()
            .admit(&project_id, crate::AdmissionClass::Write)
        {
            audit_http_decision(
                &state,
                principal,
                crate::AuditCategory::Governance,
                "write_admission",
                crate::AuditResult::Denied,
                None,
                correlation_id.as_deref(),
            );
            return governance_response(error);
        }
        audit_http_decision(
            &state,
            principal,
            crate::AuditCategory::Governance,
            "write_admission",
            crate::AuditResult::Allowed,
            None,
            correlation_id.as_deref(),
        );
        let current = match state.authorization().project_collection_count(&project_id) {
            Ok(current) => current,
            Err(_) => return forbidden_response(),
        };
        if let Err(error) = state
            .governance()
            .check_collection_quota(&project_id, current, 1)
        {
            return governance_response(error);
        }
        return next.run(request).await;
    }
    if request.method() == Method::GET && path == "/v0/collections" {
        let project_id = match state
            .authorization()
            .authorize_principal_project(principal, AuthorizationAction::CollectionDiscover)
        {
            Ok(project_id) => project_id,
            Err(_) => return forbidden_response(),
        };
        if let Err(error) = state
            .governance()
            .admit(&project_id, crate::AdmissionClass::Read)
        {
            return governance_response(error);
        }
        return next.run(request).await;
    }
    if let Some(collection_id) = collection_id_from_path(path) {
        let action = match *request.method() {
            Method::GET => AuthorizationAction::CollectionRead,
            Method::DELETE => AuthorizationAction::CollectionDelete,
            _ => AuthorizationAction::CollectionWrite,
        };
        let resource =
            match state
                .authorization()
                .authorize_collection(principal, action, collection_id)
            {
                Ok(resource) => resource,
                Err(_) => {
                    audit_http_decision(
                        &state,
                        principal,
                        crate::AuditCategory::Authorization,
                        "collection_access",
                        crate::AuditResult::Denied,
                        Some(collection_id),
                        correlation_id.as_deref(),
                    );
                    return undiscoverable_response();
                }
            };
        audit_http_decision(
            &state,
            principal,
            crate::AuditCategory::Authorization,
            "collection_access",
            crate::AuditResult::Allowed,
            Some(collection_id),
            correlation_id.as_deref(),
        );
        let AuthorizationResource::Collection { project_id, .. } = resource else {
            return forbidden_response();
        };
        if let Err(error) = state.governance().admit(
            &project_id,
            crate::AdmissionClass::from_authorization_action(action),
        ) {
            audit_http_decision(
                &state,
                principal,
                crate::AuditCategory::Governance,
                "request_admission",
                crate::AuditResult::Denied,
                Some(collection_id),
                correlation_id.as_deref(),
            );
            return governance_response(error);
        }
        audit_http_decision(
            &state,
            principal,
            crate::AuditCategory::Governance,
            "request_admission",
            crate::AuditResult::Allowed,
            Some(collection_id),
            correlation_id.as_deref(),
        );
        return next.run(request).await;
    }
    let project_id = principal.project_id().unwrap_or("development");
    let class = match *request.method() {
        Method::GET => crate::AdmissionClass::Read,
        Method::DELETE => crate::AdmissionClass::Admin,
        _ => crate::AdmissionClass::Write,
    };
    if let Err(error) = state.governance().admit(project_id, class) {
        return governance_response(error);
    }
    next.run(request).await
}

fn collection_id_from_path(path: &str) -> Option<&str> {
    let mut parts = path.split('/').filter(|part| !part.is_empty());
    while let Some(part) = parts.next() {
        if part == "collections" {
            return parts.next().filter(|value| !value.is_empty());
        }
    }
    None
}

fn governance_response(error: crate::GovernanceError) -> Response {
    let (code, message) = match &error {
        crate::GovernanceError::RateLimited { .. } => {
            ("rate_limited", "project request rate limit exceeded")
        }
        crate::GovernanceError::CollectionQuotaExceeded { .. }
        | crate::GovernanceError::RecordQuotaExceeded { .. } => {
            ("quota_exceeded", "project resource quota exceeded")
        }
        _ => ("governance_error", "governance admission failed"),
    };
    let status = match error {
        crate::GovernanceError::RateLimited { .. }
        | crate::GovernanceError::CollectionQuotaExceeded { .. }
        | crate::GovernanceError::RecordQuotaExceeded { .. } => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut response = (
        status,
        Json(serde_json::json!({"error":{"code":code,"message":message}})),
    )
        .into_response();
    if let crate::GovernanceError::RateLimited { retry_after } = error {
        let seconds = retry_after.as_secs().max(1);
        if let Ok(value) = seconds.to_string().parse() {
            response.headers_mut().insert("retry-after", value);
        }
    }
    response
}

fn forbidden_response() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({"error":{"code":"forbidden","message":"authorization denied"}})),
    )
        .into_response()
}
fn undiscoverable_response() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({"error":{"code":"collection_not_found","message":"collection was not found"}}))).into_response()
}

pub(crate) fn grpc_principal<T>(request: &tonic::Request<T>) -> Result<Principal, tonic::Status> {
    request
        .extensions()
        .get::<Principal>()
        .cloned()
        .ok_or_else(|| tonic::Status::unauthenticated("authentication required"))
}
pub(crate) fn grpc_authorize_collection(
    state: &crate::AppState,
    principal: &Principal,
    action: AuthorizationAction,
    collection_id: &str,
) -> Result<(), tonic::Status> {
    let resource = state
        .authorization()
        .authorize_collection(principal, action, collection_id)
        .map_err(|_| tonic::Status::not_found("collection was not found"))?;
    let AuthorizationResource::Collection { project_id, .. } = resource else {
        return Err(tonic::Status::permission_denied("authorization denied"));
    };
    state
        .governance()
        .admit(
            &project_id,
            crate::AdmissionClass::from_authorization_action(action),
        )
        .map(|_| ())
        .map_err(grpc_governance_status)
}
pub(crate) fn grpc_authorize_create(
    state: &crate::AppState,
    principal: &Principal,
) -> Result<(), tonic::Status> {
    let project_id = state
        .authorization()
        .authorize_principal_project(principal, AuthorizationAction::CollectionCreate)
        .map_err(|_| tonic::Status::permission_denied("authorization denied"))?;
    state
        .governance()
        .admit(&project_id, crate::AdmissionClass::Write)
        .map_err(grpc_governance_status)?;
    let current = state
        .authorization()
        .project_collection_count(&project_id)
        .map_err(|_| tonic::Status::internal("governance collection count failed"))?;
    state
        .governance()
        .check_collection_quota(&project_id, current, 1)
        .map_err(grpc_governance_status)
}
pub(crate) fn grpc_authorize_discover(
    state: &crate::AppState,
    principal: &Principal,
) -> Result<(), tonic::Status> {
    let project_id = state
        .authorization()
        .authorize_principal_project(principal, AuthorizationAction::CollectionDiscover)
        .map_err(|_| tonic::Status::permission_denied("authorization denied"))?;
    state
        .governance()
        .admit(&project_id, crate::AdmissionClass::Read)
        .map(|_| ())
        .map_err(grpc_governance_status)
}
fn grpc_governance_status(error: crate::GovernanceError) -> tonic::Status {
    match error {
        crate::GovernanceError::RateLimited { retry_after } => {
            tonic::Status::resource_exhausted(format!(
                "project request rate limit exceeded; retry_after_ms={}",
                retry_after.as_millis()
            ))
        }
        crate::GovernanceError::CollectionQuotaExceeded { limit } => {
            tonic::Status::resource_exhausted(format!(
                "project collection quota exceeded; limit={limit}"
            ))
        }
        crate::GovernanceError::RecordQuotaExceeded { limit } => tonic::Status::resource_exhausted(
            format!("project record quota exceeded; limit={limit}"),
        ),
        _ => tonic::Status::internal("governance admission failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AuthenticationError, AuthenticationService, Credential, CredentialAuthenticator, Principal,
    };
    use std::sync::Arc;

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ketebe-rbac-{name}-{}", std::process::id()))
    }
    fn principal(subject: &str, project: &str) -> Principal {
        Principal::for_project(subject, project).unwrap()
    }

    #[test]
    fn cross_project_resources_are_undiscoverable_and_roles_are_enforced() {
        let dir = temp_dir("cross");
        let service = AuthorizationService::required(&dir).unwrap();
        let a = principal("a", "project-a");
        let b = principal("b", "project-b");
        service.claim_collection(&a, "docs").unwrap();
        assert!(
            service
                .authorize_collection(&a, AuthorizationAction::CollectionRead, "docs")
                .is_ok()
        );
        assert!(matches!(
            service.authorize_collection(&b, AuthorizationAction::CollectionRead, "docs"),
            Err(AuthorizationError::Undiscoverable)
        ));
        service
            .set_project_role("project-a", "a", ProjectRole::Reader)
            .unwrap();
        assert!(
            service
                .authorize_collection(&a, AuthorizationAction::CollectionRead, "docs")
                .is_ok()
        );
        assert!(
            service
                .authorize_collection(&a, AuthorizationAction::CollectionWrite, "docs")
                .is_err()
        );
        service
            .set_collection_permission("project-a", "docs", "a", CollectionPermission::Write)
            .unwrap();
        assert!(
            service
                .authorize_collection(&a, AuthorizationAction::CollectionWrite, "docs")
                .is_ok()
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn same_collection_name_permissions_are_project_scoped() {
        let dir = temp_dir("permission-scope");
        let service = AuthorizationService::required(&dir).unwrap();
        let a = principal("a", "project-a");
        let b = principal("b", "project-b");
        service.claim_collection(&a, "docs").unwrap();
        service.claim_collection(&b, "docs").unwrap();
        service
            .set_project_role("project-a", "a", ProjectRole::Reader)
            .unwrap();
        service
            .set_project_role("project-b", "b", ProjectRole::Reader)
            .unwrap();
        service
            .set_collection_permission("project-a", "docs", "a", CollectionPermission::Write)
            .unwrap();
        assert!(
            service
                .authorize_collection(&a, AuthorizationAction::CollectionWrite, "docs")
                .is_ok()
        );
        assert!(
            service
                .authorize_collection(&b, AuthorizationAction::CollectionWrite, "docs")
                .is_err()
        );
        let _ = fs::remove_dir_all(dir);
    }

    struct TestAuthenticator;
    impl CredentialAuthenticator for TestAuthenticator {
        fn authenticate(&self, credential: &Credential) -> Result<Principal, AuthenticationError> {
            match credential.expose_secret() {
                "a" => Principal::for_project("subject-a", "project-a"),
                "b" => Principal::for_project("subject-b", "project-b"),
                _ => Err(AuthenticationError::InvalidCredential),
            }
        }
    }

    #[tokio::test]
    async fn governance_http_rate_limit_rejects_before_collection_write() {
        use crate::{
            AppState, GovernancePolicy, GovernanceService, ProjectQuota, RateLimit, RuntimeCatalog,
            app_with_authentication,
        };
        use axum::body::Body;
        use axum::http::Request;
        use std::time::Duration;
        use tower::ServiceExt;

        let dir = temp_dir("governance-http");
        let authorization = AuthorizationService::required(&dir).unwrap();
        let governance = GovernanceService::new();
        governance
            .set_project_policy(
                "project-a",
                GovernancePolicy {
                    write: Some(RateLimit::new(1, Duration::from_secs(60)).unwrap()),
                    quota: ProjectQuota {
                        max_collections: Some(10),
                        max_records: None,
                    },
                    ..GovernancePolicy::default()
                },
            )
            .unwrap();
        let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone())
            .with_authorization(authorization)
            .with_governance(governance);
        let auth = AuthenticationService::required(Arc::new(TestAuthenticator));
        let app = app_with_authentication(state.clone(), auth);
        let body = r#"{"id":"first","dimension":3,"metric":"cosine"}"#;
        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v0/collections")
                    .header("authorization", "Bearer a")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::CREATED);
        let before = state.catalog.read().await.collections.len();
        let second = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v0/collections")
                    .header("authorization", "Bearer a")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"id":"second","dimension":3,"metric":"cosine"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(second.headers().contains_key("retry-after"));
        assert_eq!(state.catalog.read().await.collections.len(), before);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn governance_grpc_create_uses_resource_exhausted_and_shared_project_quota() {
        use crate::{
            AppState, GovernancePolicy, GovernanceService, ProjectQuota, RateLimit, RuntimeCatalog,
        };
        use std::time::Duration;

        let dir = temp_dir("governance-grpc");
        let authorization = AuthorizationService::required(&dir).unwrap();
        let principal = principal("a", "project-a");
        let governance = GovernanceService::new();
        governance
            .set_project_policy(
                "project-a",
                GovernancePolicy {
                    write: Some(RateLimit::new(1, Duration::from_secs(60)).unwrap()),
                    quota: ProjectQuota {
                        max_collections: Some(10),
                        max_records: None,
                    },
                    ..GovernancePolicy::default()
                },
            )
            .unwrap();
        let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone())
            .with_authorization(authorization)
            .with_governance(governance);
        assert!(grpc_authorize_create(&state, &principal).is_ok());
        let denied = grpc_authorize_create(&state, &principal).unwrap_err();
        assert_eq!(denied.code(), tonic::Code::ResourceExhausted);
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn governance_http_collection_quota_is_project_scoped() {
        use crate::{
            AppState, GovernancePolicy, GovernanceService, ProjectQuota, RuntimeCatalog,
            app_with_authentication,
        };
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let dir = temp_dir("governance-quota");
        let authorization = AuthorizationService::required(&dir).unwrap();
        let governance = GovernanceService::new();
        for project in ["project-a", "project-b"] {
            governance
                .set_project_policy(
                    project,
                    GovernancePolicy {
                        quota: ProjectQuota {
                            max_collections: Some(1),
                            max_records: None,
                        },
                        ..GovernancePolicy::default()
                    },
                )
                .unwrap();
        }
        let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone())
            .with_authorization(authorization)
            .with_governance(governance);
        let auth = AuthenticationService::required(Arc::new(TestAuthenticator));
        let app = app_with_authentication(state, auth);
        for (token, id, expected) in [
            ("a", "a-one", StatusCode::CREATED),
            ("a", "a-two", StatusCode::TOO_MANY_REQUESTS),
            ("b", "b-one", StatusCode::CREATED),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v0/collections")
                        .header("authorization", format!("Bearer {token}"))
                        .header("content-type", "application/json")
                        .body(Body::from(format!(
                            r#"{{"id":"{id}","dimension":3,"metric":"cosine"}}"#
                        )))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
        }
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn http_cross_project_collection_is_not_discoverable() {
        use crate::{AppState, RuntimeCatalog, app_with_authentication};
        use axum::body::{Body, to_bytes};
        use axum::http::Request;
        use tower::ServiceExt;

        let dir = temp_dir("http");
        let authorization = AuthorizationService::required(&dir).unwrap();
        let state = AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir.clone())
            .with_authorization(authorization.clone());
        let auth = AuthenticationService::required(Arc::new(TestAuthenticator));
        let app = app_with_authentication(state, auth);
        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v0/collections")
                    .header("authorization", "Bearer a")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"id":"docs","dimension":1,"metric":"l2"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);
        let denied = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v0/collections/docs/query")
                    .header("authorization", "Bearer b")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[1.0],"metric":"l2","top_k":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::NOT_FOUND);
        let list = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v0/collections")
                    .header("authorization", "Bearer b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::OK);
        let body = to_bytes(list.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["collections"].as_array().unwrap().len(), 0);
        let _ = fs::remove_dir_all(dir);
    }
}
