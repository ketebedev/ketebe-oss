use axum::extract::{FromRequestParts, Path, State};
use axum::http::request::Parts;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use ketebe_core::{Organization, OrganizationId, Project, ProjectId, ResourceLifecycleState};
use serde::{Deserialize, Serialize};

use crate::{
    AppState, AuditContext, AuditOrigin, ControlPlaneApiError, ControlPlaneApiService,
    CreateOrganizationInput, CreateProjectInput, OrganizationMembership,
    OrganizationMembershipError, OrganizationRole, Principal, ProjectMembership,
    ProjectMembershipError, ProjectRole, UpdateOrganizationInput, UpdateProjectInput,
};

pub(crate) fn routes(state: AppState) -> Router {
    Router::new()
        .route(
            "/v0/organizations",
            post(create_organization).get(list_organizations),
        )
        .route(
            "/v0/organizations/{organization_id}",
            get(get_organization)
                .patch(update_organization)
                .delete(delete_organization),
        )
        .route(
            "/v0/organizations/{organization_id}/projects",
            post(create_project).get(list_projects),
        )
        .route(
            "/v0/projects/{project_id}",
            get(get_project)
                .patch(update_project)
                .delete(delete_project),
        )
        .route(
            "/v0/organizations/{organization_id}/memberships",
            get(list_organization_memberships),
        )
        .route(
            "/v0/organizations/{organization_id}/memberships/{subject}",
            put(upsert_organization_membership).delete(remove_organization_membership),
        )
        .route(
            "/v0/projects/{project_id}/memberships",
            get(list_project_memberships),
        )
        .route(
            "/v0/projects/{project_id}/memberships/{subject}",
            put(upsert_project_membership).delete(remove_project_membership),
        )
        .with_state(state)
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LifecycleDto {
    Active,
    Suspended,
    Deleting,
}

impl From<LifecycleDto> for ResourceLifecycleState {
    fn from(value: LifecycleDto) -> Self {
        match value {
            LifecycleDto::Active => Self::Active,
            LifecycleDto::Suspended => Self::Suspended,
            LifecycleDto::Deleting => Self::Deleting,
        }
    }
}

impl From<ResourceLifecycleState> for LifecycleDto {
    fn from(value: ResourceLifecycleState) -> Self {
        match value {
            ResourceLifecycleState::Active => Self::Active,
            ResourceLifecycleState::Suspended => Self::Suspended,
            ResourceLifecycleState::Deleting => Self::Deleting,
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateOrganizationBody {
    id: String,
    name: String,
    slug: String,
}

#[derive(Debug, Deserialize)]
struct UpdateOrganizationBody {
    name: String,
    slug: String,
    lifecycle: LifecycleDto,
}

#[derive(Debug, Deserialize)]
struct CreateProjectBody {
    id: String,
    name: String,
    slug: String,
}

#[derive(Debug, Deserialize)]
struct UpdateProjectBody {
    name: String,
    slug: String,
    lifecycle: LifecycleDto,
}

#[derive(Debug, Deserialize)]
struct OrganizationMembershipBody {
    role: OrganizationRole,
}

#[derive(Debug, Deserialize)]
struct ProjectMembershipBody {
    role: ProjectRole,
}

#[derive(Debug, Serialize)]
struct OrganizationDto {
    id: String,
    name: String,
    slug: String,
    lifecycle: LifecycleDto,
    created_at_unix: u64,
    updated_at_unix: u64,
}

impl From<Organization> for OrganizationDto {
    fn from(value: Organization) -> Self {
        Self {
            id: value.id().as_str().to_string(),
            name: value.name().to_string(),
            slug: value.slug().to_string(),
            lifecycle: value.lifecycle().into(),
            created_at_unix: value.timestamps().created_at_unix(),
            updated_at_unix: value.timestamps().updated_at_unix(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ProjectDto {
    id: String,
    organization_id: String,
    name: String,
    slug: String,
    lifecycle: LifecycleDto,
    created_at_unix: u64,
    updated_at_unix: u64,
}

impl From<Project> for ProjectDto {
    fn from(value: Project) -> Self {
        Self {
            id: value.id().as_str().to_string(),
            organization_id: value.organization_id().as_str().to_string(),
            name: value.name().to_string(),
            slug: value.slug().to_string(),
            lifecycle: value.lifecycle().into(),
            created_at_unix: value.timestamps().created_at_unix(),
            updated_at_unix: value.timestamps().updated_at_unix(),
        }
    }
}

#[derive(Debug, Serialize)]
struct OrganizationMembershipDto {
    organization_id: String,
    subject: String,
    role: OrganizationRole,
}

impl From<OrganizationMembership> for OrganizationMembershipDto {
    fn from(value: OrganizationMembership) -> Self {
        Self {
            organization_id: value.organization_id().to_string(),
            subject: value.subject().to_string(),
            role: value.role(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ProjectMembershipDto {
    project_id: String,
    subject: String,
    role: ProjectRole,
}

impl From<ProjectMembership> for ProjectMembershipDto {
    fn from(value: ProjectMembership) -> Self {
        Self {
            project_id: value.project_id().to_string(),
            subject: value.subject().to_string(),
            role: value.role(),
        }
    }
}

struct ControlPlaneRequestContext {
    principal: Principal,
    audit_context: AuditContext,
}

impl<S> FromRequestParts<S> for ControlPlaneRequestContext
where
    S: Send + Sync,
{
    type Rejection = ControlPlaneHttpError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let principal = parts
            .extensions
            .get::<Principal>()
            .cloned()
            .ok_or(ControlPlaneHttpError(ControlPlaneApiError::Forbidden))?;
        let mut audit_context = AuditContext::new(AuditOrigin::Http);
        if let Some(correlation_id) = parts
            .headers
            .get("x-request-id")
            .and_then(|value| value.to_str().ok())
        {
            audit_context = audit_context.with_correlation_id(correlation_id);
        }
        Ok(Self {
            principal,
            audit_context,
        })
    }
}

fn service(state: &AppState, audit_context: &AuditContext) -> ControlPlaneApiService {
    ControlPlaneApiService::new(state.data_dir.as_path(), state.audit())
        .with_audit_context(audit_context.clone())
}

async fn create_organization(
    State(state): State<AppState>,
    context: ControlPlaneRequestContext,
    Json(body): Json<CreateOrganizationBody>,
) -> Result<(StatusCode, Json<OrganizationDto>), ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let organization = service(&state, &audit_context).create_organization(
        &principal,
        CreateOrganizationInput {
            id: body.id,
            name: body.name,
            slug: body.slug,
        },
    )?;
    Ok((StatusCode::CREATED, Json(organization.into())))
}

async fn list_organizations(
    State(state): State<AppState>,
    context: ControlPlaneRequestContext,
) -> Result<Json<Vec<OrganizationDto>>, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    Ok(Json(
        service(&state, &audit_context)
            .list_organizations(&principal)?
            .into_iter()
            .map(Into::into)
            .collect(),
    ))
}

async fn get_organization(
    State(state): State<AppState>,
    Path(organization_id): Path<String>,
    context: ControlPlaneRequestContext,
) -> Result<Json<OrganizationDto>, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let organization_id = OrganizationId::new(organization_id).map_err(invalid_identifier)?;
    Ok(Json(
        service(&state, &audit_context)
            .get_organization(&principal, &organization_id)?
            .into(),
    ))
}

async fn update_organization(
    State(state): State<AppState>,
    Path(organization_id): Path<String>,
    context: ControlPlaneRequestContext,
    Json(body): Json<UpdateOrganizationBody>,
) -> Result<Json<OrganizationDto>, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let organization_id = OrganizationId::new(organization_id).map_err(invalid_identifier)?;
    Ok(Json(
        service(&state, &audit_context)
            .update_organization(
                &principal,
                &organization_id,
                UpdateOrganizationInput {
                    name: body.name,
                    slug: body.slug,
                    lifecycle: body.lifecycle.into(),
                },
            )?
            .into(),
    ))
}

async fn delete_organization(
    State(state): State<AppState>,
    Path(organization_id): Path<String>,
    context: ControlPlaneRequestContext,
) -> Result<StatusCode, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let organization_id = OrganizationId::new(organization_id).map_err(invalid_identifier)?;
    service(&state, &audit_context).delete_organization(&principal, &organization_id)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_project(
    State(state): State<AppState>,
    Path(organization_id): Path<String>,
    context: ControlPlaneRequestContext,
    Json(body): Json<CreateProjectBody>,
) -> Result<(StatusCode, Json<ProjectDto>), ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let organization_id = OrganizationId::new(organization_id).map_err(invalid_identifier)?;
    let project = service(&state, &audit_context).create_project(
        &principal,
        &organization_id,
        CreateProjectInput {
            id: body.id,
            name: body.name,
            slug: body.slug,
        },
    )?;
    Ok((StatusCode::CREATED, Json(project.into())))
}

async fn list_projects(
    State(state): State<AppState>,
    Path(organization_id): Path<String>,
    context: ControlPlaneRequestContext,
) -> Result<Json<Vec<ProjectDto>>, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let organization_id = OrganizationId::new(organization_id).map_err(invalid_identifier)?;
    Ok(Json(
        service(&state, &audit_context)
            .list_projects(&principal, &organization_id)?
            .into_iter()
            .map(Into::into)
            .collect(),
    ))
}

async fn get_project(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    context: ControlPlaneRequestContext,
) -> Result<Json<ProjectDto>, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let project_id = ProjectId::new(project_id).map_err(invalid_identifier)?;
    Ok(Json(
        service(&state, &audit_context)
            .get_project(&principal, &project_id)?
            .into(),
    ))
}

async fn update_project(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    context: ControlPlaneRequestContext,
    Json(body): Json<UpdateProjectBody>,
) -> Result<Json<ProjectDto>, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let project_id = ProjectId::new(project_id).map_err(invalid_identifier)?;
    Ok(Json(
        service(&state, &audit_context)
            .update_project(
                &principal,
                &project_id,
                UpdateProjectInput {
                    name: body.name,
                    slug: body.slug,
                    lifecycle: body.lifecycle.into(),
                },
            )?
            .into(),
    ))
}

async fn delete_project(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    context: ControlPlaneRequestContext,
) -> Result<StatusCode, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let project_id = ProjectId::new(project_id).map_err(invalid_identifier)?;
    service(&state, &audit_context).delete_project(&principal, &project_id)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_organization_memberships(
    State(state): State<AppState>,
    Path(organization_id): Path<String>,
    context: ControlPlaneRequestContext,
) -> Result<Json<Vec<OrganizationMembershipDto>>, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let organization_id = OrganizationId::new(organization_id).map_err(invalid_identifier)?;
    Ok(Json(
        service(&state, &audit_context)
            .list_organization_memberships(&principal, &organization_id)?
            .into_iter()
            .map(Into::into)
            .collect(),
    ))
}

async fn upsert_organization_membership(
    State(state): State<AppState>,
    Path((organization_id, subject)): Path<(String, String)>,
    context: ControlPlaneRequestContext,
    Json(body): Json<OrganizationMembershipBody>,
) -> Result<Json<OrganizationMembershipDto>, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let organization_id = OrganizationId::new(organization_id).map_err(invalid_identifier)?;
    Ok(Json(
        service(&state, &audit_context)
            .upsert_organization_membership(&principal, &organization_id, &subject, body.role)?
            .into(),
    ))
}

async fn remove_organization_membership(
    State(state): State<AppState>,
    Path((organization_id, subject)): Path<(String, String)>,
    context: ControlPlaneRequestContext,
) -> Result<StatusCode, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let organization_id = OrganizationId::new(organization_id).map_err(invalid_identifier)?;
    service(&state, &audit_context).remove_organization_membership(
        &principal,
        &organization_id,
        &subject,
    )?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_project_memberships(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    context: ControlPlaneRequestContext,
) -> Result<Json<Vec<ProjectMembershipDto>>, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let project_id = ProjectId::new(project_id).map_err(invalid_identifier)?;
    Ok(Json(
        service(&state, &audit_context)
            .list_project_memberships(&principal, &project_id)?
            .into_iter()
            .map(Into::into)
            .collect(),
    ))
}

async fn upsert_project_membership(
    State(state): State<AppState>,
    Path((project_id, subject)): Path<(String, String)>,
    context: ControlPlaneRequestContext,
    Json(body): Json<ProjectMembershipBody>,
) -> Result<Json<ProjectMembershipDto>, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let project_id = ProjectId::new(project_id).map_err(invalid_identifier)?;
    Ok(Json(
        service(&state, &audit_context)
            .upsert_project_membership(&principal, &project_id, &subject, body.role)?
            .into(),
    ))
}

async fn remove_project_membership(
    State(state): State<AppState>,
    Path((project_id, subject)): Path<(String, String)>,
    context: ControlPlaneRequestContext,
) -> Result<StatusCode, ControlPlaneHttpError> {
    let ControlPlaneRequestContext {
        principal,
        audit_context,
    } = context;
    let project_id = ProjectId::new(project_id).map_err(invalid_identifier)?;
    service(&state, &audit_context).remove_project_membership(&principal, &project_id, &subject)?;
    Ok(StatusCode::NO_CONTENT)
}

fn invalid_identifier(error: impl std::fmt::Display) -> ControlPlaneHttpError {
    ControlPlaneHttpError(ControlPlaneApiError::InvalidInput(error.to_string()))
}

struct ControlPlaneHttpError(ControlPlaneApiError);

impl From<ControlPlaneApiError> for ControlPlaneHttpError {
    fn from(value: ControlPlaneApiError) -> Self {
        Self(value)
    }
}

impl IntoResponse for ControlPlaneHttpError {
    fn into_response(self) -> Response {
        let (status, code) = match &self.0 {
            ControlPlaneApiError::InvalidInput(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            ControlPlaneApiError::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            ControlPlaneApiError::Undiscoverable => (StatusCode::NOT_FOUND, "not_found"),
            ControlPlaneApiError::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            ControlPlaneApiError::ResourceNotDeleting => {
                (StatusCode::CONFLICT, "resource_not_deleting")
            }
            ControlPlaneApiError::InvalidLifecycleTransition => {
                (StatusCode::CONFLICT, "invalid_lifecycle_transition")
            }
            ControlPlaneApiError::ProjectHasCollections => {
                (StatusCode::CONFLICT, "project_has_collections")
            }
            ControlPlaneApiError::OrganizationMembership(
                OrganizationMembershipError::Undiscoverable,
            )
            | ControlPlaneApiError::ProjectMembership(ProjectMembershipError::Undiscoverable) => {
                (StatusCode::NOT_FOUND, "not_found")
            }
            ControlPlaneApiError::OrganizationMembership(
                OrganizationMembershipError::LastOwner,
            )
            | ControlPlaneApiError::ProjectMembership(ProjectMembershipError::LastOwner) => {
                (StatusCode::CONFLICT, "last_owner")
            }
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "control_plane_error"),
        };
        (
            status,
            Json(serde_json::json!({
                "error": {
                    "code": code,
                    "message": self.0.to_string()
                }
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthorizationService, RuntimeCatalog};
    use axum::body::Body;
    use axum::http::{Method, Request};
    use tower::ServiceExt;

    fn state() -> AppState {
        let dir =
            std::env::temp_dir().join(format!("ketebe-control-plane-http-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        AppState::with_data_dir(RuntimeCatalog::empty_ready(), dir).with_authorization(
            AuthorizationService::required(
                std::env::temp_dir()
                    .join(format!("ketebe-control-plane-http-{}", std::process::id())),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn lifecycle_routes_are_versioned_and_use_stable_error_envelopes() {
        let state = state();
        let principal = Principal::new("owner-a", crate::PrincipalKind::Credential).unwrap();
        let app = routes(state.clone()).layer(axum::middleware::from_fn(
            move |mut request: axum::extract::Request, next: axum::middleware::Next| {
                let principal = principal.clone();
                async move {
                    request.extensions_mut().insert(principal);
                    next.run(request).await
                }
            },
        ));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v0/organizations")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"id":"org-a","name":"ACME","slug":"acme"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/v0/organizations/org-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }
}
