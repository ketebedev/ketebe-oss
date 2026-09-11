use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ketebe_core::{
    Organization, OrganizationId, Project, ProjectId, ResourceLifecycleState, ResourceTimestamps,
};
use ketebe_storage::{ControlPlaneStore, ControlPlaneStoreError};

use crate::{
    AuditCategory, AuditContext, AuditEvent, AuditOrigin, AuditResult, AuditService,
    AuthorizationAction, AuthorizationService, CollectionNamespaceCatalog, OrganizationAction,
    OrganizationMembership, OrganizationMembershipError, OrganizationMembershipService,
    OrganizationRole, Principal, PrincipalKind, ProjectMembership, ProjectMembershipError,
    ProjectMembershipService, ProjectRole,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateOrganizationInput {
    pub id: String,
    pub name: String,
    pub slug: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateOrganizationInput {
    pub name: String,
    pub slug: String,
    pub lifecycle: ResourceLifecycleState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateProjectInput {
    pub id: String,
    pub name: String,
    pub slug: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateProjectInput {
    pub name: String,
    pub slug: String,
    pub lifecycle: ResourceLifecycleState,
}

#[derive(Clone, Copy, Debug, Default)]
struct AuditResourceRef<'a> {
    kind: &'a str,
    id: Option<&'a str>,
    project_id: Option<&'a str>,
    organization_id: Option<&'a str>,
}

#[derive(Clone)]
pub struct ControlPlaneApiService {
    data_dir: Arc<PathBuf>,
    audit: Arc<AuditService>,
    audit_context: AuditContext,
}

impl fmt::Debug for ControlPlaneApiService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneApiService")
            .field("data_dir", &self.data_dir)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub enum ControlPlaneApiError {
    InvalidInput(String),
    Undiscoverable,
    Forbidden,
    Conflict(String),
    ResourceNotDeleting,
    InvalidLifecycleTransition,
    ProjectHasCollections,
    Store(ControlPlaneStoreError),
    OrganizationMembership(OrganizationMembershipError),
    ProjectMembership(ProjectMembershipError),
    CollectionCatalog(String),
    Authorization(String),
}

impl fmt::Display for ControlPlaneApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => {
                write!(formatter, "invalid control-plane input: {message}")
            }
            Self::Undiscoverable => formatter.write_str("resource is not discoverable"),
            Self::Forbidden => formatter.write_str("control-plane authorization denied"),
            Self::Conflict(message) => write!(formatter, "control-plane conflict: {message}"),
            Self::ResourceNotDeleting => {
                formatter.write_str("resource must be in deleting lifecycle before deletion")
            }
            Self::InvalidLifecycleTransition => {
                formatter.write_str("invalid control-plane lifecycle transition")
            }
            Self::ProjectHasCollections => formatter.write_str("project still owns collections"),
            Self::Store(error) => write!(formatter, "control-plane store error: {error}"),
            Self::OrganizationMembership(error) => {
                write!(formatter, "organization membership error: {error}")
            }
            Self::ProjectMembership(error) => {
                write!(formatter, "project membership error: {error}")
            }
            Self::CollectionCatalog(message) => {
                write!(formatter, "collection catalog error: {message}")
            }
            Self::Authorization(message) => write!(formatter, "authorization error: {message}"),
        }
    }
}

impl std::error::Error for ControlPlaneApiError {}

impl From<ControlPlaneStoreError> for ControlPlaneApiError {
    fn from(value: ControlPlaneStoreError) -> Self {
        Self::Store(value)
    }
}

impl From<OrganizationMembershipError> for ControlPlaneApiError {
    fn from(value: OrganizationMembershipError) -> Self {
        Self::OrganizationMembership(value)
    }
}

impl From<ProjectMembershipError> for ControlPlaneApiError {
    fn from(value: ProjectMembershipError) -> Self {
        Self::ProjectMembership(value)
    }
}

impl ControlPlaneApiService {
    #[must_use]
    pub fn new(data_dir: impl AsRef<Path>, audit: Arc<AuditService>) -> Self {
        Self {
            data_dir: Arc::new(data_dir.as_ref().to_path_buf()),
            audit,
            audit_context: AuditContext::default(),
        }
    }

    #[must_use]
    pub fn with_audit_context(mut self, audit_context: AuditContext) -> Self {
        self.audit_context = audit_context;
        self
    }

    pub fn create_organization(
        &self,
        principal: &Principal,
        input: CreateOrganizationInput,
    ) -> Result<Organization, ControlPlaneApiError> {
        if principal.kind() == PrincipalKind::WorkloadCredential {
            self.audit_decision(
                principal,
                "organization_create",
                AuditResult::Denied,
                AuditResourceRef {
                    kind: "organization",
                    ..AuditResourceRef::default()
                },
            );
            return Err(ControlPlaneApiError::Forbidden);
        }
        let now = unix_now();
        let organization_id = OrganizationId::new(input.id)
            .map_err(|error| ControlPlaneApiError::InvalidInput(error.to_string()))?;
        let organization = Organization::new(
            organization_id.clone(),
            input.name,
            input.slug,
            ResourceLifecycleState::Active,
            ResourceTimestamps::new(now, now)
                .map_err(|error| ControlPlaneApiError::InvalidInput(error.to_string()))?,
        )
        .map_err(|error| ControlPlaneApiError::InvalidInput(error.to_string()))?;
        let store = self.store()?;
        store
            .create_organization(organization.clone())
            .map_err(map_store_error)?;

        let memberships = OrganizationMembershipService::open(
            self.data_dir.as_path(),
            self.store()?,
            Arc::clone(&self.audit),
        )?
        .with_audit_context(self.audit_context.clone());
        if let Err(error) = memberships.bootstrap_owner(&organization_id, principal.subject()) {
            let _ = store.delete_organization(&organization_id);
            return Err(error.into());
        }
        self.audit_resource(
            principal,
            "organization_create",
            "organization",
            organization.id().as_str(),
            None,
            Some(organization.id().as_str()),
        );
        Ok(organization)
    }

    pub fn list_organizations(
        &self,
        principal: &Principal,
    ) -> Result<Vec<Organization>, ControlPlaneApiError> {
        let store = self.store()?;
        let memberships = OrganizationMembershipService::open(
            self.data_dir.as_path(),
            store.clone(),
            Arc::clone(&self.audit),
        )?;
        let mut visible = Vec::new();
        for organization in store.list_organizations()? {
            if memberships
                .authorize(
                    principal,
                    organization.id(),
                    OrganizationAction::OrganizationRead,
                )
                .is_ok()
            {
                visible.push(organization);
            }
        }
        Ok(visible)
    }

    pub fn get_organization(
        &self,
        principal: &Principal,
        organization_id: &OrganizationId,
    ) -> Result<Organization, ControlPlaneApiError> {
        self.authorize_organization(
            principal,
            organization_id,
            OrganizationAction::OrganizationRead,
        )?;
        self.store()?
            .organization(organization_id)?
            .ok_or(ControlPlaneApiError::Undiscoverable)
    }

    pub fn update_organization(
        &self,
        principal: &Principal,
        organization_id: &OrganizationId,
        input: UpdateOrganizationInput,
    ) -> Result<Organization, ControlPlaneApiError> {
        self.authorize_organization(
            principal,
            organization_id,
            OrganizationAction::OrganizationUpdate,
        )?;
        let store = self.store()?;
        let current = store
            .organization(organization_id)?
            .ok_or(ControlPlaneApiError::Undiscoverable)?;
        validate_lifecycle_transition(current.lifecycle(), input.lifecycle)?;
        let now = unix_now();
        let updated = Organization::new(
            current.id().clone(),
            input.name,
            input.slug,
            input.lifecycle,
            ResourceTimestamps::new(current.timestamps().created_at_unix(), now)
                .map_err(|error| ControlPlaneApiError::InvalidInput(error.to_string()))?,
        )
        .map_err(|error| ControlPlaneApiError::InvalidInput(error.to_string()))?;
        store
            .update_organization(updated.clone())
            .map_err(map_store_error)?;
        self.audit_resource(
            principal,
            "organization_update",
            "organization",
            updated.id().as_str(),
            None,
            Some(updated.id().as_str()),
        );
        Ok(updated)
    }

    pub fn delete_organization(
        &self,
        principal: &Principal,
        organization_id: &OrganizationId,
    ) -> Result<(), ControlPlaneApiError> {
        self.authorize_organization(
            principal,
            organization_id,
            OrganizationAction::OrganizationUpdate,
        )?;
        let store = self.store()?;
        let organization = store
            .organization(organization_id)?
            .ok_or(ControlPlaneApiError::Undiscoverable)?;
        if organization.lifecycle() != ResourceLifecycleState::Deleting {
            return Err(ControlPlaneApiError::ResourceNotDeleting);
        }
        store
            .delete_organization(organization_id)
            .map_err(map_store_error)?;
        let memberships = OrganizationMembershipService::open(
            self.data_dir.as_path(),
            self.store()?,
            Arc::clone(&self.audit),
        )?
        .with_audit_context(self.audit_context.clone());
        memberships.purge_organization(organization_id)?;
        self.audit_resource(
            principal,
            "organization_delete",
            "organization",
            organization_id.as_str(),
            None,
            Some(organization_id.as_str()),
        );
        Ok(())
    }

    pub fn create_project(
        &self,
        principal: &Principal,
        organization_id: &OrganizationId,
        input: CreateProjectInput,
    ) -> Result<Project, ControlPlaneApiError> {
        self.authorize_organization(
            principal,
            organization_id,
            OrganizationAction::OrganizationUpdate,
        )?;
        let store = self.store()?;
        let organization = store
            .organization(organization_id)?
            .ok_or(ControlPlaneApiError::Undiscoverable)?;
        if organization.lifecycle() != ResourceLifecycleState::Active {
            return Err(ControlPlaneApiError::Conflict(
                "organization is not active".to_string(),
            ));
        }
        let project_id = ProjectId::new(input.id)
            .map_err(|error| ControlPlaneApiError::InvalidInput(error.to_string()))?;
        let now = unix_now();
        let project = Project::new(
            project_id.clone(),
            organization_id.clone(),
            input.name,
            input.slug,
            ResourceLifecycleState::Active,
            ResourceTimestamps::new(now, now)
                .map_err(|error| ControlPlaneApiError::InvalidInput(error.to_string()))?,
        )
        .map_err(|error| ControlPlaneApiError::InvalidInput(error.to_string()))?;
        store
            .create_project(project.clone())
            .map_err(map_store_error)?;

        let memberships = ProjectMembershipService::open(
            self.data_dir.as_path(),
            self.store()?,
            Arc::clone(&self.audit),
        )?
        .with_audit_context(self.audit_context.clone());
        if let Err(error) = memberships.bootstrap_owner(&project_id, principal.subject()) {
            let _ = store.delete_project(&project_id);
            return Err(error.into());
        }
        self.audit_resource(
            principal,
            "project_create",
            "project",
            project.id().as_str(),
            Some(project.id().as_str()),
            Some(organization_id.as_str()),
        );
        Ok(project)
    }

    pub fn list_projects(
        &self,
        principal: &Principal,
        organization_id: &OrganizationId,
    ) -> Result<Vec<Project>, ControlPlaneApiError> {
        self.authorize_organization(
            principal,
            organization_id,
            OrganizationAction::OrganizationRead,
        )?;
        Ok(self.store()?.list_projects(Some(organization_id))?)
    }

    pub fn get_project(
        &self,
        principal: &Principal,
        project_id: &ProjectId,
    ) -> Result<Project, ControlPlaneApiError> {
        let store = self.store()?;
        let project = store
            .project(project_id)?
            .ok_or(ControlPlaneApiError::Undiscoverable)?;
        if !self.organization_authorized(
            principal,
            project.organization_id(),
            OrganizationAction::OrganizationRead,
        )? && self
            .authorization()?
            .authorize_project(
                principal,
                AuthorizationAction::CollectionDiscover,
                project_id.as_str(),
            )
            .is_err()
        {
            self.audit_decision(
                principal,
                "project_discovery",
                AuditResult::Denied,
                AuditResourceRef {
                    kind: "project",
                    id: Some(project_id.as_str()),
                    project_id: Some(project_id.as_str()),
                    organization_id: Some(project.organization_id().as_str()),
                },
            );
            return Err(ControlPlaneApiError::Undiscoverable);
        }
        Ok(project)
    }

    pub fn update_project(
        &self,
        principal: &Principal,
        project_id: &ProjectId,
        input: UpdateProjectInput,
    ) -> Result<Project, ControlPlaneApiError> {
        let store = self.store()?;
        let current = store
            .project(project_id)?
            .ok_or(ControlPlaneApiError::Undiscoverable)?;
        self.authorize_project_management(principal, &current)?;
        validate_lifecycle_transition(current.lifecycle(), input.lifecycle)?;
        let now = unix_now();
        let updated = Project::new(
            current.id().clone(),
            current.organization_id().clone(),
            input.name,
            input.slug,
            input.lifecycle,
            ResourceTimestamps::new(current.timestamps().created_at_unix(), now)
                .map_err(|error| ControlPlaneApiError::InvalidInput(error.to_string()))?,
        )
        .map_err(|error| ControlPlaneApiError::InvalidInput(error.to_string()))?;
        store
            .update_project(updated.clone())
            .map_err(map_store_error)?;
        self.audit_resource(
            principal,
            "project_update",
            "project",
            project_id.as_str(),
            Some(project_id.as_str()),
            Some(updated.organization_id().as_str()),
        );
        Ok(updated)
    }

    pub fn delete_project(
        &self,
        principal: &Principal,
        project_id: &ProjectId,
    ) -> Result<(), ControlPlaneApiError> {
        let store = self.store()?;
        let project = store
            .project(project_id)?
            .ok_or(ControlPlaneApiError::Undiscoverable)?;
        self.authorize_project_management(principal, &project)?;
        if project.lifecycle() != ResourceLifecycleState::Deleting {
            return Err(ControlPlaneApiError::ResourceNotDeleting);
        }
        let catalog = CollectionNamespaceCatalog::open(self.data_dir.as_path())
            .map_err(|error| ControlPlaneApiError::CollectionCatalog(error.to_string()))?;
        if !catalog
            .list_project(project_id)
            .map_err(|error| ControlPlaneApiError::CollectionCatalog(error.to_string()))?
            .is_empty()
        {
            return Err(ControlPlaneApiError::ProjectHasCollections);
        }
        store.delete_project(project_id).map_err(map_store_error)?;
        let memberships = ProjectMembershipService::open(
            self.data_dir.as_path(),
            self.store()?,
            Arc::clone(&self.audit),
        )?
        .with_audit_context(self.audit_context.clone());
        memberships.purge_project(project_id)?;
        self.audit_resource(
            principal,
            "project_delete",
            "project",
            project_id.as_str(),
            Some(project_id.as_str()),
            Some(project.organization_id().as_str()),
        );
        Ok(())
    }

    pub fn list_organization_memberships(
        &self,
        principal: &Principal,
        organization_id: &OrganizationId,
    ) -> Result<Vec<OrganizationMembership>, ControlPlaneApiError> {
        let service = OrganizationMembershipService::open(
            self.data_dir.as_path(),
            self.store()?,
            Arc::clone(&self.audit),
        )?
        .with_audit_context(self.audit_context.clone());
        match service.list_memberships(principal, organization_id) {
            Ok(memberships) => Ok(memberships),
            Err(error) => {
                self.audit_decision(
                    principal,
                    "organization_membership_list",
                    AuditResult::Denied,
                    AuditResourceRef {
                        kind: "organization",
                        id: Some(organization_id.as_str()),
                        organization_id: Some(organization_id.as_str()),
                        ..AuditResourceRef::default()
                    },
                );
                Err(error.into())
            }
        }
    }

    pub fn upsert_organization_membership(
        &self,
        principal: &Principal,
        organization_id: &OrganizationId,
        subject: &str,
        role: OrganizationRole,
    ) -> Result<OrganizationMembership, ControlPlaneApiError> {
        let service = OrganizationMembershipService::open(
            self.data_dir.as_path(),
            self.store()?,
            Arc::clone(&self.audit),
        )?
        .with_audit_context(self.audit_context.clone());
        let result = match service.membership(principal, organization_id, subject) {
            Ok(_) => service.update_role(principal, organization_id, subject, role),
            Err(OrganizationMembershipError::Undiscoverable) => {
                service.create_membership(principal, organization_id, subject, role)
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(membership) => Ok(membership),
            Err(error) => {
                self.audit_decision(
                    principal,
                    "organization_membership_write",
                    AuditResult::Denied,
                    AuditResourceRef {
                        kind: "organization_membership",
                        id: Some(subject),
                        organization_id: Some(organization_id.as_str()),
                        ..AuditResourceRef::default()
                    },
                );
                Err(error.into())
            }
        }
    }

    pub fn remove_organization_membership(
        &self,
        principal: &Principal,
        organization_id: &OrganizationId,
        subject: &str,
    ) -> Result<(), ControlPlaneApiError> {
        let service = OrganizationMembershipService::open(
            self.data_dir.as_path(),
            self.store()?,
            Arc::clone(&self.audit),
        )?
        .with_audit_context(self.audit_context.clone());
        match service.remove_membership(principal, organization_id, subject) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.audit_decision(
                    principal,
                    "organization_membership_delete",
                    AuditResult::Denied,
                    AuditResourceRef {
                        kind: "organization_membership",
                        id: Some(subject),
                        organization_id: Some(organization_id.as_str()),
                        ..AuditResourceRef::default()
                    },
                );
                Err(error.into())
            }
        }
    }

    pub fn list_project_memberships(
        &self,
        principal: &Principal,
        project_id: &ProjectId,
    ) -> Result<Vec<ProjectMembership>, ControlPlaneApiError> {
        let service = ProjectMembershipService::open(
            self.data_dir.as_path(),
            self.store()?,
            Arc::clone(&self.audit),
        )?
        .with_audit_context(self.audit_context.clone());
        match service.list_memberships(principal, project_id) {
            Ok(memberships) => Ok(memberships),
            Err(error) => {
                let organization_id = self
                    .store()?
                    .resolve_project_organization(project_id)
                    .ok()
                    .map(|organization| organization.id().as_str().to_string());
                self.audit_decision(
                    principal,
                    "project_membership_list",
                    AuditResult::Denied,
                    AuditResourceRef {
                        kind: "project",
                        id: Some(project_id.as_str()),
                        project_id: Some(project_id.as_str()),
                        organization_id: organization_id.as_deref(),
                    },
                );
                Err(error.into())
            }
        }
    }

    pub fn upsert_project_membership(
        &self,
        principal: &Principal,
        project_id: &ProjectId,
        subject: &str,
        role: ProjectRole,
    ) -> Result<ProjectMembership, ControlPlaneApiError> {
        let service = ProjectMembershipService::open(
            self.data_dir.as_path(),
            self.store()?,
            Arc::clone(&self.audit),
        )?
        .with_audit_context(self.audit_context.clone());
        let result = match service.membership(principal, project_id, subject) {
            Ok(_) => service.update_role(principal, project_id, subject, role),
            Err(ProjectMembershipError::Undiscoverable) => {
                service.create_membership(principal, project_id, subject, role)
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(membership) => Ok(membership),
            Err(error) => {
                let organization_id = self
                    .store()?
                    .resolve_project_organization(project_id)
                    .ok()
                    .map(|organization| organization.id().as_str().to_string());
                self.audit_decision(
                    principal,
                    "project_membership_write",
                    AuditResult::Denied,
                    AuditResourceRef {
                        kind: "project_membership",
                        id: Some(subject),
                        project_id: Some(project_id.as_str()),
                        organization_id: organization_id.as_deref(),
                    },
                );
                Err(error.into())
            }
        }
    }

    pub fn remove_project_membership(
        &self,
        principal: &Principal,
        project_id: &ProjectId,
        subject: &str,
    ) -> Result<(), ControlPlaneApiError> {
        let service = ProjectMembershipService::open(
            self.data_dir.as_path(),
            self.store()?,
            Arc::clone(&self.audit),
        )?
        .with_audit_context(self.audit_context.clone());
        match service.remove_membership(principal, project_id, subject) {
            Ok(()) => Ok(()),
            Err(error) => {
                let organization_id = self
                    .store()?
                    .resolve_project_organization(project_id)
                    .ok()
                    .map(|organization| organization.id().as_str().to_string());
                self.audit_decision(
                    principal,
                    "project_membership_delete",
                    AuditResult::Denied,
                    AuditResourceRef {
                        kind: "project_membership",
                        id: Some(subject),
                        project_id: Some(project_id.as_str()),
                        organization_id: organization_id.as_deref(),
                    },
                );
                Err(error.into())
            }
        }
    }

    fn store(&self) -> Result<ControlPlaneStore, ControlPlaneApiError> {
        ControlPlaneStore::open(self.data_dir.join("control-plane")).map_err(Into::into)
    }

    fn authorization(&self) -> Result<AuthorizationService, ControlPlaneApiError> {
        AuthorizationService::required(self.data_dir.as_path())
            .map_err(|error| ControlPlaneApiError::Authorization(error.to_string()))
    }

    fn authorize_organization(
        &self,
        principal: &Principal,
        organization_id: &OrganizationId,
        action: OrganizationAction,
    ) -> Result<(), ControlPlaneApiError> {
        if self.organization_authorized(principal, organization_id, action)? {
            return Ok(());
        }
        self.audit_decision(
            principal,
            "organization_discovery",
            AuditResult::Denied,
            AuditResourceRef {
                kind: "organization",
                id: Some(organization_id.as_str()),
                organization_id: Some(organization_id.as_str()),
                ..AuditResourceRef::default()
            },
        );
        Err(ControlPlaneApiError::Undiscoverable)
    }

    fn organization_authorized(
        &self,
        principal: &Principal,
        organization_id: &OrganizationId,
        action: OrganizationAction,
    ) -> Result<bool, ControlPlaneApiError> {
        let service = OrganizationMembershipService::open(
            self.data_dir.as_path(),
            self.store()?,
            Arc::clone(&self.audit),
        )?
        .with_audit_context(self.audit_context.clone());
        Ok(service
            .authorize(principal, organization_id, action)
            .is_ok())
    }

    fn authorize_project_management(
        &self,
        principal: &Principal,
        project: &Project,
    ) -> Result<(), ControlPlaneApiError> {
        if self.organization_authorized(
            principal,
            project.organization_id(),
            OrganizationAction::OrganizationUpdate,
        )? {
            return Ok(());
        }
        if self
            .authorization()?
            .authorize_project(
                principal,
                AuthorizationAction::ProjectAdmin,
                project.id().as_str(),
            )
            .is_ok()
        {
            return Ok(());
        }
        self.audit_decision(
            principal,
            "project_management",
            AuditResult::Denied,
            AuditResourceRef {
                kind: "project",
                id: Some(project.id().as_str()),
                project_id: Some(project.id().as_str()),
                organization_id: Some(project.organization_id().as_str()),
            },
        );
        Err(ControlPlaneApiError::Undiscoverable)
    }

    fn audit_resource(
        &self,
        principal: &Principal,
        action: &str,
        kind: &str,
        id: &str,
        project_id: Option<&str>,
        organization_id: Option<&str>,
    ) {
        self.audit_decision(
            principal,
            action,
            AuditResult::Allowed,
            AuditResourceRef {
                kind,
                id: Some(id),
                project_id,
                organization_id,
            },
        );
    }

    fn audit_decision(
        &self,
        principal: &Principal,
        action: &str,
        result: AuditResult,
        resource: AuditResourceRef<'_>,
    ) {
        let mut event = AuditEvent::new(
            AuditCategory::Authorization,
            action,
            result,
            AuditOrigin::Internal,
        )
        .with_actor(principal.subject());
        if let Some(id) = resource.id {
            event = event.with_resource(resource.kind, id);
        }
        if let Some(project_id) = resource.project_id {
            event = event.with_project(project_id);
        }
        if let Some(organization_id) = resource.organization_id {
            event = event.with_organization(organization_id);
        }
        let event = self.audit_context.apply(event);
        let _ = self.audit.record(&event);
    }
}

fn validate_lifecycle_transition(
    current: ResourceLifecycleState,
    next: ResourceLifecycleState,
) -> Result<(), ControlPlaneApiError> {
    if current == ResourceLifecycleState::Deleting && next != ResourceLifecycleState::Deleting {
        return Err(ControlPlaneApiError::InvalidLifecycleTransition);
    }
    Ok(())
}

fn map_store_error(error: ControlPlaneStoreError) -> ControlPlaneApiError {
    match error {
        ControlPlaneStoreError::OrganizationAlreadyExists(_)
        | ControlPlaneStoreError::OrganizationSlugConflict(_)
        | ControlPlaneStoreError::OrganizationHasProjects(_)
        | ControlPlaneStoreError::ProjectAlreadyExists(_)
        | ControlPlaneStoreError::ProjectSlugConflict { .. }
        | ControlPlaneStoreError::ProjectOwnershipImmutable { .. } => {
            ControlPlaneApiError::Conflict(error.to_string())
        }
        ControlPlaneStoreError::OrganizationNotFound(_)
        | ControlPlaneStoreError::ProjectNotFound(_) => ControlPlaneApiError::Undiscoverable,
        other => ControlPlaneApiError::Store(other),
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PrincipalKind;

    fn temp_dir(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ketebe-control-plane-api-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn human(subject: &str) -> Principal {
        Principal::new(subject, PrincipalKind::Credential).unwrap()
    }

    #[test]
    fn organization_and_project_lifecycle_are_durable_and_cross_org_discovery_fails_closed() {
        let dir = temp_dir("lifecycle");
        let service = ControlPlaneApiService::new(&dir, Arc::new(AuditService::noop()));
        let owner_a = human("owner-a");
        let owner_b = human("owner-b");
        let org_a = service
            .create_organization(
                &owner_a,
                CreateOrganizationInput {
                    id: "org-a".to_string(),
                    name: "ACME".to_string(),
                    slug: "acme".to_string(),
                },
            )
            .unwrap();
        service
            .create_organization(
                &owner_b,
                CreateOrganizationInput {
                    id: "org-b".to_string(),
                    name: "Beta".to_string(),
                    slug: "beta".to_string(),
                },
            )
            .unwrap();
        let project = service
            .create_project(
                &owner_a,
                org_a.id(),
                CreateProjectInput {
                    id: "project-a".to_string(),
                    name: "Search".to_string(),
                    slug: "search".to_string(),
                },
            )
            .unwrap();

        assert_eq!(service.list_organizations(&owner_a).unwrap().len(), 1);
        assert!(matches!(
            service.get_project(&owner_b, project.id()),
            Err(ControlPlaneApiError::Undiscoverable)
        ));

        service
            .update_project(
                &owner_a,
                project.id(),
                UpdateProjectInput {
                    name: "Search".to_string(),
                    slug: "search".to_string(),
                    lifecycle: ResourceLifecycleState::Suspended,
                },
            )
            .unwrap();
        let reopened = ControlPlaneApiService::new(&dir, Arc::new(AuditService::noop()));
        assert_eq!(
            reopened
                .get_project(&owner_a, project.id())
                .unwrap()
                .lifecycle(),
            ResourceLifecycleState::Suspended
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn project_delete_requires_deleting_state_and_rejects_owned_collections() {
        let dir = temp_dir("delete");
        let service = ControlPlaneApiService::new(&dir, Arc::new(AuditService::noop()));
        let owner = human("owner");
        let organization = service
            .create_organization(
                &owner,
                CreateOrganizationInput {
                    id: "org-a".to_string(),
                    name: "ACME".to_string(),
                    slug: "acme".to_string(),
                },
            )
            .unwrap();
        let project = service
            .create_project(
                &owner,
                organization.id(),
                CreateProjectInput {
                    id: "project-a".to_string(),
                    name: "Search".to_string(),
                    slug: "search".to_string(),
                },
            )
            .unwrap();
        assert!(matches!(
            service.delete_project(&owner, project.id()),
            Err(ControlPlaneApiError::ResourceNotDeleting)
        ));

        let catalog = CollectionNamespaceCatalog::open(&dir).unwrap();
        catalog
            .create(
                project.id(),
                &ketebe_core::CollectionName::new("docs").unwrap(),
            )
            .unwrap();
        service
            .update_project(
                &owner,
                project.id(),
                UpdateProjectInput {
                    name: "Search".to_string(),
                    slug: "search".to_string(),
                    lifecycle: ResourceLifecycleState::Deleting,
                },
            )
            .unwrap();
        assert!(matches!(
            service.delete_project(&owner, project.id()),
            Err(ControlPlaneApiError::ProjectHasCollections)
        ));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn deleting_lifecycle_cannot_be_reactivated() {
        let dir = temp_dir("transition");
        let service = ControlPlaneApiService::new(&dir, Arc::new(AuditService::noop()));
        let owner = human("owner");
        let organization = service
            .create_organization(
                &owner,
                CreateOrganizationInput {
                    id: "org-a".to_string(),
                    name: "ACME".to_string(),
                    slug: "acme".to_string(),
                },
            )
            .unwrap();
        service
            .update_organization(
                &owner,
                organization.id(),
                UpdateOrganizationInput {
                    name: "ACME".to_string(),
                    slug: "acme".to_string(),
                    lifecycle: ResourceLifecycleState::Deleting,
                },
            )
            .unwrap();
        assert!(matches!(
            service.update_organization(
                &owner,
                organization.id(),
                UpdateOrganizationInput {
                    name: "ACME".to_string(),
                    slug: "acme".to_string(),
                    lifecycle: ResourceLifecycleState::Active,
                },
            ),
            Err(ControlPlaneApiError::InvalidLifecycleTransition)
        ));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn cross_organization_discovery_is_non_enumerable_and_denials_are_correlated() {
        let dir = temp_dir("discovery-audit");
        let audit = Arc::new(AuditService::durable(&dir).unwrap());
        let service = ControlPlaneApiService::new(&dir, audit).with_audit_context(
            AuditContext::new(AuditOrigin::Http).with_correlation_id("request-cross-org-1"),
        );
        let owner_a = human("owner-a");
        let owner_b = human("owner-b");

        let org_a = service
            .create_organization(
                &owner_a,
                CreateOrganizationInput {
                    id: "org-a".to_string(),
                    name: "ACME".to_string(),
                    slug: "acme".to_string(),
                },
            )
            .unwrap();
        service
            .create_organization(
                &owner_b,
                CreateOrganizationInput {
                    id: "org-b".to_string(),
                    name: "Beta".to_string(),
                    slug: "beta".to_string(),
                },
            )
            .unwrap();
        let project_a = service
            .create_project(
                &owner_a,
                org_a.id(),
                CreateProjectInput {
                    id: "project-a".to_string(),
                    name: "Search".to_string(),
                    slug: "search".to_string(),
                },
            )
            .unwrap();

        assert!(matches!(
            service.get_organization(&owner_b, org_a.id()),
            Err(ControlPlaneApiError::Undiscoverable)
        ));
        assert!(matches!(
            service.get_organization(&owner_b, &OrganizationId::new("org-missing").unwrap()),
            Err(ControlPlaneApiError::Undiscoverable)
        ));
        assert!(matches!(
            service.get_project(&owner_b, project_a.id()),
            Err(ControlPlaneApiError::Undiscoverable)
        ));
        assert!(matches!(
            service.get_project(&owner_b, &ProjectId::new("project-missing").unwrap()),
            Err(ControlPlaneApiError::Undiscoverable)
        ));
        assert!(matches!(
            service.list_project_memberships(&owner_b, project_a.id()),
            Err(ControlPlaneApiError::ProjectMembership(
                ProjectMembershipError::Undiscoverable
            ))
        ));

        let text = std::fs::read_to_string(dir.join("security/audit.jsonl")).unwrap();
        assert!(text.contains("request-cross-org-1"));
        assert!(text.contains("owner-b"));
        assert!(text.contains("organization_discovery"));
        assert!(text.contains("project_discovery"));
        assert!(text.contains("project_membership_list"));
        assert!(text.contains("\"result\":\"denied\""));
        assert!(!text.contains("secret"));
        assert!(!text.contains("payload"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn workload_cannot_self_create_organization() {
        let dir = temp_dir("workload-create");
        let service = ControlPlaneApiService::new(&dir, Arc::new(AuditService::noop()));
        let workload = Principal::for_workload_project("service", "project-a").unwrap();
        assert!(matches!(
            service.create_organization(
                &workload,
                CreateOrganizationInput {
                    id: "org-a".to_string(),
                    name: "ACME".to_string(),
                    slug: "acme".to_string(),
                },
            ),
            Err(ControlPlaneApiError::Forbidden)
        ));
        let _ = std::fs::remove_dir_all(dir);
    }
}
