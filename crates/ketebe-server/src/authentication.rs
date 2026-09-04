use std::fmt;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use tonic::{Request as GrpcRequest, Status};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthenticationMode {
    Development,
    Required,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrincipalKind {
    Development,
    Credential,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    subject: String,
    kind: PrincipalKind,
    project_id: Option<String>,
}

impl Principal {
    pub fn new(
        subject: impl Into<String>,
        kind: PrincipalKind,
    ) -> Result<Self, AuthenticationError> {
        let subject = subject.into();
        if subject.trim().is_empty() {
            return Err(AuthenticationError::InvalidPrincipal);
        }
        Ok(Self {
            subject,
            kind,
            project_id: None,
        })
    }

    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    #[must_use]
    pub fn kind(&self) -> PrincipalKind {
        self.kind
    }

    #[must_use]
    pub fn project_id(&self) -> Option<&str> {
        self.project_id.as_deref()
    }

    pub fn for_project(
        subject: impl Into<String>,
        project_id: impl Into<String>,
    ) -> Result<Self, AuthenticationError> {
        let mut principal = Self::new(subject, PrincipalKind::Credential)?;
        let project_id = project_id.into();
        if project_id.trim().is_empty() {
            return Err(AuthenticationError::InvalidPrincipal);
        }
        principal.project_id = Some(project_id);
        Ok(principal)
    }

    pub(crate) fn development() -> Self {
        Self {
            subject: "development".to_string(),
            kind: PrincipalKind::Development,
            project_id: Some("default".to_string()),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Credential(String);

impl Credential {
    pub fn new(value: impl Into<String>) -> Result<Self, AuthenticationError> {
        let value = value.into();
        if value.is_empty() {
            return Err(AuthenticationError::InvalidCredential);
        }
        Ok(Self(value))
    }

    /// Returns the opaque secret to an authenticator. Never log or persist this value.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Credential").field(&"[REDACTED]").finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthenticationError {
    MissingCredential,
    InvalidCredential,
    InvalidPrincipal,
}

impl fmt::Display for AuthenticationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingCredential | Self::InvalidCredential => {
                f.write_str("authentication required")
            }
            Self::InvalidPrincipal => f.write_str("invalid principal"),
        }
    }
}

impl std::error::Error for AuthenticationError {}

pub trait CredentialAuthenticator: Send + Sync {
    fn authenticate(&self, credential: &Credential) -> Result<Principal, AuthenticationError>;
}

#[derive(Clone)]
pub struct AuthenticationService {
    mode: AuthenticationMode,
    authenticator: Option<Arc<dyn CredentialAuthenticator>>,
}

impl AuthenticationService {
    #[must_use]
    pub fn development() -> Self {
        Self {
            mode: AuthenticationMode::Development,
            authenticator: None,
        }
    }

    #[must_use]
    pub fn required(authenticator: Arc<dyn CredentialAuthenticator>) -> Self {
        Self {
            mode: AuthenticationMode::Required,
            authenticator: Some(authenticator),
        }
    }

    #[must_use]
    pub fn mode(&self) -> AuthenticationMode {
        self.mode
    }

    pub fn authenticate(
        &self,
        credential: Option<&Credential>,
    ) -> Result<Principal, AuthenticationError> {
        match self.mode {
            AuthenticationMode::Development => Ok(Principal::development()),
            AuthenticationMode::Required => {
                let credential = credential.ok_or(AuthenticationError::MissingCredential)?;
                self.authenticator
                    .as_ref()
                    .ok_or(AuthenticationError::MissingCredential)?
                    .authenticate(credential)
            }
        }
    }

    pub fn authenticate_authorization_value(
        &self,
        authorization: Option<&str>,
    ) -> Result<Principal, AuthenticationError> {
        let credential = authorization.map(parse_bearer_value).transpose()?;
        self.authenticate(credential.as_ref())
    }
}

#[derive(Clone)]
pub(crate) struct HttpAuthenticationState {
    authentication: AuthenticationService,
    audit: Arc<crate::AuditService>,
}

impl HttpAuthenticationState {
    #[must_use]
    pub(crate) fn new(
        authentication: AuthenticationService,
        audit: Arc<crate::AuditService>,
    ) -> Self {
        Self {
            authentication,
            audit,
        }
    }
}

pub(crate) async fn http_authenticate(
    State(state): State<HttpAuthenticationState>,
    mut request: Request,
    next: Next,
) -> Response {
    let credential = match bearer_credential(request.headers().get(header::AUTHORIZATION)) {
        Ok(credential) => credential,
        Err(error) => {
            record_authentication(
                &state.audit,
                crate::AuditOrigin::Http,
                crate::AuditResult::Denied,
                None,
            );
            return authentication_error_response(error);
        }
    };
    match state.authentication.authenticate(credential.as_ref()) {
        Ok(principal) => {
            record_authentication(
                &state.audit,
                crate::AuditOrigin::Http,
                crate::AuditResult::Allowed,
                Some(&principal),
            );
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(error) => {
            record_authentication(
                &state.audit,
                crate::AuditOrigin::Http,
                crate::AuditResult::Denied,
                None,
            );
            authentication_error_response(error)
        }
    }
}

fn bearer_credential(
    header: Option<&axum::http::HeaderValue>,
) -> Result<Option<Credential>, AuthenticationError> {
    let Some(header) = header else {
        return Ok(None);
    };
    let value = header
        .to_str()
        .map_err(|_| AuthenticationError::InvalidCredential)?;
    parse_bearer_value(value).map(Some)
}

fn parse_bearer_value(value: &str) -> Result<Credential, AuthenticationError> {
    let token = value
        .strip_prefix("Bearer ")
        .ok_or(AuthenticationError::InvalidCredential)?;
    Credential::new(token)
}

fn authentication_error_code(error: AuthenticationError) -> &'static str {
    match error {
        AuthenticationError::MissingCredential => "missing_credential",
        AuthenticationError::InvalidCredential => "invalid_credential",
        AuthenticationError::InvalidPrincipal => "invalid_principal",
    }
}

fn authentication_error_response(error: AuthenticationError) -> Response {
    let status = StatusCode::UNAUTHORIZED;
    let code = authentication_error_code(error);
    (
        status,
        Json(AuthenticationErrorEnvelope {
            error: AuthenticationErrorBody {
                code,
                message: error.to_string(),
            },
        }),
    )
        .into_response()
}

#[derive(Serialize)]
struct AuthenticationErrorEnvelope {
    error: AuthenticationErrorBody,
}

#[derive(Serialize)]
struct AuthenticationErrorBody {
    code: &'static str,
    message: String,
}

pub(crate) fn grpc_authenticate<T>(
    authentication: &AuthenticationService,
    audit: &Arc<crate::AuditService>,
    mut request: GrpcRequest<T>,
) -> Result<GrpcRequest<T>, Status> {
    let credential = match grpc_bearer_credential(request.metadata().get("authorization")) {
        Ok(credential) => credential,
        Err(status) => {
            record_authentication(
                audit,
                crate::AuditOrigin::Grpc,
                crate::AuditResult::Denied,
                None,
            );
            return Err(status);
        }
    };
    let principal = match authentication.authenticate(credential.as_ref()) {
        Ok(principal) => principal,
        Err(error) => {
            record_authentication(
                audit,
                crate::AuditOrigin::Grpc,
                crate::AuditResult::Denied,
                None,
            );
            return Err(authentication_error_status(error));
        }
    };
    record_authentication(
        audit,
        crate::AuditOrigin::Grpc,
        crate::AuditResult::Allowed,
        Some(&principal),
    );
    request.extensions_mut().insert(principal);
    Ok(request)
}

fn grpc_bearer_credential(
    metadata: Option<&tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
) -> Result<Option<Credential>, Status> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let value = metadata
        .to_str()
        .map_err(|_| Status::unauthenticated("authentication required"))?;
    parse_bearer_value(value)
        .map(Some)
        .map_err(authentication_error_status)
}

fn authentication_error_status(error: AuthenticationError) -> Status {
    match error {
        AuthenticationError::MissingCredential
        | AuthenticationError::InvalidCredential
        | AuthenticationError::InvalidPrincipal => Status::unauthenticated(error.to_string()),
    }
}

fn record_authentication(
    audit: &crate::AuditService,
    origin: crate::AuditOrigin,
    result: crate::AuditResult,
    principal: Option<&Principal>,
) {
    let mut event = crate::AuditEvent::new(
        crate::AuditCategory::Authentication,
        "authenticate",
        result,
        origin,
    );
    if let Some(principal) = principal {
        event = event.with_actor(principal.subject());
        if let Some(project_id) = principal.project_id() {
            event = event.with_project(project_id);
        }
    }
    let _ = audit.record(&event);
}
