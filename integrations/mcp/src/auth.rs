use std::fmt;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::{ketebe::KetebeApi, observability::observe_auth_denial};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AuthMode {
    #[default]
    Development,
    Required,
}

#[derive(Clone, PartialEq, Eq)]
pub struct RequestCredential(String);

impl RequestCredential {
    pub fn from_authorization(value: &str) -> Result<Self, AuthError> {
        let token = value
            .strip_prefix("Bearer ")
            .filter(|token| !token.is_empty())
            .ok_or(AuthError::InvalidCredential)?;
        Ok(Self(token.to_string()))
    }

    pub fn from_token(token: impl Into<String>) -> Result<Self, AuthError> {
        let token = token.into();
        if token.is_empty() {
            return Err(AuthError::InvalidCredential);
        }
        Ok(Self(token))
    }

    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RequestCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RequestCredential")
            .field(&"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    MissingCredential,
    InvalidCredential,
    UpstreamRejected,
    UpstreamUnavailable,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingCredential => f.write_str("authentication credential is missing"),
            Self::InvalidCredential => f.write_str("authentication credential is invalid"),
            Self::UpstreamRejected => f.write_str("Ketebe rejected the authentication credential"),
            Self::UpstreamUnavailable => {
                f.write_str("Ketebe authentication service is unavailable")
            }
        }
    }
}

impl std::error::Error for AuthError {}

#[derive(Clone, Debug)]
pub struct RemoteAuthState {
    pub mode: AuthMode,
    pub api: KetebeApi,
}

pub async fn authenticate_remote(
    State(state): State<RemoteAuthState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    if state.mode == AuthMode::Development {
        return next.run(request).await;
    }
    let Some(value) = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return auth_response(StatusCode::UNAUTHORIZED, "authentication required");
    };
    let credential = match RequestCredential::from_authorization(value) {
        Ok(credential) => credential,
        Err(_) => return auth_response(StatusCode::UNAUTHORIZED, "authentication required"),
    };
    match state.api.authenticate(credential.expose_secret()).await {
        Ok(()) => {
            request.extensions_mut().insert(credential);
            next.run(request).await
        }
        Err(crate::ketebe::AuthenticationProbeError::Unauthenticated) => {
            auth_response(StatusCode::UNAUTHORIZED, "authentication required")
        }
        Err(crate::ketebe::AuthenticationProbeError::Forbidden) => {
            auth_response(StatusCode::FORBIDDEN, "authorization denied")
        }
        Err(crate::ketebe::AuthenticationProbeError::Unavailable) => auth_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Ketebe authentication unavailable",
        ),
    }
}

fn auth_response(status: StatusCode, message: &'static str) -> Response {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        observe_auth_denial(status == StatusCode::FORBIDDEN);
    }
    let mut response = (
        status,
        axum::Json(serde_json::json!({
            "error": {
                "code": if status == StatusCode::FORBIDDEN { "forbidden" } else { "unauthenticated" },
                "message": message
            }
        })),
    )
        .into_response();
    if status == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            axum::http::HeaderValue::from_static("Bearer"),
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_debug_never_exposes_secret() {
        let credential = RequestCredential::from_token("do-not-log").unwrap();
        let debug = format!("{credential:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("do-not-log"));
    }
}
