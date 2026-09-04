use axum::Json;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::{
    AdmissionClass, AppState, AuditCategory, AuditEvent, AuditOrigin, AuditResult,
    AuthorizationAction, AuthorizationResource, JobId, JobService, Principal,
};

pub(crate) async fn http_authorize(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if !is_read_only_collection_post(request.method(), request.uri().path()) {
        return authorize_default(State(state), request, next).await;
    }

    let path = request.uri().path().to_string();
    let correlation_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let Some(principal) = request.extensions().get::<Principal>().cloned() else {
        return forbidden_response();
    };
    let Some(collection_id) = collection_id_from_path(&path) else {
        return authorize_default(State(state), request, next).await;
    };

    let resource = match state.authorization().authorize_collection(
        &principal,
        AuthorizationAction::CollectionRead,
        collection_id,
    ) {
        Ok(resource) => resource,
        Err(_) => {
            audit_decision(
                &state,
                &principal,
                AuditCategory::Authorization,
                "collection_access",
                AuditResult::Denied,
                Some(collection_id),
                correlation_id.as_deref(),
            );
            return undiscoverable_response();
        }
    };
    audit_decision(
        &state,
        &principal,
        AuditCategory::Authorization,
        "collection_access",
        AuditResult::Allowed,
        Some(collection_id),
        correlation_id.as_deref(),
    );

    let AuthorizationResource::Collection { project_id, .. } = resource else {
        return forbidden_response();
    };
    if let Err(error) = state.governance().admit(&project_id, AdmissionClass::Read) {
        audit_decision(
            &state,
            &principal,
            AuditCategory::Governance,
            "request_admission",
            AuditResult::Denied,
            Some(collection_id),
            correlation_id.as_deref(),
        );
        return governance_response(error);
    }
    audit_decision(
        &state,
        &principal,
        AuditCategory::Governance,
        "request_admission",
        AuditResult::Allowed,
        Some(collection_id),
        correlation_id.as_deref(),
    );
    next.run(request).await
}

async fn authorize_default(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if is_profile_discovery_path(path) {
        let Some(principal) = request.extensions().get::<Principal>() else {
            return forbidden_response();
        };
        let Some(project) = principal.project_id() else {
            return forbidden_response();
        };
        if state
            .authorization()
            .authorize_project(principal, AuthorizationAction::CollectionRead, project)
            .is_err()
        {
            return forbidden_response();
        }
    }
    if is_jobs_path(path) {
        let Some(principal) = request.extensions().get::<Principal>() else {
            return forbidden_response();
        };
        let action = if request.method() == Method::GET {
            AuthorizationAction::CollectionRead
        } else {
            AuthorizationAction::CollectionWrite
        };
        let project = crate::job_access::principal_project(principal);
        if state
            .authorization()
            .authorize_project(principal, action, &project)
            .is_err()
        {
            return forbidden_response();
        }

        if let Some(raw_job_id) = job_id_from_path(path) {
            let job = raw_job_id
                .parse::<u64>()
                .ok()
                .and_then(|value| JobId::new(value).ok())
                .and_then(|id| JobService::new(state.clone()).get(id).ok());
            let Some(job) = job else {
                return job_not_found_response();
            };
            match crate::job_access::can_access_job(&state, principal, &job) {
                Ok(true) => {}
                Ok(false) => return job_not_found_response(),
                Err(_) => return internal_error_response(),
            }
        }
    }
    crate::authorization::http_authorize(State(state), request, next).await
}

fn is_read_only_collection_post(method: &Method, path: &str) -> bool {
    if method != Method::POST || collection_id_from_path(path).is_none() {
        return false;
    }
    path.ends_with("/query") || path.ends_with("/query:embed") || path.ends_with("/records:fetch")
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

fn is_profile_discovery_path(path: &str) -> bool {
    let mut parts = path.split('/').filter(|part| !part.is_empty());
    if parts.next() != Some("v0") {
        return false;
    }
    matches!(
        parts.next(),
        Some("embedding-profiles" | "reranker-profiles")
    )
}

fn is_jobs_path(path: &str) -> bool {
    let mut parts = path.split('/').filter(|part| !part.is_empty());
    parts.next() == Some("v0") && parts.next() == Some("jobs")
}

fn job_id_from_path(path: &str) -> Option<&str> {
    let mut parts = path.split('/').filter(|part| !part.is_empty());
    if parts.next()? != "v0" || parts.next()? != "jobs" {
        return None;
    }
    parts.next().filter(|value| !value.is_empty())
}

fn audit_decision(
    state: &AppState,
    principal: &Principal,
    category: AuditCategory,
    action: &str,
    result: AuditResult,
    resource_id: Option<&str>,
    correlation_id: Option<&str>,
) {
    let mut event = AuditEvent::new(category, action, result, AuditOrigin::Http)
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
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({"error":{"code":"collection_not_found","message":"collection was not found"}})),
    )
        .into_response()
}

fn job_not_found_response() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({"error":{"code":"job_not_found","message":"job was not found"}})),
    )
        .into_response()
}

fn internal_error_response() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({"error":{"code":"internal_error","message":"internal server error"}})),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_post_routes_are_classified_as_reads() {
        for path in [
            "/v0/collections/docs/query",
            "/v1/collections/docs/query",
            "/v1/collections/docs/query:embed",
            "/v0/collections/docs/records:fetch",
        ] {
            assert!(is_read_only_collection_post(&Method::POST, path), "{path}");
        }
        assert!(!is_read_only_collection_post(
            &Method::POST,
            "/v0/collections/docs/records:batchUpsert"
        ));
        assert!(!is_read_only_collection_post(
            &Method::PUT,
            "/v0/collections/docs/query"
        ));
    }

    #[test]
    fn profile_discovery_paths_are_recognized() {
        assert!(is_profile_discovery_path("/v0/embedding-profiles"));
        assert!(is_profile_discovery_path("/v0/embedding-profiles/default"));
        assert!(is_profile_discovery_path("/v0/reranker-profiles"));
        assert!(!is_profile_discovery_path("/v0/search-profiles"));
    }

    #[test]
    fn job_paths_are_recognized_without_treating_list_as_one_job() {
        assert!(is_jobs_path("/v0/jobs"));
        assert_eq!(job_id_from_path("/v0/jobs/42"), Some("42"));
        assert_eq!(job_id_from_path("/v0/jobs/42/cancel"), Some("42"));
        assert_eq!(job_id_from_path("/v0/jobs"), None);
    }
}
