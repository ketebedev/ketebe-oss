use rmcp::schemars::JsonSchema;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpErrorCategory {
    Authentication,
    Authorization,
    Validation,
    NotFound,
    Conflict,
    RateLimit,
    Unavailable,
    Internal,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
pub struct McpError {
    pub code: String,
    pub category: McpErrorCategory,
    pub retryable: bool,
}

impl McpError {
    #[must_use]
    pub fn from_http(status: u16, code: impl Into<String>) -> Self {
        let code = sanitize_code(code.into());
        let (category, retryable) = match status {
            401 => (McpErrorCategory::Authentication, false),
            403 => (McpErrorCategory::Authorization, false),
            404 => (McpErrorCategory::NotFound, false),
            409 => (McpErrorCategory::Conflict, false),
            429 => (McpErrorCategory::RateLimit, true),
            400..=499 => (McpErrorCategory::Validation, false),
            500..=599 => (McpErrorCategory::Unavailable, true),
            _ => (McpErrorCategory::Internal, false),
        };
        Self {
            code,
            category,
            retryable,
        }
    }

    #[must_use]
    pub fn transport() -> Self {
        Self {
            code: "service_unavailable".to_string(),
            category: McpErrorCategory::Unavailable,
            retryable: true,
        }
    }

    #[must_use]
    pub fn validation(code: impl Into<String>) -> Self {
        Self {
            code: sanitize_code(code.into()),
            category: McpErrorCategory::Validation,
            retryable: false,
        }
    }

    #[must_use]
    pub fn from_stable_message(message: &str) -> Self {
        if message.contains("service unavailable") {
            return Self::transport();
        }
        let mut parts = message.split_whitespace().rev();
        let code = parts.next().unwrap_or("request_failed");
        if let Some(status) = parts.next().and_then(|value| value.parse::<u16>().ok()) {
            return Self::from_http(status, code);
        }
        Self::validation(code)
    }
}

fn sanitize_code(code: String) -> String {
    if !code.is_empty()
        && code.len() <= 96
        && code
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return code;
    }
    "request_failed".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_http_statuses_map_to_stable_categories() {
        assert_eq!(
            McpError::from_http(401, "unauthenticated").category,
            McpErrorCategory::Authentication
        );
        assert_eq!(
            McpError::from_http(403, "forbidden").category,
            McpErrorCategory::Authorization
        );
        assert_eq!(
            McpError::from_http(404, "not_found").category,
            McpErrorCategory::NotFound
        );
        assert_eq!(
            McpError::from_http(409, "conflict").category,
            McpErrorCategory::Conflict
        );
        assert!(McpError::from_http(429, "rate_limited").retryable);
        assert!(McpError::from_http(503, "unavailable").retryable);
    }

    #[test]
    fn unsafe_error_details_are_not_exposed_as_codes() {
        assert_eq!(
            McpError::from_http(500, "postgres://secret@internal-host").code,
            "request_failed"
        );
    }

    #[test]
    fn stable_messages_map_without_exposing_free_form_details() {
        assert_eq!(
            McpError::from_stable_message("Ketebe search request failed: 404 collection_not_found"),
            McpError::from_http(404, "collection_not_found")
        );
        assert_eq!(
            McpError::from_stable_message("Ketebe search service unavailable"),
            McpError::transport()
        );
    }
}
