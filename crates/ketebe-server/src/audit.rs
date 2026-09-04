use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditCategory {
    Authentication,
    Authorization,
    Governance,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditResult {
    Allowed,
    Denied,
    Error,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOrigin {
    Http,
    Grpc,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    timestamp_ms: u64,
    category: AuditCategory,
    action: String,
    result: AuditResult,
    actor_subject: Option<String>,
    project_id: Option<String>,
    resource_kind: Option<String>,
    resource_id: Option<String>,
    origin: AuditOrigin,
    correlation_id: Option<String>,
}

impl AuditEvent {
    #[must_use]
    pub fn new(
        category: AuditCategory,
        action: impl Into<String>,
        result: AuditResult,
        origin: AuditOrigin,
    ) -> Self {
        Self {
            timestamp_ms: now_ms(),
            category,
            action: action.into(),
            result,
            actor_subject: None,
            project_id: None,
            resource_kind: None,
            resource_id: None,
            origin,
            correlation_id: None,
        }
    }

    #[must_use]
    pub fn with_actor(mut self, subject: impl Into<String>) -> Self {
        self.actor_subject = non_empty(subject.into());
        self
    }

    #[must_use]
    pub fn with_project(mut self, project_id: impl Into<String>) -> Self {
        self.project_id = non_empty(project_id.into());
        self
    }

    #[must_use]
    pub fn with_resource(mut self, kind: impl Into<String>, id: impl Into<String>) -> Self {
        self.resource_kind = non_empty(kind.into());
        self.resource_id = non_empty(id.into());
        self
    }

    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = non_empty(correlation_id.into());
        self
    }

    #[must_use]
    pub fn category(&self) -> AuditCategory {
        self.category
    }

    #[must_use]
    pub fn result(&self) -> AuditResult {
        self.result
    }

    #[must_use]
    pub fn actor_subject(&self) -> Option<&str> {
        self.actor_subject.as_deref()
    }

    #[must_use]
    pub fn project_id(&self) -> Option<&str> {
        self.project_id.as_deref()
    }

    #[must_use]
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }
}

fn non_empty(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Debug)]
pub enum AuditError {
    Io(std::io::Error),
    Json(serde_json::Error),
    LockPoisoned,
}

impl fmt::Display for AuditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "audit sink I/O error: {error}"),
            Self::Json(error) => write!(f, "audit serialization error: {error}"),
            Self::LockPoisoned => f.write_str("audit sink lock poisoned"),
        }
    }
}

impl std::error::Error for AuditError {}
impl From<std::io::Error> for AuditError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<serde_json::Error> for AuditError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

pub trait AuditSink: Send + Sync + 'static {
    fn append(&self, event: &AuditEvent) -> Result<(), AuditError>;
}

#[derive(Default)]
pub struct NoopAuditSink;
impl AuditSink for NoopAuditSink {
    fn append(&self, _event: &AuditEvent) -> Result<(), AuditError> {
        Ok(())
    }
}

pub struct JsonlAuditSink {
    path: PathBuf,
    lock: Mutex<()>,
}

impl JsonlAuditSink {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, AuditError> {
        let path = data_dir.as_ref().join("security").join("audit.jsonl");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self {
            path,
            lock: Mutex::new(()),
        })
    }
}

impl AuditSink for JsonlAuditSink {
    fn append(&self, event: &AuditEvent) -> Result<(), AuditError> {
        let _guard = self.lock.lock().map_err(|_| AuditError::LockPoisoned)?;
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&self.path)?;
        serde_json::to_writer(&mut file, event)?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AuditService {
    sink: Arc<dyn AuditSink>,
}

impl fmt::Debug for AuditService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuditService").finish_non_exhaustive()
    }
}

impl Default for AuditService {
    fn default() -> Self {
        Self::noop()
    }
}

impl AuditService {
    #[must_use]
    pub fn noop() -> Self {
        Self {
            sink: Arc::new(NoopAuditSink),
        }
    }

    pub fn durable(data_dir: impl AsRef<Path>) -> Result<Self, AuditError> {
        Ok(Self {
            sink: Arc::new(JsonlAuditSink::open(data_dir)?),
        })
    }

    #[must_use]
    pub fn with_sink(sink: Arc<dyn AuditSink>) -> Self {
        Self { sink }
    }

    pub fn record(&self, event: &AuditEvent) -> Result<(), AuditError> {
        self.sink.append(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialized_event_has_no_secret_or_payload_fields() {
        let event = AuditEvent::new(
            AuditCategory::Authentication,
            "authenticate",
            AuditResult::Denied,
            AuditOrigin::Http,
        )
        .with_actor("subject-a")
        .with_project("project-a")
        .with_correlation_id("request-1");
        let json = serde_json::to_string(&event).unwrap();
        assert!(!json.contains("credential"));
        assert!(!json.contains("secret"));
        assert!(!json.contains("payload"));
        assert!(json.contains("subject-a"));
        assert!(json.contains("project-a"));
        assert!(json.contains("request-1"));
    }

    #[test]
    fn durable_sink_appends_jsonl_without_sensitive_surface() {
        let dir = std::env::temp_dir().join(format!("ketebe-audit-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let service = AuditService::durable(&dir).unwrap();
        let event = AuditEvent::new(
            AuditCategory::Governance,
            "write_admission",
            AuditResult::Allowed,
            AuditOrigin::Internal,
        )
        .with_project("project-a");
        service.record(&event).unwrap();
        let text = fs::read_to_string(dir.join("security/audit.jsonl")).unwrap();
        assert_eq!(text.lines().count(), 1);
        assert!(text.contains("write_admission"));
        assert!(!text.contains("secret"));
        let _ = fs::remove_dir_all(dir);
    }
}
