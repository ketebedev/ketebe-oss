use ketebe_core::FieldPath;
use ketebe_storage::{DEFAULT_RRF_K, ExecutionPreference};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::reranking::RerankFailurePolicy;

pub const DEFAULT_QUERY_TOP_K: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchProfileExecution {
    Auto,
    Exact,
    Hnsw,
}

impl From<SearchProfileExecution> for ExecutionPreference {
    fn from(value: SearchProfileExecution) -> Self {
        match value {
            SearchProfileExecution::Auto => Self::Auto,
            SearchProfileExecution::Exact => Self::Exact,
            SearchProfileExecution::Hnsw => Self::Hnsw,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchProfileFailurePolicy {
    Fail,
    PreserveCandidateOrder,
}

impl From<SearchProfileFailurePolicy> for RerankFailurePolicy {
    fn from(value: SearchProfileFailurePolicy) -> Self {
        match value {
            SearchProfileFailurePolicy::Fail => Self::Fail,
            SearchProfileFailurePolicy::PreserveCandidateOrder => Self::PreserveCandidateOrder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchProfileRerank {
    pub profile: String,
    pub top_n: usize,
    #[serde(default)]
    pub text_fields: Vec<Vec<String>>,
    #[serde(default)]
    pub include_metadata: bool,
    pub failure_policy: SearchProfileFailurePolicy,
}

impl SearchProfileRerank {
    pub fn validate(&self, final_top_k: usize) -> Result<(), SearchProfileError> {
        if self.profile.trim().is_empty() {
            return Err(SearchProfileError::Invalid(
                "reranker profile must not be empty".into(),
            ));
        }
        if self.top_n < final_top_k {
            return Err(SearchProfileError::Invalid(
                "rerank top_n must be greater than or equal to final_top_k".into(),
            ));
        }
        if self.text_fields.is_empty() {
            return Err(SearchProfileError::Invalid(
                "rerank text_fields must contain at least one field".into(),
            ));
        }
        for path in &self.text_fields {
            FieldPath::new(path.clone())
                .map_err(|error| SearchProfileError::Invalid(error.to_string()))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchProfile {
    pub name: String,
    pub version: u64,
    pub execution: SearchProfileExecution,
    pub dense_candidates: Option<usize>,
    pub lexical_candidates: Option<usize>,
    pub rrf_k: u32,
    pub final_top_k: usize,
    pub timeout_ms: Option<u64>,
    pub rerank: Option<SearchProfileRerank>,
}

impl SearchProfile {
    pub fn validate(&self) -> Result<(), SearchProfileError> {
        validate_name(&self.name)?;
        if self.version == 0 {
            return Err(SearchProfileError::Invalid(
                "profile version must be greater than zero".into(),
            ));
        }
        if self.final_top_k == 0 {
            return Err(SearchProfileError::Invalid(
                "final_top_k must be greater than zero".into(),
            ));
        }
        if self.dense_candidates == Some(0) || self.lexical_candidates == Some(0) {
            return Err(SearchProfileError::Invalid(
                "candidate limits must be greater than zero when provided".into(),
            ));
        }
        if self.rrf_k == 0 {
            return Err(SearchProfileError::Invalid(
                "rrf_k must be greater than zero".into(),
            ));
        }
        if self.timeout_ms == Some(0) {
            return Err(SearchProfileError::Invalid(
                "timeout_ms must be greater than zero when provided".into(),
            ));
        }
        if let Some(rerank) = &self.rerank {
            rerank.validate(self.final_top_k)?;
        }
        Ok(())
    }

    #[must_use]
    pub fn pinned_id(&self) -> String {
        format!("{}@{}", self.name, self.version)
    }
}

impl Default for SearchProfile {
    fn default() -> Self {
        Self {
            name: "default".into(),
            version: 1,
            execution: SearchProfileExecution::Auto,
            dense_candidates: None,
            lexical_candidates: None,
            rrf_k: DEFAULT_RRF_K,
            final_top_k: DEFAULT_QUERY_TOP_K,
            timeout_ms: None,
            rerank: None,
        }
    }
}

#[derive(Debug)]
pub enum SearchProfileError {
    Invalid(String),
    AlreadyExists(String),
    NotFound(String),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for SearchProfileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "invalid search profile: {message}"),
            Self::AlreadyExists(selector) => {
                write!(f, "search profile '{selector}' already exists")
            }
            Self::NotFound(selector) => write!(f, "search profile '{selector}' was not found"),
            Self::Io(error) => write!(f, "search profile store failed: {error}"),
            Self::Json(error) => write!(f, "search profile data is corrupt: {error}"),
        }
    }
}

impl std::error::Error for SearchProfileError {}

impl From<std::io::Error> for SearchProfileError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for SearchProfileError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Debug, Clone)]
pub struct SearchProfileStore {
    data_dir: PathBuf,
}

impl SearchProfileStore {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    pub fn create(
        &self,
        collection_id: &str,
        profile: SearchProfile,
    ) -> Result<SearchProfile, SearchProfileError> {
        profile.validate()?;
        let dir = self.profile_dir(collection_id);
        fs::create_dir_all(&dir)?;
        let path = dir.join(file_name(&profile.name, profile.version));
        if path.exists() {
            return Err(SearchProfileError::AlreadyExists(profile.pinned_id()));
        }
        atomic_write_json(&path, &profile)?;
        Ok(profile)
    }

    pub fn list(&self, collection_id: &str) -> Result<Vec<SearchProfile>, SearchProfileError> {
        let dir = self.profile_dir(collection_id);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut profiles = Vec::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            profiles.push(read_profile(&path)?);
        }
        profiles.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then(left.version.cmp(&right.version))
        });
        Ok(profiles)
    }

    pub fn get(
        &self,
        collection_id: &str,
        selector: &str,
    ) -> Result<SearchProfile, SearchProfileError> {
        let (name, version) = parse_selector(selector)?;
        if let Some(version) = version {
            let path = self
                .profile_dir(collection_id)
                .join(file_name(name, version));
            return if path.exists() {
                read_profile(&path)
            } else {
                Err(SearchProfileError::NotFound(selector.to_string()))
            };
        }
        self.list(collection_id)?
            .into_iter()
            .filter(|profile| profile.name == name)
            .max_by_key(|profile| profile.version)
            .ok_or_else(|| SearchProfileError::NotFound(selector.to_string()))
    }

    pub fn delete(
        &self,
        collection_id: &str,
        selector: &str,
    ) -> Result<SearchProfile, SearchProfileError> {
        let profile = self.get(collection_id, selector)?;
        let path = self
            .profile_dir(collection_id)
            .join(file_name(&profile.name, profile.version));
        fs::remove_file(path)?;
        Ok(profile)
    }

    fn profile_dir(&self, collection_id: &str) -> PathBuf {
        self.data_dir
            .join("collections")
            .join(collection_id)
            .join("search_profiles")
    }
}

fn validate_name(name: &str) -> Result<(), SearchProfileError> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'-' | b'_'));
    if valid {
        Ok(())
    } else {
        Err(SearchProfileError::Invalid(
            "profile name must be 1..=64 ASCII letters, digits, '-' or '_'".into(),
        ))
    }
}

fn parse_selector(selector: &str) -> Result<(&str, Option<u64>), SearchProfileError> {
    let mut parts = selector.split('@');
    let name = parts.next().unwrap_or_default();
    validate_name(name)?;
    let Some(version) = parts.next() else {
        return Ok((name, None));
    };
    if parts.next().is_some() {
        return Err(SearchProfileError::Invalid(
            "profile selector contains multiple '@' separators".into(),
        ));
    }
    let version = version.parse::<u64>().map_err(|_| {
        SearchProfileError::Invalid("profile version must be an unsigned integer".into())
    })?;
    if version == 0 {
        return Err(SearchProfileError::Invalid(
            "profile version must be greater than zero".into(),
        ));
    }
    Ok((name, Some(version)))
}

fn file_name(name: &str, version: u64) -> String {
    format!("{name}@{version}.json")
}

fn read_profile(path: &Path) -> Result<SearchProfile, SearchProfileError> {
    let profile = serde_json::from_slice::<SearchProfile>(&fs::read(path)?)?;
    profile.validate()?;
    Ok(profile)
}

fn atomic_write_json(path: &Path, profile: &SearchProfile) -> Result<(), SearchProfileError> {
    let bytes = serde_json::to_vec_pretty(profile)?;
    let tmp = path.with_extension(format!("json.tmp.{}", std::process::id()));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        let directory = fs::File::open(parent)?;
        directory.sync_all()?;
    }
    Ok(())
}

#[must_use]
pub fn profiles_by_name(profiles: &[SearchProfile]) -> BTreeMap<String, Vec<u64>> {
    let mut result = BTreeMap::<String, Vec<u64>>::new();
    for profile in profiles {
        result
            .entry(profile.name.clone())
            .or_default()
            .push(profile.version);
    }
    result
}
