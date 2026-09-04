use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolClass {
    Read,
    Write,
    Admin,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolMetadata {
    pub description: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolPolicy {
    enabled: bool,
    allow_write: bool,
    allow_admin: bool,
    allow_tools: Option<HashSet<String>>,
    deny_tools: HashSet<String>,
    metadata: HashMap<String, ToolMetadata>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PolicyError {
    InvalidBoolean { name: &'static str, value: String },
}

impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBoolean { name, value } => {
                write!(f, "invalid boolean value {value:?} for {name}")
            }
        }
    }
}

impl std::error::Error for PolicyError {}

impl ToolPolicy {
    pub fn from_env() -> Result<Self, PolicyError> {
        Self::from_map(&std::env::vars().collect())
    }

    pub fn from_map(values: &HashMap<String, String>) -> Result<Self, PolicyError> {
        Ok(Self {
            enabled: parse_bool(values, "KETEBE_MCP_ENABLED", false)?,
            allow_write: parse_bool(values, "KETEBE_MCP_ALLOW_WRITE", false)?,
            allow_admin: parse_bool(values, "KETEBE_MCP_ALLOW_ADMIN", false)?,
            allow_tools: values
                .get("KETEBE_MCP_TOOL_ALLOW")
                .map(|value| parse_tool_set(value)),
            deny_tools: values
                .get("KETEBE_MCP_TOOL_DENY")
                .map_or_else(HashSet::new, |value| parse_tool_set(value)),
            metadata: HashMap::new(),
        })
    }

    #[must_use]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    #[must_use]
    pub fn class_enabled(&self, class: ToolClass) -> bool {
        match class {
            ToolClass::Read => true,
            ToolClass::Write => self.allow_write,
            ToolClass::Admin => self.allow_admin,
        }
    }

    #[must_use]
    pub fn tool_visible(&self, name: &str, class: ToolClass) -> bool {
        if !self.enabled || !self.class_enabled(class) || self.deny_tools.contains(name) {
            return false;
        }
        self.allow_tools
            .as_ref()
            .is_none_or(|allowed| allowed.contains(name))
    }

    #[must_use]
    pub fn execution_allowed(
        &self,
        name: &str,
        class: ToolClass,
        underlying_authorized: bool,
    ) -> bool {
        underlying_authorized && self.tool_visible(name, class)
    }

    pub fn set_description(&mut self, name: impl Into<String>, description: impl Into<String>) {
        self.metadata.insert(
            name.into(),
            ToolMetadata {
                description: Some(description.into()),
            },
        );
    }

    #[must_use]
    pub fn metadata(&self, name: &str) -> Option<&ToolMetadata> {
        self.metadata.get(name)
    }
}

fn parse_bool(
    values: &HashMap<String, String>,
    name: &'static str,
    default: bool,
) -> Result<bool, PolicyError> {
    match values.get(name).map(String::as_str) {
        None => Ok(default),
        Some("true" | "1" | "yes") => Ok(true),
        Some("false" | "0" | "no") => Ok(false),
        Some(value) => Err(PolicyError::InvalidBoolean {
            name,
            value: value.to_string(),
        }),
    }
}

fn parse_tool_set(value: &str) -> HashSet<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_and_mutation_classes_are_disabled_by_default() {
        let policy = ToolPolicy::default();
        assert!(!policy.enabled());
        assert!(!policy.tool_visible("search", ToolClass::Read));
        assert!(!policy.tool_visible("upsert_records", ToolClass::Write));
        assert!(!policy.tool_visible("delete_collection", ToolClass::Admin));
    }

    #[test]
    fn write_and_admin_gates_are_independent() {
        let values = HashMap::from([
            ("KETEBE_MCP_ENABLED".into(), "true".into()),
            ("KETEBE_MCP_ALLOW_WRITE".into(), "true".into()),
        ]);
        let policy = ToolPolicy::from_map(&values).unwrap();
        assert!(policy.tool_visible("search", ToolClass::Read));
        assert!(policy.tool_visible("upsert_records", ToolClass::Write));
        assert!(!policy.tool_visible("delete_collection", ToolClass::Admin));
    }

    #[test]
    fn deny_overrides_allow_list() {
        let values = HashMap::from([
            ("KETEBE_MCP_ENABLED".into(), "true".into()),
            ("KETEBE_MCP_TOOL_ALLOW".into(), "search,get_record".into()),
            ("KETEBE_MCP_TOOL_DENY".into(), "search".into()),
        ]);
        let policy = ToolPolicy::from_map(&values).unwrap();
        assert!(!policy.tool_visible("search", ToolClass::Read));
        assert!(policy.tool_visible("get_record", ToolClass::Read));
        assert!(!policy.tool_visible("describe_collection", ToolClass::Read));
    }

    #[test]
    fn metadata_customization_cannot_elevate_authorization() {
        let values = HashMap::from([("KETEBE_MCP_ENABLED".into(), "true".into())]);
        let mut policy = ToolPolicy::from_map(&values).unwrap();
        policy.set_description("search", "Custom agent-facing description");
        assert_eq!(
            policy
                .metadata("search")
                .and_then(|metadata| metadata.description.as_deref()),
            Some("Custom agent-facing description")
        );
        assert!(!policy.execution_allowed("search", ToolClass::Read, false));
        assert!(policy.execution_allowed("search", ToolClass::Read, true));
    }
}
