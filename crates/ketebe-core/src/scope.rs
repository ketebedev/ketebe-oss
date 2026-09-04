use crate::{CollectionId, DomainError};
use std::fmt;

const MAX_PROJECT_ID_LEN: usize = 128;
const MAX_COLLECTION_NAME_LEN: usize = 128;

/// Stable project identity used by authentication, authorization and data-plane routing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectId(String);

impl ProjectId {
    pub fn new(value: impl Into<String>) -> Result<Self, ScopeError> {
        let value = value.into();
        validate_identifier("project id", &value, MAX_PROJECT_ID_LEN)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Compatibility scope for pre-multi-tenant OSS data.
    #[must_use]
    pub fn default_project() -> Self {
        Self("default".to_string())
    }
}

impl fmt::Display for ProjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// User-visible collection name. It is resolved within a project and is not a storage identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CollectionName(String);

impl CollectionName {
    pub fn new(value: impl Into<String>) -> Result<Self, ScopeError> {
        let value = value.into();
        validate_identifier("collection name", &value, MAX_COLLECTION_NAME_LEN)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CollectionName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Canonical immutable scope for every protected data-plane operation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataPlaneScope {
    project_id: ProjectId,
    collection_id: CollectionId,
}

impl DataPlaneScope {
    #[must_use]
    pub fn new(project_id: ProjectId, collection_id: CollectionId) -> Self {
        Self {
            project_id,
            collection_id,
        }
    }

    #[must_use]
    pub fn project_id(&self) -> &ProjectId {
        &self.project_id
    }

    #[must_use]
    pub fn collection_id(&self) -> &CollectionId {
        &self.collection_id
    }

    /// Maps pre-scope single-tenant collections into the deterministic default project.
    pub fn legacy_default(collection_id: CollectionId) -> Self {
        Self::new(ProjectId::default_project(), collection_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeError {
    Empty(&'static str),
    TooLong {
        kind: &'static str,
        max: usize,
        actual: usize,
    },
    InvalidCharacter {
        kind: &'static str,
        character: char,
        index: usize,
    },
    InvalidCollectionId(DomainError),
}

impl fmt::Display for ScopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty(kind) => write!(formatter, "{kind} must not be empty"),
            Self::TooLong { kind, max, actual } => {
                write!(formatter, "{kind} exceeds {max} bytes: {actual}")
            }
            Self::InvalidCharacter {
                kind,
                character,
                index,
            } => write!(
                formatter,
                "invalid character '{character}' in {kind} at byte {index}"
            ),
            Self::InvalidCollectionId(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ScopeError {}

fn validate_identifier(kind: &'static str, value: &str, max: usize) -> Result<(), ScopeError> {
    if value.is_empty() {
        return Err(ScopeError::Empty(kind));
    }
    if value.len() > max {
        return Err(ScopeError::TooLong {
            kind,
            max,
            actual: value.len(),
        });
    }
    for (index, character) in value.char_indices() {
        let valid = character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || character == '-'
            || character == '_';
        if !valid {
            return Err(ScopeError::InvalidCharacter {
                kind,
                character,
                index,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_collection_id_in_different_projects_has_distinct_scope() {
        let collection = CollectionId::new("documents").unwrap();
        let a = DataPlaneScope::new(ProjectId::new("project-a").unwrap(), collection.clone());
        let b = DataPlaneScope::new(ProjectId::new("project-b").unwrap(), collection);
        assert_ne!(a, b);
    }

    #[test]
    fn legacy_scope_is_deterministic() {
        let collection = CollectionId::new("documents").unwrap();
        let scope = DataPlaneScope::legacy_default(collection);
        assert_eq!(scope.project_id().as_str(), "default");
        assert_eq!(scope.collection_id().as_str(), "documents");
    }

    #[test]
    fn collection_name_and_stable_id_are_separate_types() {
        let name = CollectionName::new("documents").unwrap();
        let id = CollectionId::new("collection-01").unwrap();
        assert_eq!(name.as_str(), "documents");
        assert_eq!(id.as_str(), "collection-01");
    }
}
