use std::fmt;

use crate::ProjectId;

const MAX_ORGANIZATION_ID_LEN: usize = 128;
const MAX_RESOURCE_NAME_LEN: usize = 256;
const MAX_RESOURCE_SLUG_LEN: usize = 128;

/// Stable organization identity used by Ketebe's control plane.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrganizationId(String);

impl OrganizationId {
    pub fn new(value: impl Into<String>) -> Result<Self, ControlPlaneDomainError> {
        let value = value.into();
        validate_identifier("organization id", &value, MAX_ORGANIZATION_ID_LEN)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Deterministic organization identity used by simple self-hosted bootstrap.
    #[must_use]
    pub fn default_organization() -> Self {
        Self("default".to_string())
    }
}

impl fmt::Display for OrganizationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Mutable lifecycle state for first-class control-plane resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceLifecycleState {
    Active,
    Suspended,
    Deleting,
}

/// Creation/update metadata owned by the control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceTimestamps {
    created_at_unix: u64,
    updated_at_unix: u64,
}

impl ResourceTimestamps {
    pub fn new(
        created_at_unix: u64,
        updated_at_unix: u64,
    ) -> Result<Self, ControlPlaneDomainError> {
        if updated_at_unix < created_at_unix {
            return Err(ControlPlaneDomainError::UpdatedBeforeCreated);
        }
        Ok(Self {
            created_at_unix,
            updated_at_unix,
        })
    }

    #[must_use]
    pub fn created_at_unix(&self) -> u64 {
        self.created_at_unix
    }

    #[must_use]
    pub fn updated_at_unix(&self) -> u64 {
        self.updated_at_unix
    }
}

/// Top-level Ketebe customer/account ownership resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Organization {
    id: OrganizationId,
    name: String,
    slug: String,
    lifecycle: ResourceLifecycleState,
    timestamps: ResourceTimestamps,
}

impl Organization {
    pub fn new(
        id: OrganizationId,
        name: impl Into<String>,
        slug: impl Into<String>,
        lifecycle: ResourceLifecycleState,
        timestamps: ResourceTimestamps,
    ) -> Result<Self, ControlPlaneDomainError> {
        let name = normalize_name(name.into())?;
        let slug = slug.into();
        validate_identifier("organization slug", &slug, MAX_RESOURCE_SLUG_LEN)?;
        Ok(Self {
            id,
            name,
            slug,
            lifecycle,
            timestamps,
        })
    }

    #[must_use]
    pub fn id(&self) -> &OrganizationId {
        &self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn slug(&self) -> &str {
        &self.slug
    }

    #[must_use]
    pub fn lifecycle(&self) -> ResourceLifecycleState {
        self.lifecycle
    }

    #[must_use]
    pub fn timestamps(&self) -> ResourceTimestamps {
        self.timestamps
    }
}

/// First-class workload/team/application isolation resource inside one Organization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Project {
    id: ProjectId,
    organization_id: OrganizationId,
    name: String,
    slug: String,
    lifecycle: ResourceLifecycleState,
    timestamps: ResourceTimestamps,
}

impl Project {
    pub fn new(
        id: ProjectId,
        organization_id: OrganizationId,
        name: impl Into<String>,
        slug: impl Into<String>,
        lifecycle: ResourceLifecycleState,
        timestamps: ResourceTimestamps,
    ) -> Result<Self, ControlPlaneDomainError> {
        let name = normalize_name(name.into())?;
        let slug = slug.into();
        validate_identifier("project slug", &slug, MAX_RESOURCE_SLUG_LEN)?;
        Ok(Self {
            id,
            organization_id,
            name,
            slug,
            lifecycle,
            timestamps,
        })
    }

    #[must_use]
    pub fn id(&self) -> &ProjectId {
        &self.id
    }

    #[must_use]
    pub fn organization_id(&self) -> &OrganizationId {
        &self.organization_id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn slug(&self) -> &str {
        &self.slug
    }

    #[must_use]
    pub fn lifecycle(&self) -> ResourceLifecycleState {
        self.lifecycle
    }

    #[must_use]
    pub fn timestamps(&self) -> ResourceTimestamps {
        self.timestamps
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneDomainError {
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
    UpdatedBeforeCreated,
}

impl fmt::Display for ControlPlaneDomainError {
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
            Self::UpdatedBeforeCreated => {
                formatter.write_str("updated timestamp must not precede created timestamp")
            }
        }
    }
}

impl std::error::Error for ControlPlaneDomainError {}

fn normalize_name(value: String) -> Result<String, ControlPlaneDomainError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ControlPlaneDomainError::Empty("resource name"));
    }
    if trimmed.len() > MAX_RESOURCE_NAME_LEN {
        return Err(ControlPlaneDomainError::TooLong {
            kind: "resource name",
            max: MAX_RESOURCE_NAME_LEN,
            actual: trimmed.len(),
        });
    }
    Ok(trimmed.to_string())
}

fn validate_identifier(
    kind: &'static str,
    value: &str,
    max: usize,
) -> Result<(), ControlPlaneDomainError> {
    if value.is_empty() {
        return Err(ControlPlaneDomainError::Empty(kind));
    }
    if value.len() > max {
        return Err(ControlPlaneDomainError::TooLong {
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
            return Err(ControlPlaneDomainError::InvalidCharacter {
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

    fn timestamps() -> ResourceTimestamps {
        ResourceTimestamps::new(10, 20).unwrap()
    }

    #[test]
    fn organization_uses_stable_identity_separate_from_name_and_slug() {
        let organization = Organization::new(
            OrganizationId::new("org-01").unwrap(),
            "ACME Corporation",
            "acme",
            ResourceLifecycleState::Active,
            timestamps(),
        )
        .unwrap();

        assert_eq!(organization.id().as_str(), "org-01");
        assert_eq!(organization.name(), "ACME Corporation");
        assert_eq!(organization.slug(), "acme");
    }

    #[test]
    fn project_has_exactly_one_immutable_organization_owner() {
        let project = Project::new(
            ProjectId::new("project-01").unwrap(),
            OrganizationId::new("org-01").unwrap(),
            "Product Search",
            "product-search",
            ResourceLifecycleState::Active,
            timestamps(),
        )
        .unwrap();

        assert_eq!(project.id().as_str(), "project-01");
        assert_eq!(project.organization_id().as_str(), "org-01");
    }

    #[test]
    fn same_project_slug_can_be_represented_under_distinct_organizations() {
        let a = Project::new(
            ProjectId::new("project-a").unwrap(),
            OrganizationId::new("org-a").unwrap(),
            "Search",
            "search",
            ResourceLifecycleState::Active,
            timestamps(),
        )
        .unwrap();
        let b = Project::new(
            ProjectId::new("project-b").unwrap(),
            OrganizationId::new("org-b").unwrap(),
            "Search",
            "search",
            ResourceLifecycleState::Active,
            timestamps(),
        )
        .unwrap();

        assert_ne!(a.id(), b.id());
        assert_ne!(a.organization_id(), b.organization_id());
        assert_eq!(a.slug(), b.slug());
    }

    #[test]
    fn display_names_are_trimmed_but_identifiers_remain_strict() {
        let organization = Organization::new(
            OrganizationId::new("org-a").unwrap(),
            "  ACME  ",
            "acme",
            ResourceLifecycleState::Active,
            timestamps(),
        )
        .unwrap();
        assert_eq!(organization.name(), "ACME");

        assert!(matches!(
            OrganizationId::new("ACME"),
            Err(ControlPlaneDomainError::InvalidCharacter { .. })
        ));
        assert!(matches!(
            Organization::new(
                OrganizationId::new("org-a").unwrap(),
                "ACME",
                "ACME",
                ResourceLifecycleState::Active,
                timestamps(),
            ),
            Err(ControlPlaneDomainError::InvalidCharacter { .. })
        ));
    }

    #[test]
    fn timestamps_reject_updates_before_creation() {
        assert_eq!(
            ResourceTimestamps::new(20, 10),
            Err(ControlPlaneDomainError::UpdatedBeforeCreated)
        );
    }

    #[test]
    fn default_organization_identity_is_deterministic() {
        assert_eq!(OrganizationId::default_organization().as_str(), "default");
    }
}
