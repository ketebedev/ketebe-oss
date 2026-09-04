use crate::DataPlaneScope;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataEncryptionKeyRef(String);

impl DataEncryptionKeyRef {
    pub fn new(value: impl Into<String>) -> Result<Self, DataEncryptionError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DataEncryptionError::InvalidKeyReference);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DataEncryptionKeyRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataEncryptionKeyVersion(u32);

impl DataEncryptionKeyVersion {
    pub fn new(value: u32) -> Result<Self, DataEncryptionError> {
        if value == 0 {
            return Err(DataEncryptionError::InvalidKeyVersion);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    pub fn next(self) -> Result<Self, DataEncryptionError> {
        self.0
            .checked_add(1)
            .ok_or(DataEncryptionError::InvalidKeyVersion)
            .and_then(Self::new)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataEncryptionPolicy {
    key_ref: DataEncryptionKeyRef,
    key_version: DataEncryptionKeyVersion,
}

impl DataEncryptionPolicy {
    #[must_use]
    pub const fn new(key_ref: DataEncryptionKeyRef, key_version: DataEncryptionKeyVersion) -> Self {
        Self {
            key_ref,
            key_version,
        }
    }

    #[must_use]
    pub const fn key_ref(&self) -> &DataEncryptionKeyRef {
        &self.key_ref
    }

    #[must_use]
    pub const fn key_version(&self) -> DataEncryptionKeyVersion {
        self.key_version
    }

    pub fn rotate_to(&self, key_ref: DataEncryptionKeyRef) -> Result<Self, DataEncryptionError> {
        Ok(Self::new(key_ref, self.key_version.next()?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataEncryptionOwnership {
    scope: DataPlaneScope,
    policy: DataEncryptionPolicy,
}

impl DataEncryptionOwnership {
    #[must_use]
    pub const fn new(scope: DataPlaneScope, policy: DataEncryptionPolicy) -> Self {
        Self { scope, policy }
    }

    #[must_use]
    pub const fn scope(&self) -> &DataPlaneScope {
        &self.scope
    }

    #[must_use]
    pub const fn policy(&self) -> &DataEncryptionPolicy {
        &self.policy
    }

    pub fn validate_scope(&self, scope: &DataPlaneScope) -> Result<(), DataEncryptionError> {
        if &self.scope == scope {
            Ok(())
        } else {
            Err(DataEncryptionError::OwnershipMismatch)
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedDataEncryptionKey {
    ownership: DataEncryptionOwnership,
    material: Arc<[u8]>,
}

impl ResolvedDataEncryptionKey {
    pub fn new(
        ownership: DataEncryptionOwnership,
        material: impl Into<Vec<u8>>,
    ) -> Result<Self, DataEncryptionError> {
        let material = material.into();
        if material.is_empty() {
            return Err(DataEncryptionError::InvalidKeyMaterial);
        }
        Ok(Self {
            ownership,
            material: Arc::<[u8]>::from(material),
        })
    }

    #[must_use]
    pub const fn ownership(&self) -> &DataEncryptionOwnership {
        &self.ownership
    }

    #[must_use]
    pub fn expose_material(&self) -> &[u8] {
        &self.material
    }
}

impl fmt::Debug for ResolvedDataEncryptionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedDataEncryptionKey")
            .field("ownership", &self.ownership)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataEncryptionError {
    InvalidKeyReference,
    InvalidKeyVersion,
    InvalidKeyMaterial,
    MissingKey,
    RevokedKey,
    OwnershipMismatch,
    LockPoisoned,
}

impl fmt::Display for DataEncryptionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKeyReference => {
                formatter.write_str("data-encryption key reference is invalid")
            }
            Self::InvalidKeyVersion => {
                formatter.write_str("data-encryption key version is invalid")
            }
            Self::InvalidKeyMaterial => {
                formatter.write_str("data-encryption key material is invalid")
            }
            Self::MissingKey => formatter.write_str("data-encryption key is unavailable"),
            Self::RevokedKey => formatter.write_str("data-encryption key is revoked"),
            Self::OwnershipMismatch => {
                formatter.write_str("data-encryption key ownership does not match data-plane scope")
            }
            Self::LockPoisoned => formatter.write_str("data-encryption key resolver lock poisoned"),
        }
    }
}

impl std::error::Error for DataEncryptionError {}

pub trait DataEncryptionKeyResolver: Send + Sync {
    fn resolve(
        &self,
        ownership: &DataEncryptionOwnership,
    ) -> Result<ResolvedDataEncryptionKey, DataEncryptionError>;
}

#[derive(Clone)]
struct LocalKeyEntry {
    ownership: DataEncryptionOwnership,
    material: Arc<[u8]>,
    revoked: bool,
}

#[derive(Clone, Default)]
pub struct LocalDataEncryptionKeyResolver {
    keys: Arc<Mutex<BTreeMap<(String, u32), LocalKeyEntry>>>,
}

impl LocalDataEncryptionKeyResolver {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &self,
        ownership: DataEncryptionOwnership,
        material: impl Into<Vec<u8>>,
    ) -> Result<(), DataEncryptionError> {
        let material = material.into();
        if material.is_empty() {
            return Err(DataEncryptionError::InvalidKeyMaterial);
        }
        let key = (
            ownership.policy().key_ref().as_str().to_string(),
            ownership.policy().key_version().get(),
        );
        self.keys
            .lock()
            .map_err(|_| DataEncryptionError::LockPoisoned)?
            .insert(
                key,
                LocalKeyEntry {
                    ownership,
                    material: Arc::<[u8]>::from(material),
                    revoked: false,
                },
            );
        Ok(())
    }

    pub fn revoke(
        &self,
        key_ref: &DataEncryptionKeyRef,
        version: DataEncryptionKeyVersion,
    ) -> Result<(), DataEncryptionError> {
        let mut keys = self
            .keys
            .lock()
            .map_err(|_| DataEncryptionError::LockPoisoned)?;
        let entry = keys
            .get_mut(&(key_ref.as_str().to_string(), version.get()))
            .ok_or(DataEncryptionError::MissingKey)?;
        entry.revoked = true;
        Ok(())
    }
}

impl DataEncryptionKeyResolver for LocalDataEncryptionKeyResolver {
    fn resolve(
        &self,
        ownership: &DataEncryptionOwnership,
    ) -> Result<ResolvedDataEncryptionKey, DataEncryptionError> {
        let key = (
            ownership.policy().key_ref().as_str().to_string(),
            ownership.policy().key_version().get(),
        );
        let keys = self
            .keys
            .lock()
            .map_err(|_| DataEncryptionError::LockPoisoned)?;
        let entry = keys.get(&key).ok_or(DataEncryptionError::MissingKey)?;
        if entry.revoked {
            return Err(DataEncryptionError::RevokedKey);
        }
        if &entry.ownership != ownership {
            return Err(DataEncryptionError::OwnershipMismatch);
        }
        ResolvedDataEncryptionKey::new(ownership.clone(), entry.material.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CollectionId, ProjectId};

    fn scope(project: &str, collection: &str) -> DataPlaneScope {
        DataPlaneScope::new(
            ProjectId::new(project).expect("project"),
            CollectionId::new(collection).expect("collection"),
        )
    }

    fn ownership(project: &str, collection: &str) -> DataEncryptionOwnership {
        DataEncryptionOwnership::new(
            scope(project, collection),
            DataEncryptionPolicy::new(
                DataEncryptionKeyRef::new("local/key-a").expect("key ref"),
                DataEncryptionKeyVersion::new(1).expect("version"),
            ),
        )
    }

    #[test]
    fn local_resolver_is_bound_to_exact_project_collection_scope() {
        let resolver = LocalDataEncryptionKeyResolver::new();
        let owner = ownership("p_a", "c_docs");
        resolver
            .insert(owner.clone(), b"local-test-key-material".to_vec())
            .expect("insert");

        assert!(resolver.resolve(&owner).is_ok());

        let wrong_project = ownership("p_b", "c_docs");
        assert_eq!(
            resolver.resolve(&wrong_project),
            Err(DataEncryptionError::OwnershipMismatch)
        );
    }

    #[test]
    fn missing_and_revoked_keys_fail_closed() {
        let resolver = LocalDataEncryptionKeyResolver::new();
        let owner = ownership("p_a", "c_docs");
        assert_eq!(
            resolver.resolve(&owner),
            Err(DataEncryptionError::MissingKey)
        );

        resolver
            .insert(owner.clone(), b"local-test-key-material".to_vec())
            .expect("insert");
        resolver
            .revoke(owner.policy().key_ref(), owner.policy().key_version())
            .expect("revoke");
        assert_eq!(
            resolver.resolve(&owner),
            Err(DataEncryptionError::RevokedKey)
        );
    }

    #[test]
    fn key_material_is_redacted_from_debug_output() {
        let owner = ownership("p_a", "c_docs");
        let key = ResolvedDataEncryptionKey::new(owner, b"plaintext-must-never-appear".to_vec())
            .expect("key");
        let debug = format!("{key:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("plaintext-must-never-appear"));
    }

    #[test]
    fn rotation_advances_version_without_changing_storage_identity() {
        let owner = ownership("p_a", "c_docs");
        let rotated = owner
            .policy()
            .rotate_to(DataEncryptionKeyRef::new("local/key-b").expect("key ref"))
            .expect("rotate");
        assert_eq!(rotated.key_version().get(), 2);
        assert_eq!(owner.scope(), &scope("p_a", "c_docs"));
    }
}
