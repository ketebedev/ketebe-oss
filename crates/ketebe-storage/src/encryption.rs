use ketebe_core::{DataEncryptionOwnership, DataEncryptionPolicy, DataPlaneScope};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageEncryptionArtifact {
    Wal,
    Segment,
    Checkpoint,
    HnswIndex,
    LexicalIndex,
}

impl StorageEncryptionArtifact {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Wal => "wal",
            Self::Segment => "segment",
            Self::Checkpoint => "checkpoint",
            Self::HnswIndex => "hnsw_index",
            Self::LexicalIndex => "lexical_index",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageEncryptionContext {
    scope: DataPlaneScope,
    policy: Option<DataEncryptionPolicy>,
}

impl StorageEncryptionContext {
    #[must_use]
    pub const fn unencrypted(scope: DataPlaneScope) -> Self {
        Self {
            scope,
            policy: None,
        }
    }

    #[must_use]
    pub const fn encrypted(scope: DataPlaneScope, policy: DataEncryptionPolicy) -> Self {
        Self {
            scope,
            policy: Some(policy),
        }
    }

    #[must_use]
    pub const fn scope(&self) -> &DataPlaneScope {
        &self.scope
    }

    #[must_use]
    pub const fn policy(&self) -> Option<&DataEncryptionPolicy> {
        self.policy.as_ref()
    }

    #[must_use]
    pub fn ownership_for(
        &self,
        _artifact: StorageEncryptionArtifact,
    ) -> Option<DataEncryptionOwnership> {
        self.policy
            .clone()
            .map(|policy| DataEncryptionOwnership::new(self.scope.clone(), policy))
    }
}

pub trait StorageEncryptionPolicyProvider: Send + Sync {
    fn context_for(&self, scope: &DataPlaneScope) -> StorageEncryptionContext;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UnencryptedStoragePolicy;

impl StorageEncryptionPolicyProvider for UnencryptedStoragePolicy {
    fn context_for(&self, scope: &DataPlaneScope) -> StorageEncryptionContext {
        StorageEncryptionContext::unencrypted(scope.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ketebe_core::{CollectionId, DataEncryptionKeyRef, DataEncryptionKeyVersion, ProjectId};

    fn scope() -> DataPlaneScope {
        DataPlaneScope::new(
            ProjectId::new("p_a").expect("project"),
            CollectionId::new("c_docs").expect("collection"),
        )
    }

    fn policy() -> DataEncryptionPolicy {
        DataEncryptionPolicy::new(
            DataEncryptionKeyRef::new("local/key-a").expect("key ref"),
            DataEncryptionKeyVersion::new(3).expect("version"),
        )
    }

    #[test]
    fn every_persistent_artifact_uses_the_same_scoped_key_ownership() {
        let scope = scope();
        let context = StorageEncryptionContext::encrypted(scope.clone(), policy());
        for artifact in [
            StorageEncryptionArtifact::Wal,
            StorageEncryptionArtifact::Segment,
            StorageEncryptionArtifact::Checkpoint,
            StorageEncryptionArtifact::HnswIndex,
            StorageEncryptionArtifact::LexicalIndex,
        ] {
            let ownership = context
                .ownership_for(artifact)
                .expect("encrypted artifact ownership");
            assert_eq!(ownership.scope(), &scope);
            assert_eq!(ownership.policy().key_version().get(), 3);
        }
    }

    #[test]
    fn local_default_does_not_require_encryption_key_material() {
        let context = UnencryptedStoragePolicy.context_for(&scope());
        assert!(context.policy().is_none());
        assert!(
            context
                .ownership_for(StorageEncryptionArtifact::Wal)
                .is_none()
        );
    }
}
