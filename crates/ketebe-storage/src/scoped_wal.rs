use crate::{NamespaceError, ReplayResult, ScopedStorageNamespace, Wal, WalError, WalMutation};
use ketebe_core::DataPlaneScope;
use std::fmt;
use std::path::Path;

/// WAL access bound to one validated project+collection namespace.
pub struct ScopedWal {
    namespace: ScopedStorageNamespace,
    wal: Wal,
}

impl ScopedWal {
    pub fn open(data_dir: impl AsRef<Path>, scope: DataPlaneScope) -> Result<Self, ScopedWalError> {
        let namespace = ScopedStorageNamespace::open(data_dir, scope)?;
        let wal = Wal::open(namespace.wal_path())?;
        let scoped = Self { namespace, wal };
        scoped.validate_replay()?;
        Ok(scoped)
    }

    pub fn open_existing(
        data_dir: impl AsRef<Path>,
        scope: DataPlaneScope,
    ) -> Result<Self, ScopedWalError> {
        let namespace = ScopedStorageNamespace::open_existing(data_dir, scope)?;
        let wal = Wal::open(namespace.wal_path())?;
        let scoped = Self { namespace, wal };
        scoped.validate_replay()?;
        Ok(scoped)
    }

    #[must_use]
    pub fn scope(&self) -> &DataPlaneScope {
        self.namespace.scope()
    }

    pub fn append(&mut self, mutation: &WalMutation) -> Result<(), ScopedWalError> {
        validate_mutation(self.scope(), mutation)?;
        self.wal.append(mutation)?;
        Ok(())
    }

    pub fn append_batch(&mut self, mutations: &[WalMutation]) -> Result<(), ScopedWalError> {
        for mutation in mutations {
            validate_mutation(self.scope(), mutation)?;
        }
        self.wal.append_batch(mutations)?;
        Ok(())
    }

    pub fn replay(&self) -> Result<ReplayResult, ScopedWalError> {
        let replay = self.wal.replay()?;
        for mutation in &replay.entries {
            validate_mutation(self.scope(), mutation)?;
        }
        Ok(replay)
    }

    fn validate_replay(&self) -> Result<(), ScopedWalError> {
        self.replay().map(|_| ())
    }
}

fn validate_mutation(scope: &DataPlaneScope, mutation: &WalMutation) -> Result<(), ScopedWalError> {
    let collection_id = match mutation {
        WalMutation::Upsert { collection_id, .. } | WalMutation::Delete { collection_id, .. } => {
            collection_id
        }
    };
    if collection_id != scope.collection_id() {
        return Err(ScopedWalError::CollectionMismatch {
            expected: scope.collection_id().as_str().to_string(),
            actual: collection_id.as_str().to_string(),
        });
    }
    Ok(())
}

#[derive(Debug)]
pub enum ScopedWalError {
    Namespace(NamespaceError),
    Wal(WalError),
    CollectionMismatch { expected: String, actual: String },
}

impl fmt::Display for ScopedWalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Namespace(error) => error.fmt(formatter),
            Self::Wal(error) => error.fmt(formatter),
            Self::CollectionMismatch { expected, actual } => write!(
                formatter,
                "WAL mutation collection scope mismatch: expected {expected}, found {actual}"
            ),
        }
    }
}

impl std::error::Error for ScopedWalError {}

impl From<NamespaceError> for ScopedWalError {
    fn from(error: NamespaceError) -> Self {
        Self::Namespace(error)
    }
}

impl From<WalError> for ScopedWalError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ketebe_core::{CollectionId, ProjectId, Record, RecordId, SequenceNumber, Vector};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn scope(project: &str, collection: &str) -> DataPlaneScope {
        DataPlaneScope::new(
            ProjectId::new(project).unwrap(),
            CollectionId::new(collection).unwrap(),
        )
    }

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ketebe-{label}-{}-{nonce}", std::process::id()))
    }

    fn upsert(collection: &str, sequence: u64, marker: f32) -> WalMutation {
        WalMutation::Upsert {
            collection_id: CollectionId::new(collection).unwrap(),
            record: Record::new(
                RecordId::string("same-record").unwrap(),
                Vector::new(vec![marker, 1.0]).unwrap(),
                Default::default(),
                SequenceNumber::new(sequence),
            ),
        }
    }

    #[test]
    fn same_collection_name_and_record_id_are_independent_across_projects() {
        let root = temp_root("scoped-wal-record");
        let a = scope("project-a", "documents");
        let b = scope("project-b", "documents");
        let mut wal_a = ScopedWal::open(&root, a.clone()).unwrap();
        let mut wal_b = ScopedWal::open(&root, b.clone()).unwrap();
        wal_a.append(&upsert("documents", 1, 1.0)).unwrap();
        wal_b.append(&upsert("documents", 1, 2.0)).unwrap();
        let a_replay = wal_a.replay().unwrap();
        let b_replay = wal_b.replay().unwrap();
        let a_record = match &a_replay.entries[0] {
            WalMutation::Upsert { record, .. } => record,
            WalMutation::Delete { .. } => unreachable!(),
        };
        let b_record = match &b_replay.entries[0] {
            WalMutation::Upsert { record, .. } => record,
            WalMutation::Delete { .. } => unreachable!(),
        };
        assert_eq!(a_record.id(), b_record.id());
        assert_ne!(a_record.vector(), b_record.vector());
        assert_ne!(
            ScopedStorageNamespace::open_existing(&root, a)
                .unwrap()
                .wal_path(),
            ScopedStorageNamespace::open_existing(&root, b)
                .unwrap()
                .wal_path()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cross_scope_wal_injection_fails_closed_on_reopen() {
        let root = temp_root("scoped-wal-injection");
        let a = scope("project-a", "collection-a");
        let b = scope("project-b", "collection-b");
        let mut wal_b = ScopedWal::open(&root, b.clone()).unwrap();
        wal_b.append(&upsert("collection-b", 1, 1.0)).unwrap();
        drop(wal_b);

        let a_namespace = ScopedStorageNamespace::open(&root, a.clone()).unwrap();
        let b_namespace = ScopedStorageNamespace::open_existing(&root, b).unwrap();
        fs::copy(b_namespace.wal_path(), a_namespace.wal_path()).unwrap();

        let error = match ScopedWal::open_existing(&root, a) {
            Ok(_) => panic!("cross-scope WAL injection must fail"),
            Err(error) => error,
        };
        assert!(matches!(error, ScopedWalError::CollectionMismatch { .. }));
        fs::remove_dir_all(root).unwrap();
    }
}
