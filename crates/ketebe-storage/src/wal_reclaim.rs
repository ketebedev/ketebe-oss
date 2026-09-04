use crate::{Wal, WalError, WalMutation};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

pub fn reclaim_wal(
    wal_path: impl AsRef<Path>,
    retained: &[WalMutation],
) -> Result<(), WalReclaimError> {
    let wal_path = wal_path.as_ref();
    let parent = wal_path.parent().ok_or(WalReclaimError::InvalidWalPath)?;
    fs::create_dir_all(parent)?;

    let temp_path = temp_path(wal_path);
    if temp_path.exists() {
        fs::remove_file(&temp_path)?;
    }

    {
        let mut replacement = Wal::open(&temp_path)?;
        for mutation in retained {
            replacement.append(mutation)?;
        }
    }

    fs::rename(&temp_path, wal_path)?;
    sync_directory(parent)?;
    Ok(())
}

fn temp_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".reclaim.tmp");
    PathBuf::from(value)
}

fn sync_directory(path: &Path) -> Result<(), WalReclaimError> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

#[derive(Debug)]
pub enum WalReclaimError {
    Io(std::io::Error),
    Wal(WalError),
    InvalidWalPath,
}

impl std::fmt::Display for WalReclaimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "WAL reclamation I/O error: {error}"),
            Self::Wal(error) => write!(f, "WAL reclamation error: {error}"),
            Self::InvalidWalPath => f.write_str("WAL path has no parent directory"),
        }
    }
}

impl std::error::Error for WalReclaimError {}

impl From<std::io::Error> for WalReclaimError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<WalError> for WalReclaimError {
    fn from(value: WalError) -> Self {
        Self::Wal(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ketebe_core::{CollectionId, RecordId, SequenceNumber};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "ketebe-wal-reclaim-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    #[test]
    fn reclaim_keeps_only_supplied_entries() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("wal.log");
        let collection_id = CollectionId::new("docs").expect("collection");
        let entries = vec![
            WalMutation::Delete {
                collection_id: collection_id.clone(),
                record_id: RecordId::unsigned(1),
                sequence_number: SequenceNumber::new(3),
            },
            WalMutation::Delete {
                collection_id,
                record_id: RecordId::unsigned(2),
                sequence_number: SequenceNumber::new(4),
            },
        ];
        reclaim_wal(&path, &entries).expect("reclaim");
        assert_eq!(
            Wal::open(&path)
                .expect("wal")
                .replay()
                .expect("replay")
                .entries,
            entries
        );
        fs::remove_dir_all(dir).expect("cleanup");
    }
}
