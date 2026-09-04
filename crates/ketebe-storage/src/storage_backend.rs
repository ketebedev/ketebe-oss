use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentLocation(String);

impl SegmentLocation {
    pub fn new(key: impl Into<String>) -> Result<Self, StorageBackendError> {
        let key = key.into();
        let path = Path::new(&key);
        if key.is_empty()
            || path.is_absolute()
            || path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(StorageBackendError::InvalidLocation(key));
        }
        Ok(Self(key))
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn uri(&self) -> String {
        format!("segment://{}", self.0)
    }
}

impl fmt::Display for SegmentLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.uri())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendCapabilities {
    pub range_read: bool,
    pub atomic_publish: bool,
    pub list: bool,
    pub delete: bool,
}

impl BackendCapabilities {
    #[must_use]
    pub const fn local_filesystem() -> Self {
        Self {
            range_read: true,
            atomic_publish: true,
            list: true,
            delete: true,
        }
    }

    pub fn validate_segment_store(self) -> Result<(), StorageBackendError> {
        if !self.atomic_publish {
            return Err(StorageBackendError::UnsupportedCapability("atomic_publish"));
        }
        if !self.list {
            return Err(StorageBackendError::UnsupportedCapability("list"));
        }
        if !self.delete {
            return Err(StorageBackendError::UnsupportedCapability("delete"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum StorageBackendError {
    Io(std::io::Error),
    InvalidLocation(String),
    UnsupportedCapability(&'static str),
    AlreadyExists(SegmentLocation),
}

impl fmt::Display for StorageBackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "storage backend I/O error: {error}"),
            Self::InvalidLocation(value) => write!(f, "invalid segment location: {value}"),
            Self::UnsupportedCapability(value) => write!(
                f,
                "storage backend does not support required capability: {value}"
            ),
            Self::AlreadyExists(location) => write!(f, "storage object already exists: {location}"),
        }
    }
}
impl std::error::Error for StorageBackendError {}
impl From<std::io::Error> for StorageBackendError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub trait StorageBackend: Send + Sync {
    fn capabilities(&self) -> BackendCapabilities;
    fn read(&self, location: &SegmentLocation) -> Result<Vec<u8>, StorageBackendError>;
    fn read_range(
        &self,
        location: &SegmentLocation,
        range: Range<u64>,
    ) -> Result<Vec<u8>, StorageBackendError>;
    fn exists(&self, location: &SegmentLocation) -> Result<bool, StorageBackendError>;
    fn list(&self) -> Result<Vec<SegmentLocation>, StorageBackendError>;
    fn publish_atomic(
        &self,
        location: &SegmentLocation,
        bytes: &[u8],
    ) -> Result<(), StorageBackendError>;
    fn delete(&self, location: &SegmentLocation) -> Result<bool, StorageBackendError>;
}

#[derive(Debug, Clone)]
pub struct LocalFilesystemBackend {
    root: PathBuf,
}

impl LocalFilesystemBackend {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StorageBackendError> {
        fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
        })
    }

    fn resolve(&self, location: &SegmentLocation) -> PathBuf {
        self.root.join(location.key())
    }

    fn sync_root(&self) -> Result<(), StorageBackendError> {
        #[cfg(unix)]
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

impl StorageBackend for LocalFilesystemBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::local_filesystem()
    }

    fn read(&self, location: &SegmentLocation) -> Result<Vec<u8>, StorageBackendError> {
        Ok(fs::read(self.resolve(location))?)
    }

    fn read_range(
        &self,
        location: &SegmentLocation,
        range: Range<u64>,
    ) -> Result<Vec<u8>, StorageBackendError> {
        if range.end < range.start {
            return Err(StorageBackendError::InvalidLocation(format!(
                "invalid byte range {}..{}",
                range.start, range.end
            )));
        }
        let mut file = File::open(self.resolve(location))?;
        file.seek(SeekFrom::Start(range.start))?;
        let mut bytes = Vec::new();
        file.take(range.end - range.start).read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn exists(&self, location: &SegmentLocation) -> Result<bool, StorageBackendError> {
        Ok(self.resolve(location).exists())
    }

    fn list(&self) -> Result<Vec<SegmentLocation>, StorageBackendError> {
        let mut locations = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str() {
                locations.push(SegmentLocation::new(name.to_string())?);
            }
        }
        locations.sort();
        Ok(locations)
    }

    fn publish_atomic(
        &self,
        location: &SegmentLocation,
        bytes: &[u8],
    ) -> Result<(), StorageBackendError> {
        let final_path = self.resolve(location);
        if final_path.exists() {
            return Err(StorageBackendError::AlreadyExists(location.clone()));
        }
        let temp_path = self.root.join(format!("{}.tmp", location.key()));
        if temp_path.exists() {
            fs::remove_file(&temp_path)?;
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_data()?;
        drop(file);
        fs::rename(&temp_path, &final_path)?;
        self.sync_root()?;
        Ok(())
    }

    fn delete(&self, location: &SegmentLocation) -> Result<bool, StorageBackendError> {
        let path = self.resolve(location);
        if !path.exists() {
            return Ok(false);
        }
        fs::remove_file(path)?;
        self.sync_root()?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "ketebe-storage-backend-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn local_backend_supports_atomic_publish_list_range_and_delete() {
        let dir = temp_dir();
        let backend = LocalFilesystemBackend::open(&dir).unwrap();
        backend.capabilities().validate_segment_store().unwrap();
        let location = SegmentLocation::new("00000000000000000001.kseg").unwrap();
        backend.publish_atomic(&location, b"abcdef").unwrap();
        assert!(backend.exists(&location).unwrap());
        assert_eq!(backend.read(&location).unwrap(), b"abcdef");
        assert_eq!(backend.read_range(&location, 2..5).unwrap(), b"cde");
        assert_eq!(backend.list().unwrap(), vec![location.clone()]);
        assert!(backend.delete(&location).unwrap());
        assert!(!backend.exists(&location).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn location_rejects_parent_traversal() {
        assert!(SegmentLocation::new("../escape.kseg").is_err());
    }
}
