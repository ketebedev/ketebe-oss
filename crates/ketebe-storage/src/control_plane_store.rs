use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ketebe_core::{
    ControlPlaneDomainError, Organization, OrganizationId, Project, ProjectId,
    ResourceLifecycleState, ResourceTimestamps,
};

const MAGIC: [u8; 4] = *b"KTCP";
const VERSION: u8 = 1;
const FILE_NAME: &str = "control-plane.ktcp";
const TEMP_FILE_NAME: &str = "control-plane.ktcp.tmp";
const HEADER_LEN: usize = 20;

#[derive(Debug, Clone, Default)]
struct ControlPlaneState {
    organizations: BTreeMap<String, Organization>,
    projects: BTreeMap<String, Project>,
}

#[derive(Debug, Clone)]
pub struct ControlPlaneStore {
    directory: Arc<PathBuf>,
    state: Arc<Mutex<ControlPlaneState>>,
}

impl ControlPlaneStore {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, ControlPlaneStoreError> {
        fs::create_dir_all(directory.as_ref())?;
        let directory = directory.as_ref().to_path_buf();
        let path = directory.join(FILE_NAME);
        let state = if path.exists() {
            let mut bytes = Vec::new();
            File::open(&path)?.read_to_end(&mut bytes)?;
            decode_state(&bytes)?
        } else {
            ControlPlaneState::default()
        };
        Ok(Self {
            directory: Arc::new(directory),
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub fn bootstrap_default(&self, created_at_unix: u64) -> Result<bool, ControlPlaneStoreError> {
        let mut guard = self.lock()?;
        let organization_id = OrganizationId::default_organization();
        let project_id = ProjectId::default_project();
        let existing_organization = guard.organizations.get(organization_id.as_str());
        let existing_project = guard.projects.get(project_id.as_str());

        match (existing_organization, existing_project) {
            (Some(organization), Some(project)) => {
                if organization.slug() != "default"
                    || project.organization_id() != &organization_id
                    || project.slug() != "default"
                {
                    return Err(ControlPlaneStoreError::InconsistentDefaultBootstrap);
                }
                Ok(false)
            }
            (None, None) => {
                let timestamps = ResourceTimestamps::new(created_at_unix, created_at_unix)?;
                let organization = Organization::new(
                    organization_id.clone(),
                    "Default",
                    "default",
                    ResourceLifecycleState::Active,
                    timestamps,
                )?;
                let project = Project::new(
                    project_id,
                    organization_id,
                    "Default",
                    "default",
                    ResourceLifecycleState::Active,
                    timestamps,
                )?;
                let mut next = guard.clone();
                next.organizations
                    .insert(organization.id().as_str().to_string(), organization);
                next.projects
                    .insert(project.id().as_str().to_string(), project);
                self.persist(&next)?;
                *guard = next;
                Ok(true)
            }
            _ => Err(ControlPlaneStoreError::InconsistentDefaultBootstrap),
        }
    }

    pub fn create_organization(
        &self,
        organization: Organization,
    ) -> Result<(), ControlPlaneStoreError> {
        let mut guard = self.lock()?;
        if guard.organizations.contains_key(organization.id().as_str()) {
            return Err(ControlPlaneStoreError::OrganizationAlreadyExists(
                organization.id().clone(),
            ));
        }
        ensure_organization_slug_unique(&guard, &organization, None)?;

        let mut next = guard.clone();
        next.organizations
            .insert(organization.id().as_str().to_string(), organization);
        self.persist(&next)?;
        *guard = next;
        Ok(())
    }

    pub fn update_organization(
        &self,
        organization: Organization,
    ) -> Result<(), ControlPlaneStoreError> {
        let mut guard = self.lock()?;
        if !guard.organizations.contains_key(organization.id().as_str()) {
            return Err(ControlPlaneStoreError::OrganizationNotFound(
                organization.id().clone(),
            ));
        }
        ensure_organization_slug_unique(&guard, &organization, Some(organization.id().as_str()))?;

        let mut next = guard.clone();
        next.organizations
            .insert(organization.id().as_str().to_string(), organization);
        self.persist(&next)?;
        *guard = next;
        Ok(())
    }

    pub fn create_project(&self, project: Project) -> Result<(), ControlPlaneStoreError> {
        let mut guard = self.lock()?;
        if guard.projects.contains_key(project.id().as_str()) {
            return Err(ControlPlaneStoreError::ProjectAlreadyExists(
                project.id().clone(),
            ));
        }
        ensure_project_parent_exists(&guard, &project)?;
        ensure_project_slug_unique(&guard, &project, None)?;

        let mut next = guard.clone();
        next.projects
            .insert(project.id().as_str().to_string(), project);
        self.persist(&next)?;
        *guard = next;
        Ok(())
    }

    pub fn update_project(&self, project: Project) -> Result<(), ControlPlaneStoreError> {
        let mut guard = self.lock()?;
        let existing = guard
            .projects
            .get(project.id().as_str())
            .ok_or_else(|| ControlPlaneStoreError::ProjectNotFound(project.id().clone()))?;
        if existing.organization_id() != project.organization_id() {
            return Err(ControlPlaneStoreError::ProjectOwnershipImmutable {
                project_id: project.id().clone(),
                expected: existing.organization_id().clone(),
                actual: project.organization_id().clone(),
            });
        }
        ensure_project_parent_exists(&guard, &project)?;
        ensure_project_slug_unique(&guard, &project, Some(project.id().as_str()))?;

        let mut next = guard.clone();
        next.projects
            .insert(project.id().as_str().to_string(), project);
        self.persist(&next)?;
        *guard = next;
        Ok(())
    }

    pub fn organization(
        &self,
        organization_id: &OrganizationId,
    ) -> Result<Option<Organization>, ControlPlaneStoreError> {
        let guard = self.lock()?;
        Ok(guard.organizations.get(organization_id.as_str()).cloned())
    }

    pub fn project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<Project>, ControlPlaneStoreError> {
        let guard = self.lock()?;
        Ok(guard.projects.get(project_id.as_str()).cloned())
    }

    pub fn list_organizations(&self) -> Result<Vec<Organization>, ControlPlaneStoreError> {
        let guard = self.lock()?;
        Ok(guard.organizations.values().cloned().collect())
    }

    pub fn list_projects(
        &self,
        organization_id: Option<&OrganizationId>,
    ) -> Result<Vec<Project>, ControlPlaneStoreError> {
        let guard = self.lock()?;
        Ok(guard
            .projects
            .values()
            .filter(|project| match organization_id {
                Some(organization_id) => project.organization_id() == organization_id,
                None => true,
            })
            .cloned()
            .collect())
    }

    pub fn delete_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Project, ControlPlaneStoreError> {
        let mut guard = self.lock()?;
        let mut next = guard.clone();
        let project = next
            .projects
            .remove(project_id.as_str())
            .ok_or_else(|| ControlPlaneStoreError::ProjectNotFound(project_id.clone()))?;
        self.persist(&next)?;
        *guard = next;
        Ok(project)
    }

    pub fn delete_organization(
        &self,
        organization_id: &OrganizationId,
    ) -> Result<Organization, ControlPlaneStoreError> {
        let mut guard = self.lock()?;
        if guard
            .projects
            .values()
            .any(|project| project.organization_id() == organization_id)
        {
            return Err(ControlPlaneStoreError::OrganizationHasProjects(
                organization_id.clone(),
            ));
        }
        let mut next = guard.clone();
        let organization = next
            .organizations
            .remove(organization_id.as_str())
            .ok_or_else(|| ControlPlaneStoreError::OrganizationNotFound(organization_id.clone()))?;
        self.persist(&next)?;
        *guard = next;
        Ok(organization)
    }

    pub fn resolve_project_organization(
        &self,
        project_id: &ProjectId,
    ) -> Result<Organization, ControlPlaneStoreError> {
        let guard = self.lock()?;
        let project = guard
            .projects
            .get(project_id.as_str())
            .ok_or_else(|| ControlPlaneStoreError::ProjectNotFound(project_id.clone()))?;
        guard
            .organizations
            .get(project.organization_id().as_str())
            .cloned()
            .ok_or(ControlPlaneStoreError::Corrupt(
                "project references missing organization",
            ))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, ControlPlaneState>, ControlPlaneStoreError> {
        self.state
            .lock()
            .map_err(|_| ControlPlaneStoreError::LockPoisoned)
    }

    fn persist(&self, state: &ControlPlaneState) -> Result<(), ControlPlaneStoreError> {
        let final_path = self.directory.join(FILE_NAME);
        let temp_path = self.directory.join(TEMP_FILE_NAME);
        if temp_path.exists() {
            fs::remove_file(&temp_path)?;
        }

        let bytes = encode_state(state)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        file.write_all(&bytes)?;
        file.flush()?;
        file.sync_data()?;
        drop(file);

        fs::rename(&temp_path, &final_path)?;
        sync_directory(&self.directory)?;
        Ok(())
    }
}

fn ensure_organization_slug_unique(
    state: &ControlPlaneState,
    organization: &Organization,
    except_id: Option<&str>,
) -> Result<(), ControlPlaneStoreError> {
    if state.organizations.values().any(|candidate| {
        candidate.slug() == organization.slug() && Some(candidate.id().as_str()) != except_id
    }) {
        return Err(ControlPlaneStoreError::OrganizationSlugConflict(
            organization.slug().to_string(),
        ));
    }
    Ok(())
}

fn ensure_project_parent_exists(
    state: &ControlPlaneState,
    project: &Project,
) -> Result<(), ControlPlaneStoreError> {
    if state
        .organizations
        .contains_key(project.organization_id().as_str())
    {
        Ok(())
    } else {
        Err(ControlPlaneStoreError::OrganizationNotFound(
            project.organization_id().clone(),
        ))
    }
}

fn ensure_project_slug_unique(
    state: &ControlPlaneState,
    project: &Project,
    except_id: Option<&str>,
) -> Result<(), ControlPlaneStoreError> {
    if state.projects.values().any(|candidate| {
        candidate.organization_id() == project.organization_id()
            && candidate.slug() == project.slug()
            && Some(candidate.id().as_str()) != except_id
    }) {
        return Err(ControlPlaneStoreError::ProjectSlugConflict {
            organization_id: project.organization_id().clone(),
            slug: project.slug().to_string(),
        });
    }
    Ok(())
}

#[derive(Debug)]
pub enum ControlPlaneStoreError {
    Io(std::io::Error),
    InvalidMagic,
    UnsupportedVersion(u8),
    ChecksumMismatch,
    Corrupt(&'static str),
    Domain(String),
    OrganizationAlreadyExists(OrganizationId),
    OrganizationNotFound(OrganizationId),
    OrganizationSlugConflict(String),
    OrganizationHasProjects(OrganizationId),
    ProjectAlreadyExists(ProjectId),
    ProjectNotFound(ProjectId),
    ProjectSlugConflict {
        organization_id: OrganizationId,
        slug: String,
    },
    ProjectOwnershipImmutable {
        project_id: ProjectId,
        expected: OrganizationId,
        actual: OrganizationId,
    },
    InconsistentDefaultBootstrap,
    LockPoisoned,
}

impl fmt::Display for ControlPlaneStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "control-plane store I/O error: {error}"),
            Self::InvalidMagic => formatter.write_str("invalid control-plane store magic"),
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported control-plane store version: {version}"
                )
            }
            Self::ChecksumMismatch => formatter.write_str("control-plane store checksum mismatch"),
            Self::Corrupt(message) => write!(formatter, "corrupt control-plane store: {message}"),
            Self::Domain(message) => {
                write!(formatter, "invalid control-plane domain value: {message}")
            }
            Self::OrganizationAlreadyExists(id) => {
                write!(formatter, "organization already exists: {id}")
            }
            Self::OrganizationNotFound(id) => write!(formatter, "organization not found: {id}"),
            Self::OrganizationSlugConflict(slug) => {
                write!(formatter, "organization slug already exists: {slug}")
            }
            Self::OrganizationHasProjects(id) => {
                write!(formatter, "organization still owns projects: {id}")
            }
            Self::ProjectAlreadyExists(id) => write!(formatter, "project already exists: {id}"),
            Self::ProjectNotFound(id) => write!(formatter, "project not found: {id}"),
            Self::ProjectSlugConflict {
                organization_id,
                slug,
            } => write!(
                formatter,
                "project slug already exists in organization {organization_id}: {slug}"
            ),
            Self::ProjectOwnershipImmutable {
                project_id,
                expected,
                actual,
            } => write!(
                formatter,
                "project {project_id} ownership is immutable: expected {expected}, got {actual}"
            ),
            Self::InconsistentDefaultBootstrap => formatter.write_str(
                "default Organization/Project bootstrap state is incomplete or inconsistent",
            ),
            Self::LockPoisoned => formatter.write_str("control-plane store lock poisoned"),
        }
    }
}

impl std::error::Error for ControlPlaneStoreError {}

impl From<std::io::Error> for ControlPlaneStoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ControlPlaneDomainError> for ControlPlaneStoreError {
    fn from(value: ControlPlaneDomainError) -> Self {
        Self::Domain(value.to_string())
    }
}

fn encode_state(state: &ControlPlaneState) -> Result<Vec<u8>, ControlPlaneStoreError> {
    let mut payload = Vec::new();
    write_u32(&mut payload, state.organizations.len())?;
    for organization in state.organizations.values() {
        write_string(&mut payload, organization.id().as_str())?;
        write_string(&mut payload, organization.name())?;
        write_string(&mut payload, organization.slug())?;
        payload.push(encode_lifecycle(organization.lifecycle()));
        payload.extend_from_slice(&organization.timestamps().created_at_unix().to_le_bytes());
        payload.extend_from_slice(&organization.timestamps().updated_at_unix().to_le_bytes());
    }

    write_u32(&mut payload, state.projects.len())?;
    for project in state.projects.values() {
        write_string(&mut payload, project.id().as_str())?;
        write_string(&mut payload, project.organization_id().as_str())?;
        write_string(&mut payload, project.name())?;
        write_string(&mut payload, project.slug())?;
        payload.push(encode_lifecycle(project.lifecycle()));
        payload.extend_from_slice(&project.timestamps().created_at_unix().to_le_bytes());
        payload.extend_from_slice(&project.timestamps().updated_at_unix().to_le_bytes());
    }

    let payload_len = u64::try_from(payload.len())
        .map_err(|_| ControlPlaneStoreError::Corrupt("payload too large"))?;
    let mut output = Vec::with_capacity(HEADER_LEN + payload.len());
    output.extend_from_slice(&MAGIC);
    output.push(VERSION);
    output.extend_from_slice(&[0, 0, 0]);
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(&crc32(&payload).to_le_bytes());
    output.extend_from_slice(&payload);
    Ok(output)
}

fn decode_state(bytes: &[u8]) -> Result<ControlPlaneState, ControlPlaneStoreError> {
    if bytes.len() < HEADER_LEN {
        return Err(ControlPlaneStoreError::Corrupt("truncated header"));
    }
    if bytes[0..4] != MAGIC {
        return Err(ControlPlaneStoreError::InvalidMagic);
    }
    if bytes[4] != VERSION {
        return Err(ControlPlaneStoreError::UnsupportedVersion(bytes[4]));
    }

    let payload_len = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed slice"));
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| ControlPlaneStoreError::Corrupt("payload length overflow"))?;
    if bytes.len() != HEADER_LEN + payload_len {
        return Err(ControlPlaneStoreError::Corrupt("payload length mismatch"));
    }

    let checksum = u32::from_le_bytes(bytes[16..20].try_into().expect("fixed slice"));
    let payload = &bytes[HEADER_LEN..];
    if crc32(payload) != checksum {
        return Err(ControlPlaneStoreError::ChecksumMismatch);
    }

    let mut cursor = Cursor::new(payload);
    let organization_count = cursor.read_u32()? as usize;
    let mut organizations = BTreeMap::new();
    for _ in 0..organization_count {
        let id = OrganizationId::new(cursor.read_string()?)?;
        let name = cursor.read_string()?;
        let slug = cursor.read_string()?;
        let lifecycle = decode_lifecycle(cursor.read_u8()?)?;
        let timestamps = ResourceTimestamps::new(cursor.read_u64()?, cursor.read_u64()?)?;
        let organization = Organization::new(id.clone(), name, slug, lifecycle, timestamps)?;
        if organizations
            .insert(id.as_str().to_string(), organization)
            .is_some()
        {
            return Err(ControlPlaneStoreError::Corrupt("duplicate organization ID"));
        }
    }

    let project_count = cursor.read_u32()? as usize;
    let mut projects = BTreeMap::new();
    for _ in 0..project_count {
        let id = ProjectId::new(cursor.read_string()?)
            .map_err(|error| ControlPlaneStoreError::Domain(error.to_string()))?;
        let organization_id = OrganizationId::new(cursor.read_string()?)?;
        let name = cursor.read_string()?;
        let slug = cursor.read_string()?;
        let lifecycle = decode_lifecycle(cursor.read_u8()?)?;
        let timestamps = ResourceTimestamps::new(cursor.read_u64()?, cursor.read_u64()?)?;
        let project = Project::new(
            id.clone(),
            organization_id,
            name,
            slug,
            lifecycle,
            timestamps,
        )?;
        if projects.insert(id.as_str().to_string(), project).is_some() {
            return Err(ControlPlaneStoreError::Corrupt("duplicate project ID"));
        }
    }

    if !cursor.is_empty() {
        return Err(ControlPlaneStoreError::Corrupt("trailing payload bytes"));
    }

    let state = ControlPlaneState {
        organizations,
        projects,
    };
    validate_loaded_state(&state)?;
    Ok(state)
}

fn validate_loaded_state(state: &ControlPlaneState) -> Result<(), ControlPlaneStoreError> {
    for organization in state.organizations.values() {
        ensure_organization_slug_unique(state, organization, Some(organization.id().as_str()))?;
    }
    for project in state.projects.values() {
        ensure_project_parent_exists(state, project)
            .map_err(|_| ControlPlaneStoreError::Corrupt("orphan project"))?;
        ensure_project_slug_unique(state, project, Some(project.id().as_str()))?;
    }
    Ok(())
}

fn write_u32(output: &mut Vec<u8>, value: usize) -> Result<(), ControlPlaneStoreError> {
    let value = u32::try_from(value)
        .map_err(|_| ControlPlaneStoreError::Corrupt("record count too large"))?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<(), ControlPlaneStoreError> {
    write_u32(output, value.len())?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_lifecycle(lifecycle: ResourceLifecycleState) -> u8 {
    match lifecycle {
        ResourceLifecycleState::Active => 1,
        ResourceLifecycleState::Suspended => 2,
        ResourceLifecycleState::Deleting => 3,
    }
}

fn decode_lifecycle(value: u8) -> Result<ResourceLifecycleState, ControlPlaneStoreError> {
    match value {
        1 => Ok(ResourceLifecycleState::Active),
        2 => Ok(ResourceLifecycleState::Suspended),
        3 => Ok(ResourceLifecycleState::Deleting),
        _ => Err(ControlPlaneStoreError::Corrupt("unknown lifecycle state")),
    }
}

fn sync_directory(path: &Path) -> Result<(), ControlPlaneStoreError> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ControlPlaneStoreError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(ControlPlaneStoreError::Corrupt("cursor overflow"))?;
        if end > self.bytes.len() {
            return Err(ControlPlaneStoreError::Corrupt("truncated payload"));
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8, ControlPlaneStoreError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, ControlPlaneStoreError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("fixed slice"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, ControlPlaneStoreError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("fixed slice"),
        ))
    }

    fn read_string(&mut self) -> Result<String, ControlPlaneStoreError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_exact(len)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| ControlPlaneStoreError::Corrupt("string is not UTF-8"))?;
        Ok(value.to_string())
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ketebe-control-plane-{label}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    fn timestamps(created: u64, updated: u64) -> ResourceTimestamps {
        ResourceTimestamps::new(created, updated).unwrap()
    }

    fn organization(id: &str, slug: &str, lifecycle: ResourceLifecycleState) -> Organization {
        Organization::new(
            OrganizationId::new(id).unwrap(),
            format!("{id} display"),
            slug,
            lifecycle,
            timestamps(10, 20),
        )
        .unwrap()
    }

    fn project(
        id: &str,
        organization_id: &str,
        slug: &str,
        lifecycle: ResourceLifecycleState,
    ) -> Project {
        Project::new(
            ProjectId::new(id).unwrap(),
            OrganizationId::new(organization_id).unwrap(),
            format!("{id} display"),
            slug,
            lifecycle,
            timestamps(30, 40),
        )
        .unwrap()
    }

    #[test]
    fn partial_default_bootstrap_state_fails_closed() {
        let dir = temp_dir("partial-default");
        let store = ControlPlaneStore::open(&dir).unwrap();
        store
            .create_organization(
                Organization::new(
                    OrganizationId::default_organization(),
                    "Default",
                    "default",
                    ResourceLifecycleState::Active,
                    timestamps(1, 1),
                )
                .unwrap(),
            )
            .unwrap();

        assert!(matches!(
            store.bootstrap_default(2),
            Err(ControlPlaneStoreError::InconsistentDefaultBootstrap)
        ));
        assert!(
            store
                .project(&ProjectId::default_project())
                .unwrap()
                .is_none()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn restart_preserves_organization_project_ownership_and_lifecycle() {
        let dir = temp_dir("restart");
        let store = ControlPlaneStore::open(&dir).unwrap();
        store
            .create_organization(organization(
                "org-a",
                "acme",
                ResourceLifecycleState::Suspended,
            ))
            .unwrap();
        store
            .create_project(project(
                "project-a",
                "org-a",
                "search",
                ResourceLifecycleState::Deleting,
            ))
            .unwrap();
        drop(store);

        let reopened = ControlPlaneStore::open(&dir).unwrap();
        let loaded_project = reopened
            .project(&ProjectId::new("project-a").unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            loaded_project.organization_id(),
            &OrganizationId::new("org-a").unwrap()
        );
        assert_eq!(loaded_project.lifecycle(), ResourceLifecycleState::Deleting);
        let owner = reopened
            .resolve_project_organization(&ProjectId::new("project-a").unwrap())
            .unwrap();
        assert_eq!(owner.lifecycle(), ResourceLifecycleState::Suspended);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn orphan_project_creation_is_rejected_without_mutating_durable_state() {
        let dir = temp_dir("orphan");
        let store = ControlPlaneStore::open(&dir).unwrap();
        let result = store.create_project(project(
            "project-a",
            "missing-org",
            "search",
            ResourceLifecycleState::Active,
        ));
        assert!(matches!(
            result,
            Err(ControlPlaneStoreError::OrganizationNotFound(_))
        ));
        drop(store);

        let reopened = ControlPlaneStore::open(&dir).unwrap();
        assert!(
            reopened
                .project(&ProjectId::new("project-a").unwrap())
                .unwrap()
                .is_none()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn project_ownership_cannot_be_changed_by_update() {
        let dir = temp_dir("ownership");
        let store = ControlPlaneStore::open(&dir).unwrap();
        store
            .create_organization(organization(
                "org-a",
                "org-a",
                ResourceLifecycleState::Active,
            ))
            .unwrap();
        store
            .create_organization(organization(
                "org-b",
                "org-b",
                ResourceLifecycleState::Active,
            ))
            .unwrap();
        store
            .create_project(project(
                "project-a",
                "org-a",
                "search",
                ResourceLifecycleState::Active,
            ))
            .unwrap();

        let result = store.update_project(project(
            "project-a",
            "org-b",
            "search",
            ResourceLifecycleState::Active,
        ));
        assert!(matches!(
            result,
            Err(ControlPlaneStoreError::ProjectOwnershipImmutable { .. })
        ));
        assert_eq!(
            store
                .resolve_project_organization(&ProjectId::new("project-a").unwrap())
                .unwrap()
                .id()
                .as_str(),
            "org-a"
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn slugs_are_unique_at_their_domain_scope_but_names_are_display_labels() {
        let dir = temp_dir("slug");
        let store = ControlPlaneStore::open(&dir).unwrap();
        store
            .create_organization(organization(
                "org-a",
                "acme",
                ResourceLifecycleState::Active,
            ))
            .unwrap();
        assert!(matches!(
            store.create_organization(organization(
                "org-b",
                "acme",
                ResourceLifecycleState::Active,
            )),
            Err(ControlPlaneStoreError::OrganizationSlugConflict(_))
        ));

        store
            .create_organization(organization(
                "org-b",
                "beta",
                ResourceLifecycleState::Active,
            ))
            .unwrap();
        store
            .create_project(project(
                "project-a",
                "org-a",
                "search",
                ResourceLifecycleState::Active,
            ))
            .unwrap();
        assert!(matches!(
            store.create_project(project(
                "project-b",
                "org-a",
                "search",
                ResourceLifecycleState::Active,
            )),
            Err(ControlPlaneStoreError::ProjectSlugConflict { .. })
        ));
        store
            .create_project(project(
                "project-c",
                "org-b",
                "search",
                ResourceLifecycleState::Active,
            ))
            .unwrap();

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn updates_are_atomic_and_survive_restart() {
        let dir = temp_dir("update");
        let store = ControlPlaneStore::open(&dir).unwrap();
        store
            .create_organization(organization(
                "org-a",
                "acme",
                ResourceLifecycleState::Active,
            ))
            .unwrap();
        store
            .create_project(project(
                "project-a",
                "org-a",
                "search",
                ResourceLifecycleState::Active,
            ))
            .unwrap();

        let updated_organization = Organization::new(
            OrganizationId::new("org-a").unwrap(),
            "ACME renamed",
            "acme",
            ResourceLifecycleState::Suspended,
            timestamps(10, 50),
        )
        .unwrap();
        store.update_organization(updated_organization).unwrap();

        let updated_project = Project::new(
            ProjectId::new("project-a").unwrap(),
            OrganizationId::new("org-a").unwrap(),
            "Search renamed",
            "search",
            ResourceLifecycleState::Deleting,
            timestamps(30, 60),
        )
        .unwrap();
        store.update_project(updated_project).unwrap();
        drop(store);

        let reopened = ControlPlaneStore::open(&dir).unwrap();
        let loaded_organization = reopened
            .organization(&OrganizationId::new("org-a").unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(loaded_organization.name(), "ACME renamed");
        assert_eq!(
            loaded_organization.lifecycle(),
            ResourceLifecycleState::Suspended
        );
        assert_eq!(loaded_organization.timestamps().updated_at_unix(), 50);

        let loaded_project = reopened
            .project(&ProjectId::new("project-a").unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(loaded_project.name(), "Search renamed");
        assert_eq!(loaded_project.lifecycle(), ResourceLifecycleState::Deleting);
        assert_eq!(loaded_project.timestamps().updated_at_unix(), 60);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn checksum_and_version_corruption_fail_closed() {
        let checksum_dir = temp_dir("checksum");
        let store = ControlPlaneStore::open(&checksum_dir).unwrap();
        store
            .create_organization(organization(
                "org-a",
                "acme",
                ResourceLifecycleState::Active,
            ))
            .unwrap();
        drop(store);

        let path = checksum_dir.join(FILE_NAME);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            ControlPlaneStore::open(&checksum_dir),
            Err(ControlPlaneStoreError::ChecksumMismatch)
        ));
        fs::remove_dir_all(checksum_dir).unwrap();

        let version_dir = temp_dir("version");
        fs::create_dir_all(&version_dir).unwrap();
        let mut bytes = vec![0_u8; HEADER_LEN];
        bytes[0..4].copy_from_slice(&MAGIC);
        bytes[4] = VERSION + 1;
        fs::write(version_dir.join(FILE_NAME), bytes).unwrap();
        assert!(matches!(
            ControlPlaneStore::open(&version_dir),
            Err(ControlPlaneStoreError::UnsupportedVersion(_))
        ));
        fs::remove_dir_all(version_dir).unwrap();
    }

    #[test]
    fn stale_temp_file_does_not_replace_committed_snapshot() {
        let dir = temp_dir("temp");
        let store = ControlPlaneStore::open(&dir).unwrap();
        store
            .create_organization(organization(
                "org-a",
                "acme",
                ResourceLifecycleState::Active,
            ))
            .unwrap();
        fs::write(dir.join(TEMP_FILE_NAME), b"incomplete").unwrap();
        drop(store);

        let reopened = ControlPlaneStore::open(&dir).unwrap();
        assert!(
            reopened
                .organization(&OrganizationId::new("org-a").unwrap())
                .unwrap()
                .is_some()
        );
        fs::remove_dir_all(dir).unwrap();
    }
}
