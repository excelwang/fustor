use std::ffi::OsStr;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

use capanix_app_sdk::{CnxError, Result};
use local_fs_source::ResolvedLocalFsEndpoint;
use local_fs_source::shared_types::{
    LocalFsEntry, LocalFsFieldCatalog, LocalFsFieldDescriptor, LocalFsProbeStatus,
    LocalFsSnapshotCursor,
};
use source_kit::{
    SourceOperationBudget, encode_msgpack, filesystem_revision, source_native_revision_digest,
};

pub trait LocalFsSourceDriver: Send + Sync {
    fn sample_content(&self, _endpoint: &ResolvedLocalFsEndpoint, _path: &str, _format: &str,
        _limit: usize, _budget: &mut SourceOperationBudget) -> Result<source_kit::SourceContentSample> {
        Err(CnxError::NotSupported("bounded content reader not linked".into()))
    }
    fn probe(&self, endpoint: &ResolvedLocalFsEndpoint) -> Result<LocalFsProbeStatus>;

    fn probe_with_budget(
        &self,
        endpoint: &ResolvedLocalFsEndpoint,
        budget: &mut SourceOperationBudget,
    ) -> Result<LocalFsProbeStatus> {
        budget.check_deadline()?;
        self.probe(endpoint)
    }

    fn discover_fields(&self, endpoint: &ResolvedLocalFsEndpoint) -> Result<LocalFsFieldCatalog>;

    fn discover_fields_with_budget(
        &self,
        endpoint: &ResolvedLocalFsEndpoint,
        budget: &mut SourceOperationBudget,
    ) -> Result<LocalFsFieldCatalog> {
        budget.check_deadline()?;
        self.discover_fields(endpoint)
    }

    fn snapshot_page(
        &self,
        endpoint: &ResolvedLocalFsEndpoint,
        path: Option<&str>,
        limit: usize,
        cursor: Option<&LocalFsSnapshotCursor>,
    ) -> Result<LocalFsEntryPage>;

    fn snapshot_page_with_budget(
        &self,
        endpoint: &ResolvedLocalFsEndpoint,
        path: Option<&str>,
        limit: usize,
        cursor: Option<&LocalFsSnapshotCursor>,
        budget: &mut SourceOperationBudget,
    ) -> Result<LocalFsEntryPage> {
        budget.check_deadline()?;
        self.snapshot_page(endpoint, path, limit, cursor)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocalFsEntryPage {
    pub entries: Vec<LocalFsEntry>,
    pub source_revision: String,
    pub next_cursor: Option<LocalFsSnapshotCursor>,
    pub has_more: bool,
    pub diagnostics: Option<String>,
}

#[derive(Default)]
pub struct NativeLocalFsSourceDriver;

impl LocalFsSourceDriver for NativeLocalFsSourceDriver {
    fn sample_content(&self, endpoint: &ResolvedLocalFsEndpoint, path: &str, format: &str,
        limit: usize, budget: &mut SourceOperationBudget) -> Result<source_kit::SourceContentSample> {
        use std::io::Read;
        budget.check_deadline()?;
        budget.observe_fanout(1)?;
        budget.consume_call(1)?;
        let root = canonical_root(&endpoint.root_path)?;
        let target = resolve_path(&root, path)?;
        let mut file = fs::File::open(&target).map_err(fs_error("open content sample"))?;
        // Verify the opened descriptor, not just a potentially swapped symlink.
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let opened = fs::canonicalize(format!("/proc/self/fd/{}", file.as_raw_fd())).map_err(fs_error("resolve opened sample"))?;
            if opened != target || !opened.starts_with(&root) { return Err(CnxError::InvalidInput("content path changed during open".into())); }
        }
        let metadata = file.metadata().map_err(fs_error("stat opened sample"))?;
        if !metadata.is_file() { return Err(CnxError::InvalidInput("content sample requires regular file".into())); }
        let revision = filesystem_revision(&metadata)?;
        let cap = budget.remaining_bytes().unwrap_or(source_kit::MAX_CONTENT_PREFIX_BYTES).min(source_kit::MAX_CONTENT_PREFIX_BYTES);
        let mut raw = Vec::new();
        let mut buffer = [0u8; 8192];
        while (raw.len() as u64) < metadata.len().min(cap) {
            budget.check_deadline()?;
            let available = buffer.len().min((metadata.len().min(cap) - raw.len() as u64) as usize);
            let count = file.read(&mut buffer[..available]).map_err(fs_error("read content prefix"))?;
            if count == 0 { break; }
            budget.consume_bytes(count as u64)?;
            raw.extend_from_slice(&buffer[..count]);
        }
        if filesystem_revision(&file.metadata().map_err(fs_error("restat sample"))?)? != revision {
            return Err(CnxError::NotReady("file changed while sampling content".into()));
        }
        let complete = raw.len() as u64 == metadata.len();
        let rows = source_kit::parse_content_prefix(&raw, format, complete, limit, budget)?;
        Ok(source_kit::SourceContentSample { object_ref: endpoint.object_ref.clone(), native_ref: target.to_string_lossy().into_owned(),
            source_revision: revision, format: format.into(), grant_epoch: endpoint.grant_epoch,
            complete: complete && rows.len() < limit, rows, prefix_sha256: source_kit::content_prefix_digest(&raw), bytes_read: raw.len() as u64 })
    }
    fn probe(&self, endpoint: &ResolvedLocalFsEndpoint) -> Result<LocalFsProbeStatus> {
        let mut budget = SourceOperationBudget::unbounded();
        self.probe_with_budget(endpoint, &mut budget)
    }

    fn probe_with_budget(
        &self,
        endpoint: &ResolvedLocalFsEndpoint,
        budget: &mut SourceOperationBudget,
    ) -> Result<LocalFsProbeStatus> {
        budget.check_deadline()?;
        let root = canonical_root(&endpoint.root_path)?;
        let metadata = fs::metadata(&root).map_err(fs_error("probe local-fs root"))?;
        budget.check_deadline()?;
        Ok(LocalFsProbeStatus {
            object_ref: endpoint.object_ref.clone(),
            root_path: root.to_string_lossy().to_string(),
            reachable: metadata.is_dir(),
            diagnostics: if metadata.is_dir() {
                None
            } else {
                Some("root_path is not a directory".into())
            },
        })
    }

    fn discover_fields(&self, endpoint: &ResolvedLocalFsEndpoint) -> Result<LocalFsFieldCatalog> {
        let mut budget = SourceOperationBudget::unbounded();
        self.discover_fields_with_budget(endpoint, &mut budget)
    }

    fn discover_fields_with_budget(
        &self,
        endpoint: &ResolvedLocalFsEndpoint,
        budget: &mut SourceOperationBudget,
    ) -> Result<LocalFsFieldCatalog> {
        budget.check_deadline()?;
        Ok(LocalFsFieldCatalog {
            object_ref: endpoint.object_ref.clone(),
            fields: default_local_fs_fields(),
        })
    }

    fn snapshot_page(
        &self,
        endpoint: &ResolvedLocalFsEndpoint,
        path: Option<&str>,
        limit: usize,
        cursor: Option<&LocalFsSnapshotCursor>,
    ) -> Result<LocalFsEntryPage> {
        let mut budget = SourceOperationBudget::unbounded();
        self.snapshot_page_with_budget(endpoint, path, limit, cursor, &mut budget)
    }

    fn snapshot_page_with_budget(
        &self,
        endpoint: &ResolvedLocalFsEndpoint,
        path: Option<&str>,
        limit: usize,
        cursor: Option<&LocalFsSnapshotCursor>,
        budget: &mut SourceOperationBudget,
    ) -> Result<LocalFsEntryPage> {
        budget.check_deadline()?;
        let root = canonical_root(&endpoint.root_path)?;
        let target = resolve_path(&root, path.unwrap_or_default())?;
        budget.check_deadline()?;
        let offset = cursor.map(|cursor| cursor.offset).unwrap_or(0);
        let metadata = fs::symlink_metadata(&target).map_err(fs_error("read local-fs path"))?;
        let metadata_revision = filesystem_revision(&metadata)?;

        if metadata.is_file() {
            if let Some(cursor) = cursor
                && cursor.source_revision != metadata_revision
            {
                return Err(CnxError::NotReady(
                    "local-fs path changed during snapshot continuation".into(),
                ));
            }
            let entries = if offset == 0 {
                let entry = entry_from_path(&root, &target, metadata)?;
                budget.consume_bytes(encoded_len(&entry)?)?;
                vec![entry]
            } else {
                Vec::new()
            };
            return Ok(LocalFsEntryPage {
                entries,
                source_revision: metadata_revision,
                next_cursor: None,
                has_more: false,
                diagnostics: Some("path resolved to a file".into()),
            });
        }
        if !metadata.is_dir() {
            return Err(CnxError::InvalidInput(format!(
                "local-fs path '{}' is not a regular file or directory",
                display_request_path(path)
            )));
        }

        let mut entries = Vec::new();
        for entry in fs::read_dir(&target).map_err(fs_error("list local-fs directory"))? {
            budget.check_deadline()?;
            let entry = entry.map_err(fs_error("read local-fs directory entry"))?;
            let entry_path = entry.path();
            let metadata = fs::symlink_metadata(&entry_path)
                .map_err(fs_error("read local-fs directory entry metadata"))?;
            if metadata.file_type().is_symlink() {
                return Err(CnxError::ScopeDenied(format!(
                    "local-fs directory entry '{}' is a symbolic link",
                    path_to_slash(entry_path.strip_prefix(&root).unwrap_or(&entry_path))
                )));
            }
            let entry = entry_from_path(&root, &entry_path, metadata)?;
            budget.consume_bytes(encoded_len(&entry)?)?;
            entries.push(entry);
        }
        budget.check_deadline()?;
        let final_metadata_revision = filesystem_revision(
            &fs::symlink_metadata(&target).map_err(fs_error("restat local-fs path"))?,
        )?;
        if final_metadata_revision != metadata_revision {
            return Err(CnxError::NotReady(
                "local-fs directory changed while reading snapshot page".into(),
            ));
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        let encoded_entries = encode_msgpack(&entries)?;
        let source_revision = source_native_revision_digest(
            "fs-directory",
            &[metadata_revision.as_bytes(), &encoded_entries],
        );
        if let Some(cursor) = cursor
            && cursor.source_revision != source_revision
        {
            return Err(CnxError::NotReady(
                "local-fs directory entries changed during snapshot continuation".into(),
            ));
        }

        let mut page = entries
            .into_iter()
            .skip(offset)
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>();
        let has_more = page.len() > limit;
        if has_more {
            page.truncate(limit);
        }
        let next_cursor = has_more.then_some(LocalFsSnapshotCursor {
            offset: offset + page.len(),
            source_revision: source_revision.clone(),
        });
        Ok(LocalFsEntryPage {
            entries: page,
            source_revision,
            next_cursor,
            has_more,
            diagnostics: None,
        })
    }
}

fn encoded_len<T: serde::Serialize>(value: &T) -> Result<u64> {
    u64::try_from(encode_msgpack(value)?.len())
        .map_err(|_| CnxError::ResourceExhausted("source value is too large".into()))
}

#[derive(Default)]
pub struct UnsupportedLocalFsSourceDriver;

impl LocalFsSourceDriver for UnsupportedLocalFsSourceDriver {
    fn probe(&self, _endpoint: &ResolvedLocalFsEndpoint) -> Result<LocalFsProbeStatus> {
        Err(CnxError::NotSupported(
            "local-fs-source native filesystem driver is not linked in this runtime artifact"
                .into(),
        ))
    }

    fn discover_fields(&self, _endpoint: &ResolvedLocalFsEndpoint) -> Result<LocalFsFieldCatalog> {
        Err(CnxError::NotSupported(
            "local-fs-source field discovery requires the native filesystem driver".into(),
        ))
    }

    fn snapshot_page(
        &self,
        _endpoint: &ResolvedLocalFsEndpoint,
        _path: Option<&str>,
        _limit: usize,
        _cursor: Option<&LocalFsSnapshotCursor>,
    ) -> Result<LocalFsEntryPage> {
        Err(CnxError::NotSupported(
            "local-fs-source snapshot requires the native filesystem driver".into(),
        ))
    }
}

fn canonical_root(root_path: &str) -> Result<PathBuf> {
    let root = Path::new(root_path);
    if !root.is_absolute() {
        return Err(CnxError::InvalidInput(format!(
            "local-fs root_path '{root_path}' must be absolute"
        )));
    }
    let root = root
        .canonicalize()
        .map_err(fs_error("canonicalize local-fs root_path"))?;
    let metadata = fs::metadata(&root).map_err(fs_error("stat local-fs root_path"))?;
    if !metadata.is_dir() {
        return Err(CnxError::InvalidInput(format!(
            "local-fs root_path '{}' is not a directory",
            root.to_string_lossy()
        )));
    }
    Ok(root)
}

fn resolve_path(root: &Path, request_path: &str) -> Result<PathBuf> {
    let request_path = request_path.trim();
    let mut candidate = root.to_path_buf();
    if request_path.is_empty() || request_path == "." {
        return Ok(root.to_path_buf());
    }
    let relative = Path::new(request_path);
    for component in relative.components() {
        match component {
            Component::Normal(part) => candidate.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(CnxError::ScopeDenied(format!(
                    "local-fs path '{request_path}' escapes its configured root"
                )));
            }
        }
    }
    let canonical = candidate
        .canonicalize()
        .map_err(fs_error("canonicalize local-fs request path"))?;
    if !canonical.starts_with(root) {
        return Err(CnxError::ScopeDenied(format!(
            "local-fs path '{request_path}' escapes its configured root"
        )));
    }
    Ok(canonical)
}

fn entry_from_path(root: &Path, path: &Path, metadata: fs::Metadata) -> Result<LocalFsEntry> {
    let relative = path.strip_prefix(root).map_err(|_| {
        CnxError::ScopeDenied(format!(
            "local-fs entry '{}' is outside root '{}'",
            path.to_string_lossy(),
            root.to_string_lossy()
        ))
    })?;
    let file_name = path
        .file_name()
        .unwrap_or_else(|| OsStr::new(""))
        .to_string_lossy()
        .to_string();
    Ok(LocalFsEntry {
        path: path_to_slash(relative),
        file_name,
        is_dir: metadata.is_dir(),
        size: metadata.len(),
        modified_unix_ms: metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis() as u64),
    })
}

fn default_local_fs_fields() -> Vec<LocalFsFieldDescriptor> {
    [
        ("path", "string"),
        ("file_name", "string"),
        ("is_dir", "bool"),
        ("size", "u64"),
        ("modified_unix_ms", "u64?"),
    ]
    .into_iter()
    .map(|(name, field_type)| LocalFsFieldDescriptor {
        name: name.into(),
        field_type: field_type.into(),
    })
    .collect()
}

fn path_to_slash(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy().to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn display_request_path(path: Option<&str>) -> String {
    path.unwrap_or_default().to_string()
}

fn fs_error(context: &'static str) -> impl Fn(std::io::Error) -> CnxError {
    move |err| CnxError::PeerError(format!("{context}: {err}"))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use local_fs_source::ResolvedLocalFsEndpoint;
    use tempfile::tempdir;

    use super::*;

    fn endpoint(root_path: String) -> ResolvedLocalFsEndpoint {
        ResolvedLocalFsEndpoint {
            object_ref: "local".into(),
            root_path,
            path_scopes: vec!["*".into()],
            grant_epoch: Some(1),
        }
    }

    #[test]
    fn snapshot_pages_directory_entries() {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("b.txt"), b"b").expect("write b");
        fs::write(dir.path().join("a.txt"), b"a").expect("write a");
        let driver = NativeLocalFsSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());

        let page = driver
            .snapshot_page(&endpoint, None, 1, None)
            .expect("first page");

        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].path, "a.txt");
        assert!(page.has_more);
        let cursor = page.next_cursor.as_ref().expect("cursor");
        assert_eq!(cursor.offset, 1);
        assert!(!cursor.source_revision.is_empty());

        let page = driver
            .snapshot_page(&endpoint, None, 10, page.next_cursor.as_ref())
            .expect("second page");
        assert_eq!(page.entries[0].path, "b.txt");
        assert!(!page.has_more);
    }

    #[test]
    fn parent_path_is_rejected() {
        let dir = tempdir().expect("tempdir");
        let driver = NativeLocalFsSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());

        let err = driver
            .snapshot_page(&endpoint, Some("../outside"), 10, None)
            .unwrap_err();

        assert!(matches!(err, CnxError::ScopeDenied(_)));
    }

    #[test]
    fn directory_mutation_invalidates_snapshot_cursor() {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("a.txt"), b"a").expect("write a");
        fs::write(dir.path().join("b.txt"), b"b").expect("write b");
        let driver = NativeLocalFsSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());
        let first = driver
            .snapshot_page(&endpoint, None, 1, None)
            .expect("first page");
        let cursor = first.next_cursor.expect("cursor");
        fs::write(dir.path().join("c.txt"), b"c").expect("mutate directory");

        let error = driver
            .snapshot_page(&endpoint, None, 1, Some(&cursor))
            .expect_err("mutated directory must invalidate cursor");

        assert!(matches!(error, CnxError::NotReady(_)));
    }

    #[test]
    fn child_metadata_mutation_invalidates_snapshot_cursor_when_directory_metadata_is_stable() {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("a.txt"), b"a").expect("write a");
        fs::write(dir.path().join("b.txt"), b"b").expect("write b");
        let driver = NativeLocalFsSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());
        let first = driver
            .snapshot_page(&endpoint, None, 1, None)
            .expect("first page");
        let cursor = first.next_cursor.expect("cursor");
        let directory_revision_before = filesystem_revision(
            &fs::symlink_metadata(dir.path()).expect("directory metadata before mutation"),
        )
        .expect("directory revision before mutation");

        fs::write(dir.path().join("b.txt"), b"changed-size").expect("mutate child metadata");

        let directory_revision_after = filesystem_revision(
            &fs::symlink_metadata(dir.path()).expect("directory metadata after mutation"),
        )
        .expect("directory revision after mutation");
        assert_eq!(directory_revision_before, directory_revision_after);
        let error = driver
            .snapshot_page(&endpoint, None, 1, Some(&cursor))
            .expect_err("changed child metadata must invalidate cursor");

        assert!(matches!(error, CnxError::NotReady(_)));
    }

    #[cfg(unix)]
    #[test]
    fn directory_listing_rejects_symlink_entries_before_target_metadata_is_read() {
        let root = tempdir().expect("root tempdir");
        let outside = tempdir().expect("outside tempdir");
        fs::write(outside.path().join("secret.txt"), b"secret").expect("write outside");
        std::os::unix::fs::symlink(
            outside.path().join("secret.txt"),
            root.path().join("secret-link"),
        )
        .expect("create symlink");
        let driver = NativeLocalFsSourceDriver;
        let endpoint = endpoint(root.path().to_string_lossy().to_string());

        let error = driver
            .snapshot_page(&endpoint, None, 10, None)
            .expect_err("directory symlink must fail closed");

        assert!(matches!(error, CnxError::ScopeDenied(_)));
    }

    #[test]
    fn directory_listing_checks_byte_and_deadline_budgets_inside_scan() {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("a.txt"), b"a").expect("write a");
        let driver = NativeLocalFsSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());
        let mut byte_budget = SourceOperationBudget::new(None, Some(1));

        let error = driver
            .snapshot_page_with_budget(&endpoint, None, 10, None, &mut byte_budget)
            .expect_err("entry must exceed one-byte scan budget");
        assert!(matches!(error, CnxError::ResourceExhausted(_)));

        let mut deadline_budget = SourceOperationBudget::new(Some(Duration::ZERO), None);
        let error = driver
            .snapshot_page_with_budget(&endpoint, None, 10, None, &mut deadline_budget)
            .expect_err("expired operation must stop before scan");
        assert!(matches!(error, CnxError::Timeout));
    }
}
