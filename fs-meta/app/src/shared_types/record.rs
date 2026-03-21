use serde::{Deserialize, Serialize};

use capanix_host_fs_types::UnixStat;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetaRecord {
    #[serde(with = "serde_bytes")]
    pub path: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub file_name: Vec<u8>,
    pub unix_stat: UnixStat,
    pub event_kind: EventKind,
    pub is_atomic_write: bool,
    pub source: SyncTrack,
    #[serde(with = "serde_bytes")]
    pub parent_path: Vec<u8>,
    pub parent_mtime_us: u64,
    pub audit_skipped: bool,
}

impl FileMetaRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn from_unix(
        path: Vec<u8>,
        file_name: Vec<u8>,
        unix_stat: UnixStat,
        event_kind: EventKind,
        is_atomic_write: bool,
        source: SyncTrack,
        parent_path: Vec<u8>,
        parent_mtime_us: u64,
        audit_skipped: bool,
    ) -> Self {
        Self {
            path,
            file_name,
            unix_stat,
            event_kind,
            is_atomic_write,
            source,
            parent_path,
            parent_mtime_us,
            audit_skipped,
        }
    }

    pub fn scan_update(
        path: Vec<u8>,
        file_name: Vec<u8>,
        unix_stat: UnixStat,
        parent_path: Vec<u8>,
        parent_mtime_us: u64,
        audit_skipped: bool,
    ) -> Self {
        Self::from_unix(
            path,
            file_name,
            unix_stat,
            EventKind::Update,
            true,
            SyncTrack::Scan,
            parent_path,
            parent_mtime_us,
            audit_skipped,
        )
    }

    pub fn realtime_update(
        path: Vec<u8>,
        file_name: Vec<u8>,
        unix_stat: UnixStat,
        is_atomic_write: bool,
    ) -> Self {
        Self::from_unix(
            path,
            file_name,
            unix_stat,
            EventKind::Update,
            is_atomic_write,
            SyncTrack::Realtime,
            Vec::new(),
            0,
            false,
        )
    }

    pub fn realtime_delete(path: Vec<u8>, file_name: Vec<u8>, is_directory: bool) -> Self {
        Self::from_unix(
            path,
            file_name,
            UnixStat::tombstone(is_directory),
            EventKind::Delete,
            false,
            SyncTrack::Realtime,
            Vec::new(),
            0,
            false,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    Update,
    Delete,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncTrack {
    #[default]
    Realtime,
    Scan,
}
