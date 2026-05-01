//! Source app configuration.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GrantedMountRoot {
    pub object_ref: String,
    pub host_ref: String,
    pub host_ip: String,
    pub host_name: Option<String>,
    pub site: Option<String>,
    pub zone: Option<String>,
    #[serde(default)]
    pub host_labels: std::collections::BTreeMap<String, String>,
    pub mount_point: PathBuf,
    pub fs_source: String,
    pub fs_type: String,
    #[serde(default)]
    pub mount_options: Vec<String>,
    #[serde(default)]
    pub interfaces: Vec<String>,
    pub active: bool,
}

const SOURCE_SCAN_WORKERS_ENV: &str = "FS_META_SOURCE_SCAN_WORKERS";
const SOURCE_SCAN_WORKERS_DEFAULT: usize = 2;
const SOURCE_SCAN_WORKERS_MIN: usize = 1;
const SOURCE_SCAN_WORKERS_MAX: usize = 16;

const SOURCE_MAX_SCAN_EVENTS_ENV: &str = "FS_META_SOURCE_MAX_SCAN_EVENTS";
const SOURCE_MAX_SCAN_EVENTS_DEFAULT: usize = 100_000;
const SOURCE_MAX_SCAN_EVENTS_MIN: usize = 1;
const SOURCE_MAX_SCAN_EVENTS_MAX: usize = 10_000_000;

const SOURCE_AUDIT_INTERVAL_MS_ENV: &str = "FS_META_SOURCE_AUDIT_INTERVAL_MS";
const SOURCE_AUDIT_INTERVAL_MS_DEFAULT: u64 = 300_000;
const SOURCE_AUDIT_INTERVAL_MS_MIN: u64 = 5_000;
const SOURCE_AUDIT_INTERVAL_MS_MAX: u64 = 3_600_000;

const SOURCE_THROTTLE_INTERVAL_MS_ENV: &str = "FS_META_SOURCE_THROTTLE_INTERVAL_MS";
const SOURCE_THROTTLE_INTERVAL_MS_DEFAULT: u64 = 500;
const SOURCE_THROTTLE_INTERVAL_MS_MIN: u64 = 50;
const SOURCE_THROTTLE_INTERVAL_MS_MAX: u64 = 60_000;

const SINK_TOMBSTONE_TTL_MS_ENV: &str = "FS_META_SINK_TOMBSTONE_TTL_MS";
const SINK_TOMBSTONE_TTL_MS_DEFAULT: u64 = 90_000;
const SINK_TOMBSTONE_TTL_MS_MIN: u64 = 1_000;
const SINK_TOMBSTONE_TTL_MS_MAX: u64 = 3_600_000;

const SINK_TOMBSTONE_TOLERANCE_US_ENV: &str = "FS_META_SINK_TOMBSTONE_TOLERANCE_US";
const SINK_TOMBSTONE_TOLERANCE_US_DEFAULT: u64 = 1_000_000;
const SINK_TOMBSTONE_TOLERANCE_US_MAX: u64 = 10_000_000;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RootSelector {
    #[serde(default)]
    pub mount_point: Option<PathBuf>,
    #[serde(default)]
    pub fs_source: Option<String>,
    #[serde(default)]
    pub fs_type: Option<String>,
    #[serde(default)]
    pub host_ip: Option<String>,
    #[serde(default)]
    pub host_ref: Option<String>,
}

impl RootSelector {
    pub fn is_empty(&self) -> bool {
        self.mount_point.is_none()
            && self.fs_source.is_none()
            && self.fs_type.is_none()
            && self.host_ip.is_none()
            && self.host_ref.is_none()
    }

    pub fn matches(&self, grant: &GrantedMountRoot) -> bool {
        if let Some(expected) = &self.mount_point
            && grant.mount_point != *expected
        {
            return false;
        }
        if let Some(expected) = &self.fs_source
            && &grant.fs_source != expected
        {
            return false;
        }
        if let Some(expected) = &self.fs_type
            && &grant.fs_type != expected
        {
            return false;
        }
        if let Some(expected) = &self.host_ip
            && &grant.host_ip != expected
        {
            return false;
        }
        if let Some(expected) = &self.host_ref
            && &grant.host_ref != expected
        {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RootSpec {
    pub id: String,
    pub selector: RootSelector,
    pub subpath_scope: PathBuf,
    pub watch: bool,
    pub scan: bool,
    pub audit_interval_ms: Option<u64>,
}

impl RootSpec {
    pub fn new(id: impl Into<String>, mount_point: impl Into<PathBuf>) -> Self {
        Self {
            id: id.into(),
            selector: RootSelector {
                mount_point: Some(mount_point.into()),
                ..RootSelector::default()
            },
            subpath_scope: PathBuf::from("/"),
            watch: true,
            scan: true,
            audit_interval_ms: None,
        }
    }

    pub fn selected_mount_point(&self) -> Option<&Path> {
        self.selector.mount_point.as_deref()
    }

    pub fn monitor_path_for(&self, grant: &GrantedMountRoot) -> Result<PathBuf, String> {
        let mount_point = grant.mount_point.clone();
        if self.subpath_scope == Path::new("/") {
            return Ok(mount_point);
        }
        if !self.subpath_scope.is_absolute() {
            return Err(format!(
                "source root '{}' subpath_scope must be absolute",
                self.id
            ));
        }
        let relative = self.subpath_scope.strip_prefix("/").map_err(|_| {
            format!(
                "source root '{}' subpath_scope must stay under '/'",
                self.id
            )
        })?;
        Ok(mount_point.join(relative))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceConfig {
    pub roots: Vec<RootSpec>,
    pub batch_size: usize,
    pub throttle_interval: Duration,
    pub audit_interval: Duration,
    pub scan_workers: usize,
    pub lru_capacity: usize,
    pub min_monitoring_window: Duration,
    pub max_scan_events: usize,
    pub sink_tombstone_ttl: Duration,
    pub sink_tombstone_tolerance_us: u64,
    pub drift_window_size: usize,
    pub drift_graduation_threshold: u64,
    pub drift_max_jump_us: i64,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            roots: Vec::new(),
            batch_size: 1000,
            throttle_interval: Self::throttle_interval_from_env(),
            audit_interval: Self::audit_interval_from_env(),
            scan_workers: Self::scan_workers_from_env(),
            lru_capacity: 65536,
            min_monitoring_window: Duration::from_secs(86400),
            max_scan_events: Self::max_scan_events_from_env(),
            sink_tombstone_ttl: Self::sink_tombstone_ttl_from_env(),
            sink_tombstone_tolerance_us: Self::sink_tombstone_tolerance_us_from_env(),
            drift_window_size: 10_000,
            drift_graduation_threshold: 10_000,
            drift_max_jump_us: 10_000_000,
        }
    }
}

impl SourceConfig {
    pub(crate) fn normalize_scan_workers(raw: usize) -> usize {
        raw.clamp(SOURCE_SCAN_WORKERS_MIN, SOURCE_SCAN_WORKERS_MAX)
    }

    pub(crate) fn normalize_max_scan_events(raw: usize) -> usize {
        raw.clamp(SOURCE_MAX_SCAN_EVENTS_MIN, SOURCE_MAX_SCAN_EVENTS_MAX)
    }

    pub(crate) fn normalize_audit_interval_ms(raw: u64) -> Duration {
        Duration::from_millis(raw.clamp(SOURCE_AUDIT_INTERVAL_MS_MIN, SOURCE_AUDIT_INTERVAL_MS_MAX))
    }

    pub(crate) fn normalize_throttle_interval_ms(raw: u64) -> Duration {
        Duration::from_millis(raw.clamp(
            SOURCE_THROTTLE_INTERVAL_MS_MIN,
            SOURCE_THROTTLE_INTERVAL_MS_MAX,
        ))
    }

    pub(crate) fn normalize_sink_tombstone_ttl_ms(raw: u64) -> Duration {
        Duration::from_millis(raw.clamp(SINK_TOMBSTONE_TTL_MS_MIN, SINK_TOMBSTONE_TTL_MS_MAX))
    }

    pub(crate) fn normalize_sink_tombstone_tolerance_us(raw: u64) -> u64 {
        raw.clamp(0, SINK_TOMBSTONE_TOLERANCE_US_MAX)
    }

    fn parse_env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(default)
    }

    fn parse_env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .unwrap_or(default)
    }

    fn scan_workers_from_env() -> usize {
        Self::normalize_scan_workers(Self::parse_env_usize(
            SOURCE_SCAN_WORKERS_ENV,
            SOURCE_SCAN_WORKERS_DEFAULT,
        ))
    }

    fn max_scan_events_from_env() -> usize {
        Self::normalize_max_scan_events(Self::parse_env_usize(
            SOURCE_MAX_SCAN_EVENTS_ENV,
            SOURCE_MAX_SCAN_EVENTS_DEFAULT,
        ))
    }

    fn audit_interval_from_env() -> Duration {
        Self::normalize_audit_interval_ms(Self::parse_env_u64(
            SOURCE_AUDIT_INTERVAL_MS_ENV,
            SOURCE_AUDIT_INTERVAL_MS_DEFAULT,
        ))
    }

    fn throttle_interval_from_env() -> Duration {
        Self::normalize_throttle_interval_ms(Self::parse_env_u64(
            SOURCE_THROTTLE_INTERVAL_MS_ENV,
            SOURCE_THROTTLE_INTERVAL_MS_DEFAULT,
        ))
    }

    fn sink_tombstone_ttl_from_env() -> Duration {
        Self::normalize_sink_tombstone_ttl_ms(Self::parse_env_u64(
            SINK_TOMBSTONE_TTL_MS_ENV,
            SINK_TOMBSTONE_TTL_MS_DEFAULT,
        ))
    }

    fn sink_tombstone_tolerance_us_from_env() -> u64 {
        Self::normalize_sink_tombstone_tolerance_us(Self::parse_env_u64(
            SINK_TOMBSTONE_TOLERANCE_US_ENV,
            SINK_TOMBSTONE_TOLERANCE_US_DEFAULT,
        ))
    }

    pub fn effective_roots(&self) -> Result<Vec<RootSpec>, String> {
        let mut roots = self.roots.clone();
        if roots.is_empty() {
            return Ok(roots);
        }

        let mut ids = std::collections::BTreeSet::new();
        for root in &mut roots {
            if root.id.trim().is_empty() {
                return Err("source root id must not be empty".into());
            }
            if !ids.insert(root.id.clone()) {
                return Err(format!("duplicate source root id '{}'", root.id));
            }
            if root.selector.is_empty() {
                return Err(format!(
                    "source root '{}' selector must constrain at least one host/object descriptor",
                    root.id
                ));
            }
            if let Some(mount_point) = &root.selector.mount_point
                && !mount_point.is_absolute()
            {
                return Err(format!(
                    "source root '{}' selector.mount_point must be absolute: {}",
                    root.id,
                    mount_point.display()
                ));
            }
            if !root.subpath_scope.is_absolute() {
                return Err(format!(
                    "source root '{}' subpath_scope must be absolute: {}",
                    root.id,
                    root.subpath_scope.display()
                ));
            }
            if !root.watch && !root.scan {
                return Err(format!(
                    "source root '{}' must enable watch or scan",
                    root.id
                ));
            }
        }
        Ok(roots)
    }

    pub fn matching_grants_by_root(&self) -> HashMap<String, Vec<GrantedMountRoot>> {
        let mut grouped = HashMap::new();
        for root in &self.roots {
            grouped.insert(root.id.clone(), Vec::new());
        }
        grouped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn sample_grant() -> GrantedMountRoot {
        GrantedMountRoot {
            object_ref: "node-a::exp1".to_string(),
            interfaces: vec!["posix-fs".to_string()],
            host_ref: "node-a".to_string(),
            host_ip: "10.0.0.11".to_string(),
            host_name: Some("node-a".to_string()),
            site: None,
            zone: None,
            host_labels: BTreeMap::new(),
            mount_point: PathBuf::from("/mnt/nfs1"),
            fs_source: "server:/nfs1".to_string(),
            fs_type: "nfs".to_string(),
            mount_options: Vec::new(),
            active: true,
        }
    }

    #[test]
    fn effective_roots_rejects_duplicate_ids() {
        let mut cfg = SourceConfig::default();
        cfg.roots = vec![
            RootSpec::new("a", "/mnt/nfs1"),
            RootSpec::new("a", "/mnt/nfs2"),
        ];
        let err = cfg.effective_roots().expect_err("duplicate id must fail");
        assert!(err.contains("duplicate"));
    }

    #[test]
    fn effective_roots_rejects_non_absolute_selector_mount_point() {
        let mut cfg = SourceConfig::default();
        let mut root = RootSpec::new("a", "/mnt/nfs1");
        root.selector.mount_point = Some(PathBuf::from("relative/path"));
        cfg.roots = vec![root];
        let err = cfg
            .effective_roots()
            .expect_err("relative selector must fail");
        assert!(err.contains("selector.mount_point"));
    }

    #[test]
    fn effective_roots_allows_empty_roots() {
        let cfg = SourceConfig::default();
        let roots = cfg
            .effective_roots()
            .expect("empty roots should stay valid");
        assert!(roots.is_empty());
    }

    #[test]
    fn selector_matches_host_and_mount_descriptors() {
        let grant = sample_grant();
        let selector = RootSelector {
            mount_point: Some(PathBuf::from("/mnt/nfs1")),
            host_ip: Some("10.0.0.11".to_string()),
            ..RootSelector::default()
        };
        assert!(selector.matches(&grant));
    }

    #[test]
    fn monitor_path_joins_subpath_scope_under_mount_root() {
        let grant = sample_grant();
        let mut root = RootSpec::new("a", "/mnt/nfs1");
        root.subpath_scope = PathBuf::from("/hot");
        let path = root.monitor_path_for(&grant).expect("monitor path");
        assert_eq!(path, PathBuf::from("/mnt/nfs1/hot"));
    }

    #[test]
    fn normalize_scan_workers_clamps_bounds() {
        assert_eq!(
            SourceConfig::normalize_scan_workers(0),
            SOURCE_SCAN_WORKERS_MIN
        );
        assert_eq!(
            SourceConfig::normalize_scan_workers(99),
            SOURCE_SCAN_WORKERS_MAX
        );
        assert_eq!(SourceConfig::normalize_scan_workers(4), 4);
    }

    #[test]
    fn normalize_max_scan_events_clamps_bounds() {
        assert_eq!(
            SourceConfig::normalize_max_scan_events(0),
            SOURCE_MAX_SCAN_EVENTS_MIN
        );
        assert_eq!(
            SourceConfig::normalize_max_scan_events(usize::MAX),
            SOURCE_MAX_SCAN_EVENTS_MAX
        );
        assert_eq!(SourceConfig::normalize_max_scan_events(512), 512);
    }

    #[test]
    fn normalize_intervals_clamp_bounds() {
        assert_eq!(
            SourceConfig::normalize_throttle_interval_ms(1),
            Duration::from_millis(SOURCE_THROTTLE_INTERVAL_MS_MIN)
        );
        assert_eq!(
            SourceConfig::normalize_audit_interval_ms(u64::MAX),
            Duration::from_millis(SOURCE_AUDIT_INTERVAL_MS_MAX)
        );
        assert_eq!(
            SourceConfig::normalize_sink_tombstone_ttl_ms(0),
            Duration::from_millis(SINK_TOMBSTONE_TTL_MS_MIN)
        );
    }
}
