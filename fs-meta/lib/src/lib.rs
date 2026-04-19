use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::{CnxError, Result};

pub mod api;
pub mod domain_state;
pub mod product_model;
pub mod query;
pub mod shared_types;
pub mod source;

pub use api::{ApiAuthConfig, ApiConfig, BootstrapAdminConfig, BootstrapManagementConfig};
pub use query::GroupReliability;
pub use shared_types::{
    ControlCommand, ControlEvent, EpochType, EventKind, FileMetaRecord, LogicalClock, SyncTrack,
};
use source::config::SourceConfig;
pub use source::config::{GrantedMountRoot, RootSelector, RootSpec};

#[derive(Clone, Debug)]
pub struct FSMetaConfig {
    pub source: SourceConfig,
    pub api: api::ApiConfig,
}

impl Default for FSMetaConfig {
    fn default() -> Self {
        Self {
            source: SourceConfig::default(),
            api: api::ApiConfig::default(),
        }
    }
}

impl FSMetaConfig {
    pub fn from_product_manifest_config(
        cfg: &std::collections::HashMap<String, ConfigValue>,
    ) -> Result<Self> {
        fn get_int(cfg: &std::collections::HashMap<String, ConfigValue>, key: &str) -> Option<i64> {
            match cfg.get(key) {
                Some(ConfigValue::Int(v)) => Some(*v),
                _ => None,
            }
        }
        fn get_bool(
            row: &std::collections::HashMap<String, ConfigValue>,
            key: &str,
            default: bool,
        ) -> bool {
            match row.get(key) {
                Some(ConfigValue::Bool(v)) => *v,
                _ => default,
            }
        }
        fn get_str<'a>(
            row: &'a std::collections::HashMap<String, ConfigValue>,
            key: &str,
        ) -> Option<&'a str> {
            match row.get(key) {
                Some(ConfigValue::String(s)) => Some(s.as_str()),
                _ => None,
            }
        }
        fn get_map<'a>(
            cfg: &'a std::collections::HashMap<String, ConfigValue>,
            key: &str,
        ) -> Option<&'a std::collections::HashMap<String, ConfigValue>> {
            match cfg.get(key) {
                Some(ConfigValue::Map(v)) => Some(v),
                _ => None,
            }
        }
        fn normalize_root_id(raw: &str) -> String {
            let mut out = String::with_capacity(raw.len());
            for ch in raw.chars() {
                if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                    out.push(ch);
                } else {
                    out.push('-');
                }
            }
            out.trim_matches('-').to_string()
        }
        let mut out = Self::default();
        if cfg.contains_key("unit_authority_state_carrier")
            || cfg.contains_key("unit_authority_state_dir")
        {
            return Err(CnxError::InvalidInput(
                "unit_authority_state_carrier/unit_authority_state_dir are removed".into(),
            ));
        }
        if cfg.contains_key("sink_execution_mode") {
            return Err(CnxError::InvalidInput(
                "sink_execution_mode has been removed; declare generic workers.<role>.mode/startup.path/socket_dir instead".into(),
            ));
        }
        if cfg.contains_key("source_execution_mode") {
            return Err(CnxError::InvalidInput(
                "source_execution_mode has been removed; declare generic workers.<role>.mode/startup.path/socket_dir instead".into(),
            ));
        }
        if cfg.contains_key("sink_worker_bin_path") {
            return Err(CnxError::InvalidInput(
                "sink_worker_bin_path has been removed".into(),
            ));
        }
        if cfg.contains_key("source_worker_bin_path") {
            return Err(CnxError::InvalidInput(
                "source_worker_bin_path has been removed".into(),
            ));
        }
        if let Some(ConfigValue::Array(roots)) = cfg.get("roots") {
            out.source.roots.clear();
            let mut used_ids = std::collections::BTreeSet::new();
            for root in roots {
                let ConfigValue::Map(row) = root else {
                    return Err(CnxError::InvalidInput("roots[] item must be map".into()));
                };
                if row.contains_key("path") {
                    return Err(CnxError::InvalidInput(
                        "roots[].path is forbidden; use roots[].selector.mount_point".into(),
                    ));
                }
                if row.contains_key("source_locator") {
                    return Err(CnxError::InvalidInput(
                        "roots[].source_locator is forbidden".into(),
                    ));
                }
                let selector_map = get_map(row, "selector")
                    .ok_or_else(|| CnxError::InvalidInput("roots[].selector is required".into()))?;
                let mut id = get_str(row, "id")
                    .map(normalize_root_id)
                    .unwrap_or_else(|| {
                        normalize_root_id(get_str(selector_map, "mount_point").unwrap_or("root"))
                    });
                if id.is_empty() {
                    id = "root".to_string();
                }
                let base = id.clone();
                let mut n = 2usize;
                while !used_ids.insert(id.clone()) {
                    id = format!("{base}-{n}");
                    n += 1;
                }
                let selector = RootSelector {
                    mount_point: get_str(selector_map, "mount_point").map(std::path::PathBuf::from),
                    fs_source: get_str(selector_map, "fs_source").map(str::to_string),
                    fs_type: get_str(selector_map, "fs_type").map(str::to_string),
                    host_ip: get_str(selector_map, "host_ip").map(str::to_string),
                    host_ref: get_str(selector_map, "host_ref").map(str::to_string),
                };
                let mut spec = RootSpec {
                    id,
                    selector,
                    subpath_scope: get_str(row, "subpath_scope")
                        .map(std::path::PathBuf::from)
                        .unwrap_or_else(|| std::path::PathBuf::from("/")),
                    watch: true,
                    scan: true,
                    audit_interval_ms: None,
                };
                spec.watch = get_bool(row, "watch", true);
                spec.scan = get_bool(row, "scan", true);
                spec.audit_interval_ms = match row.get("audit_interval_ms") {
                    Some(ConfigValue::Int(v)) if *v > 0 => Some(*v as u64),
                    _ => None,
                };
                out.source.roots.push(spec);
            }
        }
        if cfg.contains_key("root_path") {
            return Err(CnxError::InvalidInput(
                "root_path is forbidden; use roots[] only".into(),
            ));
        }
        if let Some(value) = get_int(cfg, "scan_workers")
            && value > 0
        {
            out.source.scan_workers =
                source::config::SourceConfig::normalize_scan_workers(value as usize);
        }
        if let Some(value) = get_int(cfg, "audit_interval_ms")
            && value > 0
        {
            out.source.audit_interval =
                source::config::SourceConfig::normalize_audit_interval_ms(value as u64);
        }
        if let Some(value) = get_int(cfg, "throttle_interval_ms")
            && value > 0
        {
            out.source.throttle_interval =
                source::config::SourceConfig::normalize_throttle_interval_ms(value as u64);
        }
        if let Some(value) = get_int(cfg, "sink_tombstone_ttl_ms")
            && value > 0
        {
            out.source.sink_tombstone_ttl =
                source::config::SourceConfig::normalize_sink_tombstone_ttl_ms(value as u64);
        }
        if let Some(value) = get_int(cfg, "sink_tombstone_tolerance_us")
            && value >= 0
        {
            out.source.sink_tombstone_tolerance_us =
                source::config::SourceConfig::normalize_sink_tombstone_tolerance_us(value as u64);
        }
        out.source.roots = out
            .source
            .effective_roots()
            .map_err(CnxError::InvalidInput)?;
        let api_map = get_map(cfg, "api")
            .ok_or_else(|| CnxError::InvalidInput("api config is required".into()))?;
        out.api.enabled = get_bool(api_map, "enabled", true);
        if let Some(id) = get_str(api_map, "facade_resource_id") {
            out.api.facade_resource_id = id.to_string();
        }
        let auth_map = get_map(api_map, "auth")
            .ok_or_else(|| CnxError::InvalidInput("api.auth config is required".into()))?;
        if let Some(path) = get_str(auth_map, "passwd_path") {
            out.api.auth.passwd_path = std::path::PathBuf::from(path);
        }
        if let Some(path) = get_str(auth_map, "shadow_path") {
            out.api.auth.shadow_path = std::path::PathBuf::from(path);
        }
        if let Some(path) = get_str(auth_map, "query_keys_path") {
            out.api.auth.query_keys_path = std::path::PathBuf::from(path);
        }
        if let Some(v) = get_int(auth_map, "session_ttl_secs")
            && v > 0
        {
            out.api.auth.session_ttl_secs = v as u64;
        }
        if let Some(v) = get_str(auth_map, "management_group") {
            out.api.auth.management_group = v.to_string();
        }
        if let Some(bootstrap_map) = get_map(auth_map, "bootstrap_management") {
            let mut bootstrap = api::config::BootstrapManagementConfig::default();
            if let Some(v) = get_str(bootstrap_map, "username") {
                bootstrap.username = v.to_string();
            }
            if let Some(v) = get_str(bootstrap_map, "password") {
                bootstrap.password = v.to_string();
            }
            if let Some(v) = get_int(bootstrap_map, "uid")
                && v >= 0
            {
                bootstrap.uid = v as u32;
            }
            if let Some(v) = get_int(bootstrap_map, "gid")
                && v >= 0
            {
                bootstrap.gid = v as u32;
            }
            if let Some(v) = get_str(bootstrap_map, "home") {
                bootstrap.home = v.to_string();
            }
            if let Some(v) = get_str(bootstrap_map, "shell") {
                bootstrap.shell = v.to_string();
            }
            out.api.auth.bootstrap_management = Some(bootstrap);
        } else {
            out.api.auth.bootstrap_management = None;
        }
        out.api.validate().map_err(CnxError::InvalidInput)?;
        Ok(out)
    }
}
