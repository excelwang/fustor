use capanix_app_sdk::runtime::ConfigValue;
use capanix_app_sdk::worker::WorkerMode;
use capanix_app_sdk::{CnxError, Result};
use capanix_worker_runtime_support::{
    WorkerArtifactBinding, WorkerArtifactDefaults, resolve_worker_artifact_binding,
};

pub mod api;
pub mod product;
pub mod query;
mod runtime;
mod runtime_app;
pub mod shared_types;
pub mod sink;
pub mod source;
mod state;
pub mod workers;

pub use query::GroupReliability;
pub use runtime_app::{FSMetaApp, FSMetaRuntimeApp};
pub use shared_types::{
    ControlCommand, ControlEvent, EpochType, EventKind, FileMetaRecord, LogicalClock, SyncTrack,
};
pub use source::config::{GrantedMountRoot, RootSelector, RootSpec};
use source::config::SourceConfig;

#[derive(Clone, Debug)]
pub struct FSMetaRuntimeWorkers {
    pub facade: WorkerArtifactBinding,
    pub source: WorkerArtifactBinding,
    pub scan: WorkerArtifactBinding,
    pub sink: WorkerArtifactBinding,
}

#[derive(Clone, Debug)]
pub struct FSMetaConfig {
    pub source: SourceConfig,
    pub api: api::ApiConfig,
    pub runtime_workers: FSMetaRuntimeWorkers,
}

impl Default for FSMetaConfig {
    fn default() -> Self {
        Self {
            source: SourceConfig::default(),
            api: api::ApiConfig::default(),
            runtime_workers: FSMetaRuntimeWorkers::default(),
        }
    }
}

impl Default for FSMetaRuntimeWorkers {
    fn default() -> Self {
        let cfg = std::collections::HashMap::new();
        Self {
            facade: resolve_worker_artifact_binding(
                &cfg,
                WorkerArtifactDefaults {
                    app_package: env!("CARGO_PKG_NAME"),
                    role_id: "facade",
                    default_mode: WorkerMode::Embedded,
                    default_binary_name: None,
                },
            )
            .unwrap_or(WorkerArtifactBinding {
                role_id: "facade".to_string(),
                mode: WorkerMode::Embedded,
                startup_path: None,
                socket_dir: std::env::temp_dir(),
            }),
            source: resolve_worker_artifact_binding(
                &cfg,
                WorkerArtifactDefaults {
                    app_package: env!("CARGO_PKG_NAME"),
                    role_id: "source",
                    default_mode: WorkerMode::External,
                    default_binary_name: Some("fs_meta_source_worker"),
                },
            )
            .unwrap(),
            scan: resolve_worker_artifact_binding(
                &cfg,
                WorkerArtifactDefaults {
                    app_package: env!("CARGO_PKG_NAME"),
                    role_id: "scan",
                    default_mode: WorkerMode::External,
                    default_binary_name: Some("fs_meta_source_worker"),
                },
            )
            .unwrap(),
            sink: resolve_worker_artifact_binding(
                &cfg,
                WorkerArtifactDefaults {
                    app_package: env!("CARGO_PKG_NAME"),
                    role_id: "sink",
                    default_mode: WorkerMode::External,
                    default_binary_name: Some("fs_meta_sink_worker"),
                },
            )
            .unwrap(),
        }
    }
}

impl FSMetaConfig {
    pub fn from_manifest_config(
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
        fn local_announced_ingress_resources(
            cfg: &std::collections::HashMap<String, ConfigValue>,
        ) -> Vec<api::config::ApiListenerResource> {
            let Some(runtime) = get_map(cfg, "__cnx_runtime") else {
                return Vec::new();
            };
            let Some(local_host_ref) = get_str(runtime, "local_host_ref") else {
                return Vec::new();
            };
            let local_host_ref = local_host_ref.trim();
            if local_host_ref.is_empty() {
                return Vec::new();
            }
            let Some(ConfigValue::Array(resources)) = runtime.get("announced_resources") else {
                return Vec::new();
            };
            let mut local = Vec::new();
            for resource in resources {
                let ConfigValue::Map(row) = resource else {
                    continue;
                };
                let Some(resource_kind) = get_str(row, "resource_kind") else {
                    continue;
                };
                if resource_kind.trim() != "tcp_listener" {
                    continue;
                }
                let Some(resource_id) = get_str(row, "resource_id") else {
                    continue;
                };
                let Some(node_id) = get_str(row, "node_id") else {
                    continue;
                };
                if node_id.trim() != local_host_ref {
                    continue;
                }
                let Some(bind_addr) = get_str(row, "bind_addr") else {
                    continue;
                };
                if resource_id.trim().is_empty() || bind_addr.trim().is_empty() {
                    continue;
                }
                local.push(api::config::ApiListenerResource {
                    resource_id: resource_id.to_string(),
                    bind_addr: bind_addr.to_string(),
                });
            }
            local
        }

        let mut out = Self::default();
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
        if let Some(runtime) = get_map(cfg, "__cnx_runtime")
            && let Some(ConfigValue::Array(grants)) = runtime.get("host_object_grants")
        {
            out.source.host_object_grants.clear();
            for item in grants {
                let ConfigValue::Map(row) = item else {
                    continue;
                };
                let Some(object_ref) = get_str(row, "object_ref") else {
                    continue;
                };
                let Some(host_ref) = get_str(row, "host_ref") else {
                    continue;
                };
                let Some(object_descriptors) = get_map(row, "object_descriptors") else {
                    continue;
                };
                let Some(host_descriptors) = get_map(row, "host_descriptors") else {
                    continue;
                };
                let Some(mount_point) = get_str(object_descriptors, "mount_point") else {
                    continue;
                };
                let Some(fs_source) = get_str(object_descriptors, "fs_source") else {
                    continue;
                };
                let Some(fs_type) = get_str(object_descriptors, "fs_type") else {
                    continue;
                };
                let Some(host_ip) = get_str(host_descriptors, "host_ip") else {
                    continue;
                };
                out.source.host_object_grants.push(GrantedMountRoot {
                    object_ref: object_ref.to_string(),
                    interfaces: match row.get("interfaces") {
                        Some(ConfigValue::Array(items)) => items
                            .iter()
                            .filter_map(|v| match v {
                                ConfigValue::String(s) => Some(s.clone()),
                                _ => None,
                            })
                            .collect(),
                        _ => Vec::new(),
                    },
                    host_ref: host_ref.to_string(),
                    host_ip: host_ip.to_string(),
                    host_name: get_str(host_descriptors, "host_name").map(str::to_string),
                    site: get_str(host_descriptors, "site").map(str::to_string),
                    zone: get_str(host_descriptors, "zone").map(str::to_string),
                    host_labels: match host_descriptors.get("host_labels") {
                        Some(ConfigValue::Map(labels)) => labels
                            .iter()
                            .filter_map(|(k, v)| match v {
                                ConfigValue::String(s) => Some((k.clone(), s.clone())),
                                _ => None,
                            })
                            .collect::<std::collections::BTreeMap<_, _>>(),
                        _ => std::collections::BTreeMap::new(),
                    },
                    mount_point: std::path::PathBuf::from(mount_point),
                    fs_source: fs_source.to_string(),
                    fs_type: fs_type.to_string(),
                    mount_options: match object_descriptors.get("mount_options") {
                        Some(ConfigValue::Array(items)) => items
                            .iter()
                            .filter_map(|v| match v {
                                ConfigValue::String(s) => Some(s.clone()),
                                _ => None,
                            })
                            .collect(),
                        _ => Vec::new(),
                    },
                    active: match row.get("grant_state") {
                        Some(ConfigValue::String(state)) => state.eq_ignore_ascii_case("active"),
                        _ => true,
                    },
                });
            }
        }
        if let Some(v) = get_int(cfg, "scan_workers")
            && v > 0
        {
            out.source.scan_workers = SourceConfig::normalize_scan_workers(v as usize);
        }
        if let Some(v) = get_int(cfg, "audit_interval_ms")
            && v > 0
        {
            out.source.audit_interval = SourceConfig::normalize_audit_interval_ms(v as u64);
        }
        if let Some(v) = get_int(cfg, "throttle_interval_ms")
            && v > 0
        {
            out.source.throttle_interval = SourceConfig::normalize_throttle_interval_ms(v as u64);
        }
        if let Some(v) = get_int(cfg, "batch_size")
            && v > 0
        {
            out.source.batch_size = v as usize;
        }
        if let Some(v) = get_int(cfg, "max_scan_events")
            && v > 0
        {
            out.source.max_scan_events = v as usize;
        }
        if let Some(v) = get_int(cfg, "sink_tombstone_ttl_ms")
            && v > 0
        {
            out.source.sink_tombstone_ttl = SourceConfig::normalize_sink_tombstone_ttl_ms(v as u64);
        }
        if let Some(v) = get_int(cfg, "sink_tombstone_tolerance_us")
            && v >= 0
        {
            out.source.sink_tombstone_tolerance_us =
                SourceConfig::normalize_sink_tombstone_tolerance_us(v as u64);
        }
        if cfg.contains_key("unit_authority_state_carrier")
            || cfg.contains_key("unit_authority_state_dir")
        {
            return Err(CnxError::InvalidInput(
                "unit_authority_state_carrier/unit_authority_state_dir are removed; authority state is kernel-owned via statecell".into(),
            ));
        }
        for removed in [
            "sink_execution_mode",
            "source_execution_mode",
            "source_worker_bin_path",
            "source_worker_socket_dir",
            "sink_worker_bin_path",
            "sink_worker_socket_dir",
        ] {
            if cfg.contains_key(removed) {
                return Err(CnxError::InvalidInput(format!(
                    "{removed} has been removed; declare generic workers.<role>.mode/startup.path/socket_dir instead"
                )));
            }
        }

        let facade = resolve_worker_artifact_binding(
            cfg,
            WorkerArtifactDefaults {
                app_package: env!("CARGO_PKG_NAME"),
                role_id: "facade",
                default_mode: WorkerMode::Embedded,
                default_binary_name: None,
            },
        )?;
        if facade.mode != WorkerMode::Embedded {
            return Err(CnxError::InvalidInput(
                "workers.facade.mode=external is not supported; facade-worker remains embedded in the current implementation".into(),
            ));
        }
        let source_role_cfg = get_map(cfg, "workers").and_then(|workers| get_map(workers, "source"));
        let scan_role_cfg = get_map(cfg, "workers").and_then(|workers| get_map(workers, "scan"));
        let explicit_worker_path = |role_cfg: Option<&std::collections::HashMap<String, ConfigValue>>| {
            role_cfg.and_then(|row| {
                get_map(row, "startup")
                    .and_then(|startup| get_str(startup, "path"))
                    .or_else(|| get_str(row, "binary_path"))
                    .map(str::to_string)
            })
        };

        let mut source = resolve_worker_artifact_binding(
            cfg,
            WorkerArtifactDefaults {
                app_package: env!("CARGO_PKG_NAME"),
                role_id: "source",
                default_mode: WorkerMode::External,
                default_binary_name: Some("fs_meta_source_worker"),
            },
        )?;
        let mut scan = resolve_worker_artifact_binding(
            cfg,
            WorkerArtifactDefaults {
                app_package: env!("CARGO_PKG_NAME"),
                role_id: "scan",
                default_mode: source.mode,
                default_binary_name: Some("fs_meta_source_worker"),
            },
        )?;
        if explicit_worker_path(source_role_cfg).is_none() && explicit_worker_path(scan_role_cfg).is_some() {
            source.startup_path = scan.startup_path.clone();
        }
        if explicit_worker_path(scan_role_cfg).is_none() && explicit_worker_path(source_role_cfg).is_some() {
            scan.startup_path = source.startup_path.clone();
        }
        if source.mode != scan.mode {
            return Err(CnxError::InvalidInput(
                "workers.source.mode and workers.scan.mode must match while source-worker and scan-worker still share one realization".into(),
            ));
        }
        if let (Some(source_path), Some(scan_path)) = (&source.startup_path, &scan.startup_path)
            && source_path != scan_path
        {
            return Err(CnxError::InvalidInput(
                "workers.source.startup.path and workers.scan.startup.path must match while source-worker and scan-worker still share one realization".into(),
            ));
        }
        let sink = resolve_worker_artifact_binding(
            cfg,
            WorkerArtifactDefaults {
                app_package: env!("CARGO_PKG_NAME"),
                role_id: "sink",
                default_mode: WorkerMode::External,
                default_binary_name: Some("fs_meta_sink_worker"),
            },
        )?;
        out.runtime_workers = FSMetaRuntimeWorkers {
            facade,
            source,
            scan,
            sink,
        };

        out.source
            .effective_roots()
            .map_err(CnxError::InvalidInput)?;
        if let Some(api_cfg) = get_map(cfg, "api") {
            out.api.enabled = get_bool(api_cfg, "enabled", true);
            if api_cfg.contains_key("bind_addr") {
                return Err(CnxError::InvalidInput(
                    "legacy api address field is forbidden; use api.facade_resource_id".into(),
                ));
            }
            if let Some(v) = get_str(api_cfg, "facade_resource_id")
                && !v.trim().is_empty()
            {
                out.api.facade_resource_id = v.to_string();
            }
            if let Some(auth_cfg) = get_map(api_cfg, "auth") {
                if let Some(v) = get_str(auth_cfg, "passwd_path")
                    && !v.trim().is_empty()
                {
                    out.api.auth.passwd_path = std::path::PathBuf::from(v);
                }
                if let Some(v) = get_str(auth_cfg, "shadow_path")
                    && !v.trim().is_empty()
                {
                    out.api.auth.shadow_path = std::path::PathBuf::from(v);
                }
                if let Some(v) = get_str(auth_cfg, "query_keys_path")
                    && !v.trim().is_empty()
                {
                    out.api.auth.query_keys_path = std::path::PathBuf::from(v);
                }
                if let Some(v) = get_int(auth_cfg, "session_ttl_secs")
                    && v > 0
                {
                    out.api.auth.session_ttl_secs = v as u64;
                }
                if let Some(v) = get_str(auth_cfg, "management_group")
                    && !v.trim().is_empty()
                {
                    out.api.auth.management_group = v.to_string();
                }
                if let Some(v) = get_str(auth_cfg, "admin_group")
                    && !v.trim().is_empty()
                {
                    out.api.auth.management_group = v.to_string();
                }
                if let Some(bootstrap_cfg) = get_map(auth_cfg, "bootstrap_management")
                    .or_else(|| get_map(auth_cfg, "bootstrap_admin"))
                {
                    let mut bootstrap = api::config::BootstrapManagementConfig::default();
                    if let Some(v) = get_str(bootstrap_cfg, "username")
                        && !v.trim().is_empty()
                    {
                        bootstrap.username = v.to_string();
                    }
                    if let Some(v) = get_str(bootstrap_cfg, "password") {
                        bootstrap.password = v.to_string();
                    }
                    if let Some(v) = get_int(bootstrap_cfg, "uid")
                        && v >= 0
                    {
                        bootstrap.uid = v as u32;
                    }
                    if let Some(v) = get_int(bootstrap_cfg, "gid")
                        && v >= 0
                    {
                        bootstrap.gid = v as u32;
                    }
                    if let Some(v) = get_str(bootstrap_cfg, "home")
                        && !v.trim().is_empty()
                    {
                        bootstrap.home = v.to_string();
                    }
                    if let Some(v) = get_str(bootstrap_cfg, "shell")
                        && !v.trim().is_empty()
                    {
                        bootstrap.shell = v.to_string();
                    }
                    out.api.auth.bootstrap_management = Some(bootstrap);
                }
            }
        }
        out.api.local_listener_resources = local_announced_ingress_resources(cfg);
        out.api.validate().map_err(CnxError::InvalidInput)?;
        Ok(out)
    }
}
