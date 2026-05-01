use fs_meta::{RootSelector, RootSpec};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

const FULL_NFS_ROOTS_FILE_ENV: &str = "FSMETA_FULL_NFS_ROOTS_FILE";
const FULL_DEMO_MAX_SCAN_EVENTS: i64 = 512;

#[derive(Clone, Debug)]
pub struct FullDemoRoot {
    pub id: String,
    pub host_ip: String,
    pub mount_point: PathBuf,
    pub source: String,
}

impl FullDemoRoot {
    pub fn root_spec(&self) -> RootSpec {
        RootSpec {
            id: self.id.clone(),
            selector: RootSelector {
                mount_point: Some(self.mount_point.clone()),
                fs_source: None,
                fs_type: Some("nfs".to_string()),
                host_ip: Some(self.host_ip.clone()),
                host_ref: None,
            },
            subpath_scope: PathBuf::from("/"),
            watch: true,
            scan: true,
            audit_interval_ms: None,
        }
    }
}

pub fn logical_roots_from_env(logical_ids: &[&str]) -> Result<Option<Vec<FullDemoRoot>>, String> {
    let Some(roots_file) = roots_file_from_env() else {
        return Ok(None);
    };
    let roots = load_roots_file(&roots_file)?;
    if roots.len() < logical_ids.len() {
        return Err(format!(
            "full demo roots file {} must contain at least {} roots; found {}",
            roots_file.display(),
            logical_ids.len(),
            roots.len()
        ));
    }
    validate_demo_mounts(&roots, &roots_file)?;
    Ok(Some(
        logical_ids
            .iter()
            .zip(roots.into_iter())
            .map(|(logical_id, root)| {
                let source = root_source(&root);
                FullDemoRoot {
                    id: (*logical_id).to_string(),
                    host_ip: root
                        .selector
                        .host_ip
                        .expect("validated full demo root host_ip"),
                    mount_point: root
                        .selector
                        .mount_point
                        .expect("validated full demo root mount_point"),
                    source,
                }
            })
            .collect(),
    ))
}

pub fn apply_bounded_audit_config(release: &mut Value) -> Result<(), String> {
    let key = if release.get("workers").and_then(Value::as_array).is_some() {
        "workers"
    } else {
        "units"
    };
    let entries = release
        .get_mut(key)
        .and_then(Value::as_array_mut)
        .ok_or_else(|| format!("release doc missing {key}[] for full demo audit budget"))?;
    let entry = entries
        .first_mut()
        .ok_or_else(|| format!("release doc {key}[] is empty for full demo audit budget"))?;
    let entry_obj = entry
        .as_object_mut()
        .ok_or_else(|| format!("release doc {key}[0] is not an object"))?;
    let config = entry_obj
        .entry("config".to_string())
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| format!("release doc {key}[0].config is not an object"))?;
    config.insert(
        "max_scan_events".to_string(),
        json!(FULL_DEMO_MAX_SCAN_EVENTS),
    );
    Ok(())
}

fn roots_file_from_env() -> Option<PathBuf> {
    std::env::var_os(FULL_NFS_ROOTS_FILE_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn load_roots_file(path: &PathBuf) -> Result<Vec<RootSpec>, String> {
    let bytes = fs::read(path)
        .map_err(|e| format!("read full demo roots {} failed: {e}", path.display()))?;
    let value = serde_json::from_slice::<Value>(&bytes)
        .map_err(|e| format!("parse full demo roots {} failed: {e}", path.display()))?;
    let roots_value = value.get("roots").cloned().unwrap_or(value);
    serde_json::from_value::<Vec<RootSpec>>(roots_value)
        .map_err(|e| format!("decode full demo roots {} failed: {e}", path.display()))
}

fn validate_demo_mounts(roots: &[RootSpec], roots_file: &PathBuf) -> Result<(), String> {
    let mounted_paths = mounted_paths()?;
    for root in roots {
        let host_ip = root
            .selector
            .host_ip
            .as_ref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                format!(
                    "full demo root {} in {} must set selector.host_ip",
                    root.id,
                    roots_file.display()
                )
            })?;
        let mount_point = root.selector.mount_point.as_ref().ok_or_else(|| {
            format!(
                "full demo root {} in {} must set selector.mount_point",
                root.id,
                roots_file.display()
            )
        })?;
        let metadata = fs::metadata(mount_point).map_err(|e| {
            format!(
                "full demo root {} mount {} for host {} is not available: {e}",
                root.id,
                mount_point.display(),
                host_ip
            )
        })?;
        if !metadata.is_dir() {
            return Err(format!(
                "full demo root {} mount {} for host {} is not a directory",
                root.id,
                mount_point.display(),
                host_ip
            ));
        }
        if !mounted_paths.contains(mount_point) {
            return Err(format!(
                "full demo root {} path {} for host {} is not an active mount",
                root.id,
                mount_point.display(),
                host_ip
            ));
        }
        fs::read_dir(mount_point).map_err(|e| {
            format!(
                "full demo root {} mount {} for host {} is not readable: {e}",
                root.id,
                mount_point.display(),
                host_ip
            )
        })?;
    }
    Ok(())
}

fn mounted_paths() -> Result<BTreeSet<PathBuf>, String> {
    let mountinfo = fs::read_to_string("/proc/self/mountinfo")
        .map_err(|e| format!("read /proc/self/mountinfo failed: {e}"))?;
    Ok(mountinfo
        .lines()
        .filter_map(|line| line.split(" - ").next())
        .filter_map(|prefix| prefix.split_whitespace().nth(4))
        .map(|field| PathBuf::from(decode_mountinfo_path(field)))
        .collect())
}

fn decode_mountinfo_path(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\'
            && index + 3 < bytes.len()
            && bytes[index + 1].is_ascii_digit()
            && bytes[index + 2].is_ascii_digit()
            && bytes[index + 3].is_ascii_digit()
        {
            if let Ok(decoded) = u8::from_str_radix(&value[index + 1..index + 4], 8) {
                out.push(decoded);
                index += 4;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn root_source(root: &RootSpec) -> String {
    let host_ip = root
        .selector
        .host_ip
        .as_deref()
        .expect("validated full demo root host_ip");
    let mount_point = root
        .selector
        .mount_point
        .as_ref()
        .expect("validated full demo root mount_point");
    format!("{host_ip}:{}", mount_point.display())
}
