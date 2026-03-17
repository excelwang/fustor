#![cfg(target_os = "linux")]
#![allow(dead_code)]

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use base64::Engine;
use serde_json::{json, Map, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

#[derive(Clone, Debug)]
struct OracleNode {
    path: String,
    file_name: String,
    size: u64,
    modified_time_us: u64,
    is_dir: bool,
}

pub struct FsTreeOracle;

impl FsTreeOracle {
    pub fn grouped_tree_response(
        mounts: &[(String, PathBuf)],
        query_path: &str,
        recursive: bool,
        max_depth: Option<usize>,
        entry_page_size: usize,
        group_order: &str,
        group_page_size: usize,
    ) -> Result<Value, String> {
        let ordered = ordered_mounts(mounts, query_path, recursive, max_depth, group_order)?;
        let has_more_groups = ordered.len() > group_page_size;
        let mut groups = Vec::new();
        for (group_key, mount_path) in ordered.into_iter().take(group_page_size) {
            groups.push(Self::tree_group_envelope(
                &group_key,
                &mount_path,
                query_path,
                recursive,
                max_depth,
                entry_page_size,
                false,
            )?);
        }
        let mut body = Map::new();
        body.insert("path".into(), Value::String(query_path.to_string()));
        maybe_insert_b64(&mut body, "path_b64", query_path.as_bytes());
        body.insert("status".into(), Value::String("ok".into()));
        body.insert("group_order".into(), Value::String(group_order.to_string()));
        body.insert(
            "pit".into(),
            json!({
                "id": "oracle-pit",
                "expires_at_ms": 0u64,
            }),
        );
        body.insert("groups".into(), Value::Array(groups));
        body.insert(
            "group_page".into(),
            json!({
                "returned_groups": body["groups"].as_array().map_or(0, |v| v.len()),
                "has_more_groups": has_more_groups,
                "next_cursor": Value::Null,
                "next_entry_after": Value::Null,
            }),
        );
        Ok(Value::Object(body))
    }

    pub fn grouped_force_find_response(
        mounts: &[(String, PathBuf)],
        query_path: &str,
        recursive: bool,
        max_depth: Option<usize>,
        entry_page_size: usize,
        group_order: &str,
        group_page_size: usize,
    ) -> Result<Value, String> {
        let ordered = ordered_mounts(mounts, query_path, recursive, max_depth, group_order)?;
        let has_more_groups = ordered.len() > group_page_size;
        let mut groups = Vec::new();
        for (group_key, mount_path) in ordered.into_iter().take(group_page_size) {
            groups.push(Self::tree_group_envelope(
                &group_key,
                &mount_path,
                query_path,
                recursive,
                max_depth,
                entry_page_size,
                true,
            )?);
        }
        let mut body = Map::new();
        body.insert("path".into(), Value::String(query_path.to_string()));
        maybe_insert_b64(&mut body, "path_b64", query_path.as_bytes());
        body.insert("status".into(), Value::String("ok".into()));
        body.insert("group_order".into(), Value::String(group_order.to_string()));
        body.insert(
            "pit".into(),
            json!({
                "id": "oracle-pit",
                "expires_at_ms": 0u64,
            }),
        );
        body.insert("groups".into(), Value::Array(groups));
        body.insert(
            "group_page".into(),
            json!({
                "returned_groups": body["groups"].as_array().map_or(0, |v| v.len()),
                "has_more_groups": has_more_groups,
                "next_cursor": Value::Null,
                "next_entry_after": Value::Null,
            }),
        );
        Ok(Value::Object(body))
    }

    pub fn tree_group_envelope(
        group_key: &str,
        mount_path: &Path,
        query_path: &str,
        recursive: bool,
        max_depth: Option<usize>,
        entry_page_size: usize,
        force_find: bool,
    ) -> Result<Value, String> {
        let mut nodes = collect_nodes(mount_path, query_path, recursive, max_depth)?;
        nodes.sort_by(|a, b| a.path.cmp(&b.path));
        let query_root = normalize_logical_path(query_path);
        let mut has_children = std::collections::BTreeSet::<String>::new();
        for node in &nodes {
            if let Some(parent) = parent_logical_path(&node.path) {
                has_children.insert(parent);
            }
        }
        let root_node = nodes.iter().find(|node| node.path == query_root);
        let root = if let Some(root_node) = root_node {
            let mut obj = Map::new();
            obj.insert("path".into(), Value::String(root_node.path.clone()));
            maybe_insert_b64(&mut obj, "path_b64", root_node.path.as_bytes());
            obj.insert("size".into(), json!(root_node.size));
            obj.insert("modified_time_us".into(), json!(root_node.modified_time_us));
            obj.insert("is_dir".into(), json!(root_node.is_dir));
            obj.insert("exists".into(), json!(true));
            obj.insert(
                "has_children".into(),
                json!(has_children.contains(&root_node.path)),
            );
            Value::Object(obj)
        } else {
            let mut obj = Map::new();
            obj.insert("path".into(), Value::String(query_root.clone()));
            maybe_insert_b64(&mut obj, "path_b64", query_root.as_bytes());
            obj.insert("size".into(), json!(0u64));
            obj.insert("modified_time_us".into(), json!(0u64));
            obj.insert("is_dir".into(), json!(true));
            obj.insert("exists".into(), json!(false));
            obj.insert(
                "has_children".into(),
                json!(nodes.iter().any(|node| node.path != query_root)),
            );
            Value::Object(obj)
        };
        let mut entries = nodes
            .into_iter()
            .filter(|node| node.path != query_root)
            .map(|node| {
                let parent_path =
                    parent_logical_path(&node.path).unwrap_or_else(|| "/".to_string());
                let depth = if parent_path == "/" {
                    node.path
                        .trim_start_matches('/')
                        .split('/')
                        .filter(|seg| !seg.is_empty())
                        .count()
                } else {
                    node.path
                        .trim_start_matches('/')
                        .split('/')
                        .filter(|seg| !seg.is_empty())
                        .count()
                };
                let mut obj = Map::new();
                obj.insert("path".into(), Value::String(node.path.clone()));
                maybe_insert_b64(&mut obj, "path_b64", node.path.as_bytes());
                obj.insert("depth".into(), json!(depth));
                obj.insert("size".into(), json!(node.size));
                obj.insert("modified_time_us".into(), json!(node.modified_time_us));
                obj.insert("is_dir".into(), json!(node.is_dir));
                obj.insert(
                    "has_children".into(),
                    json!(has_children.contains(&node.path)),
                );
                Value::Object(obj)
            })
            .collect::<Vec<_>>();
        let has_more_entries = entries.len() > entry_page_size;
        if has_more_entries {
            entries.truncate(entry_page_size);
        }
        Ok(json!({
            "group": group_key,
            "status": "ok",
            "reliable": force_find,
            "unreliable_reason": if force_find { Value::Null } else { Value::String("Unattested".into()) },
            "stability": {
                "mode": "none",
                "state": "not-evaluated",
                "quiet_window_ms": null,
                "observed_quiet_for_ms": null,
                "remaining_ms": null,
                "blocked_reasons": [],
            },
            "meta": {
                "metadata_mode": "full",
                "metadata_available": true,
            },
            "root": root,
            "entries": entries,
            "entry_page": {
                "order": "path-lex",
                "page_size": entry_page_size,
                "returned_entries": entries.len(),
                "has_more_entries": has_more_entries,
                "next_cursor": Value::Null,
            }
        }))
    }

    pub fn tree_group_payload(
        mount_path: &Path,
        query_path: &str,
        recursive: bool,
        max_depth: Option<usize>,
        limit: usize,
    ) -> Result<Value, String> {
        let mut nodes = collect_nodes(mount_path, query_path, recursive, max_depth)?;
        nodes.sort_by(|a, b| {
            a.path
                .len()
                .cmp(&b.path.len())
                .then_with(|| a.path.cmp(&b.path))
        });
        let total_nodes = nodes.len();
        if nodes.len() > limit {
            nodes.truncate(limit);
        }
        Ok(json!({
            "status": "ok",
            "reliable": false,
            "unreliable_reason": "Unattested",
            "stability": {
                "mode": "none",
                "state": "not-evaluated",
                "quiet_window_ms": null,
                "observed_quiet_for_ms": null,
                "remaining_ms": null,
                "blocked_reasons": [],
            },
            "data": build_json_tree(&nodes, query_path),
            "meta": {
                "metadata_mode": "full",
                "metadata_available": true,
                "limit_applied": limit,
                "total_nodes": total_nodes,
                "returned_nodes": nodes.len(),
                "truncated": nodes.len() < total_nodes,
            },
            "partial_failure": false,
            "errors": [],
        }))
    }

    pub fn force_find_group_payload(
        mount_path: &Path,
        query_path: &str,
        recursive: bool,
        max_depth: Option<usize>,
        limit: usize,
    ) -> Result<Value, String> {
        let mut payload =
            Self::tree_group_payload(mount_path, query_path, recursive, max_depth, limit)?;
        if let Some(group) = payload.as_object_mut() {
            group.insert("reliable".into(), Value::Bool(true));
            group.insert("unreliable_reason".into(), Value::Null);
            group.insert(
                "stability".into(),
                json!({
                    "mode": "none",
                    "state": "not-evaluated",
                    "quiet_window_ms": null,
                    "observed_quiet_for_ms": null,
                    "remaining_ms": null,
                    "blocked_reasons": [],
                }),
            );
        }
        Ok(payload)
    }

    pub fn stats_group_payload(
        mount_path: &Path,
        query_path: &str,
        recursive: bool,
        max_depth: Option<usize>,
    ) -> Result<Value, String> {
        let nodes = collect_nodes(mount_path, query_path, recursive, max_depth)?;
        let mut total_files = 0u64;
        let mut total_dirs = 0u64;
        let mut total_size = 0u64;
        let mut latest_file_mtime_us = None::<u64>;
        for node in &nodes {
            if node.is_dir {
                total_dirs += 1;
            } else {
                total_files += 1;
                total_size += node.size;
                latest_file_mtime_us = Some(
                    latest_file_mtime_us.map_or(node.modified_time_us, |current| {
                        current.max(node.modified_time_us)
                    }),
                );
            }
        }
        Ok(json!({
            "status": "ok",
            "data": {
                "total_nodes": nodes.len() as u64,
                "total_files": total_files,
                "total_dirs": total_dirs,
                "total_size": total_size,
                "latest_file_mtime_us": latest_file_mtime_us,
                "attested_count": 0u64,
                "blind_spot_count": 0u64,
            },
            "partial_failure": false,
            "errors": [],
        }))
    }

    pub fn stats_response(
        mounts: &[(String, PathBuf)],
        query_path: &str,
        recursive: bool,
        max_depth: Option<usize>,
    ) -> Result<Value, String> {
        let mut groups = Map::new();
        for (group_key, mount_path) in mounts {
            groups.insert(
                group_key.clone(),
                Self::stats_group_payload(mount_path, query_path, recursive, max_depth)?,
            );
        }
        let mut body = Map::new();
        body.insert("path".into(), Value::String(query_path.to_string()));
        maybe_insert_b64(&mut body, "path_b64", query_path.as_bytes());
        body.insert("groups".into(), Value::Object(groups));
        Ok(Value::Object(body))
    }

    pub fn best_group_by_count(
        mounts: &[(String, PathBuf)],
        query_path: &str,
        recursive: bool,
        max_depth: Option<usize>,
    ) -> Result<Option<String>, String> {
        Self::best_group_by_metric(mounts, query_path, recursive, max_depth, |nodes| {
            nodes.iter().filter(|node| !node.is_dir).count() as u64
        })
    }

    pub fn best_group_by_age(
        mounts: &[(String, PathBuf)],
        query_path: &str,
        recursive: bool,
        max_depth: Option<usize>,
    ) -> Result<Option<String>, String> {
        Self::best_group_by_metric(mounts, query_path, recursive, max_depth, |nodes| {
            nodes
                .iter()
                .filter(|node| !node.is_dir)
                .map(|node| node.modified_time_us)
                .max()
                .unwrap_or(0)
        })
    }

    fn best_group_by_metric(
        mounts: &[(String, PathBuf)],
        query_path: &str,
        recursive: bool,
        max_depth: Option<usize>,
        metric: impl Fn(&[OracleNode]) -> u64,
    ) -> Result<Option<String>, String> {
        let mut candidates = Vec::<(String, u64)>::new();
        for (group_key, mount_path) in mounts {
            let nodes = collect_nodes(mount_path, query_path, recursive, max_depth)?;
            candidates.push((group_key.clone(), metric(&nodes)));
        }
        Ok(candidates
            .into_iter()
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
            .map(|(group_key, _)| group_key))
    }
}

fn b64(bytes: &[u8]) -> String {
    B64URL.encode(bytes)
}

fn maybe_insert_b64(map: &mut Map<String, Value>, key: &str, bytes: &[u8]) {
    if std::str::from_utf8(bytes).is_err() {
        map.insert(key.into(), Value::String(b64(bytes)));
    }
}

fn ordered_mounts(
    mounts: &[(String, PathBuf)],
    query_path: &str,
    recursive: bool,
    max_depth: Option<usize>,
    group_order: &str,
) -> Result<Vec<(String, PathBuf)>, String> {
    let mut ordered = mounts.to_vec();
    match group_order {
        "group-key" => ordered.sort_by(|a, b| a.0.cmp(&b.0)),
        "file-count" => {
            ordered.sort_by(|a, b| {
                let a_nodes =
                    collect_nodes(&a.1, query_path, recursive, max_depth).unwrap_or_default();
                let b_nodes =
                    collect_nodes(&b.1, query_path, recursive, max_depth).unwrap_or_default();
                let a_metric = a_nodes.iter().filter(|node| !node.is_dir).count() as u64;
                let b_metric = b_nodes.iter().filter(|node| !node.is_dir).count() as u64;
                b_metric.cmp(&a_metric).then_with(|| a.0.cmp(&b.0))
            });
        }
        "file-age" => {
            ordered.sort_by(|a, b| {
                let a_nodes =
                    collect_nodes(&a.1, query_path, recursive, max_depth).unwrap_or_default();
                let b_nodes =
                    collect_nodes(&b.1, query_path, recursive, max_depth).unwrap_or_default();
                let a_metric = a_nodes
                    .iter()
                    .filter(|node| !node.is_dir)
                    .map(|node| node.modified_time_us)
                    .max()
                    .unwrap_or(0);
                let b_metric = b_nodes
                    .iter()
                    .filter(|node| !node.is_dir)
                    .map(|node| node.modified_time_us)
                    .max()
                    .unwrap_or(0);
                b_metric.cmp(&a_metric).then_with(|| a.0.cmp(&b.0))
            });
        }
        other => {
            return Err(format!("unsupported group_order oracle: {other}"));
        }
    }
    Ok(ordered)
}

fn collect_nodes(
    mount_path: &Path,
    query_path: &str,
    recursive: bool,
    max_depth: Option<usize>,
) -> Result<Vec<OracleNode>, String> {
    let query_fs_path = fs_path_for_query(mount_path, query_path);
    if !query_fs_path.exists() {
        return Ok(Vec::new());
    }

    let mut nodes = Vec::new();
    let root_display_name = mount_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("/")
        .to_string();
    visit(
        &query_fs_path,
        query_path,
        &root_display_name,
        0,
        recursive,
        max_depth,
        &mut nodes,
    )?;
    Ok(nodes)
}

fn visit(
    fs_path: &Path,
    logical_path: &str,
    root_display_name: &str,
    depth: usize,
    recursive: bool,
    max_depth: Option<usize>,
    nodes: &mut Vec<OracleNode>,
) -> Result<(), String> {
    let meta =
        fs::metadata(fs_path).map_err(|e| format!("stat {} failed: {e}", fs_path.display()))?;
    nodes.push(OracleNode {
        path: normalize_logical_path(logical_path),
        file_name: if logical_path == "/" {
            root_display_name.to_string()
        } else {
            root_file_name_for_query_path(logical_path)
        },
        size: if meta.is_dir() { 0 } else { meta.len() },
        modified_time_us: modified_time_us(&meta).unwrap_or(0),
        is_dir: meta.is_dir(),
    });

    if !meta.is_dir() {
        return Ok(());
    }
    if !recursive {
        if depth > 0 {
            return Ok(());
        }
    } else if let Some(limit) = max_depth {
        if depth >= limit {
            return Ok(());
        }
    }

    let mut entries = fs::read_dir(fs_path)
        .map_err(|e| format!("read_dir {} failed: {e}", fs_path.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read_dir entry {} failed: {e}", fs_path.display()))?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let child_fs_path = entry.path();
        let child_name = entry.file_name().to_string_lossy().to_string();
        let child_logical_path = join_logical_path(logical_path, &child_name);
        visit(
            &child_fs_path,
            &child_logical_path,
            root_display_name,
            depth + 1,
            recursive,
            max_depth,
            nodes,
        )?;
    }
    Ok(())
}

fn fs_path_for_query(mount_path: &Path, query_path: &str) -> PathBuf {
    if query_path == "/" {
        return mount_path.to_path_buf();
    }
    mount_path.join(query_path.trim_start_matches('/'))
}

fn join_logical_path(parent: &str, child_name: &str) -> String {
    if parent == "/" {
        format!("/{child_name}")
    } else {
        format!("{}/{}", normalize_logical_path(parent), child_name)
    }
}

fn normalize_logical_path(path: &str) -> String {
    if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

fn build_json_tree(nodes: &[OracleNode], query_path: &str) -> Value {
    let mut node_index = std::collections::HashMap::<String, Map<String, Value>>::new();
    let mut file_name_by_path = std::collections::HashMap::<String, String>::new();
    let mut children_by_parent = std::collections::HashMap::<String, Vec<String>>::new();
    let mut root_paths = Vec::<String>::new();
    let mut root_seen = std::collections::BTreeSet::<String>::new();

    for node in nodes {
        let mut obj = Map::new();
        obj.insert("path".into(), Value::String(node.path.clone()));
        obj.insert("file_name".into(), Value::String(node.file_name.clone()));
        obj.insert("size".into(), Value::Number(node.size.into()));
        obj.insert(
            "modified_time_us".into(),
            Value::Number(node.modified_time_us.into()),
        );
        obj.insert("is_dir".into(), Value::Bool(node.is_dir));
        if node.is_dir {
            obj.insert("children".into(), Value::Object(Map::new()));
        }
        file_name_by_path.insert(node.path.clone(), node.file_name.clone());
        node_index.insert(node.path.clone(), obj);
    }

    for node in nodes {
        let parent = parent_logical_path(&node.path);
        let is_root = parent.is_none()
            || !parent
                .as_ref()
                .is_some_and(|parent_path| node_index.contains_key(parent_path));
        if is_root {
            if root_seen.insert(node.path.clone()) {
                root_paths.push(node.path.clone());
            }
            continue;
        }
        children_by_parent
            .entry(parent.expect("checked above"))
            .or_default()
            .push(node.path.clone());
    }

    let normalized_query_path = normalize_logical_path(query_path);
    if let Some(root_obj) = render_json_tree_node(
        &normalized_query_path,
        &node_index,
        &file_name_by_path,
        &children_by_parent,
    ) {
        return root_obj;
    }

    let mut synthetic_root = empty_tree_root(query_path);
    let Some(Value::Object(children)) = synthetic_root.get_mut("children") else {
        return Value::Object(synthetic_root);
    };
    for root_path in root_paths {
        let Some(root_name) = file_name_by_path.get(&root_path) else {
            continue;
        };
        let Some(root_obj) = render_json_tree_node(
            &root_path,
            &node_index,
            &file_name_by_path,
            &children_by_parent,
        ) else {
            continue;
        };
        children.insert(root_name.clone(), root_obj);
    }
    Value::Object(synthetic_root)
}

fn render_json_tree_node(
    path: &str,
    node_index: &std::collections::HashMap<String, Map<String, Value>>,
    file_name_by_path: &std::collections::HashMap<String, String>,
    children_by_parent: &std::collections::HashMap<String, Vec<String>>,
) -> Option<Value> {
    let mut obj = node_index.get(path)?.clone();
    if let Some(Value::Object(children)) = obj.get_mut("children") {
        if let Some(child_paths) = children_by_parent.get(path) {
            for child_path in child_paths {
                let Some(child_name) = file_name_by_path.get(child_path) else {
                    continue;
                };
                let Some(child_obj) = render_json_tree_node(
                    child_path,
                    node_index,
                    file_name_by_path,
                    children_by_parent,
                ) else {
                    continue;
                };
                children.insert(child_name.clone(), child_obj);
            }
        }
    }
    Some(Value::Object(obj))
}

fn parent_logical_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    let trimmed = path.trim_end_matches('/');
    if let Some(index) = trimmed.rfind('/') {
        if index == 0 {
            Some("/".to_string())
        } else {
            Some(trimmed[..index].to_string())
        }
    } else {
        None
    }
}

fn root_file_name_for_query_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() || trimmed == "/" {
        return "/".to_string();
    }
    trimmed
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or(trimmed)
        .to_string()
}

fn empty_tree_root(path: &str) -> Map<String, Value> {
    let mut root = Map::new();
    root.insert("path".into(), Value::String(normalize_logical_path(path)));
    root.insert(
        "file_name".into(),
        Value::String(root_file_name_for_query_path(path)),
    );
    root.insert("size".into(), Value::Number(0u64.into()));
    root.insert("modified_time_us".into(), Value::Number(0u64.into()));
    root.insert("is_dir".into(), Value::Bool(true));
    root.insert("children".into(), Value::Object(Map::new()));
    root
}

fn modified_time_us(meta: &fs::Metadata) -> Option<u64> {
    meta.modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_micros() as u64)
}
