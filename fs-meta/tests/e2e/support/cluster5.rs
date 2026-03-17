#![cfg(target_os = "linux")]
#![allow(dead_code)]

use super::api_client::FsMetaApiClient;
use super::control_protocol::{
    canonical_ctl_command_value_bytes, AnnouncedResourceExport, ControlEnvelope, CtlRequest,
    CtlResponse,
};
use super::runtime_admin::{
    canonical_runtime_admin_command_value_bytes, decode_runtime_admin_or_kernel_response_value,
    encode_runtime_admin_request_value,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use capanix_app_fs_meta::{
    api::config::{ApiAuthConfig, BootstrapAdminConfig},
    product::{build_release_doc_value, FsMetaReleaseSpec},
    RootSpec,
};
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEV_ADMIN_SIGNING_KEY_B64: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
const FULL_NODE_DELEGATION_SCOPES: &[&str] = &[
    "cluster_read",
    "metrics_read",
    "audit_read",
    "channel_read",
    "config_read",
    "config_write",
    "process_read",
    "tx_execute",
    "principal_admin",
    "boundary_compose",
    "announced_resource_read",
    "announced_resource_write",
    "boundary_read_self",
];
const NODE_NAMES: &[&str] = &["node-a", "node-b", "node-c", "node-d", "node-e"];

fn trace_command_name(command: &Value) -> String {
    command
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or("<unknown>")
        .to_string()
}

fn trace_exports_summary(command: &Value) -> String {
    command
        .get("exports")
        .and_then(Value::as_array)
        .map(|rows| format!(" exports={}", rows.len()))
        .unwrap_or_default()
}

#[derive(Clone)]
pub struct NodeIdentity {
    pub name: String,
    pub node_id: String,
    node_sk_b64: String,
    node_pk_b64: String,
}

pub struct RunningNode {
    pub name: String,
    pub node_id: String,
    pub bind_addr: String,
    pub home_dir: PathBuf,
    pub socket_path: PathBuf,
    stdout_log: PathBuf,
    stderr_log: PathBuf,
    child: Child,
}

pub struct Cluster5 {
    pub nodes: Vec<RunningNode>,
    pub identities: Vec<NodeIdentity>,
    pub admin_pub_b64: String,
    capanixd_bin: PathBuf,
}

impl Cluster5 {
    pub fn start() -> Result<Self, String> {
        let _guard = cluster_lock();
        let Some(capanixd_bin) = try_find_capanixd_bin() else {
            return Err(
                "capanixd binary not found; set CAPANIXD_BIN or build capanix-daemon".into(),
            );
        };
        let suffix = unique_suffix();
        let identities = NODE_NAMES
            .iter()
            .map(|name| generate_node_identity(name, suffix))
            .collect::<Vec<_>>();
        let admin_pub_b64 = admin_public_key_b64();
        let bind_addrs = reserve_distinct_bind_addrs(NODE_NAMES.len())?;
        let mut nodes = Vec::with_capacity(NODE_NAMES.len());
        for (index, identity) in identities.iter().enumerate() {
            let seeds = bind_addrs
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != index)
                .map(|(_, addr)| addr.clone())
                .collect::<Vec<_>>();
            let management_peers = identities
                .iter()
                .zip(bind_addrs.iter())
                .enumerate()
                .filter(|(i, _)| *i != index)
                .map(|(_, (peer, addr))| (peer.node_id.clone(), addr.clone()))
                .collect::<Vec<_>>();
            let all_refs = identities.iter().collect::<Vec<_>>();
            let cfg = build_cluster_config(
                identity,
                &all_refs,
                &seeds,
                &management_peers,
                &admin_pub_b64,
                FULL_NODE_DELEGATION_SCOPES,
            );
            let mut node = RunningNode::start(
                &capanixd_bin,
                identity,
                &cfg,
                &bind_addrs[index],
                true,
                true,
            )?;
            node.wait_for_socket(control_socket_startup_timeout())?;
            nodes.push(node);
        }
        let cluster = Self {
            nodes,
            identities,
            admin_pub_b64,
            capanixd_bin,
        };
        cluster.wait_cluster_ready(Duration::from_secs(120))?;
        Ok(cluster)
    }

    pub fn fs_meta_app_runtime_path(&self) -> Result<PathBuf, String> {
        try_find_fs_meta_app_cdylib().ok_or_else(|| {
            "fs-meta app runtime path not found; set CAPANIX_FS_META_APP_BINARY or build capanix-app-fs-meta-worker-facade"
                .to_string()
        })
    }

    pub fn node_id(&self, node_name: &str) -> Result<String, String> {
        self.identities
            .iter()
            .find(|id| id.name == node_name)
            .map(|id| id.node_id.clone())
            .ok_or_else(|| format!("unknown node {node_name}"))
    }

    pub fn ctl_ok(&self, node_name: &str, command: Value) -> Result<Value, String> {
        let node = self.node(node_name)?;
        let mut last_err = String::new();
        for _ in 0..3 {
            let seq = next_auth_seq_global();
            let nonce = format!("{}-{node_name}", unique_suffix());
            let auth = signed_auth_for_command(
                &command,
                FULL_NODE_DELEGATION_SCOPES,
                seq,
                &nonce,
                "local-admin-ed25519-1",
            );
            match request_ctl_ok(&node.socket_path, command.clone(), auth) {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if e.contains("\"code\":\"replay_detected\"") {
                        last_err = e;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        Err(last_err)
    }

    pub fn ctl_err_code(
        &self,
        node_name: &str,
        command: Value,
        scopes: &[&str],
    ) -> Result<String, String> {
        let node = self.node(node_name)?;
        let auth = signed_auth_for_command(
            &command,
            scopes,
            next_auth_seq_global(),
            &format!("{}-err", unique_suffix()),
            "local-admin-ed25519-1",
        );
        request_ctl_error_code(&node.socket_path, command, auth)
    }

    pub fn config_get(&self, node_name: &str) -> Result<Value, String> {
        self.ctl_ok(node_name, json!({"command": "config_get"}))
    }

    pub fn config_set(&self, node_name: &str, config: Value) -> Result<Value, String> {
        self.ctl_ok(
            node_name,
            json!({"command": "config_set", "config": config}),
        )
    }

    pub fn status(&self, node_name: &str) -> Result<Value, String> {
        let node = self.node(node_name)?;
        request_layered_local_status(&node.socket_path)
    }

    pub fn runtime_admin_ok(&self, node_name: &str, command: Value) -> Result<Value, String> {
        let node = self.node(node_name)?;
        let mut last_err = String::new();
        let command_name = trace_command_name(&command);
        let command_summary = trace_exports_summary(&command);
        for attempt in 0..3 {
            let seq = next_auth_seq_global();
            let nonce = format!("{}-{node_name}", unique_suffix());
            let auth = signed_auth_for_runtime_admin_command(
                &command,
                FULL_NODE_DELEGATION_SCOPES,
                seq,
                &nonce,
                "local-admin-ed25519-1",
            );
            let started = Instant::now();
            eprintln!(
                "cluster5: runtime-admin begin node={} command={} attempt={}{}",
                node_name,
                command_name,
                attempt + 1,
                command_summary
            );
            match request_runtime_admin_ok(&node.socket_path, command.clone(), auth) {
                Ok(v) => {
                    eprintln!(
                        "cluster5: runtime-admin ok node={} command={} attempt={} elapsed_ms={}{}",
                        node_name,
                        command_name,
                        attempt + 1,
                        started.elapsed().as_millis(),
                        command_summary
                    );
                    return Ok(v);
                }
                Err(e) => {
                    eprintln!(
                        "cluster5: runtime-admin err node={} command={} attempt={} elapsed_ms={}{} error={}",
                        node_name,
                        command_name,
                        attempt + 1,
                        started.elapsed().as_millis(),
                        command_summary,
                        e
                    );
                    if e.contains("\"code\":\"replay_detected\"") {
                        last_err = e;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        Err(last_err)
    }

    pub fn cluster_status(&self, node_name: &str) -> Result<Value, String> {
        let node = self.node(node_name)?;
        request_cluster_state(&node.socket_path)
    }

    pub fn apply_release(&self, node_name: &str, release: Value) -> Result<Value, String> {
        let node = self.node(node_name)?;
        let cnxctl_bin = try_find_cnxctl_bin().ok_or_else(|| {
            "cnxctl binary not found; build capanix-cli or set CNXCTL_BIN".to_string()
        })?;
        let app_id = release
            .get("app_id")
            .or_else(|| release.get("app_id"))
            .and_then(Value::as_str)
            .unwrap_or("<unknown>")
            .to_string();
        let generation = release
            .get("target_generation")
            .and_then(Value::as_i64)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "<unknown>".to_string());
        let file = write_temp_json("relation-target-apply", &release)?;
        let file_arg = file.to_string_lossy().to_string();
        let started = Instant::now();
        eprintln!(
            "cluster5: apply-release begin node={} app_id={} generation={}",
            node_name, app_id, generation
        );
        let result = run_cnxctl_json(
            &cnxctl_bin,
            &node.socket_path,
            ["app", "apply", file_arg.as_str()],
        );
        match &result {
            Ok(_) => eprintln!(
                "cluster5: apply-release ok node={} app_id={} generation={} elapsed_ms={}",
                node_name,
                app_id,
                generation,
                started.elapsed().as_millis()
            ),
            Err(err) => eprintln!(
                "cluster5: apply-release err node={} app_id={} generation={} elapsed_ms={} error={}",
                node_name,
                app_id,
                generation,
                started.elapsed().as_millis(),
                err
            ),
        }
        result
    }

    pub fn clear_release(&self, node_name: &str, app_id: &str) -> Result<Value, String> {
        let node = self.node(node_name)?;
        let cnxctl_bin = try_find_cnxctl_bin().ok_or_else(|| {
            "cnxctl binary not found; build capanix-cli or set CNXCTL_BIN".to_string()
        })?;
        run_cnxctl_json(
            &cnxctl_bin,
            &node.socket_path,
            ["config", "relation-target-clear", app_id],
        )
    }

    pub fn announce_resources(
        &self,
        node_name: &str,
        exports: Vec<Value>,
    ) -> Result<Value, String> {
        let exports = serde_json::from_value::<Vec<AnnouncedResourceExport>>(json!(exports))
            .map_err(|e| format!("decode announced exports: {e}"))?;
        self.runtime_admin_ok(
            node_name,
            json!({
                "command": "resource_export_announce",
                "exports": exports,
            }),
        )
    }

    pub fn announce_resources_clusterwide(&self, exports: Vec<Value>) -> Result<(), String> {
        let exports = serde_json::from_value::<Vec<AnnouncedResourceExport>>(json!(exports))
            .map_err(|e| format!("decode announced exports: {e}"))?;
        eprintln!(
            "cluster5: announce-resources-clusterwide begin nodes={} exports={}",
            self.nodes.len(),
            exports.len()
        );
        for node in &self.nodes {
            self.runtime_admin_ok(
                &node.name,
                json!({
                    "command": "resource_export_announce",
                    "exports": exports,
                }),
            )?;
        }
        eprintln!(
            "cluster5: announce-resources-clusterwide ok nodes={} exports={}",
            self.nodes.len(),
            exports.len()
        );
        Ok(())
    }

    pub fn withdraw_resources(
        &self,
        node_name: &str,
        withdraw_node_id: &str,
        resource_ids: Vec<String>,
    ) -> Result<Value, String> {
        self.runtime_admin_ok(
            node_name,
            json!({
                "command": "resource_export_withdraw",
                "node_id": withdraw_node_id,
                "resource_ids": resource_ids,
            }),
        )
    }

    pub fn withdraw_resources_clusterwide(
        &self,
        withdraw_node_id: &str,
        resource_ids: Vec<String>,
    ) -> Result<(), String> {
        for node in &self.nodes {
            self.runtime_admin_ok(
                &node.name,
                json!({
                    "command": "resource_export_withdraw",
                    "node_id": withdraw_node_id,
                    "resource_ids": resource_ids,
                }),
            )?;
        }
        Ok(())
    }

    pub fn wait_cluster_ready(&self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        let expected = self
            .identities
            .iter()
            .map(|n| n.node_id.clone())
            .collect::<Vec<_>>();
        loop {
            let mut all_ok = true;
            for node in &self.nodes {
                let status = request_cluster_state(&node.socket_path)?;
                for expected_id in &expected {
                    if !status_has_reachable_node(&status, expected_id) {
                        all_ok = false;
                        break;
                    }
                }
                if !all_ok {
                    break;
                }
            }
            if all_ok {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err("five-node cluster did not converge before timeout".into());
            }
            thread::sleep(Duration::from_millis(250));
        }
    }

    pub fn write_fs_meta_auth_files(&self, tag: &str) -> Result<(String, String), String> {
        let dir = std::env::temp_dir().join(format!("dx-fsmeta-auth-{tag}-{}", unique_suffix()));
        fs::create_dir_all(&dir)
            .map_err(|e| format!("create fs-meta auth temp dir failed: {e}"))?;
        let passwd_path = dir.join("fs-meta.passwd");
        let shadow_path = dir.join("fs-meta.shadow");
        fs::write(
            &passwd_path,
            "operator:1000:1000:fsmeta_management:/home/operator:/bin/bash:0\n",
        )
        .map_err(|e| format!("write fs-meta passwd fixture failed: {e}"))?;
        fs::write(&shadow_path, "operator:plain$operator123:0\n")
            .map_err(|e| format!("write fs-meta shadow fixture failed: {e}"))?;
        Ok((
            passwd_path.to_string_lossy().to_string(),
            shadow_path.to_string_lossy().to_string(),
        ))
    }

    pub fn build_fs_meta_release(
        &self,
        app_id: &str,
        facade_resource_id: &str,
        roots: Vec<RootSpec>,
        generation: i64,
        bootstrap_management: bool,
    ) -> Result<Value, String> {
        let app_path = self.fs_meta_app_runtime_path()?;
        let source_worker_path = try_find_fs_meta_source_worker_bin()
            .ok_or_else(|| "fs_meta_source_worker binary not found".to_string())?;
        let sink_worker_path = try_find_fs_meta_sink_worker_bin()
            .ok_or_else(|| "fs_meta_sink_worker binary not found".to_string())?;
        let (passwd_path, shadow_path) = self.write_fs_meta_auth_files(app_id)?;
        let query_keys_path = PathBuf::from(&passwd_path)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("query-keys.json");
        let mut auth = ApiAuthConfig {
            passwd_path: PathBuf::from(passwd_path),
            shadow_path: PathBuf::from(shadow_path),
            query_keys_path,
            session_ttl_secs: 3600,
            management_group: "fsmeta_management".to_string(),
            bootstrap_management: None,
        };
        if bootstrap_management {
            auth.bootstrap_management = Some(BootstrapAdminConfig {
                username: "operator".to_string(),
                password: "operator123".to_string(),
                uid: 1000,
                gid: 1000,
                home: "/home/operator".to_string(),
                shell: "/bin/bash".to_string(),
            });
        }
        let spec = FsMetaReleaseSpec {
            app_id: app_id.to_string(),
            api_facade_resource_id: facade_resource_id.to_string(),
            auth,
            roots,
        };
        let mut value = build_release_doc_value(&spec);
        value["target_generation"] = json!(generation);
        value["units"][0]["startup"]["path"] = json!(app_path.to_string_lossy().to_string());
        value["units"][0]["startup"]["manifest"] = json!(repo_root()
            .join("fs-meta/fixtures/manifests/capanix-app-fs-meta.yaml")
            .display()
            .to_string());
        value["units"][0]["config"]["workers"] = json!({
            "source": {
                "mode": "external",
                "binary_path": source_worker_path.display().to_string(),
            },
            "scan": {
                "mode": "external",
                "binary_path": source_worker_path.display().to_string(),
            },
            "sink": {
                "mode": "external",
                "binary_path": sink_worker_path.display().to_string(),
            }
        });
        value["units"][0]["version"] = json!(format!("real-nfs-{generation}"));
        value["units"][0]["restart_policy"] = json!("Never");
        value["units"][0]["policy"]["generation"] = json!(generation);
        value["units"][0]["policy"]["replicas"] = json!(1_i64);
        scope_unit_intent_to_scope_worker_intent(&value)
    }

    pub fn wait_http_login_ready(
        &self,
        base_urls: &[String],
        username: &str,
        password: &str,
        timeout: Duration,
    ) -> Result<String, String> {
        let deadline = Instant::now() + timeout;
        let mut last_err = String::new();
        loop {
            for base in base_urls {
                let client = FsMetaApiClient::new(base.clone())?;
                match client.login(username, password) {
                    Ok(_) => return Ok(base.clone()),
                    Err(e) => last_err = format!("{base}: {e}"),
                }
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "timeout waiting for facade login readiness: {last_err}"
                ));
            }
            thread::sleep(Duration::from_millis(250));
        }
    }

    pub fn managed_pids_for_instance(
        &self,
        node_name: &str,
        instance_id: &str,
    ) -> Result<BTreeSet<u32>, String> {
        let status = self.status(node_name)?;
        Ok(status
            .get("daemon")
            .and_then(|v| v.get("managed_processes"))
            .and_then(Value::as_array)
            .map(|rows| {
                rows.iter()
                    .filter(|row| {
                        row.get("instance_id").and_then(Value::as_str) == Some(instance_id)
                    })
                    .filter_map(|row| row.get("pid").and_then(Value::as_u64).map(|v| v as u32))
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default())
    }

    pub fn unit_active_pids_for_instance(
        &self,
        node_name: &str,
        instance_id: &str,
        unit_id: &str,
    ) -> Result<BTreeSet<u32>, String> {
        let status = self.status(node_name)?;
        let managed = self.managed_pids_for_instance(node_name, instance_id)?;
        let routes = status
            .get("daemon")
            .and_then(|v| v.get("activation"))
            .and_then(|v| v.get("routes"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut out = BTreeSet::new();
        for route in routes {
            let Some(apps) = route.get("apps").and_then(Value::as_array) else {
                continue;
            };
            for row in apps {
                let active_unit = row
                    .get("unit_ids")
                    .and_then(Value::as_array)
                    .is_some_and(|units| units.iter().any(|v| v.as_str() == Some(unit_id)));
                if !active_unit {
                    continue;
                }
                if row.get("delivered").and_then(Value::as_bool) != Some(true) {
                    continue;
                }
                if row.get("gate").and_then(Value::as_str) != Some("activated") {
                    continue;
                }
                if row.get("op").and_then(Value::as_str) != Some("activate") {
                    continue;
                }
                if let Some(pid) = row.get("pid").and_then(Value::as_u64).map(|v| v as u32) {
                    if managed.contains(&pid) {
                        out.insert(pid);
                    }
                }
            }
        }
        Ok(out)
    }

    pub fn facade_pids_for_instance(
        &self,
        node_name: &str,
        instance_id: &str,
    ) -> Result<BTreeSet<u32>, String> {
        self.unit_active_pids_for_instance(node_name, instance_id, "runtime.exec.facade")
    }

    pub fn kill_pid(&self, pid: u32) -> Result<(), String> {
        let status = Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status()
            .map_err(|e| format!("kill -9 {pid} failed to start: {e}"))?;
        if !status.success() {
            return Err(format!("kill -9 {pid} failed with status {status}"));
        }
        Ok(())
    }

    pub fn runtime_target_state(&self, node_name: &str) -> Result<Value, String> {
        let cfg = self.config_get(node_name)?;
        Ok(cfg
            .get("runtime_target_state")
            .cloned()
            .unwrap_or_else(|| json!({})))
    }

    pub fn node_active_processes(&self, node_name: &str) -> Result<u64, String> {
        let status = self.status(node_name)?;
        Ok(status
            .get("node")
            .and_then(|v| v.get("metrics"))
            .and_then(|v| v.get("active_processes"))
            .and_then(Value::as_u64)
            .unwrap_or(0))
    }

    pub fn daemon_pid(&self, node_name: &str) -> Result<u32, String> {
        Ok(self.node(node_name)?.child.id())
    }

    pub fn node(&self, node_name: &str) -> Result<&RunningNode, String> {
        self.nodes
            .iter()
            .find(|node| node.name == node_name)
            .ok_or_else(|| format!("unknown node {node_name}"))
    }
}

fn scope_unit_intent_to_scope_worker_intent(doc: &Value) -> Result<Value, String> {
    let units = doc
        .get("units")
        .and_then(Value::as_array)
        .ok_or_else(|| "scope-unit-intent doc missing units".to_string())?;
    let target_id = doc
        .get("target_id")
        .and_then(Value::as_str)
        .unwrap_or("target");
    let target_generation = doc
        .get("target_generation")
        .and_then(Value::as_i64)
        .unwrap_or(1);
    let workers = units
        .iter()
        .map(|entry| {
            let worker_role = entry
                .get("worker_role")
                .or_else(|| {
                    entry
                        .get("runtime")
                        .and_then(|runtime| runtime.get("worker_role"))
                })
                .and_then(Value::as_str)
                .unwrap_or("main");
            let worker_id = entry
                .get("unit_id")
                .or_else(|| entry.get("app"))
                .and_then(Value::as_str)
                .unwrap_or("worker");
            let startup_path = entry
                .get("startup")
                .and_then(|startup| startup.get("path"))
                .and_then(Value::as_str)
                .unwrap_or("app");
            let mut startup = serde_json::Map::new();
            startup.insert("path".into(), Value::String(startup_path.to_string()));
            if let Some(manifest) = entry
                .get("startup")
                .and_then(|startup| startup.get("manifest"))
                .cloned()
            {
                startup.insert("manifest".into(), manifest);
            }
            json!({
                "worker_role": worker_role,
                "worker_id": worker_id,
                "scope_ids": entry.get("scope_ids").cloned().unwrap_or_else(|| json!([])),
                "startup": Value::Object(startup),
                "config": entry.get("config").cloned().unwrap_or_else(|| json!({})),
                "runtime": entry.get("runtime").cloned().unwrap_or_else(|| json!({})),
                "policy": entry.get("policy").cloned().unwrap_or_else(|| json!({})),
                "restart_policy": entry
                    .get("restart_policy")
                    .cloned()
                    .unwrap_or_else(|| json!("Always")),
                "version": entry.get("version").cloned().unwrap_or_else(|| json!("dev")),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": "scope-worker-intent-v1",
        "target_id": target_id,
        "target_generation": target_generation,
        "workers": workers,
    }))
}

fn repo_root() -> PathBuf {
    crate::path_support::workspace_root()
}

fn capanix_repo_root() -> PathBuf {
    crate::path_support::capanix_repo_root()
}

fn cluster_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    match LOCK.get_or_init(|| Mutex::new(())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn tmp_dir(prefix: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    static NEXT_TMP: OnceLock<AtomicU64> = OnceLock::new();
    let seq = NEXT_TMP
        .get_or_init(|| AtomicU64::new(1))
        .fetch_add(1, Ordering::Relaxed);
    let short_prefix = prefix
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(8)
        .collect::<String>();
    let dir = std::env::temp_dir().join(format!("dx-{short_prefix}-{ts:x}-{seq:x}"));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn reserve_distinct_bind_addrs(count: usize) -> Result<Vec<String>, String> {
    let mut addrs = Vec::with_capacity(count);
    let mut used = BTreeSet::new();
    while addrs.len() < count {
        let socket =
            TcpListener::bind("127.0.0.1:0").map_err(|e| format!("bind tcp probe failed: {e}"))?;
        let port = socket
            .local_addr()
            .map_err(|e| format!("probe addr failed: {e}"))?
            .port();
        let addr = format!("127.0.0.1:{port}");
        if used.insert(addr.clone()) {
            addrs.push(addr);
        }
    }
    Ok(addrs)
}

fn log_excerpt(path: &Path) -> String {
    let Ok(content) = fs::read_to_string(path) else {
        return String::new();
    };
    let mut lines = content.lines().rev().take(20).collect::<Vec<_>>();
    lines.reverse();
    lines.join("\n")
}

fn latest_mtime(path: &Path) -> Option<SystemTime> {
    let mut latest = None;
    let mut stack = vec![path.to_path_buf()];
    while let Some(next) = stack.pop() {
        let meta = fs::metadata(&next).ok()?;
        if let Ok(modified) = meta.modified() {
            match latest {
                Some(current) if modified <= current => {}
                _ => latest = Some(modified),
            }
        }
        if meta.is_dir() {
            let entries = fs::read_dir(&next).ok()?;
            for entry in entries.flatten() {
                stack.push(entry.path());
            }
        }
    }
    latest
}

fn newest_mtime(paths: &[PathBuf]) -> Option<SystemTime> {
    let mut latest = None;
    for path in paths {
        let Some(modified) = latest_mtime(path) else {
            continue;
        };
        match latest {
            Some(current) if modified <= current => {}
            _ => latest = Some(modified),
        }
    }
    latest
}

fn should_rebuild_binary(binary: &Path, watch_paths: &[PathBuf]) -> bool {
    let Ok(meta) = fs::metadata(binary) else {
        return true;
    };
    let Ok(bin_modified) = meta.modified() else {
        return true;
    };
    match newest_mtime(watch_paths) {
        Some(src_modified) => src_modified > bin_modified,
        None => false,
    }
}

fn try_find_capanixd_bin() -> Option<PathBuf> {
    static BIN: OnceLock<Option<PathBuf>> = OnceLock::new();
    BIN.get_or_init(resolve_capanixd_bin).clone()
}

fn try_find_cnxctl_bin() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("CNXCTL_BIN") {
        let resolved = PathBuf::from(path);
        if resolved.exists() {
            return Some(resolved);
        }
    }

    let root = capanix_repo_root();
    let candidates = [
        root.join("target/debug/cnxctl"),
        root.join(".target/debug/cnxctl"),
    ];
    let watch_paths = vec![root.join("Cargo.lock"), root.join("crates")];
    if let Some(found) = candidates.iter().find(|p| p.exists()).cloned() {
        if !should_rebuild_binary(&found, &watch_paths) {
            return Some(found);
        }
    }

    for cargo_bin in [
        std::env::var("CARGO").ok().map(PathBuf::from),
        Some(PathBuf::from("cargo")),
        std::env::var("HOME")
            .ok()
            .map(|home| PathBuf::from(home).join(".cargo/bin/cargo")),
    ]
    .into_iter()
    .flatten()
    {
        let status = Command::new(&cargo_bin)
            .current_dir(&root)
            .arg("build")
            .arg("-p")
            .arg("capanix-cli")
            .arg("--bin")
            .arg("cnxctl")
            .status();
        let Ok(status) = status else {
            continue;
        };
        if !status.success() {
            continue;
        }
        if let Some(found) = candidates.iter().find(|p| p.exists()) {
            return Some(found.clone());
        }
    }

    candidates.iter().find(|p| p.exists()).cloned()
}

fn write_temp_json(prefix: &str, value: &Value) -> Result<PathBuf, String> {
    let path = std::env::temp_dir().join(format!("capanix-{prefix}-{}.json", unique_suffix()));
    let body = serde_json::to_vec_pretty(value)
        .map_err(|e| format!("serialize {prefix} json failed: {e}"))?;
    fs::write(&path, body).map_err(|e| format!("write {prefix} json failed: {e}"))?;
    Ok(path)
}

fn run_cnxctl_json<const N: usize>(
    cnxctl_bin: &Path,
    socket_path: &Path,
    args: [&str; N],
) -> Result<Value, String> {
    let output = Command::new(cnxctl_bin)
        .arg("-s")
        .arg(socket_path)
        .arg("--output")
        .arg("json")
        .arg("--admin-sk-b64")
        .arg(DEV_ADMIN_SIGNING_KEY_B64)
        .args(args)
        .output()
        .map_err(|e| format!("spawn cnxctl failed: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

    if !output.status.success() {
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        return Err(format!("cnxctl request failed: {detail}"));
    }

    let envelope: Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| format!("decode cnxctl json output failed: {e}; stdout={stdout}"))?;
    if envelope.get("status").and_then(Value::as_str) != Some("ok") {
        return Err(format!("cnxctl returned non-ok envelope: {envelope}"));
    }
    envelope
        .get("result")
        .cloned()
        .ok_or_else(|| format!("cnxctl json output missing result: {envelope}"))
}

fn resolve_capanixd_bin() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("CAPANIXD_BIN") {
        let resolved = PathBuf::from(path);
        if resolved.exists() {
            return Some(resolved);
        }
    }
    if let Ok(path) = std::env::var("DATANIXD_BIN") {
        let resolved = PathBuf::from(path);
        if resolved.exists() {
            return Some(resolved);
        }
    }
    let root = capanix_repo_root();
    let candidates = [
        root.join("target/debug/capanixd"),
        root.join(".target/debug/capanixd"),
    ];
    let watch_paths = vec![root.join("Cargo.lock"), root.join("crates")];
    if let Some(found) = candidates.iter().find(|p| p.exists()).cloned() {
        if !should_rebuild_binary(&found, &watch_paths) {
            return Some(found);
        }
    }
    for cargo_bin in [
        std::env::var("CARGO").ok().map(PathBuf::from),
        Some(PathBuf::from("cargo")),
        std::env::var("HOME")
            .ok()
            .map(|home| PathBuf::from(home).join(".cargo/bin/cargo")),
    ]
    .into_iter()
    .flatten()
    {
        let status = Command::new(&cargo_bin)
            .current_dir(&root)
            .arg("build")
            .arg("-p")
            .arg("capanix-daemon")
            .arg("--bin")
            .arg("capanixd")
            .status();
        let Ok(status) = status else {
            continue;
        };
        if !status.success() {
            continue;
        }
        if let Some(found) = candidates.iter().find(|p| p.exists()) {
            return Some(found.clone());
        }
    }
    candidates.iter().find(|p| p.exists()).cloned()
}

fn fs_meta_app_lib_filename() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "libcapanix_app_fs_meta_worker_facade.dylib"
    }
    #[cfg(target_os = "windows")]
    {
        "capanix_app_fs_meta_worker_facade.dll"
    }
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    {
        "libcapanix_app_fs_meta_worker_facade.so"
    }
}

fn try_find_fs_meta_app_cdylib() -> Option<PathBuf> {
    static BIN: OnceLock<Option<PathBuf>> = OnceLock::new();
    BIN.get_or_init(resolve_fs_meta_app_cdylib).clone()
}

fn resolve_fs_meta_app_cdylib() -> Option<PathBuf> {
    for name in ["CAPANIX_FS_META_APP_BINARY", "DATANIX_FS_META_APP_SO"] {
        if let Ok(path) = std::env::var(name) {
            let resolved = PathBuf::from(path);
            if resolved.exists() {
                return Some(resolved);
            }
        }
    }
    let root = repo_root();
    let lib_name = fs_meta_app_lib_filename();
    let candidates = [
        root.join("target/debug").join(lib_name),
        root.join(".target/debug").join(lib_name),
    ];
    let watch_paths = vec![
        root.join("Cargo.lock"),
        root.join("fs-meta/app/src"),
        root.join("fs-meta/worker-facade/src"),
        root.join("fs-meta/tests"),
        root.join("src"),
    ];
    if let Some(found) = candidates.iter().find(|p| p.exists()).cloned() {
        if !should_rebuild_binary(&found, &watch_paths) {
            return Some(found);
        }
    }
    for cargo_bin in [
        std::env::var("CARGO").ok().map(PathBuf::from),
        Some(PathBuf::from("cargo")),
        std::env::var("HOME")
            .ok()
            .map(|home| PathBuf::from(home).join(".cargo/bin/cargo")),
    ]
    .into_iter()
    .flatten()
    {
        let status = Command::new(&cargo_bin)
            .current_dir(&root)
            .arg("build")
            .arg("-p")
            .arg("capanix-app-fs-meta-worker-facade")
            .arg("--lib")
            .status();
        let Ok(status) = status else {
            continue;
        };
        if !status.success() {
            continue;
        }
        if let Some(found) = candidates.iter().find(|p| p.exists()) {
            return Some(found.clone());
        }
    }
    candidates.iter().find(|p| p.exists()).cloned()
}

fn try_find_fs_meta_source_worker_bin() -> Option<PathBuf> {
    static BIN: OnceLock<Option<PathBuf>> = OnceLock::new();
    BIN.get_or_init(|| resolve_fs_meta_worker_bin("source"))
        .clone()
}

fn try_find_fs_meta_sink_worker_bin() -> Option<PathBuf> {
    static BIN: OnceLock<Option<PathBuf>> = OnceLock::new();
    BIN.get_or_init(|| resolve_fs_meta_worker_bin("sink"))
        .clone()
}

fn resolve_fs_meta_worker_bin(kind: &str) -> Option<PathBuf> {
    let root = repo_root();
    let (bin_name, package_name, watch_paths) = match kind {
        "source" => (
            "fs_meta_source_worker",
            "capanix-app-fs-meta-worker-source",
            vec![
                root.join("Cargo.lock"),
                root.join("fs-meta/worker-source/src"),
                root.join("fs-meta/worker-source/Cargo.toml"),
                root.join("fs-meta/runtime-support/src"),
                root.join("fs-meta/runtime-support/Cargo.toml"),
                root.join("fs-meta/app/src/workers/source.rs"),
                root.join("fs-meta/app/src/workers/source_ipc.rs"),
                root.join("fs-meta/app/src/source"),
                root.join("fs-meta/app/src"),
                capanix_repo_root().join("Cargo.lock"),
                capanix_repo_root().join("crates/unit-sidecar/src"),
                capanix_repo_root().join("crates/unit-sidecar/Cargo.toml"),
            ],
        ),
        "sink" => (
            "fs_meta_sink_worker",
            "capanix-app-fs-meta-worker-sink",
            vec![
                root.join("Cargo.lock"),
                root.join("fs-meta/worker-sink/src"),
                root.join("fs-meta/worker-sink/Cargo.toml"),
                root.join("fs-meta/runtime-support/src"),
                root.join("fs-meta/runtime-support/Cargo.toml"),
                root.join("fs-meta/app/src/workers/sink.rs"),
                root.join("fs-meta/app/src/workers/sink_ipc.rs"),
                root.join("fs-meta/app/src/sink"),
                root.join("fs-meta/app/src"),
                capanix_repo_root().join("Cargo.lock"),
                capanix_repo_root().join("crates/unit-sidecar/src"),
                capanix_repo_root().join("crates/unit-sidecar/Cargo.toml"),
            ],
        ),
        _ => return None,
    };
    let candidates = [
        root.join("target/debug").join(bin_name),
        root.join(".target/debug").join(bin_name),
    ];
    if let Some(found) = candidates.iter().find(|p| p.exists()).cloned() {
        if !should_rebuild_binary(&found, &watch_paths) {
            return Some(found);
        }
    }
    for cargo_bin in [
        std::env::var("CARGO").ok().map(PathBuf::from),
        Some(PathBuf::from("cargo")),
        std::env::var("HOME")
            .ok()
            .map(|home| PathBuf::from(home).join(".cargo/bin/cargo")),
    ]
    .into_iter()
    .flatten()
    {
        let status = Command::new(&cargo_bin)
            .current_dir(&root)
            .arg("build")
            .arg("-p")
            .arg(package_name)
            .arg("--bin")
            .arg(bin_name)
            .status();
        let Ok(status) = status else {
            continue;
        };
        if !status.success() {
            continue;
        }
        if let Some(found) = candidates.iter().find(|p| p.exists()) {
            return Some(found.clone());
        }
    }
    candidates.iter().find(|p| p.exists()).cloned()
}

fn generate_node_identity(node_name: &str, suffix: u128) -> NodeIdentity {
    let rng = SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate node keypair");
    let keypair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("decode generated keypair");
    let node_id = format!("{node_name}-{suffix}");
    NodeIdentity {
        name: node_name.to_string(),
        node_id,
        node_sk_b64: B64.encode(pkcs8.as_ref()),
        node_pk_b64: B64.encode(keypair.public_key().as_ref()),
    }
}

fn admin_public_key_b64() -> String {
    let seed = B64
        .decode(DEV_ADMIN_SIGNING_KEY_B64)
        .expect("decode admin signing seed");
    let mut fixed = [0u8; 32];
    fixed.copy_from_slice(&seed);
    let keypair = Ed25519KeyPair::from_seed_unchecked(&fixed).expect("decode admin signing seed");
    B64.encode(keypair.public_key().as_ref())
}

fn sign_node_delegation(node: &NodeIdentity, delegation_scopes: &[&str]) -> String {
    let pkcs8 = B64
        .decode(&node.node_sk_b64)
        .expect("decode node signing key for delegation");
    let keypair = Ed25519KeyPair::from_pkcs8(&pkcs8).expect("decode node keypair");
    let scopes = delegation_scopes.join(",");
    let message = format!(
        "cnx.node_delegation.v1\0{}\0local\0local-admin-ed25519-1\0{}\01\01\0n1",
        node.node_id, scopes
    );
    B64.encode(keypair.sign(message.as_bytes()).as_ref())
}

fn build_cluster_config(
    local: &NodeIdentity,
    all_nodes: &[&NodeIdentity],
    seeds: &[String],
    management_peers: &[(String, String)],
    admin_pub_b64: &str,
    delegation_scopes: &[&str],
) -> String {
    let delegation_sig = sign_node_delegation(local, delegation_scopes);
    let mut out = String::new();
    out.push_str("schema_version: v5\n");
    out.push_str(&format!("node_id: {}\n", local.node_id));
    if seeds.is_empty() {
        out.push_str("seeds: []\n");
    } else {
        out.push_str("seeds:\n");
        for seed in seeds {
            out.push_str(&format!("  - \"{seed}\"\n"));
        }
    }
    if management_peers.is_empty() {
        out.push_str("management_peers: []\n");
    } else {
        out.push_str("management_peers:\n");
        for (peer_node_id, peer_addr) in management_peers {
            out.push_str("  - domain_id: local\n");
            out.push_str(&format!("    node_id: \"{peer_node_id}\"\n"));
            out.push_str(&format!("    addr: \"{peer_addr}\"\n"));
        }
    }
    out.push_str("trust_bundle:\n");
    out.push_str("  version: dev\n");
    out.push_str("  node_keys:\n");
    for node in all_nodes {
        out.push_str(&format!("    - key_id: {}-k1\n", node.node_id));
        out.push_str(&format!("      principal_id: {}\n", node.node_id));
        out.push_str("      algorithm: ed25519\n");
        out.push_str(&format!("      public_key: \"{}\"\n", node.node_pk_b64));
        out.push_str("      state: Active\n");
    }
    out.push_str("  management_domains:\n");
    out.push_str("    - domain_id: local\n");
    out.push_str("      admin_keys:\n");
    out.push_str("        - key_id: local-admin-ed25519-1\n");
    out.push_str("          principal_id: local-admin\n");
    out.push_str("          algorithm: ed25519\n");
    out.push_str(&format!("          public_key: \"{admin_pub_b64}\"\n"));
    out.push_str("          state: Active\n");
    out.push_str("  owner_keys: []\n");
    out.push_str("  ingress_keys: []\n");
    out.push_str("node_delegations:\n");
    out.push_str(&format!("  - node_id: \"{}\"\n", local.node_id));
    out.push_str("    domain_id: local\n");
    out.push_str("    admin_key_id: local-admin-ed25519-1\n");
    out.push_str("    granted_scopes:\n");
    for scope in delegation_scopes {
        out.push_str(&format!("      - {scope}\n"));
    }
    out.push_str("    epoch: 1\n");
    out.push_str("    seq: 1\n");
    out.push_str("    nonce: n1\n");
    out.push_str(&format!("    signature_b64: \"{delegation_sig}\"\n"));
    out.push_str("resource_limits:\n");
    out.push_str("  max_channels: 64\n");
    out.push_str("  max_processes: 64\n");
    out.push_str("  channel_buffer_size: 128\n");
    out.push_str("  max_audit_entries: 512\n");
    out
}

impl RunningNode {
    fn start(
        bin: &Path,
        node: &NodeIdentity,
        config_text: &str,
        bind_addr: &str,
        include_node_key: bool,
        include_admin_key: bool,
    ) -> Result<Self, String> {
        let home_dir = tmp_dir(&format!("cluster-{}", node.name));
        let config_path = home_dir.join("config.yaml");
        fs::write(&config_path, config_text).map_err(|e| format!("write config: {e}"))?;
        let stdout_log = home_dir.join("stdout.log");
        let stderr_log = home_dir.join("stderr.log");
        let stdout = File::create(&stdout_log).map_err(|e| format!("create stdout log: {e}"))?;
        let stderr = File::create(&stderr_log).map_err(|e| format!("create stderr log: {e}"))?;
        let mut cmd = Command::new(bin);
        cmd.arg("--config")
            .arg(&config_path)
            .arg("--bind")
            .arg(bind_addr)
            .env("CAPANIX_HOME", &home_dir)
            .env("DATANIX_HOME", &home_dir)
            .env("CAPANIX_DECLARE_QUORUM_TIMEOUT_MS", "15000")
            .env("DATANIX_DECLARE_QUORUM_TIMEOUT_MS", "15000")
            .env("RUST_LOG", "debug")
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        if include_node_key {
            cmd.env("CAPANIX_NODE_SK_B64", &node.node_sk_b64);
            cmd.env("DATANIX_NODE_SK_B64", &node.node_sk_b64);
        } else {
            cmd.env_remove("CAPANIX_NODE_SK_B64");
            cmd.env_remove("DATANIX_NODE_SK_B64");
        }
        if include_admin_key {
            cmd.env("CAPANIX_ADMIN_SK_B64", DEV_ADMIN_SIGNING_KEY_B64);
            cmd.env("DATANIX_ADMIN_SK_B64", DEV_ADMIN_SIGNING_KEY_B64);
        } else {
            cmd.env_remove("CAPANIX_ADMIN_SK_B64");
            cmd.env_remove("DATANIX_ADMIN_SK_B64");
        }
        let child = cmd.spawn().map_err(|e| format!("spawn capanixd: {e}"))?;
        Ok(Self {
            name: node.name.clone(),
            node_id: node.node_id.clone(),
            bind_addr: bind_addr.to_string(),
            home_dir: home_dir.clone(),
            socket_path: home_dir.join("core.sock"),
            stdout_log,
            stderr_log,
            child,
        })
    }

    fn wait_for_socket(&mut self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.socket_path.exists() {
                return Ok(());
            }
            if let Some(status) = self
                .child
                .try_wait()
                .map_err(|e| format!("wait child {}: {e}", self.node_id))?
            {
                return Err(format!(
                    "node {} exited before socket ready ({status}); stderr:\n{}",
                    self.node_id,
                    log_excerpt(&self.stderr_log)
                ));
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "node {} did not create control socket in {:?}; stdout:\n{}\nstderr:\n{}",
                    self.node_id,
                    timeout,
                    log_excerpt(&self.stdout_log),
                    log_excerpt(&self.stderr_log)
                ));
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.child.try_wait().ok().flatten()
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = fs::remove_file(&self.socket_path);
        let keep = std::env::var("DATANIX_KEEP_E2E_ARTIFACTS")
            .ok()
            .is_some_and(|raw| raw == "1" || raw.eq_ignore_ascii_case("true"));
        if keep {
            eprintln!(
                "keeping e2e node artifacts for {} at {}",
                self.name,
                self.home_dir.display()
            );
        } else {
            let _ = fs::remove_dir_all(&self.home_dir);
        }
    }
}

fn control_socket_startup_timeout() -> Duration {
    let secs = std::env::var("DATANIX_TEST_SOCKET_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(40);
    Duration::from_secs(secs.max(1))
}

fn unique_suffix() -> u128 {
    static NEXT_NONCE: OnceLock<AtomicU64> = OnceLock::new();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = NEXT_NONCE
        .get_or_init(|| AtomicU64::new(1))
        .fetch_add(1, Ordering::Relaxed) as u128;
    (ts << 24) ^ seq
}

fn request_cluster_state(socket_path: &Path) -> Result<Value, String> {
    let command = json!({ "command": "cluster_state_get" });
    let seq = next_auth_seq_global();
    let nonce = format!("{}-cluster-state", unique_suffix());
    let auth = signed_auth_for_command(
        &command,
        &["cluster_read"],
        seq,
        &nonce,
        "local-admin-ed25519-1",
    );
    let req = json!({ "command": command, "auth": auth, "forwarded": false });
    let ctl_req: CtlRequest =
        serde_json::from_value(req).map_err(|e| format!("decode ctl request: {e}"))?;
    let body = rmp_serde::to_vec_named(&ControlEnvelope::Command(ctl_req))
        .map_err(|e| format!("encode request envelope: {e}"))?;
    let mut stream =
        UnixStream::connect(socket_path).map_err(|e| format!("connect socket: {e}"))?;
    let n = body.len() as u32;
    stream
        .write_all(&n.to_le_bytes())
        .map_err(|e| format!("write request length: {e}"))?;
    stream
        .write_all(&body)
        .map_err(|e| format!("write request body: {e}"))?;
    stream.flush().map_err(|e| format!("flush request: {e}"))?;
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| format!("read response length: {e}"))?;
    let resp_len = u32::from_le_bytes(len_buf) as usize;
    let mut resp = vec![0u8; resp_len];
    stream
        .read_exact(&mut resp)
        .map_err(|e| format!("read response body: {e}"))?;
    let envelope: ControlEnvelope =
        rmp_serde::from_slice(&resp).map_err(|e| format!("decode response envelope: {e}"))?;
    let CtlResponse { result, error } = match envelope {
        ControlEnvelope::Result(r) => r,
        _ => return Err("unexpected non-result envelope".to_string()),
    };
    let payload = json!({ "result": result, "error": error });
    if payload.get("error").is_some_and(|e| !e.is_null()) {
        return Err(format!(
            "cluster_state_get returned error: {}",
            payload["error"]
        ));
    }
    payload
        .get("result")
        .cloned()
        .ok_or_else(|| "cluster_state_get missing result".to_string())
}

fn request_layered_local_status(socket_path: &Path) -> Result<Value, String> {
    let cluster = request_cluster_state(socket_path)?;
    let command = json!({ "command": "metrics_get" });
    let seq = next_auth_seq_global();
    let nonce = format!("{}-layered-metrics", unique_suffix());
    let auth = signed_auth_for_command(
        &command,
        &["metrics_read"],
        seq,
        &nonce,
        "local-admin-ed25519-1",
    );
    let payload = request_ctl(
        socket_path,
        &json!({ "command": command, "auth": auth, "forwarded": false }),
    )?;
    if payload.get("error").is_some_and(|e| !e.is_null()) {
        return Err(format!("metrics_get returned error: {}", payload["error"]));
    }
    let metrics = payload
        .get("result")
        .cloned()
        .ok_or_else(|| "metrics_get missing result".to_string())?;
    let daemon = metrics.get("daemon").cloned().unwrap_or_else(|| json!({}));
    Ok(json!({
        "cluster": cluster,
        "metrics": metrics,
        "daemon": daemon,
    }))
}

fn status_has_reachable_node(status: &Value, node_id: &str) -> bool {
    status
        .get("nodes")
        .and_then(Value::as_array)
        .is_some_and(|nodes| {
            nodes.iter().any(|n| {
                let direct = n.get("node_id").and_then(Value::as_str);
                let wrapped = n
                    .get("node_id")
                    .and_then(|x| x.get("0"))
                    .and_then(Value::as_str);
                let reachable = n.get("reachable").and_then(Value::as_bool).unwrap_or(false);
                (direct == Some(node_id) || wrapped == Some(node_id)) && reachable
            })
        })
}

fn request_ctl(socket_path: &Path, req: &Value) -> Result<Value, String> {
    let ctl_req: CtlRequest =
        serde_json::from_value(req.clone()).map_err(|e| format!("decode ctl request: {e}"))?;
    let body = rmp_serde::to_vec_named(&ControlEnvelope::Command(ctl_req))
        .map_err(|e| format!("encode request envelope: {e}"))?;
    let mut stream =
        UnixStream::connect(socket_path).map_err(|e| format!("connect socket: {e}"))?;
    let n = body.len() as u32;
    stream
        .write_all(&n.to_le_bytes())
        .map_err(|e| format!("write request length: {e}"))?;
    stream
        .write_all(&body)
        .map_err(|e| format!("write request body: {e}"))?;
    stream.flush().map_err(|e| format!("flush request: {e}"))?;
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| format!("read response length: {e}"))?;
    let resp_len = u32::from_le_bytes(len_buf) as usize;
    let mut resp = vec![0u8; resp_len];
    stream
        .read_exact(&mut resp)
        .map_err(|e| format!("read response body: {e}"))?;
    let envelope: ControlEnvelope =
        rmp_serde::from_slice(&resp).map_err(|e| format!("decode response envelope: {e}"))?;
    let CtlResponse { result, error } = match envelope {
        ControlEnvelope::Result(r) => r,
        _ => return Err("unexpected non-result envelope".to_string()),
    };
    Ok(json!({ "result": result, "error": error }))
}

fn request_ctl_ok(socket_path: &Path, command: Value, auth: Value) -> Result<Value, String> {
    let req = json!({ "command": command, "auth": auth, "forwarded": false });
    let payload = request_ctl(socket_path, &req)?;
    if let Some(err) = payload.get("error").filter(|e| !e.is_null()) {
        return Err(format!("control returned error: {err}"));
    }
    payload
        .get("result")
        .cloned()
        .ok_or_else(|| "control response missing result".to_string())
}

fn request_runtime_admin(socket_path: &Path, req: &Value) -> Result<Value, String> {
    let body = encode_runtime_admin_request_value(req.clone())
        .map_err(|e| format!("encode runtime-admin request envelope: {e}"))?;
    let mut stream =
        UnixStream::connect(socket_path).map_err(|e| format!("connect socket: {e}"))?;
    let n = body.len() as u32;
    stream
        .write_all(&n.to_le_bytes())
        .map_err(|e| format!("write request length: {e}"))?;
    stream
        .write_all(&body)
        .map_err(|e| format!("write request body: {e}"))?;
    stream.flush().map_err(|e| format!("flush request: {e}"))?;
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| format!("read response length: {e}"))?;
    let resp_len = u32::from_le_bytes(len_buf) as usize;
    let mut resp = vec![0u8; resp_len];
    stream
        .read_exact(&mut resp)
        .map_err(|e| format!("read response body: {e}"))?;
    let CtlResponse { result, error } = serde_json::from_value(
        decode_runtime_admin_or_kernel_response_value(&resp)
            .map_err(|e| format!("decode runtime-admin response envelope: {e}"))?,
    )
    .map_err(|e| format!("decode runtime-admin response payload: {e}"))?;
    Ok(json!({ "result": result, "error": error }))
}

fn request_runtime_admin_ok(
    socket_path: &Path,
    command: Value,
    auth: Value,
) -> Result<Value, String> {
    let req = json!({ "command": command, "auth": auth, "forwarded": false });
    let payload = request_runtime_admin(socket_path, &req)?;
    if let Some(err) = payload.get("error").filter(|e| !e.is_null()) {
        return Err(format!("runtime admin returned error: {err}"));
    }
    payload
        .get("result")
        .cloned()
        .ok_or_else(|| "runtime admin response missing result".to_string())
}

fn request_ctl_error_code(
    socket_path: &Path,
    command: Value,
    auth: Value,
) -> Result<String, String> {
    let req = json!({ "command": command, "auth": auth, "forwarded": false });
    let payload = request_ctl(socket_path, &req)?;
    payload
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .ok_or_else(|| format!("expected control error code, got: {payload}"))
}

fn canonicalize_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(String, serde_json::Value)> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut canonical = serde_json::Map::new();
            for (k, v) in entries {
                canonical.insert(k, canonicalize_value(v));
            }
            serde_json::Value::Object(canonical)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(canonicalize_value).collect())
        }
        other => other,
    }
}

fn signed_auth_for_payload_bytes(
    command_bytes: Vec<u8>,
    scopes: &[&str],
    seq: u64,
    nonce: &str,
    key_id: &str,
) -> Value {
    let payload_sha256 = format!("{:x}", Sha256::digest(command_bytes));
    let seed = B64
        .decode(DEV_ADMIN_SIGNING_KEY_B64)
        .expect("decode admin signing seed");
    let mut fixed = [0u8; 32];
    fixed.copy_from_slice(&seed);
    let keypair = Ed25519KeyPair::from_seed_unchecked(&fixed).expect("decode admin signing seed");
    let message = format!(
        "cnx.platform.v1\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
        "local", "test-admin", key_id, "ed25519", payload_sha256, 1, seq, nonce
    );
    let signature_b64 = B64.encode(keypair.sign(message.as_bytes()).as_ref());
    json!({
        "actor_id": "test-admin",
        "domain_id": "local",
        "key_id": key_id,
        "algorithm": "ed25519",
        "epoch": 1,
        "seq": seq,
        "nonce": nonce,
        "payload_sha256": payload_sha256,
        "signature_b64": signature_b64,
        "scopes": scopes,
    })
}

fn signed_auth_for_command(
    command: &Value,
    scopes: &[&str],
    seq: u64,
    nonce: &str,
    key_id: &str,
) -> Value {
    let command_bytes = canonical_ctl_command_value_bytes(&command)
        .expect("serialize canonicalized control command");
    signed_auth_for_payload_bytes(command_bytes, scopes, seq, nonce, key_id)
}

fn signed_auth_for_runtime_admin_command(
    command: &Value,
    scopes: &[&str],
    seq: u64,
    nonce: &str,
    key_id: &str,
) -> Value {
    let command_bytes = canonical_runtime_admin_command_value_bytes(command)
        .expect("serialize typed runtime admin command");
    signed_auth_for_payload_bytes(command_bytes, scopes, seq, nonce, key_id)
}

fn next_auth_seq_global() -> u64 {
    static NEXT: OnceLock<AtomicU64> = OnceLock::new();
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(1);
    NEXT.get_or_init(|| AtomicU64::new(seed))
        .fetch_add(1, Ordering::Relaxed)
}
