use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use capanix_kernel_api::control::CtlCommand as KernelCtlCommand;
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::control_protocol::{
    canonical_ctl_command_value_bytes, ControlEnvelope, CtlRequest, CtlResponse,
};
use super::runtime_admin::{
    canonical_runtime_admin_command_value_bytes, decode_runtime_admin_or_kernel_response_value,
    encode_runtime_admin_request_value,
};

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
];

fn configure_test_child_process(cmd: &mut Command) {
    cmd.process_group(0);
    unsafe {
        cmd.pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::getppid() == 1 {
                return Err(std::io::Error::other(
                    "parent exited before capanixd child completed exec",
                ));
            }
            Ok(())
        });
    }
}

#[derive(Clone)]
struct NodeIdentity {
    node_id: String,
    node_sk_b64: String,
    node_pk_b64: String,
}

struct RunningNode {
    node_id: String,
    socket_path: PathBuf,
    stdout_log: PathBuf,
    stderr_log: PathBuf,
    child: Child,
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
    // Keep UNIX socket paths under platform sun_path limits (macOS is strict).
    let short_prefix = prefix
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(8)
        .collect::<String>();
    let dir = std::env::temp_dir().join(format!("dx-{short_prefix}-{ts:x}-{seq:x}"));
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn reserve_bind_addr() -> String {
    let socket = TcpListener::bind("127.0.0.1:0").expect("bind tcp probe");
    let port = socket.local_addr().expect("probe addr").port();
    format!("127.0.0.1:{port}")
}

fn reserve_distinct_bind_addrs() -> Result<(String, String), String> {
    let addr_a = reserve_bind_addr();
    let mut addr_b = reserve_bind_addr();
    for _ in 0..16 {
        if addr_b != addr_a {
            break;
        }
        addr_b = reserve_bind_addr();
    }
    if addr_b == addr_a {
        return Err("failed to reserve distinct bind addrs".to_string());
    }
    Ok((addr_a, addr_b))
}

fn is_transport_bind_conflict(err: &str) -> bool {
    err.contains("failed to bind transport")
        || err.contains("Address already in use")
        || err.contains("addr_in_use")
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
        let meta = match fs::metadata(&next) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if let Ok(modified) = meta.modified() {
            match latest {
                Some(current) if modified <= current => {}
                _ => latest = Some(modified),
            }
        }
        if meta.is_dir() {
            let entries = match fs::read_dir(&next) {
                Ok(v) => v,
                Err(_) => continue,
            };
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

    let mut cargo_bins = Vec::<PathBuf>::new();
    if let Ok(bin) = std::env::var("CARGO") {
        cargo_bins.push(PathBuf::from(bin));
    }
    cargo_bins.push(PathBuf::from("cargo"));
    if let Ok(home) = std::env::var("HOME") {
        cargo_bins.push(PathBuf::from(home).join(".cargo/bin/cargo"));
    }

    for cargo_bin in cargo_bins {
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

fn control_socket_startup_timeout() -> Duration {
    let secs = std::env::var("DATANIX_TEST_SOCKET_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        // Cluster specs boot two daemons + compile/link paths; 20s is occasionally too tight on CI-like hosts.
        .unwrap_or(40);
    Duration::from_secs(secs.max(1))
}

fn first_existing_env_path(names: &[&str]) -> Option<PathBuf> {
    names.iter().find_map(|name| {
        std::env::var(name).ok().and_then(|raw| {
            let resolved = PathBuf::from(raw);
            resolved.exists().then_some(resolved)
        })
    })
}

fn test_app_lib_filename() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "libcapanix_app_test_runtime.dylib"
    }
    #[cfg(target_os = "windows")]
    {
        "capanix_app_test_runtime.dll"
    }
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    {
        "libcapanix_app_test_runtime.so"
    }
}

fn try_find_test_app_cdylib() -> Option<PathBuf> {
    static BIN: OnceLock<Option<PathBuf>> = OnceLock::new();
    BIN.get_or_init(resolve_test_app_cdylib).clone()
}

fn resolve_test_app_cdylib() -> Option<PathBuf> {
    if let Some(path) = first_existing_env_path(&["CAPANIX_TEST_APP_BINARY", "DATANIX_TEST_APP_SO"])
    {
        return Some(path);
    }

    let root = repo_root();
    let lib_name = test_app_lib_filename();
    let fixture_manifest = root
        .join("fs-meta")
        .join("fixtures")
        .join("apps")
        .join("app-test-runtime")
        .join("Cargo.toml");
    let candidates = [
        root.join("target/debug").join(lib_name),
        root.join(".target/debug").join(lib_name),
        root.join("fs-meta")
            .join("fixtures")
            .join("apps")
            .join("app-test-runtime")
            .join("target/debug")
            .join(lib_name),
        root.join("fs-meta")
            .join("fixtures")
            .join("apps")
            .join("app-test-runtime")
            .join("target/debug/deps")
            .join(lib_name),
    ];
    let watch_paths = vec![
        root.join("Cargo.lock"),
        root.join("fs-meta/fixtures/apps/app-test-runtime"),
        root.join("fs-meta/app/src"),
        root.join("fs-meta/tests"),
        root.join("src"),
    ];

    if let Some(found) = candidates.iter().find(|p| p.exists()).cloned() {
        if !should_rebuild_binary(&found, &watch_paths) {
            return Some(found);
        }
    }

    let Ok(status) = Command::new("cargo")
        .current_dir(&root)
        .arg("build")
        .arg("--manifest-path")
        .arg(fixture_manifest)
        .arg("--lib")
        .status()
    else {
        return None;
    };
    if !status.success() {
        return None;
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
    if let Some(path) =
        first_existing_env_path(&["CAPANIX_FS_META_APP_BINARY", "DATANIX_FS_META_APP_SO"])
    {
        return Some(path);
    }

    let root = repo_root();
    let lib_name = fs_meta_app_lib_filename();
    let candidates = [
        root.join("target/debug").join(lib_name),
        root.join(".target/debug").join(lib_name),
        root.join("fs-meta")
            .join("fixtures")
            .join("apps")
            .join("app-test-runtime")
            .join("target/debug")
            .join(lib_name),
    ];
    let watch_paths = vec![
        root.join("Cargo.lock"),
        root.join("fs-meta/app/src"),
        root.join("fs-meta/worker-facade/src"),
        root.join("fs-meta/worker-source/src"),
        root.join("fs-meta/worker-source/Cargo.toml"),
        root.join("fs-meta/worker-sink/src"),
        root.join("fs-meta/worker-sink/Cargo.toml"),
        root.join("fs-meta/worker-scan/src"),
        root.join("fs-meta/worker-scan/Cargo.toml"),
        root.join("fs-meta/tests"),
        root.join("src"),
    ];

    if let Some(found) = candidates.iter().find(|p| p.exists()).cloned() {
        if !should_rebuild_binary(&found, &watch_paths) {
            return Some(found);
        }
    }

    let mut cargo_bins = Vec::<PathBuf>::new();
    if let Ok(bin) = std::env::var("CARGO") {
        cargo_bins.push(PathBuf::from(bin));
    }
    cargo_bins.push(PathBuf::from("cargo"));
    if let Ok(home) = std::env::var("HOME") {
        cargo_bins.push(PathBuf::from(home).join(".cargo/bin/cargo"));
    }

    for cargo_bin in cargo_bins {
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

fn generate_node_identity(node_id: &str) -> NodeIdentity {
    let rng = SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate node keypair");
    let keypair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("decode generated keypair");
    NodeIdentity {
        node_id: node_id.to_string(),
        node_sk_b64: B64.encode(pkcs8.as_ref()),
        node_pk_b64: B64.encode(keypair.public_key().as_ref()),
    }
}

fn admin_public_key_b64() -> String {
    let seed = B64
        .decode(DEV_ADMIN_SIGNING_KEY_B64)
        .expect("decode admin signing seed");
    assert_eq!(seed.len(), 32, "admin signing seed must be 32 bytes");
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
    out.push_str("  max_channels: 16\n");
    out.push_str("  max_processes: 16\n");
    out.push_str("  channel_buffer_size: 64\n");
    out.push_str("  max_audit_entries: 64\n");
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
        let home_dir = tmp_dir(&format!("cluster-{}", node.node_id));
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
            .env("CAPANIX_DECLARE_QUORUM_TIMEOUT_MS", "15000")
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
        configure_test_child_process(&mut cmd);
        let child = cmd.spawn().map_err(|e| format!("spawn capanixd: {e}"))?;
        Ok(Self {
            node_id: node.node_id.clone(),
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
        if self.child.try_wait().ok().flatten().is_some() {
            return;
        }
        let _ = unsafe { libc::killpg(self.child.id() as i32, libc::SIGKILL) };
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn dummy_auth() -> Value {
    json!({
        "actor_id": "public-read",
        "domain_id": "public-read",
        "key_id": "public-read",
        "algorithm": "none",
        "epoch": 1,
        "seq": 1,
        "nonce": "n1",
        "payload_sha256": "00",
        "signature_b64": "00",
        "scopes": [],
    })
}

fn next_auth_seq_for_socket(socket_path: &Path) -> u64 {
    if socket_path.to_string_lossy().contains("node-b") {
        next_auth_seq_b()
    } else {
        next_auth_seq_a()
    }
}

fn request_cluster_state(socket_path: &Path) -> Result<Value, String> {
    let command = json!({ "command": "cluster_state_get" });
    let seq = next_auth_seq_for_socket(socket_path);
    let nonce = format!("{}-cluster-state", unique_suffix());
    let auth = signed_auth_for_command(
        &command,
        &["cluster_read"],
        seq,
        &nonce,
        "local-admin-ed25519-1",
    );
    let req = json!({
        "command": command,
        "auth": auth,
        "forwarded": false,
    });
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
    let payload = json!({
        "result": result,
        "error": error,
    });
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

fn request_layered_status(socket_path: &Path, target: Value) -> Result<Value, String> {
    let cluster_command = json!({ "command": "cluster_state_get" });
    let cluster_auth = signed_auth_for_command(
        &cluster_command,
        &["cluster_read"],
        next_auth_seq_for_socket(socket_path),
        &format!("{}-layered-cluster", unique_suffix()),
        "local-admin-ed25519-1",
    );
    let cluster = request_ctl_ok(socket_path, cluster_command, cluster_auth, target.clone())?;

    let metrics_command = json!({ "command": "metrics_get" });
    let metrics_auth = signed_auth_for_command(
        &metrics_command,
        &["metrics_read"],
        next_auth_seq_for_socket(socket_path),
        &format!("{}-layered-metrics", unique_suffix()),
        "local-admin-ed25519-1",
    );
    let metrics = request_ctl_ok(socket_path, metrics_command, metrics_auth, target)?;
    let daemon = metrics.get("daemon").cloned().unwrap_or_else(|| json!({}));

    Ok(json!({
        "cluster": cluster,
        "metrics": metrics,
        "daemon": daemon,
    }))
}

fn status_has_node(status: &Value, node_id: &str) -> bool {
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
                direct == Some(node_id) || wrapped == Some(node_id)
            })
        })
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

fn status_node_count(status: &Value) -> usize {
    status
        .get("summary")
        .and_then(|s| s.get("node_count"))
        .and_then(Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or_else(|| {
            status
                .get("nodes")
                .and_then(Value::as_array)
                .map(|v| v.len())
                .unwrap_or(0)
        })
}

fn wait_cluster_ready(node_a: &mut RunningNode, node_b: &mut RunningNode) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last_a = String::new();
    let mut last_b = String::new();
    loop {
        if let Some(status) = node_a.try_wait() {
            return Err(format!(
                "node {} exited while waiting for cluster convergence ({status}); stderr:\n{}",
                node_a.node_id,
                log_excerpt(&node_a.stderr_log)
            ));
        }
        if let Some(status) = node_b.try_wait() {
            return Err(format!(
                "node {} exited while waiting for cluster convergence ({status}); stderr:\n{}",
                node_b.node_id,
                log_excerpt(&node_b.stderr_log)
            ));
        }
        let status_a = request_cluster_state(&node_a.socket_path);
        let status_b = request_cluster_state(&node_b.socket_path);
        if let Ok(sa) = &status_a {
            let a_sees_both = status_has_reachable_node(sa, &node_a.node_id)
                && status_has_reachable_node(sa, &node_b.node_id);
            if let Ok(sb) = &status_b {
                let b_sees_both = status_has_reachable_node(sb, &node_b.node_id)
                    && status_has_reachable_node(sb, &node_a.node_id);
                let a_self = status_has_reachable_node(sa, &node_a.node_id);
                let b_self = status_has_reachable_node(sb, &node_b.node_id);
                if (a_sees_both && b_self) || (b_sees_both && a_self) {
                    return Ok(());
                }
            } else if let Err(e) = &status_b {
                last_b = e.clone();
            }
            last_a = sa.to_string();
        } else if let Err(e) = &status_a {
            last_a = e.clone();
        }
        if let Ok(sb) = &status_b {
            last_b = sb.to_string();
        } else if let Err(e) = &status_b {
            last_b = e.clone();
        }
        if Instant::now() > deadline {
            return Err(format!(
                "two-node cluster did not converge in time\nnode-a status: {last_a}\nnode-b status: {last_b}"
            ));
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn wait_fail_closed(node: &mut RunningNode) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        if let Some(status) = node.try_wait() {
            if status.success() {
                return Err(format!(
                    "node {} unexpectedly succeeded without identity material",
                    node.node_id
                ));
            }
            if node.socket_path.exists() {
                return Err(format!(
                    "node {} must fail before control socket appears",
                    node.node_id
                ));
            }
            return Ok(());
        }
        if node.socket_path.exists() {
            return Err(format!(
                "node {} exposed control socket despite missing bootstrap identity",
                node.node_id
            ));
        }
        if Instant::now() > deadline {
            return Err(format!(
                "node {} did not fail-closed in time; stderr:\n{}",
                node.node_id,
                log_excerpt(&node.stderr_log)
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
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

fn local_target() -> Value {
    json!({ "scope": "local" })
}

fn cluster_target() -> Value {
    json!({ "scope": "cluster" })
}

fn is_auth_rejection(code: &str) -> bool {
    matches!(
        code,
        "invalid_auth_context" | "signature_invalid" | "access_denied" | "scope_denied"
    )
}

fn request_ctl(socket_path: &Path, req: &Value) -> Result<Value, String> {
    let req = normalize_request(req.clone());
    let is_kernel_control = req
        .get("command")
        .cloned()
        .and_then(|command| serde_json::from_value::<KernelCtlCommand>(command).ok())
        .is_some();
    let body = if is_kernel_control {
        let ctl_req = serde_json::from_value::<CtlRequest>(req.clone())
            .map_err(|e| format!("decode control request: {e}"))?;
        rmp_serde::to_vec_named(&ControlEnvelope::Command(ctl_req))
            .map_err(|e| format!("encode request envelope: {e}"))?
    } else {
        encode_runtime_admin_request_value(req.clone())
            .map_err(|e| format!("encode runtime-admin request envelope: {e}"))?
    };
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
    let CtlResponse { result, error } =
        if let Ok(envelope) = rmp_serde::from_slice::<ControlEnvelope>(&resp) {
            match envelope {
                ControlEnvelope::Result(r) => r,
                _ => return Err("unexpected non-result control envelope".to_string()),
            }
        } else {
            serde_json::from_value(
                decode_runtime_admin_or_kernel_response_value(&resp)
                    .map_err(|e| format!("decode runtime-admin response envelope: {e}"))?,
            )
            .map_err(|e| format!("decode runtime-admin response payload: {e}"))?
        };
    Ok(json!({
        "result": result,
        "error": error,
    }))
}

fn normalize_request(req: Value) -> Value {
    let mut req_obj = match req {
        Value::Object(map) => map,
        other => return other,
    };
    let Some(command) = req_obj.get("command").cloned() else {
        return Value::Object(req_obj);
    };
    let adjusted_command = normalize_control_command(command.clone())
        .or_else(|| normalize_runtime_admin_command(command.clone()))
        .unwrap_or(command);
    if adjusted_command == *req_obj.get("command").unwrap_or(&Value::Null) {
        return Value::Object(req_obj);
    }
    let auth = req_obj.get("auth").cloned().unwrap_or_else(dummy_auth);
    let adjusted_auth = resign_auth_for_command(&adjusted_command, auth);
    req_obj.insert("command".into(), adjusted_command);
    req_obj.insert("auth".into(), adjusted_auth);
    Value::Object(req_obj)
}

fn normalize_control_command(command: Value) -> Option<Value> {
    let mut command_obj = command.as_object()?.clone();
    if command_obj.get("command").and_then(Value::as_str) != Some("config_set") {
        return None;
    }
    let config = command_obj.get_mut("config")?.as_object_mut()?;
    let apps = config.get_mut("apps")?.as_array_mut()?;
    for app in apps {
        let Some(app_obj) = app.as_object_mut() else {
            continue;
        };
        let Some(app_cfg) = app_obj.get_mut("config").and_then(Value::as_object_mut) else {
            continue;
        };
        app_cfg
            .entry("__cnx_compiled_v5".to_string())
            .or_insert(Value::Bool(true));
    }
    Some(Value::Object(command_obj))
}

fn normalize_runtime_admin_command(command: Value) -> Option<Value> {
    let mut command_obj = command.as_object()?.clone();
    if command_obj.get("command").and_then(Value::as_str) != Some("relation_target_apply") {
        return None;
    }
    let intent = command_obj.get_mut("intent")?.as_object_mut()?;
    let units = intent.get_mut("units")?.as_array_mut()?;
    for unit in units {
        let Some(unit_obj) = unit.as_object_mut() else {
            continue;
        };
        let Some(startup) = unit_obj.get_mut("startup").and_then(Value::as_object_mut) else {
            continue;
        };
        let Some(manifest) = startup.get_mut("manifest") else {
            continue;
        };
        let Some(raw) = manifest.as_str() else {
            continue;
        };
        let path = PathBuf::from(raw);
        if path.is_absolute() {
            continue;
        }
        *manifest = Value::String(repo_root().join(path).display().to_string());
    }
    Some(Value::Object(command_obj))
}

fn resign_auth_for_command(command: &Value, auth: Value) -> Value {
    let mut auth_obj = match auth {
        Value::Object(map) => map,
        other => return other,
    };
    let key_id = auth_obj
        .get("key_id")
        .and_then(Value::as_str)
        .unwrap_or("local-admin-ed25519-1")
        .to_string();
    let seq = auth_obj.get("seq").and_then(Value::as_u64).unwrap_or(1);
    let nonce = auth_obj
        .get("nonce")
        .and_then(Value::as_str)
        .unwrap_or("n1")
        .to_string();
    let scopes = auth_obj
        .get("scopes")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect::<Vec<_>>())
        .unwrap_or_default();
    let refreshed = signed_auth_for_command(command, &scopes, seq, &nonce, &key_id);
    if let Value::Object(refreshed_obj) = refreshed {
        for (key, value) in refreshed_obj {
            auth_obj.insert(key, value);
        }
    }
    Value::Object(auth_obj)
}

fn request_ctl_ok(
    socket_path: &Path,
    command: Value,
    auth: Value,
    _target: Value,
) -> Result<Value, String> {
    let req = json!({
        "command": command,
        "auth": auth,
        "forwarded": false,
    });
    let payload = request_ctl(socket_path, &req)?;
    if let Some(err) = payload.get("error").filter(|e| !e.is_null()) {
        return Err(format!("control returned error: {err}"));
    }
    payload
        .get("result")
        .cloned()
        .ok_or_else(|| "control response missing result".to_string())
}

fn request_ctl_error_code(
    socket_path: &Path,
    command: Value,
    auth: Value,
    _target: Value,
) -> Result<String, String> {
    let req = json!({
        "command": command,
        "auth": auth,
        "forwarded": false,
    });
    let payload = request_ctl(socket_path, &req)?;
    payload
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .ok_or_else(|| format!("expected control error code, got: {payload}"))
}

fn signed_auth_for_command(
    command: &Value,
    scopes: &[&str],
    seq: u64,
    nonce: &str,
    key_id: &str,
) -> Value {
    let command_bytes = serde_json::from_value::<KernelCtlCommand>(command.clone())
        .ok()
        .map(|_| {
            canonical_ctl_command_value_bytes(command)
                .expect("serialize canonicalized control command")
        })
        .or_else(|| canonical_runtime_admin_command_value_bytes(command).ok())
        .expect("serialize canonicalized command payload");
    let payload_sha256 = format!("{:x}", Sha256::digest(command_bytes));

    let seed = B64
        .decode(DEV_ADMIN_SIGNING_KEY_B64)
        .expect("decode admin signing seed");
    assert_eq!(seed.len(), 32, "admin signing seed must be 32 bytes");
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

fn next_auth_seq_global() -> u64 {
    static NEXT: OnceLock<AtomicU64> = OnceLock::new();
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(1);
    NEXT.get_or_init(|| AtomicU64::new(seed))
        .fetch_add(1, Ordering::Relaxed)
}

fn next_auth_seq_a() -> u64 {
    next_auth_seq_global()
}

fn next_auth_seq_b() -> u64 {
    next_auth_seq_global()
}

struct ClusterContext {
    node_a: RunningNode,
    node_b: RunningNode,
    node_a_identity: NodeIdentity,
    node_b_identity: NodeIdentity,
    addr_a: String,
    addr_b: String,
    admin_pub_b64: String,
    capanixd_bin: PathBuf,
}

impl ClusterContext {
    fn start(scopes_a: &[&str], scopes_b: &[&str]) -> Result<Self, String> {
        let Some(capanixd_bin) = try_find_capanixd_bin() else {
            return Err(
                "capanixd binary not found; set CAPANIXD_BIN or prebuild capanix-daemon".into(),
            );
        };

        let suffix = unique_suffix();
        let node_a_identity = generate_node_identity(&format!("cluster-node-a-{suffix}"));
        let node_b_identity = generate_node_identity(&format!("cluster-node-b-{suffix}"));
        let all_nodes = vec![&node_a_identity, &node_b_identity];
        let admin_pub_b64 = admin_public_key_b64();
        const MAX_BIND_RETRIES: usize = 6;
        let mut last_err = String::new();
        for attempt in 1..=MAX_BIND_RETRIES {
            let (addr_a, addr_b) = match reserve_distinct_bind_addrs() {
                Ok(v) => v,
                Err(err) => {
                    last_err = err;
                    continue;
                }
            };

            let cfg_a = build_cluster_config(
                &node_a_identity,
                &all_nodes,
                std::slice::from_ref(&addr_b),
                &[(node_b_identity.node_id.clone(), addr_b.clone())],
                &admin_pub_b64,
                scopes_a,
            );
            let cfg_b = build_cluster_config(
                &node_b_identity,
                &all_nodes,
                std::slice::from_ref(&addr_a),
                &[(node_a_identity.node_id.clone(), addr_a.clone())],
                &admin_pub_b64,
                scopes_b,
            );

            let mut node_a = match RunningNode::start(
                &capanixd_bin,
                &node_a_identity,
                &cfg_a,
                &addr_a,
                true,
                true,
            ) {
                Ok(node) => node,
                Err(e) => {
                    last_err = format!("node-a start failed: {e}");
                    if attempt < MAX_BIND_RETRIES && is_transport_bind_conflict(&last_err) {
                        continue;
                    }
                    return Err(last_err);
                }
            };
            if let Err(e) = node_a.wait_for_socket(control_socket_startup_timeout()) {
                let err = format!("node-a bootstrap failed: {e}");
                if attempt < MAX_BIND_RETRIES && is_transport_bind_conflict(&err) {
                    last_err = err;
                    continue;
                }
                return Err(err);
            }

            let mut node_b = match RunningNode::start(
                &capanixd_bin,
                &node_b_identity,
                &cfg_b,
                &addr_b,
                true,
                true,
            ) {
                Ok(node) => node,
                Err(e) => {
                    let err = format!("node-b start failed: {e}");
                    if attempt < MAX_BIND_RETRIES && is_transport_bind_conflict(&err) {
                        last_err = err;
                        continue;
                    }
                    return Err(err);
                }
            };
            if let Err(e) = node_b.wait_for_socket(control_socket_startup_timeout()) {
                let err = format!("node-b bootstrap failed: {e}");
                if attempt < MAX_BIND_RETRIES && is_transport_bind_conflict(&err) {
                    last_err = err;
                    continue;
                }
                return Err(err);
            }

            wait_cluster_ready(&mut node_a, &mut node_b)?;
            return Ok(Self {
                node_a,
                node_b,
                node_a_identity,
                node_b_identity,
                addr_a,
                addr_b,
                admin_pub_b64,
                capanixd_bin,
            });
        }

        Err(format!(
            "cluster start exhausted bind retries ({MAX_BIND_RETRIES}): {last_err}"
        ))
    }

    fn node_a_pid(&self) -> u32 {
        self.node_a.child.id()
    }

    fn node_b_pid(&self) -> u32 {
        self.node_b.child.id()
    }

    fn cluster_state_a(&self) -> Result<Value, String> {
        request_cluster_state(&self.node_a.socket_path)
    }

    fn cluster_state_b(&self) -> Result<Value, String> {
        request_cluster_state(&self.node_b.socket_path)
    }

    fn layered_status_a_local(&self) -> Result<Value, String> {
        request_layered_status(&self.node_a.socket_path, local_target())
    }

    fn layered_status_b_local(&self) -> Result<Value, String> {
        request_layered_status(&self.node_b.socket_path, local_target())
    }

    fn layered_status_a_target(&self, target: Value) -> Result<Value, String> {
        request_layered_status(&self.node_a.socket_path, target)
    }

    fn ctl_ok_a_local(&self, command: Value) -> Result<Value, String> {
        let mut last_err = String::new();
        for _ in 0..3 {
            let seq = next_auth_seq_a();
            let nonce = format!("{}-a", unique_suffix());
            let auth = signed_auth_for_command(
                &command,
                FULL_NODE_DELEGATION_SCOPES,
                seq,
                &nonce,
                "local-admin-ed25519-1",
            );
            match request_ctl_ok(
                &self.node_a.socket_path,
                command.clone(),
                auth,
                local_target(),
            ) {
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

    fn ctl_ok_b_local(&self, command: Value) -> Result<Value, String> {
        let mut last_err = String::new();
        for _ in 0..3 {
            let seq = next_auth_seq_b();
            let nonce = format!("{}-b", unique_suffix());
            let auth = signed_auth_for_command(
                &command,
                FULL_NODE_DELEGATION_SCOPES,
                seq,
                &nonce,
                "local-admin-ed25519-1",
            );
            match request_ctl_ok(
                &self.node_b.socket_path,
                command.clone(),
                auth,
                local_target(),
            ) {
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

    fn ctl_ok_a(&self, command: Value, auth: Value, target: Value) -> Result<Value, String> {
        request_ctl_ok(&self.node_a.socket_path, command, auth, target)
    }

    fn ctl_err_code_a(&self, command: Value, auth: Value, target: Value) -> Result<String, String> {
        request_ctl_error_code(&self.node_a.socket_path, command, auth, target)
    }

    fn assert_fail_closed_node_without_identity(&self) -> Result<(), String> {
        let node_c = generate_node_identity(&format!("cluster-node-c-{}", unique_suffix()));
        let all_with_c = vec![&self.node_a_identity, &self.node_b_identity, &node_c];
        let cfg_c = build_cluster_config(
            &node_c,
            &all_with_c,
            &[self.addr_a.clone(), self.addr_b.clone()],
            &[
                (self.node_a_identity.node_id.clone(), self.addr_a.clone()),
                (self.node_b_identity.node_id.clone(), self.addr_b.clone()),
            ],
            &self.admin_pub_b64,
            FULL_NODE_DELEGATION_SCOPES,
        );
        const MAX_BIND_RETRIES: usize = 6;
        let mut last_err = String::new();
        let mut daemon_c = None;
        for attempt in 1..=MAX_BIND_RETRIES {
            let addr_c = reserve_bind_addr();
            match RunningNode::start(&self.capanixd_bin, &node_c, &cfg_c, &addr_c, false, true) {
                Ok(node) => {
                    daemon_c = Some(node);
                    break;
                }
                Err(e) => {
                    let err = format!("node-c start failed: {e}");
                    if attempt < MAX_BIND_RETRIES && is_transport_bind_conflict(&err) {
                        last_err = err;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        let mut daemon_c = daemon_c.ok_or_else(|| {
            format!("node-c start exhausted bind retries ({MAX_BIND_RETRIES}): {last_err}")
        })?;
        wait_fail_closed(&mut daemon_c)?;

        let status_a = self
            .cluster_state_a()
            .map_err(|e| format!("cluster_state_get from node-a failed: {e}"))?;
        if status_has_node(&status_a, &node_c.node_id) {
            return Err("fail-closed node must not appear in cluster membership".to_string());
        }
        Ok(())
    }
}

fn with_cluster<R, F>(scopes_a: &[&str], scopes_b: &[&str], f: F) -> Result<R, String>
where
    F: FnOnce(&ClusterContext) -> Result<R, String>,
{
    let _guard = cluster_lock();
    let cluster = ClusterContext::start(scopes_a, scopes_b)?;
    f(&cluster)
}

fn ensure_layered_local_status_shape(status: &Value) -> Result<(), String> {
    if status.get("cluster").is_none() {
        return Err(format!("status missing cluster field: {status}"));
    }
    if status.get("metrics").is_none() {
        return Err(format!("status missing metrics field: {status}"));
    }
    if status.get("daemon").is_none() {
        return Err(format!("status missing daemon field: {status}"));
    }
    Ok(())
}

mod release_intent;
mod scenarios_api_boundary;
mod scenarios_app_runtime;
mod scenarios_lifecycle_orchestration;
mod scenarios_observability_path;
mod scenarios_runtime_scope;
mod scenarios_system_integration;

pub(super) use release_intent::*;
pub(super) use scenarios_api_boundary::*;
pub(super) use scenarios_app_runtime::*;
pub(super) use scenarios_lifecycle_orchestration::*;
pub(super) use scenarios_observability_path::*;
pub(super) use scenarios_runtime_scope::*;
pub(super) use scenarios_system_integration::*;
