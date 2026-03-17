use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::SystemTime;
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;

pub struct HttpResponse {
    pub status: u16,
    pub body: String,
    pub json: Option<Value>,
}

pub struct FsMetaApiFixture {
    child: Child,
    _temp: TempDir,
    api_base_url: String,
    root_a: String,
    root_b: String,
}

impl FsMetaApiFixture {
    pub fn start() -> Result<Self, String> {
        let temp = tempfile::tempdir().map_err(|e| format!("create tempdir failed: {e}"))?;
        let base = temp.path();

        let root_a = base.join("nfs").join("nfs1");
        let root_b = base.join("nfs").join("nfs2");
        std::fs::create_dir_all(&root_a).map_err(|e| format!("create root_a failed: {e}"))?;
        std::fs::create_dir_all(&root_b).map_err(|e| format!("create root_b failed: {e}"))?;
        std::fs::write(root_a.join("a.txt"), "alpha\n")
            .map_err(|e| format!("write root_a seed file failed: {e}"))?;
        std::fs::write(root_b.join("b.txt"), "beta\n")
            .map_err(|e| format!("write root_b seed file failed: {e}"))?;

        let auth_dir = base.join("auth");
        std::fs::create_dir_all(&auth_dir).map_err(|e| format!("create auth dir failed: {e}"))?;
        let passwd_path = auth_dir.join("passwd");
        let shadow_path = auth_dir.join("shadow");
        let query_keys_path = auth_dir.join("query-keys.json");
        std::fs::write(
            &passwd_path,
            concat!(
                "operator:1000:1000:fsmeta_management:/home/operator:/bin/bash:0\n",
                "reader:1001:1001:fsmeta_reader:/home/reader:/bin/bash:0\n",
                "locked:1002:1002:fsmeta_management:/home/locked:/bin/bash:1\n",
                "disabled:1003:1003:fsmeta_management:/home/disabled:/bin/bash:0\n",
            ),
        )
        .map_err(|e| format!("write passwd failed: {e}"))?;
        std::fs::write(
            &shadow_path,
            concat!(
                "operator:plain$operator123:0\n",
                "reader:plain$reader123:0\n",
                "locked:plain$locked123:0\n",
                "disabled:plain$disabled123:1\n",
            ),
        )
        .map_err(|e| format!("write shadow failed: {e}"))?;

        let roots_json = serde_json::json!([
            {
                "id":"root-a",
                "selector":{"mount_point":root_a.display().to_string()},
                "subpath_scope":"/",
                "watch":true,
                "scan":true
            },
            {
                "id":"root-b",
                "selector":{"mount_point":root_b.display().to_string()},
                "subpath_scope":"/",
                "watch":true,
                "scan":true
            }
        ])
        .to_string();
        let host_object_grants_json = serde_json::json!([
            {"object_ref":"fs-meta-blackbox-node::source-1","host_ref":"fs-meta-blackbox-node","host_ip":"10.0.0.11","mount_point":root_a.display().to_string(),"fs_source":"fixture:root-a:1","fs_type":"fixture","interfaces":["posix-fs","inotify"],"active":true},
            {"object_ref":"fs-meta-blackbox-node::source-2","host_ref":"fs-meta-blackbox-node","host_ip":"10.0.0.12","mount_point":root_a.display().to_string(),"fs_source":"fixture:root-a:2","fs_type":"fixture","interfaces":["posix-fs","inotify"],"active":true},
            {"object_ref":"fs-meta-blackbox-node::source-3","host_ref":"fs-meta-blackbox-node","host_ip":"10.0.0.13","mount_point":root_b.display().to_string(),"fs_source":"fixture:root-b:1","fs_type":"fixture","interfaces":["posix-fs","inotify"],"active":true},
            {"object_ref":"fs-meta-blackbox-node::source-4","host_ref":"fs-meta-blackbox-node","host_ip":"10.0.0.14","mount_point":root_b.display().to_string(),"fs_source":"fixture:root-b:2","fs_type":"fixture","interfaces":["posix-fs","inotify"],"active":true}
        ])
        .to_string();

        let port = reserve_local_port()?;
        let bind = format!("127.0.0.1:{port}");
        let api_base_url = format!("http://{bind}");

        let fixture_bin = ensure_fixture_binary()?;
        let stdout_log = base.join("fixture.stdout.log");
        let stderr_log = base.join("fixture.stderr.log");
        let stdout_file = std::fs::File::create(&stdout_log)
            .map_err(|e| format!("create fixture stdout log failed: {e}"))?;
        let stderr_file = std::fs::File::create(&stderr_log)
            .map_err(|e| format!("create fixture stderr log failed: {e}"))?;

        let mut child = Command::new(&fixture_bin)
            .env(
                "FS_META_API_FACADE_RESOURCE_ID",
                "fs-meta-blackbox-listener",
            )
            .env("FS_META_API_LISTENER_BIND_ADDR", &bind)
            .env("FS_META_ROOTS_JSON", roots_json)
            .env("FS_META_HOST_OBJECT_GRANTS_JSON", host_object_grants_json)
            .env("FS_META_PASSWD_PATH", &passwd_path)
            .env("FS_META_SHADOW_PATH", &shadow_path)
            .env("FS_META_QUERY_KEYS_PATH", &query_keys_path)
            .env("FS_META_NODE_ID", "fs-meta-blackbox-node")
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file))
            .spawn()
            .map_err(|e| format!("spawn fixture failed: {e}"))?;

        wait_fixture_ready(&api_base_url, &mut child, &stdout_log, &stderr_log)?;

        Ok(Self {
            child,
            _temp: temp,
            api_base_url,
            root_a: root_a.display().to_string(),
            root_b: root_b.display().to_string(),
        })
    }

    pub fn api_base_url(&self) -> &str {
        &self.api_base_url
    }

    pub fn root_a(&self) -> &str {
        &self.root_a
    }

    pub fn root_b(&self) -> &str {
        &self.root_b
    }
}

impl Drop for FsMetaApiFixture {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

fn reserve_local_port() -> Result<u16, String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| format!("reserve local port failed: {e}"))?;
    listener
        .local_addr()
        .map(|addr| addr.port())
        .map_err(|e| format!("read reserved local port failed: {e}"))
}

fn workspace_root() -> Result<PathBuf, String> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(PathBuf::from)
        .expect("fs-meta container root");
    manifest_dir
        .join("..")
        .canonicalize()
        .map_err(|e| format!("resolve workspace root failed: {e}"))
}

fn ensure_fixture_binary() -> Result<PathBuf, String> {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_fs_meta_api_fixture") {
        let p = PathBuf::from(path);
        if p.exists() {
            return Ok(p);
        }
    }

    let root = workspace_root()?;
    let bin = root.join("target/debug/fs_meta_api_fixture");
    if bin.exists() && fixture_binary_is_fresh(&bin, &root)? {
        return Ok(bin);
    }

    let cargo_bin = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let status = Command::new(cargo_bin)
        .current_dir(&root)
        .arg("build")
        .arg("-p")
        .arg("capanix-app-fs-meta-worker-facade")
        .arg("--bin")
        .arg("fs_meta_api_fixture")
        .status()
        .map_err(|e| format!("spawn cargo build for fixture failed: {e}"))?;
    if !status.success() {
        return Err("cargo build for fixture failed".to_string());
    }

    if !bin.exists() {
        return Err(format!(
            "fixture binary not found after build: {}",
            bin.display()
        ));
    }
    Ok(bin)
}

fn fixture_binary_is_fresh(bin: &Path, workspace_root: &Path) -> Result<bool, String> {
    let bin_mtime = std::fs::metadata(bin)
        .and_then(|m| m.modified())
        .map_err(|e| format!("read fixture binary mtime failed: {e}"))?;
    let app_root = workspace_root.join("fs-meta");
    let src_root = app_root.join("app").join("src");
    let facade_root = app_root.join("worker-facade").join("src");
    let manifest = app_root.join("app").join("Cargo.toml");
    let facade_manifest = app_root.join("worker-facade").join("Cargo.toml");
    let mut latest_src_mtime =
        latest_mtime_recursive(&src_root).unwrap_or(std::time::SystemTime::UNIX_EPOCH);
    if let Some(mtime) = latest_mtime_recursive(&facade_root)
        && mtime > latest_src_mtime
    {
        latest_src_mtime = mtime;
    }
    if let Ok(mtime) = std::fs::metadata(&manifest).and_then(|m| m.modified()) {
        if mtime > latest_src_mtime {
            latest_src_mtime = mtime;
        }
    }
    if let Ok(mtime) = std::fs::metadata(&facade_manifest).and_then(|m| m.modified()) {
        if mtime > latest_src_mtime {
            latest_src_mtime = mtime;
        }
    }
    Ok(bin_mtime >= latest_src_mtime)
}

fn latest_mtime_recursive(path: &Path) -> Option<SystemTime> {
    let metadata = std::fs::metadata(path).ok()?;
    if metadata.is_file() {
        return metadata.modified().ok();
    }
    if !metadata.is_dir() {
        return None;
    }
    let mut latest = SystemTime::UNIX_EPOCH;
    let entries = std::fs::read_dir(path).ok()?;
    for entry in entries.flatten() {
        if let Some(child_mtime) = latest_mtime_recursive(&entry.path()) {
            if child_mtime > latest {
                latest = child_mtime;
            }
        }
    }
    Some(latest)
}

fn wait_fixture_ready(
    api_base_url: &str,
    child: &mut Child,
    stdout_log: &Path,
    stderr_log: &Path,
) -> Result<(), String> {
    const READY_MARKER: &str = "FS_META_API_FIXTURE_READY";
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Some(status) = child
            .try_wait()
            .map_err(|e| format!("poll fixture process failed: {e}"))?
        {
            let stdout = std::fs::read_to_string(stdout_log).unwrap_or_default();
            let stderr = std::fs::read_to_string(stderr_log).unwrap_or_default();
            return Err(format!(
                "fixture exited before ready (status={status})
stdout:
{stdout}
stderr:
{stderr}"
            ));
        }
        let stdout = std::fs::read_to_string(stdout_log).unwrap_or_default();
        if stdout.lines().any(|line| line.contains(READY_MARKER)) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let stdout = std::fs::read_to_string(stdout_log).unwrap_or_default();
    let stderr = std::fs::read_to_string(stderr_log).unwrap_or_default();
    Err(format!(
        "fixture readiness timeout for {api_base_url}
stdout:
{stdout}
stderr:
{stderr}"
    ))
}
pub fn http_json(
    api_base_url: &str,
    method: &str,
    path: &str,
    bearer_token: Option<&str>,
    body: Option<&Value>,
) -> Result<HttpResponse, String> {
    let url = format!("{api_base_url}{path}");
    let out_file = tempfile::NamedTempFile::new()
        .map_err(|e| format!("create curl output tempfile failed: {e}"))?;

    let mut cmd = Command::new("curl");
    cmd.arg("-sS")
        .arg("-o")
        .arg(out_file.path())
        .arg("-w")
        .arg("%{http_code}")
        .arg("-X")
        .arg(method)
        .arg(&url)
        .arg("-H")
        .arg("accept: application/json");
    if let Some(token) = bearer_token {
        cmd.arg("-H").arg(format!("authorization: Bearer {token}"));
    }
    if let Some(payload) = body {
        cmd.arg("-H")
            .arg("content-type: application/json")
            .arg("--data")
            .arg(payload.to_string());
    }

    let output = cmd
        .output()
        .map_err(|e| format!("spawn curl failed for {method} {url}: {e}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        return Err(format!(
            "curl failed for {method} {url} (status={})\nstderr:\n{}",
            output.status, stderr
        ));
    }
    let status = stdout
        .parse::<u16>()
        .map_err(|e| format!("parse curl status code failed ({stdout}): {e}"))?;
    let body = std::fs::read_to_string(out_file.path())
        .map_err(|e| format!("read curl output body failed: {e}"))?;
    let json = serde_json::from_str::<Value>(&body).ok();

    Ok(HttpResponse { status, body, json })
}
