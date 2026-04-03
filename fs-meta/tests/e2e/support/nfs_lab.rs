#![cfg(target_os = "linux")]
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use tempfile::TempDir;

const LAB_MARKER_FILENAME: &str = ".fs_meta_nfs_lab";
const E2E_TMP_ROOT_ENV: &str = "DATANIX_E2E_TMP_ROOT";
const LEGACY_E2E_TMP_ROOT_ENV: &str = "CAPANIX_E2E_TMP_ROOT";
const NFS_LAB_COMPONENT: &str = "nfs-lab";

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaleLabCleanupPlan {
    root: PathBuf,
    mount_targets: Vec<PathBuf>,
}

fn lab_marker_path(root: &Path) -> PathBuf {
    root.join(LAB_MARKER_FILENAME)
}

fn e2e_tmp_root() -> PathBuf {
    if let Ok(raw) =
        std::env::var(E2E_TMP_ROOT_ENV).or_else(|_| std::env::var(LEGACY_E2E_TMP_ROOT_ENV))
    {
        let dir = PathBuf::from(raw);
        fs::create_dir_all(&dir).expect("create e2e temp root");
        return dir;
    }
    let dir = crate::path_support::workspace_root()
        .join(".tmp")
        .join("fs-meta-e2e");
    fs::create_dir_all(&dir).expect("create default e2e temp root");
    dir
}

fn nfs_lab_parent_dir() -> PathBuf {
    let dir = e2e_tmp_root().join(NFS_LAB_COMPONENT);
    fs::create_dir_all(&dir).expect("create nfs lab parent dir");
    dir
}

fn decode_mountinfo_path(raw: &str) -> PathBuf {
    let decoded = raw
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\");
    PathBuf::from(decoded)
}

fn nfs_lab_root_for_mount_target(target: &Path) -> Option<PathBuf> {
    if !target.components().any(
        |component| matches!(component, Component::Normal(name) if name == OsStr::new("mounts")),
    ) {
        return None;
    }
    for ancestor in target.ancestors() {
        if ancestor
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(".tmp"))
        {
            return Some(ancestor.to_path_buf());
        }
        if lab_marker_path(ancestor).exists()
            || (ancestor.join("mounts").exists() && ancestor.join("exports").exists())
        {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

fn discover_candidate_lab_roots_under(base_dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(base_dir) else {
        return Vec::new();
    };
    let mut roots = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".tmp"))
        })
        .filter(|path| {
            lab_marker_path(path).exists()
                || (path.join("mounts").exists() && path.join("exports").exists())
        })
        .collect::<Vec<_>>();
    roots.sort();
    roots
}

fn stale_lab_cleanup_plans_from_mountinfo(
    mountinfo: &str,
    filesystem_roots: &[PathBuf],
) -> Vec<StaleLabCleanupPlan> {
    let mut mounts_by_root = BTreeMap::<PathBuf, Vec<PathBuf>>::new();
    for line in mountinfo.lines() {
        let Some(raw_target) = line.split_whitespace().nth(4) else {
            continue;
        };
        let target = decode_mountinfo_path(raw_target);
        let Some(root) = nfs_lab_root_for_mount_target(&target) else {
            continue;
        };
        mounts_by_root.entry(root).or_default().push(target);
    }
    for targets in mounts_by_root.values_mut() {
        targets.sort_by(|a, b| {
            let depth_a = a.components().count();
            let depth_b = b.components().count();
            depth_b.cmp(&depth_a).then_with(|| a.cmp(b))
        });
    }
    let mut roots = mounts_by_root.keys().cloned().collect::<Vec<_>>();
    for root in filesystem_roots {
        if !roots.iter().any(|existing| existing == root) {
            roots.push(root.clone());
        }
    }
    roots.sort();
    roots.dedup();
    roots
        .into_iter()
        .map(|root| StaleLabCleanupPlan {
            mount_targets: mounts_by_root.remove(&root).unwrap_or_default(),
            root,
        })
        .collect()
}

fn join_cleanup_errors(context: &str, errors: &[String]) -> String {
    format!("{context}: {}", errors.join("; "))
}

fn stale_lab_cleanup_plans() -> Result<Vec<StaleLabCleanupPlan>, String> {
    let mountinfo = fs::read_to_string("/proc/self/mountinfo")
        .map_err(|e| format!("read /proc/self/mountinfo failed: {e}"))?;
    let mut filesystem_roots = discover_candidate_lab_roots_under(Path::new("/tmp"));
    filesystem_roots.extend(discover_candidate_lab_roots_under(&nfs_lab_parent_dir()));
    filesystem_roots.sort();
    filesystem_roots.dedup();
    Ok(stale_lab_cleanup_plans_from_mountinfo(
        &mountinfo,
        &filesystem_roots,
    ))
}

fn cleanup_stale_lab_mount_target(target: &Path) -> Result<(), String> {
    let output = sudo_output(["umount", target.to_string_lossy().as_ref()])?;
    if !output.status.success() {
        if output_indicates_absent_mount(&output) {
            return Ok(());
        }
        let fallback = sudo_output(["umount", "-f", "-l", target.to_string_lossy().as_ref()])?;
        if !fallback.status.success() && !output_indicates_absent_mount(&fallback) {
            return Err(format!(
                "umount {} failed with status {}; fallback umount -f -l failed with status {}",
                target.display(),
                output.status,
                fallback.status
            ));
        }
    }
    Ok(())
}

fn unexport_dir_path(dir: &Path) -> Result<(), String> {
    let output = sudo_output(["exportfs", "-u", &format!("127.0.0.1:{}", dir.display())])?;
    if !output.status.success() && !output_indicates_absent_export(&output) {
        return Err(format!(
            "exportfs remove {} failed with status {}",
            dir.display(),
            output.status
        ));
    }
    Ok(())
}

fn error_indicates_stale_file_handle(err: &std::io::Error) -> bool {
    err.raw_os_error() == Some(116)
        || err
            .to_string()
            .to_ascii_lowercase()
            .contains("stale file handle")
}

fn remove_stale_lab_root(root: &Path) -> Result<(), String> {
    match fs::remove_dir_all(root) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) if error_indicates_stale_file_handle(&err) => {
            let output = sudo_output(["rm", "-rf", root.to_string_lossy().as_ref()])?;
            if output.status.success() {
                Ok(())
            } else {
                Err(format!(
                    "remove stale lab root {} failed after stale file handle fallback with status {}",
                    root.display(),
                    output.status
                ))
            }
        }
        Err(err) => Err(format!("remove stale lab root {} failed: {err}", root.display())),
    }
}

fn cleanup_stale_lab_plan(plan: &StaleLabCleanupPlan) -> Result<(), String> {
    let mut errors = Vec::new();
    for target in &plan.mount_targets {
        if let Err(err) = cleanup_stale_lab_mount_target(target) {
            errors.push(err);
        }
    }
    let exports_dir = plan.root.join("exports");
    if let Ok(entries) = fs::read_dir(&exports_dir) {
        for dir in entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
        {
            if let Err(err) = unexport_dir_path(&dir) {
                errors.push(err);
            }
        }
    }
    if let Err(err) = remove_stale_lab_root(&plan.root) {
        errors.push(err);
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(join_cleanup_errors(
            &format!("cleanup stale NFS lab root {}", plan.root.display()),
            &errors,
        ))
    }
}

fn cleanup_stale_labs_before_start() -> Result<(), String> {
    let plans = stale_lab_cleanup_plans()?;
    let mut errors = Vec::new();
    for plan in plans {
        if let Err(err) = cleanup_stale_lab_plan(&plan) {
            errors.push(err);
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(join_cleanup_errors(
            "cleanup stale NFS labs before start",
            &errors,
        ))
    }
}

fn output_stderr_string(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_ascii_lowercase()
}

fn output_indicates_absent_mount(output: &std::process::Output) -> bool {
    let stderr = output_stderr_string(output);
    stderr.contains("no mount point specified")
        || stderr.contains("not mounted")
        || stderr.contains("not a mountpoint")
}

fn output_indicates_absent_export(output: &std::process::Output) -> bool {
    let stderr = output_stderr_string(output);
    stderr.contains("could not find") && stderr.contains("to unexport")
}

#[derive(Debug, Clone)]
pub struct RealNfsPreflight {
    pub enabled: bool,
    pub reason: Option<String>,
}

impl RealNfsPreflight {
    pub fn detect() -> Self {
        let enabled = std::env::var("CAPANIX_REAL_NFS_E2E")
            .ok()
            .or_else(|| std::env::var("DATANIX_REAL_NFS_E2E").ok());
        if enabled.as_deref() != Some("1") {
            return Self {
                enabled: false,
                reason: Some("CAPANIX_REAL_NFS_E2E!=1".into()),
            };
        }
        if !cfg!(target_os = "linux") {
            return Self {
                enabled: false,
                reason: Some("linux-only".into()),
            };
        }
        if !Path::new("/proc/fs/nfsd").exists() {
            return Self {
                enabled: false,
                reason: Some("/proc/fs/nfsd is unavailable".into()),
            };
        }
        let sudo_ok = Command::new("sudo")
            .args(["-n", "true"])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !sudo_ok {
            return Self {
                enabled: false,
                reason: Some("requires passwordless sudo".into()),
            };
        }
        for bin in [
            "rpcbind",
            "rpc.nfsd",
            "rpc.mountd",
            "exportfs",
            "mount",
            "umount",
            "pgrep",
            "pkill",
        ] {
            let ok = Command::new("sh")
                .arg("-lc")
                .arg(format!("command -v {bin} >/dev/null 2>&1"))
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if !ok {
                return Self {
                    enabled: false,
                    reason: Some(format!("missing required command: {bin}")),
                };
            }
        }
        Self {
            enabled: true,
            reason: None,
        }
    }
}

pub struct NfsLab {
    temp: TempDir,
    exports_dir: PathBuf,
    mounts_dir: PathBuf,
    mounted: BTreeMap<(String, String), PathBuf>,
    started_rpcbind: bool,
    started_mountd: Option<Child>,
    mounted_nfsd: bool,
}

impl NfsLab {
    pub fn start() -> Result<Self, String> {
        let preflight = RealNfsPreflight::detect();
        if !preflight.enabled {
            return Err(preflight
                .reason
                .unwrap_or_else(|| "real NFS preflight failed".to_string()));
        }
        cleanup_stale_labs_before_start()?;
        let temp = tempfile::Builder::new()
            .prefix(".tmp")
            .tempdir_in(nfs_lab_parent_dir())
            .map_err(|e| format!("create NFS lab tempdir failed: {e}"))?;
        let exports_dir = temp.path().join("exports");
        let mounts_dir = temp.path().join("mounts");
        fs::create_dir_all(&exports_dir).map_err(|e| format!("create exports dir failed: {e}"))?;
        fs::create_dir_all(&mounts_dir).map_err(|e| format!("create mounts dir failed: {e}"))?;
        fs::write(lab_marker_path(temp.path()), b"fs-meta nfs lab\n")
            .map_err(|e| format!("create NFS lab marker failed: {e}"))?;

        let mut lab = Self {
            temp,
            exports_dir,
            mounts_dir,
            mounted: BTreeMap::new(),
            started_rpcbind: false,
            started_mountd: None,
            mounted_nfsd: false,
        };
        lab.ensure_nfs_stack()?;
        for export in ["nfs1", "nfs2", "nfs3"] {
            lab.create_export(export)?;
        }
        Ok(lab)
    }

    pub fn temp_root(&self) -> &Path {
        self.temp.path()
    }

    pub fn create_export(&mut self, export_name: &str) -> Result<PathBuf, String> {
        let export_dir = self.exports_dir.join(export_name);
        fs::create_dir_all(&export_dir)
            .map_err(|e| format!("create export dir {export_name} failed: {e}"))?;
        self.seed_export_tree(&export_dir, export_name)?;
        self.export_dir(&export_dir)?;
        Ok(export_dir)
    }

    pub fn retire_export(&mut self, export_name: &str) -> Result<(), String> {
        let export_dir = self.exports_dir.join(export_name);
        let mut errors = Vec::new();
        if export_dir.exists() {
            if let Err(err) = self.unexport_dir(&export_dir) {
                errors.push(err);
            }
        }
        let keys = self.mounted.keys().cloned().collect::<Vec<_>>();
        for (node, export) in keys {
            if export == export_name {
                if let Err(err) = self.unmount_export(&node, &export) {
                    errors.push(err);
                }
            }
        }
        if export_dir.exists() {
            if let Err(err) = fs::remove_dir_all(&export_dir) {
                errors.push(format!("remove export dir {export_name} failed: {err}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(join_cleanup_errors(
                &format!("retire export {export_name}"),
                &errors,
            ))
        }
    }

    pub fn append_file(
        &self,
        export_name: &str,
        relative: &str,
        content: &str,
    ) -> Result<PathBuf, String> {
        let path = self.mutable_export_path(export_name, relative)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("create parent for {} failed: {e}", path.display()))?;
        }
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("open {} for append failed: {e}", path.display()))?;
        file.write_all(content.as_bytes())
            .map_err(|e| format!("append {} failed: {e}", path.display()))?;
        Ok(path)
    }

    pub fn write_file(
        &self,
        export_name: &str,
        relative: &str,
        content: &str,
    ) -> Result<PathBuf, String> {
        let path = self.mutable_export_path(export_name, relative)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("create parent for {} failed: {e}", path.display()))?;
        }
        fs::write(&path, content).map_err(|e| format!("write {} failed: {e}", path.display()))?;
        Ok(path)
    }

    pub fn mkdir(&self, export_name: &str, relative: &str) -> Result<PathBuf, String> {
        let path = self.mutable_export_path(export_name, relative)?;
        fs::create_dir_all(&path).map_err(|e| format!("mkdir {} failed: {e}", path.display()))?;
        Ok(path)
    }

    pub fn remove_path(&self, export_name: &str, relative: &str) -> Result<(), String> {
        let path = self.mutable_export_path(export_name, relative)?;
        if !path.exists() {
            return Ok(());
        }
        let meta =
            fs::metadata(&path).map_err(|e| format!("stat {} failed: {e}", path.display()))?;
        if meta.is_dir() {
            fs::remove_dir_all(&path)
                .map_err(|e| format!("remove dir {} failed: {e}", path.display()))?;
        } else {
            fs::remove_file(&path)
                .map_err(|e| format!("remove file {} failed: {e}", path.display()))?;
        }
        Ok(())
    }

    pub fn export_source(&self, export_name: &str) -> String {
        format!("127.0.0.1:{}", self.exports_dir.join(export_name).display())
    }

    fn mutable_export_root(&self, export_name: &str) -> PathBuf {
        self.mounted
            .iter()
            .find_map(|((_, export), mount_path)| {
                (export == export_name).then(|| mount_path.clone())
            })
            .unwrap_or_else(|| self.exports_dir.join(export_name))
    }

    fn mutable_export_path(&self, export_name: &str, relative: &str) -> Result<PathBuf, String> {
        Ok(self
            .mutable_export_root(export_name)
            .join(normalize_relative(relative)?))
    }

    pub fn mount_export(&mut self, node_name: &str, export_name: &str) -> Result<PathBuf, String> {
        let export_dir = self.exports_dir.join(export_name);
        if !export_dir.exists() {
            return Err(format!("export {export_name} does not exist"));
        }
        let mount_dir = self.mounts_dir.join(node_name).join(export_name);
        fs::create_dir_all(&mount_dir)
            .map_err(|e| format!("create mount dir {} failed: {e}", mount_dir.display()))?;
        let source = format!("127.0.0.1:{}", export_dir.display());
        let status = sudo_status([
            "mount",
            "-t",
            "nfs",
            "-o",
            "vers=4,tcp,timeo=50,retrans=1,noac,actimeo=0,lookupcache=none,nordirplus",
            source.as_str(),
            mount_dir.to_string_lossy().as_ref(),
        ])?;
        if !status.success() {
            return Err(format!(
                "mount {} -> {} failed with status {}",
                source,
                mount_dir.display(),
                status
            ));
        }
        self.mounted.insert(
            (node_name.to_string(), export_name.to_string()),
            mount_dir.clone(),
        );
        Ok(mount_dir)
    }

    pub fn unmount_export(&mut self, node_name: &str, export_name: &str) -> Result<(), String> {
        let Some(path) = self
            .mounted
            .remove(&(node_name.to_string(), export_name.to_string()))
        else {
            return Ok(());
        };
        let status = sudo_status(["umount", path.to_string_lossy().as_ref()])?;
        if !status.success() {
            let fallback = sudo_status(["umount", "-f", "-l", path.to_string_lossy().as_ref()])?;
            if !fallback.success() {
                return Err(format!(
                    "umount {} failed with status {}; fallback umount -f -l failed with status {}",
                    path.display(),
                    status,
                    fallback
                ));
            }
        }
        Ok(())
    }

    pub fn mount_path(&self, node_name: &str, export_name: &str) -> Option<PathBuf> {
        self.mounted
            .get(&(node_name.to_string(), export_name.to_string()))
            .cloned()
    }

    fn cleanup(&mut self) -> Result<(), String> {
        let mut errors = Vec::new();
        let mounts = self.mounted.keys().cloned().collect::<Vec<_>>();
        for (node, export) in mounts {
            if let Err(err) = self.unmount_export(&node, &export) {
                errors.push(err);
            }
        }
        let exports = fs::read_dir(&self.exports_dir)
            .ok()
            .into_iter()
            .flat_map(|rows| rows.flatten())
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        for dir in exports {
            if let Err(err) = self.unexport_dir(&dir) {
                errors.push(err);
            }
        }
        if let Some(child) = &mut self.started_mountd {
            if let Err(err) = child.kill() {
                errors.push(format!("kill rpc.mountd failed: {err}"));
            }
            if let Err(err) = child.wait() {
                errors.push(format!("wait rpc.mountd failed: {err}"));
            }
        }
        if self.started_rpcbind {
            match sudo_status(["pkill", "-x", "rpcbind"]) {
                Ok(status) if status.success() => {}
                Ok(status) => errors.push(format!("pkill rpcbind failed with status {}", status)),
                Err(err) => errors.push(err),
            }
        }
        if self.mounted_nfsd {
            match sudo_status(["umount", "/proc/fs/nfsd"]) {
                Ok(status) if status.success() => {}
                Ok(status) => errors.push(format!(
                    "umount /proc/fs/nfsd failed with status {}",
                    status
                )),
                Err(err) => errors.push(err),
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(join_cleanup_errors(
                &format!("cleanup NFS lab {}", self.temp.path().display()),
                &errors,
            ))
        }
    }

    fn seed_export_tree(&self, export_dir: &Path, export_name: &str) -> Result<(), String> {
        let root = export_dir.join("root.txt");
        fs::write(&root, format!("root-{export_name}\n"))
            .map_err(|e| format!("write {} failed: {e}", root.display()))?;
        let dir = export_dir.join("data");
        fs::create_dir_all(&dir).map_err(|e| format!("create {} failed: {e}", dir.display()))?;
        fs::write(dir.join("a.txt"), format!("a-{export_name}\n"))
            .map_err(|e| format!("seed file failed: {e}"))?;
        fs::write(dir.join("b.txt"), format!("b-{export_name}\n"))
            .map_err(|e| format!("seed file failed: {e}"))?;
        Ok(())
    }

    fn ensure_nfs_stack(&mut self) -> Result<(), String> {
        let rpcbind_running = Command::new("sh")
            .arg("-lc")
            .arg("pgrep -x rpcbind >/dev/null 2>&1")
            .status()
            .map(|s| s.success())
            .map_err(|e| format!("check rpcbind failed: {e}"))?;
        if !rpcbind_running {
            let status = sudo_status(["rpcbind"])?;
            if !status.success() {
                return Err(format!("rpcbind failed with status {}", status));
            }
            self.started_rpcbind = true;
        }

        let mounted = Command::new("sh")
            .arg("-lc")
            .arg("mountpoint -q /proc/fs/nfsd")
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !mounted {
            let status = sudo_status(["mount", "-t", "nfsd", "nfsd", "/proc/fs/nfsd"])?;
            if !status.success() {
                return Err(format!("mount nfsd failed with status {}", status));
            }
            self.mounted_nfsd = true;
        }

        let nfsd_status = sudo_status(["rpc.nfsd", "8"])?;
        if !nfsd_status.success() {
            return Err(format!("rpc.nfsd failed with status {nfsd_status}"));
        }

        let mountd_running = Command::new("sh")
            .arg("-lc")
            .arg("pgrep -x rpc.mountd >/dev/null 2>&1")
            .status()
            .map(|s| s.success())
            .map_err(|e| format!("check rpc.mountd failed: {e}"))?;
        if !mountd_running {
            let child = Command::new("sudo")
                .args(["-n", "rpc.mountd", "--foreground"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .map_err(|e| format!("spawn rpc.mountd failed: {e}"))?;
            self.started_mountd = Some(child);
        }
        Ok(())
    }

    fn export_dir(&self, dir: &Path) -> Result<(), String> {
        let status = sudo_status([
            "exportfs",
            "-i",
            "-o",
            "rw,no_subtree_check,no_root_squash",
            &format!("127.0.0.1:{}", dir.display()),
        ])?;
        if !status.success() {
            return Err(format!(
                "exportfs add {} failed with status {}",
                dir.display(),
                status
            ));
        }
        Ok(())
    }

    fn unexport_dir(&self, dir: &Path) -> Result<(), String> {
        let output = sudo_output(["exportfs", "-u", &format!("127.0.0.1:{}", dir.display())])?;
        if !output.status.success() && !output_indicates_absent_export(&output) {
            return Err(format!(
                "exportfs remove {} failed with status {}",
                dir.display(),
                output.status
            ));
        }
        Ok(())
    }
}

impl Drop for NfsLab {
    fn drop(&mut self) {
        if let Err(err) = self.cleanup() {
            eprintln!("fs-meta nfs lab cleanup failed: {err}");
        }
    }
}

fn normalize_relative(relative: &str) -> Result<PathBuf, String> {
    let normalized = relative.trim_start_matches('/');
    if normalized.is_empty() {
        return Err("relative path must not be empty".into());
    }
    Ok(PathBuf::from(normalized))
}

fn sudo_status<const N: usize>(args: [&str; N]) -> Result<ExitStatus, String> {
    Command::new("sudo")
        .arg("-n")
        .args(args)
        .status()
        .map_err(|e| format!("sudo {:?} failed to start: {e}", args.as_slice()))
}

#[allow(dead_code)]
fn sudo_output<I, S>(args: I) -> Result<std::process::Output, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new("sudo")
        .arg("-n")
        .args(args)
        .output()
        .map_err(|e| format!("sudo output failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn nfs_lab_root_for_mount_target_finds_structured_root_under_any_base() {
        let base = tempfile::tempdir().expect("base tempdir");
        let root = base.path().join(".tmpabc123");
        fs::create_dir_all(root.join("mounts/node-a/nfs1")).expect("mount path");
        fs::create_dir_all(root.join("exports/nfs1")).expect("exports path");
        fs::write(lab_marker_path(&root), b"marker\n").expect("marker file");

        assert_eq!(
            nfs_lab_root_for_mount_target(&root.join("mounts/node-a/nfs1")),
            Some(root.clone())
        );
        assert_eq!(
            nfs_lab_root_for_mount_target(&root.join("exports/nfs1")),
            None
        );
    }

    #[test]
    fn discover_candidate_lab_roots_under_finds_marker_and_structured_roots() {
        let base = tempfile::tempdir().expect("base tempdir");
        let marker_root = base.path().join(".tmp-lab-marker");
        let structured_root = base.path().join(".tmp-lab-structured");
        let unrelated_root = base.path().join("unrelated");
        fs::create_dir_all(&marker_root).expect("marker root");
        fs::write(lab_marker_path(&marker_root), b"marker\n").expect("marker file");
        fs::create_dir_all(structured_root.join("mounts")).expect("structured mounts");
        fs::create_dir_all(structured_root.join("exports")).expect("structured exports");
        fs::create_dir_all(&unrelated_root).expect("unrelated root");

        let roots = discover_candidate_lab_roots_under(base.path());
        assert_eq!(roots, vec![marker_root, structured_root]);
    }

    #[test]
    fn stale_lab_cleanup_plans_merge_mountinfo_and_filesystem_roots() {
        let mountinfo = concat!(
            "101 100 0:42 / /tmp/.tmpabc123/mounts/node-a/nfs1 rw,relatime - nfs4 127.0.0.1:/tmp/.tmpabc123/exports/nfs1 rw\n",
            "102 100 0:43 / /tmp/.tmpabc123/mounts/node-a/nfs1/child rw,relatime - nfs4 127.0.0.1:/tmp/.tmpabc123/exports/nfs1 rw\n",
            "103 100 0:44 / /tmp/.tmpabc123/mounts/node-b/nfs2\\040space rw,relatime - nfs4 127.0.0.1:/tmp/.tmpabc123/exports/nfs2 rw\n"
        );
        let filesystem_roots = vec![PathBuf::from("/tmp/.tmpdef456")];

        let plans = stale_lab_cleanup_plans_from_mountinfo(mountinfo, &filesystem_roots);
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].root, PathBuf::from("/tmp/.tmpabc123"));
        assert_eq!(plans[0].mount_targets.len(), 3);
        assert_eq!(
            plans[0].mount_targets[0],
            PathBuf::from("/tmp/.tmpabc123/mounts/node-a/nfs1/child")
        );
        assert_eq!(
            plans[0].mount_targets[2],
            PathBuf::from("/tmp/.tmpabc123/mounts/node-b/nfs2 space")
        );
        assert_eq!(plans[1].root, PathBuf::from("/tmp/.tmpdef456"));
        assert!(plans[1].mount_targets.is_empty());
    }

    #[test]
    fn error_indicates_stale_file_handle_accepts_raw_os_error_116() {
        let err = std::io::Error::from_raw_os_error(116);
        assert!(error_indicates_stale_file_handle(&err));
    }

    #[test]
    fn remove_stale_lab_root_tolerates_missing_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let missing = temp.path().join("missing-root");
        assert!(remove_stale_lab_root(&missing).is_ok());
    }

    #[test]
    fn output_indicates_absent_mount_accepts_no_mount_point_shape() {
        let output = std::process::Output {
            status: ExitStatus::from_raw(32 << 8),
            stdout: Vec::new(),
            stderr: b"umount: /tmp/lab/mounts/node-a/nfs1: no mount point specified.\n".to_vec(),
        };
        assert!(output_indicates_absent_mount(&output));
    }

    #[test]
    fn output_indicates_absent_export_accepts_not_found_shape() {
        let output = std::process::Output {
            status: ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: b"exportfs: Could not find '127.0.0.1:/tmp/lab/exports/nfs1' to unexport.\n"
                .to_vec(),
        };
        assert!(output_indicates_absent_export(&output));
    }
}
