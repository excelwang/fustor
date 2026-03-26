use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use tempfile::TempDir;

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
    _temp: TempDir,
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
        let temp =
            tempfile::tempdir().map_err(|e| format!("create NFS lab tempdir failed: {e}"))?;
        let exports_dir = temp.path().join("exports");
        let mounts_dir = temp.path().join("mounts");
        fs::create_dir_all(&exports_dir).map_err(|e| format!("create exports dir failed: {e}"))?;
        fs::create_dir_all(&mounts_dir).map_err(|e| format!("create mounts dir failed: {e}"))?;

        let mut lab = Self {
            _temp: temp,
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

    fn create_export(&mut self, export_name: &str) -> Result<PathBuf, String> {
        let export_dir = self.exports_dir.join(export_name);
        fs::create_dir_all(&export_dir)
            .map_err(|e| format!("create export dir {export_name} failed: {e}"))?;
        self.seed_export_tree(&export_dir, export_name)?;
        self.export_dir(&export_dir)?;
        Ok(export_dir)
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

    fn mutable_export_root(&self, export_name: &str) -> PathBuf {
        self.mounted
            .iter()
            .find_map(|((_, export), mount_path)| {
                (export == export_name).then(|| mount_path.clone())
            })
            .unwrap_or_else(|| self.exports_dir.join(export_name))
    }

    pub fn export_source(&self, export_name: &str) -> String {
        format!("127.0.0.1:{}", self.exports_dir.join(export_name).display())
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

    fn unmount_export(&mut self, node_name: &str, export_name: &str) -> Result<(), String> {
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
        let status = sudo_status(["exportfs", "-u", &format!("127.0.0.1:{}", dir.display())])?;
        if !status.success() {
            return Err(format!(
                "exportfs remove {} failed with status {}",
                dir.display(),
                status
            ));
        }
        Ok(())
    }
}

impl Drop for NfsLab {
    fn drop(&mut self) {
        let mounts = self.mounted.keys().cloned().collect::<Vec<_>>();
        for (node, export) in mounts {
            let _ = self.unmount_export(&node, &export);
        }
        let exports = fs::read_dir(&self.exports_dir)
            .ok()
            .into_iter()
            .flat_map(|rows| rows.flatten())
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        for dir in exports {
            let _ = self.unexport_dir(&dir);
        }
        if let Some(child) = &mut self.started_mountd {
            let _ = child.kill();
            let _ = child.wait();
        }
        if self.started_rpcbind {
            let _ = sudo_status(["pkill", "-x", "rpcbind"]);
        }
        if self.mounted_nfsd {
            let _ = sudo_status(["umount", "/proc/fs/nfsd"]);
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
