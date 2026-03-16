use std::path::PathBuf;

pub fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut current = manifest_dir.clone();
    loop {
        if current.join("fs-meta").is_dir() && current.join("Cargo.toml").is_file() {
            return current;
        }
        if !current.pop() {
            return manifest_dir;
        }
    }
}

#[allow(dead_code)]
pub fn fs_meta_root() -> PathBuf {
    workspace_root().join("fs-meta")
}

pub fn capanix_repo_root() -> PathBuf {
    if let Ok(path) = std::env::var("CAPANIX_REPO") {
        return PathBuf::from(path);
    }
    workspace_root()
        .join("../capanix")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("/home/huajin/capanix"))
}
