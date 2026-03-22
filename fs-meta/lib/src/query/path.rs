use std::path::{Path, PathBuf};

#[cfg(target_family = "unix")]
use std::os::unix::ffi::{OsStrExt, OsStringExt};

pub fn bytes_to_display_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

pub fn root_file_name_bytes(path: &[u8]) -> Vec<u8> {
    if path.is_empty() || path == b"/" {
        return b"/".to_vec();
    }
    let trimmed = path.strip_suffix(b"/").unwrap_or(path);
    let start = trimmed
        .iter()
        .rposition(|b| *b == b'/')
        .map(|idx| idx + 1)
        .unwrap_or(0);
    let name = &trimmed[start..];
    if name.is_empty() {
        b"/".to_vec()
    } else {
        name.to_vec()
    }
}

pub fn is_under_query_path(path: &[u8], query_path: &[u8]) -> bool {
    if query_path.is_empty() || query_path == b"/" {
        return true;
    }
    if path == query_path {
        return true;
    }
    path.starts_with(query_path) && path.get(query_path.len()).is_some_and(|next| *next == b'/')
}

pub fn normalized_path_for_query(path: &[u8], query_path: &[u8]) -> Option<Vec<u8>> {
    if is_under_query_path(path, query_path) {
        return Some(path.to_vec());
    }
    if query_path.is_empty() || query_path == b"/" {
        return Some(path.to_vec());
    }
    if !query_path.starts_with(b"/") {
        return None;
    }
    if query_path.len() > path.len() {
        return None;
    }
    for idx in 0..=path.len() - query_path.len() {
        if &path[idx..idx + query_path.len()] != query_path {
            continue;
        }
        let candidate = path[idx..].to_vec();
        if is_under_query_path(&candidate, query_path) {
            return Some(candidate);
        }
    }
    None
}

pub fn parent_path(path: &[u8]) -> Option<Vec<u8>> {
    if path.is_empty() || path == b"/" {
        return None;
    }
    let idx = path.iter().rposition(|b| *b == b'/')?;
    if idx == 0 {
        Some(b"/".to_vec())
    } else {
        Some(path[..idx].to_vec())
    }
}

pub fn relative_depth_under_root(path: &[u8], root: &[u8]) -> Option<usize> {
    if path == root {
        return Some(0);
    }
    if root == b"/" {
        if !path.starts_with(b"/") {
            return None;
        }
        return Some(
            path.split(|b| *b == b'/')
                .filter(|seg| !seg.is_empty())
                .count(),
        );
    }
    if !path.starts_with(root) {
        return None;
    }
    let remainder = path.get(root.len()..)?;
    if remainder.is_empty() || remainder[0] != b'/' {
        return None;
    }
    Some(
        remainder
            .split(|b| *b == b'/')
            .filter(|seg| !seg.is_empty())
            .count(),
    )
}

#[cfg(target_family = "unix")]
pub fn path_to_bytes(path: &Path) -> Vec<u8> {
    path.as_os_str().as_bytes().to_vec()
}

#[cfg(target_family = "unix")]
pub fn path_buf_from_bytes(bytes: &[u8]) -> PathBuf {
    std::ffi::OsString::from_vec(bytes.to_vec()).into()
}

#[cfg(not(target_family = "unix"))]
pub fn path_to_bytes(path: &Path) -> Vec<u8> {
    path.to_string_lossy().into_owned().into_bytes()
}

#[cfg(not(target_family = "unix"))]
pub fn path_buf_from_bytes(bytes: &[u8]) -> PathBuf {
    PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
}
