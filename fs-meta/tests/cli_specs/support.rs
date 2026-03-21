use std::fs;
use std::path::{Path, PathBuf};

pub fn combined_source_text() -> String {
    let mut buf = String::new();
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(PathBuf::from)
        .expect("fs-meta container root");
    visit(&manifest_dir.join("tooling/src"), &mut buf);
    visit(&manifest_dir.join("app/src"), &mut buf);
    buf
}

fn visit(path: &Path, out: &mut String) {
    if path.is_file() {
        if path.extension().and_then(|v| v.to_str()) == Some("rs") {
            out.push_str(&fs::read_to_string(path).unwrap_or_default());
            out.push('\n');
        }
        return;
    }
    let Ok(read_dir) = fs::read_dir(path) else {
        return;
    };
    let mut entries: Vec<_> = read_dir.filter_map(Result::ok).collect();
    entries.sort_by_key(|e| e.path());
    for entry in entries {
        visit(&entry.path(), out);
    }
}

pub fn package_manifest_text() -> String {
    fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(PathBuf::from)
            .expect("fs-meta container root")
            .join("tooling/Cargo.toml"),
    )
    .expect("read fs-meta cli manifest")
}

pub fn fsmeta_source_text() -> String {
    fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(PathBuf::from)
            .expect("fs-meta container root")
            .join("tooling/src/bin/fsmeta.rs"),
    )
    .expect("read fs-meta cli source")
}

pub fn launcher_source_text() -> String {
    fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(PathBuf::from)
            .expect("fs-meta container root")
            .join("tooling/src/bin/fsmeta-locald.rs"),
    )
    .expect("read fs-meta daemon launcher source")
}
