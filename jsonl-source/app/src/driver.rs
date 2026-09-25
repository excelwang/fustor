use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use capanix_app_sdk::{CnxError, Result};
use jsonl_source::ResolvedJsonlEndpoint;
use jsonl_source::shared_types::{
    JsonlFieldCatalog, JsonlFieldDescriptor, JsonlProbeStatus, JsonlRecord, JsonlSnapshotCursor,
};
use serde_json::Value;
use source_kit::{SourceOperationBudget, filesystem_revision};

pub trait JsonlSourceDriver: Send + Sync {
    fn probe(&self, endpoint: &ResolvedJsonlEndpoint) -> Result<JsonlProbeStatus>;

    fn probe_with_budget(
        &self,
        endpoint: &ResolvedJsonlEndpoint,
        budget: &mut SourceOperationBudget,
    ) -> Result<JsonlProbeStatus> {
        budget.check_deadline()?;
        self.probe(endpoint)
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedJsonlEndpoint,
        path: &str,
    ) -> Result<JsonlFieldCatalog>;

    fn discover_fields_with_budget(
        &self,
        endpoint: &ResolvedJsonlEndpoint,
        path: &str,
        budget: &mut SourceOperationBudget,
    ) -> Result<JsonlFieldCatalog> {
        budget.check_deadline()?;
        self.discover_fields(endpoint, path)
    }

    fn snapshot_page(
        &self,
        endpoint: &ResolvedJsonlEndpoint,
        path: &str,
        limit: usize,
        cursor: Option<&JsonlSnapshotCursor>,
    ) -> Result<JsonlRecordPage>;

    fn snapshot_page_with_budget(
        &self,
        endpoint: &ResolvedJsonlEndpoint,
        path: &str,
        limit: usize,
        cursor: Option<&JsonlSnapshotCursor>,
        budget: &mut SourceOperationBudget,
    ) -> Result<JsonlRecordPage> {
        budget.check_deadline()?;
        self.snapshot_page(endpoint, path, limit, cursor)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JsonlRecordPage {
    pub records: Vec<JsonlRecord>,
    pub source_revision: String,
    pub next_cursor: Option<JsonlSnapshotCursor>,
    pub has_more: bool,
    pub diagnostics: Option<String>,
}

#[derive(Default)]
pub struct NativeJsonlSourceDriver;

impl JsonlSourceDriver for NativeJsonlSourceDriver {
    fn probe(&self, endpoint: &ResolvedJsonlEndpoint) -> Result<JsonlProbeStatus> {
        let mut budget = SourceOperationBudget::unbounded();
        self.probe_with_budget(endpoint, &mut budget)
    }

    fn probe_with_budget(
        &self,
        endpoint: &ResolvedJsonlEndpoint,
        budget: &mut SourceOperationBudget,
    ) -> Result<JsonlProbeStatus> {
        budget.check_deadline()?;
        let root = canonical_root(&endpoint.root_path)?;
        let metadata = fs::metadata(&root).map_err(fs_error("probe jsonl root"))?;
        budget.check_deadline()?;
        Ok(JsonlProbeStatus {
            object_ref: endpoint.object_ref.clone(),
            root_path: root.to_string_lossy().to_string(),
            reachable: metadata.is_dir(),
            diagnostics: if metadata.is_dir() {
                None
            } else {
                Some("root_path is not a directory".into())
            },
        })
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedJsonlEndpoint,
        path: &str,
    ) -> Result<JsonlFieldCatalog> {
        let mut budget = SourceOperationBudget::unbounded();
        self.discover_fields_with_budget(endpoint, path, &mut budget)
    }

    fn discover_fields_with_budget(
        &self,
        endpoint: &ResolvedJsonlEndpoint,
        path: &str,
        budget: &mut SourceOperationBudget,
    ) -> Result<JsonlFieldCatalog> {
        budget.check_deadline()?;
        let target = resolve_file(endpoint, path)?;
        let file = File::open(target).map_err(fs_error("open jsonl file"))?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        let mut line_number = 0_u64;
        let mut sampled = 0_usize;
        let mut fields = BTreeMap::<String, String>::new();
        while sampled < endpoint.field_sample_limit {
            line.clear();
            let bytes = read_line_with_budget(&mut reader, &mut line, budget)?;
            if bytes == 0 {
                break;
            }
            line_number += 1;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            sampled += 1;
            let value = parse_json_line(trimmed, line_number)?;
            if let Value::Object(object) = value {
                for (key, value) in object {
                    fields
                        .entry(key)
                        .or_insert_with(|| json_value_type(&value).to_string());
                }
            }
        }
        Ok(JsonlFieldCatalog {
            object_ref: endpoint.object_ref.clone(),
            path: path.to_string(),
            fields: fields
                .into_iter()
                .map(|(name, field_type)| JsonlFieldDescriptor { name, field_type })
                .collect(),
        })
    }

    fn snapshot_page(
        &self,
        endpoint: &ResolvedJsonlEndpoint,
        path: &str,
        limit: usize,
        cursor: Option<&JsonlSnapshotCursor>,
    ) -> Result<JsonlRecordPage> {
        let mut budget = SourceOperationBudget::unbounded();
        self.snapshot_page_with_budget(endpoint, path, limit, cursor, &mut budget)
    }

    fn snapshot_page_with_budget(
        &self,
        endpoint: &ResolvedJsonlEndpoint,
        path: &str,
        limit: usize,
        cursor: Option<&JsonlSnapshotCursor>,
        budget: &mut SourceOperationBudget,
    ) -> Result<JsonlRecordPage> {
        budget.check_deadline()?;
        let target = resolve_file(endpoint, path)?;
        let metadata = fs::metadata(&target).map_err(fs_error("stat jsonl snapshot file"))?;
        let source_revision = filesystem_revision(&metadata)?;
        if let Some(cursor) = cursor {
            if cursor.source_revision != source_revision {
                return Err(CnxError::NotReady(
                    "jsonl file changed during snapshot continuation".into(),
                ));
            }
            if cursor.byte_offset > metadata.len() {
                return Err(CnxError::InvalidInput(
                    "jsonl cursor byte_offset exceeds current file size".into(),
                ));
            }
        }
        let file = File::open(target).map_err(fs_error("open jsonl file"))?;
        let mut reader = BufReader::new(file);
        let mut byte_offset = cursor.map(|cursor| cursor.byte_offset).unwrap_or(0);
        let mut line_number = cursor.map(|cursor| cursor.line_number).unwrap_or(0);
        if byte_offset > 0 {
            budget.check_deadline()?;
            reader
                .seek(SeekFrom::Start(byte_offset))
                .map_err(fs_error("seek jsonl cursor"))?;
        }

        let mut line = String::new();
        let mut records = Vec::new();
        let mut last_included_offset = byte_offset;
        let mut last_included_line = line_number;
        let mut has_more = false;
        loop {
            line.clear();
            let bytes = read_line_with_budget(&mut reader, &mut line, budget)?;
            if bytes == 0 {
                break;
            }
            byte_offset += bytes as u64;
            line_number += 1;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if records.len() == limit {
                has_more = true;
                break;
            }
            let values = parse_json_line(trimmed, line_number)?;
            records.push(JsonlRecord {
                line_number,
                values,
            });
            last_included_offset = byte_offset;
            last_included_line = line_number;
        }
        let next_cursor = has_more.then_some(JsonlSnapshotCursor {
            byte_offset: last_included_offset,
            line_number: last_included_line,
            source_revision: source_revision.clone(),
        });
        let final_revision = filesystem_revision(
            &reader
                .get_ref()
                .metadata()
                .map_err(fs_error("restat jsonl snapshot file"))?,
        )?;
        if final_revision != source_revision {
            return Err(CnxError::NotReady(
                "jsonl file changed while reading snapshot page".into(),
            ));
        }
        Ok(JsonlRecordPage {
            records,
            source_revision,
            next_cursor,
            has_more,
            diagnostics: None,
        })
    }
}

fn read_line_with_budget<R: BufRead>(
    reader: &mut R,
    line: &mut String,
    budget: &mut SourceOperationBudget,
) -> Result<usize> {
    line.clear();
    let mut bytes = Vec::new();
    loop {
        budget.check_deadline()?;
        let (take, terminal) = {
            let available = reader.fill_buf().map_err(fs_error("read jsonl line"))?;
            if available.is_empty() {
                break;
            }
            let take = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |position| position + 1);
            budget.consume_bytes(take as u64)?;
            bytes.extend_from_slice(&available[..take]);
            (take, available.get(take - 1) == Some(&b'\n'))
        };
        reader.consume(take);
        if terminal {
            break;
        }
    }
    if bytes.is_empty() {
        return Ok(0);
    }
    *line = String::from_utf8(bytes)
        .map_err(|_| CnxError::PeerError("jsonl input is not valid UTF-8".into()))?;
    Ok(line.len())
}

#[derive(Default)]
pub struct UnsupportedJsonlSourceDriver;

impl JsonlSourceDriver for UnsupportedJsonlSourceDriver {
    fn probe(&self, _endpoint: &ResolvedJsonlEndpoint) -> Result<JsonlProbeStatus> {
        Err(CnxError::NotSupported(
            "jsonl-source native JSONL driver is not linked in this runtime artifact".into(),
        ))
    }

    fn discover_fields(
        &self,
        _endpoint: &ResolvedJsonlEndpoint,
        _path: &str,
    ) -> Result<JsonlFieldCatalog> {
        Err(CnxError::NotSupported(
            "jsonl-source field discovery requires the native JSONL driver".into(),
        ))
    }

    fn snapshot_page(
        &self,
        _endpoint: &ResolvedJsonlEndpoint,
        _path: &str,
        _limit: usize,
        _cursor: Option<&JsonlSnapshotCursor>,
    ) -> Result<JsonlRecordPage> {
        Err(CnxError::NotSupported(
            "jsonl-source snapshot requires the native JSONL driver".into(),
        ))
    }
}

fn resolve_file(endpoint: &ResolvedJsonlEndpoint, path: &str) -> Result<PathBuf> {
    let root = canonical_root(&endpoint.root_path)?;
    let target = resolve_path(&root, path)?;
    let metadata = fs::metadata(&target).map_err(fs_error("stat jsonl path"))?;
    if !metadata.is_file() {
        return Err(CnxError::InvalidInput(format!(
            "jsonl path '{path}' must resolve to a regular file"
        )));
    }
    Ok(target)
}

fn canonical_root(root_path: &str) -> Result<PathBuf> {
    let root = Path::new(root_path);
    if !root.is_absolute() {
        return Err(CnxError::InvalidInput(format!(
            "jsonl root_path '{root_path}' must be absolute"
        )));
    }
    let root = root
        .canonicalize()
        .map_err(fs_error("canonicalize jsonl root_path"))?;
    let metadata = fs::metadata(&root).map_err(fs_error("stat jsonl root_path"))?;
    if !metadata.is_dir() {
        return Err(CnxError::InvalidInput(format!(
            "jsonl root_path '{}' is not a directory",
            root.to_string_lossy()
        )));
    }
    Ok(root)
}

fn resolve_path(root: &Path, request_path: &str) -> Result<PathBuf> {
    let request_path = request_path.trim();
    if request_path.is_empty() {
        return Err(CnxError::InvalidInput("jsonl path is required".into()));
    }
    let mut candidate = root.to_path_buf();
    for component in Path::new(request_path).components() {
        match component {
            Component::Normal(part) => candidate.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(CnxError::ScopeDenied(format!(
                    "jsonl path '{request_path}' escapes its configured root"
                )));
            }
        }
    }
    let canonical = candidate
        .canonicalize()
        .map_err(fs_error("canonicalize jsonl request path"))?;
    if !canonical.starts_with(root) {
        return Err(CnxError::ScopeDenied(format!(
            "jsonl path '{request_path}' escapes its configured root"
        )));
    }
    Ok(canonical)
}

fn parse_json_line(line: &str, line_number: u64) -> Result<Value> {
    serde_json::from_str(line)
        .map_err(|err| CnxError::PeerError(format!("decode jsonl line {line_number}: {err}")))
}

fn json_value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn fs_error(context: &'static str) -> impl Fn(std::io::Error) -> CnxError {
    move |err| CnxError::PeerError(format!("{context}: {err}"))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use jsonl_source::ResolvedJsonlEndpoint;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn endpoint(root_path: String) -> ResolvedJsonlEndpoint {
        ResolvedJsonlEndpoint {
            object_ref: "jsonl".into(),
            root_path,
            path_scopes: vec!["*".into()],
            field_sample_limit: 100,
            grant_epoch: Some(1),
        }
    }

    #[test]
    fn snapshot_pages_jsonl_records() {
        let dir = tempdir().expect("tempdir");
        fs::write(
            dir.path().join("a.jsonl"),
            b"{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n",
        )
        .expect("write jsonl");
        let driver = NativeJsonlSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());

        let page = driver
            .snapshot_page(&endpoint, "a.jsonl", 1, None)
            .expect("first page");

        assert_eq!(page.records[0].line_number, 1);
        assert_eq!(page.records[0].values, json!({"id": 1, "name": "alice"}));
        assert_eq!(
            page.next_cursor.as_ref().map(|cursor| cursor.line_number),
            Some(1)
        );

        let page = driver
            .snapshot_page(&endpoint, "a.jsonl", 10, page.next_cursor.as_ref())
            .expect("second page");
        assert_eq!(page.records[0].values, json!({"id": 2, "name": "bob"}));
        assert!(!page.has_more);
    }

    #[test]
    fn discovers_object_fields_from_sample() {
        let dir = tempdir().expect("tempdir");
        fs::write(
            dir.path().join("a.jsonl"),
            b"{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"active\":true}\n",
        )
        .expect("write jsonl");
        let driver = NativeJsonlSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());

        let catalog = driver
            .discover_fields(&endpoint, "a.jsonl")
            .expect("discover fields");

        assert_eq!(
            catalog
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["active", "id", "name"]
        );
    }

    #[test]
    fn parent_path_is_rejected() {
        let dir = tempdir().expect("tempdir");
        let driver = NativeJsonlSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());

        let err = driver
            .snapshot_page(&endpoint, "../outside.jsonl", 10, None)
            .unwrap_err();

        assert!(matches!(err, CnxError::ScopeDenied(_)));
    }

    #[test]
    fn file_mutation_invalidates_snapshot_cursor() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("changing.jsonl");
        fs::write(&path, b"{\"id\":1}\n{\"id\":2}\n").expect("write jsonl");
        let driver = NativeJsonlSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());
        let first = driver
            .snapshot_page(&endpoint, "changing.jsonl", 1, None)
            .expect("first page");
        let cursor = first.next_cursor.expect("cursor");
        fs::write(&path, b"{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n").expect("mutate jsonl");

        let error = driver
            .snapshot_page(&endpoint, "changing.jsonl", 1, Some(&cursor))
            .expect_err("mutated jsonl must invalidate cursor");

        assert!(matches!(error, CnxError::NotReady(_)));
    }

    #[test]
    fn jsonl_reader_enforces_byte_and_deadline_budgets_before_line_allocation() {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("a.jsonl"), b"{\"id\":1}\n").expect("write jsonl");
        let driver = NativeJsonlSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());
        let mut byte_budget = SourceOperationBudget::new(None, Some(4));

        let error = driver
            .snapshot_page_with_budget(&endpoint, "a.jsonl", 10, None, &mut byte_budget)
            .expect_err("JSONL read must stop before oversized line allocation");
        assert!(matches!(error, CnxError::ResourceExhausted(_)));

        let mut deadline_budget = SourceOperationBudget::new(Some(Duration::ZERO), None);
        let error = driver
            .snapshot_page_with_budget(&endpoint, "a.jsonl", 10, None, &mut deadline_budget)
            .expect_err("JSONL read must stop at deadline");
        assert!(matches!(error, CnxError::Timeout));
    }
}
