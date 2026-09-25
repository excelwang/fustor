use std::fs;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

use capanix_app_sdk::{CnxError, Result};
use csv::{Reader, ReaderBuilder, StringRecord};
use csv_source::ResolvedCsvEndpoint;
use csv_source::shared_types::{
    CsvFieldCatalog, CsvFieldDescriptor, CsvProbeStatus, CsvRecord, CsvSnapshotCursor,
};
use serde_json::{Map, Value};
use source_kit::{SourceOperationBudget, filesystem_revision};

pub trait CsvSourceDriver: Send + Sync {
    fn probe(&self, endpoint: &ResolvedCsvEndpoint) -> Result<CsvProbeStatus>;

    fn probe_with_budget(
        &self,
        endpoint: &ResolvedCsvEndpoint,
        budget: &mut SourceOperationBudget,
    ) -> Result<CsvProbeStatus> {
        budget.check_deadline()?;
        self.probe(endpoint)
    }

    fn discover_fields(
        &self,
        endpoint: &ResolvedCsvEndpoint,
        path: &str,
    ) -> Result<CsvFieldCatalog>;

    fn discover_fields_with_budget(
        &self,
        endpoint: &ResolvedCsvEndpoint,
        path: &str,
        budget: &mut SourceOperationBudget,
    ) -> Result<CsvFieldCatalog> {
        budget.check_deadline()?;
        self.discover_fields(endpoint, path)
    }

    fn snapshot_page(
        &self,
        endpoint: &ResolvedCsvEndpoint,
        path: &str,
        limit: usize,
        cursor: Option<&CsvSnapshotCursor>,
    ) -> Result<CsvRecordPage>;

    fn snapshot_page_with_budget(
        &self,
        endpoint: &ResolvedCsvEndpoint,
        path: &str,
        limit: usize,
        cursor: Option<&CsvSnapshotCursor>,
        budget: &mut SourceOperationBudget,
    ) -> Result<CsvRecordPage> {
        budget.check_deadline()?;
        self.snapshot_page(endpoint, path, limit, cursor)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CsvRecordPage {
    pub fields: Vec<String>,
    pub records: Vec<CsvRecord>,
    pub source_revision: String,
    pub next_cursor: Option<CsvSnapshotCursor>,
    pub has_more: bool,
    pub diagnostics: Option<String>,
}

#[derive(Default)]
pub struct NativeCsvSourceDriver;

impl CsvSourceDriver for NativeCsvSourceDriver {
    fn probe(&self, endpoint: &ResolvedCsvEndpoint) -> Result<CsvProbeStatus> {
        let mut budget = SourceOperationBudget::unbounded();
        self.probe_with_budget(endpoint, &mut budget)
    }

    fn probe_with_budget(
        &self,
        endpoint: &ResolvedCsvEndpoint,
        budget: &mut SourceOperationBudget,
    ) -> Result<CsvProbeStatus> {
        budget.check_deadline()?;
        let root = canonical_root(&endpoint.root_path)?;
        let metadata = fs::metadata(&root).map_err(fs_error("probe csv root"))?;
        budget.check_deadline()?;
        Ok(CsvProbeStatus {
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
        endpoint: &ResolvedCsvEndpoint,
        path: &str,
    ) -> Result<CsvFieldCatalog> {
        let mut budget = SourceOperationBudget::unbounded();
        self.discover_fields_with_budget(endpoint, path, &mut budget)
    }

    fn discover_fields_with_budget(
        &self,
        endpoint: &ResolvedCsvEndpoint,
        path: &str,
        budget: &mut SourceOperationBudget,
    ) -> Result<CsvFieldCatalog> {
        budget.check_deadline()?;
        let target = resolve_file(endpoint, path)?;
        let mut reader = csv_reader_with_budget(endpoint, &target, budget)?;
        let names = if endpoint.has_headers {
            reader
                .headers()
                .map_err(csv_error("read csv header"))?
                .iter()
                .map(normalize_field_name)
                .collect()
        } else {
            reader
                .records()
                .next()
                .transpose()
                .map_err(csv_error("read csv field sample"))?
                .map(|record| generated_fields(record.len()))
                .unwrap_or_default()
        };
        Ok(CsvFieldCatalog {
            object_ref: endpoint.object_ref.clone(),
            path: path.to_string(),
            fields: names
                .into_iter()
                .map(|name| CsvFieldDescriptor {
                    name,
                    field_type: "string".into(),
                })
                .collect(),
        })
    }

    fn snapshot_page(
        &self,
        endpoint: &ResolvedCsvEndpoint,
        path: &str,
        limit: usize,
        cursor: Option<&CsvSnapshotCursor>,
    ) -> Result<CsvRecordPage> {
        let mut budget = SourceOperationBudget::unbounded();
        self.snapshot_page_with_budget(endpoint, path, limit, cursor, &mut budget)
    }

    fn snapshot_page_with_budget(
        &self,
        endpoint: &ResolvedCsvEndpoint,
        path: &str,
        limit: usize,
        cursor: Option<&CsvSnapshotCursor>,
        budget: &mut SourceOperationBudget,
    ) -> Result<CsvRecordPage> {
        budget.check_deadline()?;
        let target = resolve_file(endpoint, path)?;
        let metadata = fs::metadata(&target).map_err(fs_error("stat csv snapshot file"))?;
        let source_revision = filesystem_revision(&metadata)?;
        if let Some(cursor) = cursor
            && cursor.source_revision != source_revision
        {
            return Err(CnxError::NotReady(
                "csv file changed during snapshot continuation".into(),
            ));
        }
        let mut reader = csv_reader_with_budget(endpoint, &target, budget)?;
        let mut fields = if endpoint.has_headers {
            reader
                .headers()
                .map_err(csv_error("read csv header"))?
                .iter()
                .map(normalize_field_name)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let offset = cursor.map(|cursor| cursor.row_offset).unwrap_or(0);
        let mut records = Vec::new();
        let mut has_more = false;

        let mut record_index = 0_u64;
        let mut record = StringRecord::new();
        loop {
            reader.get_ref().budget.check_deadline()?;
            if !reader
                .read_record(&mut record)
                .map_err(csv_error("read csv record"))?
            {
                break;
            }
            let record_offset = record_index;
            record_index += 1;
            if record_offset < offset {
                continue;
            }
            if fields.is_empty() {
                fields = generated_fields(record.len());
            }
            if records.len() == limit {
                has_more = true;
                break;
            }
            let row_number = if endpoint.has_headers {
                record_offset + 2
            } else {
                record_offset + 1
            };
            records.push(CsvRecord {
                row_number,
                values: record_to_json(&fields, &record),
            });
        }

        let next_cursor = has_more.then_some(CsvSnapshotCursor {
            row_offset: offset + records.len() as u64,
            source_revision: source_revision.clone(),
        });
        let final_revision = filesystem_revision(
            &fs::metadata(&target).map_err(fs_error("restat csv snapshot file"))?,
        )?;
        if final_revision != source_revision {
            return Err(CnxError::NotReady(
                "csv file changed while reading snapshot page".into(),
            ));
        }
        Ok(CsvRecordPage {
            fields,
            records,
            source_revision,
            next_cursor,
            has_more,
            diagnostics: None,
        })
    }
}

#[derive(Default)]
pub struct UnsupportedCsvSourceDriver;

impl CsvSourceDriver for UnsupportedCsvSourceDriver {
    fn probe(&self, _endpoint: &ResolvedCsvEndpoint) -> Result<CsvProbeStatus> {
        Err(CnxError::NotSupported(
            "csv-source native CSV driver is not linked in this runtime artifact".into(),
        ))
    }

    fn discover_fields(
        &self,
        _endpoint: &ResolvedCsvEndpoint,
        _path: &str,
    ) -> Result<CsvFieldCatalog> {
        Err(CnxError::NotSupported(
            "csv-source field discovery requires the native CSV driver".into(),
        ))
    }

    fn snapshot_page(
        &self,
        _endpoint: &ResolvedCsvEndpoint,
        _path: &str,
        _limit: usize,
        _cursor: Option<&CsvSnapshotCursor>,
    ) -> Result<CsvRecordPage> {
        Err(CnxError::NotSupported(
            "csv-source snapshot requires the native CSV driver".into(),
        ))
    }
}

fn resolve_file(endpoint: &ResolvedCsvEndpoint, path: &str) -> Result<PathBuf> {
    let root = canonical_root(&endpoint.root_path)?;
    let target = resolve_path(&root, path)?;
    let metadata = fs::metadata(&target).map_err(fs_error("stat csv path"))?;
    if !metadata.is_file() {
        return Err(CnxError::InvalidInput(format!(
            "csv path '{path}' must resolve to a regular file"
        )));
    }
    Ok(target)
}

fn canonical_root(root_path: &str) -> Result<PathBuf> {
    let root = Path::new(root_path);
    if !root.is_absolute() {
        return Err(CnxError::InvalidInput(format!(
            "csv root_path '{root_path}' must be absolute"
        )));
    }
    let root = root
        .canonicalize()
        .map_err(fs_error("canonicalize csv root_path"))?;
    let metadata = fs::metadata(&root).map_err(fs_error("stat csv root_path"))?;
    if !metadata.is_dir() {
        return Err(CnxError::InvalidInput(format!(
            "csv root_path '{}' is not a directory",
            root.to_string_lossy()
        )));
    }
    Ok(root)
}

fn resolve_path(root: &Path, request_path: &str) -> Result<PathBuf> {
    let request_path = request_path.trim();
    if request_path.is_empty() {
        return Err(CnxError::InvalidInput("csv path is required".into()));
    }
    let mut candidate = root.to_path_buf();
    for component in Path::new(request_path).components() {
        match component {
            Component::Normal(part) => candidate.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(CnxError::ScopeDenied(format!(
                    "csv path '{request_path}' escapes its configured root"
                )));
            }
        }
    }
    let canonical = candidate
        .canonicalize()
        .map_err(fs_error("canonicalize csv request path"))?;
    if !canonical.starts_with(root) {
        return Err(CnxError::ScopeDenied(format!(
            "csv path '{request_path}' escapes its configured root"
        )));
    }
    Ok(canonical)
}

fn csv_reader_with_budget<'a>(
    endpoint: &ResolvedCsvEndpoint,
    path: &Path,
    budget: &'a mut SourceOperationBudget,
) -> Result<Reader<BudgetedReader<'a, File>>> {
    let file = File::open(path).map_err(fs_error("open csv file"))?;
    Ok(ReaderBuilder::new()
        .delimiter(endpoint.delimiter)
        .has_headers(endpoint.has_headers)
        .from_reader(BudgetedReader {
            inner: file,
            budget,
        }))
}

struct BudgetedReader<'a, R> {
    inner: R,
    budget: &'a mut SourceOperationBudget,
}

impl<R: Read> Read for BudgetedReader<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.budget.check_deadline().map_err(budget_io_error)?;
        let allowed = match self.budget.remaining_bytes() {
            Some(0) => {
                let mut probe = [0_u8; 1];
                return match self.inner.read(&mut probe)? {
                    0 => Ok(0),
                    _ => Err(budget_io_error(CnxError::ResourceExhausted(
                        "source operation max_bytes exhausted".into(),
                    ))),
                };
            }
            Some(remaining) => usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(buffer.len()),
            None => buffer.len(),
        };
        let read = self.inner.read(&mut buffer[..allowed])?;
        self.budget
            .consume_bytes(read as u64)
            .map_err(budget_io_error)?;
        Ok(read)
    }
}

#[derive(Debug)]
enum BudgetIoFailure {
    Timeout,
    Bytes,
}

impl std::fmt::Display for BudgetIoFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => formatter.write_str("source operation deadline exhausted"),
            Self::Bytes => formatter.write_str("source operation max_bytes exhausted"),
        }
    }
}

impl std::error::Error for BudgetIoFailure {}

fn budget_io_error(error: CnxError) -> io::Error {
    let failure = match error {
        CnxError::Timeout => BudgetIoFailure::Timeout,
        _ => BudgetIoFailure::Bytes,
    };
    io::Error::other(failure)
}

fn record_to_json(fields: &[String], record: &StringRecord) -> Value {
    let mut object = Map::with_capacity(record.len());
    for (index, value) in record.iter().enumerate() {
        let field = fields
            .get(index)
            .cloned()
            .unwrap_or_else(|| format!("field_{}", index + 1));
        object.insert(field, Value::String(value.to_string()));
    }
    Value::Object(object)
}

fn generated_fields(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("field_{}", index + 1))
        .collect()
}

fn normalize_field_name(name: &str) -> String {
    if name.trim().is_empty() {
        "field".into()
    } else {
        name.trim().to_string()
    }
}

fn fs_error(context: &'static str) -> impl Fn(std::io::Error) -> CnxError {
    move |err| CnxError::PeerError(format!("{context}: {err}"))
}

fn csv_error(context: &'static str) -> impl Fn(csv::Error) -> CnxError {
    move |err| {
        if let csv::ErrorKind::Io(error) = err.kind()
            && let Some(failure) = error
                .get_ref()
                .and_then(|error| error.downcast_ref::<BudgetIoFailure>())
        {
            return match failure {
                BudgetIoFailure::Timeout => CnxError::Timeout,
                BudgetIoFailure::Bytes => {
                    CnxError::ResourceExhausted("source operation max_bytes exhausted".into())
                }
            };
        }
        CnxError::PeerError(format!("{context}: {err}"))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use csv_source::ResolvedCsvEndpoint;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn endpoint(root_path: String) -> ResolvedCsvEndpoint {
        ResolvedCsvEndpoint {
            object_ref: "csv".into(),
            root_path,
            path_scopes: vec!["*".into()],
            has_headers: true,
            delimiter: b',',
            grant_epoch: Some(1),
        }
    }

    #[test]
    fn snapshot_pages_csv_records() {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("a.csv"), b"id,name\n1,alice\n2,bob\n").expect("write csv");
        let driver = NativeCsvSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());

        let page = driver
            .snapshot_page(&endpoint, "a.csv", 1, None)
            .expect("first page");

        assert_eq!(page.fields, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(page.records[0].row_number, 2);
        assert_eq!(page.records[0].values, json!({"id": "1", "name": "alice"}));
        let cursor = page.next_cursor.as_ref().expect("cursor");
        assert_eq!(cursor.row_offset, 1);
        assert!(!cursor.source_revision.is_empty());

        let page = driver
            .snapshot_page(&endpoint, "a.csv", 10, page.next_cursor.as_ref())
            .expect("second page");
        assert_eq!(page.records[0].values, json!({"id": "2", "name": "bob"}));
        assert!(!page.has_more);
    }

    #[test]
    fn parent_path_is_rejected() {
        let dir = tempdir().expect("tempdir");
        let driver = NativeCsvSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());

        let err = driver
            .snapshot_page(&endpoint, "../outside.csv", 10, None)
            .unwrap_err();

        assert!(matches!(err, CnxError::ScopeDenied(_)));
    }

    #[test]
    fn parser_handles_quoted_fields() {
        let dir = tempdir().expect("tempdir");
        fs::write(
            dir.path().join("quoted.csv"),
            b"id,name,note\n1,\"alice, a.\",\"x\"\"y\"\n",
        )
        .expect("write csv");
        let driver = NativeCsvSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());

        let page = driver
            .snapshot_page(&endpoint, "quoted.csv", 10, None)
            .expect("quoted page");

        assert_eq!(page.records[0].values["name"], json!("alice, a."));
        assert_eq!(page.records[0].values["note"], json!("x\"y"));
    }

    #[test]
    fn file_mutation_invalidates_snapshot_cursor() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("changing.csv");
        fs::write(&path, b"id,name\n1,alice\n2,bob\n").expect("write csv");
        let driver = NativeCsvSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());
        let first = driver
            .snapshot_page(&endpoint, "changing.csv", 1, None)
            .expect("first page");
        let cursor = first.next_cursor.expect("cursor");
        fs::write(&path, b"id,name\n1,alice\n2,bob\n3,carol\n").expect("mutate csv");

        let error = driver
            .snapshot_page(&endpoint, "changing.csv", 1, Some(&cursor))
            .expect_err("mutated csv must invalidate cursor");

        assert!(matches!(error, CnxError::NotReady(_)));
    }

    #[test]
    fn csv_reader_enforces_byte_and_deadline_budgets_during_parse() {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("a.csv"), b"id,name\n1,alice\n").expect("write csv");
        let driver = NativeCsvSourceDriver;
        let endpoint = endpoint(dir.path().to_string_lossy().to_string());
        let mut byte_budget = SourceOperationBudget::new(None, Some(4));

        let error = driver
            .snapshot_page_with_budget(&endpoint, "a.csv", 10, None, &mut byte_budget)
            .expect_err("CSV read must stop at byte budget");
        assert!(matches!(error, CnxError::ResourceExhausted(_)));

        let mut deadline_budget = SourceOperationBudget::new(Some(Duration::ZERO), None);
        let error = driver
            .snapshot_page_with_budget(&endpoint, "a.csv", 10, None, &mut deadline_budget)
            .expect_err("CSV read must stop at deadline");
        assert!(matches!(error, CnxError::Timeout));
    }
}
