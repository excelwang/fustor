//! Bounded format inspection shared by file/object owners. No transforms or writes.
use crate::{
    SourceContinuationState, SourceNativeAnchor, SourceOperationBudget, SourcePayloadObservation,
};
use capanix_app_sdk::{CnxError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceContentSample {
    pub object_ref: String,
    pub native_ref: String,
    pub source_revision: String,
    pub format: String,
    pub grant_epoch: Option<u64>,
    pub rows: Vec<Value>,
    /// SHA-256 of the fetched prefix only, never an object ETag or full-file digest.
    pub prefix_sha256: String,
    pub bytes_read: u64,
    pub complete: bool,
}
impl SourceContentSample {
    pub fn observation(&self) -> SourcePayloadObservation {
        SourcePayloadObservation {
            authority_object_ref: self.object_ref.clone(),
            native_anchors: vec![SourceNativeAnchor {
                anchor_kind: "bounded-content-prefix".into(),
                native_ref: self.native_ref.clone(),
                native_pointer: Some(format!("bytes:0-{}", self.bytes_read)),
                native_version: Some(self.source_revision.clone()),
            }],
            grant_epoch: self.grant_epoch,
            actual_rows: self.rows.len() as u64,
            continuation_state: SourceContinuationState::Terminal,
            continuation: None,
        }
    }
}

pub const MAX_CONTENT_PREFIX_BYTES: u64 = 4 * 1024 * 1024;

/// Input must already be a bounded prefix, with the owner's native version pinned.
/// Incomplete final records are never accepted as valid samples.
pub fn parse_content_prefix(
    raw: &[u8],
    format: &str,
    complete: bool,
    limit: usize,
    budget: &SourceOperationBudget,
) -> Result<Vec<Value>> {
    if limit == 0 || raw.len() as u64 > MAX_CONTENT_PREFIX_BYTES {
        return Err(CnxError::InvalidInput(
            "invalid content sample bounds".into(),
        ));
    }
    let mut rows = Vec::new();
    match format {
        "jsonl" => {
            for line in raw.split_inclusive(|b| *b == b'\n') {
                budget.check_deadline()?;
                if !complete && !line.ends_with(b"\n") {
                    break;
                }
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                let value: Value = serde_json::from_slice(line)
                    .map_err(|_| CnxError::InvalidInput("malformed JSONL sample".into()))?;
                if !value.is_object() {
                    return Err(CnxError::InvalidInput(
                        "JSONL record must be an object".into(),
                    ));
                }
                rows.push(value);
                if rows.len() == limit {
                    break;
                }
            }
        }
        "csv" => {
            let mut reader = csv::ReaderBuilder::new().from_reader(raw);
            let headers = reader
                .headers()
                .map_err(|_| CnxError::InvalidInput("malformed CSV header".into()))?
                .clone();
            let unique: std::collections::HashSet<_> = headers.iter().collect();
            if headers.is_empty()
                || unique.len() != headers.len()
                || headers.iter().any(str::is_empty)
            {
                return Err(CnxError::InvalidInput(
                    "CSV headers must be nonempty and unique".into(),
                ));
            }
            let mut record = csv::StringRecord::new();
            loop {
                budget.check_deadline()?;
                let result = reader.read_record(&mut record);
                // csv treats EOF as a record terminator. A range boundary isn't EOF.
                if !complete && reader.position().byte() >= raw.len() as u64 {
                    break;
                }
                if !result.map_err(|_| CnxError::InvalidInput("malformed CSV sample".into()))? {
                    break;
                }
                rows.push(Value::Object(
                    headers
                        .iter()
                        .zip(record.iter())
                        .map(|(key, value)| (key.to_owned(), Value::String(value.to_owned())))
                        .collect(),
                ));
                if rows.len() == limit {
                    break;
                }
            }
        }
        _ => {
            return Err(CnxError::InvalidInput(
                "content format must be jsonl or csv".into(),
            ));
        }
    }
    if rows.is_empty() && !complete {
        return Err(CnxError::ResourceExhausted(
            "source operation max_bytes exhausted before a complete record".into(),
        ));
    }
    Ok(rows)
}

pub fn content_prefix_digest(raw: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(raw))
}
