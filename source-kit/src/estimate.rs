//! Passive, caller-neutral budget estimation. No source execution or permission issuance.
use crate::{SourceExecutionLimits, SourceObservationReceipt};
use capanix_app_sdk::{CnxError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessUsage {
    pub context_key: String,
    pub observed_at_ms: u64,
    pub outcome: String,
    pub allocated: SourceExecutionLimits,
    #[serde(default)]
    pub receipt: Option<SourceObservationReceipt>,
    #[serde(default)]
    pub truncated: bool,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BudgetOverrides {
    pub max_latency_ms: Option<u64>,
    pub max_bytes: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetEstimateRequest {
    pub now_ms: u64,
    pub context_key: String,
    pub defaults: SourceExecutionLimits,
    pub hard_caps: Vec<SourceExecutionLimits>,
    #[serde(default)]
    pub history: Vec<AccessUsage>,
    #[serde(default)]
    pub overrides: BudgetOverrides,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetFieldEstimate {
    pub effective: u64,
    pub hard_cap: u64,
    pub mode: String,
    pub basis: String,
    pub sample_count: usize,
    pub insufficient: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetEstimate {
    pub limits: SourceExecutionLimits,
    pub max_latency_ms: BudgetFieldEstimate,
    pub max_bytes: BudgetFieldEstimate,
}

pub fn estimate_source_budget(request: &BudgetEstimateRequest) -> Result<BudgetEstimate> {
    request.defaults.validate()?;
    if request.context_key.trim().is_empty() || request.hard_caps.is_empty() {
        return Err(CnxError::InvalidInput(
            "budget context and hard bounds are required".into(),
        ));
    }
    let mut caps = request.hard_caps[0].clone();
    for cap in &request.hard_caps {
        cap.validate()?;
        caps.max_rows = caps.max_rows.min(cap.max_rows);
        caps.max_bytes = caps.max_bytes.min(cap.max_bytes);
        caps.max_latency_ms = caps.max_latency_ms.min(cap.max_latency_ms);
        caps.max_calls = caps.max_calls.min(cap.max_calls);
        caps.max_fanout = caps.max_fanout.min(cap.max_fanout);
        caps.max_cost_units = caps.max_cost_units.min(cap.max_cost_units);
    }
    let mut limits = request.defaults.clone();
    if limits.max_rows > caps.max_rows
        || limits.max_calls > caps.max_calls
        || limits.max_fanout > caps.max_fanout
        || limits.max_cost_units > caps.max_cost_units
    {
        return Err(CnxError::InvalidInput(
            "declared non-adaptive limit exceeds hard bound".into(),
        ));
    }
    let mut history: Vec<_> = request
        .history
        .iter()
        .rev()
        .filter(|s| {
            s.context_key == request.context_key
                && s.observed_at_ms <= request.now_ms
                && request.now_ms.saturating_sub(s.observed_at_ms) < 86_400_000
        })
        .collect();
    history.sort_by_key(|s| std::cmp::Reverse(s.observed_at_ms));
    history.truncate(100);
    let successes: Vec<_> = history
        .iter()
        .filter_map(|s| {
            let receipt = s.receipt.as_ref()?;
            (s.outcome == "success"
                && !s.truncated
                && !receipt.receipt_digest.is_empty()
                && receipt.actual.bytes > 0
                && receipt.observed_at_ms == s.observed_at_ms)
                .then_some(&receipt.actual)
        })
        .collect();
    let newest = history.first().copied();
    let latency = estimate_field(
        limits.max_latency_ms,
        caps.max_latency_ms,
        1000,
        request.overrides.max_latency_ms,
        successes.iter().map(|v| v.latency_ms).collect(),
        newest
            .filter(|s| s.outcome == "timeout")
            .map(|s| s.allocated.max_latency_ms),
    );
    let bytes = estimate_field(
        limits.max_bytes,
        caps.max_bytes,
        65536,
        request.overrides.max_bytes,
        successes.iter().map(|v| v.bytes).collect(),
        newest
            .filter(|s| s.outcome == "bytes-exhausted")
            .map(|s| s.allocated.max_bytes),
    );
    let latency = latency?;
    let bytes = bytes?;
    limits.max_latency_ms = latency.effective;
    limits.max_bytes = bytes.effective;
    Ok(BudgetEstimate {
        limits,
        max_latency_ms: latency,
        max_bytes: bytes,
    })
}

fn estimate_field(
    default: u64,
    cap: u64,
    floor: u64,
    manual: Option<u64>,
    mut samples: Vec<u64>,
    exhausted: Option<u64>,
) -> Result<BudgetFieldEstimate> {
    let count = samples.len();
    if let Some(value) = manual {
        if value == 0 || value > cap {
            return Err(CnxError::InvalidInput(
                "manual budget exceeds hard bound or is zero".into(),
            ));
        }
        return Ok(BudgetFieldEstimate {
            effective: value,
            hard_cap: cap,
            mode: "manual".into(),
            basis: "operator override".into(),
            sample_count: count,
            insufficient: false,
        });
    }
    let (mut estimate, mut basis) = if count < 10 {
        (
            default,
            "native default: fewer than 10 valid successes".to_string(),
        )
    } else {
        samples.sort_unstable();
        let p95 = samples[(95 * count).div_ceil(100) - 1];
        (
            p95.saturating_add(p95.div_ceil(2)).max(floor),
            "ceil(1.5 * nearest-rank P95), with floor".to_string(),
        )
    };
    if let Some(failed) = exhausted {
        estimate = estimate.max(failed.saturating_mul(2));
        basis.push_str("; next allocation >= 2x explicit exhaustion (no retry)");
    }
    let insufficient = estimate > cap;
    if insufficient {
        basis.push_str("; hard bound insufficient: narrow request or review deployment policy");
    }
    Ok(BudgetFieldEstimate {
        effective: estimate.min(cap),
        hard_cap: cap,
        mode: "automatic".into(),
        basis,
        sample_count: count,
        insufficient,
    })
}
