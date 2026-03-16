#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct CpuSampleSummary {
    pub per_node_mean_delta: BTreeMap<String, f64>,
    pub cluster_mean_delta: f64,
    pub per_node_p95_delta: BTreeMap<String, f64>,
}

pub fn measure_cpu_budget(
    baseline_pids_by_node: &BTreeMap<String, Vec<u32>>,
    steady_pids_by_node: &BTreeMap<String, Vec<u32>>,
    sample_interval: Duration,
    duration: Duration,
) -> Result<CpuSampleSummary, String> {
    let baseline = sample_many(baseline_pids_by_node, sample_interval, duration)?;
    let steady = sample_many(steady_pids_by_node, sample_interval, duration)?;
    let mut per_node_mean_delta = BTreeMap::new();
    let mut per_node_p95_delta = BTreeMap::new();
    let mut all_means = Vec::new();
    for (node, steady_samples) in &steady {
        let base_samples = baseline.get(node).cloned().unwrap_or_default();
        let deltas = steady_samples
            .iter()
            .zip(base_samples.iter().chain(std::iter::repeat(&0.0)))
            .map(|(s, b)| (s - b).max(0.0))
            .collect::<Vec<_>>();
        let mean = if deltas.is_empty() {
            0.0
        } else {
            deltas.iter().sum::<f64>() / deltas.len() as f64
        };
        let mut sorted = deltas.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let p95 = if sorted.is_empty() {
            0.0
        } else {
            sorted[((sorted.len() as f64 * 0.95).floor() as usize).min(sorted.len() - 1)]
        };
        per_node_mean_delta.insert(node.clone(), mean);
        per_node_p95_delta.insert(node.clone(), p95);
        all_means.push(mean);
    }
    let cluster_mean_delta = if all_means.is_empty() {
        0.0
    } else {
        all_means.iter().sum::<f64>() / all_means.len() as f64
    };
    Ok(CpuSampleSummary {
        per_node_mean_delta,
        cluster_mean_delta,
        per_node_p95_delta,
    })
}

pub fn assert_cpu_budget(summary: &CpuSampleSummary) -> Result<(), String> {
    for (node, mean) in &summary.per_node_mean_delta {
        if *mean > 5.0 {
            return Err(format!(
                "node {node} steady cpu mean delta exceeded budget: {mean:.2}% > 5%"
            ));
        }
    }
    if summary.cluster_mean_delta > 5.0 {
        return Err(format!(
            "cluster steady cpu mean delta exceeded budget: {:.2}% > 5%",
            summary.cluster_mean_delta
        ));
    }
    for (node, p95) in &summary.per_node_p95_delta {
        if *p95 > 8.0 {
            return Err(format!(
                "node {node} steady cpu p95 delta exceeded budget: {p95:.2}% > 8%"
            ));
        }
    }
    Ok(())
}

fn sample_many(
    pids_by_node: &BTreeMap<String, Vec<u32>>,
    sample_interval: Duration,
    duration: Duration,
) -> Result<BTreeMap<String, Vec<f64>>, String> {
    let sample_count = (duration.as_secs_f64() / sample_interval.as_secs_f64()).ceil() as usize;
    let mut out = BTreeMap::<String, Vec<f64>>::new();
    for _ in 0..sample_count.max(1) {
        let start = Instant::now();
        for (node, pids) in pids_by_node {
            out.entry(node.clone())
                .or_default()
                .push(sample_total_percent(pids)?);
        }
        let elapsed = start.elapsed();
        if elapsed < sample_interval {
            thread::sleep(sample_interval - elapsed);
        }
    }
    Ok(out)
}

fn sample_total_percent(pids: &[u32]) -> Result<f64, String> {
    let clk_tck = clock_ticks_per_second()? as f64;
    let proc1 = read_process_jiffies(pids)?;
    let total1 = read_total_jiffies()?;
    thread::sleep(Duration::from_millis(200));
    let proc2 = read_process_jiffies(pids)?;
    let total2 = read_total_jiffies()?;
    let proc_delta = (proc2.saturating_sub(proc1)) as f64;
    let total_delta = (total2.saturating_sub(total1)) as f64;
    if total_delta <= 0.0 {
        return Ok(0.0);
    }
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1) as f64;
    let _ = clk_tck;
    Ok((proc_delta / total_delta) * cpu_count * 100.0)
}

fn read_process_jiffies(pids: &[u32]) -> Result<u64, String> {
    let mut total = 0u64;
    for pid in pids {
        let path = PathBuf::from(format!("/proc/{pid}/stat"));
        let raw = fs::read_to_string(&path)
            .map_err(|e| format!("read {} failed: {e}", path.display()))?;
        let rparen = raw
            .rfind(')')
            .ok_or_else(|| format!("unexpected stat format in {}", path.display()))?;
        let fields = raw[rparen + 2..].split_whitespace().collect::<Vec<_>>();
        let utime: u64 = fields
            .get(11)
            .ok_or_else(|| format!("utime missing in {}", path.display()))?
            .parse()
            .map_err(|e| format!("parse utime in {} failed: {e}", path.display()))?;
        let stime: u64 = fields
            .get(12)
            .ok_or_else(|| format!("stime missing in {}", path.display()))?
            .parse()
            .map_err(|e| format!("parse stime in {} failed: {e}", path.display()))?;
        total += utime + stime;
    }
    Ok(total)
}

fn read_total_jiffies() -> Result<u64, String> {
    let raw =
        fs::read_to_string("/proc/stat").map_err(|e| format!("read /proc/stat failed: {e}"))?;
    let cpu = raw
        .lines()
        .find(|line| line.starts_with("cpu "))
        .ok_or_else(|| "missing aggregate cpu line in /proc/stat".to_string())?;
    let total = cpu
        .split_whitespace()
        .skip(1)
        .map(|v| {
            v.parse::<u64>()
                .map_err(|e| format!("parse /proc/stat field failed: {e}"))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sum();
    Ok(total)
}

fn clock_ticks_per_second() -> Result<u64, String> {
    let output = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .map_err(|e| format!("run getconf CLK_TCK failed: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "getconf CLK_TCK failed with status {}",
            output.status
        ));
    }
    let raw = String::from_utf8(output.stdout)
        .map_err(|e| format!("decode getconf CLK_TCK failed: {e}"))?;
    raw.trim()
        .parse::<u64>()
        .map_err(|e| format!("parse CLK_TCK failed: {e}"))
}
