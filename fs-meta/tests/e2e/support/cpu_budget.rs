#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

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
        let proc_before = pids_by_node
            .iter()
            .map(|(node, pids)| Ok((node.clone(), read_process_jiffies(pids)?)))
            .collect::<Result<BTreeMap<_, _>, String>>()?;
        let total_before = read_total_jiffies()?;
        thread::sleep(sample_interval);
        let total_after = read_total_jiffies()?;
        for (node, pids) in pids_by_node {
            let proc_after = read_process_jiffies(pids)?;
            let proc_before_node = proc_before.get(node).copied().unwrap_or(0);
            out.entry(node.clone()).or_default().push(sample_percent(
                proc_before_node,
                proc_after,
                total_before,
                total_after,
            ));
        }
    }
    Ok(out)
}

fn sample_percent(proc_before: u64, proc_after: u64, total_before: u64, total_after: u64) -> f64 {
    let proc_delta = (proc_after.saturating_sub(proc_before)) as f64;
    let total_delta = (total_after.saturating_sub(total_before)) as f64;
    if total_delta <= 0.0 {
        return 0.0;
    }
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1) as f64;
    (proc_delta / total_delta) * cpu_count * 100.0
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



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_many_does_not_serially_spend_one_full_window_per_node() {
        let pid = std::process::id();
        let pids_by_node = BTreeMap::from([
            ("node-a".to_string(), vec![pid]),
            ("node-b".to_string(), vec![pid]),
        ]);
        let interval = Duration::from_millis(50);
        let start = std::time::Instant::now();
        let samples = sample_many(&pids_by_node, interval, interval)
            .expect("sample_many should read live /proc data for the current process pid");
        let elapsed = start.elapsed();

        assert_eq!(samples.get("node-a").map(Vec::len), Some(1));
        assert_eq!(samples.get("node-b").map(Vec::len), Some(1));
        assert!(
            elapsed < Duration::from_millis(85),
            "sample_many should spend roughly one window per sample across all nodes; elapsed={elapsed:?}"
        );
    }
}
