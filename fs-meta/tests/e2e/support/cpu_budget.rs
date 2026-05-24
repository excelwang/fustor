#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct CpuSampleSummary {
    pub per_node_mean_delta: BTreeMap<String, f64>,
    pub cluster_mean_delta: f64,
    pub per_node_p95_delta: BTreeMap<String, f64>,
}

pub fn sample_cpu_usage(
    pids_by_node: &BTreeMap<String, Vec<u32>>,
    sample_interval: Duration,
    duration: Duration,
) -> Result<BTreeMap<String, Vec<f64>>, String> {
    sample_many(pids_by_node, sample_interval, duration)
}

pub fn summarize_cpu_budget(
    baseline: &BTreeMap<String, Vec<f64>>,
    steady: &BTreeMap<String, Vec<f64>>,
) -> CpuSampleSummary {
    let mut per_node_mean_delta = BTreeMap::new();
    let mut per_node_p95_delta = BTreeMap::new();
    let mut all_means = Vec::new();
    for (node, steady_samples) in steady {
        let base_samples = baseline.get(node).cloned().unwrap_or_default();
        let deltas = steady_samples
            .iter()
            .zip(base_samples.iter().chain(std::iter::repeat(&0.0)))
            .map(|(steady_sample, baseline_sample)| (steady_sample - baseline_sample).max(0.0))
            .collect::<Vec<_>>();
        let mean = sustained_mean_sample(&deltas);
        let p95 = percentile_sample(&deltas, 0.95);
        per_node_mean_delta.insert(node.clone(), mean);
        per_node_p95_delta.insert(node.clone(), p95);
        all_means.push(mean);
    }
    let cluster_mean_delta = mean_sample(&all_means);
    CpuSampleSummary {
        per_node_mean_delta,
        cluster_mean_delta,
        per_node_p95_delta,
    }
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

fn mean_sample(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        0.0
    } else {
        samples.iter().sum::<f64>() / samples.len() as f64
    }
}

fn sustained_mean_sample(samples: &[f64]) -> f64 {
    trimmed_mean_sample(samples, 0.10)
}

fn trimmed_mean_sample(samples: &[f64], trim_fraction: f64) -> f64 {
    if samples.len() < 4 {
        return mean_sample(samples);
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let trim_count = ((sorted.len() as f64) * trim_fraction).floor() as usize;
    if trim_count == 0 || trim_count * 2 >= sorted.len() {
        return mean_sample(&sorted);
    }
    mean_sample(&sorted[trim_count..(sorted.len() - trim_count)])
}

fn percentile_sample(samples: &[f64], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let index = ((sorted.len() as f64 * percentile).floor() as usize).min(sorted.len() - 1);
    sorted[index]
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
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => return Err(format!("read {} failed: {err}", path.display())),
        };
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
    use std::process::Command;

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

    #[test]
    fn summarize_cpu_budget_reports_incremental_overhead_not_absolute_runtime_cpu() {
        let idle_samples = BTreeMap::from([("node-a".to_string(), vec![80.0, 82.0, 81.0])]);
        let polling_samples = BTreeMap::from([("node-a".to_string(), vec![81.0, 83.0, 81.5])]);

        let summary = summarize_cpu_budget(&idle_samples, &polling_samples);

        let mean = summary
            .per_node_mean_delta
            .get("node-a")
            .copied()
            .expect("node-a mean delta");
        assert!((mean - 0.8333333333333334).abs() < 0.0001);
        assert_eq!(summary.per_node_p95_delta.get("node-a").copied(), Some(1.0));
        assert!(
            assert_cpu_budget(&summary).is_ok(),
            "high absolute runtime CPU from background work should not fail when polling overhead is low"
        );
    }

    #[test]
    fn assert_cpu_budget_rejects_sustained_polling_overhead() {
        let idle_samples = BTreeMap::from([("node-a".to_string(), vec![1.0, 1.0, 1.0])]);
        let polling_samples = BTreeMap::from([("node-a".to_string(), vec![9.0, 9.0, 9.0])]);

        let summary = summarize_cpu_budget(&idle_samples, &polling_samples);

        assert!(
            assert_cpu_budget(&summary).is_err(),
            "sustained polling overhead remains a resource-budget failure"
        );
    }

    #[test]
    fn assert_cpu_budget_allows_transient_tail_spikes_when_sustained_overhead_is_low() {
        let mut idle = vec![1.0; 40];
        let mut polling = vec![1.5; 40];
        polling[2] = 103.0;
        polling[21] = 96.0;
        idle[7] = 4.0;

        let summary = summarize_cpu_budget(
            &BTreeMap::from([("node-a".to_string(), idle)]),
            &BTreeMap::from([("node-a".to_string(), polling)]),
        );

        assert!(
            assert_cpu_budget(&summary).is_ok(),
            "cpu budget should reject sustained polling loops, not transient host/NFS tail spikes: {summary:?}"
        );
        assert!(
            summary
                .per_node_p95_delta
                .get("node-a")
                .is_some_and(|p95| *p95 > 90.0),
            "tail spike must remain visible as diagnostic evidence: {summary:?}"
        );
    }

    #[test]
    fn summarize_cpu_budget_uses_paired_positive_deltas_for_sustained_overhead() {
        let idle = vec![1.0, 1.0, 1.0, 1.0, 40.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let polling = vec![1.5, 1.5, 1.5, 1.5, 41.0, 1.5, 1.5, 1.5, 1.5, 1.5, 16.0, 1.5];

        let summary = summarize_cpu_budget(
            &BTreeMap::from([("node-a".to_string(), idle)]),
            &BTreeMap::from([("node-a".to_string(), polling)]),
        );

        assert!(
            summary
                .per_node_mean_delta
                .get("node-a")
                .is_some_and(|mean| *mean < 2.0),
            "sustained CPU overhead should be based on paired positive deltas, not separate window means that amplify unrelated host noise: {summary:?}"
        );
        assert!(
            summary
                .per_node_p95_delta
                .get("node-a")
                .is_some_and(|p95| *p95 > 10.0),
            "tail noise should remain visible in diagnostics: {summary:?}"
        );
    }

    #[test]
    fn read_process_jiffies_treats_vanished_pid_as_zero_instead_of_error() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("exit 0")
            .spawn()
            .expect("spawn short-lived process");
        let vanished_pid = child.id();
        let status = child.wait().expect("wait for short-lived process");
        assert!(status.success(), "child must exit cleanly");

        let jiffies = read_process_jiffies(&[vanished_pid])
            .expect("vanished pid should be treated as zero jiffies rather than a hard error");
        assert_eq!(jiffies, 0);
    }

    #[test]
    fn sample_many_ignores_vanished_pid_in_selected_steady_set() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("exit 0")
            .spawn()
            .expect("spawn short-lived process");
        let vanished_pid = child.id();
        let status = child.wait().expect("wait for short-lived process");
        assert!(status.success(), "child must exit cleanly");

        let pids_by_node =
            BTreeMap::from([("node-a".to_string(), vec![std::process::id(), vanished_pid])]);
        let samples = sample_many(
            &pids_by_node,
            Duration::from_millis(20),
            Duration::from_millis(20),
        )
        .expect("vanished steady pid should not abort cpu budget sampling");

        assert_eq!(samples.get("node-a").map(Vec::len), Some(1));
    }
}
