//! Drift estimation for NFS shadow time compensation.
//!
//! Implements the P999 drift estimator from L3 CLOCK.md.
//! Thread-safety: wrap in `Arc<Mutex<DriftEstimator>>`.

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

/// Drift estimator using P999 sliding window.
///
/// Estimates the clock drift between the NFS server (mtime domain)
/// and the local system clock. All mutation methods take `&mut self`;
/// callers must use `Arc<Mutex<>>` for shared access.
pub struct DriftEstimator {
    /// Sliding window of drift samples (mtime - local_now) in seconds.
    samples: VecDeque<f64>,
    /// Total Realtime samples ever received (monotonically increasing).
    realtime_count: u64,
    /// Current drift in microseconds.
    current_drift_us: i64,

    // ── Configuration ──
    window_size: usize,
    graduation_threshold: u64,
    max_jump_us: i64,
}

impl DriftEstimator {
    pub fn new(window_size: usize, graduation_threshold: u64, max_jump_us: i64) -> Self {
        Self {
            samples: VecDeque::with_capacity(window_size),
            realtime_count: 0,
            current_drift_us: 0,
            window_size,
            graduation_threshold,
            max_jump_us,
        }
    }

    /// Whether the estimator has graduated (Realtime-only mode).
    pub fn is_graduated(&self) -> bool {
        self.realtime_count >= self.graduation_threshold
    }

    /// Push a Scan sample (directory mtime from Epoch 0 or Audit rescan).
    /// Ignored in Graduated mode.
    pub fn push_scan_sample(&mut self, dir_mtime_secs: f64, local_now_secs: f64) {
        if self.is_graduated() {
            return;
        }
        let sample = dir_mtime_secs - local_now_secs;
        self.push_raw(sample);
    }

    /// Push a Realtime sample (IN_CLOSE_WRITE, IN_CREATE dir, IN_ATTRIB).
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub fn push_realtime_sample(&mut self, file_mtime_secs: f64, local_now_secs: f64) {
        self.realtime_count += 1;
        // No explicit purge at graduation boundary.
        // Sliding window naturally ages out Scan samples.
        let sample = file_mtime_secs - local_now_secs;
        self.push_raw(sample);
    }

    fn push_raw(&mut self, sample: f64) {
        self.samples.push_back(sample);
        if self.samples.len() > self.window_size {
            self.samples.pop_front();
        }
        if self.samples.len() >= 10 {
            self.recompute();
        }
    }

    fn recompute(&mut self) {
        let mut sorted: Vec<f64> = self.samples.iter().copied().collect();
        sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // P999: 99.9th percentile — discard top 0.1% outliers
        let p999_idx = ((sorted.len() as f64 * 0.999) as usize).saturating_sub(1);
        let proposed_drift_us = (sorted[p999_idx] * 1_000_000.0) as i64;

        // Anti-jump protection (Graduated mode only)
        if self.is_graduated() {
            let delta = (proposed_drift_us - self.current_drift_us).abs();
            if delta > self.max_jump_us {
                log::warn!(
                    "Drift jump rejected: proposed={}µs, current={}µs, delta={}µs > max_jump={}µs",
                    proposed_drift_us,
                    self.current_drift_us,
                    delta,
                    self.max_jump_us
                );
                return;
            }
        }

        self.current_drift_us = proposed_drift_us;
    }

    /// Read current drift in microseconds.
    pub fn drift_us(&self) -> i64 {
        self.current_drift_us
    }

    /// Number of samples currently in the window.
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Total Realtime samples ever received.
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn realtime_count(&self) -> u64 {
        self.realtime_count
    }
}

/// Compute the current shadow time (NFS-domain microseconds since epoch).
pub fn shadow_now_us(drift_us: i64) -> u64 {
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;
    (now_us + drift_us) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_scan_sample_populates_drift() {
        let mut est = DriftEstimator::new(100, 1000, 10_000_000);
        // Simulate: NFS time is 2.0 seconds ahead of local time
        for i in 0..20 {
            let local = 1000.0 + i as f64;
            let mtime = local + 2.0; // NFS is +2s ahead
            est.push_scan_sample(mtime, local);
        }
        // drift should be approximately +2_000_000 µs
        let drift = est.drift_us();
        assert!(
            (drift - 2_000_000).abs() < 100_000,
            "Expected drift ~2_000_000µs, got {drift}"
        );
    }

    #[test]
    fn test_graduation_blocks_scan_samples() {
        let mut est = DriftEstimator::new(100, 10, 10_000_000);
        // Push 10 realtime samples to graduate
        for i in 0..10 {
            est.push_realtime_sample(1000.0 + i as f64, 1000.0 + i as f64);
        }
        assert!(est.is_graduated());

        let drift_before = est.drift_us();
        // Now push scan samples — should be ignored
        for i in 0..20 {
            est.push_scan_sample(1000.0 + i as f64 + 100.0, 1000.0 + i as f64);
        }
        assert_eq!(
            est.drift_us(),
            drift_before,
            "Scan samples must be ignored after graduation"
        );
    }

    #[test]
    fn test_anti_jump_protection() {
        let mut est = DriftEstimator::new(100, 5, 1_000_000); // max jump = 1s
        // Establish baseline drift near 0
        for i in 0..10 {
            est.push_realtime_sample(1000.0 + i as f64, 1000.0 + i as f64);
        }
        assert!(est.is_graduated());
        let baseline = est.drift_us();

        // Suddenly push samples with huge drift (50s) — should be rejected
        for _ in 0..20 {
            est.push_realtime_sample(1050.0, 1000.0);
        }
        assert_eq!(
            est.drift_us(),
            baseline,
            "Jump >1s must be rejected in Graduated mode"
        );
    }

    #[test]
    fn test_p999_filters_single_extreme_outlier() {
        let mut est = DriftEstimator::new(2_000, 10_000, 10_000_000);
        for i in 0..1_000 {
            let local = 1_000.0 + i as f64;
            est.push_scan_sample(local, local); // nominal drift ~0
        }
        // One extreme outlier (future mtime) should be ignored by P999.
        est.push_scan_sample(1_000_000.0, 0.0);

        let drift = est.drift_us();
        assert!(
            drift.abs() < 200_000,
            "P999 should suppress single extreme outlier, got drift={drift}µs"
        );
    }
}
