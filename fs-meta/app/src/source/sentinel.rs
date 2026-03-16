//! Sentinel: health monitoring and self-healing for fs-meta source pipelines.
//!
//! The sentinel is the fourth index lifecycle mechanism (alongside realtime watch,
//! baseline scan, and periodic audit). It monitors observable health signals from
//! concrete root pipelines and triggers corrective actions when degradation is
//! detected.
//!
//! ## Spec coverage
//!
//! - L0 §VISION.INDEX_LIFECYCLE.SENTINEL_FEEDBACK
//! - L1 §CONTRACTS.INDEX_LIFECYCLE.REALTIME_PLUS_BASELINE_PLUS_AUDIT_PLUS_SENTINEL
//! - L3 §StartupAndIndexBootstrap step 8

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Health signal types that the sentinel monitors.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum HealthSignal {
    /// Watch queue overflow — indicates the realtime watch is losing events.
    WatchOverflow { root_key: String },
    /// Watch backend initialization failure.
    WatchInitFailed { root_key: String, error: String },
    /// Audit scan discovered drift: number of corrections applied.
    AuditDrift {
        root_key: String,
        corrections: usize,
    },
    /// A concrete root pipeline experienced a transient error.
    PipelineError { root_key: String, error: String },
    /// Pipeline recovered after a previous error.
    PipelineRecovered { root_key: String },
}

/// Self-healing action emitted by the sentinel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SentinelAction {
    /// Trigger a targeted rescan for a specific concrete root.
    TriggerRescan { root_key: String },
    /// Report degraded visibility status for observability.
    ReportDegraded { root_key: String, reason: String },
    /// Report recovery from a previously degraded state.
    ReportRecovered { root_key: String },
}

/// Per-root health state tracked by the sentinel.
#[derive(Debug, Clone)]
struct RootHealthEntry {
    /// Consecutive overflow count within the current window.
    overflow_count: u32,
    /// Timestamp of the last overflow signal.
    last_overflow_at: Option<Instant>,
    /// Whether this root is currently considered degraded.
    is_degraded: bool,
    /// Consecutive pipeline errors.
    consecutive_errors: u32,
    /// Last audit drift correction count.
    last_audit_drift: usize,
    /// Timestamp of last audit signal.
    last_audit_at: Option<Instant>,
}

impl Default for RootHealthEntry {
    fn default() -> Self {
        Self {
            overflow_count: 0,
            last_overflow_at: None,
            is_degraded: false,
            consecutive_errors: 0,
            last_audit_drift: 0,
            last_audit_at: None,
        }
    }
}

/// Configuration for sentinel thresholds.
#[derive(Debug, Clone)]
pub(crate) struct SentinelConfig {
    /// Number of overflow events within `overflow_window` before triggering rescan.
    pub overflow_threshold: u32,
    /// Time window for counting overflow events.
    pub overflow_window: Duration,
    /// Number of audit corrections before flagging the root as degraded.
    pub audit_drift_threshold: usize,
    /// Number of consecutive pipeline errors before flagging degraded.
    pub error_threshold: u32,
}

impl Default for SentinelConfig {
    fn default() -> Self {
        Self {
            overflow_threshold: 3,
            overflow_window: Duration::from_secs(60),
            audit_drift_threshold: 50,
            error_threshold: 3,
        }
    }
}

/// Sentinel monitor: processes health signals and emits self-healing actions.
///
/// Thread-safe — the inner state is behind a Mutex so multiple concrete root
/// tasks can report signals concurrently.
pub(crate) struct Sentinel {
    config: SentinelConfig,
    state: Mutex<HashMap<String, RootHealthEntry>>,
}

impl Sentinel {
    pub(crate) fn new(config: SentinelConfig) -> Self {
        Self {
            config,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Process a health signal and return any resulting actions.
    pub(crate) fn process(&self, signal: HealthSignal) -> Vec<SentinelAction> {
        let mut state = self.state.lock().unwrap_or_else(|p| {
            log::warn!("sentinel: mutex poisoned; recovering");
            p.into_inner()
        });
        let mut actions = Vec::new();
        match signal {
            HealthSignal::WatchOverflow { ref root_key } => {
                let entry = state.entry(root_key.clone()).or_default();
                let now = Instant::now();

                // Reset counter if outside window.
                if let Some(last) = entry.last_overflow_at {
                    if now.duration_since(last) > self.config.overflow_window {
                        entry.overflow_count = 0;
                    }
                }

                entry.overflow_count += 1;
                entry.last_overflow_at = Some(now);

                if entry.overflow_count >= self.config.overflow_threshold {
                    log::warn!(
                        "sentinel: root '{}' hit overflow threshold ({}/{}), triggering rescan",
                        root_key,
                        entry.overflow_count,
                        self.config.overflow_threshold,
                    );
                    actions.push(SentinelAction::TriggerRescan {
                        root_key: root_key.clone(),
                    });
                    // Reset counter after triggering.
                    entry.overflow_count = 0;

                    if !entry.is_degraded {
                        entry.is_degraded = true;
                        actions.push(SentinelAction::ReportDegraded {
                            root_key: root_key.clone(),
                            reason: "repeated watch overflow".into(),
                        });
                    }
                }
            }

            HealthSignal::WatchInitFailed {
                ref root_key,
                ref error,
            } => {
                let entry = state.entry(root_key.clone()).or_default();
                if !entry.is_degraded {
                    entry.is_degraded = true;
                    actions.push(SentinelAction::ReportDegraded {
                        root_key: root_key.clone(),
                        reason: format!("watch init failed: {error}"),
                    });
                }
            }

            HealthSignal::AuditDrift {
                ref root_key,
                corrections,
            } => {
                let entry = state.entry(root_key.clone()).or_default();
                entry.last_audit_drift = corrections;
                entry.last_audit_at = Some(Instant::now());

                if corrections >= self.config.audit_drift_threshold {
                    log::warn!(
                        "sentinel: root '{}' audit found {} corrections (threshold {}), \
                         indicating significant index drift",
                        root_key,
                        corrections,
                        self.config.audit_drift_threshold,
                    );
                    if !entry.is_degraded {
                        entry.is_degraded = true;
                        actions.push(SentinelAction::ReportDegraded {
                            root_key: root_key.clone(),
                            reason: format!("audit drift: {} corrections", corrections),
                        });
                    }
                } else if entry.is_degraded && corrections == 0 {
                    // A clean audit pass while previously degraded → recovery.
                    entry.is_degraded = false;
                    actions.push(SentinelAction::ReportRecovered {
                        root_key: root_key.clone(),
                    });
                }
            }

            HealthSignal::PipelineError {
                ref root_key,
                ref error,
            } => {
                let entry = state.entry(root_key.clone()).or_default();
                entry.consecutive_errors += 1;

                if entry.consecutive_errors >= self.config.error_threshold && !entry.is_degraded {
                    entry.is_degraded = true;
                    log::warn!(
                        "sentinel: root '{}' hit error threshold ({} consecutive): {}",
                        root_key,
                        entry.consecutive_errors,
                        error,
                    );
                    actions.push(SentinelAction::TriggerRescan {
                        root_key: root_key.clone(),
                    });
                    actions.push(SentinelAction::ReportDegraded {
                        root_key: root_key.clone(),
                        reason: format!("{} consecutive errors", entry.consecutive_errors),
                    });
                }
            }

            HealthSignal::PipelineRecovered { ref root_key } => {
                let entry = state.entry(root_key.clone()).or_default();
                entry.consecutive_errors = 0;
                if entry.is_degraded {
                    entry.is_degraded = false;
                    actions.push(SentinelAction::ReportRecovered {
                        root_key: root_key.clone(),
                    });
                }
            }
        }
        actions
    }

    /// Get the current degraded root keys and their reasons (for diagnostics).
    #[allow(dead_code)]
    pub(crate) fn degraded_roots(&self) -> Vec<(String, String)> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state
            .iter()
            .filter(|(_, e)| e.is_degraded)
            .map(|(k, e)| {
                let reason = if e.overflow_count > 0 {
                    format!("overflow({})", e.overflow_count)
                } else if e.consecutive_errors > 0 {
                    format!("errors({})", e.consecutive_errors)
                } else if e.last_audit_drift > 0 {
                    format!("drift({})", e.last_audit_drift)
                } else {
                    "degraded".into()
                };
                (k.clone(), reason)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sentinel() -> Sentinel {
        Sentinel::new(SentinelConfig {
            overflow_threshold: 3,
            overflow_window: Duration::from_secs(60),
            audit_drift_threshold: 10,
            error_threshold: 2,
        })
    }

    #[test]
    fn overflow_below_threshold_no_action() {
        let s = sentinel();
        let actions = s.process(HealthSignal::WatchOverflow {
            root_key: "r1".into(),
        });
        assert!(actions.is_empty());
    }

    #[test]
    fn overflow_at_threshold_triggers_rescan_and_degraded() {
        let s = sentinel();
        for _ in 0..2 {
            s.process(HealthSignal::WatchOverflow {
                root_key: "r1".into(),
            });
        }
        let actions = s.process(HealthSignal::WatchOverflow {
            root_key: "r1".into(),
        });
        assert!(actions.contains(&SentinelAction::TriggerRescan {
            root_key: "r1".into(),
        }));
        assert!(actions.iter().any(
            |a| matches!(a, SentinelAction::ReportDegraded { root_key, .. } if root_key == "r1")
        ));
    }

    #[test]
    fn audit_drift_triggers_degraded() {
        let s = sentinel();
        let actions = s.process(HealthSignal::AuditDrift {
            root_key: "r1".into(),
            corrections: 50,
        });
        assert!(actions.iter().any(
            |a| matches!(a, SentinelAction::ReportDegraded { root_key, .. } if root_key == "r1")
        ));
    }

    #[test]
    fn clean_audit_after_drift_recovers() {
        let s = sentinel();
        s.process(HealthSignal::AuditDrift {
            root_key: "r1".into(),
            corrections: 50,
        });
        let actions = s.process(HealthSignal::AuditDrift {
            root_key: "r1".into(),
            corrections: 0,
        });
        assert!(actions.contains(&SentinelAction::ReportRecovered {
            root_key: "r1".into(),
        }));
    }

    #[test]
    fn pipeline_error_threshold() {
        let s = sentinel();
        let a1 = s.process(HealthSignal::PipelineError {
            root_key: "r1".into(),
            error: "io error".into(),
        });
        assert!(a1.is_empty());
        let a2 = s.process(HealthSignal::PipelineError {
            root_key: "r1".into(),
            error: "io error".into(),
        });
        assert!(
            a2.iter().any(
                |a| matches!(a, SentinelAction::TriggerRescan { root_key } if root_key == "r1")
            )
        );
        assert!(
            a2.iter()
                .any(|a| matches!(a, SentinelAction::ReportDegraded { .. }))
        );
    }

    #[test]
    fn pipeline_recovery_clears_degraded() {
        let s = sentinel();
        s.process(HealthSignal::PipelineError {
            root_key: "r1".into(),
            error: "e".into(),
        });
        s.process(HealthSignal::PipelineError {
            root_key: "r1".into(),
            error: "e".into(),
        });
        let actions = s.process(HealthSignal::PipelineRecovered {
            root_key: "r1".into(),
        });
        assert!(actions.contains(&SentinelAction::ReportRecovered {
            root_key: "r1".into(),
        }));
        assert!(s.degraded_roots().is_empty());
    }

    #[test]
    fn degraded_roots_reports_correctly() {
        let s = sentinel();
        s.process(HealthSignal::WatchInitFailed {
            root_key: "r1".into(),
            error: "no inotify".into(),
        });
        let degraded = s.degraded_roots();
        assert_eq!(degraded.len(), 1);
        assert_eq!(degraded[0].0, "r1");
    }
}
