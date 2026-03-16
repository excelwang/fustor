//! Sink clock: shadow time tracking.
//!
//! The SinkClock tracks the high-water mark of `EventMetadata.timestamp_us`
//! across all received events. This provides the sink's notion of "current time"
//! in the NFS shadow time domain.

/// Shadow time tracker for the sink.
pub struct SinkClock {
    /// High-water mark of received event timestamps (microseconds).
    pub(crate) shadow_time_high_us: u64,
}

impl SinkClock {
    pub fn new() -> Self {
        Self {
            shadow_time_high_us: 0,
        }
    }

    /// Update with an incoming event timestamp.
    pub fn advance(&mut self, timestamp_us: u64) {
        if timestamp_us > self.shadow_time_high_us {
            self.shadow_time_high_us = timestamp_us;
        }
    }

    /// Current shadow time in microseconds.
    pub fn now_us(&self) -> u64 {
        self.shadow_time_high_us
    }

    /// Current shadow time in seconds (f64 for sub-second precision).
    pub fn now_secs(&self) -> f64 {
        self.shadow_time_high_us as f64 / 1_000_000.0
    }
}

impl Default for SinkClock {
    fn default() -> Self {
        Self::new()
    }
}
