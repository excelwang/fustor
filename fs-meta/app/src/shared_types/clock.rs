use std::sync::atomic::{AtomicU64, Ordering};

pub struct LogicalClock {
    counter: AtomicU64,
}

impl LogicalClock {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }

    pub fn tick(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn current(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }
}

impl Default for LogicalClock {
    fn default() -> Self {
        Self::new()
    }
}
