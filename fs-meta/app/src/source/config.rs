use std::collections::HashMap;
use std::time::Duration;

pub use fs_meta::source::config::{GrantedMountRoot, RootSelector, RootSpec};
use fs_meta::source::config::SourceConfig as ProductSourceConfig;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceConfig {
    pub roots: Vec<RootSpec>,
    pub host_object_grants: Vec<GrantedMountRoot>,
    pub batch_size: usize,
    pub throttle_interval: Duration,
    pub audit_interval: Duration,
    pub scan_workers: usize,
    pub lru_capacity: usize,
    pub min_monitoring_window: Duration,
    pub max_scan_events: usize,
    pub sink_tombstone_ttl: Duration,
    pub sink_tombstone_tolerance_us: u64,
    pub drift_window_size: usize,
    pub drift_graduation_threshold: u64,
    pub drift_max_jump_us: i64,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self::from_product_config(ProductSourceConfig::default(), Vec::new())
    }
}

impl From<ProductSourceConfig> for SourceConfig {
    fn from(value: ProductSourceConfig) -> Self {
        Self::from_product_config(value, Vec::new())
    }
}

impl SourceConfig {
    pub fn from_product_config(
        product: ProductSourceConfig,
        host_object_grants: Vec<GrantedMountRoot>,
    ) -> Self {
        Self {
            roots: product.roots,
            host_object_grants,
            batch_size: product.batch_size,
            throttle_interval: product.throttle_interval,
            audit_interval: product.audit_interval,
            scan_workers: product.scan_workers,
            lru_capacity: product.lru_capacity,
            min_monitoring_window: product.min_monitoring_window,
            max_scan_events: product.max_scan_events,
            sink_tombstone_ttl: product.sink_tombstone_ttl,
            sink_tombstone_tolerance_us: product.sink_tombstone_tolerance_us,
            drift_window_size: product.drift_window_size,
            drift_graduation_threshold: product.drift_graduation_threshold,
            drift_max_jump_us: product.drift_max_jump_us,
        }
    }

    pub(crate) fn product_config(&self) -> ProductSourceConfig {
        ProductSourceConfig {
            roots: self.roots.clone(),
            batch_size: self.batch_size,
            throttle_interval: self.throttle_interval,
            audit_interval: self.audit_interval,
            scan_workers: self.scan_workers,
            lru_capacity: self.lru_capacity,
            min_monitoring_window: self.min_monitoring_window,
            max_scan_events: self.max_scan_events,
            sink_tombstone_ttl: self.sink_tombstone_ttl,
            sink_tombstone_tolerance_us: self.sink_tombstone_tolerance_us,
            drift_window_size: self.drift_window_size,
            drift_graduation_threshold: self.drift_graduation_threshold,
            drift_max_jump_us: self.drift_max_jump_us,
        }
    }

    pub fn effective_roots(&self) -> Result<Vec<RootSpec>, String> {
        self.product_config().effective_roots()
    }

    pub fn matching_grants_by_root(&self) -> HashMap<String, Vec<GrantedMountRoot>> {
        let mut grouped = HashMap::new();
        for root in &self.roots {
            let matches = self
                .host_object_grants
                .iter()
                .filter(|grant| root.selector.matches(grant))
                .cloned()
                .collect::<Vec<_>>();
            grouped.insert(root.id.clone(), matches);
        }
        grouped
    }
}
