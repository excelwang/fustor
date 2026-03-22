/// Runtime unit id for source execution scheduling domain.
pub const SOURCE_RUNTIME_UNIT_ID: &str = "runtime.exec.source";

/// Runtime unit id for source scan execution scheduling domain.
pub const SOURCE_SCAN_RUNTIME_UNIT_ID: &str = "runtime.exec.scan";

/// Runtime unit id for sink execution scheduling domain.
pub const SINK_RUNTIME_UNIT_ID: &str = "runtime.exec.sink";

/// Runtime unit id for app-level HTTP facade execution.
pub const FACADE_RUNTIME_UNIT_ID: &str = "runtime.exec.facade";

/// Runtime unit id for app-level internal query execution.
pub const QUERY_RUNTIME_UNIT_ID: &str = "runtime.exec.query";

/// Runtime unit id for per-root-scope peer query fanout execution.
pub const QUERY_PEER_RUNTIME_UNIT_ID: &str = "runtime.exec.query-peer";

/// Source-side accepted runtime unit ids.
pub const SOURCE_RUNTIME_UNITS: &[&str] = &[SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID];

/// Sink-side accepted runtime unit ids.
pub const SINK_RUNTIME_UNITS: &[&str] = &[SINK_RUNTIME_UNIT_ID];
