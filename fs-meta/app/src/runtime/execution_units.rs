/// Runtime unit id for source execution scheduling domain.
pub(crate) const SOURCE_RUNTIME_UNIT_ID: &str = "runtime.exec.source";

/// Runtime unit id for source scan execution scheduling domain.
pub(crate) const SOURCE_SCAN_RUNTIME_UNIT_ID: &str = "runtime.exec.scan";

/// Runtime unit id for sink execution scheduling domain.
pub(crate) const SINK_RUNTIME_UNIT_ID: &str = "runtime.exec.sink";

/// Runtime unit id for app-level HTTP facade execution.
pub(crate) const FACADE_RUNTIME_UNIT_ID: &str = "runtime.exec.facade";

/// Source-side accepted runtime unit ids.
pub(crate) const SOURCE_RUNTIME_UNITS: &[&str] =
    &[SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID];

/// Sink-side accepted runtime unit ids.
pub(crate) const SINK_RUNTIME_UNITS: &[&str] = &[SINK_RUNTIME_UNIT_ID];
