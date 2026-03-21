use capanix_app_sdk::runtime::{ControlEnvelope, ControlFrame};
use capanix_app_sdk::{CnxError, Result};
use capanix_runtime_host_sdk::control::{
    BoundScope, ExecControl, RuntimeHostObjectGrantsChanged, UnitExposureConfirmed,
    decode_exec_control_envelope, decode_runtime_host_object_grants_changed_envelope,
    decode_unit_exposure_confirmed_envelope, decode_unit_tick_envelope,
};

use crate::runtime::execution_units::{
    FACADE_RUNTIME_UNIT_ID, QUERY_PEER_RUNTIME_UNIT_ID, QUERY_RUNTIME_UNIT_ID,
    SINK_RUNTIME_UNIT_ID, SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID,
};
use crate::source::config::RootSpec;

pub(crate) const MANUAL_RESCAN_CONTROL_FRAME_KIND: &str = "fs-meta.manual-rescan-control:v1";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ManualRescanControlPayload {
    pub requested_at_us: u64,
}

pub(crate) fn encode_manual_rescan_envelope(requested_at_us: u64) -> Result<ControlEnvelope> {
    Ok(ControlEnvelope::Frame(ControlFrame {
        kind: MANUAL_RESCAN_CONTROL_FRAME_KIND.to_string(),
        payload: rmp_serde::to_vec_named(&ManualRescanControlPayload { requested_at_us }).map_err(
            |err| {
                CnxError::Internal(format!(
                    "encode manual rescan control payload failed: {err}"
                ))
            },
        )?,
    }))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogicalRootsControlPayload {
    pub roots: Vec<RootSpec>,
}

pub(crate) fn encode_logical_roots_control_payload(roots: &[RootSpec]) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(&LogicalRootsControlPayload {
        roots: roots.to_vec(),
    })
    .map_err(|err| {
        CnxError::Internal(format!(
            "encode logical roots control payload failed: {err}"
        ))
    })
}

pub(crate) fn decode_logical_roots_control_payload(
    payload: &[u8],
) -> Result<LogicalRootsControlPayload> {
    rmp_serde::from_slice::<LogicalRootsControlPayload>(payload).map_err(|err| {
        CnxError::InvalidInput(format!(
            "decode logical roots control payload failed: {err}"
        ))
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceRuntimeUnit {
    Source,
    Scan,
}

impl SourceRuntimeUnit {
    pub(crate) fn unit_id(self) -> &'static str {
        match self {
            Self::Source => SOURCE_RUNTIME_UNIT_ID,
            Self::Scan => SOURCE_SCAN_RUNTIME_UNIT_ID,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinkRuntimeUnit {
    Sink,
}

impl SinkRuntimeUnit {
    pub(crate) fn unit_id(self) -> &'static str {
        match self {
            Self::Sink => SINK_RUNTIME_UNIT_ID,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FacadeRuntimeUnit {
    Facade,
    Query,
    QueryPeer,
}

impl FacadeRuntimeUnit {
    pub(crate) fn unit_id(self) -> &'static str {
        match self {
            Self::Facade => FACADE_RUNTIME_UNIT_ID,
            Self::Query => QUERY_RUNTIME_UNIT_ID,
            Self::QueryPeer => QUERY_PEER_RUNTIME_UNIT_ID,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SourceControlSignal {
    Activate {
        unit: SourceRuntimeUnit,
        route_key: String,
        generation: u64,
        bound_scopes: Vec<BoundScope>,
        envelope: ControlEnvelope,
    },
    Deactivate {
        unit: SourceRuntimeUnit,
        route_key: String,
        generation: u64,
        envelope: ControlEnvelope,
    },
    Tick {
        unit: SourceRuntimeUnit,
        route_key: String,
        generation: u64,
        envelope: ControlEnvelope,
    },
    RuntimeHostObjectGrantsChanged {
        changed: RuntimeHostObjectGrantsChanged,
        envelope: ControlEnvelope,
    },
    ManualRescan {
        envelope: ControlEnvelope,
    },
    Passthrough(ControlEnvelope),
}

impl SourceControlSignal {
    pub(crate) fn envelope(&self) -> ControlEnvelope {
        match self {
            Self::Activate { envelope, .. }
            | Self::Deactivate { envelope, .. }
            | Self::Tick { envelope, .. }
            | Self::RuntimeHostObjectGrantsChanged { envelope, .. }
            | Self::ManualRescan { envelope, .. } => envelope.clone(),
            Self::Passthrough(envelope) => envelope.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SinkControlSignal {
    Activate {
        unit: SinkRuntimeUnit,
        route_key: String,
        generation: u64,
        bound_scopes: Vec<BoundScope>,
        envelope: ControlEnvelope,
    },
    Deactivate {
        unit: SinkRuntimeUnit,
        route_key: String,
        generation: u64,
        envelope: ControlEnvelope,
    },
    Tick {
        unit: SinkRuntimeUnit,
        route_key: String,
        generation: u64,
        envelope: ControlEnvelope,
    },
    RuntimeHostObjectGrantsChanged {
        changed: RuntimeHostObjectGrantsChanged,
        envelope: ControlEnvelope,
    },
    Passthrough(ControlEnvelope),
}

impl SinkControlSignal {
    pub(crate) fn envelope(&self) -> ControlEnvelope {
        match self {
            Self::Activate { envelope, .. }
            | Self::Deactivate { envelope, .. }
            | Self::Tick { envelope, .. }
            | Self::RuntimeHostObjectGrantsChanged { envelope, .. } => envelope.clone(),
            Self::Passthrough(envelope) => envelope.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum FacadeControlSignal {
    Activate {
        unit: FacadeRuntimeUnit,
        route_key: String,
        generation: u64,
        bound_scopes: Vec<BoundScope>,
    },
    Deactivate {
        unit: FacadeRuntimeUnit,
        route_key: String,
        generation: u64,
    },
    Tick {
        unit: FacadeRuntimeUnit,
        route_key: String,
        generation: u64,
    },
    ExposureConfirmed {
        unit: FacadeRuntimeUnit,
        route_key: String,
        generation: u64,
        confirmed_at_us: u64,
    },
    RuntimeHostObjectGrantsChanged {
        _changed: RuntimeHostObjectGrantsChanged,
    },
    Passthrough,
}

fn source_unit_from_id(unit_id: &str) -> Option<SourceRuntimeUnit> {
    match unit_id {
        SOURCE_RUNTIME_UNIT_ID => Some(SourceRuntimeUnit::Source),
        SOURCE_SCAN_RUNTIME_UNIT_ID => Some(SourceRuntimeUnit::Scan),
        _ => None,
    }
}

fn sink_unit_from_id(unit_id: &str) -> Option<SinkRuntimeUnit> {
    match unit_id {
        SINK_RUNTIME_UNIT_ID => Some(SinkRuntimeUnit::Sink),
        _ => None,
    }
}

fn facade_unit_from_id(unit_id: &str) -> Option<FacadeRuntimeUnit> {
    match unit_id {
        FACADE_RUNTIME_UNIT_ID => Some(FacadeRuntimeUnit::Facade),
        QUERY_RUNTIME_UNIT_ID => Some(FacadeRuntimeUnit::Query),
        QUERY_PEER_RUNTIME_UNIT_ID => Some(FacadeRuntimeUnit::QueryPeer),
        _ => None,
    }
}

fn facade_signal_from_trusted_exposure_confirmed(
    confirmed: &UnitExposureConfirmed,
) -> Option<FacadeControlSignal> {
    facade_unit_from_id(&confirmed.unit_id).map(|unit| FacadeControlSignal::ExposureConfirmed {
        unit,
        route_key: confirmed.route_key.clone(),
        generation: confirmed.generation,
        confirmed_at_us: confirmed.confirmed_at_us,
    })
}

pub(crate) fn source_control_signals_from_envelopes(
    envelopes: &[ControlEnvelope],
) -> Result<Vec<SourceControlSignal>> {
    let mut signals = Vec::new();
    for envelope in envelopes {
        if let Some(ctrl) = decode_exec_control_envelope(envelope)? {
            match ctrl {
                ExecControl::Activate(activate) => {
                    let Some(unit) = source_unit_from_id(&activate.unit_id) else {
                        return Err(CnxError::NotSupported(format!(
                            "source-fs-meta: unsupported unit_id '{}' in control envelope",
                            activate.unit_id
                        )));
                    };
                    signals.push(SourceControlSignal::Activate {
                        unit,
                        route_key: activate.route_key.clone(),
                        generation: activate.generation,
                        bound_scopes: activate.bound_scopes.clone(),
                        envelope: envelope.clone(),
                    });
                }
                ExecControl::Deactivate(deactivate) => {
                    let Some(unit) = source_unit_from_id(&deactivate.unit_id) else {
                        return Err(CnxError::NotSupported(format!(
                            "source-fs-meta: unsupported unit_id '{}' in control envelope",
                            deactivate.unit_id
                        )));
                    };
                    signals.push(SourceControlSignal::Deactivate {
                        unit,
                        route_key: deactivate.route_key.clone(),
                        generation: deactivate.generation,
                        envelope: envelope.clone(),
                    });
                }
            }
            continue;
        }

        if let Some(tick) = decode_unit_tick_envelope(envelope)? {
            let Some(unit) = source_unit_from_id(&tick.unit_id) else {
                return Err(CnxError::NotSupported(format!(
                    "source-fs-meta: unsupported unit_id '{}' in control envelope",
                    tick.unit_id
                )));
            };
            signals.push(SourceControlSignal::Tick {
                unit,
                route_key: tick.route_key.clone(),
                generation: tick.generation,
                envelope: envelope.clone(),
            });
            continue;
        }

        if let Some(changed) = decode_runtime_host_object_grants_changed_envelope(envelope)? {
            signals.push(SourceControlSignal::RuntimeHostObjectGrantsChanged {
                changed,
                envelope: envelope.clone(),
            });
            continue;
        }

        if let ControlEnvelope::Frame(frame) = envelope
            && frame.kind == MANUAL_RESCAN_CONTROL_FRAME_KIND
        {
            signals.push(SourceControlSignal::ManualRescan {
                envelope: envelope.clone(),
            });
            continue;
        }

        signals.push(SourceControlSignal::Passthrough(envelope.clone()));
    }
    Ok(signals)
}

pub(crate) fn sink_control_signals_from_envelopes(
    envelopes: &[ControlEnvelope],
) -> Result<Vec<SinkControlSignal>> {
    let mut signals = Vec::new();
    for envelope in envelopes {
        if let Some(ctrl) = decode_exec_control_envelope(envelope)? {
            match ctrl {
                ExecControl::Activate(activate) => {
                    let Some(unit) = sink_unit_from_id(&activate.unit_id) else {
                        return Err(CnxError::NotSupported(format!(
                            "sink-file-meta: unsupported unit_id '{}' in control envelope",
                            activate.unit_id
                        )));
                    };
                    signals.push(SinkControlSignal::Activate {
                        unit,
                        route_key: activate.route_key.clone(),
                        generation: activate.generation,
                        bound_scopes: activate.bound_scopes.clone(),
                        envelope: envelope.clone(),
                    });
                }
                ExecControl::Deactivate(deactivate) => {
                    let Some(unit) = sink_unit_from_id(&deactivate.unit_id) else {
                        return Err(CnxError::NotSupported(format!(
                            "sink-file-meta: unsupported unit_id '{}' in control envelope",
                            deactivate.unit_id
                        )));
                    };
                    signals.push(SinkControlSignal::Deactivate {
                        unit,
                        route_key: deactivate.route_key.clone(),
                        generation: deactivate.generation,
                        envelope: envelope.clone(),
                    });
                }
            }
            continue;
        }

        if let Some(tick) = decode_unit_tick_envelope(envelope)? {
            let Some(unit) = sink_unit_from_id(&tick.unit_id) else {
                return Err(CnxError::NotSupported(format!(
                    "sink-file-meta: unsupported unit_id '{}' in control envelope",
                    tick.unit_id
                )));
            };
            signals.push(SinkControlSignal::Tick {
                unit,
                route_key: tick.route_key.clone(),
                generation: tick.generation,
                envelope: envelope.clone(),
            });
            continue;
        }

        if let Some(changed) = decode_runtime_host_object_grants_changed_envelope(envelope)? {
            signals.push(SinkControlSignal::RuntimeHostObjectGrantsChanged {
                changed,
                envelope: envelope.clone(),
            });
            continue;
        }

        signals.push(SinkControlSignal::Passthrough(envelope.clone()));
    }
    Ok(signals)
}

pub(crate) fn split_app_control_signals(
    envelopes: &[ControlEnvelope],
) -> Result<(
    Vec<SourceControlSignal>,
    Vec<SinkControlSignal>,
    Vec<FacadeControlSignal>,
)> {
    let mut source = Vec::new();
    let mut sink = Vec::new();
    let mut facade = Vec::new();
    for envelope in envelopes {
        if let Some(ctrl) = decode_exec_control_envelope(envelope)? {
            match ctrl {
                ExecControl::Activate(activate) => {
                    let to_source = source_unit_from_id(&activate.unit_id);
                    let to_sink = sink_unit_from_id(&activate.unit_id);
                    let to_facade = facade_unit_from_id(&activate.unit_id);
                    if to_source.is_none() && to_sink.is_none() && to_facade.is_none() {
                        return Err(CnxError::NotSupported(format!(
                            "fs-meta: unsupported unit_id '{}' in control envelope",
                            activate.unit_id
                        )));
                    }
                    if let Some(unit) = to_source {
                        source.push(SourceControlSignal::Activate {
                            unit,
                            route_key: activate.route_key.clone(),
                            generation: activate.generation,
                            bound_scopes: activate.bound_scopes.clone(),
                            envelope: envelope.clone(),
                        });
                    }
                    if let Some(unit) = to_sink {
                        sink.push(SinkControlSignal::Activate {
                            unit,
                            route_key: activate.route_key.clone(),
                            generation: activate.generation,
                            bound_scopes: activate.bound_scopes.clone(),
                            envelope: envelope.clone(),
                        });
                    }
                    if let Some(unit) = to_facade {
                        facade.push(FacadeControlSignal::Activate {
                            unit,
                            route_key: activate.route_key.clone(),
                            generation: activate.generation,
                            bound_scopes: activate.bound_scopes.clone(),
                        });
                    }
                }
                ExecControl::Deactivate(deactivate) => {
                    let to_source = source_unit_from_id(&deactivate.unit_id);
                    let to_sink = sink_unit_from_id(&deactivate.unit_id);
                    let to_facade = facade_unit_from_id(&deactivate.unit_id);
                    if to_source.is_none() && to_sink.is_none() && to_facade.is_none() {
                        return Err(CnxError::NotSupported(format!(
                            "fs-meta: unsupported unit_id '{}' in control envelope",
                            deactivate.unit_id
                        )));
                    }
                    if let Some(unit) = to_source {
                        source.push(SourceControlSignal::Deactivate {
                            unit,
                            route_key: deactivate.route_key.clone(),
                            generation: deactivate.generation,
                            envelope: envelope.clone(),
                        });
                    }
                    if let Some(unit) = to_sink {
                        sink.push(SinkControlSignal::Deactivate {
                            unit,
                            route_key: deactivate.route_key.clone(),
                            generation: deactivate.generation,
                            envelope: envelope.clone(),
                        });
                    }
                    if let Some(unit) = to_facade {
                        facade.push(FacadeControlSignal::Deactivate {
                            unit,
                            route_key: deactivate.route_key.clone(),
                            generation: deactivate.generation,
                        });
                    }
                }
            }
            continue;
        }

        if let Some(tick) = decode_unit_tick_envelope(envelope)? {
            let to_source = source_unit_from_id(&tick.unit_id);
            let to_sink = sink_unit_from_id(&tick.unit_id);
            let to_facade = facade_unit_from_id(&tick.unit_id);
            if to_source.is_none() && to_sink.is_none() && to_facade.is_none() {
                return Err(CnxError::NotSupported(format!(
                    "fs-meta: unsupported unit_id '{}' in control envelope",
                    tick.unit_id
                )));
            }
            if let Some(unit) = to_source {
                source.push(SourceControlSignal::Tick {
                    unit,
                    route_key: tick.route_key.clone(),
                    generation: tick.generation,
                    envelope: envelope.clone(),
                });
            }
            if let Some(unit) = to_sink {
                sink.push(SinkControlSignal::Tick {
                    unit,
                    route_key: tick.route_key.clone(),
                    generation: tick.generation,
                    envelope: envelope.clone(),
                });
            }
            if let Some(unit) = to_facade {
                facade.push(FacadeControlSignal::Tick {
                    unit,
                    route_key: tick.route_key.clone(),
                    generation: tick.generation,
                });
            }
            continue;
        }

        if let Some(confirmed) = decode_unit_exposure_confirmed_envelope(envelope)? {
            if let Some(signal) = facade_signal_from_trusted_exposure_confirmed(&confirmed) {
                facade.push(signal);
            }
            continue;
        }

        if let Some(changed) = decode_runtime_host_object_grants_changed_envelope(envelope)? {
            source.push(SourceControlSignal::RuntimeHostObjectGrantsChanged {
                changed: changed.clone(),
                envelope: envelope.clone(),
            });
            sink.push(SinkControlSignal::RuntimeHostObjectGrantsChanged {
                changed: changed.clone(),
                envelope: envelope.clone(),
            });
            facade.push(FacadeControlSignal::RuntimeHostObjectGrantsChanged { _changed: changed });
            continue;
        }

        if let ControlEnvelope::Frame(frame) = envelope
            && frame.kind == MANUAL_RESCAN_CONTROL_FRAME_KIND
        {
            source.push(SourceControlSignal::ManualRescan {
                envelope: envelope.clone(),
            });
            continue;
        }

        source.push(SourceControlSignal::Passthrough(envelope.clone()));
        sink.push(SinkControlSignal::Passthrough(envelope.clone()));
        facade.push(FacadeControlSignal::Passthrough);
    }
    Ok((source, sink, facade))
}
