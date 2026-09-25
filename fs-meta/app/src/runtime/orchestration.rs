use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use capanix_app_sdk::runtime::{ControlEnvelope, ControlFrame};
use capanix_app_sdk::{CnxError, Result};
use capanix_runtime_entry_sdk::control::{
    RuntimeBoundScope, RuntimeExecControl, RuntimeHostGrantChange, RuntimeUnitExposure,
    decode_runtime_exec_control, decode_runtime_host_grant_change, decode_runtime_unit_exposure,
    decode_runtime_unit_tick,
};

use crate::runtime::execution_units::{
    FACADE_RUNTIME_UNIT_ID, QUERY_PEER_RUNTIME_UNIT_ID, QUERY_RUNTIME_UNIT_ID,
    SINK_RUNTIME_UNIT_ID, SOURCE_RUNTIME_UNIT_ID, SOURCE_SCAN_RUNTIME_UNIT_ID,
};
use crate::source::config::RootSpec;

pub(crate) const MANUAL_RESCAN_CONTROL_FRAME_KIND: &str = "fs-meta.manual-rescan-control:v1";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EventRouteScopeProjection {
    pub route_scope: String,
    pub generation: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ManualRescanControlPayload {
    pub requested_at_us: u64,
    #[serde(default)]
    pub request_id: String,
    #[serde(default)]
    pub scoped_target_acceptance_timeout_ms: Option<u64>,
    /// Fustor-resolved opaque output route scopes for the named logical groups.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub event_route_scopes_by_group: BTreeMap<String, EventRouteScopeProjection>,
}

#[cfg(test)]
pub(crate) fn encode_manual_rescan_envelope(requested_at_us: u64) -> Result<ControlEnvelope> {
    encode_manual_rescan_envelope_with_scoped_target_acceptance_timeout(requested_at_us, None)
}

#[cfg(test)]
pub(crate) fn encode_manual_rescan_envelope_with_scoped_target_acceptance_timeout(
    requested_at_us: u64,
    scoped_target_acceptance_timeout: Option<std::time::Duration>,
) -> Result<ControlEnvelope> {
    encode_manual_rescan_envelope_with_request_id_and_scoped_target_acceptance_timeout(
        String::new(),
        requested_at_us,
        scoped_target_acceptance_timeout,
    )
}

pub(crate) fn encode_manual_rescan_envelope_with_request_id_and_scoped_target_acceptance_timeout(
    request_id: String,
    requested_at_us: u64,
    scoped_target_acceptance_timeout: Option<std::time::Duration>,
) -> Result<ControlEnvelope> {
    encode_manual_rescan_envelope_with_event_route_scopes(
        request_id,
        requested_at_us,
        scoped_target_acceptance_timeout,
        BTreeMap::new(),
    )
}

pub(crate) fn encode_manual_rescan_envelope_with_event_route_scopes(
    request_id: String,
    requested_at_us: u64,
    scoped_target_acceptance_timeout: Option<std::time::Duration>,
    event_route_scopes_by_group: BTreeMap<String, EventRouteScopeProjection>,
) -> Result<ControlEnvelope> {
    Ok(ControlEnvelope::Frame(ControlFrame {
        kind: MANUAL_RESCAN_CONTROL_FRAME_KIND.to_string(),
        payload: rmp_serde::to_vec_named(&ManualRescanControlPayload {
            requested_at_us,
            request_id,
            scoped_target_acceptance_timeout_ms: scoped_target_acceptance_timeout
                .map(|timeout| timeout.as_millis().min(u128::from(u64::MAX)) as u64),
            event_route_scopes_by_group,
        })
        .map_err(|err| {
            CnxError::Internal(format!(
                "encode manual rescan control payload failed: {err}"
            ))
        })?,
    }))
}

pub(crate) fn decode_manual_rescan_control_payload_from_event_payload(
    payload: &[u8],
) -> Result<Option<ManualRescanControlPayload>> {
    if payload == b"manual-rescan" {
        return Ok(None);
    }
    let envelope = rmp_serde::from_slice::<ControlEnvelope>(payload).map_err(|err| {
        CnxError::InvalidInput(format!(
            "decode manual rescan control envelope failed: {err}"
        ))
    })?;
    let ControlEnvelope::Frame(frame) = envelope else {
        return Ok(None);
    };
    if frame.kind != MANUAL_RESCAN_CONTROL_FRAME_KIND {
        return Ok(None);
    }
    rmp_serde::from_slice::<ManualRescanControlPayload>(&frame.payload)
        .map(Some)
        .map_err(|err| {
            CnxError::InvalidInput(format!(
                "decode manual rescan control payload failed: {err}"
            ))
        })
}

pub(crate) fn manual_rescan_scoped_target_acceptance_timeout_from_payload(
    payload: &[u8],
) -> Option<std::time::Duration> {
    decode_manual_rescan_control_payload_from_event_payload(payload)
        .ok()
        .flatten()
        .and_then(|payload| payload.scoped_target_acceptance_timeout_ms)
        .map(std::time::Duration::from_millis)
}

#[cfg(test)]
mod manual_rescan_control_payload_tests {
    use super::*;

    #[derive(serde::Serialize)]
    struct LegacyManualRescanControlPayload {
        requested_at_us: u64,
        request_id: String,
        scoped_target_acceptance_timeout_ms: Option<u64>,
    }

    fn encode_event_payload(envelope: &ControlEnvelope) -> Vec<u8> {
        rmp_serde::to_vec_named(envelope).expect("encode manual-rescan event payload")
    }

    #[test]
    fn manual_rescan_event_route_projection_round_trips_and_legacy_payload_defaults_empty() {
        let envelope = encode_manual_rescan_envelope_with_event_route_scopes(
            "repair-7".to_string(),
            700,
            Some(std::time::Duration::from_millis(250)),
            BTreeMap::from([
                (
                    "nfs1".to_string(),
                    EventRouteScopeProjection {
                        route_scope: "node-b".to_string(),
                        generation: 7,
                    },
                ),
                (
                    "nfs2".to_string(),
                    EventRouteScopeProjection {
                        route_scope: "node-c".to_string(),
                        generation: 6,
                    },
                ),
            ]),
        )
        .expect("encode current manual-rescan envelope");
        let decoded = decode_manual_rescan_control_payload_from_event_payload(
            &encode_event_payload(&envelope),
        )
        .expect("decode current manual-rescan event payload")
        .expect("manual-rescan frame");
        assert_eq!(decoded.requested_at_us, 700);
        assert_eq!(decoded.request_id, "repair-7");
        assert_eq!(decoded.scoped_target_acceptance_timeout_ms, Some(250));
        assert_eq!(
            decoded.event_route_scopes_by_group,
            BTreeMap::from([
                (
                    "nfs1".to_string(),
                    EventRouteScopeProjection {
                        route_scope: "node-b".to_string(),
                        generation: 7,
                    },
                ),
                (
                    "nfs2".to_string(),
                    EventRouteScopeProjection {
                        route_scope: "node-c".to_string(),
                        generation: 6,
                    },
                ),
            ])
        );

        let legacy = ControlEnvelope::Frame(ControlFrame {
            kind: MANUAL_RESCAN_CONTROL_FRAME_KIND.to_string(),
            payload: rmp_serde::to_vec_named(&LegacyManualRescanControlPayload {
                requested_at_us: 600,
                request_id: "legacy".to_string(),
                scoped_target_acceptance_timeout_ms: Some(125),
            })
            .expect("encode legacy manual-rescan payload"),
        });
        let decoded_legacy =
            decode_manual_rescan_control_payload_from_event_payload(&encode_event_payload(&legacy))
                .expect("decode legacy manual-rescan event payload")
                .expect("legacy manual-rescan frame");
        assert_eq!(decoded_legacy.requested_at_us, 600);
        assert!(decoded_legacy.event_route_scopes_by_group.is_empty());

        assert!(
            decode_manual_rescan_control_payload_from_event_payload(b"manual-rescan")
                .expect("accept legacy raw manual-rescan token")
                .is_none()
        );
        assert!(
            decode_manual_rescan_control_payload_from_event_payload(b"corrupt-control-payload")
                .is_err(),
            "unknown malformed payloads must not bypass projection validation"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub(crate) struct RootsControlApplicationAttemptId {
    pub producer_process_id: u32,
    pub producer_epoch_us: u64,
    pub sequence: u64,
}

fn next_roots_control_application_attempt_id() -> RootsControlApplicationAttemptId {
    static PRODUCER_EPOCH_US: OnceLock<u64> = OnceLock::new();
    static NEXT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    let producer_epoch_us = *PRODUCER_EPOCH_US.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64
    });
    RootsControlApplicationAttemptId {
        producer_process_id: std::process::id(),
        producer_epoch_us,
        sequence: NEXT_SEQUENCE.fetch_add(1, Ordering::Relaxed),
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogicalRootsControlPayload {
    pub roots: Vec<RootSpec>,
    #[serde(default)]
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_attempt_id: Option<RootsControlApplicationAttemptId>,
    #[serde(default, skip_serializing_if = "bool_is_false")]
    pub defer_scan_audit: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sink_replay_envelopes: Vec<ControlEnvelope>,
}

fn bool_is_false(value: &bool) -> bool {
    !*value
}

#[allow(dead_code)]
pub(crate) fn encode_logical_roots_control_payload(roots: &[RootSpec]) -> Result<Vec<u8>> {
    encode_logical_roots_control_payload_with_generation(roots, 0)
}

pub(crate) fn encode_logical_roots_control_payload_with_generation(
    roots: &[RootSpec],
    generation: u64,
) -> Result<Vec<u8>> {
    encode_logical_roots_control_payload_with_generation_and_sink_replay(
        roots,
        generation,
        Vec::new(),
    )
}

pub(crate) fn encode_logical_roots_control_payload_with_generation_and_defer_scan_audit(
    roots: &[RootSpec],
    generation: u64,
    defer_scan_audit: bool,
) -> Result<Vec<u8>> {
    encode_logical_roots_control_payload_with_generation_sink_replay_and_defer_scan_audit(
        roots,
        generation,
        Vec::new(),
        defer_scan_audit,
    )
}

pub(crate) fn encode_logical_roots_control_payload_with_generation_and_sink_replay(
    roots: &[RootSpec],
    generation: u64,
    sink_replay_envelopes: Vec<ControlEnvelope>,
) -> Result<Vec<u8>> {
    encode_logical_roots_control_payload_with_generation_sink_replay_and_defer_scan_audit(
        roots,
        generation,
        sink_replay_envelopes,
        false,
    )
}

fn encode_logical_roots_control_payload_with_generation_sink_replay_and_defer_scan_audit(
    roots: &[RootSpec],
    generation: u64,
    sink_replay_envelopes: Vec<ControlEnvelope>,
    defer_scan_audit: bool,
) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(&LogicalRootsControlPayload {
        roots: roots.to_vec(),
        generation,
        application_attempt_id: Some(next_roots_control_application_attempt_id()),
        defer_scan_audit,
        sink_replay_envelopes,
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

#[cfg(test)]
mod logical_roots_control_payload_tests {
    use super::*;

    #[derive(serde::Serialize)]
    struct LegacyLogicalRootsControlPayload {
        roots: Vec<RootSpec>,
        generation: u64,
        defer_scan_audit: bool,
        sink_replay_envelopes: Vec<ControlEnvelope>,
    }

    #[test]
    fn encoded_application_waves_have_stable_unique_ids_and_legacy_payloads_still_decode() {
        let first_bytes = encode_logical_roots_control_payload_with_generation(&[], 7)
            .expect("encode first roots-control application wave");
        let first = decode_logical_roots_control_payload(&first_bytes)
            .expect("decode first roots-control application wave");
        let first_again = decode_logical_roots_control_payload(&first_bytes)
            .expect("decode cloned route payload");
        let second = decode_logical_roots_control_payload(
            &encode_logical_roots_control_payload_with_generation(&[], 7)
                .expect("encode explicit same-generation recovery wave"),
        )
        .expect("decode explicit same-generation recovery wave");

        assert!(first.application_attempt_id.is_some());
        assert_eq!(
            first.application_attempt_id, first_again.application_attempt_id,
            "route copies of one encoded payload must retain one application-wave identity"
        );
        assert_ne!(
            first.application_attempt_id, second.application_attempt_id,
            "an explicit same-generation recovery publication must create a new application wave"
        );

        let legacy_bytes = rmp_serde::to_vec_named(&LegacyLogicalRootsControlPayload {
            roots: Vec::new(),
            generation: 6,
            defer_scan_audit: false,
            sink_replay_envelopes: Vec::new(),
        })
        .expect("encode legacy roots-control payload");
        let legacy = decode_logical_roots_control_payload(&legacy_bytes)
            .expect("decode legacy roots-control payload without attempt id");
        assert_eq!(legacy.generation, 6);
        assert_eq!(legacy.application_attempt_id, None);
    }
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
        bound_scopes: Vec<RuntimeBoundScope>,
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
    RuntimeHostGrantChange {
        changed: RuntimeHostGrantChange,
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
            | Self::RuntimeHostGrantChange { envelope, .. }
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
        bound_scopes: Vec<RuntimeBoundScope>,
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
    RuntimeHostGrantChange {
        changed: RuntimeHostGrantChange,
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
            | Self::RuntimeHostGrantChange { envelope, .. } => envelope.clone(),
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
        bound_scopes: Vec<RuntimeBoundScope>,
    },
    Deactivate {
        unit: FacadeRuntimeUnit,
        route_key: String,
        generation: u64,
        restart_deferred_retire_pending: bool,
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
    RuntimeHostGrantChange {
        _changed: RuntimeHostGrantChange,
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
    confirmed: &RuntimeUnitExposure,
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
        if let Some(ctrl) = decode_runtime_exec_control(envelope)? {
            match ctrl {
                RuntimeExecControl::Activate(activate) => {
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
                RuntimeExecControl::Deactivate(deactivate) => {
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

        if let Some(tick) = decode_runtime_unit_tick(envelope)? {
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

        if let Some(changed) = decode_runtime_host_grant_change(envelope)? {
            signals.push(SourceControlSignal::RuntimeHostGrantChange {
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
        if let Some(ctrl) = decode_runtime_exec_control(envelope)? {
            match ctrl {
                RuntimeExecControl::Activate(activate) => {
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
                RuntimeExecControl::Deactivate(deactivate) => {
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

        if let Some(tick) = decode_runtime_unit_tick(envelope)? {
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

        if let Some(changed) = decode_runtime_host_grant_change(envelope)? {
            signals.push(SinkControlSignal::RuntimeHostGrantChange {
                changed,
                envelope: envelope.clone(),
            });
            continue;
        }

        signals.push(SinkControlSignal::Passthrough(envelope.clone()));
    }
    Ok(signals)
}

pub(crate) fn sink_signal_has_restart_deferred_events_stream_deactivate_reason(
    signal: &SinkControlSignal,
    events_stream_route_key: &str,
) -> bool {
    matches!(
        signal,
        SinkControlSignal::Deactivate {
            unit: SinkRuntimeUnit::Sink,
            route_key,
            envelope,
            ..
        } if route_key == events_stream_route_key
            && matches!(
                decode_runtime_exec_control(envelope),
                Ok(Some(RuntimeExecControl::Deactivate(deactivate)))
                    if deactivate.reason == "restart_deferred_retire_pending"
                        || deactivate.reason == "deferred_retire"
            )
    )
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
        if let Some(ctrl) = decode_runtime_exec_control(envelope)? {
            match ctrl {
                RuntimeExecControl::Activate(activate) => {
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
                RuntimeExecControl::Deactivate(deactivate) => {
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
                            restart_deferred_retire_pending: deactivate.reason
                                == "restart_deferred_retire_pending"
                                && deactivate
                                    .lease
                                    .as_ref()
                                    .and_then(|lease| lease.drain_started_at_ms)
                                    .is_some(),
                        });
                    }
                }
            }
            continue;
        }

        if let Some(tick) = decode_runtime_unit_tick(envelope)? {
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

        if let Some(confirmed) = decode_runtime_unit_exposure(envelope)? {
            if let Some(signal) = facade_signal_from_trusted_exposure_confirmed(&confirmed) {
                facade.push(signal);
            }
            continue;
        }

        if let Some(changed) = decode_runtime_host_grant_change(envelope)? {
            source.push(SourceControlSignal::RuntimeHostGrantChange {
                changed: changed.clone(),
                envelope: envelope.clone(),
            });
            sink.push(SinkControlSignal::RuntimeHostGrantChange {
                changed: changed.clone(),
                envelope: envelope.clone(),
            });
            facade.push(FacadeControlSignal::RuntimeHostGrantChange { _changed: changed });
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
