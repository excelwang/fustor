use capanix_app_sdk::runtime::ControlEnvelope;
use capanix_app_sdk::{CnxError, Result};
use capanix_kernel_api::control::ControlFrame;

pub fn encode_control_frame(kind: &str, payload: Vec<u8>) -> ControlEnvelope {
    ControlEnvelope::Frame(ControlFrame {
        kind: kind.to_string(),
        payload,
    })
}

pub fn decode_control_payload<'a>(
    envelope: &'a ControlEnvelope,
    expected_kind: &str,
) -> Result<&'a [u8]> {
    match envelope {
        ControlEnvelope::Frame(ControlFrame { kind, payload }) if kind == expected_kind => {
            Ok(payload.as_slice())
        }
        _ => Err(CnxError::ProtocolViolation(format!(
            "unexpected control envelope kind (expected {expected_kind})"
        ))),
    }
}
