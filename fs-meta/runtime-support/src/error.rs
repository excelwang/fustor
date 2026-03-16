use std::time::{Duration, Instant};

use capanix_app_sdk::{CnxError, Result};

pub(crate) fn contextualize_transport_error(unavailable_label: &str, err: CnxError) -> CnxError {
    match err {
        CnxError::AccessDenied(message) => {
            CnxError::AccessDenied(format!("{unavailable_label}: {message}"))
        }
        CnxError::InvalidAuthContext(message) => {
            CnxError::InvalidAuthContext(format!("{unavailable_label}: {message}"))
        }
        CnxError::SignatureInvalid(message) => {
            CnxError::SignatureInvalid(format!("{unavailable_label}: {message}"))
        }
        CnxError::ReplayDetected(message) => {
            CnxError::ReplayDetected(format!("{unavailable_label}: {message}"))
        }
        CnxError::ScopeDenied(message) => {
            CnxError::ScopeDenied(format!("{unavailable_label}: {message}"))
        }
        CnxError::NotSupported(message) => {
            CnxError::NotSupported(format!("{unavailable_label}: {message}"))
        }
        CnxError::InvalidManifest(message) => {
            CnxError::InvalidManifest(format!("{unavailable_label}: {message}"))
        }
        CnxError::ResourceExhausted(message) => {
            CnxError::ResourceExhausted(format!("{unavailable_label}: {message}"))
        }
        CnxError::Timeout => CnxError::Timeout,
        CnxError::NotReady(message) => {
            CnxError::NotReady(format!("{unavailable_label}: {message}"))
        }
        CnxError::Internal(message) => {
            CnxError::Internal(format!("{unavailable_label}: {message}"))
        }
        CnxError::ProtocolViolation(message) => {
            CnxError::ProtocolViolation(format!("{unavailable_label}: {message}"))
        }
        CnxError::LinkError(message) => {
            CnxError::LinkError(format!("{unavailable_label}: {message}"))
        }
        CnxError::TransportClosed(message) => {
            CnxError::TransportClosed(format!("{unavailable_label}: {message}"))
        }
        CnxError::PeerError(message) => {
            CnxError::PeerError(format!("{unavailable_label}: {message}"))
        }
        CnxError::InvalidInput(message) => {
            CnxError::InvalidInput(format!("{unavailable_label}: {message}"))
        }
        CnxError::TxInvalid(message) => {
            CnxError::TxInvalid(format!("{unavailable_label}: {message}"))
        }
        CnxError::TxPrecondition(message) => {
            CnxError::TxPrecondition(format!("{unavailable_label}: {message}"))
        }
        CnxError::TxCommitFailed(message) => {
            CnxError::TxCommitFailed(format!("{unavailable_label}: {message}"))
        }
        CnxError::Backpressure => CnxError::Backpressure,
        CnxError::ChannelClosed => CnxError::ChannelClosed,
        CnxError::TxBusy => CnxError::TxBusy,
    }
}

pub(crate) fn effective_rpc_timeout(deadline: Instant, rpc_timeout: Duration) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(CnxError::Timeout);
    }
    Ok(rpc_timeout.min(remaining))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contextualize_transport_error_preserves_timeout_category() {
        let err = contextualize_transport_error("source worker unavailable", CnxError::Timeout);
        assert!(matches!(err, CnxError::Timeout));
    }

    #[test]
    fn contextualize_transport_error_preserves_transport_closed_category_with_context() {
        let err = contextualize_transport_error(
            "sink worker unavailable",
            CnxError::TransportClosed("ipc reset".into()),
        );
        match err {
            CnxError::TransportClosed(message) => {
                assert!(message.contains("sink worker unavailable"));
                assert!(message.contains("ipc reset"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn effective_rpc_timeout_never_exceeds_remaining_total_timeout() {
        let deadline = Instant::now() + Duration::from_millis(20);
        let timeout =
            effective_rpc_timeout(deadline, Duration::from_secs(60)).expect("effective timeout");
        assert!(timeout <= Duration::from_millis(20));
    }
}
