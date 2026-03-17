use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::{ControlEnvelope, NodeId, RouteKey};
use capanix_app_sdk::{CnxError, Event, Result, RuntimeBoundary};

use crate::error::effective_rpc_timeout;
use crate::transport::RuntimeSupportTransport;

const RETRY_BACKOFF: Duration = Duration::from_millis(100);

pub trait TypedWorkerRpc {
    type Request;
    type Response;

    fn encode_request(request: &Self::Request) -> Result<Vec<u8>>;
    fn decode_response(payload: &[u8]) -> Result<Self::Response>;
    fn into_result(response: Self::Response) -> Result<Self::Response>;
    fn unavailable_label() -> &'static str;
}

pub struct TypedWorkerClient<Rpc> {
    transport: Arc<RuntimeSupportTransport>,
    _rpc: PhantomData<Rpc>,
}

impl<Rpc> Clone for TypedWorkerClient<Rpc> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            _rpc: PhantomData,
        }
    }
}

impl<Rpc: TypedWorkerRpc> TypedWorkerClient<Rpc> {
    pub fn spawn<T>(
        node_id: &NodeId,
        route_key: RouteKey,
        bin_path: &Path,
        socket_dir: &Path,
        label: &str,
        boundary: Arc<T>,
    ) -> Result<Self>
    where
        T: RuntimeBoundary + ChannelIoSubset + 'static,
    {
        Ok(Self {
            transport: Arc::new(RuntimeSupportTransport::spawn(
                node_id, route_key, bin_path, socket_dir, label, boundary,
            )?),
            _rpc: PhantomData,
        })
    }

    pub fn call_with_timeout(
        &self,
        request: Rpc::Request,
        timeout: Duration,
    ) -> Result<Rpc::Response> {
        let payload = Rpc::encode_request(&request)?;
        let replies = self
            .transport
            .ask(Bytes::from(payload), timeout, Rpc::unavailable_label())?;
        let first = first_reply(&replies)?;
        let response = Rpc::decode_response(first.payload_bytes())?;
        Rpc::into_result(response)
    }

    pub fn retry_until(
        &self,
        mut build_request: impl FnMut() -> Rpc::Request,
        rpc_timeout: Duration,
        total_timeout: Duration,
        mut validate: impl FnMut(Rpc::Response) -> Result<()>,
    ) -> Result<()> {
        let deadline = std::time::Instant::now() + total_timeout;
        loop {
            let attempt_timeout = effective_rpc_timeout(deadline, rpc_timeout)?;
            match self.call_with_timeout(build_request(), attempt_timeout) {
                Ok(response) => return validate(response),
                Err(CnxError::Timeout) => {
                    eprintln!(
                        "runtime-support: {} timed out after {:?} (remaining total {:?})",
                        Rpc::unavailable_label(),
                        attempt_timeout,
                        deadline.saturating_duration_since(std::time::Instant::now())
                    );
                    if std::time::Instant::now() >= deadline {
                        return Err(CnxError::Timeout);
                    }
                    std::thread::sleep(RETRY_BACKOFF);
                }
                Err(err) => {
                    eprintln!(
                        "runtime-support: {} attempt failed after {:?}: {:?}",
                        Rpc::unavailable_label(),
                        attempt_timeout,
                        err
                    );
                    if std::time::Instant::now() >= deadline {
                        return Err(err);
                    }
                    std::thread::sleep(RETRY_BACKOFF);
                }
            }
        }
    }

    pub fn control_frames(&self, envelopes: &[ControlEnvelope], timeout: Duration) -> Result<()> {
        self.transport.on_control_frame(envelopes, timeout)
    }

    pub fn retry_control_until(
        &self,
        mut build_envelopes: impl FnMut() -> Result<Vec<ControlEnvelope>>,
        rpc_timeout: Duration,
        total_timeout: Duration,
    ) -> Result<()> {
        let deadline = std::time::Instant::now() + total_timeout;
        loop {
            let attempt_timeout = effective_rpc_timeout(deadline, rpc_timeout)?;
            match build_envelopes()
                .and_then(|envelopes| self.control_frames(&envelopes, attempt_timeout))
            {
                Ok(()) => return Ok(()),
                Err(CnxError::Timeout) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(CnxError::Timeout);
                    }
                    std::thread::sleep(RETRY_BACKOFF);
                }
                Err(err) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(err);
                    }
                    std::thread::sleep(RETRY_BACKOFF);
                }
            }
        }
    }

    pub fn close(&self, request: Rpc::Request, timeout: Duration) -> Result<()> {
        let payload = Rpc::encode_request(&request)?;
        self.transport.close(Bytes::from(payload), timeout);
        Ok(())
    }

    pub fn shutdown(&self) -> Result<()> {
        self.transport.shutdown();
        Ok(())
    }
}

fn first_reply(replies: &[Event]) -> Result<&Event> {
    replies
        .first()
        .ok_or_else(|| CnxError::ProtocolViolation("worker response batch is empty".into()))
}
