use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::{NodeId, RouteKey};
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
    transport: Arc<Mutex<RuntimeSupportTransport>>,
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
            transport: Arc::new(Mutex::new(RuntimeSupportTransport::spawn(
                node_id, route_key, bin_path, socket_dir, label, boundary,
            )?)),
            _rpc: PhantomData,
        })
    }

    pub fn call_with_timeout(
        &self,
        request: Rpc::Request,
        timeout: Duration,
    ) -> Result<Rpc::Response> {
        let payload = Rpc::encode_request(&request)?;
        let guard = self.transport.lock().map_err(|_| {
            CnxError::Internal("typed worker client transport lock poisoned".into())
        })?;
        let replies = guard.ask(Bytes::from(payload), timeout, Rpc::unavailable_label())?;
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
        let mut guard = self.transport.lock().map_err(|_| {
            CnxError::Internal("typed worker client transport lock poisoned".into())
        })?;
        guard.close(Bytes::from(payload), timeout);
        Ok(())
    }
}

fn first_reply(replies: &[Event]) -> Result<&Event> {
    replies
        .first()
        .ok_or_else(|| CnxError::ProtocolViolation("worker response batch is empty".into()))
}
