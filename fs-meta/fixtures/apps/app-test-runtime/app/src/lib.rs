use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use capanix_app_sdk::{Event, Result, RuntimeBoundary, RuntimeBoundaryApp};
use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::{ControlEnvelope, EventMetadata, NodeId, RecvOpts};

pub struct TestRuntimeApp {
    _boundary: Arc<dyn RuntimeBoundary>,
    _data_boundary: Arc<dyn ChannelIoSubset>,
}

impl TestRuntimeApp {
    pub fn new(boundary: Arc<dyn RuntimeBoundary>, data_boundary: Arc<dyn ChannelIoSubset>) -> Self {
        Self {
            _boundary: boundary,
            _data_boundary: data_boundary,
        }
    }
}

#[async_trait]
impl RuntimeBoundaryApp for TestRuntimeApp {
    async fn send(&self, _events: &[Event], _timeout: Duration) -> Result<()> {
        Ok(())
    }

    async fn recv(&self, _opts: RecvOpts) -> Result<Vec<Event>> {
        Ok(vec![Event::new(
            EventMetadata {
                origin_id: NodeId("test-runtime".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: None,
                ingress_auth: None,
                trace: None,
            },
            Bytes::from_static(b"{}"),
        )])
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn on_control_frame(&self, _envelopes: &[ControlEnvelope]) -> Result<()> {
        Ok(())
    }
}
