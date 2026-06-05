use std::sync::{Arc, Mutex};

use capanix_app_sdk::runtime::{EventMetadata, NodeId};
use capanix_app_sdk::{CnxError, Event, Result};
use union_graph::{
    UnionGraphPayload, UnionGraphRequest, UnionGraphStore, decode_msgpack, encode_msgpack,
    handle_store_request,
};

#[derive(Clone)]
pub struct UnionGraphService {
    store: Arc<Mutex<UnionGraphStore>>,
    origin_id: NodeId,
}

impl UnionGraphService {
    pub fn new(store: UnionGraphStore, origin_id: NodeId) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            origin_id,
        }
    }

    pub fn handle_events(&self, events: Vec<Event>) -> Vec<Event> {
        events
            .into_iter()
            .map(|event| self.handle_event(event))
            .collect()
    }

    pub fn handle_event(&self, event: Event) -> Event {
        let correlation_id = event.metadata().correlation_id;
        let payload = match self.handle_event_payload(event.payload_bytes()) {
            Ok(payload) => payload,
            Err(err) => UnionGraphPayload::Error(source_kit::error_payload_from_cnx(
                err,
                "union-graph-transport",
            )),
        };
        self.reply_event(payload, correlation_id)
            .unwrap_or_else(|err| self.fallback_error_event(err, correlation_id))
    }

    fn handle_event_payload(&self, payload: &[u8]) -> Result<UnionGraphPayload> {
        let request: UnionGraphRequest = decode_msgpack(payload)?;
        let mut store = self
            .store
            .lock()
            .map_err(|_| CnxError::Internal("union graph store lock poisoned".into()))?;
        handle_store_request(&mut store, request)
    }

    fn reply_event(
        &self,
        payload: UnionGraphPayload,
        correlation_id: Option<u64>,
    ) -> Result<Event> {
        Ok(Event::new(
            EventMetadata {
                origin_id: self.origin_id.clone(),
                timestamp_us: now_us(),
                logical_ts: None,
                correlation_id,
                ingress_auth: None,
                trace: None,
            },
            encode_msgpack(&payload)?,
        ))
    }

    fn fallback_error_event(&self, err: CnxError, correlation_id: Option<u64>) -> Event {
        let payload = UnionGraphPayload::Error(source_kit::SourceErrorPayload::new(
            "encode-error",
            err.to_string(),
            false,
        ));
        let bytes = rmp_serde::to_vec_named(&payload).unwrap_or_else(|_| b"{}".to_vec());
        Event::new(
            EventMetadata {
                origin_id: self.origin_id.clone(),
                timestamp_us: now_us(),
                logical_ts: None,
                correlation_id,
                ingress_auth: None,
                trace: None,
            },
            bytes.into(),
        )
    }
}

fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_micros() as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use capanix_app_sdk::runtime::EventMetadata;
    use union_graph::{
        NodeKind, SourceGraphNode, SourceGraphSkeleton, SourceKind, UnionGraphPayload,
        UnionGraphRequest,
    };

    use super::*;

    #[test]
    fn service_ingests_and_queries_msgpack_payloads() {
        let service = UnionGraphService::new(
            UnionGraphStore::default(),
            NodeId("union-graph-test".into()),
        );
        let request = UnionGraphRequest::IngestSkeleton {
            skeleton: SourceGraphSkeleton {
                nodes: vec![SourceGraphNode {
                    source_kind: SourceKind::Fs,
                    source_instance: "nfs-a".into(),
                    local_id: "/data/a.txt".into(),
                    node_kind: NodeKind::Asset,
                    native_ref: "fs://nfs-a/data/a.txt".into(),
                    display_name: "a.txt".into(),
                    native_pointer: "fs-meta:nfs-a:/data/a.txt".into(),
                    fingerprint: None,
                    observed_at: 1,
                    grant_epoch: None,
                }],
                edges: Vec::new(),
                cross_edges: Vec::new(),
                evidence: Vec::new(),
            },
        };
        let event = Event::new(
            EventMetadata {
                origin_id: NodeId("caller".into()),
                timestamp_us: 1,
                logical_ts: None,
                correlation_id: Some(7),
                ingress_auth: None,
                trace: None,
            },
            encode_msgpack(&request).expect("encode"),
        );
        let reply = service.handle_event(event);
        let payload: UnionGraphPayload = decode_msgpack(reply.payload_bytes()).expect("decode");
        assert!(matches!(payload, UnionGraphPayload::IngestAck(_)));

        let query = Event::new(
            EventMetadata {
                origin_id: NodeId("caller".into()),
                timestamp_us: 2,
                logical_ts: None,
                correlation_id: Some(8),
                ingress_auth: None,
                trace: None,
            },
            encode_msgpack(&UnionGraphRequest::ListNodes {
                source_kind: Some(SourceKind::Fs),
                node_kind: None,
                limit: None,
            })
            .expect("encode"),
        );
        let reply = service.handle_event(query);
        let payload: UnionGraphPayload = decode_msgpack(reply.payload_bytes()).expect("decode");
        match payload {
            UnionGraphPayload::Nodes(batch) => assert_eq!(batch.nodes.len(), 1),
            other => panic!("expected nodes payload, got {other:?}"),
        }
    }
}
