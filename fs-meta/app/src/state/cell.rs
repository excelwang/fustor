use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use capanix_app_sdk::CnxError;
#[cfg(test)]
use capanix_app_sdk::runtime::in_memory_state_boundary;
use capanix_app_sdk::runtime::{
    StateCellHandle, StateCellReadRequest, StateCellWatchRequest, StateCellWriteRequest, StateClass,
};
use capanix_runtime_entry_sdk::advanced::boundary::{BoundaryContext, StateBoundary};

const AUTHORITY_JOURNAL_MAX_ENTRIES: usize = 4_096;
const AUTHORITY_SCHEMA_REV: u64 = 1;
const SIGNAL_SCHEMA_REV: u64 = 1;
fn now_us() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_micros() as u64,
        Err(_) => 0,
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct AuthorityEntry {
    seq: u64,
    scope: String,
    op: String,
    detail: String,
    committed_at_us: u64,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct AuthoritySnapshot {
    scope: String,
    next_seq: u64,
    entries: VecDeque<AuthorityEntry>,
}

impl AuthoritySnapshot {
    fn empty(scope: &str) -> Self {
        Self {
            scope: scope.to_string(),
            next_seq: 0,
            entries: VecDeque::new(),
        }
    }

    fn trim_tail(&mut self) {
        while self.entries.len() > AUTHORITY_JOURNAL_MAX_ENTRIES {
            self.entries.pop_front();
        }
    }
}

fn authority_handle(scope: &str) -> StateCellHandle {
    StateCellHandle {
        cell_id: format!("fs-meta.authority.{scope}"),
        schema_rev: AUTHORITY_SCHEMA_REV,
        state_class: StateClass::Authoritative,
    }
}

fn signal_handle(scope: &str, signal: &str) -> StateCellHandle {
    StateCellHandle {
        cell_id: format!("fs-meta.signal.{signal}.{scope}"),
        schema_rev: SIGNAL_SCHEMA_REV,
        state_class: StateClass::Authoritative,
    }
}

fn local_state_boundary_bridge(scope: &str) -> BoundaryContext {
    BoundaryContext::for_unit(scope)
}

fn is_statecell_not_found(err: &CnxError) -> bool {
    match err {
        CnxError::InvalidInput(msg) => msg.contains("statecell not found"),
        _ => false,
    }
}

#[derive(Clone)]
pub(crate) struct AuthorityJournal {
    scope: Arc<str>,
    handle: StateCellHandle,
    state_boundary: Arc<dyn StateBoundary>,
    state: Arc<Mutex<AuthoritySnapshot>>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct SignalSnapshot {
    scope: String,
    signal: String,
    seq: u64,
    requested_at_us: u64,
    requested_by: String,
}

impl SignalSnapshot {
    fn empty(scope: &str, signal: &str) -> Self {
        Self {
            scope: scope.to_string(),
            signal: signal.to_string(),
            seq: 0,
            requested_at_us: 0,
            requested_by: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SignalEvent {
    pub seq: u64,
    pub requested_by: String,
}

#[derive(Clone)]
pub(crate) struct SignalCell {
    scope: Arc<str>,
    signal: Arc<str>,
    handle: StateCellHandle,
    state_boundary: Arc<dyn StateBoundary>,
    state: Arc<Mutex<SignalSnapshot>>,
}

#[derive(Debug, serde::Deserialize)]
struct SignalWatchReply {
    next_offset: u64,
    updates: Vec<SignalWatchUpdate>,
}

#[derive(Debug, serde::Deserialize)]
struct SignalWatchUpdate {
    payload_b64: String,
}

impl SignalCell {
    pub(crate) fn from_state_boundary(
        scope: &str,
        signal: &str,
        state_boundary: Arc<dyn StateBoundary>,
    ) -> std::io::Result<Self> {
        let handle = signal_handle(scope, signal);
        let loaded = match state_boundary.statecell_read(
            local_state_boundary_bridge(scope),
            StateCellReadRequest {
                handle: handle.clone(),
            },
        ) {
            Ok(resp) => {
                if resp.status != "ok" {
                    return Err(std::io::Error::other(format!(
                        "statecell_read failed for signal={signal} scope={scope}: status={}",
                        resp.status
                    )));
                }
                if resp.payload.is_empty() {
                    SignalSnapshot::empty(scope, signal)
                } else {
                    let decoded: SignalSnapshot =
                        rmp_serde::from_slice(&resp.payload).map_err(|err| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("decode signal snapshot failed: {err}"),
                            )
                        })?;
                    if decoded.scope != scope || decoded.signal != signal {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "signal snapshot mismatch: expected {signal}@{scope}, got {}@{}",
                                decoded.signal, decoded.scope
                            ),
                        ));
                    }
                    decoded
                }
            }
            Err(err) if is_statecell_not_found(&err) => SignalSnapshot::empty(scope, signal),
            Err(err) => {
                return Err(std::io::Error::other(format!(
                    "statecell_read failed for signal={signal} scope={scope}: {err}"
                )));
            }
        };
        Ok(Self {
            scope: Arc::<str>::from(scope),
            signal: Arc::<str>::from(signal),
            handle,
            state_boundary,
            state: Arc::new(Mutex::new(loaded)),
        })
    }

    pub(crate) fn current_seq(&self) -> u64 {
        match self.state.lock() {
            Ok(state) => state.seq,
            Err(poisoned) => poisoned.into_inner().seq,
        }
    }

    pub(crate) fn emit(&self, requested_by: &str) -> Result<u64, CnxError> {
        let (payload, seq) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| CnxError::Internal("signal state lock poisoned".into()))?;
            state.seq = state.seq.wrapping_add(1);
            state.requested_at_us = now_us();
            state.requested_by = requested_by.to_string();
            let payload = rmp_serde::to_vec_named(&*state).map_err(|err| {
                CnxError::Internal(format!("encode signal snapshot failed: {err}"))
            })?;
            (payload, state.seq)
        };
        let result = self.state_boundary.statecell_write(
            local_state_boundary_bridge(&self.scope),
            StateCellWriteRequest {
                handle: self.handle.clone(),
                payload,
                lease_epoch: Some(seq),
            },
        )?;
        if result.status != "committed" && result.status != "ok" {
            return Err(CnxError::Internal(format!(
                "statecell_write returned non-committed status for signal={} scope={}: {}",
                self.signal, self.scope, result.status
            )));
        }
        Ok(seq)
    }

    pub(crate) fn watch_since(
        &self,
        from_offset: u64,
    ) -> Result<(u64, Vec<SignalEvent>), CnxError> {
        let response = match self.state_boundary.statecell_watch(
            local_state_boundary_bridge(&self.scope),
            StateCellWatchRequest {
                handle: self.handle.clone(),
                from_offset: Some(from_offset),
            },
        ) {
            Ok(response) => response,
            Err(err) if is_statecell_not_found(&err) => return Ok((from_offset, Vec::new())),
            Err(err) => return Err(err),
        };
        if response.status != "ok" {
            return Err(CnxError::Internal(format!(
                "statecell_watch failed for signal={} scope={}: {}",
                self.signal, self.scope, response.status
            )));
        }
        let payload: SignalWatchReply =
            serde_json::from_slice(&response.payload).map_err(|err| {
                CnxError::Internal(format!("decode signal watch payload failed: {err}"))
            })?;
        let mut events = Vec::with_capacity(payload.updates.len());
        for update in payload.updates {
            let raw = B64.decode(update.payload_b64).map_err(|err| {
                CnxError::Internal(format!("decode signal watch payload base64 failed: {err}"))
            })?;
            let snapshot: SignalSnapshot = rmp_serde::from_slice(&raw).map_err(|err| {
                CnxError::Internal(format!("decode signal watch snapshot failed: {err}"))
            })?;
            if snapshot.scope != self.scope.as_ref() || snapshot.signal != self.signal.as_ref() {
                continue;
            }
            events.push(SignalEvent {
                seq: snapshot.seq,
                requested_by: snapshot.requested_by,
            });
        }
        Ok((payload.next_offset, events))
    }
}

impl AuthorityJournal {
    pub(crate) fn from_state_boundary(
        scope: &str,
        state_boundary: Arc<dyn StateBoundary>,
    ) -> std::io::Result<Self> {
        let handle = authority_handle(scope);
        let loaded = match state_boundary.statecell_read(
            local_state_boundary_bridge(scope),
            StateCellReadRequest {
                handle: handle.clone(),
            },
        ) {
            Ok(resp) => {
                if resp.status != "ok" {
                    return Err(std::io::Error::other(format!(
                        "statecell_read failed for scope={scope}: status={}",
                        resp.status
                    )));
                }
                if resp.payload.is_empty() {
                    AuthoritySnapshot::empty(scope)
                } else {
                    let mut decoded: AuthoritySnapshot = rmp_serde::from_slice(&resp.payload)
                        .map_err(|err| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("decode authority snapshot failed: {err}"),
                            )
                        })?;
                    if decoded.scope != scope {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "authority snapshot scope mismatch: expected '{}', got '{}'",
                                scope, decoded.scope
                            ),
                        ));
                    }
                    decoded.trim_tail();
                    if let Some(last) = decoded.entries.back() {
                        decoded.next_seq = decoded.next_seq.max(last.seq);
                    }
                    decoded
                }
            }
            Err(err) if is_statecell_not_found(&err) => AuthoritySnapshot::empty(scope),
            Err(err) => {
                return Err(std::io::Error::other(format!(
                    "statecell_read failed for scope={scope}: {err}"
                )));
            }
        };
        Ok(Self {
            scope: Arc::<str>::from(scope),
            handle,
            state_boundary,
            state: Arc::new(Mutex::new(loaded)),
        })
    }

    pub(crate) fn append(&self, op: &str, detail: String) {
        let (payload, lease_epoch) = {
            let mut state = match self.state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    log::warn!(
                        "state-cell authority state lock poisoned for scope={}; recovering",
                        self.scope
                    );
                    poisoned.into_inner()
                }
            };
            state.next_seq = state.next_seq.wrapping_add(1);
            let entry = AuthorityEntry {
                seq: state.next_seq,
                scope: self.scope.to_string(),
                op: op.to_string(),
                detail,
                committed_at_us: now_us(),
            };
            state.entries.push_back(entry);
            state.trim_tail();
            let payload = match rmp_serde::to_vec_named(&*state) {
                Ok(bytes) => bytes,
                Err(err) => {
                    log::warn!(
                        "state-cell authority serialize failed for scope={}: {err}",
                        self.scope
                    );
                    return;
                }
            };
            (payload, Some(state.next_seq))
        };
        match self.state_boundary.statecell_write(
            local_state_boundary_bridge(&self.scope),
            StateCellWriteRequest {
                handle: self.handle.clone(),
                payload,
                lease_epoch,
            },
        ) {
            Ok(result) if result.status == "committed" || result.status == "ok" => {}
            Ok(result) => {
                log::warn!(
                    "state-cell authority write returned non-committed status scope={} status={} diagnostics={:?}",
                    self.scope,
                    result.status,
                    result.diagnostics
                );
            }
            Err(err) => {
                log::warn!(
                    "state-cell authority write failed for scope={}: {}",
                    self.scope,
                    err
                );
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        match self.state.lock() {
            Ok(state) => state.entries.len(),
            Err(poisoned) => poisoned.into_inner().entries.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct UnsupportedBoundary;

    impl StateBoundary for UnsupportedBoundary {}

    fn authoritative_handle(scope: &str) -> StateCellHandle {
        authority_handle(scope)
    }

    #[test]
    fn authority_journal_roundtrip_on_local_state_boundary() {
        let boundary = in_memory_state_boundary();
        let journal =
            AuthorityJournal::from_state_boundary("runtime.exec.source", boundary.clone())
                .expect("init");
        journal.append("source.bootstrap", "roots=1 exports=1".to_string());
        journal.append(
            "source.update_logical_roots",
            "roots=2 exports=1".to_string(),
        );

        let reopened =
            AuthorityJournal::from_state_boundary("runtime.exec.source", boundary).expect("reopen");
        assert_eq!(reopened.len(), 2);
    }

    #[test]
    fn authority_journal_rejects_scope_mismatch_payload() {
        let boundary = in_memory_state_boundary();
        let handle = authoritative_handle("runtime.exec.source");
        let bad_payload = rmp_serde::to_vec_named(&AuthoritySnapshot {
            scope: "runtime.exec.sink".to_string(),
            next_seq: 1,
            entries: VecDeque::new(),
        })
        .expect("encode");
        boundary
            .statecell_write(
                BoundaryContext::default(),
                StateCellWriteRequest {
                    handle,
                    payload: bad_payload,
                    lease_epoch: Some(1),
                },
            )
            .expect("seed payload");

        let err = match AuthorityJournal::from_state_boundary("runtime.exec.source", boundary) {
            Ok(_) => panic!("scope mismatch must fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string()
                .contains("authority snapshot scope mismatch")
        );
    }

    #[test]
    fn authority_journal_fails_closed_when_runtime_state_carrier_is_unsupported() {
        let err = match AuthorityJournal::from_state_boundary(
            "runtime.exec.source",
            Arc::new(UnsupportedBoundary),
        ) {
            Ok(_) => panic!("unsupported runtime state carrier must fail closed"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("statecell_read failed for scope=runtime.exec.source"),
            "unexpected authority init error: {err}"
        );
    }
}
