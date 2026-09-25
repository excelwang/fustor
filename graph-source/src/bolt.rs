//! Minimal single-transaction Bolt transport for bounded scalar/table observation.
//! Not a graph client: no routing, retry, commit, procedures or graph hydration.
use crate::{GraphEndpoint, GraphSourceRequest};
use capanix_app_sdk::{CnxError, Result};
use serde_json::{Map, Value, json};
use std::{collections::BTreeSet, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_DEPTH: usize = 32;
const MAX_VALUES: usize = 65_536;
const REQUEST_CAP: usize = 262_144;
const PROTOCOL_ALLOWANCE: usize = 65_536;

fn peer(stage: &'static str) -> CnxError {
    CnxError::PeerError(format!("graph provider {stage} failed (details withheld)"))
}
fn bounds() -> CnxError {
    CnxError::ResourceExhausted("graph bounded Bolt input exhausted".into())
}

// All lengths are checked against remaining input BEFORE allocating a container.
struct Decoder<'a> {
    rest: &'a [u8],
    values: usize,
}
impl<'a> Decoder<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if len > self.rest.len() {
            return Err(peer("bolt-decode"));
        }
        let (head, tail) = self.rest.split_at(len);
        self.rest = tail;
        Ok(head)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn uint(&mut self, bytes: usize) -> Result<usize> {
        self.take(bytes)?.iter().try_fold(0_usize, |n, b| {
            n.checked_mul(256)
                .and_then(|n| n.checked_add(usize::from(*b)))
                .ok_or_else(bounds)
        })
    }
    fn value(&mut self, depth: usize) -> Result<Value> {
        if depth > MAX_DEPTH || self.values >= MAX_VALUES {
            return Err(bounds());
        }
        self.values += 1;
        let tag = self.byte()?;
        let value = match tag {
            0x00..=0x7f => Value::from(tag),
            0xf0..=0xff => Value::from(i64::from(tag as i8)),
            0xc0 => Value::Null,
            0xc2 => Value::Bool(false),
            0xc3 => Value::Bool(true),
            0xc8..=0xcb => {
                let len = 1 << (tag - 0xc8);
                let raw = self.take(len)?;
                let mut bytes = [if raw[0] & 0x80 != 0 { 0xff } else { 0 }; 8];
                bytes[8 - len..].copy_from_slice(raw);
                Value::from(i64::from_be_bytes(bytes))
            }
            0xc1 => {
                let mut bytes = [0; 8];
                bytes.copy_from_slice(self.take(8)?);
                let number = serde_json::Number::from_f64(f64::from_be_bytes(bytes))
                    .ok_or_else(|| peer("bolt-non-json-value"))?;
                Value::Number(number)
            }
            0x80..=0x8f | 0xd0..=0xd2 => {
                let len = if tag < 0x90 {
                    usize::from(tag & 15)
                } else {
                    self.uint(1 << (tag - 0xd0))?
                };
                Value::String(
                    std::str::from_utf8(self.take(len)?)
                        .map_err(|_| peer("bolt-decode"))?
                        .to_owned(),
                )
            }
            0x90..=0x9f | 0xd4..=0xd6 => {
                let len = if tag < 0xa0 {
                    usize::from(tag & 15)
                } else {
                    self.uint(1 << (tag - 0xd4))?
                };
                if len > self.rest.len() || len > MAX_VALUES - self.values {
                    return Err(bounds());
                }
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.value(depth + 1)?);
                }
                Value::Array(values)
            }
            0xa0..=0xaf | 0xd8..=0xda => {
                let len = if tag < 0xb0 {
                    usize::from(tag & 15)
                } else {
                    self.uint(1 << (tag - 0xd8))?
                };
                if len > self.rest.len() / 2 || len > (MAX_VALUES - self.values) / 2 {
                    return Err(bounds());
                }
                let mut map = Map::new();
                for _ in 0..len {
                    let Value::String(key) = self.value(depth + 1)? else {
                        return Err(peer("bolt-map-key"));
                    };
                    let value = self.value(depth + 1)?;
                    if map.insert(key, value).is_some() {
                        return Err(peer("bolt-duplicate-key"));
                    }
                }
                Value::Object(map)
            }
            // Byte arrays and typed graph/temporal structures are not scalar/table JSON.
            _ => return Err(peer("bolt-non-json-value")),
        };
        Ok(value)
    }
}

fn size(out: &mut Vec<u8>, len: usize, tiny: u8, full: u8) -> Result<()> {
    if len > REQUEST_CAP {
        return Err(bounds());
    }
    if len < 16 {
        out.push(tiny | len as u8);
    } else if len <= u8::MAX as usize {
        out.extend_from_slice(&[full, len as u8]);
    } else if len <= u16::MAX as usize {
        out.push(full + 1);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(full + 2);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    Ok(())
}
fn encode(out: &mut Vec<u8>, value: &Value, depth: usize) -> Result<()> {
    if depth > MAX_DEPTH || out.len() > REQUEST_CAP {
        return Err(bounds());
    }
    match value {
        Value::Null => out.push(0xc0),
        Value::Bool(v) => out.push(if *v { 0xc3 } else { 0xc2 }),
        Value::Number(v) => {
            if let Some(n) = v.as_i64() {
                if (-16..128).contains(&n) {
                    out.push(n as u8);
                } else {
                    out.push(0xcb);
                    out.extend_from_slice(&n.to_be_bytes());
                }
            } else if v.is_f64() {
                out.push(0xc1);
                out.extend_from_slice(&v.as_f64().ok_or_else(bounds)?.to_be_bytes());
            } else {
                return Err(crate::config::invalid(
                    "graph integer exceeds signed 64-bit range",
                ));
            }
        }
        Value::String(v) => {
            size(out, v.len(), 0x80, 0xd0)?;
            out.extend_from_slice(v.as_bytes());
        }
        Value::Array(v) => {
            size(out, v.len(), 0x90, 0xd4)?;
            for item in v {
                encode(out, item, depth + 1)?;
            }
        }
        Value::Object(v) => {
            size(out, v.len(), 0xa0, 0xd8)?;
            for (key, item) in v {
                encode(out, &Value::String(key.clone()), depth + 1)?;
                encode(out, item, depth + 1)?;
            }
        }
    }
    if out.len() > REQUEST_CAP {
        return Err(bounds());
    }
    Ok(())
}

trait Socket: AsyncRead + AsyncWrite + Unpin {}
impl<T: AsyncRead + AsyncWrite + Unpin> Socket for T {}
struct Connection {
    socket: Box<dyn Socket>,
    wire_remaining: usize,
}
impl Connection {
    async fn send(&mut self, signature: u8, fields: &[Value]) -> Result<()> {
        if fields.len() > 15 {
            return Err(peer("bolt-request"));
        }
        let mut bytes = vec![0xb0 | fields.len() as u8, signature];
        for field in fields {
            encode(&mut bytes, field, 0)?;
        }
        for chunk in bytes.chunks(u16::MAX as usize) {
            self.socket
                .write_all(&(chunk.len() as u16).to_be_bytes())
                .await
                .map_err(|_| peer("bolt-send"))?;
            self.socket
                .write_all(chunk)
                .await
                .map_err(|_| peer("bolt-send"))?;
        }
        self.socket
            .write_all(&[0, 0])
            .await
            .map_err(|_| peer("bolt-send"))?;
        self.socket.flush().await.map_err(|_| peer("bolt-send"))
    }
    async fn receive(&mut self) -> Result<(u8, Value)> {
        let mut bytes = Vec::new();
        loop {
            self.wire_remaining = self.wire_remaining.checked_sub(2).ok_or_else(bounds)?;
            let len = self
                .socket
                .read_u16()
                .await
                .map_err(|_| peer("bolt-receive"))? as usize;
            if len == 0 {
                if bytes.is_empty() {
                    continue;
                } // NOOP, still bounded by time and wire bytes.
                break;
            }
            self.wire_remaining = self.wire_remaining.checked_sub(len).ok_or_else(bounds)?;
            let start = bytes.len();
            bytes.resize(start + len, 0);
            self.socket
                .read_exact(&mut bytes[start..])
                .await
                .map_err(|_| peer("bolt-receive"))?;
        }
        let mut decoder = Decoder {
            rest: &bytes,
            values: 0,
        };
        if decoder.byte()? != 0xb1 {
            return Err(peer("bolt-message-shape"));
        }
        let signature = decoder.byte()?;
        let value = decoder.value(0)?;
        if !decoder.rest.is_empty() {
            return Err(peer("bolt-trailing-bytes"));
        }
        Ok((signature, value))
    }
    async fn success(&mut self, stage: &'static str) -> Result<Map<String, Value>> {
        let (signature, value) = self.receive().await?;
        match (signature, value) {
            (0x70, Value::Object(map)) => Ok(map),
            _ => Err(peer(stage)), // Never expose provider error metadata.
        }
    }
    async fn rows(
        &mut self,
        fields: Vec<String>,
        row_limit: usize,
        byte_limit: u64,
    ) -> Result<(Vec<Value>, u64)> {
        let mut rows = Vec::new();
        let mut bytes = 0_u64;
        // Each PULL must produce one record or terminate, never a progress-free loop.
        loop {
            self.send(0x3f, &[json!({"n": 1})]).await?;
            let mut seen = false;
            loop {
                let (signature, value) = self.receive().await?;
                match (signature, value) {
                    (0x71, Value::Array(values)) => {
                        if seen || values.len() != fields.len() {
                            return Err(peer("bolt-record-shape"));
                        }
                        seen = true;
                        if rows.len() >= row_limit {
                            return Err(CnxError::ResourceExhausted(
                                "graph query exceeds explicit max_rows".into(),
                            ));
                        }
                        let row = Value::Object(fields.iter().cloned().zip(values).collect());
                        bytes = bytes
                            .checked_add(
                                serde_json::to_vec(&row)
                                    .map_err(|_| peer("bolt-result"))?
                                    .len() as u64,
                            )
                            .ok_or_else(bounds)?;
                        if bytes > byte_limit {
                            return Err(CnxError::ResourceExhausted(
                                "graph query max_bytes exhausted".into(),
                            ));
                        }
                        rows.push(row);
                    }
                    (0x70, Value::Object(metadata)) => match metadata.get("has_more") {
                        None | Some(Value::Bool(false)) => return Ok((rows, bytes)),
                        Some(Value::Bool(true)) if seen => break,
                        _ => return Err(peer("bolt-result-progress")),
                    },
                    _ => return Err(peer("bolt-result")),
                }
            }
        }
    }
}

pub(crate) async fn query(
    endpoint: &GraphEndpoint,
    statement: &str,
    request: &GraphSourceRequest,
    username: &str,
    password: &str,
    timeout: Duration,
    byte_limit: u64,
) -> Result<(Vec<Value>, u64)> {
    let deadline = tokio::time::Instant::now() + timeout;
    let url = url::Url::parse(&endpoint.endpoint_uri).map_err(|_| peer("bolt-config"))?;
    let host = match url.host().ok_or_else(|| peer("bolt-config"))? {
        url::Host::Domain(v) => v.to_owned(),
        url::Host::Ipv4(v) => v.to_string(),
        url::Host::Ipv6(v) => v.to_string(),
    };
    let stream = tokio::net::TcpStream::connect((host.as_str(), url.port().unwrap_or(7687)))
        .await
        .map_err(|_| peer("bolt-connect"))?;
    stream.set_nodelay(true).map_err(|_| peer("bolt-connect"))?;
    let socket: Box<dyn Socket> = match url.scheme() {
        "bolt" => Box::new(stream),
        "bolt+s" => {
            let tls = native_tls::TlsConnector::new().map_err(|_| peer("bolt-tls"))?;
            let stream = tokio_native_tls::TlsConnector::from(tls)
                .connect(&host, stream)
                .await
                .map_err(|_| peer("bolt-tls"))?;
            Box::new(stream)
        }
        _ => return Err(peer("bolt-config")),
    };
    let mut connection = Connection {
        socket,
        wire_remaining: usize::try_from(byte_limit)
            .map_err(|_| bounds())?
            .checked_add(PROTOCOL_ALLOWANCE)
            .ok_or_else(bounds)?,
    };
    connection
        .socket
        .write_all(&[
            0x60, 0x60, 0xb0, 0x17, 0, 0, 3, 5, 0, 0, 4, 4, 0, 0, 1, 4, 0, 0, 0, 4,
        ])
        .await
        .map_err(|_| peer("bolt-handshake"))?;
    connection
        .socket
        .flush()
        .await
        .map_err(|_| peer("bolt-handshake"))?;
    let mut selected = [0; 4];
    connection
        .socket
        .read_exact(&mut selected)
        .await
        .map_err(|_| peer("bolt-handshake"))?;
    if ![[0, 0, 3, 5], [0, 0, 4, 4], [0, 0, 1, 4], [0, 0, 0, 4]].contains(&selected) {
        return Err(peer("bolt-version"));
    }
    let auth = json!({"scheme": "basic", "principal": username, "credentials": password});
    let mut hello = json!({"user_agent": "fustor-graph-source/0.1.0"});
    if selected == [0, 0, 3, 5] {
        hello["bolt_agent"] = json!({"product": "fustor-bounded-bolt/0.1.0"});
    } else {
        hello
            .as_object_mut()
            .ok_or_else(|| peer("bolt-auth"))?
            .extend(auth.as_object().ok_or_else(|| peer("bolt-auth"))?.clone());
    }
    connection.send(0x01, &[hello]).await?;
    connection.success("bolt-hello").await?;
    if selected == [0, 0, 3, 5] {
        connection.send(0x6a, &[auth]).await?;
        connection.success("bolt-auth").await?;
    }
    let remaining = deadline
        .checked_duration_since(tokio::time::Instant::now())
        .ok_or(CnxError::Timeout)?;
    let mut transaction = json!({"mode": "r", "tx_timeout": remaining.as_millis().max(1) as u64});
    if let Some(database) = &endpoint.database {
        transaction["db"] = Value::String(database.clone());
    }
    connection.send(0x11, &[transaction]).await?;
    connection.success("bolt-begin").await?;
    let result = async {
        connection
            .send(
                0x10,
                &[
                    Value::String(statement.to_owned()),
                    serde_json::to_value(&request.parameters)
                        .map_err(|_| peer("bolt-parameters"))?,
                    json!({}),
                ],
            )
            .await?;
        let metadata = connection.success("bolt-execute").await?;
        let fields = metadata
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| peer("bolt-columns"))?;
        if fields.is_empty() || fields.len() > 256 {
            return Err(peer("bolt-columns"));
        }
        let fields = fields
            .iter()
            .map(|field| {
                field
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| peer("bolt-columns"))
            })
            .collect::<Result<Vec<_>>>()?;
        if fields.iter().collect::<BTreeSet<_>>().len() != fields.len() {
            return Err(peer("bolt-duplicate-column"));
        }
        connection
            .rows(fields, request.limit.unwrap_or(1), byte_limit)
            .await
    }
    .await;
    // A rejected stream may still be STREAMING/FAILED: rollback is best effort,
    // then owned-socket drop terminates the session. A successful read requires ACK.
    let closed = async {
        connection.send(0x13, &[]).await?;
        connection.success("bolt-rollback").await?;
        Ok(())
    }
    .await;
    drop(connection);
    match result {
        Ok(rows) => {
            closed?;
            Ok(rows)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod codec_tests {
    // Supplemental codec tests, not L1 or real-provider certification.
    use super::*;
    #[test]
    fn scalar_json_codec_and_boundaries() {
        let values = json!([null, true, false, -17, -16, 127, 128, i64::MIN, i64::MAX, 1.5,
            "真实数据", {"fields": ["n"], "n": 485}]);
        let mut bytes = Vec::new();
        encode(&mut bytes, &values, 0).unwrap();
        let mut decoder = Decoder {
            rest: &bytes,
            values: 0,
        };
        assert_eq!(decoder.value(0).unwrap(), values);
        assert!(decoder.rest.is_empty());
        for bytes in [
            vec![0xd6, 0xff, 0xff, 0xff, 0xff],
            vec![0xa2, 0x81, b'a', 1, 0x81, b'a', 2],
            vec![0xb0, 0x4e],
            vec![0xc1, 0x7f, 0xf0, 0, 0, 0, 0, 0, 0],
        ] {
            assert!(
                Decoder {
                    rest: &bytes,
                    values: 0
                }
                .value(0)
                .is_err()
            );
        }
        let mut deep = vec![0x91; MAX_DEPTH + 2];
        deep.push(0xc0);
        assert!(
            Decoder {
                rest: &deep,
                values: 0
            }
            .value(0)
            .is_err()
        );
    }
}
