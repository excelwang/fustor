use capanix_app_sdk::runtime::NodeId;
use capanix_kernel_api::control::{CtlCommand, canonical_ctl_command_bytes};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CtlRequest {
    pub command: Value,
    pub auth: Value,
    #[serde(default)]
    pub forwarded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CtlResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControlFrame {
    pub kind: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum ControlEnvelope {
    Command(CtlRequest),
    Result(CtlResponse),
    Frame(ControlFrame),
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnouncedResourceExport {
    pub resource_id: String,
    pub node_id: NodeId,
    pub resource_kind: String,
    pub source: String,
    #[serde(default)]
    pub mount_hint: Option<String>,
    #[serde(default)]
    pub bind_addr: Option<String>,
    #[serde(default)]
    pub owner_uid: Option<u32>,
    #[serde(default)]
    pub owner_gid: Option<u32>,
    #[serde(default)]
    pub mode: Option<u32>,
}

pub fn canonical_ctl_command_value_bytes(command: &Value) -> Result<Vec<u8>, serde_json::Error> {
    match serde_json::from_value::<CtlCommand>(command.clone()) {
        Ok(typed) => canonical_ctl_command_bytes(&typed),
        Err(_) => serde_json::to_vec(&canonicalize_json(command.clone())),
    }
}

fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let mut canonical = serde_json::Map::new();
            for (key, value) in entries {
                canonical.insert(key, canonicalize_json(value));
            }
            Value::Object(canonical)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(canonicalize_json).collect()),
        other => other,
    }
}
