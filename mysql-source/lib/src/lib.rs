use std::collections::HashMap;

use capanix_app_sdk::Result;
use capanix_app_sdk::runtime::ConfigValue;

pub mod config;
pub mod product_model;
pub mod shared_types;

pub use config::{
    GrantedMysqlEndpoint, MysqlEndpointConfig, MysqlSourceConfig, MysqlSourceRuntimeConfig,
    MysqlSourceRuntimeInputs, ResolvedMysqlEndpoint,
};
pub use shared_types::{
    MysqlFieldCatalog, MysqlFieldDescriptor, MysqlProbeStatus, MysqlRowBatch, MysqlRowEvent,
    MysqlSnapshotCursor, MysqlSourceCursor, MysqlSourceOperation, MysqlSourcePayload,
    MysqlSourceRequest,
};
pub use source_kit::{
    CredentialSource, ResolvedCredential, SourceCredentialConfig, SourceErrorPayload,
    decode_msgpack, encode_msgpack,
};

pub fn from_manifest_config(
    cfg: &HashMap<String, ConfigValue>,
) -> Result<MysqlSourceRuntimeConfig> {
    MysqlSourceRuntimeConfig::from_manifest_config(cfg)
}
