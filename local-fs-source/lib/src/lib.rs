use std::collections::HashMap;

use capanix_app_sdk::Result;
use capanix_app_sdk::runtime::ConfigValue;

pub mod config;
pub mod product_model;
pub mod shared_types;

pub use config::{
    GrantedLocalFsEndpoint, LocalFsEndpointConfig, LocalFsSourceConfig, LocalFsSourceRuntimeConfig,
    LocalFsSourceRuntimeInputs, ResolvedLocalFsEndpoint,
};
pub use shared_types::{
    LocalFsEntry, LocalFsEntryBatch, LocalFsFieldCatalog, LocalFsFieldDescriptor,
    LocalFsProbeStatus, LocalFsSnapshotCursor, LocalFsSourceCursor, LocalFsSourceOperation,
    LocalFsSourcePayload, LocalFsSourceRequest,
};
pub use source_kit::{SourceErrorPayload, SourceRuntimeStatus, decode_msgpack, encode_msgpack};

pub fn from_manifest_config(
    cfg: &HashMap<String, ConfigValue>,
) -> Result<LocalFsSourceRuntimeConfig> {
    LocalFsSourceRuntimeConfig::from_manifest_config(cfg)
}
