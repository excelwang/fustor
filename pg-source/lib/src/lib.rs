use std::collections::HashMap;

use capanix_app_sdk::Result;
use capanix_app_sdk::runtime::ConfigValue;

pub mod config;
pub mod product_model;
pub mod shared_types;

pub use config::{
    GrantedPgEndpoint, PgEndpointConfig, PgSourceConfig, PgSourceRuntimeConfig,
    PgSourceRuntimeInputs, ResolvedPgEndpoint,
};
pub use shared_types::{
    PgCatalogMetadata, PgCatalogObject,
    PgFieldCatalog, PgFieldDescriptor, PgProbeStatus, PgRowBatch, PgRowEvent, PgSnapshotCursor,
    PgSourceCursor, PgSourceOperation, PgSourcePayload, PgSourceRequest,
};
pub use source_kit::{
    CredentialSource, ResolvedCredential, SourceCredentialConfig, SourceErrorPayload,
    SourceRuntimeStatus, decode_msgpack, encode_msgpack,
};

pub fn from_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<PgSourceRuntimeConfig> {
    PgSourceRuntimeConfig::from_manifest_config(cfg)
}
