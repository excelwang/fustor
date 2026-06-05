use std::collections::HashMap;

use capanix_app_sdk::Result;
use capanix_app_sdk::runtime::ConfigValue;

pub mod config;
pub mod product_model;
pub mod shared_types;

pub use config::{
    GrantedS3Endpoint, ResolvedS3Endpoint, S3EndpointConfig, S3SourceConfig, S3SourceRuntimeConfig,
    S3SourceRuntimeInputs,
};
pub use shared_types::{
    S3FieldCatalog, S3FieldDescriptor, S3ObjectBatch, S3ObjectEvent, S3PollCursor, S3ProbeStatus,
    S3SnapshotCursor, S3SourceCursor, S3SourceOperation, S3SourcePayload, S3SourceRequest,
};
pub use source_kit::{
    CredentialSource, ResolvedCredential, SourceCredentialConfig, SourceErrorPayload,
    decode_msgpack, encode_msgpack,
};

pub fn from_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<S3SourceRuntimeConfig> {
    S3SourceRuntimeConfig::from_manifest_config(cfg)
}
