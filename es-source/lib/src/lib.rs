use std::collections::HashMap;

use capanix_app_sdk::Result;
use capanix_app_sdk::runtime::ConfigValue;

pub mod config;
pub mod product_model;
pub mod shared_types;

pub use config::{
    CredentialSource, EsCredentialConfig, EsEndpointConfig, EsSourceConfig, EsSourceRuntimeConfig,
    EsSourceRuntimeInputs, GrantedEsEndpoint, ResolvedCredential, ResolvedEsEndpoint,
};
pub use shared_types::{
    EsDocumentEvent, EsDocumentEventKind, EsFieldCatalog, EsFieldDescriptor, EsPollCursor,
    EsProbeStatus, EsSnapshotCursor, EsSourceBatch, EsSourceCursor, EsSourceErrorPayload,
    EsSourceOperation, EsSourcePayload, EsSourceRequest, decode_msgpack, encode_msgpack,
};

pub fn from_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<EsSourceRuntimeConfig> {
    EsSourceRuntimeConfig::from_manifest_config(cfg)
}
