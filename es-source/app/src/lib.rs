use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

pub mod driver;
mod runtime_app;
pub mod service;

pub use driver::{EsSourceDriver, HttpEsSourceDriver};
pub use es_source::shared_types;
pub use es_source::{
    CredentialSource, EsCredentialConfig, EsEndpointConfig, EsSourceConfig, EsSourceRuntimeConfig,
    EsSourceRuntimeInputs, GrantedEsEndpoint, ResolvedCredential, ResolvedEsEndpoint,
};
pub use runtime_app::EsSourceRuntimeApp;
pub use service::EsSourceService;

capanix_unit_entry!(EsSourceRuntimeApp);
