#[cfg(not(feature = "embedded-inspection"))]
use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

pub mod driver;
mod runtime_app;
pub mod service;

pub use driver::{
    JsonlRecordPage, JsonlSourceDriver, NativeJsonlSourceDriver, UnsupportedJsonlSourceDriver,
};
pub use jsonl_source::shared_types;
pub use jsonl_source::{
    GrantedJsonlEndpoint, JsonlEndpointConfig, JsonlSourceConfig, JsonlSourceRuntimeConfig,
    JsonlSourceRuntimeInputs, ResolvedJsonlEndpoint,
};
pub use runtime_app::JsonlSourceRuntimeApp;
pub use service::JsonlSourceService;

#[cfg(not(feature = "embedded-inspection"))]
capanix_unit_entry!(JsonlSourceRuntimeApp);
