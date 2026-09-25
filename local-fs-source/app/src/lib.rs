#[cfg(not(feature = "embedded-inspection"))]
use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

pub mod driver;
mod runtime_app;
pub mod service;

pub use driver::{
    LocalFsEntryPage, LocalFsSourceDriver, NativeLocalFsSourceDriver,
    UnsupportedLocalFsSourceDriver,
};
pub use local_fs_source::shared_types;
pub use local_fs_source::{
    GrantedLocalFsEndpoint, LocalFsEndpointConfig, LocalFsSourceConfig, LocalFsSourceRuntimeConfig,
    LocalFsSourceRuntimeInputs, ResolvedLocalFsEndpoint,
};
pub use runtime_app::LocalFsSourceRuntimeApp;
pub use service::LocalFsSourceService;

#[cfg(not(feature = "embedded-inspection"))]
capanix_unit_entry!(LocalFsSourceRuntimeApp);
