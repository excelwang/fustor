#[cfg(not(feature = "embedded-inspection"))]
use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

pub mod driver;
mod runtime_app;
pub mod service;

pub use driver::{
    NativePgSourceDriver, PgSnapshotPage, PgSnapshotSession, PgSourceDriver,
    UnsupportedPgSourceDriver,
};
pub use pg_source::shared_types;
pub use pg_source::{
    GrantedPgEndpoint, PgEndpointConfig, PgSourceConfig, PgSourceRuntimeConfig,
    PgSourceRuntimeInputs, ResolvedPgEndpoint,
};
pub use runtime_app::PgSourceRuntimeApp;
pub use service::PgSourceService;

#[cfg(not(feature = "embedded-inspection"))]
capanix_unit_entry!(PgSourceRuntimeApp);
