use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

pub mod driver;
mod runtime_app;
pub mod service;

pub use driver::{
    MysqlSnapshotPage, MysqlSnapshotSession, MysqlSourceDriver, NativeMysqlSourceDriver,
    UnsupportedMysqlSourceDriver,
};
pub use mysql_source::shared_types;
pub use mysql_source::{
    GrantedMysqlEndpoint, MysqlEndpointConfig, MysqlSourceConfig, MysqlSourceRuntimeConfig,
    MysqlSourceRuntimeInputs, ResolvedMysqlEndpoint,
};
pub use runtime_app::MysqlSourceRuntimeApp;
pub use service::MysqlSourceService;

capanix_unit_entry!(MysqlSourceRuntimeApp);
