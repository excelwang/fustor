#[cfg(not(feature = "embedded-inspection"))]
use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

pub mod driver;
mod runtime_app;
pub mod service;

pub use csv_source::shared_types;
pub use csv_source::{
    CsvEndpointConfig, CsvSourceConfig, CsvSourceRuntimeConfig, CsvSourceRuntimeInputs,
    GrantedCsvEndpoint, ResolvedCsvEndpoint,
};
pub use driver::{
    CsvRecordPage, CsvSourceDriver, NativeCsvSourceDriver, UnsupportedCsvSourceDriver,
};
pub use runtime_app::CsvSourceRuntimeApp;
pub use service::CsvSourceService;

#[cfg(not(feature = "embedded-inspection"))]
capanix_unit_entry!(CsvSourceRuntimeApp);
