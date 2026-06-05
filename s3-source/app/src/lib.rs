use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

pub mod driver;
mod runtime_app;
pub mod service;

pub use driver::{NativeS3SourceDriver, S3ObjectPage, S3SourceDriver, UnsupportedS3SourceDriver};
pub use runtime_app::S3SourceRuntimeApp;
pub use s3_source::shared_types;
pub use s3_source::{
    GrantedS3Endpoint, ResolvedS3Endpoint, S3EndpointConfig, S3SourceConfig, S3SourceRuntimeConfig,
    S3SourceRuntimeInputs,
};
pub use service::S3SourceService;

capanix_unit_entry!(S3SourceRuntimeApp);
