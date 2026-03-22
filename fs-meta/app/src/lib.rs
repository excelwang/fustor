use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

pub mod api;
mod config;
pub mod query;
pub mod runtime;
mod runtime_app;
pub mod sink;
pub mod source;
mod state;
pub mod workers;

pub use config::{FSMetaConfig, FSMetaRuntimeInputs};
pub use fs_meta::FSMetaConfig as FSMetaProductConfig;
pub use fs_meta::shared_types;
pub use fs_meta::{
    ControlCommand, ControlEvent, EpochType, EventKind, FileMetaRecord, GrantedMountRoot,
    GroupReliability, LogicalClock, RootSelector, RootSpec, SyncTrack,
};
pub use runtime_app::{FSMetaApp, FSMetaRuntimeApp};

capanix_unit_entry!(FSMetaRuntimeApp);
