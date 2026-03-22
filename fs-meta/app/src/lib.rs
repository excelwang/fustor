use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

mod config;
pub mod api;
pub mod query;
pub mod runtime;
mod runtime_app;
pub mod sink;
pub mod source;
mod state;
pub mod workers;

pub use fs_meta::shared_types;
pub use fs_meta::{
    ControlCommand, ControlEvent, EpochType, EventKind, FileMetaRecord, GrantedMountRoot,
    GroupReliability, LogicalClock, RootSelector, RootSpec, SyncTrack,
};
pub use config::{FSMetaConfig, FSMetaRuntimeInputs};
pub use fs_meta::FSMetaConfig as FSMetaProductConfig;
pub use runtime_app::{FSMetaApp, FSMetaRuntimeApp};

capanix_unit_entry!(FSMetaRuntimeApp);
