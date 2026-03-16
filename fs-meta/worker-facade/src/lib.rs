pub use capanix_app_fs_meta::FSMetaRuntimeApp;
use capanix_unit_entry_macros::capanix_unit_entry;

// Embedded facade-worker entry wiring stays artifact-local; product mode remains
// only `embedded | external`.
capanix_unit_entry!(FSMetaRuntimeApp);
