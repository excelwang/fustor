use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

// fs-meta predates the workspace-wide `-D warnings` gate. Keep its existing
// state-machine lint debt scoped to the owning subsystem instead of weakening
// clippy for the newer Source/DataView packages.
#[allow(
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_if,
    clippy::filter_map_bool_then,
    clippy::if_same_then_else,
    clippy::needless_borrow,
    clippy::nonminimal_bool,
    clippy::question_mark,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::unnecessary_to_owned
)]
pub mod api;
mod config;
pub(crate) mod domain_state;
#[allow(
    clippy::bool_assert_comparison,
    clippy::collapsible_if,
    clippy::field_reassign_with_default,
    clippy::filter_map_bool_then,
    clippy::manual_range_patterns,
    clippy::needless_range_loop,
    clippy::nonminimal_bool,
    clippy::too_many_arguments
)]
pub mod query;
#[allow(
    clippy::items_after_test_module,
    clippy::manual_is_multiple_of,
    clippy::question_mark,
    clippy::redundant_iter_cloned,
    clippy::too_many_arguments
)]
pub mod runtime;
#[allow(
    clippy::await_holding_lock,
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_if,
    clippy::enum_variant_names,
    clippy::if_same_then_else,
    clippy::let_unit_value,
    clippy::needless_arbitrary_self_type,
    clippy::needless_borrow,
    clippy::needless_update,
    clippy::nonminimal_bool,
    clippy::redundant_field_names,
    clippy::redundant_guards,
    clippy::single_match,
    clippy::too_many_arguments,
    clippy::unnecessary_map_or,
    clippy::useless_asref,
    clippy::useless_conversion,
    clippy::useless_vec
)]
mod runtime_app;
#[allow(
    clippy::collapsible_if,
    clippy::field_reassign_with_default,
    clippy::obfuscated_if_else,
    clippy::redundant_field_names,
    clippy::too_many_arguments,
    clippy::unnecessary_map_or,
    clippy::useless_vec
)]
pub mod sink;
#[allow(
    clippy::await_holding_lock,
    clippy::clone_on_copy,
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_if,
    clippy::derivable_impls,
    clippy::field_reassign_with_default,
    clippy::if_same_then_else,
    clippy::let_and_return,
    clippy::needless_update,
    clippy::redundant_locals,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::unnecessary_to_owned,
    clippy::useless_vec
)]
pub mod source;
mod state;
#[allow(
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_if,
    clippy::enum_variant_names,
    clippy::field_reassign_with_default,
    clippy::large_enum_variant,
    clippy::manual_contains,
    clippy::manual_is_multiple_of,
    clippy::manual_map,
    clippy::manual_repeat_n,
    clippy::needless_borrow,
    clippy::needless_return,
    clippy::nonminimal_bool,
    clippy::obfuscated_if_else,
    clippy::print_literal,
    clippy::question_mark,
    clippy::redundant_pattern_matching,
    clippy::single_match,
    clippy::type_complexity,
    clippy::useless_conversion,
    clippy::useless_vec
)]
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
