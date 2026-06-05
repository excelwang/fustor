use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

mod runtime_app;
pub mod service;

pub use runtime_app::UnionGraphRuntimeApp;
pub use service::UnionGraphService;
pub use union_graph::*;

capanix_unit_entry!(UnionGraphRuntimeApp);
