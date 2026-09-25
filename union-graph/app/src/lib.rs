#[cfg(not(feature = "embedded-inspection"))]
use capanix_runtime_entry_sdk::entry::capanix_unit_entry;

pub mod data_view_service;
mod runtime_app;
pub mod service;

pub use data_view_service::{
    BuildScopedDataViewRequest, DataViewRuntimePayload, DataViewRuntimeRequest,
    DataViewRuntimeService,
};
pub use runtime_app::UnionGraphRuntimeApp;
pub use service::UnionGraphService;
pub use union_graph::*;

#[cfg(not(feature = "embedded-inspection"))]
capanix_unit_entry!(UnionGraphRuntimeApp);
