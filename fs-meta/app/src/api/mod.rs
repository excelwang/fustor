pub mod auth;
pub mod config;
mod errors;
pub(crate) mod facade_status;
mod handlers;
pub(crate) mod rollout_status;
mod server;
pub(crate) mod state;
pub mod types;

pub use config::{ApiAuthConfig, ApiConfig, BootstrapAdminConfig, BootstrapManagementConfig};
#[cfg(test)]
pub(crate) use handlers::{
    RescanPauseHook, RootsPutBeforeResponseHook, RootsPutPauseHook, StatusPauseHook,
    clear_rescan_pause_hook, clear_roots_put_before_response_hook, clear_roots_put_pause_hook,
    clear_status_pause_hook, clear_status_route_trace_capture, install_rescan_pause_hook,
    install_roots_put_before_response_hook, install_roots_put_pause_hook,
    install_status_pause_hook, install_status_route_trace_capture,
};
pub use server::ApiServerHandle;
pub(crate) use server::{spawn, spawn_with_rollout_status};
pub(crate) use state::{ApiControlGate, ApiRequestTracker, ManagementWriteRecovery};
