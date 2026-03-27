pub mod auth;
pub mod config;
mod errors;
pub(crate) mod facade_status;
mod handlers;
mod server;
mod state;
pub mod types;

pub use config::{ApiAuthConfig, ApiConfig, BootstrapAdminConfig, BootstrapManagementConfig};
pub use server::ApiServerHandle;
pub(crate) use server::spawn;
pub(crate) use state::{ApiControlGate, ApiRequestTracker};
#[cfg(test)]
pub(crate) use handlers::{
    RootsPutPauseHook, clear_roots_put_pause_hook, install_roots_put_pause_hook,
};
