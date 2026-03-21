use std::process::ExitCode;
use std::sync::Arc;

#[tokio::main]
async fn main() -> ExitCode {
    capanix_daemon::init_tracing();
    capanix_daemon::run_with_host_passthrough_bootstrap(Some(Arc::new(|boundary, node_id| {
        capanix_host_adapter_fs::spawn_host_passthrough_endpoint(
            boundary,
            node_id,
            tokio_util::sync::CancellationToken::new(),
        );
    })))
    .await
}
