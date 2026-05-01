use std::collections::BTreeSet;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, Method, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;

use capanix_app_sdk::runtime::NodeId;
use capanix_app_sdk::{CnxError, Result};
use capanix_runtime_entry_sdk::advanced::boundary::ChannelIoSubset;

use crate::query::api::{
    create_local_router, projection_policy_from_host_object_grants,
    refresh_policy_from_host_object_grants,
};
use crate::workers::sink::SinkFacade;
use crate::workers::source::{SourceFacade, SourceFailure};

use super::auth::AuthService;
use super::config::ResolvedApiConfig;
use super::errors::ApiError;
use super::facade_status::{
    PublishedFacadeStatusReader, SharedFacadePendingStatusCell, SharedFacadeServiceStateCell,
};
use super::handlers;
use super::rollout_status::{
    PublishedRolloutStatusReader, SharedRolloutStatusCell, shared_rollout_status_cell,
};
use super::state::{
    ApiControlGate, ApiRequestGuard, ApiRequestTracker, ApiState, ForceFindRunnerEvidence,
};

enum ApiServerJoin {
    Thread(std::thread::JoinHandle<()>),
}

pub struct ApiServerHandle {
    shutdown: CancellationToken,
    join: Option<ApiServerJoin>,
}

#[cfg(test)]
static SHUTDOWN_BLOCKING_JOIN_INFLIGHT: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
fn shutdown_blocking_join_inflight() -> usize {
    SHUTDOWN_BLOCKING_JOIN_INFLIGHT.load(Ordering::SeqCst)
}

impl ApiServerHandle {
    pub(crate) fn is_running(&self) -> bool {
        match self.join.as_ref() {
            Some(ApiServerJoin::Thread(join)) => !join.is_finished(),
            None => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn cancel_for_tests(&self) {
        self.shutdown.cancel();
    }

    pub async fn shutdown(mut self, timeout: Duration) {
        eprintln!("fs_meta_api_server: shutdown requested");
        self.shutdown.cancel();
        let Some(join) = self.join.take() else {
            return;
        };
        match join {
            ApiServerJoin::Thread(join) => {
                let deadline = tokio::time::Instant::now() + timeout;
                while !join.is_finished() {
                    if tokio::time::Instant::now() >= deadline {
                        log::warn!("fs-meta api server shutdown timed out");
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                if let Err(err) = join.join() {
                    log::warn!("fs-meta api server thread panicked: {:?}", err);
                }
            }
        }
    }
}

pub async fn spawn(
    cfg: ResolvedApiConfig,
    node_id: NodeId,
    runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    source: Arc<SourceFacade>,
    sink: Arc<SinkFacade>,
    query_sink: Arc<SinkFacade>,
    query_runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    facade_pending: SharedFacadePendingStatusCell,
    facade_service_state: SharedFacadeServiceStateCell,
    request_tracker: Arc<ApiRequestTracker>,
    control_gate: Arc<ApiControlGate>,
) -> Result<ApiServerHandle> {
    spawn_with_rollout_status(
        cfg,
        node_id,
        runtime_boundary,
        source,
        sink,
        query_sink,
        query_runtime_boundary,
        facade_pending,
        facade_service_state,
        shared_rollout_status_cell(),
        request_tracker,
        control_gate,
    )
    .await
}

pub(crate) async fn spawn_with_rollout_status(
    cfg: ResolvedApiConfig,
    node_id: NodeId,
    runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    source: Arc<SourceFacade>,
    sink: Arc<SinkFacade>,
    query_sink: Arc<SinkFacade>,
    query_runtime_boundary: Option<Arc<dyn ChannelIoSubset>>,
    facade_pending: SharedFacadePendingStatusCell,
    facade_service_state: SharedFacadeServiceStateCell,
    rollout_status: SharedRolloutStatusCell,
    request_tracker: Arc<ApiRequestTracker>,
    control_gate: Arc<ApiControlGate>,
) -> Result<ApiServerHandle> {
    eprintln!(
        "fs_meta_api_server: spawn begin bind_addr={}",
        cfg.bind_addr
    );
    cfg.auth
        .ensure_materialized()
        .map_err(CnxError::InvalidInput)?;
    eprintln!(
        "fs_meta_api_server: auth materialized bind_addr={}",
        cfg.bind_addr
    );
    let auth = Arc::new(AuthService::new(cfg.auth.clone()).map_err(|e| {
        CnxError::InvalidInput(format!("fs-meta api auth init failed: {}", e.message))
    })?);
    eprintln!(
        "fs_meta_api_server: auth service ready bind_addr={}",
        cfg.bind_addr
    );

    let initial_policy = projection_policy_from_host_object_grants(
        &source
            .cached_host_object_grants_snapshot_with_failure()
            .map_err(SourceFailure::into_error)?,
    );
    eprintln!(
        "fs_meta_api_server: initial policy ready bind_addr={}",
        cfg.bind_addr
    );
    let projection_policy = Arc::new(RwLock::new(initial_policy));
    let force_find_inflight = Arc::new(Mutex::new(BTreeSet::new()));
    let force_find_runner_evidence = ForceFindRunnerEvidence::default();
    let state = ApiState {
        node_id,
        runtime_boundary,
        query_runtime_boundary,
        force_find_inflight: force_find_inflight.clone(),
        force_find_runner_evidence: force_find_runner_evidence.clone(),
        source,
        sink,
        query_sink,
        auth,
        projection_policy,
        published_facade_status: PublishedFacadeStatusReader::new(
            facade_service_state,
            facade_pending,
        ),
        published_rollout_status: PublishedRolloutStatusReader::new(rollout_status),
        request_tracker,
        control_gate: control_gate.clone(),
    };
    refresh_policy_from_host_object_grants(
        &state.projection_policy,
        &state
            .source
            .cached_host_object_grants_snapshot_with_failure()
            .map_err(SourceFailure::into_error)?,
    );
    eprintln!(
        "fs_meta_api_server: policy refreshed bind_addr={}",
        cfg.bind_addr
    );
    let app = router(state, control_gate)?;
    eprintln!(
        "fs_meta_api_server: router ready bind_addr={}",
        cfg.bind_addr
    );

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    let bind_addr_for_log = cfg.bind_addr.clone();
    let (ready_tx, ready_rx) = sync_channel(1);

    let join = ApiServerJoin::Thread(std::thread::spawn(move || {
        eprintln!(
            "fs_meta_api_server: thread starting bind_addr={}",
            bind_addr_for_log
        );
        let worker_threads = std::thread::available_parallelism()
            .map(|n| n.get().clamp(4, 8))
            .unwrap_or(4);
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("fs-meta-api")
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(err) => {
                let _ = ready_tx.send(Err(format!("fs-meta api runtime build failed: {err}")));
                return;
            }
        };
        eprintln!(
            "fs_meta_api_server: runtime ready bind_addr={}",
            bind_addr_for_log
        );

        runtime.block_on(async move {
            eprintln!("fs_meta_api_server: binding {}", bind_addr_for_log);
            let listener = match tokio::net::TcpListener::bind(&bind_addr_for_log).await {
                Ok(listener) => listener,
                Err(err) => {
                    let _ = ready_tx.send(Err(format!("fs-meta api bind failed: {err}")));
                    return;
                }
            };
            let _ = ready_tx.send(Ok(()));
            eprintln!("fs_meta_api_server: bound {}", bind_addr_for_log);
            eprintln!("fs_meta_api_server: serving {}", bind_addr_for_log);
            let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                shutdown_signal.cancelled().await;
            });

            if let Err(err) = server.await {
                log::error!("fs-meta api server failed: {:?}", err);
            }
            eprintln!("fs_meta_api_server: serve loop ended {}", bind_addr_for_log);
        });
    }));

    eprintln!(
        "fs_meta_api_server: waiting for bind readiness bind_addr={}",
        cfg.bind_addr
    );
    let ready = tokio::task::spawn_blocking(move || ready_rx.recv_timeout(Duration::from_secs(10)))
        .await
        .map_err(|err| {
            CnxError::Internal(format!("fs-meta api server ready wait join failed: {err}"))
        })?;
    match ready {
        Ok(Ok(())) => Ok(ApiServerHandle {
            shutdown,
            join: Some(join),
        }),
        Ok(Err(err)) => {
            shutdown.cancel();
            match join {
                ApiServerJoin::Thread(join) => {
                    let _ = tokio::task::spawn_blocking(move || join.join()).await;
                }
            }
            Err(CnxError::InvalidInput(err))
        }
        Err(err) => {
            shutdown.cancel();
            match join {
                ApiServerJoin::Thread(join) => {
                    let _ = tokio::task::spawn_blocking(move || join.join()).await;
                }
            }
            Err(CnxError::InvalidInput(format!(
                "fs-meta api bind readiness timed out: {err:?}"
            )))
        }
    }
}

fn router(state: ApiState, control_gate: Arc<ApiControlGate>) -> Result<Router> {
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]);
    let projection_router = create_local_router(
        state.query_sink.clone(),
        state.source.clone(),
        state.query_runtime_boundary.clone(),
        state.node_id.clone(),
        state.projection_policy.clone(),
        state.force_find_inflight.clone(),
        state.force_find_runner_evidence.clone(),
    )
    .layer(middleware::from_fn_with_state(
        state.auth.clone(),
        query_api_key_guard,
    ));

    let management = Router::new()
        .route("/api/fs-meta/v1/session/login", post(handlers::login))
        .route("/api/fs-meta/v1/status", get(handlers::status))
        .route(
            "/api/fs-meta/v1/runtime/grants",
            get(handlers::runtime_grants),
        )
        .route("/api/fs-meta/v1/monitoring/roots", get(handlers::roots_get))
        .route("/api/fs-meta/v1/monitoring/roots", put(handlers::roots_put))
        .route(
            "/api/fs-meta/v1/monitoring/roots/preview",
            post(handlers::roots_preview),
        )
        .route("/api/fs-meta/v1/index/rescan", post(handlers::rescan))
        .route(
            "/api/fs-meta/v1/query-api-keys",
            get(handlers::query_api_keys_list),
        )
        .route(
            "/api/fs-meta/v1/query-api-keys",
            post(handlers::query_api_keys_create),
        )
        .route(
            "/api/fs-meta/v1/query-api-keys/:key_id",
            delete(handlers::query_api_keys_revoke),
        )
        .with_state(state.clone());
    Ok(management
        .nest("/api/fs-meta/v1", projection_router)
        // Management writes must count as in-flight facade/control work before
        // they block on control readiness, otherwise facade shutdown can tear
        // the listener down mid-request and surface transport errors upstream.
        .layer(middleware::from_fn_with_state(
            control_gate.clone(),
            request_control_readiness_guard,
        ))
        .layer(middleware::from_fn_with_state(
            control_gate.clone(),
            projection_request_facade_guard,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            request_logging,
        ))
        .layer(cors))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ApiManagementReadinessGate {
    FullManagementWrite,
    SourceRepair,
}

impl ApiManagementReadinessGate {
    fn is_ready(self, control_gate: &ApiControlGate) -> bool {
        match self {
            Self::FullManagementWrite => control_gate.is_management_write_ready(),
            Self::SourceRepair => control_gate.is_source_repair_ready(),
        }
    }

    fn blocked_by_management_write_drain(self) -> bool {
        matches!(self, Self::FullManagementWrite)
    }

    async fn wait_ready(self, control_gate: &ApiControlGate) {
        match self {
            Self::FullManagementWrite => control_gate.wait_management_write_ready().await,
            Self::SourceRepair => control_gate.wait_source_repair_ready().await,
        }
    }

    fn recovery(
        self,
        control_gate: &ApiControlGate,
    ) -> Option<super::state::ManagementWriteRecovery> {
        match self {
            Self::FullManagementWrite => control_gate.management_write_recovery(),
            Self::SourceRepair => control_gate.source_repair_recovery(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ApiRequestRoutePolicy {
    management_readiness_gate: Option<ApiManagementReadinessGate>,
    counts_toward_control_drain: bool,
    counts_toward_facade_request_drain: bool,
}

fn api_request_route_policy(method: &Method, path: &str) -> ApiRequestRoutePolicy {
    let roots_put = matches!(method, &Method::PUT) && path == "/api/fs-meta/v1/monitoring/roots";
    let rescan = matches!(method, &Method::POST) && path == "/api/fs-meta/v1/index/rescan";
    let management_readiness_gate = if roots_put {
        Some(ApiManagementReadinessGate::FullManagementWrite)
    } else if rescan {
        Some(ApiManagementReadinessGate::SourceRepair)
    } else {
        None
    };
    let management_write = management_readiness_gate.is_some();

    ApiRequestRoutePolicy {
        management_readiness_gate,
        counts_toward_control_drain: management_write,
        counts_toward_facade_request_drain: matches!(
            (method, path),
            (&Method::POST, "/api/fs-meta/v1/session/login")
                | (&Method::GET, "/api/fs-meta/v1/status")
                | (&Method::GET, "/api/fs-meta/v1/runtime/grants")
                | (&Method::GET, "/api/fs-meta/v1/monitoring/roots")
                | (&Method::PUT, "/api/fs-meta/v1/monitoring/roots")
                | (&Method::POST, "/api/fs-meta/v1/monitoring/roots/preview")
                | (&Method::POST, "/api/fs-meta/v1/index/rescan")
                | (&Method::GET, "/api/fs-meta/v1/query-api-keys")
                | (&Method::POST, "/api/fs-meta/v1/query-api-keys")
                | (&Method::GET, "/api/fs-meta/v1/tree")
                | (&Method::GET, "/api/fs-meta/v1/stats")
                | (&Method::GET, "/api/fs-meta/v1/on-demand-force-find")
                | (&Method::GET, "/api/fs-meta/v1/bound-route-metrics")
        ) || (matches!(method, &Method::DELETE)
            && path.starts_with("/api/fs-meta/v1/query-api-keys/")),
    }
}

async fn projection_request_facade_guard(
    State(control_gate): State<Arc<ApiControlGate>>,
    request: Request,
    next: Next,
) -> Response {
    let policy = api_request_route_policy(request.method(), request.uri().path());
    let facade_request_guard = if policy.counts_toward_facade_request_drain
        && policy.management_readiness_gate.is_none()
    {
        Some(control_gate.begin_facade_request())
    } else {
        None
    };
    response_with_owned_guards(next.run(request).await, None, facade_request_guard)
}

async fn request_control_readiness_guard(
    State(control_gate): State<Arc<ApiControlGate>>,
    request: Request,
    next: Next,
) -> Response {
    let policy = api_request_route_policy(request.method(), request.uri().path());
    let Some(readiness_gate) = policy.management_readiness_gate else {
        return next.run(request).await;
    };
    let facade_request_guard = policy
        .management_readiness_gate
        .is_some()
        .then(|| control_gate.begin_facade_request());
    if readiness_gate.is_ready(&control_gate) {
        return response_with_owned_guards(next.run(request).await, None, facade_request_guard);
    }
    if readiness_gate.blocked_by_management_write_drain()
        && control_gate.is_management_write_drain_closed()
    {
        return response_with_owned_guards(
            ApiError::service_unavailable(
            "fs-meta management request handling is unavailable until runtime control initializes the app",
        )
            .into_response(),
            None,
            facade_request_guard,
        );
    }
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    if let Some(recovery) = readiness_gate.recovery(&control_gate) {
        let recovery_budget = ready_deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .min(Duration::from_secs(10));
        if !recovery_budget.is_zero() {
            match tokio::time::timeout(recovery_budget, recovery()).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    eprintln!(
                        "fs_meta_api_server: management write readiness recovery failed err={err}"
                    );
                }
                Err(_) => {
                    eprintln!("fs_meta_api_server: management write readiness recovery timed out");
                }
            }
        }
    }
    let remaining = ready_deadline.saturating_duration_since(tokio::time::Instant::now());
    if !readiness_gate.is_ready(&control_gate)
        && (remaining.is_zero()
            || tokio::time::timeout(remaining, readiness_gate.wait_ready(&control_gate))
                .await
                .is_err())
    {
        return response_with_owned_guards(
            ApiError::service_unavailable(
            "fs-meta management request handling is unavailable until runtime control initializes the app",
        )
            .into_response(),
            None,
            facade_request_guard,
        );
    }
    response_with_owned_guards(next.run(request).await, None, facade_request_guard)
}

async fn request_logging(State(state): State<ApiState>, request: Request, next: Next) -> Response {
    static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

    let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let request_guard = api_request_route_policy(&method, &path)
        .counts_toward_control_drain
        .then(|| state.request_tracker.begin());
    let started_at = std::time::Instant::now();
    eprintln!(
        "fs_meta_api_server: request begin id={} method={} path={}",
        request_id, method, path
    );
    let response = next.run(request).await;
    eprintln!(
        "fs_meta_api_server: request done id={} method={} path={} status={} elapsed_ms={}",
        request_id,
        method,
        path,
        response.status().as_u16(),
        started_at.elapsed().as_millis()
    );
    response_with_owned_guards(response, request_guard, None)
}

fn response_with_owned_guards(
    response: Response,
    request_guard: Option<ApiRequestGuard>,
    facade_request_guard: Option<ApiRequestGuard>,
) -> Response {
    if request_guard.is_none() && facade_request_guard.is_none() {
        return response;
    }

    let (parts, body) = response.into_parts();
    let guarded_stream = async_stream::stream! {
        let _request_guard = request_guard;
        let _facade_request_guard = facade_request_guard;
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            yield chunk;
        }
    };

    Response::from_parts(parts, Body::from_stream(guarded_stream))
}

async fn query_api_key_guard(
    State(auth): State<Arc<AuthService>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    match auth.authorize_query_api_key(auth_header) {
        Ok(_) => next.run(request).await,
        Err(err) => err.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::convert::Infallible;
    use std::path::PathBuf;

    use axum::body::Body;
    use bytes::Bytes;
    use tempfile::TempDir;
    use tokio::sync::Notify;
    use tower::ServiceExt;

    use crate::api::config::ApiAuthConfig;
    use crate::api::facade_status::{
        shared_facade_pending_status_cell, shared_facade_service_state_cell,
    };
    use crate::api::rollout_status::{PublishedRolloutStatusReader, shared_rollout_status_cell};
    use crate::domain_state::FacadeServiceState;
    use crate::query::api::ProjectionPolicy;
    use crate::sink::SinkFileMeta;
    use crate::source::FSMetaSource;
    use crate::source::config::SourceConfig;

    #[test]
    fn api_server_policy_bootstrap_uses_typed_cached_source_helper() {
        let source = include_str!("server.rs");
        let production = source
            .split("#[cfg(test)]\nmod tests {")
            .next()
            .unwrap_or(source);

        for typed_surface in [
            ".cached_host_object_grants_snapshot_with_failure()",
            ".map_err(SourceFailure::into_error)?",
        ] {
            assert!(
                production.contains(typed_surface),
                "api/server hard cut regressed; policy bootstrap should stay on typed cached source helpers: {typed_surface}",
            );
        }

        for legacy_surface in [
            "projection_policy_from_host_object_grants(&source.cached_host_object_grants_snapshot()?);",
            "&state.source.cached_host_object_grants_snapshot()?,",
        ] {
            assert!(
                !production.contains(legacy_surface),
                "api/server hard cut regressed; policy bootstrap bounced back through raw cached source helpers: {legacy_surface}",
            );
        }
    }

    fn write_auth_files(dir: &TempDir) -> (PathBuf, PathBuf, PathBuf) {
        let passwd_path = dir.path().join("passwd");
        let shadow_path = dir.path().join("shadow");
        let query_keys_path = dir.path().join("query-api-keys.json");
        std::fs::write(
            &passwd_path,
            "admin:1000:1000:fs-meta-admin:/home/admin:/bin/sh:false\n",
        )
        .expect("write passwd");
        std::fs::write(&shadow_path, "admin:plain$secret:false\n").expect("write shadow");
        std::fs::write(&query_keys_path, "{\"keys\":[]}\n").expect("write query-api-keys");
        (passwd_path, shadow_path, query_keys_path)
    }

    fn pending_body(release: Arc<Notify>) -> Body {
        Body::from_stream(async_stream::stream! {
            release.notified().await;
            yield Ok::<Bytes, Infallible>(Bytes::from_static(b"ok"));
        })
    }

    fn test_api_state(request_tracker: Arc<ApiRequestTracker>) -> ApiState {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let (passwd_path, shadow_path, query_keys_path) = write_auth_files(&tmp);
        let auth = Arc::new(
            AuthService::new(ApiAuthConfig {
                passwd_path,
                shadow_path,
                query_keys_path,
                ..ApiAuthConfig::default()
            })
            .expect("auth"),
        );
        let node_id = NodeId("node-a".into());
        let source_cfg = SourceConfig::default();
        let source = Arc::new(SourceFacade::local(Arc::new(
            FSMetaSource::with_boundaries(source_cfg.clone(), node_id.clone(), None)
                .expect("source"),
        )));
        let sink = Arc::new(SinkFacade::local(Arc::new(
            SinkFileMeta::with_boundaries(node_id.clone(), None, source_cfg).expect("sink"),
        )));
        let facade_service_state = shared_facade_service_state_cell();
        *facade_service_state
            .write()
            .expect("write facade service state") = FacadeServiceState::Serving;
        ApiState {
            node_id,
            runtime_boundary: None,
            query_runtime_boundary: None,
            force_find_inflight: Arc::new(Mutex::new(BTreeSet::new())),
            force_find_runner_evidence: crate::api::state::ForceFindRunnerEvidence::default(),
            source,
            sink: sink.clone(),
            query_sink: sink,
            auth,
            projection_policy: Arc::new(RwLock::new(ProjectionPolicy::default())),
            published_facade_status: PublishedFacadeStatusReader::new(
                facade_service_state,
                shared_facade_pending_status_cell(),
            ),
            published_rollout_status: PublishedRolloutStatusReader::new(
                shared_rollout_status_cell(),
            ),
            request_tracker,
            control_gate: Arc::new(ApiControlGate::new(true)),
        }
    }
    #[test]
    fn request_control_drain_counts_only_management_writes() {
        for (method, path) in [
            (Method::PUT, "/api/fs-meta/v1/monitoring/roots"),
            (Method::POST, "/api/fs-meta/v1/index/rescan"),
        ] {
            assert!(
                api_request_route_policy(&method, path).counts_toward_control_drain,
                "{method} {path} should count toward global control drain"
            );
        }

        for (method, path) in [
            (Method::POST, "/api/fs-meta/v1/session/login"),
            (Method::GET, "/api/fs-meta/v1/status"),
            (Method::GET, "/api/fs-meta/v1/runtime/grants"),
            (Method::GET, "/api/fs-meta/v1/monitoring/roots"),
            (Method::POST, "/api/fs-meta/v1/monitoring/roots/preview"),
            (Method::GET, "/api/fs-meta/v1/query-api-keys"),
            (Method::GET, "/api/fs-meta/v1/tree"),
            (Method::GET, "/api/fs-meta/v1/stats"),
            (Method::GET, "/api/fs-meta/v1/on-demand-force-find"),
            (Method::GET, "/api/fs-meta/v1/bound-route-metrics"),
        ] {
            assert!(
                !api_request_route_policy(&method, path).counts_toward_control_drain,
                "{method} {path} should not count toward global control drain"
            );
        }
    }

    #[test]
    fn request_control_readiness_covers_management_writes() {
        for (method, path, expected_gate) in [
            (
                Method::PUT,
                "/api/fs-meta/v1/monitoring/roots",
                ApiManagementReadinessGate::FullManagementWrite,
            ),
            (
                Method::POST,
                "/api/fs-meta/v1/index/rescan",
                ApiManagementReadinessGate::SourceRepair,
            ),
        ] {
            assert_eq!(
                api_request_route_policy(&method, path).management_readiness_gate,
                Some(expected_gate),
                "{method} {path} should require the expected management readiness gate"
            );
        }

        for (method, path) in [
            (Method::POST, "/api/fs-meta/v1/session/login"),
            (Method::GET, "/api/fs-meta/v1/status"),
            (Method::GET, "/api/fs-meta/v1/runtime/grants"),
            (Method::POST, "/api/fs-meta/v1/monitoring/roots/preview"),
            (Method::GET, "/api/fs-meta/v1/query-api-keys"),
            (Method::POST, "/api/fs-meta/v1/query-api-keys"),
            (Method::DELETE, "/api/fs-meta/v1/query-api-keys/key-1"),
            (Method::GET, "/api/fs-meta/v1/tree"),
        ] {
            assert!(
                api_request_route_policy(&method, path)
                    .management_readiness_gate
                    .is_none(),
                "{method} {path} should not require management readiness"
            );
        }
    }
    #[test]
    fn projection_requests_count_toward_facade_request_drain() {
        for (method, path) in [
            (Method::POST, "/api/fs-meta/v1/session/login"),
            (Method::GET, "/api/fs-meta/v1/status"),
            (Method::GET, "/api/fs-meta/v1/runtime/grants"),
            (Method::GET, "/api/fs-meta/v1/monitoring/roots"),
            (Method::PUT, "/api/fs-meta/v1/monitoring/roots"),
            (Method::POST, "/api/fs-meta/v1/monitoring/roots/preview"),
            (Method::POST, "/api/fs-meta/v1/index/rescan"),
            (Method::GET, "/api/fs-meta/v1/query-api-keys"),
            (Method::POST, "/api/fs-meta/v1/query-api-keys"),
            (Method::DELETE, "/api/fs-meta/v1/query-api-keys/key-1"),
            (Method::GET, "/api/fs-meta/v1/tree"),
            (Method::GET, "/api/fs-meta/v1/stats"),
            (Method::GET, "/api/fs-meta/v1/on-demand-force-find"),
            (Method::GET, "/api/fs-meta/v1/bound-route-metrics"),
        ] {
            assert!(
                api_request_route_policy(&method, path).counts_toward_facade_request_drain,
                "{method} {path} should count toward facade request drain"
            );
        }

        for (method, path) in [(Method::OPTIONS, "/healthz")] {
            assert!(
                !api_request_route_policy(&method, path).counts_toward_facade_request_drain,
                "{method} {path} should not count toward facade request drain"
            );
        }
    }

    #[test]
    fn api_request_route_policy_assigns_roots_put_to_all_three_gates() {
        assert_eq!(
            api_request_route_policy(&Method::PUT, "/api/fs-meta/v1/monitoring/roots"),
            ApiRequestRoutePolicy {
                management_readiness_gate: Some(ApiManagementReadinessGate::FullManagementWrite),
                counts_toward_control_drain: true,
                counts_toward_facade_request_drain: true,
            }
        );
    }

    #[test]
    fn api_request_route_policy_assigns_rescan_to_source_repair_gate() {
        assert_eq!(
            api_request_route_policy(&Method::POST, "/api/fs-meta/v1/index/rescan"),
            ApiRequestRoutePolicy {
                management_readiness_gate: Some(ApiManagementReadinessGate::SourceRepair),
                counts_toward_control_drain: true,
                counts_toward_facade_request_drain: true,
            }
        );
    }

    #[test]
    fn api_request_route_policy_keeps_query_key_writes_on_facade_drain_only() {
        for (method, path) in [
            (Method::POST, "/api/fs-meta/v1/query-api-keys"),
            (Method::DELETE, "/api/fs-meta/v1/query-api-keys/key-1"),
        ] {
            assert_eq!(
                api_request_route_policy(&method, path),
                ApiRequestRoutePolicy {
                    management_readiness_gate: None,
                    counts_toward_control_drain: false,
                    counts_toward_facade_request_drain: true,
                },
                "{method} {path} updates local query-key authority and must not require runtime control write readiness"
            );
        }
    }

    #[test]
    fn api_request_route_policy_assigns_tree_to_facade_only_lane() {
        assert_eq!(
            api_request_route_policy(&Method::GET, "/api/fs-meta/v1/tree"),
            ApiRequestRoutePolicy {
                management_readiness_gate: None,
                counts_toward_control_drain: false,
                counts_toward_facade_request_drain: true,
            }
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn projection_request_facade_guard_holds_drain_until_tree_response_finishes() {
        let control_gate = Arc::new(ApiControlGate::new(true));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let app = Router::new()
            .route(
                "/api/fs-meta/v1/tree",
                get({
                    let entered = entered.clone();
                    let release = release.clone();
                    move || {
                        let entered = entered.clone();
                        let release = release.clone();
                        async move {
                            entered.notify_waiters();
                            release.notified().await;
                            "ok"
                        }
                    }
                }),
            )
            .with_state(control_gate.clone())
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                projection_request_facade_guard,
            ));

        let response_task = tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/fs-meta/v1/tree")
                    .body(Body::empty())
                    .expect("build tree request"),
            )
            .await
            .expect("route tree request")
        });

        entered.notified().await;
        assert!(
            tokio::time::timeout(
                Duration::from_millis(200),
                control_gate.wait_for_facade_request_drain(),
            )
            .await
            .is_err(),
            "projection tree request should keep facade drain blocked while response is still in flight"
        );

        release.notify_waiters();
        let response = tokio::time::timeout(Duration::from_secs(2), response_task)
            .await
            .expect("tree response should settle")
            .expect("join tree response task");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        drop(response);
        tokio::time::timeout(
            Duration::from_secs(1),
            control_gate.wait_for_facade_request_drain(),
        )
        .await
        .expect("facade drain should clear after tree response settles");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_api_key_request_facade_guard_holds_drain_until_response_finishes() {
        let control_gate = Arc::new(ApiControlGate::new(true));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let app = Router::new()
            .route(
                "/api/fs-meta/v1/query-api-keys",
                post({
                    let entered = entered.clone();
                    let release = release.clone();
                    move || {
                        let entered = entered.clone();
                        let release = release.clone();
                        async move {
                            entered.notify_waiters();
                            release.notified().await;
                            "ok"
                        }
                    }
                }),
            )
            .with_state(control_gate.clone())
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                projection_request_facade_guard,
            ));

        let response_task = tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/fs-meta/v1/query-api-keys")
                    .body(Body::empty())
                    .expect("build query-api-keys request"),
            )
            .await
            .expect("route query-api-keys request")
        });

        entered.notified().await;
        assert!(
            tokio::time::timeout(
                Duration::from_millis(200),
                control_gate.wait_for_facade_request_drain(),
            )
            .await
            .is_err(),
            "query-api-keys request should keep facade drain blocked while response is still in flight"
        );

        release.notify_waiters();
        let response = tokio::time::timeout(Duration::from_secs(2), response_task)
            .await
            .expect("query-api-keys response should settle")
            .expect("join query-api-keys response task");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        drop(response);
        tokio::time::timeout(
            Duration::from_secs(1),
            control_gate.wait_for_facade_request_drain(),
        )
        .await
        .expect("facade drain should clear after query-api-keys response settles");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn status_request_bypasses_control_readiness_guard_when_gate_is_unready() {
        let control_gate = Arc::new(ApiControlGate::new(false));
        let app = Router::new()
            .route("/api/fs-meta/v1/status", get(|| async { "ok" }))
            .with_state(control_gate.clone())
            .layer(middleware::from_fn_with_state(
                control_gate,
                request_control_readiness_guard,
            ));

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            app.oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/fs-meta/v1/status")
                    .body(Body::empty())
                    .expect("build status request"),
            ),
        )
        .await
        .expect("status request should not wait for control readiness")
        .expect("route status request");

        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn roots_put_request_facade_guard_holds_drain_while_control_readiness_waits() {
        let control_gate = Arc::new(ApiControlGate::new(false));
        let handler_entered = Arc::new(Notify::new());
        let app = Router::new()
            .route(
                "/api/fs-meta/v1/monitoring/roots",
                put({
                    let handler_entered = handler_entered.clone();
                    move || {
                        let handler_entered = handler_entered.clone();
                        async move {
                            handler_entered.notify_waiters();
                            "ok"
                        }
                    }
                }),
            )
            .with_state(control_gate.clone())
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                request_control_readiness_guard,
            ))
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                projection_request_facade_guard,
            ));

        let response_task = tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/api/fs-meta/v1/monitoring/roots")
                    .body(Body::empty())
                    .expect("build roots_put request"),
            )
            .await
            .expect("route roots_put request")
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(200), handler_entered.notified())
                .await
                .is_err(),
            "roots_put should remain blocked in control-readiness guard while the gate is unready"
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(200),
                control_gate.wait_for_facade_request_drain(),
            )
            .await
            .is_err(),
            "roots_put blocked on control readiness must still hold facade drain"
        );

        control_gate.set_ready(true);
        handler_entered.notified().await;
        let response = tokio::time::timeout(Duration::from_secs(2), response_task)
            .await
            .expect("roots_put response should settle")
            .expect("join roots_put response task");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        drop(response);
        tokio::time::timeout(
            Duration::from_secs(1),
            control_gate.wait_for_facade_request_drain(),
        )
        .await
        .expect("facade drain should clear after roots_put settles");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rescan_control_readiness_guard_runs_source_repair_recovery_before_waiting_not_ready() {
        let control_gate = Arc::new(ApiControlGate::new(false));
        let recovery_calls = Arc::new(AtomicUsize::new(0));
        control_gate.set_source_repair_recovery(Some(Arc::new({
            let control_gate = control_gate.clone();
            let recovery_calls = recovery_calls.clone();
            move || {
                let control_gate = control_gate.clone();
                let recovery_calls = recovery_calls.clone();
                Box::pin(async move {
                    recovery_calls.fetch_add(1, Ordering::SeqCst);
                    control_gate.set_ready_state_with_source_repair(true, false, true);
                    Ok(())
                })
            }
        })));
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/fs-meta/v1/index/rescan",
                post({
                    let handler_calls = handler_calls.clone();
                    move || {
                        let handler_calls = handler_calls.clone();
                        async move {
                            handler_calls.fetch_add(1, Ordering::SeqCst);
                            "ok"
                        }
                    }
                }),
            )
            .with_state(control_gate.clone())
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                request_control_readiness_guard,
            ));

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            app.oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/fs-meta/v1/index/rescan")
                    .body(Body::empty())
                    .expect("build rescan request"),
            ),
        )
        .await
        .expect("rescan should run bounded management-write recovery before timing out")
        .expect("route rescan request");

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            recovery_calls.load(Ordering::SeqCst),
            1,
            "rescan readiness guard must drive exactly one bounded source-repair recovery before allowing the handler"
        );
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            1,
            "rescan handler must run after source-repair recovery reopens the gate"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rescan_control_readiness_guard_uses_source_repair_ready_without_full_management() {
        let control_gate = Arc::new(ApiControlGate::new(false));
        control_gate.set_ready_state_with_source_repair(true, false, true);
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/fs-meta/v1/index/rescan",
                post({
                    let handler_calls = handler_calls.clone();
                    move || {
                        let handler_calls = handler_calls.clone();
                        async move {
                            handler_calls.fetch_add(1, Ordering::SeqCst);
                            "ok"
                        }
                    }
                }),
            )
            .with_state(control_gate.clone())
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                request_control_readiness_guard,
            ));

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            app.oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/fs-meta/v1/index/rescan")
                    .body(Body::empty())
                    .expect("build rescan request"),
            ),
        )
        .await
        .expect("rescan source-repair readiness must not wait for full management readiness")
        .expect("route rescan request");

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            1,
            "source-repair-ready rescan should enter handler even while sink/full management readiness is closed"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn roots_put_control_readiness_guard_requires_full_management_when_source_repair_ready() {
        let control_gate = Arc::new(ApiControlGate::new(false));
        control_gate.set_ready_state_with_source_repair(true, false, true);
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/fs-meta/v1/monitoring/roots",
                put({
                    let handler_calls = handler_calls.clone();
                    move || {
                        let handler_calls = handler_calls.clone();
                        async move {
                            handler_calls.fetch_add(1, Ordering::SeqCst);
                            "ok"
                        }
                    }
                }),
            )
            .with_state(control_gate.clone())
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                request_control_readiness_guard,
            ));

        assert!(
            tokio::time::timeout(
                Duration::from_millis(200),
                app.oneshot(
                    Request::builder()
                        .method(Method::PUT)
                        .uri("/api/fs-meta/v1/monitoring/roots")
                        .body(Body::empty())
                        .expect("build roots_put request"),
                ),
            )
            .await
            .is_err(),
            "roots_put must still wait for full management-write readiness when only source repair is ready"
        );
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            0,
            "full-management roots_put must not enter the handler on source-repair readiness alone"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn roots_put_control_readiness_guard_does_not_recover_after_full_management_drain_close()
    {
        let control_gate = Arc::new(ApiControlGate::new(true));
        let recovery_calls = Arc::new(AtomicUsize::new(0));
        control_gate.set_management_write_recovery(Some(Arc::new({
            let control_gate = control_gate.clone();
            let recovery_calls = recovery_calls.clone();
            move || {
                let control_gate = control_gate.clone();
                let recovery_calls = recovery_calls.clone();
                Box::pin(async move {
                    recovery_calls.fetch_add(1, Ordering::SeqCst);
                    control_gate.set_ready(true);
                    Ok(())
                })
            }
        })));
        control_gate.close_management_write_gate();
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/fs-meta/v1/monitoring/roots",
                put({
                    let handler_calls = handler_calls.clone();
                    move || {
                        let handler_calls = handler_calls.clone();
                        async move {
                            handler_calls.fetch_add(1, Ordering::SeqCst);
                            "ok"
                        }
                    }
                }),
            )
            .with_state(control_gate.clone())
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                request_control_readiness_guard,
            ));

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            app.oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/api/fs-meta/v1/monitoring/roots")
                    .body(Body::empty())
                    .expect("build roots_put request"),
            ),
        )
        .await
        .expect("drain-closed roots_put must fail without waiting for recovery")
        .expect("route rescan request");

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            recovery_calls.load(Ordering::SeqCst),
            0,
            "drain-closed full management writes must not run readiness recovery"
        );
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            0,
            "drain-closed full management writes must fail before handler execution"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rescan_control_readiness_guard_runs_source_repair_recovery_during_full_management_drain()
     {
        let control_gate = Arc::new(ApiControlGate::new(true));
        let recovery_calls = Arc::new(AtomicUsize::new(0));
        control_gate.set_source_repair_recovery(Some(Arc::new({
            let control_gate = control_gate.clone();
            let recovery_calls = recovery_calls.clone();
            move || {
                let control_gate = control_gate.clone();
                let recovery_calls = recovery_calls.clone();
                Box::pin(async move {
                    recovery_calls.fetch_add(1, Ordering::SeqCst);
                    control_gate.set_ready_state_with_source_repair(false, false, true);
                    Ok(())
                })
            }
        })));
        control_gate.close_management_write_gate();
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/fs-meta/v1/index/rescan",
                post({
                    let handler_calls = handler_calls.clone();
                    move || {
                        let handler_calls = handler_calls.clone();
                        async move {
                            handler_calls.fetch_add(1, Ordering::SeqCst);
                            "ok"
                        }
                    }
                }),
            )
            .with_state(control_gate.clone())
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                request_control_readiness_guard,
            ));

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            app.oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/fs-meta/v1/index/rescan")
                    .body(Body::empty())
                    .expect("build rescan request"),
            ),
        )
        .await
        .expect("drain-closed source repair must run bounded recovery")
        .expect("route rescan request");

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            recovery_calls.load(Ordering::SeqCst),
            1,
            "manual rescan must run source-repair recovery while full management drain is closed"
        );
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            1,
            "source-repair recovery should reopen the rescan gate without reopening full management writes"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rescan_control_readiness_guard_allows_source_repair_ready_during_full_management_drain()
     {
        let control_gate = Arc::new(ApiControlGate::new(true));
        control_gate.close_management_write_gate();
        control_gate.set_ready_state_with_source_repair(true, false, true);
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/fs-meta/v1/index/rescan",
                post({
                    let handler_calls = handler_calls.clone();
                    move || {
                        let handler_calls = handler_calls.clone();
                        async move {
                            handler_calls.fetch_add(1, Ordering::SeqCst);
                            "ok"
                        }
                    }
                }),
            )
            .with_state(control_gate.clone())
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                request_control_readiness_guard,
            ));

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            app.oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/fs-meta/v1/index/rescan")
                    .body(Body::empty())
                    .expect("build rescan request"),
            ),
        )
        .await
        .expect("source-repair-ready rescan must not be blocked by full-management drain")
        .expect("route rescan request");

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            1,
            "manual rescan uses the source-repair plane and must not wait for full sink/materialization management readiness"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn roots_put_request_facade_guard_holds_drain_until_response_body_drops() {
        let control_gate = Arc::new(ApiControlGate::new(true));
        let release = Arc::new(Notify::new());
        let app = Router::new()
            .route(
                "/api/fs-meta/v1/monitoring/roots",
                put({
                    let release = release.clone();
                    move || {
                        let release = release.clone();
                        async move { Response::new(pending_body(release)) }
                    }
                }),
            )
            .with_state(control_gate.clone())
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                request_control_readiness_guard,
            ))
            .layer(middleware::from_fn_with_state(
                control_gate.clone(),
                projection_request_facade_guard,
            ));

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            app.oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/api/fs-meta/v1/monitoring/roots")
                    .body(Body::empty())
                    .expect("build roots_put request"),
            ),
        )
        .await
        .expect("roots_put response headers should settle")
        .expect("route roots_put request");

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(200),
                control_gate.wait_for_facade_request_drain(),
            )
            .await
            .is_err(),
            "roots_put facade drain must remain blocked until the response body settles or drops"
        );

        drop(response);
        tokio::time::timeout(
            Duration::from_secs(1),
            control_gate.wait_for_facade_request_drain(),
        )
        .await
        .expect("facade drain should clear after the response body drops");
        release.notify_waiters();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn request_logging_holds_global_drain_until_roots_put_response_body_drops() {
        let request_tracker = Arc::new(ApiRequestTracker::default());
        let state = test_api_state(request_tracker.clone());
        let release = Arc::new(Notify::new());
        let app = Router::new()
            .route(
                "/api/fs-meta/v1/monitoring/roots",
                put({
                    let release = release.clone();
                    move || {
                        let release = release.clone();
                        async move { Response::new(pending_body(release)) }
                    }
                }),
            )
            .with_state(state.clone())
            .layer(middleware::from_fn_with_state(state, request_logging));

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            app.oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/api/fs-meta/v1/monitoring/roots")
                    .body(Body::empty())
                    .expect("build roots_put request"),
            ),
        )
        .await
        .expect("roots_put response headers should settle")
        .expect("route roots_put request");

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), request_tracker.wait_for_drain(),)
                .await
                .is_err(),
            "global request drain must remain blocked until the response body settles or drops"
        );

        drop(response);
        tokio::time::timeout(Duration::from_secs(1), request_tracker.wait_for_drain())
            .await
            .expect("global request drain should clear after the response body drops");
        release.notify_waiters();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_timeout_must_not_leave_blocking_join_task_inflight() {
        SHUTDOWN_BLOCKING_JOIN_INFLIGHT.store(0, Ordering::SeqCst);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel::<()>(1);
        let thread_exited = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let thread_exited_flag = thread_exited.clone();
        let join = std::thread::spawn(move || {
            let _ = release_rx.recv();
            thread_exited_flag.store(true, Ordering::SeqCst);
        });
        let handle = ApiServerHandle {
            shutdown: CancellationToken::new(),
            join: Some(ApiServerJoin::Thread(join)),
        };

        handle.shutdown(Duration::from_millis(30)).await;
        let returned_while_thread_alive = !thread_exited.load(Ordering::SeqCst);
        let inflight_after_shutdown_return = shutdown_blocking_join_inflight();

        let _ = release_tx.send(());
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if thread_exited.load(Ordering::SeqCst) && shutdown_blocking_join_inflight() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("join task should settle after releasing the synthetic server thread");

        assert_eq!(
            inflight_after_shutdown_return, 0,
            "shutdown returned with an inflight blocking join task; returned_while_thread_alive={returned_while_thread_alive}"
        );
    }
}
