use std::collections::BTreeSet;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderMap, Method, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
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
use crate::workers::source::SourceFacade;

use super::auth::AuthService;
use super::config::ResolvedApiConfig;
use super::errors::ApiError;
use super::facade_status::{SharedFacadePendingStatusCell, SharedFacadeServiceStateCell};
use super::handlers;
use super::state::{ApiControlGate, ApiRequestTracker, ApiState};

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

    let initial_policy =
        projection_policy_from_host_object_grants(&source.cached_host_object_grants_snapshot()?);
    eprintln!(
        "fs_meta_api_server: initial policy ready bind_addr={}",
        cfg.bind_addr
    );
    let projection_policy = Arc::new(RwLock::new(initial_policy));
    let force_find_inflight = Arc::new(Mutex::new(BTreeSet::new()));
    let state = ApiState {
        node_id,
        runtime_boundary,
        query_runtime_boundary,
        force_find_inflight: force_find_inflight.clone(),
        source,
        sink,
        query_sink,
        auth,
        projection_policy,
        facade_pending,
        facade_service_state,
        request_tracker,
        control_gate,
    };
    refresh_policy_from_host_object_grants(
        &state.projection_policy,
        &state.source.cached_host_object_grants_snapshot()?,
    );
    eprintln!(
        "fs_meta_api_server: policy refreshed bind_addr={}",
        cfg.bind_addr
    );
    let app = router(state)?;
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

fn router(state: ApiState) -> Result<Router> {
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
        .layer(middleware::from_fn_with_state(
            state.control_gate.clone(),
            projection_request_facade_guard,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            request_logging,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            request_control_readiness_guard,
        ))
        .layer(cors))
}

fn request_requires_control_readiness(method: &Method, path: &str) -> bool {
    (matches!(method, &Method::PUT) && path == "/api/fs-meta/v1/monitoring/roots")
        || (matches!(method, &Method::GET) && path == "/api/fs-meta/v1/status")
}

fn request_counts_toward_control_drain(method: &Method, path: &str) -> bool {
    matches!(
        (method, path),
        (&Method::PUT, "/api/fs-meta/v1/monitoring/roots")
            | (&Method::POST, "/api/fs-meta/v1/query-api-keys")
    ) || (matches!(method, &Method::DELETE) && path.starts_with("/api/fs-meta/v1/query-api-keys/"))
}
fn request_counts_toward_facade_request_drain(method: &Method, path: &str) -> bool {
    matches!(
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
    ) || (matches!(method, &Method::DELETE) && path.starts_with("/api/fs-meta/v1/query-api-keys/"))
}

async fn projection_request_facade_guard(
    State(control_gate): State<Arc<ApiControlGate>>,
    request: Request,
    next: Next,
) -> Response {
    let _facade_request_guard =
        request_counts_toward_facade_request_drain(request.method(), request.uri().path())
            .then(|| control_gate.begin_facade_request());
    next.run(request).await
}

async fn request_control_readiness_guard(
    State(state): State<ApiState>,
    request: Request,
    next: Next,
) -> Response {
    if !request_requires_control_readiness(request.method(), request.uri().path()) {
        return next.run(request).await;
    }
    if state.control_gate.is_ready() {
        return next.run(request).await;
    }
    if tokio::time::timeout(Duration::from_secs(15), state.control_gate.wait_ready())
        .await
        .is_err()
    {
        return ApiError::service_unavailable(
            "fs-meta management request handling is unavailable until runtime control initializes the app",
        )
        .into_response();
    }
    next.run(request).await
}

async fn request_logging(State(state): State<ApiState>, request: Request, next: Next) -> Response {
    static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

    let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let _request_guard =
        request_counts_toward_control_drain(&method, &path).then(|| state.request_tracker.begin());
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
    response
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

    use axum::body::Body;
    use tokio::sync::Notify;
    use tower::ServiceExt;
    #[test]
    fn request_control_drain_counts_only_management_writes() {
        for (method, path) in [
            (Method::PUT, "/api/fs-meta/v1/monitoring/roots"),
            (Method::POST, "/api/fs-meta/v1/query-api-keys"),
            (Method::DELETE, "/api/fs-meta/v1/query-api-keys/key-1"),
        ] {
            assert!(
                request_counts_toward_control_drain(&method, path),
                "{method} {path} should count toward global control drain"
            );
        }

        for (method, path) in [
            (Method::POST, "/api/fs-meta/v1/session/login"),
            (Method::GET, "/api/fs-meta/v1/status"),
            (Method::GET, "/api/fs-meta/v1/runtime/grants"),
            (Method::GET, "/api/fs-meta/v1/monitoring/roots"),
            (Method::POST, "/api/fs-meta/v1/monitoring/roots/preview"),
            (Method::POST, "/api/fs-meta/v1/index/rescan"),
            (Method::GET, "/api/fs-meta/v1/query-api-keys"),
            (Method::GET, "/api/fs-meta/v1/tree"),
            (Method::GET, "/api/fs-meta/v1/stats"),
            (Method::GET, "/api/fs-meta/v1/on-demand-force-find"),
            (Method::GET, "/api/fs-meta/v1/bound-route-metrics"),
        ] {
            assert!(
                !request_counts_toward_control_drain(&method, path),
                "{method} {path} should not count toward global control drain"
            );
        }
    }

    #[test]
    fn request_control_readiness_stays_limited_to_status_and_roots_put() {
        for (method, path) in [
            (Method::PUT, "/api/fs-meta/v1/monitoring/roots"),
            (Method::GET, "/api/fs-meta/v1/status"),
        ] {
            assert!(
                request_requires_control_readiness(&method, path),
                "{method} {path} should require control readiness"
            );
        }

        for (method, path) in [
            (Method::POST, "/api/fs-meta/v1/session/login"),
            (Method::GET, "/api/fs-meta/v1/runtime/grants"),
            (Method::POST, "/api/fs-meta/v1/monitoring/roots/preview"),
            (Method::POST, "/api/fs-meta/v1/index/rescan"),
            (Method::GET, "/api/fs-meta/v1/tree"),
        ] {
            assert!(
                !request_requires_control_readiness(&method, path),
                "{method} {path} should not require control readiness"
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
                request_counts_toward_facade_request_drain(&method, path),
                "{method} {path} should count toward facade request drain"
            );
        }

        for (method, path) in [(Method::OPTIONS, "/healthz")] {
            assert!(
                !request_counts_toward_facade_request_drain(&method, path),
                "{method} {path} should not count toward facade request drain"
            );
        }
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
        tokio::time::timeout(
            Duration::from_secs(1),
            control_gate.wait_for_facade_request_drain(),
        )
        .await
        .expect("facade drain should clear after query-api-keys response settles");
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
