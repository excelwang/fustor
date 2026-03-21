use std::collections::BTreeSet;
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

use capanix_app_sdk::raw::ChannelIoSubset;
use capanix_app_sdk::runtime::NodeId;
use capanix_app_sdk::{CnxError, Result};

use crate::query::api::{
    create_inprocess_router, projection_policy_from_host_object_grants,
    refresh_policy_from_host_object_grants,
};
use crate::workers::sink::SinkFacade;
use crate::workers::source::SourceFacade;

use super::auth::AuthService;
use super::config::ResolvedApiConfig;
use super::facade_status::SharedFacadePendingStatusCell;
use super::handlers;
use super::state::ApiState;

enum ApiServerJoin {
    Thread(std::thread::JoinHandle<()>),
}

pub struct ApiServerHandle {
    shutdown: CancellationToken,
    join: Option<ApiServerJoin>,
}

impl ApiServerHandle {
    pub async fn shutdown(mut self, timeout: Duration) {
        eprintln!("fs_meta_api_server: shutdown requested");
        self.shutdown.cancel();
        let Some(join) = self.join.take() else {
            return;
        };
        match join {
            ApiServerJoin::Thread(join) => {
                let blocking_join = tokio::task::spawn_blocking(move || join.join());
                match tokio::time::timeout(timeout, blocking_join).await {
                    Ok(Ok(Ok(()))) => {}
                    Ok(Ok(Err(err))) => {
                        log::warn!("fs-meta api server thread panicked: {:?}", err);
                    }
                    Ok(Err(err)) => {
                        log::warn!("fs-meta api server join wrapper failed: {:?}", err);
                    }
                    Err(_) => {
                        log::warn!("fs-meta api server shutdown timed out");
                    }
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
) -> Result<ApiServerHandle> {
    eprintln!("fs_meta_api_server: spawn begin bind_addr={}", cfg.bind_addr);
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
    eprintln!("fs_meta_api_server: auth service ready bind_addr={}", cfg.bind_addr);

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
    eprintln!("fs_meta_api_server: router ready bind_addr={}", cfg.bind_addr);

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
    let projection_router = create_inprocess_router(
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
        .with_state(state);
    Ok(management
        .nest("/api/fs-meta/v1", projection_router)
        .layer(cors))
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
