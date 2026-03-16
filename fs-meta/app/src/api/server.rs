use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderMap, Method, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use tokio::task::JoinHandle;
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

pub struct ApiServerHandle {
    shutdown: CancellationToken,
    join: JoinHandle<()>,
}

impl ApiServerHandle {
    pub async fn shutdown(self, timeout: Duration) {
        self.shutdown.cancel();
        match tokio::time::timeout(timeout, self.join).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                log::warn!("fs-meta api server join failed: {:?}", err);
            }
            Err(_) => {
                log::warn!("fs-meta api server shutdown timed out");
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
    facade_pending: SharedFacadePendingStatusCell,
) -> Result<ApiServerHandle> {
    cfg.auth
        .ensure_materialized()
        .map_err(CnxError::InvalidInput)?;
    let auth = Arc::new(AuthService::new(cfg.auth.clone()).map_err(|e| {
        CnxError::InvalidInput(format!("fs-meta api auth init failed: {}", e.message))
    })?);

    let initial_policy =
        projection_policy_from_host_object_grants(&source.host_object_grants_snapshot()?);
    let projection_policy = Arc::new(RwLock::new(initial_policy));
    let state = ApiState {
        node_id,
        runtime_boundary,
        source,
        sink,
        auth,
        projection_policy,
        facade_pending,
    };
    refresh_policy_from_host_object_grants(
        &state.projection_policy,
        &state.source.host_object_grants_snapshot()?,
    );
    let app = router(state)?;

    let listener = tokio::net::TcpListener::bind(&cfg.bind_addr)
        .await
        .map_err(|e| CnxError::InvalidInput(format!("fs-meta api bind failed: {e}")))?;

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();

    let join = tokio::spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            shutdown_signal.cancelled().await;
        });

        if let Err(err) = server.await {
            log::error!("fs-meta api server failed: {:?}", err);
        }
    });

    Ok(ApiServerHandle { shutdown, join })
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
        state.sink.clone(),
        state.source.clone(),
        state.projection_policy.clone(),
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
