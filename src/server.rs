use std::{fs, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::{
    catch_panic::CatchPanicLayer, limit::RequestBodyLimitLayer, timeout::TimeoutLayer, trace::TraceLayer,
};
use tracing::{error, info};

use crate::{
    config::{Paths, load_config},
    engine::{Engine, QueryError},
    sql::SqlValidationError,
};

#[derive(Clone)]
struct AppState {
    engine: Arc<Engine>,
    admin_token: Arc<str>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub sql: String,
}

pub async fn run(paths: Paths) -> Result<()> {
    paths.ensure()?;
    let config = load_config(&paths)?;
    let engine = Arc::new(Engine::load(paths.clone())?);
    let admin_token = fs::read_to_string(&paths.admin_token).context("could not read admin token")?;
    let state = AppState {
        engine,
        admin_token: Arc::from(admin_token.trim()),
    };
    let timeout = Duration::from_secs(config.request_timeout_seconds);
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/query", post(query))
        .route("/v1/admin/reload", post(reload))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(CatchPanicLayer::new())
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            timeout,
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("could not listen on {}", config.listen))?;
    info!(listen = %config.listen, workers = config.workers, "gateway started");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("HTTP server failed")?;
    info!("gateway stopped");
    Ok(())
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let pool_stats = state.engine.stats();
    Json(json!({
        "ok": true,
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "pid": std::process::id(),
        "workers": pool_stats.workers,
        "enabled_backends": pool_stats.enabled_backends,
        "active_views": pool_stats.active_views,
        "unavailable_views": pool_stats.unavailable_views,
    }))
}

async fn query(
    State(state): State<AppState>,
    request: Result<Json<QueryRequest>, JsonRejection>,
) -> Result<Json<crate::engine::QueryResult>, ApiError> {
    let Json(request) = request.map_err(|rejection| ApiError::json_request(&rejection))?;
    let sql_bytes = request.sql.len();
    let result = state
        .engine
        .query(request.sql)
        .await
        .map_err(|error| ApiError::query(&error))?;
    info!(
        sql_bytes,
        rows = result.row_count,
        elapsed_ms = result.elapsed_ms,
        "query completed"
    );
    Ok(Json(result))
}

async fn not_found() -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "route_not_found",
        "HTTP route does not exist",
    )
}

async fn method_not_allowed() -> ApiError {
    ApiError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "HTTP method is not allowed for this route",
    )
}

async fn reload(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let supplied = headers
        .get("x-duckdoor-admin-token")
        .and_then(|value| value.to_str().ok());
    if supplied != Some(state.admin_token.as_ref()) {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid admin token",
        ));
    }
    let pool_stats = state.engine.reload().map_err(|error| ApiError::reload(&error))?;
    info!(
        workers = pool_stats.workers,
        enabled_backends = pool_stats.enabled_backends,
        active_views = pool_stats.active_views,
        unavailable_views = pool_stats.unavailable_views,
        "configuration reloaded"
    );
    Ok(Json(json!({
        "ok": true,
        "status": "reloaded",
        "workers": pool_stats.workers,
        "enabled_backends": pool_stats.enabled_backends,
        "active_views": pool_stats.active_views,
        "unavailable_views": pool_stats.unavailable_views,
    })))
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn query(error: &anyhow::Error) -> Self {
        error!(error = %error, "request failed");
        if let Some(validation) = error.downcast_ref::<SqlValidationError>() {
            let code = match validation {
                SqlValidationError::Empty
                | SqlValidationError::Parse(_)
                | SqlValidationError::StatementCount(_) => "invalid_sql",
                SqlValidationError::NotReadOnly(_) => "query_not_read_only",
                SqlValidationError::SourceFunctionNotAllowed(_) => "query_source_function_not_allowed",
                SqlValidationError::SourceRelationNotAllowed(_) => "query_source_relation_not_allowed",
            };
            return Self::new(StatusCode::BAD_REQUEST, code, validation.to_string());
        }
        if let Some(query) = error.downcast_ref::<QueryError>() {
            let (status, code) = match query {
                QueryError::WorkersBusy => (StatusCode::SERVICE_UNAVAILABLE, "query_workers_busy"),
                QueryError::WorkerStopped => (StatusCode::INTERNAL_SERVER_ERROR, "query_worker_stopped"),
                QueryError::Timeout(_) => (StatusCode::REQUEST_TIMEOUT, "query_timeout"),
                QueryError::Execution(_) => (StatusCode::BAD_REQUEST, "query_execution_failed"),
            };
            return Self::new(status, code, query.to_string());
        }
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "query failed because of an internal gateway error",
        )
    }

    fn reload(error: &anyhow::Error) -> Self {
        error!(error = %error, "reload rejected");
        Self::new(
            StatusCode::BAD_REQUEST,
            "reload_rejected",
            format!("configuration could not be activated: {error:#}"),
        )
    }

    fn json_request(rejection: &JsonRejection) -> Self {
        Self::new(rejection.status(), "invalid_request_json", rejection.body_text())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "ok": false,
                "error": {
                    "code": self.code,
                    "message": self.message,
                },
            })),
        )
            .into_response()
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { () = ctrl_c => {}, () = terminate => {} }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_errors_use_the_stable_envelope() {
        let response = ApiError::new(StatusCode::UNAUTHORIZED, "unauthorized", "bad token").into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
