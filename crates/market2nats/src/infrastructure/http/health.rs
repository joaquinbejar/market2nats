use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Serialize;
use tracing::{info, instrument};

use crate::application::HealthMonitor;
use crate::domain::ServiceHealth;

/// Shared state for the HTTP server.
#[derive(Clone)]
pub struct HttpState {
    /// Health monitor for reporting status.
    pub health_monitor: Arc<HealthMonitor>,
    /// Prometheus metrics handle for rendering.
    pub metrics_handle: PrometheusHandle,
}

/// Health response body.
#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    nats_connected: bool,
    venues: Vec<VenueStatus>,
}

/// Per-venue status in the health response.
#[derive(Debug, Serialize)]
struct VenueStatus {
    id: String,
    state: String,
}

/// Builds the HTTP router with health and metrics endpoints.
pub fn build_router(state: HttpState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

/// GET /health — returns service health status as JSON.
#[instrument(skip(state))]
async fn health_handler(State(state): State<HttpState>) -> impl IntoResponse {
    let monitor = &state.health_monitor;
    let overall = monitor.overall_health();
    let venues: Vec<VenueStatus> = monitor
        .venue_states_snapshot()
        .into_iter()
        .map(|(id, conn_state)| VenueStatus {
            id,
            state: conn_state.to_string(),
        })
        .collect();

    let status_code = match overall {
        ServiceHealth::Healthy | ServiceHealth::Degraded => StatusCode::OK,
        ServiceHealth::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    let body = HealthResponse {
        status: overall.to_string(),
        nats_connected: monitor.is_nats_connected(),
        venues,
    };

    (status_code, Json(body))
}

/// GET /metrics — returns Prometheus metrics.
#[instrument(skip(state))]
async fn metrics_handler(State(state): State<HttpState>) -> impl IntoResponse {
    let output = state.metrics_handle.render();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        output,
    )
}

/// Starts the HTTP health/metrics server on the given address.
///
/// # Errors
///
/// Returns an error if the server fails to bind or run.
pub async fn start_http_server(
    addr: &str,
    state: HttpState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr = %addr, "http health server started");
    axum::serve(listener, router).await?;
    Ok(())
}
