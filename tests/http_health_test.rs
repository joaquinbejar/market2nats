//! Integration tests for the HTTP health and metrics endpoints.
//!
//! These tests do NOT require NATS — they test the HTTP layer in isolation.

use std::sync::Arc;
use std::time::Duration;

use market2nats::application::HealthMonitor;
use market2nats::domain::{ConnectionState, VenueId};
use market2nats::infrastructure::http::{HttpState, start_http_server};

/// Finds a free TCP port for testing.
fn free_port() -> u16 {
    portpicker::pick_unused_port().expect("no free port available")
}

/// Helper: spawns the HTTP server and returns the base URL.
async fn spawn_server(state: HttpState) -> String {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let addr_clone = addr.clone();

    tokio::spawn(async move {
        start_http_server(&addr_clone, state).await.unwrap();
    });

    // Wait for server to be ready.
    tokio::time::sleep(Duration::from_millis(100)).await;
    format!("http://{addr}")
}

/// Test /health returns 200 with healthy status when NATS is up and all venues connected.
#[tokio::test]
async fn test_health_endpoint_healthy() {
    let monitor = Arc::new(HealthMonitor::new());
    monitor.set_nats_connected(true);
    let venue = VenueId::try_new("binance").unwrap();
    monitor.set_venue_state(&venue, ConnectionState::Connected);

    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();

    let state = HttpState {
        health_monitor: monitor,
        metrics_handle,
    };

    let base = spawn_server(state).await;
    let resp = reqwest::get(format!("{base}/health")).await.unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "healthy");
    assert_eq!(body["nats_connected"], true);
    assert!(body["venues"].is_array());

    let venues = body["venues"].as_array().unwrap();
    assert_eq!(venues.len(), 1);
    assert_eq!(venues[0]["id"], "binance");
    assert_eq!(venues[0]["state"], "connected");
}

/// Test /health returns 200 with degraded status when some venues are down.
#[tokio::test]
async fn test_health_endpoint_degraded() {
    let monitor = Arc::new(HealthMonitor::new());
    monitor.set_nats_connected(true);
    let v1 = VenueId::try_new("binance").unwrap();
    let v2 = VenueId::try_new("kraken").unwrap();
    monitor.set_venue_state(&v1, ConnectionState::Connected);
    monitor.set_venue_state(&v2, ConnectionState::Reconnecting);

    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();

    let state = HttpState {
        health_monitor: monitor,
        metrics_handle,
    };

    let base = spawn_server(state).await;
    let resp = reqwest::get(format!("{base}/health")).await.unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "degraded");
}

/// Test /health returns 503 when NATS is down.
#[tokio::test]
async fn test_health_endpoint_unhealthy() {
    let monitor = Arc::new(HealthMonitor::new());
    monitor.set_nats_connected(false);

    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();

    let state = HttpState {
        health_monitor: monitor,
        metrics_handle,
    };

    let base = spawn_server(state).await;
    let resp = reqwest::get(format!("{base}/health")).await.unwrap();

    assert_eq!(resp.status(), 503);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "unhealthy");
    assert_eq!(body["nats_connected"], false);
}

/// Test /health with no venues registered.
#[tokio::test]
async fn test_health_endpoint_no_venues() {
    let monitor = Arc::new(HealthMonitor::new());
    monitor.set_nats_connected(true);

    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();

    let state = HttpState {
        health_monitor: monitor,
        metrics_handle,
    };

    let base = spawn_server(state).await;
    let resp = reqwest::get(format!("{base}/health")).await.unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "healthy");
    let venues = body["venues"].as_array().unwrap();
    assert!(venues.is_empty());
}

/// Test /health reports all connection states correctly.
#[tokio::test]
async fn test_health_endpoint_all_connection_states() {
    let monitor = Arc::new(HealthMonitor::new());
    monitor.set_nats_connected(true);

    let states = [
        ("venue_connected", ConnectionState::Connected),
        ("venue_disconnected", ConnectionState::Disconnected),
        ("venue_reconnecting", ConnectionState::Reconnecting),
        ("venue_circuit_open", ConnectionState::CircuitOpen),
    ];

    for (name, state) in &states {
        let venue = VenueId::try_new(*name).unwrap();
        monitor.set_venue_state(&venue, *state);
    }

    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();

    let http_state = HttpState {
        health_monitor: monitor,
        metrics_handle,
    };

    let base = spawn_server(http_state).await;
    let resp = reqwest::get(format!("{base}/health")).await.unwrap();

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "degraded");

    let venues = body["venues"].as_array().unwrap();
    assert_eq!(venues.len(), 4);

    // Verify each state is reported.
    let state_strings: Vec<String> = venues
        .iter()
        .map(|v| v["state"].as_str().unwrap().to_owned())
        .collect();
    assert!(state_strings.contains(&"connected".to_owned()));
    assert!(state_strings.contains(&"disconnected".to_owned()));
    assert!(state_strings.contains(&"reconnecting".to_owned()));
    assert!(state_strings.contains(&"circuit_open".to_owned()));
}

/// Test /metrics endpoint returns Prometheus-format text.
#[tokio::test]
async fn test_metrics_endpoint() {
    let monitor = Arc::new(HealthMonitor::new());
    monitor.set_nats_connected(true);

    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();

    let state = HttpState {
        health_monitor: monitor,
        metrics_handle,
    };

    let base = spawn_server(state).await;
    let resp = reqwest::get(format!("{base}/metrics")).await.unwrap();

    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    assert!(
        content_type.contains("text/plain"),
        "expected text/plain, got: {content_type}"
    );

    // Body should be valid text (may be empty if no metrics recorded yet).
    let body = resp.text().await.unwrap();
    assert!(body.is_ascii() || body.is_empty());
}

/// Test /health response structure matches expected schema.
#[tokio::test]
async fn test_health_response_schema() {
    let monitor = Arc::new(HealthMonitor::new());
    monitor.set_nats_connected(true);
    let venue = VenueId::try_new("binance").unwrap();
    monitor.set_venue_state(&venue, ConnectionState::Connected);

    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .build_recorder()
        .handle();

    let state = HttpState {
        health_monitor: monitor,
        metrics_handle,
    };

    let base = spawn_server(state).await;
    let resp = reqwest::get(format!("{base}/health")).await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();

    // Verify all expected fields exist and have correct types.
    assert!(body["status"].is_string(), "status must be a string");
    assert!(
        body["nats_connected"].is_boolean(),
        "nats_connected must be a boolean"
    );
    assert!(body["venues"].is_array(), "venues must be an array");

    for venue in body["venues"].as_array().unwrap() {
        assert!(venue["id"].is_string(), "venue.id must be a string");
        assert!(venue["state"].is_string(), "venue.state must be a string");
    }
}
