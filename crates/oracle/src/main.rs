//! Oracle binary entrypoint.
//!
//! Connects to NATS, subscribes to trade subjects, runs periodic oracle
//! price computation, and exposes health/metrics on HTTP.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use market2nats_domain::{CanonicalSymbol, ServiceHealth};
use tokio::sync::watch;
use tracing::info;

use oracle::application::{OracleHealthMonitor, OracleService, register_metrics};
use oracle::config;
use oracle::config::builder::build_pipeline;
use oracle::domain::OracleError;
use oracle::infrastructure::{NatsTradeSubscriber, OraclePricePublisher};

/// Service error aggregating all layer errors for the oracle binary.
#[derive(Debug, thiserror::Error)]
enum ServiceError {
    /// Configuration loading or validation failed.
    #[error("config: {0}")]
    Config(#[from] config::ConfigError),
    /// Oracle domain or infrastructure error.
    #[error("oracle: {0}")]
    Oracle(#[from] OracleError),
    /// I/O error (e.g., HTTP server bind failure).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[tokio::main]
async fn main() -> Result<(), ServiceError> {
    let config_path = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("ORACLE_CONFIG").ok())
        .unwrap_or_else(|| "config/oracle.toml".to_owned());

    let app_config = config::load_config(&config_path)?;

    init_tracing(
        &app_config.service.log_level,
        &app_config.service.log_format,
    );
    info!(config_path = %config_path, "oracle starting");

    // Install Prometheus metrics recorder.
    let prom_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| OracleError::nats(format!("failed to install metrics recorder: {e}")))?;
    register_metrics();

    // Connect to NATS.
    let nats_url = app_config.nats.urls.join(",");
    let nats_client = async_nats::connect(&nats_url)
        .await
        .map_err(|e| OracleError::nats(format!("connect to {nats_url}: {e}")))?;
    info!("connected to NATS");

    // Build the subscriber (implements TradeSource).
    let subscriber = Arc::new(NatsTradeSubscriber::new(&app_config.subscriptions)?);

    // Build the publisher (implements OraclePublisher).
    let publisher = Arc::new(OraclePricePublisher::new(
        nats_client.clone(),
        app_config.publish.subject_pattern.clone(),
    ));

    // Build pipelines: one per configured subscription symbol.
    let mut pipelines = HashMap::new();
    for sub in &app_config.subscriptions {
        let symbol = CanonicalSymbol::try_new(&sub.symbol).map_err(OracleError::Domain)?;
        let pipeline = build_pipeline(&app_config.pipeline)?;
        pipelines.insert(symbol, pipeline);
    }

    // Shutdown channel.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Subscribe to NATS subjects and spawn listener tasks.
    for sub in &app_config.subscriptions {
        let symbol = CanonicalSymbol::try_new(&sub.symbol).map_err(OracleError::Domain)?;
        for subject in &sub.subjects {
            let nats_sub = subscriber
                .subscribe_to(&nats_client, subject.clone())
                .await?;
            let sub_ref = Arc::clone(&subscriber);
            let sym = symbol.clone();
            let rx = shutdown_rx.clone();
            tokio::spawn(async move {
                sub_ref.run(nats_sub, sym, rx).await;
            });
        }
    }

    // Build health monitor.
    let publish_interval = Duration::from_millis(app_config.publish.publish_interval_ms);
    let health_monitor = Arc::new(OracleHealthMonitor::new(publish_interval));

    // Build and start OracleService.
    let service = OracleService::new(
        pipelines,
        Arc::clone(&subscriber),
        Arc::clone(&publisher),
        publish_interval,
        Arc::clone(&health_monitor),
    );

    let service_handle = {
        let rx = shutdown_rx.clone();
        tokio::spawn(async move { service.run(rx).await })
    };

    // HTTP health + metrics server.
    let http_handle = {
        let health = Arc::clone(&health_monitor);
        tokio::spawn(async move {
            if let Err(e) = run_http_server(health, prom_handle).await {
                tracing::error!(error = %e, "HTTP server failed");
            }
        })
    };

    info!("oracle running, press Ctrl+C to stop");

    // Wait for shutdown signal.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received ctrl-c, initiating shutdown");
        }
        _ = wait_for_sigterm() => {
            info!("received sigterm, initiating shutdown");
        }
    }

    let _ = shutdown_tx.send(true);

    // Wait for service to drain with timeout.
    let _ = tokio::time::timeout(Duration::from_secs(5), service_handle).await;

    http_handle.abort();
    info!("oracle stopped");
    Ok(())
}

/// Runs the HTTP health and metrics server on port 9091.
async fn run_http_server(
    health: Arc<OracleHealthMonitor>,
    prom_handle: metrics_exporter_prometheus::PrometheusHandle,
) -> Result<(), std::io::Error> {
    use axum::Router;
    use axum::routing::get;

    let app = Router::new()
        .route(
            "/health",
            get(move || {
                let h = health.clone();
                async move {
                    let status = h.overall_health();
                    let code = if status == ServiceHealth::Healthy {
                        axum::http::StatusCode::OK
                    } else {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    };
                    (
                        code,
                        axum::Json(serde_json::json!({"status": status.to_string()})),
                    )
                }
            }),
        )
        .route(
            "/metrics",
            get(move || {
                let p = prom_handle.clone();
                async move { p.render() }
            }),
        );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9091").await?;
    info!(addr = "0.0.0.0:9091", "HTTP health/metrics server started");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Initializes the tracing subscriber.
fn init_tracing(log_level: &str, log_format: &str) {
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{EnvFilter, fmt};

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));

    match log_format {
        "pretty" => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().pretty())
                .init();
        }
        _ => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().json())
                .init();
        }
    }
}

/// Waits for a SIGTERM signal (Unix only).
#[cfg(unix)]
async fn wait_for_sigterm() {
    let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register sigterm handler");
    signal.recv().await;
}

/// On non-Unix platforms, this just waits forever (ctrl-c handles shutdown).
#[cfg(not(unix))]
async fn wait_for_sigterm() {
    std::future::pending::<()>().await;
}
