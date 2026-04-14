//! Oracle binary entrypoint.
//!
//! Connects to NATS, subscribes to trade subjects, runs periodic oracle
//! price computation, and exposes health/metrics on HTTP. Optionally starts
//! a WebSocket server for real-time oracle price fan-out to connected clients.

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
use oracle::infrastructure::{
    FanOutPublisher, NatsTradeSubscriber, OraclePricePublisher, OracleWsServer, connect_nats,
};

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
    /// Metrics recorder installation failed.
    #[error("metrics: {0}")]
    Metrics(String),
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
        .map_err(|e| ServiceError::Metrics(format!("failed to install metrics recorder: {e}")))?;
    register_metrics();

    // Connect to NATS.
    let nats_client = connect_nats(&app_config.nats).await?;

    // Build the subscriber (implements TradeSource).
    let subscriber = Arc::new(NatsTradeSubscriber::new(&app_config.subscriptions)?);

    // Build the NATS publisher.
    let nats_publisher = Arc::new(OraclePricePublisher::new(
        nats_client.clone(),
        app_config.publish.subject_pattern.clone(),
    ));

    // Shutdown channel.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Optionally build the WebSocket server and compose with FanOutPublisher.
    let ws_server: Option<Arc<OracleWsServer>> = if app_config.websocket.enabled {
        let server = Arc::new(OracleWsServer::new());
        let ws_config = app_config.websocket.clone();
        let server_run = Arc::clone(&server);
        let ws_shutdown = shutdown_rx.clone();

        tokio::spawn(async move {
            if let Err(e) = server_run.run(&ws_config, ws_shutdown).await {
                tracing::error!(error = %e, "WebSocket server failed");
            }
        });

        info!(
            port = app_config.websocket.port,
            path = %app_config.websocket.path,
            tls = app_config.websocket.tls_enabled,
            "WebSocket server enabled"
        );
        Some(server)
    } else {
        None
    };

    // Build the composite publisher: NATS always, plus WebSocket if enabled.
    // Always use FanOutPublisher so the service has a single concrete type.
    let publisher = if let Some(ref ws) = ws_server {
        Arc::new(FanOutPublisher::new(vec![
            nats_publisher as Arc<dyn oracle::application::OraclePublisher>,
            Arc::clone(ws) as Arc<dyn oracle::application::OraclePublisher>,
        ]))
    } else {
        Arc::new(FanOutPublisher::new(vec![
            nats_publisher as Arc<dyn oracle::application::OraclePublisher>,
        ]))
    };

    // Build pipelines: one per configured subscription symbol.
    let mut pipelines = HashMap::new();
    for sub in &app_config.subscriptions {
        let symbol = CanonicalSymbol::try_new(&sub.symbol).map_err(OracleError::Domain)?;
        if pipelines.contains_key(&symbol) {
            return Err(OracleError::nats(format!(
                "duplicate subscription symbol in config: {symbol}"
            ))
            .into());
        }
        let pipeline = build_pipeline(&app_config.pipeline)?;
        pipelines.insert(symbol, pipeline);
    }

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
    let http_port = app_config.service.http_port;
    let http_handle = {
        let health = Arc::clone(&health_monitor);
        let ws_ref = ws_server.clone();
        tokio::spawn(async move {
            if let Err(e) = run_http_server(health, ws_ref, prom_handle, http_port).await {
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
        result = wait_for_sigterm() => {
            match result {
                Ok(()) => info!("received sigterm, initiating shutdown"),
                Err(e) => tracing::error!(error = %e, "failed to register sigterm handler"),
            }
        }
    }

    let _ = shutdown_tx.send(true);

    // Wait for service to drain with timeout.
    let _ = tokio::time::timeout(Duration::from_secs(5), service_handle).await;

    http_handle.abort();
    info!("oracle stopped");
    Ok(())
}

/// Runs the HTTP health and metrics server on the configured port.
async fn run_http_server(
    health: Arc<OracleHealthMonitor>,
    ws_server: Option<Arc<OracleWsServer>>,
    prom_handle: metrics_exporter_prometheus::PrometheusHandle,
    port: u16,
) -> Result<(), std::io::Error> {
    use axum::Router;
    use axum::routing::get;

    let app = Router::new()
        .route(
            "/health",
            get(move || {
                let h = health.clone();
                let ws = ws_server.clone();
                async move {
                    let status = h.overall_health();
                    let symbols = h.per_symbol_health();
                    let code = if status == ServiceHealth::Healthy {
                        axum::http::StatusCode::OK
                    } else {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    };

                    let ws_info = ws.map(|s| {
                        serde_json::json!({
                            "enabled": true,
                            "connected_clients": s.connected_clients(),
                        })
                    });

                    (
                        code,
                        axum::Json(serde_json::json!({
                            "status": status.to_string(),
                            "symbols": symbols,
                            "websocket": ws_info,
                        })),
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

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(addr = %addr, "HTTP health/metrics server started");
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
async fn wait_for_sigterm() -> Result<(), std::io::Error> {
    let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    signal.recv().await;
    Ok(())
}

/// On non-Unix platforms, this just waits forever (ctrl-c handles shutdown).
#[cfg(not(unix))]
async fn wait_for_sigterm() -> Result<(), std::io::Error> {
    std::future::pending::<()>().await;
    Ok(())
}
