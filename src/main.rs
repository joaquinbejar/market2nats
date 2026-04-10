use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use market2nats::application::ports::NatsPublisher;
use market2nats::application::{HealthMonitor, SequenceTracker, StreamRouter, SubscriptionManager};
use market2nats::config;
use market2nats::infrastructure::http::{HttpState, start_http_server};
use market2nats::infrastructure::nats::{JetStreamPublisher, connect_nats, setup_jetstream};
use market2nats::infrastructure::ws::GenericWsAdapter;
use market2nats::serialization::{self, SerializationFormat};

/// Service error aggregating all layer errors.
#[derive(Debug, thiserror::Error)]
enum ServiceError {
    #[error("config: {0}")]
    Config(#[from] config::ConfigError),
    #[error("nats: {0}")]
    Nats(#[from] market2nats::application::NatsError),
    #[error("serialization: {0}")]
    Serialization(#[from] serialization::SerializeError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[tokio::main]
async fn main() -> Result<(), ServiceError> {
    // Load config.
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/relay.toml".to_owned());

    let app_config = config::load_config(&config_path)?;

    // Initialize tracing.
    init_tracing(
        &app_config.service.log_level,
        &app_config.service.log_format,
    );
    info!(
        service = %app_config.service.name,
        config = %config_path,
        "starting service"
    );

    // Install Prometheus metrics recorder.
    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install prometheus recorder");

    // Shared components.
    let health_monitor = Arc::new(HealthMonitor::new());
    let sequence_tracker = Arc::new(SequenceTracker::new());
    let stream_router = Arc::new(StreamRouter::new());

    // Connect to NATS.
    let nats_client = connect_nats(&app_config.nats).await?;
    let publisher = Arc::new(JetStreamPublisher::new(nats_client));
    health_monitor.set_nats_connected(true);

    // Setup JetStream streams and consumers.
    setup_jetstream(publisher.as_ref(), &app_config.nats).await?;

    // Shutdown signal.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Publisher channel — all venue tasks feed into this.
    let (event_tx, mut event_rx) = mpsc::channel(50_000);

    // Spawn venue tasks.
    let sub_manager =
        SubscriptionManager::new(Arc::clone(&health_monitor), Arc::clone(&sequence_tracker));

    let mut venue_handles = Vec::new();
    for venue_config in &app_config.venues {
        if !venue_config.enabled {
            info!(venue = %venue_config.id, "venue disabled, skipping");
            continue;
        }

        let adapter = match create_adapter(venue_config) {
            Ok(a) => a,
            Err(e) => {
                error!(venue = %venue_config.id, error = %e, "failed to create adapter");
                continue;
            }
        };

        let handle = sub_manager.spawn_venue_task(
            adapter,
            venue_config.clone(),
            event_tx.clone(),
            shutdown_rx.clone(),
        );
        venue_handles.push(handle);
        info!(venue = %venue_config.id, "venue task spawned");
    }

    // Drop our copy of the event sender so the channel closes when all venue tasks finish.
    drop(event_tx);

    // Spawn the publisher task.
    let pub_publisher = Arc::clone(&publisher);
    let pub_router = Arc::clone(&stream_router);
    let pub_shutdown = shutdown_rx.clone();
    let publisher_handle = tokio::spawn(async move {
        // Default to protobuf serialization.
        let format = SerializationFormat::Protobuf;
        let ct = serialization::content_type(format);

        while let Some(envelope) = event_rx.recv().await {
            if *pub_shutdown.borrow() {
                break;
            }

            let subject = pub_router.resolve_subject(&envelope);

            match serialization::serialize_envelope(&envelope, format) {
                Ok(payload) => {
                    if let Err(e) = pub_publisher.publish(&subject, &payload, ct).await {
                        error!(subject = %subject, error = %e, "publish failed");
                    }
                }
                Err(e) => {
                    error!(
                        venue = %envelope.venue,
                        instrument = %envelope.instrument,
                        error = %e,
                        "serialization failed"
                    );
                }
            }
        }

        info!("publisher task finished");
    });

    // Spawn HTTP health server.
    let http_state = HttpState {
        health_monitor: Arc::clone(&health_monitor),
        metrics_handle,
    };
    let http_handle = tokio::spawn(async move {
        if let Err(e) = start_http_server("0.0.0.0:8080", http_state).await {
            error!(error = %e, "http server failed");
        }
    });

    // Wait for shutdown signal (SIGTERM or SIGINT).
    let shutdown_timeout_ms = app_config.service.shutdown_timeout_ms;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received ctrl-c, initiating shutdown");
        }
        _ = wait_for_sigterm() => {
            info!("received sigterm, initiating shutdown");
        }
    }

    // Signal all tasks to stop.
    let _ = shutdown_tx.send(true);

    // Wait for venue tasks to finish with timeout.
    let drain_timeout = Duration::from_millis(shutdown_timeout_ms);
    if tokio::time::timeout(drain_timeout, async {
        for handle in venue_handles {
            let _ = handle.await;
        }
        let _ = publisher_handle.await;
    })
    .await
    .is_err()
    {
        warn!("shutdown timeout reached, some tasks may not have finished");
    }

    // Abort the HTTP server (it doesn't need graceful drain).
    http_handle.abort();

    info!("service stopped");
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

/// Creates a venue adapter from config.
fn create_adapter(
    venue_config: &market2nats::config::model::VenueConfig,
) -> Result<Box<dyn market2nats::application::VenueAdapter>, String> {
    // All adapters use the generic WebSocket adapter for now.
    // Venue-specific adapters (binance, etc.) can be added as match arms.
    match venue_config.adapter.as_str() {
        "generic_ws" => {
            let ws_config = venue_config.generic_ws.clone().ok_or_else(|| {
                "generic_ws adapter requires [venues.generic_ws] config".to_owned()
            })?;

            let adapter = GenericWsAdapter::new(
                &venue_config.id,
                venue_config.connection.clone(),
                ws_config,
                venue_config.circuit_breaker.as_ref(),
            )
            .map_err(|e| e.to_string())?;

            Ok(Box::new(adapter))
        }
        _ => {
            // Default: use generic adapter with a minimal config.
            // For venues like "binance" that don't have a dedicated adapter yet,
            // we create a generic adapter with the connection config.
            // The subscribe template will need to be provided via generic_ws config.
            if let Some(ref ws_config) = venue_config.generic_ws {
                let adapter = GenericWsAdapter::new(
                    &venue_config.id,
                    venue_config.connection.clone(),
                    ws_config.clone(),
                    venue_config.circuit_breaker.as_ref(),
                )
                .map_err(|e| e.to_string())?;

                Ok(Box::new(adapter))
            } else {
                // Create a minimal generic adapter for venues without explicit generic_ws config.
                let ws_config = market2nats::config::model::GenericWsConfig {
                    subscribe_template: None,
                    batch_subscribe_template: None,
                    stream_format: "${instrument}@${channel}".to_owned(),
                    channel_map: std::collections::HashMap::new(),
                    message_format: "json".to_owned(),
                };

                let adapter = GenericWsAdapter::new(
                    &venue_config.id,
                    venue_config.connection.clone(),
                    ws_config,
                    venue_config.circuit_breaker.as_ref(),
                )
                .map_err(|e| e.to_string())?;

                Ok(Box::new(adapter))
            }
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
