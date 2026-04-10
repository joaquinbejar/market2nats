use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::config::model::VenueConfig;
use crate::domain::{ConnectionState, MarketDataEnvelope, MarketDataType, Timestamp};

use super::health_monitor::HealthMonitor;
use super::ports::{Subscription, VenueAdapter, VenueError};
use super::sequence_tracker::SequenceTracker;

/// Orchestrates venue connections and feeds normalized events to the publisher pipeline.
pub struct SubscriptionManager {
    health_monitor: Arc<HealthMonitor>,
    sequence_tracker: Arc<SequenceTracker>,
}

impl SubscriptionManager {
    /// Creates a new `SubscriptionManager`.
    #[must_use]
    pub fn new(health_monitor: Arc<HealthMonitor>, sequence_tracker: Arc<SequenceTracker>) -> Self {
        Self {
            health_monitor,
            sequence_tracker,
        }
    }

    /// Spawns an async task for a venue adapter.
    ///
    /// The task connects, subscribes, and forwards normalized events through the channel.
    /// Handles reconnection with exponential backoff.
    pub fn spawn_venue_task(
        &self,
        mut adapter: Box<dyn VenueAdapter>,
        venue_config: VenueConfig,
        tx: mpsc::Sender<MarketDataEnvelope>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let health_monitor = Arc::clone(&self.health_monitor);
        let sequence_tracker = Arc::clone(&self.sequence_tracker);

        tokio::spawn(async move {
            let venue_id = adapter.venue_id().clone();
            let subscriptions = build_subscriptions(&venue_config);
            let mut backoff_ms = venue_config.connection.reconnect_delay_ms;
            let max_backoff_ms = venue_config.connection.max_reconnect_delay_ms;
            let max_attempts = venue_config.connection.max_reconnect_attempts;
            let mut attempt: u64 = 0;

            loop {
                // Check shutdown before connecting.
                if *shutdown.borrow() {
                    info!(venue = %venue_id, "shutdown signal received, stopping venue task");
                    break;
                }

                // Connect
                info!(venue = %venue_id, attempt = attempt, "connecting to venue");
                health_monitor.set_venue_state(&venue_id, ConnectionState::Reconnecting);

                match adapter.connect().await {
                    Ok(()) => {
                        info!(venue = %venue_id, "connected to venue");
                        health_monitor.set_venue_state(&venue_id, ConnectionState::Connected);
                        backoff_ms = venue_config.connection.reconnect_delay_ms;
                        attempt = 0;
                    }
                    Err(e) => {
                        error!(venue = %venue_id, error = %e, "connection failed");
                        health_monitor.set_venue_state(&venue_id, ConnectionState::Disconnected);
                        if should_stop(max_attempts, attempt) {
                            error!(venue = %venue_id, "max reconnect attempts reached");
                            break;
                        }
                        backoff_ms =
                            wait_with_backoff(backoff_ms, max_backoff_ms, &mut shutdown).await;
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                }

                // Subscribe
                if let Err(e) = adapter.subscribe(&subscriptions).await {
                    error!(venue = %venue_id, error = %e, "subscribe failed");
                    let _ = adapter.disconnect().await;
                    backoff_ms = wait_with_backoff(backoff_ms, max_backoff_ms, &mut shutdown).await;
                    attempt = attempt.saturating_add(1);
                    continue;
                }

                // Read loop
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                info!(venue = %venue_id, "shutdown during read loop");
                                let _ = adapter.disconnect().await;
                                return;
                            }
                        }
                        result = adapter.next_events() => {
                            match result {
                                Ok(mut events) => {
                                    for event in &mut events {
                                        event.received_at = Timestamp::now();
                                        match sequence_tracker.next_sequence(
                                            &event.venue,
                                            &event.instrument,
                                            event.data_type,
                                        ) {
                                            Ok(seq) => event.sequence = seq,
                                            Err(e) => {
                                                error!(
                                                    venue = %venue_id,
                                                    error = %e,
                                                    "sequence assignment failed"
                                                );
                                                continue;
                                            }
                                        }
                                    }

                                    for event in events {
                                        if tx.send(event).await.is_err() {
                                            warn!(venue = %venue_id, "publisher channel closed");
                                            let _ = adapter.disconnect().await;
                                            return;
                                        }
                                    }
                                }
                                Err(VenueError::CircuitBreakerOpen { .. }) => {
                                    warn!(venue = %venue_id, "circuit breaker open");
                                    health_monitor.set_venue_state(
                                        &venue_id,
                                        ConnectionState::CircuitOpen,
                                    );
                                    let _ = adapter.disconnect().await;
                                    break;
                                }
                                Err(e) => {
                                    error!(venue = %venue_id, error = %e, "receive failed");
                                    health_monitor.set_venue_state(
                                        &venue_id,
                                        ConnectionState::Disconnected,
                                    );
                                    let _ = adapter.disconnect().await;
                                    break;
                                }
                            }
                        }
                    }
                }

                // Back to reconnect loop
                if *shutdown.borrow() {
                    break;
                }
                attempt = attempt.saturating_add(1);
                if should_stop(max_attempts, attempt) {
                    error!(venue = %venue_id, "max reconnect attempts reached");
                    break;
                }
                backoff_ms = wait_with_backoff(backoff_ms, max_backoff_ms, &mut shutdown).await;
            }
        })
    }
}

/// Builds subscription requests from venue config.
#[must_use]
fn build_subscriptions(config: &VenueConfig) -> Vec<Subscription> {
    config
        .subscriptions
        .iter()
        .map(|sub| {
            let data_types = sub
                .data_types
                .iter()
                .filter_map(|dt| MarketDataType::from_str_config(dt).ok())
                .collect();
            Subscription {
                instrument: sub.instrument.clone(),
                canonical_symbol: sub.canonical_symbol.clone(),
                data_types,
            }
        })
        .collect()
}

/// Returns whether we should stop reconnecting.
#[must_use]
#[inline]
fn should_stop(max_attempts: u64, current: u64) -> bool {
    max_attempts > 0 && current >= max_attempts
}

/// Waits with exponential backoff, returning the next backoff value.
/// Returns early if shutdown is signaled.
async fn wait_with_backoff(
    current_ms: u64,
    max_ms: u64,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> u64 {
    let delay = Duration::from_millis(current_ms);
    tokio::select! {
        biased;
        _ = shutdown.changed() => {}
        _ = tokio::time::sleep(delay) => {}
    }
    // Double backoff, capped at max
    std::cmp::min(current_ms.saturating_mul(2), max_ms)
}
