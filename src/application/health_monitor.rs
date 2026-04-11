use dashmap::DashMap;
use metrics::gauge;

use crate::domain::{ConnectionState, ServiceHealth, VenueId};

/// Gauge: per-venue connection state encoded as the `ConnectionState` discriminant.
pub const METRIC_VENUE_CONNECTION_STATE: &str = "market2nats_venue_connection_state";
/// Gauge: 1 if the NATS client is connected, 0 otherwise.
pub const METRIC_NATS_CONNECTED: &str = "market2nats_nats_connected";

/// Tracks connection states per venue and computes overall service health.
pub struct HealthMonitor {
    venue_states: DashMap<String, ConnectionState>,
    nats_connected: std::sync::atomic::AtomicBool,
}

impl HealthMonitor {
    /// Creates a new `HealthMonitor`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            venue_states: DashMap::new(),
            nats_connected: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Updates the connection state for a venue.
    pub fn set_venue_state(&self, venue: &VenueId, state: ConnectionState) {
        self.venue_states.insert(venue.as_str().to_owned(), state);
        gauge!(
            METRIC_VENUE_CONNECTION_STATE,
            "venue" => venue.as_str().to_owned(),
        )
        .set(f64::from(state as u8));
    }

    /// Updates the NATS connection status.
    pub fn set_nats_connected(&self, connected: bool) {
        self.nats_connected
            .store(connected, std::sync::atomic::Ordering::Relaxed);
        gauge!(METRIC_NATS_CONNECTED).set(if connected { 1.0 } else { 0.0 });
    }

    /// Returns the connection state for a specific venue.
    #[must_use]
    pub fn venue_state(&self, venue: &VenueId) -> ConnectionState {
        self.venue_states
            .get(venue.as_str())
            .map(|v| *v.value())
            .unwrap_or(ConnectionState::Disconnected)
    }

    /// Returns whether NATS is connected.
    #[must_use]
    pub fn is_nats_connected(&self) -> bool {
        self.nats_connected
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Computes the overall service health.
    ///
    /// - **Healthy**: all venues connected and NATS up.
    /// - **Degraded**: some venues disconnected but NATS up.
    /// - **Unhealthy**: NATS is down.
    #[must_use]
    pub fn overall_health(&self) -> ServiceHealth {
        if !self.is_nats_connected() {
            return ServiceHealth::Unhealthy;
        }

        let all_connected = self
            .venue_states
            .iter()
            .all(|entry| *entry.value() == ConnectionState::Connected);

        if all_connected {
            ServiceHealth::Healthy
        } else {
            ServiceHealth::Degraded
        }
    }

    /// Returns a snapshot of all venue states for health reporting.
    #[must_use]
    pub fn venue_states_snapshot(&self) -> Vec<(String, ConnectionState)> {
        self.venue_states
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }
}

impl Default for HealthMonitor {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_monitor_initial_state() {
        let monitor = HealthMonitor::new();
        assert!(!monitor.is_nats_connected());
        assert_eq!(monitor.overall_health(), ServiceHealth::Unhealthy);
    }

    #[test]
    fn test_health_monitor_healthy_state() {
        let monitor = HealthMonitor::new();
        let venue = VenueId::try_new("binance").unwrap();

        monitor.set_nats_connected(true);
        monitor.set_venue_state(&venue, ConnectionState::Connected);

        assert_eq!(monitor.overall_health(), ServiceHealth::Healthy);
    }

    #[test]
    fn test_health_monitor_degraded_state() {
        let monitor = HealthMonitor::new();
        let v1 = VenueId::try_new("binance").unwrap();
        let v2 = VenueId::try_new("kraken").unwrap();

        monitor.set_nats_connected(true);
        monitor.set_venue_state(&v1, ConnectionState::Connected);
        monitor.set_venue_state(&v2, ConnectionState::Reconnecting);

        assert_eq!(monitor.overall_health(), ServiceHealth::Degraded);
    }

    #[test]
    fn test_health_monitor_unhealthy_when_nats_down() {
        let monitor = HealthMonitor::new();
        let venue = VenueId::try_new("binance").unwrap();

        monitor.set_nats_connected(false);
        monitor.set_venue_state(&venue, ConnectionState::Connected);

        assert_eq!(monitor.overall_health(), ServiceHealth::Unhealthy);
    }

    #[test]
    fn test_venue_state_default_disconnected() {
        let monitor = HealthMonitor::new();
        let venue = VenueId::try_new("unknown").unwrap();
        assert_eq!(monitor.venue_state(&venue), ConnectionState::Disconnected);
    }
}
