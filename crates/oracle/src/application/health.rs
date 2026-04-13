//! Oracle health monitoring.
//!
//! Tracks the last successful computation time per symbol and derives
//! overall service health from staleness relative to the publish interval.

use std::time::{Duration, Instant};

use dashmap::DashMap;
use market2nats_domain::{CanonicalSymbol, ServiceHealth};
use serde::Serialize;

/// Health status for a single symbol.
#[derive(Debug, Clone, Serialize)]
pub struct SymbolHealth {
    /// Normalized symbol key (e.g. "btc-usdt").
    pub symbol: String,
    /// Health status for this symbol.
    pub status: ServiceHealth,
    /// Milliseconds since the last successful computation.
    pub staleness_ms: u64,
}

/// Monitors oracle computation health per symbol.
///
/// A symbol is considered healthy if its last computation was within
/// `2 * publish_interval` of the current time.
pub struct OracleHealthMonitor {
    /// Last successful computation instant per symbol (normalized key).
    last_computation: DashMap<String, Instant>,
    /// The configured publish interval; staleness threshold is 2x this value.
    publish_interval: Duration,
}

impl OracleHealthMonitor {
    /// Creates a new health monitor with the given publish interval.
    #[must_use]
    pub fn new(publish_interval: Duration) -> Self {
        Self {
            last_computation: DashMap::new(),
            publish_interval,
        }
    }

    /// Records a successful computation for the given symbol.
    pub fn record_computation(&self, symbol: &CanonicalSymbol) {
        self.last_computation
            .insert(symbol.normalized(), Instant::now());
    }

    /// Returns whether the given symbol has computed within the staleness threshold.
    #[must_use]
    pub fn is_healthy(&self, symbol: &CanonicalSymbol) -> bool {
        let threshold = self
            .publish_interval
            .checked_mul(2)
            .unwrap_or(self.publish_interval);

        self.last_computation
            .get(&symbol.normalized())
            .map(|instant| instant.elapsed() < threshold)
            .unwrap_or(false)
    }

    /// Returns per-symbol health status with staleness information.
    #[must_use]
    pub fn per_symbol_health(&self) -> Vec<SymbolHealth> {
        let threshold = self
            .publish_interval
            .checked_mul(2)
            .unwrap_or(self.publish_interval);

        self.last_computation
            .iter()
            .map(|entry| {
                let elapsed = entry.value().elapsed();
                let staleness_ms = elapsed.as_millis() as u64;
                let status = if elapsed < threshold {
                    ServiceHealth::Healthy
                } else {
                    ServiceHealth::Unhealthy
                };
                SymbolHealth {
                    symbol: entry.key().clone(),
                    status,
                    staleness_ms,
                }
            })
            .collect()
    }

    /// Returns overall service health.
    ///
    /// - `Healthy` if all tracked symbols are within threshold.
    /// - `Degraded` if at least one symbol is healthy but not all.
    /// - `Unhealthy` if no symbols have been computed or all are stale.
    #[must_use]
    pub fn overall_health(&self) -> ServiceHealth {
        if self.last_computation.is_empty() {
            return ServiceHealth::Unhealthy;
        }

        let threshold = self
            .publish_interval
            .checked_mul(2)
            .unwrap_or(self.publish_interval);

        let total = self.last_computation.len();
        let healthy_count = self
            .last_computation
            .iter()
            .filter(|entry| entry.value().elapsed() < threshold)
            .count();

        if healthy_count == total {
            ServiceHealth::Healthy
        } else if healthy_count > 0 {
            ServiceHealth::Degraded
        } else {
            ServiceHealth::Unhealthy
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn test_health_monitor_new_is_unhealthy() {
        let monitor = OracleHealthMonitor::new(Duration::from_millis(1000));
        assert_eq!(monitor.overall_health(), ServiceHealth::Unhealthy);
    }

    #[test]
    fn test_health_monitor_record_makes_healthy() {
        let monitor = OracleHealthMonitor::new(Duration::from_secs(10));
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        monitor.record_computation(&symbol);
        assert!(monitor.is_healthy(&symbol));
        assert_eq!(monitor.overall_health(), ServiceHealth::Healthy);
    }

    #[test]
    fn test_health_monitor_stale_becomes_unhealthy() {
        let monitor = OracleHealthMonitor::new(Duration::from_millis(10));
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        monitor.record_computation(&symbol);
        // Sleep well past the 2x threshold (20ms) to avoid flakiness on slow CI.
        thread::sleep(Duration::from_millis(60));
        assert!(!monitor.is_healthy(&symbol));
        assert_eq!(monitor.overall_health(), ServiceHealth::Unhealthy);
    }

    #[test]
    fn test_health_monitor_degraded_when_partial() {
        let monitor = OracleHealthMonitor::new(Duration::from_millis(10));
        let btc = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        let eth = CanonicalSymbol::try_new("ETH/USDT").unwrap();

        // Record both initially.
        monitor.record_computation(&btc);
        monitor.record_computation(&eth);

        // Sleep well past threshold to avoid flakiness on slow CI.
        thread::sleep(Duration::from_millis(60));

        // Only refresh BTC.
        monitor.record_computation(&btc);

        assert!(monitor.is_healthy(&btc));
        assert!(!monitor.is_healthy(&eth));
        assert_eq!(monitor.overall_health(), ServiceHealth::Degraded);
    }

    #[test]
    fn test_is_healthy_unknown_symbol_returns_false() {
        let monitor = OracleHealthMonitor::new(Duration::from_secs(10));
        let symbol = CanonicalSymbol::try_new("SOL/USDT").unwrap();
        assert!(!monitor.is_healthy(&symbol));
    }

    #[test]
    fn test_per_symbol_health_returns_entries() {
        let monitor = OracleHealthMonitor::new(Duration::from_secs(10));
        let btc = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        let eth = CanonicalSymbol::try_new("ETH/USDT").unwrap();
        monitor.record_computation(&btc);
        monitor.record_computation(&eth);

        let health = monitor.per_symbol_health();
        assert_eq!(health.len(), 2);
        for entry in &health {
            assert_eq!(entry.status, ServiceHealth::Healthy);
            // staleness_ms is always present; just verify it's a sane value (< 1s).
            assert!(entry.staleness_ms < 1_000);
        }
    }

    #[test]
    fn test_per_symbol_health_empty_when_no_computations() {
        let monitor = OracleHealthMonitor::new(Duration::from_secs(10));
        assert!(monitor.per_symbol_health().is_empty());
    }
}
