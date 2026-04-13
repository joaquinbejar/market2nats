//! Oracle service: orchestrates periodic price computation and publishing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use market2nats_domain::{CanonicalSymbol, Timestamp};
use metrics::{counter, gauge, histogram};
use tokio::sync::watch;

use super::health::OracleHealthMonitor;
use super::metrics::{
    ORACLE_COMPUTATION_ERRORS, ORACLE_COMPUTATION_LATENCY_MS, ORACLE_PRICE_COMPUTED,
    ORACLE_PRICE_SPREAD_BPS, ORACLE_SOURCES_COUNT,
};
use super::ports::{OraclePublisher, TradeSource};
use crate::domain::OraclePipeline;

/// Orchestrates periodic oracle price computation across all configured symbols.
///
/// On each tick of the publish interval:
/// 1. Fetches current price sources from the `TradeSource`.
/// 2. Runs each symbol's pipeline to compute an aggregated price.
/// 3. Publishes the result via the `OraclePublisher`.
/// 4. Records metrics and health state.
pub struct OracleService<T: TradeSource, P: OraclePublisher> {
    /// One pipeline per canonical symbol.
    pipelines: HashMap<CanonicalSymbol, OraclePipeline>,
    /// Provides current price sources.
    source: Arc<T>,
    /// Publishes computed oracle prices.
    publisher: Arc<P>,
    /// Interval between computation cycles.
    publish_interval: Duration,
    /// Health monitor to record successful computations.
    health_monitor: Arc<OracleHealthMonitor>,
}

impl<T: TradeSource, P: OraclePublisher> OracleService<T, P> {
    /// Creates a new oracle service.
    ///
    /// # Arguments
    ///
    /// * `pipelines` - Map of canonical symbol to its aggregation pipeline.
    /// * `source` - Trade source providing price observations.
    /// * `publisher` - Publisher for computed oracle prices.
    /// * `publish_interval` - How often to compute and publish.
    /// * `health_monitor` - Shared health monitor instance.
    #[must_use]
    pub fn new(
        pipelines: HashMap<CanonicalSymbol, OraclePipeline>,
        source: Arc<T>,
        publisher: Arc<P>,
        publish_interval: Duration,
        health_monitor: Arc<OracleHealthMonitor>,
    ) -> Self {
        Self {
            pipelines,
            source,
            publisher,
            publish_interval,
            health_monitor,
        }
    }

    /// Runs the oracle computation loop until shutdown is signalled.
    ///
    /// Uses `tokio::select!` on an interval tick versus the shutdown watch channel.
    #[tracing::instrument(skip(self, shutdown), fields(symbols = self.pipelines.len()))]
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.publish_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The first tick fires immediately; consume it.
        interval.tick().await;

        tracing::info!(
            interval_ms = self.publish_interval.as_millis() as u64,
            symbols = self.pipelines.len(),
            "oracle service started"
        );

        loop {
            tokio::select! {
                biased;

                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("oracle service shutdown received");
                        break;
                    }
                }

                _ = interval.tick() => {
                    self.compute_cycle().await;
                }
            }
        }

        tracing::info!("oracle service stopped");
    }

    /// Performs a single computation cycle for all symbols.
    async fn compute_cycle(&self) {
        for (symbol, pipeline) in &self.pipelines {
            let sources = self.source.get_sources(symbol);
            let symbol_normalized = symbol.normalized();

            gauge!(ORACLE_SOURCES_COUNT, "symbol" => symbol_normalized.clone())
                .set(sources.len() as f64);

            let start = Instant::now();
            let result = pipeline.compute(symbol, &sources, Timestamp::now());
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

            histogram!(ORACLE_COMPUTATION_LATENCY_MS, "symbol" => symbol_normalized.clone())
                .record(elapsed_ms);

            match result {
                Ok(oracle_price) => {
                    // Record spread from the sources in the result.
                    if let Some(spread_bps) = self.compute_spread_bps_from_price(&oracle_price) {
                        gauge!(ORACLE_PRICE_SPREAD_BPS, "symbol" => symbol_normalized.clone())
                            .set(spread_bps);
                    }

                    if let Err(e) = self.publisher.publish(&oracle_price).await {
                        tracing::warn!(
                            symbol = %symbol,
                            error = %e,
                            "failed to publish oracle price"
                        );
                        counter!(ORACLE_COMPUTATION_ERRORS, "symbol" => symbol_normalized.clone())
                            .increment(1);
                    } else {
                        counter!(ORACLE_PRICE_COMPUTED, "symbol" => symbol_normalized.clone())
                            .increment(1);
                        self.health_monitor.record_computation(symbol);

                        tracing::debug!(
                            symbol = %symbol,
                            price = %oracle_price.price.value(),
                            sources = oracle_price.sources.len(),
                            confidence = %oracle_price.confidence,
                            "oracle price computed"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        symbol = %symbol,
                        error = %e,
                        sources = sources.len(),
                        "oracle computation failed"
                    );
                    counter!(ORACLE_COMPUTATION_ERRORS, "symbol" => symbol_normalized).increment(1);
                }
            }
        }
    }

    /// Extracts the spread in basis points from an oracle price's sources.
    ///
    /// Returns `None` if there are fewer than 2 sources or if any source has zero price.
    fn compute_spread_bps_from_price(
        &self,
        oracle_price: &crate::domain::OraclePrice,
    ) -> Option<f64> {
        if oracle_price.sources.len() < 2 {
            return None;
        }

        let min = oracle_price.sources.iter().map(|s| s.price.value()).min()?;

        let max = oracle_price.sources.iter().map(|s| s.price.value()).max()?;

        if min.is_zero() {
            return None;
        }

        use rust_decimal::prelude::ToPrimitive;
        let diff = max.checked_sub(min)?;
        let bps_decimal = diff
            .checked_mul(rust_decimal::Decimal::from(10_000))?
            .checked_div(min)?;

        bps_decimal.to_f64()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use market2nats_domain::{Price, Quantity, Timestamp, VenueId};
    use rust_decimal_macros::dec;

    use super::*;
    use crate::domain::{OracleError, OraclePrice, PriceSource};

    struct MockSource {
        sources: Vec<PriceSource>,
    }

    impl TradeSource for MockSource {
        fn get_sources(&self, _symbol: &CanonicalSymbol) -> Vec<PriceSource> {
            self.sources.clone()
        }
    }

    struct MockPublisher {
        publish_count: AtomicUsize,
    }

    impl MockPublisher {
        fn new() -> Self {
            Self {
                publish_count: AtomicUsize::new(0),
            }
        }
    }

    impl OraclePublisher for MockPublisher {
        fn publish(
            &self,
            _price: &OraclePrice,
        ) -> Pin<Box<dyn Future<Output = Result<(), OracleError>> + Send + '_>> {
            self.publish_count.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        }
    }

    fn make_source(venue: &str, price: rust_decimal::Decimal) -> PriceSource {
        PriceSource {
            venue: VenueId::try_new(venue).unwrap(),
            price: Price::try_new(price).unwrap(),
            quantity: Quantity::try_new(dec!(1)).unwrap(),
            timestamp: Timestamp::now(),
            age_ms: 100,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_oracle_service_runs_and_publishes() {
        use crate::config::builder::build_pipeline;
        use crate::config::model::PipelineConfig;

        let pipeline_config = PipelineConfig {
            strategy: "median".to_owned(),
            staleness_max_ms: 10_000,
            outlier_max_deviation_bps: 500,
            min_sources: 1,
            twap_window_ms: 30_000,
        };

        let pipeline = build_pipeline(&pipeline_config).unwrap();
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();

        let mut pipelines = HashMap::new();
        pipelines.insert(symbol, pipeline);

        let source = Arc::new(MockSource {
            sources: vec![
                make_source("binance", dec!(50000)),
                make_source("kraken", dec!(50010)),
            ],
        });

        let publisher = Arc::new(MockPublisher::new());
        let health = Arc::new(OracleHealthMonitor::new(Duration::from_millis(100)));

        let service = OracleService::new(
            pipelines,
            source,
            Arc::clone(&publisher),
            Duration::from_millis(50),
            health,
        );

        let (tx, rx) = watch::channel(false);

        let handle = tokio::spawn(async move {
            service.run(rx).await;
        });

        // With start_paused, sleep auto-advances the clock deterministically.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = tx.send(true);
        let _ = handle.await;

        assert!(publisher.publish_count.load(Ordering::Relaxed) >= 2);
    }

    #[tokio::test(start_paused = true)]
    async fn test_oracle_service_records_errors_on_insufficient_sources() {
        use crate::config::builder::build_pipeline;
        use crate::config::model::PipelineConfig;

        let pipeline_config = PipelineConfig {
            strategy: "median".to_owned(),
            staleness_max_ms: 10_000,
            outlier_max_deviation_bps: 500,
            min_sources: 5, // Requires 5 sources, we provide 0.
            twap_window_ms: 30_000,
        };

        let pipeline = build_pipeline(&pipeline_config).unwrap();
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();

        let mut pipelines = HashMap::new();
        pipelines.insert(symbol, pipeline);

        let source = Arc::new(MockSource { sources: vec![] });
        let publisher = Arc::new(MockPublisher::new());
        let health = Arc::new(OracleHealthMonitor::new(Duration::from_millis(100)));

        let service = OracleService::new(
            pipelines,
            source,
            Arc::clone(&publisher),
            Duration::from_millis(50),
            health,
        );

        let (tx, rx) = watch::channel(false);

        let handle = tokio::spawn(async move {
            service.run(rx).await;
        });

        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = tx.send(true);
        let _ = handle.await;

        // No publishes should have occurred.
        assert_eq!(publisher.publish_count.load(Ordering::Relaxed), 0);
    }
}
