use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use metrics::{counter, gauge};
use tracing::info;

/// Prometheus metric names exposed by [`PipelineStats`].
pub const METRIC_PIPELINE_RECEIVED: &str = "market2nats_pipeline_received_total";
/// Counter: messages successfully published to NATS, labeled by venue and data_type.
pub const METRIC_PIPELINE_PUBLISHED: &str = "market2nats_pipeline_published_total";
/// Counter: NATS publish failures, labeled by venue and data_type.
pub const METRIC_PIPELINE_PUBLISH_ERRORS: &str = "market2nats_pipeline_publish_errors_total";
/// Counter: serialization failures, labeled by venue and data_type.
pub const METRIC_PIPELINE_SERIALIZE_ERRORS: &str = "market2nats_pipeline_serialize_errors_total";
/// Gauge: service uptime in seconds.
pub const METRIC_PIPELINE_UPTIME: &str = "market2nats_pipeline_uptime_seconds";

/// Key for per-(venue, data_type) counters.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StatsKey {
    venue: String,
    data_type: String,
}

/// Atomic counter set for a single stream.
struct StreamCounters {
    received: AtomicU64,
    published: AtomicU64,
    publish_errors: AtomicU64,
    serialize_errors: AtomicU64,
}

impl StreamCounters {
    fn new() -> Self {
        Self {
            received: AtomicU64::new(0),
            published: AtomicU64::new(0),
            publish_errors: AtomicU64::new(0),
            serialize_errors: AtomicU64::new(0),
        }
    }
}

/// Pipeline-wide totals aggregated across every (venue, data_type) stream.
///
/// Counts are `u128` because they sum an unbounded number of `u64` per-stream
/// counters. A `u128` cannot overflow from such a sum, so the accumulation
/// needs neither checked addition nor saturating arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PipelineTotals {
    /// Messages received from venue adapters.
    pub received: u128,
    /// Messages successfully published to NATS.
    pub published: u128,
    /// NATS publish failures.
    pub publish_errors: u128,
    /// Serialization failures.
    pub serialize_errors: u128,
    /// Number of distinct (venue, data_type) streams counted.
    pub streams: usize,
}

/// Sums per-stream counters into pipeline-wide totals.
///
/// Each item is `(received, published, publish_errors, serialize_errors)`.
#[must_use]
fn accumulate<'a, I>(entries: I) -> PipelineTotals
where
    I: IntoIterator<Item = &'a (u64, u64, u64, u64)>,
{
    let mut totals = PipelineTotals::default();
    for (received, published, pub_errors, ser_errors) in entries {
        totals.received += u128::from(*received);
        totals.published += u128::from(*published);
        totals.publish_errors += u128::from(*pub_errors);
        totals.serialize_errors += u128::from(*ser_errors);
        totals.streams += 1;
    }
    totals
}

/// Tracks message counts across the entire pipeline.
///
/// Thread-safe: uses `DashMap` + `AtomicU64` for lock-free concurrent access.
pub struct PipelineStats {
    counters: DashMap<StatsKey, StreamCounters>,
    started_at: Instant,
}

impl PipelineStats {
    /// Creates a new `PipelineStats`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
            started_at: Instant::now(),
        }
    }

    /// Records a message received from a venue adapter.
    pub fn record_received(&self, venue: &str, data_type: &str) {
        let key = StatsKey {
            venue: venue.to_owned(),
            data_type: data_type.to_owned(),
        };
        self.counters
            .entry(key)
            .or_insert_with(StreamCounters::new)
            .received
            .fetch_add(1, Ordering::Relaxed);
        counter!(
            METRIC_PIPELINE_RECEIVED,
            "venue" => venue.to_owned(),
            "data_type" => data_type.to_owned(),
        )
        .increment(1);
    }

    /// Records a message successfully published to NATS.
    pub fn record_published(&self, venue: &str, data_type: &str) {
        let key = StatsKey {
            venue: venue.to_owned(),
            data_type: data_type.to_owned(),
        };
        self.counters
            .entry(key)
            .or_insert_with(StreamCounters::new)
            .published
            .fetch_add(1, Ordering::Relaxed);
        counter!(
            METRIC_PIPELINE_PUBLISHED,
            "venue" => venue.to_owned(),
            "data_type" => data_type.to_owned(),
        )
        .increment(1);
    }

    /// Records a NATS publish failure.
    pub fn record_publish_error(&self, venue: &str, data_type: &str) {
        let key = StatsKey {
            venue: venue.to_owned(),
            data_type: data_type.to_owned(),
        };
        self.counters
            .entry(key)
            .or_insert_with(StreamCounters::new)
            .publish_errors
            .fetch_add(1, Ordering::Relaxed);
        counter!(
            METRIC_PIPELINE_PUBLISH_ERRORS,
            "venue" => venue.to_owned(),
            "data_type" => data_type.to_owned(),
        )
        .increment(1);
    }

    /// Records a serialization failure.
    pub fn record_serialize_error(&self, venue: &str, data_type: &str) {
        let key = StatsKey {
            venue: venue.to_owned(),
            data_type: data_type.to_owned(),
        };
        self.counters
            .entry(key)
            .or_insert_with(StreamCounters::new)
            .serialize_errors
            .fetch_add(1, Ordering::Relaxed);
        counter!(
            METRIC_PIPELINE_SERIALIZE_ERRORS,
            "venue" => venue.to_owned(),
            "data_type" => data_type.to_owned(),
        )
        .increment(1);
    }

    /// Returns a sorted snapshot of every stream's counters.
    ///
    /// Sorted by (venue, data_type) so logged output is deterministic.
    fn snapshot(&self) -> Vec<(StatsKey, (u64, u64, u64, u64))> {
        let mut entries: Vec<_> = self
            .counters
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let counts = (
                    entry.value().received.load(Ordering::Relaxed),
                    entry.value().published.load(Ordering::Relaxed),
                    entry.value().publish_errors.load(Ordering::Relaxed),
                    entry.value().serialize_errors.load(Ordering::Relaxed),
                );
                (key, counts)
            })
            .collect();
        entries.sort_by(|a, b| (&a.0.venue, &a.0.data_type).cmp(&(&b.0.venue, &b.0.data_type)));
        entries
    }

    /// Returns pipeline-wide totals across every stream.
    #[must_use]
    pub fn totals(&self) -> PipelineTotals {
        let entries = self.snapshot();
        accumulate(entries.iter().map(|(_, counts)| counts))
    }

    /// Logs a summary of all counters.
    pub fn log_summary(&self) {
        let uptime = self.started_at.elapsed();
        let uptime_secs = uptime.as_secs();

        #[allow(clippy::cast_precision_loss)]
        gauge!(METRIC_PIPELINE_UPTIME).set(uptime_secs as f64);

        let entries = self.snapshot();

        for (key, (received, published, pub_errors, ser_errors)) in &entries {
            // Uptime of 0 seconds: report the raw count as the rate.
            let rate = received.checked_div(uptime_secs).unwrap_or(*received);

            info!(
                venue = %key.venue,
                data_type = %key.data_type,
                received = received,
                published = published,
                publish_errors = pub_errors,
                serialize_errors = ser_errors,
                msgs_per_sec = rate,
                "stream stats"
            );
        }

        let totals = accumulate(entries.iter().map(|(_, counts)| counts));

        // Uptime of 0 seconds: report the raw count as the rate.
        let total_rate = totals
            .received
            .checked_div(u128::from(uptime_secs))
            .unwrap_or(totals.received);

        info!(
            uptime_secs = uptime_secs,
            total_received = totals.received,
            total_published = totals.published,
            total_publish_errors = totals.publish_errors,
            total_serialize_errors = totals.serialize_errors,
            total_msgs_per_sec = total_rate,
            streams = totals.streams,
            "pipeline stats summary"
        );
    }
}

impl Default for PipelineStats {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Spawns a background task that logs pipeline stats periodically.
///
/// Logs every `interval` until the shutdown signal is received.
pub fn spawn_stats_logger(
    stats: std::sync::Arc<PipelineStats>,
    interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // Skip the first immediate tick.

        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        // Final stats dump on shutdown.
                        stats.log_summary();
                        break;
                    }
                }
                _ = ticker.tick() => {
                    stats.log_summary();
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accumulate_empty_returns_zero_totals() {
        let entries: Vec<(u64, u64, u64, u64)> = Vec::new();
        let totals = accumulate(entries.iter());

        assert_eq!(totals, PipelineTotals::default());
        assert_eq!(totals.streams, 0);
    }

    #[test]
    fn test_accumulate_multiple_streams_sums_each_field() {
        let entries = [(10_u64, 9_u64, 1_u64, 0_u64), (5_u64, 5_u64, 0_u64, 2_u64)];

        let totals = accumulate(entries.iter());

        assert_eq!(totals.received, 15);
        assert_eq!(totals.published, 14);
        assert_eq!(totals.publish_errors, 1);
        assert_eq!(totals.serialize_errors, 2);
        assert_eq!(totals.streams, 2);
    }

    #[test]
    fn test_accumulate_saturated_u64_counters_does_not_overflow() {
        // Three streams each at u64::MAX would overflow a u64 accumulator.
        // Accumulating in u128 makes that structurally impossible.
        let max = u64::MAX;
        let entries = [
            (max, max, max, max),
            (max, max, max, max),
            (max, max, max, max),
        ];

        let totals = accumulate(entries.iter());

        let expected = u128::from(max) * 3;
        assert_eq!(totals.received, expected);
        assert_eq!(totals.published, expected);
        assert_eq!(totals.publish_errors, expected);
        assert_eq!(totals.serialize_errors, expected);
        assert!(totals.received > u128::from(u64::MAX));
    }

    #[test]
    fn test_totals_counts_recorded_messages_per_stream() {
        let stats = PipelineStats::new();
        stats.record_received("binance", "trade");
        stats.record_received("binance", "trade");
        stats.record_published("binance", "trade");
        stats.record_received("kraken", "ticker");
        stats.record_publish_error("kraken", "ticker");
        stats.record_serialize_error("kraken", "ticker");

        let totals = stats.totals();

        assert_eq!(totals.received, 3);
        assert_eq!(totals.published, 1);
        assert_eq!(totals.publish_errors, 1);
        assert_eq!(totals.serialize_errors, 1);
        assert_eq!(totals.streams, 2);
    }

    #[test]
    fn test_snapshot_orders_entries_by_venue_then_data_type() {
        let stats = PipelineStats::new();
        stats.record_received("okx", "trade");
        stats.record_received("binance", "ticker");
        stats.record_received("binance", "l2_orderbook");

        let entries = stats.snapshot();
        let order: Vec<(&str, &str)> = entries
            .iter()
            .map(|(key, _)| (key.venue.as_str(), key.data_type.as_str()))
            .collect();

        assert_eq!(
            order,
            [
                ("binance", "l2_orderbook"),
                ("binance", "ticker"),
                ("okx", "trade"),
            ]
        );
    }

    #[test]
    fn test_log_summary_with_zero_uptime_does_not_panic() {
        // started_at is Instant::now(), so uptime_secs is 0 here: the rate
        // computation must fall back to the raw count instead of dividing.
        let stats = PipelineStats::new();
        stats.record_received("binance", "trade");

        stats.log_summary();

        assert_eq!(stats.totals().received, 1);
    }
}
