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

    /// Logs a summary of all counters and returns totals.
    pub fn log_summary(&self) {
        let uptime = self.started_at.elapsed();
        let uptime_secs = uptime.as_secs();

        #[allow(clippy::cast_precision_loss)]
        gauge!(METRIC_PIPELINE_UPTIME).set(uptime_secs as f64);

        let mut total_received: u64 = 0;
        let mut total_published: u64 = 0;
        let mut total_pub_errors: u64 = 0;
        let mut total_ser_errors: u64 = 0;

        // Collect and sort entries for deterministic output.
        let mut entries: Vec<_> = self
            .counters
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let received = entry.value().received.load(Ordering::Relaxed);
                let published = entry.value().published.load(Ordering::Relaxed);
                let pub_errors = entry.value().publish_errors.load(Ordering::Relaxed);
                let ser_errors = entry.value().serialize_errors.load(Ordering::Relaxed);
                (key, received, published, pub_errors, ser_errors)
            })
            .collect();
        entries.sort_by(|a, b| (&a.0.venue, &a.0.data_type).cmp(&(&b.0.venue, &b.0.data_type)));

        for (key, received, published, pub_errors, ser_errors) in &entries {
            total_received += received;
            total_published += published;
            total_pub_errors += pub_errors;
            total_ser_errors += ser_errors;

            let rate = if uptime_secs > 0 {
                received / uptime_secs
            } else {
                *received
            };

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

        let total_rate = if uptime_secs > 0 {
            total_received / uptime_secs
        } else {
            total_received
        };

        info!(
            uptime_secs = uptime_secs,
            total_received = total_received,
            total_published = total_published,
            total_publish_errors = total_pub_errors,
            total_serialize_errors = total_ser_errors,
            total_msgs_per_sec = total_rate,
            streams = entries.len(),
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
