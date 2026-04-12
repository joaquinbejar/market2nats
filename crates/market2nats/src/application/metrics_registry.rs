//! Registers Prometheus metric descriptions and initial gauge values.
//!
//! Called once at startup, **after** the `metrics-exporter-prometheus`
//! recorder is installed. Describing metrics ahead of time makes them
//! appear on `/metrics` with help text even before any sample is recorded.

use metrics::{describe_counter, describe_gauge};

use crate::application::health_monitor::{METRIC_NATS_CONNECTED, METRIC_VENUE_CONNECTION_STATE};
use crate::application::pipeline_stats::{
    METRIC_PIPELINE_PUBLISH_ERRORS, METRIC_PIPELINE_PUBLISHED, METRIC_PIPELINE_RECEIVED,
    METRIC_PIPELINE_SERIALIZE_ERRORS, METRIC_PIPELINE_UPTIME,
};

/// Registers descriptions for every Prometheus metric emitted by the service.
///
/// Must be called after the Prometheus recorder is installed in `main`.
pub fn register_metrics() {
    describe_counter!(
        METRIC_PIPELINE_RECEIVED,
        "Total market data messages received from venue adapters, labeled by venue and data_type."
    );
    describe_counter!(
        METRIC_PIPELINE_PUBLISHED,
        "Total market data messages successfully published to NATS, labeled by venue and data_type."
    );
    describe_counter!(
        METRIC_PIPELINE_PUBLISH_ERRORS,
        "Total NATS publish failures, labeled by venue and data_type."
    );
    describe_counter!(
        METRIC_PIPELINE_SERIALIZE_ERRORS,
        "Total serialization failures, labeled by venue and data_type."
    );
    describe_gauge!(
        METRIC_PIPELINE_UPTIME,
        metrics::Unit::Seconds,
        "Service uptime in seconds, updated on each pipeline stats tick."
    );
    describe_gauge!(
        METRIC_VENUE_CONNECTION_STATE,
        "Venue connection state encoded as the ConnectionState discriminant \
         (0 = disconnected, 1 = connected, 2 = reconnecting, 3 = circuit_open)."
    );
    describe_gauge!(
        METRIC_NATS_CONNECTED,
        "1 if the NATS client is connected, 0 otherwise."
    );
}
