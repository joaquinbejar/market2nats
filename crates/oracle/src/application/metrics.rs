//! Oracle metrics constants and registration.
//!
//! All metric names are defined as `pub const` for use throughout the service.

use metrics::{describe_counter, describe_gauge};

/// Total number of oracle prices successfully computed and published.
pub const ORACLE_PRICE_COMPUTED: &str = "oracle_price_computed_total";

/// Total number of computation errors (insufficient sources, stale, arithmetic).
pub const ORACLE_COMPUTATION_ERRORS: &str = "oracle_computation_errors_total";

/// Number of price sources available per symbol at computation time.
pub const ORACLE_SOURCES_COUNT: &str = "oracle_sources_count";

/// Spread between minimum and maximum source price in basis points.
pub const ORACLE_PRICE_SPREAD_BPS: &str = "oracle_price_spread_bps";

/// Time taken to compute an oracle price, in milliseconds.
pub const ORACLE_COMPUTATION_LATENCY_MS: &str = "oracle_computation_latency_ms";

/// Registers all oracle metric descriptions with the global recorder.
///
/// Must be called once after installing the Prometheus recorder.
pub fn register_metrics() {
    describe_counter!(
        ORACLE_PRICE_COMPUTED,
        "Total oracle prices successfully computed"
    );
    describe_counter!(ORACLE_COMPUTATION_ERRORS, "Total oracle computation errors");
    describe_gauge!(ORACLE_SOURCES_COUNT, "Number of price sources per symbol");
    describe_gauge!(
        ORACLE_PRICE_SPREAD_BPS,
        "Spread between min and max source in basis points"
    );
    describe_gauge!(
        ORACLE_COMPUTATION_LATENCY_MS,
        "Oracle computation latency in milliseconds"
    );
}
