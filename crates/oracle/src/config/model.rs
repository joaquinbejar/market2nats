//! Oracle configuration model.
//!
//! All structs use `#[serde(deny_unknown_fields)]` to reject unrecognized keys
//! in the TOML config file.

use serde::Deserialize;

/// Top-level oracle configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleConfig {
    /// General service settings.
    pub service: ServiceConfig,
    /// NATS connection settings.
    pub nats: NatsConfig,
    /// Subscription entries for market data feeds.
    pub subscriptions: Vec<SubscriptionEntry>,
    /// Aggregation pipeline configuration.
    pub pipeline: PipelineConfig,
    /// Output publishing configuration.
    pub publish: PublishConfig,
}

/// Service-level settings (name, logging).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// Service name for identification.
    pub name: String,
    /// Log level filter (e.g. "info", "debug", "trace").
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// Log output format: "json" or "text".
    #[serde(default = "default_log_format")]
    pub log_format: String,
    /// HTTP port for the health/metrics server.
    #[serde(default = "default_http_port")]
    pub http_port: u16,
}

/// NATS connection configuration.
///
/// Fields align with the market2nats `NatsConfig` conventions so that a single
/// `[nats]` block can be shared across services.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsConfig {
    /// One or more NATS server URLs.
    pub urls: Vec<String>,
    /// Authentication method: "none", "token", or "userpass".
    #[serde(default = "default_auth")]
    pub auth: String,
    /// Token for token-based authentication.
    pub token: Option<String>,
    /// Username for user/password authentication.
    /// Accepts both `username` and `user` keys in TOML.
    #[serde(default, alias = "user")]
    pub username: Option<String>,
    /// Password for user/password authentication.
    pub password: Option<String>,
    /// Human-readable connection name sent to the NATS server.
    #[serde(default)]
    pub connection_name: Option<String>,
    /// Connect timeout in milliseconds.
    #[serde(default)]
    pub connect_timeout_ms: Option<u64>,
    /// Whether TLS is required for the connection.
    #[serde(default)]
    pub tls_required: Option<bool>,
    /// Path to the TLS CA certificate file.
    #[serde(default)]
    pub tls_ca_file: Option<String>,
    /// Path to the TLS client certificate file.
    #[serde(default)]
    pub tls_cert_file: Option<String>,
    /// Path to the TLS client key file.
    #[serde(default)]
    pub tls_key_file: Option<String>,
}

/// A subscription entry: a symbol and NATS subjects to consume.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionEntry {
    /// Canonical symbol (e.g. "BTC/USDT").
    pub symbol: String,
    /// NATS subjects to subscribe to for this symbol.
    pub subjects: Vec<String>,
}

/// Pipeline configuration controlling filter and strategy behavior.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    /// Aggregation strategy name: "median", "twap", "vwap", "median_filtered".
    pub strategy: String,
    /// Maximum age in milliseconds before a source is considered stale.
    #[serde(default = "default_staleness_max_ms")]
    pub staleness_max_ms: u64,
    /// Maximum deviation from median in basis points before a source is discarded as an outlier.
    #[serde(default = "default_outlier_max_deviation_bps")]
    pub outlier_max_deviation_bps: u64,
    /// Minimum number of sources required after filtering.
    #[serde(default = "default_min_sources")]
    pub min_sources: usize,
    /// Time window in milliseconds for the TWAP strategy.
    #[serde(default = "default_twap_window_ms")]
    pub twap_window_ms: u64,
}

/// Publishing configuration for oracle output.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishConfig {
    /// NATS subject pattern. Must contain `<symbol_normalized>` placeholder.
    pub subject_pattern: String,
    /// Output format: "json" or "protobuf".
    #[serde(default = "default_format")]
    pub format: String,
    /// Interval in milliseconds between oracle price publications.
    #[serde(default = "default_publish_interval_ms")]
    pub publish_interval_ms: u64,
}

// --- Default functions ---

/// Default log level: "info".
#[inline]
fn default_log_level() -> String {
    "info".to_owned()
}

/// Default log format: "json".
#[inline]
fn default_log_format() -> String {
    "json".to_owned()
}

/// Default HTTP port: 9091.
#[inline]
fn default_http_port() -> u16 {
    9091
}

/// Default auth method: "none".
#[inline]
fn default_auth() -> String {
    "none".to_owned()
}

/// Default staleness threshold: 10000 milliseconds.
#[inline]
fn default_staleness_max_ms() -> u64 {
    10_000
}

/// Default outlier deviation threshold: 100 basis points.
#[inline]
fn default_outlier_max_deviation_bps() -> u64 {
    100
}

/// Default minimum sources: 3.
#[inline]
fn default_min_sources() -> usize {
    3
}

/// Default TWAP window: 30000 milliseconds.
#[inline]
fn default_twap_window_ms() -> u64 {
    30_000
}

/// Default output format: "json".
#[inline]
fn default_format() -> String {
    "json".to_owned()
}

/// Default publish interval: 1000 milliseconds.
#[inline]
fn default_publish_interval_ms() -> u64 {
    1_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_config_defaults_applied() {
        let toml_str = r#"
[service]
name = "oracle"

[nats]
urls = ["nats://localhost:4222"]

[[subscriptions]]
symbol = "BTC/USDT"
subjects = ["market.binance.btc-usdt.trade"]

[pipeline]
strategy = "median"

[publish]
subject_pattern = "oracle.<symbol_normalized>.price"
"#;
        let config: OracleConfig = toml::from_str(toml_str).expect("valid TOML");
        assert_eq!(config.service.log_level, "info");
        assert_eq!(config.service.log_format, "json");
        assert_eq!(config.nats.auth, "none");
        assert_eq!(config.pipeline.staleness_max_ms, 10_000);
        assert_eq!(config.pipeline.outlier_max_deviation_bps, 100);
        assert_eq!(config.pipeline.min_sources, 3);
        assert_eq!(config.pipeline.twap_window_ms, 30_000);
        assert_eq!(config.publish.format, "json");
        assert_eq!(config.publish.publish_interval_ms, 1_000);
        assert_eq!(config.service.http_port, 9091);
    }

    #[test]
    fn test_oracle_config_deny_unknown_fields() {
        let toml_str = r#"
[service]
name = "oracle"
unknown_field = true

[nats]
urls = ["nats://localhost:4222"]

[[subscriptions]]
symbol = "BTC/USDT"
subjects = ["market.binance.btc-usdt.trade"]

[pipeline]
strategy = "median"

[publish]
subject_pattern = "oracle.<symbol_normalized>.price"
"#;
        let result: Result<OracleConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
    }
}
