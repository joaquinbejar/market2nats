use serde::Deserialize;

/// Top-level application configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    /// Service-level settings.
    pub service: ServiceConfig,
    /// NATS connection and JetStream settings.
    pub nats: NatsConfig,
    /// Venue definitions.
    pub venues: Vec<VenueConfig>,
}

/// Service-level settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// Service name for logging and identification.
    pub name: String,
    /// Log level (tracing EnvFilter compatible).
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// Log format: "json" or "pretty".
    #[serde(default = "default_log_format")]
    pub log_format: String,
    /// Graceful shutdown timeout in milliseconds.
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout_ms: u64,
}

/// NATS connection and JetStream configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsConfig {
    /// NATS server URLs.
    pub urls: Vec<String>,
    /// Connection timeout in milliseconds.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
    /// Buffer size during reconnection (bytes).
    #[serde(default = "default_reconnect_buffer")]
    pub reconnect_buffer_size: usize,
    /// Maximum reconnection attempts (-1 = unlimited).
    #[serde(default = "default_max_reconnects")]
    pub max_reconnects: i64,
    /// Ping interval in seconds.
    #[serde(default = "default_ping_interval")]
    pub ping_interval_secs: u64,
    /// Authentication method: "none", "token", "userpass", "nkey", "credentials".
    #[serde(default = "default_auth")]
    pub auth: String,
    /// Token for token-based auth (supports `${ENV_VAR}` substitution).
    pub token: Option<String>,
    /// Username for user/password auth (supports `${ENV_VAR}` substitution).
    pub username: Option<String>,
    /// Password for user/password auth (supports `${ENV_VAR}` substitution).
    pub password: Option<String>,
    /// Path to credentials file.
    pub credentials_path: Option<String>,
    /// NKey seed (supports `${ENV_VAR}` substitution).
    pub nkey_seed: Option<String>,
    /// TLS configuration.
    #[serde(default)]
    pub tls: NatsTlsConfig,
    /// JetStream stream definitions.
    #[serde(default)]
    pub streams: Vec<StreamConfig>,
    /// JetStream consumer definitions.
    #[serde(default)]
    pub consumers: Vec<ConsumerConfig>,
}

/// NATS TLS configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsTlsConfig {
    /// Whether TLS is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Path to CA certificate.
    pub ca_path: Option<String>,
    /// Path to client certificate.
    pub cert_path: Option<String>,
    /// Path to client private key.
    pub key_path: Option<String>,
}

/// JetStream stream configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamConfig {
    /// Stream name.
    pub name: String,
    /// Subject patterns this stream captures.
    pub subjects: Vec<String>,
    /// Storage type: "file" or "memory".
    #[serde(default = "default_storage")]
    pub storage: String,
    /// Retention policy: "limits", "interest", or "workqueue".
    #[serde(default = "default_retention")]
    pub retention: String,
    /// Maximum age of messages in seconds (0 = no limit).
    #[serde(default)]
    pub max_age_secs: u64,
    /// Maximum total bytes (0 = no limit).
    #[serde(default)]
    pub max_bytes: i64,
    /// Maximum number of messages (0 = no limit).
    #[serde(default)]
    pub max_msgs: i64,
    /// Maximum message size in bytes.
    #[serde(default = "default_max_msg_size")]
    pub max_msg_size: i32,
    /// Discard policy: "old" or "new".
    #[serde(default = "default_discard")]
    pub discard: String,
    /// Number of replicas.
    #[serde(default = "default_num_replicas")]
    pub num_replicas: usize,
    /// Deduplication window in seconds.
    #[serde(default = "default_duplicate_window")]
    pub duplicate_window_secs: u64,
}

/// JetStream consumer configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerConfig {
    /// Name of the stream this consumer belongs to.
    pub stream: String,
    /// Consumer name.
    pub name: String,
    /// Whether the consumer is durable.
    #[serde(default = "default_true")]
    pub durable: bool,
    /// Ack policy: "none", "all", or "explicit".
    #[serde(default = "default_ack_policy")]
    pub ack_policy: String,
    /// Ack wait timeout in seconds.
    #[serde(default = "default_ack_wait")]
    pub ack_wait_secs: u64,
    /// Maximum delivery attempts.
    #[serde(default = "default_max_deliver")]
    pub max_deliver: i64,
    /// Optional subject filter.
    pub filter_subject: Option<String>,
    /// Deliver policy: "all", "last", "new", or "by_start_time".
    #[serde(default = "default_deliver_policy")]
    pub deliver_policy: String,
    /// Start time for "by_start_time" deliver policy (RFC 3339).
    pub start_time: Option<String>,
    /// Maximum outstanding unacknowledged messages.
    #[serde(default)]
    pub max_ack_pending: i64,
    /// Inactive threshold in seconds before auto-delete (0 = never).
    #[serde(default)]
    pub inactive_threshold_secs: u64,
}

/// Configuration for a trading venue.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VenueConfig {
    /// Unique venue identifier (e.g., "binance").
    pub id: String,
    /// Adapter type: "binance", "generic_ws", etc.
    pub adapter: String,
    /// Whether this venue is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// WebSocket connection settings.
    pub connection: ConnectionConfig,
    /// Circuit breaker settings.
    #[serde(default)]
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    /// Generic WebSocket adapter configuration (only for adapter = "generic_ws").
    pub generic_ws: Option<GenericWsConfig>,
    /// Subscriptions for this venue.
    pub subscriptions: Vec<SubscriptionConfig>,
}

/// WebSocket connection settings for a venue.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    /// WebSocket URL.
    pub ws_url: String,
    /// Initial reconnect delay in milliseconds.
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_ms: u64,
    /// Maximum reconnect delay in milliseconds.
    #[serde(default = "default_max_reconnect_delay")]
    pub max_reconnect_delay_ms: u64,
    /// Maximum reconnect attempts (0 = unlimited).
    #[serde(default)]
    pub max_reconnect_attempts: u64,
    /// Ping interval in seconds.
    #[serde(default = "default_ws_ping_interval")]
    pub ping_interval_secs: u64,
    /// Pong timeout in seconds.
    #[serde(default = "default_pong_timeout")]
    pub pong_timeout_secs: u64,
}

/// Circuit breaker configuration for a venue.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    /// Consecutive failures before opening the circuit.
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    /// Seconds before trying half-open probe.
    #[serde(default = "default_reset_timeout")]
    pub reset_timeout_secs: u64,
    /// Probe requests allowed in half-open state.
    #[serde(default = "default_half_open_max")]
    pub half_open_max_requests: u32,
}

/// Configuration for the generic WebSocket adapter.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenericWsConfig {
    /// Template for subscribe messages. Supports `${channel}` and `${instrument}` placeholders.
    pub subscribe_template: String,
    /// Map from MarketDataType subject strings to venue-specific channel names.
    pub channel_map: std::collections::HashMap<String, String>,
    /// Message format: "json" or "binary".
    #[serde(default = "default_message_format")]
    pub message_format: String,
}

/// Subscription configuration for a venue instrument.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionConfig {
    /// Venue-local instrument identifier.
    pub instrument: String,
    /// Canonical symbol for NATS subject naming.
    pub canonical_symbol: String,
    /// Data types to subscribe to.
    pub data_types: Vec<String>,
}

// ── Default value functions ─────────────────────────────────────────────

#[inline]
fn default_log_level() -> String {
    "info".to_owned()
}

#[inline]
fn default_log_format() -> String {
    "json".to_owned()
}

#[inline]
fn default_shutdown_timeout() -> u64 {
    5000
}

#[inline]
fn default_connect_timeout() -> u64 {
    5000
}

#[inline]
fn default_reconnect_buffer() -> usize {
    8_388_608 // 8 MB
}

#[inline]
fn default_max_reconnects() -> i64 {
    -1
}

#[inline]
fn default_ping_interval() -> u64 {
    20
}

#[inline]
fn default_auth() -> String {
    "none".to_owned()
}

#[inline]
fn default_storage() -> String {
    "file".to_owned()
}

#[inline]
fn default_retention() -> String {
    "limits".to_owned()
}

#[inline]
fn default_max_msg_size() -> i32 {
    65_536 // 64 KB
}

#[inline]
fn default_discard() -> String {
    "old".to_owned()
}

#[inline]
fn default_num_replicas() -> usize {
    1
}

#[inline]
fn default_duplicate_window() -> u64 {
    120
}

#[inline]
fn default_true() -> bool {
    true
}

#[inline]
fn default_ack_policy() -> String {
    "explicit".to_owned()
}

#[inline]
fn default_ack_wait() -> u64 {
    30
}

#[inline]
fn default_max_deliver() -> i64 {
    5
}

#[inline]
fn default_deliver_policy() -> String {
    "all".to_owned()
}

#[inline]
fn default_reconnect_delay() -> u64 {
    1000
}

#[inline]
fn default_max_reconnect_delay() -> u64 {
    60_000
}

#[inline]
fn default_ws_ping_interval() -> u64 {
    30
}

#[inline]
fn default_pong_timeout() -> u64 {
    10
}

#[inline]
fn default_failure_threshold() -> u32 {
    5
}

#[inline]
fn default_reset_timeout() -> u64 {
    60
}

#[inline]
fn default_half_open_max() -> u32 {
    2
}

#[inline]
fn default_message_format() -> String {
    "json".to_owned()
}
