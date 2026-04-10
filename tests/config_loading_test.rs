//! Integration tests for configuration loading, parsing, validation, and env var substitution.
//!
//! Does NOT require NATS.

use std::io::Write;

use market2nats::config;

/// Test loading the default relay.toml config file.
#[test]
fn test_load_default_config() {
    let result = config::load_config("config/relay.toml");
    assert!(
        result.is_ok(),
        "failed to load relay.toml: {:?}",
        result.err()
    );

    let cfg = result.unwrap();
    assert_eq!(cfg.service.name, "market-data-relay");
    assert!(!cfg.venues.is_empty());
    assert!(!cfg.nats.streams.is_empty());
    assert!(!cfg.nats.consumers.is_empty());
}

/// Test that relay.toml venue subscriptions have valid data types.
#[test]
fn test_default_config_data_types_valid() {
    let cfg = config::load_config("config/relay.toml").unwrap();

    for venue in &cfg.venues {
        for sub in &venue.subscriptions {
            for dt in &sub.data_types {
                let parsed = market2nats::domain::MarketDataType::from_str_config(dt);
                assert!(
                    parsed.is_ok(),
                    "venue {} subscription {} has invalid data type: {}",
                    venue.id,
                    sub.instrument,
                    dt
                );
            }
        }
    }
}

/// Test that consumer stream references are valid.
#[test]
fn test_default_config_consumer_stream_refs() {
    let cfg = config::load_config("config/relay.toml").unwrap();
    let stream_names: Vec<&str> = cfg.nats.streams.iter().map(|s| s.name.as_str()).collect();

    for consumer in &cfg.nats.consumers {
        assert!(
            stream_names.contains(&consumer.stream.as_str()),
            "consumer {} references non-existent stream: {}",
            consumer.name,
            consumer.stream
        );
    }
}

/// Test env var substitution in config.
#[test]
fn test_config_env_var_substitution() {
    unsafe { std::env::set_var("MDR_TEST_TOKEN", "my_secret_token") };

    let toml_content = r#"
[service]
name = "test-service"
log_level = "info"
log_format = "json"
shutdown_timeout_ms = 5000

[nats]
urls = ["nats://localhost:4222"]
auth = "token"
token = "${MDR_TEST_TOKEN}"

[nats.tls]
enabled = false

[[venues]]
id = "test_venue"
adapter = "generic_ws"
enabled = true

[venues.connection]
ws_url = "wss://example.com/ws"
reconnect_delay_ms = 1000
max_reconnect_delay_ms = 60000

[venues.generic_ws]
subscribe_template = '{"subscribe":"${channel}"}'
message_format = "json"

[venues.generic_ws.channel_map]
trade = "trades"

[[venues.subscriptions]]
instrument = "BTCUSDT"
canonical_symbol = "BTC/USDT"
data_types = ["trade"]
"#;

    let tmp = tempfile("env_var_test", toml_content);
    let result = config::load_config(tmp.path().to_str().unwrap());
    assert!(result.is_ok(), "config load failed: {:?}", result.err());

    let cfg = result.unwrap();
    assert_eq!(cfg.nats.token.as_deref(), Some("my_secret_token"));

    unsafe { std::env::remove_var("MDR_TEST_TOKEN") };
}

/// Test config with missing env var leaves placeholder.
#[test]
fn test_config_missing_env_var_passthrough() {
    let toml_content = r#"
[service]
name = "test-service"
log_level = "info"
log_format = "json"

[nats]
urls = ["nats://localhost:4222"]
auth = "token"
token = "${NONEXISTENT_MDR_VAR_XYZ}"

[nats.tls]
enabled = false

[[venues]]
id = "test_venue"
adapter = "generic_ws"
enabled = true

[venues.connection]
ws_url = "wss://example.com/ws"
reconnect_delay_ms = 1000
max_reconnect_delay_ms = 60000

[venues.generic_ws]
subscribe_template = '{"sub":"${channel}"}'
message_format = "json"

[venues.generic_ws.channel_map]
trade = "trades"

[[venues.subscriptions]]
instrument = "BTCUSDT"
canonical_symbol = "BTC/USDT"
data_types = ["trade"]
"#;

    let tmp = tempfile("missing_env", toml_content);
    let cfg = config::load_config(tmp.path().to_str().unwrap()).unwrap();
    assert_eq!(
        cfg.nats.token.as_deref(),
        Some("${NONEXISTENT_MDR_VAR_XYZ}")
    );
}

/// Test config validation rejects empty venues.
#[test]
fn test_config_validation_no_venues() {
    let toml_content = r#"
[service]
name = "test"
log_level = "info"
log_format = "json"

[nats]
urls = ["nats://localhost:4222"]

[nats.tls]
enabled = false
"#;
    let tmp = tempfile("no_venues", toml_content);
    let result = config::load_config(tmp.path().to_str().unwrap());
    assert!(result.is_err());
}

/// Test config with invalid TOML syntax.
#[test]
fn test_config_invalid_toml() {
    let toml_content = r#"
[service
name = "broken"
"#;
    let tmp = tempfile("invalid_toml", toml_content);
    let result = config::load_config(tmp.path().to_str().unwrap());
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("parse"), "expected parse error, got: {err}");
}

/// Test config with nonexistent file.
#[test]
fn test_config_file_not_found() {
    let result = config::load_config("/nonexistent/path/to/config.toml");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("read") || err.contains("No such file"));
}

/// Test config with unknown fields rejected by deny_unknown_fields.
#[test]
fn test_config_unknown_fields_rejected() {
    let toml_content = r#"
[service]
name = "test"
log_level = "info"
log_format = "json"
unknown_field = "oops"

[nats]
urls = ["nats://localhost:4222"]

[nats.tls]
enabled = false

[[venues]]
id = "test"
adapter = "binance"
enabled = true

[venues.connection]
ws_url = "wss://example.com"
reconnect_delay_ms = 1000
max_reconnect_delay_ms = 60000

[[venues.subscriptions]]
instrument = "BTCUSDT"
canonical_symbol = "BTC/USDT"
data_types = ["trade"]
"#;
    let tmp = tempfile("unknown_fields", toml_content);
    let result = config::load_config(tmp.path().to_str().unwrap());
    assert!(result.is_err(), "should reject unknown fields");
}

/// Test config with all default values applied.
#[test]
fn test_config_default_values() {
    let toml_content = r#"
[service]
name = "minimal"

[nats]
urls = ["nats://localhost:4222"]

[nats.tls]
enabled = false

[[venues]]
id = "venue1"
adapter = "binance"

[venues.connection]
ws_url = "wss://example.com"

[[venues.subscriptions]]
instrument = "BTCUSDT"
canonical_symbol = "BTC/USDT"
data_types = ["trade"]
"#;
    let tmp = tempfile("defaults", toml_content);
    let cfg = config::load_config(tmp.path().to_str().unwrap()).unwrap();

    assert_eq!(cfg.service.log_level, "info");
    assert_eq!(cfg.service.log_format, "json");
    assert_eq!(cfg.service.shutdown_timeout_ms, 5000);
    assert_eq!(cfg.nats.connect_timeout_ms, 5000);
    assert_eq!(cfg.nats.ping_interval_secs, 20);
    assert_eq!(cfg.nats.auth, "none");
    assert!(cfg.venues[0].enabled);
}

/// Test config with multiple venues — mixed enabled/disabled.
#[test]
fn test_config_mixed_enabled_venues() {
    let toml_content = r#"
[service]
name = "multi"

[nats]
urls = ["nats://localhost:4222"]

[nats.tls]
enabled = false

[[venues]]
id = "enabled_venue"
adapter = "binance"
enabled = true

[venues.connection]
ws_url = "wss://example.com"

[[venues.subscriptions]]
instrument = "BTCUSDT"
canonical_symbol = "BTC/USDT"
data_types = ["trade"]

[[venues]]
id = "disabled_venue"
adapter = "binance"
enabled = false

[venues.connection]
ws_url = "wss://example2.com"

[[venues.subscriptions]]
instrument = "ETHUSDT"
canonical_symbol = "ETH/USDT"
data_types = ["ticker"]
"#;
    let tmp = tempfile("mixed_venues", toml_content);
    let cfg = config::load_config(tmp.path().to_str().unwrap()).unwrap();
    assert_eq!(cfg.venues.len(), 2);
    assert!(cfg.venues[0].enabled);
    assert!(!cfg.venues[1].enabled);
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Creates a temporary file with the given content.
fn tempfile(name: &str, content: &str) -> tempfile::NamedTempFile {
    let mut tmp = tempfile::Builder::new()
        .prefix(name)
        .suffix(".toml")
        .tempfile()
        .unwrap();
    tmp.write_all(content.as_bytes()).unwrap();
    tmp.flush().unwrap();
    tmp
}
