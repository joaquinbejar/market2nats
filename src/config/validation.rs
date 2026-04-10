use crate::domain::MarketDataType;

use super::model::AppConfig;

/// Configuration validation errors.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ConfigValidationError {
    #[error("{0}")]
    Rule(String),
}

/// Validates the parsed configuration.
///
/// # Errors
///
/// Returns a list of all validation errors found.
#[must_use]
pub fn validate_config(config: &AppConfig) -> Vec<ConfigValidationError> {
    let mut errors = Vec::new();

    validate_service(&config.service, &mut errors);
    validate_nats(&config.nats, &mut errors);
    validate_venues(&config.venues, &mut errors);
    validate_consumer_stream_refs(&config.nats, &mut errors);

    errors
}

fn validate_service(
    service: &super::model::ServiceConfig,
    errors: &mut Vec<ConfigValidationError>,
) {
    if service.name.trim().is_empty() {
        errors.push(ConfigValidationError::Rule(
            "service.name must not be empty".to_owned(),
        ));
    }

    if !["json", "pretty"].contains(&service.log_format.as_str()) {
        errors.push(ConfigValidationError::Rule(format!(
            "service.log_format must be \"json\" or \"pretty\", got \"{}\"",
            service.log_format
        )));
    }
}

fn validate_nats(nats: &super::model::NatsConfig, errors: &mut Vec<ConfigValidationError>) {
    if nats.urls.is_empty() {
        errors.push(ConfigValidationError::Rule(
            "nats.urls must contain at least one URL".to_owned(),
        ));
    }

    for (i, url) in nats.urls.iter().enumerate() {
        if !url.starts_with("nats://") && !url.starts_with("tls://") {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.urls[{i}]: invalid scheme, expected nats:// or tls://, got \"{url}\""
            )));
        }
    }

    for (i, stream) in nats.streams.iter().enumerate() {
        if stream.name.trim().is_empty() {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.streams[{i}].name must not be empty"
            )));
        }
        if stream.subjects.is_empty() {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.streams[{i}].subjects must not be empty"
            )));
        }
        if !["file", "memory"].contains(&stream.storage.as_str()) {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.streams[{i}].storage must be \"file\" or \"memory\""
            )));
        }
        if !["limits", "interest", "workqueue"].contains(&stream.retention.as_str()) {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.streams[{i}].retention must be \"limits\", \"interest\", or \"workqueue\""
            )));
        }
        if !["old", "new"].contains(&stream.discard.as_str()) {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.streams[{i}].discard must be \"old\" or \"new\""
            )));
        }
        if stream.num_replicas == 0 {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.streams[{i}].num_replicas must be >= 1"
            )));
        }
    }

    for (i, consumer) in nats.consumers.iter().enumerate() {
        if consumer.name.trim().is_empty() {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.consumers[{i}].name must not be empty"
            )));
        }
        if !["none", "all", "explicit"].contains(&consumer.ack_policy.as_str()) {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.consumers[{i}].ack_policy must be \"none\", \"all\", or \"explicit\""
            )));
        }
        if !["all", "last", "new", "by_start_time"].contains(&consumer.deliver_policy.as_str()) {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.consumers[{i}].deliver_policy must be \"all\", \"last\", \"new\", or \"by_start_time\""
            )));
        }
        if consumer.deliver_policy == "by_start_time" && consumer.start_time.is_none() {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.consumers[{i}].start_time is required when deliver_policy = \"by_start_time\""
            )));
        }
    }
}

fn validate_venues(venues: &[super::model::VenueConfig], errors: &mut Vec<ConfigValidationError>) {
    let enabled_count = venues.iter().filter(|v| v.enabled).count();
    if enabled_count == 0 {
        errors.push(ConfigValidationError::Rule(
            "at least one venue must be enabled".to_owned(),
        ));
    }

    for (i, venue) in venues.iter().enumerate() {
        if venue.id.trim().is_empty() {
            errors.push(ConfigValidationError::Rule(format!(
                "venues[{i}].id must not be empty"
            )));
        }

        if venue.enabled && venue.subscriptions.is_empty() {
            errors.push(ConfigValidationError::Rule(format!(
                "venues[{i}] ({}) has no subscriptions",
                venue.id
            )));
        }

        if venue.connection.ws_url.trim().is_empty() {
            errors.push(ConfigValidationError::Rule(format!(
                "venues[{i}].connection.ws_url must not be empty"
            )));
        }

        if venue.connection.reconnect_delay_ms == 0 {
            errors.push(ConfigValidationError::Rule(format!(
                "venues[{i}].connection.reconnect_delay_ms must be > 0"
            )));
        }

        if venue.connection.max_reconnect_delay_ms < venue.connection.reconnect_delay_ms {
            errors.push(ConfigValidationError::Rule(format!(
                "venues[{i}].connection.max_reconnect_delay_ms must be >= reconnect_delay_ms"
            )));
        }

        if let Some(ref cb) = venue.circuit_breaker {
            if cb.failure_threshold == 0 {
                errors.push(ConfigValidationError::Rule(format!(
                    "venues[{i}].circuit_breaker.failure_threshold must be > 0"
                )));
            }
            if cb.reset_timeout_secs == 0 {
                errors.push(ConfigValidationError::Rule(format!(
                    "venues[{i}].circuit_breaker.reset_timeout_secs must be > 0"
                )));
            }
        }

        for (j, sub) in venue.subscriptions.iter().enumerate() {
            if sub.instrument.trim().is_empty() {
                errors.push(ConfigValidationError::Rule(format!(
                    "venues[{i}].subscriptions[{j}].instrument must not be empty"
                )));
            }
            if sub.canonical_symbol.trim().is_empty() {
                errors.push(ConfigValidationError::Rule(format!(
                    "venues[{i}].subscriptions[{j}].canonical_symbol must not be empty"
                )));
            }
            if sub.data_types.is_empty() {
                errors.push(ConfigValidationError::Rule(format!(
                    "venues[{i}].subscriptions[{j}].data_types must not be empty"
                )));
            }
            for dt in &sub.data_types {
                if MarketDataType::from_str_config(dt).is_err() {
                    errors.push(ConfigValidationError::Rule(format!(
                        "venues[{i}].subscriptions[{j}].data_types: unknown type \"{dt}\""
                    )));
                }
            }
        }

        if venue.adapter == "generic_ws" && venue.generic_ws.is_none() {
            errors.push(ConfigValidationError::Rule(format!(
                "venues[{i}] ({}) uses adapter \"generic_ws\" but has no [venues.generic_ws] section",
                venue.id
            )));
        }
    }
}

fn validate_consumer_stream_refs(
    nats: &super::model::NatsConfig,
    errors: &mut Vec<ConfigValidationError>,
) {
    let stream_names: Vec<&str> = nats.streams.iter().map(|s| s.name.as_str()).collect();

    for (i, consumer) in nats.consumers.iter().enumerate() {
        if !stream_names.contains(&consumer.stream.as_str()) {
            errors.push(ConfigValidationError::Rule(format!(
                "nats.consumers[{i}] ({}) references unknown stream \"{}\"",
                consumer.name, consumer.stream
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::*;

    fn minimal_valid_config() -> AppConfig {
        AppConfig {
            service: ServiceConfig {
                name: "test".to_owned(),
                log_level: "info".to_owned(),
                log_format: "json".to_owned(),
                shutdown_timeout_ms: 5000,
            },
            nats: NatsConfig {
                urls: vec!["nats://localhost:4222".to_owned()],
                connect_timeout_ms: 5000,
                reconnect_buffer_size: 8_388_608,
                max_reconnects: -1,
                ping_interval_secs: 20,
                auth: "none".to_owned(),
                token: None,
                username: None,
                password: None,
                credentials_path: None,
                nkey_seed: None,
                tls: NatsTlsConfig::default(),
                streams: vec![StreamConfig {
                    name: "MARKET_TRADES".to_owned(),
                    subjects: vec!["market.*.*.trade".to_owned()],
                    storage: "file".to_owned(),
                    retention: "limits".to_owned(),
                    max_age_secs: 86400,
                    max_bytes: 0,
                    max_msgs: 0,
                    max_msg_size: 65536,
                    discard: "old".to_owned(),
                    num_replicas: 1,
                    duplicate_window_secs: 120,
                }],
                consumers: vec![ConsumerConfig {
                    stream: "MARKET_TRADES".to_owned(),
                    name: "test-consumer".to_owned(),
                    durable: true,
                    ack_policy: "explicit".to_owned(),
                    ack_wait_secs: 30,
                    max_deliver: 5,
                    filter_subject: None,
                    deliver_policy: "all".to_owned(),
                    start_time: None,
                    max_ack_pending: 1000,
                    inactive_threshold_secs: 0,
                }],
            },
            venues: vec![VenueConfig {
                id: "binance".to_owned(),
                adapter: "binance".to_owned(),
                enabled: true,
                connection: ConnectionConfig {
                    ws_url: "wss://stream.binance.com:9443/ws".to_owned(),
                    reconnect_delay_ms: 1000,
                    max_reconnect_delay_ms: 60000,
                    max_reconnect_attempts: 0,
                    ping_interval_secs: 30,
                    pong_timeout_secs: 10,
                },
                circuit_breaker: Some(CircuitBreakerConfig {
                    failure_threshold: 5,
                    reset_timeout_secs: 60,
                    half_open_max_requests: 2,
                }),
                generic_ws: None,
                subscriptions: vec![SubscriptionConfig {
                    instrument: "BTCUSDT".to_owned(),
                    canonical_symbol: "BTC/USDT".to_owned(),
                    data_types: vec!["trade".to_owned()],
                }],
            }],
        }
    }

    #[test]
    fn test_valid_config_passes() {
        let errors = validate_config(&minimal_valid_config());
        assert!(errors.is_empty(), "expected no errors, got: {errors:?}");
    }

    #[test]
    fn test_no_enabled_venues() {
        let mut config = minimal_valid_config();
        config.venues[0].enabled = false;
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| e.to_string().contains("at least one venue"))
        );
    }

    #[test]
    fn test_consumer_references_unknown_stream() {
        let mut config = minimal_valid_config();
        config.nats.consumers[0].stream = "NONEXISTENT".to_owned();
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| e.to_string().contains("unknown stream"))
        );
    }

    #[test]
    fn test_invalid_data_type() {
        let mut config = minimal_valid_config();
        config.venues[0].subscriptions[0].data_types = vec!["invalid".to_owned()];
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| e.to_string().contains("unknown type"))
        );
    }

    #[test]
    fn test_generic_ws_without_config() {
        let mut config = minimal_valid_config();
        config.venues[0].adapter = "generic_ws".to_owned();
        config.venues[0].generic_ws = None;
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| e.to_string().contains("generic_ws")));
    }

    #[test]
    fn test_invalid_nats_url() {
        let mut config = minimal_valid_config();
        config.nats.urls = vec!["http://wrong".to_owned()];
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| e.to_string().contains("invalid scheme"))
        );
    }
}
