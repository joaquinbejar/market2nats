//! Oracle configuration validation.
//!
//! Runs after TOML parsing to catch semantic errors that serde cannot enforce.

use crate::domain::AggregationStrategyKind;

use super::model::OracleConfig;

/// Validation errors for oracle configuration.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ConfigValidationError {
    /// The configured strategy name is not recognized.
    #[error("unknown strategy: {0}")]
    UnknownStrategy(String),

    /// The minimum sources value must be at least 1.
    #[error("min_sources must be >= 1, got {0}")]
    InvalidMinSources(usize),

    /// No subscription entries were configured.
    #[error("no subscriptions configured")]
    NoSubscriptions,

    /// The publish subject pattern is missing the `<symbol_normalized>` placeholder.
    #[error("subject_pattern must contain <symbol_normalized>")]
    MissingPlaceholder,

    /// A subscription entry has an empty subjects list.
    #[error("subscription for {symbol} has no subjects")]
    EmptySubjects {
        /// The symbol whose subscription has no subjects.
        symbol: String,
    },

    /// The WebSocket port must be greater than 0 when enabled.
    #[error("websocket.port must be > 0")]
    InvalidWsPort,

    /// The WebSocket path must start with '/'.
    #[error("websocket.path must start with '/'")]
    InvalidWsPath,

    /// `tls_enabled = true` requires both `tls_cert_file` and `tls_key_file`.
    #[error("websocket.tls_enabled requires both tls_cert_file and tls_key_file")]
    MissingTlsFiles,

    /// The configured TLS certificate file does not exist.
    #[error("websocket.tls_cert_file does not exist: {0}")]
    TlsCertFileMissing(String),

    /// The configured TLS key file does not exist.
    #[error("websocket.tls_key_file does not exist: {0}")]
    TlsKeyFileMissing(String),

    /// A configured `[nats]` TLS path is not a readable file on disk.
    #[error("nats.{field} is not a readable file: {path}")]
    NatsTlsFileMissing {
        /// The config key that points at the missing file.
        field: &'static str,
        /// The configured path that could not be found.
        path: String,
    },

    /// Client TLS requires both `tls_cert_file` and `tls_key_file`.
    #[error("nats.tls_cert_file and nats.tls_key_file must both be set for client TLS")]
    NatsTlsClientPairIncomplete,
}

/// Validates the oracle configuration, returning all detected errors.
///
/// An empty vector means the configuration is valid.
#[must_use]
pub fn validate_config(config: &OracleConfig) -> Vec<ConfigValidationError> {
    let mut errors = Vec::new();

    // Validate strategy name.
    if AggregationStrategyKind::from_str_config(&config.pipeline.strategy).is_err() {
        errors.push(ConfigValidationError::UnknownStrategy(
            config.pipeline.strategy.clone(),
        ));
    }

    // Validate min_sources >= 1.
    if config.pipeline.min_sources < 1 {
        errors.push(ConfigValidationError::InvalidMinSources(
            config.pipeline.min_sources,
        ));
    }

    // Validate at least one subscription.
    if config.subscriptions.is_empty() {
        errors.push(ConfigValidationError::NoSubscriptions);
    }

    // Validate each subscription has non-empty subjects.
    for sub in &config.subscriptions {
        if sub.subjects.is_empty() {
            errors.push(ConfigValidationError::EmptySubjects {
                symbol: sub.symbol.clone(),
            });
        }
    }

    // Validate subject_pattern contains placeholder.
    if !config
        .publish
        .subject_pattern
        .contains("<symbol_normalized>")
    {
        errors.push(ConfigValidationError::MissingPlaceholder);
    }

    // Validate NATS TLS certificate material.
    validate_nats_tls(&config.nats, &mut errors);

    // Validate WebSocket configuration when enabled.
    if config.websocket.enabled {
        if config.websocket.port == 0 {
            errors.push(ConfigValidationError::InvalidWsPort);
        }
        if !config.websocket.path.starts_with('/') {
            errors.push(ConfigValidationError::InvalidWsPath);
        }
        if config.websocket.tls_enabled {
            match (
                config.websocket.tls_cert_file.as_deref(),
                config.websocket.tls_key_file.as_deref(),
            ) {
                (Some(cert), Some(key)) => {
                    if !std::path::Path::new(cert).is_file() {
                        errors.push(ConfigValidationError::TlsCertFileMissing(cert.to_owned()));
                    }
                    if !std::path::Path::new(key).is_file() {
                        errors.push(ConfigValidationError::TlsKeyFileMissing(key.to_owned()));
                    }
                }
                _ => errors.push(ConfigValidationError::MissingTlsFiles),
            }
        }
    }

    errors
}

/// Validates the `[nats]` TLS certificate material.
///
/// Every configured path must be a readable file, and client TLS requires both
/// `tls_cert_file` and `tls_key_file`. Paths are checked regardless of
/// `tls_required`, because TLS also activates from a `tls://` URL scheme.
///
/// This check touches the filesystem: a config that validates on one host can
/// fail on another where the certificates are not mounted.
fn validate_nats_tls(
    nats: &crate::config::model::NatsConfig,
    errors: &mut Vec<ConfigValidationError>,
) {
    for (field, path) in [
        ("tls_ca_file", nats.tls_ca_file.as_deref()),
        ("tls_cert_file", nats.tls_cert_file.as_deref()),
        ("tls_key_file", nats.tls_key_file.as_deref()),
    ] {
        // `is_file`, not `exists`: a directory or an unreadable path would
        // otherwise pass here and fail later inside the TLS handshake with an
        // opaque I/O error, which is what this check exists to prevent.
        if let Some(path) = path
            && !std::path::Path::new(path).is_file()
        {
            errors.push(ConfigValidationError::NatsTlsFileMissing {
                field,
                path: path.to_owned(),
            });
        }
    }

    if nats.tls_cert_file.is_some() != nats.tls_key_file.is_some() {
        errors.push(ConfigValidationError::NatsTlsClientPairIncomplete);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{
        NatsConfig, OracleConfig, PipelineConfig, PublishConfig, ServiceConfig, SubscriptionEntry,
        WebSocketConfig,
    };

    /// Builds a valid baseline config for mutation in individual tests.
    fn valid_config() -> OracleConfig {
        OracleConfig {
            service: ServiceConfig {
                name: "oracle".to_owned(),
                log_level: "info".to_owned(),
                log_format: "json".to_owned(),
                http_port: 9091,
            },
            nats: NatsConfig {
                urls: vec!["nats://localhost:4222".to_owned()],
                auth: "none".to_owned(),
                token: None,
                username: None,
                password: None,
                connection_name: None,
                connect_timeout_ms: None,
                tls_required: None,
                tls_ca_file: None,
                tls_cert_file: None,
                tls_key_file: None,
            },
            subscriptions: vec![SubscriptionEntry {
                symbol: "BTC/USDT".to_owned(),
                subjects: vec!["market.binance.btc-usdt.trade".to_owned()],
            }],
            pipeline: PipelineConfig {
                strategy: "median".to_owned(),
                staleness_max_ms: 10_000,
                outlier_max_deviation_bps: 100,
                min_sources: 3,
                twap_window_ms: 30_000,
            },
            publish: PublishConfig {
                subject_pattern: "oracle.<symbol_normalized>.price".to_owned(),
                format: "json".to_owned(),
                publish_interval_ms: 1_000,
            },
            websocket: WebSocketConfig::default(),
        }
    }

    #[test]
    fn test_validate_valid_config_passes() {
        let errors = validate_config(&valid_config());
        assert!(errors.is_empty(), "expected no errors, got: {errors:?}");
    }

    #[test]
    fn test_validate_unknown_strategy_rejected() {
        let mut config = valid_config();
        config.pipeline.strategy = "magic".to_owned();
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigValidationError::UnknownStrategy(s) if s == "magic"))
        );
    }

    #[test]
    fn test_validate_min_sources_zero_rejected() {
        let mut config = valid_config();
        config.pipeline.min_sources = 0;
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigValidationError::InvalidMinSources(0)))
        );
    }

    #[test]
    fn test_validate_no_subscriptions_rejected() {
        let mut config = valid_config();
        config.subscriptions.clear();
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigValidationError::NoSubscriptions))
        );
    }

    #[test]
    fn test_validate_missing_placeholder_rejected() {
        let mut config = valid_config();
        config.publish.subject_pattern = "oracle.price".to_owned();
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigValidationError::MissingPlaceholder))
        );
    }

    #[test]
    fn test_validate_empty_subjects_rejected() {
        let mut config = valid_config();
        config.subscriptions = vec![SubscriptionEntry {
            symbol: "ETH/USDT".to_owned(),
            subjects: Vec::new(),
        }];
        let errors = validate_config(&config);
        assert!(errors.iter().any(
            |e| matches!(e, ConfigValidationError::EmptySubjects { symbol } if symbol == "ETH/USDT")
        ));
    }

    #[test]
    fn test_validate_ws_port_zero_rejected_when_enabled() {
        let mut config = valid_config();
        config.websocket.enabled = true;
        config.websocket.port = 0;
        config.websocket.path = "/".to_owned();
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigValidationError::InvalidWsPort))
        );
    }

    #[test]
    fn test_validate_ws_path_no_leading_slash_rejected_when_enabled() {
        let mut config = valid_config();
        config.websocket.enabled = true;
        config.websocket.port = 9092;
        config.websocket.path = "ws".to_owned();
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigValidationError::InvalidWsPath))
        );
    }

    #[test]
    fn test_validate_ws_disabled_skips_validation() {
        let mut config = valid_config();
        config.websocket.enabled = false;
        config.websocket.port = 0;
        config.websocket.path = "no-slash".to_owned();
        let errors = validate_config(&config);
        // No websocket errors when disabled.
        assert!(!errors.iter().any(|e| matches!(
            e,
            ConfigValidationError::InvalidWsPort | ConfigValidationError::InvalidWsPath
        )));
    }

    #[test]
    fn test_validate_ws_valid_when_enabled() {
        let mut config = valid_config();
        config.websocket.enabled = true;
        config.websocket.port = 9092;
        config.websocket.path = "/ws".to_owned();
        let errors = validate_config(&config);
        assert!(!errors.iter().any(|e| matches!(
            e,
            ConfigValidationError::InvalidWsPort | ConfigValidationError::InvalidWsPath
        )));
    }

    #[test]
    fn test_validate_ws_tls_enabled_without_cert_and_key_rejected() {
        let mut config = valid_config();
        config.websocket.enabled = true;
        config.websocket.tls_enabled = true;
        // tls_cert_file / tls_key_file left as None.
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigValidationError::MissingTlsFiles))
        );
    }

    #[test]
    fn test_validate_ws_tls_enabled_with_missing_cert_file_rejected() {
        let mut config = valid_config();
        config.websocket.enabled = true;
        config.websocket.tls_enabled = true;
        config.websocket.tls_cert_file = Some("/nonexistent/cert.pem".to_owned());
        config.websocket.tls_key_file = Some("/nonexistent/key.pem".to_owned());
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigValidationError::TlsCertFileMissing(_)))
        );
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigValidationError::TlsKeyFileMissing(_)))
        );
    }

    #[test]
    fn test_validate_nats_tls_missing_ca_file_rejected() {
        let mut config = valid_config();
        config.nats.tls_required = Some(true);
        config.nats.tls_ca_file = Some("/nonexistent/ca.pem".to_owned());
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| matches!(
            e,
            ConfigValidationError::NatsTlsFileMissing { field, .. } if *field == "tls_ca_file"
        )));
    }

    #[test]
    fn test_validate_nats_tls_cert_without_key_rejected() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let cert = dir.path().join("client.pem");
        std::fs::write(&cert, b"cert").expect("write cert");

        let mut config = valid_config();
        config.nats.tls_cert_file = Some(cert.to_string_lossy().into_owned());
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigValidationError::NatsTlsClientPairIncomplete))
        );
    }

    #[test]
    fn test_validate_nats_tls_missing_cert_file_rejected() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let key = dir.path().join("client.key");
        std::fs::write(&key, b"key").expect("write key");

        let mut config = valid_config();
        config.nats.tls_cert_file = Some("/nonexistent/client.pem".to_owned());
        config.nats.tls_key_file = Some(key.to_string_lossy().into_owned());
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| matches!(
            e,
            ConfigValidationError::NatsTlsFileMissing { field, .. } if *field == "tls_cert_file"
        )));
    }

    #[test]
    fn test_validate_nats_tls_missing_key_file_rejected() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let cert = dir.path().join("client.pem");
        std::fs::write(&cert, b"cert").expect("write cert");

        let mut config = valid_config();
        config.nats.tls_cert_file = Some(cert.to_string_lossy().into_owned());
        config.nats.tls_key_file = Some("/nonexistent/client.key".to_owned());
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| matches!(
            e,
            ConfigValidationError::NatsTlsFileMissing { field, .. } if *field == "tls_key_file"
        )));
    }

    #[test]
    fn test_validate_nats_tls_key_without_cert_rejected() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let key = dir.path().join("client.key");
        std::fs::write(&key, b"key").expect("write key");

        let mut config = valid_config();
        config.nats.tls_key_file = Some(key.to_string_lossy().into_owned());
        let errors = validate_config(&config);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, ConfigValidationError::NatsTlsClientPairIncomplete))
        );
    }

    #[test]
    fn test_validate_nats_tls_directory_instead_of_file_rejected() {
        // `exists()` would accept a directory here; the check must not.
        let dir = tempfile::tempdir().expect("create tempdir");

        let mut config = valid_config();
        config.nats.tls_ca_file = Some(dir.path().to_string_lossy().into_owned());
        let errors = validate_config(&config);
        assert!(errors.iter().any(|e| matches!(
            e,
            ConfigValidationError::NatsTlsFileMissing { field, .. } if *field == "tls_ca_file"
        )));
    }

    #[test]
    fn test_validate_nats_tls_all_paths_present_ok() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let ca = dir.path().join("ca.pem");
        let cert = dir.path().join("client.pem");
        let key = dir.path().join("client.key");
        for path in [&ca, &cert, &key] {
            std::fs::write(path, b"pem").expect("write pem");
        }

        let mut config = valid_config();
        config.nats.tls_required = Some(true);
        config.nats.tls_ca_file = Some(ca.to_string_lossy().into_owned());
        config.nats.tls_cert_file = Some(cert.to_string_lossy().into_owned());
        config.nats.tls_key_file = Some(key.to_string_lossy().into_owned());
        let errors = validate_config(&config);
        assert!(errors.is_empty(), "expected no errors, got: {errors:?}");
    }

    #[test]
    fn test_validate_nats_tls_unset_paths_skipped() {
        let config = valid_config();
        let errors = validate_config(&config);
        assert!(!errors.iter().any(|e| matches!(
            e,
            ConfigValidationError::NatsTlsFileMissing { .. }
                | ConfigValidationError::NatsTlsClientPairIncomplete
        )));
    }

    #[test]
    fn test_validate_ws_tls_disabled_skips_tls_checks() {
        let mut config = valid_config();
        config.websocket.enabled = true;
        config.websocket.tls_enabled = false;
        // tls_cert_file / tls_key_file are None.
        let errors = validate_config(&config);
        assert!(!errors.iter().any(|e| matches!(
            e,
            ConfigValidationError::MissingTlsFiles
                | ConfigValidationError::TlsCertFileMissing(_)
                | ConfigValidationError::TlsKeyFileMissing(_)
        )));
    }
}
