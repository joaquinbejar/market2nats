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

    errors
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{
        NatsConfig, OracleConfig, PipelineConfig, PublishConfig, ServiceConfig, SubscriptionEntry,
    };

    /// Builds a valid baseline config for mutation in individual tests.
    fn valid_config() -> OracleConfig {
        OracleConfig {
            service: ServiceConfig {
                name: "oracle".to_owned(),
                log_level: "info".to_owned(),
                log_format: "json".to_owned(),
            },
            nats: NatsConfig {
                urls: vec!["nats://localhost:4222".to_owned()],
                auth: "none".to_owned(),
                token: None,
                user: None,
                password: None,
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
}
