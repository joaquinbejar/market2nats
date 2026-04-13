//! Oracle configuration loading, parsing, and validation.
//!
//! Supports `${ENV_VAR}` substitution in TOML values for secrets.

pub mod builder;
pub mod model;
pub mod validation;

use std::sync::Arc;

use model::OracleConfig;

/// Errors from configuration loading.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Failed to read the config file.
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),

    /// Failed to parse the TOML content.
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),

    /// One or more validation rules failed.
    #[error("config validation failed: {errors:?}")]
    Validation {
        /// The list of validation errors found.
        errors: Vec<validation::ConfigValidationError>,
    },
}

/// Loads, parses, validates, and returns the oracle config.
///
/// Environment variable substitution is performed on string values
/// matching the `${VAR_NAME}` pattern before TOML parsing.
///
/// # Errors
///
/// Returns `ConfigError` if the file cannot be read, parsed, or validated.
pub fn load_config(path: &str) -> Result<Arc<OracleConfig>, ConfigError> {
    let raw = std::fs::read_to_string(path)?;
    let expanded = substitute_env_vars(&raw);
    let config: OracleConfig = toml::from_str(&expanded)?;

    let errors = validation::validate_config(&config);
    if !errors.is_empty() {
        return Err(ConfigError::Validation { errors });
    }

    Ok(Arc::new(config))
}

/// Replaces `${VAR_NAME}` patterns with the corresponding environment variable value.
/// If the variable is not set, the placeholder is left as-is.
#[must_use]
fn substitute_env_vars(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            for c in chars.by_ref() {
                if c == '}' {
                    break;
                }
                var_name.push(c);
            }
            match std::env::var(&var_name) {
                Ok(val) => result.push_str(&val),
                Err(_) => {
                    result.push_str("${");
                    result.push_str(&var_name);
                    result.push('}');
                }
            }
        } else {
            result.push(ch);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn test_substitute_env_vars_replaces() {
        // Safety: test binary is single-threaded for env var mutation;
        // this matches the market2nats convention for env var tests.
        unsafe { std::env::set_var("TEST_ORACLE_VAR", "secret") };
        let result = substitute_env_vars("token = \"${TEST_ORACLE_VAR}\"");
        assert_eq!(result, "token = \"secret\"");
        unsafe { std::env::remove_var("TEST_ORACLE_VAR") };
    }

    #[test]
    fn test_substitute_env_vars_missing_leaves_placeholder() {
        let result = substitute_env_vars("token = \"${NONEXISTENT_ORACLE_VAR_99999}\"");
        assert_eq!(result, "token = \"${NONEXISTENT_ORACLE_VAR_99999}\"");
    }

    #[test]
    fn test_load_config_valid_toml_parses() {
        let toml_content = r#"
[service]
name = "oracle"

[nats]
urls = ["nats://localhost:4222"]

[[subscriptions]]
symbol = "BTC/USDT"
subjects = ["market.binance.btc-usdt.trade", "market.kraken.btc-usdt.trade"]

[[subscriptions]]
symbol = "ETH/USDT"
subjects = ["market.binance.eth-usdt.trade"]

[pipeline]
strategy = "median"
min_sources = 2

[publish]
subject_pattern = "oracle.<symbol_normalized>.price"
"#;
        let mut tmp = tempfile::Builder::new()
            .prefix("oracle_test")
            .suffix(".toml")
            .tempfile()
            .expect("create temp file");
        tmp.write_all(toml_content.as_bytes())
            .expect("write temp file");
        tmp.flush().expect("flush temp file");
        let config = load_config(tmp.path().to_str().expect("valid path")).expect("should load");
        assert_eq!(config.service.name, "oracle");
        assert_eq!(config.subscriptions.len(), 2);
        assert_eq!(config.pipeline.strategy, "median");
        assert_eq!(config.pipeline.min_sources, 2);
    }

    #[test]
    fn test_load_config_defaults_applied() {
        let toml_content = r#"
[service]
name = "oracle"

[nats]
urls = ["nats://localhost:4222"]

[[subscriptions]]
symbol = "BTC/USDT"
subjects = ["market.binance.btc-usdt.trade"]

[pipeline]
strategy = "twap"

[publish]
subject_pattern = "oracle.<symbol_normalized>.price"
"#;
        let mut tmp = tempfile::Builder::new()
            .prefix("oracle_test")
            .suffix(".toml")
            .tempfile()
            .expect("create temp file");
        tmp.write_all(toml_content.as_bytes())
            .expect("write temp file");
        tmp.flush().expect("flush temp file");
        let config = load_config(tmp.path().to_str().expect("valid path")).expect("should load");
        assert_eq!(config.service.log_level, "info");
        assert_eq!(config.service.log_format, "json");
        assert_eq!(config.nats.auth, "none");
        assert_eq!(config.pipeline.staleness_max_ms, 10_000);
        assert_eq!(config.pipeline.outlier_max_deviation_bps, 100);
        assert_eq!(config.pipeline.min_sources, 3);
        assert_eq!(config.pipeline.twap_window_ms, 30_000);
        assert_eq!(config.publish.format, "json");
        assert_eq!(config.publish.publish_interval_ms, 1_000);
    }
}
