pub mod model;
pub mod validation;

use std::sync::Arc;

use model::AppConfig;

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
        errors: Vec<validation::ConfigValidationError>,
    },
}

/// Loads, parses, validates, and returns the application config.
///
/// Environment variable substitution is performed on string values
/// matching the `${VAR_NAME}` pattern.
///
/// # Errors
///
/// Returns `ConfigError` if the file cannot be read, parsed, or validated.
pub fn load_config(path: &str) -> Result<Arc<AppConfig>, ConfigError> {
    let raw = std::fs::read_to_string(path)?;
    let expanded = substitute_env_vars(&raw);
    let config: AppConfig = toml::from_str(&expanded)?;

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
    use super::*;

    #[test]
    fn test_substitute_env_vars_replaces() {
        unsafe { std::env::set_var("TEST_MDR_VAR", "hello") };
        let result = substitute_env_vars("token = \"${TEST_MDR_VAR}\"");
        assert_eq!(result, "token = \"hello\"");
        unsafe { std::env::remove_var("TEST_MDR_VAR") };
    }

    #[test]
    fn test_substitute_env_vars_missing_leaves_placeholder() {
        let result = substitute_env_vars("token = \"${NONEXISTENT_MDR_VAR_12345}\"");
        assert_eq!(result, "token = \"${NONEXISTENT_MDR_VAR_12345}\"");
    }

    #[test]
    fn test_load_config_parses_relay_toml() {
        let config = load_config("config/relay.toml").unwrap();
        assert_eq!(config.service.name, "market2nats");
        assert!(!config.venues.is_empty());
        assert!(!config.nats.streams.is_empty());
    }

    #[test]
    fn test_load_config_parses_binance_spot_trades_profile() {
        let config = load_config("config/relay.binance-spot-trades.toml").unwrap();
        assert_eq!(config.venues.len(), 1);
        let venue = &config.venues[0];
        assert_eq!(venue.id, "binance");
        assert_eq!(venue.subscriptions.len(), 2);
        for sub in &venue.subscriptions {
            assert_eq!(sub.data_types, vec!["trade".to_owned()]);
        }
    }

    #[test]
    fn test_load_config_parses_bybit_profile() {
        let config = load_config("config/relay.bybit.toml").unwrap();
        assert_eq!(config.venues.len(), 2);
        assert_eq!(config.venues[0].id, "bybit");
        assert_eq!(config.venues[1].id, "bybit-linear");
        assert!(!config.nats.streams.is_empty());
    }
}
