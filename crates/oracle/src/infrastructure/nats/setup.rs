//! NATS connection setup with authentication support.

use std::time::Duration;

use tracing::info;

use crate::config::model::NatsConfig;
use crate::domain::OracleError;

/// Connects to NATS using the provided configuration, including authentication.
///
/// Supports three auth methods configured via `config.auth`:
/// - `"none"` (default) — no authentication.
/// - `"token"` — token-based authentication using `config.token`.
/// - `"userpass"` — username/password authentication using `config.username` and `config.password`.
///
/// # Errors
///
/// Returns `OracleError::Nats` if the connection fails.
pub async fn connect_nats(config: &NatsConfig) -> Result<async_nats::Client, OracleError> {
    let urls = config.urls.join(",");

    let mut options = async_nats::ConnectOptions::new();

    if let Some(timeout_ms) = config.connect_timeout_ms {
        options = options.connection_timeout(Duration::from_millis(timeout_ms));
    }

    if let Some(ref name) = config.connection_name {
        options = options.name(name.as_str());
    }

    match config.auth.as_str() {
        "token" => {
            if let Some(ref token) = config.token {
                options = options.token(token.clone());
            }
        }
        "userpass" => {
            if let (Some(user), Some(pass)) = (&config.username, &config.password) {
                options = options.user_and_password(user.clone(), pass.clone());
            }
        }
        _ => {
            // "none" or unrecognized — no auth.
        }
    }

    let client = options
        .connect(&urls)
        .await
        .map_err(|e| OracleError::nats(format!("connect to {urls}: {e}")))?;

    info!(urls = %urls, auth = %config.auth, "connected to NATS");
    Ok(client)
}
