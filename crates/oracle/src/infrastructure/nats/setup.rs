//! NATS connection setup with authentication and TLS support.

use std::time::Duration;

use tracing::info;

use crate::config::model::NatsConfig;
use crate::domain::OracleError;

/// Connects to NATS using the provided configuration.
///
/// **Authentication** is resolved in order:
/// 1. Credentials embedded in the URL (e.g. `nats://user:pass@host:4222`).
/// 2. Explicit `auth` field: `"token"`, `"userpass"`, or `"none"` (default).
///
/// **TLS** is enabled automatically when:
/// - The URL scheme is `tls://`, or
/// - `tls_required` is set to `true` in config.
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

    // TLS support.
    if config.tls_required == Some(true) {
        options = options.require_tls(true);
    }

    // Explicit auth (only needed when credentials are NOT in the URL).
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
            // "none" or unrecognized — no explicit auth.
            // Credentials may still be embedded in the URL.
        }
    }

    let client = options
        .connect(&urls)
        .await
        .map_err(|e| OracleError::nats(format!("connect to {urls}: {e}")))?;

    info!(urls = %urls, auth = %config.auth, "connected to NATS");
    Ok(client)
}
