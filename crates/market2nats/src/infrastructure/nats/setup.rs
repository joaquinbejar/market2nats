use std::time::Duration;

use tracing::{error, info, instrument};

use crate::application::ports::{NatsError, NatsPublisher};
use crate::config::model::NatsConfig;

/// Connects to NATS and returns a client.
///
/// # Errors
///
/// Returns `NatsError::ConnectionFailed` if the connection cannot be established.
#[instrument(skip(config))]
pub async fn connect_nats(config: &NatsConfig) -> Result<async_nats::Client, NatsError> {
    let urls = config.urls.join(",");

    let mut options = async_nats::ConnectOptions::new()
        .connection_timeout(Duration::from_millis(config.connect_timeout_ms))
        .ping_interval(Duration::from_secs(config.ping_interval_secs));

    // Auth configuration.
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
        "credentials" => {
            if let Some(ref path) = config.credentials_path {
                options = options
                    .credentials_file(path)
                    .await
                    .map_err(|e| NatsError::ConnectionFailed(e.to_string()))?;
            }
        }
        "nkey" => {
            if let Some(ref seed) = config.nkey_seed {
                options = options.nkey(seed.clone());
            }
        }
        _ => {
            // "none" or unrecognized — no auth.
        }
    }

    let client = options
        .connect(&urls)
        .await
        .map_err(|e| NatsError::ConnectionFailed(e.to_string()))?;

    info!(urls = %urls, "connected to nats");
    Ok(client)
}

/// Sets up all JetStream streams and consumers from config.
///
/// # Errors
///
/// Returns `NatsError` if any stream or consumer setup fails.
#[instrument(skip(publisher, config))]
pub async fn setup_jetstream(
    publisher: &impl NatsPublisher,
    config: &NatsConfig,
) -> Result<(), NatsError> {
    // Create streams.
    for stream_config in &config.streams {
        if let Err(e) = publisher.ensure_stream(stream_config).await {
            error!(
                stream = %stream_config.name,
                error = %e,
                "failed to setup stream"
            );
            return Err(e);
        }
    }

    // Create consumers.
    for consumer_config in &config.consumers {
        if let Err(e) = publisher.ensure_consumer(consumer_config).await {
            error!(
                consumer = %consumer_config.name,
                error = %e,
                "failed to setup consumer"
            );
            return Err(e);
        }
    }

    info!(
        streams = config.streams.len(),
        consumers = config.consumers.len(),
        "jetstream setup complete"
    );
    Ok(())
}
