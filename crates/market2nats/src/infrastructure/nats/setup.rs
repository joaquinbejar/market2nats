use std::time::Duration;

use tracing::{error, info, instrument};

use crate::application::ports::{NatsError, NatsPublisher};
use crate::config::model::NatsConfig;

/// Extracts user:password from a `nats://user:pass@host:port` URL.
///
/// Returns `(clean_url, Some((user, pass)))` if credentials are found,
/// or `(original_url, None)` otherwise.
fn extract_url_credentials(url: &str) -> (String, Option<(String, String)>) {
    let Some(scheme_end) = url.find("://") else {
        return (url.to_owned(), None);
    };
    let after_scheme = &url[scheme_end + 3..];

    let Some(at_pos) = after_scheme.find('@') else {
        return (url.to_owned(), None);
    };

    let userinfo = &after_scheme[..at_pos];
    let Some(colon_pos) = userinfo.find(':') else {
        return (url.to_owned(), None);
    };

    let user = userinfo[..colon_pos].to_owned();
    let pass = userinfo[colon_pos + 1..].to_owned();
    let scheme = &url[..scheme_end + 3];
    let host_and_rest = &after_scheme[at_pos + 1..];
    let clean_url = format!("{scheme}{host_and_rest}");

    (clean_url, Some((user, pass)))
}

/// Connects to NATS and returns a client.
///
/// Credentials can be embedded in the URL (`nats://user:pass@host:4222`)
/// or configured via the `auth`/`username`/`password` fields. URL-embedded
/// credentials take priority.
///
/// # Errors
///
/// Returns `NatsError::ConnectionFailed` if the connection cannot be established.
#[instrument(skip(config))]
pub async fn connect_nats(config: &NatsConfig) -> Result<async_nats::Client, NatsError> {
    // Extract credentials from URLs if embedded.
    let mut url_user = None;
    let mut url_pass = None;
    let clean_urls: Vec<String> = config
        .urls
        .iter()
        .map(|u| {
            let (clean, creds) = extract_url_credentials(u);
            if let Some((user, pass)) = creds {
                url_user = Some(user);
                url_pass = Some(pass);
            }
            clean
        })
        .collect();

    let urls = clean_urls.join(",");

    let mut options = async_nats::ConnectOptions::new()
        .connection_timeout(Duration::from_millis(config.connect_timeout_ms))
        .ping_interval(Duration::from_secs(config.ping_interval_secs));

    // TLS support.
    if config.tls.enabled {
        options = options.require_tls(true);
    }

    // Auth: URL-embedded credentials take priority, then explicit config fields.
    if let (Some(user), Some(pass)) = (url_user, url_pass) {
        options = options.user_and_password(user, pass);
    } else {
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
