//! NATS connection setup with authentication and TLS support.

use std::time::Duration;

use tracing::info;

use crate::config::model::NatsConfig;
use crate::domain::OracleError;

/// Extracts user:password from a `nats://user:pass@host:port` URL.
///
/// Returns `(clean_url, Some((user, pass)))` if credentials are found,
/// or `(original_url, None)` otherwise.
fn extract_url_credentials(url: &str) -> (String, Option<(String, String)>) {
    // Look for the pattern: scheme://user:pass@host
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
    let mut options = async_nats::ConnectOptions::new();

    if let Some(timeout_ms) = config.connect_timeout_ms {
        options = options.connection_timeout(Duration::from_millis(timeout_ms));
    }

    if let Some(ref name) = config.connection_name {
        options = options.name(name.as_str());
    }

    // TLS support.
    // Note: tls_ca_file / tls_cert_file / tls_key_file are parsed by the config model
    // but not yet wired into ConnectOptions — only tls_required is applied here.
    if config.tls_required == Some(true) {
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
            _ => {}
        }
    }

    let client = options
        .connect(&urls)
        .await
        .map_err(|e| OracleError::nats(format!("connect to {urls}: {e}")))?;

    info!(urls = %urls, "connected to NATS");
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_url_credentials_with_userpass() {
        let (clean, creds) = extract_url_credentials("nats://myuser:mypass@localhost:4222");
        assert_eq!(clean, "nats://localhost:4222");
        let (user, pass) = creds.expect("should have credentials");
        assert_eq!(user, "myuser");
        assert_eq!(pass, "mypass");
    }

    #[test]
    fn test_extract_url_credentials_without_userpass() {
        let (clean, creds) = extract_url_credentials("nats://localhost:4222");
        assert_eq!(clean, "nats://localhost:4222");
        assert!(creds.is_none());
    }

    #[test]
    fn test_extract_url_credentials_tls_scheme() {
        let (clean, creds) = extract_url_credentials("tls://user:pass@nats.example.com:4222");
        assert_eq!(clean, "tls://nats.example.com:4222");
        let (user, pass) = creds.expect("should have credentials");
        assert_eq!(user, "user");
        assert_eq!(pass, "pass");
    }

    #[test]
    fn test_extract_url_credentials_special_chars_in_password() {
        let (clean, creds) = extract_url_credentials("nats://nats:AS09.1qa@192.168.1.6:4222");
        assert_eq!(clean, "nats://192.168.1.6:4222");
        let (user, pass) = creds.expect("should have credentials");
        assert_eq!(user, "nats");
        assert_eq!(pass, "AS09.1qa");
    }
}
