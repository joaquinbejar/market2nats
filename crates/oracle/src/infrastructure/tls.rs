//! Process-level rustls crypto provider installation.

use tokio_rustls::rustls;

/// Installs the `ring` rustls `CryptoProvider` as the process-level default.
///
/// rustls 0.23 cannot auto-select a provider when more than one provider
/// feature is enabled in the dependency graph. Here both are: `ring` comes in
/// through `async-nats`/`tokio-rustls` and `aws-lc-rs` through
/// `hyper-rustls`/`metrics-exporter-prometheus`. Without an explicit install the
/// first TLS handshake — e.g. a `tls://` NATS connection — panics.
///
/// rustls is reached through `tokio_rustls`' re-export rather than a direct
/// dependency: the provider is process-global *per rustls instance*, so it must
/// be installed into the very same instance `async-nats` performs its handshake
/// with. Going through `tokio-rustls` makes that hold by construction, even if
/// the two ever resolve to different rustls major versions.
///
/// Must be called before any TLS operation, ideally as the first statement of
/// `main`.
///
/// Idempotent: returns `true` if this call performed the installation, `false`
/// if a provider was already installed.
pub fn install_crypto_provider() -> bool {
    rustls::crypto::ring::default_provider()
        .install_default()
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_install_crypto_provider_second_call_returns_false() {
        // The first call may return either value: tests share one process and
        // another test may have installed the provider already.
        let _ = install_crypto_provider();
        assert!(!install_crypto_provider());
    }

    #[test]
    fn test_install_crypto_provider_sets_process_default() {
        let _ = install_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }
}
