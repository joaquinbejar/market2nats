use std::future::Future;
use std::pin::Pin;

use crate::application::ports::OraclePublisher;
use crate::domain::{OracleError, OraclePrice};

/// Publishes computed oracle prices to NATS as JSON.
///
/// The subject pattern must contain `<symbol_normalized>` which is replaced
/// with the normalized canonical symbol (e.g., `btc-usdt`) at publish time.
pub struct OraclePricePublisher {
    /// NATS client for publishing.
    client: async_nats::Client,
    /// Subject pattern with `<symbol_normalized>` placeholder.
    subject_pattern: String,
}

impl OraclePricePublisher {
    /// Creates a new publisher with the given NATS client and subject pattern.
    ///
    /// The `subject_pattern` must contain `<symbol_normalized>` which will be
    /// replaced with the normalized symbol at publish time.
    #[must_use]
    pub fn new(client: async_nats::Client, subject_pattern: String) -> Self {
        Self {
            client,
            subject_pattern,
        }
    }

    /// Resolves the NATS subject by replacing the `<symbol_normalized>` placeholder.
    #[must_use]
    fn resolve_subject(&self, price: &OraclePrice) -> String {
        self.subject_pattern
            .replace("<symbol_normalized>", &price.symbol.normalized())
    }
}

impl OraclePublisher for OraclePricePublisher {
    fn publish(
        &self,
        price: &OraclePrice,
    ) -> Pin<Box<dyn Future<Output = Result<(), OracleError>> + Send + '_>> {
        // Eagerly resolve subject and serialize payload before entering the async block,
        // so we do not need to borrow `price` across the await point.
        let subject = self.resolve_subject(price);
        let symbol_display = price.symbol.to_string();

        let serialized = match serde_json::to_vec(price) {
            Ok(bytes) => bytes,
            Err(e) => {
                return Box::pin(async move { Err(OracleError::Serialization(e.to_string())) });
            }
        };

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Content-Type", "application/json");

        let payload = bytes::Bytes::from(serialized);

        Box::pin(async move {
            self.client
                .publish_with_headers(subject.clone(), headers, payload)
                .await
                .map_err(|e| OracleError::Nats(format!("publish to {subject}: {e}")))?;

            tracing::debug!(subject = %subject, symbol = %symbol_display, "published oracle price");

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use market2nats_domain::CanonicalSymbol;

    #[test]
    fn test_resolve_subject_replaces_placeholder() {
        let pattern = "oracle.<symbol_normalized>.price";
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        let resolved = pattern.replace("<symbol_normalized>", &symbol.normalized());
        assert_eq!(resolved, "oracle.btc-usdt.price");
    }

    #[test]
    fn test_resolve_subject_eth() {
        let pattern = "oracle.<symbol_normalized>.price";
        let symbol = CanonicalSymbol::try_new("ETH/USDT").unwrap();
        let resolved = pattern.replace("<symbol_normalized>", &symbol.normalized());
        assert_eq!(resolved, "oracle.eth-usdt.price");
    }

    #[test]
    fn test_resolve_subject_no_placeholder() {
        let pattern = "oracle.fixed.price";
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        let resolved = pattern.replace("<symbol_normalized>", &symbol.normalized());
        assert_eq!(resolved, "oracle.fixed.price");
    }
}
