use std::future::Future;
use std::pin::Pin;

use market2nats_domain::CanonicalSymbol;

use crate::domain::{OracleError, OraclePrice, PriceSource};

/// Boxed future type alias for async trait methods without `async_trait` macro.
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Provides current price sources for a given symbol.
///
/// Infrastructure layer implements this to provide trade data from NATS
/// or other sources. The application layer consumes this trait to feed
/// the oracle aggregation pipeline.
pub trait TradeSource: Send + Sync {
    /// Returns the latest price sources for the given canonical symbol.
    ///
    /// Each returned `PriceSource` represents the most recent trade observation
    /// from a distinct venue. The `age_ms` field is recomputed at retrieval time
    /// to reflect the current staleness of each source.
    fn get_sources(&self, symbol: &CanonicalSymbol) -> Vec<PriceSource>;
}

/// Publishes computed oracle prices to a downstream sink.
///
/// Infrastructure layer implements this to publish to NATS JetStream
/// or other messaging systems. The application layer calls this after
/// the aggregation pipeline produces a new oracle price.
pub trait OraclePublisher: Send + Sync {
    /// Publishes an oracle price to the configured destination.
    ///
    /// # Errors
    ///
    /// Returns `OracleError` if serialization or publishing fails.
    fn publish(&self, price: &OraclePrice) -> BoxFuture<'_, Result<(), OracleError>>;
}
