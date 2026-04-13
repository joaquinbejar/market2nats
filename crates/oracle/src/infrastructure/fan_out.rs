//! Fan-out publisher that dispatches oracle prices to multiple sinks.
//!
//! All inner publishers are called in sequence; errors from individual
//! publishers are logged but do not prevent the remaining publishers from
//! being invoked. Returns the first error encountered, if any.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::application::ports::OraclePublisher;
use crate::domain::{OracleError, OraclePrice};

/// Publisher that fans out to multiple inner publishers sequentially.
///
/// All publishers are called regardless of individual failures.
/// The first error encountered is returned; subsequent errors are logged.
pub struct FanOutPublisher {
    /// Ordered list of publishers to invoke.
    publishers: Vec<Arc<dyn OraclePublisher>>,
}

impl FanOutPublisher {
    /// Creates a new fan-out publisher wrapping the given publishers.
    #[must_use]
    pub fn new(publishers: Vec<Arc<dyn OraclePublisher>>) -> Self {
        Self { publishers }
    }
}

impl OraclePublisher for FanOutPublisher {
    fn publish(
        &self,
        price: &OraclePrice,
    ) -> Pin<Box<dyn Future<Output = Result<(), OracleError>> + Send + '_>> {
        // Clone the price eagerly so the async block only borrows `self`,
        // matching the trait's lifetime contract (`BoxFuture<'_, ...>`).
        let price_owned = price.clone();

        Box::pin(async move {
            let mut first_error: Option<OracleError> = None;

            for publisher in &self.publishers {
                if let Err(e) = publisher.publish(&price_owned).await {
                    tracing::warn!(error = %e, "fan-out publisher encountered error");
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }

            match first_error {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use market2nats_domain::{CanonicalSymbol, Price, Quantity, Timestamp, VenueId};
    use rust_decimal_macros::dec;

    use super::*;
    use crate::domain::strategies::AggregationStrategyKind;
    use crate::domain::types::{OracleConfidence, PriceSource};

    /// Mock publisher that counts calls and optionally returns an error.
    struct MockPublisher {
        call_count: AtomicUsize,
        should_fail: bool,
    }

    impl MockPublisher {
        fn new(should_fail: bool) -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                should_fail,
            }
        }

        fn calls(&self) -> usize {
            self.call_count.load(Ordering::Relaxed)
        }
    }

    impl OraclePublisher for MockPublisher {
        fn publish(
            &self,
            _price: &OraclePrice,
        ) -> Pin<Box<dyn Future<Output = Result<(), OracleError>> + Send + '_>> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            let fail = self.should_fail;
            Box::pin(async move {
                if fail {
                    Err(OracleError::nats("mock publish error"))
                } else {
                    Ok(())
                }
            })
        }
    }

    fn make_oracle_price() -> OraclePrice {
        OraclePrice {
            symbol: CanonicalSymbol::try_new("BTC/USDT").expect("valid symbol"),
            price: Price::try_new(dec!(50000)).expect("valid price"),
            timestamp: Timestamp::now(),
            sources: vec![PriceSource {
                venue: VenueId::try_new("binance").expect("valid venue"),
                price: Price::try_new(dec!(50000)).expect("valid price"),
                quantity: Quantity::try_new(dec!(1)).expect("valid quantity"),
                timestamp: Timestamp::now(),
                age_ms: 100,
            }],
            strategy: AggregationStrategyKind::Median,
            confidence: OracleConfidence::High,
        }
    }

    #[tokio::test]
    async fn test_fan_out_calls_all_publishers() {
        let pub1 = Arc::new(MockPublisher::new(false));
        let pub2 = Arc::new(MockPublisher::new(false));

        let fan_out = FanOutPublisher::new(vec![
            Arc::clone(&pub1) as Arc<dyn OraclePublisher>,
            Arc::clone(&pub2) as Arc<dyn OraclePublisher>,
        ]);

        let price = make_oracle_price();
        let result = fan_out.publish(&price).await;
        assert!(result.is_ok());
        assert_eq!(pub1.calls(), 1);
        assert_eq!(pub2.calls(), 1);
    }

    #[tokio::test]
    async fn test_fan_out_returns_first_error_but_calls_all() {
        let pub1 = Arc::new(MockPublisher::new(true)); // will fail
        let pub2 = Arc::new(MockPublisher::new(false)); // will succeed

        let fan_out = FanOutPublisher::new(vec![
            Arc::clone(&pub1) as Arc<dyn OraclePublisher>,
            Arc::clone(&pub2) as Arc<dyn OraclePublisher>,
        ]);

        let price = make_oracle_price();
        let result = fan_out.publish(&price).await;
        assert!(result.is_err());
        // Both publishers must have been called.
        assert_eq!(pub1.calls(), 1);
        assert_eq!(pub2.calls(), 1);
    }

    #[tokio::test]
    async fn test_fan_out_empty_publishers_succeeds() {
        let fan_out = FanOutPublisher::new(Vec::new());
        let price = make_oracle_price();
        let result = fan_out.publish(&price).await;
        assert!(result.is_ok());
    }
}
