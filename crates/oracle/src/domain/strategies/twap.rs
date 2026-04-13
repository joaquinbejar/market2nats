use market2nats_domain::Price;
use rust_decimal::Decimal;

use super::{AggregationStrategy, AggregationStrategyKind};
use crate::domain::error::OracleError;
use crate::domain::types::PriceSource;

/// Time-weighted average price strategy.
///
/// Sources are weighted by their recency within a time window.
/// Weight for each source = `window_ms - age_ms` (as `Decimal`).
/// Sources outside the window (age_ms > window_ms) are discarded.
///
/// Result = `sum(price * weight) / sum(weight)`.
///
/// Rounding: `Decimal`'s default (banker's rounding) applies to the final
/// division. No additional truncation is performed.
#[derive(Debug, Clone, Copy)]
pub struct TwapStrategy {
    /// Time window in milliseconds. Sources older than this are discarded.
    window_ms: u64,
}

impl TwapStrategy {
    /// Creates a new `TwapStrategy` with the given time window in milliseconds.
    #[must_use]
    #[inline]
    pub fn new(window_ms: u64) -> Self {
        Self { window_ms }
    }

    /// Returns the configured window in milliseconds.
    #[must_use]
    #[inline]
    pub fn window_ms(&self) -> u64 {
        self.window_ms
    }
}

impl AggregationStrategy for TwapStrategy {
    #[inline]
    fn kind(&self) -> AggregationStrategyKind {
        AggregationStrategyKind::Twap
    }

    fn aggregate(&self, sources: &[PriceSource]) -> Result<Price, OracleError> {
        if sources.is_empty() {
            return Err(OracleError::insufficient_sources(1, 0));
        }

        let mut weighted_sum = Decimal::ZERO;
        let mut weight_total = Decimal::ZERO;

        for source in sources {
            if source.age_ms > self.window_ms {
                continue;
            }

            // weight = window_ms - age_ms (both non-negative, and age_ms <= window_ms)
            let weight = Decimal::from(self.window_ms)
                .checked_sub(Decimal::from(source.age_ms))
                .ok_or_else(|| {
                    OracleError::arithmetic_overflow("twap weight subtraction overflow")
                })?;

            let price_times_weight =
                source.price.value().checked_mul(weight).ok_or_else(|| {
                    OracleError::arithmetic_overflow("twap price * weight overflow")
                })?;

            weighted_sum = weighted_sum
                .checked_add(price_times_weight)
                .ok_or_else(|| OracleError::arithmetic_overflow("twap weighted sum overflow"))?;

            weight_total = weight_total
                .checked_add(weight)
                .ok_or_else(|| OracleError::arithmetic_overflow("twap weight total overflow"))?;
        }

        if weight_total.is_zero() {
            return Err(OracleError::insufficient_sources(1, 0));
        }

        let result = weighted_sum
            .checked_div(weight_total)
            .ok_or_else(|| OracleError::division_by_zero("twap weighted average"))?;

        Price::try_new(result).map_err(OracleError::from)
    }
}

#[cfg(test)]
mod tests {
    use market2nats_domain::{Quantity, Timestamp, VenueId};
    use rust_decimal_macros::dec;

    use super::*;

    fn make_source(venue: &str, price: Decimal, quantity: Decimal, age_ms: u64) -> PriceSource {
        PriceSource {
            venue: VenueId::try_new(venue).unwrap(),
            price: Price::try_new(price).unwrap(),
            quantity: Quantity::try_new(quantity).unwrap(),
            timestamp: Timestamp::new(1_700_000_000_000),
            age_ms,
        }
    }

    #[test]
    fn test_twap_recent_weighted_more() {
        // window = 1000ms
        // source A: price=100, age=100  -> weight=900
        // source B: price=200, age=900  -> weight=100
        // result = (100*900 + 200*100) / (900+100) = (90000+20000)/1000 = 110
        let strategy = TwapStrategy::new(1000);
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 100),
            make_source("b", dec!(200), dec!(1), 900),
        ];
        let result = strategy.aggregate(&sources).unwrap();
        assert_eq!(result.value(), dec!(110));
    }

    #[test]
    fn test_twap_all_outside_window_returns_error() {
        let strategy = TwapStrategy::new(1000);
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 1500),
            make_source("b", dec!(200), dec!(1), 2000),
        ];
        let result = strategy.aggregate(&sources);
        assert!(result.is_err());
    }

    #[test]
    fn test_twap_equal_ages_returns_average() {
        // window = 1000ms, both age=500 -> weight=500 each
        // result = (100*500 + 200*500) / (500+500) = 150000/1000 = 150
        let strategy = TwapStrategy::new(1000);
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 500),
            make_source("b", dec!(200), dec!(1), 500),
        ];
        let result = strategy.aggregate(&sources).unwrap();
        assert_eq!(result.value(), dec!(150));
    }

    #[test]
    fn test_twap_empty_sources_returns_error() {
        let strategy = TwapStrategy::new(1000);
        let result = strategy.aggregate(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_twap_source_at_exact_window_boundary() {
        // age_ms == window_ms => weight = 0, effectively excluded
        let strategy = TwapStrategy::new(1000);
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 1000),
            make_source("b", dec!(200), dec!(1), 500),
        ];
        let result = strategy.aggregate(&sources).unwrap();
        // only source b survives with weight 500, so result = 200
        assert_eq!(result.value(), dec!(200));
    }
}
