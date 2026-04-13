use market2nats_domain::Price;
use rust_decimal::Decimal;

use super::{AggregationStrategy, AggregationStrategyKind};
use crate::domain::error::OracleError;
use crate::domain::types::PriceSource;

/// Volume-weighted average price strategy.
///
/// Result = `sum(price * quantity) / sum(quantity)`.
///
/// Rounding: `Decimal`'s default (banker's rounding) applies to the final
/// division. No additional truncation is performed.
#[derive(Debug, Clone, Copy)]
pub struct VwapStrategy;

impl VwapStrategy {
    /// Creates a new `VwapStrategy`.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self
    }
}

impl Default for VwapStrategy {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl AggregationStrategy for VwapStrategy {
    #[inline]
    fn kind(&self) -> AggregationStrategyKind {
        AggregationStrategyKind::Vwap
    }

    fn aggregate(&self, sources: &[PriceSource]) -> Result<Price, OracleError> {
        if sources.is_empty() {
            return Err(OracleError::insufficient_sources(1, 0));
        }

        let mut weighted_sum = Decimal::ZERO;
        let mut quantity_total = Decimal::ZERO;

        for source in sources {
            let price_times_qty = source
                .price
                .value()
                .checked_mul(source.quantity.value())
                .ok_or_else(|| {
                    OracleError::arithmetic_overflow("vwap price * quantity overflow")
                })?;

            weighted_sum = weighted_sum
                .checked_add(price_times_qty)
                .ok_or_else(|| OracleError::arithmetic_overflow("vwap weighted sum overflow"))?;

            quantity_total = quantity_total
                .checked_add(source.quantity.value())
                .ok_or_else(|| OracleError::arithmetic_overflow("vwap quantity total overflow"))?;
        }

        if quantity_total.is_zero() {
            return Err(OracleError::division_by_zero("vwap total quantity is zero"));
        }

        let result = weighted_sum
            .checked_div(quantity_total)
            .ok_or_else(|| OracleError::division_by_zero("vwap weighted average"))?;

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
    fn test_vwap_weighted_by_quantity() {
        // price=100 qty=3, price=200 qty=1
        // result = (100*3 + 200*1) / (3+1) = 500/4 = 125
        let strategy = VwapStrategy::new();
        let sources = vec![
            make_source("a", dec!(100), dec!(3), 0),
            make_source("b", dec!(200), dec!(1), 0),
        ];
        let result = strategy.aggregate(&sources).unwrap();
        assert_eq!(result.value(), dec!(125));
    }

    #[test]
    fn test_vwap_equal_quantities_is_average() {
        // price=100 qty=1, price=200 qty=1
        // result = (100+200) / 2 = 150
        let strategy = VwapStrategy::new();
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 0),
            make_source("b", dec!(200), dec!(1), 0),
        ];
        let result = strategy.aggregate(&sources).unwrap();
        assert_eq!(result.value(), dec!(150));
    }

    #[test]
    fn test_vwap_zero_total_quantity_returns_error() {
        let strategy = VwapStrategy::new();
        let sources = vec![
            make_source("a", dec!(100), dec!(0), 0),
            make_source("b", dec!(200), dec!(0), 0),
        ];
        let result = strategy.aggregate(&sources);
        assert!(result.is_err());
        match result.unwrap_err() {
            OracleError::DivisionByZero { .. } => {}
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_vwap_empty_sources_returns_error() {
        let strategy = VwapStrategy::new();
        let result = strategy.aggregate(&[]);
        assert!(result.is_err());
        match result.unwrap_err() {
            OracleError::InsufficientSources { .. } => {}
            other => panic!("unexpected error: {other}"),
        }
    }
}
