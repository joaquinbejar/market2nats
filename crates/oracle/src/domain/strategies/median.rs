use market2nats_domain::Price;
use rust_decimal::Decimal;

use super::{AggregationStrategy, AggregationStrategyKind};
use crate::domain::error::OracleError;
use crate::domain::types::PriceSource;

/// Computes the median price from a set of sources.
///
/// - Odd count: picks the middle element.
/// - Even count: averages the two middle elements using checked arithmetic.
///   Rounding strategy: `Decimal`'s default (banker's rounding) applies to the
///   internal representation; no explicit truncation is performed since division
///   by 2 is exact for sums of two `Decimal` values.
#[derive(Debug, Clone, Copy)]
pub struct MedianStrategy;

impl MedianStrategy {
    /// Creates a new `MedianStrategy`.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self
    }
}

impl Default for MedianStrategy {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Two, used as divisor when averaging two middle elements.
const TWO: Decimal = Decimal::from_parts(2, 0, 0, false, 0);

/// Computes the median price from a slice of `PriceSource`.
///
/// This is extracted as a free function so it can be reused by
/// `MedianFilteredStrategy` without requiring trait object dispatch.
///
/// # Errors
///
/// Returns `OracleError::InsufficientSources` if `sources` is empty.
/// Returns `OracleError::ArithmeticOverflow` if checked add or divide fails.
#[must_use = "returns a Result that must be handled"]
pub fn compute_median(sources: &[PriceSource]) -> Result<Price, OracleError> {
    if sources.is_empty() {
        return Err(OracleError::insufficient_sources(1, 0));
    }

    let mut prices: Vec<Decimal> = sources.iter().map(|s| s.price.value()).collect();
    prices.sort();

    let mid = prices.len() / 2;

    if prices.len() % 2 == 1 {
        // Odd: pick middle. Safe because prices is non-empty and mid < len.
        let median = prices
            .get(mid)
            .ok_or_else(|| OracleError::arithmetic_overflow("median index out of bounds"))?;
        Price::try_new(*median).map_err(OracleError::from)
    } else {
        // Even: average the two middle elements.
        let left = prices
            .get(mid.wrapping_sub(1))
            .ok_or_else(|| OracleError::arithmetic_overflow("median left index out of bounds"))?;
        let right = prices
            .get(mid)
            .ok_or_else(|| OracleError::arithmetic_overflow("median right index out of bounds"))?;
        let sum = left
            .checked_add(*right)
            .ok_or_else(|| OracleError::arithmetic_overflow("median sum overflow"))?;
        let avg = sum
            .checked_div(TWO)
            .ok_or_else(|| OracleError::division_by_zero("median average"))?;
        Price::try_new(avg).map_err(OracleError::from)
    }
}

impl AggregationStrategy for MedianStrategy {
    #[inline]
    fn kind(&self) -> AggregationStrategyKind {
        AggregationStrategyKind::Median
    }

    fn aggregate(&self, sources: &[PriceSource]) -> Result<Price, OracleError> {
        compute_median(sources)
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
    fn test_median_odd_count_picks_middle() {
        let strategy = MedianStrategy::new();
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 0),
            make_source("b", dec!(200), dec!(1), 0),
            make_source("c", dec!(300), dec!(1), 0),
        ];
        let result = strategy.aggregate(&sources).unwrap();
        assert_eq!(result.value(), dec!(200));
    }

    #[test]
    fn test_median_even_count_averages_middle_pair() {
        let strategy = MedianStrategy::new();
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 0),
            make_source("b", dec!(200), dec!(1), 0),
            make_source("c", dec!(300), dec!(1), 0),
            make_source("d", dec!(400), dec!(1), 0),
        ];
        let result = strategy.aggregate(&sources).unwrap();
        assert_eq!(result.value(), dec!(250));
    }

    #[test]
    fn test_median_single_source_returns_that_price() {
        let strategy = MedianStrategy::new();
        let sources = vec![make_source("a", dec!(42), dec!(1), 0)];
        let result = strategy.aggregate(&sources).unwrap();
        assert_eq!(result.value(), dec!(42));
    }

    #[test]
    fn test_median_empty_sources_returns_error() {
        let strategy = MedianStrategy::new();
        let result = strategy.aggregate(&[]);
        assert!(result.is_err());
        match result.unwrap_err() {
            OracleError::InsufficientSources {
                required,
                available,
            } => {
                assert_eq!(required, 1);
                assert_eq!(available, 0);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_median_unsorted_input_still_correct() {
        let strategy = MedianStrategy::new();
        let sources = vec![
            make_source("a", dec!(300), dec!(1), 0),
            make_source("b", dec!(100), dec!(1), 0),
            make_source("c", dec!(200), dec!(1), 0),
        ];
        let result = strategy.aggregate(&sources).unwrap();
        assert_eq!(result.value(), dec!(200));
    }
}
