use market2nats_domain::Price;
use rust_decimal::Decimal;

use super::median::compute_median;
use super::{AggregationStrategy, AggregationStrategyKind};
use crate::domain::error::OracleError;
use crate::domain::types::PriceSource;

/// Basis points multiplier: 10000.
const BPS_MULTIPLIER: Decimal = Decimal::from_parts(10000, 0, 0, false, 0);

/// Median with outlier removal strategy.
///
/// 1. Compute the initial median of all sources.
/// 2. Discard any source whose price deviates from the median by more than
///    `max_deviation_bps` basis points.
/// 3. Recompute the median on the surviving sources.
///
/// Rounding: deviation is computed as `|price - median| / median * 10000`.
/// `Decimal`'s default (banker's rounding) applies. No additional truncation.
#[derive(Debug, Clone, Copy)]
pub struct MedianFilteredStrategy {
    /// Maximum allowed deviation from the initial median, in basis points.
    max_deviation_bps: u64,
}

impl MedianFilteredStrategy {
    /// Creates a new `MedianFilteredStrategy` with the given maximum
    /// deviation threshold in basis points.
    #[must_use]
    #[inline]
    pub fn new(max_deviation_bps: u64) -> Self {
        Self { max_deviation_bps }
    }

    /// Returns the configured maximum deviation in basis points.
    #[must_use]
    #[inline]
    pub fn max_deviation_bps(&self) -> u64 {
        self.max_deviation_bps
    }
}

impl AggregationStrategy for MedianFilteredStrategy {
    #[inline]
    fn kind(&self) -> AggregationStrategyKind {
        AggregationStrategyKind::MedianFiltered
    }

    fn aggregate(&self, sources: &[PriceSource]) -> Result<Price, OracleError> {
        if sources.is_empty() {
            return Err(OracleError::insufficient_sources(1, 0));
        }

        // Step 1: compute initial median.
        let initial_median = compute_median(sources)?;
        let median_val = initial_median.value();
        let threshold = Decimal::from(self.max_deviation_bps);

        // Step 2: filter sources that deviate too far.
        let filtered: Vec<PriceSource> = if median_val.is_zero() {
            // If median is zero, keep only sources with zero price.
            sources
                .iter()
                .filter(|s| s.price.value().is_zero())
                .cloned()
                .collect()
        } else {
            sources
                .iter()
                .filter(|s| {
                    // deviation_bps = |price - median| / median * 10000
                    let diff = if s.price.value() >= median_val {
                        s.price.value().checked_sub(median_val)
                    } else {
                        median_val.checked_sub(s.price.value())
                    };

                    match diff {
                        Some(abs_diff) => {
                            let numerator = abs_diff.checked_mul(BPS_MULTIPLIER);
                            match numerator {
                                Some(n) => match n.checked_div(median_val) {
                                    Some(bps) => bps <= threshold,
                                    None => false,
                                },
                                None => false,
                            }
                        }
                        None => false,
                    }
                })
                .cloned()
                .collect()
        };

        if filtered.is_empty() {
            return Err(OracleError::insufficient_sources(1, 0));
        }

        // Step 3: recompute median on survivors.
        compute_median(&filtered)
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
    fn test_median_filtered_removes_outlier() {
        // Sources: 100, 101, 102, 500
        // Initial median of 4 = avg(101,102) = 101.5
        // 500 deviates by |500-101.5|/101.5 * 10000 ~= 3926 bps >> 200 bps threshold
        // 100 deviates by |100-101.5|/101.5 * 10000 ~= 148 bps < 200 bps -> kept
        // After filter: 100, 101, 102 (3 sources, odd)
        // Final median: 101
        let strategy = MedianFilteredStrategy::new(200);
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 0),
            make_source("b", dec!(101), dec!(1), 0),
            make_source("c", dec!(102), dec!(1), 0),
            make_source("d", dec!(500), dec!(1), 0),
        ];
        let result = strategy.aggregate(&sources).unwrap();
        assert_eq!(result.value(), dec!(101));
    }

    #[test]
    fn test_median_filtered_all_close_keeps_all() {
        // Sources: 100, 101, 102, 103
        // Initial median = avg(101,102) = 101.5
        // Max deviation from 101.5: 103 -> |103-101.5|/101.5*10000 = ~148 bps
        // With threshold of 200 bps, all pass.
        // Final median = 101.5
        let strategy = MedianFilteredStrategy::new(200);
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 0),
            make_source("b", dec!(101), dec!(1), 0),
            make_source("c", dec!(102), dec!(1), 0),
            make_source("d", dec!(103), dec!(1), 0),
        ];
        let result = strategy.aggregate(&sources).unwrap();
        assert_eq!(result.value(), dec!(101.5));
    }

    #[test]
    fn test_median_filtered_empty_sources_returns_error() {
        let strategy = MedianFilteredStrategy::new(100);
        let result = strategy.aggregate(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_median_filtered_all_outliers_returns_error() {
        // With threshold 0 bps, only exact median survives.
        // If sources are 100, 200, the median is 150 and both deviate.
        let strategy = MedianFilteredStrategy::new(0);
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 0),
            make_source("b", dec!(200), dec!(1), 0),
        ];
        let result = strategy.aggregate(&sources);
        assert!(result.is_err());
    }
}
