use rust_decimal::Decimal;

use super::strategies::median::compute_median;
use super::types::PriceSource;

/// Basis points multiplier: 10000.
const BPS_MULTIPLIER: Decimal = Decimal::from_parts(10000, 0, 0, false, 0);

/// A filter that can discard price sources before aggregation.
pub trait PriceFilter: Send + Sync {
    /// Applies this filter to the given sources, returning only those that pass.
    fn apply(&self, sources: &[PriceSource]) -> Vec<PriceSource>;
}

/// Discards price sources older than a configured age threshold.
#[derive(Debug, Clone, Copy)]
pub struct StalenessFilter {
    /// Maximum allowed age in milliseconds.
    max_age_ms: u64,
}

impl StalenessFilter {
    /// Creates a new `StalenessFilter` that discards sources with
    /// `age_ms > max_age_ms`.
    #[must_use]
    #[inline]
    pub fn new(max_age_ms: u64) -> Self {
        Self { max_age_ms }
    }

    /// Returns the configured maximum age in milliseconds.
    #[must_use]
    #[inline]
    pub fn max_age_ms(&self) -> u64 {
        self.max_age_ms
    }
}

impl PriceFilter for StalenessFilter {
    fn apply(&self, sources: &[PriceSource]) -> Vec<PriceSource> {
        sources
            .iter()
            .filter(|s| s.age_ms <= self.max_age_ms)
            .cloned()
            .collect()
    }
}

/// Discards price sources that deviate too far from the median price.
///
/// Deviation is computed as `|price - median| / median * 10000` (basis points).
/// Sources exceeding `max_deviation_bps` are discarded.
///
/// If the median cannot be computed (empty sources) or is zero, all sources
/// are returned unchanged.
#[derive(Debug, Clone, Copy)]
pub struct OutlierFilter {
    /// Maximum allowed deviation from the median, in basis points.
    max_deviation_bps: u64,
}

impl OutlierFilter {
    /// Creates a new `OutlierFilter` that discards sources deviating more
    /// than `max_deviation_bps` basis points from the median.
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

impl PriceFilter for OutlierFilter {
    fn apply(&self, sources: &[PriceSource]) -> Vec<PriceSource> {
        if sources.is_empty() {
            return Vec::new();
        }

        let median = match compute_median(sources) {
            Ok(m) => m,
            Err(_) => return sources.to_vec(),
        };

        let median_val = median.value();
        if median_val.is_zero() {
            return sources.to_vec();
        }

        let threshold = Decimal::from(self.max_deviation_bps);

        sources
            .iter()
            .filter(|s| {
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
    }
}

#[cfg(test)]
mod tests {
    use market2nats_domain::{Price, Quantity, Timestamp, VenueId};
    use rust_decimal_macros::dec;

    use super::*;

    fn make_source(
        venue: &str,
        price: rust_decimal::Decimal,
        quantity: rust_decimal::Decimal,
        age_ms: u64,
    ) -> PriceSource {
        PriceSource {
            venue: VenueId::try_new(venue).unwrap(),
            price: Price::try_new(price).unwrap(),
            quantity: Quantity::try_new(quantity).unwrap(),
            timestamp: Timestamp::new(1_700_000_000_000),
            age_ms,
        }
    }

    #[test]
    fn test_staleness_filter_discards_old() {
        let filter = StalenessFilter::new(1000);
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 500),
            make_source("b", dec!(200), dec!(1), 1500),
            make_source("c", dec!(300), dec!(1), 800),
        ];
        let result = filter.apply(&sources);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].venue.as_str(), "a");
        assert_eq!(result[1].venue.as_str(), "c");
    }

    #[test]
    fn test_staleness_filter_keeps_recent() {
        let filter = StalenessFilter::new(5000);
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 100),
            make_source("b", dec!(200), dec!(1), 200),
        ];
        let result = filter.apply(&sources);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_staleness_filter_exact_boundary_kept() {
        let filter = StalenessFilter::new(1000);
        let sources = vec![make_source("a", dec!(100), dec!(1), 1000)];
        let result = filter.apply(&sources);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_outlier_filter_discards_deviants() {
        // Median of 100, 101, 102, 500 = avg(101,102) = 101.5
        // 500 deviates by ~3926 bps >> 200 bps threshold
        let filter = OutlierFilter::new(200);
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 0),
            make_source("b", dec!(101), dec!(1), 0),
            make_source("c", dec!(102), dec!(1), 0),
            make_source("d", dec!(500), dec!(1), 0),
        ];
        let result = filter.apply(&sources);
        assert_eq!(result.len(), 3);
        // 500 should be removed
        for s in &result {
            assert_ne!(s.price.value(), dec!(500));
        }
    }

    #[test]
    fn test_outlier_filter_keeps_close_prices() {
        let filter = OutlierFilter::new(500);
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 0),
            make_source("b", dec!(101), dec!(1), 0),
            make_source("c", dec!(102), dec!(1), 0),
        ];
        let result = filter.apply(&sources);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_outlier_filter_empty_sources() {
        let filter = OutlierFilter::new(100);
        let result = filter.apply(&[]);
        assert!(result.is_empty());
    }
}
