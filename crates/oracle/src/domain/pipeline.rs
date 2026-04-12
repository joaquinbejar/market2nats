use market2nats_domain::{CanonicalSymbol, Timestamp};
use rust_decimal::Decimal;

use super::error::OracleError;
use super::filters::PriceFilter;
use super::strategies::AggregationStrategy;
use super::types::{OracleConfidence, OraclePrice, PriceSource};

/// Basis points multiplier: 10000.
const BPS_MULTIPLIER: Decimal = Decimal::from_parts(10000, 0, 0, false, 0);

/// Orchestrates price filtering and aggregation into an `OraclePrice`.
///
/// The pipeline applies a sequence of filters to the raw sources, checks
/// that enough sources survive, then delegates to an aggregation strategy
/// to compute the final price.
pub struct OraclePipeline {
    /// Filters applied in order to the raw sources.
    filters: Vec<Box<dyn PriceFilter>>,
    /// The aggregation strategy used to compute the final price.
    strategy: Box<dyn AggregationStrategy>,
    /// Minimum number of sources required after filtering.
    min_sources: usize,
}

impl OraclePipeline {
    /// Creates a new `OraclePipeline`.
    ///
    /// # Arguments
    ///
    /// * `filters` - Filters applied in order before aggregation.
    /// * `strategy` - The aggregation strategy to compute the final price.
    /// * `min_sources` - Minimum number of sources required after filtering.
    #[must_use]
    pub fn new(
        filters: Vec<Box<dyn PriceFilter>>,
        strategy: Box<dyn AggregationStrategy>,
        min_sources: usize,
    ) -> Self {
        Self {
            filters,
            strategy,
            min_sources,
        }
    }

    /// Computes an aggregated oracle price from the given sources.
    ///
    /// This is a pure function: the caller supplies `computed_at` so the domain
    /// layer never reaches for wall-clock time.
    ///
    /// 1. Clones sources into a working vector.
    /// 2. Applies each filter in order.
    /// 3. Checks for sufficient surviving sources.
    /// 4. Delegates to the aggregation strategy.
    /// 5. Computes spread and confidence.
    ///
    /// # Errors
    ///
    /// Returns `OracleError::AllSourcesStale` if no sources survive filtering.
    /// Returns `OracleError::InsufficientSources` if too few sources remain.
    /// Returns `OracleError` from the aggregation strategy on arithmetic failure.
    #[must_use = "returns a Result that must be handled"]
    pub fn compute(
        &self,
        symbol: &CanonicalSymbol,
        sources: &[PriceSource],
        computed_at: Timestamp,
    ) -> Result<OraclePrice, OracleError> {
        // Apply filters in order.
        let mut filtered: Vec<PriceSource> = sources.to_vec();
        for filter in &self.filters {
            filtered = filter.apply(&filtered);
        }

        if filtered.is_empty() {
            return Err(OracleError::all_sources_stale(symbol.as_str()));
        }

        if filtered.len() < self.min_sources {
            return Err(OracleError::insufficient_sources(
                self.min_sources,
                filtered.len(),
            ));
        }

        // Aggregate.
        let price = self.strategy.aggregate(&filtered)?;

        // Compute spread in basis points from min/max source prices.
        let spread_bps = compute_spread_bps(&filtered)?;

        // Determine confidence.
        let confidence = OracleConfidence::compute(filtered.len(), spread_bps);

        Ok(OraclePrice {
            symbol: symbol.clone(),
            price,
            timestamp: computed_at,
            sources: filtered,
            strategy: self.strategy.kind(),
            confidence,
        })
    }
}

/// Computes the spread in basis points between the minimum and maximum source prices.
///
/// spread_bps = (max - min) / min * 10000
///
/// If all prices are zero, returns zero. If min is zero but max is positive,
/// returns 10000 bps (100%) as a sentinel for undefined/infinite spread.
///
/// Rounding: `Decimal`'s default (banker's rounding) applies. No additional truncation.
///
/// # Errors
///
/// Returns `OracleError::ArithmeticOverflow` if checked arithmetic fails.
#[must_use = "returns a Result that must be handled"]
fn compute_spread_bps(sources: &[PriceSource]) -> Result<Decimal, OracleError> {
    if sources.is_empty() {
        return Ok(Decimal::ZERO);
    }

    let min_price = sources
        .iter()
        .map(|s| s.price.value())
        .min()
        .unwrap_or(Decimal::ZERO);

    let max_price = sources
        .iter()
        .map(|s| s.price.value())
        .max()
        .unwrap_or(Decimal::ZERO);

    if min_price.is_zero() {
        // If max is also zero, spread is genuinely zero.
        // If max > 0, the spread is undefined (division by zero); return 10000 bps (100%)
        // as a sentinel indicating maximum uncertainty.
        return if max_price.is_zero() {
            Ok(Decimal::ZERO)
        } else {
            Ok(Decimal::from(10000))
        };
    }

    let diff = max_price
        .checked_sub(min_price)
        .ok_or_else(|| OracleError::arithmetic_overflow("spread diff overflow"))?;

    let numerator = diff
        .checked_mul(BPS_MULTIPLIER)
        .ok_or_else(|| OracleError::arithmetic_overflow("spread numerator overflow"))?;

    let bps = numerator
        .checked_div(min_price)
        .ok_or_else(|| OracleError::division_by_zero("spread bps division"))?;

    Ok(bps)
}

/// Computes the spread in basis points between given min and max prices.
///
/// Exposed for testing. Production code should use the pipeline.
///
/// # Errors
///
/// Returns `OracleError` on arithmetic failure.
#[cfg(test)]
fn compute_spread_bps_from_prices(
    min_price: market2nats_domain::Price,
    max_price: market2nats_domain::Price,
) -> Result<Decimal, OracleError> {
    let min_val = min_price.value();
    let max_val = max_price.value();

    if min_val.is_zero() {
        return if max_val.is_zero() {
            Ok(Decimal::ZERO)
        } else {
            Ok(Decimal::from(10000))
        };
    }

    let diff = max_val
        .checked_sub(min_val)
        .ok_or_else(|| OracleError::arithmetic_overflow("spread diff overflow"))?;

    let numerator = diff
        .checked_mul(BPS_MULTIPLIER)
        .ok_or_else(|| OracleError::arithmetic_overflow("spread numerator overflow"))?;

    numerator
        .checked_div(min_val)
        .ok_or_else(|| OracleError::division_by_zero("spread bps division"))
}

#[cfg(test)]
mod tests {
    use market2nats_domain::{CanonicalSymbol, Price, Quantity, Timestamp, VenueId};
    use rust_decimal_macros::dec;

    use super::*;
    use crate::domain::filters::StalenessFilter;
    use crate::domain::strategies::median::MedianStrategy;

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
    fn test_pipeline_compute_happy_path() {
        let pipeline = OraclePipeline::new(
            vec![Box::new(StalenessFilter::new(5000))],
            Box::new(MedianStrategy::new()),
            1,
        );
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        let sources = vec![
            make_source("binance", dec!(50000), dec!(1), 100),
            make_source("kraken", dec!(50010), dec!(1), 200),
            make_source("coinbase", dec!(50005), dec!(1), 150),
        ];
        let now = Timestamp::new(1_700_000_000_000);
        let result = pipeline.compute(&symbol, &sources, now).unwrap();
        assert_eq!(result.price.value(), dec!(50005));
        assert_eq!(result.sources.len(), 3);
        assert_eq!(result.symbol.as_str(), "BTC/USDT");
        assert_eq!(result.timestamp, now);
    }

    #[test]
    fn test_pipeline_compute_insufficient_after_filter() {
        let pipeline = OraclePipeline::new(
            vec![Box::new(StalenessFilter::new(1000))],
            Box::new(MedianStrategy::new()),
            3,
        );
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        let sources = vec![
            make_source("binance", dec!(50000), dec!(1), 500),
            make_source("kraken", dec!(50010), dec!(1), 2000),
            make_source("coinbase", dec!(50005), dec!(1), 3000),
        ];
        let now = Timestamp::new(1_700_000_000_000);
        let result = pipeline.compute(&symbol, &sources, now);
        assert!(result.is_err());
        match result.unwrap_err() {
            OracleError::InsufficientSources {
                required,
                available,
            } => {
                assert_eq!(required, 3);
                assert_eq!(available, 1);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_pipeline_compute_all_stale() {
        let pipeline = OraclePipeline::new(
            vec![Box::new(StalenessFilter::new(100))],
            Box::new(MedianStrategy::new()),
            1,
        );
        let symbol = CanonicalSymbol::try_new("ETH/USDT").unwrap();
        let sources = vec![
            make_source("binance", dec!(3000), dec!(1), 500),
            make_source("kraken", dec!(3010), dec!(1), 600),
        ];
        let now = Timestamp::new(1_700_000_000_000);
        let result = pipeline.compute(&symbol, &sources, now);
        assert!(result.is_err());
        match result.unwrap_err() {
            OracleError::AllSourcesStale { symbol } => {
                assert_eq!(symbol, "ETH/USDT");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_pipeline_compute_single_source_above_minimum() {
        let pipeline = OraclePipeline::new(Vec::new(), Box::new(MedianStrategy::new()), 1);
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        let sources = vec![make_source("binance", dec!(50000), dec!(1), 100)];
        let now = Timestamp::new(1_700_000_000_000);
        let result = pipeline.compute(&symbol, &sources, now).unwrap();
        assert_eq!(result.price.value(), dec!(50000));
        assert_eq!(result.sources.len(), 1);
    }

    #[test]
    fn test_pipeline_no_filters() {
        let pipeline = OraclePipeline::new(Vec::new(), Box::new(MedianStrategy::new()), 2);
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 0),
            make_source("b", dec!(200), dec!(1), 0),
        ];
        let now = Timestamp::new(1_700_000_000_000);
        let result = pipeline.compute(&symbol, &sources, now).unwrap();
        assert_eq!(result.price.value(), dec!(150));
    }

    #[test]
    fn test_compute_spread_bps_from_prices_same_price() {
        let min = Price::try_new(dec!(100)).unwrap();
        let max = Price::try_new(dec!(100)).unwrap();
        let bps = compute_spread_bps_from_prices(min, max).unwrap();
        assert_eq!(bps, dec!(0));
    }

    #[test]
    fn test_compute_spread_bps_from_prices_known_spread() {
        // spread = (101 - 100) / 100 * 10000 = 100 bps
        let min = Price::try_new(dec!(100)).unwrap();
        let max = Price::try_new(dec!(101)).unwrap();
        let bps = compute_spread_bps_from_prices(min, max).unwrap();
        assert_eq!(bps, dec!(100));
    }

    #[test]
    fn test_compute_spread_bps_zero_min_nonzero_max_returns_max_spread() {
        let min = Price::try_new(dec!(0)).unwrap();
        let max = Price::try_new(dec!(100)).unwrap();
        let bps = compute_spread_bps_from_prices(min, max).unwrap();
        assert_eq!(bps, dec!(10000));
    }

    #[test]
    fn test_compute_spread_bps_both_zero_returns_zero() {
        let min = Price::try_new(dec!(0)).unwrap();
        let max = Price::try_new(dec!(0)).unwrap();
        let bps = compute_spread_bps_from_prices(min, max).unwrap();
        assert_eq!(bps, dec!(0));
    }
}
