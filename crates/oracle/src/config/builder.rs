//! Pipeline builder: constructs an `OraclePipeline` from configuration.

use crate::domain::{
    AggregationStrategyKind, MedianFilteredStrategy, MedianStrategy, OracleError, OraclePipeline,
    OutlierFilter, StalenessFilter, TwapStrategy, VwapStrategy,
};

use super::model::PipelineConfig;

/// Builds an `OraclePipeline` from the given pipeline configuration.
///
/// Constructs the filter chain (`StalenessFilter`, `OutlierFilter`) and
/// the aggregation strategy based on the configured strategy name.
///
/// # Errors
///
/// Returns `OracleError::Domain` if the strategy name is not recognized.
pub fn build_pipeline(config: &PipelineConfig) -> Result<OraclePipeline, OracleError> {
    let kind = AggregationStrategyKind::from_str_config(&config.strategy)?;

    // MedianFilteredStrategy already performs outlier removal internally,
    // so we only add StalenessFilter to avoid double-filtering.
    let filters: Vec<Box<dyn crate::domain::filters::PriceFilter>> =
        if kind == AggregationStrategyKind::MedianFiltered {
            vec![Box::new(StalenessFilter::new(config.staleness_max_ms))]
        } else {
            vec![
                Box::new(StalenessFilter::new(config.staleness_max_ms)),
                Box::new(OutlierFilter::new(config.outlier_max_deviation_bps)),
            ]
        };

    let strategy: Box<dyn crate::domain::strategies::AggregationStrategy> = match kind {
        AggregationStrategyKind::Median => Box::new(MedianStrategy::new()),
        AggregationStrategyKind::Twap => Box::new(TwapStrategy::new(config.twap_window_ms)),
        AggregationStrategyKind::Vwap => Box::new(VwapStrategy::new()),
        AggregationStrategyKind::MedianFiltered => Box::new(MedianFilteredStrategy::new(
            config.outlier_max_deviation_bps,
        )),
    };

    Ok(OraclePipeline::new(filters, strategy, config.min_sources))
}

#[cfg(test)]
mod tests {
    use market2nats_domain::{CanonicalSymbol, Price, Quantity, Timestamp, VenueId};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::domain::types::PriceSource;

    fn make_pipeline_config(strategy: &str) -> PipelineConfig {
        PipelineConfig {
            strategy: strategy.to_owned(),
            staleness_max_ms: 10_000,
            outlier_max_deviation_bps: 500,
            min_sources: 1,
            twap_window_ms: 30_000,
        }
    }

    fn make_source(venue: &str, price: Decimal, quantity: Decimal, age_ms: u64) -> PriceSource {
        PriceSource {
            venue: VenueId::try_new(venue).expect("valid venue"),
            price: Price::try_new(price).expect("valid price"),
            quantity: Quantity::try_new(quantity).expect("valid quantity"),
            timestamp: Timestamp::new(1_700_000_000_000),
            age_ms,
        }
    }

    #[test]
    fn test_build_pipeline_median_strategy() {
        let config = make_pipeline_config("median");
        let pipeline = build_pipeline(&config).expect("should build");
        let symbol = CanonicalSymbol::try_new("BTC/USDT").expect("valid symbol");
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 100),
            make_source("b", dec!(102), dec!(1), 200),
            make_source("c", dec!(101), dec!(1), 150),
        ];
        let result = pipeline.compute(&symbol, &sources, Timestamp::new(1_700_000_000_000)).expect("should compute");
        assert_eq!(result.price.value(), dec!(101));
    }

    #[test]
    fn test_build_pipeline_twap_strategy() {
        let config = make_pipeline_config("twap");
        let pipeline = build_pipeline(&config).expect("should build");
        let symbol = CanonicalSymbol::try_new("BTC/USDT").expect("valid symbol");
        // Prices within 500 bps of each other so outlier filter keeps them.
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 100),
            make_source("b", dec!(102), dec!(1), 100),
        ];
        let result = pipeline.compute(&symbol, &sources, Timestamp::new(1_700_000_000_000)).expect("should compute");
        // Equal ages, equal weights -> simple average = 101
        assert_eq!(result.price.value(), dec!(101));
    }

    #[test]
    fn test_build_pipeline_vwap_strategy() {
        let config = make_pipeline_config("vwap");
        let pipeline = build_pipeline(&config).expect("should build");
        let symbol = CanonicalSymbol::try_new("BTC/USDT").expect("valid symbol");
        // Prices within 500 bps of each other so outlier filter keeps them.
        let sources = vec![
            make_source("a", dec!(100), dec!(10), 100),
            make_source("b", dec!(102), dec!(10), 100),
        ];
        let result = pipeline.compute(&symbol, &sources, Timestamp::new(1_700_000_000_000)).expect("should compute");
        // Equal volumes -> simple average = 101
        assert_eq!(result.price.value(), dec!(101));
    }

    #[test]
    fn test_build_pipeline_median_filtered_strategy() {
        let config = make_pipeline_config("median_filtered");
        let pipeline = build_pipeline(&config).expect("should build");
        let symbol = CanonicalSymbol::try_new("BTC/USDT").expect("valid symbol");
        let sources = vec![
            make_source("a", dec!(100), dec!(1), 100),
            make_source("b", dec!(101), dec!(1), 200),
            make_source("c", dec!(102), dec!(1), 150),
        ];
        let result = pipeline.compute(&symbol, &sources, Timestamp::new(1_700_000_000_000)).expect("should compute");
        assert_eq!(result.price.value(), dec!(101));
    }

    #[test]
    fn test_build_pipeline_unknown_strategy_error() {
        let config = make_pipeline_config("magic");
        let result = build_pipeline(&config);
        assert!(result.is_err());
    }
}
