pub mod median;
pub mod median_filtered;
pub mod twap;
pub mod vwap;

use std::fmt;

use market2nats_domain::Price;
use serde::{Deserialize, Serialize};

use super::error::OracleError;
use super::types::PriceSource;

pub use median::MedianStrategy;
pub use median_filtered::MedianFilteredStrategy;
pub use twap::TwapStrategy;
pub use vwap::VwapStrategy;

/// Discriminant identifying which aggregation strategy was used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "UPPERCASE")]
pub enum AggregationStrategyKind {
    /// Simple median of source prices.
    Median = 0,
    /// Time-weighted average price.
    Twap = 1,
    /// Volume-weighted average price.
    Vwap = 2,
    /// Median with outlier removal.
    MedianFiltered = 3,
}

impl AggregationStrategyKind {
    /// Parses from a lowercase config string.
    ///
    /// # Errors
    ///
    /// Returns `OracleError::Domain` wrapping an `UnknownMarketDataType` if unrecognized.
    #[must_use = "returns a Result that must be handled"]
    pub fn from_str_config(s: &str) -> Result<Self, OracleError> {
        match s {
            "median" => Ok(Self::Median),
            "twap" => Ok(Self::Twap),
            "vwap" => Ok(Self::Vwap),
            "median_filtered" => Ok(Self::MedianFiltered),
            other => Err(OracleError::Domain(
                market2nats_domain::DomainError::UnknownMarketDataType(other.to_owned()),
            )),
        }
    }
}

impl fmt::Display for AggregationStrategyKind {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Median => f.write_str("median"),
            Self::Twap => f.write_str("twap"),
            Self::Vwap => f.write_str("vwap"),
            Self::MedianFiltered => f.write_str("median_filtered"),
        }
    }
}

/// Trait for aggregation strategies that compute a single price from multiple sources.
pub trait AggregationStrategy: Send + Sync {
    /// Returns the kind discriminant for this strategy.
    fn kind(&self) -> AggregationStrategyKind;

    /// Aggregates the given price sources into a single price.
    ///
    /// # Errors
    ///
    /// Returns `OracleError` if the sources are empty, insufficient,
    /// or if arithmetic fails.
    fn aggregate(&self, sources: &[PriceSource]) -> Result<Price, OracleError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_kind_display_roundtrip() {
        let kinds = [
            AggregationStrategyKind::Median,
            AggregationStrategyKind::Twap,
            AggregationStrategyKind::Vwap,
            AggregationStrategyKind::MedianFiltered,
        ];
        for kind in kinds {
            let s = kind.to_string();
            let parsed = AggregationStrategyKind::from_str_config(&s).unwrap();
            assert_eq!(parsed, kind);
        }
    }

    #[test]
    fn test_strategy_kind_from_str_config_unknown() {
        assert!(AggregationStrategyKind::from_str_config("invalid").is_err());
    }
}
