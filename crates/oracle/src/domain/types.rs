use market2nats_domain::{Price, Quantity, Timestamp, VenueId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::strategies::AggregationStrategyKind;

/// A single price observation from a trading venue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceSource {
    /// The venue that produced this price.
    pub venue: VenueId,
    /// The observed price.
    pub price: Price,
    /// The observed quantity / volume at this price.
    pub quantity: Quantity,
    /// When this observation was created (epoch milliseconds).
    pub timestamp: Timestamp,
    /// Age of this observation in milliseconds relative to the computation time.
    pub age_ms: u64,
}

/// Confidence level of an oracle price based on source count and spread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "UPPERCASE")]
pub enum OracleConfidence {
    /// High confidence: at least 7 sources and spread below 10 basis points (0.1%).
    High = 0,
    /// Medium confidence: at least 4 sources and spread below 50 basis points (0.5%).
    Medium = 1,
    /// Low confidence: fewer sources or wider spread.
    Low = 2,
}

/// Basis-point threshold for high confidence.
const HIGH_SPREAD_BPS: Decimal = Decimal::from_parts(10, 0, 0, false, 0);
/// Minimum sources for high confidence.
const HIGH_MIN_SOURCES: usize = 7;
/// Basis-point threshold for medium confidence.
const MEDIUM_SPREAD_BPS: Decimal = Decimal::from_parts(50, 0, 0, false, 0);
/// Minimum sources for medium confidence.
const MEDIUM_MIN_SOURCES: usize = 4;

impl OracleConfidence {
    /// Computes the confidence level from the number of contributing sources
    /// and the spread in basis points between the min and max source prices.
    ///
    /// - `High`: >= 7 sources AND spread < 10 bps
    /// - `Medium`: >= 4 sources AND spread < 50 bps
    /// - `Low`: everything else
    #[must_use]
    #[inline]
    pub fn compute(source_count: usize, spread_bps: Decimal) -> Self {
        if source_count >= HIGH_MIN_SOURCES && spread_bps < HIGH_SPREAD_BPS {
            Self::High
        } else if source_count >= MEDIUM_MIN_SOURCES && spread_bps < MEDIUM_SPREAD_BPS {
            Self::Medium
        } else {
            Self::Low
        }
    }
}

impl core::fmt::Display for OracleConfidence {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::High => f.write_str("HIGH"),
            Self::Medium => f.write_str("MEDIUM"),
            Self::Low => f.write_str("LOW"),
        }
    }
}

/// The final aggregated oracle price for a symbol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OraclePrice {
    /// The canonical symbol this price is for.
    pub symbol: market2nats_domain::CanonicalSymbol,
    /// The aggregated price.
    pub price: Price,
    /// When this oracle price was computed (epoch milliseconds).
    pub timestamp: Timestamp,
    /// The price sources that contributed to this aggregate.
    pub sources: Vec<PriceSource>,
    /// Which aggregation strategy was used.
    pub strategy: AggregationStrategyKind,
    /// Confidence level of this oracle price.
    pub confidence: OracleConfidence,
}

#[cfg(test)]
mod tests {
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
    fn test_confidence_compute_high_many_sources_tight_spread() {
        let confidence = OracleConfidence::compute(8, dec!(5));
        assert_eq!(confidence, OracleConfidence::High);
    }

    #[test]
    fn test_confidence_compute_medium_enough_sources_moderate_spread() {
        let confidence = OracleConfidence::compute(5, dec!(30));
        assert_eq!(confidence, OracleConfidence::Medium);
    }

    #[test]
    fn test_confidence_compute_low_few_sources() {
        let confidence = OracleConfidence::compute(2, dec!(5));
        assert_eq!(confidence, OracleConfidence::Low);
    }

    #[test]
    fn test_confidence_compute_low_wide_spread() {
        let confidence = OracleConfidence::compute(8, dec!(60));
        assert_eq!(confidence, OracleConfidence::Low);
    }

    #[test]
    fn test_price_source_serde_roundtrip() {
        let source = make_source("binance", dec!(50000), dec!(1), 100);
        let json = serde_json::to_string(&source).unwrap();
        let parsed: PriceSource = serde_json::from_str(&json).unwrap();
        assert_eq!(source, parsed);
    }

    #[test]
    fn test_confidence_display() {
        assert_eq!(OracleConfidence::High.to_string(), "HIGH");
        assert_eq!(OracleConfidence::Medium.to_string(), "MEDIUM");
        assert_eq!(OracleConfidence::Low.to_string(), "LOW");
    }
}
