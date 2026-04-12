use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::DomainError;

/// Trade side: buy or sell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

impl Side {
    /// Parses a side from a case-insensitive string.
    ///
    /// # Errors
    ///
    /// Returns `DomainError::UnknownSide` if the string is not recognized.
    #[must_use = "returns a Result that must be handled"]
    pub fn from_str_loose(s: &str) -> Result<Self, DomainError> {
        match s.to_ascii_lowercase().as_str() {
            "buy" | "bid" | "b" => Ok(Self::Buy),
            "sell" | "ask" | "s" | "a" => Ok(Self::Sell),
            other => Err(DomainError::UnknownSide(other.to_owned())),
        }
    }
}

impl fmt::Display for Side {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Buy => f.write_str("BUY"),
            Self::Sell => f.write_str("SELL"),
        }
    }
}

/// Category of market data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataType {
    Trade = 0,
    Ticker = 1,
    L2Orderbook = 2,
    FundingRate = 3,
    Liquidation = 4,
}

impl MarketDataType {
    /// Parses from a config/subject-style string (snake_case).
    ///
    /// # Errors
    ///
    /// Returns `DomainError::UnknownMarketDataType` if unrecognized.
    #[must_use = "returns a Result that must be handled"]
    pub fn from_str_config(s: &str) -> Result<Self, DomainError> {
        match s {
            "trade" => Ok(Self::Trade),
            "ticker" => Ok(Self::Ticker),
            "l2_orderbook" => Ok(Self::L2Orderbook),
            "funding_rate" => Ok(Self::FundingRate),
            "liquidation" => Ok(Self::Liquidation),
            other => Err(DomainError::UnknownMarketDataType(other.to_owned())),
        }
    }

    /// Returns the snake_case string used in NATS subjects and config.
    #[must_use]
    #[inline]
    pub fn as_subject_str(&self) -> &'static str {
        match self {
            Self::Trade => "trade",
            Self::Ticker => "ticker",
            Self::L2Orderbook => "l2_orderbook",
            Self::FundingRate => "funding_rate",
            Self::Liquidation => "liquidation",
        }
    }
}

impl fmt::Display for MarketDataType {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_subject_str())
    }
}

/// Connection state for a venue adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Disconnected = 0,
    Connected = 1,
    Reconnecting = 2,
    CircuitOpen = 3,
}

impl fmt::Display for ConnectionState {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected => f.write_str("disconnected"),
            Self::Connected => f.write_str("connected"),
            Self::Reconnecting => f.write_str("reconnecting"),
            Self::CircuitOpen => f.write_str("circuit_open"),
        }
    }
}

/// Overall service health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum ServiceHealth {
    Healthy = 0,
    Degraded = 1,
    Unhealthy = 2,
}

impl fmt::Display for ServiceHealth {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Healthy => f.write_str("healthy"),
            Self::Degraded => f.write_str("degraded"),
            Self::Unhealthy => f.write_str("unhealthy"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_side_from_str_loose_buy_variants() {
        assert_eq!(Side::from_str_loose("buy").unwrap(), Side::Buy);
        assert_eq!(Side::from_str_loose("BUY").unwrap(), Side::Buy);
        assert_eq!(Side::from_str_loose("bid").unwrap(), Side::Buy);
        assert_eq!(Side::from_str_loose("b").unwrap(), Side::Buy);
    }

    #[test]
    fn test_side_from_str_loose_sell_variants() {
        assert_eq!(Side::from_str_loose("sell").unwrap(), Side::Sell);
        assert_eq!(Side::from_str_loose("SELL").unwrap(), Side::Sell);
        assert_eq!(Side::from_str_loose("ask").unwrap(), Side::Sell);
    }

    #[test]
    fn test_side_from_str_loose_unknown() {
        assert!(Side::from_str_loose("unknown").is_err());
    }

    #[test]
    fn test_market_data_type_roundtrip() {
        let types = [
            MarketDataType::Trade,
            MarketDataType::Ticker,
            MarketDataType::L2Orderbook,
            MarketDataType::FundingRate,
            MarketDataType::Liquidation,
        ];
        for t in types {
            let s = t.as_subject_str();
            let parsed = MarketDataType::from_str_config(s).unwrap();
            assert_eq!(parsed, t);
        }
    }

    #[test]
    fn test_market_data_type_unknown() {
        assert!(MarketDataType::from_str_config("invalid").is_err());
    }

    #[test]
    fn test_connection_state_display() {
        assert_eq!(ConnectionState::Connected.to_string(), "connected");
        assert_eq!(ConnectionState::CircuitOpen.to_string(), "circuit_open");
    }
}
