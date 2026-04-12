use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::error::DomainError;

/// Identifies a trading venue (e.g., "binance", "kraken").
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VenueId(String);

impl VenueId {
    /// Creates a new `VenueId`.
    ///
    /// # Errors
    ///
    /// Returns `DomainError::EmptyField` if the value is empty or whitespace-only.
    #[must_use = "returns a Result that must be handled"]
    pub fn try_new(value: impl Into<String>) -> Result<Self, DomainError> {
        let v: String = value.into();
        if v.trim().is_empty() {
            return Err(DomainError::EmptyField { field: "venue_id" });
        }
        Ok(Self(v))
    }

    /// Returns the inner string slice.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for VenueId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies an instrument on a venue (e.g., "BTCUSDT").
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InstrumentId(String);

impl InstrumentId {
    /// Creates a new `InstrumentId`.
    ///
    /// # Errors
    ///
    /// Returns `DomainError::EmptyField` if the value is empty or whitespace-only.
    #[must_use = "returns a Result that must be handled"]
    pub fn try_new(value: impl Into<String>) -> Result<Self, DomainError> {
        let v: String = value.into();
        if v.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "instrument_id",
            });
        }
        Ok(Self(v))
    }

    /// Returns the inner string slice.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InstrumentId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A canonical symbol used for NATS subject naming (e.g., "BTC/USDT").
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CanonicalSymbol(String);

impl CanonicalSymbol {
    /// Creates a new `CanonicalSymbol`.
    ///
    /// # Errors
    ///
    /// Returns `DomainError::EmptyField` if the value is empty or whitespace-only.
    #[must_use = "returns a Result that must be handled"]
    pub fn try_new(value: impl Into<String>) -> Result<Self, DomainError> {
        let v: String = value.into();
        if v.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "canonical_symbol",
            });
        }
        Ok(Self(v))
    }

    /// Returns the inner string slice.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the normalized form for NATS subjects: lowercased, `/` replaced with `-`.
    #[must_use]
    pub fn normalized(&self) -> String {
        self.0.to_lowercase().replace('/', "-")
    }
}

impl fmt::Display for CanonicalSymbol {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A non-negative price backed by `Decimal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Price(Decimal);

impl Price {
    /// Creates a new `Price`.
    ///
    /// # Errors
    ///
    /// Returns `DomainError::NegativeValue` if the value is negative.
    #[must_use = "returns a Result that must be handled"]
    pub fn try_new(value: Decimal) -> Result<Self, DomainError> {
        if value.is_sign_negative() {
            return Err(DomainError::NegativeValue {
                field: "price",
                value: value.to_string(),
            });
        }
        Ok(Self(value))
    }

    /// Returns the inner `Decimal`.
    #[must_use]
    #[inline]
    pub fn value(&self) -> Decimal {
        self.0
    }
}

impl fmt::Display for Price {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A non-negative quantity backed by `Decimal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Quantity(Decimal);

impl Quantity {
    /// Creates a new `Quantity`.
    ///
    /// # Errors
    ///
    /// Returns `DomainError::NegativeValue` if the value is negative.
    #[must_use = "returns a Result that must be handled"]
    pub fn try_new(value: Decimal) -> Result<Self, DomainError> {
        if value.is_sign_negative() {
            return Err(DomainError::NegativeValue {
                field: "quantity",
                value: value.to_string(),
            });
        }
        Ok(Self(value))
    }

    /// Returns the inner `Decimal`.
    #[must_use]
    #[inline]
    pub fn value(&self) -> Decimal {
        self.0
    }
}

impl fmt::Display for Quantity {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A monotonically increasing sequence number, scoped per (venue, instrument, data_type).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sequence(u64);

impl Sequence {
    /// Creates a new `Sequence` from a raw `u64`.
    #[must_use]
    #[inline]
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the inner `u64`.
    #[must_use]
    #[inline]
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Returns the next sequence number.
    ///
    /// # Errors
    ///
    /// Returns `DomainError::SequenceError` on overflow.
    #[must_use = "returns a Result that must be handled"]
    pub fn next(&self) -> Result<Self, DomainError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| DomainError::SequenceError("sequence overflow".to_owned()))
    }
}

impl fmt::Display for Sequence {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A timestamp in epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Creates a new `Timestamp` from epoch milliseconds.
    #[must_use]
    #[inline]
    pub fn new(epoch_millis: u64) -> Self {
        Self(epoch_millis)
    }

    /// Returns the epoch milliseconds value.
    #[must_use]
    #[inline]
    pub fn as_millis(&self) -> u64 {
        self.0
    }

    /// Returns the current time as a `Timestamp`.
    #[must_use]
    pub fn now() -> Self {
        let duration = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Self(duration.as_millis() as u64)
    }
}

impl fmt::Display for Timestamp {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn test_venue_id_try_new_valid() {
        let id = VenueId::try_new("binance").unwrap();
        assert_eq!(id.as_str(), "binance");
    }

    #[test]
    fn test_venue_id_try_new_empty_fails() {
        let err = VenueId::try_new("").unwrap_err();
        assert_eq!(err, DomainError::EmptyField { field: "venue_id" });
    }

    #[test]
    fn test_venue_id_try_new_whitespace_fails() {
        assert!(VenueId::try_new("  ").is_err());
    }

    #[test]
    fn test_instrument_id_try_new_valid() {
        let id = InstrumentId::try_new("BTCUSDT").unwrap();
        assert_eq!(id.as_str(), "BTCUSDT");
    }

    #[test]
    fn test_instrument_id_try_new_empty_fails() {
        assert!(InstrumentId::try_new("").is_err());
    }

    #[test]
    fn test_canonical_symbol_normalized() {
        let sym = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        assert_eq!(sym.normalized(), "btc-usdt");
    }

    #[test]
    fn test_canonical_symbol_normalized_no_slash() {
        let sym = CanonicalSymbol::try_new("BTCUSDT").unwrap();
        assert_eq!(sym.normalized(), "btcusdt");
    }

    #[test]
    fn test_price_try_new_valid() {
        let p = Price::try_new(dec!(100.50)).unwrap();
        assert_eq!(p.value(), dec!(100.50));
    }

    #[test]
    fn test_price_try_new_zero() {
        let p = Price::try_new(dec!(0)).unwrap();
        assert_eq!(p.value(), dec!(0));
    }

    #[test]
    fn test_price_try_new_negative_fails() {
        assert!(Price::try_new(dec!(-1)).is_err());
    }

    #[test]
    fn test_quantity_try_new_valid() {
        let q = Quantity::try_new(dec!(10.5)).unwrap();
        assert_eq!(q.value(), dec!(10.5));
    }

    #[test]
    fn test_quantity_try_new_negative_fails() {
        assert!(Quantity::try_new(dec!(-0.001)).is_err());
    }

    #[test]
    fn test_sequence_next() {
        let s = Sequence::new(0);
        let s2 = s.next().unwrap();
        assert_eq!(s2.value(), 1);
    }

    #[test]
    fn test_sequence_overflow() {
        let s = Sequence::new(u64::MAX);
        assert!(s.next().is_err());
    }

    #[test]
    fn test_timestamp_new() {
        let ts = Timestamp::new(1_700_000_000_000);
        assert_eq!(ts.as_millis(), 1_700_000_000_000);
    }
}
