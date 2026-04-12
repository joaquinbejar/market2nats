use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::enums::{MarketDataType, Side};
use crate::types::{CanonicalSymbol, InstrumentId, Price, Quantity, Sequence, Timestamp, VenueId};

/// Envelope wrapping all market data events with routing metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataEnvelope {
    /// Originating venue.
    pub venue: VenueId,
    /// Venue-local instrument identifier.
    pub instrument: InstrumentId,
    /// Canonical symbol for NATS subject naming.
    pub canonical_symbol: CanonicalSymbol,
    /// Discriminant for the payload type.
    pub data_type: MarketDataType,
    /// When the service received the raw WebSocket message (epoch millis).
    pub received_at: Timestamp,
    /// Timestamp provided by the venue, if available.
    pub exchange_timestamp: Option<Timestamp>,
    /// Monotonically increasing sequence number per (venue, instrument, data_type).
    pub sequence: Sequence,
    /// The typed market data content.
    pub payload: MarketDataPayload,
}

/// Tagged union of all market data payload types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MarketDataPayload {
    /// A single executed trade.
    Trade(Trade),
    /// Current best bid/ask and last price.
    Ticker(Ticker),
    /// L2 orderbook snapshot or delta.
    L2Update(L2Update),
    /// Funding rate for perpetual futures.
    FundingRate(FundingRate),
    /// Forced liquidation event.
    Liquidation(Liquidation),
}

/// A single executed trade event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trade {
    /// Execution price.
    pub price: Price,
    /// Trade quantity.
    pub quantity: Quantity,
    /// Aggressor side.
    pub side: Side,
    /// Venue-specific trade identifier, if available.
    pub trade_id: Option<String>,
}

/// Top-of-book ticker snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticker {
    /// Best bid price.
    pub bid_price: Price,
    /// Best bid quantity.
    pub bid_qty: Quantity,
    /// Best ask price.
    pub ask_price: Price,
    /// Best ask quantity.
    pub ask_qty: Quantity,
    /// Last traded price.
    pub last_price: Price,
}

/// L2 orderbook update — snapshot or incremental delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L2Update {
    /// Bid price levels (price, quantity). Quantity of zero means remove the level.
    pub bids: Vec<(Price, Quantity)>,
    /// Ask price levels (price, quantity). Quantity of zero means remove the level.
    pub asks: Vec<(Price, Quantity)>,
    /// `true` for a full snapshot, `false` for an incremental delta.
    pub is_snapshot: bool,
}

/// Funding rate update for perpetual futures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingRate {
    /// Current funding rate.
    pub rate: Decimal,
    /// Predicted next funding rate, if available.
    pub predicted_rate: Option<Decimal>,
    /// Next funding timestamp (epoch millis).
    pub next_funding_at: Timestamp,
}

/// Forced liquidation event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Liquidation {
    /// Side being liquidated.
    pub side: Side,
    /// Liquidation price.
    pub price: Price,
    /// Liquidation quantity.
    pub quantity: Quantity,
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn test_market_data_envelope_serde_roundtrip() {
        let envelope = MarketDataEnvelope {
            venue: VenueId::try_new("binance").unwrap(),
            instrument: InstrumentId::try_new("BTCUSDT").unwrap(),
            canonical_symbol: CanonicalSymbol::try_new("BTC/USDT").unwrap(),
            data_type: MarketDataType::Trade,
            received_at: Timestamp::new(1_700_000_000_000),
            exchange_timestamp: Some(Timestamp::new(1_699_999_999_999)),
            sequence: Sequence::new(42),
            payload: MarketDataPayload::Trade(Trade {
                price: Price::try_new(dec!(50000.50)).unwrap(),
                quantity: Quantity::try_new(dec!(1.5)).unwrap(),
                side: Side::Buy,
                trade_id: Some("12345".to_owned()),
            }),
        };

        let json = serde_json::to_string(&envelope).unwrap();
        let deserialized: MarketDataEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(envelope, deserialized);
    }

    #[test]
    fn test_l2_update_snapshot() {
        let update = L2Update {
            bids: vec![
                (
                    Price::try_new(dec!(100)).unwrap(),
                    Quantity::try_new(dec!(10)).unwrap(),
                ),
                (
                    Price::try_new(dec!(99)).unwrap(),
                    Quantity::try_new(dec!(20)).unwrap(),
                ),
            ],
            asks: vec![(
                Price::try_new(dec!(101)).unwrap(),
                Quantity::try_new(dec!(5)).unwrap(),
            )],
            is_snapshot: true,
        };
        assert!(update.is_snapshot);
        assert_eq!(update.bids.len(), 2);
    }

    #[test]
    fn test_funding_rate_without_prediction() {
        let fr = FundingRate {
            rate: dec!(0.0001),
            predicted_rate: None,
            next_funding_at: Timestamp::new(1_700_000_000_000),
        };
        assert!(fr.predicted_rate.is_none());
    }

    #[test]
    fn test_payload_variants_serde() {
        let payloads = vec![
            MarketDataPayload::Trade(Trade {
                price: Price::try_new(dec!(100)).unwrap(),
                quantity: Quantity::try_new(dec!(1)).unwrap(),
                side: Side::Buy,
                trade_id: None,
            }),
            MarketDataPayload::Ticker(Ticker {
                bid_price: Price::try_new(dec!(99)).unwrap(),
                bid_qty: Quantity::try_new(dec!(10)).unwrap(),
                ask_price: Price::try_new(dec!(101)).unwrap(),
                ask_qty: Quantity::try_new(dec!(5)).unwrap(),
                last_price: Price::try_new(dec!(100)).unwrap(),
            }),
            MarketDataPayload::Liquidation(Liquidation {
                side: Side::Sell,
                price: Price::try_new(dec!(50000)).unwrap(),
                quantity: Quantity::try_new(dec!(0.5)).unwrap(),
            }),
        ];

        for payload in payloads {
            let json = serde_json::to_string(&payload).unwrap();
            let parsed: MarketDataPayload = serde_json::from_str(&json).unwrap();
            assert_eq!(payload, parsed);
        }
    }
}
