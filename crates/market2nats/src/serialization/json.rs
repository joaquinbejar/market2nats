use crate::domain::MarketDataEnvelope;

/// Serializes a `MarketDataEnvelope` to JSON bytes.
///
/// # Errors
///
/// Returns an error if JSON serialization fails.
pub fn encode_envelope(envelope: &MarketDataEnvelope) -> Result<Vec<u8>, EncodeError> {
    serde_json::to_vec(envelope).map_err(|e| EncodeError(e.to_string()))
}

/// JSON encoding error.
#[derive(Debug, thiserror::Error)]
#[error("json encode error: {0}")]
pub struct EncodeError(String);

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use serde_json::{Value, json};

    use crate::domain::{
        CanonicalSymbol, FundingRate, InstrumentId, L2Update, Liquidation, MarketDataPayload,
        MarketDataType, Price, Quantity, Sequence, Side, Ticker, Timestamp, Trade, VenueId,
    };

    use super::*;

    fn base_envelope(data_type: MarketDataType, payload: MarketDataPayload) -> MarketDataEnvelope {
        MarketDataEnvelope {
            venue: VenueId::try_new("binance").unwrap(),
            instrument: InstrumentId::try_new("BTCUSDT").unwrap(),
            canonical_symbol: CanonicalSymbol::try_new("BTC/USDT").unwrap(),
            data_type,
            received_at: Timestamp::new(1_700_000_000_000),
            exchange_timestamp: Some(Timestamp::new(1_699_999_999_999)),
            sequence: Sequence::new(42),
            payload,
        }
    }

    fn as_json(envelope: &MarketDataEnvelope) -> Value {
        let bytes = encode_envelope(envelope).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn test_json_encode_roundtrip() {
        let envelope = base_envelope(
            MarketDataType::Trade,
            MarketDataPayload::Trade(Trade {
                price: Price::try_new(dec!(100)).unwrap(),
                quantity: Quantity::try_new(dec!(1)).unwrap(),
                side: Side::Buy,
                trade_id: None,
            }),
        );

        let bytes = encode_envelope(&envelope).unwrap();
        let decoded: MarketDataEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(envelope, decoded);
    }

    // ── Golden tests ────────────────────────────────────────────────────
    //
    // These lock the canonical wire format documented in docs/wire-format.md.
    // Any change here is a breaking change for NATS consumers — update the doc
    // in the same commit and call it out in the PR description.

    #[test]
    fn test_golden_trade_envelope() {
        let envelope = base_envelope(
            MarketDataType::Trade,
            MarketDataPayload::Trade(Trade {
                price: Price::try_new(dec!(50000.50)).unwrap(),
                quantity: Quantity::try_new(dec!(1.5)).unwrap(),
                side: Side::Buy,
                trade_id: Some("12345".to_owned()),
            }),
        );
        let expected = json!({
            "venue": "binance",
            "instrument": "BTCUSDT",
            "canonical_symbol": "BTC/USDT",
            "data_type": "trade",
            "received_at": 1_700_000_000_000_u64,
            "exchange_timestamp": 1_699_999_999_999_u64,
            "sequence": 42,
            "payload": {
                "type": "trade",
                "price": "50000.50",
                "quantity": "1.5",
                "side": "BUY",
                "trade_id": "12345"
            }
        });
        assert_eq!(as_json(&envelope), expected);
    }

    #[test]
    fn test_golden_ticker_envelope() {
        let envelope = MarketDataEnvelope {
            exchange_timestamp: None,
            sequence: Sequence::new(7),
            ..base_envelope(
                MarketDataType::Ticker,
                MarketDataPayload::Ticker(Ticker {
                    bid_price: Price::try_new(dec!(50000.00)).unwrap(),
                    bid_qty: Quantity::try_new(dec!(2.5)).unwrap(),
                    ask_price: Price::try_new(dec!(50001.00)).unwrap(),
                    ask_qty: Quantity::try_new(dec!(1.8)).unwrap(),
                    last_price: Price::try_new(dec!(50000.50)).unwrap(),
                }),
            )
        };
        let expected = json!({
            "venue": "binance",
            "instrument": "BTCUSDT",
            "canonical_symbol": "BTC/USDT",
            "data_type": "ticker",
            "received_at": 1_700_000_000_000_u64,
            "exchange_timestamp": null,
            "sequence": 7,
            "payload": {
                "type": "ticker",
                "bid_price": "50000.00",
                "bid_qty": "2.5",
                "ask_price": "50001.00",
                "ask_qty": "1.8",
                "last_price": "50000.50"
            }
        });
        assert_eq!(as_json(&envelope), expected);
    }

    #[test]
    fn test_golden_l2_update_envelope() {
        let envelope = MarketDataEnvelope {
            sequence: Sequence::new(1001),
            ..base_envelope(
                MarketDataType::L2Orderbook,
                MarketDataPayload::L2Update(L2Update {
                    bids: vec![
                        (
                            Price::try_new(dec!(50000.00)).unwrap(),
                            Quantity::try_new(dec!(1.5)).unwrap(),
                        ),
                        (
                            Price::try_new(dec!(49999.00)).unwrap(),
                            Quantity::try_new(dec!(2.0)).unwrap(),
                        ),
                    ],
                    asks: vec![(
                        Price::try_new(dec!(50001.00)).unwrap(),
                        Quantity::try_new(dec!(0.8)).unwrap(),
                    )],
                    is_snapshot: false,
                }),
            )
        };
        let expected = json!({
            "venue": "binance",
            "instrument": "BTCUSDT",
            "canonical_symbol": "BTC/USDT",
            "data_type": "l2_orderbook",
            "received_at": 1_700_000_000_000_u64,
            "exchange_timestamp": 1_699_999_999_999_u64,
            "sequence": 1001,
            "payload": {
                "type": "l2_update",
                "bids": [
                    ["50000.00", "1.5"],
                    ["49999.00", "2.0"]
                ],
                "asks": [
                    ["50001.00", "0.8"]
                ],
                "is_snapshot": false
            }
        });
        assert_eq!(as_json(&envelope), expected);
    }

    #[test]
    fn test_golden_funding_rate_envelope() {
        let envelope = MarketDataEnvelope {
            sequence: Sequence::new(3),
            ..base_envelope(
                MarketDataType::FundingRate,
                MarketDataPayload::FundingRate(FundingRate {
                    rate: dec!(0.0001),
                    predicted_rate: Some(dec!(0.00012)),
                    next_funding_at: Timestamp::new(1_700_028_800_000),
                }),
            )
        };
        let expected = json!({
            "venue": "binance",
            "instrument": "BTCUSDT",
            "canonical_symbol": "BTC/USDT",
            "data_type": "funding_rate",
            "received_at": 1_700_000_000_000_u64,
            "exchange_timestamp": 1_699_999_999_999_u64,
            "sequence": 3,
            "payload": {
                "type": "funding_rate",
                "rate": "0.0001",
                "predicted_rate": "0.00012",
                "next_funding_at": 1_700_028_800_000_u64
            }
        });
        assert_eq!(as_json(&envelope), expected);
    }

    #[test]
    fn test_golden_liquidation_envelope() {
        let envelope = MarketDataEnvelope {
            sequence: Sequence::new(14),
            ..base_envelope(
                MarketDataType::Liquidation,
                MarketDataPayload::Liquidation(Liquidation {
                    side: Side::Sell,
                    price: Price::try_new(dec!(50000.00)).unwrap(),
                    quantity: Quantity::try_new(dec!(0.5)).unwrap(),
                }),
            )
        };
        let expected = json!({
            "venue": "binance",
            "instrument": "BTCUSDT",
            "canonical_symbol": "BTC/USDT",
            "data_type": "liquidation",
            "received_at": 1_700_000_000_000_u64,
            "exchange_timestamp": 1_699_999_999_999_u64,
            "sequence": 14,
            "payload": {
                "type": "liquidation",
                "side": "SELL",
                "price": "50000.00",
                "quantity": "0.5"
            }
        });
        assert_eq!(as_json(&envelope), expected);
    }
}
