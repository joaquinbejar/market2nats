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

    use crate::domain::{
        CanonicalSymbol, InstrumentId, MarketDataPayload, MarketDataType, Price, Quantity,
        Sequence, Side, Timestamp, Trade, VenueId,
    };

    use super::*;

    #[test]
    fn test_json_encode_roundtrip() {
        let envelope = MarketDataEnvelope {
            venue: VenueId::try_new("binance").unwrap(),
            instrument: InstrumentId::try_new("BTCUSDT").unwrap(),
            canonical_symbol: CanonicalSymbol::try_new("BTC/USDT").unwrap(),
            data_type: MarketDataType::Trade,
            received_at: Timestamp::new(1_700_000_000_000),
            exchange_timestamp: None,
            sequence: Sequence::new(1),
            payload: MarketDataPayload::Trade(Trade {
                price: Price::try_new(dec!(100)).unwrap(),
                quantity: Quantity::try_new(dec!(1)).unwrap(),
                side: Side::Buy,
                trade_id: None,
            }),
        };

        let bytes = encode_envelope(&envelope).unwrap();
        let decoded: MarketDataEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(envelope, decoded);
    }
}
