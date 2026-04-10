use prost::Message;

use crate::domain::{
    FundingRate, L2Update, Liquidation, MarketDataEnvelope, MarketDataPayload, Ticker, Trade,
};

/// Generated protobuf types.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/market_data.rs"));
}

/// Serializes a `MarketDataEnvelope` to protobuf bytes.
///
/// # Errors
///
/// Returns an error if encoding fails.
pub fn encode_envelope(envelope: &MarketDataEnvelope) -> Result<Vec<u8>, EncodeError> {
    let pb_envelope = to_pb_envelope(envelope)?;
    let mut buf = Vec::with_capacity(pb_envelope.encoded_len());
    pb_envelope
        .encode(&mut buf)
        .map_err(|e| EncodeError(e.to_string()))?;
    Ok(buf)
}

/// Protobuf encoding error.
#[derive(Debug, thiserror::Error)]
#[error("protobuf encode error: {0}")]
pub struct EncodeError(String);

/// Converts a domain envelope to the protobuf representation.
fn to_pb_envelope(envelope: &MarketDataEnvelope) -> Result<pb::MarketDataEnvelope, EncodeError> {
    let payload = match &envelope.payload {
        MarketDataPayload::Trade(t) => {
            Some(pb::market_data_envelope::Payload::Trade(to_pb_trade(t)))
        }
        MarketDataPayload::Ticker(t) => {
            Some(pb::market_data_envelope::Payload::Ticker(to_pb_ticker(t)))
        }
        MarketDataPayload::L2Update(u) => Some(pb::market_data_envelope::Payload::L2Update(
            to_pb_l2_update(u),
        )),
        MarketDataPayload::FundingRate(f) => Some(pb::market_data_envelope::Payload::FundingRate(
            to_pb_funding_rate(f),
        )),
        MarketDataPayload::Liquidation(l) => Some(pb::market_data_envelope::Payload::Liquidation(
            to_pb_liquidation(l),
        )),
    };

    Ok(pb::MarketDataEnvelope {
        venue: envelope.venue.as_str().to_owned(),
        instrument: envelope.instrument.as_str().to_owned(),
        canonical_symbol: envelope.canonical_symbol.as_str().to_owned(),
        data_type: envelope.data_type.as_subject_str().to_owned(),
        received_at: envelope.received_at.as_millis(),
        exchange_timestamp: envelope.exchange_timestamp.map(|ts| ts.as_millis()),
        sequence: envelope.sequence.value(),
        payload,
    })
}

#[must_use]
#[inline]
fn to_pb_trade(trade: &Trade) -> pb::TradePayload {
    pb::TradePayload {
        price: trade.price.value().to_string(),
        quantity: trade.quantity.value().to_string(),
        side: trade.side.to_string(),
        trade_id: trade.trade_id.clone(),
    }
}

#[must_use]
#[inline]
fn to_pb_ticker(ticker: &Ticker) -> pb::TickerPayload {
    pb::TickerPayload {
        bid_price: ticker.bid_price.value().to_string(),
        bid_qty: ticker.bid_qty.value().to_string(),
        ask_price: ticker.ask_price.value().to_string(),
        ask_qty: ticker.ask_qty.value().to_string(),
        last_price: ticker.last_price.value().to_string(),
    }
}

#[must_use]
#[inline]
fn to_pb_l2_update(update: &L2Update) -> pb::L2UpdatePayload {
    pb::L2UpdatePayload {
        bids: update
            .bids
            .iter()
            .map(|(p, q)| pb::PriceLevel {
                price: p.value().to_string(),
                quantity: q.value().to_string(),
            })
            .collect(),
        asks: update
            .asks
            .iter()
            .map(|(p, q)| pb::PriceLevel {
                price: p.value().to_string(),
                quantity: q.value().to_string(),
            })
            .collect(),
        is_snapshot: update.is_snapshot,
    }
}

#[must_use]
#[inline]
fn to_pb_funding_rate(fr: &FundingRate) -> pb::FundingRatePayload {
    pb::FundingRatePayload {
        rate: fr.rate.to_string(),
        predicted_rate: fr.predicted_rate.map(|r| r.to_string()),
        next_funding_at: fr.next_funding_at.as_millis(),
    }
}

#[must_use]
#[inline]
fn to_pb_liquidation(liq: &Liquidation) -> pb::LiquidationPayload {
    pb::LiquidationPayload {
        side: liq.side.to_string(),
        price: liq.price.value().to_string(),
        quantity: liq.quantity.value().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use crate::domain::{
        CanonicalSymbol, InstrumentId, MarketDataType, Price, Quantity, Sequence, Side, Timestamp,
        VenueId,
    };

    use super::*;

    #[test]
    fn test_encode_trade_envelope() {
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

        let bytes = encode_envelope(&envelope).unwrap();
        assert!(!bytes.is_empty());

        // Decode and verify roundtrip.
        let decoded = pb::MarketDataEnvelope::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.venue, "binance");
        assert_eq!(decoded.sequence, 42);
        assert!(decoded.payload.is_some());
    }
}
