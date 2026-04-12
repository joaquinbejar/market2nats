use crate::domain::MarketDataEnvelope;

/// Resolves NATS subjects from market data envelopes.
///
/// Pattern: `market.<venue_id>.<canonical_symbol_normalized>.<data_type>`
pub struct StreamRouter;

impl StreamRouter {
    /// Creates a new `StreamRouter`.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self
    }

    /// Resolves the NATS subject for a given market data envelope.
    ///
    /// The canonical symbol is lowercased with `/` replaced by `-`.
    #[must_use]
    pub fn resolve_subject(&self, envelope: &MarketDataEnvelope) -> String {
        format!(
            "market.{}.{}.{}",
            envelope.venue.as_str(),
            envelope.canonical_symbol.normalized(),
            envelope.data_type.as_subject_str(),
        )
    }
}

impl Default for StreamRouter {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use crate::domain::{
        CanonicalSymbol, InstrumentId, MarketDataPayload, MarketDataType, Price, Quantity,
        Sequence, Side, Timestamp, Trade, VenueId,
    };

    use super::*;

    fn make_envelope(venue: &str, symbol: &str, data_type: MarketDataType) -> MarketDataEnvelope {
        MarketDataEnvelope {
            venue: VenueId::try_new(venue).unwrap(),
            instrument: InstrumentId::try_new("BTCUSDT").unwrap(),
            canonical_symbol: CanonicalSymbol::try_new(symbol).unwrap(),
            data_type,
            received_at: Timestamp::new(0),
            exchange_timestamp: None,
            sequence: Sequence::new(1),
            payload: MarketDataPayload::Trade(Trade {
                price: Price::try_new(dec!(100)).unwrap(),
                quantity: Quantity::try_new(dec!(1)).unwrap(),
                side: Side::Buy,
                trade_id: None,
            }),
        }
    }

    #[test]
    fn test_resolve_subject_trade() {
        let router = StreamRouter::new();
        let env = make_envelope("binance", "BTC/USDT", MarketDataType::Trade);
        assert_eq!(
            router.resolve_subject(&env),
            "market.binance.btc-usdt.trade"
        );
    }

    #[test]
    fn test_resolve_subject_l2_orderbook() {
        let router = StreamRouter::new();
        let env = make_envelope("kraken", "ETH/USD", MarketDataType::L2Orderbook);
        assert_eq!(
            router.resolve_subject(&env),
            "market.kraken.eth-usd.l2_orderbook"
        );
    }

    #[test]
    fn test_resolve_subject_funding_rate() {
        let router = StreamRouter::new();
        let env = make_envelope("binance", "BTC/USDT", MarketDataType::FundingRate);
        assert_eq!(
            router.resolve_subject(&env),
            "market.binance.btc-usdt.funding_rate"
        );
    }
}
