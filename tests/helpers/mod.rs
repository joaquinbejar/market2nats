use std::time::Duration;

use rust_decimal_macros::dec;

use market2nats::domain::{
    CanonicalSymbol, FundingRate, InstrumentId, L2Update, Liquidation, MarketDataEnvelope,
    MarketDataPayload, MarketDataType, Price, Quantity, Sequence, Side, Ticker, Timestamp, Trade,
    VenueId,
};

/// Default NATS URL for integration tests.
/// Override with `NATS_URL` env var if needed.
pub fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_owned())
}

/// Connects to NATS for testing, with a short timeout.
/// Panics if NATS is not reachable.
pub async fn connect_nats() -> async_nats::Client {
    let url = nats_url();
    async_nats::ConnectOptions::new()
        .connection_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "cannot connect to NATS at {url}: {e}. Is NATS running? Try: docker compose up -d"
            )
        })
}

/// Creates a unique stream name to avoid collisions between tests.
pub fn unique_stream_name(prefix: &str) -> String {
    let id: u64 = rand_id();
    format!("{prefix}_{id}")
}

/// Creates a unique subject prefix to avoid collisions.
pub fn unique_subject_prefix() -> String {
    let id = rand_id();
    format!("test_{id}")
}

/// Simple pseudo-random ID from timestamp.
fn rand_id() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Builds a sample Trade envelope for testing.
pub fn sample_trade_envelope(
    venue: &str,
    instrument: &str,
    canonical: &str,
    seq: u64,
) -> MarketDataEnvelope {
    MarketDataEnvelope {
        venue: VenueId::try_new(venue).unwrap(),
        instrument: InstrumentId::try_new(instrument).unwrap(),
        canonical_symbol: CanonicalSymbol::try_new(canonical).unwrap(),
        data_type: MarketDataType::Trade,
        received_at: Timestamp::now(),
        exchange_timestamp: Some(Timestamp::new(1_700_000_000_000)),
        sequence: Sequence::new(seq),
        payload: MarketDataPayload::Trade(Trade {
            price: Price::try_new(dec!(50000.50)).unwrap(),
            quantity: Quantity::try_new(dec!(1.25)).unwrap(),
            side: Side::Buy,
            trade_id: Some(format!("trade_{seq}")),
        }),
    }
}

/// Builds a sample Ticker envelope for testing.
pub fn sample_ticker_envelope(
    venue: &str,
    instrument: &str,
    canonical: &str,
    seq: u64,
) -> MarketDataEnvelope {
    MarketDataEnvelope {
        venue: VenueId::try_new(venue).unwrap(),
        instrument: InstrumentId::try_new(instrument).unwrap(),
        canonical_symbol: CanonicalSymbol::try_new(canonical).unwrap(),
        data_type: MarketDataType::Ticker,
        received_at: Timestamp::now(),
        exchange_timestamp: None,
        sequence: Sequence::new(seq),
        payload: MarketDataPayload::Ticker(Ticker {
            bid_price: Price::try_new(dec!(49999.00)).unwrap(),
            bid_qty: Quantity::try_new(dec!(10.0)).unwrap(),
            ask_price: Price::try_new(dec!(50001.00)).unwrap(),
            ask_qty: Quantity::try_new(dec!(5.0)).unwrap(),
            last_price: Price::try_new(dec!(50000.50)).unwrap(),
        }),
    }
}

/// Builds a sample L2Update envelope for testing.
pub fn sample_l2_envelope(
    venue: &str,
    instrument: &str,
    canonical: &str,
    seq: u64,
    is_snapshot: bool,
) -> MarketDataEnvelope {
    MarketDataEnvelope {
        venue: VenueId::try_new(venue).unwrap(),
        instrument: InstrumentId::try_new(instrument).unwrap(),
        canonical_symbol: CanonicalSymbol::try_new(canonical).unwrap(),
        data_type: MarketDataType::L2Orderbook,
        received_at: Timestamp::now(),
        exchange_timestamp: Some(Timestamp::new(1_700_000_000_000)),
        sequence: Sequence::new(seq),
        payload: MarketDataPayload::L2Update(L2Update {
            bids: vec![
                (
                    Price::try_new(dec!(50000)).unwrap(),
                    Quantity::try_new(dec!(10)).unwrap(),
                ),
                (
                    Price::try_new(dec!(49999)).unwrap(),
                    Quantity::try_new(dec!(20)).unwrap(),
                ),
            ],
            asks: vec![
                (
                    Price::try_new(dec!(50001)).unwrap(),
                    Quantity::try_new(dec!(5)).unwrap(),
                ),
                (
                    Price::try_new(dec!(50002)).unwrap(),
                    Quantity::try_new(dec!(15)).unwrap(),
                ),
            ],
            is_snapshot,
        }),
    }
}

/// Builds a sample FundingRate envelope for testing.
pub fn sample_funding_rate_envelope(
    venue: &str,
    instrument: &str,
    canonical: &str,
    seq: u64,
) -> MarketDataEnvelope {
    MarketDataEnvelope {
        venue: VenueId::try_new(venue).unwrap(),
        instrument: InstrumentId::try_new(instrument).unwrap(),
        canonical_symbol: CanonicalSymbol::try_new(canonical).unwrap(),
        data_type: MarketDataType::FundingRate,
        received_at: Timestamp::now(),
        exchange_timestamp: None,
        sequence: Sequence::new(seq),
        payload: MarketDataPayload::FundingRate(FundingRate {
            rate: dec!(0.0001),
            predicted_rate: Some(dec!(0.00015)),
            next_funding_at: Timestamp::new(1_700_003_600_000),
        }),
    }
}

/// Builds a sample Liquidation envelope for testing.
pub fn sample_liquidation_envelope(
    venue: &str,
    instrument: &str,
    canonical: &str,
    seq: u64,
) -> MarketDataEnvelope {
    MarketDataEnvelope {
        venue: VenueId::try_new(venue).unwrap(),
        instrument: InstrumentId::try_new(instrument).unwrap(),
        canonical_symbol: CanonicalSymbol::try_new(canonical).unwrap(),
        data_type: MarketDataType::Liquidation,
        received_at: Timestamp::now(),
        exchange_timestamp: Some(Timestamp::new(1_700_000_000_000)),
        sequence: Sequence::new(seq),
        payload: MarketDataPayload::Liquidation(Liquidation {
            side: Side::Sell,
            price: Price::try_new(dec!(48000.00)).unwrap(),
            quantity: Quantity::try_new(dec!(2.5)).unwrap(),
        }),
    }
}
