//! Integration tests for publishing and consuming market data through NATS JetStream.
//!
//! Requires a running NATS server with JetStream enabled.
//! Start one with: `docker compose -f Docker/docker-compose.yml up -d nats`

use std::time::Duration;

use async_nats::jetstream;
use futures_util::StreamExt;
use rust_decimal_macros::dec;

use market2nats::application::ports::NatsPublisher;
use market2nats::config::model::StreamConfig;
use market2nats::domain::{
    CanonicalSymbol, FundingRate, InstrumentId, L2Update, Liquidation, MarketDataEnvelope,
    MarketDataPayload, MarketDataType, Price, Quantity, Sequence, Side, Ticker, Timestamp, Trade,
    VenueId,
};
use market2nats::infrastructure::nats::JetStreamPublisher;
use market2nats::serialization::{self, SerializationFormat};

async fn connect_nats() -> async_nats::Client {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_owned());
    async_nats::ConnectOptions::new()
        .connection_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
        .unwrap_or_else(|e| {
            panic!("cannot connect to NATS at {url}: {e}. Is NATS running? Try: docker compose -f Docker/docker-compose.yml up -d nats")
        })
}

fn unique_stream_name(prefix: &str) -> String {
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    format!("{prefix}_{id}")
}

fn unique_subject_prefix() -> String {
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    format!("test_{id}")
}

// ── Sample builder helpers ───────────────────────────────────────────────

fn sample_trade_envelope(
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
        received_at: Timestamp::new(1_700_000_000_000),
        exchange_timestamp: Some(Timestamp::new(1_699_999_999_999)),
        sequence: Sequence::new(seq),
        payload: MarketDataPayload::Trade(Trade {
            price: Price::try_new(dec!(50000.50)).unwrap(),
            quantity: Quantity::try_new(dec!(1.25)).unwrap(),
            side: Side::Buy,
            trade_id: Some(format!("trade_{seq}")),
        }),
    }
}

fn sample_ticker_envelope(
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
        received_at: Timestamp::new(1_700_000_000_000),
        exchange_timestamp: Some(Timestamp::new(1_699_999_999_999)),
        sequence: Sequence::new(seq),
        payload: MarketDataPayload::Ticker(Ticker {
            bid_price: Price::try_new(dec!(49999.00)).unwrap(),
            bid_qty: Quantity::try_new(dec!(2.0)).unwrap(),
            ask_price: Price::try_new(dec!(50001.00)).unwrap(),
            ask_qty: Quantity::try_new(dec!(1.5)).unwrap(),
            last_price: Price::try_new(dec!(50000.00)).unwrap(),
        }),
    }
}

fn sample_l2_envelope(
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
        received_at: Timestamp::new(1_700_000_000_000),
        exchange_timestamp: Some(Timestamp::new(1_699_999_999_999)),
        sequence: Sequence::new(seq),
        payload: MarketDataPayload::L2Update(L2Update {
            bids: vec![
                (
                    Price::try_new(dec!(49999.00)).unwrap(),
                    Quantity::try_new(dec!(10.0)).unwrap(),
                ),
                (
                    Price::try_new(dec!(49998.00)).unwrap(),
                    Quantity::try_new(dec!(20.0)).unwrap(),
                ),
            ],
            asks: vec![
                (
                    Price::try_new(dec!(50001.00)).unwrap(),
                    Quantity::try_new(dec!(5.0)).unwrap(),
                ),
                (
                    Price::try_new(dec!(50002.00)).unwrap(),
                    Quantity::try_new(dec!(15.0)).unwrap(),
                ),
            ],
            is_snapshot,
        }),
    }
}

fn sample_funding_rate_envelope(
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
        received_at: Timestamp::new(1_700_000_000_000),
        exchange_timestamp: Some(Timestamp::new(1_699_999_999_999)),
        sequence: Sequence::new(seq),
        payload: MarketDataPayload::FundingRate(FundingRate {
            rate: dec!(0.0001),
            predicted_rate: Some(dec!(0.00015)),
            next_funding_at: Timestamp::new(1_700_003_600_000),
        }),
    }
}

fn sample_liquidation_envelope(
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
        received_at: Timestamp::new(1_700_000_000_000),
        exchange_timestamp: Some(Timestamp::new(1_699_999_999_999)),
        sequence: Sequence::new(seq),
        payload: MarketDataPayload::Liquidation(Liquidation {
            side: Side::Sell,
            price: Price::try_new(dec!(48000.00)).unwrap(),
            quantity: Quantity::try_new(dec!(0.5)).unwrap(),
        }),
    }
}

/// Helper to create a test stream.
async fn setup_test_stream(publisher: &JetStreamPublisher, prefix: &str) -> String {
    let stream_name = unique_stream_name(prefix);
    let config = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![format!("{stream_name}.>")],
        storage: "memory".to_owned(),
        retention: "limits".to_owned(),
        max_age_secs: 60,
        max_bytes: 0,
        max_msgs: 0,
        max_msg_size: 65536,
        discard: "old".to_owned(),
        num_replicas: 1,
        duplicate_window_secs: 10,
    };
    publisher.ensure_stream(&config).await.unwrap();
    stream_name
}

/// Test publishing a single trade event as protobuf and reading it back.
#[tokio::test]
#[ignore]
async fn test_publish_trade_protobuf() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client.clone());
    let stream_name = setup_test_stream(&publisher, "PUB_TRADE_PB").await;

    let envelope = sample_trade_envelope("binance", "BTCUSDT", "BTC/USDT", 1);
    let subject = format!("{stream_name}.binance.btc-usdt.trade");
    let payload =
        serialization::serialize_envelope(&envelope, SerializationFormat::Protobuf).unwrap();
    let ct = serialization::content_type(SerializationFormat::Protobuf);

    publisher.publish(&subject, &payload, ct).await.unwrap();

    // Consume the message directly from the stream.
    let js = jetstream::new(client);
    let stream = js.get_stream(&stream_name).await.unwrap();
    let msg = stream.direct_get(1).await.unwrap();

    assert!(!msg.payload.is_empty());
    let ct: Option<String> = msg.headers.get("Content-Type").map(|v| v.to_string());
    assert_eq!(ct, Some("application/protobuf".to_owned()));

    // Cleanup.
    let _ = js.delete_stream(&stream_name).await;
}

/// Test publishing a trade event as JSON and verifying the content.
#[tokio::test]
#[ignore]
async fn test_publish_trade_json_roundtrip() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client.clone());
    let stream_name = setup_test_stream(&publisher, "PUB_TRADE_JSON").await;

    let envelope = sample_trade_envelope("kraken", "XBTUSD", "BTC/USD", 42);
    let subject = format!("{stream_name}.kraken.btc-usd.trade");
    let payload = serialization::serialize_envelope(&envelope, SerializationFormat::Json).unwrap();
    let ct = serialization::content_type(SerializationFormat::Json);

    publisher.publish(&subject, &payload, ct).await.unwrap();

    // Read back and deserialize.
    let js = jetstream::new(client);
    let stream = js.get_stream(&stream_name).await.unwrap();
    let msg = stream.direct_get(1).await.unwrap();

    let deserialized: MarketDataEnvelope = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(deserialized.venue.as_str(), "kraken");
    assert_eq!(deserialized.instrument.as_str(), "XBTUSD");
    assert_eq!(deserialized.sequence.value(), 42);

    let _ = js.delete_stream(&stream_name).await;
}

/// Test publishing all five market data types.
#[tokio::test]
#[ignore]
async fn test_publish_all_data_types() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client.clone());
    let stream_name = setup_test_stream(&publisher, "PUB_ALL_TYPES").await;

    let envelopes: Vec<MarketDataEnvelope> = vec![
        sample_trade_envelope("binance", "BTCUSDT", "BTC/USDT", 1),
        sample_ticker_envelope("binance", "BTCUSDT", "BTC/USDT", 2),
        sample_l2_envelope("binance", "BTCUSDT", "BTC/USDT", 3, true),
        sample_funding_rate_envelope("binance", "BTCUSDT", "BTC/USDT", 4),
        sample_liquidation_envelope("binance", "BTCUSDT", "BTC/USDT", 5),
    ];

    let data_types = [
        "trade",
        "ticker",
        "l2_orderbook",
        "funding_rate",
        "liquidation",
    ];

    for (envelope, dt) in envelopes.iter().zip(data_types.iter()) {
        let subject = format!("{stream_name}.binance.btc-usdt.{dt}");
        let payload =
            serialization::serialize_envelope(envelope, SerializationFormat::Json).unwrap();
        let ct = serialization::content_type(SerializationFormat::Json);
        publisher.publish(&subject, &payload, ct).await.unwrap();
    }

    // Verify all 5 messages are in the stream.
    let js = jetstream::new(client);
    let mut stream = js.get_stream(&stream_name).await.unwrap();
    let info = stream.info().await.unwrap();
    assert_eq!(info.state.messages, 5, "expected 5 messages in stream");

    let _ = js.delete_stream(&stream_name).await;
}

/// Test publishing multiple messages and consuming them via a pull consumer.
#[tokio::test]
#[ignore]
async fn test_publish_and_pull_consume() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client.clone());
    let stream_name = setup_test_stream(&publisher, "PUB_PULL").await;

    let count = 10u64;
    for i in 1..=count {
        let envelope = sample_trade_envelope("binance", "BTCUSDT", "BTC/USDT", i);
        let subject = format!("{stream_name}.binance.btc-usdt.trade");
        let payload =
            serialization::serialize_envelope(&envelope, SerializationFormat::Json).unwrap();
        let ct = serialization::content_type(SerializationFormat::Json);
        publisher.publish(&subject, &payload, ct).await.unwrap();
    }

    // Create a pull consumer and fetch messages.
    let js = jetstream::new(client);
    let stream = js.get_stream(&stream_name).await.unwrap();
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some("test_puller".to_owned()),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .unwrap();

    let mut messages = consumer
        .fetch()
        .max_messages(count as usize)
        .messages()
        .await
        .unwrap();
    let mut received = 0u64;
    while let Some(Ok(msg)) = messages.next().await {
        let envelope: MarketDataEnvelope = serde_json::from_slice(&msg.payload).unwrap();
        received += 1;
        assert_eq!(envelope.sequence.value(), received);
        msg.ack().await.unwrap();
    }

    assert_eq!(received, count, "expected {count} messages, got {received}");

    let _ = js.delete_stream(&stream_name).await;
}

/// Test that messages published to different subjects land in the correct stream.
#[tokio::test]
#[ignore]
async fn test_subject_routing_isolation() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client.clone());
    let js = jetstream::new(client);

    let prefix = unique_subject_prefix();
    let trades_stream = format!("{prefix}_TRADES");
    let ticker_stream = format!("{prefix}_TICKER");

    // Create two separate streams.
    publisher
        .ensure_stream(&StreamConfig {
            name: trades_stream.clone(),
            subjects: vec![format!("{prefix}.*.*.trade")],
            storage: "memory".to_owned(),
            retention: "limits".to_owned(),
            max_age_secs: 60,
            max_bytes: 0,
            max_msgs: 0,
            max_msg_size: 65536,
            discard: "old".to_owned(),
            num_replicas: 1,
            duplicate_window_secs: 10,
        })
        .await
        .unwrap();

    publisher
        .ensure_stream(&StreamConfig {
            name: ticker_stream.clone(),
            subjects: vec![format!("{prefix}.*.*.ticker")],
            storage: "memory".to_owned(),
            retention: "limits".to_owned(),
            max_age_secs: 60,
            max_bytes: 0,
            max_msgs: 0,
            max_msg_size: 65536,
            discard: "old".to_owned(),
            num_replicas: 1,
            duplicate_window_secs: 10,
        })
        .await
        .unwrap();

    // Publish trades and tickers.
    let ct = serialization::content_type(SerializationFormat::Json);

    for i in 1..=3 {
        let trade = sample_trade_envelope("binance", "BTCUSDT", "BTC/USDT", i);
        let payload = serialization::serialize_envelope(&trade, SerializationFormat::Json).unwrap();
        publisher
            .publish(&format!("{prefix}.binance.btc-usdt.trade"), &payload, ct)
            .await
            .unwrap();
    }

    for i in 1..=2 {
        let ticker = sample_ticker_envelope("binance", "BTCUSDT", "BTC/USDT", i);
        let payload =
            serialization::serialize_envelope(&ticker, SerializationFormat::Json).unwrap();
        publisher
            .publish(&format!("{prefix}.binance.btc-usdt.ticker"), &payload, ct)
            .await
            .unwrap();
    }

    // Verify isolation.
    let mut trades = js.get_stream(&trades_stream).await.unwrap();
    let trades_info = trades.info().await.unwrap();
    assert_eq!(
        trades_info.state.messages, 3,
        "trades stream should have 3 messages"
    );

    let mut tickers = js.get_stream(&ticker_stream).await.unwrap();
    let tickers_info = tickers.info().await.unwrap();
    assert_eq!(
        tickers_info.state.messages, 2,
        "ticker stream should have 2 messages"
    );

    // Cleanup.
    let _ = js.delete_stream(&trades_stream).await;
    let _ = js.delete_stream(&ticker_stream).await;
}

/// Test that protobuf serialized data can be decoded back correctly.
#[tokio::test]
#[ignore]
async fn test_protobuf_roundtrip_through_nats() {
    use market2nats::serialization::proto;
    use prost::Message;

    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client.clone());
    let stream_name = setup_test_stream(&publisher, "PB_ROUNDTRIP").await;

    let envelope = sample_trade_envelope("binance", "ETHUSDT", "ETH/USDT", 99);
    let subject = format!("{stream_name}.binance.eth-usdt.trade");
    let payload =
        serialization::serialize_envelope(&envelope, SerializationFormat::Protobuf).unwrap();
    let ct = serialization::content_type(SerializationFormat::Protobuf);

    publisher.publish(&subject, &payload, ct).await.unwrap();

    // Read back and decode protobuf.
    let js = jetstream::new(client);
    let stream = js.get_stream(&stream_name).await.unwrap();
    let msg = stream.direct_get(1).await.unwrap();

    let pb_envelope = proto::pb::MarketDataEnvelope::decode(msg.payload.as_ref()).unwrap();
    assert_eq!(pb_envelope.venue, "binance");
    assert_eq!(pb_envelope.instrument, "ETHUSDT");
    assert_eq!(pb_envelope.canonical_symbol, "ETH/USDT");
    assert_eq!(pb_envelope.data_type, "trade");
    assert_eq!(pb_envelope.sequence, 99);
    assert!(pb_envelope.payload.is_some());

    if let Some(proto::pb::market_data_envelope::Payload::Trade(trade)) = pb_envelope.payload {
        assert_eq!(trade.price, "50000.50");
        assert_eq!(trade.quantity, "1.25");
        assert_eq!(trade.side, "BUY");
        assert_eq!(trade.trade_id, Some("trade_99".to_owned()));
    } else {
        panic!("expected Trade payload");
    }

    let _ = js.delete_stream(&stream_name).await;
}

/// Test publishing large L2 orderbook snapshot.
#[tokio::test]
#[ignore]
async fn test_publish_large_l2_snapshot() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client.clone());
    let stream_name = setup_test_stream(&publisher, "PUB_LARGE_L2").await;

    let envelope = sample_l2_envelope("binance", "BTCUSDT", "BTC/USDT", 1, true);
    let subject = format!("{stream_name}.binance.btc-usdt.l2_orderbook");
    let payload = serialization::serialize_envelope(&envelope, SerializationFormat::Json).unwrap();
    let ct = serialization::content_type(SerializationFormat::Json);

    publisher.publish(&subject, &payload, ct).await.unwrap();

    // Read back and verify L2 data.
    let js = jetstream::new(client);
    let stream = js.get_stream(&stream_name).await.unwrap();
    let msg = stream.direct_get(1).await.unwrap();

    let deserialized: MarketDataEnvelope = serde_json::from_slice(&msg.payload).unwrap();
    if let market2nats::domain::MarketDataPayload::L2Update(l2) = &deserialized.payload {
        assert!(l2.is_snapshot);
        assert_eq!(l2.bids.len(), 2);
        assert_eq!(l2.asks.len(), 2);
    } else {
        panic!("expected L2Update payload");
    }

    let _ = js.delete_stream(&stream_name).await;
}

/// Test high-throughput publishing (burst of 1000 messages).
#[tokio::test]
#[ignore]
async fn test_burst_publish_1000_messages() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client.clone());
    let stream_name = setup_test_stream(&publisher, "PUB_BURST").await;

    let ct = serialization::content_type(SerializationFormat::Protobuf);
    let count = 1000u64;

    for i in 1..=count {
        let envelope = sample_trade_envelope("binance", "BTCUSDT", "BTC/USDT", i);
        let subject = format!("{stream_name}.binance.btc-usdt.trade");
        let payload =
            serialization::serialize_envelope(&envelope, SerializationFormat::Protobuf).unwrap();
        publisher.publish(&subject, &payload, ct).await.unwrap();
    }

    // Verify message count.
    let js = jetstream::new(client);
    let mut stream = js.get_stream(&stream_name).await.unwrap();
    let info = stream.info().await.unwrap();
    assert_eq!(
        info.state.messages, count,
        "expected {count} messages after burst"
    );

    let _ = js.delete_stream(&stream_name).await;
}
