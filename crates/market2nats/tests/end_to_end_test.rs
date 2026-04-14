//! End-to-end integration tests: full pipeline from mock adapter through to NATS.
//!
//! Tests the complete path: VenueAdapter → SubscriptionManager → SequenceTracker →
//! StreamRouter → Serializer → NatsPublisher → JetStream stream.
//!
//! Requires a running NATS server with JetStream enabled.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use tokio::sync::{mpsc, watch};

use market2nats::application::ports::{NatsPublisher, Subscription, VenueAdapter, VenueError};
use market2nats::application::{HealthMonitor, SequenceTracker, StreamRouter, SubscriptionManager};
use market2nats::config::model::{
    CircuitBreakerConfig, ConnectionConfig, StreamConfig, VenueConfig,
};
use market2nats::domain::{
    CanonicalSymbol, InstrumentId, MarketDataEnvelope, MarketDataPayload, MarketDataType, Price,
    Quantity, Sequence, Side, Timestamp, Trade, VenueId,
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

fn unique_subject_prefix() -> String {
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    format!("test_{id}")
}

// ── Mock VenueAdapter ─────────────────────────────────────────────────────

/// A mock venue adapter that emits a fixed number of events, then disconnects.
struct MockVenueAdapter {
    venue_id: VenueId,
    events_to_emit: Vec<MarketDataEnvelope>,
    index: usize,
    connected: bool,
}

impl MockVenueAdapter {
    fn new(venue_id: &str, events: Vec<MarketDataEnvelope>) -> Self {
        Self {
            venue_id: VenueId::try_new(venue_id).unwrap(),
            events_to_emit: events,
            index: 0,
            connected: false,
        }
    }
}

impl VenueAdapter for MockVenueAdapter {
    fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    fn connect(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), VenueError>> + Send + '_>> {
        Box::pin(async {
            self.connected = true;
            Ok(())
        })
    }

    fn subscribe(
        &mut self,
        _subscriptions: &[Subscription],
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), VenueError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn next_events(
        &mut self,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<MarketDataEnvelope>, VenueError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async {
            if self.index < self.events_to_emit.len() {
                let event = self.events_to_emit[self.index].clone();
                self.index += 1;
                // Small delay to simulate network.
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok(vec![event])
            } else {
                // Simulate disconnection after all events.
                tokio::time::sleep(Duration::from_millis(50)).await;
                Err(VenueError::ReceiveFailed {
                    venue: self.venue_id.as_str().to_owned(),
                    reason: "mock: all events emitted".to_owned(),
                })
            }
        })
    }

    fn disconnect(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), VenueError>> + Send + '_>> {
        Box::pin(async {
            self.connected = false;
            Ok(())
        })
    }

    fn is_connected(&self) -> bool {
        self.connected
    }
}

// ── Test helpers ──────────────────────────────────────────────────────────

fn make_trade(venue: &str, instrument: &str, canonical: &str, seq: u64) -> MarketDataEnvelope {
    use rust_decimal_macros::dec;

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

fn mock_venue_config(venue_id: &str) -> VenueConfig {
    VenueConfig {
        id: venue_id.to_owned(),
        adapter: "mock".to_owned(),
        enabled: true,
        connection: ConnectionConfig {
            ws_url: "wss://mock.example.com".to_owned(),
            reconnect_delay_ms: 100,
            max_reconnect_delay_ms: 500,
            max_reconnect_attempts: 1, // Stop after first reconnect failure
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
        },
        circuit_breaker: Some(CircuitBreakerConfig {
            failure_threshold: 5,
            reset_timeout_secs: 60,
            half_open_max_requests: 2,
        }),
        generic_ws: None,
        subscriptions: vec![],
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

/// Full pipeline: mock adapter emits events → subscription manager picks them up →
/// events go through channel → we publish to NATS → verify in stream.
#[tokio::test]
#[ignore]
async fn test_end_to_end_mock_to_nats() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client.clone(), std::time::Duration::from_secs(10));
    let js = jetstream::new(client);

    // Setup a test stream.
    let prefix = unique_subject_prefix();
    let stream_name = format!("{prefix}_E2E_TRADES");

    publisher
        .ensure_stream(&StreamConfig {
            name: stream_name.clone(),
            subjects: vec![format!("market.mock_venue.*.trade")],
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

    // Create mock events.
    let events: Vec<MarketDataEnvelope> = (1..=5)
        .map(|i| make_trade("mock_venue", "BTCUSDT", "BTC/USDT", i))
        .collect();

    let adapter = MockVenueAdapter::new("mock_venue", events);

    // Setup application components.
    let health_monitor = Arc::new(HealthMonitor::new());
    let sequence_tracker = Arc::new(SequenceTracker::new());
    let stream_router = Arc::new(StreamRouter::new());

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (event_tx, mut event_rx) = mpsc::channel(1000);

    // Spawn venue task.
    let sub_manager =
        SubscriptionManager::new(Arc::clone(&health_monitor), Arc::clone(&sequence_tracker));

    let venue_config = mock_venue_config("mock_venue");
    let venue_handle = sub_manager.spawn_venue_task(
        Box::new(adapter),
        venue_config,
        event_tx,
        shutdown_rx.clone(),
    );

    // Spawn publisher task.
    let pub_publisher = Arc::new(publisher);
    let pub_router = Arc::clone(&stream_router);
    let publisher_handle = tokio::spawn(async move {
        let format = SerializationFormat::Json;
        let ct = serialization::content_type(format);
        let mut published = 0u64;

        while let Some(envelope) = event_rx.recv().await {
            let subject = pub_router.resolve_subject(&envelope);
            let payload = serialization::serialize_envelope(&envelope, format).unwrap();
            pub_publisher.publish(&subject, &payload, ct).await.unwrap();
            published += 1;
        }

        published
    });

    // Wait for venue task to finish (mock emits 5 events then errors).
    // Give it some time, then shut down.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let _ = shutdown_tx.send(true);

    let _ = venue_handle.await;
    let published = publisher_handle.await.unwrap();

    // Verify events in NATS.
    assert!(published >= 5, "expected >= 5 published, got {published}");

    let mut stream = js.get_stream(&stream_name).await.unwrap();
    let info = stream.info().await.unwrap();
    assert!(
        info.state.messages >= 5,
        "expected >= 5 messages in stream, got {}",
        info.state.messages
    );

    // Read first message and verify sequence was assigned.
    let msg = stream.get_raw_message(1).await.unwrap();
    let envelope: MarketDataEnvelope = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(envelope.venue.as_str(), "mock_venue");
    // Sequence should be 1 (first event for this stream key).
    assert_eq!(envelope.sequence.value(), 1);

    let _ = js.delete_stream(&stream_name).await;
}

/// Test that multiple venue adapters publish to different subjects correctly.
#[tokio::test]
#[ignore]
async fn test_end_to_end_multiple_venues() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client.clone(), std::time::Duration::from_secs(10));
    let js = jetstream::new(client);

    let prefix = unique_subject_prefix();
    let stream_name = format!("{prefix}_MULTI_VENUE");

    publisher
        .ensure_stream(&StreamConfig {
            name: stream_name.clone(),
            subjects: vec![
                format!("market.venue_a.*.trade"),
                format!("market.venue_b.*.trade"),
            ],
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

    let events_a: Vec<MarketDataEnvelope> = (1..=3)
        .map(|i| make_trade("venue_a", "BTCUSDT", "BTC/USDT", i))
        .collect();
    let events_b: Vec<MarketDataEnvelope> = (1..=2)
        .map(|i| make_trade("venue_b", "ETHUSDT", "ETH/USDT", i))
        .collect();

    let adapter_a = MockVenueAdapter::new("venue_a", events_a);
    let adapter_b = MockVenueAdapter::new("venue_b", events_b);

    let health_monitor = Arc::new(HealthMonitor::new());
    let sequence_tracker = Arc::new(SequenceTracker::new());
    let stream_router = Arc::new(StreamRouter::new());

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (event_tx, mut event_rx) = mpsc::channel(1000);

    let sub_manager =
        SubscriptionManager::new(Arc::clone(&health_monitor), Arc::clone(&sequence_tracker));

    let h_a = sub_manager.spawn_venue_task(
        Box::new(adapter_a),
        mock_venue_config("venue_a"),
        event_tx.clone(),
        shutdown_rx.clone(),
    );
    let h_b = sub_manager.spawn_venue_task(
        Box::new(adapter_b),
        mock_venue_config("venue_b"),
        event_tx,
        shutdown_rx.clone(),
    );

    let pub_publisher = Arc::new(publisher);
    let pub_router = Arc::clone(&stream_router);
    let publisher_handle = tokio::spawn(async move {
        let format = SerializationFormat::Json;
        let ct = serialization::content_type(format);
        let mut count = 0u64;

        while let Some(envelope) = event_rx.recv().await {
            let subject = pub_router.resolve_subject(&envelope);
            let payload = serialization::serialize_envelope(&envelope, format).unwrap();
            pub_publisher.publish(&subject, &payload, ct).await.unwrap();
            count += 1;
        }
        count
    });

    tokio::time::sleep(Duration::from_secs(2)).await;
    let _ = shutdown_tx.send(true);

    let _ = h_a.await;
    let _ = h_b.await;
    let total = publisher_handle.await.unwrap();

    assert!(total >= 5, "expected >= 5 total published, got {total}");

    let mut stream = js.get_stream(&stream_name).await.unwrap();
    let info = stream.info().await.unwrap();
    assert!(
        info.state.messages >= 5,
        "expected >= 5 messages in stream, got {}",
        info.state.messages
    );

    let _ = js.delete_stream(&stream_name).await;
}

/// Test that sequence numbers are correctly assigned per (venue, instrument, data_type).
#[tokio::test]
#[ignore]
async fn test_end_to_end_sequence_assignment() {
    let client = connect_nats().await;
    let publisher = JetStreamPublisher::new(client.clone(), std::time::Duration::from_secs(10));
    let js = jetstream::new(client);

    let prefix = unique_subject_prefix();
    let stream_name = format!("{prefix}_SEQ_ASSIGN");

    publisher
        .ensure_stream(&StreamConfig {
            name: stream_name.clone(),
            subjects: vec![format!("market.seq_venue.*.trade")],
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

    // Emit 10 events — all will get sequence reassigned by SequenceTracker.
    let events: Vec<MarketDataEnvelope> = (1..=10)
        .map(|_i| make_trade("seq_venue", "BTCUSDT", "BTC/USDT", 0)) // sequence=0, will be overwritten
        .collect();

    let adapter = MockVenueAdapter::new("seq_venue", events);

    let health_monitor = Arc::new(HealthMonitor::new());
    let sequence_tracker = Arc::new(SequenceTracker::new());
    let stream_router = Arc::new(StreamRouter::new());

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (event_tx, mut event_rx) = mpsc::channel(1000);

    let sub_manager =
        SubscriptionManager::new(Arc::clone(&health_monitor), Arc::clone(&sequence_tracker));

    let venue_handle = sub_manager.spawn_venue_task(
        Box::new(adapter),
        mock_venue_config("seq_venue"),
        event_tx,
        shutdown_rx.clone(),
    );

    let pub_publisher = Arc::new(publisher);
    let pub_router = Arc::clone(&stream_router);
    let publisher_handle = tokio::spawn(async move {
        let format = SerializationFormat::Json;
        let ct = serialization::content_type(format);

        while let Some(envelope) = event_rx.recv().await {
            let subject = pub_router.resolve_subject(&envelope);
            let payload = serialization::serialize_envelope(&envelope, format).unwrap();
            pub_publisher.publish(&subject, &payload, ct).await.unwrap();
        }
    });

    tokio::time::sleep(Duration::from_secs(2)).await;
    let _ = shutdown_tx.send(true);
    let _ = venue_handle.await;
    let _ = publisher_handle.await;

    // Read all messages and verify sequence is 1..=10.
    let mut stream = js.get_stream(&stream_name).await.unwrap();
    let info = stream.info().await.unwrap();
    let msg_count = info.state.messages;

    let mut sequences = Vec::new();
    for seq_num in 1..=msg_count {
        if let Ok(msg) = stream.get_raw_message(seq_num).await {
            let envelope: MarketDataEnvelope = serde_json::from_slice(&msg.payload).unwrap();
            sequences.push(envelope.sequence.value());
        }
    }

    // Sequences should be monotonically increasing starting at 1.
    assert!(!sequences.is_empty(), "no messages found");
    for (i, seq) in sequences.iter().enumerate() {
        assert_eq!(
            *seq,
            (i as u64) + 1,
            "sequence mismatch at index {i}: expected {}, got {seq}",
            i + 1
        );
    }

    let _ = js.delete_stream(&stream_name).await;
}
