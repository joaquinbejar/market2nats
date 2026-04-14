//! Integration tests for the Oracle WebSocket server.
//!
//! Tests spin up an in-process `OracleWsServer`, connect a
//! `tokio-tungstenite` client, and verify end-to-end message flow.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use market2nats_domain::{CanonicalSymbol, Price, Quantity, Timestamp, VenueId};
use oracle::application::ports::OraclePublisher;
use oracle::config::model::WebSocketConfig;
use oracle::domain::strategies::AggregationStrategyKind;
use oracle::domain::types::{OracleConfidence, OraclePrice, PriceSource};
use oracle::infrastructure::OracleWsServer;
use rust_decimal_macros::dec;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

fn ws_config(port: u16, path: &str) -> WebSocketConfig {
    WebSocketConfig {
        enabled: true,
        port,
        path: path.to_owned(),
        tls_enabled: false,
        tls_cert_file: None,
        tls_key_file: None,
    }
}

fn make_oracle_price(symbol: &str) -> OraclePrice {
    OraclePrice {
        symbol: CanonicalSymbol::try_new(symbol).expect("valid symbol"),
        price: Price::try_new(dec!(50000)).expect("valid price"),
        timestamp: Timestamp::now(),
        sources: vec![PriceSource {
            venue: VenueId::try_new("binance").expect("valid venue"),
            price: Price::try_new(dec!(50000)).expect("valid price"),
            quantity: Quantity::try_new(dec!(1)).expect("valid quantity"),
            timestamp: Timestamp::now(),
            age_ms: 100,
        }],
        strategy: AggregationStrategyKind::Median,
        confidence: OracleConfidence::High,
    }
}

#[tokio::test]
#[ignore] // Requires no external services, but uses real TCP ports.
async fn test_ws_client_connects_subscribes_receives_price() {
    let server = Arc::new(OracleWsServer::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Pick an available port.
    let port: u16 = portpicker::pick_unused_port().expect("no free port");

    let server_clone = Arc::clone(&server);
    let cfg = ws_config(port, "/");
    let server_handle = tokio::spawn(async move {
        let _ = server_clone.run(&cfg, shutdown_rx).await;
    });

    // Give the server a moment to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect a WebSocket client.
    let url = format!("ws://127.0.0.1:{port}/");
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("client connect failed");

    assert_eq!(server.connected_clients(), 1);

    // Subscribe to btc-usdt.
    let subscribe_msg = r#"{"action":"subscribe","symbols":["btc-usdt"]}"#;
    ws_stream
        .send(Message::Text(subscribe_msg.into()))
        .await
        .expect("send subscribe");

    // Give the server time to process the subscription message.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Publish an oracle price.
    let price = make_oracle_price("BTC/USDT");
    server
        .publish(&price)
        .await
        .expect("publish should succeed");

    // Read the message from the client.
    let msg = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
        .await
        .expect("timeout waiting for message")
        .expect("stream closed")
        .expect("ws read error");

    match msg {
        Message::Text(text) => {
            let parsed: serde_json::Value = serde_json::from_str(&text).expect("valid JSON");
            assert_eq!(
                parsed.get("symbol").and_then(|v| v.as_str()),
                Some("BTC/USDT")
            );
        }
        other => panic!("expected text message, got: {other:?}"),
    }

    // Shutdown.
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
}

#[tokio::test]
#[ignore]
async fn test_ws_client_unsubscribed_receives_nothing() {
    let server = Arc::new(OracleWsServer::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let port: u16 = portpicker::pick_unused_port().expect("no free port");

    let server_clone = Arc::clone(&server);
    let cfg = ws_config(port, "/");
    let server_handle = tokio::spawn(async move {
        let _ = server_clone.run(&cfg, shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = format!("ws://127.0.0.1:{port}/");
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("client connect failed");

    // Do NOT subscribe to anything.

    // Publish an oracle price.
    let price = make_oracle_price("BTC/USDT");
    server
        .publish(&price)
        .await
        .expect("publish should succeed");

    // Client should not receive anything; timeout expected.
    let result = tokio::time::timeout(Duration::from_millis(200), ws_stream.next()).await;
    assert!(
        result.is_err(),
        "expected timeout, client should not receive unsubscribed messages"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
}

#[tokio::test]
#[ignore]
async fn test_ws_client_subscribe_all_receives_all_symbols() {
    let server = Arc::new(OracleWsServer::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let port: u16 = portpicker::pick_unused_port().expect("no free port");

    let server_clone = Arc::clone(&server);
    let cfg = ws_config(port, "/");
    let server_handle = tokio::spawn(async move {
        let _ = server_clone.run(&cfg, shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = format!("ws://127.0.0.1:{port}/");
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("client connect failed");

    // Subscribe to all.
    let subscribe_msg = r#"{"action":"subscribe","symbols":["all"]}"#;
    ws_stream
        .send(Message::Text(subscribe_msg.into()))
        .await
        .expect("send subscribe");

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Publish BTC and ETH prices.
    let btc_price = make_oracle_price("BTC/USDT");
    let eth_price = make_oracle_price("ETH/USDT");
    server.publish(&btc_price).await.expect("publish btc");
    server.publish(&eth_price).await.expect("publish eth");

    // Should receive both.
    let msg1 = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
        .await
        .expect("timeout waiting for first message")
        .expect("stream closed")
        .expect("ws read error");
    assert!(matches!(msg1, Message::Text(_)));

    let msg2 = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
        .await
        .expect("timeout waiting for second message")
        .expect("stream closed")
        .expect("ws read error");
    assert!(matches!(msg2, Message::Text(_)));

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
}

#[tokio::test]
#[ignore]
async fn test_ws_wrong_path_rejected() {
    let server = Arc::new(OracleWsServer::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let port: u16 = portpicker::pick_unused_port().expect("no free port");

    let server_clone = Arc::clone(&server);
    let cfg = ws_config(port, "/ws");
    let server_handle = tokio::spawn(async move {
        let _ = server_clone.run(&cfg, shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Try to connect to the wrong path.
    let url = format!("ws://127.0.0.1:{port}/wrong");
    let result = tokio_tungstenite::connect_async(&url).await;
    assert!(
        result.is_err(),
        "connection to wrong path should be rejected"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
}

/// Regression test for the symbol-form bug: subscribing with the canonical
/// form `BTC/USDT` must match prices published with the normalized symbol
/// `btc-usdt`. Also verifies that ETH prices are not delivered.
#[tokio::test]
#[ignore]
async fn test_ws_client_subscribes_with_canonical_symbol_form() {
    let server = Arc::new(OracleWsServer::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let port: u16 = portpicker::pick_unused_port().expect("no free port");

    let server_clone = Arc::clone(&server);
    let cfg = ws_config(port, "/");
    let server_handle = tokio::spawn(async move {
        let _ = server_clone.run(&cfg, shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = format!("ws://127.0.0.1:{port}/");
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("client connect failed");

    // Subscribe using the canonical form.
    let subscribe_msg = r#"{"action":"subscribe","symbols":["BTC/USDT"]}"#;
    ws_stream
        .send(Message::Text(subscribe_msg.into()))
        .await
        .expect("send subscribe");
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Publish BTC and ETH; only BTC should arrive.
    server
        .publish(&make_oracle_price("BTC/USDT"))
        .await
        .expect("publish btc");
    server
        .publish(&make_oracle_price("ETH/USDT"))
        .await
        .expect("publish eth");

    let msg = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
        .await
        .expect("timeout waiting for message")
        .expect("stream closed")
        .expect("ws read error");
    match msg {
        Message::Text(text) => {
            let parsed: serde_json::Value = serde_json::from_str(&text).expect("valid JSON");
            assert_eq!(
                parsed.get("symbol").and_then(|v| v.as_str()),
                Some("BTC/USDT")
            );
        }
        other => panic!("expected text message, got: {other:?}"),
    }

    // ETH should not arrive.
    let result = tokio::time::timeout(Duration::from_millis(200), ws_stream.next()).await;
    assert!(
        result.is_err(),
        "ETH price should not be delivered to BTC subscriber"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
}

/// Verifies that unsubscribing stops further deliveries for that symbol.
#[tokio::test]
#[ignore]
async fn test_ws_client_unsubscribe_stops_delivery() {
    let server = Arc::new(OracleWsServer::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let port: u16 = portpicker::pick_unused_port().expect("no free port");

    let server_clone = Arc::clone(&server);
    let cfg = ws_config(port, "/");
    let server_handle = tokio::spawn(async move {
        let _ = server_clone.run(&cfg, shutdown_rx).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = format!("ws://127.0.0.1:{port}/");
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("client connect failed");

    // Subscribe to BTC/USDT.
    ws_stream
        .send(Message::Text(
            r#"{"action":"subscribe","symbols":["BTC/USDT"]}"#.into(),
        ))
        .await
        .expect("send subscribe");
    tokio::time::sleep(Duration::from_millis(20)).await;

    // First publish should arrive.
    server
        .publish(&make_oracle_price("BTC/USDT"))
        .await
        .expect("publish btc");
    let _ = tokio::time::timeout(Duration::from_secs(2), ws_stream.next())
        .await
        .expect("timeout waiting for first message")
        .expect("stream closed")
        .expect("ws read error");

    // Now unsubscribe.
    ws_stream
        .send(Message::Text(
            r#"{"action":"unsubscribe","symbols":["BTC/USDT"]}"#.into(),
        ))
        .await
        .expect("send unsubscribe");
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Publish again; nothing should arrive.
    server
        .publish(&make_oracle_price("BTC/USDT"))
        .await
        .expect("publish btc again");
    let result = tokio::time::timeout(Duration::from_millis(200), ws_stream.next()).await;
    assert!(
        result.is_err(),
        "no message expected after unsubscribe, but client received one"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
}
