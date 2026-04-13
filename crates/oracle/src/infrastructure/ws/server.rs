//! WebSocket server that fans out oracle prices to connected clients.
//!
//! Clients connect via WebSocket and send JSON subscribe/unsubscribe messages
//! to control which symbols they receive. The server broadcasts computed oracle
//! prices to all matching clients via a `tokio::sync::broadcast` channel.

use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, watch};
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::application::metrics::{ORACLE_WS_CONNECTED_CLIENTS, ORACLE_WS_MESSAGES_SENT};
use crate::application::ports::OraclePublisher;
use crate::domain::{OracleError, OraclePrice};

/// Broadcast channel capacity for oracle price messages.
const BROADCAST_CAPACITY: usize = 256;

/// Message sent by clients to subscribe or unsubscribe from symbols.
#[derive(Debug, Deserialize)]
struct ClientMessage {
    /// Action: "subscribe" or "unsubscribe".
    action: String,
    /// List of symbol strings. Use "all" to receive all symbols.
    symbols: Vec<String>,
}

/// WebSocket server that fans out oracle prices to connected clients.
///
/// Implements `OraclePublisher` so it can be composed with `FanOutPublisher`.
/// Each published oracle price is serialized to JSON and broadcast to all
/// connected clients whose subscription filter matches the symbol.
pub struct OracleWsServer {
    /// Broadcast sender: `(symbol_normalized, json_payload)`.
    tx: broadcast::Sender<(String, Bytes)>,
    /// Count of currently connected clients.
    connected_clients: Arc<AtomicUsize>,
}

impl Default for OracleWsServer {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleWsServer {
    /// Creates a new WebSocket server with an empty broadcast channel.
    #[must_use]
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            tx,
            connected_clients: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns the current number of connected clients.
    #[inline]
    #[must_use]
    pub fn connected_clients(&self) -> usize {
        self.connected_clients.load(Ordering::Relaxed)
    }

    /// Runs the TCP listener, accepting WebSocket connections until shutdown.
    ///
    /// Each accepted connection is spawned as an independent task that manages
    /// its own subscription filter and message dispatch.
    ///
    /// # Errors
    ///
    /// Returns `OracleError::Nats` (reusing the I/O error variant) if binding
    /// the TCP listener fails.
    #[tracing::instrument(skip(self, shutdown), fields(port = port, path = %path))]
    pub async fn run(
        self: Arc<Self>,
        port: u16,
        path: &str,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), OracleError> {
        let addr: SocketAddr = ([0, 0, 0, 0], port).into();
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| OracleError::nats(format!("ws server bind {addr}: {e}")))?;

        tracing::info!(addr = %addr, path = %path, "WebSocket server started");

        let path_owned = path.to_owned();

        loop {
            tokio::select! {
                biased;

                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("WebSocket server shutdown received");
                        break;
                    }
                }

                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            let server = Arc::clone(&self);
                            let expected_path = path_owned.clone();
                            let rx = self.tx.subscribe();
                            let shutdown_rx = shutdown.clone();

                            tokio::spawn(async move {
                                if let Err(e) = server
                                    .handle_connection(stream, peer_addr, &expected_path, rx, shutdown_rx)
                                    .await
                                {
                                    tracing::debug!(
                                        peer = %peer_addr,
                                        error = %e,
                                        "client connection ended with error"
                                    );
                                }
                            });
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to accept TCP connection");
                        }
                    }
                }
            }
        }

        tracing::info!("WebSocket server stopped");
        Ok(())
    }

    /// Handles a single WebSocket client connection.
    ///
    /// Validates the HTTP upgrade path, then enters a loop that:
    /// - Reads client messages (subscribe/unsubscribe) to update the filter
    /// - Forwards matching broadcast messages as WebSocket text frames
    async fn handle_connection(
        &self,
        stream: TcpStream,
        peer_addr: SocketAddr,
        expected_path: &str,
        mut broadcast_rx: broadcast::Receiver<(String, Bytes)>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), OracleError> {
        // Perform WebSocket handshake with path validation.
        // The error type is dictated by the `tokio-tungstenite` callback API.
        let expected = expected_path.to_owned();
        #[allow(clippy::result_large_err)]
        let ws_stream = tokio_tungstenite::accept_hdr_async(
            stream,
            move |req: &http::Request<()>,
                  resp: http::Response<()>|
                  -> Result<http::Response<()>, http::Response<Option<String>>> {
                let req_path = req.uri().path();
                if req_path == expected {
                    Ok(resp)
                } else {
                    let rejection = http::Response::builder()
                        .status(http::StatusCode::NOT_FOUND)
                        .body(Some("not found".to_owned()))
                        .unwrap_or_else(|_| http::Response::new(Some("not found".to_owned())));
                    Err(rejection)
                }
            },
        )
        .await
        .map_err(|e| OracleError::nats(format!("ws handshake failed for {peer_addr}: {e}")))?;

        tracing::debug!(peer = %peer_addr, "WebSocket client connected");

        // Track connection in metrics.
        self.connected_clients.fetch_add(1, Ordering::Relaxed);
        metrics::gauge!(ORACLE_WS_CONNECTED_CLIENTS)
            .set(self.connected_clients.load(Ordering::Relaxed) as f64);

        let (mut ws_sink, mut ws_source) = ws_stream.split();

        // Subscription filter state.
        let mut subscribed_all = false;
        let mut subscribed_symbols: HashSet<String> = HashSet::new();

        let result: Result<(), OracleError> = async {
            loop {
                tokio::select! {
                    biased;

                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break;
                        }
                    }

                    // Read client messages to update the subscription filter.
                    client_msg = ws_source.next() => {
                        match client_msg {
                            Some(Ok(Message::Text(text))) => {
                                Self::handle_client_message(
                                    &text,
                                    &mut subscribed_all,
                                    &mut subscribed_symbols,
                                    peer_addr,
                                );
                            }
                            Some(Ok(Message::Close(_))) | None => {
                                tracing::debug!(peer = %peer_addr, "client disconnected");
                                break;
                            }
                            Some(Ok(Message::Ping(data))) => {
                                if let Err(e) = ws_sink.send(Message::Pong(data)).await {
                                    tracing::debug!(peer = %peer_addr, error = %e, "failed to send pong");
                                    break;
                                }
                            }
                            Some(Ok(_)) => {
                                // Ignore binary, pong, etc.
                            }
                            Some(Err(e)) => {
                                tracing::debug!(peer = %peer_addr, error = %e, "ws read error");
                                break;
                            }
                        }
                    }

                    // Receive broadcast messages and forward if filter matches.
                    broadcast_msg = broadcast_rx.recv() => {
                        match broadcast_msg {
                            Ok((symbol_normalized, payload)) => {
                                if subscribed_all || subscribed_symbols.contains(&symbol_normalized) {
                                    let text = String::from_utf8_lossy(&payload).into_owned();
                                    if let Err(e) = ws_sink.send(Message::Text(text.into())).await {
                                        tracing::debug!(
                                            peer = %peer_addr,
                                            error = %e,
                                            "failed to send ws message"
                                        );
                                        break;
                                    }
                                    metrics::counter!(
                                        ORACLE_WS_MESSAGES_SENT,
                                        "symbol" => symbol_normalized
                                    )
                                    .increment(1);
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!(
                                    peer = %peer_addr,
                                    lagged = n,
                                    "client lagged behind broadcast, disconnecting"
                                );
                                break;
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                tracing::debug!(peer = %peer_addr, "broadcast channel closed");
                                break;
                            }
                        }
                    }
                }
            }

            Ok(())
        }
        .await;

        // Clean up: decrement connected count and update gauge.
        self.connected_clients.fetch_sub(1, Ordering::Relaxed);
        metrics::gauge!(ORACLE_WS_CONNECTED_CLIENTS)
            .set(self.connected_clients.load(Ordering::Relaxed) as f64);

        tracing::debug!(peer = %peer_addr, "WebSocket client handler exiting");
        result
    }

    /// Processes a client subscribe/unsubscribe message.
    fn handle_client_message(
        text: &str,
        subscribed_all: &mut bool,
        subscribed_symbols: &mut HashSet<String>,
        peer_addr: SocketAddr,
    ) {
        let msg: ClientMessage = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!(
                    peer = %peer_addr,
                    error = %e,
                    "failed to parse client message"
                );
                return;
            }
        };

        match msg.action.as_str() {
            "subscribe" => {
                for symbol in &msg.symbols {
                    if symbol == "all" {
                        *subscribed_all = true;
                        tracing::debug!(peer = %peer_addr, "subscribed to all symbols");
                    } else {
                        subscribed_symbols.insert(symbol.clone());
                        tracing::debug!(
                            peer = %peer_addr,
                            symbol = %symbol,
                            "subscribed to symbol"
                        );
                    }
                }
            }
            "unsubscribe" => {
                for symbol in &msg.symbols {
                    if symbol == "all" {
                        *subscribed_all = false;
                        tracing::debug!(peer = %peer_addr, "unsubscribed from all symbols");
                    } else {
                        subscribed_symbols.remove(symbol);
                        tracing::debug!(
                            peer = %peer_addr,
                            symbol = %symbol,
                            "unsubscribed from symbol"
                        );
                    }
                }
            }
            other => {
                tracing::debug!(
                    peer = %peer_addr,
                    action = %other,
                    "unknown client action"
                );
            }
        }
    }
}

impl OraclePublisher for OracleWsServer {
    fn publish(
        &self,
        price: &OraclePrice,
    ) -> Pin<Box<dyn Future<Output = Result<(), OracleError>> + Send + '_>> {
        // Eagerly serialize and compute symbol before the async block.
        let symbol_normalized = price.symbol.normalized();

        let serialized = match serde_json::to_vec(price) {
            Ok(b) => b,
            Err(e) => {
                return Box::pin(async move { Err(OracleError::serialization(e.to_string())) });
            }
        };

        let payload = Bytes::from(serialized);

        // Send to broadcast channel. If no receivers, that is fine.
        match self.tx.send((symbol_normalized, payload)) {
            Ok(_) => {}
            Err(_) => {
                // No active receivers -- not an error condition.
                tracing::trace!("no active WebSocket receivers");
            }
        }

        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_server_connected_clients_starts_at_zero() {
        let server = OracleWsServer::new();
        assert_eq!(server.connected_clients(), 0);
    }

    #[test]
    fn test_ws_server_new_has_no_receivers() {
        let server = OracleWsServer::new();
        // The broadcast channel starts with no receivers, so a send will
        // return Err (no active receivers). This is the expected initial state.
        let result = server.tx.send(("test".to_owned(), Bytes::new()));
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_client_message_subscribe_all() {
        let mut subscribed_all = false;
        let mut symbols = HashSet::new();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"subscribe","symbols":["all"]}"#;
        OracleWsServer::handle_client_message(msg, &mut subscribed_all, &mut symbols, addr);

        assert!(subscribed_all);
    }

    #[test]
    fn test_handle_client_message_subscribe_specific() {
        let mut subscribed_all = false;
        let mut symbols = HashSet::new();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"subscribe","symbols":["btc-usdt","eth-usdt"]}"#;
        OracleWsServer::handle_client_message(msg, &mut subscribed_all, &mut symbols, addr);

        assert!(!subscribed_all);
        assert!(symbols.contains("btc-usdt"));
        assert!(symbols.contains("eth-usdt"));
    }

    #[test]
    fn test_handle_client_message_unsubscribe() {
        let mut subscribed_all = false;
        let mut symbols = HashSet::new();
        symbols.insert("btc-usdt".to_owned());
        symbols.insert("eth-usdt".to_owned());
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"unsubscribe","symbols":["btc-usdt"]}"#;
        OracleWsServer::handle_client_message(msg, &mut subscribed_all, &mut symbols, addr);

        assert!(!symbols.contains("btc-usdt"));
        assert!(symbols.contains("eth-usdt"));
    }

    #[test]
    fn test_handle_client_message_invalid_json() {
        let mut subscribed_all = false;
        let mut symbols = HashSet::new();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        // Should not panic, just log and return.
        OracleWsServer::handle_client_message("not json", &mut subscribed_all, &mut symbols, addr);

        assert!(!subscribed_all);
        assert!(symbols.is_empty());
    }

    #[test]
    fn test_handle_client_message_unknown_action() {
        let mut subscribed_all = false;
        let mut symbols = HashSet::new();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"reset","symbols":["btc-usdt"]}"#;
        OracleWsServer::handle_client_message(msg, &mut subscribed_all, &mut symbols, addr);

        assert!(!subscribed_all);
        assert!(symbols.is_empty());
    }
}
