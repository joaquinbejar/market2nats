//! WebSocket server that fans out oracle prices to connected clients.
//!
//! Clients connect via WebSocket and send JSON subscribe/unsubscribe messages
//! to control which symbols they receive. The server broadcasts computed oracle
//! prices to all matching clients via a `tokio::sync::broadcast` channel.

use std::collections::HashSet;
use std::fs::File;
use std::future::Future;
use std::io::BufReader;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, ServerConfig};
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::application::metrics::{ORACLE_WS_CONNECTED_CLIENTS, ORACLE_WS_MESSAGES_SENT};
use crate::application::ports::OraclePublisher;
use crate::config::model::WebSocketConfig;
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

    /// Binds the TCP listener and prepares any TLS acceptor.
    ///
    /// Returning a `BoundWsServer` separates port acquisition (which can fail
    /// fast at startup) from the long-running accept loop in
    /// [`BoundWsServer::serve`]. `main.rs` should call this synchronously
    /// before spawning, so a port conflict or bad TLS material aborts
    /// startup instead of being logged from inside a detached task.
    ///
    /// # Errors
    ///
    /// Returns `OracleError::Nats` if the TCP bind fails or the TLS material
    /// cannot be loaded.
    pub async fn bind(
        self: Arc<Self>,
        config: &WebSocketConfig,
    ) -> Result<BoundWsServer, OracleError> {
        let addr: SocketAddr = ([0, 0, 0, 0], config.port).into();
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| OracleError::nats(format!("ws server bind {addr}: {e}")))?;

        let tls_acceptor: Option<TlsAcceptor> = if config.tls_enabled {
            let cert_path = config.tls_cert_file.as_deref().ok_or_else(|| {
                OracleError::nats("websocket.tls_enabled requires tls_cert_file".to_owned())
            })?;
            let key_path = config.tls_key_file.as_deref().ok_or_else(|| {
                OracleError::nats("websocket.tls_enabled requires tls_key_file".to_owned())
            })?;
            let server_config = load_tls_config(cert_path, key_path)?;
            Some(TlsAcceptor::from(Arc::new(server_config)))
        } else {
            None
        };

        tracing::info!(
            addr = %addr,
            path = %config.path,
            tls = config.tls_enabled,
            "WebSocket server bound"
        );

        Ok(BoundWsServer {
            server: self,
            listener,
            tls_acceptor,
            path: config.path.clone(),
        })
    }

    /// Convenience: bind and serve in one call.
    ///
    /// Equivalent to `bind(config).await?.serve(shutdown).await`. Kept for
    /// in-process integration tests; production code should call `bind` and
    /// `serve` separately so bind errors surface in `main`.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::bind`].
    #[tracing::instrument(
        skip(self, config, shutdown),
        fields(port = config.port, path = %config.path, tls = config.tls_enabled)
    )]
    pub async fn run(
        self: Arc<Self>,
        config: &WebSocketConfig,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), OracleError> {
        self.bind(config).await?.serve(shutdown).await
    }
}

/// A WebSocket server that has acquired its TCP listener and (optionally) its
/// TLS acceptor. Returned by [`OracleWsServer::bind`]; consumed by
/// [`BoundWsServer::serve`] to run the accept loop.
pub struct BoundWsServer {
    server: Arc<OracleWsServer>,
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
    path: String,
}

impl BoundWsServer {
    /// Returns the local address the listener is bound to (useful for tests
    /// that pick port `0` and then need to know what port the OS assigned).
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error if the local address cannot be read.
    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    /// Runs the accept loop until `shutdown` flips to `true`. Consumes self.
    pub async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<(), OracleError> {
        let BoundWsServer {
            server,
            listener,
            tls_acceptor,
            path,
        } = self;

        tracing::info!(
            addr = ?listener.local_addr().ok(),
            path = %path,
            tls = tls_acceptor.is_some(),
            "WebSocket server started"
        );

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
                            let conn_server = Arc::clone(&server);
                            let expected_path = path.clone();
                            let rx = server.tx.subscribe();
                            let shutdown_rx = shutdown.clone();
                            let acceptor = tls_acceptor.clone();

                            tokio::spawn(async move {
                                let result = match acceptor {
                                    Some(acc) => match acc.accept(stream).await {
                                        Ok(tls_stream) => {
                                            conn_server
                                                .handle_connection(
                                                    tls_stream,
                                                    peer_addr,
                                                    &expected_path,
                                                    rx,
                                                    shutdown_rx,
                                                )
                                                .await
                                        }
                                        Err(e) => Err(OracleError::nats(format!(
                                            "tls handshake failed for {peer_addr}: {e}"
                                        ))),
                                    },
                                    None => {
                                        conn_server
                                            .handle_connection(
                                                stream,
                                                peer_addr,
                                                &expected_path,
                                                rx,
                                                shutdown_rx,
                                            )
                                            .await
                                    }
                                };

                                if let Err(e) = result {
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
}

impl OracleWsServer {
    /// Handles a single WebSocket client connection.
    ///
    /// Generic over the underlying transport so the same code path serves both
    /// plain TCP (`ws://`) and TLS (`wss://`).
    ///
    /// Validates the HTTP upgrade path, then enters a loop that:
    /// - Reads client messages (subscribe/unsubscribe) to update the filter
    /// - Forwards matching broadcast messages as WebSocket text frames
    async fn handle_connection<S>(
        &self,
        stream: S,
        peer_addr: SocketAddr,
        expected_path: &str,
        mut broadcast_rx: broadcast::Receiver<(String, Bytes)>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), OracleError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
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

        // Subscription filter state. The matching rule is:
        //   send if subscribed_symbols.contains(symbol)
        //     OR  (subscribed_all && !excluded_symbols.contains(symbol))
        // So an explicit subscribe always wins, and `unsubscribe X` carves a
        // hole in the wildcard so users can do `subscribe all` + `unsubscribe ETH/USDT`.
        let mut subscribed_all = false;
        let mut subscribed_symbols: HashSet<String> = HashSet::new();
        let mut excluded_symbols: HashSet<String> = HashSet::new();

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
                                    &mut excluded_symbols,
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
                                let matches = subscribed_symbols.contains(&symbol_normalized)
                                    || (subscribed_all
                                        && !excluded_symbols.contains(&symbol_normalized));
                                if matches {
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
    ///
    /// Symbols are normalized to the NATS-subject form (lowercase, `/` → `-`)
    /// before being inserted into the filter set, so the client can use any
    /// of `BTC/USDT`, `btc/usdt`, `BTC-USDT`, or `btc-usdt` interchangeably.
    ///
    /// The filter has three pieces of state:
    /// - `subscribed_symbols`: explicit allowlist; an entry here is always sent.
    /// - `subscribed_all`: wildcard flag; when true, anything not in
    ///   `excluded_symbols` is sent.
    /// - `excluded_symbols`: denylist that carves holes in the wildcard, so
    ///   `subscribe all` + `unsubscribe ETH/USDT` does what users expect.
    fn handle_client_message(
        text: &str,
        subscribed_all: &mut bool,
        subscribed_symbols: &mut HashSet<String>,
        excluded_symbols: &mut HashSet<String>,
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
                    let normalized = normalize_symbol(symbol);
                    if normalized == "all" {
                        *subscribed_all = true;
                        // Re-subscribing to "all" cancels any prior exclusions.
                        excluded_symbols.clear();
                        tracing::debug!(peer = %peer_addr, "subscribed to all symbols");
                    } else {
                        // Explicit subscribe trumps an exclusion for the same symbol.
                        excluded_symbols.remove(&normalized);
                        subscribed_symbols.insert(normalized.clone());
                        tracing::debug!(
                            peer = %peer_addr,
                            symbol = %normalized,
                            "subscribed to symbol"
                        );
                    }
                }
            }
            "unsubscribe" => {
                for symbol in &msg.symbols {
                    let normalized = normalize_symbol(symbol);
                    if normalized == "all" {
                        // "all" on unsubscribe clears the wildcard *and* every
                        // explicit subscription / exclusion — the intuitive
                        // "stop sending me anything" operation.
                        *subscribed_all = false;
                        subscribed_symbols.clear();
                        excluded_symbols.clear();
                        tracing::debug!(
                            peer = %peer_addr,
                            "unsubscribed from all symbols (cleared filter)"
                        );
                    } else {
                        // Drop any explicit subscription, and add to the
                        // exclusion set so the wildcard skips this symbol too.
                        subscribed_symbols.remove(&normalized);
                        excluded_symbols.insert(normalized.clone());
                        tracing::debug!(
                            peer = %peer_addr,
                            symbol = %normalized,
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

/// Normalizes a client-provided symbol to the NATS subject form used by
/// `CanonicalSymbol::normalized()`: lowercase with `/` replaced by `-`.
#[inline]
fn normalize_symbol(raw: &str) -> String {
    raw.trim().to_lowercase().replace('/', "-")
}

/// Loads a `rustls::ServerConfig` from PEM cert and key files on disk.
///
/// Accepts certificate chains (multiple PEM blocks). For the key, tries
/// PKCS#8 first, then RSA, then EC.
fn load_tls_config(cert_path: &str, key_path: &str) -> Result<ServerConfig, OracleError> {
    let cert_file = File::open(cert_path)
        .map_err(|e| OracleError::nats(format!("open tls cert {cert_path}: {e}")))?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| OracleError::nats(format!("parse tls cert {cert_path}: {e}")))?;
    if certs.is_empty() {
        return Err(OracleError::nats(format!(
            "no certificates found in {cert_path}"
        )));
    }

    let key_file = File::open(key_path)
        .map_err(|e| OracleError::nats(format!("open tls key {key_path}: {e}")))?;
    let mut key_reader = BufReader::new(key_file);
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| OracleError::nats(format!("parse tls key {key_path}: {e}")))?
        .ok_or_else(|| OracleError::nats(format!("no private key found in {key_path}")))?;

    // Install default crypto provider (ring) the first time this is reached.
    // Idempotent: subsequent calls return Err which we ignore.
    let _ = rustls::crypto::ring::default_provider().install_default();

    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| OracleError::nats(format!("build tls server config: {e}")))
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

    /// Helper for the three filter sets used by `handle_client_message`.
    fn empty_filter() -> (bool, HashSet<String>, HashSet<String>) {
        (false, HashSet::new(), HashSet::new())
    }

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
        let (mut subscribed_all, mut symbols, mut excluded) = empty_filter();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"subscribe","symbols":["all"]}"#;
        OracleWsServer::handle_client_message(
            msg,
            &mut subscribed_all,
            &mut symbols,
            &mut excluded,
            addr,
        );

        assert!(subscribed_all);
    }

    #[test]
    fn test_handle_client_message_subscribe_specific() {
        let (mut subscribed_all, mut symbols, mut excluded) = empty_filter();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"subscribe","symbols":["btc-usdt","eth-usdt"]}"#;
        OracleWsServer::handle_client_message(
            msg,
            &mut subscribed_all,
            &mut symbols,
            &mut excluded,
            addr,
        );

        assert!(!subscribed_all);
        assert!(symbols.contains("btc-usdt"));
        assert!(symbols.contains("eth-usdt"));
    }

    #[test]
    fn test_handle_client_message_subscribe_canonical_form() {
        let (mut subscribed_all, mut symbols, mut excluded) = empty_filter();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"subscribe","symbols":["BTC/USDT","ETH/USDT"]}"#;
        OracleWsServer::handle_client_message(
            msg,
            &mut subscribed_all,
            &mut symbols,
            &mut excluded,
            addr,
        );

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
        let mut excluded = HashSet::new();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"unsubscribe","symbols":["btc-usdt"]}"#;
        OracleWsServer::handle_client_message(
            msg,
            &mut subscribed_all,
            &mut symbols,
            &mut excluded,
            addr,
        );

        assert!(!symbols.contains("btc-usdt"));
        assert!(symbols.contains("eth-usdt"));
        assert!(excluded.contains("btc-usdt"));
    }

    #[test]
    fn test_handle_client_message_unsubscribe_all_clears_everything() {
        // "all" on unsubscribe must clear the wildcard, every explicit
        // subscription, and any prior exclusions.
        let mut subscribed_all = true;
        let mut symbols = HashSet::new();
        symbols.insert("btc-usdt".to_owned());
        symbols.insert("eth-usdt".to_owned());
        let mut excluded = HashSet::new();
        excluded.insert("xrp-usdt".to_owned());
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"unsubscribe","symbols":["all"]}"#;
        OracleWsServer::handle_client_message(
            msg,
            &mut subscribed_all,
            &mut symbols,
            &mut excluded,
            addr,
        );

        assert!(!subscribed_all);
        assert!(symbols.is_empty(), "unsubscribe all must drop explicit");
        assert!(excluded.is_empty(), "unsubscribe all must drop exclusions");
    }

    #[test]
    fn test_handle_client_message_unsubscribe_canonical_form() {
        let mut subscribed_all = false;
        let mut symbols = HashSet::new();
        symbols.insert("btc-usdt".to_owned());
        symbols.insert("eth-usdt".to_owned());
        let mut excluded = HashSet::new();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"unsubscribe","symbols":["BTC/USDT"]}"#;
        OracleWsServer::handle_client_message(
            msg,
            &mut subscribed_all,
            &mut symbols,
            &mut excluded,
            addr,
        );

        assert!(!symbols.contains("btc-usdt"));
        assert!(symbols.contains("eth-usdt"));
        assert!(excluded.contains("btc-usdt"));
    }

    #[test]
    fn test_handle_client_message_unsubscribe_with_wildcard_excludes_symbol() {
        // Regression: after `subscribe all` + `unsubscribe ETH/USDT`, the
        // wildcard is still on but ETH must be excluded.
        let mut subscribed_all = true;
        let mut symbols = HashSet::new();
        let mut excluded = HashSet::new();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"unsubscribe","symbols":["ETH/USDT"]}"#;
        OracleWsServer::handle_client_message(
            msg,
            &mut subscribed_all,
            &mut symbols,
            &mut excluded,
            addr,
        );

        assert!(subscribed_all);
        assert!(excluded.contains("eth-usdt"));
        assert!(!excluded.contains("btc-usdt"));
    }

    #[test]
    fn test_handle_client_message_subscribe_clears_prior_exclusion() {
        // unsubscribe ETH then subscribe ETH should re-enable ETH.
        let mut subscribed_all = true;
        let mut symbols = HashSet::new();
        let mut excluded = HashSet::new();
        excluded.insert("eth-usdt".to_owned());
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"subscribe","symbols":["ETH/USDT"]}"#;
        OracleWsServer::handle_client_message(
            msg,
            &mut subscribed_all,
            &mut symbols,
            &mut excluded,
            addr,
        );

        assert!(symbols.contains("eth-usdt"));
        assert!(
            !excluded.contains("eth-usdt"),
            "explicit subscribe must clear the prior exclusion"
        );
    }

    #[test]
    fn test_normalize_symbol_forms() {
        assert_eq!(normalize_symbol("BTC/USDT"), "btc-usdt");
        assert_eq!(normalize_symbol("btc/usdt"), "btc-usdt");
        assert_eq!(normalize_symbol("BTC-USDT"), "btc-usdt");
        assert_eq!(normalize_symbol("  btc-usdt  "), "btc-usdt");
        assert_eq!(normalize_symbol("ALL"), "all");
    }

    #[test]
    fn test_handle_client_message_invalid_json() {
        let (mut subscribed_all, mut symbols, mut excluded) = empty_filter();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        // Should not panic, just log and return.
        OracleWsServer::handle_client_message(
            "not json",
            &mut subscribed_all,
            &mut symbols,
            &mut excluded,
            addr,
        );

        assert!(!subscribed_all);
        assert!(symbols.is_empty());
        assert!(excluded.is_empty());
    }

    #[test]
    fn test_handle_client_message_unknown_action() {
        let (mut subscribed_all, mut symbols, mut excluded) = empty_filter();
        let addr: SocketAddr = ([127, 0, 0, 1], 12345).into();

        let msg = r#"{"action":"reset","symbols":["btc-usdt"]}"#;
        OracleWsServer::handle_client_message(
            msg,
            &mut subscribed_all,
            &mut symbols,
            &mut excluded,
            addr,
        );

        assert!(!subscribed_all);
        assert!(symbols.is_empty());
        assert!(excluded.is_empty());
    }
}
