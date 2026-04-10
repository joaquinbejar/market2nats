use std::collections::HashMap;

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, instrument, warn};

use crate::application::ports::{Subscription, VenueAdapter, VenueError};
use crate::config::model::{CircuitBreakerConfig, ConnectionConfig, GenericWsConfig};
use crate::domain::{
    CanonicalSymbol, FundingRate, InstrumentId, L2Update, Liquidation, MarketDataEnvelope,
    MarketDataPayload, MarketDataType, Price, Quantity, Sequence, Side, Ticker, Timestamp, Trade,
    VenueId,
};

use super::circuit_breaker::CircuitBreaker;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A generic WebSocket adapter configurable entirely via TOML.
///
/// Uses template-based subscribe messages and a channel map to translate
/// between venue channel names and `MarketDataType`.
pub struct GenericWsAdapter {
    venue_id: VenueId,
    connection_config: ConnectionConfig,
    ws_config: GenericWsConfig,
    circuit_breaker: CircuitBreaker,
    ws: Option<WsStream>,
    /// Maps venue-local instrument → canonical symbol.
    instrument_map: HashMap<String, String>,
    /// Reverse channel map: venue channel name → MarketDataType.
    reverse_channel_map: HashMap<String, MarketDataType>,
}

impl GenericWsAdapter {
    /// Creates a new `GenericWsAdapter`.
    ///
    /// # Errors
    ///
    /// Returns `VenueError` if the venue ID is invalid.
    pub fn new(
        venue_id: &str,
        connection_config: ConnectionConfig,
        ws_config: GenericWsConfig,
        cb_config: Option<&CircuitBreakerConfig>,
    ) -> Result<Self, VenueError> {
        let vid = VenueId::try_new(venue_id).map_err(|e| VenueError::ConnectionFailed {
            venue: venue_id.to_owned(),
            reason: e.to_string(),
        })?;

        let circuit_breaker = cb_config
            .map(CircuitBreaker::new)
            .unwrap_or_else(CircuitBreaker::disabled);

        let mut reverse_channel_map = HashMap::new();
        for (data_type_str, channel_name) in &ws_config.channel_map {
            if let Ok(dt) = MarketDataType::from_str_config(data_type_str) {
                reverse_channel_map.insert(channel_name.clone(), dt);
            }
        }

        Ok(Self {
            venue_id: vid,
            connection_config,
            ws_config,
            circuit_breaker,
            ws: None,
            instrument_map: HashMap::new(),
            reverse_channel_map,
        })
    }

    /// Builds a subscribe message from the template.
    #[must_use]
    fn build_subscribe_message(&self, channel: &str, instrument: &str) -> String {
        self.ws_config
            .subscribe_template
            .replace("${channel}", channel)
            .replace("${instrument}", instrument)
    }

    /// Attempts to parse a JSON message into domain events.
    fn parse_message(&self, text: &str) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let value: serde_json::Value =
            serde_json::from_str(text).map_err(|e| VenueError::ReceiveFailed {
                venue: self.venue_id.as_str().to_owned(),
                reason: format!("json parse error: {e}"),
            })?;

        // Try to extract events from common JSON patterns.
        // This is a best-effort generic parser — specific venues with complex
        // formats should use dedicated adapters.
        let mut events = Vec::new();

        // Try to detect data type from a "channel" or "type" field.
        let channel = value
            .get("channel")
            .or_else(|| value.get("type"))
            .or_else(|| value.get("e"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let data_type = self.reverse_channel_map.get(channel).copied();

        // Try to extract instrument from common fields.
        let instrument_str = value
            .get("symbol")
            .or_else(|| value.get("s"))
            .or_else(|| value.get("pair"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let canonical = self
            .instrument_map
            .get(instrument_str)
            .cloned()
            .unwrap_or_else(|| instrument_str.to_owned());

        if let (Some(dt), false) = (data_type, instrument_str.is_empty()) {
            let venue_id = self.venue_id.clone();
            let instrument_id =
                InstrumentId::try_new(instrument_str).map_err(|e| VenueError::ReceiveFailed {
                    venue: self.venue_id.as_str().to_owned(),
                    reason: e.to_string(),
                })?;
            let canonical_symbol =
                CanonicalSymbol::try_new(&canonical).map_err(|e| VenueError::ReceiveFailed {
                    venue: self.venue_id.as_str().to_owned(),
                    reason: e.to_string(),
                })?;

            if let Some(payload) = self.try_parse_payload(dt, &value)? {
                events.push(MarketDataEnvelope {
                    venue: venue_id,
                    instrument: instrument_id,
                    canonical_symbol,
                    data_type: dt,
                    received_at: Timestamp::now(),
                    exchange_timestamp: extract_timestamp(&value),
                    sequence: Sequence::new(0), // Will be assigned by SequenceTracker
                    payload,
                });
            }
        }

        Ok(events)
    }

    /// Tries to parse the payload for a given data type.
    fn try_parse_payload(
        &self,
        data_type: MarketDataType,
        value: &serde_json::Value,
    ) -> Result<Option<MarketDataPayload>, VenueError> {
        match data_type {
            MarketDataType::Trade => {
                let price =
                    extract_decimal(value, &["p", "price"]).and_then(|d| Price::try_new(d).ok());
                let quantity = extract_decimal(value, &["q", "qty", "quantity", "amount"])
                    .and_then(|d| Quantity::try_new(d).ok());
                let side_str = value
                    .get("S")
                    .or_else(|| value.get("side"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("buy");
                let side = Side::from_str_loose(side_str).unwrap_or(Side::Buy);
                let trade_id = value
                    .get("t")
                    .or_else(|| value.get("trade_id"))
                    .or_else(|| value.get("id"))
                    .and_then(|v| {
                        if v.is_string() {
                            v.as_str().map(|s| s.to_owned())
                        } else {
                            Some(v.to_string())
                        }
                    });

                match (price, quantity) {
                    (Some(p), Some(q)) => Ok(Some(MarketDataPayload::Trade(Trade {
                        price: p,
                        quantity: q,
                        side,
                        trade_id,
                    }))),
                    _ => {
                        debug!(venue = %self.venue_id, "could not parse trade from message");
                        Ok(None)
                    }
                }
            }
            MarketDataType::Ticker => {
                let bid_price = extract_decimal(value, &["b", "bid", "bidPrice"])
                    .and_then(|d| Price::try_new(d).ok());
                let bid_qty = extract_decimal(value, &["B", "bidQty"])
                    .and_then(|d| Quantity::try_new(d).ok());
                let ask_price = extract_decimal(value, &["a", "ask", "askPrice"])
                    .and_then(|d| Price::try_new(d).ok());
                let ask_qty = extract_decimal(value, &["A", "askQty"])
                    .and_then(|d| Quantity::try_new(d).ok());
                let last_price = extract_decimal(value, &["c", "last", "lastPrice"])
                    .and_then(|d| Price::try_new(d).ok());

                match (bid_price, bid_qty, ask_price, ask_qty, last_price) {
                    (Some(bp), Some(bq), Some(ap), Some(aq), Some(lp)) => {
                        Ok(Some(MarketDataPayload::Ticker(Ticker {
                            bid_price: bp,
                            bid_qty: bq,
                            ask_price: ap,
                            ask_qty: aq,
                            last_price: lp,
                        })))
                    }
                    _ => {
                        debug!(venue = %self.venue_id, "could not parse ticker from message");
                        Ok(None)
                    }
                }
            }
            MarketDataType::L2Orderbook => {
                let bids = parse_price_levels(value, &["bids", "b"]);
                let asks = parse_price_levels(value, &["asks", "a"]);
                let is_snapshot = value
                    .get("type")
                    .and_then(|v| v.as_str())
                    .map(|s| s == "snapshot")
                    .unwrap_or(false);

                if bids.is_empty() && asks.is_empty() {
                    debug!(venue = %self.venue_id, "could not parse l2 from message");
                    Ok(None)
                } else {
                    Ok(Some(MarketDataPayload::L2Update(L2Update {
                        bids,
                        asks,
                        is_snapshot,
                    })))
                }
            }
            MarketDataType::FundingRate => {
                let rate = extract_decimal(value, &["r", "rate", "fundingRate"]);
                let predicted = extract_decimal(value, &["predictedRate", "nextRate"]);
                let next_at = value
                    .get("nextFundingTime")
                    .or_else(|| value.get("T"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                match rate {
                    Some(r) => Ok(Some(MarketDataPayload::FundingRate(FundingRate {
                        rate: r,
                        predicted_rate: predicted,
                        next_funding_at: Timestamp::new(next_at),
                    }))),
                    None => {
                        debug!(venue = %self.venue_id, "could not parse funding rate");
                        Ok(None)
                    }
                }
            }
            MarketDataType::Liquidation => {
                let side_str = value
                    .get("S")
                    .or_else(|| value.get("side"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("sell");
                let side = Side::from_str_loose(side_str).unwrap_or(Side::Sell);
                let price =
                    extract_decimal(value, &["p", "price"]).and_then(|d| Price::try_new(d).ok());
                let quantity = extract_decimal(value, &["q", "qty", "quantity"])
                    .and_then(|d| Quantity::try_new(d).ok());

                match (price, quantity) {
                    (Some(p), Some(q)) => Ok(Some(MarketDataPayload::Liquidation(Liquidation {
                        side,
                        price: p,
                        quantity: q,
                    }))),
                    _ => {
                        debug!(venue = %self.venue_id, "could not parse liquidation");
                        Ok(None)
                    }
                }
            }
        }
    }
}

impl VenueAdapter for GenericWsAdapter {
    fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    #[instrument(skip(self), fields(venue = %self.venue_id))]
    fn connect(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), VenueError>> + Send + '_>>
    {
        Box::pin(async move {
            if !self.circuit_breaker.is_allowed() {
                return Err(VenueError::CircuitBreakerOpen {
                    venue: self.venue_id.as_str().to_owned(),
                });
            }

            let url = &self.connection_config.ws_url;
            match connect_async(url).await {
                Ok((ws, _response)) => {
                    self.ws = Some(ws);
                    self.circuit_breaker.record_success();
                    debug!(venue = %self.venue_id, "websocket connected");
                    Ok(())
                }
                Err(e) => {
                    self.circuit_breaker.record_failure();
                    Err(VenueError::ConnectionFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    })
                }
            }
        })
    }

    #[instrument(skip(self, subscriptions), fields(venue = %self.venue_id))]
    fn subscribe(
        &mut self,
        subscriptions: &[Subscription],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), VenueError>> + Send + '_>>
    {
        let subs: Vec<Subscription> = subscriptions.to_vec();
        Box::pin(async move {
            if self.ws.is_none() {
                return Err(VenueError::SubscribeFailed {
                    venue: self.venue_id.as_str().to_owned(),
                    reason: "not connected".to_owned(),
                });
            }

            // Build instrument map and collect messages to send.
            let mut messages = Vec::new();
            for sub in &subs {
                self.instrument_map
                    .insert(sub.instrument.clone(), sub.canonical_symbol.clone());

                for dt in &sub.data_types {
                    let channel_name = self
                        .ws_config
                        .channel_map
                        .get(dt.as_subject_str())
                        .cloned()
                        .unwrap_or_else(|| dt.as_subject_str().to_owned());

                    let msg = self.build_subscribe_message(&channel_name, &sub.instrument);
                    debug!(venue = %self.venue_id, message = %msg, "sending subscribe");
                    messages.push(msg);
                }
            }

            // Now send all messages.
            let ws = self
                .ws
                .as_mut()
                .ok_or_else(|| VenueError::SubscribeFailed {
                    venue: self.venue_id.as_str().to_owned(),
                    reason: "not connected".to_owned(),
                })?;

            for msg in messages {
                ws.send(Message::Text(msg.into())).await.map_err(|e| {
                    VenueError::SubscribeFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?;
            }

            Ok(())
        })
    }

    fn next_events(
        &mut self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<MarketDataEnvelope>, VenueError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            if !self.circuit_breaker.is_allowed() {
                return Err(VenueError::CircuitBreakerOpen {
                    venue: self.venue_id.as_str().to_owned(),
                });
            }

            loop {
                // Read the next message, extracting text to parse.
                let msg_result = {
                    let ws = self.ws.as_mut().ok_or_else(|| VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: "not connected".to_owned(),
                    })?;
                    ws.next().await
                };

                match msg_result {
                    Some(Ok(Message::Text(text))) => {
                        let events = self.parse_message(&text)?;
                        if !events.is_empty() {
                            self.circuit_breaker.record_success();
                            return Ok(events);
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        if let Ok(text) = std::str::from_utf8(&data) {
                            let events = self.parse_message(text)?;
                            if !events.is_empty() {
                                self.circuit_breaker.record_success();
                                return Ok(events);
                            }
                        }
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {
                        // Control/raw frames, continue.
                    }
                    Some(Ok(Message::Close(_))) => {
                        self.circuit_breaker.record_failure();
                        self.ws = None;
                        return Err(VenueError::ReceiveFailed {
                            venue: self.venue_id.as_str().to_owned(),
                            reason: "connection closed by server".to_owned(),
                        });
                    }
                    Some(Err(e)) => {
                        self.circuit_breaker.record_failure();
                        self.ws = None;
                        return Err(VenueError::ReceiveFailed {
                            venue: self.venue_id.as_str().to_owned(),
                            reason: e.to_string(),
                        });
                    }
                    None => {
                        self.circuit_breaker.record_failure();
                        self.ws = None;
                        return Err(VenueError::ReceiveFailed {
                            venue: self.venue_id.as_str().to_owned(),
                            reason: "stream ended".to_owned(),
                        });
                    }
                }
            }
        })
    }

    #[instrument(skip(self), fields(venue = %self.venue_id))]
    fn disconnect(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), VenueError>> + Send + '_>>
    {
        Box::pin(async move {
            if let Some(mut ws) = self.ws.take()
                && let Err(e) = ws.close(None).await
            {
                warn!(venue = %self.venue_id, error = %e, "error during disconnect");
            }
            Ok(())
        })
    }

    fn is_connected(&self) -> bool {
        self.ws.is_some()
    }
}

// ── Helper functions ────────────────────────────────────────────────────

/// Extracts a `Decimal` value from a JSON object, trying multiple field names.
#[must_use]
fn extract_decimal(value: &serde_json::Value, keys: &[&str]) -> Option<Decimal> {
    for key in keys {
        if let Some(v) = value.get(*key) {
            if let Some(d) = v.as_str().and_then(|s| s.parse::<Decimal>().ok()) {
                return Some(d);
            }
            if let Some(d) = v.as_f64().and_then(|n| Decimal::try_from(n).ok()) {
                return Some(d);
            }
        }
    }
    None
}

/// Extracts a timestamp from common JSON fields.
#[must_use]
fn extract_timestamp(value: &serde_json::Value) -> Option<Timestamp> {
    let ts = value
        .get("T")
        .or_else(|| value.get("timestamp"))
        .or_else(|| value.get("time"))
        .or_else(|| value.get("E"))
        .and_then(|v| v.as_u64());

    ts.map(Timestamp::new)
}

/// Parses price levels (array of [price, quantity] arrays) from JSON.
#[must_use]
fn parse_price_levels(value: &serde_json::Value, keys: &[&str]) -> Vec<(Price, Quantity)> {
    for key in keys {
        if let Some(arr) = value.get(*key).and_then(|v| v.as_array()) {
            let mut levels = Vec::with_capacity(arr.len());
            for item in arr {
                if let Some(inner) = item.as_array().filter(|a| a.len() >= 2) {
                    let price = inner[0]
                        .as_str()
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .or_else(|| inner[0].as_f64().and_then(|f| Decimal::try_from(f).ok()))
                        .and_then(|d| Price::try_new(d).ok());
                    let qty = inner[1]
                        .as_str()
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .or_else(|| inner[1].as_f64().and_then(|f| Decimal::try_from(f).ok()))
                        .and_then(|d| Quantity::try_new(d).ok());
                    if let (Some(p), Some(q)) = (price, qty) {
                        levels.push((p, q));
                    }
                }
            }
            if !levels.is_empty() {
                return levels;
            }
        }
    }
    Vec::new()
}
