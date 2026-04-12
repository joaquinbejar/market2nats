use std::collections::HashMap;

use dashmap::DashMap;

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, instrument, trace, warn};

use crate::application::ports::{Subscription, VenueAdapter, VenueError};
use crate::config::model::{CircuitBreakerConfig, ConnectionConfig, GenericWsConfig};
use crate::domain::{
    CanonicalSymbol, FundingRate, InstrumentId, L2Update, Liquidation, MarketDataEnvelope,
    MarketDataPayload, MarketDataType, Price, Quantity, Sequence, Side, Ticker, Timestamp, Trade,
    VenueId,
};

use super::circuit_breaker::CircuitBreaker;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Bitfinex routes data messages by an integer `chanId` assigned in the
/// `subscribed` ack. The adapter records the mapping for every successful
/// subscribe so subsequent positional payloads can be routed back to a
/// canonical (instrument, data_type).
#[derive(Debug, Clone)]
struct BitfinexChannel {
    instrument: String,
    canonical: String,
    data_type: MarketDataType,
}

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
    /// Maps venue-local instrument (any case) → canonical symbol.
    instrument_map: HashMap<String, String>,
    /// Reverse channel map: venue channel name → MarketDataType.
    reverse_channel_map: HashMap<String, MarketDataType>,
    /// Bitfinex-only: chanId → (instrument, canonical, data_type) routing
    /// table populated from `subscribed` events. Uses interior mutability so
    /// `parse_message(&self, ...)` can update it on incoming acks.
    bitfinex_channels: DashMap<u64, BitfinexChannel>,
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
                // Also map the base channel name (strip @... suffix used in subscribe).
                if let Some(base) = channel_name.split('@').next()
                    && base != channel_name.as_str()
                {
                    reverse_channel_map.insert(base.to_owned(), dt);
                }
            }
        }

        // Add common Binance event name aliases.
        // These allow matching response "e" fields to the correct data type.
        if reverse_channel_map.contains_key("trade") {
            reverse_channel_map.insert("aggTrade".to_owned(), MarketDataType::Trade);
        }
        if reverse_channel_map
            .values()
            .any(|dt| *dt == MarketDataType::L2Orderbook)
        {
            reverse_channel_map.insert("depthUpdate".to_owned(), MarketDataType::L2Orderbook);
        }
        if reverse_channel_map
            .values()
            .any(|dt| *dt == MarketDataType::FundingRate)
        {
            reverse_channel_map.insert("markPriceUpdate".to_owned(), MarketDataType::FundingRate);
        }
        if reverse_channel_map
            .values()
            .any(|dt| *dt == MarketDataType::Liquidation)
        {
            reverse_channel_map.insert("forceOrder".to_owned(), MarketDataType::Liquidation);
        }

        Ok(Self {
            venue_id: vid,
            connection_config,
            ws_config,
            circuit_breaker,
            ws: None,
            instrument_map: HashMap::new(),
            reverse_channel_map,
            bitfinex_channels: DashMap::new(),
        })
    }

    /// Builds subscribe message(s) for the given subscriptions.
    ///
    /// Behaviour depends on `subscribe_mode`:
    ///
    /// - `"per_pair"` (default): if `batch_subscribe_template` is set, builds a single
    ///   message with all streams as a JSON array in `${params}`. Otherwise, builds one
    ///   message per (instrument, channel) using `subscribe_template`.
    ///
    /// - `"per_channel"`: groups instruments by channel and renders one subscribe frame
    ///   per channel. The template receives `${channel}` and `${instruments}` (a JSON
    ///   array of instrument strings).
    ///
    /// - `"products_channels"`: collects unique instruments into `${instruments}` and
    ///   unique channels into `${channels}`, rendering a single frame with both arrays.
    fn build_subscribe_messages(&self, subs: &[Subscription]) -> Vec<String> {
        // Collect all (instrument, channel, data_type_subject) triples.
        let mut pairs: Vec<(String, String, String)> = Vec::new();
        for sub in subs {
            for dt in &sub.data_types {
                let dt_subject = dt.as_subject_str().to_owned();
                let channel_name = self
                    .ws_config
                    .channel_map
                    .get(dt.as_subject_str())
                    .cloned()
                    .unwrap_or_else(|| dt_subject.clone());
                pairs.push((sub.instrument.clone(), channel_name, dt_subject));
            }
        }

        match self.ws_config.subscribe_mode.as_str() {
            "per_channel" => self.build_subscribe_per_channel(&pairs),
            "products_channels" => self.build_subscribe_products_channels(subs, &pairs),
            _ => self.build_subscribe_per_pair(&pairs),
        }
    }

    /// Default `per_pair` subscribe: batch or per-pair template rendering.
    fn build_subscribe_per_pair(&self, pairs: &[(String, String, String)]) -> Vec<String> {
        if let Some(batch_tpl) = &self.ws_config.batch_subscribe_template {
            let stream_names: Vec<String> = pairs
                .iter()
                .map(|(inst, ch, dt_subj)| {
                    let mut name = self
                        .ws_config
                        .stream_format
                        .replace("${instrument}", inst)
                        .replace("${channel}", ch);
                    if let Some(suffix) = self.ws_config.channel_suffix.get(dt_subj) {
                        name.push_str(suffix);
                    }
                    name
                })
                .collect();
            let params_json = if self.ws_config.args_format == "object" {
                let values: Vec<serde_json::Value> = stream_names
                    .iter()
                    .filter_map(|s| serde_json::from_str(s).ok())
                    .collect();
                serde_json::to_string(&values).unwrap_or_default()
            } else {
                serde_json::to_string(&stream_names).unwrap_or_default()
            };
            vec![batch_tpl.replace("${params}", &params_json)]
        } else if let Some(tpl) = &self.ws_config.subscribe_template {
            pairs
                .iter()
                .map(|(inst, ch, _dt_subj)| {
                    tpl.replace("${instrument}", inst).replace("${channel}", ch)
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// `per_channel` subscribe: one frame per channel with `${instruments}` as JSON array.
    fn build_subscribe_per_channel(&self, pairs: &[(String, String, String)]) -> Vec<String> {
        let tpl = match &self.ws_config.subscribe_template {
            Some(t) => t,
            None => return Vec::new(),
        };

        // Group instruments by channel, preserving insertion order via Vec of seen channels.
        let mut channel_order: Vec<String> = Vec::new();
        let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
        for (inst, ch, _dt_subj) in pairs {
            let entry = grouped.entry(ch.clone()).or_default();
            if !entry.contains(inst) {
                entry.push(inst.clone());
            }
            if !channel_order.contains(ch) {
                channel_order.push(ch.clone());
            }
        }

        channel_order
            .iter()
            .filter_map(|ch| {
                let instruments = grouped.get(ch)?;
                let instruments_json = serde_json::to_string(instruments).unwrap_or_default();
                Some(
                    tpl.replace("${channel}", ch)
                        .replace("${instruments}", &instruments_json),
                )
            })
            .collect()
    }

    /// `products_channels` subscribe: one frame with `${instruments}` and `${channels}`.
    fn build_subscribe_products_channels(
        &self,
        subs: &[Subscription],
        pairs: &[(String, String, String)],
    ) -> Vec<String> {
        let tpl = match &self.ws_config.batch_subscribe_template {
            Some(t) => t,
            None => return Vec::new(),
        };

        // Unique instruments (preserve order).
        let mut instruments: Vec<String> = Vec::new();
        for sub in subs {
            if !instruments.contains(&sub.instrument) {
                instruments.push(sub.instrument.clone());
            }
        }

        // Unique channels (preserve order).
        let mut channels: Vec<String> = Vec::new();
        for (_inst, ch, _dt_subj) in pairs {
            if !channels.contains(ch) {
                channels.push(ch.clone());
            }
        }

        let instruments_json = serde_json::to_string(&instruments).unwrap_or_default();
        let channels_json = serde_json::to_string(&channels).unwrap_or_default();
        vec![
            tpl.replace("${instruments}", &instruments_json)
                .replace("${channels}", &channels_json),
        ]
    }

    /// Looks up the canonical symbol for an instrument, case-insensitively.
    #[must_use]
    fn lookup_canonical(&self, instrument: &str) -> Option<String> {
        self.instrument_map
            .get(instrument)
            .or_else(|| self.instrument_map.get(&instrument.to_uppercase()))
            .or_else(|| self.instrument_map.get(&instrument.to_lowercase()))
            .cloned()
    }

    /// Intercepts venue-specific control messages (application heartbeats,
    /// reconnect pings) before the parser sees them.
    ///
    /// Returns `true` when the message was consumed as a control frame;
    /// the caller should `continue` the read loop in that case.
    async fn intercept_control(&mut self, text: &str) -> Result<bool, VenueError> {
        if self.venue_id.as_str() != "crypto-com" && self.venue_id.as_str() != "crypto-com-perp" {
            return Ok(false);
        }
        // Crypto.com public/heartbeat — MUST reply with public/respond-heartbeat
        // carrying the same `id`, otherwise the server drops the connection.
        let value: serde_json::Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return Ok(false),
        };
        let method = value.get("method").and_then(|v| v.as_str()).unwrap_or("");
        if method != "public/heartbeat" {
            return Ok(false);
        }
        let id = value.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
        let response = format!(r#"{{"id":{id},"method":"public/respond-heartbeat"}}"#);
        let ws = match self.ws.as_mut() {
            Some(ws) => ws,
            None => return Ok(true),
        };
        ws.send(Message::Text(response.into()))
            .await
            .map_err(|e| VenueError::ReceiveFailed {
                venue: self.venue_id.as_str().to_owned(),
                reason: format!("heartbeat response failed: {e}"),
            })?;
        debug!(venue = %self.venue_id, id = id, "responded to crypto.com heartbeat");
        Ok(true)
    }

    /// Attempts to parse a JSON message into domain events.
    ///
    /// Dispatches by `venue_id` to venue-specific parsers first; falls back
    /// to the Binance-compatible paths (combined stream, direct-top-level).
    fn parse_message(&self, text: &str) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        trace!(venue = %self.venue_id, message = %text, "received ws message");

        let value: serde_json::Value =
            serde_json::from_str(text).map_err(|e| VenueError::ReceiveFailed {
                venue: self.venue_id.as_str().to_owned(),
                reason: format!("json parse error: {e}"),
            })?;

        // Venue-specific envelope shapes are handled before the generic paths.
        // Each venue that diverges from the Binance-shaped payload gets one
        // match arm here plus a helper method below.
        match self.venue_id.as_str() {
            "bybit" | "bybit-linear" => return self.parse_bybit_envelope(&value),
            "bitmex" => return self.parse_bitmex_envelope(&value),
            "bitstamp" => return self.parse_bitstamp_envelope(&value),
            "hyperliquid" => return self.parse_hyperliquid_envelope(&value),
            "crypto-com" | "crypto-com-perp" => return self.parse_crypto_com_envelope(&value),
            "kraken" => return self.parse_kraken_envelope(&value),
            "kraken-futures" => return self.parse_kraken_futures_envelope(&value),
            "gate" | "gate-futures" => return self.parse_gate_envelope(&value),
            "okx" | "okx-swap" => return self.parse_okx_envelope(&value),
            "coinbase" => return self.parse_coinbase_envelope(&value),
            "deribit" => return self.parse_deribit_envelope(&value),
            "dydx" => return self.parse_dydx_envelope(&value),
            "bitfinex" | "bitfinex-deriv" => return self.parse_bitfinex_envelope(&value),
            _ => {}
        }

        // Handle Binance combined stream format: {"stream":"btcusdt@trade","data":{...}}
        if let Some(stream_name) = value.get("stream").and_then(|v| v.as_str())
            && let Some(data) = value.get("data")
        {
            return self.parse_combined_stream(stream_name, data);
        }

        // Direct format — try to extract from top-level fields.
        self.parse_direct_message(&value)
    }

    /// Parses a Bybit v5 public stream envelope:
    /// `{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":...,"data":[...]}`.
    ///
    /// Supported topics:
    /// - `publicTrade.{symbol}`   → [`MarketDataType::Trade`]
    /// - `tickers.{symbol}`       → [`MarketDataType::Ticker`]
    /// - `orderbook.{N}.{symbol}` → [`MarketDataType::L2Orderbook`]
    /// - `liquidation.{symbol}`   → [`MarketDataType::Liquidation`] (linear only)
    fn parse_bybit_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let topic = match value.get("topic").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => return Ok(Vec::new()),
        };

        // Topic format: "<kind>.<symbol>" or "<kind>.<depth>.<symbol>".
        let mut parts = topic.split('.');
        let kind = match parts.next() {
            Some(k) => k,
            None => return Ok(Vec::new()),
        };

        let (data_type, symbol) = match kind {
            "publicTrade" => {
                let sym = parts.next().unwrap_or("");
                (MarketDataType::Trade, sym)
            }
            "tickers" => {
                let sym = parts.next().unwrap_or("");
                (MarketDataType::Ticker, sym)
            }
            "orderbook" => {
                // orderbook.<depth>.<symbol>
                let _depth = parts.next().unwrap_or("");
                let sym = parts.next().unwrap_or("");
                (MarketDataType::L2Orderbook, sym)
            }
            "liquidation" => {
                let sym = parts.next().unwrap_or("");
                (MarketDataType::Liquidation, sym)
            }
            _ => return Ok(Vec::new()),
        };

        if symbol.is_empty() {
            return Ok(Vec::new());
        }

        let canonical = match self.lookup_canonical(symbol) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        let instrument_id =
            InstrumentId::try_new(symbol).map_err(|e| VenueError::ReceiveFailed {
                venue: self.venue_id.as_str().to_owned(),
                reason: e.to_string(),
            })?;
        let canonical_symbol =
            CanonicalSymbol::try_new(&canonical).map_err(|e| VenueError::ReceiveFailed {
                venue: self.venue_id.as_str().to_owned(),
                reason: e.to_string(),
            })?;

        let ts = value.get("ts").and_then(|v| v.as_u64()).map(Timestamp::new);
        let data = match value.get("data") {
            Some(d) => d,
            None => return Ok(Vec::new()),
        };

        let mut events = Vec::new();
        let msg_type = value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("delta");

        match data_type {
            MarketDataType::Trade => {
                // Data is an array of trade objects.
                let arr = match data.as_array() {
                    Some(a) => a,
                    None => return Ok(Vec::new()),
                };
                for item in arr {
                    if let Some(ev) =
                        self.build_bybit_trade(item, &instrument_id, &canonical_symbol, ts)?
                    {
                        events.push(ev);
                    }
                }
            }
            MarketDataType::Ticker => {
                // Data is an object with bid1Price/bid1Size/ask1Price/ask1Size/lastPrice.
                // Partial-field deltas are silently skipped; only full updates emit a Ticker.
                if let Some(ev) =
                    self.build_bybit_ticker(data, &instrument_id, &canonical_symbol, ts)?
                {
                    events.push(ev);
                }
            }
            MarketDataType::L2Orderbook => {
                // Data is an object with s, b, a, u, seq.
                if let Some(ev) = self.build_bybit_orderbook(
                    data,
                    msg_type,
                    &instrument_id,
                    &canonical_symbol,
                    ts,
                )? {
                    events.push(ev);
                }
            }
            MarketDataType::Liquidation => {
                // Data is an object with side, size, price, updatedTime.
                if let Some(ev) =
                    self.build_bybit_liquidation(data, &instrument_id, &canonical_symbol, ts)?
                {
                    events.push(ev);
                }
            }
            MarketDataType::FundingRate => {}
        }

        Ok(events)
    }

    fn build_bybit_trade(
        &self,
        item: &serde_json::Value,
        instrument: &InstrumentId,
        canonical: &CanonicalSymbol,
        envelope_ts: Option<Timestamp>,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        let price = match extract_decimal(item, &["p"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(None),
        };
        let quantity = match extract_decimal(item, &["v"]).and_then(|d| Quantity::try_new(d).ok()) {
            Some(q) => q,
            None => return Ok(None),
        };
        let side = item
            .get("S")
            .and_then(|v| v.as_str())
            .and_then(|s| Side::from_str_loose(s).ok())
            .unwrap_or(Side::Buy);
        let trade_id = item.get("i").and_then(|v| v.as_str()).map(|s| s.to_owned());
        let exchange_ts = item
            .get("T")
            .and_then(|v| v.as_u64())
            .map(Timestamp::new)
            .or(envelope_ts);

        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument: instrument.clone(),
            canonical_symbol: canonical.clone(),
            data_type: MarketDataType::Trade,
            received_at: Timestamp::now(),
            exchange_timestamp: exchange_ts,
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Trade(Trade {
                price,
                quantity,
                side,
                trade_id,
            }),
        }))
    }

    fn build_bybit_ticker(
        &self,
        data: &serde_json::Value,
        instrument: &InstrumentId,
        canonical: &CanonicalSymbol,
        envelope_ts: Option<Timestamp>,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        // Bybit v5 tickers: bid1Price, bid1Size, ask1Price, ask1Size, lastPrice.
        let bid_price =
            match extract_decimal(data, &["bid1Price"]).and_then(|d| Price::try_new(d).ok()) {
                Some(p) => p,
                None => return Ok(None),
            };
        let bid_qty =
            match extract_decimal(data, &["bid1Size"]).and_then(|d| Quantity::try_new(d).ok()) {
                Some(q) => q,
                None => return Ok(None),
            };
        let ask_price =
            match extract_decimal(data, &["ask1Price"]).and_then(|d| Price::try_new(d).ok()) {
                Some(p) => p,
                None => return Ok(None),
            };
        let ask_qty =
            match extract_decimal(data, &["ask1Size"]).and_then(|d| Quantity::try_new(d).ok()) {
                Some(q) => q,
                None => return Ok(None),
            };
        let last_price =
            match extract_decimal(data, &["lastPrice"]).and_then(|d| Price::try_new(d).ok()) {
                Some(p) => p,
                None => return Ok(None),
            };

        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument: instrument.clone(),
            canonical_symbol: canonical.clone(),
            data_type: MarketDataType::Ticker,
            received_at: Timestamp::now(),
            exchange_timestamp: envelope_ts,
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Ticker(Ticker {
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                last_price,
            }),
        }))
    }

    fn build_bybit_orderbook(
        &self,
        data: &serde_json::Value,
        msg_type: &str,
        instrument: &InstrumentId,
        canonical: &CanonicalSymbol,
        envelope_ts: Option<Timestamp>,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        let bids = parse_price_levels(data, &["b"]);
        let asks = parse_price_levels(data, &["a"]);
        if bids.is_empty() && asks.is_empty() {
            return Ok(None);
        }
        let is_snapshot = msg_type == "snapshot";
        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument: instrument.clone(),
            canonical_symbol: canonical.clone(),
            data_type: MarketDataType::L2Orderbook,
            received_at: Timestamp::now(),
            exchange_timestamp: envelope_ts,
            sequence: Sequence::new(0),
            payload: MarketDataPayload::L2Update(L2Update {
                bids,
                asks,
                is_snapshot,
            }),
        }))
    }

    fn build_bybit_liquidation(
        &self,
        data: &serde_json::Value,
        instrument: &InstrumentId,
        canonical: &CanonicalSymbol,
        envelope_ts: Option<Timestamp>,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        let price = match extract_decimal(data, &["price"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(None),
        };
        let quantity =
            match extract_decimal(data, &["size"]).and_then(|d| Quantity::try_new(d).ok()) {
                Some(q) => q,
                None => return Ok(None),
            };
        let side = data
            .get("side")
            .and_then(|v| v.as_str())
            .and_then(|s| Side::from_str_loose(s).ok())
            .unwrap_or(Side::Sell);
        let exchange_ts = data
            .get("updatedTime")
            .and_then(|v| v.as_u64())
            .map(Timestamp::new)
            .or(envelope_ts);

        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument: instrument.clone(),
            canonical_symbol: canonical.clone(),
            data_type: MarketDataType::Liquidation,
            received_at: Timestamp::now(),
            exchange_timestamp: exchange_ts,
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Liquidation(Liquidation {
                side,
                price,
                quantity,
            }),
        }))
    }

    /// Parses a Hyperliquid public stream envelope:
    /// `{"channel":"<kind>","data":{...}}` or `{"channel":"trades","data":[...]}`.
    ///
    /// Supported channels:
    /// - `trades` → [`MarketDataType::Trade`] (array of trade objects)
    /// - `bbo`    → [`MarketDataType::Ticker`] (top-of-book from `bbo` array)
    /// - `l2Book` → [`MarketDataType::L2Orderbook`] (always snapshot)
    fn parse_hyperliquid_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let channel = match value.get("channel").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };
        let data = match value.get("data") {
            Some(d) => d,
            None => return Ok(Vec::new()),
        };

        match channel {
            "trades" => {
                let arr = match data.as_array() {
                    Some(a) => a,
                    None => return Ok(Vec::new()),
                };
                let mut events = Vec::with_capacity(arr.len());
                for item in arr {
                    let coin = match item.get("coin").and_then(|v| v.as_str()) {
                        Some(c) => c,
                        None => continue,
                    };
                    let canonical = match self.lookup_canonical(coin) {
                        Some(c) => c,
                        None => continue,
                    };
                    let price =
                        match extract_decimal(item, &["px"]).and_then(|d| Price::try_new(d).ok()) {
                            Some(p) => p,
                            None => continue,
                        };
                    let quantity = match extract_decimal(item, &["sz"])
                        .and_then(|d| Quantity::try_new(d).ok())
                    {
                        Some(q) => q,
                        None => continue,
                    };
                    let side = match item.get("side").and_then(|v| v.as_str()).unwrap_or("B") {
                        "A" | "a" => Side::Sell,
                        _ => Side::Buy,
                    };
                    // Prefer hash (on-chain tx) as trade_id for idempotence; fall back to tid.
                    let trade_id = item
                        .get("hash")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_owned())
                        .or_else(|| {
                            item.get("tid").and_then(|v| {
                                if let Some(s) = v.as_str() {
                                    Some(s.to_owned())
                                } else {
                                    v.as_u64().map(|n| n.to_string())
                                }
                            })
                        });
                    let exchange_ts = item
                        .get("time")
                        .and_then(|v| v.as_u64())
                        .map(Timestamp::new);

                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument: InstrumentId::try_new(coin).map_err(|e| {
                            VenueError::ReceiveFailed {
                                venue: self.venue_id.as_str().to_owned(),
                                reason: e.to_string(),
                            }
                        })?,
                        canonical_symbol: CanonicalSymbol::try_new(&canonical).map_err(|e| {
                            VenueError::ReceiveFailed {
                                venue: self.venue_id.as_str().to_owned(),
                                reason: e.to_string(),
                            }
                        })?,
                        data_type: MarketDataType::Trade,
                        received_at: Timestamp::now(),
                        exchange_timestamp: exchange_ts,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::Trade(Trade {
                            price,
                            quantity,
                            side,
                            trade_id,
                        }),
                    });
                }
                Ok(events)
            }
            "bbo" => {
                let coin = match data.get("coin").and_then(|v| v.as_str()) {
                    Some(c) => c,
                    None => return Ok(Vec::new()),
                };
                let canonical = match self.lookup_canonical(coin) {
                    Some(c) => c,
                    None => return Ok(Vec::new()),
                };
                // bbo is a 2-element array: index 0 = bid, index 1 = ask.
                let bbo_arr = match data.get("bbo").and_then(|v| v.as_array()) {
                    Some(a) if a.len() >= 2 => a,
                    _ => return Ok(Vec::new()),
                };
                let bid_obj = match bbo_arr.first() {
                    Some(o) => o,
                    None => return Ok(Vec::new()),
                };
                let ask_obj = match bbo_arr.get(1) {
                    Some(o) => o,
                    None => return Ok(Vec::new()),
                };
                let bid_price =
                    match extract_decimal(bid_obj, &["px"]).and_then(|d| Price::try_new(d).ok()) {
                        Some(p) => p,
                        None => return Ok(Vec::new()),
                    };
                let bid_qty = match extract_decimal(bid_obj, &["sz"])
                    .and_then(|d| Quantity::try_new(d).ok())
                {
                    Some(q) => q,
                    None => return Ok(Vec::new()),
                };
                let ask_price =
                    match extract_decimal(ask_obj, &["px"]).and_then(|d| Price::try_new(d).ok()) {
                        Some(p) => p,
                        None => return Ok(Vec::new()),
                    };
                let ask_qty = match extract_decimal(ask_obj, &["sz"])
                    .and_then(|d| Quantity::try_new(d).ok())
                {
                    Some(q) => q,
                    None => return Ok(Vec::new()),
                };
                // No last_price field on bbo — use midpoint.
                let last_price = bid_price
                    .value()
                    .checked_add(ask_price.value())
                    .and_then(|sum| sum.checked_div(Decimal::TWO))
                    .and_then(|mid| Price::try_new(mid).ok());
                let last_price = match last_price {
                    Some(p) => p,
                    None => return Ok(Vec::new()),
                };
                let exchange_ts = data
                    .get("time")
                    .and_then(|v| v.as_u64())
                    .map(Timestamp::new);

                Ok(vec![MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument: InstrumentId::try_new(coin).map_err(|e| {
                        VenueError::ReceiveFailed {
                            venue: self.venue_id.as_str().to_owned(),
                            reason: e.to_string(),
                        }
                    })?,
                    canonical_symbol: CanonicalSymbol::try_new(&canonical).map_err(|e| {
                        VenueError::ReceiveFailed {
                            venue: self.venue_id.as_str().to_owned(),
                            reason: e.to_string(),
                        }
                    })?,
                    data_type: MarketDataType::Ticker,
                    received_at: Timestamp::now(),
                    exchange_timestamp: exchange_ts,
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::Ticker(Ticker {
                        bid_price,
                        bid_qty,
                        ask_price,
                        ask_qty,
                        last_price,
                    }),
                }])
            }
            "l2Book" => {
                let coin = match data.get("coin").and_then(|v| v.as_str()) {
                    Some(c) => c,
                    None => return Ok(Vec::new()),
                };
                let canonical = match self.lookup_canonical(coin) {
                    Some(c) => c,
                    None => return Ok(Vec::new()),
                };
                // levels[0] = bids, levels[1] = asks; each level is {px, sz, n}.
                let levels = match data.get("levels").and_then(|v| v.as_array()) {
                    Some(l) if l.len() >= 2 => l,
                    _ => return Ok(Vec::new()),
                };
                let bids = parse_hyperliquid_levels(levels.first());
                let asks = parse_hyperliquid_levels(levels.get(1));
                if bids.is_empty() && asks.is_empty() {
                    return Ok(Vec::new());
                }
                let exchange_ts = data
                    .get("time")
                    .and_then(|v| v.as_u64())
                    .map(Timestamp::new);
                Ok(vec![MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument: InstrumentId::try_new(coin).map_err(|e| {
                        VenueError::ReceiveFailed {
                            venue: self.venue_id.as_str().to_owned(),
                            reason: e.to_string(),
                        }
                    })?,
                    canonical_symbol: CanonicalSymbol::try_new(&canonical).map_err(|e| {
                        VenueError::ReceiveFailed {
                            venue: self.venue_id.as_str().to_owned(),
                            reason: e.to_string(),
                        }
                    })?,
                    data_type: MarketDataType::L2Orderbook,
                    received_at: Timestamp::now(),
                    exchange_timestamp: exchange_ts,
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::L2Update(L2Update {
                        bids,
                        asks,
                        is_snapshot: true,
                    }),
                }])
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Parses a Crypto.com Exchange v1 data envelope:
    /// `{"method":"subscribe","result":{"instrument_name":"BTC_USDT","channel":"trade","data":[...]}}`.
    ///
    /// Subscribe acks (`method == "subscribe"` with empty/absent `result.data`)
    /// are ignored silently. Heartbeat control messages are intercepted
    /// earlier in `intercept_control` and never reach this parser.
    fn parse_crypto_com_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let result = match value.get("result") {
            Some(r) => r,
            None => return Ok(Vec::new()),
        };
        let channel = result.get("channel").and_then(|v| v.as_str()).unwrap_or("");
        let instrument_str = match result.get("instrument_name").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        let canonical = match self.lookup_canonical(instrument_str) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };
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
        let data_arr = match result.get("data").and_then(|v| v.as_array()) {
            Some(a) => a,
            None => return Ok(Vec::new()),
        };

        let mut events = Vec::new();
        match channel {
            "trade" => {
                for item in data_arr {
                    let price =
                        match extract_decimal(item, &["p"]).and_then(|d| Price::try_new(d).ok()) {
                            Some(p) => p,
                            None => continue,
                        };
                    let quantity = match extract_decimal(item, &["q"])
                        .and_then(|d| Quantity::try_new(d).ok())
                    {
                        Some(q) => q,
                        None => continue,
                    };
                    let side = item
                        .get("s")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Side::from_str_loose(s).ok())
                        .unwrap_or(Side::Buy);
                    let trade_id = item.get("d").and_then(|v| {
                        if let Some(s) = v.as_str() {
                            Some(s.to_owned())
                        } else {
                            v.as_u64().map(|n| n.to_string())
                        }
                    });
                    let exchange_ts = item.get("t").and_then(|v| v.as_u64()).map(Timestamp::new);

                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument: instrument_id.clone(),
                        canonical_symbol: canonical_symbol.clone(),
                        data_type: MarketDataType::Trade,
                        received_at: Timestamp::now(),
                        exchange_timestamp: exchange_ts,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::Trade(Trade {
                            price,
                            quantity,
                            side,
                            trade_id,
                        }),
                    });
                }
            }
            "ticker" => {
                if let Some(item) = data_arr.first() {
                    // Crypto.com: b=bid, k=ask, a=last. Sizes are not shipped
                    // on the ticker channel; use 0 quantity as placeholder.
                    let bid_price =
                        match extract_decimal(item, &["b"]).and_then(|d| Price::try_new(d).ok()) {
                            Some(p) => p,
                            None => return Ok(Vec::new()),
                        };
                    let ask_price =
                        match extract_decimal(item, &["k"]).and_then(|d| Price::try_new(d).ok()) {
                            Some(p) => p,
                            None => return Ok(Vec::new()),
                        };
                    let last_price =
                        match extract_decimal(item, &["a"]).and_then(|d| Price::try_new(d).ok()) {
                            Some(p) => p,
                            None => return Ok(Vec::new()),
                        };
                    let zero_qty = Quantity::try_new(Decimal::ZERO).map_err(|e| {
                        VenueError::ReceiveFailed {
                            venue: self.venue_id.as_str().to_owned(),
                            reason: e.to_string(),
                        }
                    })?;
                    let exchange_ts = item.get("t").and_then(|v| v.as_u64()).map(Timestamp::new);
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument: instrument_id.clone(),
                        canonical_symbol: canonical_symbol.clone(),
                        data_type: MarketDataType::Ticker,
                        received_at: Timestamp::now(),
                        exchange_timestamp: exchange_ts,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::Ticker(Ticker {
                            bid_price,
                            bid_qty: zero_qty,
                            ask_price,
                            ask_qty: zero_qty,
                            last_price,
                        }),
                    });
                }
            }
            "book" => {
                if let Some(item) = data_arr.first() {
                    let bids = parse_price_levels(item, &["bids"]);
                    let asks = parse_price_levels(item, &["asks"]);
                    if bids.is_empty() && asks.is_empty() {
                        return Ok(Vec::new());
                    }
                    // Crypto.com book messages are periodic snapshots at the
                    // configured depth; consumers don't need delta handling.
                    let exchange_ts = item.get("t").and_then(|v| v.as_u64()).map(Timestamp::new);
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument: instrument_id.clone(),
                        canonical_symbol: canonical_symbol.clone(),
                        data_type: MarketDataType::L2Orderbook,
                        received_at: Timestamp::now(),
                        exchange_timestamp: exchange_ts,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::L2Update(L2Update {
                            bids,
                            asks,
                            is_snapshot: true,
                        }),
                    });
                }
            }
            "funding" => {
                if let Some(item) = data_arr.first() {
                    let rate = match extract_decimal(item, &["v"]) {
                        Some(r) => r,
                        None => return Ok(Vec::new()),
                    };
                    let next_at = item
                        .get("next_funding_time")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument: instrument_id.clone(),
                        canonical_symbol: canonical_symbol.clone(),
                        data_type: MarketDataType::FundingRate,
                        received_at: Timestamp::now(),
                        exchange_timestamp: item
                            .get("t")
                            .and_then(|v| v.as_u64())
                            .map(Timestamp::new),
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::FundingRate(FundingRate {
                            rate,
                            predicted_rate: None,
                            next_funding_at: Timestamp::new(next_at),
                        }),
                    });
                }
            }
            _ => {}
        }
        Ok(events)
    }

    /// Parses a Kraken v2 public stream envelope:
    /// `{"channel":"trade","type":"snapshot|update","data":[{...}]}`.
    ///
    /// Supported channels:
    /// - `trade`  → [`MarketDataType::Trade`]
    /// - `ticker` → [`MarketDataType::Ticker`]
    /// - `book`   → [`MarketDataType::L2Orderbook`]
    ///
    /// Kraken's book channel uses `{price, qty}` level objects rather than
    /// the `[price, qty]` tuples used by Binance-family venues, so it needs
    /// its own level parser.
    fn parse_kraken_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let channel = match value.get("channel").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };
        // Control events: "status", "heartbeat", "pong" — no market data.
        if matches!(channel, "status" | "heartbeat" | "pong") {
            return Ok(Vec::new());
        }
        let type_str = value
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("update");
        let data_arr = match value.get("data").and_then(|v| v.as_array()) {
            Some(a) => a,
            None => return Ok(Vec::new()),
        };

        let mut events = Vec::new();
        for item in data_arr {
            let symbol = match item.get("symbol").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let canonical = match self.lookup_canonical(symbol) {
                Some(c) => c,
                None => continue,
            };
            let instrument_id =
                InstrumentId::try_new(symbol).map_err(|e| VenueError::ReceiveFailed {
                    venue: self.venue_id.as_str().to_owned(),
                    reason: e.to_string(),
                })?;
            let canonical_symbol =
                CanonicalSymbol::try_new(&canonical).map_err(|e| VenueError::ReceiveFailed {
                    venue: self.venue_id.as_str().to_owned(),
                    reason: e.to_string(),
                })?;

            match channel {
                "trade" => {
                    let price = match extract_decimal(item, &["price"])
                        .and_then(|d| Price::try_new(d).ok())
                    {
                        Some(p) => p,
                        None => continue,
                    };
                    let quantity = match extract_decimal(item, &["qty"])
                        .and_then(|d| Quantity::try_new(d).ok())
                    {
                        Some(q) => q,
                        None => continue,
                    };
                    let side = item
                        .get("side")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Side::from_str_loose(s).ok())
                        .unwrap_or(Side::Buy);
                    let trade_id = item.get("trade_id").and_then(|v| {
                        if let Some(s) = v.as_str() {
                            Some(s.to_owned())
                        } else {
                            v.as_u64().map(|n| n.to_string())
                        }
                    });
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument: instrument_id,
                        canonical_symbol,
                        data_type: MarketDataType::Trade,
                        received_at: Timestamp::now(),
                        exchange_timestamp: None,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::Trade(Trade {
                            price,
                            quantity,
                            side,
                            trade_id,
                        }),
                    });
                }
                "ticker" => {
                    let bid_price = match extract_decimal(item, &["bid"])
                        .and_then(|d| Price::try_new(d).ok())
                    {
                        Some(p) => p,
                        None => continue,
                    };
                    let bid_qty = match extract_decimal(item, &["bid_qty"])
                        .and_then(|d| Quantity::try_new(d).ok())
                    {
                        Some(q) => q,
                        None => continue,
                    };
                    let ask_price = match extract_decimal(item, &["ask"])
                        .and_then(|d| Price::try_new(d).ok())
                    {
                        Some(p) => p,
                        None => continue,
                    };
                    let ask_qty = match extract_decimal(item, &["ask_qty"])
                        .and_then(|d| Quantity::try_new(d).ok())
                    {
                        Some(q) => q,
                        None => continue,
                    };
                    let last_price = match extract_decimal(item, &["last"])
                        .and_then(|d| Price::try_new(d).ok())
                    {
                        Some(p) => p,
                        None => continue,
                    };
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument: instrument_id,
                        canonical_symbol,
                        data_type: MarketDataType::Ticker,
                        received_at: Timestamp::now(),
                        exchange_timestamp: None,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::Ticker(Ticker {
                            bid_price,
                            bid_qty,
                            ask_price,
                            ask_qty,
                            last_price,
                        }),
                    });
                }
                "book" => {
                    let bids = parse_kraken_levels(item.get("bids"));
                    let asks = parse_kraken_levels(item.get("asks"));
                    if bids.is_empty() && asks.is_empty() {
                        continue;
                    }
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument: instrument_id,
                        canonical_symbol,
                        data_type: MarketDataType::L2Orderbook,
                        received_at: Timestamp::now(),
                        exchange_timestamp: None,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::L2Update(L2Update {
                            bids,
                            asks,
                            is_snapshot: type_str == "snapshot",
                        }),
                    });
                }
                _ => {}
            }
        }
        Ok(events)
    }

    /// Parses a Kraken Futures public envelope:
    /// `{"feed":"trade","product_id":"PF_XBTUSD",...}` or similar.
    ///
    /// Supported feeds:
    /// - `trade`         → [`MarketDataType::Trade`]; `type == "liquidation"` also emits a [`MarketDataType::Liquidation`].
    /// - `trade_snapshot` → array of trade objects in `trades`; treated as deltas for consumers.
    /// - `ticker`        → [`MarketDataType::Ticker`] AND a [`MarketDataType::FundingRate`] when `funding_rate` is present.
    /// - `book_snapshot` → [`MarketDataType::L2Orderbook`] with `is_snapshot = true`.
    /// - `book`          → per-level delta aggregated into a single-side [`MarketDataType::L2Orderbook`] update.
    fn parse_kraken_futures_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let feed = match value.get("feed").and_then(|v| v.as_str()) {
            Some(f) => f,
            None => return Ok(Vec::new()),
        };
        // Control events.
        if matches!(
            feed,
            "heartbeat" | "info" | "subscribed" | "subscribed_result" | "unsubscribed"
        ) {
            return Ok(Vec::new());
        }

        let resolve = |product: &str| -> Result<(InstrumentId, CanonicalSymbol), VenueError> {
            let canonical =
                self.lookup_canonical(product)
                    .ok_or_else(|| VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: format!("no canonical for product {product}"),
                    })?;
            let instrument_id =
                InstrumentId::try_new(product).map_err(|e| VenueError::ReceiveFailed {
                    venue: self.venue_id.as_str().to_owned(),
                    reason: e.to_string(),
                })?;
            let canonical_symbol =
                CanonicalSymbol::try_new(&canonical).map_err(|e| VenueError::ReceiveFailed {
                    venue: self.venue_id.as_str().to_owned(),
                    reason: e.to_string(),
                })?;
            Ok((instrument_id, canonical_symbol))
        };

        let mut events = Vec::new();

        match feed {
            "trade" => {
                let product = match value.get("product_id").and_then(|v| v.as_str()) {
                    Some(p) => p,
                    None => return Ok(Vec::new()),
                };
                if self.lookup_canonical(product).is_none() {
                    return Ok(Vec::new());
                }
                let (instrument, canonical) = resolve(product)?;
                if let Some(ev) =
                    self.build_kraken_futures_trade(value, instrument.clone(), canonical.clone())?
                {
                    events.push(ev);
                }
                // Liquidation tap — when type == "liquidation", also emit a Liquidation envelope.
                if value.get("type").and_then(|v| v.as_str()) == Some("liquidation")
                    && let Some(ev) =
                        self.build_kraken_futures_liquidation(value, instrument, canonical)?
                {
                    events.push(ev);
                }
            }
            "trade_snapshot" => {
                let product = match value.get("product_id").and_then(|v| v.as_str()) {
                    Some(p) => p,
                    None => return Ok(Vec::new()),
                };
                if self.lookup_canonical(product).is_none() {
                    return Ok(Vec::new());
                }
                let (instrument, canonical) = resolve(product)?;
                let trades = match value.get("trades").and_then(|v| v.as_array()) {
                    Some(a) => a,
                    None => return Ok(Vec::new()),
                };
                for t in trades {
                    if let Some(ev) =
                        self.build_kraken_futures_trade(t, instrument.clone(), canonical.clone())?
                    {
                        events.push(ev);
                    }
                }
            }
            "ticker" => {
                let product = match value.get("product_id").and_then(|v| v.as_str()) {
                    Some(p) => p,
                    None => return Ok(Vec::new()),
                };
                if self.lookup_canonical(product).is_none() {
                    return Ok(Vec::new());
                }
                let (instrument, canonical) = resolve(product)?;
                if let Some(ev) =
                    self.build_kraken_futures_ticker(value, instrument.clone(), canonical.clone())?
                {
                    events.push(ev);
                }
                // Funding tap — ticker ships funding_rate and next_funding_rate_time on perps.
                if value.get("funding_rate").is_some()
                    && let Some(ev) =
                        self.build_kraken_futures_funding(value, instrument, canonical)?
                {
                    events.push(ev);
                }
            }
            "book_snapshot" => {
                let product = match value.get("product_id").and_then(|v| v.as_str()) {
                    Some(p) => p,
                    None => return Ok(Vec::new()),
                };
                if self.lookup_canonical(product).is_none() {
                    return Ok(Vec::new());
                }
                let (instrument, canonical) = resolve(product)?;
                let bids = parse_kraken_levels(value.get("bids"));
                let asks = parse_kraken_levels(value.get("asks"));
                if !bids.is_empty() || !asks.is_empty() {
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument,
                        canonical_symbol: canonical,
                        data_type: MarketDataType::L2Orderbook,
                        received_at: Timestamp::now(),
                        exchange_timestamp: value
                            .get("timestamp")
                            .and_then(|v| v.as_u64())
                            .map(Timestamp::new),
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::L2Update(L2Update {
                            bids,
                            asks,
                            is_snapshot: true,
                        }),
                    });
                }
            }
            "book" => {
                let product = match value.get("product_id").and_then(|v| v.as_str()) {
                    Some(p) => p,
                    None => return Ok(Vec::new()),
                };
                if self.lookup_canonical(product).is_none() {
                    return Ok(Vec::new());
                }
                let (instrument, canonical) = resolve(product)?;
                // A "book" delta is a single price level; routed to bids or asks
                // based on side.
                let price =
                    match extract_decimal(value, &["price"]).and_then(|d| Price::try_new(d).ok()) {
                        Some(p) => p,
                        None => return Ok(Vec::new()),
                    };
                let quantity = match extract_decimal(value, &["qty"])
                    .and_then(|d| Quantity::try_new(d).ok())
                {
                    Some(q) => q,
                    None => return Ok(Vec::new()),
                };
                let is_bid = value.get("side").and_then(|v| v.as_str()) == Some("buy");
                let (bids, asks) = if is_bid {
                    (vec![(price, quantity)], Vec::new())
                } else {
                    (Vec::new(), vec![(price, quantity)])
                };
                events.push(MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument,
                    canonical_symbol: canonical,
                    data_type: MarketDataType::L2Orderbook,
                    received_at: Timestamp::now(),
                    exchange_timestamp: value
                        .get("timestamp")
                        .and_then(|v| v.as_u64())
                        .map(Timestamp::new),
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::L2Update(L2Update {
                        bids,
                        asks,
                        is_snapshot: false,
                    }),
                });
            }
            _ => {}
        }

        Ok(events)
    }

    fn build_kraken_futures_trade(
        &self,
        item: &serde_json::Value,
        instrument: InstrumentId,
        canonical: CanonicalSymbol,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        let price = match extract_decimal(item, &["price"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(None),
        };
        let quantity = match extract_decimal(item, &["qty"]).and_then(|d| Quantity::try_new(d).ok())
        {
            Some(q) => q,
            None => return Ok(None),
        };
        let side = item
            .get("side")
            .and_then(|v| v.as_str())
            .and_then(|s| Side::from_str_loose(s).ok())
            .unwrap_or(Side::Buy);
        let trade_id = item
            .get("uid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned());
        let exchange_ts = item
            .get("time")
            .and_then(|v| v.as_u64())
            .map(Timestamp::new);
        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::Trade,
            received_at: Timestamp::now(),
            exchange_timestamp: exchange_ts,
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Trade(Trade {
                price,
                quantity,
                side,
                trade_id,
            }),
        }))
    }

    fn build_kraken_futures_ticker(
        &self,
        item: &serde_json::Value,
        instrument: InstrumentId,
        canonical: CanonicalSymbol,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        let bid_price = match extract_decimal(item, &["bid"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(None),
        };
        let bid_qty =
            match extract_decimal(item, &["bid_size"]).and_then(|d| Quantity::try_new(d).ok()) {
                Some(q) => q,
                None => return Ok(None),
            };
        let ask_price = match extract_decimal(item, &["ask"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(None),
        };
        let ask_qty =
            match extract_decimal(item, &["ask_size"]).and_then(|d| Quantity::try_new(d).ok()) {
                Some(q) => q,
                None => return Ok(None),
            };
        let last_price = match extract_decimal(item, &["last"]).and_then(|d| Price::try_new(d).ok())
        {
            Some(p) => p,
            None => return Ok(None),
        };
        let exchange_ts = item
            .get("time")
            .and_then(|v| v.as_u64())
            .map(Timestamp::new);
        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::Ticker,
            received_at: Timestamp::now(),
            exchange_timestamp: exchange_ts,
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Ticker(Ticker {
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                last_price,
            }),
        }))
    }

    fn build_kraken_futures_funding(
        &self,
        item: &serde_json::Value,
        instrument: InstrumentId,
        canonical: CanonicalSymbol,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        let rate = match extract_decimal(item, &["funding_rate"]) {
            Some(r) => r,
            None => return Ok(None),
        };
        let predicted_rate = extract_decimal(item, &["funding_rate_prediction"]);
        let next_funding_at = item
            .get("next_funding_rate_time")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::FundingRate,
            received_at: Timestamp::now(),
            exchange_timestamp: item
                .get("time")
                .and_then(|v| v.as_u64())
                .map(Timestamp::new),
            sequence: Sequence::new(0),
            payload: MarketDataPayload::FundingRate(FundingRate {
                rate,
                predicted_rate,
                next_funding_at: Timestamp::new(next_funding_at),
            }),
        }))
    }

    fn build_kraken_futures_liquidation(
        &self,
        item: &serde_json::Value,
        instrument: InstrumentId,
        canonical: CanonicalSymbol,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        let price = match extract_decimal(item, &["price"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(None),
        };
        let quantity = match extract_decimal(item, &["qty"]).and_then(|d| Quantity::try_new(d).ok())
        {
            Some(q) => q,
            None => return Ok(None),
        };
        let side = item
            .get("side")
            .and_then(|v| v.as_str())
            .and_then(|s| Side::from_str_loose(s).ok())
            .unwrap_or(Side::Sell);
        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::Liquidation,
            received_at: Timestamp::now(),
            exchange_timestamp: item
                .get("time")
                .and_then(|v| v.as_u64())
                .map(Timestamp::new),
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Liquidation(Liquidation {
                side,
                price,
                quantity,
            }),
        }))
    }

    /// Parses a Gate.io v4 public envelope:
    /// `{"channel":"spot.trades","event":"update","result":{...}}`.
    ///
    /// Subscribe/ack events (`event == "subscribe"` without a `result` body)
    /// and heartbeats return empty. `futures.tickers` updates emit both a
    /// [`MarketDataType::Ticker`] and a [`MarketDataType::FundingRate`] when
    /// `funding_rate` is present.
    fn parse_gate_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let channel = match value.get("channel").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };
        let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
        if event != "update" && event != "all" {
            // Ignore subscribe acks, unsubscribe, etc.
            return Ok(Vec::new());
        }
        let result = match value.get("result") {
            Some(r) => r,
            None => return Ok(Vec::new()),
        };

        match channel {
            "spot.trades" => self.build_gate_spot_trade(result),
            "spot.book_ticker" => self.build_gate_spot_book_ticker(result),
            "spot.order_book_update" => self.build_gate_spot_orderbook(result),
            "futures.trades" => self.build_gate_futures_trades(result),
            "futures.book_ticker" => self.build_gate_futures_book_ticker(result),
            "futures.order_book_update" => self.build_gate_futures_orderbook(result),
            "futures.tickers" => self.build_gate_futures_tickers_with_funding(result),
            "futures.liquidates" => self.build_gate_futures_liquidations(result),
            _ => Ok(Vec::new()),
        }
    }

    fn resolve_gate_instrument(
        &self,
        instrument_str: &str,
    ) -> Option<(InstrumentId, CanonicalSymbol)> {
        let canonical = self.lookup_canonical(instrument_str)?;
        let instrument_id = InstrumentId::try_new(instrument_str).ok()?;
        let canonical_symbol = CanonicalSymbol::try_new(&canonical).ok()?;
        Some((instrument_id, canonical_symbol))
    }

    fn build_gate_spot_trade(
        &self,
        result: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let instrument_str = match result.get("currency_pair").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        let (instrument, canonical) = match self.resolve_gate_instrument(instrument_str) {
            Some(x) => x,
            None => return Ok(Vec::new()),
        };
        let price = match extract_decimal(result, &["price"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };
        let quantity =
            match extract_decimal(result, &["amount"]).and_then(|d| Quantity::try_new(d).ok()) {
                Some(q) => q,
                None => return Ok(Vec::new()),
            };
        let side = result
            .get("side")
            .and_then(|v| v.as_str())
            .and_then(|s| Side::from_str_loose(s).ok())
            .unwrap_or(Side::Buy);
        let trade_id = result.get("id").and_then(|v| {
            if let Some(s) = v.as_str() {
                Some(s.to_owned())
            } else {
                v.as_u64().map(|n| n.to_string())
            }
        });
        let exchange_ts = result
            .get("create_time")
            .and_then(|v| v.as_u64())
            .and_then(|s| s.checked_mul(1000))
            .map(Timestamp::new);
        Ok(vec![MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::Trade,
            received_at: Timestamp::now(),
            exchange_timestamp: exchange_ts,
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Trade(Trade {
                price,
                quantity,
                side,
                trade_id,
            }),
        }])
    }

    fn build_gate_spot_book_ticker(
        &self,
        result: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let instrument_str = match result.get("s").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        let (instrument, canonical) = match self.resolve_gate_instrument(instrument_str) {
            Some(x) => x,
            None => return Ok(Vec::new()),
        };
        // Gate.io spot.book_ticker: b=bid, B=bid size, a=ask, A=ask size.
        let bid_price = match extract_decimal(result, &["b"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };
        let bid_qty = match extract_decimal(result, &["B"]).and_then(|d| Quantity::try_new(d).ok())
        {
            Some(q) => q,
            None => return Ok(Vec::new()),
        };
        let ask_price = match extract_decimal(result, &["a"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };
        let ask_qty = match extract_decimal(result, &["A"]).and_then(|d| Quantity::try_new(d).ok())
        {
            Some(q) => q,
            None => return Ok(Vec::new()),
        };
        let last_price = bid_price
            .value()
            .checked_add(ask_price.value())
            .and_then(|sum| sum.checked_div(Decimal::TWO))
            .and_then(|mid| Price::try_new(mid).ok());
        let last_price = match last_price {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };
        Ok(vec![MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::Ticker,
            received_at: Timestamp::now(),
            exchange_timestamp: result.get("t").and_then(|v| v.as_u64()).map(Timestamp::new),
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Ticker(Ticker {
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                last_price,
            }),
        }])
    }

    fn build_gate_spot_orderbook(
        &self,
        result: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let instrument_str = match result.get("s").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        let (instrument, canonical) = match self.resolve_gate_instrument(instrument_str) {
            Some(x) => x,
            None => return Ok(Vec::new()),
        };
        let bids = parse_price_levels(result, &["b"]);
        let asks = parse_price_levels(result, &["a"]);
        if bids.is_empty() && asks.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::L2Orderbook,
            received_at: Timestamp::now(),
            exchange_timestamp: result.get("t").and_then(|v| v.as_u64()).map(Timestamp::new),
            sequence: Sequence::new(0),
            payload: MarketDataPayload::L2Update(L2Update {
                bids,
                asks,
                is_snapshot: false,
            }),
        }])
    }

    fn build_gate_futures_trades(
        &self,
        result: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let arr = match result.as_array() {
            Some(a) => a,
            None => return Ok(Vec::new()),
        };
        let mut events = Vec::with_capacity(arr.len());
        for item in arr {
            let instrument_str = match item.get("contract").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let (instrument, canonical) = match self.resolve_gate_instrument(instrument_str) {
                Some(x) => x,
                None => continue,
            };
            let price = match extract_decimal(item, &["price"]).and_then(|d| Price::try_new(d).ok())
            {
                Some(p) => p,
                None => continue,
            };
            // Gate futures trades ship size as a signed i64 for futures; use abs.
            let raw_size = match extract_decimal(item, &["size"]) {
                Some(s) => s,
                None => continue,
            };
            let abs_size = raw_size.abs();
            let quantity = match Quantity::try_new(abs_size) {
                Ok(q) => q,
                Err(_) => continue,
            };
            // Positive size → buy aggressor on Gate.io futures; negative → sell.
            let side = if raw_size.is_sign_negative() {
                Side::Sell
            } else {
                Side::Buy
            };
            let trade_id = item.get("id").and_then(|v| {
                if let Some(s) = v.as_str() {
                    Some(s.to_owned())
                } else {
                    v.as_u64().map(|n| n.to_string())
                }
            });
            events.push(MarketDataEnvelope {
                venue: self.venue_id.clone(),
                instrument,
                canonical_symbol: canonical,
                data_type: MarketDataType::Trade,
                received_at: Timestamp::now(),
                exchange_timestamp: item
                    .get("create_time_ms")
                    .and_then(|v| v.as_u64())
                    .map(Timestamp::new),
                sequence: Sequence::new(0),
                payload: MarketDataPayload::Trade(Trade {
                    price,
                    quantity,
                    side,
                    trade_id,
                }),
            });
        }
        Ok(events)
    }

    fn build_gate_futures_book_ticker(
        &self,
        result: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let instrument_str = match result.get("s").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        self.build_gate_spot_book_ticker(&serde_json::json!({
            "s": instrument_str,
            "b": result.get("b"),
            "B": result.get("B"),
            "a": result.get("a"),
            "A": result.get("A"),
            "t": result.get("t")
        }))
    }

    fn build_gate_futures_orderbook(
        &self,
        result: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        // Gate futures.order_book_update uses the same shape as spot.
        self.build_gate_spot_orderbook(result)
    }

    fn build_gate_futures_tickers_with_funding(
        &self,
        result: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let arr = match result.as_array() {
            Some(a) => a,
            None => return Ok(Vec::new()),
        };
        let mut events = Vec::new();
        for item in arr {
            let instrument_str = match item.get("contract").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let (instrument, canonical) = match self.resolve_gate_instrument(instrument_str) {
                Some(x) => x,
                None => continue,
            };
            // Ticker — Gate.io futures.tickers has no bid/ask in the stream;
            // use `last` for bid/ask/last and zero size. This is a known gap
            // until the spot.book_ticker cross-tap lands.
            if let Some(last) =
                extract_decimal(item, &["last"]).and_then(|d| Price::try_new(d).ok())
            {
                let zero_qty =
                    Quantity::try_new(Decimal::ZERO).map_err(|e| VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    })?;
                events.push(MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument: instrument.clone(),
                    canonical_symbol: canonical.clone(),
                    data_type: MarketDataType::Ticker,
                    received_at: Timestamp::now(),
                    exchange_timestamp: None,
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::Ticker(Ticker {
                        bid_price: last,
                        bid_qty: zero_qty,
                        ask_price: last,
                        ask_qty: zero_qty,
                        last_price: last,
                    }),
                });
            }
            // Funding tap.
            if let Some(rate) = extract_decimal(item, &["funding_rate"]) {
                let next_funding_at = item
                    .get("funding_next_apply")
                    .and_then(|v| v.as_u64())
                    .and_then(|s| s.checked_mul(1000))
                    .unwrap_or(0);
                events.push(MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument,
                    canonical_symbol: canonical,
                    data_type: MarketDataType::FundingRate,
                    received_at: Timestamp::now(),
                    exchange_timestamp: None,
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::FundingRate(FundingRate {
                        rate,
                        predicted_rate: None,
                        next_funding_at: Timestamp::new(next_funding_at),
                    }),
                });
            }
        }
        Ok(events)
    }

    fn build_gate_futures_liquidations(
        &self,
        result: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let arr = match result.as_array() {
            Some(a) => a,
            None => return Ok(Vec::new()),
        };
        let mut events = Vec::with_capacity(arr.len());
        for item in arr {
            let instrument_str = match item.get("contract").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let (instrument, canonical) = match self.resolve_gate_instrument(instrument_str) {
                Some(x) => x,
                None => continue,
            };
            let price =
                match extract_decimal(item, &["fill_price"]).and_then(|d| Price::try_new(d).ok()) {
                    Some(p) => p,
                    None => continue,
                };
            let raw_size = match extract_decimal(item, &["size"]) {
                Some(s) => s,
                None => continue,
            };
            let quantity = match Quantity::try_new(raw_size.abs()) {
                Ok(q) => q,
                Err(_) => continue,
            };
            // Positive size = long liquidation (SELL side being liquidated
            // aggressively); negative = short liquidation. Convention
            // documented in the venue issue.
            let side = if raw_size.is_sign_negative() {
                Side::Sell
            } else {
                Side::Buy
            };
            events.push(MarketDataEnvelope {
                venue: self.venue_id.clone(),
                instrument,
                canonical_symbol: canonical,
                data_type: MarketDataType::Liquidation,
                received_at: Timestamp::now(),
                exchange_timestamp: item
                    .get("time_ms")
                    .and_then(|v| v.as_u64())
                    .map(Timestamp::new),
                sequence: Sequence::new(0),
                payload: MarketDataPayload::Liquidation(Liquidation {
                    side,
                    price,
                    quantity,
                }),
            });
        }
        Ok(events)
    }

    /// Parses an OKX v5 public envelope:
    /// `{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{...}]}`.
    ///
    /// Supported channels: `trades`, `tickers`, `books`, `funding-rate`,
    /// `liquidation-orders` (subscribed by `instType`, one event per
    /// contract inside `data[].details`).
    fn parse_okx_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let arg = match value.get("arg") {
            Some(a) => a,
            None => return Ok(Vec::new()),
        };
        let channel = match arg.get("channel").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };
        let data_arr = match value.get("data").and_then(|v| v.as_array()) {
            Some(a) => a,
            None => return Ok(Vec::new()),
        };

        let action = value.get("action").and_then(|v| v.as_str()).unwrap_or("");

        let mut events = Vec::new();
        match channel {
            "trades" => {
                for item in data_arr {
                    let inst = match item.get("instId").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => continue,
                    };
                    let (instrument, canonical) = match self.resolve_okx_instrument(inst) {
                        Some(x) => x,
                        None => continue,
                    };
                    let price =
                        match extract_decimal(item, &["px"]).and_then(|d| Price::try_new(d).ok()) {
                            Some(p) => p,
                            None => continue,
                        };
                    let quantity = match extract_decimal(item, &["sz"])
                        .and_then(|d| Quantity::try_new(d).ok())
                    {
                        Some(q) => q,
                        None => continue,
                    };
                    let side = item
                        .get("side")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Side::from_str_loose(s).ok())
                        .unwrap_or(Side::Buy);
                    let trade_id = item
                        .get("tradeId")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_owned());
                    let exchange_ts = item
                        .get("ts")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<u64>().ok())
                        .map(Timestamp::new);
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument,
                        canonical_symbol: canonical,
                        data_type: MarketDataType::Trade,
                        received_at: Timestamp::now(),
                        exchange_timestamp: exchange_ts,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::Trade(Trade {
                            price,
                            quantity,
                            side,
                            trade_id,
                        }),
                    });
                }
            }
            "tickers" => {
                for item in data_arr {
                    let inst = match item.get("instId").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => continue,
                    };
                    let (instrument, canonical) = match self.resolve_okx_instrument(inst) {
                        Some(x) => x,
                        None => continue,
                    };
                    let bid_price = match extract_decimal(item, &["bidPx"])
                        .and_then(|d| Price::try_new(d).ok())
                    {
                        Some(p) => p,
                        None => continue,
                    };
                    let bid_qty = match extract_decimal(item, &["bidSz"])
                        .and_then(|d| Quantity::try_new(d).ok())
                    {
                        Some(q) => q,
                        None => continue,
                    };
                    let ask_price = match extract_decimal(item, &["askPx"])
                        .and_then(|d| Price::try_new(d).ok())
                    {
                        Some(p) => p,
                        None => continue,
                    };
                    let ask_qty = match extract_decimal(item, &["askSz"])
                        .and_then(|d| Quantity::try_new(d).ok())
                    {
                        Some(q) => q,
                        None => continue,
                    };
                    let last_price = match extract_decimal(item, &["last"])
                        .and_then(|d| Price::try_new(d).ok())
                    {
                        Some(p) => p,
                        None => continue,
                    };
                    let exchange_ts = item
                        .get("ts")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<u64>().ok())
                        .map(Timestamp::new);
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument,
                        canonical_symbol: canonical,
                        data_type: MarketDataType::Ticker,
                        received_at: Timestamp::now(),
                        exchange_timestamp: exchange_ts,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::Ticker(Ticker {
                            bid_price,
                            bid_qty,
                            ask_price,
                            ask_qty,
                            last_price,
                        }),
                    });
                }
            }
            "books" | "books5" | "books50-l2-tbt" | "books-l2-tbt" => {
                let inst = match arg.get("instId").and_then(|v| v.as_str()) {
                    Some(s) => s,
                    None => return Ok(Vec::new()),
                };
                let (instrument, canonical) = match self.resolve_okx_instrument(inst) {
                    Some(x) => x,
                    None => return Ok(Vec::new()),
                };
                for item in data_arr {
                    let bids = parse_price_levels(item, &["bids"]);
                    let asks = parse_price_levels(item, &["asks"]);
                    if bids.is_empty() && asks.is_empty() {
                        continue;
                    }
                    let exchange_ts = item
                        .get("ts")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<u64>().ok())
                        .map(Timestamp::new);
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument: instrument.clone(),
                        canonical_symbol: canonical.clone(),
                        data_type: MarketDataType::L2Orderbook,
                        received_at: Timestamp::now(),
                        exchange_timestamp: exchange_ts,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::L2Update(L2Update {
                            bids,
                            asks,
                            is_snapshot: action == "snapshot",
                        }),
                    });
                }
            }
            "funding-rate" => {
                for item in data_arr {
                    let inst = match item.get("instId").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => continue,
                    };
                    let (instrument, canonical) = match self.resolve_okx_instrument(inst) {
                        Some(x) => x,
                        None => continue,
                    };
                    let rate = match extract_decimal(item, &["fundingRate"]) {
                        Some(r) => r,
                        None => continue,
                    };
                    let predicted = extract_decimal(item, &["nextFundingRate"]);
                    let next_at = item
                        .get("nextFundingTime")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0);
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument,
                        canonical_symbol: canonical,
                        data_type: MarketDataType::FundingRate,
                        received_at: Timestamp::now(),
                        exchange_timestamp: item
                            .get("fundingTime")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<u64>().ok())
                            .map(Timestamp::new),
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::FundingRate(FundingRate {
                            rate,
                            predicted_rate: predicted,
                            next_funding_at: Timestamp::new(next_at),
                        }),
                    });
                }
            }
            "liquidation-orders" => {
                // Subscribed by instType; each data item aggregates per instFamily
                // with a `details` array of individual liquidations.
                for item in data_arr {
                    let inst = match item.get("instId").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => continue,
                    };
                    let (instrument, canonical) = match self.resolve_okx_instrument(inst) {
                        Some(x) => x,
                        None => continue,
                    };
                    let details = match item.get("details").and_then(|v| v.as_array()) {
                        Some(d) => d,
                        None => continue,
                    };
                    for detail in details {
                        let price = match extract_decimal(detail, &["bkPx"])
                            .and_then(|d| Price::try_new(d).ok())
                        {
                            Some(p) => p,
                            None => continue,
                        };
                        let quantity = match extract_decimal(detail, &["sz"])
                            .and_then(|d| Quantity::try_new(d).ok())
                        {
                            Some(q) => q,
                            None => continue,
                        };
                        let side = detail
                            .get("side")
                            .and_then(|v| v.as_str())
                            .and_then(|s| Side::from_str_loose(s).ok())
                            .unwrap_or(Side::Sell);
                        events.push(MarketDataEnvelope {
                            venue: self.venue_id.clone(),
                            instrument: instrument.clone(),
                            canonical_symbol: canonical.clone(),
                            data_type: MarketDataType::Liquidation,
                            received_at: Timestamp::now(),
                            exchange_timestamp: detail
                                .get("ts")
                                .and_then(|v| v.as_str())
                                .and_then(|s| s.parse::<u64>().ok())
                                .map(Timestamp::new),
                            sequence: Sequence::new(0),
                            payload: MarketDataPayload::Liquidation(Liquidation {
                                side,
                                price,
                                quantity,
                            }),
                        });
                    }
                }
            }
            _ => {}
        }
        Ok(events)
    }

    fn resolve_okx_instrument(
        &self,
        instrument_str: &str,
    ) -> Option<(InstrumentId, CanonicalSymbol)> {
        let canonical = self.lookup_canonical(instrument_str)?;
        let instrument_id = InstrumentId::try_new(instrument_str).ok()?;
        let canonical_symbol = CanonicalSymbol::try_new(&canonical).ok()?;
        Some((instrument_id, canonical_symbol))
    }

    /// Parses a Binance combined stream message.
    ///
    /// The stream name encodes the instrument and channel: `btcusdt@trade`.
    fn parse_combined_stream(
        &self,
        stream_name: &str,
        data: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        // Parse stream name: "btcusdt@trade" → instrument="btcusdt", channel="trade"
        let (instrument_lower, channel) = match stream_name.split_once('@') {
            Some((inst, ch)) => (inst, ch),
            None => return Ok(Vec::new()),
        };

        // Look up data type from channel name.
        // Binance channels: "trade", "bookTicker", "depth@100ms", "markPrice", "forceOrder"
        let base_channel = channel.split('@').next().unwrap_or(channel);
        let data_type = match self.reverse_channel_map.get(base_channel) {
            Some(dt) => *dt,
            None => return Ok(Vec::new()),
        };

        // Look up canonical symbol (the instrument in the response "s" field is uppercase).
        let instrument_str = data
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or(instrument_lower);

        let canonical = match self.lookup_canonical(instrument_str) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

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

        let mut events = Vec::new();
        if let Some(payload) = self.try_parse_payload(data_type, data)? {
            events.push(MarketDataEnvelope {
                venue: self.venue_id.clone(),
                instrument: instrument_id,
                canonical_symbol,
                data_type,
                received_at: Timestamp::now(),
                exchange_timestamp: extract_timestamp(data),
                sequence: Sequence::new(0), // Will be assigned by SequenceTracker
                payload,
            });
        }
        Ok(events)
    }

    /// Parses a direct (non-combined) message.
    fn parse_direct_message(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let mut events = Vec::new();

        // Try to detect data type from a "channel", "type", or "e" field.
        let channel_raw = value
            .get("channel")
            .or_else(|| value.get("type"))
            .or_else(|| value.get("e"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Infer channel from message structure if no explicit event type.
        // Binance spot bookTicker: has "b","B","a","A","s","u" but no "e" field.
        let channel = if channel_raw.is_empty()
            && value.get("b").is_some()
            && value.get("a").is_some()
            && value.get("s").is_some()
        {
            // Looks like a bookTicker message — find which channel maps to ticker.
            self.ws_config
                .channel_map
                .get("ticker")
                .map(|s| s.as_str())
                .unwrap_or("")
        } else {
            channel_raw
        };

        let data_type = self.reverse_channel_map.get(channel).copied();

        // Try to extract instrument from common fields.
        let instrument_str = value
            .get("s")
            .or_else(|| value.get("symbol"))
            .or_else(|| value.get("pair"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let canonical = self.lookup_canonical(instrument_str);

        if let (Some(dt), Some(canonical), false) =
            (data_type, canonical, instrument_str.is_empty())
        {
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

            if let Some(payload) = self.try_parse_payload(dt, value)? {
                events.push(MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument: instrument_id,
                    canonical_symbol,
                    data_type: dt,
                    received_at: Timestamp::now(),
                    exchange_timestamp: extract_timestamp(value),
                    sequence: Sequence::new(0),
                    payload,
                });
            }
        }

        // Handle Binance forceOrder wrapper: {"e":"forceOrder","o":{...}}
        if channel == "forceOrder"
            && let Some(inner) = value.get("o")
        {
            let instrument_str = inner.get("s").and_then(|v| v.as_str()).unwrap_or("");
            let canonical = self.lookup_canonical(instrument_str);

            if let (Some(canonical), false) = (canonical, instrument_str.is_empty()) {
                let instrument_id = InstrumentId::try_new(instrument_str).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?;
                let canonical_symbol = CanonicalSymbol::try_new(&canonical).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?;

                if let Some(payload) = self.try_parse_payload(MarketDataType::Liquidation, inner)? {
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument: instrument_id,
                        canonical_symbol,
                        data_type: MarketDataType::Liquidation,
                        received_at: Timestamp::now(),
                        exchange_timestamp: extract_timestamp(inner),
                        sequence: Sequence::new(0),
                        payload,
                    });
                }
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

                // Side detection:
                // 1. String field "S" or "side" (common format)
                // 2. Boolean field "m" (Binance: true = buyer is maker → aggressor is SELL)
                let side = if let Some(s) = value
                    .get("S")
                    .or_else(|| value.get("side"))
                    .and_then(|v| v.as_str())
                {
                    Side::from_str_loose(s).unwrap_or(Side::Buy)
                } else if let Some(m) = value.get("m").and_then(|v| v.as_bool()) {
                    // Binance: "m" = true means buyer is the maker → aggressor is SELL
                    if m { Side::Sell } else { Side::Buy }
                } else {
                    Side::Buy
                };

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
                // Binance bookTicker: b=bid price, B=bid qty, a=ask price, A=ask qty
                // Binance 24h ticker: b=bid, B=bidQty, a=ask, A=askQty, c=last price
                let bid_price = extract_decimal(value, &["b", "bid", "bidPrice"])
                    .and_then(|d| Price::try_new(d).ok());
                let bid_qty = extract_decimal(value, &["B", "bidQty"])
                    .and_then(|d| Quantity::try_new(d).ok());
                let ask_price = extract_decimal(value, &["a", "ask", "askPrice"])
                    .and_then(|d| Price::try_new(d).ok());
                let ask_qty = extract_decimal(value, &["A", "askQty"])
                    .and_then(|d| Quantity::try_new(d).ok());
                // Last price: "c" (Binance 24h ticker), "last", "lastPrice"
                // For bookTicker (no last price), fall back to midpoint of bid/ask.
                let last_price = extract_decimal(value, &["c", "last", "lastPrice"])
                    .and_then(|d| Price::try_new(d).ok())
                    .or_else(|| {
                        // Fallback: midpoint of bid and ask.
                        let bp = bid_price.map(|p| p.value())?;
                        let ap = ask_price.map(|p| p.value())?;
                        bp.checked_add(ap)
                            .and_then(|sum| sum.checked_div(Decimal::TWO))
                            .and_then(|mid| Price::try_new(mid).ok())
                    });

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
                // Binance depth: no "type" field; first message after subscribe is a full snapshot
                // if using depthSnapshot endpoint. For stream updates, detect via "lastUpdateId".
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
                // Binance futures markPrice stream: "r" = funding rate, "T" = next funding time
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
                // Binance forceOrder: "S" = side, "p" = price, "q" = quantity
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

    /// Parses a BitMEX realtime envelope:
    /// `{"table":"trade","action":"insert","data":[{...}]}`.
    ///
    /// `liquidation`. Order book updates/deletes require an `id → price`
    /// map the generic adapter does not yet maintain; this PR emits
    /// `partial` as snapshot and `insert` as delta and drops other
    /// actions with a `DEBUG` log — the full id map is tracked as
    /// follow-up in the BitMEX venue issue.
    fn parse_bitmex_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let table = match value.get("table").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => return Ok(Vec::new()),
        };
        let action = value
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("update");
        let data = match value.get("data").and_then(|v| v.as_array()) {
            Some(d) => d,
            None => return Ok(Vec::new()),
        };

        match table {
            "trade" => self.build_bitmex_trades(data),
            "quote" => self.build_bitmex_quote(data),
            "orderBookL2_25" | "orderBookL2" => self.build_bitmex_book(data, action),
            "funding" => self.build_bitmex_funding(data),
            "liquidation" => self.build_bitmex_liquidations(data),
            _ => Ok(Vec::new()),
        }
    }

    fn build_bitmex_trades(
        &self,
        items: &[serde_json::Value],
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let mut events = Vec::with_capacity(items.len());
        for item in items {
            let symbol = match item.get("symbol").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let canonical = match self.lookup_canonical(symbol) {
                Some(c) => c,
                None => continue,
            };
            let price = match extract_decimal(item, &["price"]).and_then(|d| Price::try_new(d).ok())
            {
                Some(p) => p,
                None => continue,
            };
            let quantity =
                match extract_decimal(item, &["size"]).and_then(|d| Quantity::try_new(d).ok()) {
                    Some(q) => q,
                    None => continue,
                };
            let side = item
                .get("side")
                .and_then(|v| v.as_str())
                .and_then(|s| Side::from_str_loose(s).ok())
                .unwrap_or(Side::Buy);
            let trade_id = item
                .get("trdMatchID")
                .and_then(|v| v.as_str())
                .map(|s| s.to_owned());
            events.push(MarketDataEnvelope {
                venue: self.venue_id.clone(),
                instrument: InstrumentId::try_new(symbol).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?,
                canonical_symbol: CanonicalSymbol::try_new(&canonical).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?,
                data_type: MarketDataType::Trade,
                received_at: Timestamp::now(),
                exchange_timestamp: None,
                sequence: Sequence::new(0),
                payload: MarketDataPayload::Trade(Trade {
                    price,
                    quantity,
                    side,
                    trade_id,
                }),
            });
        }
        Ok(events)
    }

    fn build_bitmex_quote(
        &self,
        items: &[serde_json::Value],
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let mut events = Vec::with_capacity(items.len());
        for item in items {
            let symbol = match item.get("symbol").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let canonical = match self.lookup_canonical(symbol) {
                Some(c) => c,
                None => continue,
            };
            let bid_price =
                match extract_decimal(item, &["bidPrice"]).and_then(|d| Price::try_new(d).ok()) {
                    Some(p) => p,
                    None => continue,
                };
            let bid_qty =
                match extract_decimal(item, &["bidSize"]).and_then(|d| Quantity::try_new(d).ok()) {
                    Some(q) => q,
                    None => continue,
                };
            let ask_price =
                match extract_decimal(item, &["askPrice"]).and_then(|d| Price::try_new(d).ok()) {
                    Some(p) => p,
                    None => continue,
                };
            let ask_qty =
                match extract_decimal(item, &["askSize"]).and_then(|d| Quantity::try_new(d).ok()) {
                    Some(q) => q,
                    None => continue,
                };
            // BitMEX quote has no last price; use midpoint as fallback.
            let last_price = bid_price
                .value()
                .checked_add(ask_price.value())
                .and_then(|sum| sum.checked_div(Decimal::TWO))
                .and_then(|mid| Price::try_new(mid).ok());
            let last_price = match last_price {
                Some(p) => p,
                None => continue,
            };

            events.push(MarketDataEnvelope {
                venue: self.venue_id.clone(),
                instrument: InstrumentId::try_new(symbol).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?,
                canonical_symbol: CanonicalSymbol::try_new(&canonical).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?,
                data_type: MarketDataType::Ticker,
                received_at: Timestamp::now(),
                exchange_timestamp: None,
                sequence: Sequence::new(0),
                payload: MarketDataPayload::Ticker(Ticker {
                    bid_price,
                    bid_qty,
                    ask_price,
                    ask_qty,
                    last_price,
                }),
            });
        }
        Ok(events)
    }

    fn build_bitmex_book(
        &self,
        items: &[serde_json::Value],
        action: &str,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        // partial = full snapshot; insert = new level with price/size;
        // update/delete = require an id → price map (out of scope).
        if action != "partial" && action != "insert" {
            debug!(
                venue = %self.venue_id,
                action = %action,
                "bitmex book update/delete skipped (id map not yet implemented)"
            );
            return Ok(Vec::new());
        }
        type BookSides = (Vec<(Price, Quantity)>, Vec<(Price, Quantity)>);
        // Group levels by symbol; items in one message all share the same symbol on BitMEX.
        let mut by_symbol: HashMap<String, BookSides> = HashMap::new();
        for item in items {
            let symbol = match item.get("symbol").and_then(|v| v.as_str()) {
                Some(s) => s.to_owned(),
                None => continue,
            };
            let price = match extract_decimal(item, &["price"]).and_then(|d| Price::try_new(d).ok())
            {
                Some(p) => p,
                None => continue,
            };
            let quantity =
                match extract_decimal(item, &["size"]).and_then(|d| Quantity::try_new(d).ok()) {
                    Some(q) => q,
                    None => continue,
                };
            let side = match item.get("side").and_then(|v| v.as_str()) {
                Some(s) if s.eq_ignore_ascii_case("buy") => "buy",
                Some(s) if s.eq_ignore_ascii_case("sell") => "sell",
                _ => continue, // Skip levels with missing or unrecognized side.
            };
            let entry = by_symbol
                .entry(symbol)
                .or_insert_with(|| (Vec::new(), Vec::new()));
            if side == "buy" {
                entry.0.push((price, quantity));
            } else {
                entry.1.push((price, quantity));
            }
        }

        let mut events = Vec::with_capacity(by_symbol.len());
        let is_snapshot = action == "partial";
        for (symbol, (bids, asks)) in by_symbol {
            if bids.is_empty() && asks.is_empty() {
                continue;
            }
            let canonical = match self.lookup_canonical(&symbol) {
                Some(c) => c,
                None => continue,
            };
            events.push(MarketDataEnvelope {
                venue: self.venue_id.clone(),
                instrument: InstrumentId::try_new(&symbol).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?,
                canonical_symbol: CanonicalSymbol::try_new(&canonical).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?,
                data_type: MarketDataType::L2Orderbook,
                received_at: Timestamp::now(),
                exchange_timestamp: None,
                sequence: Sequence::new(0),
                payload: MarketDataPayload::L2Update(L2Update {
                    bids,
                    asks,
                    is_snapshot,
                }),
            });
        }
        Ok(events)
    }

    fn build_bitmex_funding(
        &self,
        items: &[serde_json::Value],
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let mut events = Vec::with_capacity(items.len());
        for item in items {
            let symbol = match item.get("symbol").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let canonical = match self.lookup_canonical(symbol) {
                Some(c) => c,
                None => continue,
            };
            let rate = match extract_decimal(item, &["fundingRate"]) {
                Some(r) => r,
                None => continue,
            };
            events.push(MarketDataEnvelope {
                venue: self.venue_id.clone(),
                instrument: InstrumentId::try_new(symbol).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?,
                canonical_symbol: CanonicalSymbol::try_new(&canonical).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?,
                data_type: MarketDataType::FundingRate,
                received_at: Timestamp::now(),
                exchange_timestamp: None,
                sequence: Sequence::new(0),
                payload: MarketDataPayload::FundingRate(FundingRate {
                    rate,
                    predicted_rate: None,
                    next_funding_at: Timestamp::new(0),
                }),
            });
        }
        Ok(events)
    }

    fn build_bitmex_liquidations(
        &self,
        items: &[serde_json::Value],
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let mut events = Vec::with_capacity(items.len());
        for item in items {
            let symbol = match item.get("symbol").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let canonical = match self.lookup_canonical(symbol) {
                Some(c) => c,
                None => continue,
            };
            let price = match extract_decimal(item, &["price"]).and_then(|d| Price::try_new(d).ok())
            {
                Some(p) => p,
                None => continue,
            };
            let quantity = match extract_decimal(item, &["leavesQty", "size"])
                .and_then(|d| Quantity::try_new(d).ok())
            {
                Some(q) => q,
                None => continue,
            };
            let side = item
                .get("side")
                .and_then(|v| v.as_str())
                .and_then(|s| Side::from_str_loose(s).ok())
                .unwrap_or(Side::Sell);
            events.push(MarketDataEnvelope {
                venue: self.venue_id.clone(),
                instrument: InstrumentId::try_new(symbol).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?,
                canonical_symbol: CanonicalSymbol::try_new(&canonical).map_err(|e| {
                    VenueError::ReceiveFailed {
                        venue: self.venue_id.as_str().to_owned(),
                        reason: e.to_string(),
                    }
                })?,
                data_type: MarketDataType::Liquidation,
                received_at: Timestamp::now(),
                exchange_timestamp: None,
                sequence: Sequence::new(0),
                payload: MarketDataPayload::Liquidation(Liquidation {
                    side,
                    price,
                    quantity,
                }),
            });
        }
        Ok(events)
    }

    /// Parses a Bitstamp v2 public stream envelope:
    /// `{"event":"...","channel":"<kind>_<instrument>","data":{...}}`.
    ///
    /// Supported channels:
    /// - `live_trades_{instrument}`    → [`MarketDataType::Trade`]
    /// - `diff_order_book_{instrument}` → [`MarketDataType::L2Orderbook`] (delta)
    /// - `order_book_{instrument}`      → [`MarketDataType::L2Orderbook`] (snapshot)
    ///
    /// `bts:error` is logged at WARN; other `bts:` control events are ignored.
    fn parse_bitstamp_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
        // Log bts:error at WARN so subscription failures surface; silence other control events.
        if event == "bts:error" {
            let msg = value
                .get("data")
                .and_then(|v| v.get("message"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            warn!(venue = %self.venue_id, error = %msg, "bitstamp subscription error");
            return Ok(Vec::new());
        }
        if event.starts_with("bts:") {
            return Ok(Vec::new());
        }
        let channel = match value.get("channel").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };

        let (data_type, instrument_str, is_snapshot_channel) =
            if let Some(rest) = channel.strip_prefix("live_trades_") {
                (MarketDataType::Trade, rest, false)
            } else if let Some(rest) = channel.strip_prefix("diff_order_book_") {
                (MarketDataType::L2Orderbook, rest, false)
            } else if let Some(rest) = channel.strip_prefix("order_book_") {
                (MarketDataType::L2Orderbook, rest, true)
            } else {
                return Ok(Vec::new());
            };

        let canonical = match self.lookup_canonical(instrument_str) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };
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

        let data = match value.get("data") {
            Some(d) => d,
            None => return Ok(Vec::new()),
        };

        match data_type {
            MarketDataType::Trade => {
                let price = match extract_decimal(data, &["price_str", "price"])
                    .and_then(|d| Price::try_new(d).ok())
                {
                    Some(p) => p,
                    None => return Ok(Vec::new()),
                };
                let quantity = match extract_decimal(data, &["amount_str", "amount"])
                    .and_then(|d| Quantity::try_new(d).ok())
                {
                    Some(q) => q,
                    None => return Ok(Vec::new()),
                };
                let side = match data.get("type").and_then(|v| v.as_u64()).unwrap_or(0) {
                    0 => Side::Buy,
                    _ => Side::Sell,
                };
                let trade_id = data.get("id").and_then(|v| {
                    if let Some(s) = v.as_str() {
                        Some(s.to_owned())
                    } else {
                        v.as_u64().map(|n| n.to_string())
                    }
                });
                // microtimestamp (us string) preferred; fall back to timestamp (s string).
                let exchange_ts = data
                    .get("microtimestamp")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .and_then(|us| us.checked_div(1000))
                    .or_else(|| {
                        data.get("timestamp")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<u64>().ok())
                            .and_then(|s| s.checked_mul(1000))
                    })
                    .map(Timestamp::new);

                Ok(vec![MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument: instrument_id,
                    canonical_symbol,
                    data_type: MarketDataType::Trade,
                    received_at: Timestamp::now(),
                    exchange_timestamp: exchange_ts,
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::Trade(Trade {
                        price,
                        quantity,
                        side,
                        trade_id,
                    }),
                }])
            }
            MarketDataType::L2Orderbook => {
                let bids = parse_price_levels(data, &["bids"]);
                let asks = parse_price_levels(data, &["asks"]);
                if bids.is_empty() && asks.is_empty() {
                    return Ok(Vec::new());
                }
                let exchange_ts = data
                    .get("microtimestamp")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .and_then(|us| us.checked_div(1000))
                    .or_else(|| {
                        data.get("timestamp")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<u64>().ok())
                            .and_then(|s| s.checked_mul(1000))
                    })
                    .map(Timestamp::new);
                Ok(vec![MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument: instrument_id,
                    canonical_symbol,
                    data_type: MarketDataType::L2Orderbook,
                    received_at: Timestamp::now(),
                    exchange_timestamp: exchange_ts,
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::L2Update(L2Update {
                        bids,
                        asks,
                        is_snapshot: is_snapshot_channel,
                    }),
                }])
            }
            _ => Ok(Vec::new()),
        }
    }

    // ── Coinbase Exchange ───────────────────────────────────────────────
    //
    // Coinbase WS sends top-level `{"type": "<msg_type>", "product_id": ...}`
    // with no outer envelope. Key types:
    //   "match" / "last_match" → Trade
    //   "ticker"               → Ticker / BBO
    //   "snapshot"             → L2 full book (is_snapshot = true)
    //   "l2update"             → incremental changes array
    //   "subscriptions" / "heartbeat" / "error" / "status" / "auction" → skip
    fn parse_coinbase_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let msg_type = match value.get("type").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => return Ok(Vec::new()),
        };
        // Control / ack / heartbeat events.
        if matches!(
            msg_type,
            "subscriptions" | "heartbeat" | "error" | "status" | "auction"
        ) {
            return Ok(Vec::new());
        }

        let product_id = match value.get("product_id").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        let canonical = match self.lookup_canonical(product_id) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };
        let instrument_id =
            InstrumentId::try_new(product_id).map_err(|e| VenueError::ReceiveFailed {
                venue: self.venue_id.as_str().to_owned(),
                reason: e.to_string(),
            })?;
        let canonical_symbol =
            CanonicalSymbol::try_new(&canonical).map_err(|e| VenueError::ReceiveFailed {
                venue: self.venue_id.as_str().to_owned(),
                reason: e.to_string(),
            })?;

        match msg_type {
            "match" | "last_match" => {
                let price =
                    match extract_decimal(value, &["price"]).and_then(|d| Price::try_new(d).ok()) {
                        Some(p) => p,
                        None => return Ok(Vec::new()),
                    };
                let quantity = match extract_decimal(value, &["size"])
                    .and_then(|d| Quantity::try_new(d).ok())
                {
                    Some(q) => q,
                    None => return Ok(Vec::new()),
                };
                let side = value
                    .get("side")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Side::from_str_loose(s).ok())
                    .unwrap_or(Side::Buy);
                let trade_id = value.get("trade_id").and_then(|v| {
                    if let Some(s) = v.as_str() {
                        Some(s.to_owned())
                    } else {
                        v.as_u64().map(|n| n.to_string())
                    }
                });
                Ok(vec![MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument: instrument_id,
                    canonical_symbol,
                    data_type: MarketDataType::Trade,
                    received_at: Timestamp::now(),
                    exchange_timestamp: None,
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::Trade(Trade {
                        price,
                        quantity,
                        side,
                        trade_id,
                    }),
                }])
            }
            "ticker" => {
                let bid_price = match extract_decimal(value, &["best_bid"])
                    .and_then(|d| Price::try_new(d).ok())
                {
                    Some(p) => p,
                    None => return Ok(Vec::new()),
                };
                let bid_qty = match extract_decimal(value, &["best_bid_size"])
                    .and_then(|d| Quantity::try_new(d).ok())
                {
                    Some(q) => q,
                    None => return Ok(Vec::new()),
                };
                let ask_price = match extract_decimal(value, &["best_ask"])
                    .and_then(|d| Price::try_new(d).ok())
                {
                    Some(p) => p,
                    None => return Ok(Vec::new()),
                };
                let ask_qty = match extract_decimal(value, &["best_ask_size"])
                    .and_then(|d| Quantity::try_new(d).ok())
                {
                    Some(q) => q,
                    None => return Ok(Vec::new()),
                };
                let last_price =
                    match extract_decimal(value, &["price"]).and_then(|d| Price::try_new(d).ok()) {
                        Some(p) => p,
                        None => return Ok(Vec::new()),
                    };
                Ok(vec![MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument: instrument_id,
                    canonical_symbol,
                    data_type: MarketDataType::Ticker,
                    received_at: Timestamp::now(),
                    exchange_timestamp: None,
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::Ticker(Ticker {
                        bid_price,
                        bid_qty,
                        ask_price,
                        ask_qty,
                        last_price,
                    }),
                }])
            }
            "snapshot" => {
                let bids = parse_price_levels(value, &["bids"]);
                let asks = parse_price_levels(value, &["asks"]);
                if bids.is_empty() && asks.is_empty() {
                    return Ok(Vec::new());
                }
                Ok(vec![MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument: instrument_id,
                    canonical_symbol,
                    data_type: MarketDataType::L2Orderbook,
                    received_at: Timestamp::now(),
                    exchange_timestamp: None,
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::L2Update(L2Update {
                        bids,
                        asks,
                        is_snapshot: true,
                    }),
                }])
            }
            "l2update" => {
                // changes: [[side, price, size], ...]
                let changes = match value.get("changes").and_then(|v| v.as_array()) {
                    Some(c) => c,
                    None => return Ok(Vec::new()),
                };
                let mut bids = Vec::new();
                let mut asks = Vec::new();
                for change in changes {
                    let arr = match change.as_array() {
                        Some(a) if a.len() >= 3 => a,
                        _ => continue,
                    };
                    let side_str = arr.first().and_then(|v| v.as_str()).unwrap_or("");
                    let price = match arr
                        .get(1)
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .and_then(|d| Price::try_new(d).ok())
                    {
                        Some(p) => p,
                        None => continue,
                    };
                    let qty = match arr
                        .get(2)
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .and_then(|d| Quantity::try_new(d).ok())
                    {
                        Some(q) => q,
                        None => continue,
                    };
                    if side_str.eq_ignore_ascii_case("buy") {
                        bids.push((price, qty));
                    } else {
                        asks.push((price, qty));
                    }
                }
                if bids.is_empty() && asks.is_empty() {
                    return Ok(Vec::new());
                }
                Ok(vec![MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument: instrument_id,
                    canonical_symbol,
                    data_type: MarketDataType::L2Orderbook,
                    received_at: Timestamp::now(),
                    exchange_timestamp: None,
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::L2Update(L2Update {
                        bids,
                        asks,
                        is_snapshot: false,
                    }),
                }])
            }
            _ => Ok(Vec::new()),
        }
    }

    // ── Deribit v2 ──────────────────────────────────────────────────────
    //
    // Deribit uses JSON-RPC 2.0 over WS. Subscriptions arrive as:
    //   {"jsonrpc":"2.0","method":"subscription","params":{"channel":"<ch>","data":{...}}}
    // Ack/response frames have "id" but not "method":"subscription".
    //
    // Channel patterns:
    //   trades.<instrument>.<rate>  → Trade (+ Liquidation tap when liquidation != "none")
    //   ticker.<instrument>.<rate>  → Ticker (+ FundingRate tap for perpetuals)
    //   book.<instrument>.<group>.<depth>.<interval> → L2Update (snapshot | change)
    fn parse_deribit_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        // Acks / responses to public/subscribe have `id` but no `method`.
        if value.get("method").and_then(|v| v.as_str()) != Some("subscription") {
            return Ok(Vec::new());
        }
        let params = match value.get("params") {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };
        let channel = match params.get("channel").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };
        let data = match params.get("data") {
            Some(d) => d,
            None => return Ok(Vec::new()),
        };

        if let Some(rest) = channel.strip_prefix("trades.") {
            // Channel tail: "{instrument}.{rate}"
            let instrument_str = match rest.split('.').next() {
                Some(i) => i,
                None => return Ok(Vec::new()),
            };
            let (instrument, canonical) = match self.resolve_deribit_instrument(instrument_str) {
                Some(x) => x,
                None => return Ok(Vec::new()),
            };
            let trades_arr = match data.as_array() {
                Some(a) => a,
                None => return Ok(Vec::new()),
            };
            let mut events = Vec::with_capacity(trades_arr.len());
            for item in trades_arr {
                if let Some(ev) =
                    self.build_deribit_trade(item, instrument.clone(), canonical.clone())?
                {
                    events.push(ev);
                }
                // Liquidation tap -- trade.liquidation in {"M", "T", "MT"} is a
                // public forced-close fill; "none" or absent means normal.
                if let Some(flag) = item.get("liquidation").and_then(|v| v.as_str())
                    && flag != "none"
                    && let Some(ev) =
                        self.build_deribit_liquidation(item, instrument.clone(), canonical.clone())?
                {
                    events.push(ev);
                }
            }
            return Ok(events);
        }

        if let Some(rest) = channel.strip_prefix("ticker.") {
            let instrument_str = match rest.split('.').next() {
                Some(i) => i,
                None => return Ok(Vec::new()),
            };
            let (instrument, canonical) = match self.resolve_deribit_instrument(instrument_str) {
                Some(x) => x,
                None => return Ok(Vec::new()),
            };
            let mut events = Vec::new();
            if let Some(ev) =
                self.build_deribit_ticker(data, instrument.clone(), canonical.clone())?
            {
                events.push(ev);
            }
            // Funding tap -- perpetuals ship current_funding / funding_8h.
            if data.get("current_funding").is_some()
                && let Some(ev) = self.build_deribit_funding(data, instrument, canonical)?
            {
                events.push(ev);
            }
            return Ok(events);
        }

        if let Some(rest) = channel.strip_prefix("book.") {
            // book.{instrument}.{group}.{depth}.{interval}
            let instrument_str = match rest.split('.').next() {
                Some(i) => i,
                None => return Ok(Vec::new()),
            };
            let (instrument, canonical) = match self.resolve_deribit_instrument(instrument_str) {
                Some(x) => x,
                None => return Ok(Vec::new()),
            };
            let book_type = data
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("change");
            let bids = parse_deribit_book_side(data.get("bids"));
            let asks = parse_deribit_book_side(data.get("asks"));
            if bids.is_empty() && asks.is_empty() {
                return Ok(Vec::new());
            }
            return Ok(vec![MarketDataEnvelope {
                venue: self.venue_id.clone(),
                instrument,
                canonical_symbol: canonical,
                data_type: MarketDataType::L2Orderbook,
                received_at: Timestamp::now(),
                exchange_timestamp: data
                    .get("timestamp")
                    .and_then(|v| v.as_u64())
                    .map(Timestamp::new),
                sequence: Sequence::new(0),
                payload: MarketDataPayload::L2Update(L2Update {
                    bids,
                    asks,
                    is_snapshot: book_type == "snapshot",
                }),
            }]);
        }

        Ok(Vec::new())
    }

    fn resolve_deribit_instrument(
        &self,
        instrument_str: &str,
    ) -> Option<(InstrumentId, CanonicalSymbol)> {
        let canonical = self.lookup_canonical(instrument_str)?;
        let instrument_id = InstrumentId::try_new(instrument_str).ok()?;
        let canonical_symbol = CanonicalSymbol::try_new(&canonical).ok()?;
        Some((instrument_id, canonical_symbol))
    }

    fn build_deribit_trade(
        &self,
        item: &serde_json::Value,
        instrument: InstrumentId,
        canonical: CanonicalSymbol,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        let price = match extract_decimal(item, &["price"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(None),
        };
        let quantity =
            match extract_decimal(item, &["amount"]).and_then(|d| Quantity::try_new(d).ok()) {
                Some(q) => q,
                None => return Ok(None),
            };
        let side = item
            .get("direction")
            .and_then(|v| v.as_str())
            .and_then(|s| Side::from_str_loose(s).ok())
            .unwrap_or(Side::Buy);
        let trade_id = item
            .get("trade_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned());
        let exchange_ts = item
            .get("timestamp")
            .and_then(|v| v.as_u64())
            .map(Timestamp::new);
        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::Trade,
            received_at: Timestamp::now(),
            exchange_timestamp: exchange_ts,
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Trade(Trade {
                price,
                quantity,
                side,
                trade_id,
            }),
        }))
    }

    fn build_deribit_liquidation(
        &self,
        item: &serde_json::Value,
        instrument: InstrumentId,
        canonical: CanonicalSymbol,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        let price = match extract_decimal(item, &["price"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(None),
        };
        let quantity =
            match extract_decimal(item, &["amount"]).and_then(|d| Quantity::try_new(d).ok()) {
                Some(q) => q,
                None => return Ok(None),
            };
        let side = item
            .get("direction")
            .and_then(|v| v.as_str())
            .and_then(|s| Side::from_str_loose(s).ok())
            .unwrap_or(Side::Sell);
        let exchange_ts = item
            .get("timestamp")
            .and_then(|v| v.as_u64())
            .map(Timestamp::new);
        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::Liquidation,
            received_at: Timestamp::now(),
            exchange_timestamp: exchange_ts,
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Liquidation(Liquidation {
                side,
                price,
                quantity,
            }),
        }))
    }

    fn build_deribit_ticker(
        &self,
        data: &serde_json::Value,
        instrument: InstrumentId,
        canonical: CanonicalSymbol,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        let bid_price =
            match extract_decimal(data, &["best_bid_price"]).and_then(|d| Price::try_new(d).ok()) {
                Some(p) => p,
                None => return Ok(None),
            };
        let bid_qty = match extract_decimal(data, &["best_bid_amount"])
            .and_then(|d| Quantity::try_new(d).ok())
        {
            Some(q) => q,
            None => return Ok(None),
        };
        let ask_price =
            match extract_decimal(data, &["best_ask_price"]).and_then(|d| Price::try_new(d).ok()) {
                Some(p) => p,
                None => return Ok(None),
            };
        let ask_qty = match extract_decimal(data, &["best_ask_amount"])
            .and_then(|d| Quantity::try_new(d).ok())
        {
            Some(q) => q,
            None => return Ok(None),
        };
        let last_price =
            match extract_decimal(data, &["last_price"]).and_then(|d| Price::try_new(d).ok()) {
                Some(p) => p,
                None => return Ok(None),
            };
        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::Ticker,
            received_at: Timestamp::now(),
            exchange_timestamp: data
                .get("timestamp")
                .and_then(|v| v.as_u64())
                .map(Timestamp::new),
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Ticker(Ticker {
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                last_price,
            }),
        }))
    }

    fn build_deribit_funding(
        &self,
        data: &serde_json::Value,
        instrument: InstrumentId,
        canonical: CanonicalSymbol,
    ) -> Result<Option<MarketDataEnvelope>, VenueError> {
        let rate = match extract_decimal(data, &["current_funding"]) {
            Some(r) => r,
            None => return Ok(None),
        };
        let predicted = extract_decimal(data, &["funding_8h"]);
        Ok(Some(MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::FundingRate,
            received_at: Timestamp::now(),
            exchange_timestamp: data
                .get("timestamp")
                .and_then(|v| v.as_u64())
                .map(Timestamp::new),
            sequence: Sequence::new(0),
            payload: MarketDataPayload::FundingRate(FundingRate {
                rate,
                predicted_rate: predicted,
                next_funding_at: Timestamp::new(0),
            }),
        }))
    }

    // ── dYdX v4 ─────────────────────────────────────────────────────────
    //
    // dYdX v4 WS sends top-level messages with `type` and `channel`:
    //   type: "connected" | "subscribed" | "channel_data" | "channel_batch_data" | "unsubscribed" | "error"
    //   channel: "v4_trades" | "v4_orderbook" | "v4_markets"
    //
    // "subscribed" carries the initial snapshot; "channel_data" / "channel_batch_data" are deltas.
    // Trades carry a `liquidation: bool` flag for liquidation tap.
    // v4_markets is a global channel that fans out per-market funding rates.
    fn parse_dydx_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        let msg_type = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if matches!(msg_type, "connected" | "unsubscribed" | "error") {
            return Ok(Vec::new());
        }
        if !matches!(
            msg_type,
            "subscribed" | "channel_data" | "channel_batch_data"
        ) {
            return Ok(Vec::new());
        }

        let channel = value.get("channel").and_then(|v| v.as_str()).unwrap_or("");
        let contents = match value.get("contents") {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };
        let is_snapshot = msg_type == "subscribed";

        match channel {
            "v4_trades" => {
                let id = value.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let (instrument, canonical) = match self.resolve_dydx_instrument(id) {
                    Some(x) => x,
                    None => return Ok(Vec::new()),
                };
                let trades = match contents.get("trades").and_then(|v| v.as_array()) {
                    Some(t) => t,
                    None => return Ok(Vec::new()),
                };
                let mut events = Vec::with_capacity(trades.len());
                for item in trades {
                    let price = match extract_decimal(item, &["price"])
                        .and_then(|d| Price::try_new(d).ok())
                    {
                        Some(p) => p,
                        None => continue,
                    };
                    let quantity = match extract_decimal(item, &["size"])
                        .and_then(|d| Quantity::try_new(d).ok())
                    {
                        Some(q) => q,
                        None => continue,
                    };
                    let side = item
                        .get("side")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Side::from_str_loose(s).ok())
                        .unwrap_or(Side::Buy);
                    let trade_id = item
                        .get("id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_owned());
                    let is_liquidation = item
                        .get("liquidation")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument: instrument.clone(),
                        canonical_symbol: canonical.clone(),
                        data_type: MarketDataType::Trade,
                        received_at: Timestamp::now(),
                        exchange_timestamp: None,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::Trade(Trade {
                            price,
                            quantity,
                            side,
                            trade_id: trade_id.clone(),
                        }),
                    });
                    // Liquidation tap.
                    if is_liquidation {
                        events.push(MarketDataEnvelope {
                            venue: self.venue_id.clone(),
                            instrument: instrument.clone(),
                            canonical_symbol: canonical.clone(),
                            data_type: MarketDataType::Liquidation,
                            received_at: Timestamp::now(),
                            exchange_timestamp: None,
                            sequence: Sequence::new(0),
                            payload: MarketDataPayload::Liquidation(Liquidation {
                                side,
                                price,
                                quantity,
                            }),
                        });
                    }
                }
                Ok(events)
            }
            "v4_orderbook" => {
                let id = value.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let (instrument, canonical) = match self.resolve_dydx_instrument(id) {
                    Some(x) => x,
                    None => return Ok(Vec::new()),
                };
                let bids = parse_dydx_levels(contents.get("bids"));
                let asks = parse_dydx_levels(contents.get("asks"));
                if bids.is_empty() && asks.is_empty() {
                    return Ok(Vec::new());
                }
                Ok(vec![MarketDataEnvelope {
                    venue: self.venue_id.clone(),
                    instrument,
                    canonical_symbol: canonical,
                    data_type: MarketDataType::L2Orderbook,
                    received_at: Timestamp::now(),
                    exchange_timestamp: None,
                    sequence: Sequence::new(0),
                    payload: MarketDataPayload::L2Update(L2Update {
                        bids,
                        asks,
                        is_snapshot,
                    }),
                }])
            }
            "v4_markets" => {
                // Global feed: fan out funding rates per market in `contents.trading`.
                let trading = match contents.get("trading").and_then(|v| v.as_object()) {
                    Some(t) => t,
                    None => return Ok(Vec::new()),
                };
                let mut events = Vec::new();
                for (symbol, market) in trading {
                    let (instrument, canonical) = match self.resolve_dydx_instrument(symbol) {
                        Some(x) => x,
                        None => continue,
                    };
                    let rate = match extract_decimal(market, &["nextFundingRate"]) {
                        Some(r) => r,
                        None => continue,
                    };
                    events.push(MarketDataEnvelope {
                        venue: self.venue_id.clone(),
                        instrument,
                        canonical_symbol: canonical,
                        data_type: MarketDataType::FundingRate,
                        received_at: Timestamp::now(),
                        exchange_timestamp: None,
                        sequence: Sequence::new(0),
                        payload: MarketDataPayload::FundingRate(FundingRate {
                            rate,
                            predicted_rate: None,
                            next_funding_at: Timestamp::new(0),
                        }),
                    });
                }
                Ok(events)
            }
            _ => Ok(Vec::new()),
        }
    }

    fn resolve_dydx_instrument(
        &self,
        instrument_str: &str,
    ) -> Option<(InstrumentId, CanonicalSymbol)> {
        let canonical = self.lookup_canonical(instrument_str)?;
        let instrument_id = InstrumentId::try_new(instrument_str).ok()?;
        let canonical_symbol = CanonicalSymbol::try_new(&canonical).ok()?;
        Some((instrument_id, canonical_symbol))
    }

    // ── Bitfinex ────────────────────────────────────────────────────────
    //
    // Bitfinex assigns an integer `chanId` on each successful subscribe ack.
    // Subsequent data frames are positional JSON arrays keyed by that chanId:
    //   [chanId, [...]]             — snapshot or single payload
    //   [chanId, "tu"|"te", [...]]  — trade update / execution
    //   [chanId, "hb"]              — heartbeat (ignored)
    //
    // Supported channels: `trades` (Trade), `ticker` (Ticker), `book` (L2Update).
    fn parse_bitfinex_envelope(
        &self,
        value: &serde_json::Value,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        // Control event branch.
        if let Some(event) = value.get("event").and_then(|v| v.as_str()) {
            if event == "subscribed" {
                self.register_bitfinex_subscription(value);
            } else if event == "error" {
                let msg = value
                    .get("msg")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let code = value.get("code").and_then(|v| v.as_u64()).unwrap_or(0);
                warn!(venue = %self.venue_id, error = %msg, code = code, "bitfinex error event");
            }
            return Ok(Vec::new());
        }

        // Data frame branch -- must be a JSON array starting with chanId.
        let arr = match value.as_array() {
            Some(a) if !a.is_empty() => a,
            _ => return Ok(Vec::new()),
        };
        let chan_id = match arr.first().and_then(|v| v.as_u64()) {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };
        // Heartbeats: [chanId, "hb"]
        if arr.get(1).and_then(|v| v.as_str()) == Some("hb") {
            return Ok(Vec::new());
        }

        let route = match self.bitfinex_channels.get(&chan_id) {
            Some(r) => r.clone(),
            None => return Ok(Vec::new()),
        };
        let instrument =
            InstrumentId::try_new(&route.instrument).map_err(|e| VenueError::ReceiveFailed {
                venue: self.venue_id.as_str().to_owned(),
                reason: e.to_string(),
            })?;
        let canonical_symbol =
            CanonicalSymbol::try_new(&route.canonical).map_err(|e| VenueError::ReceiveFailed {
                venue: self.venue_id.as_str().to_owned(),
                reason: e.to_string(),
            })?;

        match route.data_type {
            MarketDataType::Trade => self.build_bitfinex_trades(arr, instrument, canonical_symbol),
            MarketDataType::Ticker => self.build_bitfinex_ticker(arr, instrument, canonical_symbol),
            MarketDataType::L2Orderbook => {
                self.build_bitfinex_book(arr, instrument, canonical_symbol)
            }
            MarketDataType::FundingRate | MarketDataType::Liquidation => Ok(Vec::new()),
        }
    }

    fn register_bitfinex_subscription(&self, value: &serde_json::Value) {
        let chan_id = match value.get("chanId").and_then(|v| v.as_u64()) {
            Some(id) => id,
            None => return,
        };
        let channel = value.get("channel").and_then(|v| v.as_str()).unwrap_or("");
        let symbol = match value
            .get("symbol")
            .and_then(|v| v.as_str())
            .or_else(|| value.get("key").and_then(|v| v.as_str()))
        {
            Some(s) => s,
            None => return,
        };
        let canonical = match self.lookup_canonical(symbol) {
            Some(c) => c,
            None => return,
        };
        let data_type = match channel {
            "trades" => MarketDataType::Trade,
            "ticker" => MarketDataType::Ticker,
            "book" => MarketDataType::L2Orderbook,
            "status" if symbol.starts_with("deriv:") => MarketDataType::FundingRate,
            "status" if symbol.starts_with("liq:") => MarketDataType::Liquidation,
            _ => return,
        };
        self.bitfinex_channels.insert(
            chan_id,
            BitfinexChannel {
                instrument: symbol.to_owned(),
                canonical,
                data_type,
            },
        );
    }

    fn build_bitfinex_trades(
        &self,
        arr: &[serde_json::Value],
        instrument: InstrumentId,
        canonical: CanonicalSymbol,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        // Two shapes:
        //   [chanId, [[id, mts, amount, price], ...]]   (snapshot)
        //   [chanId, "tu"|"te", [id, mts, amount, price]] (single update)
        let payload = if let Some(s) = arr.get(1).and_then(|v| v.as_str())
            && (s == "tu" || s == "te")
        {
            arr.get(2)
        } else {
            arr.get(1)
        };
        let payload = match payload {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };

        let trades_iter: Vec<&serde_json::Value> = if let Some(outer) = payload.as_array() {
            // Snapshot: array of trade arrays. Single update: a flat array of fields.
            // Distinguish by inspecting the first element -- a snapshot's items are
            // arrays themselves; a single update's items are scalars.
            if outer.first().map(|v| v.is_array()).unwrap_or(false) {
                outer.iter().collect()
            } else {
                vec![payload]
            }
        } else {
            return Ok(Vec::new());
        };

        let mut events = Vec::with_capacity(trades_iter.len());
        for trade in trades_iter {
            let inner = match trade.as_array().filter(|a| a.len() >= 4) {
                Some(a) => a,
                None => continue,
            };
            let trade_id = inner.first().and_then(|v| {
                if let Some(s) = v.as_str() {
                    Some(s.to_owned())
                } else {
                    v.as_u64().map(|n| n.to_string())
                }
            });
            let exchange_ts = inner.get(1).and_then(|v| v.as_u64()).map(Timestamp::new);
            let amount_dec = match inner
                .get(2)
                .and_then(|v| v.as_f64())
                .and_then(|f| Decimal::try_from(f).ok())
            {
                Some(d) => d,
                None => continue,
            };
            let side = if amount_dec.is_sign_negative() {
                Side::Sell
            } else {
                Side::Buy
            };
            let quantity = match Quantity::try_new(amount_dec.abs()) {
                Ok(q) => q,
                Err(_) => continue,
            };
            let price = match inner
                .get(3)
                .and_then(|v| v.as_f64())
                .and_then(|f| Decimal::try_from(f).ok())
                .and_then(|d| Price::try_new(d).ok())
            {
                Some(p) => p,
                None => continue,
            };
            events.push(MarketDataEnvelope {
                venue: self.venue_id.clone(),
                instrument: instrument.clone(),
                canonical_symbol: canonical.clone(),
                data_type: MarketDataType::Trade,
                received_at: Timestamp::now(),
                exchange_timestamp: exchange_ts,
                sequence: Sequence::new(0),
                payload: MarketDataPayload::Trade(Trade {
                    price,
                    quantity,
                    side,
                    trade_id,
                }),
            });
        }
        Ok(events)
    }

    fn build_bitfinex_ticker(
        &self,
        arr: &[serde_json::Value],
        instrument: InstrumentId,
        canonical: CanonicalSymbol,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        // [chanId, [bid, bid_size, ask, ask_size, daily_change, daily_change_rel, last_price, volume, high, low]]
        let payload = match arr.get(1).and_then(|v| v.as_array()) {
            Some(a) if a.len() >= 10 => a,
            _ => return Ok(Vec::new()),
        };
        let parse_dec = |idx: usize| -> Option<Decimal> {
            payload
                .get(idx)
                .and_then(|v| v.as_f64())
                .and_then(|f| Decimal::try_from(f).ok())
        };
        let bid_price = match parse_dec(0).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };
        let bid_qty = match parse_dec(1).and_then(|d| Quantity::try_new(d).ok()) {
            Some(q) => q,
            None => return Ok(Vec::new()),
        };
        let ask_price = match parse_dec(2).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };
        let ask_qty = match parse_dec(3).and_then(|d| Quantity::try_new(d).ok()) {
            Some(q) => q,
            None => return Ok(Vec::new()),
        };
        let last_price = match parse_dec(6).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };
        Ok(vec![MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::Ticker,
            received_at: Timestamp::now(),
            exchange_timestamp: None,
            sequence: Sequence::new(0),
            payload: MarketDataPayload::Ticker(Ticker {
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                last_price,
            }),
        }])
    }

    fn build_bitfinex_book(
        &self,
        arr: &[serde_json::Value],
        instrument: InstrumentId,
        canonical: CanonicalSymbol,
    ) -> Result<Vec<MarketDataEnvelope>, VenueError> {
        // Two shapes:
        //   [chanId, [[price, count, amount], ...]]  (snapshot, full book)
        //   [chanId, [price, count, amount]]         (single-level update)
        let payload = match arr.get(1).and_then(|v| v.as_array()) {
            Some(a) => a,
            None => return Ok(Vec::new()),
        };

        let levels_raw: Vec<&serde_json::Value> =
            if payload.first().map(|v| v.is_array()).unwrap_or(false) {
                payload.iter().collect()
            } else {
                // Single delta level -- wrap as a one-element list and reuse the loop.
                vec![arr.get(1).unwrap_or(&serde_json::Value::Null)]
            };
        // Snapshot: the payload contains an array of level arrays.
        // Single-level update: the payload is a flat [price, count, amount].
        // We detect snapshot by whether the first element is itself an array.
        let is_snapshot = payload.first().map(|v| v.is_array()).unwrap_or(false);

        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for lvl in levels_raw {
            let inner = match lvl.as_array().filter(|a| a.len() >= 3) {
                Some(a) => a,
                None => continue,
            };
            let price = match inner
                .first()
                .and_then(|v| v.as_f64())
                .and_then(|f| Decimal::try_from(f).ok())
                .and_then(|d| Price::try_new(d).ok())
            {
                Some(p) => p,
                None => continue,
            };
            // count is index 1; when count==0 the level should be removed
            // (emit Quantity 0). Amount is index 2 with sign indicating side.
            let count = inner.get(1).and_then(|v| v.as_u64()).unwrap_or(0);
            let amount = match inner
                .get(2)
                .and_then(|v| v.as_f64())
                .and_then(|f| Decimal::try_from(f).ok())
            {
                Some(d) => d,
                None => continue,
            };
            let qty = if count == 0 {
                // Level removal -- emit zero quantity per the domain contract.
                match Quantity::try_new(Decimal::ZERO) {
                    Ok(q) => q,
                    Err(_) => continue,
                }
            } else {
                match Quantity::try_new(amount.abs()) {
                    Ok(q) => q,
                    Err(_) => continue,
                }
            };
            if amount.is_sign_negative() {
                asks.push((price, qty));
            } else {
                bids.push((price, qty));
            }
        }
        if bids.is_empty() && asks.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![MarketDataEnvelope {
            venue: self.venue_id.clone(),
            instrument,
            canonical_symbol: canonical,
            data_type: MarketDataType::L2Orderbook,
            received_at: Timestamp::now(),
            exchange_timestamp: None,
            sequence: Sequence::new(0),
            payload: MarketDataPayload::L2Update(L2Update {
                bids,
                asks,
                is_snapshot,
            }),
        }])
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

            // Build instrument map (all case variants for matching responses).
            for sub in &subs {
                self.instrument_map
                    .insert(sub.instrument.clone(), sub.canonical_symbol.clone());
                self.instrument_map
                    .insert(sub.instrument.to_uppercase(), sub.canonical_symbol.clone());
                self.instrument_map
                    .insert(sub.instrument.to_lowercase(), sub.canonical_symbol.clone());
            }

            // Build subscribe messages.
            let messages = self.build_subscribe_messages(&subs);
            for msg in &messages {
                debug!(venue = %self.venue_id, message = %msg, "sending subscribe");
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
                        // Some venues require an application-level heartbeat
                        // response. Intercept them before parsing.
                        if self.intercept_control(&text).await? {
                            continue;
                        }
                        let events = self.parse_message(&text)?;
                        if !events.is_empty() {
                            self.circuit_breaker.record_success();
                            return Ok(events);
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        if let Ok(text) = std::str::from_utf8(&data) {
                            if self.intercept_control(text).await? {
                                continue;
                            }
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
                    let price = inner
                        .first()
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .or_else(|| {
                            inner
                                .first()
                                .and_then(|v| v.as_f64())
                                .and_then(|f| Decimal::try_from(f).ok())
                        })
                        .and_then(|d| Price::try_new(d).ok());
                    let qty = inner
                        .get(1)
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .or_else(|| {
                            inner
                                .get(1)
                                .and_then(|v| v.as_f64())
                                .and_then(|f| Decimal::try_from(f).ok())
                        })
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

/// Parses Hyperliquid book levels: array of `{px, sz, n}` objects.
#[must_use]
fn parse_hyperliquid_levels(value: Option<&serde_json::Value>) -> Vec<(Price, Quantity)> {
    let arr = match value.and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return Vec::new(),
    };
    let mut levels = Vec::with_capacity(arr.len());
    for item in arr {
        let price = match extract_decimal(item, &["px"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => continue,
        };
        let qty = match extract_decimal(item, &["sz"]).and_then(|d| Quantity::try_new(d).ok()) {
            Some(q) => q,
            None => continue,
        };
        levels.push((price, qty));
    }
    levels
}

/// Parses Kraken v2 book levels: array of `{price, qty}` objects.
#[must_use]
fn parse_kraken_levels(value: Option<&serde_json::Value>) -> Vec<(Price, Quantity)> {
    let arr = match value.and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return Vec::new(),
    };
    let mut levels = Vec::with_capacity(arr.len());
    for item in arr {
        let price = match extract_decimal(item, &["price"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => continue,
        };
        let qty = match extract_decimal(item, &["qty"]).and_then(|d| Quantity::try_new(d).ok()) {
            Some(q) => q,
            None => continue,
        };
        levels.push((price, qty));
    }
    levels
}

/// Parses Deribit order-book level arrays.
///
/// Deribit sends 3-element arrays: `["new"|"change"|"delete", price, amount]`.
/// Price and amount may be JSON numbers (f64) or occasionally strings.
fn parse_deribit_book_side(value: Option<&serde_json::Value>) -> Vec<(Price, Quantity)> {
    let arr = match value.and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return Vec::new(),
    };
    let mut levels = Vec::with_capacity(arr.len());
    for item in arr {
        let inner = match item.as_array().filter(|a| a.len() >= 3) {
            Some(i) => i,
            None => continue,
        };
        let price = match inner
            .get(1)
            .and_then(|v| v.as_f64())
            .and_then(|f| Decimal::try_from(f).ok())
            .or_else(|| {
                inner
                    .get(1)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<Decimal>().ok())
            })
            .and_then(|d| Price::try_new(d).ok())
        {
            Some(p) => p,
            None => continue,
        };
        let qty = match inner
            .get(2)
            .and_then(|v| v.as_f64())
            .and_then(|f| Decimal::try_from(f).ok())
            .or_else(|| {
                inner
                    .get(2)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<Decimal>().ok())
            })
            .and_then(|d| Quantity::try_new(d).ok())
        {
            Some(q) => q,
            None => continue,
        };
        levels.push((price, qty));
    }
    levels
}

/// Parses dYdX v4 order-book levels.
///
/// dYdX sends levels as objects: `{"price": "42000.00", "size": "0.5"}`.
fn parse_dydx_levels(value: Option<&serde_json::Value>) -> Vec<(Price, Quantity)> {
    let arr = match value.and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return Vec::new(),
    };
    let mut levels = Vec::with_capacity(arr.len());
    for item in arr {
        let price = match extract_decimal(item, &["price"]).and_then(|d| Price::try_new(d).ok()) {
            Some(p) => p,
            None => continue,
        };
        let qty = match extract_decimal(item, &["size"]).and_then(|d| Quantity::try_new(d).ok()) {
            Some(q) => q,
            None => continue,
        };
        levels.push((price, qty));
    }
    levels
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::GenericWsConfig;
    use std::collections::HashMap;

    fn make_adapter(venue_id: &str, channel_map: &[(&str, &str)]) -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        for (k, v) in channel_map {
            cm.insert((*k).to_owned(), (*v).to_owned());
        }
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}.${instrument}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter =
            GenericWsAdapter::new(venue_id, conn, ws_cfg, None).expect("adapter creation succeeds");
        // Seed the instrument map so parsers can resolve canonical symbols.
        adapter
            .instrument_map
            .insert("BTCUSDT".to_owned(), "BTC/USDT".to_owned());
        adapter
            .instrument_map
            .insert("btcusdt".to_owned(), "BTC/USDT".to_owned());
        adapter
            .instrument_map
            .insert("ETHUSDT".to_owned(), "ETH/USDT".to_owned());
        adapter
            .instrument_map
            .insert("ethusdt".to_owned(), "ETH/USDT".to_owned());
        adapter
    }

    // ── Bybit ───────────────────────────────────────────────────────────

    fn bybit_adapter() -> GenericWsAdapter {
        make_adapter(
            "bybit",
            &[
                ("trade", "publicTrade"),
                ("ticker", "tickers"),
                ("l2_orderbook", "orderbook.50"),
            ],
        )
    }

    #[test]
    fn test_bybit_parse_public_trade_snapshot_yields_trade() {
        let adapter = bybit_adapter();
        let msg = r#"{
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868,
            "data": [{
                "T": 1672304486865,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "0.001",
                "p": "16578.50",
                "L": "PlusTick",
                "i": "20f43950-d8dd-5b31-9112-a178eb6023af",
                "BT": false
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.data_type, MarketDataType::Trade);
        assert_eq!(ev.canonical_symbol.as_str(), "BTC/USDT");
        assert_eq!(
            ev.exchange_timestamp.map(|t| t.as_millis()),
            Some(1_672_304_486_865)
        );
        match &ev.payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Buy);
                assert_eq!(
                    t.trade_id.as_deref(),
                    Some("20f43950-d8dd-5b31-9112-a178eb6023af")
                );
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_bybit_parse_tickers_yields_ticker() {
        let adapter = bybit_adapter();
        let msg = r#"{
            "topic": "tickers.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868,
            "data": {
                "symbol": "BTCUSDT",
                "bid1Price": "16578.49",
                "bid1Size": "1.2",
                "ask1Price": "16578.51",
                "ask1Size": "0.8",
                "lastPrice": "16578.50"
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.data_type, MarketDataType::Ticker);
        match &ev.payload {
            MarketDataPayload::Ticker(_) => {}
            other => panic!("expected Ticker, got {other:?}"),
        }
    }

    #[test]
    fn test_bybit_parse_orderbook_snapshot_flags_is_snapshot() {
        let adapter = bybit_adapter();
        let msg = r#"{
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304484978,
            "data": {
                "s": "BTCUSDT",
                "b": [["16493.50","0.006"], ["16493.00","0.100"]],
                "a": [["16611.00","0.029"]],
                "u": 18521288,
                "seq": 7961638724
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(u.is_snapshot);
                assert_eq!(u.bids.len(), 2);
                assert_eq!(u.asks.len(), 1);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_bybit_parse_orderbook_delta_flags_not_snapshot() {
        let adapter = bybit_adapter();
        let msg = r#"{
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",
            "ts": 1672304484979,
            "data": {
                "s": "BTCUSDT",
                "b": [["16493.50","0.007"]],
                "a": [],
                "u": 18521289,
                "seq": 7961638725
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => assert!(!u.is_snapshot),
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_bybit_parse_liquidation_linear_yields_liquidation() {
        let mut adapter = make_adapter(
            "bybit-linear",
            &[("trade", "publicTrade"), ("liquidation", "liquidation")],
        );
        // Add linear symbol mapping.
        adapter
            .instrument_map
            .insert("BTCUSDT".to_owned(), "BTC/USDT".to_owned());
        let msg = r#"{
            "topic": "liquidation.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868,
            "data": {
                "updatedTime": 1672304486868,
                "symbol": "BTCUSDT",
                "side": "Sell",
                "size": "0.003",
                "price": "16578.50"
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Liquidation(l) => {
                assert_eq!(l.side, Side::Sell);
            }
            other => panic!("expected Liquidation, got {other:?}"),
        }
    }

    #[test]
    fn test_bybit_unknown_topic_returns_empty() {
        let adapter = bybit_adapter();
        let msg = r#"{"topic":"kline.1.BTCUSDT","type":"snapshot","ts":1,"data":{}}"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert!(events.is_empty());
    }

    // ── BitMEX ───────────────────────────────────────────────────────────

    fn make_bitmex_adapter() -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "trade".to_owned());
        cm.insert("ticker".to_owned(), "quote".to_owned());
        cm.insert("l2_orderbook".to_owned(), "orderBookL2_25".to_owned());
        cm.insert("funding_rate".to_owned(), "funding".to_owned());
        cm.insert("liquidation".to_owned(), "liquidation".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}:${instrument}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter =
            GenericWsAdapter::new("bitmex", conn, ws_cfg, None).expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("XBTUSDT".to_owned(), "XBT/USDT".to_owned());
        adapter
            .instrument_map
            .insert("ETHUSDT".to_owned(), "ETH/USDT".to_owned());
        adapter
    }

    #[test]
    fn test_bitmex_parse_trade_insert_yields_trade() {
        let adapter = make_bitmex_adapter();
        let msg = r#"{
            "table": "trade",
            "action": "insert",
            "data": [{
                "timestamp": "2021-04-17T09:48:53.149Z",
                "symbol": "XBTUSDT",
                "side": "Buy",
                "size": 1000,
                "price": 61385.5,
                "tickDirection": "PlusTick",
                "trdMatchID": "3c8aeb0d-1c0a-4e3a-a4f5-f4a3f5e3b1d4",
                "grossValue": 1628205,
                "homeNotional": 0.01628205,
                "foreignNotional": 1000
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.data_type, MarketDataType::Trade);
        assert_eq!(ev.canonical_symbol.as_str(), "XBT/USDT");
        match &ev.payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Buy);
                assert_eq!(
                    t.trade_id.as_deref(),
                    Some("3c8aeb0d-1c0a-4e3a-a4f5-f4a3f5e3b1d4")
                );
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_bitmex_parse_quote_yields_ticker_with_mid_fallback() {
        let adapter = make_bitmex_adapter();
        let msg = r#"{
            "table": "quote",
            "action": "insert",
            "data": [{
                "timestamp": "2021-04-17T09:48:53.149Z",
                "symbol": "XBTUSDT",
                "bidSize": 500,
                "bidPrice": 42000.0,
                "askPrice": 42002.0,
                "askSize": 300
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Ticker(t) => {
                // Mid of 42000 and 42002 is 42001.
                assert_eq!(t.last_price.value().to_string(), "42001");
            }
            other => panic!("expected Ticker, got {other:?}"),
        }
    }

    #[test]
    fn test_bitmex_parse_book_partial_yields_snapshot() {
        let adapter = make_bitmex_adapter();
        let msg = r#"{
            "table": "orderBookL2_25",
            "action": "partial",
            "data": [
                {"symbol":"XBTUSDT","id":17999992000,"side":"Buy","size":1000,"price":42000.0},
                {"symbol":"XBTUSDT","id":17999991950,"side":"Sell","size":500,"price":42001.0}
            ]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(u.is_snapshot);
                assert_eq!(u.bids.len(), 1);
                assert_eq!(u.asks.len(), 1);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_bitmex_parse_book_update_skipped() {
        let adapter = make_bitmex_adapter();
        let msg = r#"{
            "table": "orderBookL2_25",
            "action": "update",
            "data": [{"symbol":"XBTUSDT","id":17999992000,"side":"Buy","size":500}]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert!(events.is_empty(), "update action must be skipped");
    }

    #[test]
    fn test_bitmex_parse_funding_yields_funding_rate() {
        let adapter = make_bitmex_adapter();
        let msg = r#"{
            "table": "funding",
            "action": "partial",
            "data": [{
                "timestamp": "2021-04-17T12:00:00.000Z",
                "symbol": "XBTUSDT",
                "fundingInterval": "2000-01-01T08:00:00.000Z",
                "fundingRate": 0.0001,
                "fundingRateDaily": 0.0003
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::FundingRate(f) => {
                assert_eq!(f.rate.to_string(), "0.0001");
            }
            other => panic!("expected FundingRate, got {other:?}"),
        }
    }

    #[test]
    fn test_bitmex_parse_liquidation_yields_liquidation() {
        let adapter = make_bitmex_adapter();
        let msg = r#"{
            "table": "liquidation",
            "action": "insert",
            "data": [{
                "orderID": "12345678-1234-1234-1234-123456789012",
                "symbol": "XBTUSDT",
                "side": "Sell",
                "price": 60000.0,
                "leavesQty": 1000
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Liquidation(l) => assert_eq!(l.side, Side::Sell),
            other => panic!("expected Liquidation, got {other:?}"),
        }
    }

    #[test]
    fn test_bitmex_unknown_table_returns_empty() {
        let adapter = make_bitmex_adapter();
        let msg = r#"{"table":"instrument","action":"update","data":[]}"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert!(events.is_empty());
    }

    // ── Bitstamp ────────────────────────────────────────────────────────

    fn make_bitstamp_adapter() -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "live_trades".to_owned());
        cm.insert("l2_orderbook".to_owned(), "diff_order_book".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}_${instrument}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter = GenericWsAdapter::new("bitstamp", conn, ws_cfg, None)
            .expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("btcusdt".to_owned(), "BTC/USDT".to_owned());
        adapter
            .instrument_map
            .insert("ethusdt".to_owned(), "ETH/USDT".to_owned());
        adapter
    }

    #[test]
    fn test_bitstamp_parse_trade_buy_yields_trade() {
        let adapter = make_bitstamp_adapter();
        let msg = r#"{
            "event": "trade",
            "channel": "live_trades_btcusdt",
            "data": {
                "id": 312345678,
                "timestamp": "1700000000",
                "microtimestamp": "1700000000123456",
                "amount": 0.1,
                "amount_str": "0.10000000",
                "price": 42000.5,
                "price_str": "42000.50",
                "type": 0,
                "buy_order_id": 12345,
                "sell_order_id": 12346
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.data_type, MarketDataType::Trade);
        assert_eq!(ev.canonical_symbol.as_str(), "BTC/USDT");
        assert_eq!(
            ev.exchange_timestamp.map(|t| t.as_millis()),
            Some(1_700_000_000_123)
        );
        match &ev.payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Buy);
                assert_eq!(t.price.value().to_string(), "42000.50");
                assert_eq!(t.quantity.value().to_string(), "0.10000000");
                assert_eq!(t.trade_id.as_deref(), Some("312345678"));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_bitstamp_parse_trade_sell_yields_sell_side() {
        let adapter = make_bitstamp_adapter();
        let msg = r#"{
            "event": "trade",
            "channel": "live_trades_ethusdt",
            "data": {
                "id": 1,
                "microtimestamp": "1700000000000000",
                "amount_str": "1.0",
                "price_str": "2000.0",
                "type": 1
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => assert_eq!(t.side, Side::Sell),
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_bitstamp_parse_diff_order_book_yields_delta() {
        let adapter = make_bitstamp_adapter();
        let msg = r#"{
            "event": "data",
            "channel": "diff_order_book_btcusdt",
            "data": {
                "timestamp": "1700000000",
                "microtimestamp": "1700000000123456",
                "bids": [["42000.50", "0.15"], ["41999.00", "0.10"]],
                "asks": [["42001.00", "0.05"]]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(!u.is_snapshot);
                assert_eq!(u.bids.len(), 2);
                assert_eq!(u.asks.len(), 1);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_bitstamp_parse_order_book_snapshot_flags_is_snapshot() {
        let adapter = make_bitstamp_adapter();
        let msg = r#"{
            "event": "data",
            "channel": "order_book_btcusdt",
            "data": {
                "timestamp": "1700000000",
                "microtimestamp": "1700000000000000",
                "bids": [["42000.00", "0.5"]],
                "asks": [["42001.00", "0.3"]]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => assert!(u.is_snapshot),
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_bitstamp_control_events_return_empty() {
        let adapter = make_bitstamp_adapter();
        for msg in [
            r#"{"event":"bts:subscription_succeeded","channel":"live_trades_btcusdt","data":{}}"#,
            r#"{"event":"bts:heartbeat","channel":"","data":{}}"#,
            r#"{"event":"bts:request_reconnect","channel":"","data":{}}"#,
        ] {
            let events = adapter.parse_message(msg).expect("parse ok");
            assert!(events.is_empty(), "control event produced events: {msg}");
        }
    }

    #[test]
    fn test_bitstamp_unknown_channel_returns_empty() {
        let adapter = make_bitstamp_adapter();
        let msg = r#"{"event":"data","channel":"mystery_btcusdt","data":{}}"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert!(events.is_empty());
    }

    fn make_hyperliquid_adapter() -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 50,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "trades".to_owned());
        cm.insert("ticker".to_owned(), "bbo".to_owned());
        cm.insert("l2_orderbook".to_owned(), "l2Book".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}:${instrument}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter = GenericWsAdapter::new("hyperliquid", conn, ws_cfg, None)
            .expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("BTC".to_owned(), "BTC/USD".to_owned());
        adapter
            .instrument_map
            .insert("ETH".to_owned(), "ETH/USD".to_owned());
        adapter
    }

    #[test]
    fn test_hyperliquid_parse_trades_yields_trades() {
        let adapter = make_hyperliquid_adapter();
        let msg = r#"{
            "channel": "trades",
            "data": [{
                "coin": "BTC",
                "side": "B",
                "px": "42000.5",
                "sz": "0.001",
                "time": 1700000000123,
                "hash": "0xabcdef1234567890",
                "tid": 123456789
            },{
                "coin": "BTC",
                "side": "A",
                "px": "42001.0",
                "sz": "0.002",
                "time": 1700000000124,
                "hash": "0xdeadbeef",
                "tid": 123456790
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 2);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Buy);
                assert_eq!(t.trade_id.as_deref(), Some("0xabcdef1234567890"));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
        match &events[1].payload {
            MarketDataPayload::Trade(t) => assert_eq!(t.side, Side::Sell),
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_hyperliquid_parse_bbo_yields_ticker_with_midpoint_last() {
        let adapter = make_hyperliquid_adapter();
        let msg = r#"{
            "channel": "bbo",
            "data": {
                "coin": "BTC",
                "time": 1700000000123,
                "bbo": [
                    {"px": "42000.0", "sz": "0.15", "n": 3},
                    {"px": "42002.0", "sz": "0.10", "n": 2}
                ]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Ticker(t) => {
                use rust_decimal_macros::dec;
                assert_eq!(t.bid_price.value(), dec!(42000.0));
                assert_eq!(t.ask_price.value(), dec!(42002.0));
                assert_eq!(t.last_price.value(), dec!(42001));
            }
            other => panic!("expected Ticker, got {other:?}"),
        }
    }

    #[test]
    fn test_hyperliquid_parse_l2_book_always_snapshot() {
        let adapter = make_hyperliquid_adapter();
        let msg = r#"{
            "channel": "l2Book",
            "data": {
                "coin": "BTC",
                "time": 1700000000123,
                "levels": [
                    [
                        {"px": "42000.0", "sz": "0.50", "n": 4},
                        {"px": "41999.0", "sz": "1.00", "n": 6}
                    ],
                    [
                        {"px": "42001.0", "sz": "0.30", "n": 2},
                        {"px": "42002.0", "sz": "0.80", "n": 5}
                    ]
                ]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(u.is_snapshot);
                assert_eq!(u.bids.len(), 2);
                assert_eq!(u.asks.len(), 2);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_hyperliquid_unknown_channel_returns_empty() {
        let adapter = make_hyperliquid_adapter();
        let msg = r#"{"channel":"candle","data":{"coin":"BTC"}}"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert!(events.is_empty());
    }

    fn make_crypto_com_adapter(venue_id: &str) -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 25,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "trade".to_owned());
        cm.insert("ticker".to_owned(), "ticker".to_owned());
        cm.insert("l2_orderbook".to_owned(), "book".to_owned());
        cm.insert("funding_rate".to_owned(), "funding".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}.${instrument}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter =
            GenericWsAdapter::new(venue_id, conn, ws_cfg, None).expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("BTC_USDT".to_owned(), "BTC/USDT".to_owned());
        adapter
            .instrument_map
            .insert("ETH_USDT".to_owned(), "ETH/USDT".to_owned());
        adapter
            .instrument_map
            .insert("BTCUSD-PERP".to_owned(), "BTC/USD".to_owned());
        adapter
    }

    #[test]
    fn test_crypto_com_parse_trade_yields_trade() {
        let adapter = make_crypto_com_adapter("crypto-com");
        let msg = r#"{
            "method": "subscribe",
            "result": {
                "instrument_name": "BTC_USDT",
                "subscription": "trade.BTC_USDT",
                "channel": "trade",
                "data": [{
                    "d": "2367400022",
                    "t": 1700000000123,
                    "p": "42000.50",
                    "q": "0.0001",
                    "s": "BUY",
                    "i": "BTC_USDT"
                }]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Buy);
                assert_eq!(t.trade_id.as_deref(), Some("2367400022"));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_crypto_com_parse_ticker_yields_ticker() {
        let adapter = make_crypto_com_adapter("crypto-com");
        let msg = r#"{
            "method": "subscribe",
            "result": {
                "instrument_name": "BTC_USDT",
                "subscription": "ticker.BTC_USDT",
                "channel": "ticker",
                "data": [{
                    "h": "42500.00",
                    "l": "41500.00",
                    "a": "42000.50",
                    "v": "1234.56",
                    "vv": "51800000.00",
                    "c": "0.025",
                    "b": "42000.30",
                    "k": "42000.70",
                    "t": 1700000000123
                }]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Ticker(t) => {
                use rust_decimal_macros::dec;
                assert_eq!(t.bid_price.value(), dec!(42000.30));
                assert_eq!(t.ask_price.value(), dec!(42000.70));
                assert_eq!(t.last_price.value(), dec!(42000.50));
            }
            other => panic!("expected Ticker, got {other:?}"),
        }
    }

    #[test]
    fn test_crypto_com_parse_book_yields_snapshot() {
        let adapter = make_crypto_com_adapter("crypto-com");
        let msg = r#"{
            "method": "subscribe",
            "result": {
                "instrument_name": "BTC_USDT",
                "subscription": "book.BTC_USDT.50",
                "channel": "book",
                "depth": 50,
                "data": [{
                    "bids": [["42000.00","0.5","1"],["41999.00","1.0","2"]],
                    "asks": [["42001.00","0.3","1"]],
                    "t": 1700000000123,
                    "u": 12345678,
                    "pu": 12345677
                }]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(u.is_snapshot);
                assert_eq!(u.bids.len(), 2);
                assert_eq!(u.asks.len(), 1);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_crypto_com_parse_funding_yields_funding_rate() {
        let adapter = make_crypto_com_adapter("crypto-com-perp");
        let msg = r#"{
            "method": "subscribe",
            "result": {
                "instrument_name": "BTCUSD-PERP",
                "subscription": "funding.BTCUSD-PERP",
                "channel": "funding",
                "data": [{
                    "t": 1700000000123,
                    "v": "0.0001",
                    "next_funding_time": 1700028800000
                }]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::FundingRate(f) => {
                use rust_decimal_macros::dec;
                assert_eq!(f.rate, dec!(0.0001));
                assert_eq!(f.next_funding_at.as_millis(), 1_700_028_800_000);
            }
            other => panic!("expected FundingRate, got {other:?}"),
        }
    }

    #[test]
    fn test_crypto_com_unknown_channel_returns_empty() {
        let adapter = make_crypto_com_adapter("crypto-com");
        let msg = r#"{"method":"subscribe","result":{"instrument_name":"BTC_USDT","channel":"candlestick","data":[]}}"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert!(events.is_empty());
    }

    fn make_kraken_adapter() -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 25,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "trade".to_owned());
        cm.insert("ticker".to_owned(), "ticker".to_owned());
        cm.insert("l2_orderbook".to_owned(), "book".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter =
            GenericWsAdapter::new("kraken", conn, ws_cfg, None).expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("BTC/USD".to_owned(), "BTC/USD".to_owned());
        adapter
            .instrument_map
            .insert("ETH/USD".to_owned(), "ETH/USD".to_owned());
        adapter
    }

    #[test]
    fn test_kraken_parse_trade_yields_trade() {
        let adapter = make_kraken_adapter();
        let msg = r#"{
            "channel": "trade",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "side": "buy",
                "qty": 0.0001,
                "price": 42000.5,
                "ord_type": "market",
                "trade_id": 123456789,
                "timestamp": "2021-04-17T12:00:00.123456Z"
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Buy);
                assert_eq!(t.trade_id.as_deref(), Some("123456789"));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_kraken_parse_ticker_yields_ticker() {
        let adapter = make_kraken_adapter();
        let msg = r#"{
            "channel": "ticker",
            "type": "snapshot",
            "data": [{
                "symbol": "BTC/USD",
                "bid": 42000.3,
                "bid_qty": 0.5,
                "ask": 42000.7,
                "ask_qty": 0.3,
                "last": 42000.5,
                "volume": 1234.56,
                "vwap": 41900.0,
                "low": 41000.0,
                "high": 42500.0,
                "change": 100.0,
                "change_pct": 0.25
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Ticker(_) => {}
            other => panic!("expected Ticker, got {other:?}"),
        }
    }

    #[test]
    fn test_kraken_parse_book_snapshot_uses_object_levels() {
        let adapter = make_kraken_adapter();
        let msg = r#"{
            "channel": "book",
            "type": "snapshot",
            "data": [{
                "symbol": "BTC/USD",
                "bids": [
                    {"price": 42000.0, "qty": 0.5},
                    {"price": 41999.0, "qty": 1.0}
                ],
                "asks": [
                    {"price": 42001.0, "qty": 0.3},
                    {"price": 42002.0, "qty": 0.8}
                ],
                "checksum": 3145678912
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(u.is_snapshot);
                assert_eq!(u.bids.len(), 2);
                assert_eq!(u.asks.len(), 2);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_kraken_parse_book_update_flags_not_snapshot() {
        let adapter = make_kraken_adapter();
        let msg = r#"{
            "channel": "book",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "bids": [{"price": 42000.5, "qty": 0.0}],
                "asks": [],
                "checksum": 1
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => assert!(!u.is_snapshot),
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_kraken_control_channels_return_empty() {
        let adapter = make_kraken_adapter();
        for msg in [
            r#"{"channel":"status","type":"update","data":[]}"#,
            r#"{"channel":"heartbeat"}"#,
            r#"{"channel":"pong"}"#,
        ] {
            let events = adapter.parse_message(msg).expect("parse ok");
            assert!(events.is_empty(), "control channel produced events: {msg}");
        }
    }

    #[test]
    fn test_kraken_unknown_channel_returns_empty() {
        let adapter = make_kraken_adapter();
        let msg = r#"{"channel":"ohlc","type":"update","data":[]}"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert!(events.is_empty());
    }

    fn make_kraken_futures_adapter() -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 25,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "trade".to_owned());
        cm.insert("ticker".to_owned(), "ticker".to_owned());
        cm.insert("l2_orderbook".to_owned(), "book".to_owned());
        cm.insert("funding_rate".to_owned(), "ticker".to_owned());
        cm.insert("liquidation".to_owned(), "trade".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter = GenericWsAdapter::new("kraken-futures", conn, ws_cfg, None)
            .expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("PF_XBTUSD".to_owned(), "BTC/USD".to_owned());
        adapter
            .instrument_map
            .insert("PF_ETHUSD".to_owned(), "ETH/USD".to_owned());
        adapter
    }

    #[test]
    fn test_kraken_futures_parse_trade_fill_yields_trade_only() {
        let adapter = make_kraken_futures_adapter();
        let msg = r#"{
            "feed": "trade",
            "product_id": "PF_XBTUSD",
            "uid": "05af78ac-a774-478c-a50c-8b9c234e071e",
            "side": "buy",
            "type": "fill",
            "seq": 655508,
            "time": 1700000000123,
            "qty": 440,
            "price": 42000.5
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data_type, MarketDataType::Trade);
    }

    #[test]
    fn test_kraken_futures_parse_trade_liquidation_yields_both_trade_and_liquidation() {
        let adapter = make_kraken_futures_adapter();
        let msg = r#"{
            "feed": "trade",
            "product_id": "PF_XBTUSD",
            "uid": "liquid-1",
            "side": "sell",
            "type": "liquidation",
            "seq": 655509,
            "time": 1700000000200,
            "qty": 10,
            "price": 42001.0
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data_type, MarketDataType::Trade);
        assert_eq!(events[1].data_type, MarketDataType::Liquidation);
    }

    #[test]
    fn test_kraken_futures_parse_ticker_with_funding_yields_both_ticker_and_funding() {
        let adapter = make_kraken_futures_adapter();
        let msg = r#"{
            "feed": "ticker",
            "product_id": "PF_XBTUSD",
            "bid": 42000.3,
            "bid_size": 500,
            "ask": 42000.7,
            "ask_size": 300,
            "last": 42000.5,
            "time": 1700000000123,
            "funding_rate": 0.0001,
            "funding_rate_prediction": 0.00012,
            "next_funding_rate_time": 1700028800000,
            "mark_price": 42000.4,
            "index": 42000.2,
            "volume": 1234.56,
            "change": 0.5
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data_type, MarketDataType::Ticker);
        assert_eq!(events[1].data_type, MarketDataType::FundingRate);
        match &events[1].payload {
            MarketDataPayload::FundingRate(f) => {
                use rust_decimal_macros::dec;
                assert_eq!(f.rate, dec!(0.0001));
                assert_eq!(f.predicted_rate, Some(dec!(0.00012)));
            }
            other => panic!("expected FundingRate, got {other:?}"),
        }
    }

    #[test]
    fn test_kraken_futures_parse_book_snapshot_uses_object_levels() {
        let adapter = make_kraken_futures_adapter();
        let msg = r#"{
            "feed": "book_snapshot",
            "product_id": "PF_XBTUSD",
            "timestamp": 1700000000123,
            "seq": 42,
            "bids": [{"price": 42000.0, "qty": 0.5}, {"price": 41999.0, "qty": 1.0}],
            "asks": [{"price": 42001.0, "qty": 0.3}, {"price": 42002.0, "qty": 0.8}]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(u.is_snapshot);
                assert_eq!(u.bids.len(), 2);
                assert_eq!(u.asks.len(), 2);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_kraken_futures_parse_book_delta_single_level() {
        let adapter = make_kraken_futures_adapter();
        let msg = r#"{
            "feed": "book",
            "product_id": "PF_XBTUSD",
            "side": "buy",
            "price": 42000.5,
            "qty": 0.25,
            "timestamp": 1700000000124,
            "seq": 43
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(!u.is_snapshot);
                assert_eq!(u.bids.len(), 1);
                assert!(u.asks.is_empty());
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_kraken_futures_control_feeds_return_empty() {
        let adapter = make_kraken_futures_adapter();
        for msg in [
            r#"{"feed":"heartbeat"}"#,
            r#"{"feed":"info","version":1}"#,
            r#"{"feed":"subscribed_result","product_ids":["PF_XBTUSD"]}"#,
        ] {
            let events = adapter.parse_message(msg).expect("parse ok");
            assert!(events.is_empty(), "control feed produced events: {msg}");
        }
    }

    fn make_gate_adapter(venue_id: &str) -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 25,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "spot.trades".to_owned());
        cm.insert("ticker".to_owned(), "spot.book_ticker".to_owned());
        cm.insert(
            "l2_orderbook".to_owned(),
            "spot.order_book_update".to_owned(),
        );
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter =
            GenericWsAdapter::new(venue_id, conn, ws_cfg, None).expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("BTC_USDT".to_owned(), "BTC/USDT".to_owned());
        adapter
            .instrument_map
            .insert("ETH_USDT".to_owned(), "ETH/USDT".to_owned());
        adapter
    }

    #[test]
    fn test_gate_parse_spot_trade_yields_trade() {
        let adapter = make_gate_adapter("gate");
        let msg = r#"{
            "time": 1700000000,
            "channel": "spot.trades",
            "event": "update",
            "result": {
                "id": 309143071,
                "create_time": 1700000000,
                "create_time_ms": "1700000000123.456",
                "side": "sell",
                "currency_pair": "BTC_USDT",
                "amount": "0.001",
                "price": "42000.5"
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Sell);
                assert_eq!(t.trade_id.as_deref(), Some("309143071"));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_gate_parse_spot_orderbook_yields_delta() {
        let adapter = make_gate_adapter("gate");
        let msg = r#"{
            "time": 1700000000,
            "channel": "spot.order_book_update",
            "event": "update",
            "result": {
                "t": 1700000000123,
                "e": "depthUpdate",
                "s": "BTC_USDT",
                "U": 42345678,
                "u": 42345681,
                "b": [["42000.0", "0.5"]],
                "a": [["42001.0", "0.3"]]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(!u.is_snapshot);
                assert_eq!(u.bids.len(), 1);
                assert_eq!(u.asks.len(), 1);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_gate_parse_futures_tickers_emits_ticker_and_funding() {
        let adapter = make_gate_adapter("gate-futures");
        let msg = r#"{
            "time": 1700000000,
            "channel": "futures.tickers",
            "event": "update",
            "result": [{
                "contract": "BTC_USDT",
                "last": "42000.5",
                "change_percentage": "0.25",
                "funding_rate": "0.0001",
                "funding_next_apply": 1700028800,
                "mark_price": "42000.4",
                "index_price": "42000.2"
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data_type, MarketDataType::Ticker);
        assert_eq!(events[1].data_type, MarketDataType::FundingRate);
        match &events[1].payload {
            MarketDataPayload::FundingRate(f) => {
                use rust_decimal_macros::dec;
                assert_eq!(f.rate, dec!(0.0001));
                // funding_next_apply seconds × 1000 → epoch millis.
                assert_eq!(f.next_funding_at.as_millis(), 1_700_028_800_000);
            }
            other => panic!("expected FundingRate, got {other:?}"),
        }
    }

    #[test]
    fn test_gate_parse_futures_liquidates_yields_liquidation() {
        let adapter = make_gate_adapter("gate-futures");
        let msg = r#"{
            "time": 1700000000,
            "channel": "futures.liquidates",
            "event": "update",
            "result": [{
                "contract": "BTC_USDT",
                "time": 1700000000,
                "time_ms": 1700000000123,
                "left": 0,
                "size": -10,
                "fill_price": "42000.5",
                "order_id": 12345
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Liquidation(l) => {
                // Negative size → short liquidation, side documented as SELL.
                assert_eq!(l.side, Side::Sell);
            }
            other => panic!("expected Liquidation, got {other:?}"),
        }
    }

    #[test]
    fn test_gate_parse_futures_trades_yields_trade() {
        let adapter = make_gate_adapter("gate-futures");
        let msg = r#"{
            "time": 1700000000,
            "channel": "futures.trades",
            "event": "update",
            "result": [{
                "contract": "BTC_USDT",
                "id": 12345,
                "create_time": 1700000000,
                "create_time_ms": 1700000000123,
                "price": "42000.5",
                "size": 5
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Buy);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_gate_subscribe_ack_returns_empty() {
        let adapter = make_gate_adapter("gate");
        let msg = r#"{"time":1700000000,"channel":"spot.trades","event":"subscribe","result":{"status":"success"}}"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert!(events.is_empty());
    }

    fn make_okx_adapter(venue_id: &str) -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 25,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "trades".to_owned());
        cm.insert("ticker".to_owned(), "tickers".to_owned());
        cm.insert("l2_orderbook".to_owned(), "books".to_owned());
        cm.insert("funding_rate".to_owned(), "funding-rate".to_owned());
        cm.insert("liquidation".to_owned(), "liquidation-orders".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter =
            GenericWsAdapter::new(venue_id, conn, ws_cfg, None).expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("BTC-USDT".to_owned(), "BTC/USDT".to_owned());
        adapter
            .instrument_map
            .insert("ETH-USDT".to_owned(), "ETH/USDT".to_owned());
        adapter
            .instrument_map
            .insert("BTC-USDT-SWAP".to_owned(), "BTC/USDT".to_owned());
        adapter
            .instrument_map
            .insert("ETH-USDT-SWAP".to_owned(), "ETH/USDT".to_owned());
        adapter
    }

    #[test]
    fn test_okx_parse_trades_yields_trade() {
        let adapter = make_okx_adapter("okx");
        let msg = r#"{
            "arg": {"channel":"trades","instId":"BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "tradeId": "130639474",
                "px": "42219.9",
                "sz": "0.12060306",
                "side": "buy",
                "ts": "1629386267792"
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Buy);
                assert_eq!(t.trade_id.as_deref(), Some("130639474"));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_okx_parse_tickers_yields_ticker() {
        let adapter = make_okx_adapter("okx");
        let msg = r#"{
            "arg": {"channel":"tickers","instId":"BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "last": "42000.5",
                "bidPx": "42000.3",
                "bidSz": "1.0",
                "askPx": "42000.7",
                "askSz": "2.0",
                "ts": "1629386267792"
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Ticker(_) => {}
            other => panic!("expected Ticker, got {other:?}"),
        }
    }

    #[test]
    fn test_okx_parse_books_snapshot_uses_four_element_levels() {
        let adapter = make_okx_adapter("okx");
        // OKX levels are [price, size, liquidated_orders, order_count];
        // parse_price_levels reads only indices 0 and 1.
        let msg = r#"{
            "arg": {"channel":"books","instId":"BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "asks": [["42226.0","0.02","0","2"]],
                "bids": [["42225.9","0.12","0","1"]],
                "ts": "1629386267792",
                "checksum": -1076137876,
                "seqId": 123456
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(u.is_snapshot);
                assert_eq!(u.bids.len(), 1);
                assert_eq!(u.asks.len(), 1);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_okx_parse_funding_rate_yields_funding_rate() {
        let adapter = make_okx_adapter("okx-swap");
        let msg = r#"{
            "arg": {"channel":"funding-rate","instId":"BTC-USDT-SWAP"},
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "fundingRate": "0.000123",
                "nextFundingRate": "0.00011",
                "fundingTime": "1629986400000",
                "nextFundingTime": "1630015200000"
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::FundingRate(f) => {
                use rust_decimal_macros::dec;
                assert_eq!(f.rate, dec!(0.000123));
                assert_eq!(f.predicted_rate, Some(dec!(0.00011)));
            }
            other => panic!("expected FundingRate, got {other:?}"),
        }
    }

    #[test]
    fn test_okx_parse_liquidation_orders_fans_out_details() {
        let adapter = make_okx_adapter("okx-swap");
        let msg = r#"{
            "arg": {"channel":"liquidation-orders","instType":"SWAP"},
            "data": [{
                "details": [
                    {"side":"sell","sz":"0.001","bkPx":"42219.9","ts":"1629386267792"},
                    {"side":"buy","sz":"0.002","bkPx":"42220.0","ts":"1629386267793"}
                ],
                "instFamily": "BTC-USDT",
                "instId": "BTC-USDT-SWAP",
                "instType": "SWAP"
            }]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 2);
        match &events[0].payload {
            MarketDataPayload::Liquidation(l) => assert_eq!(l.side, Side::Sell),
            other => panic!("expected Liquidation, got {other:?}"),
        }
    }

    #[test]
    fn test_okx_unknown_channel_returns_empty() {
        let adapter = make_okx_adapter("okx");
        let msg = r#"{"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[]}"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert!(events.is_empty());
    }

    // ── Coinbase Exchange ───────────────────────────────────────────────

    fn make_coinbase_adapter() -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "matches".to_owned());
        cm.insert("ticker".to_owned(), "ticker".to_owned());
        cm.insert("l2_orderbook".to_owned(), "level2_batch".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter = GenericWsAdapter::new("coinbase", conn, ws_cfg, None)
            .expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("BTC-USD".to_owned(), "BTC/USD".to_owned());
        adapter
            .instrument_map
            .insert("ETH-USD".to_owned(), "ETH/USD".to_owned());
        adapter
    }

    #[test]
    fn test_coinbase_parse_match_yields_trade() {
        let adapter = make_coinbase_adapter();
        let msg = r#"{
            "type": "match",
            "trade_id": 123456789,
            "maker_order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
            "taker_order_id": "132fb6ae-456b-4654-b4e0-d681ac05cea1",
            "side": "sell",
            "size": "0.05",
            "price": "42000.50",
            "product_id": "BTC-USD",
            "sequence": 50,
            "time": "2021-04-17T12:00:00.000Z"
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Sell);
                assert_eq!(t.trade_id.as_deref(), Some("123456789"));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_coinbase_parse_ticker_yields_ticker() {
        let adapter = make_coinbase_adapter();
        let msg = r#"{
            "type": "ticker",
            "sequence": 37475248783,
            "product_id": "BTC-USD",
            "price": "42000.50",
            "open_24h": "41500.00",
            "volume_24h": "1234.56",
            "low_24h": "41000.00",
            "high_24h": "42500.00",
            "volume_30d": "40000.00",
            "best_bid": "42000.30",
            "best_bid_size": "0.10",
            "best_ask": "42000.70",
            "best_ask_size": "0.15",
            "side": "buy",
            "time": "2021-04-17T12:00:00.000Z",
            "trade_id": 12345,
            "last_size": "0.05"
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Ticker(_) => {}
            other => panic!("expected Ticker, got {other:?}"),
        }
    }

    #[test]
    fn test_coinbase_parse_snapshot_flags_is_snapshot() {
        let adapter = make_coinbase_adapter();
        let msg = r#"{
            "type": "snapshot",
            "product_id": "BTC-USD",
            "bids": [["42000.00","0.5"],["41999.00","1.0"]],
            "asks": [["42001.00","0.3"],["42002.00","0.8"]]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(u.is_snapshot);
                assert_eq!(u.bids.len(), 2);
                assert_eq!(u.asks.len(), 2);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_coinbase_parse_l2update_splits_by_side() {
        let adapter = make_coinbase_adapter();
        let msg = r#"{
            "type": "l2update",
            "product_id": "BTC-USD",
            "time": "2021-04-17T12:00:00.000Z",
            "changes": [
                ["buy", "42000.00", "0.15"],
                ["sell", "42001.00", "0.00"],
                ["sell", "42002.00", "0.30"]
            ]
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(!u.is_snapshot);
                assert_eq!(u.bids.len(), 1);
                assert_eq!(u.asks.len(), 2);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_coinbase_subscriptions_and_heartbeat_return_empty() {
        let adapter = make_coinbase_adapter();
        for msg in [
            r#"{"type":"subscriptions","channels":[]}"#,
            r#"{"type":"heartbeat","sequence":1,"last_trade_id":1,"product_id":"BTC-USD","time":"2021-04-17T12:00:00.000Z"}"#,
            r#"{"type":"status"}"#,
        ] {
            let events = adapter.parse_message(msg).expect("parse ok");
            assert!(events.is_empty(), "control event produced events: {msg}");
        }
    }

    #[test]
    fn test_coinbase_unknown_type_returns_empty() {
        let adapter = make_coinbase_adapter();
        let msg = r#"{"type":"received","product_id":"BTC-USD"}"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert!(events.is_empty());
    }

    // ── Deribit v2 ──────────────────────────────────────────────────────

    fn make_deribit_adapter() -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "trades".to_owned());
        cm.insert("ticker".to_owned(), "ticker".to_owned());
        cm.insert("l2_orderbook".to_owned(), "book".to_owned());
        cm.insert("funding_rate".to_owned(), "ticker".to_owned());
        cm.insert("liquidation".to_owned(), "trades".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter = GenericWsAdapter::new("deribit", conn, ws_cfg, None)
            .expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("BTC-PERPETUAL".to_owned(), "BTC/PERP".to_owned());
        adapter
            .instrument_map
            .insert("ETH-PERPETUAL".to_owned(), "ETH/PERP".to_owned());
        adapter
    }

    #[test]
    fn test_deribit_parse_trade_notification_yields_trade() {
        let adapter = make_deribit_adapter();
        let msg = r#"{
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": "trades.BTC-PERPETUAL.raw",
                "data": [{
                    "trade_seq": 97095,
                    "trade_id": "BTC-2100000",
                    "timestamp": 1700000000123,
                    "tick_direction": 0,
                    "price": 42000.5,
                    "mark_price": 42000.4,
                    "instrument_name": "BTC-PERPETUAL",
                    "index_price": 42000.2,
                    "direction": "buy",
                    "amount": 10.0,
                    "liquidation": "none"
                }]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Buy);
                assert_eq!(t.trade_id.as_deref(), Some("BTC-2100000"));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_deribit_parse_trade_with_liquidation_flag_emits_both() {
        let adapter = make_deribit_adapter();
        let msg = r#"{
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": "trades.BTC-PERPETUAL.raw",
                "data": [{
                    "trade_seq": 97096,
                    "trade_id": "BTC-2100001",
                    "timestamp": 1700000000200,
                    "price": 42001.0,
                    "instrument_name": "BTC-PERPETUAL",
                    "direction": "sell",
                    "amount": 20.0,
                    "liquidation": "M"
                }]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data_type, MarketDataType::Trade);
        assert_eq!(events[1].data_type, MarketDataType::Liquidation);
    }

    #[test]
    fn test_deribit_parse_ticker_with_funding_emits_both() {
        let adapter = make_deribit_adapter();
        let msg = r#"{
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": "ticker.BTC-PERPETUAL.raw",
                "data": {
                    "timestamp": 1700000000123,
                    "stats": {"volume": 12345.6, "low": 41000.0, "high": 42500.0},
                    "state": "open",
                    "settlement_price": 42000.0,
                    "open_interest": 300000000,
                    "min_price": 41800.0,
                    "max_price": 42200.0,
                    "mark_price": 42000.4,
                    "last_price": 42000.5,
                    "instrument_name": "BTC-PERPETUAL",
                    "index_price": 42000.2,
                    "funding_8h": 0.0001,
                    "current_funding": 0.00005,
                    "estimated_delivery_price": 42000.2,
                    "best_bid_price": 42000.3,
                    "best_bid_amount": 500,
                    "best_ask_price": 42000.7,
                    "best_ask_amount": 300
                }
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data_type, MarketDataType::Ticker);
        assert_eq!(events[1].data_type, MarketDataType::FundingRate);
    }

    #[test]
    fn test_deribit_parse_book_snapshot_three_element_levels() {
        let adapter = make_deribit_adapter();
        let msg = r#"{
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": "book.BTC-PERPETUAL.none.20.100ms",
                "data": {
                    "type": "snapshot",
                    "timestamp": 1700000000123,
                    "instrument_name": "BTC-PERPETUAL",
                    "change_id": 12345,
                    "bids": [["new", 42000.0, 500.0], ["new", 41999.0, 1000.0]],
                    "asks": [["new", 42001.0, 300.0]]
                }
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(u.is_snapshot);
                assert_eq!(u.bids.len(), 2);
                assert_eq!(u.asks.len(), 1);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_deribit_parse_book_change_flags_not_snapshot() {
        let adapter = make_deribit_adapter();
        let msg = r#"{
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": "book.BTC-PERPETUAL.none.20.100ms",
                "data": {
                    "type": "change",
                    "timestamp": 1700000000124,
                    "instrument_name": "BTC-PERPETUAL",
                    "change_id": 12346,
                    "bids": [["change", 42000.0, 600.0]],
                    "asks": [["delete", 42001.0, 0.0]]
                }
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => assert!(!u.is_snapshot),
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_deribit_subscribe_ack_returns_empty() {
        let adapter = make_deribit_adapter();
        let msg = r#"{"jsonrpc":"2.0","id":42,"result":["trades.BTC-PERPETUAL.raw"]}"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert!(events.is_empty());
    }

    // ── dYdX v4 ─────────────────────────────────────────────────────────

    fn make_dydx_adapter() -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 25,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "v4_trades".to_owned());
        cm.insert("l2_orderbook".to_owned(), "v4_orderbook".to_owned());
        cm.insert("funding_rate".to_owned(), "v4_markets".to_owned());
        cm.insert("liquidation".to_owned(), "v4_trades".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter =
            GenericWsAdapter::new("dydx", conn, ws_cfg, None).expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("BTC-USD".to_owned(), "BTC/USD".to_owned());
        adapter
            .instrument_map
            .insert("ETH-USD".to_owned(), "ETH/USD".to_owned());
        adapter
    }

    #[test]
    fn test_dydx_parse_trade_yields_trade() {
        let adapter = make_dydx_adapter();
        let msg = r#"{
            "type": "channel_data",
            "channel": "v4_trades",
            "id": "BTC-USD",
            "contents": {
                "trades": [{
                    "id": "0x1234abcd",
                    "side": "BUY",
                    "size": "0.001",
                    "price": "42000.5",
                    "createdAt": "2024-01-01T12:00:00.123Z",
                    "type": "LIMIT",
                    "liquidation": false
                }]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => {
                assert_eq!(t.side, Side::Buy);
                assert_eq!(t.trade_id.as_deref(), Some("0x1234abcd"));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_dydx_parse_trade_with_liquidation_emits_both() {
        let adapter = make_dydx_adapter();
        let msg = r#"{
            "type": "channel_data",
            "channel": "v4_trades",
            "id": "BTC-USD",
            "contents": {
                "trades": [{
                    "id": "0xliq",
                    "side": "SELL",
                    "size": "0.5",
                    "price": "41000.0",
                    "createdAt": "2024-01-01T12:00:01.000Z",
                    "type": "LIQUIDATED",
                    "liquidation": true
                }]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data_type, MarketDataType::Trade);
        assert_eq!(events[1].data_type, MarketDataType::Liquidation);
    }

    #[test]
    fn test_dydx_parse_orderbook_subscribed_is_snapshot() {
        let adapter = make_dydx_adapter();
        let msg = r#"{
            "type": "subscribed",
            "channel": "v4_orderbook",
            "id": "BTC-USD",
            "contents": {
                "bids": [{"price": "42000.00", "size": "0.5"}],
                "asks": [{"price": "42001.00", "size": "0.3"}]
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(u.is_snapshot);
                assert_eq!(u.bids.len(), 1);
                assert_eq!(u.asks.len(), 1);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_dydx_parse_orderbook_channel_data_is_delta() {
        let adapter = make_dydx_adapter();
        let msg = r#"{
            "type": "channel_data",
            "channel": "v4_orderbook",
            "id": "BTC-USD",
            "contents": {
                "bids": [{"price": "42000.00", "size": "0.6"}],
                "asks": []
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => assert!(!u.is_snapshot),
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_dydx_parse_v4_markets_fans_out_funding_rates() {
        let adapter = make_dydx_adapter();
        let msg = r#"{
            "type": "channel_data",
            "channel": "v4_markets",
            "contents": {
                "trading": {
                    "BTC-USD": {
                        "clobPairId": "0",
                        "ticker": "BTC-USD",
                        "status": "ACTIVE",
                        "oraclePrice": "42000.20",
                        "nextFundingRate": "0.0001",
                        "nextFundingAt": "2024-01-01T16:00:00.000Z"
                    },
                    "ETH-USD": {
                        "clobPairId": "1",
                        "ticker": "ETH-USD",
                        "status": "ACTIVE",
                        "oraclePrice": "2200.0",
                        "nextFundingRate": "0.00005",
                        "nextFundingAt": "2024-01-01T16:00:00.000Z"
                    },
                    "SOL-USD": {
                        "clobPairId": "2",
                        "ticker": "SOL-USD",
                        "status": "ACTIVE",
                        "oraclePrice": "100.0",
                        "nextFundingRate": "0.0002"
                    }
                }
            }
        }"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        // Only BTC-USD and ETH-USD are in the instrument map; SOL-USD is filtered.
        assert_eq!(events.len(), 2);
        for ev in &events {
            assert_eq!(ev.data_type, MarketDataType::FundingRate);
        }
    }

    #[test]
    fn test_dydx_control_messages_return_empty() {
        let adapter = make_dydx_adapter();
        for msg in [
            r#"{"type":"connected","connection_id":"abc"}"#,
            r#"{"type":"unsubscribed","channel":"v4_trades","id":"BTC-USD"}"#,
            r#"{"type":"error","message":"oops"}"#,
        ] {
            let events = adapter.parse_message(msg).expect("parse ok");
            assert!(events.is_empty(), "control message produced events: {msg}");
        }
    }

    // ── Bitfinex ────────────────────────────────────────────────────────

    fn make_bitfinex_adapter() -> GenericWsAdapter {
        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 25,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "trades".to_owned());
        cm.insert("ticker".to_owned(), "ticker".to_owned());
        cm.insert("l2_orderbook".to_owned(), "book".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: None,
            stream_format: "${channel}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let mut adapter = GenericWsAdapter::new("bitfinex", conn, ws_cfg, None)
            .expect("adapter creation succeeds");
        adapter
            .instrument_map
            .insert("tBTCUSD".to_owned(), "BTC/USD".to_owned());
        adapter
            .instrument_map
            .insert("tETHUSD".to_owned(), "ETH/USD".to_owned());
        adapter
    }

    fn subscribe_bitfinex(adapter: &GenericWsAdapter, channel: &str, chan_id: u64, symbol: &str) {
        let msg = serde_json::json!({
            "event": "subscribed",
            "channel": channel,
            "chanId": chan_id,
            "symbol": symbol,
            "pair": symbol.trim_start_matches('t'),
        });
        let events = adapter.parse_message(&msg.to_string()).expect("parse ok");
        assert!(events.is_empty());
    }

    #[test]
    fn test_bitfinex_subscribed_registers_chan_id() {
        let adapter = make_bitfinex_adapter();
        subscribe_bitfinex(&adapter, "trades", 17470, "tBTCUSD");
        let registered = adapter.bitfinex_channels.get(&17470);
        assert!(registered.is_some());
        let route = registered.unwrap();
        assert_eq!(route.data_type, MarketDataType::Trade);
        assert_eq!(route.canonical, "BTC/USD");
    }

    #[test]
    fn test_bitfinex_heartbeat_ignored() {
        let adapter = make_bitfinex_adapter();
        subscribe_bitfinex(&adapter, "trades", 17470, "tBTCUSD");
        let events = adapter.parse_message(r#"[17470,"hb"]"#).expect("parse ok");
        assert!(events.is_empty());
    }

    #[test]
    fn test_bitfinex_trade_snapshot_yields_trades() {
        let adapter = make_bitfinex_adapter();
        subscribe_bitfinex(&adapter, "trades", 17470, "tBTCUSD");
        // Snapshot is a single payload that contains an array of trades.
        let msg = r#"[17470,[
            [539206467, 1700000000000, 0.001, 42000.5],
            [539206466, 1699999999500, -0.002, 42000.0]
        ]]"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 2);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => assert_eq!(t.side, Side::Buy),
            other => panic!("expected Trade, got {other:?}"),
        }
        match &events[1].payload {
            MarketDataPayload::Trade(t) => assert_eq!(t.side, Side::Sell),
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_bitfinex_trade_update_yields_single_trade() {
        let adapter = make_bitfinex_adapter();
        subscribe_bitfinex(&adapter, "trades", 17470, "tBTCUSD");
        let msg = r#"[17470,"tu",[539206468, 1700000000123, -0.0005, 42000.6]]"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Trade(t) => assert_eq!(t.side, Side::Sell),
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_bitfinex_ticker_yields_ticker() {
        let adapter = make_bitfinex_adapter();
        subscribe_bitfinex(&adapter, "ticker", 11534, "tBTCUSD");
        let msg = r#"[11534,[42000.3, 1.5, 42000.7, 2.0, 500.0, 0.0125, 42000.5, 1234.56, 42500.0, 41500.0]]"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::Ticker(_) => {}
            other => panic!("expected Ticker, got {other:?}"),
        }
    }

    #[test]
    fn test_bitfinex_book_snapshot_splits_by_amount_sign() {
        let adapter = make_bitfinex_adapter();
        subscribe_bitfinex(&adapter, "book", 92105, "tBTCUSD");
        // [chanId, [[price, count, amount], ...]] -- positive amount = bid, negative = ask.
        let msg = r#"[92105,[[42000.0, 3, 0.5], [41999.0, 2, 1.5], [42001.0, 2, -0.3], [42002.0, 1, -0.4]]]"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(u.is_snapshot);
                assert_eq!(u.bids.len(), 2);
                assert_eq!(u.asks.len(), 2);
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_bitfinex_book_single_level_update() {
        let adapter = make_bitfinex_adapter();
        subscribe_bitfinex(&adapter, "book", 92105, "tBTCUSD");
        let msg = r#"[92105,[42000.0, 3, 0.5]]"#;
        let events = adapter.parse_message(msg).expect("parse ok");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            MarketDataPayload::L2Update(u) => {
                assert!(!u.is_snapshot);
                assert_eq!(u.bids.len(), 1);
                assert!(u.asks.is_empty());
            }
            other => panic!("expected L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_bitfinex_unregistered_chan_id_returns_empty() {
        let adapter = make_bitfinex_adapter();
        // No subscribe -- chan 99999 unknown.
        let events = adapter
            .parse_message(r#"[99999, [539206467, 1700000000000, 0.001, 42000.5]]"#)
            .expect("parse ok");
        assert!(events.is_empty());
    }

    // ── subscribe_mode tests ───────────────────────────────────────────────

    #[test]
    fn test_build_subscribe_per_channel_groups_by_channel() {
        use crate::application::ports::Subscription;

        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "trade".to_owned());
        cm.insert("ticker".to_owned(), "ticker".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: Some(
                r#"{"method":"subscribe","params":{"channel":"${channel}","symbol":${instruments}}}"#
                    .to_owned(),
            ),
            batch_subscribe_template: None,
            stream_format: "${channel}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_channel".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let adapter =
            GenericWsAdapter::new("kraken", conn, ws_cfg, None).expect("adapter creation succeeds");

        let subs = vec![
            Subscription {
                instrument: "BTC/USD".to_owned(),
                canonical_symbol: "BTC/USD".to_owned(),
                data_types: vec![MarketDataType::Trade, MarketDataType::Ticker],
            },
            Subscription {
                instrument: "ETH/USD".to_owned(),
                canonical_symbol: "ETH/USD".to_owned(),
                data_types: vec![MarketDataType::Trade, MarketDataType::Ticker],
            },
        ];

        let msgs = adapter.build_subscribe_messages(&subs);
        // Should produce 2 messages: one for "trade" channel, one for "ticker" channel.
        assert_eq!(msgs.len(), 2);

        // First message should be for "trade" with both instruments.
        let trade_msg: serde_json::Value = serde_json::from_str(&msgs[0]).expect("valid json");
        assert_eq!(trade_msg["params"]["channel"].as_str(), Some("trade"));
        let symbols = trade_msg["params"]["symbol"]
            .as_array()
            .expect("symbol should be array");
        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].as_str(), Some("BTC/USD"));
        assert_eq!(symbols[1].as_str(), Some("ETH/USD"));

        // Second message should be for "ticker" with both instruments.
        let ticker_msg: serde_json::Value = serde_json::from_str(&msgs[1]).expect("valid json");
        assert_eq!(ticker_msg["params"]["channel"].as_str(), Some("ticker"));
        let symbols = ticker_msg["params"]["symbol"]
            .as_array()
            .expect("symbol should be array");
        assert_eq!(symbols.len(), 2);
    }

    #[test]
    fn test_build_subscribe_object_args_produces_json_objects() {
        use crate::application::ports::Subscription;

        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "trades".to_owned());
        cm.insert("ticker".to_owned(), "tickers".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: Some(r#"{"op":"subscribe","args":${params}}"#.to_owned()),
            stream_format: r#"{"channel":"${channel}","instId":"${instrument}"}"#.to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "per_pair".to_owned(),
            args_format: "object".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let adapter =
            GenericWsAdapter::new("okx", conn, ws_cfg, None).expect("adapter creation succeeds");

        let subs = vec![Subscription {
            instrument: "BTC-USDT".to_owned(),
            canonical_symbol: "BTC/USDT".to_owned(),
            data_types: vec![MarketDataType::Trade, MarketDataType::Ticker],
        }];

        let msgs = adapter.build_subscribe_messages(&subs);
        assert_eq!(msgs.len(), 1);

        // Parse the result and verify args is an array of objects, not strings.
        let parsed: serde_json::Value = serde_json::from_str(&msgs[0]).expect("valid json");
        let args = parsed["args"].as_array().expect("args should be array");
        assert_eq!(args.len(), 2);
        // Each element must be a JSON object, not a string.
        assert!(args[0].is_object(), "expected object, got: {}", args[0]);
        assert_eq!(args[0]["channel"].as_str(), Some("trades"));
        assert_eq!(args[0]["instId"].as_str(), Some("BTC-USDT"));
        assert!(args[1].is_object(), "expected object, got: {}", args[1]);
        assert_eq!(args[1]["channel"].as_str(), Some("tickers"));
        assert_eq!(args[1]["instId"].as_str(), Some("BTC-USDT"));
    }

    #[test]
    fn test_build_subscribe_products_channels_deduplicates() {
        use crate::application::ports::Subscription;

        let conn = ConnectionConfig {
            ws_url: "wss://example.invalid/ws".to_owned(),
            reconnect_delay_ms: 1000,
            max_reconnect_delay_ms: 60000,
            max_reconnect_attempts: 0,
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
        };
        let mut cm = HashMap::new();
        cm.insert("trade".to_owned(), "matches".to_owned());
        cm.insert("ticker".to_owned(), "ticker".to_owned());
        cm.insert("l2_orderbook".to_owned(), "level2_batch".to_owned());
        let ws_cfg = GenericWsConfig {
            subscribe_template: None,
            batch_subscribe_template: Some(
                r#"{"type":"subscribe","product_ids":${instruments},"channels":${channels}}"#
                    .to_owned(),
            ),
            stream_format: "${channel}".to_owned(),
            channel_map: cm,
            message_format: "json".to_owned(),
            subscribe_mode: "products_channels".to_owned(),
            args_format: "string".to_owned(),
            channel_suffix: HashMap::new(),
        };
        let adapter = GenericWsAdapter::new("coinbase", conn, ws_cfg, None)
            .expect("adapter creation succeeds");

        // Third subscription intentionally duplicates "BTC-USD" to exercise dedup.
        let subs = vec![
            Subscription {
                instrument: "BTC-USD".to_owned(),
                canonical_symbol: "BTC/USD".to_owned(),
                data_types: vec![
                    MarketDataType::Trade,
                    MarketDataType::Ticker,
                    MarketDataType::L2Orderbook,
                ],
            },
            Subscription {
                instrument: "ETH-USD".to_owned(),
                canonical_symbol: "ETH/USD".to_owned(),
                data_types: vec![
                    MarketDataType::Trade,
                    MarketDataType::Ticker,
                    MarketDataType::L2Orderbook,
                ],
            },
            Subscription {
                instrument: "BTC-USD".to_owned(),
                canonical_symbol: "BTC/USD".to_owned(),
                data_types: vec![MarketDataType::Trade],
            },
        ];

        let msgs = adapter.build_subscribe_messages(&subs);
        // Should produce exactly 1 message.
        assert_eq!(msgs.len(), 1);

        let parsed: serde_json::Value = serde_json::from_str(&msgs[0]).expect("valid json");
        assert_eq!(parsed["type"].as_str(), Some("subscribe"));

        // product_ids should have 2 unique instruments (BTC-USD deduped).
        let products = parsed["product_ids"]
            .as_array()
            .expect("product_ids should be array");
        assert_eq!(products.len(), 2);
        assert_eq!(products[0].as_str(), Some("BTC-USD"));
        assert_eq!(products[1].as_str(), Some("ETH-USD"));

        // channels should have 3 unique channel names.
        let channels = parsed["channels"]
            .as_array()
            .expect("channels should be array");
        assert_eq!(channels.len(), 3);
        assert_eq!(channels[0].as_str(), Some("matches"));
        assert_eq!(channels[1].as_str(), Some("ticker"));
        assert_eq!(channels[2].as_str(), Some("level2_batch"));
    }
}
