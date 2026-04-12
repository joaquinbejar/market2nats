use std::collections::HashMap;

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
        })
    }

    /// Builds subscribe message(s) for the given subscriptions.
    ///
    /// If `batch_subscribe_template` is set, builds a single message with all streams
    /// as a JSON array in `${params}`. Otherwise, builds one message per (instrument, channel).
    fn build_subscribe_messages(&self, subs: &[Subscription]) -> Vec<String> {
        // Collect all (instrument, channel) pairs.
        let mut pairs: Vec<(String, String)> = Vec::new();
        for sub in subs {
            for dt in &sub.data_types {
                let channel_name = self
                    .ws_config
                    .channel_map
                    .get(dt.as_subject_str())
                    .cloned()
                    .unwrap_or_else(|| dt.as_subject_str().to_owned());
                pairs.push((sub.instrument.clone(), channel_name));
            }
        }

        if let Some(batch_tpl) = &self.ws_config.batch_subscribe_template {
            // Build stream names from stream_format, then inject as JSON array.
            let stream_names: Vec<String> = pairs
                .iter()
                .map(|(inst, ch)| {
                    self.ws_config
                        .stream_format
                        .replace("${instrument}", inst)
                        .replace("${channel}", ch)
                })
                .collect();
            let params_json = serde_json::to_string(&stream_names).unwrap_or_default();
            vec![batch_tpl.replace("${params}", &params_json)]
        } else if let Some(tpl) = &self.ws_config.subscribe_template {
            // One message per (instrument, channel) pair.
            pairs
                .iter()
                .map(|(inst, ch)| tpl.replace("${instrument}", inst).replace("${channel}", ch))
                .collect()
        } else {
            Vec::new()
        }
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
}
