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
                // Data is either an object (snapshot) or an object with partial fields (delta).
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
}
