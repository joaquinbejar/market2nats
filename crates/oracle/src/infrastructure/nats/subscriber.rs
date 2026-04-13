use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use futures_util::StreamExt;
use market2nats_domain::{CanonicalSymbol, MarketDataEnvelope, MarketDataPayload, Timestamp};

use crate::application::metrics::ORACLE_TRADE_MESSAGES_RECEIVED;
use crate::application::ports::TradeSource;
use crate::config::model::SubscriptionEntry;
use crate::domain::{OracleError, PriceSource};

/// Subscribes to NATS trade subjects and caches the latest price per (symbol, venue).
///
/// Each NATS message is expected to be a JSON-serialized `MarketDataEnvelope`.
/// Only `Trade` payloads are retained; other message types are silently filtered.
/// The latest trade per `(symbol_normalized, venue_id)` pair is kept in a `DashMap`.
pub struct NatsTradeSubscriber {
    /// Latest price source per (symbol_normalized, venue_id).
    sources: Arc<DashMap<(String, String), PriceSource>>,
    /// Maps NATS subject patterns to canonical symbols for lookup.
    symbol_map: HashMap<String, CanonicalSymbol>,
}

impl NatsTradeSubscriber {
    /// Creates a new subscriber and builds the subject-to-symbol mapping.
    ///
    /// Does not start listening; the caller must call [`subscribe_to`](Self::subscribe_to)
    /// for each subject and then spawn [`run`](Self::run) tasks.
    ///
    /// # Errors
    ///
    /// Returns `OracleError::Domain` if any symbol string is invalid.
    #[tracing::instrument(skip(subscriptions), fields(subscription_count = subscriptions.len()))]
    pub fn new(subscriptions: &[SubscriptionEntry]) -> Result<Self, OracleError> {
        let sources: Arc<DashMap<(String, String), PriceSource>> = Arc::new(DashMap::new());
        let mut symbol_map = HashMap::new();

        for entry in subscriptions {
            let canonical =
                CanonicalSymbol::try_new(entry.symbol.as_str()).map_err(OracleError::Domain)?;

            for subject in &entry.subjects {
                symbol_map.insert(subject.clone(), canonical.clone());
            }
        }

        Ok(Self {
            sources,
            symbol_map,
        })
    }

    /// Subscribes to a single NATS subject and returns the subscriber handle.
    ///
    /// The caller should spawn a task calling [`run`](Self::run) with the returned subscriber.
    ///
    /// # Errors
    ///
    /// Returns `OracleError::Nats` if the NATS subscription fails.
    pub async fn subscribe_to(
        &self,
        client: &async_nats::Client,
        subject: String,
    ) -> Result<async_nats::Subscriber, OracleError> {
        client
            .subscribe(subject.clone())
            .await
            .map_err(|e| OracleError::Nats(format!("subscribe to {subject}: {e}")))
    }

    /// Runs the message loop for a single NATS subscription.
    ///
    /// Receives messages, deserializes them as `MarketDataEnvelope`, filters for
    /// `Trade` payloads, and stores the latest `PriceSource` per (symbol, venue).
    /// Exits when the shutdown signal is received or the NATS subscription closes.
    #[tracing::instrument(skip(self, nats_subscriber, shutdown), fields(symbol = %symbol))]
    pub async fn run(
        &self,
        mut nats_subscriber: async_nats::Subscriber,
        symbol: CanonicalSymbol,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                biased;

                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!(symbol = %symbol, "shutdown received, stopping subscriber");
                        break;
                    }
                }

                msg = nats_subscriber.next() => {
                    let message = match msg {
                        Some(m) => m,
                        None => {
                            tracing::warn!(symbol = %symbol, "NATS subscription closed");
                            break;
                        }
                    };

                    self.handle_message(&message.payload, &symbol);
                }
            }
        }
    }

    /// Processes a single NATS message payload.
    ///
    /// Deserializes the payload as a `MarketDataEnvelope`, filters for trade
    /// messages, converts to `PriceSource`, and stores in the DashMap.
    fn handle_message(&self, payload: &[u8], symbol: &CanonicalSymbol) {
        let envelope: MarketDataEnvelope = match serde_json::from_slice(payload) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    symbol = %symbol,
                    error = %e,
                    "failed to deserialize MarketDataEnvelope"
                );
                return;
            }
        };

        let trade = match &envelope.payload {
            MarketDataPayload::Trade(t) => t,
            _ => {
                tracing::debug!(
                    symbol = %symbol,
                    data_type = ?envelope.data_type,
                    "filtered non-trade message"
                );
                return;
            }
        };

        let timestamp = envelope.exchange_timestamp.unwrap_or(envelope.received_at);

        let now_ms = Timestamp::now().as_millis();
        // NOTE: saturating_sub is acceptable here for time difference computation,
        // not financial math. If the clock is skewed, we clamp to 0 rather than panic.
        let age_ms = now_ms.saturating_sub(timestamp.as_millis());

        let venue_str = envelope.venue.as_str().to_owned();
        let symbol_normalized = symbol.normalized();

        let source = PriceSource {
            venue: envelope.venue,
            price: trade.price,
            quantity: trade.quantity,
            timestamp,
            age_ms,
        };

        self.sources
            .insert((symbol_normalized.clone(), venue_str.clone()), source);

        metrics::counter!(
            ORACLE_TRADE_MESSAGES_RECEIVED,
            "venue" => venue_str,
            "instrument" => symbol_normalized
        )
        .increment(1);
    }

    /// Returns a clone of the internal sources map for use in spawned tasks.
    #[must_use]
    pub fn sources(&self) -> Arc<DashMap<(String, String), PriceSource>> {
        Arc::clone(&self.sources)
    }

    /// Returns a reference to the symbol map.
    #[must_use]
    pub fn symbol_map(&self) -> &HashMap<String, CanonicalSymbol> {
        &self.symbol_map
    }
}

impl TradeSource for NatsTradeSubscriber {
    fn get_sources(&self, symbol: &CanonicalSymbol) -> Vec<PriceSource> {
        let normalized = symbol.normalized();
        let now_ms = Timestamp::now().as_millis();

        self.sources
            .iter()
            .filter(|entry| entry.key().0 == normalized)
            .map(|entry| {
                let source = entry.value();
                // Recompute age_ms at retrieval time for accurate staleness.
                // saturating_sub is acceptable here: time difference, not financial math.
                let age_ms = now_ms.saturating_sub(source.timestamp.as_millis());
                PriceSource {
                    venue: source.venue.clone(),
                    price: source.price,
                    quantity: source.quantity,
                    timestamp: source.timestamp,
                    age_ms,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use market2nats_domain::{Price, Quantity, Timestamp, VenueId};
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn test_subscriber_new_builds_symbol_map() {
        let entries = vec![SubscriptionEntry {
            symbol: "BTC/USDT".to_owned(),
            subjects: vec![
                "market.binance.btc-usdt.trade".to_owned(),
                "market.kraken.btc-usdt.trade".to_owned(),
            ],
        }];

        let subscriber = NatsTradeSubscriber::new(&entries).unwrap();
        assert_eq!(subscriber.symbol_map().len(), 2);
        assert!(
            subscriber
                .symbol_map()
                .contains_key("market.binance.btc-usdt.trade")
        );
        assert!(
            subscriber
                .symbol_map()
                .contains_key("market.kraken.btc-usdt.trade")
        );
    }

    #[test]
    fn test_subscriber_new_empty_symbol_fails() {
        let entries = vec![SubscriptionEntry {
            symbol: "".to_owned(),
            subjects: vec!["market.binance.btc-usdt.trade".to_owned()],
        }];

        let result = NatsTradeSubscriber::new(&entries);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_sources_empty_initially() {
        let subscriber = NatsTradeSubscriber::new(&[]).unwrap();
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        let result = subscriber.get_sources(&symbol);
        assert!(result.is_empty());
    }

    #[test]
    fn test_get_sources_filters_by_symbol() {
        let subscriber = NatsTradeSubscriber::new(&[]).unwrap();

        // Insert a BTC source
        subscriber.sources.insert(
            ("btc-usdt".to_owned(), "binance".to_owned()),
            PriceSource {
                venue: VenueId::try_new("binance").unwrap(),
                price: Price::try_new(dec!(50000)).unwrap(),
                quantity: Quantity::try_new(dec!(1)).unwrap(),
                timestamp: Timestamp::now(),
                age_ms: 0,
            },
        );

        // Insert an ETH source (should be filtered out)
        subscriber.sources.insert(
            ("eth-usdt".to_owned(), "binance".to_owned()),
            PriceSource {
                venue: VenueId::try_new("binance").unwrap(),
                price: Price::try_new(dec!(3000)).unwrap(),
                quantity: Quantity::try_new(dec!(10)).unwrap(),
                timestamp: Timestamp::now(),
                age_ms: 0,
            },
        );

        let btc_symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        let result = subscriber.get_sources(&btc_symbol);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].venue.as_str(), "binance");
        assert_eq!(result[0].price.value(), dec!(50000));
    }

    #[test]
    fn test_get_sources_multiple_venues() {
        let subscriber = NatsTradeSubscriber::new(&[]).unwrap();

        subscriber.sources.insert(
            ("btc-usdt".to_owned(), "binance".to_owned()),
            PriceSource {
                venue: VenueId::try_new("binance").unwrap(),
                price: Price::try_new(dec!(50000)).unwrap(),
                quantity: Quantity::try_new(dec!(1)).unwrap(),
                timestamp: Timestamp::now(),
                age_ms: 0,
            },
        );

        subscriber.sources.insert(
            ("btc-usdt".to_owned(), "kraken".to_owned()),
            PriceSource {
                venue: VenueId::try_new("kraken").unwrap(),
                price: Price::try_new(dec!(50010)).unwrap(),
                quantity: Quantity::try_new(dec!(2)).unwrap(),
                timestamp: Timestamp::now(),
                age_ms: 0,
            },
        );

        let btc_symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        let result = subscriber.get_sources(&btc_symbol);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_handle_message_valid_trade() {
        use market2nats_domain::enums::{MarketDataType, Side};
        use market2nats_domain::events::Trade;
        use market2nats_domain::{InstrumentId, Sequence};

        let subscriber = NatsTradeSubscriber::new(&[]).unwrap();

        let envelope = MarketDataEnvelope {
            venue: VenueId::try_new("binance").unwrap(),
            instrument: InstrumentId::try_new("BTCUSDT").unwrap(),
            canonical_symbol: CanonicalSymbol::try_new("BTC/USDT").unwrap(),
            data_type: MarketDataType::Trade,
            received_at: Timestamp::now(),
            exchange_timestamp: Some(Timestamp::now()),
            sequence: Sequence::new(1),
            payload: MarketDataPayload::Trade(Trade {
                price: Price::try_new(dec!(50000)).unwrap(),
                quantity: Quantity::try_new(dec!(1.5)).unwrap(),
                side: Side::Buy,
                trade_id: Some("123".to_owned()),
            }),
        };

        let json = serde_json::to_vec(&envelope).unwrap();
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        subscriber.handle_message(&json, &symbol);

        assert_eq!(subscriber.sources.len(), 1);
        let key = ("btc-usdt".to_owned(), "binance".to_owned());
        assert!(subscriber.sources.contains_key(&key));
    }

    #[test]
    fn test_handle_message_non_trade_filtered() {
        use market2nats_domain::enums::MarketDataType;
        use market2nats_domain::events::Ticker;
        use market2nats_domain::{InstrumentId, Sequence};

        let subscriber = NatsTradeSubscriber::new(&[]).unwrap();

        let envelope = MarketDataEnvelope {
            venue: VenueId::try_new("binance").unwrap(),
            instrument: InstrumentId::try_new("BTCUSDT").unwrap(),
            canonical_symbol: CanonicalSymbol::try_new("BTC/USDT").unwrap(),
            data_type: MarketDataType::Ticker,
            received_at: Timestamp::now(),
            exchange_timestamp: None,
            sequence: Sequence::new(1),
            payload: MarketDataPayload::Ticker(Ticker {
                bid_price: Price::try_new(dec!(49999)).unwrap(),
                bid_qty: Quantity::try_new(dec!(10)).unwrap(),
                ask_price: Price::try_new(dec!(50001)).unwrap(),
                ask_qty: Quantity::try_new(dec!(5)).unwrap(),
                last_price: Price::try_new(dec!(50000)).unwrap(),
            }),
        };

        let json = serde_json::to_vec(&envelope).unwrap();
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        subscriber.handle_message(&json, &symbol);

        assert!(subscriber.sources.is_empty());
    }

    #[test]
    fn test_handle_message_invalid_json() {
        let subscriber = NatsTradeSubscriber::new(&[]).unwrap();
        let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
        subscriber.handle_message(b"not valid json", &symbol);
        assert!(subscriber.sources.is_empty());
    }
}
