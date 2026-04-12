/// Publishes computed oracle prices to NATS.
pub mod publisher;
/// Subscribes to NATS trade subjects and caches the latest price per (symbol, venue).
pub mod subscriber;

pub use publisher::OraclePricePublisher;
pub use subscriber::NatsTradeSubscriber;
