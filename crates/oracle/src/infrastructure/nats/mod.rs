/// Publishes computed oracle prices to NATS.
pub mod publisher;
/// NATS connection setup with authentication.
pub mod setup;
/// Subscribes to NATS trade subjects and caches the latest price per (symbol, venue).
pub mod subscriber;

pub use publisher::OraclePricePublisher;
pub use setup::connect_nats;
pub use subscriber::NatsTradeSubscriber;
