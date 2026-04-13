/// Fan-out publisher that dispatches oracle prices to multiple sinks.
pub mod fan_out;
/// NATS subscriber and publisher implementations for the oracle.
pub mod nats;

pub use fan_out::FanOutPublisher;
pub use nats::{NatsTradeSubscriber, OraclePricePublisher, connect_nats};
