/// NATS subscriber and publisher implementations for the oracle.
pub mod nats;

pub use nats::{NatsTradeSubscriber, OraclePricePublisher};
