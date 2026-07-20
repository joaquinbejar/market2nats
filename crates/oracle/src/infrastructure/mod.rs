/// Fan-out publisher that dispatches oracle prices to multiple sinks.
pub mod fan_out;
/// NATS subscriber and publisher implementations for the oracle.
pub mod nats;
/// Process-level rustls crypto provider installation.
pub mod tls;
/// WebSocket server for real-time oracle price fan-out.
pub mod ws;

pub use fan_out::FanOutPublisher;
pub use nats::{NatsTradeSubscriber, OraclePricePublisher, connect_nats};
pub use tls::install_crypto_provider;
pub use ws::{BoundWsServer, OracleWsServer};
