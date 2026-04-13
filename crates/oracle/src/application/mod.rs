/// Port traits defining the application layer boundaries.
///
/// Infrastructure layer implements these traits to provide
/// trade data ingestion and oracle price publishing.
pub mod ports;

pub use ports::{OraclePublisher, TradeSource};
