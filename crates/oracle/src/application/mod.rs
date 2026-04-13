/// Oracle health monitoring: tracks computation freshness per symbol.
pub mod health;
/// Prometheus metric constants and registration.
pub mod metrics;
/// Oracle service: periodic computation and publishing orchestration.
pub mod oracle_service;
/// Port traits defining the application layer boundaries.
///
/// Infrastructure layer implements these traits to provide
/// trade data ingestion and oracle price publishing.
pub mod ports;

pub use health::OracleHealthMonitor;
pub use metrics::register_metrics;
pub use oracle_service::OracleService;
pub use ports::{OraclePublisher, TradeSource};
