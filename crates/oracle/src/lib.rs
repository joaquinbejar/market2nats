/// Application layer: port traits for trade sources and oracle publishing.
pub mod application;
/// Oracle configuration loading, parsing, validation, and pipeline builder.
#[cfg(feature = "config")]
pub mod config;
/// Oracle domain layer: aggregation strategies, price filters, and pipeline
/// for computing aggregated prices from multi-venue trade data.
pub mod domain;
/// Infrastructure layer: NATS subscriber and publisher implementations.
#[cfg(feature = "config")]
pub mod infrastructure;
