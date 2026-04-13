/// Oracle configuration loading, parsing, validation, and pipeline builder.
#[cfg(feature = "config")]
pub mod config;
/// Oracle domain layer: aggregation strategies, price filters, and pipeline
/// for computing aggregated prices from multi-venue trade data.
pub mod domain;
