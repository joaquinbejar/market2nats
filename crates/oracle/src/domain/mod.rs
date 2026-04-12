/// Oracle domain errors.
pub mod error;
/// Price filters for pre-processing sources before aggregation.
pub mod filters;
/// The oracle pipeline that composes filters and strategies.
pub mod pipeline;
/// Aggregation strategies for computing prices from multiple sources.
pub mod strategies;
/// Core oracle types: `PriceSource`, `OracleConfidence`, `OraclePrice`.
pub mod types;

pub use error::OracleError;
pub use filters::{OutlierFilter, PriceFilter, StalenessFilter};
pub use pipeline::OraclePipeline;
pub use strategies::{
    AggregationStrategy, AggregationStrategyKind, MedianFilteredStrategy, MedianStrategy,
    TwapStrategy, VwapStrategy,
};
pub use types::{OracleConfidence, OraclePrice, PriceSource};
