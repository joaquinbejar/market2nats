use thiserror::Error;

/// Errors produced by oracle domain computations.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum OracleError {
    /// Too few price sources to compute a reliable aggregate.
    #[error("insufficient sources: required {required}, available {available}")]
    InsufficientSources {
        /// Minimum number of sources required.
        required: usize,
        /// Number of sources actually available.
        available: usize,
    },

    /// Every price source was discarded (stale, outlier, etc.).
    #[error("all sources stale for {symbol}")]
    AllSourcesStale {
        /// The symbol for which all sources were stale.
        symbol: String,
    },

    /// A checked arithmetic operation overflowed.
    #[error("arithmetic overflow: {operation}")]
    ArithmeticOverflow {
        /// Description of the operation that overflowed.
        operation: String,
    },

    /// A division by zero was attempted.
    #[error("division by zero: {context}")]
    DivisionByZero {
        /// Description of the context where division by zero occurred.
        context: String,
    },

    /// An unrecognized aggregation strategy name was provided.
    #[error("unknown strategy: {name}")]
    UnknownStrategy {
        /// The unrecognized strategy name.
        name: String,
    },

    /// An error propagated from the underlying domain layer.
    #[error("domain error: {0}")]
    Domain(#[from] market2nats_domain::DomainError),

    /// NATS operation failed.
    #[error("nats error: {0}")]
    Nats(String),

    /// Serialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl OracleError {
    /// Creates an `ArithmeticOverflow` error.
    #[cold]
    #[inline(never)]
    #[must_use]
    pub fn arithmetic_overflow(operation: impl Into<String>) -> Self {
        Self::ArithmeticOverflow {
            operation: operation.into(),
        }
    }

    /// Creates a `DivisionByZero` error.
    #[cold]
    #[inline(never)]
    #[must_use]
    pub fn division_by_zero(context: impl Into<String>) -> Self {
        Self::DivisionByZero {
            context: context.into(),
        }
    }

    /// Creates an `InsufficientSources` error.
    #[cold]
    #[inline(never)]
    #[must_use]
    pub fn insufficient_sources(required: usize, available: usize) -> Self {
        Self::InsufficientSources {
            required,
            available,
        }
    }

    /// Creates an `UnknownStrategy` error.
    #[cold]
    #[inline(never)]
    #[must_use]
    pub fn unknown_strategy(name: impl Into<String>) -> Self {
        Self::UnknownStrategy { name: name.into() }
    }

    /// Creates an `AllSourcesStale` error.
    #[cold]
    #[inline(never)]
    #[must_use]
    pub fn all_sources_stale(symbol: impl Into<String>) -> Self {
        Self::AllSourcesStale {
            symbol: symbol.into(),
        }
    }

    /// Creates a `Nats` error.
    #[cold]
    #[inline(never)]
    #[must_use]
    pub fn nats(message: impl Into<String>) -> Self {
        Self::Nats(message.into())
    }

    /// Creates a `Serialization` error.
    #[cold]
    #[inline(never)]
    #[must_use]
    pub fn serialization(message: impl Into<String>) -> Self {
        Self::Serialization(message.into())
    }

    /// Returns a static label identifying the error variant, suitable for metrics.
    #[inline]
    #[must_use]
    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::InsufficientSources { .. } => "insufficient_sources",
            Self::AllSourcesStale { .. } => "all_sources_stale",
            Self::ArithmeticOverflow { .. } => "arithmetic_overflow",
            Self::DivisionByZero { .. } => "division_by_zero",
            Self::UnknownStrategy { .. } => "unknown_strategy",
            Self::Domain(_) => "domain",
            Self::Nats(_) => "nats",
            Self::Serialization(_) => "serialization",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kind_label_covers_all_variants() {
        let cases: Vec<(OracleError, &str)> = vec![
            (
                OracleError::insufficient_sources(3, 1),
                "insufficient_sources",
            ),
            (
                OracleError::all_sources_stale("BTC/USDT"),
                "all_sources_stale",
            ),
            (
                OracleError::arithmetic_overflow("mul"),
                "arithmetic_overflow",
            ),
            (OracleError::division_by_zero("spread"), "division_by_zero"),
            (OracleError::unknown_strategy("magic"), "unknown_strategy"),
            (
                OracleError::Domain(market2nats_domain::DomainError::EmptyField {
                    field: "symbol",
                }),
                "domain",
            ),
            (OracleError::nats("timeout"), "nats"),
            (OracleError::serialization("bad json"), "serialization"),
        ];
        for (err, expected) in cases {
            assert_eq!(err.kind_label(), expected);
        }
    }
}
