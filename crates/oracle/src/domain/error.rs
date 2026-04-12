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
}
