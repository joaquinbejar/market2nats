use thiserror::Error;

/// Domain-layer errors — pure, no I/O, no infrastructure.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum DomainError {
    /// A required field was empty or whitespace-only.
    #[error("empty field: {field}")]
    EmptyField { field: &'static str },

    /// A numeric value was negative where only non-negative is valid.
    #[error("negative value for {field}: {value}")]
    NegativeValue { field: &'static str, value: String },

    /// Arithmetic overflow or underflow in checked operations.
    #[error("arithmetic overflow: {operation}")]
    ArithmeticOverflow { operation: String },

    /// An unknown or unsupported market data type string was encountered.
    #[error("unknown market data type: {0}")]
    UnknownMarketDataType(String),

    /// An unknown or unsupported side string was encountered.
    #[error("unknown side: {0}")]
    UnknownSide(String),

    /// A sequence number operation failed (e.g., overflow).
    #[error("sequence error: {0}")]
    SequenceError(String),

    /// Invalid symbol format.
    #[error("invalid symbol: {0}")]
    InvalidSymbol(String),
}
