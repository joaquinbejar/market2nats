pub mod json;
pub mod proto;

use crate::domain::MarketDataEnvelope;

/// Serialization format for market data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// Protobuf (compact binary).
    Protobuf,
    /// JSON (human-readable).
    Json,
}

/// Serialization errors.
#[derive(Debug, thiserror::Error)]
pub enum SerializeError {
    #[error("protobuf encode: {0}")]
    Protobuf(#[from] proto::EncodeError),
    #[error("json encode: {0}")]
    Json(#[from] json::EncodeError),
}

/// Serializes a market data envelope in the given format.
///
/// # Errors
///
/// Returns `SerializeError` if encoding fails.
pub fn serialize_envelope(
    envelope: &MarketDataEnvelope,
    format: SerializationFormat,
) -> Result<Vec<u8>, SerializeError> {
    match format {
        SerializationFormat::Protobuf => Ok(proto::encode_envelope(envelope)?),
        SerializationFormat::Json => Ok(json::encode_envelope(envelope)?),
    }
}

/// Returns the Content-Type header value for a given format.
#[must_use]
#[inline]
pub fn content_type(format: SerializationFormat) -> &'static str {
    match format {
        SerializationFormat::Protobuf => "application/protobuf",
        SerializationFormat::Json => "application/json",
    }
}
