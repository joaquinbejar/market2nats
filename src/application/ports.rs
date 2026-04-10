use std::future::Future;
use std::pin::Pin;

use crate::config::model::{ConsumerConfig, StreamConfig};
use crate::domain::{MarketDataEnvelope, MarketDataType, VenueId};

/// Errors from venue adapter operations.
#[derive(Debug, thiserror::Error)]
pub enum VenueError {
    /// WebSocket connection failure.
    #[error("connection failed for {venue}: {reason}")]
    ConnectionFailed { venue: String, reason: String },

    /// WebSocket message receive failure.
    #[error("receive failed for {venue}: {reason}")]
    ReceiveFailed { venue: String, reason: String },

    /// Subscription failure.
    #[error("subscribe failed for {venue}: {reason}")]
    SubscribeFailed { venue: String, reason: String },

    /// The adapter's circuit breaker is open.
    #[error("circuit breaker open for {venue}")]
    CircuitBreakerOpen { venue: String },

    /// Disconnection failure.
    #[error("disconnect failed for {venue}: {reason}")]
    DisconnectFailed { venue: String, reason: String },
}

/// Errors from NATS publisher operations.
#[derive(Debug, thiserror::Error)]
pub enum NatsError {
    /// Failed to connect to NATS.
    #[error("nats connection failed: {0}")]
    ConnectionFailed(String),

    /// Failed to publish a message.
    #[error("publish failed to {subject}: {reason}")]
    PublishFailed { subject: String, reason: String },

    /// Failed to create or update a stream.
    #[error("stream setup failed for {stream}: {reason}")]
    StreamSetupFailed { stream: String, reason: String },

    /// Failed to create or update a consumer.
    #[error("consumer setup failed for {consumer}: {reason}")]
    ConsumerSetupFailed { consumer: String, reason: String },

    /// Health check failed.
    #[error("nats health check failed: {0}")]
    HealthCheckFailed(String),
}

/// A subscription request for a venue adapter.
#[derive(Debug, Clone)]
pub struct Subscription {
    /// Venue-local instrument identifier.
    pub instrument: String,
    /// Canonical symbol for NATS subject naming.
    pub canonical_symbol: String,
    /// Data types to subscribe to.
    pub data_types: Vec<MarketDataType>,
}

/// Raw market data received from a venue, before normalization.
#[derive(Debug, Clone)]
pub struct RawMarketData {
    /// The raw JSON bytes from the WebSocket.
    pub payload: bytes::Bytes,
    /// When the message was received (epoch millis).
    pub received_at: u64,
}

/// Boxed future type alias for trait object safety.
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Trait for venue WebSocket adapters.
///
/// Each implementation handles connection, subscription, and message
/// deserialization for a specific venue. Uses boxed futures for object safety.
pub trait VenueAdapter: Send + 'static {
    /// Returns the venue identifier.
    fn venue_id(&self) -> &VenueId;

    /// Connects to the venue WebSocket.
    fn connect(&mut self) -> BoxFuture<'_, Result<(), VenueError>>;

    /// Subscribes to the given instruments and data types.
    fn subscribe(
        &mut self,
        subscriptions: &[Subscription],
    ) -> BoxFuture<'_, Result<(), VenueError>>;

    /// Receives the next normalized market data event(s) from the venue.
    ///
    /// May return multiple events if the venue packs multiple updates in one frame.
    fn next_events(&mut self) -> BoxFuture<'_, Result<Vec<MarketDataEnvelope>, VenueError>>;

    /// Disconnects from the venue.
    fn disconnect(&mut self) -> BoxFuture<'_, Result<(), VenueError>>;

    /// Whether the adapter is currently connected.
    fn is_connected(&self) -> bool;
}

/// Trait for publishing market data to NATS JetStream.
///
/// Uses boxed futures for object safety.
pub trait NatsPublisher: Send + Sync + 'static {
    /// Publishes a serialized message to the given NATS subject.
    fn publish(
        &self,
        subject: &str,
        payload: &[u8],
        content_type: &str,
    ) -> BoxFuture<'_, Result<(), NatsError>>;

    /// Ensures a JetStream stream exists with the given configuration.
    fn ensure_stream(&self, config: &StreamConfig) -> BoxFuture<'_, Result<(), NatsError>>;

    /// Ensures a JetStream consumer exists with the given configuration.
    fn ensure_consumer(&self, config: &ConsumerConfig) -> BoxFuture<'_, Result<(), NatsError>>;

    /// Performs a health check against NATS.
    fn health_check(&self) -> BoxFuture<'_, Result<(), NatsError>>;
}
