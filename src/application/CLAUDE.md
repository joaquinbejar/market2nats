# Application Layer
 
Orchestration logic — connects domain types with infrastructure through trait ports.
 
## Rules
 
- May import from `domain/` only — never from `infrastructure/` directly
- Defines trait ports that infrastructure implements (VenueAdapter, NatsPublisher)
- Owns async task spawning and channel management
- Uses `tokio::sync::mpsc` bounded channels for backpressure
- Uses `DashMap` for concurrent state (SequenceTracker)
 
## Files
 
- `ports.rs` — VenueAdapter and NatsPublisher trait definitions
- `subscription_manager.rs` — spawns per-venue tasks, manages lifecycle
- `stream_router.rs` — maps MarketDataEnvelope → NATS subject string
- `sequence_tracker.rs` — DashMap<(VenueId, InstrumentId, MarketDataType), AtomicU64>
- `health_monitor.rs` — tracks ConnectionState per venue, overall service health
- `mod.rs` — re-exports
 
## Channel Architecture
 
```
Venue task ──► mpsc(bounded) ──► Normalizer ──► mpsc(bounded) ──► Publisher task
```
 
- Per-venue inbound channel: ~10,000 capacity
- Publisher channel: ~50,000 capacity
- Bounded channels provide backpressure up to WebSocket read pause
 
## Subject Pattern Resolution
 
`stream_router.rs` takes a MarketDataEnvelope and produces:
`market.{venue_id}.{canonical_symbol_normalized}.{data_type}`
 
Where canonical_symbol is lowercased and `/` → `-` (BTC/USDT → btc-usdt).
