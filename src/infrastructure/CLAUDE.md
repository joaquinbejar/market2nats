# Infrastructure Layer
 
Concrete implementations of application-layer trait ports + external I/O.
 
## Rules
 
- Implements traits defined in `application/ports.rs`
- May import from `domain/` and `application/`
- All I/O is async via Tokio
- Uses `tokio-tungstenite` for WebSocket
- Uses `async-nats` for NATS JetStream
- Uses `axum` for HTTP health/metrics
- Circuit breaker pattern for venue connections
- Exponential backoff on reconnection
- `tracing::instrument` on all async functions
 
## Subdirectories
 
### ws/ — WebSocket venue adapters
- `adapter.rs` — re-export of VenueAdapter trait from application
- `generic.rs` — GenericWsAdapter: template-based, configured entirely via TOML
- `circuit_breaker.rs` — Closed → Open → HalfOpen state machine
 
### nats/ — NATS JetStream
- `publisher.rs` — JetStreamPublisher implementing NatsPublisher trait
- `setup.rs` — stream and consumer creation/reconciliation at startup
 
### http/ — observability
- `health.rs` — GET /health (JSON status) and GET /metrics (Prometheus)
 
## Circuit Breaker Config (per venue)
 
```
failure_threshold: u32     — consecutive failures to open
reset_timeout_secs: u64    — wait before half-open probe
half_open_max_requests: u32 — probes before closing again
```
 
## NATS Publish Retry
 
3 attempts: 100ms → 500ms → 2000ms. After exhaustion, log ERROR and drop.
Never buffer unboundedly — that's a memory leak.
