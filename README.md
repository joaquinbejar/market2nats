# Market Data Relay

Rust service that connects to public WebSocket feeds from multiple trading venues, normalizes market data (trades, ticker, L2 orderbook, funding rates, liquidations), and publishes it to NATS JetStream.

## Architecture

```
WebSocket Feeds ──► Venue Adapters ──► mpsc channels ──► Publisher ──► NATS JetStream
     (per venue)       (normalize)      (backpressure)    (serialize)    (persist)
```

**Layered DDD design:**

| Layer | Path | Responsibility |
|---|---|---|
| Domain | `src/domain/` | Pure types, enums, value objects. No I/O, no async. |
| Config | `src/config/` | TOML parsing, validation, env var substitution. |
| Application | `src/application/` | Orchestration: subscription manager, stream router, sequence tracker, health monitor. |
| Infrastructure | `src/infrastructure/` | WebSocket adapters, NATS publisher, HTTP health/metrics server. |
| Serialization | `src/serialization/` | Protobuf (prost) and JSON encoding for market data events. |

## NATS Subject Pattern

```
market.<venue_id>.<canonical_symbol_normalized>.<data_type>
```

Examples:
- `market.binance.btc-usdt.trade`
- `market.kraken.eth-usd.l2_orderbook`
- `market.binance.eth-usdt.liquidation`

## Market Data Types

- **Trade** — executed trades with price, quantity, side
- **Ticker** — best bid/ask and last price
- **L2 Orderbook** — depth snapshots and incremental deltas
- **Funding Rate** — perpetual futures funding rates
- **Liquidation** — forced liquidation events

## Configuration

All configuration is declarative via a single TOML file (`config/relay.toml`):

- Service settings (logging, shutdown timeout)
- NATS connection (URLs, auth, TLS)
- JetStream streams and consumers (storage, retention, ack policies)
- Venue definitions (WebSocket URLs, reconnect, circuit breaker)
- Subscriptions per venue (instruments, data types)

```bash
# Run with default config
cargo run

# Run with custom config
cargo run -- path/to/config.toml
```

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | JSON health status (per-venue state, NATS status, overall health) |
| `GET /metrics` | Prometheus metrics |

## Resilience

- **Exponential backoff** reconnection per venue with configurable delays
- **Circuit breaker** (Closed → Open → HalfOpen) per venue to avoid hammering down endpoints
- **Bounded channels** between layers for backpressure propagation
- **Publish retries** with 3 attempts (100ms → 500ms → 2s) before dropping
- **Graceful shutdown** with configurable drain timeout on SIGTERM/SIGINT

## Build

```bash
# Build
cargo build --release

# Test
cargo test --all-features

# Lint
cargo clippy --all-targets --all-features -- -D warnings

# Format
cargo +stable fmt --all
```

## Key Dependencies

| Crate | Purpose |
|---|---|
| `tokio` | Async runtime |
| `tokio-tungstenite` | WebSocket client |
| `async-nats` | NATS client with JetStream |
| `prost` | Protobuf serialization |
| `rust_decimal` | Decimal arithmetic (no f64 for prices) |
| `tracing` | Structured logging |
| `axum` | HTTP server for health/metrics |
| `dashmap` | Concurrent hash maps |
| `thiserror` | Typed error enums |

## License

Proprietary.
