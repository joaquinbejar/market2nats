# market2nats

A Rust workspace with two services for normalized cryptocurrency market data:

- **[`market2nats`](./docs/market2nats.md)** — connects to public WebSocket
  feeds from a configurable set of trading venues, normalizes every event
  (trade, ticker, L2 orderbook, funding rate, liquidation) to a single
  envelope, and republishes it to NATS JetStream.
- **[`oracle`](./docs/oracle.md)** — subscribes to the trade subjects produced
  by `market2nats`, runs a configurable aggregation pipeline (median, TWAP,
  VWAP, median-filtered) per canonical symbol, and publishes the resulting
  oracle price to NATS **and** (optionally) to live WebSocket clients.

Both services are stateless, single-binary, and configured via a single TOML
file each.

## Repository layout

```
crates/
  market2nats-domain/        Shared domain newtypes (Price, Quantity, …)
  market2nats/               Relay service (WebSocket → NATS JetStream)
  oracle/                    Aggregator service (NATS trades → oracle price)
config/                      Reference TOML configs
docs/
  market2nats.md             Relay service — architecture, config, metrics
  oracle.md                  Oracle service — pipeline, WebSocket protocol, TLS
  wire-format.md             NATS wire contract (single source of truth)
Docker/
  docker-compose.yml         NATS + both services with public images
  Dockerfile                 Multi-stage build for the relay
  oracle.Dockerfile          Multi-stage build for the oracle
.github/workflows/           Multi-arch (amd64, arm64) CI builds
rules/global_rules.md        Binding coding rules (DDD, no f64, checked math, …)
```

## Quickstart

### 1. Bring up NATS

```bash
docker compose -f Docker/docker-compose.yml up -d nats
```

NATS is exposed on `localhost:4222` (auth `nats / AS09.1qa` per the compose
file) and management on `localhost:8222`.

### 2. Run the relay

```bash
NATS_URL='nats://nats:AS09.1qa@localhost:4222' \
  cargo run --release -p market2nats -- config/relay.all-spot-trades.toml
```

This subscribes to BTC/USDT and ETH/USDT trades on every supported spot
venue (Binance, Bitstamp, Bybit, Coinbase, Crypto.com, Gate.io, Kraken,
OKX) and publishes them to `market.<venue>.<symbol>.trade`.

### 3. Run the oracle

```bash
NATS_URL='nats://nats:AS09.1qa@localhost:4222' \
  cargo run --release -p oracle -- config/oracle.toml
```

The oracle aggregates the trade stream and publishes one price per symbol
per second on `oracle.<symbol_normalized>` (e.g. `oracle.btc-usdt`). With
the default config it also opens a WebSocket server on `:9092`.

### 4. Subscribe over WebSocket

```bash
# Plain ws://
websocat ws://localhost:9092
> {"action":"subscribe","symbols":["BTC/USDT"]}

# Or get every symbol the oracle knows
> {"action":"subscribe","symbols":["all"]}

# Drop a subscription
> {"action":"unsubscribe","symbols":["ETH/USDT"]}
```

For `wss://` setup with a self-signed cert and full WebSocket protocol
details see [`docs/oracle.md`](./docs/oracle.md#tls-wss).

### 5. With Docker (public images)

```bash
docker compose -f Docker/docker-compose.yml up -d
```

This pulls `ghcr.io/joaquinbejar/market2nats:latest` and
`ghcr.io/joaquinbejar/oracle:latest` (multi-arch, amd64 + arm64) and starts
NATS, the relay (using `relay.all-spot-trades.toml`), and the oracle.

## Architecture in one picture

```
            ┌────────────────────────────────────────────────────────────────┐
            │                       market2nats relay                       │
WebSocket ──►   GenericWsAdapter ──► mpsc ──► Pipeline ──► NatsPublisher    │
(per venue) │       (parse,         (back-     (sequence,    (3 retries)    │
            │       normalize)      pressure)  fan-out)                     │
            └─────────────────┬──────────────────────────────────────────────┘
                              │
                              ▼
                  market.<venue>.<symbol>.<type>
                              │
                              ▼
            ┌────────────────────────────────────────────────────────────────┐
            │                          oracle                               │
            │  NatsTradeSubscriber ──► OracleService (1s tick)              │
            │      (filter,            (staleness, outlier, strategy)       │
            │      bookkeep)                       │                        │
            │                                       ▼                        │
            │                         FanOutPublisher                        │
            │                          ├─► OraclePricePublisher (NATS)      │
            │                          └─► OracleWsServer (ws:// / wss://)  │
            └─────────────────┬──────────────────────────────────────────────┘
                              │                                       │
                              ▼                                       ▼
                 oracle.<symbol_normalized>           ws://host:9092 (clients)
```

The full data envelope is locked by [`docs/wire-format.md`](./docs/wire-format.md).
Every adapter must produce events that match it; consumers depend on it.

## Coding rules

Binding rules live in [`rules/global_rules.md`](./rules/global_rules.md). The
short version:

- **DDD** — `domain → application → infrastructure`. Domain has zero async,
  zero I/O, zero infra imports.
- `rust_decimal::Decimal` for every monetary value — never `f64`.
- **Checked arithmetic only** — `checked_add` / `checked_sub` / `checked_mul`
  / `checked_div`. No `saturating_*`, no `wrapping_*`, no raw operators on
  decimals.
- `thiserror` enums per layer. No `anyhow`.
- `tracing` only for logs. Never `println!`, `eprintln!`, `dbg!`, `log`.
- Newtypes at every boundary (`VenueId`, `InstrumentId`, `CanonicalSymbol`,
  `Price`, `Quantity`, `Sequence`, `Timestamp`).
- **Zero `.unwrap()` / `.expect()`** in production code. Pattern match or
  `.ok_or_else()`.

## Build & test

```bash
# Whole workspace
cargo build --release
cargo test --all-features

# One crate at a time
cargo build --release -p market2nats
cargo build --release -p oracle

# Pre-push (clippy + fmt + tests + docs)
make pre-push
```

The pre-push lane runs the same checks CI does — clippy with `-D warnings`,
`cargo fmt --check`, `cargo test --all-features`, and `cargo build --release`.

## Endpoints

Both services expose health and metrics on HTTP:

| Service | Port | Endpoints |
|---|---|---|
| `market2nats` | `8080` (default) | `GET /health`, `GET /metrics` |
| `oracle` | `9091` (default) | `GET /health`, `GET /metrics` |
| `oracle` (WebSocket) | `9092` (default) | `ws://` or `wss://` for live oracle prices |

Quick checks:

```bash
# Relay
curl -s http://localhost:8080/health  | jq
curl -s http://localhost:8080/metrics | grep '^market2nats_'

# Oracle
curl -s http://localhost:9091/health  | jq
curl -s http://localhost:9091/metrics | grep '^oracle_'
```

Detailed metric tables and more `curl` examples (per-venue counters,
WebSocket gauges, scriptable probes) are in the per-service docs.

## Key dependencies

| Crate | Used by | Purpose |
|---|---|---|
| `tokio` | both | Async runtime |
| `tokio-tungstenite` | both | WebSocket (client in relay, server in oracle) |
| `tokio-rustls` + `rustls-pemfile` | oracle | TLS termination for the WebSocket server |
| `async-nats` | both | NATS client (JetStream in the relay) |
| `prost` | relay | Protobuf serialization |
| `serde_json` | both | JSON serialization |
| `rust_decimal` | both | Decimal arithmetic (no f64 for prices) |
| `tracing` | both | Structured logging |
| `axum` | both | HTTP server for `/health` and `/metrics` |
| `dashmap` | both | Concurrent hash maps |
| `metrics` + `metrics-exporter-prometheus` | both | Prometheus instrumentation |
| `thiserror` | both | Typed error enums |

## Contact

If you have questions, issues, or would like to provide feedback, please
contact the project maintainer:

- **Author**: Joaquín Béjar García
- **Email**: jb@taunais.com
- **Telegram**: [@joaquin_bejar](https://t.me/joaquin_bejar)
- **Repository**: <https://github.com/joaquinbejar/market2nats>
- **Documentation**: <https://docs.rs/market2nats>

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE)
file for details.
