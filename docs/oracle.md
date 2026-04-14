# Oracle service

The **oracle** crate is a stateless aggregator that turns raw trade messages
published by `market2nats` into a single, tamper-resistant reference price per
canonical symbol. It runs on a periodic tick (default 1 second), recomputes
each symbol from the most recent trades it has seen across all venues, and
publishes the result to:

1. **NATS** — under `oracle.<symbol_normalized>` (always on).
2. **WebSocket clients** — `ws://` or `wss://`, opt-in per `[websocket]` config.

It does **not** persist state. Every restart begins with empty source buffers
and warms up over the staleness window.

## Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │                  Oracle                       │
NATS               │                                              │
market.*.<sym>.*  ─►│  NatsTradeSubscriber                         │
                    │      │ DashMap<Symbol, Vec<PriceSource>>     │
                    │      ▼                                       │
                    │  OracleService (periodic tick)               │
                    │      │ pipeline.compute(sym, sources)        │
                    │      ▼                                       │
                    │  FanOutPublisher                              │
                    │     ├─► OraclePricePublisher (NATS)          │
                    │     └─► OracleWsServer (broadcast → clients) │
                    └──────────────────────────────────────────────┘
                                       │
                                       ▼
                          oracle.<symbol_normalized>  (NATS)
                          ws://host:9092               (WebSocket)
```

DDD layout (binding):

| Layer | Path | Responsibility |
|---|---|---|
| Domain | `crates/oracle/src/domain/` | Pure types, errors, aggregation strategies (median, TWAP, VWAP, median-filtered). No I/O, no async. |
| Config | `crates/oracle/src/config/` | TOML model + validation + env-var substitution (`${VAR}`, `${VAR:-default}`). |
| Application | `crates/oracle/src/application/` | `OracleService`, `OracleHealthMonitor`, metric constants, `OraclePublisher` / `TradeSource` trait ports. |
| Infrastructure | `crates/oracle/src/infrastructure/` | `NatsTradeSubscriber`, `OraclePricePublisher` (NATS), `FanOutPublisher`, `OracleWsServer` (WebSocket + TLS). |

The `OracleService` only knows about traits — it has no idea whether a publish
goes to NATS, a WebSocket, both, or neither. `FanOutPublisher` makes that
composition trivial in `main.rs`.

## Aggregation pipeline

`PipelineConfig` (TOML `[pipeline]`) controls four stages applied in order:

1. **Staleness filter** — drop sources older than `staleness_max_ms`.
2. **Outlier filter** (`median_filtered` strategy only) — drop sources whose
   price deviates from the median by more than `outlier_max_deviation_bps`.
3. **Min-sources gate** — if fewer than `min_sources` survive, emit
   `OracleError::InsufficientSources` and skip publishing.
4. **Aggregation strategy** — one of:
   - `median` — robust against single-venue spikes.
   - `twap` — time-weighted average over `twap_window_ms`.
   - `vwap` — volume-weighted average.
   - `median_filtered` — median + outlier filter (default and recommended).

Confidence levels are derived from `(num_sources, spread_bps)`:

| Sources | Spread (bps) | Confidence |
|---|---|---|
| ≥ 7 | < 10 | `HIGH` |
| ≥ 4 | < 50 | `MEDIUM` |
| else | — | `LOW` |

## Configuration reference

Full example: `config/oracle.toml`. Required sections:

```toml
[service]
name       = "oracle"
log_level  = "info"          # error | warn | info | debug | trace
log_format = "json"          # json | pretty
http_port  = 9091             # /health and /metrics

[nats]
urls = ["${NATS_URL:-nats://localhost:4222}"]
# Auth: "none" (default), "token", or "userpass". Credentials embedded in the
# URL (nats://user:pass@host) take priority over explicit fields.

[[subscriptions]]
symbol   = "BTC/USDT"
subjects = [
  "market.*.btc-usdt.trade",
  "market.*.btc-usd.trade",
]

[[subscriptions]]
symbol   = "ETH/USDT"
subjects = [
  "market.*.eth-usdt.trade",
  "market.*.eth-usd.trade",
]

[pipeline]
strategy                  = "median_filtered"
staleness_max_ms          = 10000
outlier_max_deviation_bps = 100
min_sources               = 1
twap_window_ms            = 30000

[publish]
subject_pattern     = "oracle.<symbol_normalized>"   # placeholder mandatory
format              = "json"
publish_interval_ms = 1000

[websocket]
enabled = true
port    = 9092
path    = "/"
# Optional TLS — see the WebSocket TLS section below.
# tls_enabled   = false
# tls_cert_file = "/etc/oracle/tls/server.crt"
# tls_key_file  = "/etc/oracle/tls/server.key"
```

All structs use `#[serde(deny_unknown_fields)]`, so a typo in any field
fails fast at startup.

### Environment variables

Anywhere a string appears in the TOML you can use:
- `${VAR}` — substituted with the env var value, or left as-is if unset.
- `${VAR:-default}` — substituted, or replaced by `default` if the env var is
  unset.

Common pattern:

```toml
[nats]
urls = ["${NATS_URL:-nats://localhost:4222}"]
```

## Subscription wildcards

`subjects` accepts NATS wildcards. `market.*.btc-usdt.trade` matches every
venue’s `BTC/USDT` trade subject (`market.binance.btc-usdt.trade`,
`market.kraken.btc-usdt.trade`, …) without listing them all. The oracle
deduplicates per `(venue, symbol)` so the same venue cannot inflate the
source count.

## Output: NATS

Each tick the oracle publishes a JSON envelope per symbol to the subject
resolved from `subject_pattern`. With the default `oracle.<symbol_normalized>`
the topics are `oracle.btc-usdt`, `oracle.eth-usdt`, etc.

```json
{
  "symbol": "BTC/USDT",
  "price": "74694.15",
  "timestamp": 1776151250139,
  "sources": [
    {"venue":"coinbase","price":"74723.32","quantity":"0.000066","timestamp":1776151250135,"age_ms":4},
    {"venue":"kraken","price":"74716.20","quantity":"0.0003384","timestamp":1776151247116,"age_ms":3023}
  ],
  "strategy": "MEDIANFILTERED",
  "confidence": "HIGH"
}
```

All decimal fields are JSON strings to preserve `rust_decimal::Decimal`
precision. Consumers must parse them as decimals, not floats.

## Output: WebSocket (`[websocket]`)

When `enabled = true` the service exposes a WebSocket server on `port`/`path`.
Each connected client maintains its own subscription filter and only receives
prices for symbols it has subscribed to.

### Wire protocol

Client → server (JSON text frames):

```jsonc
// Subscribe to specific symbols (any of these forms work — server normalizes):
//   "BTC/USDT", "btc/usdt", "BTC-USDT", "btc-usdt"
{"action":"subscribe","symbols":["BTC/USDT","ETH/USDT"]}

// Subscribe to every symbol the oracle knows about:
{"action":"subscribe","symbols":["all"]}

// Drop one or more subscriptions:
{"action":"unsubscribe","symbols":["ETH/USDT"]}

// Drop the wildcard:
{"action":"unsubscribe","symbols":["all"]}
```

Server → client: every matching tick is forwarded as a text frame containing
the same JSON envelope published to NATS. There is no acknowledgement frame
for subscribe/unsubscribe — just send and trust.

A client that lags more than `BROADCAST_CAPACITY` (256) ticks behind is
disconnected with a `client lagged behind broadcast` warning. This is a
deliberate back-pressure policy: a slow consumer must reconnect rather than
hold up everyone else.

### TLS (`wss://`)

Set `tls_enabled = true` together with PEM cert and key files:

```toml
[websocket]
enabled       = true
port          = 9093
path          = "/"
tls_enabled   = true
tls_cert_file = "/etc/oracle/tls/server.crt"
tls_key_file  = "/etc/oracle/tls/server.key"
```

Implementation notes:

- TLS is terminated with `tokio-rustls` (rustls + ring crypto provider).
- The cert file may contain a chain (multiple `BEGIN CERTIFICATE` blocks).
- The key file accepts PKCS#8, RSA (`BEGIN RSA PRIVATE KEY`), or EC private
  keys in PEM form.
- Validation rejects the config at startup if `tls_enabled = true` and either
  file is missing or unreadable.
- Self-signed dev cert example:
  ```bash
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout server.key -out server.crt -days 30 \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
  ```

### Quick test with `websocat`

```bash
# Plain ws://
websocat ws://localhost:9092
> {"action":"subscribe","symbols":["BTC/USDT"]}

# wss:// (skip cert verification for self-signed dev cert)
websocat -k wss://localhost:9093
> {"action":"subscribe","symbols":["all"]}
```

## HTTP endpoints (`http_port`)

| Endpoint | Description |
|---|---|
| `GET /health` | JSON status. Returns `200` when overall health is `healthy`, else `503`. Includes per-symbol staleness and (when enabled) the WebSocket connected-client count. |
| `GET /metrics` | Prometheus exposition. |

`/health` shape:

```json
{
  "status": "healthy",
  "symbols": [
    {"symbol":"BTC/USDT","staleness_ms":250,"healthy":true},
    {"symbol":"ETH/USDT","staleness_ms":270,"healthy":true}
  ],
  "websocket": {"enabled": true, "connected_clients": 3}
}
```

The `websocket` field is `null` when `[websocket].enabled = false`.

## Metrics reference

All metrics are exposed on `GET /metrics` in Prometheus text format.

| Name | Type | Labels | Description |
|---|---|---|---|
| `oracle_price_computed_total` | counter | `symbol`, `strategy` | Successful publishes per symbol. |
| `oracle_publish_errors_total` | counter | `symbol` | Publishes that failed downstream (NATS or WS). |
| `oracle_computation_errors_total` | counter | `symbol`, `error_kind` | Pipeline failures (insufficient sources, all stale, arithmetic). |
| `oracle_sources_count` | gauge | `symbol` | Number of sources at the start of the last tick. |
| `oracle_price_spread_bps` | gauge | `symbol` | Min↔max spread of the sources used in the last successful publish. |
| `oracle_computation_latency_ms` | histogram | `symbol` | End-to-end pipeline latency. |
| `oracle_trade_messages_received_total` | counter | `venue`, `instrument` | Trade messages consumed from NATS, broken down per venue and instrument. |
| `oracle_ws_connected_clients` | gauge | — | Currently open WebSocket clients. |
| `oracle_ws_messages_sent_total` | counter | `symbol` | WebSocket frames forwarded to clients. |

## Run

```bash
# With NATS at localhost:4222 (no auth)
cargo run --release -p oracle -- config/oracle.toml

# With env-substituted NATS URL
NATS_URL='nats://nats:secret@localhost:4222' \
  cargo run --release -p oracle -- config/oracle.toml

# Or via env-var path indirection
ORACLE_CONFIG=config/oracle.toml cargo run --release -p oracle
```

Graceful shutdown on `SIGINT` / `SIGTERM`: the service interrupts the
publication tick, lets in-flight publishes finish (5-second timeout), then
closes WebSocket connections and exits.
