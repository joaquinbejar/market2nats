# market2nats relay

`market2nats` is a Rust service that maintains long-lived WebSocket
connections to public market-data feeds from a configurable set of trading
venues, normalizes every event to a single canonical envelope, and republishes
it on NATS JetStream. It is the **producer** half of the system; the
[`oracle`](./oracle.md) crate is one consumer.

The full **NATS wire contract** lives in [`docs/wire-format.md`](./wire-format.md).
This document covers the service itself: architecture, configuration, the
generic WebSocket adapter, observability, and operational notes.

## Architecture

```
                                                        ┌────────────────────────────┐
   Venue WebSocket  ───► VenueAdapter (parse/normalize) │                            │
        ...        ───►        │                        │  ┌─► subject:               │
                              ▼                         │  │   market.<venue>.       │
                       mpsc(bounded ~10k)               │  │   <symbol>.<type>       │
                              │                         │  │                          │
                              ▼                         │  └─► JetStream             │
                       Pipeline (sequence,              │     (durable, replayable)  │
                        sequence_tracker)               │                            │
                              │                         └────────────────────────────┘
                              ▼
                       mpsc(bounded ~50k)
                              │
                              ▼
                       NatsPublisher  ─────────────────────►  NATS JetStream
```

Layered DDD design (binding):

| Layer | Path | Responsibility |
|---|---|---|
| Domain | `crates/market2nats/src/domain/` | Pure types (`VenueId`, `InstrumentId`, `CanonicalSymbol`, `Price`, `Quantity`, `Sequence`), enums, error variants. No I/O, no async. |
| Config | `crates/market2nats/src/config/` | TOML parsing, schema validation, env-var substitution. |
| Application | `crates/market2nats/src/application/` | Trait ports (`VenueAdapter`, `NatsPublisher`), `SubscriptionManager`, `StreamRouter`, `SequenceTracker`, `HealthMonitor`. |
| Infrastructure | `crates/market2nats/src/infrastructure/` | `GenericWsAdapter`, NATS publisher, JetStream stream/consumer setup, HTTP health/metrics server. |
| Serialization | `crates/market2nats/src/serialization/` | JSON (default) and Protobuf encoders for `MarketDataEnvelope`. |

Domain imports nothing from application or infrastructure. Application imports
only domain and trait ports — never an infra type. Infrastructure is the
*only* layer where third-party I/O crates (`tokio-tungstenite`, `async-nats`,
`axum`) appear.

## Subject pattern

```
market.<venue_id>.<canonical_symbol_normalized>.<data_type>
```

- `venue_id` — lowercase venue identifier (`binance`, `kraken`, `okx`, …).
- `canonical_symbol_normalized` — canonical symbol lowercased with `/` replaced
  by `-`. `BTC/USDT` → `btc-usdt`.
- `data_type` — one of `trade`, `ticker`, `l2_orderbook`, `funding_rate`,
  `liquidation`.

Examples:
- `market.binance.btc-usdt.trade`
- `market.kraken.eth-usd.l2_orderbook`
- `market.okx.btc-usdt.funding_rate`

The subject is rendered in `application::stream_router::resolve_subject`. The
domain-side `CanonicalSymbol::normalized()` enforces the lowercasing rule.

## Configuration

A single TOML file describes everything. Common templates live in `config/`:

- `config/relay.all-spot-trades.toml` — every supported spot venue, BTC + ETH
  trades only.
- `config/relay.binance.toml` — single-venue example (Binance).
- Per-venue branches (`venue/<name>`) carry venue-specific configs.

Top-level sections:

```toml
[service]
name = "market2nats-all-spot-trades"
log_level = "info"               # error | warn | info | debug | trace
log_format = "json"              # json | pretty
shutdown_timeout_ms = 5000

[nats]
urls = ["${NATS_URL:-nats://localhost:4222}"]
connect_timeout_ms = 5000
reconnect_buffer_size = 8388608
max_reconnects = -1               # -1 = unlimited
ping_interval_secs = 20
publish_ack_timeout_ms = 10000    # per-publish JetStream PubAck wait;
                                  # raise on slow brokers, lower to fail
                                  # fast when the stream is not bound

[nats.tls]
enabled = false
# When true, also set:
# ca_file   = "/path/to/ca.pem"
# cert_file = "/path/to/client.crt"
# key_file  = "/path/to/client.key"

[[nats.streams]]
name        = "MARKET_TRADES"
subjects    = ["market.*.*.trade"]
storage     = "file"               # file | memory
retention   = "limits"             # limits | interest | workqueue
max_age_secs = 86400
max_bytes    = 1073741824          # 1 GiB
max_msg_size = 65536
discard      = "old"               # old | new
num_replicas = 1
duplicate_window_secs = 120
```

NATS authentication mirrors the oracle: credentials embedded in the URL
(`nats://user:pass@host:4222`) take priority over explicit fields. Use
`${VAR}` / `${VAR:-default}` env-var substitution anywhere in the file.

### Venues

Each venue is one `[[venues]]` block:

```toml
[[venues]]
id      = "kraken"
adapter = "generic_ws"            # only adapter currently shipped
enabled = true

[venues.connection]
ws_url                 = "wss://ws.kraken.com/v2"
reconnect_delay_ms     = 1000     # initial backoff
max_reconnect_delay_ms = 60000    # cap of exponential backoff
max_reconnect_attempts = 0        # 0 = unlimited
ping_interval_secs     = 30
pong_timeout_secs      = 10

[venues.circuit_breaker]
failure_threshold      = 5         # consecutive failures to trip Open
reset_timeout_secs     = 60        # Open → HalfOpen
half_open_max_requests = 2

[venues.generic_ws]
subscribe_mode      = "per_channel"
subscribe_template  = '{"method":"subscribe","params":{"channel":"${channel}","symbol":${instruments}}}'
message_format      = "json"

[venues.generic_ws.channel_map]
trade = "trade"

[[venues.subscriptions]]
instrument        = "BTC/USD"
canonical_symbol  = "BTC/USD"
data_types        = ["trade"]

[[venues.subscriptions]]
instrument        = "ETH/USD"
canonical_symbol  = "ETH/USD"
data_types        = ["trade"]
```

Disable a venue without removing it: `enabled = false`.

## The `generic_ws` adapter

`GenericWsAdapter` is a single, table-driven implementation that covers every
venue currently shipped. Differences between venues are expressed entirely as
configuration — no per-venue Rust code.

### Subscribe modes

`subscribe_mode` controls how subscription frames are built from the
`[[venues.subscriptions]]` list and `channel_map`:

| Mode | When to use | Template fields used |
|---|---|---|
| `per_pair` (default) | Venue accepts one stream identifier per data-type/instrument pair. | `subscribe_template` (rendered once per pair) **or** `batch_subscribe_template` with `${params}` (rendered once with the joined array). |
| `per_channel` | Venue groups all instruments under a single channel per frame (`{"channel":"trade","symbol":["BTC/USD","ETH/USD"]}`). | `subscribe_template` with `${channel}` and `${instruments}` placeholders — rendered once per channel. |
| `products_channels` | Coinbase-style: one frame, two arrays — products and channels. | `batch_subscribe_template` with `${instruments}` and `${channels}` — rendered once. |

### Template placeholders

| Placeholder | Substituted by |
|---|---|
| `${channel}` | The venue-side channel name from `channel_map` (e.g. `trade` → `publicTrade` for Bybit). |
| `${instrument}` | The venue-local instrument id (e.g. `BTCUSDT`, `BTC-USD`). |
| `${instruments}` | A JSON array of instrument ids. |
| `${channels}` | A JSON array of venue channel names. |
| `${params}` | A JSON array of stream identifiers as rendered by `stream_format`. |

`stream_format` describes what one stream identifier looks like — the adapter
substitutes `${channel}` and `${instrument}` and uses the result inside
`${params}`. Common shapes:

- `"${channel}_${instrument}"` — Bitstamp (`live_trades_btcusdt`).
- `"${channel}.${instrument}"` — Crypto.com (`trade.BTC_USDT`), Bybit
  (`publicTrade.BTCUSDT`).
- `'{"channel":"${channel}","instId":"${instrument}"}'` — OKX (one JSON
  object per stream).

### `message_format`

For now: `"json"` only. The adapter parses each text frame as JSON and
dispatches to a per-channel parser hard-coded per venue id (so Coinbase’s
`matches` event maps to `Trade`, Kraken’s `trade` event with its v2 schema
maps to `Trade`, etc.). Adding a new venue with a brand-new wire shape is the
case where a parser tweak is required (`parser-tweak` issue label).

### Symbol normalization

Each venue’s instrument id (`BTCUSDT`, `BTC-USD`, `XBT/USD`, …) is paired
with a `canonical_symbol` in TOML. The adapter never guesses — the operator
declares the mapping. Internally the canonical symbol is held in the
`CanonicalSymbol` newtype, which enforces the slash form on the public side
and exposes `normalized()` for subject rendering.

## Resilience

- **Exponential backoff reconnect** per venue, between
  `reconnect_delay_ms` and `max_reconnect_delay_ms`, with full jitter.
- **Circuit breaker** per venue: `Closed → Open → HalfOpen → Closed`.
  Trips after `failure_threshold` consecutive connection failures, refuses
  reconnects for `reset_timeout_secs`, then re-tests with up to
  `half_open_max_requests` attempts.
- **Bounded mpsc channels** between venue tasks → pipeline → publisher.
  Backpressure propagates upstream all the way to the WebSocket reader, so a
  slow NATS publisher pauses venue intake instead of OOM-ing the process.
- **Sequence tracking** per `(venue, instrument, data_type)` via a
  `DashMap<_, AtomicU64>`. Gaps emit a warn-level log with the missing range
  but do not stop the stream.
- **Publish retries** — 3 attempts (100ms → 500ms → 2s) before the message
  is dropped and `market2nats_pipeline_publish_errors_total` increments.
- **Graceful shutdown** on `SIGINT`/`SIGTERM`, with `shutdown_timeout_ms`
  granted to the publisher to drain.

## HTTP endpoints

The relay's HTTP server binds on the port reported in the startup log
(`http health server started`). With the default config this is `8080`.

| Endpoint | Description |
|---|---|
| `GET /health` | JSON with per-venue `ConnectionState`, NATS connection state, and overall health (`healthy` / `degraded` / `unhealthy`). Returns `200` when overall is `healthy`, else `503`. |
| `GET /metrics` | Prometheus exposition. |

Quick checks with `curl`:

```bash
# Pretty-printed health (requires jq)
curl -s http://localhost:8080/health | jq

# Just the overall status
curl -s http://localhost:8080/health | jq -r '.status'

# Per-venue connection state
curl -s http://localhost:8080/health | jq '.venues'

# Health that fails the script when not 200 (use in a probe)
curl -fsS http://localhost:8080/health > /dev/null && echo OK

# Full Prometheus metrics dump
curl -s http://localhost:8080/metrics

# Filter to just the relay-specific metrics
curl -s http://localhost:8080/metrics | grep -E '^market2nats_'

# Per-venue/data_type pipeline counters
curl -s http://localhost:8080/metrics | grep '^market2nats_pipeline_received_total'
curl -s http://localhost:8080/metrics | grep '^market2nats_pipeline_published_total'

# Venue connection-state gauge (0 disc, 1 conn, 2 reconn, 3 circuit_open)
curl -s http://localhost:8080/metrics | grep '^market2nats_venue_connection_state'

# Is the NATS client connected? (1 = yes, 0 = no)
curl -s http://localhost:8080/metrics | grep '^market2nats_nats_connected'
```

## Metrics reference

| Name | Type | Labels | Description |
|---|---|---|---|
| `market2nats_pipeline_received_total` | counter | `venue`, `data_type` | Messages received from venue adapters. |
| `market2nats_pipeline_published_total` | counter | `venue`, `data_type` | Messages successfully published to NATS. |
| `market2nats_pipeline_publish_errors_total` | counter | `venue`, `data_type` | NATS publish failures (after retries exhausted). |
| `market2nats_pipeline_serialize_errors_total` | counter | `venue`, `data_type` | Encoder failures (rare; usually a domain bug). |
| `market2nats_pipeline_uptime_seconds` | gauge | — | Service uptime; useful as a liveness signal. |
| `market2nats_venue_connection_state` | gauge | `venue` | `0` disconnected, `1` connected, `2` reconnecting, `3` circuit_open. |
| `market2nats_nats_connected` | gauge | — | `1` while the NATS client is connected, `0` otherwise. |

## Run

```bash
# Build
cargo build --release -p market2nats

# Run with a config file
NATS_URL='nats://nats:secret@localhost:4222' \
  ./target/release/market2nats config/relay.all-spot-trades.toml

# Override config path via env
RELAY_CONFIG=config/relay.binance.toml ./target/release/market2nats
```

## Adding a new venue

1. Open an issue with the `venue` label. Use `drop-in` if the venue fits
   `generic_ws` with TOML only, or `parser-tweak` if it needs a small
   adapter change.
2. Create the `venue/<name>` branch and add `config/relay.<name>.toml`.
3. If a parser tweak is needed, add the venue’s message handler in
   `crates/market2nats/src/infrastructure/ws/generic.rs` behind a venue-id
   match.
4. Add a fixture under `tests/fixtures/<venue>/` and a parsing test.

See `rules/global_rules.md` for the binding coding rules and
`docs/wire-format.md` for the immovable output contract.
