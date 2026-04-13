# Oracle Crate — Claude Code Prompts

Sequential prompts for building the `oracle` crate inside the `market2nats` workspace.
Execute each prompt as a separate Claude Code session, in order.
Each prompt assumes the previous one completed successfully.

---

## Prompt 0 — Convert to Cargo Workspace

```
The repo at the current directory is a single Rust crate (`market2nats`, edition 2024).
I need to convert it into a Cargo workspace so I can add sibling crates.

Steps:
1. Create a new directory `crates/market2nats/`.
2. Move ALL of `src/`, `proto/`, `build.rs` (if any), and the current `Cargo.toml` into `crates/market2nats/`.
3. Move `tests/` into `crates/market2nats/` (integration tests belong to that crate).
4. Create a NEW root `Cargo.toml` with:
   ```toml
   [workspace]
   resolver = "3"
   members = ["crates/*"]

   [workspace.package]
   edition = "2024"

   [workspace.dependencies]
   # Hoist ALL dependencies from the old Cargo.toml here with exact versions.
   # In the member Cargo.toml, reference them as: tokio = { workspace = true }
   ```
5. Update `crates/market2nats/Cargo.toml`:
   - Remove `edition` from `[package]` and add `edition.workspace = true`.
   - Replace every dependency version with `workspace = true` (keep feature flags).
6. Ensure `config/` and `rules/` stay at the repo root (they are not Rust source).
7. Run `cargo build --release` and `cargo test --all-features` — fix anything that breaks.
8. Run `cargo clippy --all-targets --all-features -- -D warnings` and `cargo fmt --check`.

Do NOT rename any modules, types, or change any code logic. This is purely structural.
Read `rules/global_rules.md` before starting — all conventions apply.
```

---

## Prompt 1 — Shared Domain Crate

```
Inside the workspace, create a new crate `crates/market2nats-domain/`.

This crate extracts the domain types that will be shared between `market2nats` and
the upcoming `oracle` crate. It MUST follow every rule in `rules/global_rules.md` and
the DDD conventions in the existing `crates/market2nats/src/domain/CLAUDE.md`.

Steps:
1. Create `crates/market2nats-domain/Cargo.toml` with workspace deps:
   - `rust_decimal` (with `serde-with-str`), `serde`, `thiserror`
   - Dev: `rust_decimal_macros`, `serde_json`
2. Copy these files from `crates/market2nats/src/domain/` into `crates/market2nats-domain/src/`:
   - `types.rs` (VenueId, InstrumentId, CanonicalSymbol, Price, Quantity, Sequence, Timestamp)
   - `enums.rs` (Side, MarketDataType, ConnectionState, ServiceHealth)
   - `events.rs` (MarketDataEnvelope, MarketDataPayload, Trade, Ticker, L2Update, FundingRate, Liquidation)
   - `error.rs` (DomainError)
   - Create `lib.rs` that re-exports everything with `pub use`.
3. Update `crates/market2nats/Cargo.toml` to depend on `market2nats-domain`:
   ```toml
   market2nats-domain = { path = "../market2nats-domain" }
   ```
4. In `crates/market2nats/`, remove the duplicated domain files and replace the `domain`
   module with re-exports from `market2nats-domain`. Every `use crate::domain::X` in the
   existing code must still compile without changes — use a thin `src/domain/mod.rs` that
   does `pub use market2nats_domain::*;`.
5. Run `cargo build --release`, `cargo test --all-features`, `cargo clippy --all-targets --all-features -- -D warnings`.

Do NOT change any type signatures, field names, or behavior. The only change is where the
types physically live.
```

---

## Prompt 2 — Oracle Crate: Domain Layer

```
Create `crates/oracle/` — a new library crate for computing aggregated oracle prices
from multi-venue trade data received via NATS.

Read `rules/global_rules.md` before writing any code. Follow DDD: domain layer first,
zero async, zero I/O, zero infrastructure imports.

This crate depends on:
- `market2nats-domain` (for VenueId, CanonicalSymbol, Price, Quantity, Timestamp, Trade, MarketDataEnvelope, MarketDataPayload)
- `rust_decimal` (with `serde-with-str`), `serde`, `thiserror`

### Domain types to create in `src/domain/`:

#### `src/domain/types.rs`
- `OraclePrice` — the final aggregated output:
  - `symbol: CanonicalSymbol`
  - `price: Price` — the computed oracle price
  - `timestamp: Timestamp` — when this oracle price was computed
  - `sources: Vec<PriceSource>` — which venues contributed
  - `strategy: AggregationStrategyKind` — which strategy produced this
  - `confidence: OracleConfidence` — High/Medium/Low based on source count and spread

- `PriceSource` — a single venue's contribution:
  - `venue: VenueId`
  - `price: Price`
  - `quantity: Quantity` — trade quantity (used for VWAP weighting)
  - `timestamp: Timestamp` — venue's trade timestamp
  - `age_ms: u64` — staleness at computation time

- `OracleConfidence` — `#[repr(u8)]` enum: `High`, `Medium`, `Low`
  - `High`: >=7 sources, spread <0.1%
  - `Medium`: >=4 sources, spread <0.5%
  - `Low`: everything else
  - Implement a `#[must_use] fn compute(source_count: usize, spread_bps: Decimal) -> Self`

#### `src/domain/strategies.rs`
- `AggregationStrategyKind` — `#[repr(u8)]` enum: `Median`, `Twap`, `Vwap`, `MedianFiltered`
  - Serialize as UPPERCASE.

- `AggregationStrategy` trait (NOT async, domain-pure):
  ```rust
  pub trait AggregationStrategy: Send + Sync {
      fn kind(&self) -> AggregationStrategyKind;
      fn aggregate(&self, sources: &[PriceSource]) -> Result<Price, OracleError>;
  }
  ```

- Implementations (each in its own file under `src/domain/strategies/`):
  1. `MedianStrategy` — sorts by price, picks the middle value (or avg of two middles).
  2. `TwapStrategy` — time-weighted average: weights each price by `(window_ms - age_ms) / window_ms`.
     Constructor takes `window_ms: u64`. Sources with `age_ms > window_ms` are discarded.
  3. `VwapStrategy` — volume-weighted: `sum(price * quantity) / sum(quantity)`.
     All arithmetic uses `checked_*` on `Decimal`.
  4. `MedianFilteredStrategy` — composes: first discard outliers beyond `max_deviation_bps: u64`
     from the initial median, then recompute the median on survivors.
     Constructor takes `max_deviation_bps: u64` (basis points, e.g. 100 = 1%).

#### `src/domain/filters.rs`
- `StalenessFilter` — discards `PriceSource` entries where `age_ms > max_age_ms`.
  Constructor takes `max_age_ms: u64`. Pure function: `fn filter(&self, sources: &[PriceSource]) -> Vec<PriceSource>`.
- `OutlierFilter` — discards sources deviating more than `max_deviation_bps` from median.
  Constructor takes `max_deviation_bps: u64`. Pure function: `fn filter(&self, sources: &[PriceSource]) -> Vec<PriceSource>`.
- `MinSourcesFilter` — returns `Err(OracleError::InsufficientSources)` if fewer than `min` sources remain.

#### `src/domain/error.rs`
- `OracleError` enum:
  - `InsufficientSources { required: usize, available: usize }`
  - `AllSourcesStale { symbol: String }`
  - `ArithmeticOverflow { operation: String }`
  - `DivisionByZero { context: String }`
  - `Domain(market2nats_domain::DomainError)` — wraps upstream with `#[from]`

#### `src/domain/pipeline.rs`
- `OraclePipeline` — orchestrates filters + strategy. Pure, no async.
  - Fields: `filters: Vec<Box<dyn PriceFilter>>`, `strategy: Box<dyn AggregationStrategy>`, `min_sources: usize`
  - Method: `fn compute(&self, symbol: &CanonicalSymbol, sources: &[PriceSource]) -> Result<OraclePrice, OracleError>`
    1. Apply each filter in order
    2. Check min_sources
    3. Run strategy.aggregate()
    4. Compute confidence
    5. Return OraclePrice

- `PriceFilter` trait:
  ```rust
  pub trait PriceFilter: Send + Sync {
      fn apply(&self, sources: &[PriceSource]) -> Vec<PriceSource>;
  }
  ```
  Implement `PriceFilter` for `StalenessFilter` and `OutlierFilter`.

### Unit tests:
- Test each strategy with known inputs and expected outputs using `dec!()`.
- Test filters: staleness discards old, outlier discards deviants.
- Test pipeline end-to-end with a mix of good and bad sources.
- Test edge cases: empty sources, single source, all stale, all outliers.
- Test confidence computation thresholds.

All `pub` items need `///` doc comments. Follow `#[must_use]`, `#[inline]`, `#[cold]` conventions
from `rules/global_rules.md`.

Run `cargo test -p oracle`, `cargo clippy -p oracle -- -D warnings`, `cargo fmt --check`.
```

---

## Prompt 3 — Oracle Crate: Config Layer

```
Add TOML configuration to `crates/oracle/`. Read `rules/global_rules.md` and
`crates/market2nats/src/config/CLAUDE.md` for patterns.

Create `src/config/mod.rs` and `src/config/model.rs`:

### `OracleConfig` (top-level):
```toml
# Example config: oracle.toml
[service]
name = "oracle"
log_level = "info"
log_format = "json"

[nats]
urls = ["nats://localhost:4222"]
auth = "none"

# Subjects to subscribe to for trade data.
# Supports NATS wildcards. The oracle subscribes to these and extracts Trade payloads.
[[subscriptions]]
symbol = "BTC/USDT"
subjects = [
  "market.binance.btc-usdt.trade",
  "market.bybit.btc-usdt.trade",
  "market.bitmex.btc-usdt.trade",
  "market.bitstamp.btc-usdt.trade",
  "market.hyperliquid.btc-usdt.trade",
  "market.crypto-com.btc-usdt.trade",
  "market.gate-io.btc-usdt.trade",
  "market.dydx.btc-usdt.trade",
  "market.bitfinex.btc-usdt.trade",
]

[[subscriptions]]
symbol = "ETH/USDT"
subjects = [
  "market.binance.eth-usdt.trade",
  "market.bybit.eth-usdt.trade",
  # ...etc
]

[pipeline]
strategy = "median_filtered"       # "median", "twap", "vwap", "median_filtered"
staleness_max_ms = 10000           # Discard sources older than 10s
outlier_max_deviation_bps = 100    # 1% max deviation from median
min_sources = 3                    # Minimum contributing venues
twap_window_ms = 30000             # Only used if strategy = "twap"

[publish]
# Where to publish oracle prices
subject_pattern = "oracle.<symbol_normalized>"  # e.g., oracle.btc-usdt
format = "json"
publish_interval_ms = 1000         # Emit oracle price every 1s
```

### Config structs:
- `OracleConfig` (top-level, `#[serde(deny_unknown_fields)]`)
- `ServiceConfig` (reuse pattern from market2nats)
- `NatsConfig` (reuse the EXACT same struct from market2nats — import from market2nats-domain or duplicate with same fields)
- `SubscriptionEntry` { symbol: String, subjects: Vec<String> }
- `PipelineConfig` { strategy, staleness_max_ms, outlier_max_deviation_bps, min_sources, twap_window_ms }
- `PublishConfig` { subject_pattern, format, publish_interval_ms }

### `src/config/validation.rs`:
- Validate strategy is one of the known kinds
- Validate min_sources >= 1
- Validate at least one subscription
- Validate subject_pattern contains `<symbol_normalized>`

### `src/config/builder.rs`:
- `fn build_pipeline(config: &PipelineConfig) -> Result<OraclePipeline, OracleError>`
  Constructs the correct filters and strategy from config values.

Add `config` feature flag in Cargo.toml: depends on `toml` and `serde`.

Tests: valid config parses, invalid strategy rejected, missing fields error.
Run the full pre-submission checklist.
```

---

## Prompt 4 — Oracle Crate: Infrastructure Layer (NATS subscriber + publisher)

```
Add the infrastructure layer to `crates/oracle/`. Read `rules/global_rules.md` and
`crates/market2nats/src/infrastructure/CLAUDE.md` for conventions.

New dependencies for this crate: `async-nats`, `tokio`, `serde_json`, `tracing`, `dashmap`, `bytes`.

### `src/infrastructure/nats/subscriber.rs`
- `NatsTradeSubscriber`:
  - Connects to NATS using the NatsConfig.
  - For each `SubscriptionEntry`, subscribes to all listed subjects.
  - Deserializes incoming messages as `MarketDataEnvelope` (JSON).
  - Filters for `MarketDataPayload::Trade` only — ignores other payload types.
  - Converts each trade into a `PriceSource`:
    - `venue` from envelope.venue
    - `price` from trade.price
    - `quantity` from trade.quantity
    - `timestamp` from envelope.exchange_timestamp.unwrap_or(envelope.received_at)
    - `age_ms` computed as `now - timestamp`
  - Stores the LATEST `PriceSource` per (symbol, venue) in a `DashMap<(CanonicalSymbol, VenueId), PriceSource>`.
    Only the most recent trade per venue matters for the oracle.
  - Exposes `fn get_sources(&self, symbol: &CanonicalSymbol) -> Vec<PriceSource>` that
    returns a snapshot of all current sources for a symbol.

### `src/infrastructure/nats/publisher.rs`
- `OraclePricePublisher`:
  - Publishes `OraclePrice` to NATS as JSON.
  - Subject resolved from `publish.subject_pattern` replacing `<symbol_normalized>` with
    `symbol.normalized()` (same normalization as market2nats: lowercase, `/` → `-`).
  - Sets `Content-Type: application/json` header.

### `src/infrastructure/mod.rs`
Re-exports both.

### Port traits in `src/application/ports.rs`:
```rust
pub trait TradeSource: Send + Sync {
    fn get_sources(&self, symbol: &CanonicalSymbol) -> Vec<PriceSource>;
}

pub trait OraclePublisher: Send + Sync {
    fn publish(&self, price: &OraclePrice) -> impl Future<Output = Result<(), OracleError>> + Send;
}
```

The infrastructure types implement these traits. The application layer depends only on the traits.

Follow all DDD boundary rules: application and domain never import infrastructure.
Run the full pre-submission checklist.
```

---

## Prompt 5 — Oracle Crate: Application Layer (Orchestration + main)

```
Add the application orchestration and binary entrypoint to `crates/oracle/`.
Read `rules/global_rules.md`.

### `src/application/oracle_service.rs`
- `OracleService`:
  - Generic over `T: TradeSource` and `P: OraclePublisher`.
  - Holds: `pipelines: HashMap<CanonicalSymbol, OraclePipeline>` (one per symbol from config),
    `source: Arc<T>`, `publisher: Arc<P>`, `publish_interval: Duration`.
  - `async fn run(&self, shutdown: tokio::sync::watch::Receiver<bool>)`:
    - Spawns a `tokio::time::interval` loop at `publish_interval_ms`.
    - Each tick, for each symbol:
      1. `source.get_sources(symbol)` → get current PriceSources
      2. `pipeline.compute(symbol, &sources)` → OraclePrice or error
      3. If Ok, `publisher.publish(&oracle_price)`.await
      4. If Err, `tracing::warn!` with structured fields (symbol, error).
    - Listens for shutdown signal to exit gracefully.

### `src/application/health.rs`
- Simple health state: tracks last successful computation per symbol.
- Exposes whether the oracle is healthy (has computed within 2x publish interval).

### `src/application/metrics.rs`
- Prometheus-style metrics via the `metrics` crate:
  - `oracle_price_computed_total` (counter, labels: symbol, strategy)
  - `oracle_computation_errors_total` (counter, labels: symbol, error_kind)
  - `oracle_sources_count` (gauge, labels: symbol)
  - `oracle_price_spread_bps` (gauge, labels: symbol) — spread between min and max source
  - `oracle_computation_latency_ms` (histogram, labels: symbol)

### `src/main.rs`
- Load config from CLI arg or `ORACLE_CONFIG` env var (default: `config/oracle.toml`).
- Init tracing subscriber (JSON or pretty, from config).
- Connect NATS subscriber.
- Connect NATS publisher.
- Build pipelines from config (one per subscription symbol).
- Start OracleService.
- Start HTTP health/metrics server on a configurable port (default 9091).
- Handle SIGTERM/SIGINT for graceful shutdown.
- Wire everything with `Arc` — no `.unwrap()` in production code.

### Example config file
Create `config/oracle.toml` with the BTC/USDT and ETH/USDT subscriptions for all 9 venues
listed earlier (Binance, Bybit, BitMEX, Bitstamp, Hyperliquid, Crypto.com, Gate.io, dYdX, Bitfinex).
Use `median_filtered` strategy, `staleness_max_ms = 10000`, `outlier_max_deviation_bps = 100`,
`min_sources = 3`.

Run the full pre-submission checklist:
- cargo clippy --all-targets --all-features -- -D warnings
- cargo fmt --check
- cargo test --all-features
- cargo build --release (zero warnings)
```

---

## Prompt 6 — Integration Tests

```
Add integration tests for the `oracle` crate in `crates/oracle/tests/`.
Read `rules/global_rules.md` for testing conventions.

### `tests/pipeline_integration.rs`
Test the full pipeline (filters + strategy) with realistic multi-venue data:
1. Construct 9 PriceSources simulating the 9 venues with prices that are close but not identical
   (e.g., BTC between 67,400 and 67,450, one outlier at 68,000, one stale at age 15s).
2. Run the pipeline with `MedianFilteredStrategy(max_deviation_bps=100)` + `StalenessFilter(10_000ms)`.
3. Assert:
   - The stale source was filtered out.
   - The outlier was filtered out.
   - The resulting oracle price is the median of the 7 survivors.
   - Confidence is High (7 sources, tight spread).
4. Test with only 2 sources → InsufficientSources error (min_sources=3).
5. Test with all sources stale → AllSourcesStale error.

### `tests/strategy_properties.rs`
Property-based style tests (no proptest needed, just parameterized):
1. Median of N identical prices = that price.
2. VWAP with equal quantities = simple average.
3. TWAP with identical ages = simple average.
4. Adding an outlier doesn't change the median (odd count).
5. Filtered median excludes the outlier and produces same result as unfiltered median without it.

### `tests/config_integration.rs`
1. Load the example `config/oracle.toml`, verify it parses correctly.
2. Build the pipeline from config, assert strategy kind and filter count.

All test names follow `test_<unit>_<scenario>_<expected>` convention.
Use `dec!()` for all decimal literals.
Run `cargo test --all-features -p oracle`.
```

---

## Execution Notes

- Run prompts in order (0 → 6). Each builds on the previous.
- After each prompt, verify the full workspace compiles: `cargo build --release && cargo test --all-features`.
- The `rules/global_rules.md` file is the authority on all code conventions — every prompt references it.
- NATS subject pattern for trades: `market.<venue>.<symbol_normalized>.trade` (already published by market2nats).
- Oracle output subject pattern: `oracle.<symbol_normalized>` (e.g., `oracle.btc-usdt`).
