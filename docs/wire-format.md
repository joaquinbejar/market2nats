# NATS wire format

This document is the **single source of truth** for the shape of messages that
`market2nats` publishes to NATS JetStream. Every venue adapter — Binance today,
and the venues added under the tracking issues in `venue/*` branches — must
produce events that serialize to this exact envelope. Consumers depend on this
contract.

## Format and transport

- **Default wire format:** JSON (`application/json`), human-readable and
  self-describing.
- **Alternative:** Protobuf (`application/protobuf`), compact binary, opt-in
  per deployment.
- **Selection:** configured in the `[serialization]` section of the TOML config:

  ```toml
  [serialization]
  format = "json"   # or "protobuf"
  ```

  If the section is omitted, the default is `"json"`.

- The format is set on the `Content-Type` header of every published NATS
  message, so downstream consumers can branch without needing to know the
  publisher config.

## Subject pattern

```
market.<venue_id>.<canonical_symbol_normalized>.<data_type>
```

- `venue_id` — lowercase venue identifier (e.g., `binance`, `bybit`, `okx`).
- `canonical_symbol_normalized` — canonical symbol lowercased with `/` replaced
  by `-`. Example: `BTC/USDT` → `btc-usdt`.
- `data_type` — one of `trade`, `ticker`, `l2_orderbook`, `funding_rate`,
  `liquidation`.

Examples:
- `market.binance.btc-usdt.trade`
- `market.bybit.eth-usdt.l2_orderbook`
- `market.okx.btc-usdt.funding_rate`

## Envelope

Every message shares the same outer envelope regardless of the data type. The
variant-specific fields are namespaced inside the `payload` object and
discriminated by `payload.type`.

| Field                | Type            | Description                                                                       |
|----------------------|-----------------|-----------------------------------------------------------------------------------|
| `venue`              | string          | Venue identifier (e.g., `"binance"`).                                             |
| `instrument`         | string          | Venue-local instrument identifier as received from the venue (e.g., `"BTCUSDT"`). |
| `canonical_symbol`   | string          | Canonical symbol with slash notation (e.g., `"BTC/USDT"`).                        |
| `data_type`          | string          | One of the five canonical types listed above.                                     |
| `received_at`        | integer (u64)   | Epoch milliseconds when the service received the raw WebSocket frame.             |
| `exchange_timestamp` | integer \| null | Venue-provided timestamp in epoch milliseconds, when available.                   |
| `sequence`           | integer (u64)   | Monotonic sequence per (venue, instrument, data_type), assigned by the service.   |
| `payload`            | object          | Tagged union. `payload.type` matches `data_type`.                                 |

All decimal fields (`price`, `quantity`, `rate`, …) are serialized as
**strings** to preserve full `rust_decimal::Decimal` precision. Consumers must
parse them as decimals, not IEEE-754 floats.

## Payload variants

### `trade`

A single executed trade.

```json
{
  "venue": "binance",
  "instrument": "BTCUSDT",
  "canonical_symbol": "BTC/USDT",
  "data_type": "trade",
  "received_at": 1700000000000,
  "exchange_timestamp": 1699999999999,
  "sequence": 42,
  "payload": {
    "type": "trade",
    "price": "50000.50",
    "quantity": "1.5",
    "side": "BUY",
    "trade_id": "12345"
  }
}
```

Fields inside `payload`:

| Field      | Type            | Description                                                                 |
|------------|-----------------|-----------------------------------------------------------------------------|
| `type`     | string          | Always `"trade"`.                                                           |
| `price`    | string          | Execution price as a decimal string.                                        |
| `quantity` | string          | Trade quantity as a decimal string.                                         |
| `side`     | string          | Aggressor side: `"BUY"` or `"SELL"` (uppercase, per global rules).          |
| `trade_id` | string \| null  | Venue-specific trade identifier when the venue provides one.                |

### `ticker`

Top-of-book snapshot.

```json
{
  "venue": "binance",
  "instrument": "BTCUSDT",
  "canonical_symbol": "BTC/USDT",
  "data_type": "ticker",
  "received_at": 1700000000000,
  "exchange_timestamp": null,
  "sequence": 7,
  "payload": {
    "type": "ticker",
    "bid_price": "50000.00",
    "bid_qty": "2.5",
    "ask_price": "50001.00",
    "ask_qty": "1.8",
    "last_price": "50000.50"
  }
}
```

All price and quantity fields are decimal strings.

### `l2_update`

Orderbook snapshot or incremental delta. The subject uses `l2_orderbook`; the
payload discriminator is `l2_update`.

```json
{
  "venue": "binance",
  "instrument": "BTCUSDT",
  "canonical_symbol": "BTC/USDT",
  "data_type": "l2_orderbook",
  "received_at": 1700000000000,
  "exchange_timestamp": 1700000000000,
  "sequence": 1001,
  "payload": {
    "type": "l2_update",
    "bids": [
      ["50000.00", "1.5"],
      ["49999.00", "2.0"]
    ],
    "asks": [
      ["50001.00", "0.8"]
    ],
    "is_snapshot": false
  }
}
```

- `bids` and `asks` are arrays of `[price, quantity]` tuples (both as decimal
  strings).
- A level with `quantity == "0"` is a **remove**.
- `is_snapshot == true` means the message is a full replace of the book; the
  consumer should drop any prior state. `false` means an incremental delta on
  top of the last known state.

### `funding_rate`

```json
{
  "venue": "binance-futures",
  "instrument": "BTCUSDT",
  "canonical_symbol": "BTC/USDT",
  "data_type": "funding_rate",
  "received_at": 1700000000000,
  "exchange_timestamp": 1700000000000,
  "sequence": 3,
  "payload": {
    "type": "funding_rate",
    "rate": "0.0001",
    "predicted_rate": "0.00012",
    "next_funding_at": 1700028800000
  }
}
```

- `rate` is the current funding rate as a decimal string.
- `predicted_rate` is the venue's predicted next rate when available, else `null`.
- `next_funding_at` is epoch milliseconds.

### `liquidation`

```json
{
  "venue": "binance-futures",
  "instrument": "BTCUSDT",
  "canonical_symbol": "BTC/USDT",
  "data_type": "liquidation",
  "received_at": 1700000000000,
  "exchange_timestamp": 1700000000000,
  "sequence": 14,
  "payload": {
    "type": "liquidation",
    "side": "SELL",
    "price": "50000.00",
    "quantity": "0.5"
  }
}
```

`side` is the side being liquidated: `"BUY"` (long liquidation) or `"SELL"`
(short liquidation).

## Canonical invariants

Every adapter must guarantee:

1. **One envelope shape** — no venue may add, rename, or reorder top-level
   fields. Variant differences live inside `payload`.
2. **Decimal strings** — prices, quantities, and rates are strings, never
   JSON numbers, to avoid float rounding.
3. **Monotonic `sequence`** — `sequence` is assigned by the service per
   (venue, instrument, data_type). Gaps indicate loss and must be logged and
   recovered.
4. **`received_at` is always present** — it is the service's own monotonic
   wall-clock at the moment the raw frame was read, used for latency monitoring
   and event ordering when venues ship out-of-order timestamps.
5. **`exchange_timestamp` is optional but preferred** — if the venue ships a
   per-event timestamp, pass it through; otherwise leave `null`.
6. **`side` is always uppercase** — `"BUY"` / `"SELL"`. This follows the
   `#[serde(rename_all = "UPPERCASE")]` convention in `rules/global_rules.md`
   for external-facing enums.
7. **`canonical_symbol` is always the slash form** — `"BTC/USDT"`, not
   `"BTCUSDT"` and not `"btc-usdt"`. Subject normalization to `btc-usdt`
   happens at publish time inside `StreamRouter::resolve_subject`.

## Golden tests

The exact shapes above are locked by golden tests in
`src/serialization/json.rs`. Any change to a field name or serialization
representation breaks them on purpose — if you need to evolve the wire format,
update both the tests and this document in the same commit, and call it out
in the PR description.
