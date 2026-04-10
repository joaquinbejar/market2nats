# Domain Layer
 
Pure domain model — the heart of the system. Everything else depends on this; this depends on nothing.
 
## Rules
 
- ZERO imports from `application/`, `infrastructure/`, `config/`, or `serialization/`
- Only allowed external crates: `rust_decimal`, `serde`, `thiserror`
- No async, no I/O, no Tokio, no network
- All types use newtypes with private inner fields
- Constructors: `new()` / `try_new()` → `Result<Self, DomainError>`
- `#[must_use]` on all pure functions
- `#[repr(u8)]` on small enums (Side, MarketDataType)
- Derive order: Debug, Clone, Copy, PartialEq, Eq, Hash — only what's needed
 
## Files
 
- `types.rs` — VenueId, InstrumentId, Price, Quantity, Sequence, Timestamp newtypes
- `enums.rs` — Side, MarketDataType, ConnectionState
- `events.rs` — MarketDataEnvelope, Trade, Ticker, L2Update, FundingRate, Liquidation
- `error.rs` — DomainError enum with thiserror
- `mod.rs` — re-exports all pub types
 
## MarketDataEnvelope Fields
 
```
venue: VenueId
instrument: InstrumentId
data_type: MarketDataType
received_at: u64              (epoch millis)
exchange_timestamp: Option<u64>
sequence: Sequence            (per venue+instrument+data_type)
payload: MarketDataPayload    (tagged enum)
```
 
## MarketDataPayload Variants
 
- Trade { price: Price, quantity: Quantity, side: Side, trade_id: Option<String> }
- Ticker { bid_price: Price, bid_qty: Quantity, ask_price: Price, ask_qty: Quantity, last_price: Price }
- L2Update { bids: Vec<(Price, Quantity)>, asks: Vec<(Price, Quantity)>, is_snapshot: bool }
- FundingRate { rate: Decimal, predicted_rate: Option<Decimal>, next_funding_at: u64 }
- Liquidation { side: Side, price: Price, quantity: Quantity }
