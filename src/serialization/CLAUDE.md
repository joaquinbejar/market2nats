# Serialization Module
 
Encodes MarketDataEnvelope into wire format for NATS publishing.
 
## Rules
 
- Two formats: protobuf (default, via prost) and JSON (via serde_json)
- Proto definitions live in proto/market_data.proto
- Serialization format is set in NATS message headers: Content-Type header
- Serializer is selected per-stream or globally in config
 
## Files
 
- `proto.rs` — protobuf encode/decode using prost-generated types
- `json.rs` — JSON encode/decode using serde_json
- `mod.rs` — Serializer trait + format selection logic
