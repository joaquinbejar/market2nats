# Config Module
 
TOML configuration parsing and validation.
 
## Rules
 
- Uses `serde` + `toml` crate for deserialization
- `#[serde(deny_unknown_fields)]` on all config structs
- All config structs live in `model.rs`
- Validation in `validation.rs` — runs after parsing, returns `Vec<ConfigError>`
- Environment variable substitution for secrets: `${NATS_TOKEN}` pattern
- Config is loaded once at startup, then wrapped in `Arc<AppConfig>` and shared immutably
 
## Files
 
- `model.rs` — ServiceConfig, NatsConfig, StreamConfig, ConsumerConfig, VenueConfig, etc.
- `validation.rs` — validate_config(AppConfig) → Result<(), Vec<ConfigError>>
- `mod.rs` — load_config(path) → Result<Arc<AppConfig>, ConfigError>
 
## Validation Checks
 
- At least one venue enabled
- Each venue has at least one subscription
- Stream subjects match declared subscription data types
- Consumer stream references exist in streams list
- NATS URL format is valid
- Reconnect delays are positive
- Circuit breaker thresholds are positive
