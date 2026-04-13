//! Integration tests for oracle configuration loading and pipeline building.

use market2nats_domain::{CanonicalSymbol, Price, Quantity, Timestamp, VenueId};
use oracle::config::{builder::build_pipeline, load_config};
use oracle::domain::PriceSource;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Resolves a path relative to the workspace root.
fn ws_path(relative: &str) -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root must exist")
        .join(relative)
        .to_str()
        .expect("valid UTF-8")
        .to_owned()
}

fn make_source(venue: &str, price: Decimal, quantity: Decimal, age_ms: u64) -> PriceSource {
    PriceSource {
        venue: VenueId::try_new(venue).unwrap(),
        price: Price::try_new(price).unwrap(),
        quantity: Quantity::try_new(quantity).unwrap(),
        timestamp: Timestamp::new(1_700_000_000_000),
        age_ms,
    }
}

#[test]
fn test_load_oracle_config_parses_correctly() {
    let path = ws_path("config/oracle.toml");
    let config = load_config(&path).expect("config/oracle.toml should parse successfully");

    assert_eq!(config.service.name, "oracle", "service name mismatch");
    assert_eq!(
        config.subscriptions.len(),
        2,
        "expected 2 subscription entries"
    );
    assert_eq!(
        config.pipeline.strategy, "median_filtered",
        "pipeline strategy mismatch"
    );
    assert_eq!(
        config.pipeline.min_sources, 1,
        "pipeline min_sources mismatch"
    );
}

#[test]
fn test_build_pipeline_from_config() {
    let path = ws_path("config/oracle.toml");
    let config = load_config(&path).expect("config should load");

    let pipeline =
        build_pipeline(&config.pipeline).expect("build_pipeline should succeed for valid config");

    // Feed it some test sources and verify it computes a result.
    let sources = vec![
        make_source("binance", dec!(50000), dec!(1.0), 500),
        make_source("kraken", dec!(50010), dec!(1.0), 600),
        make_source("coinbase", dec!(50005), dec!(1.0), 700),
        make_source("bitstamp", dec!(50008), dec!(1.0), 800),
    ];
    let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
    let result = pipeline
        .compute(&symbol, &sources, Timestamp::now())
        .expect("pipeline should compute with valid sources");

    // All prices are within 100 bps of each other, so all should survive.
    assert_eq!(
        result.sources.len(),
        4,
        "all sources should survive filtering"
    );
    // Sorted: 50000, 50005, 50008, 50010 -> median_filtered produces median
    // of survivors. Even count: avg(50005, 50008) = 50006.5
    assert_eq!(result.price.value(), dec!(50006.5));
}

#[test]
fn test_config_validation_catches_invalid_strategy() {
    let dir = std::env::temp_dir();
    let path = dir.join("oracle_test_invalid_strategy.toml");
    let toml_content = r#"
[service]
name = "oracle"

[nats]
urls = ["nats://localhost:4222"]

[[subscriptions]]
symbol = "BTC/USDT"
subjects = ["market.binance.btc-usdt.trade"]

[pipeline]
strategy = "nonexistent_strategy"

[publish]
subject_pattern = "oracle.<symbol_normalized>.price"
"#;
    std::fs::write(&path, toml_content).expect("write temp file");

    // The config itself parses (strategy is just a string), but build_pipeline
    // should reject an unknown strategy name.
    let config = load_config(path.to_str().expect("valid path"));

    match config {
        Ok(cfg) => {
            // Config loaded fine; verify the build step rejects this exact
            // unknown strategy rather than failing for an unrelated reason.
            let pipeline_result = build_pipeline(&cfg.pipeline);
            match pipeline_result {
                Ok(_) => panic!("build_pipeline should reject unknown strategy"),
                Err(err) => {
                    let err_text = format!("{err:?}");
                    assert!(
                        err_text.contains("UnknownStrategy")
                            && err_text.contains("nonexistent_strategy"),
                        "expected UnknownStrategy(\"nonexistent_strategy\") from build_pipeline, got: {err_text}"
                    );
                }
            }
        }
        Err(err) => {
            // If validation catches it at load time, verify that it is the
            // expected unknown-strategy error.
            let err_text = format!("{err:?}");
            assert!(
                err_text.contains("UnknownStrategy") && err_text.contains("nonexistent_strategy"),
                "expected UnknownStrategy(\"nonexistent_strategy\") from load_config, got: {err_text}"
            );
        }
    }

    let _ = std::fs::remove_file(&path);
}
