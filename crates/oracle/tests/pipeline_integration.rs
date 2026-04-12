//! Integration tests for the oracle pipeline with realistic multi-venue data.

use market2nats_domain::{CanonicalSymbol, Price, Quantity, Timestamp, VenueId};
use oracle::domain::{
    MedianFilteredStrategy, MedianStrategy, OracleConfidence, OracleError, OraclePipeline,
    OutlierFilter, PriceSource, StalenessFilter,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

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
fn test_pipeline_realistic_btc_filters_and_computes() {
    // 9 sources simulating real venues with BTC prices.
    // dydx is an outlier (>100bps from median), bitfinex is stale (>10000ms).
    let sources = vec![
        make_source("binance", dec!(67420.50), dec!(2.5), 500),
        make_source("bybit", dec!(67418.00), dec!(1.8), 800),
        make_source("bitmex", dec!(67425.00), dec!(3.0), 1200),
        make_source("bitstamp", dec!(67415.50), dec!(0.5), 2000),
        make_source("hyperliquid", dec!(67422.00), dec!(1.0), 600),
        make_source("crypto-com", dec!(67430.00), dec!(0.8), 1500),
        make_source("gate-io", dec!(67419.00), dec!(1.2), 900),
        make_source("dydx", dec!(68500.00), dec!(0.3), 3000), // outlier (~160bps from median)
        make_source("bitfinex", dec!(67421.00), dec!(0.1), 15000), // stale
    ];

    let pipeline = OraclePipeline::new(
        vec![
            Box::new(StalenessFilter::new(10_000)),
            Box::new(OutlierFilter::new(100)),
        ],
        Box::new(MedianFilteredStrategy::new(100)),
        3,
    );

    let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
    let result = pipeline
        .compute(&symbol, &sources)
        .expect("pipeline should succeed with 7 surviving sources");

    // bitfinex (stale, age=15000 > 10000) should be filtered out by StalenessFilter.
    // dydx (68000) deviates ~86 bps from median of remaining 8 sources -- let's verify
    // it gets filtered by the OutlierFilter(100bps).
    // After both filters, we expect 7 sources to survive.
    assert_eq!(
        result.sources.len(),
        7,
        "expected 7 sources after filtering stale (bitfinex) and outlier (dydx)"
    );

    // Verify stale source (bitfinex) was removed.
    assert!(
        !result
            .sources
            .iter()
            .any(|s| s.venue.as_str() == "bitfinex"),
        "bitfinex should be filtered as stale"
    );

    // Verify outlier (dydx) was removed.
    assert!(
        !result.sources.iter().any(|s| s.venue.as_str() == "dydx"),
        "dydx should be filtered as outlier"
    );

    // With 7 sources and tight spread, confidence should be High.
    assert_eq!(
        result.confidence,
        OracleConfidence::High,
        "7 tightly-clustered sources should yield High confidence"
    );

    // The 7 surviving prices sorted:
    // 67415.50, 67418.00, 67419.00, 67420.50, 67422.00, 67425.00, 67430.00
    // Median (odd count, middle element index 3) = 67420.50
    // However, MedianFilteredStrategy does its own internal outlier pass on
    // the 7 sources, which should keep all of them (spread is <100bps).
    // So final median of 7 = 67420.50.
    assert_eq!(
        result.price.value(),
        dec!(67420.50),
        "median of 7 surviving sources should be 67420.50"
    );
}

#[test]
fn test_pipeline_insufficient_sources_after_filtering() {
    let sources = vec![
        make_source("binance", dec!(50000), dec!(1.0), 500),
        make_source("kraken", dec!(50010), dec!(1.0), 600),
    ];

    let pipeline = OraclePipeline::new(
        vec![Box::new(StalenessFilter::new(10_000))],
        Box::new(MedianStrategy::new()),
        3,
    );

    let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
    let result = pipeline.compute(&symbol, &sources);

    match result {
        Err(OracleError::InsufficientSources {
            required,
            available,
        }) => {
            assert_eq!(required, 3, "required should be 3");
            assert_eq!(available, 2, "available should be 2");
        }
        other => panic!("expected InsufficientSources error, got: {:?}", other),
    }
}

#[test]
fn test_pipeline_all_sources_stale() {
    let sources = vec![
        make_source("binance", dec!(50000), dec!(1.0), 15000),
        make_source("kraken", dec!(50010), dec!(1.0), 20000),
        make_source("coinbase", dec!(50005), dec!(1.0), 12000),
    ];

    let pipeline = OraclePipeline::new(
        vec![Box::new(StalenessFilter::new(10_000))],
        Box::new(MedianStrategy::new()),
        1,
    );

    let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
    let result = pipeline.compute(&symbol, &sources);

    match result {
        Err(OracleError::AllSourcesStale { symbol: s }) => {
            assert_eq!(s, "BTC/USDT", "symbol should be preserved in error");
        }
        other => panic!("expected AllSourcesStale error, got: {:?}", other),
    }
}

#[test]
fn test_pipeline_all_sources_are_outliers_except_one() {
    // One "normal" price and three far-away outliers.
    // Median of [100, 500, 600, 700] = avg(500, 600) = 550
    // 100 deviates by |100-550|/550*10000 = ~8181 bps >> 100
    // 500 deviates by |500-550|/550*10000 = ~909 bps >> 100
    // 600 deviates by |600-550|/550*10000 = ~909 bps >> 100
    // 700 deviates by |700-550|/550*10000 = ~2727 bps >> 100
    // With threshold=100 bps, likely all get filtered.
    // Actually let's make it clearer: use prices tightly clustered except one lonely one.
    let sources = vec![
        make_source("a", dec!(100), dec!(1.0), 500),
        make_source("b", dec!(200), dec!(1.0), 500),
        make_source("c", dec!(300), dec!(1.0), 500),
        make_source("d", dec!(400), dec!(1.0), 500),
    ];

    // OutlierFilter(10) means only sources within 10 bps (~0.1%) of median pass.
    // Median of [100,200,300,400] = 250. All deviate far more than 10 bps.
    let pipeline = OraclePipeline::new(
        vec![Box::new(OutlierFilter::new(10))],
        Box::new(MedianStrategy::new()),
        2,
    );

    let symbol = CanonicalSymbol::try_new("ETH/USDT").unwrap();
    let result = pipeline.compute(&symbol, &sources);

    // All sources are filtered as outliers, so AllSourcesStale is returned
    // (since filtered vec is empty).
    match result {
        Err(OracleError::AllSourcesStale { .. }) => {}
        Err(OracleError::InsufficientSources { .. }) => {}
        other => panic!(
            "expected AllSourcesStale or InsufficientSources error, got: {:?}",
            other
        ),
    }
}

#[test]
fn test_pipeline_no_filters_passes_all_sources() {
    let sources = vec![
        make_source("a", dec!(100), dec!(1.0), 50000),
        make_source("b", dec!(200), dec!(1.0), 99000),
        make_source("c", dec!(150), dec!(1.0), 1),
    ];

    // No filters at all, just a median strategy with min_sources=1.
    let pipeline = OraclePipeline::new(Vec::new(), Box::new(MedianStrategy::new()), 1);

    let symbol = CanonicalSymbol::try_new("BTC/USDT").unwrap();
    let result = pipeline
        .compute(&symbol, &sources)
        .expect("no filters means all sources pass");

    assert_eq!(
        result.sources.len(),
        3,
        "all 3 sources should survive without filters"
    );
    // Sorted prices: 100, 150, 200 -> median = 150
    assert_eq!(result.price.value(), dec!(150));
}
