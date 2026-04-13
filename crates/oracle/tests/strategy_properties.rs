//! Property-based style tests for aggregation strategies.
//! Parameterized test cases validating mathematical properties of each strategy.

use market2nats_domain::{Price, Quantity, Timestamp, VenueId};
use oracle::domain::{
    AggregationStrategy, MedianFilteredStrategy, MedianStrategy, PriceSource, TwapStrategy,
    VwapStrategy,
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
fn test_median_identical_prices_returns_that_price() {
    let strategy = MedianStrategy::new();
    let sources: Vec<PriceSource> = (0..5)
        .map(|i| make_source(&format!("v{i}"), dec!(100.00), dec!(1.0), 100))
        .collect();

    let result = strategy.aggregate(&sources).unwrap();
    assert_eq!(
        result.value(),
        dec!(100.00),
        "median of identical prices should be that price"
    );
}

#[test]
fn test_vwap_equal_quantities_is_simple_average() {
    let strategy = VwapStrategy::new();
    let sources = vec![
        make_source("a", dec!(100), dec!(1.0), 0),
        make_source("b", dec!(200), dec!(1.0), 0),
        make_source("c", dec!(300), dec!(1.0), 0),
    ];

    let result = strategy.aggregate(&sources).unwrap();
    // VWAP = (100*1 + 200*1 + 300*1) / (1+1+1) = 600/3 = 200
    assert_eq!(
        result.value(),
        dec!(200),
        "VWAP with equal quantities should be simple average"
    );
}

#[test]
fn test_twap_identical_ages_is_simple_average() {
    let strategy = TwapStrategy::new(5000);
    let sources = vec![
        make_source("a", dec!(100), dec!(1.0), 1000),
        make_source("b", dec!(200), dec!(1.0), 1000),
        make_source("c", dec!(300), dec!(1.0), 1000),
    ];

    // All age_ms=1000, window=5000 -> weight = 5000-1000 = 4000 for each
    // TWAP = (100*4000 + 200*4000 + 300*4000) / (4000*3) = 200
    let result = strategy.aggregate(&sources).unwrap();
    assert_eq!(
        result.value(),
        dec!(200),
        "TWAP with identical ages should be simple average"
    );
}

#[test]
fn test_median_odd_count_outlier_does_not_change_result() {
    let strategy = MedianStrategy::new();
    let sources = vec![
        make_source("a", dec!(100), dec!(1.0), 0),
        make_source("b", dec!(101), dec!(1.0), 0),
        make_source("c", dec!(102), dec!(1.0), 0),
        make_source("d", dec!(103), dec!(1.0), 0),
        make_source("e", dec!(999), dec!(1.0), 0),
    ];

    let result = strategy.aggregate(&sources).unwrap();
    // Sorted: 100, 101, 102, 103, 999 -> median (index 2) = 102
    assert_eq!(
        result.value(),
        dec!(102),
        "median with odd count should pick middle value, unaffected by outlier"
    );
}

#[test]
fn test_filtered_median_excludes_outlier_matches_unfiltered_without_it() {
    // 5 sources: 100, 101, 102, 103, 999
    // MedianFiltered(100bps) should exclude 999 (huge outlier).
    // Initial median of 5 = 102 (odd, middle).
    // 999 deviates |999-102|/102*10000 = ~87941 bps >> 100.
    // 100 deviates |100-102|/102*10000 = ~196 bps >> 100 bps threshold!
    // Actually 100 bps = 1%, and 100 vs 102 is ~1.96%, so 100 might also be filtered.
    // Let's use a threshold that keeps 100-103 but removes 999.
    // |103-102|/102*10000 = ~98 bps < 200 -> kept
    // |100-102|/102*10000 = ~196 bps < 200 -> kept
    // |999-102|/102*10000 = huge -> removed
    // Use 200 bps threshold.
    let strategy = MedianFilteredStrategy::new(200);
    let sources = vec![
        make_source("a", dec!(100), dec!(1.0), 0),
        make_source("b", dec!(101), dec!(1.0), 0),
        make_source("c", dec!(102), dec!(1.0), 0),
        make_source("d", dec!(103), dec!(1.0), 0),
        make_source("e", dec!(999), dec!(1.0), 0),
    ];

    let result = strategy.aggregate(&sources).unwrap();

    // After removing 999, survivors: [100, 101, 102, 103] (even count)
    // Median of [100, 101, 102, 103] = avg(101, 102) = 101.5
    assert_eq!(
        result.value(),
        dec!(101.5),
        "filtered median should exclude outlier and compute median of remaining"
    );

    // Verify this matches plain median on the non-outlier set.
    let plain_median = MedianStrategy::new();
    let clean_sources = vec![
        make_source("a", dec!(100), dec!(1.0), 0),
        make_source("b", dec!(101), dec!(1.0), 0),
        make_source("c", dec!(102), dec!(1.0), 0),
        make_source("d", dec!(103), dec!(1.0), 0),
    ];
    let plain_result = plain_median.aggregate(&clean_sources).unwrap();
    assert_eq!(
        result.value(),
        plain_result.value(),
        "filtered median result should match plain median on clean set"
    );
}

#[test]
fn test_vwap_heavier_quantity_pulls_price() {
    let strategy = VwapStrategy::new();
    let sources = vec![
        make_source("a", dec!(100), dec!(9), 0),
        make_source("b", dec!(200), dec!(1), 0),
    ];

    // VWAP = (100*9 + 200*1) / (9+1) = (900+200)/10 = 110
    let result = strategy.aggregate(&sources).unwrap();
    assert_eq!(
        result.value(),
        dec!(110),
        "VWAP should be pulled toward heavier-weighted source"
    );
}

#[test]
fn test_twap_recent_price_weighted_more() {
    let strategy = TwapStrategy::new(5000);
    let sources = vec![
        make_source("a", dec!(100), dec!(1.0), 100), // weight = 5000-100 = 4900
        make_source("b", dec!(200), dec!(1.0), 4900), // weight = 5000-4900 = 100
    ];

    // TWAP = (100*4900 + 200*100) / (4900+100) = (490000+20000)/5000 = 510000/5000 = 102
    let result = strategy.aggregate(&sources).unwrap();
    assert_eq!(
        result.value(),
        dec!(102),
        "TWAP should heavily weight the more recent source (age=100ms)"
    );
}
