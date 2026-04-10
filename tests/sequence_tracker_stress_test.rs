//! Stress tests for SequenceTracker under concurrent access.
//!
//! Does NOT require NATS.

use std::collections::HashSet;
use std::sync::Arc;

use market2nats::application::SequenceTracker;
use market2nats::domain::{InstrumentId, MarketDataType, VenueId};

/// Test that sequences are unique under single-threaded access.
#[test]
fn test_sequence_uniqueness_single_thread() {
    let tracker = SequenceTracker::new();
    let venue = VenueId::try_new("binance").unwrap();
    let instrument = InstrumentId::try_new("BTCUSDT").unwrap();
    let dt = MarketDataType::Trade;

    let mut seen = HashSet::new();
    for _ in 0..10_000 {
        let seq = tracker.next_sequence(&venue, &instrument, dt).unwrap();
        assert!(
            seen.insert(seq.value()),
            "duplicate sequence: {}",
            seq.value()
        );
    }

    assert_eq!(seen.len(), 10_000);
}

/// Test that sequences are monotonically increasing.
#[test]
fn test_sequence_monotonic() {
    let tracker = SequenceTracker::new();
    let venue = VenueId::try_new("binance").unwrap();
    let instrument = InstrumentId::try_new("BTCUSDT").unwrap();
    let dt = MarketDataType::Trade;

    let mut prev = 0u64;
    for _ in 0..10_000 {
        let seq = tracker.next_sequence(&venue, &instrument, dt).unwrap();
        assert!(
            seq.value() > prev,
            "non-monotonic: prev={prev}, current={}",
            seq.value()
        );
        prev = seq.value();
    }
}

/// Test concurrent access from multiple threads — no duplicates or gaps.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_sequence_concurrent_no_duplicates() {
    let tracker = Arc::new(SequenceTracker::new());
    let venue = VenueId::try_new("binance").unwrap();
    let instrument = InstrumentId::try_new("BTCUSDT").unwrap();
    let dt = MarketDataType::Trade;

    let per_task = 5_000u64;
    let num_tasks = 8;

    let mut handles = Vec::new();
    for _ in 0..num_tasks {
        let tracker = Arc::clone(&tracker);
        let venue = venue.clone();
        let instrument = instrument.clone();

        handles.push(tokio::spawn(async move {
            let mut sequences = Vec::with_capacity(per_task as usize);
            for _ in 0..per_task {
                let seq = tracker.next_sequence(&venue, &instrument, dt).unwrap();
                sequences.push(seq.value());
            }
            sequences
        }));
    }

    let mut all_sequences = Vec::new();
    for handle in handles {
        all_sequences.extend(handle.await.unwrap());
    }

    let total = (per_task * num_tasks) as usize;
    assert_eq!(all_sequences.len(), total);

    // Check uniqueness.
    let unique: HashSet<u64> = all_sequences.iter().copied().collect();
    assert_eq!(
        unique.len(),
        total,
        "found {} duplicates in {total} sequences",
        total - unique.len()
    );

    // Check no gaps: the set should be {1, 2, ..., total}.
    let min = *unique.iter().min().unwrap();
    let max = *unique.iter().max().unwrap();
    assert_eq!(min, 1);
    assert_eq!(max, total as u64);
}

/// Test independent streams don't interfere with each other under concurrency.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sequence_concurrent_independent_streams() {
    let tracker = Arc::new(SequenceTracker::new());

    let streams = vec![
        ("binance", "BTCUSDT", MarketDataType::Trade),
        ("binance", "ETHUSDT", MarketDataType::Trade),
        ("binance", "BTCUSDT", MarketDataType::Ticker),
        ("kraken", "BTCUSD", MarketDataType::Trade),
    ];

    let per_stream = 2_500u64;
    let mut handles = Vec::new();

    for (venue_str, instr_str, dt) in &streams {
        let tracker = Arc::clone(&tracker);
        let venue = VenueId::try_new(*venue_str).unwrap();
        let instrument = InstrumentId::try_new(*instr_str).unwrap();
        let dt = *dt;

        handles.push(tokio::spawn(async move {
            let mut sequences = Vec::with_capacity(per_stream as usize);
            for _ in 0..per_stream {
                let seq = tracker.next_sequence(&venue, &instrument, dt).unwrap();
                sequences.push(seq.value());
            }
            sequences
        }));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        let sequences = handle.await.unwrap();
        assert_eq!(sequences.len(), per_stream as usize);

        // Each stream should have sequences 1..=per_stream.
        let unique: HashSet<u64> = sequences.iter().copied().collect();
        assert_eq!(
            unique.len(),
            per_stream as usize,
            "stream {i} has duplicates"
        );

        let min = *unique.iter().min().unwrap();
        let max = *unique.iter().max().unwrap();
        assert_eq!(min, 1, "stream {i} min should be 1");
        assert_eq!(max, per_stream, "stream {i} max should be {per_stream}");
    }
}

/// Test that current_sequence reflects the latest assigned value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_current_sequence_after_concurrent_writes() {
    let tracker = Arc::new(SequenceTracker::new());
    let venue = VenueId::try_new("binance").unwrap();
    let instrument = InstrumentId::try_new("BTCUSDT").unwrap();
    let dt = MarketDataType::Trade;

    let total = 10_000u64;
    let num_tasks = 4;
    let per_task = total / num_tasks;

    let mut handles = Vec::new();
    for _ in 0..num_tasks {
        let tracker = Arc::clone(&tracker);
        let venue = venue.clone();
        let instrument = instrument.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..per_task {
                let _ = tracker.next_sequence(&venue, &instrument, dt).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let current = tracker.current_sequence(&venue, &instrument, dt);
    assert_eq!(
        current.value(),
        total,
        "current_sequence should reflect total assigned: expected {total}, got {}",
        current.value()
    );
}

/// Test large number of independent stream keys.
#[test]
fn test_many_stream_keys() {
    let tracker = SequenceTracker::new();
    let venue = VenueId::try_new("binance").unwrap();

    // Create 100 different instruments, each with 100 sequences.
    for i in 0..100 {
        let instrument = InstrumentId::try_new(format!("INST_{i}")).unwrap();
        for _ in 0..100 {
            tracker
                .next_sequence(&venue, &instrument, MarketDataType::Trade)
                .unwrap();
        }
    }

    // Verify each instrument's current sequence.
    for i in 0..100 {
        let instrument = InstrumentId::try_new(format!("INST_{i}")).unwrap();
        let current = tracker.current_sequence(&venue, &instrument, MarketDataType::Trade);
        assert_eq!(current.value(), 100, "instrument INST_{i} should be at 100");
    }
}
