use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::domain::{InstrumentId, MarketDataType, Sequence, VenueId};

/// Key for the per-stream sequence counter.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamKey {
    venue: String,
    instrument: String,
    data_type: MarketDataType,
}

/// Assigns monotonically increasing sequence numbers per (venue, instrument, data_type).
///
/// Thread-safe: uses `DashMap` + `AtomicU64` for lock-free concurrent access.
pub struct SequenceTracker {
    counters: DashMap<StreamKey, AtomicU64>,
}

impl SequenceTracker {
    /// Creates a new `SequenceTracker`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
        }
    }

    /// Returns the next sequence number for the given stream triple.
    ///
    /// The first call for a given key returns `Sequence(1)`.
    ///
    /// # Errors
    ///
    /// Returns `DomainError::SequenceError` if the counter overflows `u64::MAX`.
    pub fn next_sequence(
        &self,
        venue: &VenueId,
        instrument: &InstrumentId,
        data_type: MarketDataType,
    ) -> Result<Sequence, crate::domain::DomainError> {
        let key = StreamKey {
            venue: venue.as_str().to_owned(),
            instrument: instrument.as_str().to_owned(),
            data_type,
        };

        let entry = self
            .counters
            .entry(key)
            .or_insert_with(|| AtomicU64::new(0));
        let prev = entry.value().fetch_add(1, Ordering::Relaxed);

        // Detect overflow: if prev was u64::MAX, fetch_add wrapped to 0.
        if prev == u64::MAX {
            return Err(crate::domain::DomainError::SequenceError(
                "sequence counter overflow".to_owned(),
            ));
        }

        let seq = prev.checked_add(1).ok_or_else(|| {
            crate::domain::DomainError::SequenceError("sequence overflow".to_owned())
        })?;

        Ok(Sequence::new(seq))
    }

    /// Returns the current sequence number for a given stream triple, or 0 if not yet tracked.
    #[must_use]
    pub fn current_sequence(
        &self,
        venue: &VenueId,
        instrument: &InstrumentId,
        data_type: MarketDataType,
    ) -> Sequence {
        let key = StreamKey {
            venue: venue.as_str().to_owned(),
            instrument: instrument.as_str().to_owned(),
            data_type,
        };

        self.counters
            .get(&key)
            .map(|entry| Sequence::new(entry.value().load(Ordering::Relaxed)))
            .unwrap_or(Sequence::new(0))
    }
}

impl Default for SequenceTracker {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_tracker_increments() {
        let tracker = SequenceTracker::new();
        let venue = VenueId::try_new("binance").unwrap();
        let instrument = InstrumentId::try_new("BTCUSDT").unwrap();
        let dt = MarketDataType::Trade;

        let s1 = tracker.next_sequence(&venue, &instrument, dt).unwrap();
        let s2 = tracker.next_sequence(&venue, &instrument, dt).unwrap();
        let s3 = tracker.next_sequence(&venue, &instrument, dt).unwrap();

        assert_eq!(s1.value(), 1);
        assert_eq!(s2.value(), 2);
        assert_eq!(s3.value(), 3);
    }

    #[test]
    fn test_sequence_tracker_independent_streams() {
        let tracker = SequenceTracker::new();
        let venue = VenueId::try_new("binance").unwrap();
        let btc = InstrumentId::try_new("BTCUSDT").unwrap();
        let eth = InstrumentId::try_new("ETHUSDT").unwrap();

        let s1 = tracker
            .next_sequence(&venue, &btc, MarketDataType::Trade)
            .unwrap();
        let s2 = tracker
            .next_sequence(&venue, &eth, MarketDataType::Trade)
            .unwrap();

        assert_eq!(s1.value(), 1);
        assert_eq!(s2.value(), 1);
    }

    #[test]
    fn test_current_sequence_default_zero() {
        let tracker = SequenceTracker::new();
        let venue = VenueId::try_new("binance").unwrap();
        let instrument = InstrumentId::try_new("BTCUSDT").unwrap();

        let seq = tracker.current_sequence(&venue, &instrument, MarketDataType::Trade);
        assert_eq!(seq.value(), 0);
    }

    #[test]
    fn test_current_sequence_after_increment() {
        let tracker = SequenceTracker::new();
        let venue = VenueId::try_new("binance").unwrap();
        let instrument = InstrumentId::try_new("BTCUSDT").unwrap();
        let dt = MarketDataType::Trade;

        let _ = tracker.next_sequence(&venue, &instrument, dt).unwrap();
        let _ = tracker.next_sequence(&venue, &instrument, dt).unwrap();

        let current = tracker.current_sequence(&venue, &instrument, dt);
        assert_eq!(current.value(), 2);
    }
}
