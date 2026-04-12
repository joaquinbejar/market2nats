use std::time::Instant;

use crate::config::model::CircuitBreakerConfig;

/// Circuit breaker states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Closed,
    Open,
    HalfOpen,
}

/// Circuit breaker for venue connections.
///
/// State machine: Closed → Open → HalfOpen → Closed (or back to Open).
pub struct CircuitBreaker {
    state: State,
    failure_count: u32,
    failure_threshold: u32,
    reset_timeout: std::time::Duration,
    half_open_max_requests: u32,
    half_open_successes: u32,
    last_failure_at: Option<Instant>,
}

impl CircuitBreaker {
    /// Creates a new circuit breaker from config.
    #[must_use]
    pub fn new(config: &CircuitBreakerConfig) -> Self {
        Self {
            state: State::Closed,
            failure_count: 0,
            failure_threshold: config.failure_threshold,
            reset_timeout: std::time::Duration::from_secs(config.reset_timeout_secs),
            half_open_max_requests: config.half_open_max_requests,
            half_open_successes: 0,
            last_failure_at: None,
        }
    }

    /// Creates a circuit breaker that is always closed (no-op).
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            state: State::Closed,
            failure_count: 0,
            failure_threshold: u32::MAX,
            reset_timeout: std::time::Duration::from_secs(60),
            half_open_max_requests: 1,
            half_open_successes: 0,
            last_failure_at: None,
        }
    }

    /// Returns whether a request is currently allowed.
    #[must_use]
    pub fn is_allowed(&mut self) -> bool {
        match self.state {
            State::Closed => true,
            State::Open => {
                if let Some(last) = self.last_failure_at {
                    if last.elapsed() >= self.reset_timeout {
                        self.state = State::HalfOpen;
                        self.half_open_successes = 0;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            State::HalfOpen => self.half_open_successes < self.half_open_max_requests,
        }
    }

    /// Records a successful request.
    pub fn record_success(&mut self) {
        match self.state {
            State::Closed => {
                self.failure_count = 0;
            }
            State::HalfOpen => {
                self.half_open_successes = self.half_open_successes.saturating_add(1);
                if self.half_open_successes >= self.half_open_max_requests {
                    self.state = State::Closed;
                    self.failure_count = 0;
                }
            }
            State::Open => {}
        }
    }

    /// Records a failed request.
    pub fn record_failure(&mut self) {
        self.last_failure_at = Some(Instant::now());
        match self.state {
            State::Closed => {
                self.failure_count = self.failure_count.saturating_add(1);
                if self.failure_count >= self.failure_threshold {
                    self.state = State::Open;
                }
            }
            State::HalfOpen => {
                self.state = State::Open;
                self.half_open_successes = 0;
            }
            State::Open => {}
        }
    }

    /// Returns whether the circuit is currently open.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.state == State::Open
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(threshold: u32) -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            failure_threshold: threshold,
            reset_timeout_secs: 1,
            half_open_max_requests: 2,
        }
    }

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let mut cb = CircuitBreaker::new(&test_config(3));
        assert!(!cb.is_open());
        assert!(cb.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let mut cb = CircuitBreaker::new(&test_config(3));
        cb.record_failure();
        cb.record_failure();
        assert!(!cb.is_open());
        cb.record_failure();
        assert!(cb.is_open());
        assert!(!cb.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_success_resets_count() {
        let mut cb = CircuitBreaker::new(&test_config(3));
        cb.record_failure();
        cb.record_failure();
        cb.record_success();
        cb.record_failure();
        cb.record_failure();
        assert!(!cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_half_open_closes_on_success() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            reset_timeout_secs: 0, // Immediate reset for testing
            half_open_max_requests: 2,
        };
        let mut cb = CircuitBreaker::new(&config);
        cb.record_failure();
        assert!(cb.is_open());

        // Should transition to half-open immediately (reset_timeout = 0)
        assert!(cb.is_allowed());
        cb.record_success();
        cb.record_success();
        assert!(!cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_half_open_reopens_on_failure() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            reset_timeout_secs: 0,
            half_open_max_requests: 2,
        };
        let mut cb = CircuitBreaker::new(&config);
        cb.record_failure();
        assert!(cb.is_open());

        assert!(cb.is_allowed()); // transitions to half-open
        cb.record_failure();
        assert!(cb.is_open());
    }

    #[test]
    fn test_disabled_circuit_breaker_never_opens() {
        let mut cb = CircuitBreaker::disabled();
        for _ in 0..1000 {
            cb.record_failure();
        }
        assert!(!cb.is_open());
        assert!(cb.is_allowed());
    }
}
