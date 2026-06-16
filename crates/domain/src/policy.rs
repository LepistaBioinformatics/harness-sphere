//! Pure domain policies: circuit breaker and criticality decision.
//! No IO — fully testable.

use crate::ports::Criticality;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
}

/// Circuit breaker with per-source exponential backoff.
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    state: BreakerState,
    consecutive_failures: u32,
    /// Consecutive failures at which a Critical source becomes fatal.
    crit_threshold: u32,
    base_backoff: Duration,
    max_backoff: Duration,
}

impl CircuitBreaker {
    pub fn new(crit_threshold: u32) -> Self {
        CircuitBreaker {
            state: BreakerState::Closed,
            consecutive_failures: 0,
            crit_threshold,
            base_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
        }
    }

    pub fn state(&self) -> BreakerState {
        self.state
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    pub fn is_open(&self) -> bool {
        self.state == BreakerState::Open
    }

    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.state = BreakerState::Closed;
    }

    pub fn record_failure(&mut self) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.state = BreakerState::Open;
    }

    /// Opens the breaker without counting as a failure (e.g. NotApplicable/Unavailable target at boot).
    pub fn trip_open(&mut self) {
        self.state = BreakerState::Open;
    }

    /// Current backoff = base * 2^(failures-1), saturated at max_backoff.
    pub fn backoff(&self) -> Duration {
        if self.consecutive_failures == 0 {
            return self.base_backoff;
        }
        let shift = self.consecutive_failures.min(16) - 1;
        let factor = 1u64 << shift;
        self.base_backoff
            .saturating_mul(factor as u32)
            .min(self.max_backoff)
    }
}

/// Action the supervisor must take after a failure, given the criticality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureAction {
    /// Log and continue (Optional, or transient Critical).
    Degrade,
    /// Persistent critical failure — terminate the process (exit != 0).
    Fatal,
}

/// Central critical-vs-optional rule. Critical is only fatal above the threshold
/// (tolerates transients); Optional never brings the process down.
pub fn classify_failure(criticality: Criticality, breaker: &CircuitBreaker) -> FailureAction {
    match criticality {
        Criticality::Optional => FailureAction::Degrade,
        Criticality::Critical => {
            if breaker.consecutive_failures() >= breaker.crit_threshold {
                FailureAction::Fatal
            } else {
                FailureAction::Degrade
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_never_fatal() {
        let mut b = CircuitBreaker::new(3);
        for _ in 0..100 {
            b.record_failure();
        }
        assert_eq!(
            classify_failure(Criticality::Optional, &b),
            FailureAction::Degrade
        );
    }

    #[test]
    fn critical_tolerates_transient_then_fatal() {
        let mut b = CircuitBreaker::new(3);
        b.record_failure();
        b.record_failure();
        assert_eq!(
            classify_failure(Criticality::Critical, &b),
            FailureAction::Degrade
        );
        b.record_failure();
        assert_eq!(
            classify_failure(Criticality::Critical, &b),
            FailureAction::Fatal
        );
    }

    #[test]
    fn success_resets() {
        let mut b = CircuitBreaker::new(3);
        b.record_failure();
        b.record_failure();
        b.record_success();
        assert_eq!(b.consecutive_failures(), 0);
        assert!(!b.is_open());
    }

    #[test]
    fn backoff_grows_and_saturates() {
        let mut b = CircuitBreaker::new(3);
        assert_eq!(b.backoff(), Duration::from_secs(1));
        b.record_failure(); // 1 -> 1s
        b.record_failure(); // 2 -> 2s
        assert_eq!(b.backoff(), Duration::from_secs(2));
        for _ in 0..20 {
            b.record_failure();
        }
        assert_eq!(b.backoff(), Duration::from_secs(60));
    }
}
