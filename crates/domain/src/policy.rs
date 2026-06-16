//! Políticas puras do domínio: circuit breaker e decisão de criticidade.
//! Sem IO — totalmente testável.

use crate::ports::Criticality;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
}

/// Circuit breaker com backoff exponencial por source.
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    state: BreakerState,
    consecutive_failures: u32,
    /// Falhas consecutivas a partir das quais um source Critical vira fatal.
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

    /// Abre o breaker sem contar como falha (ex.: alvo NotApplicable/Unavailable no boot).
    pub fn trip_open(&mut self) {
        self.state = BreakerState::Open;
    }

    /// Backoff atual = base * 2^(falhas-1), saturado em max_backoff.
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

/// Ação que o supervisor deve tomar após uma falha, dada a criticidade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureAction {
    /// Loga e continua (Optional, ou Critical transitório).
    Degrade,
    /// Falha crítica persistente — encerra o processo (exit != 0).
    Fatal,
}

/// Regra central crítico-vs-opcional. Critical só é fatal acima do threshold
/// (tolera transitórios); Optional nunca derruba o processo.
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
