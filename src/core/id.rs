//! Determinism primitives: id generation and clock, injectable for tests
//! (plan §5 "determinism": seeded ids/clocks make golden fixtures possible).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Correlation id for one submission/event pair.
pub type ReqId = String;

/// Injectable id generator. Tests use [`SeqGen`] for reproducible fixtures.
pub trait IdGen: Send + Sync {
    fn next_id(&self) -> String;
}

pub struct UuidGen;

impl IdGen for UuidGen {
    fn next_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}

impl IdGen for Arc<dyn IdGen> {
    fn next_id(&self) -> String {
        (**self).next_id()
    }
}

/// Deterministic sequential ids: `id-1`, `id-2`, ...
pub struct SeqGen(AtomicU64);

impl SeqGen {
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }
}

impl Default for SeqGen {
    fn default() -> Self {
        Self::new()
    }
}

impl IdGen for SeqGen {
    fn next_id(&self) -> String {
        format!("id-{}", self.0.fetch_add(1, Ordering::Relaxed) + 1)
    }
}

/// Injectable wall clock.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_gen_is_deterministic() {
        let g = SeqGen::new();
        assert_eq!(g.next_id(), "id-1");
        assert_eq!(g.next_id(), "id-2");
    }

    #[test]
    fn uuid_gen_shape() {
        let g = UuidGen;
        let id = g.next_id();
        assert_eq!(id.len(), 36);
        assert_eq!(id.matches('-').count(), 4);
    }
}
