//! Retry transport (地基件 5 的重试半边): wraps any Transport with a policy
//! mirroring provider-retry.ts — honor `retry-after` and `x-should-retry`,
//! retry 408/429/5xx, cap total waiting at 60s, exponential backoff otherwise.
//! Exhaustion surfaces as Err; the turn loop turns it into a terminal
//! `StopReason::Error` stream event (errors never become exceptions).

use std::sync::Arc;
use std::time::Duration;

use super::wire::{ByteStream, RequestSpec, Transport, WireError};

pub struct RetryTransport {
    inner: Arc<dyn Transport>,
    /// Total wall-clock budget for waiting across attempts (provider-retry 60s 上限).
    pub max_wait: Duration,
    pub max_attempts: u32,
}

impl RetryTransport {
    pub fn new(inner: Arc<dyn Transport>) -> Self {
        Self {
            inner,
            max_wait: Duration::from_secs(60),
            max_attempts: 4,
        }
    }

    fn retryable(err: &WireError) -> bool {
        match err {
            WireError::BadResponse {
                status,
                x_should_retry,
                ..
            } => {
                x_should_retry == &Some(true)
                    || matches!(status, 408 | 409 | 429 | 500 | 502 | 503 | 504)
            }
            WireError::Transport(_) => true, // connection-level: worth one more try
            _ => false,
        }
    }

    fn delay(err: &WireError, attempt: u32) -> Duration {
        if let WireError::BadResponse {
            retry_after_ms: Some(ms),
            ..
        } = err
        {
            return Duration::from_millis(*ms);
        }
        Duration::from_millis(500u64.saturating_mul(1 << attempt.min(4)))
    }
}

impl Transport for RetryTransport {
    fn send(
        &self,
        spec: RequestSpec,
    ) -> futures::future::BoxFuture<'static, Result<ByteStream, WireError>> {
        let inner = Arc::clone(&self.inner);
        let max_wait = self.max_wait;
        let max_attempts = self.max_attempts;
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + max_wait;
            let mut attempt = 0u32;
            loop {
                attempt += 1;
                match inner.send(spec.clone()).await {
                    Ok(bytes) => return Ok(bytes),
                    Err(e) if attempt < max_attempts && Self::retryable(&e) => {
                        let wait = Self::delay(&e, attempt);
                        if tokio::time::Instant::now() + wait > deadline {
                            return Err(WireError::Transport(format!(
                                "retry budget (60s) exhausted after {attempt} attempts: {e}"
                            )));
                        }
                        tokio::time::sleep(wait).await;
                    }
                    Err(e) => return Err(e),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct FlakyTransport {
        fails: AtomicU32,
        status: u16,
        retry_after_ms: Option<u64>,
        x_should_retry: Option<bool>,
    }

    impl Transport for FlakyTransport {
        fn send(
            &self,
            _spec: RequestSpec,
        ) -> futures::future::BoxFuture<'static, Result<ByteStream, WireError>> {
            let left = self.fails.load(Ordering::Relaxed);
            if left > 0 {
                self.fails.store(left - 1, Ordering::Relaxed);
            }
            let status = self.status;
            let retry_after_ms = self.retry_after_ms;
            let x_should_retry = self.x_should_retry;
            Box::pin(async move {
                if left > 0 {
                    Err(WireError::BadResponse {
                        status,
                        body: "flaky".into(),
                        retry_after_ms,
                        x_should_retry,
                    })
                } else {
                    Ok(Box::pin(stream::empty()) as ByteStream)
                }
            })
        }
    }

    fn spec() -> RequestSpec {
        RequestSpec {
            url: "http://x".into(),
            headers: vec![],
            body: serde_json::json!(null),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retries_429_then_succeeds() {
        let inner = Arc::new(FlakyTransport {
            fails: AtomicU32::new(2),
            status: 429,
            retry_after_ms: Some(10),
            x_should_retry: None,
        });
        let counter = Arc::clone(&inner);
        let rt = RetryTransport::new(inner);
        let _ = rt.send(spec()).await.expect("should succeed after retries");
        // both injected failures consumed = retry-after honored (paused clock
        // advances sleeps instantly; real elapsed assertions are meaningless here)
        assert_eq!(counter.fails.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn x_should_retry_forces_retry_on_odd_status() {
        let inner = Arc::new(FlakyTransport {
            fails: AtomicU32::new(1),
            status: 418, // teapot, normally not retryable
            retry_after_ms: None,
            x_should_retry: Some(true),
        });
        let rt = RetryTransport::new(inner);
        let _ = rt.send(spec()).await.expect("x-should-retry honored");
    }

    #[tokio::test(start_paused = true)]
    async fn unauthorized_fails_immediately() {
        let inner = Arc::new(FlakyTransport {
            fails: AtomicU32::new(99),
            status: 401,
            retry_after_ms: None,
            x_should_retry: None,
        });
        let counter = Arc::clone(&inner);
        let rt = RetryTransport::new(inner);
        let err = match rt.send(spec()).await {
            Err(e) => e,
            Ok(_) => panic!("401 must not retry"),
        };
        assert!(matches!(err, WireError::BadResponse { status: 401, .. }));
        assert_eq!(
            counter.fails.load(Ordering::Relaxed),
            98,
            "exactly one attempt consumed (no retry)"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_budget_capped() {
        let inner = Arc::new(FlakyTransport {
            fails: AtomicU32::new(999),
            status: 503,
            retry_after_ms: Some(30_000),
            x_should_retry: None,
        });
        let mut rt = RetryTransport::new(inner);
        rt.max_attempts = 100;
        let err = match rt.send(spec()).await {
            Err(e) => e,
            Ok(_) => panic!("budget must exhaust"),
        };
        assert!(err.to_string().contains("retry budget"));
    }
}
