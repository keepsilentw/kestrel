//! Cancellation discipline (codex study, "取消三件套"):
//! every await crosses `or_cancel`; child tokens fork from the root;
//! spawned tasks are wrapped in `AbortOnDropHandle`.

use futures::Stream;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

/// Poll the next item, or `None` if the token fires first.
/// The canonical way to await a stream item inside the engine.
pub async fn next_or_cancel<S>(stream: &mut S, token: &CancellationToken) -> Option<S::Item>
where
    S: Stream + Unpin,
{
    tokio::select! {
        biased;
        _ = token.cancelled() => None,
        item = stream.next() => item,
    }
}

/// Sleep, or return early (`false`) when cancelled.
pub async fn sleep_or_cancel(d: std::time::Duration, token: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = token.cancelled() => false,
        _ = tokio::time::sleep(d) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn next_or_cancel_wins_on_cancel() {
        let token = CancellationToken::new();
        let child = token.child_token();
        let mut s = stream::pending::<u8>();
        token.cancel();
        assert_eq!(next_or_cancel(&mut s, &child).await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn next_or_cancel_yields_items_normally() {
        let token = CancellationToken::new();
        let mut s = stream::iter(vec![1u8, 2, 3]);
        assert_eq!(next_or_cancel(&mut s, &token).await, Some(1));
        assert_eq!(next_or_cancel(&mut s, &token).await, Some(2));
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_or_cancel_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(!sleep_or_cancel(Duration::from_secs(10), &token).await);
    }
}
