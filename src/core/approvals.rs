//! Approval board: pending reverse-requests waiting for an `Op::Approval`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::oneshot;

use super::ops::ApprovalDecision;

#[derive(Default)]
pub struct ApprovalBoard {
    seq: AtomicU64,
    waiters: Mutex<HashMap<String, oneshot::Sender<ApprovalDecision>>>,
}

impl ApprovalBoard {
    /// Open a waiter under an externally generated id (engine IdGen).
    pub fn open_with(&self, req: String) -> oneshot::Receiver<ApprovalDecision> {
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().unwrap().insert(req.clone(), tx);
        rx
    }

    /// Convenience: board-local sequential id.
    pub fn open(&self) -> (String, oneshot::Receiver<ApprovalDecision>) {
        let req = format!("approval-{}", self.seq.fetch_add(1, Ordering::Relaxed) + 1);
        let rx = self.open_with(req.clone());
        (req, rx)
    }

    pub fn complete(&self, req: &str, decision: ApprovalDecision) -> bool {
        self.waiters
            .lock()
            .unwrap()
            .remove(req)
            .map(|tx| tx.send(decision))
            .is_some()
    }

    /// Called when a turn ends so stale waiters never leak.
    pub fn drop_pending(&self) {
        self.waiters.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_complete_roundtrip() {
        let board = ApprovalBoard::default();
        let (req, rx) = board.open();
        assert!(board.complete(&req, ApprovalDecision::AllowOnce));
        assert_eq!(rx.await.ok(), Some(ApprovalDecision::AllowOnce));
    }

    #[tokio::test]
    async fn complete_unknown_is_false() {
        let board = ApprovalBoard::default();
        assert!(!board.complete("nope", ApprovalDecision::Deny));
    }
}
