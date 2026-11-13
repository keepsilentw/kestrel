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
        let Some(tx) = self.waiters.lock().unwrap().remove(req) else {
            return false;
        };
        tx.send(decision).is_ok()
    }

    /// Drop a waiter WITHOUT sending: the approval window elapsed, so nobody is
    /// listening any more. Without this the timed-out oneshot stays in the map
    /// and a late `complete` reports success — which the CLI layer then books as
    /// an applied approval in the artifact, while the tool already returned an
    /// error to the model. A receipt that lies is worse than no receipt.
    pub fn abandon(&self, req: &str) -> bool {
        self.waiters.lock().unwrap().remove(req).is_some()
    }

    /// Is anyone still waiting on this request?
    pub fn has_waiter(&self, req: &str) -> bool {
        self.waiters.lock().unwrap().contains_key(req)
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

    #[tokio::test]
    async fn complete_reports_a_dropped_waiter() {
        let board = ApprovalBoard::default();
        let (req, rx) = board.open();
        drop(rx);
        assert!(
            !board.complete(&req, ApprovalDecision::AllowOnce),
            "removing a sender is not enough: nobody received the decision"
        );
    }

    /// 到点之后没人听：`abandon` 必须让后来的批复变成「没有生效」，而不是
    /// 假装唤醒了一个已经消失的等待者。
    #[tokio::test]
    async fn abandon_makes_a_late_reply_report_no_waiter() {
        let board = ApprovalBoard::default();
        let (req, _rx) = board.open();
        assert!(board.has_waiter(&req));
        // 模拟丢弃 rx（等待者消失）+ 到点撤摊：真实路径是 timeout 那一条
        assert!(board.abandon(&req), "撤摊要说成功撤了一个");
        assert!(!board.has_waiter(&req));
        assert!(
            !board.complete(&req, ApprovalDecision::AllowOnce),
            "撤摊之后的批复不能算生效"
        );
        assert!(!board.abandon(&req), "撤两次是假的：第二次没有等待者");
    }
}
