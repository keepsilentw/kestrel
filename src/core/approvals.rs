//! Approval board: pending reverse-requests waiting for an `Op::Approval`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::oneshot;

use super::ops::{ApprovalCompletion, ApprovalDecision};

pub const DEFAULT_DEFERRED_APPROVAL_TTL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone)]
struct ExpiredRequest {
    detail: Value,
    expires_at: Instant,
}

struct DeferredApproval {
    source_req: String,
    decision: ApprovalDecision,
    expires_at: Instant,
    detail: Value,
}

#[derive(Serialize, Deserialize)]
struct StoredApprovalState {
    version: u32,
    expired: Vec<StoredExpired>,
    deferred: Vec<StoredDeferred>,
}

#[derive(Serialize, Deserialize)]
struct StoredExpired {
    req: String,
    detail: Value,
    expires_unix_secs: u64,
}

#[derive(Serialize, Deserialize)]
struct StoredDeferred {
    source_req: String,
    decision: ApprovalDecision,
    expires_unix_secs: u64,
    detail: Value,
}

pub struct ApprovalBoard {
    seq: AtomicU64,
    waiters: Mutex<HashMap<String, oneshot::Sender<ApprovalDecision>>>,
    requests: Mutex<HashMap<String, Value>>,
    expired: Mutex<HashMap<String, ExpiredRequest>>,
    deferred: Mutex<HashMap<String, DeferredApproval>>,
    deferred_ttl: Duration,
    store_path: Mutex<Option<PathBuf>>,
}

impl Default for ApprovalBoard {
    fn default() -> Self {
        Self {
            seq: AtomicU64::new(0),
            waiters: Mutex::new(HashMap::new()),
            requests: Mutex::new(HashMap::new()),
            expired: Mutex::new(HashMap::new()),
            deferred: Mutex::new(HashMap::new()),
            deferred_ttl: DEFAULT_DEFERRED_APPROVAL_TTL,
            store_path: Mutex::new(None),
        }
    }
}

impl ApprovalBoard {
    pub fn with_deferred_ttl(deferred_ttl: Duration) -> Self {
        Self {
            deferred_ttl,
            ..Self::default()
        }
    }

    /// Restore deferred approval state from a task-specific file. A missing
    /// file is a normal first start; an unreadable file is returned as an error
    /// so the caller can surface it instead of silently discarding approvals.
    pub fn with_store(path: PathBuf) -> anyhow::Result<Self> {
        let board = Self {
            store_path: Mutex::new(Some(path.clone())),
            ..Self::default()
        };
        if !path.exists() {
            return Ok(board);
        }
        let bytes = std::fs::read(&path)
            .map_err(|error| anyhow::anyhow!("read approval store {}: {error}", path.display()))?;
        let stored: StoredApprovalState = serde_json::from_slice(&bytes)
            .map_err(|error| anyhow::anyhow!("parse approval store {}: {error}", path.display()))?;
        if stored.version != 1 {
            anyhow::bail!(
                "unsupported approval store version {} in {}",
                stored.version,
                path.display()
            );
        }
        let now = Instant::now();
        let now_unix = unix_secs(SystemTime::now())?;
        for item in stored.expired {
            if item.expires_unix_secs > now_unix {
                let remaining = Duration::from_secs(item.expires_unix_secs - now_unix);
                board.expired.lock().unwrap().insert(
                    item.req,
                    ExpiredRequest {
                        detail: item.detail,
                        expires_at: now + remaining,
                    },
                );
            }
        }
        for item in stored.deferred {
            if item.expires_unix_secs > now_unix {
                let remaining = Duration::from_secs(item.expires_unix_secs - now_unix);
                board.deferred.lock().unwrap().insert(
                    approval_key(&item.detail),
                    DeferredApproval {
                        source_req: item.source_req,
                        decision: item.decision,
                        expires_at: now + remaining,
                        detail: item.detail,
                    },
                );
            }
        }
        Ok(board)
    }

    /// Open a waiter under an externally generated id (engine IdGen).
    pub fn open_with(&self, req: String) -> oneshot::Receiver<ApprovalDecision> {
        self.open_with_detail(req, Value::Null)
    }

    /// Open a waiter and retain the exact tool+args payload that a late
    /// decision may be authorized against.
    pub fn open_with_detail(
        &self,
        req: String,
        detail: Value,
    ) -> oneshot::Receiver<ApprovalDecision> {
        let (tx, rx) = oneshot::channel();
        self.requests.lock().unwrap().insert(req.clone(), detail);
        self.waiters.lock().unwrap().insert(req.clone(), tx);
        rx
    }

    /// Convenience: board-local sequential id.
    pub fn open(&self) -> (String, oneshot::Receiver<ApprovalDecision>) {
        let req = format!("approval-{}", self.seq.fetch_add(1, Ordering::Relaxed) + 1);
        let rx = self.open_with(req.clone());
        (req, rx)
    }

    pub fn complete(&self, req: &str, decision: ApprovalDecision) -> ApprovalCompletion {
        if let Some(tx) = self.waiters.lock().unwrap().remove(req) {
            self.requests.lock().unwrap().remove(req);
            return if tx.send(decision).is_ok() {
                ApprovalCompletion::Applied
            } else {
                ApprovalCompletion::Stale
            };
        }

        let expired = {
            let mut expired = self.expired.lock().unwrap();
            let Some(entry) = expired.get(req) else {
                return ApprovalCompletion::Stale;
            };
            if Instant::now() > entry.expires_at {
                expired.remove(req);
                drop(expired);
                self.persist_best_effort("expire suspended request");
                return ApprovalCompletion::Stale;
            }
            entry.clone()
        };
        let key = approval_key(&expired.detail);
        {
            let mut deferred = self.deferred.lock().unwrap();
            deferred
                .entry(key)
                .and_modify(|slot| {
                    // A contradictory replay of the same decision must fail closed.
                    if decision == ApprovalDecision::Deny {
                        slot.decision = ApprovalDecision::Deny;
                    }
                })
                .or_insert(DeferredApproval {
                    source_req: req.to_string(),
                    decision,
                    expires_at: expired.expires_at,
                    detail: expired.detail,
                });
        }
        self.persist_best_effort("defer decision");
        ApprovalCompletion::Deferred
    }

    /// The approval window elapsed. Drop the waiter without sending, but retain
    /// the exact detail for a bounded one-use future slot.
    pub fn suspend(&self, req: &str) -> bool {
        let removed = self.waiters.lock().unwrap().remove(req).is_some();
        if !removed {
            return false;
        }
        if let Some(detail) = self.requests.lock().unwrap().remove(req) {
            {
                self.expired.lock().unwrap().insert(
                    req.to_string(),
                    ExpiredRequest {
                        detail,
                        expires_at: Instant::now() + self.deferred_ttl,
                    },
                );
            }
            self.persist_best_effort("suspend request");
        }
        true
    }

    /// The turn was cancelled or failed. Drop the waiter and its detail without
    /// creating a future approval slot.
    pub fn abandon(&self, req: &str) -> bool {
        let removed = self.waiters.lock().unwrap().remove(req).is_some();
        self.requests.lock().unwrap().remove(req);
        self.expired.lock().unwrap().remove(req);
        self.persist_best_effort("discard request");
        removed
    }

    /// Is anyone still waiting on this request?
    pub fn has_waiter(&self, req: &str) -> bool {
        self.waiters.lock().unwrap().contains_key(req)
    }

    /// Called when a turn ends so stale waiters never leak.
    pub fn drop_pending(&self) {
        self.waiters.lock().unwrap().clear();
        self.requests.lock().unwrap().clear();
    }

    /// Consume one decision for an exact tool+args payload. Matching is a
    /// canonical JSON comparison, not a prefix, path pattern or tool name.
    pub fn take_deferred(&self, detail: &Value) -> Option<(String, ApprovalDecision)> {
        let now = Instant::now();
        let key = approval_key(detail);
        let slot = {
            let mut deferred = self.deferred.lock().unwrap();
            deferred.retain(|_, slot| slot.expires_at >= now);
            deferred.remove(&key)?
        };
        // A duplicate reply for the source request must not recreate the slot
        // after it has been consumed once.
        self.expired
            .lock()
            .unwrap()
            .retain(|_, request| request.expires_at >= now && approval_key(&request.detail) != key);
        self.persist_best_effort("consume deferred request");
        Some((slot.source_req, slot.decision))
    }

    fn persist_best_effort(&self, operation: &str) {
        if let Err(error) = self.persist() {
            tracing::error!("approval store failed during {operation}: {error}");
        }
    }

    fn persist(&self) -> anyhow::Result<()> {
        let Some(path) = self.store_path.lock().unwrap().clone() else {
            return Ok(());
        };
        let now_system = SystemTime::now();
        let now_instant = Instant::now();
        let expired = self
            .expired
            .lock()
            .unwrap()
            .iter()
            .map(|(req, item)| {
                Ok(StoredExpired {
                    req: req.clone(),
                    detail: item.detail.clone(),
                    expires_unix_secs: unix_secs(
                        now_system + item.expires_at.saturating_duration_since(now_instant),
                    )?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let deferred = self
            .deferred
            .lock()
            .unwrap()
            .values()
            .map(|item| {
                Ok(StoredDeferred {
                    source_req: item.source_req.clone(),
                    decision: item.decision,
                    detail: item.detail.clone(),
                    expires_unix_secs: unix_secs(
                        now_system + item.expires_at.saturating_duration_since(now_instant),
                    )?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let body = serde_json::to_vec_pretty(&StoredApprovalState {
            version: 1,
            expired,
            deferred,
        })?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("approval-slots.json");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let staged = path.with_file_name(format!(".{file_name}.{}.new", std::process::id()));
        std::fs::write(&staged, body)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::File::open(&staged)?.sync_all()?;
        std::fs::rename(&staged, &path)?;
        if let Some(parent) = path.parent()
            && let Ok(dir) = std::fs::File::open(parent)
        {
            let _ = dir.sync_all();
        }
        Ok(())
    }
}

fn approval_key(detail: &Value) -> String {
    serde_json::to_string(detail).unwrap_or_default()
}

fn unix_secs(time: SystemTime) -> anyhow::Result<u64> {
    Ok(time
        .duration_since(UNIX_EPOCH)
        .map_err(|error| anyhow::anyhow!("approval expiry precedes Unix epoch: {error}"))?
        .as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_complete_roundtrip() {
        let board = ApprovalBoard::default();
        let (req, rx) = board.open();
        assert_eq!(
            board.complete(&req, ApprovalDecision::AllowOnce),
            ApprovalCompletion::Applied
        );
        assert_eq!(rx.await.ok(), Some(ApprovalDecision::AllowOnce));
    }

    #[tokio::test]
    async fn complete_unknown_is_false() {
        let board = ApprovalBoard::default();
        assert_eq!(
            board.complete("nope", ApprovalDecision::Deny),
            ApprovalCompletion::Stale
        );
    }

    #[tokio::test]
    async fn complete_reports_a_dropped_waiter() {
        let board = ApprovalBoard::default();
        let (req, rx) = board.open();
        drop(rx);
        assert_eq!(
            board.complete(&req, ApprovalDecision::AllowOnce),
            ApprovalCompletion::Stale,
            "removing a sender is not enough: nobody received the decision"
        );
    }

    /// 到点之后没人听：`suspend` 必须让后来的批复进入未来槽位，而不是假装
    /// 唤醒了已经消失的等待者。
    #[tokio::test]
    async fn suspend_makes_a_late_reply_a_deferred_slot() {
        let board = ApprovalBoard::default();
        let (req, _rx) = board.open();
        assert!(board.has_waiter(&req));
        // 模拟丢弃 rx（等待者消失）+ 到点撤摊：真实路径是 timeout 那一条
        assert!(board.suspend(&req), "撤摊要说成功撤了一个");
        assert!(!board.has_waiter(&req));
        assert_eq!(
            board.complete(&req, ApprovalDecision::AllowOnce),
            ApprovalCompletion::Deferred,
            "the expired waiter cannot apply the decision, but the exact slot may"
        );
        assert!(!board.suspend(&req), "撤两次是假的：第二次没有等待者");
        assert_eq!(
            board.take_deferred(&Value::Null),
            Some((req.clone(), ApprovalDecision::AllowOnce))
        );
        assert_eq!(board.take_deferred(&Value::Null), None, "slots are one-use");
        assert_eq!(
            board.complete(&req, ApprovalDecision::AllowOnce),
            ApprovalCompletion::Stale,
            "a duplicate reply must not recreate the consumed slot"
        );
    }

    #[test]
    fn deferred_approval_matches_exact_detail_and_expires() {
        let board = ApprovalBoard::with_deferred_ttl(Duration::from_secs(60));
        let allowed = serde_json::json!({"tool":"bash","args":{"command":"cargo test"}});
        let req = "req-1".to_string();
        let _rx = board.open_with_detail(req.clone(), allowed.clone());
        assert!(board.suspend(&req));
        assert_eq!(
            board.complete(&req, ApprovalDecision::AllowOnce),
            ApprovalCompletion::Deferred
        );
        assert_eq!(
            board.take_deferred(
                &serde_json::json!({"tool":"bash","args":{"command":"cargo build"}})
            ),
            None,
            "a similar command is not the approved call"
        );
        assert_eq!(
            board.take_deferred(&allowed),
            Some((req, ApprovalDecision::AllowOnce))
        );

        let expired = ApprovalBoard::with_deferred_ttl(Duration::ZERO);
        let req = "req-2".to_string();
        let _rx = expired.open_with_detail(req.clone(), allowed.clone());
        assert!(expired.suspend(&req));
        assert_eq!(
            expired.complete(&req, ApprovalDecision::AllowOnce),
            ApprovalCompletion::Stale
        );
    }

    #[test]
    fn contradictory_replays_of_one_request_fail_closed() {
        let board = ApprovalBoard::default();
        let detail = serde_json::json!({"tool":"bash","args":{"command":"cargo test"}});
        let req = "req-3".to_string();
        let _rx = board.open_with_detail(req.clone(), detail.clone());
        assert!(board.suspend(&req));
        assert_eq!(
            board.complete(&req, ApprovalDecision::AllowOnce),
            ApprovalCompletion::Deferred
        );
        assert_eq!(
            board.complete(&req, ApprovalDecision::Deny),
            ApprovalCompletion::Deferred
        );
        assert_eq!(
            board.take_deferred(&detail),
            Some((req, ApprovalDecision::Deny))
        );
    }

    #[test]
    fn cancellation_discards_the_request_without_deferring_it() {
        let board = ApprovalBoard::default();
        let detail = serde_json::json!({"tool":"bash","args":{"command":"cargo test"}});
        let req = "req-cancelled".to_string();
        let _rx = board.open_with_detail(req.clone(), detail.clone());
        assert!(board.abandon(&req));
        assert_eq!(
            board.complete(&req, ApprovalDecision::AllowOnce),
            ApprovalCompletion::Stale
        );
        assert_eq!(board.take_deferred(&detail), None);
    }

    #[test]
    fn deferred_approvals_survive_a_board_restart() {
        let path = std::env::temp_dir().join(format!(
            "kestrel-approval-store-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let detail = serde_json::json!({"tool":"bash","args":{"command":"cargo test"}});
        let req = "req-persisted".to_string();
        {
            let board = ApprovalBoard::with_store(path.clone()).unwrap();
            let _rx = board.open_with_detail(req.clone(), detail.clone());
            assert!(board.suspend(&req));
            assert!(path.exists(), "suspending writes the pending slot");
        }
        {
            let restored = ApprovalBoard::with_store(path.clone()).unwrap();
            assert_eq!(
                restored.complete(&req, ApprovalDecision::AllowOnce),
                ApprovalCompletion::Deferred
            );
            assert_eq!(
                restored.take_deferred(&detail),
                Some((req.clone(), ApprovalDecision::AllowOnce))
            );
        }
        let restored = ApprovalBoard::with_store(path.clone()).unwrap();
        assert_eq!(
            restored.take_deferred(&detail),
            None,
            "consumption is durable"
        );
        std::fs::remove_file(path).ok();
    }
}
