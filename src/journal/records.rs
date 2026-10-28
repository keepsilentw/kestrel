//! Journal record types + reconciliation (plan §2.9 恢复序列).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type TaskId = String;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Spec,
    Dev,
    Review,
    Test,
    Build,
    Release,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Verdict {
    pub pass: bool,
    #[serde(default)]
    pub issues: Vec<String>,
}

/// 薄消息 (harness-v3 §5.2): 门铃正文只含 {role, stage, check, iteration, ref}。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ThinBell {
    pub role: String,
    pub stage: Stage,
    /// QA 的验证项 (review|test|build)；非 QA 角色为 None。
    #[serde(default)]
    pub check: Option<String>,
    pub iteration: u64,
    /// Opaque reference blob (spec_ref / issues_ref…), read-from-disk only.
    #[serde(default, rename = "ref")]
    pub r#ref: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JournalRecord {
    StageWritten {
        task: TaskId,
        stage: Stage,
        iteration: u64,
    },
    BellSent {
        task: TaskId,
        to: String,
        bell: ThinBell,
    },
    BellReceived {
        task: TaskId,
        from: String,
        bell: ThinBell,
    },
    ResultCommitted {
        task: TaskId,
        stage: Stage,
        verdict: Verdict,
    },
    BudgetSpent {
        peer: String,
        usage: crate::provider::wire::Usage,
        usd: f64,
    },
    /// An approval reverse-request the SA answered for a worker
    /// (`ApproveMode::Bus`). Appended BEFORE the reply is sent: who widened a
    /// sandbox is audit trail, not an afterthought (design §5 不变式 5).
    ApprovalRequested {
        task: TaskId,
        peer: String,
        req: String,
        /// The command the worker wanted to run; empty when the request did
        /// not carry one (then the SA has nothing to decide on).
        command: String,
    },
    ApprovalDecided {
        req: String,
        /// `"allow"` | `"deny"` — anything else is a deny upstream.
        decision: String,
        by: String,
    },
    /// Token usage as reported by the worker. `BudgetSpent.usd` is a nominal
    /// per-dispatch fixture charge (no pricing table yet); this record is the
    /// measurement, so the ledger no longer implies a cost model it does not
    /// have.
    UsageReported {
        peer: String,
        usage: crate::provider::wire::Usage,
    },
    /// Checkpoint written by a rotation (design §10 快照/截断语义): it summarizes
    /// the raw records that rotations archived out of the live file. `covers` is
    /// CUMULATIVE from the beginning of the journal, not just this rotation's
    /// share, so the next segment can be named by the range it covers: a rotation
    /// that crashed before the rename and is retried overwrites the same segment
    /// instead of archiving the same prefix twice. Recovery reads the live file
    /// only and still knows the unsettled bells. A fold that sees this record
    /// RESTARTS from the checkpoint — the archived prefix is already folded into
    /// it, replaying both would double-count the budget.
    Snapshot { covers: u64, checkpoint: Checkpoint },
}

/// State projection rebuilt from the journal (state.json 的内容来源).
#[derive(Debug, Default, PartialEq)]
pub struct StateSnapshot {
    pub stage: Option<Stage>,
    pub iteration: u64,
    pub spent_usd: BTreeMap<String, f64>,
}

/// Reconciliation report: in-flight bells = BellSent without a later
/// BellReceived for the same iteration AND without a ResultCommitted covering
/// them. Those are the "worker may have finished but the bell was lost" cases
/// the SA 巡检 resolves by reading disk artifacts.
#[derive(Debug, Default)]
pub struct ReconcileReport {
    pub in_flight: Vec<InFlightBell>,
    pub state: StateSnapshot,
}

/// One BellSent that was never settled: (task, destination peer, bell).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InFlightBell {
    pub task: TaskId,
    pub to: String,
    pub bell: ThinBell,
}

/// Everything a replay would otherwise have to re-derive from the archived
/// prefix — INCLUDING the unsettled bells. A snapshot that forgot them would
/// turn a lost result bell into a silent re-dispatch after truncation, which is
/// exactly the failure the journal exists to prevent.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub stage: Option<Stage>,
    pub iteration: u64,
    pub spent_usd: BTreeMap<String, f64>,
    pub in_flight: Vec<InFlightBell>,
}

impl Checkpoint {
    pub fn capture(report: &ReconcileReport) -> Self {
        Self {
            stage: report.state.stage,
            iteration: report.state.iteration,
            spent_usd: report.state.spent_usd.clone(),
            in_flight: report.in_flight.clone(),
        }
    }
}

#[derive(Default, Clone)]
struct Settled {
    /// (role, stage, iteration) 已收到回铃/已提交结果。
    keys: std::collections::HashSet<(String, String, u64)>,
    last_stage: Option<Stage>,
    max_iteration: u64,
}

/// The ONE fold of journal records. `reconcile` below is just this driven over a
/// slice, and `Journal` keeps one of these alive so the online projection and a
/// full replay cannot diverge — design §5 不变式 5 says state is DEFINED by the
/// replay, so an incremental shortcut that disagrees with it would be a bug in
/// the worst possible place (the budget stop-loss and the 巡检 decision).
#[derive(Default, Clone)]
pub struct Folder {
    settled: Settled,
    in_flight: Vec<(TaskId, String, ThinBell)>,
    spent: BTreeMap<String, f64>,
    /// records physically present in the live file (a checkpoint line counts
    /// as one), which is what the rotation trigger looks at
    live_records: usize,
}

impl Folder {
    pub fn apply(&mut self, r: &JournalRecord) {
        self.live_records += 1;
        match r {
            JournalRecord::BellSent { task, to, bell } => {
                self.in_flight
                    .push((task.clone(), to.clone(), bell.clone()));
                self.settled.max_iteration = self.settled.max_iteration.max(bell.iteration);
                self.settled.last_stage = Some(bell.stage);
            }
            JournalRecord::BellReceived { bell, .. } => {
                self.settled.keys.insert(bell_key(bell));
                self.settled.max_iteration = self.settled.max_iteration.max(bell.iteration);
            }
            JournalRecord::ResultCommitted { stage, .. } => {
                self.settled.last_stage = Some(*stage);
                // dev 结果提交结算该 stage 的在途铃（任何 iteration）
                self.in_flight.retain(|(_, _, bell)| bell.stage != *stage);
            }
            JournalRecord::StageWritten {
                stage, iteration, ..
            } => {
                self.settled.last_stage = Some(*stage);
                self.settled.max_iteration = self.settled.max_iteration.max(*iteration);
            }
            JournalRecord::BudgetSpent { peer, usd, .. } => {
                *self.spent.entry(peer.clone()).or_insert(0.0) += usd;
            }
            // Approvals and usage are audit records: they never move the stage
            // projection, and a replay must not count them as bells.
            JournalRecord::ApprovalRequested { .. }
            | JournalRecord::ApprovalDecided { .. }
            | JournalRecord::UsageReported { .. } => {}
            JournalRecord::Snapshot { checkpoint, .. } => {
                // hard reset of the fold: the prefix is already inside the
                // checkpoint, and its in-flight set replaces whatever we had.
                self.in_flight.clear();
                for b in &checkpoint.in_flight {
                    self.in_flight
                        .push((b.task.clone(), b.to.clone(), b.bell.clone()));
                }
                self.spent.clear();
                self.spent.extend(checkpoint.spent_usd.clone());
                self.settled.last_stage = checkpoint.stage;
                self.settled.max_iteration = checkpoint.iteration;
                // Receipt keys of the archived prefix go with it: the bells they
                // settled are no longer in `in_flight`, and keeping the keys
                // alive could wrongly settle a reused (role, stage, iteration).
                self.settled.keys.clear();
            }
        }
    }

    pub fn report(&self) -> ReconcileReport {
        ReconcileReport {
            in_flight: self
                .in_flight
                .iter()
                .filter(|(_, _, bell)| !self.settled.keys.contains(&bell_key(bell)))
                .cloned()
                .map(|(task, to, bell)| InFlightBell { task, to, bell })
                .collect(),
            state: StateSnapshot {
                stage: self.settled.last_stage,
                iteration: self.settled.max_iteration,
                spent_usd: self.spent.clone(),
            },
        }
    }

    /// How many records the live file holds right now.
    pub fn live_records(&self) -> usize {
        self.live_records
    }
}

pub fn reconcile(records: &[JournalRecord]) -> ReconcileReport {
    let mut folder = Folder::default();
    for r in records {
        folder.apply(r);
    }
    folder.report()
}

fn bell_key(bell: &ThinBell) -> (String, String, u64) {
    (
        bell.role.clone(),
        format!("{:?}", bell.stage).to_lowercase(),
        bell.iteration,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn bell(role: &str, stage: Stage, iteration: u64) -> ThinBell {
        ThinBell {
            role: role.into(),
            stage,
            check: None,
            iteration,
            r#ref: json!({}),
        }
    }

    #[test]
    fn dev_result_settles_dev_bells() {
        let records = vec![
            JournalRecord::BellSent {
                task: "t".into(),
                to: "rd".into(),
                bell: bell("rd", Stage::Dev, 1),
            },
            JournalRecord::ResultCommitted {
                task: "t".into(),
                stage: Stage::Dev,
                verdict: Verdict {
                    pass: true,
                    issues: vec![],
                },
            },
        ];
        let rep = reconcile(&records);
        assert!(rep.in_flight.is_empty());
        assert_eq!(rep.state.stage, Some(Stage::Dev));
    }

    #[test]
    fn matching_bell_received_settles_by_iteration() {
        let records = vec![
            JournalRecord::BellSent {
                task: "t".into(),
                to: "rd".into(),
                bell: bell("rd", Stage::Dev, 1),
            },
            JournalRecord::BellReceived {
                task: "t".into(),
                from: "rd".into(),
                bell: bell("rd", Stage::Dev, 1),
            },
            JournalRecord::BellSent {
                task: "t".into(),
                to: "rd".into(),
                bell: bell("rd", Stage::Dev, 2),
            },
        ];
        let rep = reconcile(&records);
        assert_eq!(rep.in_flight.len(), 1);
        assert_eq!(rep.state.iteration, 2);
    }

    /// Approval and usage records are audit trail: they roundtrip through the
    /// wire format and never settle a bell, so a replay cannot mistake a
    /// decision for a worker receipt.
    #[test]
    fn approval_and_usage_records_are_audit_only() {
        let records = vec![
            JournalRecord::BellSent {
                task: "t".into(),
                to: "rd".into(),
                bell: bell("rd", Stage::Dev, 1),
            },
            JournalRecord::ApprovalRequested {
                task: "t".into(),
                peer: "rd".into(),
                req: "a-1".into(),
                command: "cargo test".into(),
            },
            JournalRecord::ApprovalDecided {
                req: "a-1".into(),
                decision: "allow".into(),
                by: "harness-sa".into(),
            },
            JournalRecord::UsageReported {
                peer: "rd".into(),
                usage: Default::default(),
            },
        ];
        for r in &records {
            let line = serde_json::to_string(r).unwrap();
            let back: JournalRecord = serde_json::from_str(&line).unwrap();
            assert_eq!(serde_json::to_string(&back).unwrap(), line, "roundtrip");
        }
        let rep = reconcile(&records);
        assert_eq!(
            rep.in_flight.len(),
            1,
            "an approval decision is not a bell receipt"
        );
        assert_eq!(rep.state.stage, Some(Stage::Dev));
        assert!(rep.state.spent_usd.is_empty());
    }

    /// A checkpoint resets the settled-receipt keys too. Without that, a bell
    /// key that was settled *before* the archived prefix would silently settle a
    /// re-dispatched bell with the same (role, stage, iteration) after it — the
    /// SA would see "nothing in flight" and never re-dispatch.
    #[test]
    fn checkpoint_clears_stale_receipt_keys() {
        let records = vec![
            JournalRecord::BellSent {
                task: "t".into(),
                to: "rd".into(),
                bell: bell("rd", Stage::Dev, 1),
            },
            JournalRecord::BellReceived {
                task: "t".into(),
                from: "rd".into(),
                bell: bell("rd", Stage::Dev, 1),
            },
            JournalRecord::Snapshot {
                covers: 2,
                checkpoint: Checkpoint::default(),
            },
            // same key again: a re-dispatch after the rotation
            JournalRecord::BellSent {
                task: "t".into(),
                to: "rd".into(),
                bell: bell("rd", Stage::Dev, 1),
            },
        ];
        let rep = reconcile(&records);
        assert_eq!(
            rep.in_flight.len(),
            1,
            "the reused bell is genuinely in flight: {rep:?}"
        );
    }

    #[test]
    fn qa_checks_settle_by_check_key() {
        // review bell settled by its own receipt; test bell remains in flight
        let records = vec![
            JournalRecord::BellSent {
                task: "t".into(),
                to: "qa".into(),
                bell: ThinBell {
                    role: "qa".into(),
                    stage: Stage::Review,
                    check: Some("review".into()),
                    iteration: 1,
                    r#ref: json!({}),
                },
            },
            JournalRecord::BellReceived {
                task: "t".into(),
                from: "qa".into(),
                bell: ThinBell {
                    role: "qa".into(),
                    stage: Stage::Review,
                    check: Some("review".into()),
                    iteration: 1,
                    r#ref: json!({}),
                },
            },
            JournalRecord::BellSent {
                task: "t".into(),
                to: "qa".into(),
                bell: ThinBell {
                    role: "qa".into(),
                    stage: Stage::Test,
                    check: Some("test".into()),
                    iteration: 1,
                    r#ref: json!({}),
                },
            },
        ];
        let rep = reconcile(&records);
        assert_eq!(rep.in_flight.len(), 1);
        assert_eq!(rep.state.stage, Some(Stage::Test));
    }
}
