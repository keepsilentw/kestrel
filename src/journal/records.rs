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

#[derive(Default)]
struct Settled {
    /// (role, stage, iteration) 已收到回铃/已提交结果。
    keys: std::collections::HashSet<(String, String, u64)>,
    last_stage: Option<Stage>,
    max_iteration: u64,
}

pub fn reconcile(records: &[JournalRecord]) -> ReconcileReport {
    let mut settled = Settled::default();
    let mut in_flight: Vec<(TaskId, String, ThinBell)> = Vec::new();
    let mut spent: BTreeMap<String, f64> = BTreeMap::new();

    for r in records {
        match r {
            JournalRecord::BellSent { task, to, bell } => {
                in_flight.push((task.clone(), to.clone(), bell.clone()));
                settled.max_iteration = settled.max_iteration.max(bell.iteration);
                settled.last_stage = Some(bell.stage);
            }
            JournalRecord::BellReceived { bell, .. } => {
                settled.keys.insert(bell_key(bell));
                settled.max_iteration = settled.max_iteration.max(bell.iteration);
            }
            JournalRecord::ResultCommitted { stage, .. } => {
                settled.last_stage = Some(*stage);
                // dev 结果提交结算该 stage 的在途铃（任何 iteration）
                in_flight.retain(|(_, _, bell)| bell.stage != *stage);
            }
            JournalRecord::StageWritten {
                stage, iteration, ..
            } => {
                settled.last_stage = Some(*stage);
                settled.max_iteration = settled.max_iteration.max(*iteration);
            }
            JournalRecord::BudgetSpent { peer, usd, .. } => {
                *spent.entry(peer.clone()).or_insert(0.0) += usd;
            }
            // Approvals and usage are audit records: they never move the stage
            // projection, and a replay must not count them as bells.
            JournalRecord::ApprovalRequested { .. }
            | JournalRecord::ApprovalDecided { .. }
            | JournalRecord::UsageReported { .. } => {}
            JournalRecord::Snapshot { checkpoint, .. } => {
                // hard reset of the fold: the prefix is already inside the
                // checkpoint, and its in-flight set replaces whatever we had.
                in_flight.clear();
                for b in &checkpoint.in_flight {
                    in_flight.push((b.task.clone(), b.to.clone(), b.bell.clone()));
                }
                spent.clear();
                spent.extend(checkpoint.spent_usd.clone());
                settled.last_stage = checkpoint.stage;
                settled.max_iteration = checkpoint.iteration;
            }
        }
    }
    // settle bells whose (role, stage, iteration) has a matching receipt
    in_flight.retain(|(_, _, bell)| !settled.keys.contains(&bell_key(bell)));

    ReconcileReport {
        in_flight: in_flight
            .into_iter()
            .map(|(task, to, bell)| InFlightBell { task, to, bell })
            .collect(),
        state: StateSnapshot {
            stage: settled.last_stage,
            iteration: settled.max_iteration,
            spent_usd: spent,
        },
    }
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
