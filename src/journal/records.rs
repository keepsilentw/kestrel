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
        /// dispatch 坐标：一笔预留和它的结算靠这四处配对。
        #[serde(default)]
        task: TaskId,
        peer: String,
        #[serde(default)]
        stage: Option<Stage>,
        #[serde(default)]
        iteration: u64,
        usd: f64,
        /// true = 金额来自价格表（`providers.toml` 的 cost）；false = 这个
        /// fixture 的名义值。账本必须能把两者分开说清楚，否则「按名义值停损」
        /// 会被读成「按账单停损」。
        #[serde(default)]
        priced: bool,
    },
    /// 结算：把上面那笔预留换成实际 token 的账单。没有这条记录，账本就只能永远
    /// 按派发次数计钱——design §10 划出的边界「usd 才能真正按 token 结算」说的
    /// 就是它。结算只替换同坐标的预留；找不到预留的结算是重复投递或漏预留，
    /// 一律不记账（钱只计一次，宁可少计一次重复的）。
    BudgetSettled {
        #[serde(default)]
        task: TaskId,
        peer: String,
        #[serde(default)]
        stage: Option<Stage>,
        #[serde(default)]
        iteration: u64,
        usage: crate::provider::wire::Usage,
        usd: f64,
        #[serde(default)]
        priced: bool,
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
    /// The worker's own token measurement, straight off its result bell. It is
    /// the raw reading; the money is derived from it by the ledger (`BudgetSettled`)
    /// at the rate the registry declares, never by the worker itself. Kept as an
    /// audit record: when the bill and the reading disagree, both are on disk.
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

impl JournalRecord {
    /// 派发前的预算预留。构造只在这一处，免得调用方各自拼字段、把配对坐标拼歪。
    pub fn reserved(
        task: impl Into<TaskId>,
        peer: &str,
        stage: Stage,
        iteration: u64,
        usd: f64,
        priced: bool,
    ) -> Self {
        JournalRecord::BudgetSpent {
            task: task.into(),
            peer: peer.to_string(),
            stage: Some(stage),
            iteration,
            usd,
            priced,
        }
    }

    /// 结果到手后的结算：把这笔派发的名义预留换成实际 token 的账单。
    pub fn settled(
        task: impl Into<TaskId>,
        peer: &str,
        stage: Stage,
        iteration: u64,
        usage: crate::provider::wire::Usage,
        usd: f64,
        priced: bool,
    ) -> Self {
        JournalRecord::BudgetSettled {
            task: task.into(),
            peer: peer.to_string(),
            stage: Some(stage),
            iteration,
            usage,
            usd,
            priced,
        }
    }
}

/// State projection rebuilt from the journal (state.json 的内容来源).
#[derive(Debug, Default, PartialEq)]
pub struct StateSnapshot {
    pub stage: Option<Stage>,
    pub iteration: u64,
    pub spent_usd: BTreeMap<String, f64>,
    /// 那笔花费里有多少仍是名义值（没有价格表可结算的预留）。停损报账时必须
    /// 说得出这个差值，否则「按名义停损」会被读成「按账单停损」。
    pub nominal_usd: BTreeMap<String, f64>,
    /// 已结算派发的 token 合计：钱是算出来的，这里留下算的那份输入。
    pub tokens: BTreeMap<String, crate::provider::wire::Usage>,
    /// 还没结算的预留（dispatch 坐标 -> 金额/是否按价）。轮转把它抄进检查点，
    /// 结算才配对得上——丢了它就等于同一笔派发计两次钱。
    pub reserved: BTreeMap<BudgetKey, Booking>,
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
    pub nominal_usd: BTreeMap<String, f64>,
    pub tokens: BTreeMap<String, crate::provider::wire::Usage>,
    pub reserved: Vec<ReservationEntry>,
}

impl Checkpoint {
    pub fn capture(report: &ReconcileReport) -> Self {
        Self {
            stage: report.state.stage,
            iteration: report.state.iteration,
            spent_usd: report.state.spent_usd.clone(),
            in_flight: report.in_flight.clone(),
            nominal_usd: report.state.nominal_usd.clone(),
            tokens: report.state.tokens.clone(),
            // BTreeMap 的迭代顺序是确定的，所以同一份账本捕获出的检查点字节一致
            // ——「在线投影 == 全量重放」那条断言依赖这一点。
            reserved: report
                .state
                .reserved
                .iter()
                .map(|(key, booking)| (key.clone(), *booking))
                .collect(),
        }
    }
}

/// (task, peer, stage 的小写线名, iteration)：一笔预留与它的结算共用的坐标。
/// stage 走字符串（与 `bell_key` 同一个理由），检查点进出序列化时不会换算法。
pub type BudgetKey = (TaskId, String, String, u64);

/// 一笔记账：金额 + 是否按价格表结算。
pub type Booking = (f64, bool);

/// 检查点里的一个预留。检查点要序列化成 JSON，而 `serde_json` 的 map key 只收
/// 字符串：拿元组当 key 会直接报 “key must be a string”，而**空的恰好能过**——
/// 所以这里存数组，并由「带未结算预留的往返」测试钉住，不靠写法好看。
pub type ReservationEntry = (BudgetKey, Booking);

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
    /// `spent` 里还没有价格表支撑的那部分（名义预留）。停损报账时要说得出
    /// 这个差值，否则「按名义停损」会被读成「按账单停损」。
    nominal: BTreeMap<String, f64>,
    /// 已结算派发的 token 合计：账单的输入，跟着账本一起留盘。
    tokens: BTreeMap<String, crate::provider::wire::Usage>,
    /// 尚未结算的预留：dispatch 坐标 -> (金额, 是否按价)。结算靠它配对，
    /// 轮转靠它跨检查点存活——丢了它，同一笔派发会被计两次钱。
    reserved: BTreeMap<BudgetKey, Booking>,
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
            JournalRecord::ResultCommitted { task, stage, .. } => {
                self.settled.last_stage = Some(*stage);
                // dev 结果提交结算该 stage 的在途铃（任何 iteration）
                self.in_flight.retain(|(_, _, bell)| bell.stage != *stage);
                // stage 结了账，它的预留配对信息就不再有人需要：钱已经记进
                // spent（未结算的那笔按预留额留在账上，宁可不退）。留着这些键
                // 会让账本随 pipeline 无限增长，所以按 (task, stage) 收掉。
                let name = stage_name(*stage);
                self.reserved
                    .retain(|(t, _, s, _), _| !(s == &name && t == task));
            }
            JournalRecord::StageWritten {
                stage, iteration, ..
            } => {
                self.settled.last_stage = Some(*stage);
                self.settled.max_iteration = self.settled.max_iteration.max(*iteration);
            }
            JournalRecord::BudgetSpent {
                task,
                peer,
                stage,
                iteration,
                usd,
                priced,
            } => {
                *self.spent.entry(peer.clone()).or_insert(0.0) += usd;
                if !priced {
                    *self.nominal.entry(peer.clone()).or_insert(0.0) += usd;
                }
                self.reserved
                    .insert(budget_key(task, peer, *stage, *iteration), (*usd, *priced));
            }
            // 结算换掉同坐标的预留：钱只计一次，多出来的部分退回去。找不到预留
            // 的结算（重复投递、或该 stage 早已结了账）不记账——宁可漏计一次重复
            // 的，也不把一笔派发烧成两笔。
            JournalRecord::BudgetSettled {
                task,
                peer,
                stage,
                iteration,
                usage,
                usd,
                priced,
            } => {
                let key = budget_key(task, peer, *stage, *iteration);
                if let Some((prior_usd, prior_priced)) = self.reserved.remove(&key) {
                    *self.spent.entry(peer.clone()).or_insert(0.0) += usd - prior_usd;
                    if !prior_priced {
                        let slot = self.nominal.entry(peer.clone()).or_insert(0.0);
                        *slot -= prior_usd;
                        if slot.abs() < 1e-12 {
                            *slot = 0.0;
                        }
                    }
                    if !priced {
                        *self.nominal.entry(peer.clone()).or_insert(0.0) += usd;
                    }
                    self.tokens
                        .entry(peer.clone())
                        .or_default()
                        .accumulate(usage);
                }
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
                // 结算配对必须跟着检查点走：归档掉的前缀里那些还没结算的预留，
                // 若不在这里恢复，之后到达的结算会当成「无预留」而再记一次钱。
                self.nominal.clear();
                self.nominal.extend(checkpoint.nominal_usd.clone());
                self.tokens.clear();
                self.tokens.extend(checkpoint.tokens.clone());
                self.reserved.clear();
                for (key, booking) in &checkpoint.reserved {
                    self.reserved.insert(key.clone(), *booking);
                }
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
                nominal_usd: self.nominal.clone(),
                tokens: self.tokens.clone(),
                reserved: self.reserved.clone(),
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

/// stage 的线名（与 `bell_key` 同一份算法，两处不会各拼一遍）。
fn stage_name(stage: Stage) -> String {
    format!("{stage:?}").to_lowercase()
}

/// 一笔派发在账本里的坐标：预留与结算共用它配对。
fn budget_key(task: &TaskId, peer: &str, stage: Option<Stage>, iteration: u64) -> BudgetKey {
    (
        task.clone(),
        peer.to_string(),
        stage.map(stage_name).unwrap_or_default(),
        iteration,
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

    use crate::provider::wire::Usage;

    fn usage(input: u64, output: u64) -> Usage {
        Usage {
            input_tokens: input,
            output_tokens: output,
            ..Usage::default()
        }
    }

    /// 预留与结算配对：结算把那一次派发的预留**换掉**，不是加在它旁边。
    /// 这一条是「按 token 结算」的全部意义——账本不再按派发次数计钱。
    #[test]
    fn settlement_replaces_the_reservation() {
        let records = vec![
            JournalRecord::reserved("t", "rd", Stage::Dev, 1, 0.5, true),
            JournalRecord::settled("t", "rd", Stage::Dev, 1, usage(100, 20), 0.12, true),
        ];
        let rep = reconcile(&records);
        assert!(
            (rep.state.spent_usd["rd"] - 0.12).abs() < 1e-9,
            "{:?}",
            rep.state.spent_usd
        );
        assert!(rep.state.nominal_usd.is_empty(), "按价结算的不是名义值");
        assert_eq!(rep.state.tokens["rd"], usage(100, 20));
        assert!(rep.state.reserved.is_empty(), "配对键随结算消耗掉");
    }

    /// 名义预留被真账单换掉时，nominal 那一片也要跟着退——否则报告里会出现
    /// 「花了 0.12，其中 0.5 是名义值」这种自相矛盾的账。
    #[test]
    fn settling_a_nominal_reservation_clears_the_nominal_slice() {
        let records = vec![
            JournalRecord::reserved("t", "rd", Stage::Dev, 1, 0.5, false),
            JournalRecord::settled("t", "rd", Stage::Dev, 1, usage(10, 5), 0.12, true),
        ];
        let rep = reconcile(&records);
        assert!((rep.state.spent_usd["rd"] - 0.12).abs() < 1e-9);
        assert_eq!(rep.state.nominal_usd.get("rd").copied().unwrap_or(0.0), 0.0);
    }

    /// 说不清价就是说不清价，不是「这次免费」：名义预留既计进 spent 也计进
    /// nominal，停损因此仍然会触发，只是报账时标得清清楚楚。
    #[test]
    fn unpriced_reservation_stays_on_the_books_as_nominal() {
        let records = vec![JournalRecord::reserved(
            "t",
            "rd",
            Stage::Dev,
            1,
            0.5,
            false,
        )];
        let rep = reconcile(&records);
        assert!((rep.state.spent_usd["rd"] - 0.5).abs() < 1e-9);
        assert!((rep.state.nominal_usd["rd"] - 0.5).abs() < 1e-9);
    }

    /// 找不到预留的结算不记账：重复投递、或该 stage 早已结了账，都不该把一笔
    /// 派发烧成两笔钱。
    #[test]
    fn settlement_without_a_reservation_is_ignored() {
        let records = vec![
            JournalRecord::settled("t", "rd", Stage::Dev, 1, usage(50, 50), 0.3, true),
            JournalRecord::settled("t", "rd", Stage::Dev, 1, usage(50, 50), 0.3, true),
        ];
        let rep = reconcile(&records);
        assert_eq!(rep.state.spent_usd.get("rd").copied().unwrap_or(0.0), 0.0);
        assert!(rep.state.tokens.is_empty(), "被丢弃的结算不留 token 痕迹");
    }

    /// 一次派发的预留与结算坐标必须完全对齐；iteration 不同就是另一次派发。
    #[test]
    fn settlement_pairs_by_iteration_not_just_stage() {
        let records = vec![
            // iter1 的预留从未结算（比如铃丢了），iter2 的结算不该把它换掉
            JournalRecord::reserved("t", "rd", Stage::Dev, 1, 0.5, true),
            JournalRecord::settled("t", "rd", Stage::Dev, 2, usage(10, 10), 0.1, true),
        ];
        let rep = reconcile(&records);
        assert!(
            (rep.state.spent_usd["rd"] - 0.5).abs() < 1e-9,
            "配不上对的结算被忽略，iter1 的预留原样留在账上: {:?}",
            rep.state.spent_usd
        );
        assert_eq!(rep.state.reserved.len(), 1, "iter1 还欠着一次结算");
    }

    /// stage 结了账就收掉它的配对键：账本不随 pipeline 无限增长，而钱已经
    /// 记进 spent（未结算的那笔按预留额留在账上，宁可不退）。
    #[test]
    fn committed_stage_prunes_pairing_but_keeps_the_money() {
        let records = vec![
            JournalRecord::reserved("t", "rd", Stage::Dev, 1, 0.5, false),
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
        assert!(rep.state.reserved.is_empty(), "配对键随 stage 结算收掉");
        assert!((rep.state.spent_usd["rd"] - 0.5).abs() < 1e-9);
        // 另一个 task 的同名 stage 不受影响
        let other = vec![
            JournalRecord::reserved("t2", "rd", Stage::Dev, 1, 0.5, false),
            JournalRecord::ResultCommitted {
                task: "t1".into(),
                stage: Stage::Dev,
                verdict: Verdict {
                    pass: true,
                    issues: vec![],
                },
            },
        ];
        assert_eq!(reconcile(&other).state.reserved.len(), 1, "不跨 task 串台");
    }

    /// 轮转把未结算的预留抄进检查点：之后到达的结算仍然配得上。
    /// 少了这一步，「预留被归档、结算找不到配对」会把一次派发计成两笔钱。
    #[test]
    fn checkpoint_carries_unsettled_reservations() {
        let key: BudgetKey = ("t".into(), "rd".into(), "dev".into(), 1u64);
        let checkpoint = Checkpoint {
            stage: Some(Stage::Dev),
            iteration: 1,
            spent_usd: [("rd".to_string(), 0.5)].into_iter().collect(),
            in_flight: vec![],
            nominal_usd: Default::default(),
            tokens: Default::default(),
            reserved: vec![(key, (0.5, true))],
        };
        let records = vec![
            JournalRecord::Snapshot {
                covers: 1,
                checkpoint,
            },
            JournalRecord::settled("t", "rd", Stage::Dev, 1, usage(30, 12), 0.12, true),
        ];
        let rep = reconcile(&records);
        assert!(
            (rep.state.spent_usd["rd"] - 0.12).abs() < 1e-9,
            "{:?}",
            rep.state.spent_usd
        );
        assert_eq!(rep.state.tokens["rd"], usage(30, 12));
    }

    /// 带**未结算预留**的检查点必须能序列化往返：先预留、轮转、后结算是真实序列，
    /// 而 `serde_json` 只接受字符串 map key（元组 key 直接报错，空的恰好能过）。
    /// 这一条钉住那个坑：换成 map 存元组 key，本测试立刻红。
    #[test]
    fn checkpoint_with_unsettled_reservations_roundtrips_through_json() {
        let key: BudgetKey = ("t".into(), "rd".into(), "dev".into(), 1u64);
        let checkpoint = Checkpoint {
            stage: Some(Stage::Dev),
            iteration: 1,
            spent_usd: [("rd".to_string(), 0.5)].into_iter().collect(),
            in_flight: vec![InFlightBell {
                task: "t".into(),
                to: "rd".into(),
                bell: bell("rd", Stage::Dev, 1),
            }],
            nominal_usd: Default::default(),
            tokens: [("rd".to_string(), usage(8, 8))].into_iter().collect(),
            reserved: vec![(key, (0.5, true))],
        };
        let record = JournalRecord::Snapshot {
            covers: 1,
            checkpoint,
        };
        let line = serde_json::to_string(&record).expect("a checkpoint serializes");
        let back: JournalRecord = serde_json::from_str(&line).expect("and parses back");
        assert_eq!(serde_json::to_string(&back).unwrap(), line, "字节级往返");
        // 为什么存数组而不是 map：这份断言把理由钉在代码里，而不是留在注释里——
        // `serde_json` 的 map key 只收字符串，元组 key 当场就失败（而空 map 恰好能过，
        // 所以只有非空的检查点才测得出这个形状）。
        let as_map: std::collections::BTreeMap<BudgetKey, Booking> = [(
            ("t".to_string(), "rd".to_string(), "dev".to_string(), 1u64),
            (0.5, true),
        )]
        .into_iter()
        .collect();
        assert!(
            serde_json::to_string(&as_map).is_err(),
            "元组 key 的 map 若能序列化成 JSON，就该改用 map 存预留"
        );
        // 往返之后，迟到的结算仍然配得上那笔预留
        let rep = reconcile(&[
            back,
            JournalRecord::settled("t", "rd", Stage::Dev, 1, usage(4, 4), 0.12, true),
        ]);
        assert!(
            (rep.state.spent_usd["rd"] - 0.12).abs() < 1e-9,
            "检查点带过来的 0.5 预留被结算换成 0.12，而不是加在旁边: {:?}",
            rep.state.spent_usd
        );
    }
}
