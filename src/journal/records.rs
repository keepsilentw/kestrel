//! Journal record types + reconciliation (plan §2.9 恢复序列).

use std::collections::BTreeMap;

use std::borrow::Borrow;
use std::ops::Deref;

use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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

/// 一次 stage 判定的产出。`test_runs` 是 2026-11-04 加的第二格：证据不只证明
/// 「有一份像测试日志的东西」，还把它数出来的结论带上盘（design §9.34）。
/// `#[serde(default)]` 让改动之前的 journal 仍然可重放。
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Verdict {
    pub pass: bool,
    #[serde(default)]
    pub issues: Vec<JudgmentIssue>,
    /// 本轮核对过的测试日志（每份一条）。空 = 这一关没有可数出来的测试证据。
    #[serde(default)]
    pub test_runs: Vec<TestRun>,
    /// 核对时的**认账说明**（不是问题，不影响 pass）。它们必须跟着判定落进真相源：
    /// 重启之后没人知道当初 SA 把哪句声明认成了哪个文件，就没法复现那次判定。
    #[serde(default)]
    pub notes: Vec<String>,
    /// 这次 FAIL（若有过）说的是**产物**还是**验证者那一回合**。路由读它，不再读
    /// issue 的措辞（§9.41）：分类在判定产出的那一刻做，因为只有那里知道自己凭什么说
    /// 这句话。默认 `Artifact` 是刻意的下限——分不出来源时宁可多问写者一次，
    /// 也绝不因为「读不懂」而放过去。
    #[serde(default)]
    pub attribution: Attribution,
}

impl Verdict {
    /// Construct a verdict whose global attribution is derived from its typed
    /// issues. A new issue cannot enter this struct without choosing a side.
    pub fn new(
        pass: bool,
        issues: Vec<JudgmentIssue>,
        test_runs: Vec<TestRun>,
        notes: Vec<String>,
    ) -> Self {
        let attribution = if issues
            .iter()
            .any(|issue| issue.attribution == Attribution::Turn)
        {
            Attribution::Turn
        } else {
            Attribution::Artifact
        };
        Self {
            pass,
            issues,
            test_runs,
            notes,
            attribution,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JudgmentIssue {
    pub message: String,
    pub attribution: Attribution,
}

impl JudgmentIssue {
    pub fn artifact(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            attribution: Attribution::Artifact,
        }
    }

    pub fn turn(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            attribution: Attribution::Turn,
        }
    }
}

impl Deref for JudgmentIssue {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl AsRef<str> for JudgmentIssue {
    fn as_ref(&self) -> &str {
        &self.message
    }
}

impl Borrow<str> for JudgmentIssue {
    fn borrow(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for JudgmentIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl PartialEq<str> for JudgmentIssue {
    fn eq(&self, other: &str) -> bool {
        self.message == other
    }
}

impl PartialEq<String> for JudgmentIssue {
    fn eq(&self, other: &String) -> bool {
        self.message == *other
    }
}

impl PartialEq<&str> for JudgmentIssue {
    fn eq(&self, other: &&str) -> bool {
        self.message == *other
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum IssueWire {
    Legacy(String),
    Detailed {
        message: String,
        #[serde(default)]
        attribution: Attribution,
    },
}

impl Serialize for JudgmentIssue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut issue = serializer.serialize_struct("JudgmentIssue", 2)?;
        issue.serialize_field("message", &self.message)?;
        issue.serialize_field("attribution", &self.attribution)?;
        issue.end()
    }
}

impl<'de> Deserialize<'de> for JudgmentIssue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match IssueWire::deserialize(deserializer)? {
            IssueWire::Legacy(message) => Ok(Self::artifact(message)),
            IssueWire::Detailed {
                message,
                attribution,
            } => Ok(Self {
                message,
                attribution,
            }),
        }
    }
}

/// FAIL 的归属。`Turn` = 这次判定本身不可信（空结果、abort、worker 侧 stop=error、
/// 没回铃）：产物**没被可信地看过**，不能拿它定罪谁。`Artifact` = 判定读了盘并且得出了
/// 关于产物的结论。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Attribution {
    #[default]
    Artifact,
    Turn,
}

impl Attribution {
    /// 返工铃与报账用的一句话。
    pub fn label(&self) -> &'static str {
        match self {
            Attribution::Artifact => "the artifact",
            Attribution::Turn => "the verifying turn itself",
        }
    }
}
/// 一份测试日志读出来的结论。字段全可缺：`None` 说的是「这份日志里没写」，
/// 不是「0 条通过」——把缺席当 0 就是把「说不清」记成「没问题」。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestRun {
    pub path: String,
    /// 内核 `output_path` 落的那行 `exit: ExitStatus { code: Some(0), signal: None }`。
    /// 别的来源（worker 自己 tee 的）没有这行，那就是 `None`。
    #[serde(default)]
    pub exit_code: Option<i64>,
    #[serde(default)]
    pub exit_signal: Option<i64>,
    #[serde(default)]
    pub passed: Option<u64>,
    #[serde(default)]
    pub failed: Option<u64>,
    /// unittest 的 `Ran 26 tests`。
    #[serde(default)]
    pub ran: Option<u64>,
}

impl TestRun {
    /// 「这一次跑挂了」——只在盘上真读得出来时下断言，绝不从「读不出来」推「没事」。
    pub fn failed_run(&self) -> bool {
        self.exit_signal.is_some()
            || self.exit_code.is_some_and(|c| c != 0)
            || self.failed.is_some_and(|f| f > 0)
    }

    /// 读得出结论（退出码或计数）就算它带结论；只有汇总行的旧日志不带。
    pub fn carries_verdict(&self) -> bool {
        self.exit_code.is_some()
            || self.exit_signal.is_some()
            || self.passed.is_some()
            || self.failed.is_some()
    }
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
        /// 金额。字段名是**单位中立**的：单位由价格表声明（`currency`，见 §9.44），
        /// 不写进 key 里。`alias = "usd"` 让 2026-11-05 之前落盘的 journal 行照旧可读。
        #[serde(rename = "cost", alias = "usd")]
        cost: f64,
        /// true = 金额来自价格表（`providers.toml` 的 cost）；false = 这个
        /// fixture 的名义值。账本必须能把两者分开说清楚，否则「按名义值停损」
        /// 会被读成「按账单停损」。
        #[serde(default)]
        priced: bool,
    },
    /// 结算：把上面那笔预留换成实际 token 的账单。没有这条记录，账本就只能永远
    /// 按派发次数计钱——design §10 划出的边界「钱才能真正按 token 结算」说的
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
        #[serde(rename = "cost", alias = "usd")]
        cost: f64,
        #[serde(default)]
        priced: bool,
        /// 当 `priced = false`：这笔钱为什么算不成账单。老记录读出来是 `None`
        /// （折叠时按 `NoRate` 处理），所以加了这一格之后旧 journal 仍然可重放。
        #[serde(default)]
        nominal_reason: Option<NominalReason>,
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
    /// worker 对那份批复的回执（design §10「批复的可靠投递」剩下的那一半）：
    /// `applied = false` 意思是「我收到了，但已经没有等待者可唤醒了」——到点撤摊或
    /// 回合结束。审批者只有拿到这句话，才能区分「批复丢了」与「批了但没人听」：
    /// 没有它，这两种在账上长得一模一样。审计记录，不参与状态折叠。
    ApprovalAcked {
        req: String,
        /// 回执来自哪条 session（worker 侧）
        by: String,
        applied: bool,
    },
    /// The worker's own token measurement, straight off its result bell. It is
    /// the raw reading; the money is derived from it by the ledger (`BudgetSettled`)
    /// at the rate the registry declares, never by the worker itself. The reading
    /// is also what feeds the projection's per-peer token totals — NOT the
    /// settlement: a peer with no rate never gets settled (§9.36's `None` arm), so
    /// counting tokens at settlement would report `0 / 0` for a run that burned
    /// tens of thousands of them. Money and usage keep separate sources.
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
        cost: f64,
        priced: bool,
    ) -> Self {
        JournalRecord::BudgetSpent {
            task: task.into(),
            peer: peer.to_string(),
            stage: Some(stage),
            iteration,
            cost,
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
        cost: f64,
        priced: bool,
    ) -> Self {
        JournalRecord::BudgetSettled {
            task: task.into(),
            peer: peer.to_string(),
            stage: Some(stage),
            iteration,
            usage,
            cost,
            priced,
            nominal_reason: None,
        }
    }

    /// 按名义落账的结算，并且**说得出这笔为什么算不成账单**。结算本身照做：预留必须
    /// 配对掉，否则同一笔派发可能被后来的结算再计一次钱。
    pub fn settled_as_nominal(
        task: impl Into<TaskId>,
        peer: &str,
        stage: Stage,
        iteration: u64,
        usage: crate::provider::wire::Usage,
        cost: f64,
        reason: NominalReason,
    ) -> Self {
        JournalRecord::BudgetSettled {
            task: task.into(),
            peer: peer.to_string(),
            stage: Some(stage),
            iteration,
            usage,
            cost,
            priced: false,
            nominal_reason: Some(reason),
        }
    }
}

/// 名义钱的来源。两类都是「说不清」，但不是同一件事：一类是**没人填价**，一类是
/// **端点报了个顶不起账单的读数**（§9.36）。合并成一个数字，看账的人就分不出该去补价
/// 还是该去修读数。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NominalReason {
    /// 价格表里查不到这个 peer 的 model 费率（预留按名义记账时就是这个原因）。
    NoRate,
    /// 有价，但读数是 0 输入 + 有输出：按价算只会得到一张少掉整个输入侧的假账单。
    UnderReported,
}

impl NominalReason {
    /// 报账用的线名（`{reason:?}` 会把变体名直接印进人类读的那一行）。
    pub fn label(&self) -> &'static str {
        match self {
            NominalReason::NoRate => "no rate",
            NominalReason::UnderReported => "under-reported",
        }
    }
}

/// State projection rebuilt from the journal (state.json 的内容来源).
#[derive(Debug, Default, PartialEq)]
pub struct StateSnapshot {
    pub stage: Option<Stage>,
    pub iteration: u64,
    /// 每个 peer 花掉的钱。**单位中立命名**：单位由价格表声明（§9.44），
    /// 一个把币种写进字段名的结构，迟早会在另一种币种上报错数。
    pub spent: BTreeMap<String, f64>,
    /// 那笔花费里有多少仍是名义值（没有价格表可结算的预留）。停损报账时必须
    /// 说得出这个差值，否则「按名义停损」会被读成「按账单停损」。
    pub nominal: BTreeMap<String, f64>,
    /// 已结算派发的 token 合计：钱是算出来的，这里留下算的那份输入。
    pub tokens: BTreeMap<String, crate::provider::wire::Usage>,
    /// 还没结算的预留（dispatch 坐标 -> 金额/是否按价）。轮转把它抄进检查点，
    /// 结算才配对得上——丢了它就等于同一笔派发计两次钱。
    pub reserved: BTreeMap<BudgetKey, Booking>,
    /// 名义钱按来源分类（总额仍在 `nominal` 里）。看账的人要能分得出
    /// 「该补价」与「该修读数」是哪些钱（§9.37）。
    pub nominal_by_reason: BTreeMap<NominalReason, f64>,
    /// 同上，但保留 peer 维度：报账要能指认是哪台 worker 说不清。
    pub nominal_by_peer_reason: BTreeMap<String, BTreeMap<NominalReason, f64>>,
    /// 每个 (task, stage) **最后一次提交**读出的测试结论（§9.35）。这是状态不是
    /// 历史：历史在 `result_committed` 记录里，这里只回答「现在这一关的测试结论是
    /// 什么」，所以停损报账与返工铃都能 O(1) 读到，不必重读工件。
    pub tests: Vec<StageTest>,
}

/// 一关的测试结论。按 `(task, stage)` 唯一；runs 是那份判定当时从盘上读出来的每一条。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageTest {
    pub task: TaskId,
    pub stage: Stage,
    pub runs: Vec<TestRun>,
}

impl StageTest {
    /// 这一关现在到底是不是绿的：只要有一条读得出的结论失败就不算绿。
    /// `None`（没有结论）不算绿也不算红——它交给调用方去说「这一关没交测试日志」。
    pub fn green(&self) -> Option<bool> {
        self.runs
            .iter()
            .filter(|r| r.carries_verdict())
            .map(|r| !r.failed_run())
            .min()
    }
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
    // 单位中立的新名 + 老名 alias：检查点是落盘的，2026-11-05 之前的归档段
    // 必须还能读回来，否则「截断后结论不变」那条不变式会因为改名而失效。
    #[serde(rename = "spent", alias = "spent_usd")]
    pub spent: BTreeMap<String, f64>,
    pub in_flight: Vec<InFlightBell>,
    #[serde(rename = "nominal", alias = "nominal_usd")]
    pub nominal: BTreeMap<String, f64>,
    pub tokens: BTreeMap<String, crate::provider::wire::Usage>,
    pub reserved: Vec<ReservationEntry>,
    /// 名义钱的来源分类也必须跨检查点活着，否则截断之后「说不清」就只剩一个总数了。
    #[serde(default)]
    pub nominal_by_reason: BTreeMap<NominalReason, f64>,
    /// peer × 来源分类，跨轮转仍能回答「哪个 peer 为什么说不清」。
    #[serde(default)]
    pub nominal_by_peer_reason: BTreeMap<String, BTreeMap<NominalReason, f64>>,
    /// 每个 (task, stage) 最后一次的测试结论。截断把旧前缀折进检查点，若这里空着，
    /// 「这一关测试过没过」就会在轮转后凭空消失——而它现在是状态。
    #[serde(default)]
    pub tests: Vec<StageTest>,
}

impl Checkpoint {
    pub fn capture(report: &ReconcileReport) -> Self {
        Self {
            stage: report.state.stage,
            iteration: report.state.iteration,
            spent: report.state.spent.clone(),
            in_flight: report.in_flight.clone(),
            nominal: report.state.nominal.clone(),
            tokens: report.state.tokens.clone(),
            // BTreeMap 的迭代顺序是确定的，所以同一份账本捕获出的检查点字节一致
            // ——「在线投影 == 全量重放」那条断言依赖这一点。
            reserved: report
                .state
                .reserved
                .iter()
                .map(|(key, booking)| (key.clone(), *booking))
                .collect(),
            tests: report.state.tests.clone(),
            nominal_by_reason: report.state.nominal_by_reason.clone(),
            nominal_by_peer_reason: report.state.nominal_by_peer_reason.clone(),
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
    /// 每个 (task, stage) 最后一次的测试结论（覆盖，不追加），所以它的长度上界是
    /// 「并存的 task × stage 数」，不随 iteration 增长。
    tests: Vec<StageTest>,
    /// records physically present in the live file (a checkpoint line counts
    /// as one), which is what the rotation trigger looks at
    live_records: usize,
    /// 名义钱按来源的总额（`nominal` 的分类账）。归零就删键，于是报账不会写出
    /// 「under_reported 0.0000」这种空话。
    nominal_by_reason: BTreeMap<NominalReason, f64>,
    nominal_by_peer_reason: BTreeMap<String, BTreeMap<NominalReason, f64>>,
}

impl Folder {
    /// 挪动某个来源的名义金额：加完就删零，避免分类账里留一堆 0.0 的键。
    fn shift_nominal(&mut self, peer: &str, reason: NominalReason, delta: f64) {
        let slot = self.nominal_by_reason.entry(reason).or_insert(0.0);
        *slot += delta;
        if slot.abs() < 1e-12 {
            self.nominal_by_reason.remove(&reason);
        }
        let peer_empty = {
            let by_reason = self
                .nominal_by_peer_reason
                .entry(peer.to_string())
                .or_default();
            let slot = by_reason.entry(reason).or_insert(0.0);
            *slot += delta;
            if slot.abs() < 1e-12 {
                by_reason.remove(&reason);
            }
            by_reason.is_empty()
        };
        if peer_empty {
            self.nominal_by_peer_reason.remove(peer);
        }
    }

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
            JournalRecord::ResultCommitted {
                task,
                stage,
                verdict,
            } => {
                self.settled.last_stage = Some(*stage);
                // dev 结果提交结算该 stage 的在途铃（任何 iteration）
                self.in_flight.retain(|(_, _, bell)| bell.stage != *stage);
                // stage 结了账，它的预留配对信息就不再有人需要：钱已经记进
                // spent（未结算的那笔按预留额留在账上，宁可不退）。留着这些键
                // 会让账本随 pipeline 无限增长，所以按 (task, stage) 收掉。
                let name = stage_name(*stage);
                self.reserved
                    .retain(|(t, _, s, _), _| !(s == &name && t == task));
                // 结论同样按 (task, stage) 覆盖：这里要回答的是「现在这一关的测试
                // 结论是什么」。一份什么都没数出来的提交，就是把这一关的结论撤掉，
                // 而不是留着上一轮那个更好看的数字。
                self.tests
                    .retain(|t| !(t.task == *task && t.stage == *stage));
                if !verdict.test_runs.is_empty() {
                    self.tests.push(StageTest {
                        task: task.clone(),
                        stage: *stage,
                        runs: verdict.test_runs.clone(),
                    });
                    // 定序：检查点要字节可比，投影相等那条断言也依赖这一点。
                    self.tests
                        .sort_by(|a, b| a.task.cmp(&b.task).then(a.stage.cmp(&b.stage)));
                }
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
                cost,
                priced,
            } => {
                *self.spent.entry(peer.clone()).or_insert(0.0) += cost;
                if !priced {
                    *self.nominal.entry(peer.clone()).or_insert(0.0) += cost;
                    // 预留在记下来时就是名义的，只可能是「价格表里查不到这个 peer 的
                    // model」——按构造如此（§9.27），所以这里不需要额外字段。
                    self.shift_nominal(peer, NominalReason::NoRate, *cost);
                }
                self.reserved
                    .insert(budget_key(task, peer, *stage, *iteration), (*cost, *priced));
            }
            // 结算换掉同坐标的预留：钱只计一次，多出来的部分退回去。找不到预留
            // 的结算（重复投递、或该 stage 早已结了账）不记账——宁可漏计一次重复
            // 的，也不把一笔派发烧成两笔。这里**不**累加 token：结算是「钱的动作」，
            // 用量住在 UsageReported 那一格，两者分开才不会有「没有费率 => 没用东西」。
            JournalRecord::BudgetSettled {
                task,
                peer,
                stage,
                iteration,
                cost,
                priced,
                nominal_reason,
                // `usage` 留在这条记录里是审计（结算对着哪份读数算的钱），但折叠不读它：
                // 用量的来源只有 `UsageReported` 那一格。
                usage: _,
            } => {
                let key = budget_key(task, peer, *stage, *iteration);
                if let Some((prior_cost, prior_priced)) = self.reserved.remove(&key) {
                    *self.spent.entry(peer.clone()).or_insert(0.0) += cost - prior_cost;
                    if !prior_priced {
                        let slot = self.nominal.entry(peer.clone()).or_insert(0.0);
                        *slot -= prior_cost;
                        if slot.abs() < 1e-12 {
                            *slot = 0.0;
                        }
                        // 之前那笔名义钱来自预留（BudgetSpent），来源只能是「没有价」。
                        self.shift_nominal(peer, NominalReason::NoRate, -prior_cost);
                    }
                    if !priced {
                        *self.nominal.entry(peer.clone()).or_insert(0.0) += cost;
                        // 老记录没有这个字段：按「没有价」处理，而不是按 0 或猜别的。
                        self.shift_nominal(
                            peer,
                            nominal_reason.unwrap_or(NominalReason::NoRate),
                            *cost,
                        );
                    }
                }
            }
            // Approvals are audit records: they never move the stage projection,
            // and a replay must not count them as bells.
            JournalRecord::ApprovalRequested { .. }
            | JournalRecord::ApprovalDecided { .. }
            | JournalRecord::ApprovalAcked { .. } => {}
            // 用量读数是**用量**的真相，不是钱的真相。没有费率时结算根本不会发生，
            // 所以这一格必须自己把 token 记进投影——否则收尾那行会印出
            // `tokens in 0 / out 0`，把一次烧掉两万多 token 的运行说成什么都没花。
            JournalRecord::UsageReported { peer, usage } => {
                self.tokens
                    .entry(peer.clone())
                    .or_default()
                    .accumulate(usage);
            }
            JournalRecord::Snapshot { checkpoint, .. } => {
                // hard reset of the fold: the prefix is already inside the
                // checkpoint, and its in-flight set replaces whatever we had.
                self.in_flight.clear();
                for b in &checkpoint.in_flight {
                    self.in_flight
                        .push((b.task.clone(), b.to.clone(), b.bell.clone()));
                }
                self.spent.clear();
                self.spent.extend(checkpoint.spent.clone());
                // 结算配对必须跟着检查点走：归档掉的前缀里那些还没结算的预留，
                // 若不在这里恢复，之后到达的结算会当成「无预留」而再记一次钱。
                self.nominal.clear();
                self.nominal.extend(checkpoint.nominal.clone());
                self.tokens.clear();
                self.tokens.extend(checkpoint.tokens.clone());
                self.reserved.clear();
                for (key, booking) in &checkpoint.reserved {
                    self.reserved.insert(key.clone(), *booking);
                }
                self.tests.clear();
                self.tests.extend(checkpoint.tests.clone());
                self.nominal_by_reason.clear();
                self.nominal_by_reason
                    .extend(checkpoint.nominal_by_reason.clone());
                self.nominal_by_peer_reason.clear();
                self.nominal_by_peer_reason
                    .extend(checkpoint.nominal_by_peer_reason.clone());
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
                spent: self.spent.clone(),
                nominal: self.nominal.clone(),
                nominal_by_reason: self.nominal_by_reason.clone(),
                nominal_by_peer_reason: self.nominal_by_peer_reason.clone(),
                tokens: self.tokens.clone(),
                reserved: self.reserved.clone(),
                tests: self.tests.clone(),
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
                    test_runs: vec![],
                    notes: vec![],
                    attribution: Default::default(),
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
            JournalRecord::ApprovalAcked {
                req: "a-1".into(),
                by: "harness-rd".into(),
                applied: true,
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
        assert!(rep.state.spent.is_empty());
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
            JournalRecord::UsageReported {
                peer: "rd".into(),
                usage: usage(100, 20),
            },
            JournalRecord::settled("t", "rd", Stage::Dev, 1, usage(100, 20), 0.12, true),
        ];
        let rep = reconcile(&records);
        assert!(
            (rep.state.spent["rd"] - 0.12).abs() < 1e-9,
            "{:?}",
            rep.state.spent
        );
        assert!(rep.state.nominal.is_empty(), "按价结算的不是名义值");
        assert_eq!(
            rep.state.tokens["rd"],
            usage(100, 20),
            "用量来自读数那条，不是结算那条"
        );
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
        assert!((rep.state.spent["rd"] - 0.12).abs() < 1e-9);
        assert_eq!(rep.state.nominal.get("rd").copied().unwrap_or(0.0), 0.0);
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
        assert!((rep.state.spent["rd"] - 0.5).abs() < 1e-9);
        assert!((rep.state.nominal["rd"] - 0.5).abs() < 1e-9);
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
        assert_eq!(rep.state.spent.get("rd").copied().unwrap_or(0.0), 0.0);
        assert!(
            rep.state.tokens.is_empty(),
            "结算不再是用量的来源：被丢弃的结算既不记钱也不记 token"
        );
    }

    /// 没有费率的运行也要报得出用量（2026-11-05 真端点联跑抓到的那一格）：那条路径
    /// 根本不落 `BudgetSettled`，所以用量若只在结算时累加，收尾那行就印成
    /// `tokens in 0 / out 0`——把一次烧掉两万多 token 的运行说成什么都没花。
    /// 钱的部分不变：仍按名义计一次。
    #[test]
    fn a_nominal_run_reports_the_tokens_it_read() {
        let records = vec![
            JournalRecord::reserved("t", "rd", Stage::Dev, 1, 0.5, false),
            JournalRecord::UsageReported {
                peer: "rd".into(),
                usage: usage(5_646, 6_210),
            },
        ];
        let rep = reconcile(&records);
        assert_eq!(
            rep.state.tokens["rd"],
            usage(5_646, 6_210),
            "读数进账，与有没有费率无关"
        );
        assert!(
            (rep.state.nominal["rd"] - 0.5).abs() < 1e-9,
            "钱仍按名义计一次: {:?}",
            rep.state.nominal
        );

        // 一份读数配两次结算（重复投递）也只能算一次用量
        let doubled = vec![
            JournalRecord::reserved("t", "qa", Stage::Test, 1, 0.2, true),
            JournalRecord::UsageReported {
                peer: "qa".into(),
                usage: usage(10, 5),
            },
            JournalRecord::settled("t", "qa", Stage::Test, 1, usage(10, 5), 0.02, true),
            JournalRecord::settled("t", "qa", Stage::Test, 1, usage(10, 5), 0.02, true),
        ];
        let rep = reconcile(&doubled);
        assert_eq!(
            rep.state.tokens["qa"],
            usage(10, 5),
            "重复结算不烧第二笔用量"
        );
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
            (rep.state.spent["rd"] - 0.5).abs() < 1e-9,
            "配不上对的结算被忽略，iter1 的预留原样留在账上: {:?}",
            rep.state.spent
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
                    test_runs: vec![],
                    notes: vec![],
                    attribution: Default::default(),
                },
            },
        ];
        let rep = reconcile(&records);
        assert!(rep.state.reserved.is_empty(), "配对键随 stage 结算收掉");
        assert!((rep.state.spent["rd"] - 0.5).abs() < 1e-9);
        // 另一个 task 的同名 stage 不受影响
        let other = vec![
            JournalRecord::reserved("t2", "rd", Stage::Dev, 1, 0.5, false),
            JournalRecord::ResultCommitted {
                task: "t1".into(),
                stage: Stage::Dev,
                verdict: Verdict {
                    pass: true,
                    issues: vec![],
                    test_runs: vec![],
                    notes: vec![],
                    attribution: Default::default(),
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
            spent: [("rd".to_string(), 0.5)].into_iter().collect(),
            in_flight: vec![],
            nominal: Default::default(),
            tokens: Default::default(),
            reserved: vec![(key, (0.5, true))],
            tests: Default::default(),
            nominal_by_reason: Default::default(),
            nominal_by_peer_reason: Default::default(),
        };
        let records = vec![
            JournalRecord::Snapshot {
                covers: 1,
                checkpoint,
            },
            JournalRecord::UsageReported {
                peer: "rd".into(),
                usage: usage(30, 12),
            },
            JournalRecord::settled("t", "rd", Stage::Dev, 1, usage(30, 12), 0.12, true),
        ];
        let rep = reconcile(&records);
        assert!(
            (rep.state.spent["rd"] - 0.12).abs() < 1e-9,
            "{:?}",
            rep.state.spent
        );
        assert_eq!(rep.state.tokens["rd"], usage(30, 12));
    }

    /// 把金额字段从 `usd` 改成单位中立的 `cost`（§9.44）**不许让旧账失忆**：
    /// journal 是真相源，昨天的 `usd` 行读不回金额，停损/截断/审计就都从既有花费上
    /// 重新开始数——那正是「同一笔派发计两次钱」的入口。alias 由这一条钉住。
    #[test]
    fn old_usd_keyed_lines_still_replay() {
        let line = r#"{"type":"budget_spent","task":"demo","peer":"rd","stage":"dev","iteration":1,"usd":0.5,"priced":false}"#;
        let back: JournalRecord = serde_json::from_str(line).expect("旧 key 仍可读");
        match &back {
            JournalRecord::BudgetSpent { cost, priced, .. } => {
                assert!((*cost - 0.5).abs() < 1e-9, "{cost}");
                assert!(!priced, "老行没有 priced 之外的信息，也不该编出");
            }
            other => panic!("wrong record: {other:?}"),
        }
        // 折叠也认得它：钱与名义那一份都落进来
        let rep = reconcile(&[back]);
        assert!(
            (rep.state.spent["rd"] - 0.5).abs() < 1e-9,
            "{:?}",
            rep.state.spent
        );
        assert!((rep.state.nominal["rd"] - 0.5).abs() < 1e-9);

        // 旧检查点里的 `spent_usd` / `nominal_usd` 同样读得回来
        let ckpt = r#"{"type":"snapshot","covers":2,"checkpoint":{"stage":"dev","iteration":1,"spent_usd":{"rd":0.5},"in_flight":[],"nominal_usd":{"rd":0.5},"tokens":{},"reserved":[]}}"#;
        let back: JournalRecord = serde_json::from_str(ckpt).expect("老检查点仍能解析");
        let rep = reconcile(&[back]);
        assert!(
            (rep.state.spent["rd"] - 0.5).abs() < 1e-9,
            "检查点带过来的钱不因改名而蒸发: {:?}",
            rep.state.spent
        );
        assert!(
            (rep.state.nominal["rd"] - 0.5).abs() < 1e-9,
            "名义那一份也要跟着活下来: {:?}",
            rep.state.nominal
        );

        // 新行写出去的是单位中立的名字（alias 只进不出，不会把两种拼法都留在盘上）
        let fresh = serde_json::to_string(&JournalRecord::reserved(
            "demo",
            "rd",
            Stage::Dev,
            1,
            0.5,
            false,
        ))
        .unwrap();
        assert!(
            fresh.contains("\"cost\":0.5") && !fresh.contains("\"usd\""),
            "{fresh}"
        );
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
            spent: [("rd".to_string(), 0.5)].into_iter().collect(),
            in_flight: vec![InFlightBell {
                task: "t".into(),
                to: "rd".into(),
                bell: bell("rd", Stage::Dev, 1),
            }],
            nominal: Default::default(),
            tokens: [("rd".to_string(), usage(8, 8))].into_iter().collect(),
            reserved: vec![(key, (0.5, true))],
            tests: Default::default(),
            nominal_by_reason: Default::default(),
            nominal_by_peer_reason: Default::default(),
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
            (rep.state.spent["rd"] - 0.12).abs() < 1e-9,
            "检查点带过来的 0.5 预留被结算换成 0.12，而不是加在旁边: {:?}",
            rep.state.spent
        );
    }

    /// `Verdict.test_runs` 是这一轮新加的字段。除往返一致之外，更要紧的是**没有这个
    /// 字段的老 journal 行仍然必须能重放**（不变式 5：真相源不能因为加了一格就读不了
    /// 历史）；而读回来的「缺席」是空列表，不是「0 条失败」。
    #[test]
    fn verdict_test_runs_roundtrip_and_old_lines_still_replay() {
        let line = r#"{"type":"result_committed","task":"t","stage":"dev","verdict":{"pass":true,"issues":[]}}"#;
        let old: JournalRecord = serde_json::from_str(line).unwrap();
        let JournalRecord::ResultCommitted { verdict, .. } = &old else {
            panic!("expected a result_committed line");
        };
        assert!(
            verdict.test_runs.is_empty(),
            "老行的缺字段读成空: {:?}",
            verdict.test_runs
        );
        assert_eq!(
            verdict.attribution,
            Attribution::Artifact,
            "老行没有归因字段：落在产物那一侧（更贵的默认），不是猜个「回合」把这一关放过去"
        );

        // `JournalRecord` 不实现 PartialEq（它带 Value/浮点，等值比较本来就没意义），
        // 所以这里比的是「读回来的 verdict」与「序列化文本逐字节一致」。
        let verdict = Verdict {
            pass: false,
            issues: vec![JudgmentIssue::artifact("did not pass")],
            notes: vec![],
            attribution: Attribution::Artifact,
            test_runs: vec![TestRun {
                path: "/tmp/unittest.log".into(),
                exit_code: Some(1),
                exit_signal: None,
                passed: Some(24),
                failed: Some(2),
                ran: Some(26),
            }],
        };
        let fresh = JournalRecord::ResultCommitted {
            task: "t".into(),
            stage: Stage::Dev,
            verdict: verdict.clone(),
        };
        let text = serde_json::to_string(&fresh).unwrap();
        let back: JournalRecord = serde_json::from_str(&text).unwrap();
        let JournalRecord::ResultCommitted {
            verdict: read_back, ..
        } = &back
        else {
            panic!("roundtrip changed the record kind");
        };
        assert_eq!(read_back, &verdict, "读出来的结论要能原样落盘再读回");
        assert_eq!(serde_json::to_string(&back).unwrap(), text);
    }

    /// New issues carry their own attribution. Legacy string issues remain
    /// readable and default to the cheaper-to-route artifact side, while a
    /// legacy global `attribution: turn` is preserved.
    #[test]
    fn judgment_issues_are_typed_and_legacy_strings_still_parse() {
        let legacy = json!({
            "pass": false,
            "issues": ["old issue"],
            "attribution": "turn"
        });
        let parsed: Verdict = serde_json::from_value(legacy).unwrap();
        assert_eq!(parsed.attribution, Attribution::Turn);
        assert_eq!(parsed.issues, vec![JudgmentIssue::artifact("old issue")]);

        let verdict = Verdict::new(
            false,
            vec![
                JudgmentIssue::artifact("artifact problem"),
                JudgmentIssue::turn("turn problem"),
            ],
            vec![],
            vec![],
        );
        assert_eq!(
            verdict.attribution,
            Attribution::Turn,
            "any turn-side issue keeps the existing conservative routing"
        );
        let line = serde_json::to_string(&verdict).unwrap();
        assert!(line.contains("\"message\":\"turn problem\""), "{line}");
        assert!(line.contains("\"attribution\":\"turn\""), "{line}");
        assert_eq!(serde_json::from_str::<Verdict>(&line).unwrap(), verdict);
    }

    /// 「有价但读数不可信」的结算（§9.36）：钱必须留在账上、并且留在**名义那一份**里，
    /// 同时配对键要消耗掉——否则同一次派发会被再来一次结算计成两笔。
    #[test]
    fn booking_a_priced_reservation_back_as_nominal_keeps_the_money() {
        let records = vec![
            JournalRecord::reserved("t", "qa", Stage::Test, 1, 0.160_768, true),
            JournalRecord::UsageReported {
                peer: "qa".into(),
                usage: usage(0, 671),
            },
            JournalRecord::settled_as_nominal(
                "t",
                "qa",
                Stage::Test,
                1,
                usage(0, 671),
                0.160_768,
                NominalReason::UnderReported,
            ),
        ];
        let rep = reconcile(&records);
        assert!(
            (rep.state.spent["qa"] - 0.160_768).abs() < 1e-9,
            "按名义多计，不是按少报的读数少计: {:?}",
            rep.state.spent
        );
        assert!(
            (rep.state.nominal["qa"] - 0.160_768).abs() < 1e-9,
            "名义那一份要跟着涨，报账才说不清这钱是「算不出来」的: {:?}",
            rep.state.nominal
        );
        assert!(
            rep.state.reserved.is_empty(),
            "配对键消耗掉了，不会被二次结算烧穿"
        );
        assert_eq!(rep.state.tokens["qa"], usage(0, 671), "读数照样留档");
        assert!(
            (rep.state.nominal_by_reason[&NominalReason::UnderReported] - 0.160_768).abs() < 1e-12,
            "来源还要说得出这笔为什么算不成账单: {:?}",
            rep.state.nominal_by_reason
        );
        assert!(
            (rep.state.nominal_by_peer_reason["qa"][&NominalReason::UnderReported] - 0.160_768)
                .abs()
                < 1e-12,
            "分类还要落到具体 peer: {:?}",
            rep.state.nominal_by_peer_reason
        );
    }

    /// 名义钱分得清来源了（§9.37）：「没填价」与「读数不可信」是两种不同的说不清，
    /// 而分类账加起来必须等于那个总数——否则分类只是装饰。
    #[test]
    fn nominal_money_says_which_reason_it_is() {
        let got = |rep: &ReconcileReport, reason: NominalReason| -> f64 {
            rep.state
                .nominal_by_reason
                .get(&reason)
                .copied()
                .unwrap_or(-1.0)
        };
        let rep = reconcile(&[
            // 没有价：预留按名义记
            JournalRecord::reserved("t", "rd", Stage::Dev, 1, 0.5, false),
            // 有价但读数不可信：结算按名义落，并把来源说出来
            JournalRecord::reserved("t", "qa", Stage::Test, 1, 0.032, true),
            JournalRecord::settled_as_nominal(
                "t",
                "qa",
                Stage::Test,
                1,
                usage(0, 8197),
                0.032,
                NominalReason::UnderReported,
            ),
        ]);
        assert!(
            (got(&rep, NominalReason::NoRate) - 0.5).abs() < 1e-12,
            "{:?}",
            rep.state.nominal_by_reason
        );
        assert!(
            (got(&rep, NominalReason::UnderReported) - 0.032).abs() < 1e-12,
            "{:?}",
            rep.state.nominal_by_reason
        );
        let by_peer: f64 = rep.state.nominal.values().sum();
        let by_reason: f64 = rep.state.nominal_by_reason.values().sum();
        let by_peer_reason: f64 = rep
            .state
            .nominal_by_peer_reason
            .values()
            .flat_map(|reasons| reasons.values())
            .sum();
        assert!(
            (by_peer - by_reason).abs() < 1e-12,
            "分类账加起来必须等于总数: {by_peer} vs {by_reason}"
        );
        assert!(
            (by_peer - by_peer_reason).abs() < 1e-12,
            "peer × 原因也必须等于总数: {by_peer} vs {by_peer_reason}"
        );
        assert!(
            (rep.state.nominal_by_peer_reason["rd"][&NominalReason::NoRate] - 0.5).abs() < 1e-12,
            "{:?}",
            rep.state.nominal_by_peer_reason
        );
        assert!(
            (rep.state.nominal_by_peer_reason["qa"][&NominalReason::UnderReported] - 0.032).abs()
                < 1e-12,
            "{:?}",
            rep.state.nominal_by_peer_reason
        );

        // 老记录没有这个字段：按「没有价」处理，不去猜别的来源。
        let legacy = reconcile(&[
            JournalRecord::reserved("t", "qa", Stage::Test, 1, 0.4, true),
            JournalRecord::settled("t", "qa", Stage::Test, 1, usage(0, 10), 0.4, false),
        ]);
        assert!(
            (got(&legacy, NominalReason::NoRate) - 0.4).abs() < 1e-12,
            "{:?}",
            legacy.state.nominal_by_reason
        );
        assert!(
            !legacy
                .state
                .nominal_by_reason
                .contains_key(&NominalReason::UnderReported),
            "没有来源标记就不能算成「少报」: {:?}",
            legacy.state.nominal_by_reason
        );
        assert!(
            (legacy.state.nominal_by_peer_reason["qa"][&NominalReason::NoRate] - 0.4).abs() < 1e-12,
            "{:?}",
            legacy.state.nominal_by_peer_reason
        );

        // 检查点必须带着分类账：否则截断之后「说不清」只剩一个总数。
        let cp = Checkpoint::capture(&rep);
        assert_eq!(cp.nominal_by_reason.len(), 2, "{:?}", cp.nominal_by_reason);
        assert_eq!(
            cp.nominal_by_peer_reason, rep.state.nominal_by_peer_reason,
            "peer 维度也必须进检查点"
        );
        let line = serde_json::to_string(&JournalRecord::Snapshot {
            covers: 3,
            checkpoint: cp,
        })
        .expect("a checkpoint with nominal reasons serializes");
        let back: JournalRecord = serde_json::from_str(&line).expect("and parses back");
        let after = reconcile(std::slice::from_ref(&back));
        assert_eq!(
            after.state.nominal_by_reason, rep.state.nominal_by_reason,
            "截断前后分类必须一致"
        );
        assert_eq!(
            after.state.nominal_by_peer_reason, rep.state.nominal_by_peer_reason,
            "截断前后 peer × 原因分类必须一致"
        );
    }

    fn run(code: Option<i64>, passed: Option<u64>, failed: Option<u64>) -> TestRun {
        TestRun {
            path: "/tmp/unittest.log".into(),
            exit_code: code,
            exit_signal: None,
            passed,
            failed,
            ran: passed,
        }
    }

    fn committed(task: &str, stage: Stage, runs: Vec<TestRun>) -> JournalRecord {
        JournalRecord::ResultCommitted {
            task: task.into(),
            stage,
            verdict: Verdict {
                pass: true,
                issues: vec![],
                test_runs: runs,
                notes: vec![],
                attribution: Default::default(),
            },
        }
    }

    /// 「这一关的测试结论」现在是**状态**（§9.35）：它随提交覆盖而不是追加、被什么都没
    /// 数出来的提交撤掉、并且必须跨轮转与序列化活着。三条都在一条测试里，因为它们共用
    /// 同一次折叠——`reconcile` 与在线投影是同一份代码（不变式 5）。
    #[test]
    fn the_test_conclusion_is_state_not_just_payload() {
        // (a) 覆盖而不是追加
        let rep = reconcile(&[
            committed("t", Stage::Dev, vec![run(Some(0), Some(32), Some(0))]),
            committed("t", Stage::Dev, vec![run(Some(1), Some(30), Some(2))]),
        ]);
        assert_eq!(rep.state.tests.len(), 1, "同一关只留最后一次结论");
        assert_eq!(rep.state.tests[0].green(), Some(false), "最后一次是挂的");

        // (b) 撤掉：一份什么都没数出来的提交，不能留着上一轮那个更好看的数字
        let rep = reconcile(&[
            committed("t", Stage::Dev, vec![run(Some(0), Some(32), Some(0))]),
            committed("t", Stage::Dev, vec![]),
        ]);
        assert!(
            rep.state.tests.is_empty(),
            "撤掉就是撤掉: {:?}",
            rep.state.tests
        );

        // (c) 跨轮转与序列化活着：先折进检查点，再从检查点重放
        let prefix = [
            committed("t", Stage::Dev, vec![run(Some(0), Some(32), Some(0))]),
            committed("t", Stage::Review, vec![run(Some(0), Some(7), Some(0))]),
        ];
        let live = reconcile(&prefix);
        let checkpoint = Checkpoint::capture(&live);
        assert_eq!(checkpoint.tests.len(), 2, "两条结论都要抄进去");
        let line = serde_json::to_string(&JournalRecord::Snapshot {
            covers: 2,
            checkpoint,
        })
        .expect("a checkpoint with conclusions serializes");
        let back: JournalRecord = serde_json::from_str(&line).expect("and parses back");
        let JournalRecord::Snapshot { checkpoint, .. } = &back else {
            panic!("roundtrip changed the record kind");
        };
        let after = reconcile(std::slice::from_ref(&back));
        assert_eq!(
            after.state.tests, live.state.tests,
            "截断前后的结论必须一致"
        );
        assert_eq!(
            checkpoint
                .tests
                .iter()
                .map(|t| (t.stage, t.green()))
                .collect::<Vec<_>>(),
            vec![(Stage::Dev, Some(true)), (Stage::Review, Some(true))],
            "定序：检查点要字节可比"
        );
        assert_eq!(serde_json::to_string(&back).unwrap(), line, "字节级往返");
    }
}
