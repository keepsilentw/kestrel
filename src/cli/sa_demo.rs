//! `kestrel demo-sa`: scripted SA driver for the 3-terminal demo (plan §4-M4
//! demo 场景 1-4)。真实派发智能由 SA 角色 prompt + LLM 在后续版本接管；本
//! 驱动验证的是 write-ahead 顺序、崩溃对账、fail 回环与预算停损的机械语义。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;

use crate::bus::frame::Envelope;
use crate::bus::rpc::BusClient;
use crate::cli::evidence::{
    Evidence, EvidenceKind, check_evidence, describe_run, evidence_from_answer,
    missing_requirements,
};
use crate::cli::serve::{approval_command, approval_receipt_from_frame, decision_name};
use crate::cli::task::artifact_path;
use crate::core::ops::ApprovalDecision;
use crate::journal::Journal;
use crate::journal::records::Attribution;
use crate::journal::records::{JournalRecord, JudgmentIssue, Stage, TestRun, ThinBell, Verdict};
use crate::permissions::{Decision, PermissionEngine};
use crate::provider::config::Registry;
use crate::provider::wire::{CostPerMTok, Usage};

pub struct DemoSaArgs {
    pub bus_socket: PathBuf,
    pub task: String,
    pub journal: PathBuf,
    pub artifacts: PathBuf,
    /// 单次派发的名义记账金额：没有价格表时它按派发次数计钱，是停损单位而不是账单。
    /// 真实 token 用量另记 `usage_reported`（见下）。
    pub per_dispatch_cost: f64,
    pub max_cost: f64,
    /// 名义记账用的币种。**只在价格表没人声明币种时才有意义**：有价可查时以表里
    /// 声明的为准（`currency`，design §9.44），因为钱的单位要么有来源，要么当场
    /// 说清，不能由字段名猜。
    pub currency: String,
    pub max_iterations: u64,
    /// 一次 pipeline 允许把「验证没过」的活儿退回给写者几次。验证类 stage 的 FAIL
    /// 归因在被验证的产物上，而产物在写者手里（design §9.32④）——回跳要有额度，
    /// 否则一条永不满足的验收标准可以在 dev↔review 之间来回打铃打到预算停损。
    pub max_reworks: u64,
    /// How long to wait for a worker's result bell before declaring the bell
    /// lost (the worker keeps the turn; the SA falls back to disk inspection).
    pub bell_timeout: Duration,
    /// The dispatch worktree RD writes into. It travels in the bell `ref` so QA
    /// verifies the same tree instead of guessing — without it the reviewer has
    /// nothing on disk to look at and stages fail for the wrong reason.
    pub worktree: Option<PathBuf>,
    /// Operator-configured build command run by the SA before the build stage.
    /// Its kernel-written log is trusted evidence.
    pub build_command: Option<String>,
    /// Approval authority for workers running `approve = "bus"`: a three-state
    /// prefix table (same format as a role policy). Absent means the SA denies
    /// every reverse request — fail closed, no default yes.
    pub approvals: Option<PathBuf>,
    /// Journal truncation threshold (design §10 快照/截断语义): once the live
    /// file holds this many records, everything older than half of it is folded
    /// into a checkpoint and archived into `<journal>.seg<from>-<to>.jsonl`. 0 = never
    /// rotate, which is the default: the journal stays one file until somebody
    /// decides a long run needs the bounded replay.
    pub rotate_after: usize,
    /// Byte ceiling for the live journal: records vary in size (an approval
    /// record carries stderr), so a record count alone can leave a long-running
    /// journal file huge. 0 disables this condition.
    pub rotate_max_bytes: u64,
    /// Task namespace for this pipeline: it stamps every journal record, every
    /// bell, and — through `cli::task` — the artifact names both ends agree on.
    /// Two `demo-sa` processes with different ids can share one hub.
    pub task_id: String,
    /// Peer the dev stage is dispatched to (a second pipeline points this at its
    /// own worker, e.g. `--rd-peer t2:harness-rd`).
    pub rd_peer: String,
    /// Peer the review/test/build stages are dispatched to.
    pub qa_peer: String,
    /// role 目录：账本从这里知道「哪个 peer 跑在哪个 provider/model 上」。
    /// 缺省 => 一律按名义值记账。
    pub roles_dir: Option<PathBuf>,
    /// 费率表（`~/.kestrel/providers.toml`）。缺省 => 用用户层那个（存在才读）。
    pub prices: Option<PathBuf>,
    /// stage 的证据要求表（JSON：`{"test": ["test_log"], ...}`）。没给 = 不要求任何
    /// 类型，只要 worker 点名的东西在盘上成立——也就是类型化之前的语义。
    pub evidence_spec: Option<PathBuf>,
}

/// 一个 peer 后端的定价依据：SA 用它把 token 换成钱。
#[derive(Debug, Clone)]
pub struct BackendPrice {
    /// role 的 `[llm]`：哪个 provider 的哪个 model（写进账本，事后能复算）。
    pub provider: String,
    pub model: String,
    /// registry 声明的费率。`None` = 这张表没人填——**不等于免费**。
    pub cost: Option<CostPerMTok>,
    /// 那张价目表写的什么币种（provider 级 `currency`，缺省读成 `usd`）。
    pub currency: Option<String>,
    /// 预留上界的两个输入：整窗输入 + 满输出。
    pub context_window: u32,
    pub max_out: u32,
}

/// 账本的价格来源。方向很重要：**worker 报读数，SA 定价**——费率只在用户层
/// `providers.toml` 里写着（provider/key 的唯一归属地，design §6），由记账的人去
/// 查表；花钱的一方不能自己开价。
#[derive(Debug, Default)]
pub struct PriceLedger {
    peers: BTreeMap<String, BackendPrice>,
}

impl PriceLedger {
    /// 从 role 目录 + registry 组装价格表。两个输入都可缺：缺哪个就退回名义记账
    /// （fail-closed on money：说不清价就按名义扣，绝不按 0 扣）。
    pub fn discover(roles_dir: Option<&Path>, registry: Option<&Registry>) -> Self {
        let Some(dir) = roles_dir else {
            return Self::default();
        };
        let Ok(entries) = std::fs::read_dir(dir) else {
            tracing::warn!("price ledger: cannot read roles dir {}", dir.display());
            return Self::default();
        };
        let mut peers = BTreeMap::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("toml") {
                continue;
            }
            let Ok(role) = crate::config::RoleConfig::load(&path) else {
                continue;
            };
            let Some(llm) = role.llm.as_ref() else {
                continue;
            };
            // peer 名以 role 的 `peer_id` 为准（门铃与账本用的就是它），
            // 没有声明就退回文件名——与 serve 的 session 习惯一致。
            let peer = role.peer_id.clone().unwrap_or_else(|| {
                path.file_stem()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default()
            });
            let meta = registry.and_then(|r| r.model_meta(&llm.provider, &llm.model).ok());
            let cost = registry.and_then(|r| r.model_cost(&llm.provider, &llm.model));
            peers.insert(
                peer,
                BackendPrice {
                    provider: llm.provider.clone(),
                    model: llm.model.clone(),
                    cost,
                    currency: registry
                        .and_then(|r| r.provider_currency(&llm.provider))
                        .map(|c| c.to_lowercase()),
                    context_window: meta.as_ref().map(|m| m.context_window).unwrap_or(128_000),
                    max_out: meta.as_ref().map(|m| m.max_out).unwrap_or(16_384),
                },
            );
        }
        Self { peers }
    }

    /// 这一次运行的钱是什么币种（design §9.44）。所有**有价** peer 必须同一种：
    /// 账本是要加总的，一次运行里混两种币种的「总额」没有意义。混了 = 表写错了
    /// => 启动即失败，而不是挑一个猜。全部没价 => 用 CLI 声明的 `fallback`。
    pub fn unit(&self, fallback: &str) -> anyhow::Result<String> {
        let mut by_currency: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (peer, price) in &self.peers {
            if price.cost.is_some() {
                let currency = price
                    .currency
                    .clone()
                    .unwrap_or_else(|| "usd".to_string())
                    .to_lowercase();
                by_currency.entry(currency).or_default().push(peer.clone());
            }
        }
        match by_currency.len() {
            0 => Ok(fallback.to_lowercase()),
            1 => Ok(by_currency
                .keys()
                .next()
                .cloned()
                .unwrap_or_else(|| fallback.to_lowercase())),
            _ => {
                let parts: Vec<String> = by_currency
                    .iter()
                    .map(|(currency, peers)| format!("{currency} for {}", peers.join(",")))
                    .collect();
                anyhow::bail!(
                    "[sa] the price table mixes currencies in one run: {} — one ledger sums \
                     one unit, so declare the same `currency` on every priced provider",
                    parts.join(" / ")
                );
            }
        }
    }

    /// 派发前的预留金额 `(cost, priced)`：有费率 => 该模型的最坏情况账单；
    /// 没有费率 => 名义值，并带着「这是名义值」的标记进账本。
    pub fn reservation(&self, peer: &str, nominal_cost: f64) -> (f64, bool) {
        match self.peers.get(peer).and_then(|p| p.cost) {
            Some(cost) => (
                Usage::worst_case_cost(&cost, self.context_window(peer), self.max_out(peer)),
                true,
            ),
            None => (nominal_cost, false),
        }
    }

    /// 结算金额：实际 token × 费率。查不到费率返回 `None`——预留原样留在账上，
    /// 这比把「没人填价」记成「这次免费」诚实。
    pub fn settle(&self, peer: &str, usage: &Usage) -> Option<f64> {
        self.peers
            .get(peer)
            .and_then(|p| p.cost)
            .map(|cost| usage.cost(&cost))
    }

    pub fn is_priced(&self, peer: &str) -> bool {
        self.peers.get(peer).and_then(|p| p.cost).is_some()
    }

    /// 账本里已知的 peer（报告用：说不清价时告诉人停损按名义走）。
    pub fn known_peers(&self) -> Vec<&str> {
        self.peers.keys().map(String::as_str).collect()
    }

    fn context_window(&self, peer: &str) -> u32 {
        self.peers.get(peer).map(|p| p.context_window).unwrap_or(0)
    }

    fn max_out(&self, peer: &str) -> u32 {
        self.peers.get(peer).map(|p| p.max_out).unwrap_or(0)
    }
}

/// 用户层的费率表：存在才读，读失败只意味着「没有价」，不是启动失败——
/// SA 的派发能力不该依赖一个可选文件（但账本会因此退回名义值，报告里说清楚）。
fn load_prices(explicit: Option<&Path>) -> Option<Registry> {
    let path = explicit
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| crate::config::kestrel_home().join("providers.toml"));
    if !path.exists() {
        return None;
    }
    match Registry::load(&path) {
        Ok(reg) => Some(reg),
        Err(e) => {
            tracing::warn!("price ledger: cannot read {}: {e}", path.display());
            None
        }
    }
}

impl DemoSaArgs {
    /// Which peer serves a stage. Absent a prefix, all pipelines talk to the
    /// same `harness-rd` / `harness-qa` — the single-task demo behaviour.
    pub fn peer_for(&self, stage: Stage) -> &str {
        match stage {
            Stage::Dev => &self.rd_peer,
            _ => &self.qa_peer,
        }
    }
}

/// stage 的证据要求：这一关**必须**拿出哪些类型的证据才算收敛。
///
/// 表是数据（`--evidence-spec` 指向的 JSON），不是写在代码里的 if：验收标准由定
/// 标准的人改，不该为此重新编译 harness。
#[derive(Debug, Default)]
pub struct EvidenceSpec {
    per_stage: BTreeMap<String, Vec<EvidenceKind>>,
}

impl EvidenceSpec {
    /// `{"dev": ["file"], "test": ["test_log"], "build": ["test_log", "screenshot"]}`
    ///
    /// 读不出来就硬失败：一份坏掉的要求表若被当成「没有要求」，验收标准会在无人
    /// 察觉时被静默放宽——那是 fail-open，而这张表存在的理由正是防它。
    pub fn from_json_file(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read evidence spec {}", path.display()))?;
        let raw: BTreeMap<String, Vec<String>> =
            serde_json::from_str(&text).with_context(|| {
                format!(
                    "parse evidence spec {} (want {{\"stage\": [\"kind\"]}})",
                    path.display()
                )
            })?;
        let mut per_stage = BTreeMap::new();
        for (stage, kinds) in raw {
            if !["spec", "dev", "review", "test", "build", "release"].contains(&stage.as_str()) {
                tracing::warn!("evidence spec: no such stage {stage} — ignored");
                continue;
            }
            let mut parsed = Vec::with_capacity(kinds.len());
            for kind in &kinds {
                let k = EvidenceKind::parse(kind);
                if k == EvidenceKind::Unknown {
                    // 认不出的要求类型永远满足不了：留着它就是「永远回派」。
                    // 说清楚，让人去修表，而不是让 pipeline 空转。
                    anyhow::bail!(
                        "evidence spec: stage {stage} requires unknown evidence kind {kind} \
                         (file | test_log | build_log | diff | screenshot)"
                    );
                }
                parsed.push(k);
            }
            per_stage.insert(stage, parsed);
        }
        Ok(Self { per_stage })
    }

    /// 该 stage 的要求（表里没这一项 = 空 = 只核存在性）。
    pub fn required_for(&self, stage: Stage) -> &[EvidenceKind] {
        let name = format!("{stage:?}").to_lowercase();
        self.per_stage.get(&name).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn is_empty(&self) -> bool {
        self.per_stage.is_empty()
    }

    /// 开工时该对用户讲的话：哪些关被要求了什么。
    pub fn describe(&self) -> String {
        if self.per_stage.is_empty() {
            return "no kind requirements (existence-only)".to_string();
        }
        self.per_stage
            .iter()
            .map(|(stage, kinds)| {
                let names: Vec<&str> = kinds.iter().map(EvidenceKind::as_str).collect();
                format!("{stage}:{}", names.join("+"))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// 工件里的 `evidence` 数组：既接受类型化对象（`{"path":..., "kind":...}`），也接受
/// 类型化之前那份裸路径（一律按 `file`）。类型名的唯一来源是 `EvidenceKind::parse`，
/// 所以这里手写解析而不引 serde 派生——同一组线名不该有两份定义。
fn evidence_from_artifact(json: &serde_json::Value) -> Vec<Evidence> {
    let Some(items) = json["evidence"].as_array() else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|v| match v {
            serde_json::Value::String(path) => Some(Evidence::file(path.clone())),
            serde_json::Value::Object(obj) => {
                let path = obj.get("path").and_then(|p| p.as_str())?;
                let kind = obj
                    .get("kind")
                    .and_then(|k| k.as_str())
                    .map(EvidenceKind::parse)
                    .unwrap_or(EvidenceKind::File);
                Some(Evidence {
                    path: path.to_string(),
                    kind,
                })
            }
            _ => None,
        })
        .collect()
}

/// When to fold the journal into a checkpoint: either ceiling trips it, and a
/// zero ceiling disables that condition. Both inputs are O(1) reads (the live
/// record count comes from the online projection, the size from metadata) —
/// a rotation check must never cost more than the append it is guarding.
pub fn rotation_due(
    live_records: usize,
    live_bytes: u64,
    cap_records: usize,
    cap_bytes: u64,
) -> bool {
    if live_records < 2 {
        // folding a single record into a checkpoint is pure overhead
        return false;
    }
    (cap_records > 0 && live_records >= cap_records) || (cap_bytes > 0 && live_bytes >= cap_bytes)
}

/// 默认策略：**活文件是一条可读的边界，不是无限长的历史**。
/// 条数上限挡派发次数，字节上限挡记录大小不齐（一条 `approval_requested` 可以带着
/// 几十 KB 的 stderr，只按条数封顶挡不住文件长大）。这两个数不是性能猜测：它们是
/// 「崩溃对账与巡检要从活文件里读多少东西」的上界，而完整的审计历史在归档段里，
/// `replay_all()` 永远读得到（§9.22：截断不是删除）。
pub const DEFAULT_ROTATE_RECORDS: usize = 512;
pub const DEFAULT_ROTATE_MAX_BYTES: u64 = 1_048_576;

// 量级护栏放在**编译期**：上限太小会天天折文件（一次派发约 7 条记录），太大就挡不住增长。
// 它是常数，写成运行时断言就得等有人跑到那一行才说话——const block 直接钉死。
const _: () = {
    assert!(
        DEFAULT_ROTATE_RECORDS >= 64,
        "journal 条数上限太小，会天天折文件"
    );
    assert!(
        DEFAULT_ROTATE_MAX_BYTES <= 8 * 1_048_576,
        "journal 字节上限太大，挡不住活文件增长"
    );
};

/// CLI 的三态解析：没给 => 默认策略；给 0 => 明确关闭；其余 => 自定义。
/// 两个上限各自独立解析：只按条数封顶时，字节上限仍跟着默认值——「填了一个」
/// 不等于「另一个想要无限」。
pub fn resolve_rotation_policy(cap_records: Option<usize>, cap_bytes: Option<u64>) -> (usize, u64) {
    (
        cap_records.unwrap_or(DEFAULT_ROTATE_RECORDS),
        cap_bytes.unwrap_or(DEFAULT_ROTATE_MAX_BYTES),
    )
}

/// 在 stage 结算边界执行一次轮转策略：触顶就把较旧的约一半折进检查点并归档。
/// 抽成函数是为了让「默认策略」这条路被测试走的是生产代码，而不是测试自己重写一遍
/// 判断（那种测试会通过，而生产仍然不轮转）。
pub fn rotate_at_boundary(
    journal: &Journal,
    cap_records: usize,
    cap_bytes: u64,
) -> anyhow::Result<Option<crate::journal::Rotation>> {
    if rotation_due(
        journal.live_records(),
        journal.file_bytes(),
        cap_records,
        cap_bytes,
    ) {
        journal.rotate(journal.live_records() / 2)
    } else {
        Ok(None)
    }
}

/// 一个 stage 的派发结果：收敛，或者「这一关没过，但缺陷不在验证者手里」。
enum StageOutcome {
    Pass,
    /// 验证类 stage FAIL：产物要由写者返工，本 stage 等返工完再重跑。
    Rework,
}

/// 「这一关的 FAIL 该派给谁」——路由规则，不是判定强度。
///
/// Dev 的 FAIL 就是「你还没做成」，回派给同一个写者（原地 iter+1）。Review / Test /
/// Build 的 FAIL 说的是「被验证的产物不行」，而验证者没有写范围（`qa.toml` 的
/// `write_roots = []`），再给它一颗铃只会得到同一份 FAIL——本轮的盘上证据就是 QA
/// 连着三次说「RD 未做任何返工」（design §9.32④）。
///
/// 「铃丢了」不走这条：那是投递问题，原地重派同一个 peer 才对。
fn rework_target(stage: Stage) -> Option<Stage> {
    match stage {
        Stage::Dev => None,
        _ => Some(Stage::Dev),
    }
}

/// Build the rework note from typed issues. The writer receives the last Dev
/// test conclusion as state (`null` means nothing was counted), not as a guess.
fn rework_payload(
    journal: &Journal,
    task_id: &str,
    stage: Stage,
    iter: u64,
    issues: &[JudgmentIssue],
    verdict_artifact: &Path,
) -> serde_json::Value {
    let last_test = journal
        .projection()
        .state
        .tests
        .iter()
        .find(|test| test.task == task_id && test.stage == Stage::Dev)
        .map(|test| {
            serde_json::json!({
                "green": test.green(),
                "runs": test.runs.iter().map(describe_run).collect::<Vec<String>>(),
            })
        });
    serde_json::json!({
        "from": format!("{} iter{iter}", format!("{stage:?}").to_lowercase()),
        "issues": issues,
        "verdict_artifact": verdict_artifact.display().to_string(),
        "last_test": last_test,
    })
}

/// Execute the operator-declared build once, with the same command/exit log
/// shape as `bash.output_path`. This is trusted evidence: no verifier model is
/// involved in producing it.
struct BuildPreflight {
    evidence: Evidence,
    issues: Vec<String>,
}

async fn run_build_preflight(
    command: &str,
    worktree: Option<&Path>,
    artifacts: &Path,
) -> anyhow::Result<BuildPreflight> {
    let evidence_dir = artifacts.join("evidence");
    tokio::fs::create_dir_all(&evidence_dir)
        .await
        .with_context(|| format!("create evidence dir {}", evidence_dir.display()))?;
    let cwd = worktree.unwrap_or_else(|| Path::new("."));
    let output = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new("/bin/bash")
            .arg("-lc")
            .arg(command)
            .current_dir(cwd)
            .env("PYTHONPYCACHEPREFIX", evidence_dir.join("pycache"))
            .output(),
    )
    .await
    .with_context(|| format!("build preflight timed out after 120s: {command}"))??;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let exit = crate::sandbox::denial::ExitStatus {
        code: output.status.code(),
        signal: std::os::unix::process::ExitStatusExt::signal(&output.status),
    };
    let path = evidence_dir.join("build.log");
    let log = format!(
        "command: {command}\nexit: {exit:?}\n--- stdout ---\n{stdout}--- stderr ---\n{stderr}"
    );
    tokio::fs::write(&path, log)
        .await
        .with_context(|| format!("write build preflight log {}", path.display()))?;
    let evidence = Evidence {
        path: path.display().to_string(),
        kind: EvidenceKind::BuildLog,
    };
    let report = check_evidence(std::slice::from_ref(&evidence));
    Ok(BuildPreflight {
        evidence,
        issues: report.issues,
    })
}

/// dispatch → wait bell → reconcile（读盘为准）→ 推进/回派，直至 release。
pub async fn run_demo_sa(args: DemoSaArgs) -> anyhow::Result<()> {
    let journal = Journal::create(&args.journal)?;
    // 审批台账从 journal 播种：SA 重启后仍然认得「这个 req 我已经答过了」，
    // 于是 worker 的重发只会拿到同一份批复，而不是第二次判定 + 第二对审计记录。
    let mut approvals = ApprovalLedger::seed(&journal);
    if approvals.decided_len() > 0 {
        println!(
            "[sa] approval ledger: {} decision(s) re-read from the journal",
            approvals.decided_len()
        );
    }
    let sa = BusClient::connect(&args.bus_socket, "harness-sa", None).await?;
    // 判过却没拿到回执的批复：各补投一次。这里不循环补投——闭环靠 worker 的回执，
    // 这一步只保证「SA 重启不等于失忆」。补投落到 worker 那边要么唤醒还在等的工具，
    // 要么换回一张 `applied: false` 的回执，两种都会留下记录。
    for (peer, req, decision) in approvals.unacked() {
        println!("[sa] re-sending unreceipted approval {decision} for {req} to {peer}");
        sa.notify(
            &peer,
            serde_json::json!({
                "type": "approval_decision",
                "req": req,
                "decision": decision,
                "by": "harness-sa",
            }),
        );
    }
    let authority = match &args.approvals {
        Some(path) => Some(
            PermissionEngine::from_json_file(path)
                .with_context(|| format!("load approval authority {}", path.display()))?,
        ),
        None => None,
    };
    let kill_after = std::env::var("KESTREL_TEST_KILL_AFTER").unwrap_or_default();
    let stages = [Stage::Dev, Stage::Review, Stage::Test, Stage::Build];
    println!(
        "[sa] journal policy: live file bounded by {} record(s) / {} byte(s){}",
        args.rotate_after,
        args.rotate_max_bytes,
        match (args.rotate_after, args.rotate_max_bytes) {
            (0, 0) => " — 两个上限都关：单文件、永不截断（审计与对账读整个文件）",
            (0, _) => " — 条数上限关，只按字节封顶",
            (_, 0) => " — 字节上限关，只按条数封顶",
            _ => " — 触顶就在 stage 结算边界折进检查点并归档（截断不是删除）",
        }
    );
    // 价格表：role 目录说「谁跑在哪个 model 上」，registry 说「那个 model 多少钱」。
    // 两者任一缺席 => 退回名义记账，并在开工时就把这事说出来——不让人误以为
    // `--max-cost` 是账单上限。
    let prices = load_prices(args.prices.as_deref());
    let ledger = PriceLedger::discover(args.roles_dir.as_deref(), prices.as_ref());
    // 钱有一个币种，而且这个币种有来源（价格表声明 > `--currency`）。同一次运行里
    // 混两种币种直接失败：账本是加总的，「0.1 美元 + 0.5 元」不是任何数（§9.44）。
    let unit = ledger.unit(&args.currency)?;
    // 证据要求表：坏表硬失败（读不出来就当「没有要求」是 fail-open，验收标准会被
    // 静默放宽）。没给 = 只核存在性。
    let requirements = match &args.evidence_spec {
        Some(path) => {
            let spec = EvidenceSpec::from_json_file(path)
                .with_context(|| format!("load evidence spec {}", path.display()))?;
            println!("[sa] evidence requirements: {}", spec.describe());
            spec
        }
        None => {
            println!("[sa] evidence requirements: none (--evidence-spec not given)");
            EvidenceSpec::default()
        }
    };
    let unpriced: Vec<&str> = [args.rd_peer.as_str(), args.qa_peer.as_str()]
        .into_iter()
        .filter(|p| !ledger.is_priced(p))
        .collect();
    if !unpriced.is_empty() {
        println!(
            "[sa] budget stays NOMINAL for {} (known peers: {}) — each dispatch books {} {unit} \
             instead of a bill; declare [models.cost] in providers.toml and pass --roles-dir \
             to settle by token",
            unpriced.join(","),
            if ledger.known_peers().is_empty() {
                "none".to_string()
            } else {
                ledger.known_peers().join(",")
            },
            args.per_dispatch_cost
        );
    }

    // 走「索引 + 每关自己的 iter」而不是 `for stage`：验证没过时要把活儿退回写者、
    // 返工完再重新验证（§9.32④）。回跳之后各关的重试次数必须接着上次算——清零等于
    // 没有上限，一条永不满足的验收标准就能把 pipeline 转到预算停损为止。
    let mut iters = [1u64; 4];
    let writer_idx = stages
        .iter()
        .position(|s| *s == Stage::Dev)
        .unwrap_or_default();
    let mut rework: Option<serde_json::Value> = None;
    let mut reworks_left = args.max_reworks;
    let mut idx = 0usize;
    while idx < stages.len() {
        let stage = stages[idx];
        let target = args.peer_for(stage);
        let stage_name = format!("{stage:?}").to_lowercase();
        let mut iter = iters[idx];
        // 返工说明随这一关的每颗铃带出去（原地重派也带着，别让它中途丢掉）。
        let rework_note = rework.clone();
        // A build is a mechanical fact. Run the operator-declared command once
        // when entering Build and pass its kernel-written log to the verdict.
        // A failed build is an artifact issue and follows the normal Dev rework
        // path; it never asks the verifier model to invent a substitute.
        let mut trusted_evidence = Vec::new();
        if stage == Stage::Build
            && let Some(command) = args
                .build_command
                .as_deref()
                .filter(|command| !command.trim().is_empty())
        {
            let preflight =
                run_build_preflight(command, args.worktree.as_deref(), &args.artifacts).await?;
            if preflight.issues.is_empty() {
                println!("[sa] build preflight: {}", preflight.evidence.path);
                trusted_evidence.push(preflight.evidence);
            } else {
                let issues: Vec<JudgmentIssue> = preflight
                    .issues
                    .iter()
                    .cloned()
                    .map(JudgmentIssue::artifact)
                    .collect();
                let verdict = Verdict::new(false, issues.clone(), vec![], vec![]);
                journal.append(&JournalRecord::StageWritten {
                    task: args.task_id.clone(),
                    stage,
                    iteration: iter,
                })?;
                journal.append(&JournalRecord::ResultCommitted {
                    task: args.task_id.clone(),
                    stage,
                    verdict,
                })?;
                println!(
                    "[sa] build iter{iter}: deterministic preflight failed ({})",
                    issues
                        .iter()
                        .map(|issue| issue.message.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if reworks_left == 0 {
                    anyhow::bail!(
                        "[sa] build iter{iter}: deterministic build failed ({}) and the \
                             rework budget is spent",
                        issues
                            .iter()
                            .map(|issue| issue.message.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
                reworks_left -= 1;
                rework = Some(rework_payload(
                    &journal,
                    &args.task_id,
                    stage,
                    iter,
                    &issues,
                    Path::new(&preflight.evidence.path),
                ));
                iters[writer_idx] += 1;
                idx = writer_idx;
                println!(
                    "[sa] build iter{iter}: -> rework by {} ({} credit(s) left)",
                    args.peer_for(Stage::Dev),
                    reworks_left
                );
                continue;
            }
        }

        // 值循环：Pass = stage 收敛；Fail/重试走 continue（iter 已自增）。
        let outcome: StageOutcome = loop {
            // --- 预算停损：账本先看，超限 STOP escalate（退出码非 0）。
            // O(1) 投影，不是每次派发都重放整个活文件（不变式 5：两者由同一次折叠产
            // 生，`projection_equals_a_full_replay_after_every_append` 钉住这条）
            // 停损看的是**下一次派发的预留**：有费率时是那台模型的最坏情况账单
            // （整窗输入 + 满输出），没有费率时才是名义值。只看「事后账单」的停损
            // 拦不住正在跑的派发，崩溃重来还能反复把上限烧穿。
            let state = journal.projection().state;
            let total: f64 = state.spent.values().sum();
            let nominal: f64 = state.nominal.values().sum();
            let (reserve, priced) = ledger.reservation(target, args.per_dispatch_cost);
            if total + reserve > args.max_cost {
                anyhow::bail!(
                    "[sa] budget stop-loss: spent {total:.4} (nominal {nominal:.4}) + next \
                     reservation {reserve:.4}{} > max {:.4} {unit}",
                    if priced {
                        " = priced upper bound"
                    } else {
                        " (nominal)"
                    },
                    args.max_cost
                );
            }
            if iter > args.max_iterations {
                // 上限要说清是**怎么**撞上的：写者的 iter 里混着验证 FAIL 换来的返工
                // 派发（2026-11-04 强制 `review:diff` 那一轮实测：dev iter 从 1 涨到 4，
                // 而「返工」那两次不是 RD 自己没做成）。只说 cap reached 会把人引向
                // 「模型不稳」，真正该改的是那条验收标准。
                let reworks_used = args.max_reworks.saturating_sub(reworks_left);
                anyhow::bail!(
                    "[sa] iteration cap reached for {stage:?}{}",
                    if stage == Stage::Dev && reworks_used > 0 {
                        format!(
                            " — {reworks_used} of those iterations were rework dispatches asked \
                             for by a verifier that cannot fix the artifact itself"
                        )
                    } else {
                        String::new()
                    }
                );
            }

            // --- 先写盘，再拉铃（规则 1：顺序即纪律）。
            journal.append(&JournalRecord::StageWritten {
                task: args.task_id.clone(),
                stage,
                iteration: iter,
            })?;
            let bell = ThinBell {
                role: target.strip_prefix("harness-").unwrap_or(target).into(),
                stage,
                check: Some(stage_name.clone()),
                iteration: iter,
                r#ref: {
                    let mut bell_ref = serde_json::json!({
                        "task": args.task,
                        "artifacts": args.artifacts,
                        "evidence": args.artifacts.join("evidence"),
                        "worktree": args.worktree,
                    });
                    // 「上一关为什么没过」进铃的 ref：写者拿到的是可核对的失败原因
                    // （连同一个判定工件路径），不是「再试一次」这种措辞。
                    if let Some(note) = &rework_note {
                        bell_ref["rework"] = note.clone();
                    }
                    bell_ref
                },
            };
            journal.append(&JournalRecord::BellSent {
                task: args.task_id.clone(),
                to: target.into(),
                bell: bell.clone(),
            })?;
            // 预留先落盘，带着 dispatch 坐标（task/stage/iteration）——结算靠它配对。
            journal.append(&JournalRecord::reserved(
                args.task_id.clone(),
                target,
                stage,
                iter,
                reserve,
                priced,
            ))?;
            if kill_after == "bell_sent" {
                eprintln!("[demo-sa] crash injection: after BellSent");
                std::process::abort();
            }
            sa.notify(
                target,
                serde_json::json!({
                    "role": bell.role, "stage": stage_name,
                    "iteration": iter, "task": args.task, "ref": bell.r#ref,
                    "task_id": args.task_id,
                }),
            );

            // --- 等回铃；超时/丢失则巡检（journal 对账 + 读盘工件）。worker 是
            // 常驻引擎，一个回合可能跑满 8 轮工具调用，超时给足（可 --bell-timeout-secs 调）。
            // 等待期间可能插进 worker 的审批反向请求（approve = "bus"）：答完接着等，
            // 但截止时间不因答铃顺延——审批不能把 stage 的等待拖成无限。
            let deadline = tokio::time::Instant::now() + args.bell_timeout;
            let result = loop {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    break None;
                }
                match tokio::time::timeout(deadline - now, sa.recv_notify()).await {
                    // 回执必须先认：它是 worker 发来的普通帧，若落到下面那一条，
                    // 就会被当成「这一 stage 的结果铃」，于是 stage 拿着一条回执结了账
                    Ok(Ok(env)) if env.payload["type"].as_str() == Some("approval_receipt") => {
                        record_receipt(&journal, &env, &mut approvals)?;
                    }
                    Ok(Ok(env)) if env.payload["type"].as_str() == Some("approval_request") => {
                        answer_approval(
                            &journal,
                            &sa,
                            authority.as_ref(),
                            &env,
                            &args.task_id,
                            &mut approvals,
                        )?;
                    }
                    Ok(Ok(env)) => break Some(env),
                    Ok(Err(e)) => {
                        println!("[sa] bell read failed: {e} -> inspecting disk");
                        break None;
                    }
                    Err(_) => break None,
                }
            };
            // 命名走 cli::task 里唯一的一份契约：worker 写哪个文件与 SA 找哪个文件
            // 必须由同一个函数算出（§9.11④ 的教训正是两端各拼一遍 format!）。
            let artifact = artifact_path(&args.artifacts, target, &args.task_id, &stage_name, iter);

            match result {
                Some(env) if env.payload["result_ref"].is_string() => {
                    journal.append(&JournalRecord::BellReceived {
                        task: args.task_id.clone(),
                        from: env.sender.clone(),
                        bell: bell.clone(),
                    })?;
                    if let Some(usage) = env
                        .payload
                        .get("usage")
                        .and_then(|u| serde_json::from_value::<Usage>(u.clone()).ok())
                        && usage.total() > 0
                    {
                        journal.append(&JournalRecord::UsageReported {
                            peer: env.sender.clone(),
                            usage,
                        })?;
                        // worker 报的是读数（token），钱由账本按 registry 的费率算。
                        // 算得出来就把上面那笔预留换成实际账单；算不出来就让预留
                        // 原样留着——宁可按名义多计，也不把「没人填价」记成免费。
                        // 配对用的是这次派发的 peer（预留记在它名下）。
                        // 有价不代表能算账：读数是 0 输入的，结算照做（预留得配对掉，
                        // 否则同一笔派发会被后来者再计一次），但**按名义落账**并说出理由。
                        // 金额取本次的预留：宁可按最坏情况多计，也不按一个不可信的读数少计。
                        // 用二元组匹配而不是 guard：`Some(x) if !flag` / `Some(_) if flag`
                        // 让穷尽性检查看不见那个布尔，编译器会要求一条多余的兜底臂。
                        let untrustworthy = under_reports_input(&usage);
                        match (ledger.settle(target, &usage), untrustworthy) {
                            (Some(cost), false) => {
                                journal.append(&JournalRecord::settled(
                                    args.task_id.clone(),
                                    target,
                                    stage,
                                    iter,
                                    usage,
                                    cost,
                                    true,
                                ))?;
                                println!(
                                    "[sa] {stage_name} iter{iter}: settled {cost:.4} {unit} for \
                                     {} in / {} out tokens",
                                    usage.input_tokens, usage.output_tokens
                                );
                            }
                            (Some(_), true) => {
                                journal.append(&JournalRecord::settled_as_nominal(
                                    args.task_id.clone(),
                                    target,
                                    stage,
                                    iter,
                                    usage,
                                    reserve,
                                    crate::journal::records::NominalReason::UnderReported,
                                ))?;
                                println!(
                                    "[sa] {stage_name} iter{iter}: {target} reported 0 input \
                                     tokens with {} out — the gateway under-reports (§9.7), so \
                                     this books the reservation {reserve:.4} {unit} as NOMINAL \
                                     instead of a bill that would read too cheap",
                                    usage.output_tokens
                                );
                            }
                            (None, _) => println!(
                                "[sa] {stage_name} iter{iter}: {target} has no rate — the \
                                 reservation stays as the bill (nominal)"
                            ),
                        }
                    }
                    if kill_after == "result_received" {
                        eprintln!("[demo-sa] crash injection: after result received");
                        std::process::abort();
                    }
                }
                _ => {
                    // 铃丢了：巡检 = journal 对账 + 读盘。工件在盘上即视为完成
                    // （worker 写完工件、死于拉铃前——不重复派发）。
                    let still_in_flight = journal
                        .projection()
                        .in_flight
                        .iter()
                        .any(|b| b.bell.stage == stage && b.bell.iteration == iter);
                    if !(still_in_flight && artifact.exists()) {
                        println!("[sa] {stage_name} iter{iter}: no result -> re-dispatch");
                        iter += 1;
                        continue;
                    }
                    println!(
                        "[sa] bell lost but artifact on disk: {artifact:?} -> judging from disk"
                    );
                }
            }

            // 判定只读盘，不读铃（thin messages：铃正文只是指针）。工件存在
            // 不等于 stage 收敛——空结果、abort、worker 侧 stop=error 都是 fail，
            // 而「有文件」也不等于「有这一关要求的证据」。
            let verdict = verdict_from_artifact_with_evidence(
                &artifact,
                stage,
                requirements.required_for(stage),
                &trusted_evidence,
            );
            // 把「这一次测试到底过了几条、failed 几条、退了几」打到 stdout：判定行只
            // 说 pass/fail，读日志的人要能看见结论是从哪份文件读出来的（§9.34）。
            for run in &verdict.test_runs {
                println!(
                    "[sa] {stage_name} iter{iter}: test log {} -> {}",
                    run.path,
                    describe_run(run)
                );
            }
            // 认账说出来：它不改判定结果，但读日志的人要看得见「声明的那句」与「核对的那个
            // 文件」不是同一个字符串。
            for note in &verdict.notes {
                println!("[sa] {stage_name} iter{iter}: note: {note}");
            }
            journal.append(&JournalRecord::ResultCommitted {
                task: args.task_id.clone(),
                stage,
                verdict: verdict.clone(),
            })?;
            // 一次 stage 结算是一个天然的检查点边界：此后的前缀不再影响对账，
            // 折叠它才能让长时间运行的 pipeline 的 replay 保持有界。触发由
            // `rotation_due` 判定：条数或字节，两个输入都是 O(1) 读，不重放文件。
            if let Some(rotated) =
                rotate_at_boundary(&journal, args.rotate_after, args.rotate_max_bytes)?
            {
                println!(
                    "[sa] journal rotated: {} record(s) archived into {}, live file now {} record(s)",
                    rotated.covered,
                    rotated.archived_to.display(),
                    journal.len()
                );
            }
            if verdict.pass {
                println!("[sa] {stage_name} iter{iter}: pass");
                break StageOutcome::Pass;
            }
            // FAIL 之后「派给谁」是路由问题，不是判定强度：验证类 stage 说不达标，
            // 说的是**被验证的产物**，而验证者没有写范围，再给它一颗铃只会得到同一份
            // FAIL。返工要有额度（`max_reworks`），额度用尽就把它说成显式失败。
            // 归因是判定那一刻就写进 `verdict.attribution` 的字段（§9.41），路由只是读它——
            // 不在这里重新解释 issue 的措辞，否则改一句文案就会悄悄改掉路由。
            if rework_target(stage) == Some(Stage::Dev)
                && verdict.attribution == Attribution::Artifact
            {
                if reworks_left == 0 {
                    anyhow::bail!(
                        "[sa] {stage_name} iter{iter}: fail ({}) and the rework budget is spent \
                         — re-dispatching the verifier would only repeat this verdict",
                        verdict.issues.join(", ")
                    );
                }
                reworks_left -= 1;
                // 返工铃除了「上一关为什么没过」，还带上写者那一关**账上**的测试结论
                // （§9.35：结论成为状态之后，这里就是它的读者之一）。省掉一轮「我到底
                // 哪儿没过」的试探；`null` 说的是「你上关什么都没数出来」，不是「没问题」。
                rework = Some(rework_payload(
                    &journal,
                    &args.task_id,
                    stage,
                    iter,
                    &verdict.issues,
                    &artifact,
                ));
                println!(
                    "[sa] {stage_name} iter{iter}: fail ({}) -> rework by {} ({} credit(s) left)",
                    verdict.issues.join(", "),
                    args.peer_for(Stage::Dev),
                    reworks_left
                );
                iter += 1;
                break StageOutcome::Rework;
            }
            // 走到这里有两种：Dev 自己的 FAIL（「你还没做成」），或验证者那一回合坏了
            // （空结果 / abort / stop=error）。后者的产物**还没被可信地看过**，重派验证者、
            // 不扣返工额度——把 QA 的哑火算到 RD 头上，只会换来两轮辩护。
            let why = match verdict.attribution {
                Attribution::Turn => "this turn broke, so the artifact was never credibly seen",
                Attribution::Artifact => "the writer did not finish the job",
            };
            println!(
                "[sa] {stage_name} iter{iter}: fail ({}) -> re-dispatch {target} ({why})",
                verdict.issues.join(", ")
            );
            iter += 1;
            continue;
        };
        iters[idx] = iter;
        match outcome {
            StageOutcome::Pass => {
                if stage == Stage::Dev {
                    // 写者交活了，那份返工说明就此作废：带着它进下一关，只会让 QA
                    // 去追一个已经过去的失败。
                    rework = None;
                }
                idx += 1;
            }
            StageOutcome::Rework => {
                // 返工是写者的**下一次**派发，不是把已通过的那一轮重跑：工件名按
                // iter 走，复用同一个 iter 会覆盖掉盘上那一份已经通过的产物。
                iters[writer_idx] += 1;
                idx = writer_idx;
            }
        }
    }
    report_ledger(&journal, &ledger, &unit);
    println!("[sa] pipeline complete");
    Ok(())
}

/// 把金额渲染成四位小数，并且**零就写零**。
///
/// 起因是 2026-11-05 第七轮联跑（全端点都有价、名义值本该归零）报出的一行：
/// `ledger total: 0.1806 cny, of which -0.0000 nominal`。那个负号不是算错的账：
/// `f64` 的 `Iterator::sum` 单位元是 `-0.0`（有符号零要守 IEEE 的符号规则），所以
/// **对空集合求和得到的是 -0.0**，`format!("{:.4}", -0.0)` 就印成 `-0.0000`。
/// 「一分名义钱都没花」被渲染成「倒欠了钱」——报账那一行的全部意义就是让人读得懂，
/// 这种读感不能留给标准库决定。
///
/// 钳位只吞掉显示精度以下的量（4 位小数 => 5e-5），**真负数照原样印**：那说明折叠
/// 出了 bug，藏起来比露出来危险。
fn money(amount: f64) -> String {
    if amount.abs() < 0.00005 {
        "0.0000".to_string()
    } else {
        format!("{amount:.4}")
    }
}

/// 收尾报账：花了多少、其中多少仍是名义值、换出了多少 token。`--max-cost` 到底是
/// 「账单上限」还是「名义上限」，由这行说清楚，不靠人记住这次有没有传 `--roles-dir`。
/// 这份读数能不能按 token 算成账单。
///
/// 网关在 `message_start` 不回 input usage（§9.7 对 BigModel 的实测），于是
/// `input_tokens = 0` 而 output 照报。这时候「按价格表结算」会算出一张**少掉整个输入侧**
/// 的账单——它比名义值更危险，因为它看起来是算出来的。§9.7 的字符估算只该用来兜
/// 「要不要压上下文」那个判断，拿来当钱是另一回事：钱的来源必须是读数。
/// 所以这里不下「这次免费」的结论，只说「这份读数顶不起一张账单」。
fn under_reports_input(usage: &Usage) -> bool {
    usage.input_tokens == 0 && usage.output_tokens > 0
}

/// `unit` 是这一次运行的钱的单位（`PriceLedger::unit`），报账当场把它印出来：
/// 一个不带单位的金额数字，在两种币种面前就是误导（§9.44）。
fn report_ledger(journal: &Journal, ledger: &PriceLedger, unit: &str) {
    let state = journal.projection().state;
    // 结论也是账的一部分（§9.35）：报账时说出「每一关最后一次提交读出来的测试结论」。
    // 读的是投影，不是重新去翻那几份工件——它和预算停损看的必须是同一份状态。
    if state.tests.is_empty() {
        println!("[sa] test conclusions: none (no stage handed in a counted test log)");
    } else {
        for t in &state.tests {
            let verdict = match t.green() {
                Some(true) => "green",
                Some(false) => "NOT green",
                None => "shape only, nothing counted",
            };
            let counts: Vec<String> = t.runs.iter().map(describe_run).collect();
            println!(
                "[sa] test conclusion [{}] {:?}: {verdict} — {}",
                t.task,
                t.stage,
                counts.join("; ")
            );
        }
    }
    if state.spent.is_empty() {
        println!("[sa] ledger: nothing booked");
        return;
    }
    let nominal_total: f64 = state.nominal.values().sum();
    let total: f64 = state.spent.values().sum();
    // 「说不清的钱」要分得出为什么说不清：该补价，还是该修读数（§9.37）。
    let reasons: Vec<String> = state
        .nominal_by_reason
        .iter()
        .map(|(reason, amount)| format!("{} {}", reason.label(), money(*amount)))
        .collect();
    for (peer, cost) in &state.spent {
        let tokens = state.tokens.get(peer).copied().unwrap_or_default();
        let peer_reasons: Vec<String> = state
            .nominal_by_peer_reason
            .get(peer)
            .into_iter()
            .flatten()
            .map(|(reason, amount)| format!("{} {}", reason.label(), money(*amount)))
            .collect();
        let nominal_note = if peer_reasons.is_empty() {
            if ledger.is_priced(peer) {
                String::new()
            } else {
                " (nominal backend: no rate)".to_string()
            }
        } else {
            format!(" (nominal: {})", peer_reasons.join(", "))
        };
        let unsettled = state
            .reserved
            .iter()
            .filter(|((_, p, _, _), _)| p == peer)
            .count();
        println!(
            "[sa] ledger {peer}: {} {unit}{}{} ; tokens in {} / out {}{}",
            money(*cost),
            nominal_note,
            if unsettled > 0 {
                format!("; {unsettled} reservation(s) unsettled")
            } else {
                String::new()
            },
            tokens.input_tokens,
            tokens.output_tokens,
            if tokens.cache_read_tokens > 0 {
                format!(" ; cache read {}", tokens.cache_read_tokens)
            } else {
                String::new()
            }
        );
    }
    println!(
        "[sa] ledger total: {} {unit}, of which {} nominal{}",
        money(total),
        money(nominal_total),
        if reasons.is_empty() {
            String::new()
        } else {
            format!(" ({})", reasons.join(", "))
        }
    );
}

/// 回答一颗审批铃：先落 journal（谁请求、SA 判了什么），再回铃给 worker。
/// 反向请求的授权同样是真相源的一部分——事后只存在于进程内存里的批准，崩溃后
/// 无人能复现「是谁把沙箱放宽的」（design §5 不变式 5）。
fn answer_approval(
    journal: &Journal,
    sa: &BusClient,
    authority: Option<&PermissionEngine>,
    env: &Envelope,
    task: &str,
    ledger: &mut ApprovalLedger,
) -> anyhow::Result<()> {
    let req = env.payload["req"].as_str().unwrap_or_default().to_string();
    let worker = env.payload["session"]
        .as_str()
        .unwrap_or(env.sender.as_str())
        .to_string();
    // 重复投递（worker 的重发，或停车补投送来的一份旧帧）：原样重发上次那个批复。
    // 不重判、不再落一对记录——审计链上「谁放宽了沙箱」必须一个 req 只有一答，
    // 否则重发这个机制自己就把 journal 写脏了。
    if let Some(prior) = ledger.decision_for(&req).map(str::to_string) {
        sa.notify(
            &worker,
            serde_json::json!({
                "type": "approval_decision",
                "req": req,
                "decision": prior,
                "by": "harness-sa",
            }),
        );
        println!(
            "[sa] approval {prior} for {worker}: {req} re-sent (duplicate request; waiting for the receipt)"
        );
        return Ok(());
    }
    // The frame carries the command at the top level; fall back to the detail
    // blob so an older worker is still judged on something real.
    let command = match env.payload["command"].as_str() {
        Some(c) => c.to_string(),
        None => approval_command(&env.payload["detail"]),
    };
    if ledger.note_request(&req, &worker) {
        journal.append(&JournalRecord::ApprovalRequested {
            task: task.into(),
            peer: worker.clone(),
            req: req.clone(),
            command: command.clone(),
        })?;
    }
    let decision = decide_approval(authority, &command);
    journal.append(&JournalRecord::ApprovalDecided {
        req: req.clone(),
        decision: decision_name(decision).to_string(),
        by: "harness-sa".into(),
    })?;
    ledger.record_decision(req.clone(), decision_name(decision).to_string());
    sa.notify(
        &worker,
        serde_json::json!({
            "type": "approval_decision",
            "req": req,
            "decision": decision_name(decision),
            "by": "harness-sa",
        }),
    );
    println!(
        "[sa] approval {} for {worker}: {command}",
        decision_name(decision)
    );
    Ok(())
}

/// 审批的幂等台账：`req` -> 已给出的批复，外加「哪些 req 已经落过请求记录」。
///
/// 投递是 at-least-once（worker 会重发同一颗请求，§10 批复的可靠投递），所以审批者
/// 必须按 `req` 去重：重发不该再判一次，也不该在 journal 里多出第二对记录。
/// 重启后从 journal 播种——这一次读是有界的：活文件的长度由 §9.28 的默认策略管住，
/// 更早的记录在归档段里，`replay_all()` 一并读到。
#[derive(Debug, Default)]
pub struct ApprovalLedger {
    /// req -> 请求它的那条 session（重启后要把未回执的批复送回去，得知道送给谁）
    requests: BTreeMap<String, String>,
    decisions: BTreeMap<String, String>,
    /// 已经拿到回执的 req -> **这张账上最强的那个 `applied`**。回执是唯一能区分
    /// 「批复丢了」与「批了但没人听」的东西，而投递是 at-least-once：同一颗 req
    /// 可能收到两张回执（第一份批复唤醒了工具，重投的那份只换来「没有等待者」）。
    /// 先到先得会把真相留在门外，所以这里按事实强弱合并，只朝一个方向升级。
    acked: BTreeMap<String, bool>,
}

/// 一张回执对台账做了什么，也决定它该不该落 journal。
#[derive(Debug, PartialEq, Eq)]
pub enum AckOutcome {
    /// 这颗 req 的第一张回执。
    Recorded,
    /// 第二张（或更晚）回执说「生效了」，而账上记的是「没生效」——补一条更正：
    /// 工具真的被唤醒过，这是比「没人听」更强、也更容易核实的事实。
    Upgraded,
    /// 第二张回执没带来更强的事实（重复投递，或试图把生效的降级成没生效）。
    /// §9.31 的「没生效的批准不许记成生效」反过来同样成立：**生效过的事不许被
    /// 后来的迟到副本说成没生效**，否则 journal 与 worker 工件会互相打脸。
    Duplicate,
}

impl ApprovalLedger {
    /// 只播种**活文件**：它的长度由 §9.28 的默认上限管住，这一次读因此有界。
    /// 为什么不读归档段（`replay_all()`）：一颗请求最多在它的窗口里被重发
    /// `MAX_APPROVAL_SENDS` 次，回合结束就在途表里关掉了——比活文件还老的请求
    /// 不可能再被重发。真出现「极老 req 的迟到副本」，最坏的后果也只是多一条批复
    /// 记录，而 worker 那边按在途表去重，**工具不会被唤醒第二次**。
    pub fn seed(journal: &Journal) -> Self {
        let mut ledger = Self::default();
        for record in journal.replay() {
            match record {
                JournalRecord::ApprovalRequested { req, peer, .. } => {
                    ledger.requests.insert(req, peer);
                }
                JournalRecord::ApprovalDecided { req, decision, .. } => {
                    ledger.decisions.insert(req, decision);
                }
                JournalRecord::ApprovalAcked { req, applied, .. } => {
                    let _ = ledger.note_ack(&req, applied);
                }
                _ => {}
            }
        }
        ledger
    }

    /// 判过但没有回执的批复：**这是「我不知道它到底生没生效」的那一格**。SA 重启后
    /// 各补投一次（不循环补投——真正的可靠投递靠 worker 的回执闭环，不靠这里猛发）。
    /// 迟到的那份到了 worker 那边要么唤醒还在等的工具，要么换回一张
    /// `applied: false` 的回执，两种都会有记录。
    pub fn unacked(&self) -> Vec<(String, String, String)> {
        self.decisions
            .iter()
            .filter(|(req, _)| !self.acked.contains_key(req.as_str()))
            .filter_map(|(req, decision)| {
                self.requests
                    .get(req)
                    .map(|peer| (peer.clone(), req.clone(), decision.clone()))
            })
            .collect()
    }

    /// 记一张回执，并说出它对这张账做了什么（`Duplicate` = 不该再落第二张）。
    pub fn note_ack(&mut self, req: &str, applied: bool) -> AckOutcome {
        match self.acked.get(req) {
            None => {
                self.acked.insert(req.to_string(), applied);
                AckOutcome::Recorded
            }
            Some(true) => AckOutcome::Duplicate,
            Some(false) => {
                if applied {
                    self.acked.insert(req.to_string(), true);
                    AckOutcome::Upgraded
                } else {
                    AckOutcome::Duplicate
                }
            }
        }
    }

    /// 这颗 req 在账上的 `applied`（没有回执过则 `None`）。
    pub fn applied_of(&self, req: &str) -> Option<bool> {
        self.acked.get(req).copied()
    }

    pub fn acked_len(&self) -> usize {
        self.acked.len()
    }

    /// 已经判过的批复；没有则 `None`（调用方去判）。
    pub fn decision_for(&self, req: &str) -> Option<&str> {
        self.decisions.get(req).map(String::as_str)
    }

    /// 记下「这颗请求已经落过盘了」，并记住是谁请求的。返回 false = 之前就记过，
    /// 别再写第二遍。
    pub fn note_request(&mut self, req: &str, peer: &str) -> bool {
        self.requests
            .insert(req.to_string(), peer.to_string())
            .is_none()
    }

    pub fn record_decision(&mut self, req: String, decision: String) {
        self.decisions.insert(req, decision);
    }

    pub fn decided_len(&self) -> usize {
        self.decisions.len()
    }
}

/// worker 的回执落进 journal。这是唯一能区分「批复丢了」与「批了但没人听」的东西：
/// 没有回执，SA 那本账上这两种情形长得一模一样，而它们的处置完全不同（前者该重发，
/// 后者该记住「这一格已经过期」）。读不懂的回执不记（不猜它说哪颗请求、有没有生效）。
/// 投递是 at-least-once，所以同一颗 req 可能收到两张回执：账本按事实强弱合并
/// （`applied=true` 覆盖 `false`，反向不成立），落盘的是合并后的那一条，而不是
/// 「先到的那一张」——否则 journal 与 worker 工件会对同一颗请求各说一套。
fn record_receipt(
    journal: &Journal,
    env: &Envelope,
    ledger: &mut ApprovalLedger,
) -> anyhow::Result<()> {
    let Some(receipt) = approval_receipt_from_frame(&env.payload) else {
        println!(
            "[sa] ignoring an unreadable approval receipt from {}",
            env.sender
        );
        return Ok(());
    };
    let outcome = ledger.note_ack(&receipt.req, receipt.applied);
    if outcome == AckOutcome::Duplicate {
        // 没有带来更强的事实：不落第二张。但若它与账上那句相矛盾（迟到副本想把
        // 「生效过」说成「没生效」），要把矛盾印出来——静默丢弃等于把「批复被投了
        // 两次」这条线索抹掉。
        let booked = ledger.applied_of(&receipt.req).unwrap_or(receipt.applied);
        println!(
            "[sa] approval receipt for {} already recorded ({})",
            receipt.req,
            if receipt.applied == booked {
                "duplicate"
            } else {
                "later copy says applied=false, ledger keeps the applied=true it already booked"
            }
        );
        return Ok(());
    }
    journal.append(&JournalRecord::ApprovalAcked {
        req: receipt.req.clone(),
        by: env.sender.clone(),
        applied: receipt.applied,
    })?;
    println!(
        "[sa] approval receipt {}: applied={}{}{}",
        receipt.req,
        receipt.applied,
        if receipt.reason.is_empty() {
            String::new()
        } else {
            format!(" ({})", receipt.reason)
        },
        if outcome == AckOutcome::Upgraded {
            " — upgrades the earlier applied=false for the same req"
        } else {
            ""
        }
    );
    Ok(())
}

/// SA 的授权是一张表，不是一种心情：只有显式 Allow 才放行。没有表、没有可判定的
/// 命令、Ask 或 Deny 一律回 DENY——协调者不能放宽角色策略里没写过的东西。
pub fn decide_approval(authority: Option<&PermissionEngine>, command: &str) -> ApprovalDecision {
    let Some(engine) = authority else {
        return ApprovalDecision::Deny;
    };
    if command.trim().is_empty() {
        return ApprovalDecision::Deny;
    }
    match engine.evaluate_command(command) {
        Decision::Allow => ApprovalDecision::AllowOnce,
        Decision::Ask | Decision::Deny => ApprovalDecision::Deny,
    }
}

/// 从 worker 工件推出该 stage 的判定。契约字段由 `serve` 写入
/// （`result` / `aborted` / `stop.*` / `evidence`），`result` 头部必须给出判定词。
/// 三层都要过：通路层（有答案、未中断、worker 侧 stop 不是 error）、语义层
/// （worker 自己声明的结论）、证据层（它点名的文件在盘上成立为**它声称的那种东西**，
/// 并且齐全到 `required` 要求的每一类）——「工件在盘上」从来不等于「stage 通过」。
pub fn verdict_from_artifact(path: &Path, required: &[EvidenceKind]) -> Verdict {
    verdict_from_artifact_for_stage(path, Stage::Dev, required)
}

/// Stage-aware verdict. Evidence that the verifier failed to hand in belongs
/// to that verifier turn: retrying QA is the only way to obtain a new proof.
/// For the writer stage the same omission remains an artifact issue.
pub fn verdict_from_artifact_for_stage(
    path: &Path,
    stage: Stage,
    required: &[EvidenceKind],
) -> Verdict {
    verdict_from_artifact_with_evidence(path, stage, required, &[])
}

/// Stage-aware verdict with evidence produced by a trusted kernel-side
/// executor. On build this lets a deterministic build log satisfy the contract
/// without trusting the verifier model to run the command.
pub fn verdict_from_artifact_with_evidence(
    path: &Path,
    stage: Stage,
    required: &[EvidenceKind],
    trusted_evidence: &[Evidence],
) -> Verdict {
    let evidence_issue = |message: String| {
        if stage == Stage::Dev {
            JudgmentIssue::artifact(message)
        } else {
            JudgmentIssue::turn(message)
        }
    };
    let Ok(text) = std::fs::read_to_string(path) else {
        // 工件没落地 = 这一关的产出本身没出来，不能拿去定罪被审的产物。
        return Verdict::new(
            false,
            vec![JudgmentIssue::turn(format!(
                "artifact missing: {}",
                path.display()
            ))],
            vec![],
            vec![],
        );
    };
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) else {
        return Verdict::new(
            false,
            vec![JudgmentIssue::turn(format!(
                "artifact is not valid JSON: {}",
                path.display()
            ))],
            vec![],
            vec![],
        );
    };
    let mut issues = Vec::new();
    let result = json["result"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_string();
    let mut converged = false;
    if result.is_empty() {
        issues.push(JudgmentIssue::turn("empty result"));
    } else {
        match parse_verdict(&result) {
            WorkerVerdict::Converged => converged = true,
            WorkerVerdict::Failed(line) => {
                issues.push(JudgmentIssue::artifact(format!("verdict: {line}")))
            }
            WorkerVerdict::Missing => issues.push(JudgmentIssue::artifact(
                "no PASS/DONE/FAIL verdict in the head of result",
            )),
        }
    }
    if json["aborted"].as_bool().unwrap_or(false) {
        issues.push(JudgmentIssue::turn("worker aborted"));
    }
    if json["stop"]["type"].as_str() == Some("error") {
        issues.push(JudgmentIssue::turn(format!(
            "worker stop: {}",
            json["stop"]["message"].as_str().unwrap_or("error")
        )));
    }
    // 第三层：先核「类型」再核「齐全」（design §10 从「工件 + 证据文件」再走一格）。
    // 「这里有个文件」只证明有东西；声称 `test_log`/`diff`/`screenshot` 就多了一个
    // 能在盘上被反驳的属性——没有测试汇总行的日志不是测试日志，改名的 txt 不是截图。
    // stage 要求看的还是**核对通过**的类型：一份普通文件顶不出 `test_log` 那一格。
    let mut test_runs: Vec<TestRun> = Vec::new();
    let mut notes: Vec<String> = Vec::new();
    // 写侧（`serve`/池）折进工件的形状警告，跟着判定一起落账：谁在用什么形状写声明，
    // 应该在看判定的人面前显形，而不是只在 worker 那侧的日志里闪一下（§9.39）。
    if let Some(warnings) = json["evidence_warnings"].as_array() {
        notes.extend(
            warnings
                .iter()
                .filter_map(|w| w.as_str())
                .map(|w| format!("worker-side shape warning: {w}")),
        );
    }
    if converged {
        let mut evidence = evidence_from_artifact(&json);
        if evidence.is_empty() {
            // 兼容没有 evidence 字段的旧工件：从正文里提（裸路径按 `file` 档）。
            evidence = evidence_from_answer(&result);
        }
        evidence.extend_from_slice(trusted_evidence);
        if evidence.is_empty() {
            issues.push(evidence_issue("no evidence paths to verify".into()));
        } else {
            let report = check_evidence(&evidence);
            issues.extend(report.issues.into_iter().map(evidence_issue));
            // 认账说明跟着判定落进真相源：重启后复现这次判定，要连「当初认了什么」一起复现。
            // `extend` 而不是赋值——上面已经并进了写侧的形状警告，覆盖就等于把它们丢掉
            // （这个测试第一次跑就抓到了这里）。
            notes.extend(report.notes);
            // 每份 test_log 读出来的结论随判定一起落 journal（§9.34）：事后能问
            // 「那一次到底过了几条」，而不是只有一句 pass。
            test_runs = report.test_runs;
            for kind in missing_requirements(&report.verified_kinds, required) {
                issues.push(evidence_issue(format!(
                    "no verified {} evidence: this stage requires one",
                    kind.as_str()
                )));
            }
        }
    }
    Verdict::new(issues.is_empty(), issues, test_runs, notes)
}

/// What the worker's own answer says about the stage.
enum WorkerVerdict {
    Converged,
    /// 带判定那一句（进 issues，人工读 journal 即可定位）。
    Failed(String),
    /// 通篇没给判定词——不猜，按未收敛处理。
    Missing,
}

/// 在回答头部（前 600 字符）找判定词。真实模型会先写铺垫再给结论，所以只认
/// 「首行首字符」会把明确的 PASS 判成 fail（2026-10-26 联跑实测）。取值顺序
/// 保守：先找 FAIL/BLOCKED/REJECTED——头部任何地方出现否定判定都算未收敛，
/// 宁可回派一次，不把 verify 说成通过。
fn parse_verdict(result: &str) -> WorkerVerdict {
    let head_chars: String = result.chars().take(600).collect();
    let upper = head_chars.to_ascii_uppercase();
    for token in ["FAIL", "BLOCKED", "REJECTED"] {
        if let Some(byte) = upper.find(token) {
            // 判定词所在的那一行就是结论句（to_ascii_uppercase 不改变字节长度，
            // 所以索引可以直接映射回原文，且必然落在字符边界上）。
            let line_start = head_chars[..byte].rfind('\n').map_or(0, |i| i + 1);
            let line_end = byte
                + head_chars[byte..]
                    .find('\n')
                    .unwrap_or(head_chars.len() - byte);
            let line: String = head_chars[line_start..line_end].chars().take(160).collect();
            let line = line.trim().to_string();
            return WorkerVerdict::Failed(if line.is_empty() {
                token.to_string()
            } else {
                line
            });
        }
    }
    for token in ["PASS", "DONE"] {
        if upper.contains(token) {
            return WorkerVerdict::Converged;
        }
    }
    // 中文判定词只认「行首」位置，避免正文里的「不通过」字样被当成立场。
    for line in head_chars.lines().take(8) {
        let trimmed = line.trim_start_matches(['#', '*', '>', '`', '-', ' ', '「', '【']);
        if ["不通过", "未通过", "失败", "阻塞"]
            .iter()
            .any(|t| trimmed.starts_with(t))
        {
            return WorkerVerdict::Failed(trimmed.chars().take(160).collect());
        }
        if trimmed.starts_with("通过") {
            return WorkerVerdict::Converged;
        }
    }
    WorkerVerdict::Missing
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 轮转触发是纯函数：条数或字节任一超限即折，0 表示该条件关闭；
    /// 只有一条记录时不折（检查点比原文还长就是自欺）。
    #[test]
    fn rotation_trigger_accepts_either_ceiling() {
        assert!(!rotation_due(10, 1_000, 0, 0), "both ceilings off");
        assert!(rotation_due(10, 1_000, 10, 0), "record ceiling trips");
        assert!(!rotation_due(9, 1_000, 10, 0));
        assert!(rotation_due(3, 5_000, 0, 5_000), "byte ceiling trips alone");
        assert!(!rotation_due(2, 5_000, 0, 5_001));
        assert!(
            !rotation_due(1, 9_999_999, 1, 1),
            "a one-record journal is never worth a checkpoint"
        );
    }

    /// 默认策略是三态的，而且两个上限各自独立解析：
    /// 没给 = 默认；给 0 = 明确关闭那一个条件；只给一个 = 另一个仍按默认封顶
    /// （「封顶了条数」不等于「字节也要无限」）。
    #[test]
    fn rotation_policy_is_three_state_per_ceiling() {
        assert_eq!(
            resolve_rotation_policy(None, None),
            (DEFAULT_ROTATE_RECORDS, DEFAULT_ROTATE_MAX_BYTES),
            "缺省就是默认策略，不再是永不截断"
        );
        assert_eq!(
            resolve_rotation_policy(Some(0), Some(0)),
            (0, 0),
            "两个 0 = 显式关掉整条策略"
        );
        assert_eq!(
            resolve_rotation_policy(Some(100), None),
            (100, DEFAULT_ROTATE_MAX_BYTES),
            "只封顶条数时字节上限仍跟着默认"
        );
        assert_eq!(
            resolve_rotation_policy(None, Some(0)),
            (DEFAULT_ROTATE_RECORDS, 0),
            "单独关掉字节条件"
        );
        // 默认值的量级护栏在编译期（上面的 const block），这里只验策略行为
        // 默认策略下，一个刚起步的 journal 不该触发
        assert!(!rotation_due(
            7,
            1_024,
            DEFAULT_ROTATE_RECORDS,
            DEFAULT_ROTATE_MAX_BYTES
        ));
        assert!(rotation_due(
            DEFAULT_ROTATE_RECORDS,
            0,
            DEFAULT_ROTATE_RECORDS,
            DEFAULT_ROTATE_MAX_BYTES
        ));
    }

    /// 路由规则本身要能单独测——m4_bus / m5_pool 验的是「铃送没送到」，不是
    /// 「这一关没过该派给谁」（design §9.32④）。
    #[test]
    fn a_verifier_fail_routes_back_to_the_writer() {
        // Dev 的 FAIL 是「你还没做成」：原地重派同一个写者，不该跳到别处。
        assert_eq!(rework_target(Stage::Dev), None, "dev fails in place");
        // 验证类 stage 的 FAIL 说的是「产物不行」：回跳给写者，返工后重跑这一关。
        for stage in [Stage::Review, Stage::Test, Stage::Build] {
            assert_eq!(
                rework_target(stage),
                Some(Stage::Dev),
                "{stage:?} 的产物在写者手里，回派验证者只是重复同一次判定"
            );
        }
    }

    #[test]
    fn the_verdict_carries_its_own_attribution() {
        // 归因写在判定产出的那一刻（§9.41），所以这里测的是**产出端**：从真工件长出来的
        // verdict 必须自带正确的 attribution，路由那端只是读它。改一句 issue 的措辞不该
        // 改掉路由——那正是分类住在路由里时的毛病。

        // 回合坏掉：工件在、result 是空的、worker 侧 stop = error。
        let broken = write_artifact(
            "broken-turn.json",
            r#"{"result":"","aborted":false,"stop":{"type":"error","message":"round budget exhausted"}}"#,
        );
        let v = verdict_from_artifact(&broken, &[]);
        assert!(!v.pass);
        assert_eq!(v.attribution, Attribution::Turn, "{:?}", v.issues);

        // 产物坏掉：说了 DONE，点名的文件不在盘上。
        let gone = evidence_file("attr-lru.py");
        std::fs::remove_file(&gone).unwrap();
        let art = write_artifact(
            "attr-artifact.json",
            &format!(
                r#"{{"result":"DONE: shipped","evidence":[{{"kind":"file","path":"{}"}}],"aborted":false,"stop":{{"type":"stop"}}}}"#,
                gone.display()
            ),
        );
        let v = verdict_from_artifact(&art, &[]);
        assert!(!v.pass);
        assert_eq!(
            v.attribution,
            Attribution::Artifact,
            "缺文件是产物的账，不是回合的账: {:?}",
            v.issues
        );

        // 工件根本没落地：这一关的产出没出来，不能拿去定罪被审的产物。
        let v = verdict_from_artifact(
            std::path::Path::new(&format!(
                "/tmp/kestrel-never-written-artifact-{}.json",
                std::process::id()
            )),
            &[],
        );
        assert_eq!(v.attribution, Attribution::Turn);

        // 新建 issue 必须显式选边；artifact 助手不会被文案悄悄改成 Turn。
        assert_eq!(
            JudgmentIssue::artifact("no verified test_log evidence: this stage requires one")
                .attribution,
            Attribution::Artifact
        );

        // 验证者没交要求的证据，是这一回合没完成取证：重派验证者，不扣写者返工额度。
        let ev = evidence_file("build-claim.txt");
        let art = write_artifact(
            "missing-build-log.json",
            &format!(
                r#"{{"result":"PASS: tested","evidence":[{{"kind":"file","path":"{}"}}],"aborted":false,"stop":{{"type":"stop"}}}}"#,
                ev.display()
            ),
        );
        let v = verdict_from_artifact_for_stage(&art, Stage::Build, &[EvidenceKind::BuildLog]);
        assert!(!v.pass);
        assert_eq!(v.attribution, Attribution::Turn, "{:?}", v.issues);
        assert!(
            v.issues
                .iter()
                .any(|issue| issue.message.contains("no verified build_log evidence")),
            "{:?}",
            v.issues
        );
    }

    #[tokio::test]
    async fn deterministic_build_log_fills_the_build_requirement() {
        let root =
            std::env::temp_dir().join(format!("kestrel-build-preflight-{}", std::process::id()));
        let worktree = root.join("worktree");
        let artifacts = root.join("artifacts");
        std::fs::create_dir_all(&worktree).unwrap();
        let source = worktree.join("lru.py");
        std::fs::write(&source, "class LRU:\n    pass\n").unwrap();
        let command = format!("python3 -m py_compile {}", source.display());
        let preflight = run_build_preflight(&command, Some(&worktree), &artifacts)
            .await
            .expect("deterministic build");
        assert!(preflight.issues.is_empty(), "{:?}", preflight.issues);
        let evidence = preflight.evidence;
        assert_eq!(evidence.kind, EvidenceKind::BuildLog);

        let artifact = write_artifact(
            "trusted-build.json",
            &format!(
                r#"{{"result":"PASS: verified","evidence":[{{"kind":"file","path":"{}"}}],"aborted":false,"stop":{{"type":"stop"}}}}"#,
                source.display()
            ),
        );
        let verdict = verdict_from_artifact_with_evidence(
            &artifact,
            Stage::Build,
            &[EvidenceKind::BuildLog],
            std::slice::from_ref(&evidence),
        );
        assert!(verdict.pass, "{:?}", verdict.issues);

        let bad = run_build_preflight(
            &format!(
                "python3 -m py_compile {}",
                worktree.join("missing.py").display()
            ),
            Some(&worktree),
            &artifacts,
        )
        .await
        .expect("the command ran and produced a failing log");
        assert!(
            bad.issues.iter().any(|issue| issue.contains("status 1")),
            "{:?}",
            bad.issues
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// 有价 ≠ 能算账。BigModel 实测在 `message_start` 不回 input usage（§9.7），
    /// 于是按 token 结算会算出一张少掉整个输入侧的账单。这里只判「这份读数顶不顶得起
    /// 账单」，不判「这次多少钱」——钱的来源必须是读数，不是字符估算。
    #[test]
    fn a_zero_input_reading_is_not_a_bill() {
        let reading = |i, o| Usage {
            input_tokens: i,
            output_tokens: o,
            ..Default::default()
        };
        assert!(
            under_reports_input(&reading(0, 671)),
            "output 照报而 input 为 0"
        );
        assert!(
            !under_reports_input(&reading(442, 640)),
            "报了输入就不是少报"
        );
        // 什么都没报 ≠ 少报：那走的是「没有价」那条路，别在这里重复计一次。
        assert!(
            !under_reports_input(&reading(0, 0)),
            "没有读数不等于读数不可信"
        );
    }

    /// task id 是 journal 记录、门铃、工件名三处**同源**的命名空间；peer 由 stage
    /// 决定并可被第二条 pipeline 改写。§9.11④ 的 bug 正是这三处各拼一遍 format!。
    #[test]
    fn task_namespace_and_peer_wiring() {
        use crate::cli::task::DEFAULT_TASK;
        let args = DemoSaArgs {
            bus_socket: PathBuf::from("/tmp/bus.sock"),
            task: "demo: 实现一个 LRU cache".into(),
            journal: PathBuf::from("/tmp/journal.jsonl"),
            artifacts: PathBuf::from("/tmp/artifacts"),
            per_dispatch_cost: 0.5,
            max_cost: 6.0,
            currency: "usd".into(),
            max_iterations: 3,
            max_reworks: 2,
            bell_timeout: Duration::from_secs(300),
            worktree: None,
            build_command: None,
            approvals: None,
            rotate_after: 0,
            rotate_max_bytes: 0,
            task_id: "demo".into(),
            rd_peer: "harness-rd".into(),
            qa_peer: "harness-qa".into(),
            roles_dir: None,
            prices: None,
            evidence_spec: None,
        };
        assert_eq!(args.peer_for(Stage::Dev), "harness-rd");
        assert_eq!(args.peer_for(Stage::Build), "harness-qa");

        // 第二条 pipeline：自己的 peer 前缀 + 自己的 task id => 自己的工件
        let second = DemoSaArgs {
            task_id: "t2".into(),
            rd_peer: "t2:harness-rd".into(),
            ..args
        };
        let first_name = artifact_path(&PathBuf::from("/a"), "harness-rd", "demo", "dev", 1);
        let second_name = artifact_path(
            &PathBuf::from("/a"),
            second.peer_for(Stage::Dev),
            &second.task_id,
            "dev",
            1,
        );
        assert_ne!(first_name, second_name, "two pipelines never share a file");
        assert_eq!(
            second_name.file_name().unwrap().to_str().unwrap(),
            "t2-harness-rd-t2-dev-iter1.json"
        );
        // a bell that carries no task id lands in the default namespace
        assert_eq!(
            artifact_path(&PathBuf::from("/a"), "harness-rd", DEFAULT_TASK, "dev", 2)
                .file_name()
                .unwrap(),
            "harness-rd-default-dev-iter2.json"
        );
    }

    fn write_artifact(name: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kestrel-verdict-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn a_worker_side_shape_warning_rides_the_verdict() {
        let file = evidence_file("shape-lru.py");
        let art = write_artifact(
            "shape.json",
            &format!(
                r#"{{"result":"DONE: shipped","evidence":[{{"kind":"file","path":"{}"}}],"evidence_warnings":["evidence claim /tmp/wt/a.log（注） was read as path /tmp/wt/a.log"],"aborted":false,"stop":{{"type":"stop"}}}}"#,
                file.display()
            ),
        );
        let v = verdict_from_artifact(&art, &[EvidenceKind::File]);
        assert!(v.pass, "形状警告不是缺陷，不该把一关判死: {:?}", v.issues);
        assert!(
            v.notes
                .iter()
                .any(|n| n.contains("worker-side shape warning")),
            "写侧说了什么，判定这端要能复述: {:?}",
            v.notes
        );
    }

    /// 一句 `DONE` 配一份**跑挂了**的测试日志：日志反驳判定词，stage 不能过。
    /// 这是 §9.34 的全部意义——「过了几条、failed 几条」不再只活在模型的一段话里。
    #[test]
    fn a_done_claim_with_a_failing_test_log_is_rebutted() {
        let log = write_artifact(
            "unittest.log",
            "command: python3 -m unittest test_lru_cache\n\
             exit: ExitStatus { code: Some(1), signal: None }\n\
             --- stdout ---\nRan 26 tests in 0.011s\n\nFAILED (failures=2)\n",
        );
        let src = evidence_file("lru_cache.py");
        let art = write_artifact(
            "rebutted.json",
            &format!(
                r#"{{"result":"DONE: shipped and green","evidence":[{{"kind":"file","path":"{}"}},{{"kind":"test_log","path":"{}"}}],"aborted":false,"stop":{{"type":"stop"}}}}"#,
                src.display(),
                log.display()
            ),
        );
        let v = verdict_from_artifact(&art, &[EvidenceKind::File, EvidenceKind::TestLog]);
        assert!(!v.pass, "跑挂的日志必须能顶回那句 DONE: {:?}", v.issues);
        assert!(
            v.issues.iter().any(|i| i.contains("did not pass")),
            "{:?}",
            v.issues
        );
        assert_eq!(v.test_runs.len(), 1, "{:?}", v.test_runs);
        assert_eq!(v.test_runs[0].exit_code, Some(1));
        assert_eq!(v.test_runs[0].failed, Some(2));

        // 反过来：同一份工件把日志换成真过的，就该过——这一层不是「有 exit 行就 FAIL」。
        let ok = write_artifact(
            "unittest-ok.log",
            "command: python3 -m unittest test_lru_cache\n\
             exit: ExitStatus { code: Some(0), signal: None }\n\
             --- stdout ---\nRan 26 tests in 0.011s\n\nOK\n",
        );
        let art = write_artifact(
            "shipped.json",
            &format!(
                r#"{{"result":"DONE: shipped and green","evidence":[{{"kind":"file","path":"{}"}},{{"kind":"test_log","path":"{}"}}],"aborted":false,"stop":{{"type":"stop"}}}}"#,
                src.display(),
                ok.display()
            ),
        );
        let v = verdict_from_artifact(&art, &[EvidenceKind::File, EvidenceKind::TestLog]);
        assert!(v.pass, "{:?}", v.issues);
        assert_eq!(v.test_runs[0].exit_code, Some(0));
    }
    /// 一份 role 目录 + 一份 providers.toml：账本的两个输入。
    /// `priced` 决定 registry 里那个 model 有没有写 cost。
    fn scratch_prices(tag: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!("kestrel-prices-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("rd.toml"),
            "role = \"rd\"\npeer_id = \"harness-rd\"\n\n[llm]\nprovider = \"p1\"\nmodel = \"m1\"\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("qa.toml"),
            "role = \"qa\"\npeer_id = \"harness-qa\"\n\n[llm]\nprovider = \"p1\"\nmodel = \"bare-model\"\n",
        )
        .unwrap();
        let reg_path = dir.join("providers.toml");
        std::fs::write(
            &reg_path,
            "[providers.p1]\nbase_url = \"https://example.invalid\"\napi = \"anthropic-messages\"\nkey = \"k\"\n\n[[providers.p1.models]]\nid = \"m1\"\ncontext_window = 200000\nmax_out = 8000\n\n[providers.p1.models.cost]\ninput = 0.5\noutput = 2.0\n\n[[providers.p1.models]]\nid = \"bare-model\"\ncontext_window = 32000\nmax_out = 4000\n",
        )
        .unwrap();
        (dir, reg_path)
    }

    /// 账本先问 role（这个 peer 跑在哪个 model 上），再问 registry（那个 model
    /// 多少钱）：worker 报的是 token 读数，价格由记账的人查表。方向反了就成了
    /// 花钱的一方自己开价。
    #[test]
    fn price_ledger_prices_a_peer_from_role_then_registry() {
        let (roles, reg_path) = scratch_prices("ok");
        let reg = Registry::load(&reg_path).unwrap();
        let ledger = PriceLedger::discover(Some(&roles), Some(&reg));
        // 预留 = 最坏情况：整窗输入 + 满输出 => 200000*0.5/1e6 + 8000*2.0/1e6
        let (reserve, priced) = ledger.reservation("harness-rd", 0.5);
        assert!(priced, "写了 cost 的 model 按价结算");
        assert!((reserve - 0.116).abs() < 1e-9, "worst case: {reserve}");
        let bill = ledger
            .settle(
                "harness-rd",
                &Usage {
                    input_tokens: 1_000,
                    output_tokens: 500,
                    ..Usage::default()
                },
            )
            .unwrap();
        assert!((bill - 0.0015).abs() < 1e-9, "{bill}");
    }

    /// 说不清价的三条路——model 没写 cost、registry 文件缺席、role 目录缺席——
    /// 全都退回名义值，**不是 0**。按 0 记账的停损等于没有停损。
    #[test]
    fn an_unpriced_backend_books_nominal_not_zero() {
        let (roles, reg_path) = scratch_prices("unpriced");
        let reg = Registry::load(&reg_path).unwrap();
        let ledger = PriceLedger::discover(Some(&roles), Some(&reg));
        assert!(ledger.is_priced("harness-rd"));
        assert!(!ledger.is_priced("harness-qa"), "没写 cost 的 model 不算价");
        assert_eq!(ledger.reservation("harness-qa", 0.5), (0.5, false));
        assert!(ledger.settle("harness-qa", &Usage::default()).is_none());
        assert_eq!(ledger.reservation("nobody", 0.5), (0.5, false));
        // 没有 registry：谁都不按价结算
        let no_rates = PriceLedger::discover(Some(&roles), None);
        assert_eq!(no_rates.reservation("harness-rd", 0.25), (0.25, false));
        // 没有 role 目录：连 peer->model 都不知道
        let nothing = PriceLedger::discover(None, None);
        assert_eq!(nothing.reservation("harness-rd", 0.25), (0.25, false));
        assert!(nothing.known_peers().is_empty());
    }

    /// 钱的单位有一个**来源**，而不是写在字段名里（design §9.44）：
    /// ① 价格表声明的币种赢过 CLI 的 fallback；② 全无价时才用 fallback；
    /// ③ 同一次运行里混两种币种 => 启动即失败——账本是加总的，混币种的「总额」
    /// 不是任何数，挑一个猜也不是。
    #[test]
    fn the_money_unit_comes_from_the_table_and_never_gues() {
        let dir = std::env::temp_dir().join(format!("kestrel-unit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let roles = dir.join("roles");
        std::fs::create_dir_all(&roles).unwrap();
        std::fs::write(
            roles.join("rd.toml"),
            "role = \"rd\"\npeer_id = \"harness-rd\"\n\n[llm]\nprovider = \"cn\"\nmodel = \"m\"\n",
        )
        .unwrap();
        std::fs::write(
            roles.join("qa.toml"),
            "role = \"qa\"\npeer_id = \"harness-qa\"\n\n[llm]\nprovider = \"us\"\nmodel = \"m\"\n",
        )
        .unwrap();
        let priced = |currency: Option<&str>| {
            let currency = currency
                .map(|c| format!("currency = \"{c}\"\n"))
                .unwrap_or_default();
            format!(
                "[providers.cn]\nbase_url = \"https://cn.invalid\"\napi = \"anthropic-messages\"\nkey = \"k\"\n{currency}\n\
                 [[providers.cn.models]]\nid = \"m\"\n\n[providers.cn.models.cost]\ninput = 0.8\noutput = 2.8\n\n\
                 [providers.us]\nbase_url = \"https://us.invalid\"\napi = \"anthropic-messages\"\nkey = \"k\"\n{currency}\n\
                 [[providers.us.models]]\nid = \"m\"\n\n[providers.us.models.cost]\ninput = 0.3\noutput = 1.2\n"
            )
        };
        let registry = |text: String| {
            let path = dir.join(format!("providers-{}.toml", std::process::id()));
            std::fs::write(&path, text).unwrap();
            Registry::load(&path).unwrap()
        };

        // ① 表说了算：两个 peer 都声明 cny，fallback 写 usd 也没用
        let reg = registry(priced(Some("cny")));
        let ledger = PriceLedger::discover(Some(&roles), Some(&reg));
        assert_eq!(ledger.unit("usd").unwrap(), "cny", "币种跟着价目表走");

        // ② 全无价 => 用 CLI 声明的币种（名义记账也得说得出单位）
        let no_rates = PriceLedger::discover(Some(&roles), None);
        assert_eq!(no_rates.unit("cny").unwrap(), "cny");
        assert_eq!(no_rates.unit("USD").unwrap(), "usd", "大小写归一");

        // ③ 混币种 => 失败，并且失败里要点名是谁们
        let mixed = registry(
            "[providers.cn]\nbase_url = \"https://cn.invalid\"\napi = \"anthropic-messages\"\nkey = \"k\"\ncurrency = \"cny\"\n\n\
             [[providers.cn.models]]\nid = \"m\"\n\n[providers.cn.models.cost]\ninput = 0.8\noutput = 2.8\n\n\
             [providers.us]\nbase_url = \"https://us.invalid\"\napi = \"anthropic-messages\"\nkey = \"k\"\ncurrency = \"usd\"\n\n\
             [[providers.us.models]]\nid = \"m\"\n\n[providers.us.models.cost]\ninput = 0.3\noutput = 1.2\n"
                .to_string(),
        );
        let ledger = PriceLedger::discover(Some(&roles), Some(&mixed));
        let err = ledger.unit("usd").expect_err("一次运行不能同时记两种钱");
        let text = err.to_string();
        assert!(text.contains("cny") && text.contains("usd"), "{text}");
        assert!(
            text.contains("harness-rd") && text.contains("harness-qa"),
            "失败要点名到 peer: {text}"
        );

        // 有价但没声明币种 => 读成 usd（历史默认，不是猜出来的第三种）
        let legacy = registry(priced(None));
        let ledger = PriceLedger::discover(Some(&roles), Some(&legacy));
        assert_eq!(
            ledger.unit("cny").unwrap(),
            "usd",
            "缺省 = usd，与旧行为一致"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 投递是 at-least-once，所以审批者必须按 `req` 去重：重发不该再判一次，也不该
    /// 在 journal 里留下第二对记录——否则「谁放宽了沙箱」这条审计链自己就先被重发
    /// 写脏了。重启之后台账从 journal 播种，同一个 req 仍然只有一份答案。
    #[test]
    fn approval_ledger_dedups_by_req_and_survives_a_restart() {
        let dir = std::env::temp_dir().join(format!("kestrel-apledger-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        {
            let j = Journal::create(&path).unwrap();
            let mut ledger = ApprovalLedger::seed(&j);
            assert!(
                ledger.note_request("r-1", "harness-rd"),
                "第一次见到这颗请求，该落审计记录"
            );
            j.append(&JournalRecord::ApprovalRequested {
                task: "demo".into(),
                peer: "harness-rd".into(),
                req: "r-1".into(),
                command: "cargo test".into(),
            })
            .unwrap();
            j.append(&JournalRecord::ApprovalDecided {
                req: "r-1".into(),
                decision: "allow".into(),
                by: "harness-sa".into(),
            })
            .unwrap();
            ledger.record_decision("r-1".into(), "allow".into());

            // worker 的重发：同一个 req 不再算新请求，答案原样重发
            assert!(
                !ledger.note_request("r-1", "harness-rd"),
                "重发的请求不该再落第二份 approval_requested"
            );
            assert_eq!(ledger.decision_for("r-1"), Some("allow"));
        }

        // 重启：台账从盘上播种，同一个 req 仍然只有那一个答案
        let reopened = Journal::open(&path).unwrap();
        let mut seeded = ApprovalLedger::seed(&reopened);
        assert_eq!(seeded.decision_for("r-1"), Some("allow"), "重启不重判");
        assert_eq!(seeded.decided_len(), 1);
        assert!(
            !seeded.note_request("r-1", "harness-rd"),
            "请求记录也已经在盘上了"
        );

        // 崩溃点：请求落了盘、批复还没落 —— 重启后不能再记一次请求，但必须去判
        let half = dir.join("half.jsonl");
        let j2 = Journal::create(&half).unwrap();
        j2.append(&JournalRecord::ApprovalRequested {
            task: "demo".into(),
            peer: "harness-rd".into(),
            req: "r-2".into(),
            command: "rm -rf /tmp/x".into(),
        })
        .unwrap();
        let mut ledger2 = ApprovalLedger::seed(&j2);
        assert!(!ledger2.note_request("r-2", "harness-rd"), "请求已经记过了");
        assert_eq!(ledger2.decision_for("r-2"), None, "但答案还没有，得去判");

        // 回执闭环：判了但没回执的那一格必须被列出来补投；回执到了就不再补
        let acked_path = dir.join("acked.jsonl");
        let j3 = Journal::create(&acked_path).unwrap();
        j3.append(&JournalRecord::ApprovalRequested {
            task: "demo".into(),
            peer: "harness-rd".into(),
            req: "r-3".into(),
            command: "cargo build".into(),
        })
        .unwrap();
        j3.append(&JournalRecord::ApprovalDecided {
            req: "r-3".into(),
            decision: "deny".into(),
            by: "harness-sa".into(),
        })
        .unwrap();
        let mut ledger3 = ApprovalLedger::seed(&j3);
        assert_eq!(
            ledger3.unacked(),
            vec![(
                "harness-rd".to_string(),
                "r-3".to_string(),
                "deny".to_string()
            )],
            "判过没回执的批复要能被找出来补投"
        );
        assert_eq!(
            ledger3.note_ack("r-3", false),
            AckOutcome::Recorded,
            "第一张回执"
        );
        j3.append(&JournalRecord::ApprovalAcked {
            req: "r-3".into(),
            by: "harness-rd".into(),
            applied: false,
        })
        .unwrap();
        assert_eq!(
            ledger3.note_ack("r-3", false),
            AckOutcome::Duplicate,
            "重复回执不再记第二张"
        );
        assert!(
            ledger3.unacked().is_empty(),
            "回执到过就不该再补投（applied=false 也是回执）"
        );
        // 重启后仍然认得这张回执
        let reseeded = ApprovalLedger::seed(&Journal::open(&acked_path).unwrap());
        assert_eq!(reseeded.acked_len(), 1, "回执从活文件里读回来");
        assert!(reseeded.unacked().is_empty(), "重启后不该再补投一次");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 名义账归零之后那一行要印 `0.0000`，不是 `-0.0000`：`f64` 的 `sum()` 对空集合
    /// 给的是 `-0.0`（第七轮联跑现场：`of which -0.0000 nominal`），而「没花名义钱」
    /// 被读成「倒欠了钱」是报账那一行不能犯的错误。真负数照印——那是折叠的 bug，
    /// 藏起来比露出来危险。
    #[test]
    fn a_zero_ledger_line_does_not_read_as_a_debt() {
        assert_eq!(money(-0.0), "0.0000", "空账的单位元不是负号");
        assert_eq!(money(0.0), "0.0000");
        assert_eq!(money(1e-9), "0.0000", "显示精度以下的量按零处理");
        assert_eq!(money(-1e-9), "0.0000");
        assert_eq!(money(0.1806), "0.1806");
        assert_eq!(money(-0.0001), "-0.0001", "真负数照印，不钳位");
    }

    /// 真端点联跑抓到的一格（2026-11-05）：投递是 at-least-once，所以同一颗 `req`
    /// 会收到两张**互相矛盾**的回执——第一份批复唤醒了还在等的工具（worker 工件记
    /// `applied:true`），SA 按 req 去重时重投的那一份找不到等待者，只换回
    /// `applied:false (no open request)`。先到先得会把 journal 写成 `applied:false`，
    /// 与工件对不上，而哪一张赢只取决于帧到达顺序。台账因此按事实强弱合并：
    /// 「生效过」盖住「没生效」，反向不成立——§9.31 那句「没生效的批准不许记成生效」
    /// 的两面是同一条纪律。
    #[test]
    fn a_contradicting_receipt_upgrades_without_downgrading() {
        let dir = std::env::temp_dir().join(format!("kestrel-apack-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let _ = std::fs::remove_file(&path);
        let j = Journal::create(&path).unwrap();
        let mut ledger = ApprovalLedger::seed(&j);

        // 「没生效」先到：落一条；随后「生效了」到达：升级，并补一条更正记录
        assert_eq!(ledger.note_ack("r-9", false), AckOutcome::Recorded);
        j.append(&JournalRecord::ApprovalAcked {
            req: "r-9".into(),
            by: "harness-rd".into(),
            applied: false,
        })
        .unwrap();
        assert_eq!(
            ledger.note_ack("r-9", true),
            AckOutcome::Upgraded,
            "生效是更强的事实，必须盖过「没人听」"
        );
        j.append(&JournalRecord::ApprovalAcked {
            req: "r-9".into(),
            by: "harness-rd".into(),
            applied: true,
        })
        .unwrap();
        assert_eq!(ledger.applied_of("r-9"), Some(true));
        assert_eq!(ledger.acked_len(), 1, "升级不新增一颗 req");

        // 反方向：记过生效之后，迟到的副本不能把它说成没生效
        assert_eq!(ledger.note_ack("r-8", true), AckOutcome::Recorded);
        assert_eq!(
            ledger.note_ack("r-8", false),
            AckOutcome::Duplicate,
            "不把生效的降级"
        );
        assert_eq!(ledger.applied_of("r-8"), Some(true));
        j.append(&JournalRecord::ApprovalAcked {
            req: "r-8".into(),
            by: "harness-rd".into(),
            applied: true,
        })
        .unwrap();
        drop(j);

        // 重启播种读回来的仍是那颗最强的事实（不是文件里最后一条）
        let reseeded = ApprovalLedger::seed(&Journal::open(&path).unwrap());
        assert_eq!(reseeded.applied_of("r-9"), Some(true), "播种也按强弱合并");
        assert_eq!(reseeded.acked_len(), 2, "两颗 req 各自一条账");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A real file an artifact can point at: a converged verdict now has to be
    /// checkable on disk, so the fixtures need something checkable.
    fn evidence_file(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kestrel-verdict-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, "content\n").unwrap();
        path
    }

    #[test]
    fn artifact_with_answer_is_pass() {
        let ev = evidence_file("lru.py");
        let p = write_artifact(
            "ok.json",
            &format!(
                r#"{{"result":"DONE: LRU implemented","evidence":["{}"],"aborted":false,"stop":{{"type":"stop"}}}}"#,
                ev.display()
            ),
        );
        let v = verdict_from_artifact(&p, &[]);
        assert!(v.pass, "{:?}", v.issues);
    }

    #[test]
    fn empty_result_and_error_stop_are_fail() {
        let p = write_artifact(
            "empty.json",
            r#"{"result":"","aborted":false,"stop":{"type":"error","message":"round budget exhausted"}}"#,
        );
        let v = verdict_from_artifact(&p, &[]);
        assert!(!v.pass);
        assert!(
            v.issues.iter().any(|i| i == "empty result")
                && v.issues.iter().any(|i| i.starts_with("worker stop:")),
            "{:?}",
            v.issues
        );
    }

    /// 「判定 = 工件 + 证据文件」（design §10）：声称收敛却给不出文件、点名的
    /// 文件不在盘上，都不算收敛；正文里的绝对路径也照样能被核。
    #[test]
    fn converged_answer_without_checkable_evidence_is_fail() {
        let bare = write_artifact(
            "bare.json",
            r#"{"result":"PASS: 全部通过","aborted":false,"stop":{"type":"stop"}}"#,
        );
        let v = verdict_from_artifact(&bare, &[]);
        assert!(!v.pass, "a claim with nothing to check is not a pass");
        assert!(
            v.issues.iter().any(|i| i == "no evidence paths to verify"),
            "{:?}",
            v.issues
        );

        let missing = evidence_file("tmp-for-path.txt");
        let gone = missing.with_file_name("definitely-not-there.py");
        std::fs::remove_file(&missing).ok();
        let p = write_artifact(
            "ghost.json",
            &format!(
                r#"{{"result":"DONE: shipped","evidence":["{}"],"aborted":false,"stop":{{"type":"stop"}}}}"#,
                gone.display()
            ),
        );
        let v = verdict_from_artifact(&p, &[]);
        assert!(!v.pass);
        assert!(
            v.issues.iter().any(|i| i.starts_with("evidence missing:")),
            "{:?}",
            v.issues
        );

        // prose paths are read when the artifact has no contract field
        let real = evidence_file("cache.rs");
        let p = write_artifact(
            "prose.json",
            &format!(
                r#"{{"result":"PASS — 审阅了 {}","aborted":false,"stop":{{"type":"stop"}}}}"#,
                real.display()
            ),
        );
        assert!(
            verdict_from_artifact(&p, &[]).pass,
            "fallback to the answer text"
        );
    }

    /// SA 的授权是策略表，不是心情：只有显式 Allow 放行；没有表、命令为空、
    /// Ask/Deny 全部回 deny（fail closed）。
    #[test]
    fn approval_authority_is_a_table_not_a_mood() {
        use crate::core::ops::ApprovalDecision;
        use crate::permissions::PolicyTable;
        // The same JSON shape the shipped authority file uses, so the fixture
        // cannot drift from what `--approvals` actually loads.
        let table: PolicyTable = serde_json::from_value(serde_json::json!({
            "default": "ask",
            "rules": [{ "pattern": ["cargo", ["test", "build"]], "decision": "allow" }],
            "forbidden_prefixes": [["rm"]]
        }))
        .unwrap();
        let engine = PermissionEngine::new(table);
        assert_eq!(
            decide_approval(Some(&engine), "cargo test"),
            ApprovalDecision::AllowOnce
        );
        // Ask (not in the table) and Deny (forbidden) both come back as deny:
        // the coordinator cannot widen what the role policy never named.
        assert_eq!(
            decide_approval(Some(&engine), "python3 -m pytest"),
            ApprovalDecision::Deny
        );
        assert_eq!(
            decide_approval(Some(&engine), "rm -rf /tmp/x"),
            ApprovalDecision::Deny
        );
        assert_eq!(
            decide_approval(Some(&engine), "   "),
            ApprovalDecision::Deny,
            "nothing to judge is not a licence to run"
        );
        assert_eq!(
            decide_approval(None, "cargo test"),
            ApprovalDecision::Deny,
            "an SA without an authority table denies"
        );
    }

    /// The worker's own verdict is what counts: text without PASS/DONE is not a
    /// pass, and an explicit FAIL is a fail even when the transport succeeded.
    #[test]
    fn verdict_marker_decides_the_stage() {
        let fail = write_artifact(
            "fail.json",
            r#"{"result":"FAIL: no LRU cache on disk\n细节","aborted":false,"stop":{"type":"stop"}}"#,
        );
        let v = verdict_from_artifact(&fail, &[]);
        assert!(!v.pass, "{:?}", v.issues);
        assert_eq!(v.issues, vec!["verdict: FAIL: no LRU cache on disk"]);

        let rambling = write_artifact(
            "ramble.json",
            r#"{"result":"收到门铃，我先看了目录","aborted":false,"stop":{"type":"stop"}}"#,
        );
        let v = verdict_from_artifact(&rambling, &[]);
        assert!(!v.pass, "an answer without a verdict is not a pass");

        for head in [
            "PASS: verified",
            "pass",
            "DONE: shipped",
            "BLOCKED: no perms",
        ] {
            // Converged heads need evidence on disk; FAIL/BLOCKED ones are
            // already fails and are not asked to produce any.
            let ev = evidence_file("reviewed.rs");
            let p = write_artifact(
                "head.json",
                &format!(
                    r#"{{"result":"{head}","evidence":["{}"],"aborted":false,"stop":{{"type":"stop"}}}}"#,
                    ev.display()
                ),
            );
            let pass = verdict_from_artifact(&p, &[]).pass;
            assert_eq!(
                pass,
                !head.starts_with("FAIL") && !head.starts_with("BLOCKED"),
                "{head}"
            );
        }
    }

    /// 真实模型会先写铺垫再给结论：判定词允许出现在回答头部任意位置，但否定
    /// 判定优先（2026-10-26 联跑：QA 明确写了 PASS 却因不在首字符被判 fail）。
    #[test]
    fn verdict_marker_may_follow_a_preamble() {
        for text in [
            "**PASS** — reviewed 3 files",
            "DONE: shipped lru.py",
            "铺垫一句。\n\nPASS（与上轮结论一致）",
            "通过（review 维度通过）",
        ] {
            assert!(
                matches!(parse_verdict(text), WorkerVerdict::Converged),
                "should converge: {text}"
            );
        }
        for text in [
            "先说背景\nFAIL: 无测试文件",
            "BLOCKED: 权限拒绝",
            "通过前先看…\n实际 FAIL",
            "不通过：缺测试",
        ] {
            assert!(
                matches!(parse_verdict(text), WorkerVerdict::Failed(_)),
                "should fail: {text}"
            );
        }
        assert!(matches!(
            parse_verdict("我看了目录，一切正常"),
            WorkerVerdict::Missing
        ));
    }

    #[test]
    fn abort_flag_and_missing_artifact_are_fail() {
        let p = write_artifact(
            "abort.json",
            r#"{"result":"partial","aborted":true,"stop":{"type":"aborted"}}"#,
        );
        assert!(
            verdict_from_artifact(&p, &[])
                .issues
                .iter()
                .any(|i| i == "worker aborted")
        );
        let v = verdict_from_artifact(
            std::path::Path::new("/tmp/kestrel-no-such-artifact.json"),
            &[],
        );
        assert!(!v.pass);
        assert!(v.issues[0].starts_with("artifact missing"));
    }

    /// 写一份带 evidence 数组的工件；`evidence_json` 原样插进 `evidence` 字段。
    fn typed_artifact(name: &str, verdict_head: &str, evidence_json: &str) -> PathBuf {
        write_artifact(
            name,
            &format!(
                r#"{{"result":"{verdict_head}","evidence":{evidence_json},"aborted":false,"stop":{{"type":"stop"}}}}"#
            ),
        )
    }

    fn write_named(tag: &str, name: &str, body: &[u8]) -> String {
        let dir = std::env::temp_dir().join(format!("kestrel-evspec-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        path.display().to_string()
    }

    /// 类型化的意义就在这：**「有个文件」不等于「那是它声称的那种东西」**。
    /// worker 把一个普通文本指认成测试日志，判定层要能在盘上反驳它。
    #[test]
    fn a_claimed_kind_is_verified_not_trusted() {
        let log = write_named("claim", "pytest.log", b"==== 3 passed in 0.01s ====\n");
        let p = typed_artifact(
            "claim-ok.json",
            "DONE: tests green",
            &format!(r#"[{{"path":"{log}","kind":"test_log"}}]"#),
        );
        assert!(
            verdict_from_artifact(&p, &[EvidenceKind::TestLog]).pass,
            "真日志该顶起真要求"
        );

        let fake = write_named("claim", "note.txt", b"DONE: I ran the tests, trust me\n");
        let p = typed_artifact(
            "claim-fake.json",
            "DONE: tests green",
            &format!(r#"[{{"path":"{fake}","kind":"test_log"}}]"#),
        );
        let v = verdict_from_artifact(&p, &[]);
        assert!(!v.pass, "{:?}", v.issues);
        assert!(
            v.issues.iter().any(|i| i.contains("not a test log")),
            "{:?}",
            v.issues
        );
    }

    /// 要求看的是**核对通过的**类型：一份普通文件（哪怕存在且有内容）顶不出
    /// `test_log` 那一格——否则「按 stage 要求齐全」就退化成了措辞比赛。
    #[test]
    fn a_plain_file_cannot_fill_a_typed_requirement() {
        let plain = write_named("req", "cache.rs", b"class LRUCache: pass\n");
        let p = typed_artifact(
            "plain-only.json",
            "DONE: implemented",
            &format!(r#"["{plain}"]"#),
        );
        let v = verdict_from_artifact(&p, &[EvidenceKind::TestLog]);
        assert!(!v.pass, "{:?}", v.issues);
        assert!(
            v.issues
                .iter()
                .any(|i| i.contains("no verified test_log evidence")),
            "{:?}",
            v.issues
        );
        // 没有要求时，同一份工件仍然通过（类型化之前的语义不被打断）
        assert!(verdict_from_artifact(&p, &[]).pass);
    }

    /// 老工件（裸路径数组 / 只有正文）继续按 `file` 档核对。
    #[test]
    fn legacy_artifact_shapes_still_parse() {
        let file = write_named("legacy", "a.py", b"print(1)\n");
        let bare = typed_artifact("legacy.json", "PASS: done", &format!(r#"["{file}"]"#));
        assert!(verdict_from_artifact(&bare, &[EvidenceKind::File]).pass);
        // 对象缺 kind 也认：按 file
        let obj = typed_artifact(
            "legacy-nokind.json",
            "PASS: done",
            &format!(r#"[{{"path":"{file}"}}]"#),
        );
        assert!(verdict_from_artifact(&obj, &[]).pass);
        // 认不出的 kind 不猜：它连 file 档都不算
        let weird = typed_artifact(
            "legacy-unknown.json",
            "PASS: done",
            &format!(r#"[{{"path":"{file}","kind":"vibe"}}]"#),
        );
        let v = verdict_from_artifact(&weird, &[]);
        assert!(!v.pass, "{:?}", v.issues);
        assert!(
            v.issues.iter().any(|i| i.contains("unknown kind")),
            "{:?}",
            v.issues
        );
    }

    /// 要求表是数据：stage 名与类型名都从线名解析；**认不出的类型硬失败**——
    /// 「永远满足不了的要求」会让 pipeline 无限回派，比报错难查得多。
    #[test]
    fn evidence_spec_parses_stages_and_fails_closed_on_unknown_kind() {
        let dir = std::env::temp_dir().join(format!("kestrel-spec-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let good = dir.join("spec.json");
        std::fs::write(
            &good,
            r#"{"dev": ["file", "diff"], "test": ["test_log"], "build": ["build_log"], "whatever": ["file"]}"#,
        )
        .unwrap();
        let spec = EvidenceSpec::from_json_file(&good).unwrap();
        assert_eq!(
            spec.required_for(Stage::Dev),
            &[EvidenceKind::File, EvidenceKind::Diff]
        );
        assert_eq!(spec.required_for(Stage::Test), &[EvidenceKind::TestLog]);
        assert_eq!(spec.required_for(Stage::Build), &[EvidenceKind::BuildLog]);
        // 表里没写的 stage = 不要求（只核存在性）
        assert!(spec.required_for(Stage::Review).is_empty());
        assert!(!spec.is_empty());
        let described = spec.describe();
        assert!(described.contains("test:test_log"), "{described}");

        let bogus = dir.join("bogus.json");
        std::fs::write(&bogus, r#"{"test": ["vibes"]}"#).unwrap();
        let err = EvidenceSpec::from_json_file(&bogus)
            .expect_err("an unsatisfiable requirement must be a hard error");
        assert!(err.to_string().contains("unknown evidence kind"), "{err}");

        let notjson = dir.join("bad.json");
        std::fs::write(&notjson, "dev: file").unwrap();
        assert!(
            EvidenceSpec::from_json_file(&notjson).is_err(),
            "坏表不能当成没表"
        );
    }
}
