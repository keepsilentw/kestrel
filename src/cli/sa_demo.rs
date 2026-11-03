//! `kestrel demo-sa`: scripted SA driver for the 3-terminal demo (plan §4-M4
//! demo 场景 1-4)。真实派发智能由 SA 角色 prompt + LLM 在后续版本接管；本
//! 驱动验证的是 write-ahead 顺序、崩溃对账、fail 回环与预算停损的机械语义。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;

use crate::bus::frame::Envelope;
use crate::bus::rpc::BusClient;
use crate::cli::evidence::{check_evidence, evidence_from_answer};
use crate::cli::serve::{approval_command, decision_name};
use crate::cli::task::artifact_path;
use crate::core::ops::ApprovalDecision;
use crate::journal::Journal;
use crate::journal::records::{JournalRecord, Stage, ThinBell, Verdict};
use crate::permissions::{Decision, PermissionEngine};
use crate::provider::config::Registry;
use crate::provider::wire::{CostPerMTok, Usage};

pub struct DemoSaArgs {
    pub bus_socket: PathBuf,
    pub task: String,
    pub journal: PathBuf,
    pub artifacts: PathBuf,
    /// fixture 单次派发的名义记账金额：demo 没有价格表，`usd` 是按派发次数计的
    /// 停损单位，不是账单。真实 token 用量另记 `usage_reported`（见下）。
    pub per_dispatch_usd: f64,
    pub max_usd: f64,
    pub max_iterations: u64,
    /// How long to wait for a worker's result bell before declaring the bell
    /// lost (the worker keeps the turn; the SA falls back to disk inspection).
    pub bell_timeout: Duration,
    /// The dispatch worktree RD writes into. It travels in the bell `ref` so QA
    /// verifies the same tree instead of guessing — without it the reviewer has
    /// nothing on disk to look at and stages fail for the wrong reason.
    pub worktree: Option<PathBuf>,
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
}

/// 一个 peer 后端的定价依据：SA 用它把 token 换成钱。
#[derive(Debug, Clone)]
pub struct BackendPrice {
    /// role 的 `[llm]`：哪个 provider 的哪个 model（写进账本，事后能复算）。
    pub provider: String,
    pub model: String,
    /// registry 声明的费率。`None` = 这张表没人填——**不等于免费**。
    pub cost: Option<CostPerMTok>,
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
                    context_window: meta.as_ref().map(|m| m.context_window).unwrap_or(128_000),
                    max_out: meta.as_ref().map(|m| m.max_out).unwrap_or(16_384),
                },
            );
        }
        Self { peers }
    }

    /// 派发前的预留金额 `(usd, priced)`：有费率 => 该模型的最坏情况账单；
    /// 没有费率 => 名义值，并带着「这是名义值」的标记进账本。
    pub fn reservation(&self, peer: &str, nominal_usd: f64) -> (f64, bool) {
        match self.peers.get(peer).and_then(|p| p.cost) {
            Some(cost) => (
                Usage::worst_case_usd(&cost, self.context_window(peer), self.max_out(peer)),
                true,
            ),
            None => (nominal_usd, false),
        }
    }

    /// 结算金额：实际 token × 费率。查不到费率返回 `None`——预留原样留在账上，
    /// 这比把「没人填价」记成「这次免费」诚实。
    pub fn settle(&self, peer: &str, usage: &Usage) -> Option<f64> {
        self.peers
            .get(peer)
            .and_then(|p| p.cost)
            .map(|cost| usage.cost_usd(&cost))
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

/// 一个 stage 的派发结果：收敛（pass）。
enum StageOutcome {
    Pass,
}

/// dispatch → wait bell → reconcile（读盘为准）→ 推进/回派，直至 release。
pub async fn run_demo_sa(args: DemoSaArgs) -> anyhow::Result<()> {
    let journal = Journal::create(&args.journal)?;
    let sa = BusClient::connect(&args.bus_socket, "harness-sa", None).await?;
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
    // `--max-usd` 是账单上限。
    let prices = load_prices(args.prices.as_deref());
    let ledger = PriceLedger::discover(args.roles_dir.as_deref(), prices.as_ref());
    let unpriced: Vec<&str> = [args.rd_peer.as_str(), args.qa_peer.as_str()]
        .into_iter()
        .filter(|p| !ledger.is_priced(p))
        .collect();
    if !unpriced.is_empty() {
        println!(
            "[sa] budget stays NOMINAL for {} (known peers: {}) — each dispatch books {} usd \
             instead of a bill; declare [models.cost] in providers.toml and pass --roles-dir \
             to settle by token",
            unpriced.join(","),
            if ledger.known_peers().is_empty() {
                "none".to_string()
            } else {
                ledger.known_peers().join(",")
            },
            args.per_dispatch_usd
        );
    }

    for stage in stages {
        let target = args.peer_for(stage);
        let stage_name = format!("{stage:?}").to_lowercase();
        let mut iter = 1u64;

        // 值循环：Pass = stage 收敛；Fail/重试走 continue（iter 已自增）。
        let _outcome: StageOutcome = loop {
            // --- 预算停损：账本先看，超限 STOP escalate（退出码非 0）。
            // O(1) 投影，不是每次派发都重放整个活文件（不变式 5：两者由同一次折叠产
            // 生，`projection_equals_a_full_replay_after_every_append` 钉住这条）
            // 停损看的是**下一次派发的预留**：有费率时是那台模型的最坏情况账单
            // （整窗输入 + 满输出），没有费率时才是名义值。只看「事后账单」的停损
            // 拦不住正在跑的派发，崩溃重来还能反复把上限烧穿。
            let state = journal.projection().state;
            let total: f64 = state.spent_usd.values().sum();
            let nominal: f64 = state.nominal_usd.values().sum();
            let (reserve, priced) = ledger.reservation(target, args.per_dispatch_usd);
            if total + reserve > args.max_usd {
                anyhow::bail!(
                    "[sa] budget stop-loss: spent {total:.4} (nominal {nominal:.4}) + next \
                     reservation {reserve:.4}{} > max {:.4}",
                    if priced {
                        " = priced upper bound"
                    } else {
                        " (nominal)"
                    },
                    args.max_usd
                );
            }
            if iter > args.max_iterations {
                anyhow::bail!("[sa] iteration cap reached for {stage:?}");
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
                r#ref: serde_json::json!({
                    "task": args.task,
                    "artifacts": args.artifacts,
                    "worktree": args.worktree,
                }),
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
                    Ok(Ok(env)) if env.payload["type"].as_str() == Some("approval_request") => {
                        answer_approval(&journal, &sa, authority.as_ref(), &env, &args.task_id)?;
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
                        match ledger.settle(target, &usage) {
                            Some(usd) => {
                                journal.append(&JournalRecord::settled(
                                    args.task_id.clone(),
                                    target,
                                    stage,
                                    iter,
                                    usage,
                                    usd,
                                    true,
                                ))?;
                                println!(
                                    "[sa] {stage_name} iter{iter}: settled {usd:.4} usd for \
                                     {} in / {} out tokens",
                                    usage.input_tokens, usage.output_tokens
                                );
                            }
                            None => println!(
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
            // 不等于 stage 收敛——空结果、abort、worker 侧 stop=error 都是 fail。
            let verdict = verdict_from_artifact(&artifact);
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
            println!(
                "[sa] {stage_name} iter{iter}: fail ({}) -> re-dispatch",
                verdict.issues.join(", ")
            );
            iter += 1;
            continue;
        };
    }
    report_ledger(&journal, &ledger);
    println!("[sa] pipeline complete");
    Ok(())
}

/// 收尾报账：花了多少、其中多少仍是名义值、换出了多少 token。`--max-usd` 到底是
/// 「账单上限」还是「名义上限」，由这行说清楚，不靠人记住这次有没有传 `--roles-dir`。
fn report_ledger(journal: &Journal, ledger: &PriceLedger) {
    let state = journal.projection().state;
    if state.spent_usd.is_empty() {
        println!("[sa] ledger: nothing booked");
        return;
    }
    let nominal_total: f64 = state.nominal_usd.values().sum();
    let total: f64 = state.spent_usd.values().sum();
    for (peer, usd) in &state.spent_usd {
        let tokens = state.tokens.get(peer).copied().unwrap_or_default();
        let unsettled = state
            .reserved
            .iter()
            .filter(|((_, p, _, _), _)| p == peer)
            .count();
        println!(
            "[sa] ledger {peer}: {usd:.4} usd{}{} ; tokens in {} / out {}{}",
            if ledger.is_priced(peer) {
                ""
            } else {
                " (nominal backend: no rate)"
            },
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
    println!("[sa] ledger total: {total:.4} usd, of which {nominal_total:.4} nominal");
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
) -> anyhow::Result<()> {
    let req = env.payload["req"].as_str().unwrap_or_default().to_string();
    let worker = env.payload["session"]
        .as_str()
        .unwrap_or(env.sender.as_str())
        .to_string();
    // The frame carries the command at the top level; fall back to the detail
    // blob so an older worker is still judged on something real.
    let command = match env.payload["command"].as_str() {
        Some(c) => c.to_string(),
        None => approval_command(&env.payload["detail"]),
    };
    journal.append(&JournalRecord::ApprovalRequested {
        task: task.into(),
        peer: worker.clone(),
        req: req.clone(),
        command: command.clone(),
    })?;
    let decision = decide_approval(authority, &command);
    journal.append(&JournalRecord::ApprovalDecided {
        req: req.clone(),
        decision: decision_name(decision).to_string(),
        by: "harness-sa".into(),
    })?;
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
/// （worker 自己声明的结论）、证据层（它点名的文件确实在盘上有内容）——
/// 「工件在盘上」从来不等于「stage 通过」。
pub fn verdict_from_artifact(path: &Path) -> Verdict {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Verdict {
            pass: false,
            issues: vec![format!("artifact missing: {}", path.display())],
        };
    };
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) else {
        return Verdict {
            pass: false,
            issues: vec![format!("artifact is not valid JSON: {}", path.display())],
        };
    };
    let mut issues = Vec::new();
    let result = json["result"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_string();
    let mut converged = false;
    if result.is_empty() {
        issues.push("empty result".to_string());
    } else {
        match parse_verdict(&result) {
            WorkerVerdict::Converged => converged = true,
            WorkerVerdict::Failed(line) => issues.push(format!("verdict: {line}")),
            WorkerVerdict::Missing => {
                issues.push("no PASS/DONE/FAIL verdict in the head of result".into())
            }
        }
    }
    if json["aborted"].as_bool().unwrap_or(false) {
        issues.push("worker aborted".to_string());
    }
    if json["stop"]["type"].as_str() == Some("error") {
        issues.push(format!(
            "worker stop: {}",
            json["stop"]["message"].as_str().unwrap_or("error")
        ));
    }
    // 第三层（design §10 二期「工件 + 证据文件」的最小形态）：只有 worker 声称
    // 收敛时才核证据——判定词是声明，它点名的路径要在盘上真实存在且有内容。
    // 「PASS 但给不出可查的文件」不是收敛；FAIL 已经是 fail，不再要求它举证。
    if converged {
        let mut evidence: Vec<String> = json["evidence"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if evidence.is_empty() {
            // 兼容没有 evidence 字段的旧工件：从正文里提绝对路径。
            evidence = evidence_from_answer(&result);
        }
        if evidence.is_empty() {
            issues.push("no evidence paths to verify".to_string());
        } else {
            issues.extend(check_evidence(&evidence));
        }
    }
    Verdict {
        pass: issues.is_empty(),
        issues,
    }
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
            per_dispatch_usd: 0.5,
            max_usd: 6.0,
            max_iterations: 3,
            bell_timeout: Duration::from_secs(300),
            worktree: None,
            approvals: None,
            rotate_after: 0,
            rotate_max_bytes: 0,
            task_id: "demo".into(),
            rd_peer: "harness-rd".into(),
            qa_peer: "harness-qa".into(),
            roles_dir: None,
            prices: None,
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
        let v = verdict_from_artifact(&p);
        assert!(v.pass, "{:?}", v.issues);
    }

    #[test]
    fn empty_result_and_error_stop_are_fail() {
        let p = write_artifact(
            "empty.json",
            r#"{"result":"","aborted":false,"stop":{"type":"error","message":"round budget exhausted"}}"#,
        );
        let v = verdict_from_artifact(&p);
        assert!(!v.pass);
        assert!(
            v.issues.contains(&"empty result".to_string())
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
        let v = verdict_from_artifact(&bare);
        assert!(!v.pass, "a claim with nothing to check is not a pass");
        assert!(
            v.issues
                .contains(&"no evidence paths to verify".to_string()),
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
        let v = verdict_from_artifact(&p);
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
            verdict_from_artifact(&p).pass,
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
        let v = verdict_from_artifact(&fail);
        assert!(!v.pass, "{:?}", v.issues);
        assert_eq!(v.issues, vec!["verdict: FAIL: no LRU cache on disk"]);

        let rambling = write_artifact(
            "ramble.json",
            r#"{"result":"收到门铃，我先看了目录","aborted":false,"stop":{"type":"stop"}}"#,
        );
        let v = verdict_from_artifact(&rambling);
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
            let pass = verdict_from_artifact(&p).pass;
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
            verdict_from_artifact(&p)
                .issues
                .iter()
                .any(|i| i == "worker aborted")
        );
        let v = verdict_from_artifact(std::path::Path::new("/tmp/kestrel-no-such-artifact.json"));
        assert!(!v.pass);
        assert!(v.issues[0].starts_with("artifact missing"));
    }
}
