//! `kestrel demo-sa`: scripted SA driver for the 3-terminal demo (plan §4-M4
//! demo 场景 1-4)。真实派发智能由 SA 角色 prompt + LLM 在后续版本接管；本
//! 驱动验证的是 write-ahead 顺序、崩溃对账、fail 回环与预算停损的机械语义。

use std::path::PathBuf;
use std::time::Duration;

use crate::bus::rpc::BusClient;
use crate::journal::Journal;
use crate::journal::records::{JournalRecord, Stage, ThinBell, Verdict, reconcile};

pub struct DemoSaArgs {
    pub bus_socket: PathBuf,
    pub task: String,
    pub journal: PathBuf,
    pub artifacts: PathBuf,
    /// fixture 单次派发的记账金额（demo 无真实 usage）。
    pub per_dispatch_usd: f64,
    pub max_usd: f64,
    pub max_iterations: u64,
}

/// 一个 stage 的派发结果：收敛（pass）。
enum StageOutcome {
    Pass,
}

/// dispatch → wait bell → reconcile（读盘为准）→ 推进/回派，直至 release。
pub async fn run_demo_sa(args: DemoSaArgs) -> anyhow::Result<()> {
    let journal = Journal::create(&args.journal)?;
    let sa = BusClient::connect(&args.bus_socket, "harness-sa", None).await?;
    let kill_after = std::env::var("KESTREL_TEST_KILL_AFTER").unwrap_or_default();
    let stages = [Stage::Dev, Stage::Review, Stage::Test, Stage::Build];

    for stage in stages {
        let target = match stage {
            Stage::Dev => "harness-rd",
            _ => "harness-qa",
        };
        let stage_name = format!("{stage:?}").to_lowercase();
        let mut iter = 1u64;

        // 值循环：Pass = stage 收敛；Fail/重试走 continue（iter 已自增）。
        let _outcome: StageOutcome = loop {
            // --- 预算停损：账本先看，超限 STOP escalate（退出码非 0）。
            let total: f64 = reconcile(&journal.replay()).state.spent_usd.values().sum();
            if total + args.per_dispatch_usd > args.max_usd {
                anyhow::bail!(
                    "[sa] budget stop-loss: spent {total:.2} + next {} > max {}",
                    args.per_dispatch_usd,
                    args.max_usd
                );
            }
            if iter > args.max_iterations {
                anyhow::bail!("[sa] iteration cap reached for {stage:?}");
            }

            // --- 先写盘，再拉铃（规则 1：顺序即纪律）。
            journal.append(&JournalRecord::StageWritten {
                task: "demo".into(),
                stage,
                iteration: iter,
            })?;
            let bell = ThinBell {
                role: target.strip_prefix("harness-").unwrap_or(target).into(),
                stage,
                check: Some(stage_name.clone()),
                iteration: iter,
                r#ref: serde_json::json!({ "task": args.task, "artifacts": args.artifacts }),
            };
            journal.append(&JournalRecord::BellSent {
                task: "demo".into(),
                to: target.into(),
                bell: bell.clone(),
            })?;
            journal.append(&JournalRecord::BudgetSpent {
                peer: target.into(),
                usage: Default::default(),
                usd: args.per_dispatch_usd,
            })?;
            if kill_after == "bell_sent" {
                eprintln!("[demo-sa] crash injection: after BellSent");
                std::process::abort();
            }
            sa.notify(
                target,
                serde_json::json!({
                    "role": bell.role, "stage": stage_name,
                    "iteration": iter, "task": args.task, "ref": bell.r#ref,
                }),
            );

            // --- 等回铃；超时/丢失则巡检（journal 对账 + 读盘工件）。
            let result =
                match tokio::time::timeout(Duration::from_secs(180), sa.recv_notify()).await {
                    Ok(Ok(env)) => Some(env),
                    _ => None,
                };
            let artifact = args
                .artifacts
                .join(format!("{target}-{stage_name}-iter{iter}.json"));

            match result {
                Some(env) if env.payload["result_ref"].is_string() => {
                    journal.append(&JournalRecord::BellReceived {
                        task: "demo".into(),
                        from: env.sender.clone(),
                        bell: bell.clone(),
                    })?;
                    if kill_after == "result_received" {
                        eprintln!("[demo-sa] crash injection: after result received");
                        std::process::abort();
                    }
                    journal.append(&JournalRecord::ResultCommitted {
                        task: "demo".into(),
                        stage,
                        verdict: Verdict {
                            pass: true,
                            issues: vec![],
                        },
                    })?;
                    println!("[sa] {stage_name} iter{iter}: pass");
                    break StageOutcome::Pass;
                }
                _ => {
                    // 铃丢了：巡检 = journal 对账 + 读盘。工件在盘上即视为完成
                    // （worker 写完工件、死于拉铃前——不重复派发）。
                    let still_in_flight = reconcile(&journal.replay())
                        .in_flight
                        .iter()
                        .any(|b| b.bell.stage == stage && b.bell.iteration == iter);
                    if still_in_flight && artifact.exists() {
                        println!("[sa] bell lost but artifact on disk: {artifact:?} -> settled");
                        journal.append(&JournalRecord::ResultCommitted {
                            task: "demo".into(),
                            stage,
                            verdict: Verdict {
                                pass: true,
                                issues: vec![],
                            },
                        })?;
                        break StageOutcome::Pass;
                    }
                    println!("[sa] {stage_name} iter{iter}: no result -> re-dispatch");
                    iter += 1;
                    continue;
                }
            }
        };
    }
    println!("[sa] pipeline complete");
    Ok(())
}
