//! `kestrel demo-sa`: scripted SA driver for the 3-terminal demo (plan §4-M4
//! demo 场景 1-4)。真实派发智能由 SA 角色 prompt + LLM 在后续版本接管；本
//! 驱动验证的是 write-ahead 顺序、崩溃对账、fail 回环与预算停损的机械语义。

use std::path::{Path, PathBuf};
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
    /// How long to wait for a worker's result bell before declaring the bell
    /// lost (the worker keeps the turn; the SA falls back to disk inspection).
    pub bell_timeout: Duration,
    /// The dispatch worktree RD writes into. It travels in the bell `ref` so QA
    /// verifies the same tree instead of guessing — without it the reviewer has
    /// nothing on disk to look at and stages fail for the wrong reason.
    pub worktree: Option<PathBuf>,
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
                r#ref: serde_json::json!({
                    "task": args.task,
                    "artifacts": args.artifacts,
                    "worktree": args.worktree,
                }),
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

            // --- 等回铃；超时/丢失则巡检（journal 对账 + 读盘工件）。worker 是
            // 常驻引擎，一个回合可能跑满 8 轮工具调用，超时给足（可 --bell-timeout-secs 调）。
            let result = match tokio::time::timeout(args.bell_timeout, sa.recv_notify()).await {
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
                }
                _ => {
                    // 铃丢了：巡检 = journal 对账 + 读盘。工件在盘上即视为完成
                    // （worker 写完工件、死于拉铃前——不重复派发）。
                    let still_in_flight = reconcile(&journal.replay())
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
                task: "demo".into(),
                stage,
                verdict: verdict.clone(),
            })?;
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
    println!("[sa] pipeline complete");
    Ok(())
}

/// 从 worker 工件推出该 stage 的判定。契约字段由 `serve` 写入
/// （`result` / `aborted` / `stop.*`），`result` 头部必须给出判定词。
/// 两层都要过：通路层（有答案、未中断、worker 侧 stop 不是 error）与语义层
/// （worker 自己声明的结论）——「工件在盘上」从来不等于「stage 通过」。
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
    if result.is_empty() {
        issues.push("empty result".to_string());
    } else {
        match parse_verdict(&result) {
            WorkerVerdict::Converged => {}
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

    fn write_artifact(name: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kestrel-verdict-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn artifact_with_answer_is_pass() {
        let p = write_artifact(
            "ok.json",
            r#"{"result":"DONE: LRU implemented in lru.py","aborted":false,"stop":{"type":"stop"}}"#,
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
        assert_eq!(v.issues.len(), 2, "{:?}", v.issues);
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
            let p = write_artifact(
                "head.json",
                &format!(r#"{{"result":"{head}","aborted":false,"stop":{{"type":"stop"}}}}"#),
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
