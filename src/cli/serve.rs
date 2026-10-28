//! Worker resident mode (`kestrel serve`): bus doorbell → engine turn →
//! result artifact + result bell back to the sender. Thin messages only:
//! the worker NEVER treats bell contents as conclusions (读盘为准, v3 §5.2).
//!
//! One engine and one session tree per worker: every bell is the NEXT turn of
//! the same conversation, so history, the read-before-edit ledger and the
//! compaction checkpoint carry across dispatches instead of resetting per bell.

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use serde_json::Value;

use crate::bus::frame::{Envelope, FrameKind};
use crate::bus::rpc::BusClient;
use crate::cli::evidence::evidence_from_answer;
use crate::config::RoleConfig;
use crate::core::actor::{AgentHandle, EngineConfig};
use crate::core::events::{Event, EventMsg, MessageItem};
use crate::core::id::{SystemClock, UuidGen};
use crate::core::ops::{ApprovalDecision, Op};
use crate::permissions::PermissionEngine;
use crate::provider::anthropic::AnthropicWire;
use crate::provider::openai_chat::OpenAiChatWire;
use crate::provider::retry::RetryTransport;
use crate::provider::wire::{
    HttpTransport, ModelMeta, ResolvedAuth, StopReason, Usage, WireFormat, WireId,
};
use crate::sandbox::platform_sandbox;
use crate::session::SessionStore;
use crate::tools::ApproveMode;
use crate::tools::bash::Bash;
use crate::tools::edit::Edit;
use crate::tools::read::Read;
use crate::tools::write::Write;

pub struct ServeArgs {
    pub bus_socket: PathBuf,
    pub session: String,
    pub role_file: PathBuf,
    pub model: ModelMeta,
    pub api_key: String,
    /// Where result artifacts are written (SA reads disk, not bells).
    pub artifacts_dir: PathBuf,
    /// `--approve` override; absent falls back to the role's
    /// `[permissions].approve`, absent that deny (runtime > role > fail closed).
    pub approve: Option<ApproveMode>,
}

/// Resident worker: one long-lived engine, one turn per doorbell, one
/// append-only session file for the whole run.
pub async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let client = BusClient::connect(&args.bus_socket, &args.session, Some(&args.session))
        .await
        .with_context(|| format!("connect to bus at {}", args.bus_socket.display()))?;
    let role = RoleConfig::load(&args.role_file)?;
    let base = args
        .role_file
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .to_path_buf();
    let fs = role
        .fs_policy(&base)
        .with_context(|| format!("role fs policy (base {})", base.display()))?;
    let engine_policy = Arc::new(PermissionEngine::new(
        role.policy_table(&base).context("role policy table")?,
    ));
    let system = role.system_prompt(&base).context("role system prompt")?;
    std::fs::create_dir_all(&args.artifacts_dir)
        .with_context(|| format!("create artifacts dir {}", args.artifacts_dir.display()))?;
    let approve = args.approve.unwrap_or_else(|| role.approve_mode());

    // The worker lives in its first writable root (the dispatch worktree);
    // that is also the cwd the session tree gets filed under.
    let cwd = fs
        .writable_roots
        .first()
        .cloned()
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
    let store = SessionStore::create(
        &crate::config::kestrel_home(),
        &cwd,
        &role.role,
        args.model.wire_id_str(),
        &args.model.id,
        Arc::new(UuidGen),
        Arc::new(SystemClock),
    )
    .with_context(|| format!("open worker session under cwd {}", cwd.display()))?;

    let wire: Arc<dyn WireFormat> = match args.model.wire {
        WireId::AnthropicMessages => Arc::new(AnthropicWire),
        WireId::OpenAiCompletions => Arc::new(OpenAiChatWire),
    };
    let handle = AgentHandle::spawn(EngineConfig {
        provider: wire,
        model: args.model.clone(),
        auth: ResolvedAuth::key(args.api_key.clone()),
        transport: Arc::new(RetryTransport::new(Arc::new(HttpTransport::new()))),
        system,
        tools: vec![
            Arc::new(Bash),
            Arc::new(Read),
            Arc::new(Write),
            Arc::new(Edit),
        ],
        max_rounds: 8,
        cwd,
        role: role.role.clone(),
        idgen: Arc::new(UuidGen),
        sandbox: platform_sandbox(),
        fs,
        engine: Arc::clone(&engine_policy),
        approve,
        session: Some(Arc::new(store)),
    });
    let mut rx = handle.take_event_rx().expect("single consumer");
    // A bell that lands mid-turn is buffered, not dropped: the engine has one
    // active slot, so the next bell is the next turn either way.
    let mut pending_bells: VecDeque<Envelope> = VecDeque::new();
    // `approve = "bus"`: the role names the approver; silent means "ask whoever
    // rang this bell", which in the star topology is the SA.
    let approver_override = role.approver().map(str::to_string);

    loop {
        let bell = match pending_bells.pop_front() {
            Some(b) => b,
            None => client.recv_notify().await?,
        };
        let sender = bell.sender.clone();
        let iteration = bell.payload["iteration"].as_u64().unwrap_or(0);
        let task = bell_prompt(&bell.payload);
        let approver = approver_override.clone().unwrap_or_else(|| sender.clone());

        // 注入面防线: bell 以带来源标注的 item 进入 prompt（acceptance 4）。
        handle.turn(vec![MessageItem::BusBell {
            sender: sender.clone(),
            text: task,
        }]);

        let mut result_text = String::new();
        let mut aborted = false;
        let mut usage = Usage::default();
        // Audit trail of the reverse requests raised in this turn: the artifact
        // shows who widened what (SA 判定读盘为准).
        let mut approvals: Vec<Value> = Vec::new();
        let mut open: HashSet<String> = HashSet::new();
        // Recorded so an empty artifact explains itself (a turn that burns its
        // round budget on tool calls never emits a final answer).
        // Every turn ends with TurnComplete or Fatal, both of which assign it.
        let stop_seen: Option<StopReason>;
        loop {
            tokio::select! {
                ev = rx.recv() => {
                    let ev: Event = match ev {
                        Some(e) => e,
                        None => anyhow::bail!("engine closed"),
                    };
                    match ev.msg {
                        EventMsg::TurnComplete { stop, usage: seen } => {
                            if matches!(stop, StopReason::Aborted) {
                                aborted = true;
                            }
                            usage = seen;
                            stop_seen = Some(stop);
                            // The turn is over: any unanswered request is already
                            // denied by the orchestrator's deadline, so a late
                            // reply must not be interpreted in the NEXT turn.
                            open.clear();
                            break;
                        }
                        EventMsg::Fatal { error } => {
                            // The turn died, not the worker: say so in the artifact
                            // and stay available for the next bell.
                            result_text = format!("fatal: {error}");
                            stop_seen = Some(StopReason::error(error, true));
                            open.clear();
                            break;
                        }
                        EventMsg::MessageDone {
                            item: MessageItem::AssistantText { text },
                        } => {
                            result_text = text;
                        }
                        EventMsg::ApprovalRequest { req, prompt, detail } => {
                            let frame = approval_request_frame(
                                &args.session,
                                &role.role,
                                &req,
                                &prompt,
                                &detail,
                            );
                            client.notify(&approver, frame);
                            open.insert(req);
                            tracing::info!("approval request sent to {approver}");
                        }
                        _ => {}
                    }
                }
                // Read the bus ONLY while a request is open, otherwise this
                // branch would swallow the next doorbell. The orchestrator's own
                // deadline bounds the wait: silence is a deny, never a hang.
                frame = client.recv_notify(), if !open.is_empty() => {
                    let frame: Envelope = match frame {
                        Ok(f) => f,
                        Err(e) => {
                            // The bus went away while a request was open: there
                            // is nobody left to answer, so deny everything open
                            // (fail closed) and let the turn converge on those
                            // is_error results instead of spinning on a dead
                            // socket.
                            tracing::warn!(
                                "bus lost during approval: {e}; denying {} open request(s)",
                                open.len()
                            );
                            for req in open.drain() {
                                handle.submit(Op::Approval {
                                    req: req.clone(),
                                    decision: ApprovalDecision::Deny,
                                });
                                approvals.push(serde_json::json!({
                                    "req": req,
                                    "decision": "deny",
                                    "by": "bus-lost",
                                }));
                            }
                            continue;
                        }
                    };
                    if frame.kind != FrameKind::Notify {
                        tracing::warn!(
                            "dropping non-notify frame while awaiting approval: {:?}",
                            frame.kind
                        );
                    } else if frame.payload["type"].as_str() == Some("approval_decision") {
                        let req = frame.payload["req"].as_str().unwrap_or_default().to_string();
                        if open.remove(&req) {
                            let decision = decision_from_frame(&frame.payload);
                            handle.submit(Op::Approval {
                                req: req.clone(),
                                decision,
                            });
                            approvals.push(serde_json::json!({
                                "req": req,
                                "decision": decision_name(decision),
                                "by": frame.sender,
                            }));
                        } else {
                            tracing::warn!("approval reply for an unknown request: {req}");
                        }
                    } else {
                        pending_bells.push_back(frame);
                    }
                }
            }
        }

        // Write the result artifact (SA 巡检读盘为准), then ring the result.
        // The name is the SA's disk-polling contract: {session}-{stage}-iter{n}.json
        // — demo-sa reconstructs exactly this path, so both ends must agree.
        let stage = bell.payload["stage"].as_str().unwrap_or("unknown");
        let artifact = args
            .artifacts_dir
            .join(format!("{}-{}-iter{}.json", args.session, stage, iteration));
        let artifact_json = serde_json::json!({
            "session": args.session,
            "stage": bell.payload["stage"],
            "iteration": iteration,
            "aborted": aborted,
            "stop": stop_seen,
            "result": result_text,
            "evidence": evidence_from_answer(&result_text),
            "usage": usage,
            "approvals": approvals,
            "ref": bell.payload["ref"],
        });
        std::fs::write(&artifact, serde_json::to_string_pretty(&artifact_json)?)
            .with_context(|| format!("write artifact {}", artifact.display()))?;

        client.notify(
            &sender,
            serde_json::json!({
                "role": args.session,
                "stage": bell.payload["stage"],
                "iteration": iteration,
                "usage": usage,
                "result_ref": artifact.display().to_string(),
            }),
        );
    }
}

/// `approve = "bus"` wire shape: the approver decides on data — tool,
/// arguments, command string — not on the human-readable prompt alone.
pub fn approval_request_frame(
    session: &str,
    role: &str,
    req: &str,
    prompt: &str,
    detail: &Value,
) -> Value {
    serde_json::json!({
        "type": "approval_request",
        "session": session,
        "role": role,
        "req": req,
        "prompt": prompt,
        "detail": detail,
        "command": approval_command(detail),
    })
}

/// The command line an authority table can judge. Non-shell tools come back
/// empty, and an empty command is denied rather than guessed at.
pub fn approval_command(detail: &Value) -> String {
    detail["args"]["command"]
        .as_str()
        .or_else(|| detail["command"].as_str())
        .unwrap_or_default()
        .to_string()
}

/// Reply frame -> decision. Anything that is not an explicit "allow" is a DENY
/// (fail closed): a garbled, stale or spoofed reply must never widen a sandbox.
pub fn decision_from_frame(payload: &Value) -> ApprovalDecision {
    match payload["decision"].as_str() {
        Some(s) if s.eq_ignore_ascii_case("allow") => ApprovalDecision::AllowOnce,
        _ => ApprovalDecision::Deny,
    }
}

/// Wire spelling of a decision.
pub fn decision_name(decision: ApprovalDecision) -> &'static str {
    match decision {
        ApprovalDecision::AllowOnce => "allow",
        ApprovalDecision::Deny => "deny",
    }
}

/// 薄消息 = 指针，但指针本身必须交到 worker 手里：task 之外还要带上 check /
/// iteration / ref，否则「读盘为准」无从执行——2026-10-26 冒烟里 QA 收到的铃
/// 只有一句 task 文本，它连「验哪个目录、验什么项」都不知道，只能判 FAIL。
pub fn bell_prompt(payload: &serde_json::Value) -> String {
    let task = payload["task"]
        .as_str()
        .or_else(|| payload["ref"]["task"].as_str())
        .unwrap_or("(no task text in bell; see ref on disk)");
    let mut out = String::from(task);
    if let Some(check) = payload["check"].as_str() {
        out.push_str("\ncheck: ");
        out.push_str(check);
    }
    if let Some(iter) = payload["iteration"].as_u64() {
        out.push_str(&format!("\niteration: {iter}"));
    }
    let reference = &payload["ref"];
    if !reference.is_null() {
        out.push_str("\nref: ");
        out.push_str(&serde_json::to_string(reference).unwrap_or_default());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        ApprovalDecision, approval_command, approval_request_frame, bell_prompt,
        decision_from_frame, decision_name,
    };
    use serde_json::json;

    /// The approver judges the command, so the frame must carry it: a request
    /// that only quotes the prompt text cannot be put through a policy table.
    #[test]
    fn approval_frame_carries_the_command_to_judge() {
        let detail = json!({ "tool": "bash", "args": { "command": "git status" } });
        let frame = approval_request_frame("harness-rd", "rd", "req-1", "run command?", &detail);
        assert_eq!(frame["type"], "approval_request");
        assert_eq!(frame["session"], "harness-rd");
        assert_eq!(frame["req"], "req-1");
        assert_eq!(frame["command"], "git status");
        assert_eq!(approval_command(&detail), "git status");
        // escalation-shaped payloads carry the command at the top level
        assert_eq!(
            approval_command(&json!({ "command": "cargo test" })),
            "cargo test"
        );
        // no command to judge => empty, and the authority denies rather than guesses
        assert_eq!(approval_command(&json!({ "tool": "write" })), "");
    }

    /// Only an explicit "allow" allows. A missing, garbled or spoofed reply is
    /// a deny (fail closed).
    #[test]
    fn only_an_explicit_allow_approves() {
        for (payload, want) in [
            (json!({ "decision": "allow" }), "allow"),
            (json!({ "decision": "ALLOW" }), "allow"),
            (json!({ "decision": "maybe" }), "deny"),
            (json!({ "decision": "deny" }), "deny"),
            (json!({}), "deny"),
        ] {
            assert_eq!(
                decision_name(decision_from_frame(&payload)),
                want,
                "{payload}"
            );
        }
        assert_eq!(
            decision_from_frame(&json!({ "decision": "allow" })),
            ApprovalDecision::AllowOnce
        );
    }

    #[test]
    fn bell_prompt_hands_the_pointer_to_the_worker() {
        let text = bell_prompt(&json!({
            "role": "qa",
            "stage": "review",
            "check": "review",
            "iteration": 2,
            "task": "demo: 实现一个 LRU cache",
            "ref": { "artifacts": "/tmp/art", "worktree": "/tmp/kestrel-worktree" },
        }));
        assert!(text.starts_with("demo: 实现一个 LRU cache"), "{text}");
        assert!(text.contains("check: review"), "{text}");
        assert!(text.contains("iteration: 2"), "{text}");
        assert!(text.contains("/tmp/kestrel-worktree"), "{text}");
        assert!(text.contains("/tmp/art"), "{text}");
    }

    #[test]
    fn bell_prompt_degrades_without_a_ref() {
        let text = bell_prompt(&json!({ "task": "only task" }));
        assert_eq!(text, "only task");
        let fallback = bell_prompt(&json!({ "ref": { "task": "task inside ref" } }));
        assert!(fallback.starts_with("task inside ref"), "{fallback}");
    }
}
