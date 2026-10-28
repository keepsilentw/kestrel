//! Worker resident mode (`kestrel serve`): bus doorbell → engine turn →
//! result artifact + result bell back to the sender. Thin messages only:
//! the worker NEVER treats bell contents as conclusions (读盘为准, v3 §5.2).
//!
//! 一个进程里 **每个 task 一个引擎、一棵 append-only 会话树**（§10 多任务并行）：
//! task 之间真并发，task 内部仍是单活跃槽——门铃是同一棵树上的一接一个回合，
//! 历史、read-before-edit 台账与压缩检查点跨派发存活，但不会跨 task 串台。
//! 总线的读、写与审批归属都在 `cli::pool` 的 pump 里；这里只负责装载角色配置并
//! 提供「为一个 task 造一台引擎」的工厂。

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use serde_json::Value;

use crate::bus::rpc::BusClient;
use crate::cli::pool::{EngineFactory, PoolConfig, PooledEngine, run_pool};
use crate::config::RoleConfig;
use crate::core::actor::{AgentHandle, EngineConfig};
use crate::core::id::{SystemClock, UuidGen};
use crate::core::ops::ApprovalDecision;
use crate::permissions::PermissionEngine;
use crate::provider::anthropic::AnthropicWire;
use crate::provider::openai_chat::OpenAiChatWire;
use crate::provider::openai_responses::OpenAiResponsesWire;
use crate::provider::retry::RetryTransport;
use crate::provider::wire::{HttpTransport, ModelMeta, ResolvedAuth, WireFormat, WireId};
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
    /// How many tasks this worker will host engines for. Over the ceiling a
    /// bell is REFUSED with a visible error bell; 0 means no ceiling.
    pub max_tasks: usize,
}

/// Resident worker: one long-lived process, one engine + session tree per task.
pub async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let client = Arc::new(
        BusClient::connect(&args.bus_socket, &args.session, Some(&args.session))
            .await
            .with_context(|| format!("connect to bus at {}", args.bus_socket.display()))?,
    );
    let role = RoleConfig::load(&args.role_file)?;
    let base = args
        .role_file
        .parent()
        .unwrap_or(Path::new("."))
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
    // that is also the cwd every task's session tree gets filed under.
    let cwd = fs
        .writable_roots
        .first()
        .cloned()
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
    let role_name = role.role.clone();
    let cfg = Arc::new(PoolConfig {
        session: args.session.clone(),
        role: role_name.clone(),
        artifacts_dir: args.artifacts_dir.clone(),
        // `approve = "bus"`: the role names the approver; silent means "ask
        // whoever rang this bell", which in the star topology is the SA.
        approver: role.approver().map(str::to_string),
        max_tasks: args.max_tasks,
    });

    let model = args.model.clone();
    let api_key = args.api_key.clone();
    let home = crate::config::kestrel_home();
    let wire: Arc<dyn WireFormat> = match model.wire {
        WireId::AnthropicMessages => Arc::new(AnthropicWire),
        WireId::OpenAiCompletions => Arc::new(OpenAiChatWire),
        WireId::OpenAiResponses => Arc::new(OpenAiResponsesWire),
    };
    let engine_policy = Arc::clone(&engine_policy);
    let factory: EngineFactory = Arc::new(move |task: &str| -> anyhow::Result<PooledEngine> {
        // One session tree per task: two pipelines must not share a history, a
        // read-before-edit ledger or a compaction checkpoint.
        let store = SessionStore::create(
            &home,
            &cwd,
            &role_name,
            model.wire_id_str(),
            &model.id,
            Arc::new(UuidGen),
            Arc::new(SystemClock),
        )
        .with_context(|| {
            format!(
                "open the session tree for task {task} under cwd {}",
                cwd.display()
            )
        })?;
        let handle = AgentHandle::spawn(EngineConfig {
            provider: Arc::clone(&wire),
            model: model.clone(),
            auth: ResolvedAuth::key(api_key.clone()),
            transport: Arc::new(RetryTransport::new(Arc::new(HttpTransport::new()))),
            system: system.clone(),
            tools: vec![
                Arc::new(Bash),
                Arc::new(Read),
                Arc::new(Write),
                Arc::new(Edit),
            ],
            max_rounds: 8,
            cwd: cwd.clone(),
            role: role_name.clone(),
            idgen: Arc::new(UuidGen),
            sandbox: platform_sandbox(),
            fs: fs.clone(),
            engine: Arc::clone(&engine_policy),
            approve,
            session: Some(Arc::new(store)),
        });
        let events = handle
            .take_event_rx()
            .context("event stream was already taken")?;
        Ok(PooledEngine { handle, events })
    });

    run_pool(client, cfg, factory).await
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
    // 谁在派这活：一个 worker 服务多条 pipeline 时，task id 是唯一的区分线索
    if let Some(id) = payload["task_id"].as_str().filter(|t| !t.trim().is_empty()) {
        out.push_str("\ntask id: ");
        out.push_str(id);
    }
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
            "task_id": "t2",
            "ref": { "artifacts": "/tmp/art", "worktree": "/tmp/kestrel-worktree" },
        }));
        assert!(text.starts_with("demo: 实现一个 LRU cache"), "{text}");
        assert!(text.contains("task id: t2"), "{text}");
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
