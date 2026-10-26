//! Worker resident mode (`kestrel serve`): bus doorbell → engine turn →
//! result artifact + result bell back to the sender. Thin messages only:
//! the worker NEVER treats bell contents as conclusions (读盘为准, v3 §5.2).
//!
//! One engine and one session tree per worker: every bell is the NEXT turn of
//! the same conversation, so history, the read-before-edit ledger and the
//! compaction checkpoint carry across dispatches instead of resetting per bell.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;

use crate::bus::rpc::BusClient;
use crate::config::RoleConfig;
use crate::core::actor::{AgentHandle, EngineConfig};
use crate::core::events::{Event, EventMsg, MessageItem};
use crate::core::id::{SystemClock, UuidGen};
use crate::permissions::PermissionEngine;
use crate::provider::anthropic::AnthropicWire;
use crate::provider::openai_chat::OpenAiChatWire;
use crate::provider::retry::RetryTransport;
use crate::provider::wire::{
    HttpTransport, ModelMeta, ResolvedAuth, StopReason, WireFormat, WireId,
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

    loop {
        let bell = client.recv_notify().await?;
        let sender = bell.sender.clone();
        let iteration = bell.payload["iteration"].as_u64().unwrap_or(0);
        let task = bell_prompt(&bell.payload);

        // 注入面防线: bell 以带来源标注的 item 进入 prompt（acceptance 4）。
        handle.turn(vec![MessageItem::BusBell {
            sender: sender.clone(),
            text: task,
        }]);

        let mut result_text = String::new();
        let mut aborted = false;
        // Recorded so an empty artifact explains itself (a turn that burns its
        // round budget on tool calls never emits a final answer).
        // Every turn ends with TurnComplete or Fatal, both of which assign it.
        let stop_seen: Option<StopReason>;
        loop {
            let ev: Event = rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("engine closed"))?;
            match &ev.msg {
                EventMsg::TurnComplete { stop, .. } => {
                    if matches!(stop, StopReason::Aborted) {
                        aborted = true;
                    }
                    stop_seen = Some(stop.clone());
                    break;
                }
                EventMsg::Fatal { error } => {
                    // The turn died, not the worker: say so in the artifact and
                    // stay available for the next bell.
                    result_text = format!("fatal: {error}");
                    stop_seen = Some(StopReason::error(error.clone(), true));
                    break;
                }
                EventMsg::MessageDone {
                    item: MessageItem::AssistantText { text },
                } => {
                    result_text = text.clone();
                }
                _ => {}
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
                "result_ref": artifact.display().to_string(),
            }),
        );
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
    use super::bell_prompt;
    use serde_json::json;

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
