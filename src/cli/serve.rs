//! Worker resident mode (`kestrel serve`): bus doorbell → engine turn →
//! result artifact + result bell back to the sender. Thin messages only:
//! the worker NEVER treats bell contents as conclusions (读盘为准, v3 §5.2).

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;

use crate::bus::rpc::BusClient;
use crate::config::RoleConfig;
use crate::core::actor::{AgentHandle, EngineConfig};
use crate::core::events::{Event, EventMsg, MessageItem};
use crate::core::id::UuidGen;
use crate::permissions::PermissionEngine;
use crate::provider::anthropic::AnthropicWire;
use crate::provider::openai_chat::OpenAiChatWire;
use crate::provider::retry::RetryTransport;
use crate::provider::wire::{
    HttpTransport, ModelMeta, ResolvedAuth, StopReason, WireFormat, WireId,
};
use crate::sandbox::platform_sandbox;
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
}

/// Resident worker: wait for doorbells, run one turn per bell, write the
/// result artifact, then ring the result back to the sender.
pub async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let client = BusClient::connect(&args.bus_socket, &args.session, Some(&args.session))
        .await
        .with_context(|| format!("connect to bus at {}", args.bus_socket.display()))?;
    let role = RoleConfig::load(&args.role_file)
        .with_context(|| format!("load role {}", args.role_file.display()))?;
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

    loop {
        let bell = client.recv_notify().await?;
        let sender = bell.sender.clone();
        let iteration = bell.payload["iteration"].as_u64().unwrap_or(0);
        let task = bell.payload["task"]
            .as_str()
            .or_else(|| bell.payload["ref"]["task"].as_str())
            .unwrap_or("(no task text in bell; see ref on disk)")
            .to_string();

        let wire: Arc<dyn WireFormat> = match args.model.wire {
            WireId::AnthropicMessages => Arc::new(AnthropicWire),
            WireId::OpenAiCompletions => Arc::new(OpenAiChatWire),
        };
        let tools: Vec<Arc<dyn crate::tools::Tool>> = vec![
            Arc::new(Bash),
            Arc::new(Read),
            Arc::new(Write),
            Arc::new(Edit),
        ];
        let handle = AgentHandle::spawn(EngineConfig {
            provider: wire,
            model: args.model.clone(),
            auth: ResolvedAuth::key(args.api_key.clone()),
            transport: Arc::new(RetryTransport::new(Arc::new(HttpTransport::new()))),
            system: system.clone(),
            tools,
            max_rounds: 8,
            cwd: fs
                .writable_roots
                .first()
                .cloned()
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_default()),
            role: role.role.clone(),
            idgen: Arc::new(UuidGen),
            sandbox: platform_sandbox(),
            fs: fs.clone(),
            engine: Arc::clone(&engine_policy),
            approve: ApproveMode::HeadlessDeny,
            session: None,
        });
        let mut rx = handle.take_event_rx().expect("single consumer");
        // 注入面防线: bell 以带来源标注的 item 进入 prompt（acceptance 4）。
        handle.turn(vec![MessageItem::BusBell {
            sender: sender.clone(),
            text: task,
        }]);

        let mut result_text = String::new();
        let mut aborted = false;
        // Recorded so an empty artifact explains itself (a turn that burns its
        // round budget on tool calls never emits a final answer).
        let mut stop_seen: Option<StopReason> = None;
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
                    result_text = format!("fatal: {error}");
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
        handle.shutdown().await;

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
        std::fs::write(&artifact, serde_json::to_string_pretty(&artifact_json)?)?;

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
