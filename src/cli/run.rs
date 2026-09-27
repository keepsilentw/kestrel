//! One-shot headless run: prompt in, NDJSON event stream out.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;

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
use crate::sandbox::{FsPolicy, Unsandboxed};
use crate::session::SessionStore;
use crate::tools::ApproveMode;
use crate::tools::bash::Bash;
use crate::tools::edit::Edit;
use crate::tools::read::Read;
use crate::tools::write::Write;

pub struct RunArgs {
    pub prompt: String,
    /// Fully-resolved model metadata (wire/base_url/compat from the registry
    /// or direct flags — resolution happens in main.rs).
    pub model: ModelMeta,
    pub api_key: String,
    pub system: String,
    pub timeout: Duration,
    pub role_file: Option<PathBuf>,
    pub approve: ApproveMode,
    pub tools_enabled: bool,
}

/// Run a single turn against a real endpoint, printing normalized events as
/// NDJSON on stdout. Exit code: 0 ok / 1 error / 2 budget / 3 aborted.
pub async fn run_once(args: RunArgs) -> anyhow::Result<i32> {
    let model = args.model.clone();
    let wire: Arc<dyn WireFormat> = match model.wire {
        WireId::AnthropicMessages => Arc::new(AnthropicWire),
        WireId::OpenAiCompletions => Arc::new(OpenAiChatWire),
    };

    // Role config: from --role file, or permissive defaults for ad-hoc runs.
    let (role_name, fs, engine, system) = match &args.role_file {
        Some(path) => {
            let cfg = RoleConfig::load(path)?;
            let base = path
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .to_path_buf();
            let fs = cfg.fs_policy(&base)?;
            let engine = Arc::new(PermissionEngine::new(cfg.policy_table(&base)?));
            // A declared prompt_file that cannot be read is an error, not a
            // silent fallback: swallowing it hid a wrong relative path across
            // the whole harness/ demo (the worker failed, `-p` looked fine).
            let from_role = cfg.system_prompt(&base).context("role system prompt")?;
            let system = if from_role.is_empty() {
                args.system.clone()
            } else {
                from_role
            };
            (cfg.role, fs, engine, system)
        }
        None => {
            let cwd = std::env::current_dir()?;
            let fs = FsPolicy {
                writable_roots: vec![cwd.clone()],
                ..Default::default()
            };
            let engine = Arc::new(PermissionEngine::new(Default::default()));
            (String::from("cli"), fs, engine, args.system.clone())
        }
    };

    let tools: Vec<Arc<dyn crate::tools::Tool>> = if args.tools_enabled {
        vec![
            Arc::new(Bash),
            Arc::new(Read),
            Arc::new(Write),
            Arc::new(Edit),
        ]
    } else {
        vec![]
    };

    // Session persistence: ~/.kestrel/sessions/... (KESTREL_HOME overrides).
    let session = SessionStore::create(
        &crate::config::kestrel_home(),
        &std::env::current_dir()?,
        &role_name,
        model.wire_id_str(),
        &model.id,
        Arc::new(UuidGen),
        Arc::new(SystemClock),
    )
    .ok()
    .map(Arc::new);

    let handle = AgentHandle::spawn(EngineConfig {
        provider: wire,
        model,
        auth: ResolvedAuth::key(args.api_key),
        transport: Arc::new(RetryTransport::new(Arc::new(HttpTransport::new()))),
        system,
        tools,
        max_rounds: 8,
        cwd: std::env::current_dir()?,
        role: role_name,
        idgen: Arc::new(UuidGen),
        sandbox: if args.tools_enabled {
            crate::sandbox::platform_sandbox()
        } else {
            Arc::new(Unsandboxed)
        },
        fs,
        engine,
        approve: args.approve,
        session,
    });
    let mut rx = handle.take_event_rx().expect("event rx available once");
    handle.turn(vec![MessageItem::user_text(args.prompt)]);

    let mut code = 0;
    loop {
        let ev: Event = tokio::select! {
            _ = tokio::time::sleep(args.timeout) => {
                handle.cancel_root();
                anyhow::bail!("timeout after {:?}", args.timeout);
            }
            ev = rx.recv() => match ev {
                Some(ev) => ev,
                None => break,
            },
        };
        match &ev.msg {
            EventMsg::TurnComplete { stop, .. } => {
                println!("{}", serde_json::to_string(&ev)?);
                if matches!(stop, StopReason::Aborted) {
                    code = 3;
                }
                break;
            }
            EventMsg::Fatal { .. } => {
                println!("{}", serde_json::to_string(&ev)?);
                code = 1;
                break;
            }
            _ => println!("{}", serde_json::to_string(&ev)?),
        }
    }
    handle.shutdown().await;
    Ok(code)
}
