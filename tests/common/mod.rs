//! Shared test helpers: faux engine spawn + event collection.
//! Items not used by every including test binary; dead_code is expected.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use kestrel::core::actor::{AgentHandle, EngineConfig};
use kestrel::core::events::{Event, EventMsg};
use kestrel::core::id::SeqGen;
use kestrel::permissions::PermissionEngine;
use kestrel::provider::faux::FauxWire;
use kestrel::provider::wire::{NullTransport, ResolvedAuth, StopReason};
use kestrel::sandbox::{FsPolicy, Unsandboxed};
use kestrel::tools::ApproveMode;

pub fn fixture(name: &str) -> std::path::PathBuf {
    [env!("CARGO_MANIFEST_DIR"), "tests", "fixtures", name]
        .iter()
        .collect()
}

pub struct EngineSpec {
    pub max_rounds: u32,
    pub tools: Vec<Arc<dyn kestrel::tools::Tool>>,
    pub fs: FsPolicy,
    pub approve: ApproveMode,
}

impl Default for EngineSpec {
    fn default() -> Self {
        Self {
            max_rounds: 8,
            tools: vec![],
            fs: FsPolicy::default(),
            approve: ApproveMode::HeadlessDeny,
        }
    }
}

pub fn spawn_engine(
    wire: Arc<FauxWire>,
    spec: EngineSpec,
) -> (AgentHandle, tokio::sync::mpsc::UnboundedReceiver<Event>) {
    let EngineSpec {
        max_rounds,
        tools,
        fs,
        approve,
    } = spec;
    let sandbox: Arc<dyn kestrel::sandbox::Sandbox> = Arc::new(Unsandboxed);
    let handle = AgentHandle::spawn(EngineConfig {
        provider: wire,
        model: kestrel::provider::wire::ModelMeta::minimal(
            "faux-model",
            kestrel::provider::wire::WireId::AnthropicMessages,
            "faux://unused",
        ),
        auth: ResolvedAuth::key("test"),
        transport: Arc::new(NullTransport),
        system: "test system".into(),
        tools,
        max_rounds,
        cwd: std::env::temp_dir(),
        role: "test".into(),
        idgen: Arc::new(SeqGen::new()),
        fs,
        sandbox,
        engine: Arc::new(PermissionEngine::new(Default::default())),
        approve,
        session: None,
    });
    let rx = handle.take_event_rx().expect("single consumer");
    (handle, rx)
}

/// Collect events until TurnComplete/Fatal; returns (events, stop).
pub async fn collect_until_complete(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
) -> (Vec<Event>, StopReason) {
    let mut out = Vec::new();
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("event stream timed out")
            .expect("event rx closed before TurnComplete");
        let stop = match &ev.msg {
            EventMsg::TurnComplete { stop, .. } => Some(stop.clone()),
            EventMsg::Fatal { .. } => Some(StopReason::error("fatal", false)),
            _ => None,
        };
        out.push(ev);
        if let Some(stop) = stop {
            return (out, stop);
        }
    }
}

/// Test-only tool that sleeps then returns; used for concurrency gates.
pub struct SleepTool {
    pub name: &'static str,
    pub ms: u64,
    pub sequential: bool,
}

#[async_trait::async_trait]
impl kestrel::tools::Tool for SleepTool {
    fn name(&self) -> &str {
        self.name
    }

    fn schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }

    fn mode(&self) -> kestrel::tools::ExecMode {
        if self.sequential {
            kestrel::tools::ExecMode::Sequential
        } else {
            kestrel::tools::ExecMode::Parallel
        }
    }

    async fn prepare(
        &self,
        _call: &kestrel::tools::ToolCall,
        _ctx: &kestrel::tools::ToolCtx,
    ) -> Result<kestrel::tools::Prepared, kestrel::tools::Reject> {
        Ok(kestrel::tools::Prepared(serde_json::json!({})))
    }

    async fn execute(
        &self,
        _prep: kestrel::tools::Prepared,
        _ctx: &kestrel::tools::ToolCtx,
        cancel: tokio_util::sync::CancellationToken,
        _update: tokio::sync::mpsc::UnboundedSender<kestrel::tools::ToolUpdate>,
    ) -> Result<kestrel::tools::ToolOutcome, kestrel::tools::ToolError> {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(kestrel::tools::ToolError::Cancelled),
            _ = tokio::time::sleep(Duration::from_millis(self.ms)) => {
                Ok(kestrel::tools::ToolOutcome::text(format!("slept {}", self.ms)))
            }
        }
    }
}
