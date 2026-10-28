//! Tool layer seed (M0): the Tool contract and registry. Concrete tools
//! (bash/read/write/edit) and the concurrency orchestrator land in M1;
//! the turn loop already dispatches through `run_tool_call` so the
//! reverse-approval path is exercised from day one.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::core::events::MessageItem;
use crate::provider::wire::ToolContent;

pub mod bash;
pub mod edit;
pub mod orchestrator;
pub mod read;
pub mod read_state;
pub mod write;
pub use orchestrator::dispatch_batch;

/// One parsed tool call arriving from the model.
#[derive(Clone, Debug)]
pub struct ToolCall {
    pub call_id: String,
    pub name: String,
    pub args: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecMode {
    /// May run concurrently with other parallel tools (read-lock gate).
    Parallel,
    /// Exclusive (write-lock gate); also forces the whole batch serial (M1).
    Sequential,
}

/// Validated, tool-owned preparation result (opaque payload).
#[derive(Clone, Debug)]
pub struct Prepared(pub Value);

#[derive(Debug)]
pub enum Reject {
    /// Argument-level problem: fed back to the model as an is_error result;
    /// the loop continues (codex 纪律 5).
    RespondToModel { message: String },
    /// Permission/sandbox refusal: hard block, also fed back but terminal-ish.
    Blocked { reason: String },
    /// Reverse request: orchestrator emits ApprovalRequest and awaits the Op.
    NeedsApproval { prompt: String, detail: Value },
}

#[derive(Debug, Clone)]
pub struct ToolUpdate {
    pub call_id: String,
    pub chunk: String,
}

#[derive(Debug, Clone)]
pub struct ToolOutcome {
    pub content: Vec<ToolContent>,
    pub is_error: bool,
    pub terminate: bool,
}

impl ToolOutcome {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            content: vec![ToolContent::Text { text: text.into() }],
            is_error: false,
            terminate: false,
        }
    }

    pub fn error(text: impl Into<String>) -> Self {
        Self {
            content: vec![ToolContent::Text { text: text.into() }],
            is_error: true,
            terminate: false,
        }
    }

    /// Concatenated text content (test/assert convenience).
    pub fn content_text(&self) -> String {
        self.content
            .iter()
            .map(|c| match c {
                ToolContent::Text { text } => text.clone(),
                ToolContent::Image { .. } => String::new(),
            })
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error("sandbox denied: {0}")]
    SandboxDenied(String),
    #[error("io: {0}")]
    Io(String),
    #[error("cancelled")]
    Cancelled,
}

impl ToolError {
    pub fn to_outcome(&self) -> ToolOutcome {
        match self {
            ToolError::Cancelled => ToolOutcome {
                content: vec![ToolContent::Text {
                    text: "aborted by user".into(),
                }],
                is_error: true,
                terminate: false,
            },
            other => ToolOutcome::error(other.to_string()),
        }
    }
}

/// How `NeedsApproval` resolves. The mode is decided by the priority chain
/// runtime(`--approve`) > role(`[permissions].approve`) > deny (fail closed).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApproveMode {
    /// Reverse request over the bus to a peer (SA or a human bridge): the
    /// worker emits `EventMsg::ApprovalRequest`, the CLI layer carries it to
    /// the approver, and the answer comes back as `Op::Approval`. `timeout`
    /// bounds the wait — a silent approver DENIES, it never blocks the turn
    /// forever and never widens by default (design §10, 一期遗留项).
    Bus { timeout: Duration },
    /// Headless: deny immediately (CLI --approve none).
    HeadlessDeny,
    /// Headless: allow once (CLI --approve all).
    HeadlessAllow,
}

impl ApproveMode {
    /// Fallback wait for a bus approver when the role stays silent.
    pub const DEFAULT_BUS_TIMEOUT: Duration = Duration::from_secs(120);
}

/// Per-call context: role profile + sandbox + permission engine.
#[derive(Clone)]
pub struct ToolCtx {
    pub cwd: PathBuf,
    pub role: String,
    pub fs: crate::sandbox::FsPolicy,
    pub sandbox: std::sync::Arc<dyn crate::sandbox::Sandbox>,
    pub engine: std::sync::Arc<crate::permissions::PermissionEngine>,
    pub approve: ApproveMode,
    /// Set by the orchestrator on sandbox-denial escalation retries.
    pub unsandboxed: bool,
    /// True on the post-approval re-prepare pass: policy-Ask may then pass.
    pub reprepare_approved: bool,
    /// Session-scoped "what has the model actually seen" ledger (read-before-edit).
    pub reads: Arc<read_state::ReadTracker>,
}

impl std::fmt::Debug for ToolCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolCtx")
            .field("cwd", &self.cwd)
            .field("role", &self.role)
            .field("unsandboxed", &self.unsandboxed)
            .finish()
    }
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn schema(&self) -> Value;
    fn mode(&self) -> ExecMode {
        ExecMode::Parallel
    }

    /// Serial phase: validate args, run permission hooks. Never touches the
    /// filesystem beyond reads needed for validation.
    async fn prepare(&self, call: &ToolCall, ctx: &ToolCtx) -> Result<Prepared, Reject>;

    /// Concurrent phase: every await must cross the cancel token; partial
    /// results stream through `update`.
    async fn execute(
        &self,
        prep: Prepared,
        ctx: &ToolCtx,
        cancel: CancellationToken,
        update: mpsc::UnboundedSender<ToolUpdate>,
    ) -> Result<ToolOutcome, ToolError>;
}

/// Registry: linear lookup is fine at this scale (pi uses a plain array).
#[derive(Default, Clone)]
pub struct ToolRegistry {
    tools: Vec<Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new(tools: Vec<Arc<dyn Tool>>) -> Self {
        Self { tools }
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.iter().find(|t| t.name() == name).cloned()
    }

    pub fn specs(&self) -> Vec<crate::provider::wire::ToolSpec> {
        self.tools
            .iter()
            .map(|t| crate::provider::wire::ToolSpec {
                name: t.name().to_string(),
                description: String::new(),
                parameters: t.schema(),
            })
            .collect()
    }
}

/// Default dispatch result for a call the registry doesn't know: fed back to
/// the model as an is_error result so the loop keeps going (codex registry.rs:516).
pub fn unknown_tool_outcome(name: &str) -> ToolOutcome {
    ToolOutcome::error(format!("unknown tool: {name}"))
}

/// History for a finished tool batch: every `tool_use` first, then every
/// `tool_result`.
///
/// A parallel batch must stay in those two groups rather than interleaving
/// call/result pairs: Anthropic wants all `tool_use` blocks of one assistant
/// message with their results in the following user message, and on
/// thinking-enabled endpoints (DeepSeek) an assistant message holding a
/// `tool_use` without its thinking block is rejected outright with
/// "`content[].thinking` in the thinking mode must be passed back to the API"
/// (2026-09-17 demo).
pub fn outcomes_to_history(pairs: &[(ToolCall, ToolOutcome)]) -> Vec<MessageItem> {
    let mut items: Vec<MessageItem> = pairs
        .iter()
        .map(|(call, _)| MessageItem::AssistantToolUse {
            call_id: call.call_id.clone(),
            name: call.name.clone(),
            args: call.args.clone(),
        })
        .collect();
    items.extend(pairs.iter().map(|(call, outcome)| MessageItem::ToolResult {
        call_id: call.call_id.clone(),
        content: outcome.content.clone(),
        is_error: outcome.is_error,
    }));
    items
}
