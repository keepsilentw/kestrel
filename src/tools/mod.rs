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

    /// 一个审批请求最多投递几次（首发 + 两次重发）。有界是刻意的：hub 的停车窗是按
    /// session 计数的（§9.21：每 session 32 帧、drop-oldest 并 warn），在不在线的
    /// 审批者面前猛发同一颗请求，会把别人的帧挤掉。
    pub const MAX_APPROVAL_SENDS: u32 = 3;

    /// 审批请求的重发节奏（design §10「批复的可靠投递」）：一个窗口里放得下
    /// `MAX_APPROVAL_SENDS` 次投递，所以取窗口的三分之一。
    ///
    /// 太密会坏事：hub 的停车窗是按 session 计的（§9.21：每 session 32 帧、
    /// drop-oldest 并 warn），一个不在线的审批者面前猛发同一颗请求，会把别的
    /// 帧挤出去。太疏也白搭：窗尾才发出的重发来不及在 deny 之前回来。
    /// 节奏属于这个窗口，所以它在拥有窗口的类型上，而不是在调用方各算一遍。
    pub fn approval_retry_period(&self) -> Duration {
        match self {
            ApproveMode::Bus { timeout } => {
                let third = *timeout / Self::MAX_APPROVAL_SENDS;
                if third < Duration::from_millis(50) {
                    Duration::from_millis(50)
                } else if third > Duration::from_secs(30) {
                    Duration::from_secs(30)
                } else {
                    third
                }
            }
            // headless 两种模式都不上总线，也就没有重发这回事
            ApproveMode::HeadlessDeny | ApproveMode::HeadlessAllow => Duration::from_secs(30),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 可靠投递的全部意义：**最后一次重发要早于 deny 的时刻**。否则那次重发只是
    /// 发给自己的安慰，窗口照样等满、批复照样丢。
    #[test]
    fn approval_retries_land_before_the_deny() {
        for secs in [5u64, 60, 120] {
            let period = ApproveMode::Bus {
                timeout: Duration::from_secs(secs),
            }
            .approval_retry_period();
            let last_send = period * (ApproveMode::MAX_APPROVAL_SENDS - 1);
            assert!(
                last_send < Duration::from_secs(secs),
                "{secs}s 窗口里第三次投递落在 {last_send:?}，已经过了 deny 的时刻"
            );
        }
    }

    /// 两端都有夹子：太密会挤占 hub 的停车窗，太疏则慢端点等不到第三次投递。
    /// 窗口小于 `3 × 50ms` 时下限会赢——那是「窗口本来就短到没法可靠投递」的情形，
    /// 语义仍然停在超时 deny，不会变成无限等待。
    #[test]
    fn approval_retry_period_is_clamped_at_both_ends() {
        assert_eq!(
            ApproveMode::Bus {
                timeout: Duration::from_millis(60)
            }
            .approval_retry_period(),
            Duration::from_millis(50)
        );
        assert_eq!(
            ApproveMode::Bus {
                timeout: Duration::from_secs(3600)
            }
            .approval_retry_period(),
            Duration::from_secs(30)
        );
        // demo 的 60s 窗口：20s 一次，三次投递都赶得上
        assert_eq!(
            ApproveMode::Bus {
                timeout: Duration::from_secs(60)
            }
            .approval_retry_period(),
            Duration::from_secs(20)
        );
        // headless 不上总线：给一个不会真被使用的缺省值
        assert_eq!(
            ApproveMode::HeadlessDeny.approval_retry_period(),
            Duration::from_secs(30)
        );
    }
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
