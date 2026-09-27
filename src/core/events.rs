//! Outbound event stream consumed by the UI/CLI/bus side.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::id::ReqId;
use crate::provider::wire::{StopReason, ToolContent, Usage};

/// One conversation item. Persisted projection lives in the session tree (M2);
/// this is the in-memory + LLM-bound form (pi AgentMessage/convertToLlm split).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageItem {
    UserText {
        text: String,
    },
    /// Bus bell rendered with explicit source tag (anti agent-to-agent injection).
    BusBell {
        sender: String,
        text: String,
    },
    AssistantText {
        text: String,
    },
    /// Opaque thinking cookie replayed verbatim on the next request (地基件 4).
    ThinkingOpaque {
        blob: Value,
    },
    AssistantToolUse {
        call_id: String,
        name: String,
        args: Value,
    },
    ToolResult {
        call_id: String,
        content: Vec<ToolContent>,
        is_error: bool,
    },
}

impl MessageItem {
    pub fn user_text(text: impl Into<String>) -> Self {
        MessageItem::UserText { text: text.into() }
    }

    /// LLM-bound projection (M4 renders BusBell with a source tag).
    pub fn to_context(&self) -> crate::provider::wire::ContextItem {
        use crate::provider::wire::ContextItem;
        match self {
            MessageItem::UserText { text } => ContextItem::UserText { text: text.clone() },
            // 注入面防线：bus 来源的 item 渲染为带来源标注的显式块，让角色能
            // 区分「用户指令」与「其他 worker 产出」(plan §2.8 / pi-study §3.5)。
            MessageItem::BusBell { sender, text } => ContextItem::UserText {
                text: format!("<bell from=\"{sender}\">{text}</bell>"),
            },
            MessageItem::AssistantText { text } => {
                ContextItem::AssistantText { text: text.clone() }
            }
            MessageItem::ThinkingOpaque { blob } => {
                ContextItem::ThinkingOpaque { blob: blob.clone() }
            }
            MessageItem::AssistantToolUse {
                call_id,
                name,
                args,
            } => ContextItem::AssistantToolUse {
                call_id: call_id.clone(),
                name: name.clone(),
                args: args.clone(),
            },
            MessageItem::ToolResult {
                call_id,
                content,
                is_error,
            } => ContextItem::ToolResult {
                call_id: call_id.clone(),
                content: content.clone(),
                is_error: *is_error,
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventMsg {
    TurnStarted {
        turn: u64,
    },
    TextDelta {
        text: String,
    },
    ThinkingDelta {
        text: String,
    },
    MessageDone {
        item: MessageItem,
    },
    ToolBegin {
        call_id: String,
        name: String,
        args: Value,
    },
    ToolUpdate {
        call_id: String,
        chunk: String,
    },
    ToolEnd {
        call_id: String,
        output: Vec<ToolContent>,
        is_error: bool,
        terminate: bool,
    },
    ApprovalRequest {
        req: ReqId,
        prompt: String,
        detail: Value,
    },
    Compacted {
        first_kept: String,
        tokens_before: u32,
    },
    ModelSwitched {
        provider: String,
        model: String,
    },
    TurnComplete {
        usage: Usage,
        stop: StopReason,
    },
    Fatal {
        error: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: ReqId,
    pub msg: EventMsg,
}

impl Event {
    pub fn new(id: impl Into<String>, msg: EventMsg) -> Self {
        Event { id: id.into(), msg }
    }
}
