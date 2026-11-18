//! Provider layer: wire abstraction + implementations (faux/anthropic/openai-chat)
//! + partial-JSON (地基件 3), retry transport, provider registry.

pub mod anthropic;
pub mod config;
pub mod faux;
pub mod openai_chat;
pub mod openai_responses;
pub mod partial_json;
pub mod retry;
pub mod wire;

use std::sync::Arc;

use wire::{ModelMeta, WireFormat, WireId};

/// The wire implementation a resolved model points at. One mapping, shared by
/// `-p`, resident workers and the SA planner — a second copy would drift the
/// moment a new wire is added.
pub fn wire_for(model: &ModelMeta) -> Arc<dyn WireFormat> {
    match model.wire {
        WireId::AnthropicMessages => Arc::new(anthropic::AnthropicWire),
        WireId::OpenAiCompletions => Arc::new(openai_chat::OpenAiChatWire),
        WireId::OpenAiResponses => Arc::new(openai_responses::OpenAiResponsesWire),
    }
}
