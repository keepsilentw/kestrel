//! Provider layer: wire abstraction + implementations (faux/anthropic/openai-chat)
//! + partial-JSON (地基件 3), retry transport, provider registry.

pub mod anthropic;
pub mod config;
pub mod faux;
pub mod openai_chat;
pub mod partial_json;
pub mod retry;
pub mod wire;
