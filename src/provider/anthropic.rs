//! Minimal Anthropic Messages wire (M0 scope): text + tool_use + tool_result +
//! thinking-blob echo + stop_reason mapping over SSE. Honesty upgrades
//! (compat flags, full thinking replay, retry policy) land in M3.

use futures::stream::{BoxStream, StreamExt};
use serde_json::{Value, json};

use super::wire::{
    ByteStream, ContextItem, ModelMeta, RequestSpec, ResolvedAuth, StopReason, ThinkingBudget,
    ToolContent, ToolSpec, TurnContext, Usage, WireEvent, WireFormat, WireId, sse_events,
};

pub struct AnthropicWire;

impl AnthropicWire {
    pub const VERSION: &'static str = "2023-06-01";

    fn messages_url(base_url: &str) -> String {
        let base = base_url.trim_end_matches('/');
        format!("{base}/v1/messages")
    }

    fn render_content_blocks(items: &[ContextItem]) -> Vec<Value> {
        // Anthropic pairs tool_use (assistant) with tool_result (user). We walk
        // the context and emit alternating role messages.
        let mut messages: Vec<Value> = Vec::new();
        let push = |role: &str, block: Value, messages: &mut Vec<Value>| match messages.last_mut() {
            Some(last) if last["role"] == role => {
                last["content"].as_array_mut().unwrap().push(block);
            }
            _ => messages.push(json!({ "role": role, "content": [block] })),
        };
        for item in items {
            match item {
                ContextItem::UserText { text } => {
                    push(
                        "user",
                        json!({ "type": "text", "text": text }),
                        &mut messages,
                    );
                }
                ContextItem::AssistantText { text } => {
                    push(
                        "assistant",
                        json!({ "type": "text", "text": text }),
                        &mut messages,
                    );
                }
                ContextItem::ThinkingOpaque { blob } => {
                    // Echo-only cookie: pass the block verbatim.
                    push("assistant", blob.clone(), &mut messages);
                }
                ContextItem::AssistantToolUse {
                    call_id,
                    name,
                    args,
                } => {
                    push(
                        "assistant",
                        json!({ "type": "tool_use", "id": call_id, "name": name, "input": args }),
                        &mut messages,
                    );
                }
                ContextItem::ToolResult {
                    call_id,
                    content,
                    is_error,
                } => {
                    let content_json: Vec<Value> = content
                        .iter()
                        .map(|c| match c {
                            ToolContent::Text { text } => json!({ "type": "text", "text": text }),
                            ToolContent::Image { media_type, data } => json!({
                                "type": "image",
                                "source": { "type": "base64", "media_type": media_type, "data": data }
                            }),
                        })
                        .collect();
                    push(
                        "user",
                        json!({
                            "type": "tool_result",
                            "tool_use_id": call_id,
                            "content": content_json,
                            "is_error": is_error,
                        }),
                        &mut messages,
                    );
                }
            }
        }
        messages
    }

    fn map_stop_reason(reason: Option<&str>) -> StopReason {
        match reason {
            Some("tool_use") => StopReason::ToolUse,
            Some("max_tokens") => StopReason::Length,
            Some("end_turn") | Some("stop_sequence") | None => StopReason::Stop,
            other => StopReason::error(format!("unknown stop_reason: {other:?}"), false),
        }
    }
}

impl WireFormat for AnthropicWire {
    fn id(&self) -> WireId {
        WireId::AnthropicMessages
    }

    fn render_request(
        &self,
        model: &ModelMeta,
        ctx: &TurnContext,
        max_out: u32,
        thinking: Option<ThinkingBudget>,
        auth: &ResolvedAuth,
    ) -> RequestSpec {
        let messages = Self::render_content_blocks(&ctx.items);
        let mut body = json!({
            "model": model.id,
            "max_tokens": max_out,
            "stream": true,
            "messages": messages,
        });
        if !ctx.system.is_empty() {
            body["system"] = json!(ctx.system);
        }
        if !ctx.tools.is_empty() {
            let tools: Vec<Value> = ctx
                .tools
                .iter()
                .map(|t: &ToolSpec| {
                    json!({ "name": t.name, "description": t.description, "input_schema": t.parameters })
                })
                .collect();
            body["tools"] = json!(tools);
        }
        if let Some(t) = thinking {
            body["thinking"] = json!({ "type": "enabled", "budget_tokens": t.budget_tokens });
        }
        RequestSpec {
            url: Self::messages_url(&model.base_url),
            headers: vec![
                ("x-api-key".into(), auth.api_key.clone()),
                // Anthropic-compat gateways vary: official API uses x-api-key,
                // Bearer-style compat endpoints (BigModel/DeepSeek) ignore it
                // and read Authorization — send both (pi anthropic.ts:18-40).
                ("authorization".into(), format!("Bearer {}", auth.api_key)),
                ("anthropic-version".into(), Self::VERSION.into()),
            ],
            body,
        }
    }

    fn parse_stream(&self, _model: &ModelMeta, bytes: ByteStream) -> BoxStream<'static, WireEvent> {
        // Stateful scan: only tool blocks (opened via content_block_start) emit
        // ToolCallEnd on content_block_stop; text blocks carry no event.
        let opened = std::collections::HashSet::<u32>::new();
        let inner = sse_events(bytes)
            .scan(opened, |st, res| {
                let mut events: Vec<WireEvent> = match res {
                    Err(e) => {
                        vec![WireEvent::Done {
                            stop: StopReason::error(format!("sse: {e}"), true),
                        }]
                    }
                    Ok(ev) => {
                        // Some Anthropic-compatible gateways omit `event:` lines
                        // (SSE default name "message"); fall back to data.type.
                        let name = if ev.event.is_empty() || ev.event == "message" {
                            serde_json::from_str::<Value>(&ev.data)
                                .map(|v| v["type"].as_str().unwrap_or("message").to_string())
                                .unwrap_or_else(|_| "message".to_string())
                        } else {
                            ev.event.clone()
                        };
                        match handle_sse_event(&name, &ev.data) {
                            Ok(v) => v,
                            Err(msg) => {
                                vec![WireEvent::Done {
                                    stop: StopReason::error(msg, false),
                                }]
                            }
                        }
                    }
                };
                // content_block_stop fires for every block type; only keep
                // ToolCallEnd for blocks that actually opened a tool call.
                events.retain(|ev| match ev {
                    WireEvent::ToolCallEnd { index } => st.contains(index),
                    _ => true,
                });
                for ev in &events {
                    match ev {
                        WireEvent::ToolCallBegin { index, .. } => {
                            st.insert(*index);
                        }
                        WireEvent::ToolCallEnd { index } => {
                            st.remove(index);
                        }
                        _ => {}
                    }
                }
                async move { Some(events) }
            })
            .flat_map(futures::stream::iter);
        Box::pin(super::wire::ensure_done(Box::pin(inner)))
    }
}

/// One SSE event -> zero or more normalized events.
fn handle_sse_event(name: &str, data: &str) -> Result<Vec<WireEvent>, String> {
    let v: Value =
        serde_json::from_str(data).map_err(|e| format!("bad sse json for {name}: {e}"))?;
    let out = match name {
        "message_start" => {
            let input = v["message"]["usage"]["input_tokens"].as_u64().unwrap_or(0);
            let mut evs = vec![WireEvent::Start];
            if input > 0 {
                evs.push(WireEvent::Usage(Usage {
                    input_tokens: input,
                    ..Default::default()
                }));
            }
            evs
        }
        "content_block_start" => {
            let idx = v["index"].as_u64().unwrap_or(0) as u32;
            let block = &v["content_block"];
            match block["type"].as_str() {
                Some("tool_use") => vec![WireEvent::ToolCallBegin {
                    index: idx,
                    call_id: block["id"].as_str().unwrap_or_default().to_string(),
                    name: block["name"].as_str().unwrap_or_default().to_string(),
                }],
                _ => vec![],
            }
        }
        "content_block_delta" => {
            let idx = v["index"].as_u64().unwrap_or(0) as u32;
            let delta = &v["delta"];
            match delta["type"].as_str() {
                Some("text_delta") => vec![WireEvent::TextDelta {
                    text: delta["text"].as_str().unwrap_or_default().to_string(),
                }],
                Some("thinking_delta") => vec![WireEvent::ThinkingDelta {
                    text: delta["thinking"].as_str().unwrap_or_default().to_string(),
                }],
                Some("signature_delta") => vec![WireEvent::ThinkingSigned {
                    blob: json!({ "type": "signature_delta", "signature": delta["signature"] }),
                }],
                Some("input_json_delta") => vec![WireEvent::ToolCallArgsDelta {
                    index: idx,
                    delta: delta["partial_json"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                }],
                _ => vec![],
            }
        }
        "content_block_stop" => {
            let idx = v["index"].as_u64().unwrap_or(0) as u32;
            // Only tool blocks are tracked; text block stops carry no event.
            // The turn loop tracks which indices opened a tool call.
            vec![WireEvent::ToolCallEnd { index: idx }]
        }
        "message_delta" => {
            let stop = AnthropicWire::map_stop_reason(v["delta"]["stop_reason"].as_str());
            let out_tokens = v["usage"]["output_tokens"].as_u64().unwrap_or(0);
            let mut evs = Vec::new();
            if out_tokens > 0 {
                evs.push(WireEvent::Usage(Usage {
                    output_tokens: out_tokens,
                    ..Default::default()
                }));
            }
            evs.push(WireEvent::Done { stop });
            evs
        }
        "message_stop" => vec![],
        "error" => {
            let msg = v["error"]["message"].as_str().unwrap_or("unknown error");
            vec![WireEvent::Done {
                stop: StopReason::error(msg.to_string(), false),
            }]
        }
        _ => vec![],
    };
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A parallel tool batch must render as ONE assistant message carrying every
    /// tool_use (with the thinking block in front), then ONE user message with
    /// the tool_results. Interleaving per call is what DeepSeek's thinking mode
    /// rejects (2026-09-17 demo: "content[].thinking ... must be passed back").
    #[test]
    fn parallel_tool_batch_renders_grouped() {
        let wire = AnthropicWire;
        let model = ModelMeta::minimal("m", WireId::AnthropicMessages, "https://x");
        let ctx = TurnContext {
            system: String::new(),
            items: vec![
                ContextItem::UserText {
                    text: "run two things".into(),
                },
                ContextItem::ThinkingOpaque {
                    blob: json!({"type":"thinking","thinking":"plan","signature":"sig-1"}),
                },
                ContextItem::AssistantToolUse {
                    call_id: "t1".into(),
                    name: "bash".into(),
                    args: json!({"command": "echo one"}),
                },
                ContextItem::AssistantToolUse {
                    call_id: "t2".into(),
                    name: "bash".into(),
                    args: json!({"command": "echo two"}),
                },
                ContextItem::ToolResult {
                    call_id: "t1".into(),
                    content: vec![ToolContent::Text { text: "one".into() }],
                    is_error: false,
                },
                ContextItem::ToolResult {
                    call_id: "t2".into(),
                    content: vec![ToolContent::Text { text: "two".into() }],
                    is_error: false,
                },
            ],
            tools: vec![],
        };
        let spec = wire.render_request(&model, &ctx, 64, None, &ResolvedAuth::key("k"));
        let msgs = spec.body["messages"].as_array().unwrap();
        assert_eq!(
            msgs.len(),
            3,
            "user + one assistant batch + one user: {msgs:?}"
        );
        let blocks = msgs[1]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 3, "thinking + both tool_use in one message");
        assert_eq!(blocks[0]["type"], "thinking");
        assert_eq!(blocks.iter().filter(|b| b["type"] == "tool_use").count(), 2);
        let results = msgs[2]["content"].as_array().unwrap();
        assert_eq!(results.len(), 2, "both tool_results in one message");
        assert!(results.iter().all(|b| b["type"] == "tool_result"));
    }

    #[test]
    fn render_request_golden_shape() {
        let wire = AnthropicWire;
        let model = ModelMeta::minimal(
            "test-model",
            WireId::AnthropicMessages,
            "https://api.anthropic.com",
        );
        let ctx = TurnContext {
            system: "be terse".into(),
            items: vec![
                ContextItem::UserText { text: "hi".into() },
                ContextItem::AssistantToolUse {
                    call_id: "call_1".into(),
                    name: "noop".into(),
                    args: json!({}),
                },
                ContextItem::ToolResult {
                    call_id: "call_1".into(),
                    content: vec![ToolContent::Text {
                        text: "done".into(),
                    }],
                    is_error: false,
                },
            ],
            tools: vec![ToolSpec {
                name: "noop".into(),
                description: "d".into(),
                parameters: json!({"type": "object"}),
            }],
        };
        let spec = wire.render_request(&model, &ctx, 1024, None, &ResolvedAuth::key("sk-test"));
        assert_eq!(spec.url, "https://api.anthropic.com/v1/messages");
        assert!(
            spec.headers
                .iter()
                .any(|(k, v)| k == "x-api-key" && v == "sk-test")
        );
        assert_eq!(spec.body["model"], "test-model");
        assert_eq!(spec.body["max_tokens"], 1024);
        assert_eq!(spec.body["system"], "be terse");
        let msgs = spec.body["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["role"], "user");
        assert_eq!(msgs[1]["role"], "assistant");
        assert_eq!(msgs[1]["content"][0]["type"], "tool_use");
        assert_eq!(msgs[2]["role"], "user");
        assert_eq!(msgs[2]["content"][0]["type"], "tool_result");
        assert_eq!(spec.body["tools"][0]["input_schema"]["type"], "object");
    }

    #[tokio::test]
    async fn parse_stream_basic_sse() {
        let sse = "\
event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":12}}}\n\
\n\
event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"he\"}}\n\
\n\
event: message_delta\n\
data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\
\n";
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let wire = AnthropicWire;
        let (events, stop) = super::super::wire::collect_stream(wire.parse_stream(
            &ModelMeta::minimal("m", WireId::AnthropicMessages, "https://api.anthropic.com"),
            bytes,
        ))
        .await;
        assert!(matches!(stop, StopReason::Stop));
        assert!(events.iter().any(|e| matches!(e, WireEvent::Start)));
        assert!(
            events
                .iter()
                .any(|e| matches!(e, WireEvent::TextDelta { text } if text == "he"))
        );
        // usage arrives as two partial events (input on message_start, output on
        // message_delta); the turn loop accumulates them.
        assert!(
            events
                .iter()
                .any(|e| matches!(e, WireEvent::Usage(u) if u.input_tokens == 12))
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, WireEvent::Usage(u) if u.output_tokens == 3))
        );
    }

    #[test]
    fn golden_body_echoes_thinking_signature() {
        // 地基件 4: ThinkingOpaque blob must be echoed VERBATIM into the
        // assistant content of the next request.
        let wire = AnthropicWire;
        let model = ModelMeta::minimal("m", WireId::AnthropicMessages, "https://x");
        let blob = serde_json::json!({
            "type": "thinking",
            "thinking": "step one, then two",
            "signature": "sig-abc123"
        });
        let ctx = TurnContext {
            system: String::new(),
            items: vec![
                ContextItem::ThinkingOpaque { blob: blob.clone() },
                ContextItem::UserText {
                    text: "continue".into(),
                },
            ],
            tools: vec![],
        };
        let spec = wire.render_request(&model, &ctx, 512, None, &ResolvedAuth::key("k"));
        let msgs = spec.body["messages"].as_array().unwrap();
        assert_eq!(msgs[0]["role"], "assistant");
        assert_eq!(
            msgs[0]["content"][0], blob,
            "signature blob echoed verbatim"
        );
        assert_eq!(msgs[1]["role"], "user");
    }

    #[tokio::test]
    async fn sse_without_event_lines_still_parses() {
        // gateway tolerance: data-only SSE (no "event:" lines)
        let sse = concat!(
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3}}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"z\"}}\n\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
        );
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let wire = AnthropicWire;
        let model = ModelMeta::minimal("m", WireId::AnthropicMessages, "https://x");
        let (evs, stop) =
            super::super::wire::collect_stream(wire.parse_stream(&model, bytes)).await;
        assert!(
            matches!(stop, StopReason::Stop),
            "stop was {stop:?}, events {evs:?}"
        );
        assert!(
            evs.iter()
                .any(|e| matches!(e, WireEvent::TextDelta { text } if text == "z"))
        );
    }

    #[tokio::test]
    async fn parse_stream_error_event_is_terminal() {
        let sse = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded\",\"message\":\"slow down\"}}\n\n";
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let wire = AnthropicWire;
        let (_, stop) = super::super::wire::collect_stream(wire.parse_stream(
            &ModelMeta::minimal("m", WireId::AnthropicMessages, "https://api.anthropic.com"),
            bytes,
        ))
        .await;
        match stop {
            StopReason::Error {
                message, retryable, ..
            } => {
                assert!(message.contains("slow down"));
                assert!(!retryable);
            }
            other => panic!("expected error stop, got {other:?}"),
        }
    }
}
