//! OpenAI Chat-Completions wire (M3): the second of two v1 wires.
//! Delta reassembly tolerates missing/late tool-call ids (pi
//! openai-completions.ts:490-547); compat flags switch max_tokens field name,
//! unknown finish_reason tolerance, and stream_include_usage.

use futures::stream::{BoxStream, StreamExt};
use serde_json::{Value, json};

use super::wire::{
    ByteStream, CompatFlags, ContextItem, MaxTokensField, ModelMeta, RequestSpec, ResolvedAuth,
    StopReason, ToolContent, TurnContext, Usage, WireEvent, WireFormat, WireId, sse_events,
};

pub struct OpenAiChatWire;

fn chat_url(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    format!("{base}/v1/chat/completions")
}

impl OpenAiChatWire {
    fn render_messages(ctx: &TurnContext) -> Vec<Value> {
        let mut out = Vec::new();
        if !ctx.system.is_empty() {
            out.push(json!({ "role": "system", "content": ctx.system }));
        }
        for item in &ctx.items {
            match item {
                ContextItem::UserText { text } => {
                    out.push(json!({ "role": "user", "content": text }));
                }
                ContextItem::AssistantText { text } => {
                    out.push(json!({ "role": "assistant", "content": text }));
                }
                // openai-chat has no replayable thinking blob: DeepSeek strips
                // reasoning server-side; do NOT resend (pi "Armin is wrong" 原则:
                // 不透明 blob 只回显——而这条 wire 的回显是"不发").
                ContextItem::ThinkingOpaque { .. } => {}
                ContextItem::AssistantToolUse {
                    call_id,
                    name,
                    args,
                } => {
                    let call = json!({
                        "id": call_id, "type": "function",
                        "function": { "name": name, "arguments": args.to_string() }
                    });
                    // A parallel batch is ONE assistant message carrying every
                    // tool_call, followed by the tool results it asked for.
                    match out.last_mut() {
                        Some(last)
                            if last["role"] == "assistant" && last["tool_calls"].is_array() =>
                        {
                            last["tool_calls"].as_array_mut().unwrap().push(call);
                        }
                        _ => out.push(json!({
                            "role": "assistant", "content": null, "tool_calls": [call]
                        })),
                    }
                }
                ContextItem::ToolResult {
                    call_id,
                    content,
                    is_error,
                } => {
                    let text: String = content
                        .iter()
                        .map(|c| match c {
                            ToolContent::Text { text } => text.clone(),
                            ToolContent::Image { .. } => "<image unsupported on this wire>".into(),
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    let mut msg =
                        json!({ "role": "tool", "tool_call_id": call_id, "content": text });
                    if *is_error {
                        msg["is_error"] = json!(true); // tolerated extra by most gateways
                    }
                    out.push(msg);
                }
            }
        }
        out
    }

    fn map_finish(reason: Option<&str>, compat: &CompatFlags) -> StopReason {
        match reason {
            Some("tool_calls") | Some("function_call") => StopReason::ToolUse,
            Some("stop") => StopReason::Stop,
            Some("length") => StopReason::Length,
            None => StopReason::Stop, // some gateways omit finish_reason
            other => {
                if compat.tolerate_unknown_finish == Some(true) {
                    StopReason::Stop
                } else {
                    StopReason::error(format!("unknown finish_reason: {other:?}"), false)
                }
            }
        }
    }
}

impl WireFormat for OpenAiChatWire {
    fn id(&self) -> WireId {
        WireId::OpenAiCompletions
    }

    fn render_request(
        &self,
        model: &ModelMeta,
        ctx: &TurnContext,
        max_out: u32,
        _thinking: Option<super::wire::ThinkingBudget>,
        auth: &ResolvedAuth,
    ) -> RequestSpec {
        let mut body = json!({
            "model": model.id,
            "messages": Self::render_messages(ctx),
            "stream": true,
        });
        let field = match model.compat.max_tokens_field.unwrap_or_default() {
            MaxTokensField::MaxTokens => "max_tokens",
            MaxTokensField::MaxCompletionTokens => "max_completion_tokens",
        };
        body[field] = json!(max_out);
        if model.compat.stream_include_usage == Some(true) {
            body["stream_options"] = json!({ "include_usage": true });
        }
        if !ctx.tools.is_empty() {
            let tools: Vec<Value> = ctx
                .tools
                .iter()
                .map(|t| {
                    json!({
                        "type": "function",
                        "function": { "name": t.name, "description": t.description, "parameters": t.parameters }
                    })
                })
                .collect();
            body["tools"] = json!(tools);
        }
        RequestSpec {
            url: chat_url(&model.base_url),
            headers: vec![("authorization".into(), format!("Bearer {}", auth.api_key))],
            body,
        }
    }

    fn parse_stream(&self, model: &ModelMeta, bytes: ByteStream) -> BoxStream<'static, WireEvent> {
        // Reassembly state: index -> (call_id, name, args); missing index
        // defaults to 0 and the id may arrive late on the same index.
        //
        // `pending_stop` holds the vendor's `finish_reason` until the stream
        // terminator. Terminating at `finish_reason` would drop whatever the
        // gateway sends afterwards — Bailian (Qwen) puts the usage chunk *after*
        // it, which silently zeroed the usage ledger (2026-09-17 smoke).
        struct Reasm {
            open: std::collections::HashMap<u32, (String, String, String)>,
            started: bool,
            pending_stop: Option<StopReason>,
            done: bool,
            compat: CompatFlags,
        }

        impl Reasm {
            /// Tool calls are complete by the time `finish_reason` arrives: emit
            /// begin/args/end triples in wire index order. Drains the map, so
            /// the terminal flush below is a no-op when both run.
            fn flush_calls(&mut self) -> Vec<WireEvent> {
                let mut slots: Vec<(u32, (String, String, String))> = self.open.drain().collect();
                slots.sort_by_key(|(i, _)| *i);
                let mut evs = Vec::new();
                for (index, (call_id, name, args)) in slots {
                    evs.push(WireEvent::ToolCallBegin {
                        index,
                        call_id,
                        name,
                    });
                    evs.push(WireEvent::ToolCallArgsDelta { index, delta: args });
                    evs.push(WireEvent::ToolCallEnd { index });
                }
                evs
            }

            /// Terminal event batch. Without a `finish_reason`, a clean `[DONE]`
            /// still means the model finished, while a bare EOF means the server
            /// cut the stream short.
            fn terminate(&mut self, clean_end: bool) -> Vec<WireEvent> {
                let mut evs = self.flush_calls();
                let stop = self.pending_stop.take().unwrap_or(if clean_end {
                    StopReason::Stop
                } else {
                    StopReason::Aborted
                });
                evs.push(WireEvent::Done { stop });
                evs
            }
        }

        let state = Reasm {
            open: std::collections::HashMap::new(),
            started: false,
            pending_stop: None,
            done: false,
            compat: model.compat.clone(),
        };
        let upstream = Box::pin(sse_events(bytes));
        let inner = futures::stream::unfold((upstream, state), move |(mut s, mut st)| async move {
            loop {
                // The terminator was already handed out: exactly one `Done`.
                if st.done {
                    return None;
                }
                match s.next().await {
                    None => {
                        st.done = true;
                        return Some((st.terminate(false), (s, st)));
                    }
                    Some(Err(e)) => {
                        st.done = true;
                        return Some((
                            vec![WireEvent::Done {
                                stop: StopReason::error(format!("sse: {e}"), true),
                            }],
                            (s, st),
                        ));
                    }
                    Some(Ok(ev)) => {
                        let data = ev.data.trim();
                        if data == "[DONE]" {
                            st.done = true;
                            return Some((st.terminate(true), (s, st)));
                        }
                        let v = match serde_json::from_str::<Value>(data) {
                            Ok(v) => v,
                            Err(e) => {
                                st.done = true;
                                return Some((
                                    vec![WireEvent::Done {
                                        stop: StopReason::error(
                                            format!("bad sse json: {e}"),
                                            false,
                                        ),
                                    }],
                                    (s, st),
                                ));
                            }
                        };
                        let mut evs: Vec<WireEvent> = Vec::new();
                        if !st.started {
                            evs.push(WireEvent::Start);
                            st.started = true;
                        }
                        if let Some(usage) = v.get("usage").filter(|u| u.is_object()) {
                            evs.push(WireEvent::Usage(Usage {
                                input_tokens: usage["prompt_tokens"].as_u64().unwrap_or(0),
                                output_tokens: usage["completion_tokens"].as_u64().unwrap_or(0),
                                cache_read_tokens: usage["prompt_tokens_details"]["cached_tokens"]
                                    .as_u64()
                                    .unwrap_or(0),
                                ..Default::default()
                            }));
                        }
                        if let Some(choice) = v["choices"].get(0) {
                            let delta = &choice["delta"];
                            if let Some(t) = delta["reasoning_content"]
                                .as_str()
                                .filter(|t| !t.is_empty())
                            {
                                evs.push(WireEvent::ThinkingDelta { text: t.into() });
                            }
                            if let Some(t) = delta["content"].as_str().filter(|t| !t.is_empty()) {
                                evs.push(WireEvent::TextDelta { text: t.into() });
                            }
                            if let Some(tcs) = delta["tool_calls"].as_array() {
                                for tc in tcs {
                                    let idx = tc["index"].as_u64().unwrap_or(0) as u32;
                                    let id = tc["id"].as_str().unwrap_or("");
                                    let name = tc["function"]["name"].as_str().unwrap_or("");
                                    let entry = st.open.entry(idx).or_insert_with(|| {
                                        (String::new(), String::new(), String::new())
                                    });
                                    if !id.is_empty() && entry.0.is_empty() {
                                        entry.0 = id.to_string();
                                    }
                                    if !name.is_empty() && entry.1.is_empty() {
                                        entry.1 = name.to_string();
                                    }
                                    if let Some(d) = tc["function"]["arguments"].as_str() {
                                        entry.2.push_str(d);
                                    }
                                }
                            }
                            if let Some(fin) = choice["finish_reason"].as_str() {
                                let stop = OpenAiChatWire::map_finish(Some(fin), &st.compat);
                                st.pending_stop = Some(stop);
                                evs.extend(st.flush_calls());
                            }
                        }
                        if evs.is_empty() {
                            continue;
                        }
                        return Some((evs, (s, st)));
                    }
                }
            }
        })
        .flat_map(futures::stream::iter);
        Box::pin(super::wire::ensure_done(Box::pin(inner)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    async fn collect(bytes: ByteStream) -> (Vec<WireEvent>, Option<StopReason>) {
        let wire = OpenAiChatWire;
        let model = ModelMeta::minimal("m", WireId::OpenAiCompletions, "http://x");
        async move {
            let mut stream = wire.parse_stream(&model, bytes);
            let mut evs = Vec::new();
            let mut done = None;
            while let Some(e) = stream.next().await {
                if let WireEvent::Done { stop } = e.clone() {
                    done = Some(stop);
                }
                evs.push(e);
            }
            (evs, done)
        }
        .await
    }

    #[tokio::test]
    async fn text_and_usage_stream() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n\
                   data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n\
                   data: [DONE]\n\n";
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let (evs, done) = collect(bytes).await;
        assert!(matches!(done, Some(StopReason::Stop)));
        assert!(
            evs.iter()
                .any(|e| matches!(e, WireEvent::TextDelta { text } if text == "he"))
        );
        assert!(evs.iter().any(
            |e| matches!(e, WireEvent::Usage(u) if u.input_tokens == 7 && u.output_tokens == 3)
        ));
    }

    /// Regression (2026-09-17 Qwen smoke): Bailian reports usage in a
    /// `choices: []` line *after* `finish_reason`. Terminating at
    /// `finish_reason` dropped it and every turn reported 0 tokens.
    #[tokio::test]
    async fn usage_after_finish_reason_is_kept() {
        let sse = concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}],\"usage\":null}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":null}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":63,\"completion_tokens\":37}}\n\n",
            "data: [DONE]\n\n",
        );
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let (evs, done) = collect(bytes).await;
        assert!(matches!(done, Some(StopReason::Stop)), "done {done:?}");
        assert!(
            evs.iter().any(
                |e| matches!(e, WireEvent::Usage(u) if u.input_tokens == 63 && u.output_tokens == 37)
            ),
            "usage sent after finish_reason must survive: {evs:?}"
        );
        assert!(
            matches!(evs.last(), Some(WireEvent::Done { .. })),
            "Done must remain the last event: {evs:?}"
        );
    }

    /// A stream that closes without `[DONE]` still carries the vendor's stop
    /// reason when one was sent before the close.
    #[tokio::test]
    async fn eof_without_done_marker_keeps_finish_reason() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n\
                   data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\n";
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let (evs, done) = collect(bytes).await;
        assert!(matches!(done, Some(StopReason::Length)), "done {done:?}");
        assert!(evs.iter().any(|e| matches!(
            e,
            WireEvent::Done {
                stop: StopReason::Length
            }
        )));
    }

    /// `[DONE]` without any `finish_reason` is a clean end of generation, not an
    /// abort — treating it as Aborted made the turn loop discard the output.
    #[tokio::test]
    async fn done_marker_without_finish_reason_is_stop() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"kept\"}}]}\n\ndata: [DONE]\n\n";
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let (evs, done) = collect(bytes).await;
        assert!(matches!(done, Some(StopReason::Stop)), "done {done:?}");
        assert!(
            evs.iter()
                .any(|e| matches!(e, WireEvent::TextDelta { text } if text == "kept"))
        );
    }

    /// Truncated stream (EOF, no `[DONE]`, no `finish_reason`) stays Aborted.
    #[tokio::test]
    async fn truncated_stream_without_markers_is_aborted() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n";
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let (_, done) = collect(bytes).await;
        assert!(matches!(done, Some(StopReason::Aborted)), "done {done:?}");
    }

    #[tokio::test]
    async fn tool_call_reassembly_ordered() {
        let sse = concat!(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"bash\",\"arguments\":\"{\\\"c\\\"\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\":\\\"ls\\\"}\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
        );
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let (evs, done) = collect(bytes).await;
        assert!(
            matches!(done, Some(StopReason::ToolUse)),
            "done {done:?} events {evs:?}"
        );
        let begins: Vec<&WireEvent> = evs
            .iter()
            .filter(|e| matches!(e, WireEvent::ToolCallBegin { .. }))
            .collect();
        assert_eq!(begins.len(), 1);
        if let WireEvent::ToolCallBegin { call_id, name, .. } = begins[0] {
            assert_eq!(call_id, "call_1");
            assert_eq!(name, "bash");
        }
        // args delta after begin carries the full reassembled string
        let args: Vec<&WireEvent> = evs
            .iter()
            .filter(|e| matches!(e, WireEvent::ToolCallArgsDelta { .. }))
            .collect();
        assert_eq!(args.len(), 1);
        if let WireEvent::ToolCallArgsDelta { delta, .. } = args[0] {
            assert_eq!(delta, "{\"c\":\"ls\"}");
        }
    }

    #[tokio::test]
    async fn unknown_finish_reason_gated_by_compat() {
        let sse =
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"weird\"}]}\n\ndata: [DONE]\n\n";
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let (evs, _) = collect(bytes).await;
        assert!(evs.iter().any(
            |e| matches!(e, WireEvent::Done { stop: StopReason::Error { message, .. } } if message.contains("weird"))
        ));

        // with tolerate flag -> Stop
        let wire = OpenAiChatWire;
        let mut model = ModelMeta::minimal("m", WireId::OpenAiCompletions, "http://x");
        model.compat.tolerate_unknown_finish = Some(true);
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let mut stream = wire.parse_stream(&model, bytes);
        let mut done = None;
        while let Some(e) = stream.next().await {
            if let WireEvent::Done { stop } = e {
                done = Some(stop);
            }
        }
        assert!(matches!(done, Some(StopReason::Stop)));
    }

    #[test]
    fn render_request_compat_field_switch() {
        let wire = OpenAiChatWire;
        let mut model = ModelMeta::minimal("m", WireId::OpenAiCompletions, "http://x");
        model.compat.max_tokens_field = Some(MaxTokensField::MaxCompletionTokens);
        let ctx = TurnContext {
            system: "s".into(),
            items: vec![ContextItem::UserText { text: "u".into() }],
            tools: vec![],
        };
        let spec = wire.render_request(&model, &ctx, 777, None, &ResolvedAuth::key("k"));
        assert_eq!(spec.body["max_completion_tokens"], 777);
        assert!(spec.body.get("max_tokens").is_none());
        assert_eq!(spec.url, "http://x/v1/chat/completions");
        assert!(
            spec.headers
                .iter()
                .any(|(k, v)| k == "authorization" && v == "Bearer k")
        );
    }

    #[test]
    fn render_messages_tool_pairing() {
        let ctx = TurnContext {
            system: String::new(),
            items: vec![
                ContextItem::AssistantToolUse {
                    call_id: "t1".into(),
                    name: "read".into(),
                    args: json!({"path": "a"}),
                },
                ContextItem::ToolResult {
                    call_id: "t1".into(),
                    content: vec![ToolContent::Text {
                        text: "data".into(),
                    }],
                    is_error: false,
                },
            ],
            tools: vec![],
        };
        let msgs = OpenAiChatWire::render_messages(&ctx);
        assert_eq!(msgs[0]["role"], "assistant");
        assert_eq!(msgs[0]["tool_calls"][0]["id"], "t1");
        assert_eq!(msgs[1]["role"], "tool");
        assert_eq!(msgs[1]["tool_call_id"], "t1");
        assert_eq!(msgs[1]["content"], "data");
    }

    /// A parallel batch is one assistant message holding both tool_calls (not
    /// two consecutive assistant messages), followed by both tool messages.
    #[test]
    fn render_messages_merges_parallel_tool_calls() {
        let ctx = TurnContext {
            system: String::new(),
            items: vec![
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
        let msgs = OpenAiChatWire::render_messages(&ctx);
        assert_eq!(msgs.len(), 3, "one assistant + two tool messages: {msgs:?}");
        assert_eq!(msgs[0]["role"], "assistant");
        assert_eq!(msgs[0]["tool_calls"].as_array().unwrap().len(), 2);
        assert_eq!(msgs[0]["tool_calls"][0]["id"], "t1");
        assert_eq!(msgs[0]["tool_calls"][1]["id"], "t2");
        assert_eq!(msgs[1]["tool_call_id"], "t1");
        assert_eq!(msgs[2]["tool_call_id"], "t2");
    }
}
