//! OpenAI Responses wire (§10): `POST /v1/responses` with the flat `input` item
//! list and the `response.*` SSE event family.
//!
//! 它与 chat-completions 不是一条 wire 的兼容位，而是三种结构差：系统提示走顶层
//! `instructions`；工具是扁平的 `{type:"function", name, parameters}`；历史是
//! `input` 项列表（`message` / `function_call` / `function_call_output`），并行
//! 工具批次因此天然成组——每条调用是自己的一项，不存在 chat 那条「一个 assistant
//! 装全部 tool_calls」的形状要求（design §9.12）。
//!
//! v1 的取舍：**只发有行为依据的字段**。`reasoning.effort` 没有真端点实测过，
//! 就不发（§9.3 的「不引入未用 flag」原则）；思考内容按内核纪律当不透明 blob
//! 处理——blob 本身就是 Responses 的 `reasoning` 项时原样回显，否则不发。

use futures::stream::{BoxStream, StreamExt};
use serde_json::{Value, json};

use super::wire::{
    ByteStream, ContextItem, ModelMeta, RequestSpec, ResolvedAuth, StopReason, ToolContent,
    TurnContext, Usage, WireEvent, WireFormat, WireId, sse_events,
};

pub struct OpenAiResponsesWire;

fn responses_url(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    format!("{base}/v1/responses")
}

fn content_text(content: &[ToolContent]) -> String {
    content
        .iter()
        .map(|c| match c {
            ToolContent::Text { text } => text.clone(),
            // v1 has no image input on this wire: say so in the transcript
            // instead of silently dropping the block.
            ToolContent::Image { .. } => "<image unsupported on this wire>".into(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

impl OpenAiResponsesWire {
    /// History -> `input` items. System prompt is NOT an item here (it goes to
    /// `instructions`), which the caller handles.
    fn render_input(ctx: &TurnContext) -> Vec<Value> {
        let mut out: Vec<Value> = Vec::new();
        for item in &ctx.items {
            match item {
                ContextItem::UserText { text } => out.push(json!({
                    "type": "message", "role": "user",
                    "content": [{ "type": "input_text", "text": text }]
                })),
                ContextItem::AssistantText { text } => out.push(json!({
                    "type": "message", "role": "assistant",
                    "content": [{ "type": "output_text", "text": text }]
                })),
                // Echo-only cookie: a Responses reasoning item is replayed
                // verbatim (that is how `store:false` keeps reasoning continuity);
                // anything else (e.g. an anthropic-shaped blob) is skipped.
                ContextItem::ThinkingOpaque { blob } => {
                    if blob["type"].as_str() == Some("reasoning") {
                        out.push(blob.clone());
                    }
                }
                ContextItem::AssistantToolUse {
                    call_id,
                    name,
                    args,
                } => out.push(json!({
                    "type": "function_call",
                    "call_id": call_id,
                    "name": name,
                    "arguments": args.to_string(),
                })),
                ContextItem::ToolResult {
                    call_id, content, ..
                } => {
                    // `function_call_output` has no is_error field. The kernel's
                    // error text already says what failed, so the wire neither
                    // invents a field nor prefixes the payload.
                    out.push(json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": content_text(content),
                    }));
                }
            }
        }
        out
    }

    /// `response.usage` -> normalized Usage. The Responses API reports input
    /// tokens including the cached share, so `cached_tokens` is recorded
    /// separately and never subtracted (the ledger trusts reported numbers).
    fn map_usage(response: &Value) -> Option<Usage> {
        let usage = response.get("usage")?;
        let num = |v: Option<&Value>| v.and_then(|x| x.as_u64()).unwrap_or(0);
        Some(Usage {
            input_tokens: num(usage.get("input_tokens")),
            output_tokens: num(usage.get("output_tokens")),
            cache_read_tokens: num(usage
                .get("input_tokens_details")
                .and_then(|d| d.get("cached_tokens"))),
            cache_write_tokens: 0,
        })
    }

    /// Terminal stop reason from a completed/incomplete response object.
    fn map_response_stop(response: &Value, has_calls: bool) -> StopReason {
        match response["status"].as_str() {
            Some("incomplete") => {
                let reason = response["incomplete_details"]["reason"].as_str();
                match reason {
                    // The loop's length rule fails the whole batch: this wire
                    // must map it, not soften it into Stop.
                    Some("max_output_tokens") => StopReason::Length,
                    Some(other) => {
                        StopReason::error(format!("response incomplete: {other}"), false)
                    }
                    None => StopReason::Length,
                }
            }
            _ => {
                if has_calls {
                    StopReason::ToolUse
                } else {
                    StopReason::Stop
                }
            }
        }
    }
}

impl WireFormat for OpenAiResponsesWire {
    fn id(&self) -> WireId {
        WireId::OpenAiResponses
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
            "input": Self::render_input(ctx),
            "stream": true,
            "max_output_tokens": max_out,
            // The session tree is the truth source (design §5 不变式 3): never
            // ask the vendor to keep a copy of the conversation.
            "store": false,
        });
        if !ctx.system.is_empty() {
            body["instructions"] = json!(ctx.system);
        }
        if !ctx.tools.is_empty() {
            let tools: Vec<Value> = ctx
                .tools
                .iter()
                .map(|t| {
                    json!({
                        "type": "function",
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    })
                })
                .collect();
            body["tools"] = json!(tools);
            body["tool_choice"] = json!("auto");
        }
        RequestSpec {
            url: responses_url(&model.base_url),
            headers: vec![("authorization".into(), format!("Bearer {}", auth.api_key))],
            body,
        }
    }

    fn parse_stream(&self, model: &ModelMeta, bytes: ByteStream) -> BoxStream<'static, WireEvent> {
        /// One in-flight function call, keyed by the item id the API streams.
        #[derive(Default, Clone)]
        struct Call {
            index: u32,
            call_id: String,
            name: String,
            args: String,
        }

        struct Reasm {
            by_item: std::collections::HashMap<String, usize>,
            calls: Vec<Call>,
            next_index: u32,
            done: bool,
        }

        impl Reasm {
            /// Tool calls are complete when the terminal event arrives: emit
            /// begin/args/end triples in wire `output_index` order.
            fn flush_calls(&mut self) -> Vec<WireEvent> {
                let mut slots = std::mem::take(&mut self.calls);
                slots.sort_by_key(|c| c.index);
                slots
                    .into_iter()
                    .map(|c| (c.index, c.call_id, c.name, c.args))
                    .flat_map(|(index, call_id, name, args)| {
                        [
                            WireEvent::ToolCallBegin {
                                index,
                                call_id,
                                name,
                            },
                            WireEvent::ToolCallArgsDelta { index, delta: args },
                            WireEvent::ToolCallEnd { index },
                        ]
                        .into_iter()
                    })
                    .collect()
            }

            fn slot(&mut self, item_id: &str, index: Option<u32>) -> &mut Call {
                if !self.by_item.contains_key(item_id) {
                    let index = index.unwrap_or(self.next_index);
                    self.next_index = self.next_index.max(index + 1);
                    let pos = self.calls.len();
                    self.calls.push(Call {
                        index,
                        call_id: item_id.to_string(),
                        ..Default::default()
                    });
                    self.by_item.insert(item_id.to_string(), pos);
                }
                let pos = self.by_item[item_id];
                &mut self.calls[pos]
            }

            /// A stream that ends without a terminal event was cut short: the
            /// loop discards partials, so no calls are flushed.
            fn aborted(&mut self) -> Vec<WireEvent> {
                self.calls.clear();
                vec![WireEvent::Done {
                    stop: StopReason::Aborted,
                }]
            }
        }

        // `model` is unused for now: this wire carries no compat bits yet, and
        // §9.3's rule is that only flags with observed behaviour get added.
        let _ = model;
        let state = Reasm {
            by_item: std::collections::HashMap::new(),
            calls: Vec::new(),
            next_index: 0,
            done: false,
        };
        let upstream = Box::pin(sse_events(bytes));
        let inner = futures::stream::unfold((upstream, state), move |(mut s, mut st)| async move {
            loop {
                if st.done {
                    return None;
                }
                match s.next().await {
                    None => {
                        st.done = true;
                        return Some((st.aborted(), (s, st)));
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
                    Some(Ok(frame)) => {
                        let data = frame.data.trim();
                        if data == "[DONE]" || data.is_empty() {
                            continue;
                        }
                        let Ok(v) = serde_json::from_str::<Value>(data) else {
                            st.done = true;
                            return Some((
                                vec![WireEvent::Done {
                                    stop: StopReason::error(
                                        format!("responses: unparseable stream line: {data}"),
                                        false,
                                    ),
                                }],
                                (s, st),
                            ));
                        };
                        // The event name is in the JSON `type` (the SSE `event:`
                        // line is a duplicate we do not need to trust).
                        let kind = v["type"]
                            .as_str()
                            .map(str::to_string)
                            .unwrap_or_else(|| frame.event.clone());
                        let out_events: Vec<WireEvent> = match kind.as_str() {
                            "response.created" | "response.in_progress" => vec![WireEvent::Start],
                            "response.output_text.delta" => match v["delta"].as_str() {
                                Some(text) if !text.is_empty() => {
                                    vec![WireEvent::TextDelta {
                                        text: text.to_string(),
                                    }]
                                }
                                _ => vec![],
                            },
                            "response.reasoning_summary_text.delta"
                            | "response.reasoning_text.delta" => match v["delta"].as_str() {
                                Some(text) if !text.is_empty() => {
                                    vec![WireEvent::ThinkingDelta {
                                        text: text.to_string(),
                                    }]
                                }
                                _ => vec![],
                            },
                            "response.output_item.added" => {
                                let item = &v["item"];
                                if item["type"].as_str() == Some("function_call") {
                                    let index = v["output_index"]
                                        .as_u64()
                                        .or_else(|| item["index"].as_u64())
                                        .map(|x| x as u32);
                                    let id = item["id"]
                                        .as_str()
                                        .or_else(|| item["call_id"].as_str())
                                        .unwrap_or("fc-0")
                                        .to_string();
                                    let slot = st.slot(&id, index);
                                    if let Some(name) = item["name"].as_str() {
                                        slot.name = name.to_string();
                                    }
                                    if let Some(cid) = item["call_id"].as_str() {
                                        slot.call_id = cid.to_string();
                                    }
                                }
                                vec![]
                            }
                            "response.function_call_arguments.delta" => {
                                let id = v["item_id"]
                                    .as_str()
                                    .or_else(|| v["call_id"].as_str())
                                    .unwrap_or("fc-0")
                                    .to_string();
                                let index = v["output_index"].as_u64().map(|x| x as u32);
                                let slot = st.slot(&id, index);
                                if let Some(d) = v["delta"].as_str() {
                                    slot.args.push_str(d);
                                }
                                vec![]
                            }
                            // `..._arguments.done` / `output_item.done` carry the
                            // complete arguments: a fallback for servers that skip
                            // deltas, never a replacement for what was streamed.
                            "response.function_call_arguments.done" => {
                                let id = v["item_id"]
                                    .as_str()
                                    .or_else(|| v["call_id"].as_str())
                                    .unwrap_or("fc-0")
                                    .to_string();
                                let index = v["output_index"].as_u64().map(|x| x as u32);
                                let slot = st.slot(&id, index);
                                if slot.args.is_empty()
                                    && let Some(a) = v["arguments"].as_str()
                                {
                                    slot.args = a.to_string();
                                }
                                vec![]
                            }
                            "response.output_item.done" => {
                                let item = &v["item"];
                                if item["type"].as_str() == Some("function_call") {
                                    let id = item["id"]
                                        .as_str()
                                        .or_else(|| item["call_id"].as_str())
                                        .unwrap_or("fc-0")
                                        .to_string();
                                    let index = v["output_index"].as_u64().map(|x| x as u32);
                                    let args = item["arguments"].as_str().map(str::to_string);
                                    let name = item["name"].as_str().map(str::to_string);
                                    let call_id = item["call_id"].as_str().map(str::to_string);
                                    let slot = st.slot(&id, index);
                                    if slot.args.is_empty() {
                                        slot.args = args.unwrap_or_default();
                                    }
                                    if slot.name.is_empty() {
                                        slot.name = name.unwrap_or_default();
                                    }
                                    if let Some(cid) = call_id {
                                        slot.call_id = cid;
                                    }
                                }
                                vec![]
                            }
                            "response.completed" | "response.incomplete" => {
                                let response = &v["response"];
                                let has_calls = !st.calls.is_empty();
                                let usage = Self::map_usage(response);
                                let mut evs = st.flush_calls();
                                if let Some(u) = usage {
                                    evs.push(WireEvent::Usage(u));
                                }
                                let stop = if kind == "response.incomplete" {
                                    Self::map_response_stop(response, has_calls)
                                } else if has_calls {
                                    StopReason::ToolUse
                                } else {
                                    StopReason::Stop
                                };
                                st.done = true;
                                evs.push(WireEvent::Done { stop });
                                evs
                            }
                            "response.failed" => {
                                let message = v["response"]["error"]["message"]
                                    .as_str()
                                    .or_else(|| {
                                        v["response"]["status_details"]["error"]["message"].as_str()
                                    })
                                    .unwrap_or("response failed");
                                st.done = true;
                                vec![WireEvent::Done {
                                    stop: StopReason::error(message.to_string(), false),
                                }]
                            }
                            "error" => {
                                let message = v["error"]["message"]
                                    .as_str()
                                    .or_else(|| v["message"].as_str());
                                let message = message.unwrap_or("stream error");
                                let retryable = v["error"]["type"].as_str().is_some_and(|t| {
                                    t == "server_error" || t == "rate_limit_error"
                                });
                                st.done = true;
                                vec![WireEvent::Done {
                                    stop: StopReason::error(message.to_string(), retryable),
                                }]
                            }
                            // Unknown event types are informational
                            // (`response.queue.*`, gateway-specific additions).
                            _ => vec![],
                        };
                        if !out_events.is_empty() {
                            return Some((out_events, (s, st)));
                        }
                    }
                }
            }
        });
        // The unfold step hands out batches (a terminal event may also flush the
        // tool calls that preceded it); flatten so the outer stream yields single
        // WireEvents like every other wire does.
        Box::pin(inner.flat_map(futures::stream::iter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::wire::ToolSpec;
    use futures::StreamExt;

    fn model() -> ModelMeta {
        ModelMeta::minimal(
            "resp-model",
            WireId::OpenAiResponses,
            "https://gw.example/v1",
        )
    }

    async fn collect(sse: &'static str) -> (Vec<WireEvent>, Option<StopReason>) {
        let bytes: ByteStream =
            Box::pin(futures::stream::once(
                async move { Ok(bytes::Bytes::from(sse)) },
            ));
        let mut stream = OpenAiResponsesWire.parse_stream(&model(), bytes);
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

    fn line(v: Value) -> String {
        format!("data: {}\n\n", serde_json::to_string(&v).unwrap())
    }

    #[test]
    fn request_shape_is_the_responses_api() {
        let ctx = TurnContext {
            system: "you are kestrel".into(),
            items: vec![
                ContextItem::UserText { text: "hi".into() },
                ContextItem::AssistantText { text: "yo".into() },
                ContextItem::ThinkingOpaque {
                    blob: json!({"type": "thinking", "thinking": "anthropic-shaped"}),
                },
                ContextItem::ThinkingOpaque {
                    blob: json!({"type": "reasoning", "id": "rs_1", "encrypted_content": "xyz"}),
                },
                ContextItem::AssistantToolUse {
                    call_id: "call_1".into(),
                    name: "bash".into(),
                    args: json!({"command": "ls"}),
                },
                ContextItem::ToolResult {
                    call_id: "call_1".into(),
                    content: vec![ToolContent::Text {
                        text: "file".into(),
                    }],
                    is_error: true,
                },
            ],
            tools: vec![ToolSpec {
                name: "bash".into(),
                description: "run".into(),
                parameters: json!({"type": "object"}),
            }],
        };
        let spec = OpenAiResponsesWire.render_request(
            &model(),
            &ctx,
            2048,
            None,
            &ResolvedAuth::key("secret"),
        );
        // base_url + /v1/responses, and the api key is a bearer header
        assert_eq!(spec.url, "https://gw.example/v1/v1/responses");
        assert_eq!(
            spec.headers[0],
            ("authorization".into(), "Bearer secret".into())
        );
        assert_eq!(spec.body["instructions"], "you are kestrel");
        assert_eq!(spec.body["stream"], true);
        assert_eq!(spec.body["max_output_tokens"], 2048);
        assert_eq!(
            spec.body["store"], false,
            "session tree is the truth source"
        );
        assert_eq!(spec.body["tools"][0]["type"], "function");
        assert_eq!(spec.body["tools"][0]["name"], "bash", "flat, not nested");
        assert!(spec.body["tools"][0].get("function").is_none());

        let input = spec.body["input"].as_array().unwrap();
        assert_eq!(input[0]["content"][0]["type"], "input_text");
        assert_eq!(input[1]["content"][0]["type"], "output_text");
        assert_eq!(
            input[2]["type"], "reasoning",
            "a responses reasoning blob is echoed verbatim"
        );
        assert_eq!(input[2]["encrypted_content"], "xyz");
        assert_eq!(input[3]["type"], "function_call");
        assert_eq!(input[3]["arguments"], "{\"command\":\"ls\"}");
        assert_eq!(input[4]["type"], "function_call_output");
        assert_eq!(input[4]["output"], "file");
        assert!(
            input[4].get("is_error").is_none(),
            "this wire has no is_error field; the text carries the failure"
        );
        // parallel calls are separate items, so no grouping rule is needed here
        assert_eq!(input.len(), 5, "anthropic-shaped thinking blob is skipped");
    }

    #[tokio::test]
    async fn text_stream_with_usage() {
        let leaked: &'static str = Box::leak(
            [
                line(json!({"type": "response.created", "response": {"id": "resp_1"}})),
                line(json!({"type": "response.output_text.delta", "delta": "hel"})),
                line(json!({"type": "response.output_text.delta", "delta": "lo"})),
                line(json!({
                    "type": "response.completed",
                    "response": {
                        "status": "completed",
                        "usage": {
                            "input_tokens": 41,
                            "output_tokens": 7,
                            "input_tokens_details": {"cached_tokens": 5}
                        }
                    }
                })),
            ]
            .join("")
            .into_boxed_str(),
        );
        let (evs, done) = collect(leaked).await;
        assert!(matches!(done, Some(StopReason::Stop)), "{done:?}");
        let text: String = evs
            .iter()
            .filter_map(|e| match e {
                WireEvent::TextDelta { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, "hello");
        assert!(
            evs.iter().any(
                |e| matches!(e, WireEvent::Usage(u) if u.input_tokens == 41 && u.output_tokens == 7 && u.cache_read_tokens == 5)
            ),
            "usage rides the terminal event: {evs:?}"
        );
        assert!(matches!(evs.last(), Some(WireEvent::Done { .. })));
    }

    #[tokio::test]
    async fn function_call_stream_becomes_a_tool_batch() {
        let leaked: &'static str = Box::leak(
            [
                line(json!({"type": "response.created"})),
                line(json!({
                    "type": "response.output_item.added",
                    "output_index": 1,
                    "item": {"type": "function_call", "id": "fc_1", "call_id": "call_a", "name": "bash", "arguments": ""}
                })),
                line(json!({"type": "response.function_call_arguments.delta", "item_id": "fc_1", "output_index": 1, "delta": "{\"command\""})),
                line(json!({"type": "response.function_call_arguments.delta", "item_id": "fc_1", "output_index": 1, "delta": ": \"ls\"}"})),
                line(json!({"type": "response.output_item.done", "output_index": 1, "item": {"type": "function_call", "id": "fc_1", "call_id": "call_a", "name": "bash", "arguments": "{\"command\": \"ls\"}"}})),
                line(json!({"type": "response.completed", "response": {"status": "completed", "usage": {"input_tokens": 9, "output_tokens": 3}}})),
            ]
            .join("")
            .into_boxed_str(),
        );
        let (evs, done) = collect(leaked).await;
        assert!(matches!(done, Some(StopReason::ToolUse)), "{done:?}");
        assert!(
            evs.iter().any(
                |e| matches!(e, WireEvent::ToolCallBegin { index, call_id, name }
                if *index == 1 && call_id == "call_a" && name == "bash")
            ),
            "{evs:?}"
        );
        let args: String = evs
            .iter()
            .filter_map(|e| match e {
                WireEvent::ToolCallArgsDelta { delta, .. } => Some(delta.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(args, "{\"command\": \"ls\"}");
        assert!(
            evs.iter()
                .any(|e| matches!(e, WireEvent::ToolCallEnd { index: 1 }))
        );
    }

    /// The loop's length rule (整批 fail) depends on this wire mapping
    /// `max_output_tokens` to Length rather than softening it into Stop.
    #[tokio::test]
    async fn incomplete_from_token_cap_maps_to_length() {
        let leaked: &'static str = Box::leak(
            [
                line(json!({"type": "response.output_text.delta", "delta": "cut"})),
                line(json!({
                    "type": "response.incomplete",
                    "response": {"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}, "usage": {"input_tokens": 2, "output_tokens": 2}}
                })),
            ]
            .join("")
            .into_boxed_str(),
        );
        let (_, done) = collect(leaked).await;
        assert_eq!(done, Some(StopReason::Length));
    }

    #[tokio::test]
    async fn failed_and_error_events_are_terminal_errors() {
        let leaked: &'static str = Box::leak(
            [line(json!({
                "type": "response.failed",
                "response": {"status": "failed", "error": {"message": "upstream 500"}}
            }))]
            .join("")
            .into_boxed_str(),
        );
        let (evs, done) = collect(leaked).await;
        assert!(
            matches!(&done, Some(StopReason::Error { message, retryable, .. })
                if message == "upstream 500" && !retryable),
            "{done:?}"
        );
        assert_eq!(evs.len(), 1, "a failed response emits no tool events");

        let leaked2: &'static str = Box::leak(
            [line(json!({"type": "error", "error": {"message": "slow down", "type": "rate_limit_error"}}))]
                .join("")
                .into_boxed_str(),
        );
        let (_, done2) = collect(leaked2).await;
        assert!(
            matches!(
                &done2,
                Some(StopReason::Error {
                    retryable: true,
                    ..
                })
            ),
            "rate limits are retryable: {done2:?}"
        );
    }

    /// A gateway that closes the stream mid-response must NOT look like a
    /// finished turn: the loop discards partials on Aborted.
    #[tokio::test]
    async fn eof_mid_stream_is_aborted_and_drops_calls() {
        let leaked: &'static str = Box::leak(
            [
                line(json!({"type": "response.output_text.delta", "delta": "half"})),
                line(json!({
                    "type": "response.output_item.added",
                    "output_index": 0,
                    "item": {"type": "function_call", "id": "fc_x", "call_id": "call_x", "name": "bash", "arguments": ""}
                })),
            ]
            .join("")
            .into_boxed_str(),
        );
        let (evs, done) = collect(leaked).await;
        assert_eq!(done, Some(StopReason::Aborted));
        assert!(
            !evs.iter()
                .any(|e| matches!(e, WireEvent::ToolCallBegin { .. })),
            "an aborted stream must not hand the loop a tool batch: {evs:?}"
        );
    }

    #[tokio::test]
    async fn reasoning_summary_streams_as_thinking() {
        let leaked: &'static str = Box::leak(
            [
                line(json!({"type": "response.reasoning_summary_text.delta", "delta": "think"})),
                line(json!({"type": "response.completed", "response": {"status": "completed", "usage": {"input_tokens": 1, "output_tokens": 1}}})),
            ]
            .join("")
            .into_boxed_str(),
        );
        let (evs, done) = collect(leaked).await;
        assert!(matches!(done, Some(StopReason::Stop)));
        assert!(
            evs.iter()
                .any(|e| matches!(e, WireEvent::ThinkingDelta { text } if text == "think")),
            "{evs:?}"
        );
    }
}
