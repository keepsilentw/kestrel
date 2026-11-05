//! Wire layer: normalized types shared by every provider implementation.
//!
//! Design anchors (CC repo docs/design/rust-agent/study-pi-ai.md):
//! - model-level metadata (not provider-level);
//! - errors/aborts are terminal stream events (`StopReason::Error|Aborted`), never exceptions;
//! - thinking blobs are opaque echo-only cookies (`ContextItem::ThinkingOpaque`).

use std::time::Duration;

use futures::stream::{BoxStream, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Which wire format a provider speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WireId {
    AnthropicMessages,
    #[serde(rename = "openai-completions")]
    OpenAiCompletions,
    /// The OpenAI Responses API (`/v1/responses`, `response.*` SSE family).
    /// A different wire from chat-completions, not a compat flag on it: the
    /// request takes an `input` item list, tools are flat, and the terminal
    /// event carries the usage.
    #[serde(rename = "openai-responses")]
    OpenAiResponses,
}

/// Price per million tokens, declared in the user layer
/// (`~/.kestrel/providers.toml`) — the one place money rates may be written
/// down, exactly like keys. **The unit is not part of this type**: it comes from
/// the provider's `currency` (`Registry::provider_currency`, default `usd`), so a
/// vendor's own price list can be recorded as published — 元 stays 元, no FX
/// arithmetic hides in the kernel (design §9.44). `cache_read` / `cache_write` are
/// optional because not every gateway bills them separately; when absent, cached
/// input is billed at the plain `input` rate (see `Usage::cost`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CostPerMTok {
    pub input: f64,
    pub output: f64,
    /// Reading the prompt cache. Unset => billed as ordinary input.
    #[serde(default)]
    pub cache_read: Option<f64>,
    /// Writing the prompt cache. Unset => billed as ordinary input.
    #[serde(default)]
    pub cache_write: Option<f64>,
}

impl CostPerMTok {
    /// A two-rate table with no separate cache pricing.
    pub const fn simple(input: f64, output: f64) -> Self {
        Self {
            input,
            output,
            cache_read: None,
            cache_write: None,
        }
    }

    /// Whether this table says anything at all. An all-zero table is NOT "free":
    /// it is "nobody filled this in", and treating it as a bill would silently
    /// disable a budget stop-loss. Callers price only when this is true.
    pub fn is_declared(&self) -> bool {
        self.input > 0.0
            || self.output > 0.0
            || self.cache_read.unwrap_or(0.0) > 0.0
            || self.cache_write.unwrap_or(0.0) > 0.0
    }
}

/// OpenAI-family is NOT one wire: these flags absorb per-gateway deviations
/// (pi compat 位的最小子集——每个 flag 都有具体行为挂在上面).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CompatFlags {
    /// `max_tokens` (classic) vs `max_completion_tokens` (newer gateways).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens_field: Option<MaxTokensField>,
    /// Unknown finish_reason: error (default) vs tolerate-as-stop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerate_unknown_finish: Option<bool>,
    /// Ask for usage inside the final streamed chunk (OpenAI needs a body flag).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_include_usage: Option<bool>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaxTokensField {
    #[default]
    MaxTokens,
    MaxCompletionTokens,
}

/// Static metadata of one model. Drives loop truncation and (later) budget math.
/// base_url lives here (pi semantics): one provider may mix wires/endpoints per model.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelMeta {
    pub id: String,
    pub wire: WireId,
    pub base_url: String,
    pub context_window: u32,
    pub max_out: u32,
    #[serde(default)]
    pub input_image: bool,
    #[serde(default)]
    pub reasoning: bool,
    #[serde(default)]
    pub cost: Option<CostPerMTok>,
    #[serde(default)]
    pub compat: CompatFlags,
}

impl ModelMeta {
    /// Stable string id of the wire (session header / logging).
    pub fn wire_id_str(&self) -> &'static str {
        match self.wire {
            WireId::AnthropicMessages => "anthropic-messages",
            WireId::OpenAiCompletions => "openai-completions",
            WireId::OpenAiResponses => "openai-responses",
        }
    }

    pub fn minimal(id: &str, wire: WireId, base_url: &str) -> Self {
        Self {
            id: id.to_string(),
            wire,
            base_url: base_url.to_string(),
            context_window: 128_000,
            max_out: 8_192,
            input_image: false,
            reasoning: false,
            cost: None,
            compat: CompatFlags::default(),
        }
    }
}

/// Resolved credentials handed to `render_request`. M0: single api key.
/// M3 replaces with the 4-step precedence chain.
#[derive(Clone, Debug)]
pub struct ResolvedAuth {
    pub api_key: String,
}

impl ResolvedAuth {
    pub fn key(key: impl Into<String>) -> Self {
        Self {
            api_key: key.into(),
        }
    }
}

/// One tool exposed to the model (JSON Schema parameters).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub parameters: Value,
}

/// Token usage as reported by the API. The loop trusts these numbers (codex discipline).
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_read_tokens: u64,
    #[serde(default)]
    pub cache_write_tokens: u64,
}

impl Usage {
    pub fn total(&self) -> u64 {
        self.input_tokens + self.output_tokens
    }

    pub fn accumulate(&mut self, other: &Usage) {
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.cache_read_tokens += other.cache_read_tokens;
        self.cache_write_tokens += other.cache_write_tokens;
    }

    /// The bill for this usage at `price`, in USD. The arithmetic lives next to
    /// the two types it combines; the AUTHORITY to bill lives in the ledger (the
    /// SA resolves the rate from the registry and applies it — a worker reports
    /// measurements, it does not set its own prices).
    ///
    /// Cached tokens are never dropped from the math: they are billed at their
    /// own rate when the table declares one, otherwise at the plain input rate.
    /// Cache reads are usually cheaper than that (so this overstates a little)
    /// and cache writes a bit dearer (so this understates the write premium);
    /// declare `cache_read` / `cache_write` when the difference matters.
    ///
    /// Unit-neutral by design: the number is in whatever currency the rate table
    /// declares. A name that claims a currency the table does not have is how a
    /// report line starts lying about money.
    pub fn cost(&self, price: &CostPerMTok) -> f64 {
        const M: f64 = 1_000_000.0;
        let cache_read = price.cache_read.unwrap_or(price.input);
        let cache_write = price.cache_write.unwrap_or(price.input);
        (self.input_tokens as f64) * price.input / M
            + (self.output_tokens as f64) * price.output / M
            + (self.cache_read_tokens as f64) * cache_read / M
            + (self.cache_write_tokens as f64) * cache_write / M
    }

    /// The worst-case bill a dispatch to this model can produce: the whole
    /// context window re-sent as input plus a full `max_out` of output. Used as
    /// a budget RESERVATION before the work is dispatched, so a pipeline that
    /// crashes mid-dispatch cannot re-burn the ceiling forever.
    pub fn worst_case_cost(price: &CostPerMTok, context_window: u32, max_out: u32) -> f64 {
        let billed = Usage {
            input_tokens: context_window as u64,
            output_tokens: max_out as u64,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        };
        billed.cost(price)
    }
}

/// Thinking effort hint. M0: single level; M3 maps per-provider formats.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ThinkingBudget {
    pub budget_tokens: u32,
}

/// Normalized conversation history item handed to a wire for rendering.
/// Persisted format is the session tree (M2); this is the LLM-bound projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContextItem {
    UserText {
        text: String,
    },
    AssistantText {
        text: String,
    },
    /// Opaque thinking blob: echo back verbatim, never inspect (HTTP-only cookie).
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

/// Content blocks inside a tool result / message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolContent {
    Text { text: String },
    Image { media_type: String, data: String },
}

/// System prompt + items + tool specs for one sampling request.
#[derive(Clone, Debug, Default)]
pub struct TurnContext {
    pub system: String,
    pub items: Vec<ContextItem>,
    pub tools: Vec<ToolSpec>,
}

/// Fully-rendered HTTP request, wire-agnostic on the transport side.
#[derive(Clone, Debug)]
pub struct RequestSpec {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Value,
}

/// Abstract byte stream so faux fixtures and real HTTP share one parse path.
pub type ByteStream = std::pin::Pin<Box<dyn Stream<Item = std::io::Result<bytes::Bytes>> + Send>>;

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("transport: {0}")]
    Transport(String),
    #[error("bad response {status}: {body}")]
    BadResponse {
        status: u16,
        body: String,
        /// `retry-after` / `retry-after-ms` header in ms (retry policy input).
        retry_after_ms: Option<u64>,
        /// `x-should-retry` header (provider-retry semantics).
        x_should_retry: Option<bool>,
    },
    #[error("stream decode: {0}")]
    Decode(String),
}

/// Terminal reason of one sampling stream. Always delivered exactly once as
/// `WireEvent::Done` (or synthesized by the loop when the stream is cancelled).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StopReason {
    ToolUse,
    Stop,
    Length,
    Error {
        message: String,
        #[serde(default)]
        retryable: bool,
        #[serde(default)]
        retry_after_ms: Option<u64>,
    },
    Aborted,
}

impl StopReason {
    pub fn error(message: impl Into<String>, retryable: bool) -> Self {
        StopReason::Error {
            message: message.into(),
            retryable,
            retry_after_ms: None,
        }
    }

    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            StopReason::Error { retry_after_ms, .. } => retry_after_ms.map(Duration::from_millis),
            _ => None,
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            StopReason::Error {
                retryable: true,
                ..
            }
        )
    }
}

/// Normalized streaming event. `Done` is always the last event and fires once.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WireEvent {
    Start,
    TextDelta {
        text: String,
    },
    ThinkingDelta {
        text: String,
    },
    ThinkingSigned {
        blob: Value,
    },
    ToolCallBegin {
        index: u32,
        call_id: String,
        name: String,
    },
    ToolCallArgsDelta {
        index: u32,
        delta: String,
    },
    ToolCallEnd {
        index: u32,
    },
    Usage(Usage),
    Done {
        stop: StopReason,
    },
}

/// One provider wire implementation: render + parse, both side-effect free.
pub trait WireFormat: Send + Sync + 'static {
    fn id(&self) -> WireId;

    /// Pure function: context -> vendor request. Golden-body tests target this.
    fn render_request(
        &self,
        model: &ModelMeta,
        ctx: &TurnContext,
        max_out: u32,
        thinking: Option<ThinkingBudget>,
        auth: &ResolvedAuth,
    ) -> RequestSpec;

    /// Vendor byte stream -> normalized event stream.
    fn parse_stream(&self, model: &ModelMeta, bytes: ByteStream) -> BoxStream<'static, WireEvent>;
}

/// Transport turns a rendered spec into a byte stream. Faux bypasses it.
pub trait Transport: Send + Sync + 'static {
    fn send(
        &self,
        spec: RequestSpec,
    ) -> futures::future::BoxFuture<'static, Result<ByteStream, WireError>>;
}

/// Real HTTP transport (reqwest, SSE response body as bytes).
pub struct HttpTransport {
    client: reqwest::Client,
}

impl HttpTransport {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for HttpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for HttpTransport {
    fn send(
        &self,
        spec: RequestSpec,
    ) -> futures::future::BoxFuture<'static, Result<ByteStream, WireError>> {
        let client = self.client.clone();
        Box::pin(async move {
            let mut req = client.post(&spec.url);
            for (k, v) in &spec.headers {
                req = req.header(k, v);
            }
            let resp = req
                .json(&spec.body)
                .send()
                .await
                .map_err(|e| WireError::Transport(e.to_string()))?;
            let status = resp.status();
            if !status.is_success() {
                let retry_after_ms = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(|secs| secs.saturating_mul(1000))
                    .or_else(|| {
                        resp.headers()
                            .get("retry-after-ms")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                    });
                let x_should_retry = resp
                    .headers()
                    .get("x-should-retry")
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v == "true");
                let body = resp.text().await.unwrap_or_default();
                return Err(WireError::BadResponse {
                    status: status.as_u16(),
                    body,
                    retry_after_ms,
                    x_should_retry,
                });
            }
            let stream = resp
                .bytes_stream()
                .map(|r| r.map_err(|e| std::io::Error::other(e.to_string())));
            Ok(Box::pin(stream) as ByteStream)
        })
    }
}

/// Transport used with the faux wire; never called (faux ignores the byte stream).
pub struct NullTransport;

impl Transport for NullTransport {
    fn send(
        &self,
        _spec: RequestSpec,
    ) -> futures::future::BoxFuture<'static, Result<ByteStream, WireError>> {
        Box::pin(async { Ok(Box::pin(futures::stream::empty()) as ByteStream) })
    }
}

/// SSE-decode helper shared by real wires: raw bytes -> SSE events -> custom map.
pub(crate) fn sse_events(
    bytes: ByteStream,
) -> impl Stream<Item = Result<eventsource_stream::Event, WireError>> {
    eventsource_stream::Eventsource::eventsource(bytes)
        .map(|e| e.map_err(|err| WireError::Decode(err.to_string())))
}

/// Guarantee: exactly one terminal `Done` at stream end; if the vendor stream
/// closes without one (server closed early), synthesize `Done{Aborted}`.
pub(crate) fn ensure_done<S>(inner: S) -> impl Stream<Item = WireEvent>
where
    S: Stream<Item = WireEvent> + Unpin,
{
    futures::stream::unfold((inner, false), |(mut s, done)| async move {
        match s.next().await {
            Some(ev) => {
                let is_done = matches!(ev, WireEvent::Done { .. });
                Some((ev, (s, done || is_done)))
            }
            None => {
                if done {
                    None
                } else {
                    Some((
                        WireEvent::Done {
                            stop: StopReason::Aborted,
                        },
                        (s, true),
                    ))
                }
            }
        }
    })
}

/// Drain helper: collect a normalized stream into (events, done).
pub async fn collect_stream(
    mut stream: BoxStream<'static, WireEvent>,
) -> (Vec<WireEvent>, StopReason) {
    let mut events = Vec::new();
    let stop = loop {
        match stream.next().await {
            Some(WireEvent::Done { stop }) => break stop,
            Some(ev) => events.push(ev),
            // Stream ended without Done (server closed early): treat as aborted.
            None => break StopReason::Aborted,
        }
    };
    (events, stop)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_accumulate() {
        let mut a = Usage {
            input_tokens: 10,
            output_tokens: 5,
            ..Default::default()
        };
        a.accumulate(&Usage {
            input_tokens: 1,
            output_tokens: 2,
            cache_read_tokens: 3,
            ..Default::default()
        });
        assert_eq!(a.input_tokens, 11);
        assert_eq!(a.output_tokens, 7);
        assert_eq!(a.cache_read_tokens, 3);
        assert_eq!(a.total(), 18);
    }

    #[test]
    fn stop_reason_helpers() {
        let e = StopReason::error("boom", true);
        assert!(e.is_retryable());
        assert!(e.retry_after().is_none());
        assert!(!StopReason::Length.is_retryable());
    }

    /// Money math: per-million rates over all four token buckets. A declared
    /// cache rate wins; an undeclared one bills at the input rate, so cached
    /// tokens can never fall out of the total.
    #[test]
    fn usage_cost_uses_every_bucket() {
        let u = Usage {
            input_tokens: 1_000_000,
            output_tokens: 500_000,
            cache_read_tokens: 2_000_000,
            cache_write_tokens: 100_000,
        };
        let flat = CostPerMTok::simple(2.0, 8.0);
        // 2 + 4 + 4 + 0.2 (reads and writes both at the input rate)
        assert!((u.cost(&flat) - 10.2).abs() < 1e-9, "{}", u.cost(&flat));
        let cached = CostPerMTok {
            input: 2.0,
            output: 8.0,
            cache_read: Some(0.2),
            cache_write: Some(2.5),
        };
        // 2 + 4 + 0.4 + 0.25
        assert!((u.cost(&cached) - 6.65).abs() < 1e-9);
    }

    #[test]
    fn reservation_is_the_context_window_plus_one_full_answer() {
        let price = CostPerMTok::simple(1.0, 4.0);
        let worst = Usage::worst_case_cost(&price, 128_000, 8_192);
        assert!((worst - (0.128 + 0.032768)).abs() < 1e-9, "{worst}");
    }

    /// An empty table is not a free pass: `is_declared` is what keeps the
    /// ledger from reading "nobody wrote a rate" as "this costs nothing".
    #[test]
    fn zero_rate_is_not_a_declared_price() {
        assert!(!CostPerMTok::default().is_declared());
        assert!(!CostPerMTok::simple(0.0, 0.0).is_declared());
        assert!(CostPerMTok::simple(0.0, 0.5).is_declared());
        assert!(
            CostPerMTok {
                input: 0.0,
                output: 0.0,
                cache_read: None,
                cache_write: Some(0.1),
            }
            .is_declared()
        );
    }
}
