//! Faux wire: replays scripted normalized-event scripts instead of touching network.
//!
//! Testing discipline (pi-study-notes §5, "faux provider 第一天进 trait"): every
//! acceptance test drives the engine through the same `WireFormat` trait as the
//! real wires, zero real keys. Also observes consumer drops so cancellation
//! tests can assert the upstream task exits.

use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};
use serde_json::Value;

use super::wire::{
    ModelMeta, RequestSpec, ResolvedAuth, StopReason, ThinkingBudget, ToolContent, ToolSpec,
    TurnContext, Usage, WireEvent, WireFormat, WireId,
};

/// One scripted delay + event pair, as stored in `*.events.jsonl` fixtures.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct ScriptedEvent {
    #[serde(default)]
    pub delay_ms: u64,
    #[serde(rename = "ev")]
    pub event: WireEvent,
}

#[derive(Default)]
struct ScriptQueue {
    /// One script per subsequent parse_stream call (i.e. per turn).
    turns: Mutex<VecDeque<Vec<ScriptedEvent>>>,
    /// Observers notified when a consumer drops a live stream.
    drop_txs: Mutex<Vec<tokio::sync::mpsc::UnboundedSender<()>>>,
}

/// Faux implementation of [`WireFormat`].
pub struct FauxWire {
    queue: Arc<ScriptQueue>,
    /// render_request records bodies here for request-assert tests.
    pub recorded: Arc<Mutex<Vec<Value>>>,
}

impl FauxWire {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(ScriptQueue::default()),
            recorded: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Queue scripts, one per subsequent turn.
    pub fn push_turn_script(&self, events: Vec<WireEvent>) {
        let scripted: Vec<ScriptedEvent> = events
            .into_iter()
            .map(|event| ScriptedEvent { delay_ms: 0, event })
            .collect();
        self.queue.turns.lock().unwrap().push_back(scripted);
    }

    /// Queue a delayed script (used by cancellation tests to slow the stream down).
    pub fn push_scripted_turn(&self, events: Vec<ScriptedEvent>) {
        self.queue.turns.lock().unwrap().push_back(events);
    }

    /// Subscribe to consumer-drop notifications (cancellation tests).
    pub fn subscribe_drops(&self) -> tokio::sync::mpsc::UnboundedReceiver<()> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.queue.drop_txs.lock().unwrap().push(tx);
        rx
    }

    /// Load a `*.events.jsonl` fixture: one JSON object per line.
    pub fn from_fixture(path: &Path) -> anyhow::Result<Vec<ScriptedEvent>> {
        let text = fs::read_to_string(path)?;
        let mut out = Vec::new();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let ev: ScriptedEvent = serde_json::from_str(line)
                .map_err(|e| anyhow::anyhow!("fixture {}: line: {e}", path.display()))?;
            out.push(ev);
        }
        Ok(out)
    }

    pub fn recorded_requests(&self) -> Vec<Value> {
        self.recorded.lock().unwrap().clone()
    }
}

impl Default for FauxWire {
    fn default() -> Self {
        Self::new()
    }
}

/// Lives inside the returned stream; fires observers on drop (consumer gone or
/// stream exhausted — tests only assert the mid-stream-drop case).
struct DropGuard {
    queue: Arc<ScriptQueue>,
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        for tx in self.queue.drop_txs.lock().unwrap().drain(..) {
            let _ = tx.send(());
        }
    }
}

impl WireFormat for FauxWire {
    fn id(&self) -> WireId {
        WireId::AnthropicMessages // identity is irrelevant for faux
    }

    fn render_request(
        &self,
        _model: &ModelMeta,
        ctx: &TurnContext,
        _max_out: u32,
        _thinking: Option<ThinkingBudget>,
        _auth: &ResolvedAuth,
    ) -> RequestSpec {
        // Record a simplified view of the request for body assertions.
        let body = serde_json::json!({
            "system": ctx.system,
            "items": ctx.items,
            "tools": ctx.tools,
        });
        self.recorded.lock().unwrap().push(body);
        RequestSpec {
            url: "faux://unused".into(),
            headers: vec![],
            body: Value::Null,
        }
    }

    fn parse_stream(
        &self,
        _model: &ModelMeta,
        _bytes: super::wire::ByteStream,
    ) -> BoxStream<'static, WireEvent> {
        let script = self
            .queue
            .turns
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_default();
        // The guard is owned by the final map closure, which lives as long as
        // the stream itself; dropping the stream (consumer gone or exhausted)
        // releases it and notifies the observers.
        let guard = Arc::new(DropGuard {
            queue: Arc::clone(&self.queue),
        });
        let stream = stream::iter(script).then(|se| async move {
            if se.delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(se.delay_ms)).await;
            }
            se.event
        });
        Box::pin(stream.map(move |ev| {
            let _ = &guard;
            ev
        }))
    }
}

/// Convenience: single-turn text-only Stop script.
pub fn stop_script(text: &str) -> Vec<WireEvent> {
    vec![
        WireEvent::Start,
        WireEvent::TextDelta {
            text: text.to_string(),
        },
        WireEvent::Usage(Usage {
            input_tokens: 10,
            output_tokens: 5,
            ..Default::default()
        }),
        WireEvent::Done {
            stop: StopReason::Stop,
        },
    ]
}

/// Convenience: single-turn tool-use script (text then one tool call).
pub fn tool_use_script(text: &str, call_id: &str, name: &str, args: Value) -> Vec<WireEvent> {
    vec![
        WireEvent::Start,
        WireEvent::TextDelta {
            text: text.to_string(),
        },
        WireEvent::ToolCallBegin {
            index: 0,
            call_id: call_id.to_string(),
            name: name.to_string(),
        },
        WireEvent::ToolCallArgsDelta {
            index: 0,
            delta: args.to_string(),
        },
        WireEvent::ToolCallEnd { index: 0 },
        WireEvent::Done {
            stop: StopReason::ToolUse,
        },
    ]
}

/// Convenience: length-truncation script carrying an unterminated tool call.
pub fn length_script_with_tool_call(call_id: &str, name: &str) -> Vec<WireEvent> {
    vec![
        WireEvent::Start,
        WireEvent::TextDelta {
            text: "partial".into(),
        },
        WireEvent::ToolCallBegin {
            index: 0,
            call_id: call_id.to_string(),
            name: name.to_string(),
        },
        WireEvent::ToolCallArgsDelta {
            index: 0,
            delta: "{\"x\":1".into(),
        },
        WireEvent::Usage(Usage {
            input_tokens: 10,
            output_tokens: 8192,
            ..Default::default()
        }),
        WireEvent::Done {
            stop: StopReason::Length,
        },
    ]
}

/// Convenience: build a minimal tool spec list for scripts that reference tools.
pub fn tool_specs() -> Vec<ToolSpec> {
    vec![ToolSpec {
        name: "noop".into(),
        description: "does nothing (faux tests)".into(),
        parameters: serde_json::json!({"type": "object", "properties": {}}),
    }]
}

/// Convenience: text tool-result content.
pub fn text_content(s: &str) -> Vec<ToolContent> {
    vec![ToolContent::Text {
        text: s.to_string(),
    }]
}
