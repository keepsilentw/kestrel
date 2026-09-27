//! Real-wire acceptance: fixture .sse bytes through AnthropicWire::parse_stream,
//! same path as live traffic (faux discipline applies to the byte stream too).

use futures::StreamExt;
use kestrel::provider::anthropic::AnthropicWire;
use kestrel::provider::wire::{ByteStream, ModelMeta, WireEvent, WireFormat, WireId};

#[tokio::test]
async fn tool_call_sse_fixture_normalizes_exactly_once() {
    let path: std::path::PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "tests",
        "fixtures",
        "anthropic_tool_call.sse",
    ]
    .iter()
    .collect();
    let bytes = std::fs::read(&path).expect("fixture readable");
    let byte_stream: ByteStream = Box::pin(futures::stream::once(async move {
        Ok(bytes::Bytes::from(bytes))
    }));

    let wire = AnthropicWire;
    let model = ModelMeta::minimal("m", WireId::AnthropicMessages, "https://x");
    let mut stream = wire.parse_stream(&model, byte_stream);
    let mut begins = Vec::new();
    let mut ends = Vec::new();
    let mut deltas = Vec::new();
    let mut done = 0;
    while let Some(ev) = stream.next().await {
        match ev {
            WireEvent::ToolCallBegin { call_id, name, .. } => begins.push((call_id, name)),
            WireEvent::ToolCallArgsDelta { delta, .. } => deltas.push(delta),
            WireEvent::ToolCallEnd { .. } => ends.push(()),
            WireEvent::Done { .. } => done += 1,
            _ => {}
        }
    }
    // text block stop must NOT emit ToolCallEnd; only the tool block does
    assert_eq!(begins, vec![("toolu_1".to_string(), "noop".to_string())]);
    assert_eq!(deltas, vec!["{\"k\":1}".to_string()]);
    assert_eq!(ends.len(), 1, "exactly one ToolCallEnd (tool block only)");
    assert_eq!(done, 1, "Done fires exactly once");
}

#[tokio::test]
async fn stream_ending_without_done_synthesizes_aborted() {
    // Server closes early: the normalized stream still terminates with Aborted.
    let byte_stream: ByteStream = Box::pin(futures::stream::once(async move {
        Ok(bytes::Bytes::from("event: message_start\ndata: {}\n\n"))
    }));
    let wire = AnthropicWire;
    let model = ModelMeta::minimal("m", WireId::AnthropicMessages, "https://x");
    let mut stream = wire.parse_stream(&model, byte_stream);
    let mut last = None;
    while let Some(ev) = stream.next().await {
        last = Some(ev);
    }
    assert!(matches!(
        last,
        Some(WireEvent::Done {
            stop: kestrel::provider::wire::StopReason::Aborted
        })
    ));
}
