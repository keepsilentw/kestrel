//! M0 acceptance tests (plan §4-M0): all judged via `cargo test`, zero real keys.
//! Every test drives the engine through the same WireFormat trait as production.

use std::sync::Arc;
use std::time::Duration;

use kestrel::core::actor::{AgentHandle, EngineConfig};
use kestrel::core::events::{Event, EventMsg, MessageItem};
use kestrel::core::id::SeqGen;
use kestrel::provider::faux::FauxWire;
use kestrel::provider::wire::{NullTransport, ResolvedAuth, StopReason};

fn fixture(name: &str) -> std::path::PathBuf {
    [env!("CARGO_MANIFEST_DIR"), "tests", "fixtures", name]
        .iter()
        .collect()
}

fn spawn_engine(
    wire: Arc<FauxWire>,
    max_rounds: u32,
) -> (AgentHandle, tokio::sync::mpsc::UnboundedReceiver<Event>) {
    let handle = AgentHandle::spawn(EngineConfig {
        provider: wire,
        model: kestrel::provider::wire::ModelMeta::minimal(
            "faux-model",
            kestrel::provider::wire::WireId::AnthropicMessages,
            "faux://unused",
        ),
        auth: ResolvedAuth::key("test"),
        transport: Arc::new(NullTransport),
        system: "test system".into(),
        tools: vec![],
        max_rounds,
        cwd: std::env::temp_dir(),
        role: "test".into(),
        idgen: Arc::new(SeqGen::new()),
        fs: Default::default(),
        sandbox: Arc::new(kestrel::sandbox::Unsandboxed),
        engine: Arc::new(kestrel::permissions::PermissionEngine::new(
            Default::default(),
        )),
        approve: kestrel::tools::ApproveMode::HeadlessDeny,
        session: None,
    });
    let rx = handle.take_event_rx().expect("single consumer");
    (handle, rx)
}

/// Collect events until TurnComplete; returns (events, stop).
async fn collect_until_complete(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
) -> (Vec<Event>, StopReason) {
    let mut out = Vec::new();
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("event stream timed out")
            .expect("event rx closed before TurnComplete");
        let done = matches!(
            &ev.msg,
            EventMsg::TurnComplete { .. } | EventMsg::Fatal { .. }
        );
        let stop = match &ev.msg {
            EventMsg::TurnComplete { stop, .. } => Some(stop.clone()),
            EventMsg::Fatal { .. } => Some(StopReason::error("fatal", false)),
            _ => None,
        };
        out.push(ev);
        if let Some(stop) = stop {
            return (out, stop);
        }
        let _ = done;
    }
}

/// Acceptance 1: ToolUse fixture drives a follow-up round; second fixture converges.
#[tokio::test]
async fn tooluse_drives_follow_up_then_converges() {
    let wire = Arc::new(FauxWire::new());
    // Round 1 from fixture file; round 2 inline Stop script.
    let script1 = FauxWire::from_fixture(&fixture("tooluse_then_stop.jsonl")).unwrap();
    wire.push_scripted_turn(script1);
    wire.push_turn_script(kestrel::provider::faux::stop_script("final answer"));

    let (handle, mut rx) = spawn_engine(Arc::clone(&wire), 8);
    handle.turn(vec![MessageItem::user_text("go")]);

    let (events, stop) = collect_until_complete(&mut rx).await;
    assert!(
        matches!(stop, StopReason::Stop),
        "expected Stop, got {stop:?}"
    );
    // two rounds -> two text deltas streams, one is_error tool result
    let tool_ends: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e.msg, EventMsg::ToolEnd { .. }))
        .collect();
    assert_eq!(tool_ends.len(), 1, "one unknown-tool dispatch");
    match &tool_ends[0].msg {
        EventMsg::ToolEnd { is_error, .. } => assert!(is_error, "unknown tool -> is_error result"),
        _ => unreachable!(),
    }
    // unknown tool error was recorded in history and rendered into round-2 request
    let recorded = wire.recorded_requests();
    assert_eq!(recorded.len(), 2, "two sampling requests");
    let round2 = &recorded[1];
    let items = round2["items"].as_array().unwrap();
    assert!(
        items
            .iter()
            .any(|i| i["type"] == "tool_result" && i["is_error"] == true),
        "round 2 prompt carries the error tool result"
    );
    handle.shutdown().await;
}

/// Acceptance 3: length truncation fails the whole batch, nothing executes, no follow-up.
#[tokio::test]
async fn length_truncation_fails_batch_without_executing() {
    let wire = Arc::new(FauxWire::new());
    let script = FauxWire::from_fixture(&fixture("length_toolcall.jsonl")).unwrap();
    wire.push_scripted_turn(script);
    let (handle, mut rx) = spawn_engine(wire, 8);
    handle.turn(vec![MessageItem::user_text("go")]);

    let (events, stop) = collect_until_complete(&mut rx).await;
    assert!(matches!(stop, StopReason::Length));
    // every tool call synthesized an is_error result; zero successful executions
    let tool_ends: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e.msg, EventMsg::ToolEnd { .. }))
        .collect();
    assert_eq!(tool_ends.len(), 1);
    match &tool_ends[0].msg {
        EventMsg::ToolEnd {
            is_error, output, ..
        } => {
            assert!(is_error);
            assert!(output.iter().any(|c| matches!(c, kestrel::provider::wire::ToolContent::Text { text } if text.contains("length limit"))));
        }
        _ => unreachable!(),
    }
    handle.shutdown().await;
}

/// Acceptance 1b: rounds hard cap stops the loop with BudgetStopped.
#[tokio::test]
async fn rounds_hard_cap_stops_budget() {
    let wire = Arc::new(FauxWire::new());
    // Endless tool-use scripts would loop forever without the cap.
    for _ in 0..10 {
        wire.push_turn_script(kestrel::provider::faux::tool_use_script(
            "hmm",
            "call-x",
            "noop",
            serde_json::json!({}),
        ));
    }
    let (handle, mut rx) = spawn_engine(wire, 3);
    handle.turn(vec![MessageItem::user_text("go")]);

    let (_events, stop) = collect_until_complete(&mut rx).await;
    match stop {
        StopReason::Error { message, .. } => assert!(message.contains("round budget")),
        other => panic!("expected budget stop, got {other:?}"),
    }
    handle.shutdown().await;
}

/// Acceptance 2: mid-stream interrupt -> TurnComplete{Aborted} fast + consumer drop observed.
#[tokio::test]
async fn interrupt_mid_stream_aborts_and_drops_consumer() {
    let wire = Arc::new(FauxWire::new());
    let script = FauxWire::from_fixture(&fixture("slow_deltas.jsonl")).unwrap();
    wire.push_scripted_turn(script);
    let mut drops = wire.subscribe_drops();

    let (handle, mut rx) = spawn_engine(wire, 8);
    handle.turn(vec![MessageItem::user_text("go")]);
    tokio::time::sleep(Duration::from_millis(30)).await; // let the stream start
    let t0 = std::time::Instant::now();
    handle.interrupt();

    let (events, stop) = collect_until_complete(&mut rx).await;
    assert!(
        matches!(stop, StopReason::Aborted),
        "expected Aborted, got {stop:?}"
    );
    assert!(
        t0.elapsed() < Duration::from_millis(900),
        "abort must be prompt, took {:?}",
        t0.elapsed()
    );
    // upstream faux stream observed the consumer drop
    tokio::time::timeout(Duration::from_secs(2), drops.recv())
        .await
        .expect("consumer drop not observed")
        .expect("drop channel open");
    // no partial text committed: no MessageDone for assistant text
    assert!(!events.iter().any(|e| matches!(
        &e.msg,
        EventMsg::MessageDone {
            item: MessageItem::AssistantText { .. }
        }
    )));
    handle.shutdown().await;
}

/// Acceptance 4: steering injected mid-turn lands in the next round's prompt.
#[tokio::test]
async fn steering_queues_into_next_round_prompt() {
    let wire = Arc::new(FauxWire::new());
    // Round 1: tool use (forces a follow-up round); steering arrives meanwhile.
    wire.push_turn_script(kestrel::provider::faux::tool_use_script(
        "working",
        "call-9",
        "noop",
        serde_json::json!({}),
    ));
    wire.push_turn_script(kestrel::provider::faux::stop_script("done with steering"));
    let (handle, mut rx) = spawn_engine(Arc::clone(&wire), 8);

    handle.turn(vec![MessageItem::user_text("go")]);
    // Steer while round 1 is in flight (queue drains at the round boundary).
    handle.steer(MessageItem::BusBell {
        sender: "test-user".into(),
        text: "also do X".into(),
    });

    let (_events, stop) = collect_until_complete(&mut rx).await;
    assert!(matches!(stop, StopReason::Stop));
    // Round 2 request must carry the steered item.
    let recorded = wire.recorded_requests();
    assert_eq!(recorded.len(), 2, "two sampling requests");
    let round2 = &recorded[1];
    let items = round2["items"].as_array().unwrap();
    assert!(
        items.iter().any(|i| i["type"] == "user_text"
            && i["text"]
                .as_str()
                .unwrap_or_default()
                .contains("<bell from=\"test-user\">also do X</bell>")),
        "steered item rendered with source tag into round-2 prompt: {items:?}"
    );
    handle.shutdown().await;
}
