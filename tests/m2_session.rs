//! M2 acceptance tests (plan §4-M2): golden session file, compaction trigger
//! + lossless checkpoint, update-prompt merge, model switch persistence.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

use kestrel::core::actor::{AgentHandle, EngineConfig};
use kestrel::core::events::{Event, EventMsg};
use kestrel::core::id::SeqGen;
use kestrel::permissions::PermissionEngine;
use kestrel::provider::faux::FauxWire;
use kestrel::provider::wire::{ModelMeta, NullTransport, ResolvedAuth, WireId};
use kestrel::sandbox::{FsPolicy, Unsandboxed};
use kestrel::session::SessionStore;
use kestrel::tools::ApproveMode;

struct FakeClock(AtomicI64);
impl kestrel::core::id::Clock for FakeClock {
    fn now_ms(&self) -> i64 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn tiny_model() -> ModelMeta {
    let mut m = ModelMeta::minimal("tiny", WireId::AnthropicMessages, "faux://unused");
    m.context_window = 1000;
    m.max_out = 100;
    m
}

fn fixture(name: &str) -> PathBuf {
    [env!("CARGO_MANIFEST_DIR"), "tests", "fixtures", name]
        .iter()
        .collect()
}

fn spawn(
    wire: Arc<FauxWire>,
    model: ModelMeta,
    store: Arc<SessionStore>,
    max_rounds: u32,
) -> (AgentHandle, tokio::sync::mpsc::UnboundedReceiver<Event>) {
    let handle = AgentHandle::spawn(EngineConfig {
        provider: wire,
        model,
        auth: ResolvedAuth::key("test"),
        transport: Arc::new(NullTransport),
        system: String::new(),
        tools: vec![],
        max_rounds,
        cwd: PathBuf::from("/ws"),
        role: "rd".into(),
        idgen: Arc::new(SeqGen::new()),
        fs: FsPolicy::default(),
        sandbox: Arc::new(Unsandboxed),
        engine: Arc::new(PermissionEngine::new(Default::default())),
        approve: ApproveMode::HeadlessDeny,
        session: Some(store),
    });
    let rx = handle.take_event_rx().unwrap();
    (handle, rx)
}

fn new_store(tag: &str) -> Arc<SessionStore> {
    let home = std::env::temp_dir().join(format!("kestrel-m2-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    Arc::new(
        SessionStore::create(
            &home,
            std::path::Path::new("/ws"),
            "rd",
            "faux",
            "tiny",
            Arc::new(SeqGen::new()),
            Arc::new(FakeClock(AtomicI64::new(1000))),
        )
        .unwrap(),
    )
}

/// Acceptance 1: seeded session file is byte-equal to the golden fixture.
#[tokio::test]
async fn golden_session_file() {
    let store = new_store("golden");
    let wire = Arc::new(FauxWire::new());
    wire.push_turn_script(kestrel::provider::faux::stop_script("hello"));
    let (handle, mut rx) = spawn(Arc::clone(&wire), tiny_model(), Arc::clone(&store), 8);
    handle.turn(vec![kestrel::core::events::MessageItem::user_text("go")]);
    let (_events, stop) = common::collect_until_complete(&mut rx).await;
    assert!(matches!(stop, kestrel::provider::wire::StopReason::Stop));
    handle.shutdown().await;

    let actual = std::fs::read_to_string(store.path()).unwrap();
    let expected = std::fs::read_to_string(fixture("session_golden.jsonl")).unwrap();
    assert_eq!(
        actual, expected,
        "session file must match golden byte-for-byte"
    );
}

/// Acceptance 4+5: threshold triggers compaction; second trigger uses the
/// update prompt (merge); Compacted events emitted; file gains checkpoints.
#[tokio::test]
async fn compaction_triggers_and_merges() {
    let store = new_store("compact");
    let wire = Arc::new(FauxWire::new());
    // r1: tool-use with heavy usage -> follow-up round; ctx=750 >= soft(700)
    wire.push_turn_script(vec![
        kestrel::provider::wire::WireEvent::Start,
        kestrel::provider::wire::WireEvent::ToolCallBegin {
            index: 0,
            call_id: "c1".into(),
            name: "noop".into(),
        },
        kestrel::provider::wire::WireEvent::ToolCallArgsDelta {
            index: 0,
            delta: "{}".into(),
        },
        kestrel::provider::wire::WireEvent::ToolCallEnd { index: 0 },
        kestrel::provider::wire::WireEvent::Usage(kestrel::provider::wire::Usage {
            input_tokens: 700,
            output_tokens: 50,
            ..Default::default()
        }),
        kestrel::provider::wire::WireEvent::Done {
            stop: kestrel::provider::wire::StopReason::ToolUse,
        },
    ]);
    // compaction sampling request
    wire.push_turn_script(kestrel::provider::faux::stop_script(
        "the summary of old work",
    ));
    // r2: real sampling after compaction — heavy usage again -> second trigger
    wire.push_turn_script(vec![
        kestrel::provider::wire::WireEvent::Start,
        kestrel::provider::wire::WireEvent::ToolCallBegin {
            index: 0,
            call_id: "c2".into(),
            name: "noop".into(),
        },
        kestrel::provider::wire::WireEvent::ToolCallArgsDelta {
            index: 0,
            delta: "{}".into(),
        },
        kestrel::provider::wire::WireEvent::ToolCallEnd { index: 0 },
        kestrel::provider::wire::WireEvent::Usage(kestrel::provider::wire::Usage {
            input_tokens: 800,
            output_tokens: 50,
            ..Default::default()
        }),
        kestrel::provider::wire::WireEvent::Done {
            stop: kestrel::provider::wire::StopReason::ToolUse,
        },
    ]);
    // second compaction (update prompt)
    wire.push_turn_script(kestrel::provider::faux::stop_script("merged summary"));
    // r3: converge
    wire.push_turn_script(kestrel::provider::faux::stop_script("done"));

    let (handle, mut rx) = spawn(Arc::clone(&wire), tiny_model(), Arc::clone(&store), 8);
    handle.turn(vec![kestrel::core::events::MessageItem::user_text("go")]);
    let (events, stop) = common::collect_until_complete(&mut rx).await;
    assert!(matches!(stop, kestrel::provider::wire::StopReason::Stop));

    let compacted = events
        .iter()
        .filter(|e| matches!(e.msg, EventMsg::Compacted { .. }))
        .count();
    assert_eq!(compacted, 2, "two compactions");

    let recorded = wire.recorded_requests();
    // request 0: initial round; 1: first compaction; 2: round-2; 3: second compaction; 4: round-3
    assert_eq!(recorded.len(), 5, "{:?}", recorded.len());
    let comp1 = serde_json::to_string(&recorded[1]).unwrap();
    assert!(
        comp1.contains("TRANSCRIPT:"),
        "first compaction uses the fresh prompt"
    );
    assert!(!comp1.contains("PREVIOUS SUMMARY:"));
    let comp2 = serde_json::to_string(&recorded[3]).unwrap();
    assert!(
        comp2.contains("PREVIOUS SUMMARY:"),
        "second compaction uses the update prompt"
    );
    assert!(
        comp2.contains("the summary of old work"),
        "previous summary fed into merge"
    );
    // post-compaction prompts start with the summary marker
    let r2_first = recorded[2]["items"][0]["text"].as_str().unwrap();
    assert!(r2_first.starts_with("[context summary]"), "{r2_first}");
    // lossless: file only grows, contains two checkpoints
    let entries = store.load();
    assert!(
        entries
            .iter()
            .any(|e| matches!(e, kestrel::session::Entry::Compaction { .. }))
    );
    handle.shutdown().await;
}

/// §9.7③ regression: gateways that report input_tokens=0 (BigModel) still
/// trigger compaction — the request-body char-estimate covers the gap.
#[tokio::test]
async fn compaction_triggers_on_estimated_context() {
    let store = new_store("estimate");
    let wire = Arc::new(FauxWire::new());
    // Long user text inflates the request body; usage events report input=0.
    let long_task = "x".repeat(3200); // ~800 est tokens >= soft limit(700)
    wire.push_turn_script(vec![
        kestrel::provider::wire::WireEvent::Start,
        kestrel::provider::wire::WireEvent::Usage(kestrel::provider::wire::Usage {
            input_tokens: 0,
            output_tokens: 20,
            ..Default::default()
        }),
        kestrel::provider::wire::WireEvent::ToolCallBegin {
            index: 0,
            call_id: "c1".into(),
            name: "noop".into(),
        },
        kestrel::provider::wire::WireEvent::ToolCallArgsDelta {
            index: 0,
            delta: "{}".into(),
        },
        kestrel::provider::wire::WireEvent::ToolCallEnd { index: 0 },
        kestrel::provider::wire::WireEvent::Done {
            stop: kestrel::provider::wire::StopReason::ToolUse,
        },
    ]);
    // compaction sampling (summary) then converging round
    wire.push_turn_script(kestrel::provider::faux::stop_script(
        "summary from estimate path",
    ));
    wire.push_turn_script(kestrel::provider::faux::stop_script("done"));

    let (handle, mut rx) = spawn(Arc::clone(&wire), tiny_model(), Arc::clone(&store), 8);
    handle.turn(vec![kestrel::core::events::MessageItem::user_text(
        long_task,
    )]);
    let (events, stop) = common::collect_until_complete(&mut rx).await;
    assert!(matches!(stop, kestrel::provider::wire::StopReason::Stop));
    let compacted = events
        .iter()
        .filter(|e| matches!(e.msg, EventMsg::Compacted { .. }))
        .count();
    assert_eq!(
        compacted, 1,
        "estimate-based trigger fired once (input_tokens=0)"
    );
    handle.shutdown().await;
}

/// Acceptance 5: Op::SwitchModel appends a ModelChange entry and routes the
/// next sampling request to the new provider.
#[tokio::test]
async fn model_switch_persists_and_routes() {
    let store = new_store("switch");
    let wire1 = Arc::new(FauxWire::new());
    let wire2 = Arc::new(FauxWire::new());
    wire2.push_turn_script(kestrel::provider::faux::stop_script("from wire2"));
    let (handle, mut rx) = spawn(Arc::clone(&wire1), tiny_model(), Arc::clone(&store), 8);
    handle.submit(kestrel::core::ops::Op::SwitchModel {
        spec: Box::new(kestrel::core::actor::SwitchSpec {
            provider_name: "wire2".into(),
            provider: wire2.clone(),
            model: tiny_model(),
            auth: ResolvedAuth::key("test"),
        }),
    });
    handle.turn(vec![kestrel::core::events::MessageItem::user_text("go")]);
    let (_events, stop) = common::collect_until_complete(&mut rx).await;
    assert!(matches!(stop, kestrel::provider::wire::StopReason::Stop));
    assert_eq!(wire2.recorded_requests().len(), 1, "routing moved to wire2");
    assert_eq!(wire1.recorded_requests().len(), 0);
    let entries = store.load();
    assert!(
        entries
            .iter()
            .any(|e| matches!(e, kestrel::session::Entry::ModelChange { provider, .. } if provider == "wire2")),
        "ModelChange entry persisted"
    );
    handle.shutdown().await;
}

fn reopen(store: &SessionStore, ms: i64) -> Arc<SessionStore> {
    Arc::new(
        SessionStore::open(
            store.path(),
            Arc::new(SeqGen::new()),
            Arc::new(FakeClock(AtomicI64::new(ms))),
        )
        .unwrap(),
    )
}

/// Regression (smoke 2026-10-26): a resumed tree must actually carry context.
/// This is what `serve` does per bell and what `-p --resume` does.
#[tokio::test]
async fn resumed_store_carries_history_into_the_next_turn() {
    let store = new_store("resume-carry");
    let wire = Arc::new(FauxWire::new());
    wire.push_turn_script(kestrel::provider::faux::stop_script("first answer"));
    let (handle, mut rx) = spawn(Arc::clone(&wire), tiny_model(), Arc::clone(&store), 8);
    handle.turn(vec![kestrel::core::events::MessageItem::user_text(
        "question one",
    )]);
    common::collect_until_complete(&mut rx).await;
    handle.shutdown().await;
    let before = std::fs::read_to_string(store.path()).unwrap();

    let reopened = reopen(&store, 2000);
    let wire2 = Arc::new(FauxWire::new());
    wire2.push_turn_script(kestrel::provider::faux::stop_script("second answer"));
    let (h2, mut rx2) = spawn(Arc::clone(&wire2), tiny_model(), Arc::clone(&reopened), 8);
    h2.turn(vec![kestrel::core::events::MessageItem::user_text(
        "question two",
    )]);
    common::collect_until_complete(&mut rx2).await;
    h2.shutdown().await;

    let rendered = serde_json::to_string(&wire2.recorded_requests()).unwrap();
    assert!(
        rendered.contains("first answer"),
        "resumed engine lost the earlier assistant turn: {rendered}"
    );
    assert!(
        rendered.contains("question one"),
        "resumed engine lost the earlier user turn: {rendered}"
    );
    // Append-only: the previously written lines are untouched.
    let after = std::fs::read_to_string(store.path()).unwrap();
    assert!(after.starts_with(before.as_str()), "session was truncated");
    assert!(after.len() > before.len(), "second turn wrote nothing");
}

/// Resume also restores the context *meter*: an oversized tree must compact
/// before the first sample, not after it (compaction was otherwise unreachable
/// from any CLI path).
#[tokio::test]
async fn resume_compacts_before_the_first_sample() {
    let store = new_store("resume-compact");
    for i in 0..8 {
        store.append_item(
            kestrel::core::events::MessageItem::user_text(format!("chunk{i}: {}", "y".repeat(400))),
            None,
        );
    }
    let before = std::fs::read_to_string(store.path()).unwrap();
    let reopened = reopen(&store, 2000);

    let wire = Arc::new(FauxWire::new());
    // request 1: the compaction summary; request 2: the real turn.
    wire.push_turn_script(kestrel::provider::faux::stop_script("resumed summary"));
    wire.push_turn_script(kestrel::provider::faux::stop_script("answer after resume"));

    let (handle, mut rx) = spawn(Arc::clone(&wire), tiny_model(), Arc::clone(&reopened), 8);
    handle.turn(vec![kestrel::core::events::MessageItem::user_text(
        "next bell",
    )]);
    let (events, _stop) = common::collect_until_complete(&mut rx).await;
    handle.shutdown().await;

    assert!(
        events
            .iter()
            .any(|e| matches!(e.msg, EventMsg::Compacted { .. })),
        "a resumed oversized session must compact before sampling"
    );
    let recorded = wire.recorded_requests();
    assert_eq!(recorded.len(), 2, "{:?}", recorded.len());
    let first_item = recorded[1]["items"][0]["text"].as_str().unwrap_or_default();
    assert!(
        first_item.starts_with("[context summary]"),
        "post-compaction prompt must lead with the summary: {first_item}"
    );
    assert!(
        reopened
            .load()
            .iter()
            .any(|e| matches!(e, kestrel::session::Entry::Compaction { .. })),
        "checkpoint missing from the tree"
    );
    let after = std::fs::read_to_string(store.path()).unwrap();
    assert!(after.starts_with(before.as_str()), "session was truncated");
}
