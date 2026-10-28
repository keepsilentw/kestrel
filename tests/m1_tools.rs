//! M1 acceptance tests: concurrency gates, three-state policy via headless
//! approval, tool-error loop continuity. Sandbox e2e lives in sandbox_e2e.rs.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{EngineSpec, SleepTool};
use kestrel::core::events::{EventMsg, MessageItem};
use kestrel::permissions::{Decision, PolicyTable, PrefixRule, Segment};
use kestrel::provider::faux::FauxWire;
use kestrel::tools::dispatch_batch;
use kestrel::tools::{ToolCall, ToolCtx, ToolRegistry};

fn ctx_for(cwd: std::path::PathBuf, approve: kestrel::tools::ApproveMode) -> ToolCtx {
    ToolCtx {
        cwd,
        role: "test".into(),
        fs: Default::default(),
        sandbox: Arc::new(kestrel::sandbox::Unsandboxed),
        engine: Arc::new(kestrel::permissions::PermissionEngine::new(
            Default::default(),
        )),
        approve,
        unsandboxed: false,
        reprepare_approved: false,
        reads: Default::default(),
    }
}

fn tool_call(id: &str, name: &str, args: serde_json::Value) -> ToolCall {
    ToolCall {
        call_id: id.into(),
        name: name.into(),
        args,
    }
}

/// Acceptance 1a: two parallel 200ms tools overlap -> wall clock < 350ms.
#[tokio::test]
async fn parallel_tools_overlap_in_time() {
    let registry = ToolRegistry::new(vec![
        Arc::new(SleepTool {
            name: "p1",
            ms: 200,
            sequential: false,
        }),
        Arc::new(SleepTool {
            name: "p2",
            ms: 200,
            sequential: false,
        }),
    ]);
    let calls = vec![
        tool_call("c1", "p1", serde_json::json!({})),
        tool_call("c2", "p2", serde_json::json!({})),
    ];
    let ctx = ctx_for(
        std::env::temp_dir(),
        kestrel::tools::ApproveMode::HeadlessDeny,
    );
    let t0 = Instant::now();
    let results = dispatch_batch(
        &registry,
        &calls,
        &ctx,
        &tokio_util::sync::CancellationToken::new(),
        &(Arc::new(|_m: EventMsg| {}) as kestrel::tools::orchestrator::EmitFn),
        &kestrel::core::approvals::ApprovalBoard::default(),
        &(Arc::new(kestrel::core::id::SeqGen::new()) as Arc<dyn kestrel::core::id::IdGen>),
    )
    .await;
    let wall = t0.elapsed();
    assert!(
        wall < Duration::from_millis(350),
        "parallel batch took {wall:?}"
    );
    // source order regardless of completion order
    assert_eq!(results[0].0.call_id, "c1");
    assert_eq!(results[1].0.call_id, "c2");
    assert!(!results[0].1.is_error);
}

/// Acceptance 1b: a sequential tool excludes parallel ones -> wall >= ~350ms.
#[tokio::test]
async fn sequential_tool_excludes_parallel() {
    let registry = ToolRegistry::new(vec![
        Arc::new(SleepTool {
            name: "s1",
            ms: 200,
            sequential: true,
        }),
        Arc::new(SleepTool {
            name: "p2",
            ms: 200,
            sequential: false,
        }),
    ]);
    let calls = vec![
        tool_call("c1", "s1", serde_json::json!({})),
        tool_call("c2", "p2", serde_json::json!({})),
    ];
    let ctx = ctx_for(
        std::env::temp_dir(),
        kestrel::tools::ApproveMode::HeadlessDeny,
    );
    let t0 = Instant::now();
    let results = dispatch_batch(
        &registry,
        &calls,
        &ctx,
        &tokio_util::sync::CancellationToken::new(),
        &(Arc::new(|_m: EventMsg| {}) as kestrel::tools::orchestrator::EmitFn),
        &kestrel::core::approvals::ApprovalBoard::default(),
        &(Arc::new(kestrel::core::id::SeqGen::new()) as Arc<dyn kestrel::core::id::IdGen>),
    )
    .await;
    let wall = t0.elapsed();
    assert!(
        wall >= Duration::from_millis(350),
        "sequential exclusivity violated: {wall:?}"
    );
    assert_eq!(results[0].0.call_id, "c1");
    assert_eq!(results[1].0.call_id, "c2");
}

/// Acceptance 4: three-state policy through bash prepare + headless resolution.
#[tokio::test]
async fn three_state_policy_headless() {
    let registry = ToolRegistry::new(vec![Arc::new(kestrel::tools::bash::Bash)]);
    let policy = PolicyTable {
        default: Decision::Ask,
        rules: vec![
            PrefixRule {
                pattern: vec![Segment::One("rg".into())],
                decision: Decision::Allow,
            },
            PrefixRule {
                pattern: vec![Segment::One("rm".into())],
                decision: Decision::Deny,
            },
        ],
        forbidden_prefixes: vec![vec!["bash".into()]],
    };
    let mut ctx = ctx_for(
        std::env::temp_dir(),
        kestrel::tools::ApproveMode::HeadlessDeny,
    );
    ctx.engine = Arc::new(kestrel::permissions::PermissionEngine::new(policy));
    let emit: kestrel::tools::orchestrator::EmitFn = Arc::new(|_m| {});
    let approvals = kestrel::core::approvals::ApprovalBoard::default();
    let idgen: Arc<dyn kestrel::core::id::IdGen> = Arc::new(kestrel::core::id::SeqGen::new());
    let token = tokio_util::sync::CancellationToken::new();

    // rg: allow -> executes (echo marker proves real run)
    let r = dispatch_batch(
        &registry,
        &[tool_call(
            "a",
            "bash",
            serde_json::json!({"command": "rg --version"}),
        )],
        &ctx,
        &token,
        &emit,
        &approvals,
        &idgen,
    )
    .await
    .remove(0);
    assert!(!r.1.is_error, "{:?}", r.1);

    // rm: deny -> blocked with policy copy
    let r = dispatch_batch(
        &registry,
        &[tool_call(
            "b",
            "bash",
            serde_json::json!({"command": "rm -rf /tmp/x"}),
        )],
        &ctx,
        &token,
        &emit,
        &approvals,
        &idgen,
    )
    .await
    .remove(0);
    assert!(r.1.is_error && r.1.content_text().contains("denied by policy"));

    // git push: default ask + headless none -> denied with --approve copy
    let r = dispatch_batch(
        &registry,
        &[tool_call(
            "c",
            "bash",
            serde_json::json!({"command": "git push origin main"}),
        )],
        &ctx,
        &token,
        &emit,
        &approvals,
        &idgen,
    )
    .await
    .remove(0);
    assert!(
        r.1.is_error && r.1.content_text().contains("--approve none"),
        "{:?}",
        r.1
    );

    // unparseable ($VAR): fail-closed ask -> headless deny
    let r = dispatch_batch(
        &registry,
        &[tool_call(
            "d",
            "bash",
            serde_json::json!({"command": "echo $HOME"}),
        )],
        &ctx,
        &token,
        &emit,
        &approvals,
        &idgen,
    )
    .await
    .remove(0);
    assert!(r.1.is_error);

    // interpreter prefix: forbidden even if a rule would allow it
    let r = dispatch_batch(
        &registry,
        &[tool_call(
            "e",
            "bash",
            serde_json::json!({"command": "bash -c 'echo hi'"}),
        )],
        &ctx,
        &token,
        &emit,
        &approvals,
        &idgen,
    )
    .await
    .remove(0);
    assert!(r.1.is_error && r.1.content_text().contains("denied by policy"));
}

/// Acceptance 7: tool errors never break the loop — the is_error result is
/// rendered into the next request and the conversation converges.
#[tokio::test]
async fn tool_error_fed_back_and_loop_continues() {
    let wire = Arc::new(FauxWire::new());
    wire.push_turn_script(kestrel::provider::faux::tool_use_script(
        "let me read",
        "call-r",
        "read",
        serde_json::json!({"path": "/definitely/not/here.txt"}),
    ));
    wire.push_turn_script(kestrel::provider::faux::stop_script("recovered"));
    let spec = EngineSpec {
        tools: vec![Arc::new(kestrel::tools::read::Read)],
        ..Default::default()
    };
    let (handle, mut rx) = common::spawn_engine(Arc::clone(&wire), spec);
    handle.turn(vec![MessageItem::user_text("go")]);
    let (_events, stop) = common::collect_until_complete(&mut rx).await;
    assert!(matches!(stop, kestrel::provider::wire::StopReason::Stop));
    let recorded = wire.recorded_requests();
    assert_eq!(recorded.len(), 2);
    let round2 = &recorded[1]["items"];
    assert!(
        round2
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["type"] == "tool_result" && i["is_error"] == true),
        "round 2 carries the is_error tool result: {round2:?}"
    );
    handle.shutdown().await;
}

/// Approval reverse-request path (Bus mode): NeedsApproval -> ApprovalRequest
/// event -> Op::Approval(AllowOnce) -> executes. The request must also carry
/// the data an approver judges on (tool + args), not just prose.
#[tokio::test]
async fn approval_reverse_request_roundtrip() {
    let registry = ToolRegistry::new(vec![Arc::new(kestrel::tools::bash::Bash)]);
    let mut ctx = ctx_for(
        std::env::temp_dir(),
        kestrel::tools::ApproveMode::Bus {
            timeout: Duration::from_secs(30),
        },
    );
    ctx.engine = Arc::new(kestrel::permissions::PermissionEngine::new(PolicyTable {
        default: Decision::Ask,
        rules: vec![],
        forbidden_prefixes: vec![],
    }));
    let approvals = Arc::new(kestrel::core::approvals::ApprovalBoard::default());
    let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel();
    let emit: kestrel::tools::orchestrator::EmitFn = Arc::new(move |m| {
        let _ = req_tx.send(m);
    });
    let idgen: Arc<dyn kestrel::core::id::IdGen> = Arc::new(kestrel::core::id::SeqGen::new());
    let token = tokio_util::sync::CancellationToken::new();
    let handle = tokio_util::task::AbortOnDropHandle::new(tokio::spawn({
        let registry = ToolRegistry::clone(&registry);
        let ctx = ctx.clone();
        let approvals = Arc::clone(&approvals);
        let idgen = Arc::clone(&idgen);
        let token = token.clone();
        async move {
            dispatch_batch(
                &registry,
                &[tool_call(
                    "a",
                    "bash",
                    serde_json::json!({"command": "echo reverse-ok"}),
                )],
                &ctx,
                &token,
                &emit,
                &approvals,
                &idgen,
            )
            .await
        }
    }));

    // Wait for ApprovalRequest, then approve via the board (Op::Approval path).
    let mut approval_req = None;
    for _ in 0..50 {
        match tokio::time::timeout(Duration::from_millis(100), req_rx.recv()).await {
            Ok(Some(EventMsg::ApprovalRequest { req, .. })) => {
                approval_req = Some(req);
                break;
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    let req = approval_req.expect("ApprovalRequest emitted");
    assert!(approvals.complete(&req, kestrel::core::ops::ApprovalDecision::AllowOnce));

    let results = handle.await.unwrap();
    assert!(!results[0].1.is_error, "{:?}", results[0].1);
    assert!(results[0].1.content_text().contains("reverse-ok"));
}

/// A silent approver must not hold a turn open forever: `approve = "bus"`
/// carries a deadline, and past it the worker denies the call itself
/// (fail closed, design §5 纪律 2).
#[tokio::test]
async fn bus_approval_without_answer_denies_on_deadline() {
    let registry = ToolRegistry::new(vec![Arc::new(kestrel::tools::bash::Bash)]);
    let mut ctx = ctx_for(
        std::env::temp_dir(),
        kestrel::tools::ApproveMode::Bus {
            timeout: Duration::from_millis(120),
        },
    );
    ctx.engine = Arc::new(kestrel::permissions::PermissionEngine::new(PolicyTable {
        default: Decision::Ask,
        rules: vec![],
        forbidden_prefixes: vec![],
    }));
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel();
    let emit: kestrel::tools::orchestrator::EmitFn = Arc::new(move |m| {
        let _ = seen_tx.send(m);
    });
    let started = Instant::now();
    let outcome = dispatch_batch(
        &registry,
        &[tool_call(
            "a",
            "bash",
            serde_json::json!({"command": "echo silent-approver"}),
        )],
        &ctx,
        &tokio_util::sync::CancellationToken::new(),
        &emit,
        &kestrel::core::approvals::ApprovalBoard::default(),
        &(Arc::new(kestrel::core::id::SeqGen::new()) as Arc<dyn kestrel::core::id::IdGen>),
    )
    .await
    .into_iter()
    .next()
    .expect("one outcome")
    .1;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the deadline bounds the wait, not the bell timeout"
    );
    assert!(outcome.is_error);
    let text = outcome.content_text();
    assert!(
        text.contains("timed out"),
        "expected a timeout denial: {text}"
    );

    let mut judged = false;
    while let Ok(msg) = seen_rx.try_recv() {
        if let EventMsg::ApprovalRequest { detail, .. } = msg {
            assert_eq!(detail["tool"], "bash", "approver sees the tool");
            assert_eq!(
                detail["args"]["command"], "echo silent-approver",
                "approver sees the command"
            );
            judged = true;
        }
    }
    assert!(judged, "an ApprovalRequest was emitted with tool + args");
}

/// One call through the real orchestrator, returning its outcome.
async fn one_call(
    registry: &ToolRegistry,
    ctx: &ToolCtx,
    name: &str,
    args: serde_json::Value,
) -> kestrel::tools::ToolOutcome {
    let results = dispatch_batch(
        registry,
        &[tool_call("c1", name, args)],
        ctx,
        &tokio_util::sync::CancellationToken::new(),
        &(Arc::new(|_m: EventMsg| {}) as kestrel::tools::orchestrator::EmitFn),
        &kestrel::core::approvals::ApprovalBoard::default(),
        &(Arc::new(kestrel::core::id::SeqGen::new()) as Arc<dyn kestrel::core::id::IdGen>),
    )
    .await;
    results.into_iter().next().expect("one result").1
}

/// Acceptance 5: edit is gated on having read the exact version, and echoes
/// back the rewritten lines so the model can check where it landed.
#[tokio::test]
async fn edit_requires_a_read_of_the_same_version() {
    let ws = std::env::temp_dir()
        .canonicalize()
        .unwrap()
        .join(format!("kestrel-edit-guard-{}", std::process::id()));
    std::fs::create_dir_all(&ws).unwrap();
    let file = ws.join("a.rs");
    std::fs::write(&file, "fn a() {\n    one();\n}\n").unwrap();

    let registry = ToolRegistry::new(vec![
        Arc::new(kestrel::tools::read::Read) as Arc<dyn kestrel::tools::Tool>,
        Arc::new(kestrel::tools::edit::Edit),
    ]);
    let mut ctx = ctx_for(ws.clone(), kestrel::tools::ApproveMode::HeadlessDeny);
    ctx.fs.writable_roots = vec![ws.clone()];

    // (1) never read -> refused, and the refusal names the file
    let r = one_call(
        &registry,
        &ctx,
        "edit",
        serde_json::json!({"path": file.to_str().unwrap(), "edits": [{"old": "one();", "new": "two();"}]}),
    )
    .await;
    assert!(r.is_error, "{r:?}");
    assert!(r.content_text().contains("has not been read"), "{r:?}");
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "fn a() {\n    one();\n}\n",
        "refused edit must not touch the file"
    );

    // (2) read it -> edit lands, and the outcome echoes the result by line
    let r = one_call(
        &registry,
        &ctx,
        "read",
        serde_json::json!({"path": file.to_str().unwrap()}),
    )
    .await;
    assert!(!r.is_error, "{r:?}");
    let r = one_call(
        &registry,
        &ctx,
        "edit",
        serde_json::json!({"path": file.to_str().unwrap(), "edits": [{"old": "one();", "new": "two();"}]}),
    )
    .await;
    assert!(!r.is_error, "{r:?}");
    let text = r.content_text();
    assert!(text.contains("applied 1 edit(s)"), "{text}");
    assert!(text.contains("--- edit 1 (line 2) ---"), "{text}");
    assert!(text.contains("     2|     two();"), "{text}");
    assert!(text.contains("     3| }"), "{text}"); // context line
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "fn a() {\n    two();\n}\n"
    );

    // (3) the file changes underneath -> refused until re-read
    std::fs::write(&file, "fn a() {\n    two();\n    three();\n}\n").unwrap();
    let r = one_call(
        &registry,
        &ctx,
        "edit",
        serde_json::json!({"path": file.to_str().unwrap(), "edits": [{"old": "three();", "new": "four();"}]}),
    )
    .await;
    assert!(r.is_error, "{r:?}");
    assert!(r.content_text().contains("changed on disk"), "{r:?}");

    let r = one_call(
        &registry,
        &ctx,
        "read",
        serde_json::json!({"path": file.to_str().unwrap()}),
    )
    .await;
    assert!(!r.is_error, "{r:?}");
    let r = one_call(
        &registry,
        &ctx,
        "edit",
        serde_json::json!({"path": file.to_str().unwrap(), "edits": [{"old": "three();", "new": "four();"}]}),
    )
    .await;
    assert!(!r.is_error, "{r:?}");
    assert!(std::fs::read_to_string(&file).unwrap().contains("four();"));
    std::fs::remove_dir_all(&ws).ok();
}

/// Acceptance 6: a non-unique anchor is refused instead of silently rewriting
/// the first occurrence.
#[tokio::test]
async fn edit_refuses_an_ambiguous_anchor() {
    let ws = std::env::temp_dir()
        .canonicalize()
        .unwrap()
        .join(format!("kestrel-edit-ambig-{}", std::process::id()));
    std::fs::create_dir_all(&ws).unwrap();
    let file = ws.join("dup.txt");
    std::fs::write(&file, "x = 1;\ny = 0;\nx = 1;\n").unwrap();

    let registry = ToolRegistry::new(vec![
        Arc::new(kestrel::tools::read::Read) as Arc<dyn kestrel::tools::Tool>,
        Arc::new(kestrel::tools::edit::Edit),
    ]);
    let mut ctx = ctx_for(ws.clone(), kestrel::tools::ApproveMode::HeadlessDeny);
    ctx.fs.writable_roots = vec![ws.clone()];

    one_call(
        &registry,
        &ctx,
        "read",
        serde_json::json!({"path": file.to_str().unwrap()}),
    )
    .await;

    let r = one_call(
        &registry,
        &ctx,
        "edit",
        serde_json::json!({"path": file.to_str().unwrap(), "edits": [{"old": "x = 1;", "new": "x = 9;"}]}),
    )
    .await;
    assert!(r.is_error, "{r:?}");
    let text = r.content_text();
    assert!(text.contains("not unique"), "{text}");
    assert!(text.contains("2 matches"), "{text}");
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "x = 1;\ny = 0;\nx = 1;\n",
        "refused edit must not touch the file"
    );
    std::fs::remove_dir_all(&ws).ok();
}
