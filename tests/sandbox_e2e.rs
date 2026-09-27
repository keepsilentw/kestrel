//! M1 acceptance: real Seatbelt sandbox e2e (macOS only, plan §4-M1 items 2-3).
//! Requires /usr/bin/sandbox-exec (present on every macOS).

#![cfg(target_os = "macos")]

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use kestrel::permissions::{Decision, PermissionEngine, PolicyTable};
use kestrel::sandbox::{FsPolicy, Seatbelt};
use kestrel::tools::bash::Bash;
use kestrel::tools::dispatch_batch;
use kestrel::tools::{ApproveMode, ToolCall, ToolCtx, ToolRegistry};

fn unique_ws(tag: &str) -> PathBuf {
    let ws = std::env::temp_dir().canonicalize().unwrap().join(format!(
        "kestrel-e2e-{}-{}",
        tag,
        std::process::id()
    ));
    std::fs::create_dir_all(&ws).unwrap();
    ws
}

fn sandbox_ctx(ws: &std::path::Path, approve: ApproveMode) -> ToolCtx {
    ToolCtx {
        cwd: ws.to_path_buf(),
        role: "e2e".into(),
        fs: FsPolicy {
            writable_roots: vec![ws.to_path_buf()],
            ..Default::default()
        },
        sandbox: Arc::new(Seatbelt),
        engine: Arc::new(PermissionEngine::new(PolicyTable {
            default: Decision::Allow,
            ..Default::default()
        })),
        approve,
        unsandboxed: false,
        reprepare_approved: false,
        reads: Default::default(),
    }
}

fn registry() -> ToolRegistry {
    ToolRegistry::new(vec![Arc::new(Bash)])
}

fn emit() -> kestrel::tools::orchestrator::EmitFn {
    Arc::new(|_m| {})
}

fn board() -> kestrel::core::approvals::ApprovalBoard {
    kestrel::core::approvals::ApprovalBoard::default()
}

fn idgen() -> Arc<dyn kestrel::core::id::IdGen> {
    Arc::new(kestrel::core::id::SeqGen::new())
}

fn call(id: &str, cmd: &str) -> ToolCall {
    ToolCall {
        call_id: id.into(),
        name: "bash".into(),
        args: serde_json::json!({"command": cmd}),
    }
}

/// Acceptance 2a: sandboxed write INSIDE the workspace succeeds.
#[tokio::test]
async fn sandboxed_write_inside_root_succeeds() {
    let ws = unique_ws("in");
    let ctx = sandbox_ctx(&ws, ApproveMode::HeadlessDeny);
    let results = dispatch_batch(
        &registry(),
        &[call("a", "touch ok.txt && test -f ok.txt")],
        &ctx,
        &tokio_util::sync::CancellationToken::new(),
        &emit(),
        &board(),
        &idgen(),
    )
    .await;
    assert!(!results[0].1.is_error, "{:?}", results[0].1);
    std::fs::remove_dir_all(&ws).ok();
}

/// Acceptance 2b: write OUTSIDE the root -> sandbox denial -> escalate ->
/// unsandboxed retry succeeds (headless allow).
#[tokio::test]
async fn outside_write_escalates_and_succeeds_when_approved() {
    let ws = unique_ws("esc");
    let outside = ws
        .parent()
        .unwrap()
        .join(format!("escaped-{}", std::process::id()));
    let ctx = sandbox_ctx(&ws, ApproveMode::HeadlessAllow);
    let results = dispatch_batch(
        &registry(),
        &[call("a", &format!("touch {}", outside.display()))],
        &ctx,
        &tokio_util::sync::CancellationToken::new(),
        &emit(),
        &board(),
        &idgen(),
    )
    .await;
    assert!(!results[0].1.is_error, "{:?}", results[0].1);
    assert!(
        outside.exists(),
        "unsandboxed retry should have created the file"
    );
    std::fs::remove_file(&outside).ok();
    std::fs::remove_dir_all(&ws).ok();
}

/// Acceptance 2c: same denial, approval DENIED -> is_error fed back, no file.
#[tokio::test]
async fn outside_write_denied_by_approval() {
    let ws = unique_ws("den");
    let outside = ws
        .parent()
        .unwrap()
        .join(format!("denied-{}", std::process::id()));
    let ctx = sandbox_ctx(&ws, ApproveMode::HeadlessDeny);
    let results = dispatch_batch(
        &registry(),
        &[call("a", &format!("touch {}", outside.display()))],
        &ctx,
        &tokio_util::sync::CancellationToken::new(),
        &emit(),
        &board(),
        &idgen(),
    )
    .await;
    assert!(results[0].1.is_error, "{:?}", results[0].1);
    assert!(results[0].1.content_text().contains("sandbox"));
    assert!(
        !outside.exists(),
        "denied escalation must not create the file"
    );
    std::fs::remove_dir_all(&ws).ok();
}

/// Acceptance 3: rename escape — mv from writable sub to outside the root
/// must be denied by the ancestor unlink/write rules.
#[tokio::test]
async fn rename_escape_is_denied() {
    let ws = unique_ws("mv");
    std::fs::create_dir_all(ws.join("sub")).unwrap();
    let outside = ws
        .parent()
        .unwrap()
        .join(format!("moved-{}", std::process::id()));
    let ctx = sandbox_ctx(&ws, ApproveMode::HeadlessDeny);
    let cmd = format!("touch sub/f && mv sub/f {}", outside.display());
    let results = dispatch_batch(
        &registry(),
        &[call("a", &cmd)],
        &ctx,
        &tokio_util::sync::CancellationToken::new(),
        &emit(),
        &board(),
        &idgen(),
    )
    .await;
    // `touch` inside succeeds; `mv` outside is the sandbox violation.
    assert!(
        results[0].1.is_error,
        "rename escape must fail: {:?}",
        results[0].1
    );
    assert!(
        !outside.exists(),
        "file must not appear outside the workspace"
    );
    assert!(ws.join("sub/f").exists(), "file stays inside (mv rejected)");
    std::fs::remove_dir_all(&ws).ok();
}
