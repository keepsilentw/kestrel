//! Orchestrator (M1): serial prepare (validation + permission hooks), then
//! concurrent execute with an RwLock gate (parallel = read, sequential =
//! write, codex tools/parallel.rs), source-ordered result collection, and
//! the sandbox-denial escalation loop (codex orchestrator.rs:322).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::{
    ApproveMode, ExecMode, Prepared, Reject, ToolCall, ToolCtx, ToolOutcome, ToolRegistry,
    ToolUpdate,
};
use crate::core::approvals::ApprovalBoard;
use crate::core::events::EventMsg;
use crate::core::id::IdGen;
use crate::core::ops::ApprovalDecision;
use serde_json::Value;

pub type EmitFn = Arc<dyn Fn(EventMsg) + Send + Sync>;

/// Prepare one call; `Err` carries the already-final outcome (no execute).
async fn prepare_one(
    registry: &ToolRegistry,
    call: &ToolCall,
    ctx: &ToolCtx,
    emit: &EmitFn,
    approvals: &ApprovalBoard,
    idgen: &Arc<dyn IdGen>,
    token: &CancellationToken,
) -> Result<(Prepared, ToolCtx), ToolOutcome> {
    let Some(tool) = registry.get(&call.name) else {
        return Err(super::unknown_tool_outcome(&call.name));
    };
    match tool.prepare(call, ctx).await {
        Ok(p) => Ok((p, ctx.clone())),
        Err(Reject::RespondToModel { message }) => Err(ToolOutcome::error(message)),
        Err(Reject::Blocked { reason }) => Err(ToolOutcome::error(format!("blocked: {reason}"))),
        Err(Reject::NeedsApproval { prompt, detail }) => {
            let decision = match ctx.approve {
                ApproveMode::HeadlessDeny => {
                    return Err(ToolOutcome::error(format!(
                        "denied (headless mode, --approve none): {prompt}"
                    )));
                }
                ApproveMode::HeadlessAllow => ApprovalDecision::AllowOnce,
                ApproveMode::Bus { timeout } => {
                    match request_bus_approval(
                        prompt.clone(),
                        approval_detail(&call.name, &call.args, detail),
                        timeout,
                        emit,
                        approvals,
                        idgen,
                        token,
                    )
                    .await
                    {
                        Waited::Decision(d) => d,
                        Waited::TimedOut => {
                            return Err(ToolOutcome::error(format!(
                                "approval timed out after {}s, denied (the approver never \
                                 answered): {prompt}",
                                timeout.as_secs()
                            )));
                        }
                        Waited::Cancelled => {
                            return Err(ToolOutcome::error("aborted while waiting for approval"));
                        }
                    }
                }
            };
            match decision {
                ApprovalDecision::Deny => Err(ToolOutcome::error("denied by approval")),
                ApprovalDecision::AllowOnce => {
                    // Re-prepare with the approval flag so policy-Ask passes,
                    // and hand that ctx to execution.
                    let mut ctx2 = ctx.clone();
                    ctx2.reprepare_approved = true;
                    match tool.prepare(call, &ctx2).await {
                        Ok(p) => Ok((p, ctx2)),
                        Err(r) => Err(reject_to_outcome(r)),
                    }
                }
            }
        }
    }
}

async fn wait_decision(
    rx: tokio::sync::oneshot::Receiver<ApprovalDecision>,
    token: &CancellationToken,
    timeout: Duration,
) -> Waited {
    tokio::select! {
        biased;
        _ = token.cancelled() => Waited::Cancelled,
        _ = tokio::time::sleep(timeout) => Waited::TimedOut,
        d = rx => d.map_or(Waited::Cancelled, Waited::Decision),
    }
}

/// Open one bus approval and retire its waiter on timeout or cancellation.
///
/// Every caller must use this path: leaving a waiter behind lets a later
/// `complete` report success even though the tool has already stopped waiting,
/// and the CLI would then book an approval that never applied.
async fn request_bus_approval(
    prompt: String,
    detail: Value,
    timeout: Duration,
    emit: &EmitFn,
    approvals: &ApprovalBoard,
    idgen: &Arc<dyn IdGen>,
    token: &CancellationToken,
) -> Waited {
    let req = idgen.next_id();
    emit(EventMsg::ApprovalRequest {
        req: req.clone(),
        prompt,
        detail,
    });
    let rx = approvals.open_with(req.clone());
    let waited = wait_decision(rx, token, timeout).await;
    match waited {
        Waited::Decision(d) => Waited::Decision(d),
        Waited::TimedOut => {
            // A concurrent reply may already have removed the waiter even
            // though the tool timed out before consuming it. The expired event
            // is still required so the worker can close the request and emit a
            // truthful `applied: false` receipt.
            let _ = approvals.abandon(&req);
            emit(EventMsg::ApprovalExpired {
                req,
                reason: format!("timed out after {timeout:?}"),
            });
            Waited::TimedOut
        }
        Waited::Cancelled => {
            let _ = approvals.abandon(&req);
            emit(EventMsg::ApprovalExpired {
                req,
                reason: "the wait was cancelled".into(),
            });
            Waited::Cancelled
        }
    }
}

/// How a bus-approval wait can end. A silent approver never means an
/// open-ended wait: the worker denies and hands the model an is_error result
/// it can react to (fail closed, design §5 纪律 2).
pub(crate) enum Waited {
    Decision(ApprovalDecision),
    TimedOut,
    Cancelled,
}

/// Fold the tool name and its arguments into the approval payload: the
/// approver decides from data (the actual command, the actual paths), not from
/// the prose prompt (design §10, ApproveMode::Bus).
pub(crate) fn approval_detail(tool: &str, args: &Value, detail: Value) -> Value {
    let mut out = serde_json::Map::new();
    out.insert("tool".to_string(), Value::String(tool.to_string()));
    out.insert("args".to_string(), args.clone());
    if let Value::Object(map) = detail {
        for (k, v) in map {
            out.insert(k, v);
        }
    }
    Value::Object(out)
}

fn reject_to_outcome(r: Reject) -> ToolOutcome {
    match r {
        Reject::RespondToModel { message } => ToolOutcome::error(message),
        Reject::Blocked { reason } => ToolOutcome::error(format!("blocked: {reason}")),
        Reject::NeedsApproval { .. } => ToolOutcome::error("unapproved"),
    }
}

/// Prepared call ready for the execute phase.
struct StagedCall {
    tool: Arc<dyn super::Tool>,
    prepared: Prepared,
    exec_ctx: ToolCtx,
}

type StageResult = Result<StagedCall, ToolOutcome>;

type IndexedOutcome<'a> = Pin<Box<dyn Future<Output = (usize, ToolOutcome)> + Send + 'a>>;

async fn acquire_slot(
    slots: Option<Arc<tokio::sync::Semaphore>>,
    token: &CancellationToken,
) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, ()> {
    let Some(slots) = slots else {
        return Ok(None);
    };
    tokio::select! {
        biased;
        _ = token.cancelled() => Err(()),
        permit = slots.acquire_owned() => permit.map(Some).map_err(|_| ()),
    }
}

/// Execute one prepared call with the sandbox-denial escalation loop.
async fn run_one(
    staged: StagedCall,
    call: ToolCall,
    token: &CancellationToken,
    emit: &EmitFn,
    approvals: &ApprovalBoard,
    idgen: &Arc<dyn IdGen>,
) -> ToolOutcome {
    let StagedCall {
        tool,
        prepared,
        exec_ctx: ctx,
    } = staged;
    let (update_tx, mut update_rx): (
        tokio::sync::mpsc::UnboundedSender<ToolUpdate>,
        tokio::sync::mpsc::UnboundedReceiver<ToolUpdate>,
    ) = tokio::sync::mpsc::unbounded_channel();
    // Forward streaming updates while execute runs; ends when the sender drops.
    let forwarder = tokio_util::task::AbortOnDropHandle::new(tokio::spawn({
        let emit = Arc::clone(emit);
        let call_id = call.call_id.clone();
        async move {
            while let Some(u) = update_rx.recv().await {
                emit(EventMsg::ToolUpdate {
                    call_id: call_id.clone(),
                    chunk: u.chunk,
                });
            }
        }
    }));

    let cancel = token.child_token();
    // Clone so the sandbox-denial escalation can re-execute with the same payload.
    let result = tool
        .execute(prepared.clone(), &ctx, cancel, update_tx)
        .await;
    drop(forwarder);
    match result {
        Ok(outcome) => outcome,
        Err(super::ToolError::SandboxDenied(stderr)) if !ctx.unsandboxed => {
            // Escalation: ask, then retry once outside the sandbox.
            let decision = match ctx.approve {
                ApproveMode::HeadlessDeny => {
                    return ToolOutcome::error(format!("sandbox denied (headless): {stderr}"));
                }
                ApproveMode::HeadlessAllow => ApprovalDecision::AllowOnce,
                ApproveMode::Bus { timeout } => {
                    match request_bus_approval(
                        "sandboxed command failed (suspected sandbox denial); \
                         rerun outside the sandbox?"
                            .into(),
                        approval_detail(
                            &call.name,
                            &call.args,
                            serde_json::json!({ "stderr": stderr, "call_id": call.call_id }),
                        ),
                        timeout,
                        emit,
                        approvals,
                        idgen,
                        token,
                    )
                    .await
                    {
                        Waited::Decision(d) => d,
                        Waited::TimedOut => {
                            return ToolOutcome::error(format!(
                                "sandbox escalation timed out after {}s, staying sandboxed \
                                 (no answer from the approver)",
                                timeout.as_secs()
                            ));
                        }
                        Waited::Cancelled => {
                            return ToolOutcome::error("aborted during sandbox escalation");
                        }
                    }
                }
            };
            match decision {
                ApprovalDecision::Deny => ToolOutcome::error(format!("sandbox denied: {stderr}")),
                ApprovalDecision::AllowOnce => {
                    let mut ctx2 = ctx.clone();
                    ctx2.unsandboxed = true;
                    let cancel2 = token.child_token();
                    let (tx2, _rx2) = retx();
                    match tool.execute(prepared, &ctx2, cancel2, tx2).await {
                        Ok(o) => o,
                        Err(e) => e.to_outcome(),
                    }
                }
            }
        }
        Err(e) => e.to_outcome(),
    }
}

fn retx() -> (
    tokio::sync::mpsc::UnboundedSender<ToolUpdate>,
    tokio::sync::mpsc::UnboundedReceiver<ToolUpdate>,
) {
    tokio::sync::mpsc::unbounded_channel()
}

/// Batch dispatch: serial prepare phase, then concurrent execute phase with
/// the RwLock gate; results are re-assembled in source order regardless of
/// completion order (codex FuturesOrdered semantics).
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_batch(
    registry: &ToolRegistry,
    calls: &[ToolCall],
    ctx: &ToolCtx,
    token: &CancellationToken,
    emit: &EmitFn,
    approvals: &ApprovalBoard,
    idgen: &Arc<dyn IdGen>,
) -> Vec<(ToolCall, ToolOutcome)> {
    dispatch_batch_with_limit(
        registry,
        calls,
        ctx,
        token,
        emit,
        approvals,
        idgen,
        super::DEFAULT_MAX_PARALLEL_TOOLS,
        None,
    )
    .await
}

/// Same batch semantics with a hard ceiling on concurrently executing tools.
/// `0` means no ceiling; positive values gate only the execute phase. Serial
/// preparation (including approvals) is unchanged, and results remain in source
/// order regardless of which tool finishes first.
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_batch_with_limit(
    registry: &ToolRegistry,
    calls: &[ToolCall],
    ctx: &ToolCtx,
    token: &CancellationToken,
    emit: &EmitFn,
    approvals: &ApprovalBoard,
    idgen: &Arc<dyn IdGen>,
    max_parallel_tools: usize,
    worker_tool_slots: Option<Arc<tokio::sync::Semaphore>>,
) -> Vec<(ToolCall, ToolOutcome)> {
    // --- serial prepare phase (any NeedsApproval short-circuits to an outcome).
    let mut stage: Vec<StageResult> = Vec::new();
    for call in calls {
        let r = prepare_one(registry, call, ctx, emit, approvals, idgen, token).await;
        stage.push(r.map(|(prepared, exec_ctx)| StagedCall {
            tool: registry.get(&call.name).expect("prepare resolved tool"),
            prepared,
            exec_ctx,
        }));
    }

    // --- concurrent execute phase.
    let lock = Arc::new(tokio::sync::RwLock::new(()));
    let slots =
        (max_parallel_tools > 0).then(|| Arc::new(tokio::sync::Semaphore::new(max_parallel_tools)));
    let mut futs: Vec<IndexedOutcome<'_>> = Vec::new();
    for (i, call) in calls.iter().enumerate() {
        let stage_i = std::mem::replace(&mut stage[i], Err(ToolOutcome::error("consumed")));
        match stage_i {
            Err(outcome) => {
                futs.push(Box::pin(std::future::ready((i, outcome))));
            }
            Ok(staged) => {
                let sequential = staged.tool.mode() == ExecMode::Sequential;
                let token = token.clone();
                let emit = Arc::clone(emit);
                let idgen = Arc::clone(idgen);
                let lock = Arc::clone(&lock);
                let slots = slots.clone();
                let worker_tool_slots = worker_tool_slots.clone();
                let call2 = call.clone();
                futs.push(Box::pin(async move {
                    let _worker_slot = match acquire_slot(worker_tool_slots, &token).await {
                        Ok(slot) => slot,
                        Err(()) => {
                            return (
                                i,
                                ToolOutcome::error(
                                    "aborted before a worker tool slot was available",
                                ),
                            );
                        }
                    };
                    let _task_slot = match acquire_slot(slots, &token).await {
                        Ok(slot) => slot,
                        Err(()) => {
                            return (
                                i,
                                ToolOutcome::error("aborted before a task tool slot was available"),
                            );
                        }
                    };
                    // Either keeps the two guard types unifiable; both drop-only.
                    let _guard = if sequential {
                        futures::future::Either::Left(lock.write().await)
                    } else {
                        futures::future::Either::Right(lock.read().await)
                    };
                    let outcome = run_one(staged, call2, &token, &emit, approvals, &idgen).await;
                    (i, outcome)
                }));
            }
        }
    }
    let results = futures::future::join_all(futs).await;
    let mut slots: Vec<Option<ToolOutcome>> = vec![None; calls.len()];
    for (i, outcome) in results {
        slots[i] = Some(outcome);
    }
    calls
        .iter()
        .cloned()
        .zip(
            slots
                .into_iter()
                .map(|o| o.unwrap_or_else(|| ToolOutcome::error("missing outcome"))),
        )
        .collect()
}
