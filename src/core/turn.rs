//! Turn state machine: the sampling loop (codex run_turn shape + pi details).
//!
//! Convergence: `needs_follow_up = model_needs_follow_up || has_pending_input`.
//! Length truncation fails the whole tool batch without executing (pi
//! agent-loop.ts:379). Every await crosses the cancel token.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::actor::Shared;
use super::cancel::next_or_cancel;
use super::events::{Event, EventMsg, MessageItem};
use super::id::ReqId;
use crate::provider::wire::{StopReason, TurnContext, Usage, WireEvent};
use crate::tools::{ToolCall, ToolOutcome, dispatch_batch, outcomes_to_history};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TurnEnd {
    Complete,
    Aborted,
    BudgetStopped,
    Error(String),
}

struct ToolCallAcc {
    call_id: String,
    name: String,
    args: String,
}

pub(crate) async fn run_turn(shared: &Arc<Shared>, token: &CancellationToken) -> TurnEnd {
    let turn_req: ReqId = shared.idgen.next_id();
    let emit = |msg: EventMsg| {
        let _ = shared.events.send(Event::new(turn_req.clone(), msg));
    };
    emit(EventMsg::TurnStarted { turn: 1 });

    let mut round: u32 = 0;
    let mut total_usage = Usage::default();
    let mut round_usage = Usage::default();
    let end: TurnEnd = loop {
        round += 1;
        if round > shared.max_rounds {
            emit(EventMsg::TurnComplete {
                usage: total_usage,
                stop: StopReason::error("round budget exhausted", false),
            });
            break TurnEnd::BudgetStopped;
        }

        // --- prepare_next_turn: compaction check, then inter-turn snapshot.
        if crate::session::compact::should_compact(shared) {
            crate::session::compact::run_compact(shared, token, &turn_req).await;
            if token.is_cancelled() {
                emit(EventMsg::TurnComplete {
                    usage: total_usage,
                    stop: StopReason::Aborted,
                });
                break TurnEnd::Aborted;
            }
        }
        if let Some(spec) = shared.pending_switch.lock().unwrap().take() {
            let mut active = shared.active.lock().unwrap();
            *active = super::actor::ActiveModel {
                provider: Arc::clone(&spec.provider),
                model: spec.model.clone(),
                auth: spec.auth.clone(),
            };
            drop(active);
            if let Some(store) = shared.session.get() {
                store.append_model_change(&spec.provider_name, &spec.model.id);
            }
            emit(EventMsg::ModelSwitched {
                provider: spec.provider_name.clone(),
                model: spec.model.id.clone(),
            });
        }
        let (provider, model, auth) = {
            let a = shared.active.lock().unwrap();
            (Arc::clone(&a.provider), a.model.clone(), a.auth.clone())
        };

        // --- build context from history.
        let ctx = {
            let history = shared.history.lock().unwrap();
            TurnContext {
                system: shared.system.clone(),
                items: history.iter().map(|i| i.to_context()).collect(),
                tools: shared.tools.specs(),
            }
        };

        // Char-estimate of the outbound context: fallback input-usage source
        // for gateways that under-report (BigModel: input_tokens=0, design.md
        // §9.7③). Estimated from TurnContext — wire-independent (faux renders
        // a null body; real bodies vary in shape).
        let ctx_estimate = crate::session::compact::est_tokens(&format!(
            "{}{}",
            ctx.system,
            serde_json::to_string(&ctx.items).unwrap_or_default()
        ));

        // --- sample.
        let spec = provider.render_request(&model, &ctx, model.max_out, None, &auth);
        let bytes = match shared.transport.send(spec).await {
            Ok(b) => b,
            Err(e) => {
                emit(EventMsg::Fatal {
                    error: e.to_string(),
                });
                emit(EventMsg::TurnComplete {
                    usage: total_usage,
                    stop: StopReason::error(e.to_string(), true),
                });
                break TurnEnd::Error(e.to_string());
            }
        };
        let mut stream = provider.parse_stream(&model, bytes);

        // --- consume stream.
        let mut text = String::new();
        let mut thinking_text = String::new();
        let mut thinking_sig = String::new();
        let mut calls: Vec<ToolCallAcc> = Vec::new();
        // wire block index -> position in `calls` (text blocks shift indices)
        let mut index_map: HashMap<u32, usize> = HashMap::new();
        let stop: StopReason = loop {
            match next_or_cancel(&mut stream, token).await {
                None => break StopReason::Aborted, // cancelled mid-stream
                Some(WireEvent::Done { stop }) => break stop,
                Some(WireEvent::Start) => {}
                Some(WireEvent::TextDelta { text: t }) => {
                    text.push_str(&t);
                    emit(EventMsg::TextDelta { text: t });
                }
                Some(WireEvent::ThinkingDelta { text: t }) => {
                    thinking_text.push_str(&t);
                    emit(EventMsg::ThinkingDelta { text: t });
                }
                Some(WireEvent::ThinkingSigned { blob }) => {
                    // Anthropic signature_delta arrives as {"signature": "..."}.
                    if let Some(sig) = blob.get("signature").and_then(|s| s.as_str()) {
                        thinking_sig = sig.to_string();
                    }
                }
                Some(WireEvent::ToolCallBegin {
                    index,
                    call_id,
                    name,
                }) => {
                    index_map.insert(index, calls.len());
                    calls.push(ToolCallAcc {
                        call_id,
                        name,
                        args: String::new(),
                    });
                }
                Some(WireEvent::ToolCallArgsDelta { index, delta }) => {
                    if let Some(&pos) = index_map.get(&index) {
                        calls[pos].args.push_str(&delta);
                    }
                }
                Some(WireEvent::ToolCallEnd { .. }) => {}
                Some(WireEvent::Usage(u)) => {
                    round_usage.accumulate(&u);
                    total_usage.accumulate(&u);
                }
            }
        };
        // Context size estimate for the compaction trigger: trust reported
        // input when present; on under-reporting gateways (input=0) fall back
        // to the request-body estimate (output tokens always reported).
        let ctx_total = if round_usage.input_tokens == 0 {
            ctx_estimate + round_usage.output_tokens
        } else {
            round_usage.total()
        };
        shared
            .ctx_tokens
            .store(ctx_total, std::sync::atomic::Ordering::Relaxed);

        if stop == StopReason::Aborted {
            // Discard partials; emit terminal event; token already fired.
            emit(EventMsg::TurnComplete {
                usage: total_usage,
                stop: StopReason::Aborted,
            });
            break TurnEnd::Aborted;
        }

        // --- commit assistant message: thinking cookie first (地基件 4 回放),
        // then text.
        let mut commit_items: Vec<MessageItem> = Vec::new();
        if !thinking_text.is_empty() {
            let mut blob = serde_json::json!({ "type": "thinking", "thinking": thinking_text });
            if !thinking_sig.is_empty() {
                blob["signature"] = serde_json::Value::String(thinking_sig.clone());
            }
            commit_items.push(MessageItem::ThinkingOpaque { blob });
        }
        if !text.is_empty() {
            commit_items.push(MessageItem::AssistantText { text: text.clone() });
        }
        if !commit_items.is_empty() {
            shared.commit(commit_items);
            // A thinking-only round commits the reasoning cookie (replayed next
            // request) but has no user-visible text: emitting MessageDone with
            // an empty body would just be noise in the event stream.
            if !text.is_empty() {
                emit(EventMsg::MessageDone {
                    item: MessageItem::AssistantText { text },
                });
            }
        }

        // --- length truncation: whole tool batch fails, nothing executes (pi 防呆).
        if stop == StopReason::Length && !calls.is_empty() {
            let batch: Vec<(ToolCall, ToolOutcome)> = calls
                .iter()
                .map(|c| {
                    (
                        ToolCall {
                            call_id: c.call_id.clone(),
                            name: c.name.clone(),
                            args: parse_args_or_null(&c.args),
                        },
                        ToolOutcome::error(
                            "tool call not executed: generation hit the length limit",
                        ),
                    )
                })
                .collect();
            shared.commit(outcomes_to_history(&batch));
            for (call, outcome) in &batch {
                emit(EventMsg::ToolEnd {
                    call_id: call.call_id.clone(),
                    output: outcome.content.clone(),
                    is_error: true,
                    terminate: false,
                });
            }
            emit(EventMsg::TurnComplete {
                usage: total_usage,
                stop,
            });
            break TurnEnd::Complete; // no follow-up after a failed batch
        }

        // --- error stop: surface and end; partials were discarded.
        if let StopReason::Error { message, .. } = &stop {
            emit(EventMsg::TurnComplete {
                usage: total_usage,
                stop: stop.clone(),
            });
            break TurnEnd::Error(message.clone());
        }

        // --- dispatch tools (ToolUse) with reverse-approval support.
        let mut executed_any = false;
        let mut terminated = false;
        if stop == StopReason::ToolUse {
            let call_vec: Vec<ToolCall> = calls
                .iter()
                .map(|c| ToolCall {
                    call_id: c.call_id.clone(),
                    name: c.name.clone(),
                    args: parse_args_or_null(&c.args),
                })
                .collect();
            for call in &call_vec {
                emit(EventMsg::ToolBegin {
                    call_id: call.call_id.clone(),
                    name: call.name.clone(),
                    args: call.args.clone(),
                });
            }
            let tool_ctx = crate::tools::ToolCtx {
                cwd: shared.cwd.clone(),
                role: shared.role.clone(),
                fs: shared.fs.clone(),
                sandbox: Arc::clone(&shared.sandbox),
                engine: Arc::clone(&shared.engine),
                approve: shared.approve,
                unsandboxed: false,
                reprepare_approved: false,
                reads: Arc::clone(&shared.reads),
            };
            let emit_tool: crate::tools::orchestrator::EmitFn = {
                let sh = Arc::clone(shared);
                let tr = turn_req.clone();
                Arc::new(move |msg: EventMsg| {
                    let _ = sh.events.send(Event::new(tr.clone(), msg));
                })
            };
            let results = dispatch_batch(
                &shared.tools,
                &call_vec,
                &tool_ctx,
                token,
                &emit_tool,
                &shared.approvals,
                &shared.idgen,
            )
            .await;
            // One batch, one history commit: see outcomes_to_history.
            shared.commit(outcomes_to_history(&results));
            for (call, outcome) in results {
                executed_any = true;
                if outcome.terminate {
                    terminated = true;
                }
                emit(EventMsg::ToolEnd {
                    call_id: call.call_id.clone(),
                    output: outcome.content.clone(),
                    is_error: outcome.is_error,
                    terminate: outcome.terminate,
                });
            }
        }

        // --- steering drain at round boundary (pi double-queue point).
        let steered = shared.steering.drain();
        let follow_up = (executed_any && !terminated) || !steered.is_empty();
        if !follow_up {
            emit(EventMsg::TurnComplete {
                usage: total_usage,
                stop,
            });
            break TurnEnd::Complete;
        }
        shared.commit(steered);
    };

    shared.approvals.drop_pending();
    end
}

fn parse_args_or_null(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or(Value::Null)
}
