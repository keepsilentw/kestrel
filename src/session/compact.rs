//! Compaction engine: threshold check in `prepare_next_turn`, summary via one
//! sampling request (pi 三段式), lossless checkpoint into the session tree.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio_util::sync::CancellationToken;

use super::SUMMARY_PREFIX;
use crate::core::actor::Shared;
use crate::core::cancel::next_or_cancel;
use crate::core::events::{Event, EventMsg, MessageItem};
use crate::core::id::ReqId;
use crate::provider::wire::{ContextItem, StopReason, TurnContext, WireEvent};

const COMPACT_PROMPT: &str = include_str!("../prompts/compact.md");
const COMPACT_UPDATE_PROMPT: &str = include_str!("../prompts/compact_update.md");

/// Keep roughly this many estimated tokens of recent history after compaction
/// (pi keepRecentTokens≈20k).
const KEEP_TOKENS: u64 = 20_000;

pub fn est_tokens(s: &str) -> u64 {
    (s.len() as u64) / 4 + 1
}

/// Soft limit: leave headroom for max_out inside the context window.
fn soft_limit(shared: &Shared) -> u64 {
    let model = shared.active.lock().unwrap().model.clone();
    (model.context_window as u64)
        .saturating_mul(80)
        .saturating_div(100)
        .saturating_sub(model.max_out as u64)
}

/// Should we compact now? Called from prepare_next_turn.
pub(crate) fn should_compact(shared: &Shared) -> bool {
    shared.session.get().is_some()
        && shared.ctx_tokens.load(Ordering::Relaxed) >= soft_limit(shared)
}

/// Render one history item to a compact text line for the summarizer.
fn transcript_line(i: usize, item: &MessageItem) -> String {
    let role = match item {
        MessageItem::UserText { .. } | MessageItem::BusBell { .. } => "user",
        MessageItem::AssistantText { .. } => "assistant",
        MessageItem::ThinkingOpaque { .. } => "assistant(thinking)",
        MessageItem::AssistantToolUse { .. } => "assistant(tool_use)",
        MessageItem::ToolResult { is_error, .. } => {
            if *is_error {
                "tool(error)"
            } else {
                "tool"
            }
        }
    };
    let text = match item {
        MessageItem::UserText { text } | MessageItem::BusBell { text, .. } => text.clone(),
        MessageItem::AssistantText { text } => text.clone(),
        MessageItem::ThinkingOpaque { blob } => {
            blob["thinking"].as_str().unwrap_or_default().to_string()
        }
        MessageItem::AssistantToolUse { name, args, .. } => {
            format!("call {name}({args})")
        }
        MessageItem::ToolResult { content, .. } => content
            .iter()
            .map(|c| match c {
                crate::provider::wire::ToolContent::Text { text } => text.clone(),
                crate::provider::wire::ToolContent::Image { .. } => "<image>".into(),
            })
            .collect::<Vec<_>>()
            .join("\n"),
    };
    format!(
        "[{i}] {role}: {}",
        text.lines().take(20).collect::<Vec<_>>().join("\n")
    )
}

/// Run compaction: summarize the dropped prefix, write the checkpoint, rewrite
/// in-memory history. Returns true when compaction happened.
pub(crate) async fn run_compact(
    shared: &Arc<Shared>,
    token: &CancellationToken,
    turn_req: &ReqId,
) -> bool {
    // Cutoff: keep recent items within KEEP_TOKENS (char-estimated).
    let history = shared.history.lock().unwrap().clone();
    let mut keep_from = history.len();
    let mut acc = 0u64;
    for (i, item) in history.iter().enumerate().rev() {
        let t = est_tokens(&serde_json::to_string(item).unwrap_or_default());
        if acc + t > KEEP_TOKENS && i > 0 {
            keep_from = i + 1;
            break;
        }
        acc += t;
        keep_from = i;
    }
    if keep_from == 0 {
        // everything fits in the keep window; nothing to drop — still compact
        // the usage baseline by summarizing all (rare, degenerate).
        keep_from = history.len();
    }
    let dropped: Vec<String> = history[..keep_from]
        .iter()
        .enumerate()
        .map(|(i, item)| transcript_line(i, item))
        .collect();
    if dropped.is_empty() {
        shared.ctx_tokens.store(0, Ordering::Relaxed);
        return false;
    }

    // Previous summary? Then use the update prompt (merge instead of fresh).
    let seed = shared.last_summary.lock().unwrap().clone();
    let prompt_base = if seed.is_some() {
        COMPACT_UPDATE_PROMPT
    } else {
        COMPACT_PROMPT
    };
    let full_prompt = format!(
        "{prompt_base}\n{}",
        seed.clone()
            .map(|s| format!("PREVIOUS SUMMARY:\n{s}\n\n"))
            .unwrap_or_default(),
    );
    let body = format!("{full_prompt}{}", dropped.join("\n---\n"));

    // One sampling request to the CURRENT model (pi 形: same model summarizes).
    let (provider, model, auth) = {
        let a = shared.active.lock().unwrap();
        (Arc::clone(&a.provider), a.model.clone(), a.auth.clone())
    };
    let ctx = TurnContext {
        system: String::new(),
        items: vec![ContextItem::UserText { text: body }],
        tools: vec![],
    };
    let spec = provider.render_request(&model, &ctx, model.max_out, None, &auth);
    let emit = |msg: EventMsg| {
        let _ = shared.events.send(Event::new(turn_req.clone(), msg));
    };
    let Ok(bytes) = shared.transport.send(spec).await else {
        return false; // compaction is best-effort; keep context and retry next turn
    };
    let mut stream = provider.parse_stream(&model, bytes);
    let mut summary = String::new();
    let stop: StopReason = loop {
        match next_or_cancel(&mut stream, token).await {
            None => break StopReason::Aborted,
            Some(WireEvent::Done { stop }) => break stop,
            Some(WireEvent::TextDelta { text }) => summary.push_str(&text),
            Some(WireEvent::Usage(u)) => {
                shared.ctx_tokens.fetch_add(u.total(), Ordering::Relaxed);
            }
            _ => {}
        }
    };
    if !matches!(stop, StopReason::Stop) || summary.trim().is_empty() {
        return false; // failed summary → keep context, retry next round
    }
    summary = summary.trim().to_string();

    // Checkpoint: first_kept = entry id of the first kept history item.
    let ids = shared.history_ids.lock().unwrap();
    let first_kept = ids
        .get(keep_from)
        .cloned()
        .flatten()
        .unwrap_or_else(|| shared.idgen.next_id());
    drop(ids);
    let tokens_before = shared.ctx_tokens.load(Ordering::Relaxed);
    let store = shared.session.get().expect("checked in should_compact");
    store.append_compaction(first_kept.clone(), tokens_before, summary.clone(), seed);

    // Rewrite in-memory state: summary message + kept tail.
    let kept_items: Vec<MessageItem> = history[keep_from..].to_vec();
    let mut new_history = vec![MessageItem::UserText {
        text: format!("{SUMMARY_PREFIX}{summary}"),
    }];
    new_history.extend(kept_items);
    // Reads that fell into the dropped region are no longer in the model's
    // view: rebuilding the ledger makes it re-read instead of editing against
    // content it can no longer see.
    shared.reads.rebuild_from_items(&shared.cwd, &new_history);
    *shared.history.lock().unwrap() = new_history;
    let mut new_ids = vec![None];
    new_ids.extend(shared.history_ids.lock().unwrap()[keep_from..].to_vec());
    *shared.history_ids.lock().unwrap() = new_ids;
    *shared.last_summary.lock().unwrap() = Some(summary.clone());
    shared
        .ctx_tokens
        .store(est_tokens(&summary) + acc, Ordering::Relaxed);
    emit(EventMsg::Compacted {
        first_kept,
        tokens_before: tokens_before as u32,
    });
    true
}
