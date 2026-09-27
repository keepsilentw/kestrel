//! Op/Event actor: one submission loop, one active turn slot, token tree root.
//! Shape follows codex session/submission_loop (study-core-loop §1).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use super::approvals::ApprovalBoard;
use super::events::{Event, MessageItem};
use super::id::IdGen;
use super::ops::{Op, Submission};
use super::steering::Steering;
use crate::provider::wire::{ModelMeta, ResolvedAuth, Transport, WireFormat};
use crate::tools::ToolRegistry;

/// Resolved model/provider bundle for an inter-turn switch.
pub struct SwitchSpec {
    pub provider_name: String,
    pub provider: Arc<dyn WireFormat>,
    pub model: ModelMeta,
    pub auth: ResolvedAuth,
}

impl std::fmt::Debug for SwitchSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwitchSpec")
            .field("provider_name", &self.provider_name)
            .field("model", &self.model.id)
            .finish()
    }
}

/// What the currently active turn samples against.
pub(crate) struct ActiveModel {
    pub provider: Arc<dyn WireFormat>,
    pub model: ModelMeta,
    pub auth: ResolvedAuth,
}

pub(crate) struct Shared {
    pub active: Mutex<ActiveModel>,
    pub pending_switch: Mutex<Option<SwitchSpec>>,
    pub transport: Arc<dyn Transport>,
    pub system: String,
    pub tools: ToolRegistry,
    pub history: Mutex<Vec<MessageItem>>,
    /// Aligned with `history`: session entry id per item (None when no session).
    pub history_ids: Mutex<Vec<Option<crate::session::EntryId>>>,
    pub steering: Steering,
    pub approvals: ApprovalBoard,
    pub events: mpsc::UnboundedSender<Event>,
    pub idgen: Arc<dyn IdGen>,
    pub max_rounds: u32,
    pub cwd: PathBuf,
    pub role: String,
    pub root: CancellationToken,
    pub fs: crate::sandbox::FsPolicy,
    pub sandbox: Arc<dyn crate::sandbox::Sandbox>,
    pub engine: Arc<crate::permissions::PermissionEngine>,
    pub approve: crate::tools::ApproveMode,
    /// Read-before-edit ledger; rebuilt from `history` at the compaction boundary.
    pub reads: Arc<crate::tools::read_state::ReadTracker>,
    pub session: std::sync::OnceLock<Arc<crate::session::SessionStore>>,
    /// Context size estimate (input+output of the last sampling round).
    pub ctx_tokens: std::sync::atomic::AtomicU64,
    /// Summary of the most recent compaction (drives the update prompt).
    pub last_summary: Mutex<Option<String>>,
}

pub struct EngineConfig {
    pub provider: Arc<dyn WireFormat>,
    pub model: ModelMeta,
    pub auth: ResolvedAuth,
    pub transport: Arc<dyn Transport>,
    pub system: String,
    pub tools: Vec<Arc<dyn crate::tools::Tool>>,
    /// Hard cap on sampling rounds per turn (codex uses 8).
    pub max_rounds: u32,
    pub cwd: PathBuf,
    pub role: String,
    pub idgen: Arc<dyn IdGen>,
    pub fs: crate::sandbox::FsPolicy,
    pub sandbox: Arc<dyn crate::sandbox::Sandbox>,
    pub engine: Arc<crate::permissions::PermissionEngine>,
    pub approve: crate::tools::ApproveMode,
    /// Session persistence (M2); None disables.
    pub session: Option<Arc<crate::session::SessionStore>>,
}

pub struct AgentHandle {
    tx: mpsc::UnboundedSender<Submission>,
    event_rx: Mutex<Option<mpsc::UnboundedReceiver<Event>>>,
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    root: CancellationToken,
}

struct ActiveTask {
    token: CancellationToken,
    handle: AbortOnDropHandle<()>,
}

impl AgentHandle {
    pub fn spawn(config: EngineConfig) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let root = CancellationToken::new();
        let shared = Arc::new(Shared {
            active: Mutex::new(ActiveModel {
                provider: config.provider,
                model: config.model,
                auth: config.auth,
            }),
            pending_switch: Mutex::new(None),
            transport: config.transport,
            system: config.system,
            tools: ToolRegistry::new(config.tools),
            history: Mutex::new(Vec::new()),
            history_ids: Mutex::new(Vec::new()),
            steering: Steering::new(),
            approvals: ApprovalBoard::default(),
            events: event_tx,
            idgen: config.idgen,
            max_rounds: config.max_rounds,
            cwd: config.cwd,
            role: config.role,
            root: root.clone(),
            fs: config.fs,
            sandbox: config.sandbox,
            engine: config.engine,
            approve: config.approve,
            reads: Arc::new(crate::tools::read_state::ReadTracker::new()),
            session: std::sync::OnceLock::new(),
            ctx_tokens: std::sync::atomic::AtomicU64::new(0),
            last_summary: Mutex::new(None),
        });
        if let Some(store) = config.session {
            let _ = shared.session.set(store);
        }
        // managed: joined in AgentHandle::shutdown
        let join = tokio::spawn(op_loop(rx, Arc::clone(&shared)));
        Self {
            tx,
            event_rx: Mutex::new(Some(event_rx)),
            join: Mutex::new(Some(join)),
            root,
        }
    }

    pub fn submit(&self, op: Op) {
        let _ = self.tx.send(Submission::new(op));
    }

    pub fn turn(&self, input: Vec<MessageItem>) {
        self.submit(Op::Turn { input });
    }

    pub fn steer(&self, item: MessageItem) {
        self.submit(Op::Steer { item });
    }

    pub fn interrupt(&self) {
        self.submit(Op::Interrupt);
    }

    /// Take the event stream (once). Consumer owns it afterwards.
    pub fn take_event_rx(&self) -> Option<mpsc::UnboundedReceiver<Event>> {
        self.event_rx.lock().unwrap().take()
    }

    /// Root-level kill switch (process teardown / test teardown).
    pub fn cancel_root(&self) {
        self.root.cancel();
    }

    pub async fn shutdown(self) {
        let _ = self.tx.send(Submission::new(Op::Shutdown));
        self.tx.closed().await;
        // Take the join handle in its own scope so the lock guard never
        // crosses the await point (clippy await_holding_lock).
        let join = { self.join.lock().unwrap().take() };
        if let Some(join) = join {
            let _ = join.await;
        }
    }
}

impl Shared {
    /// Push items into history AND the session tree (entry ids stay aligned
    /// with history positions as Option<EntryId>).
    pub(crate) fn commit(&self, items: Vec<MessageItem>) {
        if items.is_empty() {
            return;
        }
        self.history.lock().unwrap().extend(items.iter().cloned());
        let mut ids = self.history_ids.lock().unwrap();
        match self.session.get() {
            Some(store) => {
                for it in &items {
                    ids.push(Some(store.append_item(it.clone(), None)));
                }
            }
            None => {
                ids.extend(std::iter::repeat_n(None, items.len()));
            }
        }
    }
}

async fn op_loop(mut rx: mpsc::UnboundedReceiver<Submission>, shared: Arc<Shared>) {
    let mut active: Option<ActiveTask> = None;
    while let Some(sub) = rx.recv().await {
        match sub.op {
            Op::Turn { input } => {
                // Single active slot: abort the previous turn first (codex tasks/mod.rs:277).
                if let Some(t) = active.take() {
                    t.token.cancel();
                    // Fire-and-forget: grace abort backstop (100ms, codex tasks/mod.rs:70).
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        t.handle.abort();
                    });
                }
                shared.commit(input);
                let token = shared.root.child_token();
                let sh = Arc::clone(&shared);
                let task_token = token.clone();
                let handle = AbortOnDropHandle::new(tokio::spawn(async move {
                    super::turn::run_turn(&sh, &task_token).await;
                }));
                active = Some(ActiveTask { token, handle });
            }
            Op::Steer { item } => {
                shared.steering.push(item);
            }
            Op::Interrupt => {
                if let Some(t) = active.take() {
                    t.token.cancel();
                    // Fire-and-forget: give the turn 100ms to wind down, then hard abort.
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        t.handle.abort();
                    });
                }
            }
            Op::Approval { req, decision } => {
                shared.approvals.complete(&req, decision);
            }
            Op::SwitchModel { spec } => {
                *shared.pending_switch.lock().unwrap() = Some(*spec);
            }
            Op::Shutdown => break,
        }
    }
    // Teardown: cancel anything still running and clear stale waiters.
    if let Some(t) = active.take() {
        t.token.cancel();
    }
    shared.approvals.drop_pending();
}

/// Convenience for callers that keep a map of named switch specs (M0 CLI/tests).
pub type SwitchMap = HashMap<String, SwitchSpec>;
