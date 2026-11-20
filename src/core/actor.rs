//! Op/Event actor: one submission loop, one active turn slot, token tree root.
//! Shape follows codex session/submission_loop (study-core-loop §1).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use super::approvals::ApprovalBoard;
use super::events::{Event, EventMsg, MessageItem};
use super::id::IdGen;
use super::ops::{ApprovalCompletion, Op, Submission};
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
    pub max_parallel_tools: usize,
    /// Shared across every task engine in one worker.
    pub worker_tool_slots: Option<Arc<tokio::sync::Semaphore>>,
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
    /// Hard cap on tool executions running concurrently inside one task.
    /// `0` explicitly disables the ceiling.
    pub max_parallel_tools: usize,
    /// Hard cap shared by every task hosted by the same worker.
    pub worker_tool_slots: Option<Arc<tokio::sync::Semaphore>>,
    pub cwd: PathBuf,
    pub role: String,
    pub idgen: Arc<dyn IdGen>,
    pub fs: crate::sandbox::FsPolicy,
    pub sandbox: Arc<dyn crate::sandbox::Sandbox>,
    pub engine: Arc<crate::permissions::PermissionEngine>,
    pub approve: crate::tools::ApproveMode,
    /// Optional task-specific cross-turn approval state.
    pub approval_store: Option<PathBuf>,
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
        let approvals = match config.approval_store {
            Some(path) => ApprovalBoard::with_store(path.clone()).unwrap_or_else(|error| {
                tracing::error!(
                    "load approval store {}: {error}; deferred approvals start empty",
                    path.display()
                );
                ApprovalBoard::default()
            }),
            None => ApprovalBoard::default(),
        };
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
            approvals,
            events: event_tx,
            idgen: config.idgen,
            max_rounds: config.max_rounds,
            max_parallel_tools: config.max_parallel_tools,
            worker_tool_slots: config.worker_tool_slots,
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
        // A store that already holds entries means this is a resume (worker
        // turn #N, or `-p --resume`): rebuild history — and with it the
        // read-before-edit ledger — from the tree, so context carries across
        // turns instead of every turn starting cold.
        if let Some(store) = shared.session.get() {
            let snapshot = store.prompt_snapshot();
            if !snapshot.is_empty() {
                let items: Vec<MessageItem> = snapshot.iter().map(|(_, i)| i.clone()).collect();
                let ids: Vec<Option<crate::session::EntryId>> =
                    snapshot.iter().map(|(id, _)| id.clone()).collect();
                shared.history.lock().unwrap().extend(items.iter().cloned());
                shared.history_ids.lock().unwrap().extend(ids);
                shared.reads.rebuild_from_items(&shared.cwd, &items);
                // Seed the context meter with the restored size, otherwise a
                // resumed session samples once before the compaction check has
                // anything to go on (ctx_tokens only learns during a round).
                let est = crate::session::compact::est_tokens(
                    &serde_json::to_string(&items).unwrap_or_default(),
                );
                shared
                    .ctx_tokens
                    .store(est, std::sync::atomic::Ordering::Relaxed);
            }
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

    pub fn turn_isolated(&self, input: Vec<MessageItem>) {
        self.submit(Op::TurnIsolated { input });
    }

    pub fn steer(&self, item: MessageItem) {
        self.submit(Op::Steer { item });
    }

    pub fn interrupt(&self) {
        self.submit(Op::Interrupt);
    }

    /// Deliver an approval decision and report whether a waiter consumed it,
    /// whether it was deferred for an exact future call, or whether it is stale.
    pub async fn respond_to_approval(
        &self,
        req: String,
        decision: super::ops::ApprovalDecision,
    ) -> ApprovalCompletion {
        let (ack_tx, ack_rx) = oneshot::channel();
        let op = Op::Approval {
            req,
            decision,
            ack: Some(ack_tx),
        };
        if self.tx.send(Submission::new(op)).is_err() {
            return ApprovalCompletion::Stale;
        }
        ack_rx.await.unwrap_or(ApprovalCompletion::Stale)
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
    /// Push items into history AND the session tree. A failed write does not
    /// panic (that used to poison `history_ids` and kill the engine for the
    /// life of the process): the already-attempted items keep a `None` id so
    /// memory and ids stay aligned, and the error is returned so the caller can
    /// abort the turn loudly.
    pub(crate) fn commit(&self, items: Vec<MessageItem>) -> anyhow::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        self.history.lock().unwrap().extend(items.iter().cloned());
        let mut ids = self.history_ids.lock().unwrap();
        let Some(store) = self.session.get() else {
            ids.extend(std::iter::repeat_n(None, items.len()));
            return Ok(());
        };
        let mut failed: Option<anyhow::Error> = None;
        for it in &items {
            if failed.is_some() {
                // Unpersisted by definition: never hand compaction a dangling
                // `first_kept` id that points at a line the tree does not hold.
                ids.push(None);
                continue;
            }
            match store.try_append_item(it.clone(), None) {
                Ok(id) => ids.push(Some(id)),
                Err(error) => {
                    ids.push(None);
                    failed = Some(error);
                }
            }
        }
        match failed {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

async fn op_loop(mut rx: mpsc::UnboundedReceiver<Submission>, shared: Arc<Shared>) {
    let mut active: Option<ActiveTask> = None;
    while let Some(sub) = rx.recv().await {
        match sub.op {
            Op::Turn { input } => {
                start_turn(&shared, &mut active, input, false);
            }
            Op::TurnIsolated { input } => {
                start_turn(&shared, &mut active, input, true);
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
            Op::Approval { req, decision, ack } => {
                let applied = shared.approvals.complete(&req, decision);
                if let Some(ack) = ack {
                    let _ = ack.send(applied);
                }
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

fn start_turn(
    shared: &Arc<Shared>,
    active: &mut Option<ActiveTask>,
    input: Vec<MessageItem>,
    isolated: bool,
) {
    // Single active slot: abort the previous turn first (codex tasks/mod.rs:277).
    if let Some(t) = active.take() {
        t.token.cancel();
        // Fire-and-forget: grace abort backstop (100ms, codex tasks/mod.rs:70).
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            t.handle.abort();
        });
    }
    if isolated {
        shared.history.lock().unwrap().clear();
        shared.history_ids.lock().unwrap().clear();
    }
    if let Err(error) = shared.commit(input) {
        let message = format!("session tree write failed: {error:#}");
        let _ = shared.events.send(Event::new(
            shared.idgen.next_id(),
            EventMsg::Fatal { error: message },
        ));
        return;
    }
    let token = shared.root.child_token();
    let sh = Arc::clone(shared);
    let task_token = token.clone();
    let handle = AbortOnDropHandle::new(tokio::spawn(async move {
        super::turn::run_turn(&sh, &task_token).await;
    }));
    *active = Some(ActiveTask { token, handle });
}

/// Convenience for callers that keep a map of named switch specs (M0 CLI/tests).
pub type SwitchMap = HashMap<String, SwitchSpec>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::id::{SeqGen, SystemClock};
    use crate::provider::faux::{FauxWire, stop_script};
    use crate::provider::wire::{ModelMeta, NullTransport, ResolvedAuth, StopReason, WireId};
    use std::path::Path;

    fn engine_config(
        wire: Arc<FauxWire>,
        session: Arc<crate::session::SessionStore>,
    ) -> EngineConfig {
        EngineConfig {
            provider: wire,
            model: ModelMeta::minimal("faux-model", WireId::AnthropicMessages, "faux://unused"),
            auth: ResolvedAuth::key("test"),
            transport: Arc::new(NullTransport),
            system: "test".into(),
            tools: vec![],
            max_rounds: 8,
            max_parallel_tools: 4,
            worker_tool_slots: None,
            cwd: std::env::temp_dir(),
            role: "rd".into(),
            idgen: Arc::new(SeqGen::new()),
            fs: Default::default(),
            sandbox: Arc::new(crate::sandbox::Unsandboxed),
            engine: Arc::new(crate::permissions::PermissionEngine::new(Default::default())),
            approve: crate::tools::ApproveMode::HeadlessDeny,
            approval_store: None,
            session: Some(session),
        }
    }

    /// A session write that fails must end the turn with `Fatal` and leave the
    /// engine usable. It used to panic while holding `history_ids`, poisoning
    /// that lock and killing every later turn for the life of the process.
    #[tokio::test]
    async fn a_failed_session_write_is_fatal_but_not_terminal_for_the_engine() {
        let dir =
            std::env::temp_dir().join(format!("kestrel-actor-writefail-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let store = Arc::new(
            crate::session::SessionStore::create(
                &dir,
                Path::new("/ws"),
                "rd",
                "prov",
                "model-x",
                Arc::new(SeqGen::new()),
                Arc::new(SystemClock),
            )
            .unwrap(),
        );
        store.fail_next_write();

        let wire = Arc::new(FauxWire::new());
        wire.push_turn_script(stop_script("after the failure"));
        let handle = AgentHandle::spawn(engine_config(wire, Arc::clone(&store)));
        let mut rx = handle.take_event_rx().expect("single event consumer");
        handle.turn(vec![MessageItem::user_text("hi")]);

        let fatal = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = rx.recv().await.expect("event stream");
                if let EventMsg::Fatal { error } = event.msg {
                    return error;
                }
            }
        })
        .await
        .expect("a failed session write must be fatal, not silence");
        assert!(fatal.contains("session tree write failed"), "{fatal}");
        assert!(fatal.contains("injected session write failure"), "{fatal}");

        // No poisoned mutex: the next turn commits and completes.
        handle.turn(vec![MessageItem::user_text("again")]);
        let stop = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = rx.recv().await.expect("event stream");
                if let EventMsg::TurnComplete { stop, .. } = event.msg {
                    return stop;
                }
            }
        })
        .await
        .expect("the engine must survive a failed write");
        assert_eq!(stop, StopReason::Stop);
        std::fs::remove_dir_all(&dir).ok();
    }
}
