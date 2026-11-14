//! Worker pool (§10 多任务并行的最后一块): one `serve` process, **one engine per
//! task**, each with its own append-only session tree.
//!
//! 为什么不是「一个引擎多活跃槽」：单活跃槽是 §4 的不变式（一个回合一条取消树），
//! 把它放宽等于把 `interrupt`/steering/compaction 的时序全部重推一遍。真正的并行
//! 单位是 task：每个 task 自己一个引擎、自己一棵树、自己一条 8 轮预算，彼此隔离。
//!
//! 总线的读写只发生在 pump 一处，worker 通过 `Outbound` 递交要发的帧。理由是审批
//! 反向请求的**归属**必须有人统一记账：批复要送给打开它的那个 task，绝不能广播——
//! 一次 `allow` 被另一个 task 消费就是权限绕过（design §5）。

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::task::AbortOnDropHandle;

use crate::bus::frame::{Envelope, FrameKind};
use crate::bus::rpc::BusClient;
use crate::cli::evidence::{annotation_warnings, evidence_from_answer};
use crate::cli::serve::{
    approval_receipt_frame, approval_request_frame, bell_prompt, decision_from_frame, decision_name,
};
use crate::cli::task::{artifact_name, task_of};
use crate::core::actor::AgentHandle;
use crate::core::events::{Event, EventMsg, MessageItem};
use crate::core::ops::ApprovalDecision;
use crate::provider::wire::{StopReason, Usage};
use crate::tools::ApproveMode;

/// One unit of work handed to a task's engine.
pub enum TaskMsg {
    Bell {
        sender: String,
        payload: Value,
        /// Monotonic per-task delivery number. It prevents a stale completion
        /// from marking a newer queued bell as idle.
        generation: u64,
    },
    /// An approval reply the router says belongs to this task.
    Approval {
        req: String,
        decision: ApprovalDecision,
        by: String,
    },
}

/// What a worker asks the pump to put on the bus. The pump is the only writer
/// and reader, so request ownership is learned in exactly one place.
pub enum Outbound {
    Notify {
        session: String,
        payload: Value,
    },
    ApprovalOpened {
        req: String,
        task: String,
    },
    /// Turn over: the request can no longer be answered meaningfully.
    ApprovalClosed {
        req: String,
    },
    /// An idle worker PROPOSING its own retirement. Only the pump may drop the
    /// sender, and `mpsc` delivers everything already queued before the receiver
    /// sees `None` — so a bell that raced ahead of this proposal is still served.
    /// That is the whole reason the worker asks instead of breaking out of its
    /// own loop: a self-chosen exit is a bell-loss window.
    RetireRequested {
        task: String,
    },
    /// A task finished the turn identified by `generation`. The router only
    /// marks it idle when no newer bell has already been delivered.
    TurnFinished {
        task: String,
        generation: u64,
    },
}

/// Routing decision. Kept as data so the policy is testable without a bus or
/// an LLM.
#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    /// first bell of a task: build its engine
    Spawn(String),
    /// a task already has an engine: deliver to it
    Deliver(String),
    /// task cap reached: refuse out loud instead of queueing invisibly
    Reject { task: String, reason: String },
    /// deliver an approval reply to the task that opened the request
    Reply { task: String, req: String },
    /// drop the frame, with the reason (a reply nobody owns is never broadcast)
    Drop(String),
}

#[derive(Default)]
pub struct Router {
    tasks: HashSet<String>,
    req_owner: HashMap<String, String>,
    delivered: HashMap<String, u64>,
    idle_since: HashMap<String, Instant>,
    max_tasks: usize,
    /// How many engines this worker has released. "task_count pinned at the cap"
    /// and "tasks churning through the cap" look identical from the outside
    /// unless something counts the releases.
    retired_total: usize,
    pressure_retired_total: usize,
    /// A resource ceiling was crossed: new tasks are refused out loud, while
    /// tasks that already own an engine keep running. Refusing is the honest
    /// half of a soft ceiling — killing the process would take the in-flight
    /// turns with it, and `setrlimit`-style hard caps turn an overshoot into a
    /// random allocation failure inside somebody's tool call.
    draining: Option<String>,
}

impl Router {
    /// `max_tasks` 0 means "no ceiling" (a caller that wants to be surprised by
    /// memory growth rather than a refusal); the shipped default is a number.
    pub fn new(max_tasks: usize) -> Self {
        Self {
            tasks: HashSet::new(),
            req_owner: HashMap::new(),
            delivered: HashMap::new(),
            idle_since: HashMap::new(),
            max_tasks,
            retired_total: 0,
            pressure_retired_total: 0,
            draining: None,
        }
    }

    /// Put the worker into drain mode with the reason the operator should see.
    /// Idempotent: the first reason wins, so a repeated ceiling check does not
    /// rewrite history while the warning has already been printed.
    pub fn set_draining(&mut self, reason: impl Into<String>) {
        self.draining.get_or_insert_with(|| reason.into());
    }

    pub fn draining(&self) -> Option<&str> {
        self.draining.as_deref()
    }

    /// Pressure has fallen below the recovery watermark. The historical peak
    /// remains available in status, so recovery does not erase that the worker
    /// once crossed the ceiling.
    pub fn clear_draining(&mut self) -> bool {
        self.draining.take().is_some()
    }

    pub fn on_bell(&mut self, task: &str) -> Action {
        if self.tasks.contains(task) {
            self.idle_since.remove(task);
            return Action::Deliver(task.to_string());
        }
        if let Some(reason) = self.draining.clone() {
            return Action::Reject {
                task: task.to_string(),
                reason,
            };
        }
        if self.max_tasks > 0 && self.tasks.len() >= self.max_tasks {
            return Action::Reject {
                task: task.to_string(),
                reason: format!(
                    "worker is at its task cap ({}) — refusing a task is honest, queueing \
                     it behind an unbounded engine list is not",
                    self.max_tasks
                ),
            };
        }
        self.tasks.insert(task.to_string());
        self.idle_since.remove(task);
        Action::Spawn(task.to_string())
    }

    /// Stamp an accepted bell before it enters the task's queue. A completion
    /// carrying an older stamp cannot mark a newer bell as idle.
    pub fn begin_bell(&mut self, task: &str) -> u64 {
        let generation = self.delivered.entry(task.to_string()).or_default();
        *generation += 1;
        self.idle_since.remove(task);
        *generation
    }

    /// Record completion only when this is still the newest delivered bell.
    /// Returns whether the task actually became idle.
    pub fn on_turn_finished(&mut self, task: &str, generation: u64) -> bool {
        if self.delivered.get(task) != Some(&generation) {
            return false;
        }
        self.idle_since.insert(task.to_string(), Instant::now());
        true
    }

    /// Idle engines safe to release under memory pressure. The grace period
    /// covers the race between a completion leaving the worker and a new bell
    /// reaching the pump.
    pub fn pressure_retirement_candidates(&self, idle_for: Duration) -> Vec<String> {
        let now = Instant::now();
        let mut tasks: Vec<String> = self
            .idle_since
            .iter()
            .filter(|(_, since)| now.saturating_duration_since(**since) >= idle_for)
            .map(|(task, _)| task.clone())
            .filter(|task| self.tasks.contains(task))
            .collect();
        tasks.sort();
        tasks
    }

    pub fn on_approval_opened(&mut self, req: &str, task: &str) {
        self.req_owner.insert(req.to_string(), task.to_string());
    }

    /// Replies are routed by request id and never broadcast: an `allow` that
    /// lands in the wrong task is a permission bypass.
    pub fn on_approval_reply(&mut self, req: Option<&str>) -> Action {
        let Some(req) = req else {
            return Action::Drop("approval reply without a req id".to_string());
        };
        match self.req_owner.remove(req) {
            Some(task) => Action::Reply {
                task,
                req: req.to_string(),
            },
            None => Action::Drop(format!("no open request {req}")),
        }
    }

    pub fn close_request(&mut self, req: &str) {
        self.req_owner.remove(req);
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn open_requests(&self) -> usize {
        self.req_owner.len()
    }

    /// Release one task slot. Retirement frees an engine, never history: the
    /// next bell reopens the same session tree (`cli::serve::TaskSessions`).
    /// Approval ownership goes with the task — a worker closes every in-flight
    /// request when its turn ends, so anything left here is already stale, and a
    /// reply for a retired task must not resurrect the entry.
    pub fn retire_task(&mut self, task: &str) -> bool {
        if !self.tasks.remove(task) {
            return false;
        }
        self.req_owner.retain(|_, owner| owner != task);
        self.delivered.remove(task);
        self.idle_since.remove(task);
        self.retired_total += 1;
        true
    }

    pub fn retire_under_pressure(&mut self, task: &str) -> bool {
        if self.retire_task(task) {
            self.pressure_retired_total += 1;
            true
        } else {
            false
        }
    }

    pub fn retired_total(&self) -> usize {
        self.retired_total
    }

    pub fn pressure_retired_total(&self) -> usize {
        self.pressure_retired_total
    }

    fn idle_tasks(&self) -> Vec<String> {
        let mut tasks: Vec<String> = self.idle_since.keys().cloned().collect();
        tasks.sort();
        tasks
    }

    fn active_tasks(&self) -> Vec<String> {
        let mut tasks: Vec<String> = self.tasks.iter().cloned().collect();
        tasks.sort();
        tasks
    }
}

/// A task's engine plus the event stream the worker drives.
pub struct PooledEngine {
    pub handle: AgentHandle,
    pub events: mpsc::UnboundedReceiver<Event>,
}

/// Builds the engine for one task. Injectable so the pool's behaviour can be
/// tested with faux wires instead of a live endpoint.
pub type EngineFactory = Arc<dyn Fn(&str) -> anyhow::Result<PooledEngine> + Send + Sync>;

pub struct PoolConfig {
    /// This worker's bus address (also the artifact name's session field).
    pub session: String,
    pub role: String,
    pub artifacts_dir: PathBuf,
    /// `approve = "bus"`: the peer that answers; `None` means "whoever rang".
    pub approver: Option<String>,
    pub max_tasks: usize,
    pub max_parallel_tools: usize,
    pub max_worker_tools: usize,
    /// How often an unanswered approval request is re-sent (§10 批复的可靠投递).
    /// `serve` folds it out of the role's `approve_timeout_secs` so the retries
    /// land *inside* the window that ends in a deny.
    pub approval_retry: Duration,
    /// Optional machine-readable worker status, atomically replaced on each tick.
    pub status_file: Option<PathBuf>,
    pub status_interval: Duration,
    /// Soft memory ceiling in bytes: while current resident size is at or above
    /// it the worker drains (refuses new tasks) and reclaims engines that
    /// completed their newest bell and stayed idle. 0 = no ceiling.
    pub max_rss_bytes: u64,
    /// Idle time after which a task's engine proposes retirement. 0 = never,
    /// which leaves the slot taken for the life of the process.
    pub task_idle_timeout: Duration,
}

/// An approval request that is still in flight: the frame itself (so a retry is
/// byte-identical), who it went to, and how many times it has been sent.
struct OpenApproval {
    frame: Value,
    to: String,
    sends: u32,
}

/// Memory pressure is sampled on its own clock even when a status file is not
/// configured. The grace period gives a new bell time to turn a completed task
/// busy again before its engine is released.
const PRESSURE_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const PRESSURE_IDLE_GRACE: Duration = Duration::from_secs(5);
/// Resume below 80% of the ceiling. The gap between enter and resume is
/// deliberate hysteresis: a worker hovering at the ceiling must not flap
/// between accepting and rejecting tasks every sample.
const PRESSURE_RECOVERY_DIVISOR: u64 = 5;

// 投递上限不在这层定义：它属于审批窗口本身（`ApproveMode::MAX_APPROVAL_SENDS`），
// 与重发节奏同一个来源，免得两处各写一个数然后对不上。

/// The pump: single bus reader + single bus writer + per-task routing.
pub async fn run_pool(
    client: Arc<BusClient>,
    cfg: Arc<PoolConfig>,
    factory: EngineFactory,
) -> anyhow::Result<()> {
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Outbound>();
    let mut router = Router::new(cfg.max_tasks);
    let mut workers: HashMap<String, mpsc::UnboundedSender<TaskMsg>> = HashMap::new();
    let mut joins: Vec<AbortOnDropHandle<()>> = Vec::new();
    if cfg.status_file.is_some() && cfg.status_interval.is_zero() {
        anyhow::bail!("worker status interval must be greater than zero");
    }
    let status_interval = if cfg.status_file.is_some() {
        cfg.status_interval
    } else {
        Duration::from_secs(3600)
    };
    let mut status_tick = tokio::time::interval(status_interval);
    status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pressure_tick = tokio::time::interval(PRESSURE_CHECK_INTERVAL);
    pressure_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // A dedicated reader task, not a `select!` arm: when select! loses a race it
    // drops the losing future, and a frame that was already on its way would be
    // dropped with it. The reader never gives a frame back until it is queued.
    let (in_tx, mut in_rx) = mpsc::unbounded_channel::<Envelope>();
    let reader = {
        let client = Arc::clone(&client);
        // managed: aborted when the pump returns
        AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                match client.recv_notify().await {
                    Ok(frame) => {
                        if in_tx.send(frame).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        // The reconnect window elapsed: nobody is left to talk
                        // to, so this worker has no reason to stay alive.
                        tracing::warn!("bus read failed after reconnect window: {e}");
                        break;
                    }
                }
            }
        }))
    };

    loop {
        tokio::select! {
            frame = in_rx.recv() => {
                let Some(frame) = frame else {
                    // the reader ended (bus gone or the pump is shutting down)
                    break;
                };
                if frame.kind != FrameKind::Notify {
                    tracing::warn!("pump: ignoring {:?} frame", frame.kind);
                    continue;
                }
                if frame.payload["type"].as_str() == Some("approval_decision") {
                    let action = router.on_approval_reply(frame.payload["req"].as_str());
                    match action {
                        Action::Reply { task, req } => {
                            let decision = decision_from_frame(&frame.payload);
                            match workers.get(&task) {
                                Some(tx) => {
                                    let _ = tx.send(TaskMsg::Approval {
                                        req,
                                        decision,
                                        by: frame.sender.clone(),
                                    });
                                }
                                None => tracing::warn!("approval reply for a dead task {task}"),
                            }
                        }
                        // 一份没有归属的批复（重复投递，或那颗请求已经关掉了）不能
                        // 只是被静默丢掉：回执要说回给审批者「收到了，但没生效」，
                        // 否则「批复丢了」和「批了但没人听」在对方的账上长得一样。
                        Action::Drop(reason) => {
                            tracing::warn!("pump: dropping approval reply: {reason}");
                            let req = frame.payload["req"].as_str().unwrap_or_default();
                            client.notify(
                                &frame.sender,
                                approval_receipt_frame(
                                    &cfg.session,
                                    req,
                                    decision_from_frame(&frame.payload),
                                    false,
                                    &reason,
                                ),
                            );
                        }
                        other => tracing::warn!("pump: unexpected routing {other:?}"),
                    }
                    continue;
                }
                let task = task_of(&frame.payload);
                // The ceiling is read per bell, not on a timer: a bell is what
                // costs a new engine, and `getrusage` is one syscall. Checking
                // here (instead of only at the status tick) means the first
                // overshoot refuses the bell that crossed it, not the next one.
                update_pressure_state(&cfg, &mut router);
                match router.on_bell(&task) {
                    Action::Reject { task, reason } => {
                        tracing::warn!("pump: {reason}");
                        client.notify(
                            &frame.sender,
                            serde_json::json!({
                                "role": cfg.session,
                                "task_id": task,
                                "stage": frame.payload["stage"],
                                "iteration": frame.payload["iteration"],
                                "error": reason,
                            }),
                        );
                    }
                    Action::Spawn(ref task) | Action::Deliver(ref task) => {
                        let task = task.clone();
                        if !workers.contains_key(&task) {
                            let engine = factory(&task)
                                .with_context(|| format!("start an engine for task {task}"))?;
                            let (tx, inbox) = mpsc::unbounded_channel::<TaskMsg>();
                            let worker = spawn_worker(
                                Arc::clone(&cfg),
                                out_tx.clone(),
                                task.clone(),
                                engine,
                                inbox,
                            );
                            workers.insert(task.clone(), tx);
                            joins.push(worker);
                        }
                        if let Some(tx) = workers.get(&task) {
                            let generation = router.begin_bell(&task);
                            let _ = tx.send(TaskMsg::Bell {
                                sender: frame.sender.clone(),
                                payload: frame.payload.clone(),
                                generation,
                            });
                        }
                    }
                    other => tracing::warn!("pump: unexpected bell routing {other:?}"),
                }
            }
            msg = out_rx.recv(), if !workers.is_empty() => {
                match msg {
                    Some(Outbound::Notify { session, payload }) => client.notify(&session, payload),
                    Some(Outbound::ApprovalOpened { req, task }) => router.on_approval_opened(&req, &task),
                    Some(Outbound::ApprovalClosed { req }) => router.close_request(&req),
                    Some(Outbound::TurnFinished { task, generation }) => {
                        let _ = router.on_turn_finished(&task, generation);
                    }
                    Some(Outbound::RetireRequested { task }) => {
                        // Dropping the sender IS the retirement: the worker's
                        // pending `recv()` takes anything already queued first and
                        // only then sees `None`, so no bell can be lost to this.
                        if let Some(tx) = workers.remove(&task) {
                            drop(tx);
                            router.retire_task(&task);
                            tracing::warn!(
                                "pump: task {task} retired after {}s idle — the engine is \
                                 gone, the session tree stays for the next bell",
                                cfg.task_idle_timeout.as_secs()
                            );
                            // Reap the finished handles. Only *finished* ones:
                            // dropping an `AbortOnDropHandle` that is still
                            // running would abort that worker mid-turn. Without
                            // this, a long-lived worker accumulates one entry per
                            // task it ever hosted — retirement would have freed
                            // the engine and kept the ledger.
                            joins.retain(|join| !join.is_finished());
                        }
                    }
                    None => {
                        // every worker is gone; re-enable only when a bell returns
                        tokio::task::yield_now().await;
                    }
                }
            }
            _ = pressure_tick.tick(), if cfg.max_rss_bytes > 0 => {
                update_pressure_state(&cfg, &mut router);
                if router.draining().is_some() {
                    let candidates = router.pressure_retirement_candidates(PRESSURE_IDLE_GRACE);
                    for task in candidates {
                        let Some(tx) = workers.remove(&task) else {
                            continue;
                        };
                        drop(tx);
                        if router.retire_under_pressure(&task) {
                            tracing::warn!(
                                "pump: task {task} retired under memory pressure — the engine \
                                 is gone, the session tree stays for the next bell"
                            );
                        }
                    }
                    joins.retain(|join| !join.is_finished());
                }
            }
            _ = status_tick.tick(), if cfg.status_file.is_some() => {
                if let Some(path) = &cfg.status_file {
                    let document = status_document(&cfg, &router);
                    if let Err(error) = write_status_file(path, &document) {
                        tracing::warn!("write worker status {}: {error}", path.display());
                    }
                }
            }
        }
    }

    // Dropping the senders ends each worker loop, which shuts its engine down.
    drop(workers);
    drop(reader);
    for join in joins {
        let _ = join.await;
    }
    Ok(())
}

fn status_document(cfg: &PoolConfig, router: &Router) -> Value {
    serde_json::json!({
        "session": cfg.session,
        "role": cfg.role,
        "active_tasks": router.active_tasks(),
        "idle_tasks": router.idle_tasks(),
        "task_count": router.task_count(),
        "open_approvals": router.open_requests(),
        "max_tasks": cfg.max_tasks,
        "max_parallel_tools": cfg.max_parallel_tools,
        "max_worker_tools": cfg.max_worker_tools,
        "max_rss_bytes": cfg.max_rss_bytes,
        "peak_rss_bytes": peak_rss_bytes().ok(),
        "resident_rss_bytes": resident_rss_bytes().ok(),
        "draining": router.draining(),
        "retirements": router.retired_total(),
        "pressure_retirements": router.pressure_retired_total(),
        "updated_unix_secs": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    })
}

fn write_status_file(path: &std::path::Path, document: &Value) -> anyhow::Result<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("worker-status.json");
    let staged = path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()));
    std::fs::write(&staged, serde_json::to_vec(document)?)?;
    Ok(std::fs::rename(staged, path)?)
}

/// Enter drain while current resident memory is over the ceiling and resume
/// once it falls below the hysteresis watermark. A failed current reading never
/// clears drain: loss of observation must not turn into an accidental resume.
fn update_pressure_state(cfg: &PoolConfig, router: &mut Router) {
    if cfg.max_rss_bytes == 0 {
        return;
    }
    match resident_rss_bytes() {
        Ok(current) if current >= cfg.max_rss_bytes => {
            let peak = peak_rss_bytes().unwrap_or(current).max(current);
            let reason = format!(
                "worker is draining: resident RSS {current} bytes reached the ceiling {} \
                 (peak {peak} bytes) — new tasks are refused, running turns finish and \
                 idle engines are reclaimed",
                cfg.max_rss_bytes,
            );
            if router.draining().is_none() {
                tracing::warn!("pump: {reason}");
                router.set_draining(reason);
            }
        }
        Ok(current) => {
            let recovery_ceiling = cfg.max_rss_bytes / PRESSURE_RECOVERY_DIVISOR * 4;
            if current <= recovery_ceiling && router.clear_draining() {
                tracing::warn!(
                    "pump: resident RSS {current} bytes fell to or below the recovery \
                     watermark {recovery_ceiling} bytes — accepting new tasks again"
                );
            }
        }
        Err(error) => tracing::warn!("pump: resident RSS is unavailable: {error}"),
    }
}

/// Current resident size in bytes. Linux reads its own procfs status file;
/// elsewhere `ps` supplies the same `rss` column in KiB. Peak remains separate
/// and keeps the historical high-water mark for audit.
fn resident_rss_bytes() -> anyhow::Result<u64> {
    if cfg!(target_os = "linux") {
        let status = std::fs::read_to_string("/proc/self/status")?;
        return parse_proc_status_vm_rss_bytes(&status)
            .context("parse VmRSS from /proc/self/status");
    }
    let pid = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .context("run ps to read resident RSS")?;
    anyhow::ensure!(output.status.success(), "ps exited with {}", output.status);
    parse_ps_rss_kib(&output.stdout).context("parse resident RSS from ps")
}

fn parse_proc_status_vm_rss_bytes(status: &str) -> Option<u64> {
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    Some(kib.saturating_mul(1024))
}

fn parse_ps_rss_kib(output: &[u8]) -> anyhow::Result<u64> {
    let text = std::str::from_utf8(output).context("ps RSS is not UTF-8")?;
    let kib = text
        .trim()
        .parse::<u64>()
        .context("ps returned no RSS value")?;
    Ok(kib.saturating_mul(1024))
}

/// Peak resident size of this process, in bytes. `ru_maxrss` is the historical
/// high-water mark used for audit; admissions use current resident RSS instead.
/// macOS reports bytes, Linux kilobytes — the unit belongs to the platform.
fn peak_rss_bytes() -> anyhow::Result<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    anyhow::ensure!(rc == 0, "getrusage failed with {rc}");
    let raw = (unsafe { usage.assume_init() }.ru_maxrss).max(0) as u64;
    Ok(if cfg!(target_os = "linux") {
        raw.saturating_mul(1024)
    } else {
        raw
    })
}

/// One task's loop: bell -> turn -> artifact -> result bell, plus the approval
/// round trip. The body is the same machine `serve` used to run inline; it is
/// per-task now so two tasks can sample at the same time without sharing a
/// history or a session tree.
fn spawn_worker(
    cfg: Arc<PoolConfig>,
    out_tx: mpsc::UnboundedSender<Outbound>,
    task: String,
    engine: PooledEngine,
    mut inbox: mpsc::UnboundedReceiver<TaskMsg>,
) -> AbortOnDropHandle<()> {
    let PooledEngine { handle, mut events } = engine;
    // managed: joined when the pump stops (dropping the sender ends the loop)
    AbortOnDropHandle::new(tokio::spawn(async move {
        let mut pending: VecDeque<(String, Value, u64)> = VecDeque::new();
        // The retransmit clock lives outside the select: a timer future created
        // inside a `select!` arm is rebuilt on every iteration, and one that is
        // always rebuilt never elapses. This one is re-armed only when it fires.
        let mut retry = Box::pin(sleep(cfg.approval_retry));
        loop {
            let (sender, payload, generation) = match pending.pop_front() {
                Some(b) => b,
                None => {
                    // Idle timing runs only when there is genuinely nothing to
                    // do. A timeout is a PROPOSAL: the pump decides, because it
                    // owns the only sender (see `Outbound::RetireRequested`).
                    let received = if cfg.task_idle_timeout.is_zero() {
                        inbox.recv().await
                    } else {
                        match tokio::time::timeout(cfg.task_idle_timeout, inbox.recv()).await {
                            Ok(msg) => msg,
                            Err(_elapsed) => {
                                let _ =
                                    out_tx.send(Outbound::RetireRequested { task: task.clone() });
                                // After proposing, stop timing: either the pump
                                // drops the sender (None -> clean shutdown below)
                                // or it hands over a bell that was already on its
                                // way. A second proposal would be noise.
                                inbox.recv().await
                            }
                        }
                    };
                    match received {
                        Some(TaskMsg::Bell {
                            sender,
                            payload,
                            generation,
                        }) => (sender, payload, generation),
                        Some(TaskMsg::Approval { req, decision, by }) => {
                            // a reply with no turn running has nobody to wake
                            tracing::warn!(
                                "task {task}: late {} reply {req} from {by} is dropped, \
                                 not queued",
                                decision_name(decision)
                            );
                            continue;
                        }
                        None => break,
                    }
                }
            };
            let stage = payload["stage"].as_str().unwrap_or("unknown").to_string();
            let iteration = payload["iteration"].as_u64().unwrap_or(0);
            let approver = cfg.approver.clone().unwrap_or_else(|| sender.clone());
            let input = vec![MessageItem::BusBell {
                sender: sender.clone(),
                text: bell_prompt(&payload),
            }];
            if cfg.role == "qa" {
                // Verifier stages must be independent: a review verdict or a
                // previous build refusal is not evidence for the next stage.
                handle.turn_isolated(input);
            } else {
                handle.turn(input);
            }

            let mut result_text = String::new();
            let mut aborted = false;
            let mut usage = Usage::default();
            let mut approvals: Vec<Value> = Vec::new();
            let mut open: BTreeMap<String, OpenApproval> = BTreeMap::new();
            let stop_seen: Option<StopReason>;
            loop {
                tokio::select! {
                    ev = events.recv() => {
                        let ev: Event = match ev {
                            Some(e) => e,
                            None => {
                                stop_seen = Some(StopReason::error("engine closed", true));
                                break;
                            }
                        };
                        match ev.msg {
                            EventMsg::TurnComplete { stop, usage: seen } => {
                                aborted = matches!(stop, StopReason::Aborted);
                                usage = seen;
                                // 回合结束了，还在途的请求一律关掉：路由表必须有界
                                // （§9.26②），批复再迟到也没有人要了。关掉的同时要
                                // 回执——这一格现在是「没人听了」，审批者不必再猜
                                // 自己那份批复去了哪里。
                                for (req, waiting) in std::mem::take(&mut open) {
                                    let _ = out_tx.send(Outbound::ApprovalClosed { req: req.clone() });
                                    let _ = out_tx.send(Outbound::Notify {
                                        session: waiting.to,
                                        payload: approval_receipt_frame(
                                            &cfg.session,
                                            &req,
                                            ApprovalDecision::Deny,
                                            false,
                                            "the turn ended",
                                        ),
                                    });
                                }
                                stop_seen = Some(stop);
                                break;
                            }
                            EventMsg::Fatal { error } => {
                                result_text = format!("fatal: {error}");
                                for (req, waiting) in std::mem::take(&mut open) {
                                    let _ = out_tx.send(Outbound::ApprovalClosed { req: req.clone() });
                                    let _ = out_tx.send(Outbound::Notify {
                                        session: waiting.to,
                                        payload: approval_receipt_frame(
                                            &cfg.session,
                                            &req,
                                            ApprovalDecision::Deny,
                                            false,
                                            "the turn failed",
                                        ),
                                    });
                                }
                                stop_seen = Some(StopReason::error(error, true));
                                break;
                            }
                            EventMsg::MessageDone {
                                item: MessageItem::AssistantText { text },
                            } => {
                                result_text = text;
                            }
                            // 等待者已经撤摊（到点或被中断）：停止重发、回执一句
                            // 「没生效」，并把它记进工件。不记的话，后来那份重复的
                            // 批复就会在账上伪装成「曾经生效过」。
                            EventMsg::ApprovalExpired { req, reason } => {
                                let target = open
                                    .remove(&req)
                                    .map(|waiting| waiting.to)
                                    .unwrap_or_else(|| approver.clone());
                                let _ = out_tx.send(Outbound::ApprovalClosed { req: req.clone() });
                                approvals.push(serde_json::json!({
                                    "req": req,
                                    "decision": "expired",
                                    "by": target,
                                    "applied": false,
                                    "reason": reason,
                                }));
                                let _ = out_tx.send(Outbound::Notify {
                                    session: target,
                                    payload: approval_receipt_frame(
                                        &cfg.session,
                                        &req,
                                        ApprovalDecision::Deny,
                                        false,
                                        &reason,
                                    ),
                                });
                            }
                            EventMsg::ApprovalRequest { req, prompt, detail } => {
                                let frame = approval_request_frame(
                                    &cfg.session,
                                    &cfg.role,
                                    &req,
                                    &prompt,
                                    &detail,
                                );
                                // ownership first, then the bytes: the pump must
                                // know who to answer before the answer can exist
                                let _ = out_tx.send(Outbound::ApprovalOpened {
                                    req: req.clone(),
                                    task: task.clone(),
                                });
                                let _ = out_tx.send(Outbound::Notify {
                                    session: approver.clone(),
                                    payload: frame.clone(),
                                });
                                open.insert(
                                    req,
                                    OpenApproval {
                                        frame,
                                        to: approver.clone(),
                                        sends: 1,
                                    },
                                );
                            }
                            _ => {}
                        }
                    }
                    // at-least-once delivery (§10 批复的可靠投递): a frame written
                    // into a socket that then died is NOT replayed — §9.21 draws
                    // that line deliberately — so an unanswered request has to ask
                    // again. The approver dedups by req id, so a duplicate cannot
                    // decide twice nor book a second audit pair; the send count is
                    // capped so one lost request cannot evict other sessions from
                    // the hub's parking lot.
                    _ = retry.as_mut(), if !open.is_empty() => {
                        retry = Box::pin(sleep(cfg.approval_retry));
                        for (id, waiting) in open.iter_mut() {
                            if waiting.sends >= ApproveMode::MAX_APPROVAL_SENDS {
                                continue;
                            }
                            waiting.sends += 1;
                            tracing::warn!(
                                "task {task}: approval request {id} is unanswered, re-sending \
                                 ({} of {})",
                                ApproveMode::MAX_APPROVAL_SENDS,
                                waiting.sends
                            );
                            let _ = out_tx.send(Outbound::Notify {
                                session: waiting.to.clone(),
                                payload: waiting.frame.clone(),
                            });
                        }
                    }
                    msg = inbox.recv(), if !open.is_empty() => {
                        match msg {
                            Some(TaskMsg::Approval { req, decision, by }) => {
                                if open.remove(&req).is_some() {
                                    let applied =
                                        handle.respond_to_approval(req.clone(), decision).await;
                                    let _ = out_tx.send(Outbound::ApprovalClosed { req: req.clone() });
                                    if applied {
                                        approvals.push(serde_json::json!({
                                            "req": req,
                                            "decision": decision_name(decision),
                                            "by": by,
                                            "applied": true,
                                        }));
                                        // 生效了也要回执：审批者那条「谁放宽了沙箱」的记录
                                        // 只有拿到这句话才算闭环
                                        let _ = out_tx.send(Outbound::Notify {
                                            session: by,
                                            payload: approval_receipt_frame(
                                                &cfg.session,
                                                &req,
                                                decision,
                                                true,
                                                "the waiting tool was woken",
                                            ),
                                        });
                                    } else {
                                        // 本地 open 表与引擎的等待者之间存在事件竞态。
                                        // 引擎拒绝接收就是事实来源；随后到达的
                                        // ApprovalExpired 会落 expired 记录并回执 false。
                                        tracing::warn!(
                                            "task {task}: approval {req} arrived after its \
                                             waiter expired; not booking it as applied"
                                        );
                                    }
                                } else {
                                    // 已经答过，或回合结束时就关掉了：同一个 req 的
                                    // 第二份 allow 绝不能再把工具跑一遍
                                    tracing::debug!(
                                        "task {task}: duplicate or stale approval reply {req} ignored"
                                    );
                                }
                            }
                            Some(TaskMsg::Bell {
                                sender,
                                payload,
                                generation,
                            }) => {
                                pending.push_back((sender, payload, generation));
                            }
                            None => {
                                // the pump is gone: nobody can deliver a result
                                // bell anyway, so end this task's loop
                                stop_seen = Some(StopReason::error("worker is shutting down", false));
                                break;
                            }
                        }
                    }
                }
            }

            let artifact =
                cfg.artifacts_dir
                    .join(artifact_name(&cfg.session, &task, &stage, iteration));
            let artifact_json = serde_json::json!({
                "session": cfg.session,
                "task_id": task,
                "stage": stage,
                "iteration": iteration,
                "aborted": aborted,
                "stop": stop_seen,
                "result": result_text,
                // 证据是**带类型的声明**（`{"path":..., "kind":"test_log"}`），不是
                // 一串路径：worker 声称那是测试日志，SA 就能在盘上反驳这句话。
                // 正文里没有契约行时退回裸路径，按 `file` 档记。
                "evidence": evidence_from_answer(&result_text),
                // 声明形状的问题在这里就说出来，而不是等判定端悄悄修好（§9.39）：
                // 工件是 worker 留给 SA 的唯一物件，警告要跟着它走，不然这条信息只活在
                // 一个没人读的日志行里。
                "evidence_warnings": annotation_warnings(&result_text),
                "usage": usage,
                "approvals": approvals,
                "ref": payload["ref"],
            });
            let body = serde_json::to_string_pretty(&artifact_json);
            match body
                .map_err(|e| e.to_string())
                .and_then(|body| std::fs::write(&artifact, body).map_err(|e| e.to_string()))
            {
                Ok(()) => {
                    let _ = out_tx.send(Outbound::Notify {
                        session: sender,
                        payload: serde_json::json!({
                            "role": cfg.session,
                            "task_id": task,
                            "stage": stage,
                            "iteration": iteration,
                            "usage": usage,
                            "result_ref": artifact.display().to_string(),
                        }),
                    });
                }
                Err(e) => {
                    // One task's I/O failure must not take the process down; the
                    // SA reads the missing artifact as a fail and re-dispatches.
                    tracing::error!("task {task}: write artifact {}: {e}", artifact.display());
                    let _ = out_tx.send(Outbound::Notify {
                        session: sender,
                        payload: serde_json::json!({
                            "role": cfg.session,
                            "task_id": task,
                            "stage": stage,
                            "iteration": iteration,
                            "error": format!("artifact write failed: {e}"),
                        }),
                    });
                }
            }

            // Completion carries the delivery stamp it served. If another bell
            // was already queued, the router's newest stamp is higher and this
            // event must not make the task a pressure-retirement candidate.
            let _ = out_tx.send(Outbound::TurnFinished {
                task: task.clone(),
                generation,
            });
        }
        handle.shutdown().await;
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_bell_spawns_and_later_ones_deliver() {
        let mut r = Router::new(4);
        assert_eq!(r.on_bell("t1"), Action::Spawn("t1".into()));
        assert_eq!(r.on_bell("t1"), Action::Deliver("t1".into()));
        assert_eq!(r.task_count(), 1, "a task is one engine, not one per bell");
    }

    /// Over the ceiling the pump REFUSES out loud (a visible error bell) instead
    /// of queueing work behind an unbounded engine list.
    #[test]
    fn task_cap_refuses_instead_of_queueing() {
        let mut r = Router::new(2);
        assert!(matches!(r.on_bell("a"), Action::Spawn(_)));
        assert!(matches!(r.on_bell("b"), Action::Spawn(_)));
        let rejected = r.on_bell("c");
        match rejected {
            Action::Reject { task, reason } => {
                assert_eq!(task, "c");
                assert!(reason.contains("cap"), "{reason}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        // an existing task still gets service at the cap
        assert_eq!(r.on_bell("a"), Action::Deliver("a".into()));
        // 0 means no ceiling
        let mut unlimited = Router::new(0);
        for i in 0..16 {
            assert!(matches!(
                unlimited.on_bell(&format!("t{i}")),
                Action::Spawn(_)
            ));
        }
    }

    /// An `allow` routed to the wrong task is a permission bypass, so replies
    /// go only to whoever opened the request and are dropped otherwise.
    #[test]
    fn approval_replies_are_routed_never_broadcast() {
        let mut r = Router::new(4);
        r.on_bell("t1");
        r.on_bell("t2");
        r.on_approval_opened("req-7", "t2");
        assert_eq!(
            r.on_approval_reply(Some("req-7")),
            Action::Reply {
                task: "t2".into(),
                req: "req-7".into()
            }
        );
        // the same reply twice: the second one has no owner
        assert!(matches!(
            r.on_approval_reply(Some("req-7")),
            Action::Drop(_)
        ));
        assert!(matches!(
            r.on_approval_reply(Some("req-never-opened")),
            Action::Drop(_)
        ));
        assert!(matches!(r.on_approval_reply(None), Action::Drop(_)));
        // closing a request prunes the table, so a long-lived worker cannot grow it
        r.on_approval_opened("req-8", "t1");
        assert_eq!(r.open_requests(), 1);
        r.close_request("req-8");
        assert_eq!(r.open_requests(), 0);
    }

    /// A soft ceiling must not strand work that is already running: drain mode
    /// refuses NEW tasks and keeps delivering the ones that own an engine.
    #[test]
    fn drain_mode_refuses_new_tasks_and_keeps_running_ones() {
        let mut r = Router::new(4);
        assert!(matches!(r.on_bell("running"), Action::Spawn(_)));
        r.set_draining("peak RSS crossed the ceiling");
        assert_eq!(r.draining(), Some("peak RSS crossed the ceiling"));
        // the task that already has an engine is still served
        assert_eq!(r.on_bell("running"), Action::Deliver("running".into()));
        // a brand-new task is refused out loud, not queued behind the cap
        match r.on_bell("fresh") {
            Action::Reject { task, reason } => {
                assert_eq!(task, "fresh");
                assert!(reason.contains("peak RSS"), "{reason}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        // the first reason wins: a repeated check must not rewrite the refusal
        r.set_draining("something else");
        assert_eq!(r.draining(), Some("peak RSS crossed the ceiling"));
        // recovery is explicit: only a successful low current-RSS reading
        // clears the state, and a fresh task may then spawn.
        assert!(r.clear_draining());
        assert!(!r.clear_draining());
        assert_eq!(r.draining(), None);
        assert!(matches!(r.on_bell("fresh"), Action::Spawn(_)));
    }

    /// `ru_maxrss` is the historical high-water mark, and its units differ by
    /// platform — this pins that the conversion produced a sane size.
    #[test]
    fn peak_rss_reports_a_plausible_high_water_mark() {
        let peak = peak_rss_bytes().expect("getrusage works in a test process");
        assert!(peak > 0, "peak RSS must be non-zero: {peak}");
        assert!(
            peak < 1 << 40,
            "a unit mix-up would land far outside any plausible range: {peak}"
        );
    }

    #[test]
    fn resident_rss_reports_a_plausible_current_value() {
        let current = resident_rss_bytes().expect("current RSS is observable");
        assert!(current > 0, "current RSS must be non-zero: {current}");
        assert!(
            current < 1 << 40,
            "a unit mix-up would land far outside any plausible range: {current}"
        );
    }

    #[test]
    fn rss_parsers_convert_both_platform_units_to_bytes() {
        assert_eq!(
            parse_proc_status_vm_rss_bytes("Name:\tkestrel\nVmRSS:\t  2048 kB\n"),
            Some(2 * 1024 * 1024)
        );
        assert_eq!(parse_proc_status_vm_rss_bytes("Name:\tkestrel\n"), None);
        assert_eq!(
            parse_ps_rss_kib(b"  3072\n").expect("ps RSS parses"),
            3 * 1024 * 1024
        );
        assert!(parse_ps_rss_kib(b"\n").is_err());
    }

    /// 退休释放 slot，也释放它名下的审批归属：引擎都没了，一条给旧 req 的批复
    /// 不能被重新捡起来路由（§9.26② 的路由表有界在这条上继续成立）。
    #[test]
    fn retiring_a_task_frees_the_slot_and_prunes_its_requests() {
        let mut r = Router::new(1);
        assert!(matches!(r.on_bell("t1"), Action::Spawn(_)));
        r.on_approval_opened("req-1", "t1");
        assert_eq!(r.task_count(), 1);
        assert_eq!(r.open_requests(), 1);

        assert!(r.retire_task("t1"));
        assert!(!r.retire_task("t1"), "retiring twice is a no-op");
        assert_eq!(r.task_count(), 0);
        assert_eq!(r.open_requests(), 0, "归属跟着引擎一起走");
        assert_eq!(r.retired_total(), 1);

        // 上限现在接得住新 task —— 这格存在的理由
        assert!(matches!(r.on_bell("t2"), Action::Spawn(_)));
        assert!(matches!(
            r.on_approval_reply(Some("req-1")),
            Action::Drop(_)
        ));
    }

    /// Memory pressure may release only an engine whose newest delivered bell
    /// has completed. A stale completion cannot make a queued newer bell look
    /// idle, and a new bell makes an idle task busy again.
    #[test]
    fn pressure_retirement_tracks_the_newest_completed_bell() {
        let mut r = Router::new(4);
        assert!(matches!(r.on_bell("t1"), Action::Spawn(_)));
        let first = r.begin_bell("t1");
        assert!(
            r.pressure_retirement_candidates(Duration::ZERO).is_empty(),
            "a running turn is never a pressure-retirement candidate"
        );

        // A second bell is queued before the first turn reports completion.
        assert_eq!(r.on_bell("t1"), Action::Deliver("t1".into()));
        let second = r.begin_bell("t1");
        assert!(
            !r.on_turn_finished("t1", first),
            "the stale completion must not mark the newer queued bell idle"
        );
        assert!(r.pressure_retirement_candidates(Duration::ZERO).is_empty());

        assert!(r.on_turn_finished("t1", second));
        assert_eq!(
            r.pressure_retirement_candidates(Duration::ZERO),
            vec!["t1".to_string()]
        );
        assert!(
            r.pressure_retirement_candidates(Duration::from_secs(3600))
                .is_empty(),
            "an engine is not reclaimed before the grace period"
        );

        assert_eq!(r.on_bell("t1"), Action::Deliver("t1".into()));
        assert!(
            r.pressure_retirement_candidates(Duration::ZERO).is_empty(),
            "a new bell wins over the old idle state"
        );
    }

    #[test]
    fn pressure_retirement_releases_the_idle_engine_and_counts_it() {
        let mut r = Router::new(1);
        assert!(matches!(r.on_bell("t1"), Action::Spawn(_)));
        let generation = r.begin_bell("t1");
        assert!(r.on_turn_finished("t1", generation));
        assert_eq!(
            r.pressure_retirement_candidates(Duration::ZERO),
            vec!["t1".to_string()]
        );

        assert!(r.retire_under_pressure("t1"));
        assert!(!r.retire_under_pressure("t1"));
        assert_eq!(r.task_count(), 0);
        assert_eq!(r.retired_total(), 1);
        assert_eq!(r.pressure_retired_total(), 1);
        assert!(matches!(r.on_bell("t2"), Action::Spawn(_)));
    }

    #[test]
    fn worker_status_document_is_atomic_and_reports_router_state() {
        let mut router = Router::new(4);
        assert!(matches!(router.on_bell("t2"), Action::Spawn(_)));
        assert!(matches!(router.on_bell("t1"), Action::Spawn(_)));
        let t2_generation = router.begin_bell("t2");
        let t1_generation = router.begin_bell("t1");
        assert!(router.on_turn_finished("t1", t1_generation));
        assert!(!router.on_turn_finished("t2", t2_generation.saturating_sub(1)));
        router.on_approval_opened("req-1", "t2");
        let cfg = PoolConfig {
            session: "harness-rd".into(),
            role: "rd".into(),
            artifacts_dir: std::env::temp_dir().join("unused-artifacts"),
            approver: None,
            max_tasks: 4,
            max_parallel_tools: 4,
            max_worker_tools: 8,
            max_rss_bytes: 0,
            task_idle_timeout: Duration::ZERO,
            approval_retry: Duration::from_secs(1),
            status_file: None,
            status_interval: Duration::from_secs(5),
        };
        let document = status_document(&cfg, &router);
        assert_eq!(document["active_tasks"], serde_json::json!(["t1", "t2"]));
        assert_eq!(document["idle_tasks"], serde_json::json!(["t1"]));
        assert_eq!(document["task_count"], 2);
        assert_eq!(document["open_approvals"], 1);
        assert_eq!(document["max_worker_tools"], 8);
        assert_eq!(document["pressure_retirements"], 0);
        assert!(
            document["resident_rss_bytes"].is_u64(),
            "current resident RSS is part of the status document"
        );

        let path =
            std::env::temp_dir().join(format!("kestrel-worker-status-{}.json", std::process::id()));
        write_status_file(&path, &document).unwrap();
        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(written["session"], "harness-rd");
        assert!(
            !path
                .with_file_name(format!(
                    ".{}.{}.tmp",
                    path.file_name().unwrap().to_str().unwrap(),
                    std::process::id()
                ))
                .exists(),
            "the staged status file was renamed, not left behind"
        );
        std::fs::remove_file(path).ok();
    }
}
