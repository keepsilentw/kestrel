//! BusHub: UDS listener + per-connection tasks.哑 RPC 路由 (plan §2.8):
//! Hello 注册 peer/session；Notify 按 session 转发到目标连接的铃队列；
//! Request/Response 按 req 关联；Cancel 终止在途请求；未知 session 回
//! Error 帧且不 panic。
//!
//! 韧性（design §10 bus 重连重放）：断线只清 peer 连接表，**保留 session 归属**，
//! 于是「注册过但现在不在线」的 session 能被区分出来——给它的 Notify 进停车队列
//! （按序、有上限），等它重新 Hello 时补投；从未注册过的地址仍然立刻回 Error，
//! 因为把拼错的 session 名静默停车等于让铃永远没人听见。RPC 不跨重连：
//! Response 的路由属于那条已死的连接，客户端会 fail fast 而不是等到超期。

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;

use super::frame::{
    Envelope, FrameKind, MAX_FRAME_BYTES, PeerId, ReqId, Route, SessionId, decode_envelope,
    encode_frame,
};
use super::identity::{IdentityCheck, IdentityGuard};

/// Frames parked per offline session before the oldest is dropped. A worker
/// restart takes seconds; an unbounded queue turns a dead worker into a dead
/// hub.
const PARK_SESSION_CAP: usize = 32;
/// Global ceiling across sessions — the hub is a router, not a maildir.
const PARK_TOTAL_CAP: usize = 256;
const HUB_STATE_VERSION: u32 = 2;
/// Sessions nobody has re-Helloed for this long are stale. Seven days is long
/// enough for a multi-day outage and short enough that a renamed session does
/// not live forever in the persisted table.
const SESSION_TTL_SECS: u64 = 7 * 24 * 60 * 60;
/// Parking is bounded by frames AND bytes: 256 frames of up to 4 MiB each could
/// otherwise put ~1 GiB into the state file.
const PARK_TOTAL_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSession {
    peer: PeerId,
    seen_at: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedHubState {
    #[serde(default)]
    version: u32,
    #[serde(default)]
    sessions: BTreeMap<SessionId, PersistedSession>,
    #[serde(default)]
    parked: BTreeMap<SessionId, Vec<Envelope>>,
}

fn default_state_path(socket: &Path) -> PathBuf {
    let mut name = socket.as_os_str().to_os_string();
    name.push(".state.json");
    PathBuf::from(name)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn load_state(path: &Path) -> anyhow::Result<PersistedHubState> {
    if !path.exists() {
        return Ok(PersistedHubState {
            version: HUB_STATE_VERSION,
            ..Default::default()
        });
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read hub state {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("parse hub state {}", path.display()))?;
    match value["version"].as_u64().unwrap_or(0) {
        version if version == u64::from(HUB_STATE_VERSION) => serde_json::from_value(value)
            .with_context(|| format!("decode hub state {}", path.display())),
        // v1 stored `sessions: {session: peer}` with no timestamp. Keep those
        // sessions alive for one TTL window starting at migration time.
        0 | 1 => {
            #[derive(Deserialize)]
            struct V1 {
                #[serde(default)]
                sessions: BTreeMap<SessionId, PeerId>,
                #[serde(default)]
                parked: BTreeMap<SessionId, Vec<Envelope>>,
            }
            let old: V1 = serde_json::from_value(value)
                .with_context(|| format!("decode v1 hub state {}", path.display()))?;
            let now = now_unix();
            Ok(PersistedHubState {
                version: HUB_STATE_VERSION,
                sessions: old
                    .sessions
                    .into_iter()
                    .map(|(session, peer)| (session, PersistedSession { peer, seen_at: now }))
                    .collect(),
                parked: old.parked,
            })
        }
        version => anyhow::bail!(
            "hub state {} has unsupported version {version}",
            path.display()
        ),
    }
}

fn write_state(path: &Path, state: &PersistedHubState) -> anyhow::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let body = serde_json::to_vec_pretty(state)?;
    let staged = path.with_extension("new");
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&staged)
        .with_context(|| format!("open hub state staging file {}", staged.display()))?;
    std::io::Write::write_all(&mut file, &body)?;
    file.sync_all()?;
    std::fs::rename(&staged, path)
        .with_context(|| format!("publish hub state {}", path.display()))?;
    Ok(())
}

fn envelope_bytes(env: &Envelope) -> usize {
    serde_json::to_vec(env)
        .map(|body| body.len())
        .unwrap_or_default()
}

fn parked_bytes(parked: &HashMap<SessionId, VecDeque<Envelope>>) -> usize {
    parked
        .values()
        .flat_map(|queue| queue.iter())
        .map(envelope_bytes)
        .sum()
}

/// Bound the parked table by frames and bytes, dropping the oldest frame when
/// either ceiling would be crossed. A single frame larger than the byte ceiling
/// is not parked at all; letting it in would defeat the bound immediately.
fn park_envelope(
    parked: &mut HashMap<SessionId, VecDeque<Envelope>>,
    session: &str,
    env: Envelope,
    byte_cap: usize,
) -> bool {
    let frame_bytes = envelope_bytes(&env);
    if frame_bytes > byte_cap {
        tracing::warn!(
            "bus parking byte cap ({byte_cap}): dropping oversized notify for {session} \
             ({frame_bytes} bytes)"
        );
        return false;
    }
    let total_frames: usize = parked.values().map(VecDeque::len).sum();
    if total_frames >= PARK_TOTAL_CAP {
        tracing::warn!("bus parking full ({PARK_TOTAL_CAP}): dropping notify for {session}");
        return false;
    }

    let mut total_bytes = parked_bytes(parked);
    if let Some(queue) = parked.get_mut(session)
        && queue.len() >= PARK_SESSION_CAP
    {
        let dropped = queue.pop_front();
        if let Some(frame) = &dropped {
            total_bytes = total_bytes.saturating_sub(envelope_bytes(frame));
        }
        tracing::warn!(
            "bus parking overflow for {session}: dropping the oldest bell ({:?})",
            dropped.map(|frame| frame.route.req)
        );
    }
    while total_bytes.saturating_add(frame_bytes) > byte_cap {
        let mut dropped = parked.get_mut(session).and_then(VecDeque::pop_front);
        if dropped.is_none() {
            dropped = parked.iter_mut().find_map(|(_, queue)| queue.pop_front());
        }
        let Some(frame) = dropped else {
            break;
        };
        total_bytes = total_bytes.saturating_sub(envelope_bytes(&frame));
        tracing::warn!(
            "bus parking byte cap ({byte_cap}): dropping oldest bell ({:?}) for {session}",
            frame.route.req
        );
    }
    if total_bytes.saturating_add(frame_bytes) > byte_cap {
        tracing::warn!("bus parking byte cap ({byte_cap}): dropping notify for {session}");
        return false;
    }
    parked
        .entry(session.to_string())
        .or_default()
        .push_back(env);
    true
}

pub struct BusHub {
    peers: Mutex<HashMap<PeerId, mpsc::UnboundedSender<Envelope>>>,
    /// session id (and every peer id) → peer that owns it. Entries SURVIVE the
    /// peer disconnecting: that is what makes "known but offline" (park it)
    /// distinguishable from "never registered" (Error frame). The map is bounded
    /// by the number of distinct session ids this hub process has ever seen —
    /// parking lives and dies with the hub, so no eviction policy is worth its
    /// complexity (design §9.21).
    sessions: Mutex<HashMap<SessionId, PeerId>>,
    /// in-flight requests awaiting a Response frame.
    requests: Mutex<HashMap<ReqId, mpsc::UnboundedSender<Envelope>>>,
    /// Notify frames waiting for an offline session to come back.
    parked: Mutex<HashMap<SessionId, VecDeque<Envelope>>>,
    /// Last Hello time per session key; drives the persisted-table TTL.
    session_seen: Mutex<HashMap<SessionId, u64>>,
    path: PathBuf,
    state_path: PathBuf,
    persist_lock: Mutex<()>,
}

impl BusHub {
    /// Bind + spawn the accept loop. Stale socket from a killed run is removed.
    pub fn bind(path: &Path) -> anyhow::Result<Arc<Self>> {
        Self::bind_at(path, &default_state_path(path))
    }

    /// Bind with an explicit state file. Tests use a second socket to simulate
    /// a hub process restart without giving up the persisted parking table.
    pub fn bind_at(path: &Path, state_path: &Path) -> anyhow::Result<Arc<Self>> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let _ = std::fs::remove_file(path);
        let listener = tokio::net::UnixListener::bind(path)?;
        let persisted = match load_state(state_path) {
            Ok(state) => state,
            Err(error) => {
                tracing::error!(
                    "load hub state {}: {error}; starting with an empty parking table",
                    state_path.display()
                );
                PersistedHubState {
                    version: HUB_STATE_VERSION,
                    ..Default::default()
                }
            }
        };
        let sessions = persisted
            .sessions
            .iter()
            .map(|(session, record)| (session.clone(), record.peer.clone()))
            .collect();
        let session_seen = persisted
            .sessions
            .iter()
            .map(|(session, record)| (session.clone(), record.seen_at))
            .collect();
        let parked = persisted
            .parked
            .into_iter()
            .map(|(session, frames)| (session, frames.into()))
            .collect();
        let hub = Arc::new(Self {
            peers: Mutex::new(HashMap::new()),
            sessions: Mutex::new(sessions),
            requests: Mutex::new(HashMap::new()),
            parked: Mutex::new(parked),
            session_seen: Mutex::new(session_seen),
            path: path.to_path_buf(),
            state_path: state_path.to_path_buf(),
            persist_lock: Mutex::new(()),
        });
        if let Err(error) = hub.prune_stale_sessions(now_unix()) {
            tracing::warn!("prune stale hub sessions on bind: {error}");
        }
        let hub2 = Arc::clone(&hub);
        tokio::spawn(async move {
            let listener = listener;
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => hub2.spawn_conn(stream),
                    Err(e) => {
                        tracing::warn!("bus accept error: {e}");
                        break;
                    }
                }
            }
        });
        Ok(hub)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    /// Flush the current session/parking table. Called after Hello, park and
    /// replay; failures are logged by callers because the in-memory router must
    /// keep serving even when the state file cannot be written.
    pub fn persist_now(&self) -> anyhow::Result<()> {
        let _guard = self.persist_lock.lock().unwrap();
        let now = now_unix();
        self.prune_stale_sessions(now)?;
        let seen = self.session_seen.lock().unwrap();
        let sessions = self
            .sessions
            .lock()
            .unwrap()
            .iter()
            .map(|(session, peer)| {
                (
                    session.clone(),
                    PersistedSession {
                        peer: peer.clone(),
                        seen_at: seen.get(session).copied().unwrap_or(now),
                    },
                )
            })
            .collect();
        let parked = self
            .parked
            .lock()
            .unwrap()
            .iter()
            .map(|(session, frames)| (session.clone(), frames.iter().cloned().collect()))
            .collect();
        write_state(
            &self.state_path,
            &PersistedHubState {
                version: HUB_STATE_VERSION,
                sessions,
                parked,
            },
        )
    }

    /// Drop sessions whose last Hello is older than `SESSION_TTL_SECS`, then
    /// drop parked queues whose session key no longer exists. The TTL prevents
    /// a renamed/retired worker from being remembered forever; orphan pruning
    /// keeps a hand-edited or partially migrated state file self-consistent.
    fn prune_stale_sessions(&self, now: u64) -> anyhow::Result<usize> {
        let stale: Vec<SessionId> = {
            let seen = self.session_seen.lock().unwrap();
            seen.iter()
                .filter(|(_, at)| now.saturating_sub(**at) > SESSION_TTL_SECS)
                .map(|(session, _)| session.clone())
                .collect()
        };
        let known: HashSet<SessionId> = {
            let mut sessions = self.sessions.lock().unwrap();
            for session in &stale {
                sessions.remove(session);
            }
            sessions.keys().cloned().collect()
        };
        {
            let mut parked = self.parked.lock().unwrap();
            for session in &stale {
                parked.remove(session);
            }
            parked.retain(|session, _| known.contains(session));
        }
        {
            let mut seen = self.session_seen.lock().unwrap();
            for session in &stale {
                seen.remove(session);
            }
        }
        Ok(stale.len())
    }

    fn spawn_conn(self: &Arc<Self>, stream: tokio::net::UnixStream) {
        let hub = Arc::clone(self);
        tokio::spawn(async move {
            let (mut rd, mut wr) = stream.into_split();
            let (tx, mut rx) = mpsc::unbounded_channel::<Envelope>();
            // writer task: serialize outbound frames
            let writer = AbortOnDropHandle::new(tokio::spawn(async move {
                while let Some(env) = rx.recv().await {
                    match encode_frame(&env) {
                        Ok(bytes) => {
                            if wr.write_all(&bytes).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!("bus encode error: {e}");
                            break;
                        }
                    }
                }
            }));
            let mut guard = IdentityGuard::new();
            let mut registered_peer: Option<PeerId> = None;
            loop {
                let mut len_buf = [0u8; 4];
                if rd.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                if len > MAX_FRAME_BYTES - 4 {
                    tracing::warn!("bus frame oversized: {len}");
                    break;
                }
                let mut body = vec![0u8; len];
                if rd.read_exact(&mut body).await.is_err() {
                    break;
                }
                let env = match decode_envelope(&body) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!("bus bad frame: {e}");
                        break;
                    }
                };
                match guard.check(&env) {
                    IdentityCheck::Ok => {}
                    IdentityCheck::NeedsHello => {
                        let _ = tx.send(error_frame(&env.route, "hello required"));
                        continue;
                    }
                    IdentityCheck::Mismatch { announced, claimed } => {
                        tracing::warn!(
                            "bus identity mismatch: announced={announced} claimed={claimed}"
                        );
                        break; // drop the connection
                    }
                }
                if env.kind == FrameKind::Hello {
                    let peer = guard.peer().cloned().unwrap_or_default();
                    registered_peer = Some(peer.clone());
                    hub.peers.lock().unwrap().insert(peer.clone(), tx.clone());
                    // 每个 peer 也可按 peer id 直达（SA 没有 session），并且这个
                    // key 同样能停车——重启的 SA 不该丢掉别人发给它的审批请求。
                    let mut keys: Vec<SessionId> = vec![peer.clone()];
                    if let Some(session) = env.payload.get("session").and_then(|s| s.as_str())
                        && !keys.iter().any(|k| k == session)
                    {
                        keys.push(session.to_string());
                    }
                    {
                        let mut sessions = hub.sessions.lock().unwrap();
                        for key in &keys {
                            sessions.insert(key.clone(), peer.clone());
                        }
                    }
                    {
                        let now = now_unix();
                        let mut seen = hub.session_seen.lock().unwrap();
                        for key in &keys {
                            seen.insert(key.clone(), now);
                        }
                    }
                    // 补投离线期间停下的铃，先进先出，顺序即派发顺序
                    for key in &keys {
                        for frame in hub.take_parked(key) {
                            let _ = tx.send(frame);
                        }
                    }
                    if let Err(error) = hub.persist_now() {
                        tracing::warn!("persist bus state after Hello: {error}");
                    }
                    continue;
                }
                hub.route_frame(env, &tx).await;
            }
            drop(writer);
            if let Some(peer) = registered_peer {
                hub.peers.lock().unwrap().remove(&peer);
                // sessions 条目保留（见文件头）；但在途 Request 的回复通道属于这条
                // 死连接，清掉它们，别让调用方等到自己的超时。
                hub.requests
                    .lock()
                    .unwrap()
                    .retain(|_, reply| !reply.is_closed());
            }
        });
    }

    /// Park one Notify for a known-but-offline session (bounded, drop-oldest).
    fn park(&self, session: &str, env: Envelope) {
        let accepted = {
            let mut parked = self.parked.lock().unwrap();
            park_envelope(&mut parked, session, env, PARK_TOTAL_BYTES)
        };
        if accepted && let Err(error) = self.persist_now() {
            tracing::warn!("persist bus state after parking a notify: {error}");
        }
    }

    fn take_parked(&self, session: &str) -> VecDeque<Envelope> {
        self.parked
            .lock()
            .unwrap()
            .remove(session)
            .unwrap_or_default()
    }

    /// Route one validated frame (Notify/Request/Response/Cancel).
    async fn route_frame(&self, env: Envelope, reply: &mpsc::UnboundedSender<Envelope>) {
        match env.kind {
            FrameKind::Notify | FrameKind::Request => {
                let session = env.route.session.clone();
                // session 路由优先；peer 直达兜底（Hello 把 peer id 也登记成可寻址
                // key，所以一个 peer 既能被 session 名也能被 peer 名找到）
                let owner: Option<PeerId> = self
                    .sessions
                    .lock()
                    .unwrap()
                    .get(&session)
                    .cloned()
                    .or_else(|| {
                        self.peers
                            .lock()
                            .unwrap()
                            .contains_key(&session)
                            .then(|| session.clone())
                    });
                let live: Option<mpsc::UnboundedSender<Envelope>> = owner
                    .as_ref()
                    .and_then(|p| self.peers.lock().unwrap().get(p).cloned());
                match live {
                    Some(tx) => {
                        if env.kind == FrameKind::Request {
                            self.requests
                                .lock()
                                .unwrap()
                                .insert(env.route.req.clone(), reply.clone());
                        }
                        let _ = tx.send(env);
                    }
                    None => {
                        // 注册过但不在线：Notify 停车待补投；Request 立即回错——
                        // 它的 Response 永远不会有归属的连接了。
                        let known = owner.is_some();
                        if known && env.kind == FrameKind::Notify {
                            self.park(&session, env);
                        } else if known {
                            let _ = reply.send(error_frame(&env.route, "peer offline"));
                        } else {
                            let _ = reply.send(error_frame(&env.route, "unknown session"));
                        }
                    }
                }
            }
            FrameKind::Response => {
                let req = env.route.req.clone();
                if let Some(waiter) = self.requests.lock().unwrap().remove(&req) {
                    let _ = waiter.send(env);
                }
            }
            FrameKind::Cancel => {
                let req = env.route.req.clone();
                if let Some(waiter) = self.requests.lock().unwrap().remove(&req) {
                    let _ = waiter.send(error_frame(&env.route, "aborted"));
                }
            }
            _ => {}
        }
    }
}

fn error_frame(route: &Route, message: &str) -> Envelope {
    Envelope {
        v: 1,
        sender: "bus".into(),
        route: route.clone(),
        kind: FrameKind::Error,
        payload: serde_json::json!({ "error": message }),
    }
}

/// Client half: connect, announce, and expose typed helpers.
///
/// 韧性边界（design §10 bus 重连重放）：
/// - 送不出去的 Notify 按序进**有界**重放队列，下一次重连成功后补发；
/// - `recv_notify` 在连接断开时自动重连（退避，一个 `RECONNECT_WINDOW` 窗口内），
///   所以 hub 重启不会把 worker 打死；
/// - `request` **不跨重连**：Response 的归属是那条已经死掉的连接，所以它立刻失败，
///   而不是让调用方等到自己的超时。
pub struct BusClient {
    pub peer_id: PeerId,
    path: PathBuf,
    session: Option<SessionId>,
    conn: Mutex<Conn>,
    next_req: AtomicU64,
}

/// One live socket plus the queues that must outlive it. Everything starts
/// empty: `None` channels mean "not connected yet", which is also the state a
/// failed reconnect leaves behind.
#[derive(Default)]
struct Conn {
    /// outbound pump channel of the LIVE socket; `None` while disconnected
    tx: Option<mpsc::UnboundedSender<Envelope>>,
    /// inbound frames of the LIVE socket; taken out for the duration of a poll
    inbox: Option<mpsc::UnboundedReceiver<Envelope>>,
    /// frames that arrived while a correlated poll was waiting for another req
    parked: Vec<Envelope>,
    /// frames a dead socket never took, oldest first
    replay: VecDeque<Envelope>,
    /// reader + writer of the LIVE socket, aborted when the connection is replaced
    tasks: Vec<AbortOnDropHandle<()>>,
}

/// Frames kept while the socket is dead (drop-oldest beyond this).
const REPLAY_CAPACITY: usize = 64;
/// How long one inbound poll keeps trying to bring the connection back.
const RECONNECT_WINDOW: Duration = Duration::from_secs(10);

/// RAII owner of a borrowed inbox receiver. Dropping the guard without
/// consuming it puts the receiver back, which is what makes `recv_notify()`
/// safe to poll inside `tokio::select!` (a cancelled branch must not take the
/// inbox out of the connection for good).
struct Taken<'a> {
    conn: &'a Mutex<Conn>,
    rx: Option<mpsc::UnboundedReceiver<Envelope>>,
}

impl Taken<'_> {
    /// Put the receiver back for the next poll.
    fn restore(&mut self) {
        if let Some(rx) = self.rx.take() {
            self.conn.lock().unwrap().inbox = Some(rx);
        }
    }

    /// The connection is dead: throw the receiver away instead of restoring it.
    fn close(&mut self) {
        self.rx = None;
    }
}

impl Drop for Taken<'_> {
    fn drop(&mut self) {
        self.restore();
    }
}

impl BusClient {
    /// Connect + Hello. `session` registers this connection as the handler
    /// for Notify frames addressed to that session (worker mode). Fails fast
    /// when there is no hub to talk to; recovery starts at the first read.
    pub async fn connect(
        path: &Path,
        peer_id: &str,
        session: Option<&str>,
    ) -> anyhow::Result<Self> {
        let client = Self {
            peer_id: peer_id.to_string(),
            path: path.to_path_buf(),
            session: session.map(str::to_string),
            conn: Mutex::new(Conn::default()),
            next_req: AtomicU64::new(1),
        };
        client.dial().await?;
        Ok(client)
    }

    /// Dial + Hello + the two pump tasks. Replaces a dead connection's
    /// channels; `parked` and `replay` survive, in order.
    async fn dial(&self) -> anyhow::Result<()> {
        let stream = tokio::net::UnixStream::connect(&self.path)
            .await
            .with_context(|| format!("dial bus at {}", self.path.display()))?;
        let (rd, mut wr) = stream.into_split();
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Envelope>();
        let (in_tx, in_rx) = mpsc::unbounded_channel::<Envelope>();

        let mut payload = serde_json::json!({ "peer_id": self.peer_id });
        if let Some(s) = &self.session {
            payload["session"] = serde_json::json!(s);
        }
        let hello = Envelope {
            v: 1,
            sender: self.peer_id.clone(),
            route: Route {
                session: self.session.clone().unwrap_or_default(),
                req: String::new(),
            },
            kind: FrameKind::Hello,
            payload,
        };
        wr.write_all(&encode_frame(&hello)?).await?;

        // reader: anything from the hub lands in the shared inbox channel
        // managed: aborted when this connection is replaced or the client drops
        let reader = tokio::spawn(async move {
            let mut rd = rd;
            let mut len_buf = [0u8; 4];
            loop {
                if rd.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                if len > MAX_FRAME_BYTES - 4 {
                    break;
                }
                let mut body = vec![0u8; len];
                if rd.read_exact(&mut body).await.is_err() {
                    break;
                }
                let Ok(env) = decode_envelope(&body) else {
                    continue;
                };
                if matches!(
                    env.kind,
                    FrameKind::Notify | FrameKind::Response | FrameKind::Error
                ) {
                    let _ = in_tx.send(env);
                }
            }
        });

        // pump outbound frames from tx to socket
        // managed: aborted when this connection is replaced or the client drops
        let writer = tokio::spawn(async move {
            while let Some(env) = out_rx.recv().await {
                match encode_frame(&env) {
                    Ok(b) => {
                        if wr.write_all(&b).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let mut conn = self.conn.lock().unwrap();
        conn.tasks = vec![
            AbortOnDropHandle::new(reader),
            AbortOnDropHandle::new(writer),
        ];
        conn.tx = Some(out_tx);
        conn.inbox = Some(in_rx);
        Ok(())
    }

    /// Hand a frame to the live socket. `queue` decides what happens when there
    /// is no live socket: a Notify waits for the next reconnect, a Request does
    /// not — its Response would be routed back to the dead connection.
    /// `false` means the frame did not go out now.
    fn send(&self, env: Envelope, queue: bool) -> bool {
        let mut conn = self.conn.lock().unwrap();
        if conn
            .tx
            .as_ref()
            .is_some_and(|tx| tx.send(env.clone()).is_ok())
        {
            return true;
        }
        // the channel is gone: mark the connection dead and keep the frame
        conn.tx = None;
        if !queue {
            return false;
        }
        if conn.replay.len() >= REPLAY_CAPACITY {
            let dropped = conn.replay.pop_front();
            tracing::warn!(
                "bus replay queue full: dropping the oldest frame ({:?})",
                dropped.map(|d| d.route.req)
            );
        }
        conn.replay.push_back(env);
        false
    }

    /// Replay what the dead socket never took, oldest first. Frames already
    /// written into a socket that then died are NOT replayed: the hub may have
    /// acted on them, and the SA-side write-ahead reconciliation is what makes
    /// a duplicate or a missing bell harmless.
    fn flush_replay(&self) -> usize {
        let mut conn = self.conn.lock().unwrap();
        let Some(tx) = conn.tx.clone() else { return 0 };
        let mut sent = 0;
        while let Some(env) = conn.replay.pop_front() {
            if tx.send(env.clone()).is_err() {
                conn.replay.push_front(env);
                break;
            }
            sent += 1;
        }
        sent
    }

    /// Bring the connection back, with backoff, inside one window.
    async fn ensure_connected(&self) -> bool {
        if self.conn.lock().unwrap().tx.is_some() {
            return true;
        }
        let mut delay = Duration::from_millis(100);
        let deadline = tokio::time::Instant::now() + RECONNECT_WINDOW;
        loop {
            match self.dial().await {
                Ok(()) => {
                    let replayed = self.flush_replay();
                    tracing::info!("bus reconnected: replayed {replayed} queued frame(s)");
                    return true;
                }
                Err(e) => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        tracing::warn!("bus reconnect window ({RECONNECT_WINDOW:?}) over: {e}");
                        return false;
                    }
                    tokio::time::sleep(delay.min(deadline - now)).await;
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
            }
        }
    }

    fn next_req_id(&self, prefix: &str) -> ReqId {
        let n = self.next_req.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{n}")
    }

    /// Drop the socket deterministically so the hub sees EOF at once and starts
    /// parking for us.
    fn close(&mut self) {
        if let Ok(mut conn) = self.conn.lock() {
            conn.tasks.clear();
            conn.tx = None;
            conn.inbox = None;
        }
    }

    /// Fire-and-forget doorbell (薄消息; write-ahead 在调用侧保证).
    pub fn notify(&self, session: &str, payload: serde_json::Value) {
        let req = self.next_req_id("n");
        let env = Envelope {
            v: 1,
            sender: self.peer_id.clone(),
            route: Route {
                session: session.to_string(),
                req: req.clone(),
            },
            kind: FrameKind::Notify,
            payload,
        };
        if !self.send(env, true) {
            tracing::warn!("bus offline: notify {req} for {session} queued for replay");
        }
    }

    /// Take the next inbound frame, matching `req` when given; non-matching
    /// frames park for other waiters. Never holds the lock across await:
    /// the receiver is taken out for the duration of the poll.
    async fn take_inbox(&self, matching: Option<&ReqId>) -> anyhow::Result<Envelope> {
        loop {
            {
                let mut conn = self.conn.lock().unwrap();
                let parked_hit = matching.and_then(|req| {
                    conn.parked
                        .iter()
                        .position(|e| &e.route.req == req)
                        .map(|i| (req, i))
                });
                if let Some((req, i)) = parked_hit {
                    debug_assert_eq!(&conn.parked[i].route.req, req);
                    return Ok(conn.parked.remove(i));
                }
            }
            // The receiver is handed out through a guard: a `tokio::select!` that
            // loses the race DROPS this future mid-await, and a receiver that
            // never gets put back would leave every later poll of this client
            // failing with "inbox already polled" (observed as a silently dead
            // pump in the worker pool).
            let mut taken = Taken {
                conn: &self.conn,
                rx: self.conn.lock().unwrap().inbox.take(),
            };
            let got = match taken.rx.as_mut() {
                Some(rx) => rx.recv().await,
                None => anyhow::bail!("inbox already polled"),
            };
            match got {
                Some(env) => {
                    taken.restore();
                    match matching {
                        Some(req) if env.route.req != *req => {
                            self.conn.lock().unwrap().parked.push(env);
                        }
                        _ => return Ok(env),
                    }
                }
                None => {
                    // The socket died: drop the receiver, mark the connection
                    // dead, and keep the queues (parked/replay) intact.
                    taken.close();
                    self.conn.lock().unwrap().tx = None;
                    if matching.is_some() {
                        // An RPC cannot survive a reconnect: the hub routes the
                        // Response back over the connection that registered it.
                        anyhow::bail!("bus closed: correlated request cannot be replayed");
                    }
                    if !self.ensure_connected().await {
                        anyhow::bail!("bus closed");
                    }
                }
            }
        }
    }

    /// RPC: send Request, await the correlated Response (Error frame -> Err).
    pub async fn request(
        &self,
        session: &str,
        payload: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let req = self.next_req_id("q");
        let env = Envelope {
            v: 1,
            sender: self.peer_id.clone(),
            route: Route {
                session: session.to_string(),
                req: req.clone(),
            },
            kind: FrameKind::Request,
            payload,
        };
        // No replay queue for RPC: fail now, while the caller still has a
        // choice to make.
        if !self.send(env, false) {
            anyhow::bail!("bus offline: request {req} was not sent");
        }
        let env = self.take_inbox(Some(&req)).await?;
        if env.kind == FrameKind::Error {
            anyhow::bail!("{}", env.payload["error"].as_str().unwrap_or("bus error"));
        }
        Ok(env.payload)
    }

    /// Receive the next notification (doorbell) addressed to our session.
    pub async fn recv_notify(&self) -> anyhow::Result<Envelope> {
        // A dead socket is recovered here rather than returned as an error: a
        // hub restart must not kill a resident worker.
        self.ensure_connected()
            .await
            .ok_or_else(|| anyhow::anyhow!("bus at {} is unreachable", self.path.display()))?;
        self.take_inbox(None).await
    }
}

impl Drop for BusClient {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(req: &str, payload_bytes: usize) -> Envelope {
        Envelope {
            v: 1,
            sender: "harness-sa".into(),
            route: Route {
                session: "rd-1".into(),
                req: req.into(),
            },
            kind: FrameKind::Notify,
            payload: serde_json::json!({
                "task": req,
                "pad": "x".repeat(payload_bytes),
            }),
        }
    }

    #[test]
    fn parking_byte_cap_drops_the_oldest_frame() {
        let cap = 1200;
        let mut parked = HashMap::new();
        assert!(park_envelope(&mut parked, "rd-1", env("first", 500), cap));
        assert!(park_envelope(&mut parked, "rd-1", env("second", 500), cap));
        let queue = parked.get("rd-1").expect("queue");
        assert_eq!(queue.len(), 1, "the first frame must be evicted");
        assert_eq!(queue[0].route.req, "second");
    }

    #[test]
    fn a_frame_larger_than_the_byte_cap_is_not_parked() {
        let mut parked = HashMap::new();
        assert!(!park_envelope(&mut parked, "rd-1", env("huge", 5000), 1200));
        assert!(!parked.contains_key("rd-1"));
    }
}
