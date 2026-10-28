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

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
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
    path: PathBuf,
}

impl BusHub {
    /// Bind + spawn the accept loop. Stale socket from a killed run is removed.
    pub fn bind(path: &Path) -> anyhow::Result<Arc<Self>> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let _ = std::fs::remove_file(path);
        let listener = tokio::net::UnixListener::bind(path)?;
        let hub = Arc::new(Self {
            peers: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            requests: Mutex::new(HashMap::new()),
            parked: Mutex::new(HashMap::new()),
            path: path.to_path_buf(),
        });
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
                    // 补投离线期间停下的铃，先进先出，顺序即派发顺序
                    for key in &keys {
                        for frame in hub.take_parked(key) {
                            let _ = tx.send(frame);
                        }
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
        let mut parked = self.parked.lock().unwrap();
        let total: usize = parked.values().map(|q| q.len()).sum();
        if total >= PARK_TOTAL_CAP {
            tracing::warn!("bus parking full ({PARK_TOTAL_CAP}): dropping notify for {session}");
            return;
        }
        let queue = parked.entry(session.to_string()).or_default();
        if queue.len() >= PARK_SESSION_CAP {
            let dropped = queue.pop_front();
            tracing::warn!(
                "bus parking overflow for {session}: dropping the oldest bell ({:?})",
                dropped.map(|d| d.route.req)
            );
        }
        queue.push_back(env);
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
            let mut rx = self
                .conn
                .lock()
                .unwrap()
                .inbox
                .take()
                .ok_or_else(|| anyhow::anyhow!("inbox already polled"))?;
            let got = rx.recv().await;
            match got {
                Some(env) => {
                    self.conn.lock().unwrap().inbox = Some(rx);
                    match matching {
                        Some(req) if env.route.req != *req => {
                            self.conn.lock().unwrap().parked.push(env);
                        }
                        _ => return Ok(env),
                    }
                }
                None => {
                    // The socket died. Mark it dead without touching the queues.
                    self.conn.lock().unwrap().tx = None;
                    drop(rx);
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
