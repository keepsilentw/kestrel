//! BusHub: UDS listener + per-connection tasks.哑 RPC 路由 (plan §2.8):
//! Hello 注册 peer/session；Notify 按 session 转发到目标连接的铃队列；
//! Request/Response 按 req 关联；Cancel 终止在途请求；未知 session 回
//! Error 帧且不 panic；无鉴权、无重连重放（write-ahead 兜底）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;

use super::frame::{
    Envelope, FrameKind, MAX_FRAME_BYTES, PeerId, ReqId, Route, SessionId, decode_envelope,
    encode_frame,
};
use super::identity::{IdentityCheck, IdentityGuard};

pub struct BusHub {
    peers: Mutex<HashMap<PeerId, mpsc::UnboundedSender<Envelope>>>,
    /// session id → peer that serves it (worker registers at Hello).
    sessions: Mutex<HashMap<SessionId, PeerId>>,
    /// in-flight requests awaiting a Response frame.
    requests: Mutex<HashMap<ReqId, mpsc::UnboundedSender<Envelope>>>,
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
                    if let Some(session) = env.payload.get("session").and_then(|s| s.as_str()) {
                        hub.sessions
                            .lock()
                            .unwrap()
                            .insert(session.to_string(), peer.clone());
                    }
                    continue;
                }
                hub.route_frame(env, &tx).await;
            }
            drop(writer);
            if let Some(peer) = registered_peer {
                hub.peers.lock().unwrap().remove(&peer);
                hub.sessions.lock().unwrap().retain(|_, p| p != &peer);
            }
        });
    }

    /// Route one validated frame (Notify/Request/Response/Cancel).
    async fn route_frame(&self, env: Envelope, reply: &mpsc::UnboundedSender<Envelope>) {
        match env.kind {
            FrameKind::Notify | FrameKind::Request => {
                let session = env.route.session.clone();
                // session 路由优先；peer 直达兜底（SA 无 session，peer id 即地址）
                let target: Option<PeerId> =
                    if let Some(p) = self.sessions.lock().unwrap().get(&session) {
                        Some(p.clone())
                    } else if self.peers.lock().unwrap().contains_key(&session) {
                        Some(session.clone())
                    } else {
                        None
                    };
                match target {
                    Some(peer) => {
                        if env.kind == FrameKind::Request {
                            self.requests
                                .lock()
                                .unwrap()
                                .insert(env.route.req.clone(), reply.clone());
                        }
                        if let Some(tx) = self.peers.lock().unwrap().get(&peer).cloned() {
                            let _ = tx.send(env);
                        }
                    }
                    None => {
                        let _ = reply.send(error_frame(&env.route, "unknown session"));
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
pub struct BusClient {
    pub peer_id: PeerId,
    tx: mpsc::UnboundedSender<Envelope>,
    inbox: Mutex<Inbox>,
    next_req: AtomicU64,
}

#[derive(Default)]
struct Inbox {
    rx: Option<mpsc::UnboundedReceiver<Envelope>>,
    parked: Vec<Envelope>,
}

impl BusClient {
    /// Connect + Hello. `session` registers this connection as the handler
    /// for Notify frames addressed to that session (worker mode).
    pub async fn connect(
        path: &Path,
        peer_id: &str,
        session: Option<&str>,
    ) -> anyhow::Result<Self> {
        let stream = tokio::net::UnixStream::connect(path).await?;
        let (rd, mut wr) = stream.into_split();
        let (tx, mut out_rx) = mpsc::unbounded_channel::<Envelope>();
        let (notify_tx, notify_rx) = mpsc::unbounded_channel::<Envelope>();

        let mut payload = serde_json::json!({ "peer_id": peer_id });
        if let Some(s) = session {
            payload["session"] = serde_json::json!(s);
        }
        let hello = Envelope {
            v: 1,
            sender: peer_id.to_string(),
            route: Route {
                session: session.map_or_else(String::new, |s| s.to_string()),
                req: String::new(),
            },
            kind: FrameKind::Hello,
            payload,
        };
        wr.write_all(&encode_frame(&hello)?).await?;

        // reader: anything from the hub lands in the shared inbox channel
        tokio::spawn(async move {
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
                    let _ = notify_tx.send(env);
                }
            }
        });

        // pump outbound frames from tx to socket
        tokio::spawn(async move {
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

        Ok(Self {
            peer_id: peer_id.to_string(),
            tx,
            inbox: Mutex::new(Inbox {
                rx: Some(notify_rx),
                parked: Vec::new(),
            }),
            next_req: AtomicU64::new(1),
        })
    }

    fn next_req_id(&self, prefix: &str) -> ReqId {
        let n = self.next_req.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{n}")
    }

    /// Fire-and-forget doorbell (薄消息; write-ahead 在调用侧保证).
    pub fn notify(&self, session: &str, payload: serde_json::Value) {
        let req = self.next_req_id("n");
        let _ = self.tx.send(Envelope {
            v: 1,
            sender: self.peer_id.clone(),
            route: Route {
                session: session.to_string(),
                req,
            },
            kind: FrameKind::Notify,
            payload,
        });
    }

    /// Take the next inbound frame, matching `req` when given; non-matching
    /// frames park for other waiters. Never holds the lock across await:
    /// the receiver is taken out for the duration of the poll.
    async fn take_inbox(&self, matching: Option<&ReqId>) -> anyhow::Result<Envelope> {
        loop {
            {
                let mut inbox = self.inbox.lock().unwrap();
                let parked_hit = matching.and_then(|req| {
                    inbox
                        .parked
                        .iter()
                        .position(|e| &e.route.req == req)
                        .map(|i| (req, i))
                });
                if let Some((req, i)) = parked_hit {
                    debug_assert_eq!(&inbox.parked[i].route.req, req);
                    return Ok(inbox.parked.remove(i));
                }
            }
            let mut rx = self
                .inbox
                .lock()
                .unwrap()
                .rx
                .take()
                .ok_or_else(|| anyhow::anyhow!("inbox already polled"))?;
            let env = rx
                .recv()
                .await
                .ok_or_else(|| anyhow::anyhow!("bus closed"))?;
            self.inbox.lock().unwrap().rx = Some(rx);
            match matching {
                Some(req) if env.route.req != *req => {
                    self.inbox.lock().unwrap().parked.push(env);
                }
                _ => return Ok(env),
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
        let _ = self.tx.send(Envelope {
            v: 1,
            sender: self.peer_id.clone(),
            route: Route {
                session: session.to_string(),
                req: req.clone(),
            },
            kind: FrameKind::Request,
            payload,
        });
        let env = self.take_inbox(Some(&req)).await?;
        if env.kind == FrameKind::Error {
            anyhow::bail!("{}", env.payload["error"].as_str().unwrap_or("bus error"));
        }
        Ok(env.payload)
    }

    /// Receive the next notification (doorbell) addressed to our session.
    pub async fn recv_notify(&self) -> anyhow::Result<Envelope> {
        self.take_inbox(None).await
    }
}
