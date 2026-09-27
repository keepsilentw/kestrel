//! Identity enforcement: a connection must announce itself with Hello{peer_id}
//! first; every subsequent frame's `sender` must equal the announced id.
//! v1 threat model = 同机同用户防手滑：机械一致性校验，不造假鉴权 (plan §2.8).

use super::frame::{Envelope, PeerId};

#[derive(Debug, PartialEq)]
pub enum IdentityCheck {
    Ok,
    /// Connection hasn't sent Hello yet.
    NeedsHello,
    /// sender field doesn't match the registered peer → drop + log.
    Mismatch {
        announced: PeerId,
        claimed: PeerId,
    },
}

/// State machine per connection: None → Some(peer_id) after Hello.
#[derive(Default)]
pub struct IdentityGuard {
    announced: Option<PeerId>,
}

impl IdentityGuard {
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate one frame against the connection state.
    pub fn check(&mut self, env: &Envelope) -> IdentityCheck {
        match &self.announced {
            None => {
                if env.kind == super::frame::FrameKind::Hello {
                    let peer = env
                        .payload
                        .get("peer_id")
                        .and_then(|p| p.as_str())
                        .unwrap_or_default()
                        .to_string();
                    if peer.is_empty() {
                        return IdentityCheck::NeedsHello;
                    }
                    self.announced = Some(peer);
                    IdentityCheck::Ok
                } else {
                    IdentityCheck::NeedsHello
                }
            }
            Some(announced) => {
                if env.sender == *announced {
                    IdentityCheck::Ok
                } else {
                    IdentityCheck::Mismatch {
                        announced: announced.clone(),
                        claimed: env.sender.clone(),
                    }
                }
            }
        }
    }

    pub fn peer(&self) -> Option<&PeerId> {
        self.announced.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::super::frame::{FrameKind, Route};
    use super::*;
    use serde_json::json;

    fn env(sender: &str, kind: FrameKind, payload: serde_json::Value) -> Envelope {
        Envelope {
            v: 1,
            sender: sender.into(),
            route: Route {
                session: "s".into(),
                req: "r".into(),
            },
            kind,
            payload,
        }
    }

    #[test]
    fn hello_then_consistent_sender() {
        let mut g = IdentityGuard::new();
        assert_eq!(
            g.check(&env("sa", FrameKind::Notify, json!({}))),
            IdentityCheck::NeedsHello
        );
        assert_eq!(
            g.check(&env("sa", FrameKind::Hello, json!({"peer_id": "sa"}))),
            IdentityCheck::Ok
        );
        assert_eq!(
            g.check(&env("sa", FrameKind::Notify, json!({}))),
            IdentityCheck::Ok
        );
        assert_eq!(g.peer(), Some(&"sa".to_string()));
    }

    #[test]
    fn spoofed_sender_dropped() {
        let mut g = IdentityGuard::new();
        g.check(&env("sa", FrameKind::Hello, json!({"peer_id": "sa"})));
        assert!(matches!(
            g.check(&env("impersonator", FrameKind::Notify, json!({}))),
            IdentityCheck::Mismatch { .. }
        ));
    }

    #[test]
    fn hello_without_peer_id_rejected() {
        let mut g = IdentityGuard::new();
        assert_eq!(
            g.check(&env("sa", FrameKind::Hello, json!({}))),
            IdentityCheck::NeedsHello
        );
    }
}
