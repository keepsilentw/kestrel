//! Bus frame protocol: 4-byte BE length prefix + one JSON envelope (plan §2.8).
//! Envelope is STRICT (unknown fields rejected); payload stays opaque
//! (serde_json::Value, unknown fields pass) — the bus never understands
//! business semantics (pi protocol 869 行的全部秘密).

use serde::{Deserialize, Serialize};

pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

pub type PeerId = String;
pub type SessionId = String;
pub type ReqId = String;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    /// Protocol version; must be 1.
    pub v: u8,
    /// Sender identity; must match the peer registered at Hello (纪律: 反
    /// agent-to-agent 注入的第一道机械校验).
    pub sender: PeerId,
    pub route: Route,
    pub kind: FrameKind,
    /// Opaque business payload.
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub session: SessionId,
    pub req: ReqId,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameKind {
    /// First frame on a connection: payload = {"peer_id": "..."}.
    Hello,
    /// Bell / fire-and-forget doorbell (harness-v3 §5 薄消息).
    Notify,
    /// RPC request; Response correlates via route.req.
    Request,
    /// RPC reply (payload = result or {"error": "..."}).
    Response,
    /// Abort the in-flight request identified by route.req.
    Cancel,
    /// Error reply from the bus itself (unknown session, bad frame...).
    Error,
}

/// Encode one envelope as a full frame (4-byte BE length + JSON).
pub fn encode_frame(env: &Envelope) -> anyhow::Result<Vec<u8>> {
    let json = serde_json::to_vec(env)?;
    if 4 + json.len() > MAX_FRAME_BYTES {
        anyhow::bail!("frame exceeds {MAX_FRAME_BYTES} bytes");
    }
    let mut out = Vec::with_capacity(4 + json.len());
    out.extend_from_slice(&(json.len() as u32).to_be_bytes());
    out.extend_from_slice(&json);
    Ok(out)
}

/// Decode one frame body (JSON part) — strictness enforced by serde.
pub fn decode_envelope(json: &[u8]) -> anyhow::Result<Envelope> {
    Ok(serde_json::from_slice(json)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn env() -> Envelope {
        Envelope {
            v: 1,
            sender: "harness-sa".into(),
            route: Route {
                session: "rd-1".into(),
                req: "req-9".into(),
            },
            kind: FrameKind::Notify,
            payload: json!({ "role": "rd", "stage": "dev", "iteration": 1 }),
        }
    }

    #[test]
    fn roundtrip_preserves_everything() {
        let bytes = encode_frame(&env()).unwrap();
        assert_eq!(&bytes[..4], &(bytes.len() as u32 - 4).to_be_bytes());
        let back = decode_envelope(&bytes[4..]).unwrap();
        assert_eq!(back, env());
    }

    #[test]
    fn unknown_envelope_fields_rejected() {
        let bytes = encode_frame(&env()).unwrap();
        // splice `"extra":1` into the JSON — strict envelope must reject
        let json = &bytes[4..];
        let mut v: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(json).unwrap();
        v.insert("extra".into(), json!(1));
        let tampered = serde_json::to_vec(&v).unwrap();
        assert!(
            decode_envelope(&tampered).is_err(),
            "strict envelope must reject unknown fields"
        );
    }

    #[test]
    fn payload_unknown_fields_pass_through() {
        let mut e = env();
        e.payload = json!({ "anything": true, "even": ["nested", 1] });
        let back = decode_envelope(&encode_frame(&e).unwrap()[4..]).unwrap();
        assert_eq!(back.payload, e.payload, "payload stays opaque");
    }

    #[test]
    fn oversize_frame_rejected() {
        let mut e = env();
        e.payload = serde_json::Value::String("x".repeat(MAX_FRAME_BYTES));
        assert!(encode_frame(&e).is_err());
    }
}
