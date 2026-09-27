//! Inbound operations (the Op side of the Op/Event actor).

use super::events::MessageItem;
use super::id::ReqId;

#[derive(Debug)]
pub struct Submission {
    pub id: ReqId,
    pub op: Op,
}

impl Submission {
    pub fn new(op: Op) -> Self {
        Submission {
            id: String::new(),
            op,
        }
    }
}

#[derive(Debug)]
pub enum Op {
    /// Start a new turn; aborts any active turn first (codex single-slot rule).
    Turn {
        input: Vec<MessageItem>,
    },
    /// Inject a message into the steering queue; drained at the next round
    /// boundary without interrupting in-flight work (pi Agent.steer()).
    Steer {
        item: MessageItem,
    },
    /// Abort the active turn: cancel token first, hard abort after grace.
    Interrupt,
    /// Approval reply waking a tool blocked in `NeedsApproval` (reverse request).
    Approval {
        req: ReqId,
        decision: ApprovalDecision,
    },
    /// Inter-turn model/provider swap, applied at the next `prepare_next_turn`.
    /// M0 carries the resolved spec; M3 resolves ids through the registry.
    SwitchModel {
        spec: Box<super::actor::SwitchSpec>,
    },
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalDecision {
    AllowOnce,
    Deny,
}
