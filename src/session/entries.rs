//! One line in the session JSONL = one `Entry` (serde tagged). Every entry
//! except the Header carries `id` + `parent`, forming the tree.

use serde::{Deserialize, Serialize};

use crate::core::events::MessageItem;
use crate::provider::wire::Usage;

pub type EntryId = String;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Entry {
    /// First line of every session file.
    Header {
        session: String,
        cwd: String,
        created_ms: i64,
        role: String,
        provider: String,
        model: String,
    },
    /// One conversation item on the tree.
    Message {
        id: EntryId,
        parent: Option<EntryId>,
        item: MessageItem,
        #[serde(default)]
        usage: Option<Usage>,
        ms: i64,
    },
    /// Inter-turn model/provider switch.
    ModelChange {
        id: EntryId,
        parent: Option<EntryId>,
        provider: String,
        model: String,
        ms: i64,
    },
    /// Lossless compaction checkpoint: everything before `first_kept` (on the
    /// head path) is replaced by `summary` in prompts, but stays on disk.
    Compaction {
        id: EntryId,
        parent: Option<EntryId>,
        first_kept: EntryId,
        tokens_before: u64,
        summary: String,
        /// Previous summary when this compaction merges one (update prompt).
        #[serde(default)]
        seed: Option<String>,
        ms: i64,
    },
    /// Named pointer (branch label); kept for future UI navigation.
    Label {
        id: EntryId,
        parent: Option<EntryId>,
        target: EntryId,
        label: String,
        ms: i64,
    },
}

impl Entry {
    pub fn id(&self) -> Option<&EntryId> {
        match self {
            Entry::Header { .. } => None,
            Entry::Message { id, .. }
            | Entry::ModelChange { id, .. }
            | Entry::Compaction { id, .. }
            | Entry::Label { id, .. } => Some(id),
        }
    }

    pub fn parent(&self) -> Option<&EntryId> {
        match self {
            Entry::Header { .. } => None,
            Entry::Message { parent, .. }
            | Entry::ModelChange { parent, .. }
            | Entry::Compaction { parent, .. }
            | Entry::Label { parent, .. } => parent.as_ref(),
        }
    }
}
