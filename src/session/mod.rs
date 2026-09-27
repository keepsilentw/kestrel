//! Session persistence: append-only TREE JSONL (pi session-manager 形) +
//! lossless compaction (compaction entry records `first_kept`; history is
//! never deleted, summaries are regenerable).

pub mod compact;
pub mod entries;
pub mod store;

pub use entries::{Entry, EntryId};
pub use store::SessionStore;

/// Marker prefixed to the summary message that replaces compacted history
/// (pi SUMMARY_PREFIX semantics).
pub const SUMMARY_PREFIX: &str = "[context summary]\n";
