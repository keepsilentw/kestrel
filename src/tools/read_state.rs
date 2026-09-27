//! Read-before-edit bookkeeping.
//!
//! `ReadTracker` answers one question: does the model currently hold a valid
//! view of this file? It is updated by the tools themselves on success and
//! rebuilt from the in-memory history at the compaction boundary, so a read
//! whose content is no longer in context stops counting and the model must
//! re-read instead of editing against a view it can no longer see.
//!
//! A record is a content stamp (byte length + hash), not a "was touched" bit:
//! an edit is refused when the file on disk no longer matches the version the
//! model last saw. Both refusals are loud — a model-facing rejection it can act
//! on. The failure this module exists to prevent is the silent one: rewriting
//! the wrong version of a file and reporting success.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Component, Path, PathBuf};
use std::sync::RwLock;

use crate::core::events::MessageItem;
use crate::provider::wire::ToolContent;

/// Fingerprint of the version of a file the model last saw.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamp {
    pub len: usize,
    pub hash: u64,
}

pub fn stamp(content: &str) -> Stamp {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    content.hash(&mut h);
    Stamp {
        len: content.len(),
        hash: h.finish(),
    }
}

/// Lexical absolute-path key: relative paths resolve against `cwd`, `.`/`..`
/// fold without touching the filesystem. Symlink resolution stays the sandbox
/// layer's business; this key only has to be stable within one session.
pub fn path_key(cwd: &Path, path: &Path) -> PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let mut out = PathBuf::new();
    for c in joined.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

#[derive(Debug, Default)]
pub struct ReadTracker {
    seen: RwLock<HashMap<PathBuf, Stamp>>,
}

impl ReadTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a version the model has just seen (after read/write/edit).
    pub fn record(&self, key: PathBuf, content: &str) {
        self.seen.write().unwrap().insert(key, stamp(content));
    }

    pub fn get(&self, key: &Path) -> Option<Stamp> {
        self.seen.read().unwrap().get(key).copied()
    }

    pub fn len(&self) -> usize {
        self.seen.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Precondition for `edit`: the model must have seen exactly this version.
    /// `Err` carries the model-facing rejection text.
    pub fn check(&self, key: &Path, current: &str) -> Result<(), String> {
        let Some(seen) = self.get(key) else {
            return Err(format!(
                "{} has not been read in this session: read it first, then edit \
                 against the content you actually saw",
                key.display()
            ));
        };
        let now = stamp(current);
        if now != seen {
            return Err(format!(
                "{} changed on disk since you read it ({} -> {} bytes): re-read \
                 it, then edit against the current content",
                key.display(),
                seen.len,
                now.len
            ));
        }
        Ok(())
    }

    /// Rebuild from the model-visible history when that history is rewritten
    /// (compaction). Only entries whose full content is still in context can be
    /// recovered: `read` results and `write` arguments. `edit` results carry an
    /// echo, not the content, so those files fall back to "read again" — loud,
    /// never silent.
    pub fn rebuild_from_items(&self, cwd: &Path, items: &[MessageItem]) {
        let mut pending: HashMap<&str, (&str, &serde_json::Value)> = HashMap::new();
        let mut next: HashMap<PathBuf, Stamp> = HashMap::new();
        for item in items {
            match item {
                MessageItem::AssistantToolUse {
                    call_id,
                    name,
                    args,
                } => {
                    pending.insert(call_id.as_str(), (name.as_str(), args));
                }
                MessageItem::ToolResult {
                    call_id,
                    content,
                    is_error,
                } => {
                    if *is_error {
                        continue;
                    }
                    let Some(&(name, args)) = pending.get(call_id.as_str()) else {
                        continue;
                    };
                    let Some(path) = args["path"].as_str() else {
                        continue;
                    };
                    let body = match name {
                        "read" => content.iter().find_map(|c| match c {
                            ToolContent::Text { text } => Some(text.clone()),
                            ToolContent::Image { .. } => None,
                        }),
                        "write" => args["content"].as_str().map(str::to_string),
                        _ => None,
                    };
                    if let Some(body) = body {
                        next.insert(path_key(cwd, Path::new(path)), stamp(&body));
                    }
                }
                _ => {}
            }
        }
        *self.seen.write().unwrap() = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tool_use(call_id: &str, name: &str, path: &str) -> MessageItem {
        MessageItem::AssistantToolUse {
            call_id: call_id.into(),
            name: name.into(),
            args: serde_json::json!({ "path": path }),
        }
    }

    fn result(call_id: &str, text: &str) -> MessageItem {
        MessageItem::ToolResult {
            call_id: call_id.into(),
            content: vec![ToolContent::Text { text: text.into() }],
            is_error: false,
        }
    }

    #[test]
    fn relative_and_dotted_paths_share_one_key() {
        let cwd = Path::new("/w");
        let a = path_key(cwd, Path::new("src/a.rs"));
        assert_eq!(a, PathBuf::from("/w/src/a.rs"));
        assert_eq!(a, path_key(cwd, Path::new("./src/../src/a.rs")));
        assert_eq!(a, path_key(cwd, Path::new("/w/src/a.rs")));
    }

    #[test]
    fn unread_file_is_refused() {
        let t = ReadTracker::new();
        let err = t
            .check(Path::new("/w/a.rs"), "content")
            .expect_err("unread");
        assert!(err.contains("has not been read"), "{err}");
    }

    #[test]
    fn changed_file_is_refused_and_reread_clears_it() {
        let t = ReadTracker::new();
        let key = PathBuf::from("/w/a.rs");
        t.record(key.clone(), "v1");
        assert!(t.check(&key, "v1").is_ok());
        let err = t.check(&key, "v2").expect_err("stale");
        assert!(err.contains("changed on disk"), "{err}");
        t.record(key.clone(), "v2");
        assert!(t.check(&key, "v2").is_ok());
    }

    #[test]
    fn rebuild_recovers_reads_and_writes_only() {
        let t = ReadTracker::new();
        let cwd = Path::new("/w");
        let items = vec![
            tool_use("1", "read", "a.rs"),
            result("1", "alpha"),
            tool_use("2", "edit", "b.rs"),
            result("2", "applied 1 edit(s) to b.rs"),
            MessageItem::AssistantToolUse {
                call_id: "3".into(),
                name: "write".into(),
                args: serde_json::json!({ "path": "c.rs", "content": "gamma" }),
            },
            result("3", "wrote 5 bytes to c.rs"),
        ];
        t.record(PathBuf::from("/w/stale.rs"), "gone");
        t.rebuild_from_items(cwd, &items);
        assert_eq!(t.len(), 2);
        assert_eq!(t.get(Path::new("/w/a.rs")), Some(stamp("alpha")));
        assert_eq!(t.get(Path::new("/w/c.rs")), Some(stamp("gamma")));
        assert!(t.get(Path::new("/w/b.rs")).is_none());
        assert!(t.get(Path::new("/w/stale.rs")).is_none());
    }

    #[test]
    fn rebuild_skips_errored_results() {
        let t = ReadTracker::new();
        let items = vec![
            tool_use("1", "read", "a.rs"),
            MessageItem::ToolResult {
                call_id: "1".into(),
                content: vec![ToolContent::Text { text: "no".into() }],
                is_error: true,
            },
        ];
        t.rebuild_from_items(Path::new("/w"), &items);
        assert!(t.is_empty());
    }
}
