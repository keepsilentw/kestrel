//! SessionStore: JSONL append-only writer/reader over the entry tree.
//! - one entry per line, flush per append, never truncate;
//! - `load` tolerates a torn last line (half-written);
//! - `head` = newest appended entry; `head_path` walks parents to the root;
//! - `prompt_items` = summary (latest compaction on head path) + kept region.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::SUMMARY_PREFIX;
use super::entries::{Entry, EntryId};
use crate::core::events::MessageItem;
use crate::core::id::{Clock, IdGen};
use crate::provider::wire::Usage;

pub struct SessionStore {
    file: Mutex<File>,
    path: PathBuf,
    head: Mutex<Option<EntryId>>,
    idgen: Arc<dyn IdGen>,
    clock: Arc<dyn Clock>,
}

fn encode_cwd(cwd: &Path) -> String {
    cwd.to_string_lossy().replace(['/', ':'], "_")
}

fn sessions_dir(home: &Path, cwd: &Path) -> PathBuf {
    home.join("sessions").join(encode_cwd(cwd))
}

impl SessionStore {
    /// Create a new session file (writes the Header line).
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        home: &Path,
        cwd: &Path,
        role: &str,
        provider: &str,
        model: &str,
        idgen: Arc<dyn IdGen>,
        clock: Arc<dyn Clock>,
    ) -> anyhow::Result<Self> {
        let dir = sessions_dir(home, cwd);
        std::fs::create_dir_all(&dir)?;
        let session_id = idgen.next_id();
        let path = dir.join(format!("{}_{}.jsonl", clock.now_ms(), session_id));
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        let header = Entry::Header {
            session: session_id,
            cwd: cwd.display().to_string(),
            created_ms: clock.now_ms(),
            role: role.to_string(),
            provider: provider.to_string(),
            model: model.to_string(),
        };
        writeln!(file, "{}", serde_json::to_string(&header)?)?;
        file.flush()?;
        Ok(Self {
            file: Mutex::new(file),
            path,
            head: Mutex::new(None),
            idgen,
            clock,
        })
    }

    /// Open an existing session file (resume).
    pub fn open(path: &Path, idgen: Arc<dyn IdGen>, clock: Arc<dyn Clock>) -> anyhow::Result<Self> {
        let file = OpenOptions::new().append(true).open(path)?;
        let head = { Self::load_path(path).last().and_then(|e| e.id().cloned()) };
        Ok(Self {
            file: Mutex::new(file),
            path: path.to_path_buf(),
            head: Mutex::new(head),
            idgen,
            clock,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one conversation item as a Message entry (parent = head).
    pub fn append_item(&self, item: MessageItem, usage: Option<Usage>) -> EntryId {
        let id = self.idgen.next_id();
        let parent = self.head.lock().unwrap().clone();
        let ms = self.clock.now_ms();
        let entry = Entry::Message {
            id: id.clone(),
            parent,
            item,
            usage,
            ms,
        };
        self.write(&entry);
        id
    }

    /// Append a raw entry with an explicit parent (fork/branch primitive).
    pub fn append_entry(&self, entry: Entry) -> EntryId {
        let id = entry.id().cloned().unwrap_or_else(|| self.idgen.next_id());
        self.write(&entry);
        id
    }

    /// Fork: append a Message under `parent` and move the head there.
    pub fn fork_from(&self, parent: &EntryId, item: MessageItem) -> EntryId {
        let id = self.idgen.next_id();
        let ms = self.clock.now_ms();
        let entry = Entry::Message {
            id: id.clone(),
            parent: Some(parent.clone()),
            item,
            usage: None,
            ms,
        };
        self.write(&entry);
        id
    }

    pub fn append_model_change(&self, provider: &str, model: &str) -> EntryId {
        let id = self.idgen.next_id();
        let parent = self.head.lock().unwrap().clone();
        let ms = self.clock.now_ms();
        let entry = Entry::ModelChange {
            id: id.clone(),
            parent,
            provider: provider.into(),
            model: model.into(),
            ms,
        };
        self.write(&entry);
        id
    }

    pub fn append_compaction(
        &self,
        first_kept: EntryId,
        tokens_before: u64,
        summary: String,
        seed: Option<String>,
    ) -> EntryId {
        let id = self.idgen.next_id();
        let parent = self.head.lock().unwrap().clone();
        let ms = self.clock.now_ms();
        let entry = Entry::Compaction {
            id: id.clone(),
            parent,
            first_kept,
            tokens_before,
            summary,
            seed,
            ms,
        };
        self.write(&entry);
        id
    }

    fn write(&self, entry: &Entry) {
        let id = entry.id().cloned();
        let mut file = self.file.lock().unwrap();
        writeln!(
            file,
            "{}",
            serde_json::to_string(entry).expect("entry serializes")
        )
        .expect("session write");
        file.flush().expect("session flush");
        if let Some(id) = id {
            *self.head.lock().unwrap() = Some(id);
        }
    }

    /// Load all entries; a torn last line is skipped (half-write tolerance).
    pub fn load(&self) -> Vec<Entry> {
        Self::load_path(&self.path)
    }

    fn load_path(path: &Path) -> Vec<Entry> {
        let Ok(f) = File::open(path) else {
            return vec![];
        };
        let mut out = Vec::new();
        for line in BufReader::new(f).lines().map_while(Result::ok) {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Entry>(&line) {
                Ok(e) => out.push(e),
                Err(_) => break, // torn tail: stop replay here
            }
        }
        out
    }

    /// Head -> root parent chain (chronological order).
    pub fn head_path_entries(&self) -> Vec<Entry> {
        let entries = self.load();
        let Some(mut cur) = self.head.lock().unwrap().clone() else {
            return vec![];
        };
        let by_id: std::collections::HashMap<&str, &Entry> = entries
            .iter()
            .filter_map(|e| e.id().map(|i| (i.as_str(), e)))
            .collect();
        let mut path = Vec::new();
        while let Some(e) = by_id.get(cur.as_str()) {
            path.push((*e).clone());
            match e.parent() {
                Some(p) => cur = p.clone(),
                None => break,
            }
        }
        path.reverse();
        path
    }

    /// LLM-bound projection: latest compaction on the head path splits the
    /// region — [first_kept..compaction) messages + everything after the
    /// compaction, with the summary sitting at the bottom.
    pub fn prompt_items(&self) -> Vec<MessageItem> {
        self.prompt_snapshot()
            .into_iter()
            .map(|(_, item)| item)
            .collect()
    }

    /// Same projection as `prompt_items` but carrying each item's session
    /// entry id (None for the synthesized summary line), so a resumed engine
    /// can keep `history` and `history_ids` aligned with the tree on disk.
    pub fn prompt_snapshot(&self) -> Vec<(Option<EntryId>, MessageItem)> {
        let path_entries = self.head_path_entries();
        let msg = |e: &Entry| match e {
            Entry::Message { id, item, .. } => Some((Some(id.clone()), item.clone())),
            _ => None,
        };
        let Some(cut) = path_entries
            .iter()
            .rposition(|e| matches!(e, Entry::Compaction { .. }))
        else {
            return path_entries.iter().filter_map(msg).collect();
        };
        let (summary, first_kept) = match &path_entries[cut] {
            Entry::Compaction {
                summary,
                first_kept,
                ..
            } => (summary.clone(), first_kept.clone()),
            _ => unreachable!(),
        };
        // Kept region: from the entry first_kept POINTS TO, up to (not incl.)
        // the compaction entry, plus anything appended after the compaction.
        let start = path_entries
            .iter()
            .position(|e| e.id() == Some(&first_kept))
            .unwrap_or(cut);
        let kept: Vec<(Option<EntryId>, MessageItem)> = path_entries[start..cut]
            .iter()
            .chain(&path_entries[cut + 1..])
            .filter_map(msg)
            .collect();
        let mut out = vec![(
            None,
            MessageItem::UserText {
                text: format!("{SUMMARY_PREFIX}{summary}"),
            },
        )];
        out.extend(kept);
        out
    }

    /// Session files recorded for `cwd`, oldest first (the `<created_ms>_<id>`
    /// name makes lexical order chronological).
    pub fn list(home: &Path, cwd: &Path) -> Vec<PathBuf> {
        let dir = sessions_dir(home, cwd);
        let Ok(rd) = std::fs::read_dir(dir) else {
            return vec![];
        };
        let mut files: Vec<PathBuf> = rd
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("jsonl"))
            .collect();
        files.sort();
        files
    }

    /// Resolve a `--resume` selector into a session file: an explicit path,
    /// `last` (newest under this cwd), or a session id / unique id prefix.
    pub fn resolve(home: &Path, cwd: &Path, selector: &str) -> anyhow::Result<PathBuf> {
        let p = Path::new(selector);
        if p.extension().and_then(|x| x.to_str()) == Some("jsonl") {
            let abs = if p.is_absolute() {
                p.to_path_buf()
            } else {
                std::env::current_dir()?.join(p)
            };
            if abs.exists() {
                return Ok(abs);
            }
            anyhow::bail!("no session file at {}", abs.display());
        }
        let files = Self::list(home, cwd);
        if selector == "last" || selector.is_empty() {
            return files.last().cloned().ok_or_else(|| {
                anyhow::anyhow!("no sessions recorded under cwd {}", cwd.display())
            });
        }
        let hits: Vec<PathBuf> = files
            .into_iter()
            .filter(|f| {
                f.file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.rsplit_once('_'))
                    .is_some_and(|(_, id)| id.starts_with(selector))
            })
            .collect();
        match hits.len() {
            1 => Ok(hits[0].clone()),
            0 => anyhow::bail!(
                "no session matching {selector:?} under cwd {} (try `--resume last`)",
                cwd.display()
            ),
            n => anyhow::bail!(
                "{n} sessions match {selector:?} under cwd {}; pass a longer id prefix",
                cwd.display()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::id::SeqGen;
    use std::sync::atomic::AtomicI64;

    struct FakeClock(AtomicI64);
    impl Clock for FakeClock {
        fn now_ms(&self) -> i64 {
            self.0.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    fn store_fixture(dir: &Path) -> SessionStore {
        SessionStore::create(
            dir,
            Path::new("/ws"),
            "rd",
            "prov",
            "model-x",
            Arc::new(SeqGen::new()),
            Arc::new(FakeClock(AtomicI64::new(1_000))),
        )
        .unwrap()
    }

    #[test]
    fn golden_lines_shape() {
        let dir = std::env::temp_dir().join(format!("kestrel-golden-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let st = store_fixture(&dir);
        st.append_item(MessageItem::user_text("hello"), None);
        st.append_item(
            MessageItem::AssistantText { text: "hi".into() },
            Some(Usage {
                input_tokens: 5,
                output_tokens: 2,
                ..Default::default()
            }),
        );
        let content = std::fs::read_to_string(st.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("\"type\":\"header\""));
        // create() consumed id-1 for the session id; first item is id-2
        assert!(lines[1].contains("\"id\":\"id-2\""));
        assert!(lines[1].contains("\"parent\":null"));
        assert!(lines[2].contains("\"parent\":\"id-2\""));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn torn_tail_skipped() {
        let dir = std::env::temp_dir().join(format!("kestrel-torn-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let st = store_fixture(&dir);
        st.append_item(MessageItem::user_text("a"), None);
        let path = st.path().to_path_buf();
        drop(st);
        // simulate half-written tail
        use std::io::Write as _;
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(f, "{{\"type\":\"message\",\"id\":\"id-9\"").unwrap();
        drop(f);
        let st2 = SessionStore::open(
            &path,
            Arc::new(SeqGen::new()),
            Arc::new(FakeClock(AtomicI64::new(2_000))),
        )
        .unwrap();
        assert_eq!(st2.load().len(), 2); // header + first message only
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn fork_switches_head_and_keeps_both_branches() {
        let dir = std::env::temp_dir().join(format!("kestrel-fork-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let st = store_fixture(&dir);
        let e1 = st.append_item(MessageItem::user_text("trunk"), None);
        let e2 = st.append_item(
            MessageItem::AssistantText {
                text: "on trunk".into(),
            },
            None,
        );
        // fork from the first message
        let e3 = st.fork_from(&e1, MessageItem::user_text("branch"));
        let path_entries = st.head_path_entries();
        assert_eq!(
            path_entries.len(),
            2,
            "e1 + branch (header has no id, not on path)"
        );
        assert!(
            matches!(path_entries.last(), Some(Entry::Message { item, .. }) if item.to_context() == crate::provider::wire::ContextItem::UserText { text: "branch".into() })
        );
        // lossless: both branch tips still on disk
        let all = st.load();
        assert_eq!(all.len(), 4, "header + trunk(2) + fork(1)");
        assert!(all.iter().any(|e| e.id() == Some(&e2)));
        assert!(all.iter().any(|e| e.id() == Some(&e3)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn compaction_splits_prompt_region() {
        let dir = std::env::temp_dir().join(format!("kestrel-comp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let st = store_fixture(&dir);
        st.append_item(MessageItem::user_text("old-1"), None);
        st.append_item(
            MessageItem::AssistantText {
                text: "old-2".into(),
            },
            None,
        );
        let first_kept = st.append_item(MessageItem::user_text("kept-1"), None);
        st.append_item(MessageItem::user_text("kept-2"), None);
        st.append_compaction(first_kept.clone(), 9_999, "summary of old".into(), None);
        let items = st.prompt_items();
        assert_eq!(items.len(), 3, "summary + kept-1 + kept-2");
        match &items[0] {
            MessageItem::UserText { text } => {
                assert!(text.starts_with(SUMMARY_PREFIX) && text.contains("summary of old"))
            }
            _ => panic!("expected summary first"),
        }
        assert!(items[1..].iter().any(|i| i.to_context()
            == crate::provider::wire::ContextItem::UserText {
                text: "kept-2".into()
            }));
        // lossless: file unchanged by prompt projection
        assert_eq!(st.load().len(), 6);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resume_replays_head_path() {
        let dir = std::env::temp_dir().join(format!("kestrel-resume-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let st = store_fixture(&dir);
        st.append_item(MessageItem::user_text("q"), None);
        st.append_item(MessageItem::AssistantText { text: "a".into() }, None);
        let path = st.path().to_path_buf();
        drop(st);
        let st2 = SessionStore::open(
            &path,
            Arc::new(SeqGen::new()),
            Arc::new(FakeClock(AtomicI64::new(3_000))),
        )
        .unwrap();
        let items = st2.prompt_items();
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0].to_context(),
            crate::provider::wire::ContextItem::UserText { text: "q".into() }
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn list_and_resume_selectors() {
        let home = std::env::temp_dir().join(format!("kestrel-list-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        let cwd = Path::new("/ws-list");
        let first = SessionStore::create(
            &home,
            cwd,
            "rd",
            "prov",
            "m",
            Arc::new(SeqGen::new()),
            Arc::new(FakeClock(AtomicI64::new(1_000))),
        )
        .unwrap();
        let second = SessionStore::create(
            &home,
            cwd,
            "rd",
            "prov",
            "m",
            Arc::new(SeqGen::new()),
            Arc::new(FakeClock(AtomicI64::new(2_000))),
        )
        .unwrap();

        let files = SessionStore::list(&home, cwd);
        assert_eq!(files.len(), 2, "both sessions listed for the cwd");
        assert_eq!(files[0], first.path(), "oldest first");
        assert_eq!(
            SessionStore::resolve(&home, cwd, "last").unwrap(),
            second.path()
        );
        // An explicit path selector works even outside the cwd bucket.
        assert_eq!(
            SessionStore::resolve(&home, cwd, first.path().to_str().unwrap()).unwrap(),
            first.path()
        );
        // Both files carry the same id part (each create restarts SeqGen), so a
        // shared prefix is ambiguous: say so instead of guessing a winner.
        let err = SessionStore::resolve(&home, cwd, "id")
            .unwrap_err()
            .to_string();
        assert!(err.contains("2 sessions match"), "{err}");
        assert!(
            SessionStore::resolve(&home, Path::new("/nowhere-cwd"), "last")
                .unwrap_err()
                .to_string()
                .contains("no sessions recorded")
        );
        std::fs::remove_dir_all(&home).ok();
    }
}
