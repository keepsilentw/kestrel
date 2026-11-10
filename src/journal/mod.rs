//! Journal: SA-side write-ahead truth source (plan §2.9).
//! Rule 1 「先写盘再拉铃」由调用序保证：append(StageWritten) → append(BellSent)
//! → bus.notify()。恢复 = replay → 对账 → 重生成 state 投影。
//! state.json 与 journal 的关系：journal 是事件真相，state.json 是投影。
//!
//! 截断语义（design §10）：活文件可以变短，但**不是删除历史**——`rotate` 把旧前缀
//! 折成一个 `snapshot` 检查点并搬进归档段 `<stem>.seg<N>.jsonl`，活文件只剩检查点
//! 加最近的尾巴。检查点必须自带「未结算的铃」，否则截断会把「结果铃丢了」这件事
//! 悄悄变成一次重派发。完整审计 = `replay_all()`（段文件 + 活文件）。

pub mod records;

use records::reconcile;
pub use records::{
    Checkpoint, Folder, JournalRecord, ReconcileReport, StateSnapshot, ThinBell, Verdict,
};

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Serialize;

pub struct Journal {
    file: Mutex<File>,
    path: std::path::PathBuf,
    /// Online projection: the same fold `reconcile` performs, advanced on every
    /// append so the SA's budget stop-loss and the 巡检 stay O(1) per dispatch
    /// instead of re-reading the whole journal (design §5 不变式 5: state is
    /// DEFINED by the replay, so both must come out of ONE fold).
    projection: Mutex<Folder>,
}

/// Result of one successful rotation.
#[derive(Debug)]
pub struct Rotation {
    /// Segment file that now holds the folded prefix.
    pub archived_to: PathBuf,
    /// How many live-file records the checkpoint summarizes.
    pub covered: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SegmentStatus {
    pub from: u64,
    pub to: u64,
    pub bytes: u64,
    pub archived: bool,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JournalStatus {
    pub live_records: usize,
    pub live_bytes: u64,
    pub root_segments: usize,
    pub archived_segments: usize,
    pub duplicate_segments: usize,
    pub total_segment_bytes: u64,
    pub staged_files: usize,
    pub segments: Vec<SegmentStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approvals: Option<ApprovalStatus>,
}

impl JournalStatus {
    /// Operational health: replay can resolve duplicate ranges deterministically,
    /// but a duplicate or staged residue means a previous archive/rotation needs
    /// inspection. It is not "healthy enough to ignore".
    pub fn is_healthy(&self) -> bool {
        self.duplicate_segments == 0 && self.staged_files == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArchiveMove {
    pub from: u64,
    pub to: u64,
    pub bytes: u64,
    pub source: PathBuf,
    pub destination: PathBuf,
    pub destination_exists: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ApprovalStatus {
    pub requests: usize,
    pub decisions: usize,
    pub receipts: usize,
    pub pending: usize,
    pub unacked: usize,
    pub applied: usize,
    pub stale: usize,
    pub orphan_decisions: usize,
    pub orphan_receipts: usize,
}

#[derive(Default)]
struct ApprovalAudit {
    requested: bool,
    decided: bool,
    applied: Option<bool>,
}

use std::sync::Mutex;

impl Journal {
    pub fn create(path: &Path) -> anyhow::Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let journal = Self {
            file: Mutex::new(file),
            path: path.to_path_buf(),
            projection: Mutex::new(Folder::default()),
        };
        // A "created" journal may already hold yesterday's records (`create`
        // opens in append mode and `KESTREL_DEMO_KEEP_JOURNAL=1` relies on it):
        // seeding the projection from disk is what keeps the budget stop-loss
        // honest across runs instead of forgetting what was already spent.
        journal.reseed()?;
        Ok(journal)
    }

    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let file = OpenOptions::new().append(true).open(path)?;
        let journal = Self {
            file: Mutex::new(file),
            path: path.to_path_buf(),
            projection: Mutex::new(Folder::default()),
        };
        journal.reseed()?;
        Ok(journal)
    }

    /// Append one record and fsync — the ONLY way records enter the journal.
    pub fn append(&self, record: &JournalRecord) -> anyhow::Result<()> {
        let mut file = self.file.lock().unwrap();
        writeln!(file, "{}", serde_json::to_string(record)?)?;
        file.flush()?;
        file.sync_all()?;
        // same lock order as `rotate` (file -> projection)
        self.projection.lock().unwrap().apply(record);
        Ok(())
    }

    /// Rebuild the online projection by replaying the live file. Called at open
    /// (history may pre-exist) and after a rotation swapped the file.
    pub fn reseed(&self) -> anyhow::Result<()> {
        let mut folder = Folder::default();
        for record in self.replay() {
            folder.apply(&record);
        }
        *self.projection.lock().unwrap() = folder;
        Ok(())
    }

    /// Current projection: stage / iteration / spend per peer + unsettled bells.
    pub fn projection(&self) -> ReconcileReport {
        self.projection.lock().unwrap().report()
    }

    /// Bytes the live file occupies (metadata only — no read, no parse).
    pub fn file_bytes(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    /// Replay all records; a torn tail (crash mid-write) is skipped.
    pub fn replay(&self) -> Vec<JournalRecord> {
        let Ok(f) = File::open(&self.path) else {
            return vec![];
        };
        let mut out = Vec::new();
        for line in BufReader::new(f).lines().map_while(Result::ok) {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<JournalRecord>(&line) {
                Ok(r) => out.push(r),
                Err(_) => break, // torn tail
            }
        }
        out
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Cheap operational inventory: no journal record is parsed and no
    /// projection is rebuilt. File sizes come from metadata.
    pub fn status(&self) -> JournalStatus {
        let mut segments = Vec::new();
        let (root_segments, archived_segments, duplicate_segments) = match (
            self.path.parent(),
            self.path.file_stem().and_then(|s| s.to_str()),
        ) {
            (Some(dir), Some(stem)) => {
                let prefix = format!("{stem}.seg");
                let root = segment_files(dir, &prefix);
                let archived = self
                    .segment_archive_dir()
                    .map(|archive| segment_files(&archive, &prefix))
                    .unwrap_or_default();
                let root_len = root.len();
                let archive_len = archived.len();
                let root_ranges: std::collections::BTreeSet<(u64, u64)> =
                    root.iter().map(|(from, to, _)| (*from, *to)).collect();
                let duplicates = archived
                    .iter()
                    .filter(|(from, to, _)| root_ranges.contains(&(*from, *to)))
                    .count();
                for (from, to, path) in &root {
                    segments.push(SegmentStatus {
                        from: *from,
                        to: *to,
                        bytes: file_bytes(path),
                        archived: false,
                        path: path.clone(),
                    });
                }
                for (from, to, path) in archived {
                    segments.push(SegmentStatus {
                        from,
                        to,
                        bytes: file_bytes(path.as_path()),
                        archived: true,
                        path,
                    });
                }
                (root_len, archive_len, duplicates)
            }
            _ => (0, 0, 0),
        };
        JournalStatus {
            live_records: self.live_records(),
            live_bytes: self.file_bytes(),
            root_segments,
            archived_segments,
            duplicate_segments,
            total_segment_bytes: segments.iter().map(|segment| segment.bytes).sum(),
            staged_files: staged_files(self.path.parent()),
            segments,
            approvals: None,
        }
    }

    /// Full approval audit, including archived segments. This reads records, so
    /// it is opt-in rather than part of the cheap default status path.
    pub fn approval_status(&self) -> ApprovalStatus {
        let mut by_req: BTreeMap<String, ApprovalAudit> = BTreeMap::new();
        for record in self.replay_all() {
            match record {
                JournalRecord::ApprovalRequested { req, .. } => {
                    by_req.entry(req).or_default().requested = true;
                }
                JournalRecord::ApprovalDecided { req, .. } => {
                    by_req.entry(req).or_default().decided = true;
                }
                JournalRecord::ApprovalAcked { req, applied, .. } => {
                    let entry = by_req.entry(req).or_default();
                    entry.applied = Some(entry.applied.unwrap_or(false) || applied);
                }
                _ => {}
            }
        }
        let mut status = ApprovalStatus::default();
        for entry in by_req.values() {
            status.requests += usize::from(entry.requested);
            status.decisions += usize::from(entry.decided);
            status.receipts += usize::from(entry.applied.is_some());
            status.pending += usize::from(entry.requested && !entry.decided);
            status.unacked += usize::from(entry.decided && entry.applied.is_none());
            status.applied += usize::from(entry.applied == Some(true));
            status.stale += usize::from(entry.applied == Some(false));
            status.orphan_decisions += usize::from(!entry.requested && entry.decided);
            status.orphan_receipts += usize::from(!entry.requested && entry.applied.is_some());
        }
        status
    }

    /// Records currently in the live file (torn tail excluded).
    pub fn len(&self) -> usize {
        self.projection.lock().unwrap().live_records()
    }

    /// Same as `len()`, named for the rotation trigger (records the live file
    /// physically holds, read without parsing it).
    pub fn live_records(&self) -> usize {
        self.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The whole truth: every archived segment, oldest first, then the live
    /// file. Folding this gives the same state as folding the never-truncated
    /// history — that is the property rotation must preserve.
    pub fn replay_all(&self) -> Vec<JournalRecord> {
        let mut out = Vec::new();
        for segment in self.segments() {
            out.extend(replay_path(&segment));
        }
        out.extend(self.replay());
        out
    }

    /// Archived segments, oldest range first: `<stem>.seg<from>-<to>.jsonl`.
    /// Both the live directory and `<stem>.segments/` are scanned. When a crash
    /// leaves the same range in both places, the live copy wins: it is the one a
    /// retried rotation most recently fsynced.
    fn segments(&self) -> Vec<PathBuf> {
        let Some(dir) = self.path.parent() else {
            return vec![];
        };
        let Some(stem) = self.path.file_stem().and_then(|s| s.to_str()) else {
            return vec![];
        };
        let prefix = format!("{stem}.seg");
        let mut found = BTreeMap::new();
        for (from, to, path) in segment_files(dir, &prefix) {
            found.insert((from, to), path);
        }
        if let Some(archive) = self.segment_archive_dir() {
            for (from, to, path) in segment_files(&archive, &prefix) {
                found.entry((from, to)).or_insert(path);
            }
        }
        found.into_values().collect()
    }

    /// Where `archive_segments` parks sealed files. The directory is derived
    /// from the journal name, so `replay_all` finds it after a restart without
    /// another configuration source.
    pub fn segment_archive_dir(&self) -> Option<PathBuf> {
        let dir = self.path.parent()?;
        let stem = self.path.file_stem()?.to_str()?;
        Some(dir.join(format!("{stem}.segments")))
    }

    /// Preview exactly what `archive_segments` would move. The append lock keeps
    /// a concurrent rotation from changing the answer halfway through planning.
    pub fn archive_plan(&self, keep_latest: usize) -> anyhow::Result<Vec<ArchiveMove>> {
        let _file = self.file.lock().unwrap();
        self.archive_plan_locked(keep_latest)
    }

    fn archive_plan_locked(&self, keep_latest: usize) -> anyhow::Result<Vec<ArchiveMove>> {
        let Some(dir) = self.path.parent() else {
            anyhow::bail!("journal path has no parent directory");
        };
        let Some(stem) = self.path.file_stem().and_then(|s| s.to_str()) else {
            anyhow::bail!("journal path has no usable file stem");
        };
        let prefix = format!("{stem}.seg");
        let mut segments = segment_files(dir, &prefix);
        let move_count = segments.len().saturating_sub(keep_latest);
        segments.truncate(move_count);
        let archive = self
            .segment_archive_dir()
            .context("journal path has no archive directory")?;
        Ok(segments
            .into_iter()
            .map(|(from, to, source)| {
                let destination = archive.join(
                    source
                        .file_name()
                        .expect("segment_files only returns named files"),
                );
                let destination_exists = destination.exists();
                ArchiveMove {
                    from,
                    to,
                    bytes: file_bytes(&source),
                    source,
                    destination,
                    destination_exists,
                }
            })
            .collect())
    }

    /// Move sealed segments out of the journal directory without deleting them.
    /// The newest `keep_latest` segments stay in place, so a caller can retain a
    /// small hot window. `replay_all` reads both locations; a destination
    /// collision is refused before any file moves.
    pub fn archive_segments(&self, keep_latest: usize) -> anyhow::Result<usize> {
        let _file = self.file.lock().unwrap();
        let moves = self.archive_plan_locked(keep_latest)?;
        if moves.is_empty() {
            return Ok(0);
        }
        if let Some(conflict) = moves.iter().find(|plan| plan.destination_exists) {
            anyhow::bail!(
                "refusing to overwrite archived segment {} while moving {}",
                conflict.destination.display(),
                conflict.source.display()
            );
        }
        let archive = self
            .segment_archive_dir()
            .context("journal path has no archive directory")?;
        let dir = self
            .path
            .parent()
            .context("journal path has no parent directory")?;
        std::fs::create_dir_all(&archive)
            .with_context(|| format!("create segment archive {}", archive.display()))?;
        for plan in &moves {
            std::fs::rename(&plan.source, &plan.destination).with_context(|| {
                format!(
                    "move segment {} into {}",
                    plan.source.display(),
                    archive.display()
                )
            })?;
        }
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
        if let Ok(d) = File::open(&archive) {
            let _ = d.sync_all();
        }
        Ok(moves.len())
    }

    /// Fold everything older than the newest `keep` records into a checkpoint
    /// and move that prefix into an archive segment. `None` = nothing old enough
    /// to be worth a rotation.
    ///
    /// 顺序即安全性：拿住 append 锁（杜绝读到一半有人追加）→ 归档段落盘并 fsync
    /// → 新活文件写临时名并 fsync → rename 原子替换 → 换掉持有的 fd。任何一步崩
    /// 在中间，盘上都还有一份可读的 journal：要么还是原文件（什么都没丢），要么
    /// 是新文件（前缀已在段文件里）。
    pub fn rotate(&self, keep: usize) -> anyhow::Result<Option<Rotation>> {
        let mut file = self.file.lock().unwrap();
        let records = self.replay();
        if records.len() <= keep {
            return Ok(None);
        }
        let cut = records.len() - keep;
        let (prefix, tail) = records.split_at(cut);
        let checkpoint = Checkpoint::capture(&reconcile(prefix));
        // Cumulative bookkeeping: a leading checkpoint already covers the records
        // of earlier segments; this rotation adds the raw records in front of it.
        // `raw_in_prefix` is what actually leaves the live file, and a prefix
        // that is only a previous checkpoint has nothing new to fold.
        let archived_before = prefix
            .iter()
            .find_map(|r| match r {
                JournalRecord::Snapshot { covers, .. } => Some(*covers),
                _ => None,
            })
            .unwrap_or(0);
        let raw_in_prefix = prefix
            .iter()
            .filter(|r| !matches!(r, JournalRecord::Snapshot { .. }))
            .count() as u64;
        if raw_in_prefix < 2 {
            // Folding one record into a checkpoint grows the live file (the
            // checkpoint line costs more than the record it replaces), so the
            // library refuses the degenerate case instead of trusting the
            // caller's threshold.
            return Ok(None);
        }
        let covers = archived_before + raw_in_prefix;
        let dir = self
            .path
            .parent()
            .context("journal path has no parent directory to archive into")?;
        let stem = self
            .path
            .file_stem()
            .and_then(|s| s.to_str())
            .context("journal path has no usable file stem")?;
        let archive = dir.join(format!("{stem}.seg{archived_before}-{covers}.jsonl"));
        write_records(&archive, prefix)
            .with_context(|| format!("write archive segment {}", archive.display()))?;

        let staged = dir.join(format!("{stem}.seg{archived_before}-{covers}.jsonl.new"));
        let mut live: Vec<JournalRecord> = Vec::with_capacity(tail.len() + 1);
        live.push(JournalRecord::Snapshot { covers, checkpoint });
        live.extend_from_slice(tail);
        write_records(&staged, &live)
            .with_context(|| format!("write staged journal {}", staged.display()))?;
        std::fs::rename(&staged, &self.path)
            .with_context(|| format!("swap journal {}", self.path.display()))?;
        // make the rename itself durable where the fs supports it
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }

        let reopened = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("reopen journal after rotation {}", self.path.display()))?;
        // swap the held handle through the guard we already own: releasing the
        // lock first would let an append land on the old inode and be lost
        *file = reopened;
        // the live file was swapped under us: re-derive the projection from it.
        // Dropping the file guard first keeps the lock order (file -> projection).
        drop(file);
        self.reseed()?;
        Ok(Some(Rotation {
            archived_to: archive,
            covered: raw_in_prefix as usize,
        }))
    }
}

/// Replay one journal file; a torn tail is skipped (same rule as `replay`).
fn replay_path(path: &Path) -> Vec<JournalRecord> {
    let Ok(f) = File::open(path) else {
        return vec![];
    };
    let mut out = Vec::new();
    for line in BufReader::new(f).lines().map_while(Result::ok) {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<JournalRecord>(&line) {
            Ok(r) => out.push(r),
            Err(_) => break,
        }
    }
    out
}

fn segment_files(dir: &Path, prefix: &str) -> Vec<(u64, u64, PathBuf)> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.map_while(Result::ok) {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix(prefix) else {
            continue;
        };
        let Some(range) = rest.strip_suffix(".jsonl") else {
            continue;
        };
        let Some((from, to)) = range.split_once('-') else {
            continue;
        };
        if let (Ok(from), Ok(to)) = (from.parse::<u64>(), to.parse::<u64>()) {
            found.push((from, to, entry.path()));
        }
    }
    found.sort();
    found
}

fn file_bytes(path: &Path) -> u64 {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn staged_files(dir: Option<&Path>) -> usize {
    let Some(dir) = dir else { return 0 };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".new"))
        .count()
}

fn write_records(path: &Path, records: &[JournalRecord]) -> anyhow::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    for record in records {
        writeln!(file, "{}", serde_json::to_string(record)?)?;
    }
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::records::{Stage, Verdict};
    use serde_json::json;

    fn bell(iteration: u64) -> ThinBell {
        ThinBell {
            role: "rd".into(),
            stage: Stage::Dev,
            check: None,
            iteration,
            r#ref: json!({ "spec_ref": "Spec.md" }),
        }
    }

    #[test]
    fn append_replay_roundtrip_tolerant() {
        let path = std::env::temp_dir().join(format!("kestrel-j-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let j = Journal::create(&path).unwrap();
        j.append(&JournalRecord::StageWritten {
            task: "t".into(),
            stage: Stage::Dev,
            iteration: 1,
        })
        .unwrap();
        j.append(&JournalRecord::BellSent {
            task: "t".into(),
            to: "harness-rd".into(),
            bell: bell(1),
        })
        .unwrap();
        drop(j);
        // torn tail
        {
            use std::io::Write as _;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            writeln!(f, "{{\"type\":\"bell_sent\"").unwrap();
        }
        let j2 = Journal::open(&path).unwrap();
        let records = j2.replay();
        assert_eq!(records.len(), 2);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn reconcile_finds_in_flight_bells() {
        let path = std::env::temp_dir().join(format!("kestrel-j2-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let j = Journal::create(&path).unwrap();
        j.append(&JournalRecord::BellSent {
            task: "t".into(),
            to: "harness-rd".into(),
            bell: bell(1),
        })
        .unwrap();
        j.append(&JournalRecord::ResultCommitted {
            task: "t".into(),
            stage: Stage::Dev,
            verdict: Verdict {
                pass: true,
                issues: vec![],
                test_runs: vec![],
                notes: vec![],
                attribution: Default::default(),
            },
        })
        .unwrap();
        j.append(&JournalRecord::BellSent {
            task: "t".into(),
            to: "harness-rd".into(),
            bell: bell(2),
        })
        .unwrap();
        let report = Journal::open(&path).unwrap().replay();
        let reconciled = records::reconcile(&report);
        // only the SECOND bell (bell 2) is in flight: first was settled
        assert_eq!(reconciled.in_flight.len(), 1);
        assert_eq!(reconciled.in_flight[0].bell.iteration, 2);
        // state projection: dev settled once
        assert_eq!(reconciled.state.stage, Some(Stage::Dev));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn budget_accumulates_per_peer() {
        let path = std::env::temp_dir().join(format!("kestrel-j3-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let j = Journal::create(&path).unwrap();
        for cost in [0.5, 0.75] {
            j.append(&JournalRecord::reserved(
                "demo",
                "harness-rd",
                Stage::Dev,
                1,
                cost,
                false,
            ))
            .unwrap();
        }
        let records = Journal::open(&path).unwrap().replay();
        let reconciled = records::reconcile(&records);
        let spent = reconciled
            .state
            .spent
            .get("harness-rd")
            .copied()
            .unwrap_or(0.0);
        assert!((spent - 1.25).abs() < 1e-9);
        std::fs::remove_file(&path).ok();
    }

    /// A fresh directory per rotation test (same pid, parallel threads).
    fn scratch(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("kestrel-jrot-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// One pipeline run: dispatch + charge per iteration,回铃 for every
    /// iteration but the last one — so the newest bell is genuinely unsettled.
    fn seed(journal: &Journal, bells: u64) {
        for i in 1..=bells {
            journal
                .append(&JournalRecord::StageWritten {
                    task: "demo".into(),
                    stage: Stage::Dev,
                    iteration: i,
                })
                .unwrap();
            journal
                .append(&JournalRecord::BellSent {
                    task: "demo".into(),
                    to: "harness-rd".into(),
                    bell: bell(i),
                })
                .unwrap();
            journal
                .append(&JournalRecord::reserved(
                    "demo",
                    "harness-rd",
                    Stage::Dev,
                    i,
                    0.5,
                    false,
                ))
                .unwrap();
            if i < bells {
                journal
                    .append(&JournalRecord::BellReceived {
                        task: "demo".into(),
                        from: "harness-rd".into(),
                        bell: bell(i),
                    })
                    .unwrap();
            }
        }
    }

    /// 截断不改变投影：折叠「活文件」与折叠「段文件 + 活文件」必须给出同一个
    /// state 和同一批未结算的铃；历史没有消失，只是搬进了可读的段。
    #[test]
    fn rotate_archives_the_prefix_without_changing_the_projection() {
        let dir = scratch("proj");
        let path = dir.join("journal.jsonl");
        let journal = Journal::create(&path).unwrap();
        seed(&journal, 4); // 15 records, iteration 4 still unanswered
        let before = journal.replay();
        assert_eq!(before.len(), 15);

        let rotated = journal.rotate(5).unwrap().expect("rotation");
        assert_eq!(rotated.covered, 10);
        assert!(rotated.archived_to.exists(), "prefix archived");
        assert_eq!(replay_path(&rotated.archived_to).len(), 10);
        assert_eq!(journal.len(), 6, "checkpoint + newest 5");
        assert_eq!(
            staged_leftovers(&dir),
            Vec::<String>::new(),
            "no staged file left behind"
        );
        assert_eq!(
            rotated.archived_to.file_name().unwrap(),
            "journal.seg0-10.jsonl",
            "the segment is named by the record range it covers"
        );

        let live = reconcile(&journal.replay());
        let all = reconcile(&journal.replay_all());
        assert_eq!(live.state, all.state, "budget/stage/iteration survive");
        assert_eq!(
            live.in_flight, all.in_flight,
            "the same unsettled bells from either fold"
        );
        assert_eq!(
            live.in_flight
                .iter()
                .map(|b| b.bell.iteration)
                .collect::<Vec<_>>(),
            vec![4],
            "the unsettled bell of the newest iteration is still in flight"
        );
        let spent = all.state.spent.get("harness-rd").copied().unwrap_or(0.0);
        assert!((spent - 2.0).abs() < 1e-9, "{spent}");

        // the held handle follows the renamed file: appending must still work
        journal
            .append(&JournalRecord::BellReceived {
                task: "demo".into(),
                from: "harness-rd".into(),
                bell: bell(4),
            })
            .unwrap();
        assert_eq!(journal.len(), 7, "checkpoint + 5 kept + the receipt");
        assert!(reconcile(&journal.replay()).in_flight.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Sealed segments may leave the journal directory, but not the truth:
    /// `replay_all` reads the archive directory too, and a later rotation can
    /// coexist with the already-archived older range.
    #[test]
    fn sealed_segments_move_to_an_archive_without_changing_history() {
        let dir = scratch("offline-archive");
        let path = dir.join("journal.jsonl");
        let journal = Journal::create(&path).unwrap();
        seed(&journal, 4);
        let before = reconcile(&journal.replay());

        let rotated = journal.rotate(5).unwrap().expect("rotation");
        assert_eq!(journal.archive_segments(0).unwrap(), 1);
        let archive = journal.segment_archive_dir().unwrap();
        let archived = archive.join(rotated.archived_to.file_name().unwrap());
        assert!(
            !rotated.archived_to.exists(),
            "the sealed file left the root"
        );
        assert!(
            archived.exists(),
            "the sealed file moved rather than vanished"
        );
        assert_eq!(replay_path(&archived).len(), 10);
        assert_eq!(
            journal.archive_segments(0).unwrap(),
            0,
            "archiving an empty root is idempotent"
        );

        let after_move = reconcile(&journal.replay_all());
        assert_eq!(after_move.state, before.state);
        assert_eq!(after_move.in_flight, before.in_flight);

        for iteration in 5..=8 {
            journal
                .append(&JournalRecord::StageWritten {
                    task: "demo".into(),
                    stage: Stage::Dev,
                    iteration,
                })
                .unwrap();
        }
        let before_second = reconcile(&journal.replay());
        journal.rotate(0).unwrap().expect("second rotation");
        assert_eq!(
            journal.segments().len(),
            2,
            "replay sees one archived and one newly sealed range"
        );
        assert_eq!(
            reconcile(&journal.replay_all()).state,
            before_second.state,
            "moving files changes location, not state"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn archive_keeps_the_requested_newest_segments_in_place() {
        let dir = scratch("archive-keep");
        let path = dir.join("journal.jsonl");
        let journal = Journal::create(&path).unwrap();
        seed(&journal, 2);
        journal.rotate(0).unwrap().expect("first sealed segment");
        for iteration in 3..=6 {
            journal
                .append(&JournalRecord::StageWritten {
                    task: "demo".into(),
                    stage: Stage::Dev,
                    iteration,
                })
                .unwrap();
        }
        journal.rotate(0).unwrap().expect("second sealed segment");
        for iteration in 7..=10 {
            journal
                .append(&JournalRecord::StageWritten {
                    task: "demo".into(),
                    stage: Stage::Dev,
                    iteration,
                })
                .unwrap();
        }
        journal.rotate(0).unwrap().expect("third sealed segment");
        let before = reconcile(&journal.replay_all());
        assert_eq!(segment_files(&dir, "journal.seg").len(), 3);

        assert_eq!(journal.archive_segments(1).unwrap(), 2);
        let archive = journal.segment_archive_dir().unwrap();
        assert_eq!(segment_files(&dir, "journal.seg").len(), 1);
        assert_eq!(segment_files(&archive, "journal.seg").len(), 2);
        assert_eq!(
            journal.archive_segments(10).unwrap(),
            0,
            "keep larger than the hot window moves nothing"
        );
        let after = reconcile(&journal.replay_all());
        assert_eq!(after.state, before.state);
        assert_eq!(after.in_flight, before.in_flight);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn journal_status_counts_live_segments_archive_and_staged_residue() {
        let dir = scratch("status");
        let path = dir.join("journal.jsonl");
        let journal = Journal::create(&path).unwrap();
        seed(&journal, 2);
        journal.rotate(0).unwrap().expect("sealed segment");

        let root = journal.status();
        assert!(root.live_records > 0);
        assert!(root.live_bytes > 0);
        assert_eq!(root.root_segments, 1);
        assert_eq!(root.archived_segments, 0);
        assert!(root.total_segment_bytes > 0);
        assert_eq!(root.staged_files, 0);
        assert_eq!(root.segments.len(), 1);
        assert!(!root.segments[0].archived);
        let as_json = serde_json::to_value(&root).unwrap();
        assert_eq!(as_json["root_segments"], 1);
        assert_eq!(as_json["segments"][0]["archived"], false);

        let plan = journal.archive_plan(0).unwrap();
        assert_eq!(plan.len(), 1);
        assert!(!plan[0].destination_exists);
        assert!(plan[0].source.exists(), "planning must not move a file");
        assert_eq!(plan[0].bytes, root.total_segment_bytes);

        journal.archive_segments(0).unwrap();
        std::fs::write(dir.join("journal.seg0-0.jsonl.new"), "").unwrap();
        let archived = journal.status();
        assert_eq!(archived.root_segments, 0);
        assert_eq!(archived.archived_segments, 1);
        assert_eq!(archived.staged_files, 1);
        assert_eq!(archived.segments.len(), 1);
        assert!(archived.segments[0].archived);
        assert_eq!(archived.total_segment_bytes, root.total_segment_bytes);
        assert!(!archived.is_healthy());
        let archived_path = archived.segments[0].path.clone();
        let duplicate_path = dir.join(archived_path.file_name().unwrap());
        std::fs::copy(&archived_path, &duplicate_path).unwrap();
        let duplicate = journal.status();
        assert_eq!(duplicate.root_segments, 1);
        assert_eq!(duplicate.archived_segments, 1);
        assert_eq!(duplicate.duplicate_segments, 1);
        assert!(!duplicate.is_healthy());
        std::fs::remove_file(duplicate_path).unwrap();
        std::fs::remove_file(dir.join("journal.seg0-0.jsonl.new")).unwrap();
        let healthy = journal.status();
        assert_eq!(healthy.duplicate_segments, 0);
        assert_eq!(healthy.staged_files, 0);
        assert!(healthy.is_healthy());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn approval_status_counts_pending_unacked_applied_stale_and_orphans() {
        let dir = scratch("approval-status");
        let path = dir.join("journal.jsonl");
        let journal = Journal::create(&path).unwrap();
        for record in [
            JournalRecord::ApprovalRequested {
                task: "t".into(),
                peer: "rd".into(),
                req: "a".into(),
                command: "cargo test".into(),
            },
            JournalRecord::ApprovalDecided {
                req: "a".into(),
                decision: "allow".into(),
                by: "sa".into(),
            },
            JournalRecord::ApprovalAcked {
                req: "a".into(),
                by: "rd".into(),
                applied: false,
            },
            JournalRecord::ApprovalAcked {
                req: "a".into(),
                by: "rd".into(),
                applied: true,
            },
            JournalRecord::ApprovalRequested {
                task: "t".into(),
                peer: "rd".into(),
                req: "b".into(),
                command: "cargo build".into(),
            },
            JournalRecord::ApprovalDecided {
                req: "c".into(),
                decision: "deny".into(),
                by: "sa".into(),
            },
            JournalRecord::ApprovalAcked {
                req: "d".into(),
                by: "rd".into(),
                applied: false,
            },
        ] {
            journal.append(&record).unwrap();
        }

        let approvals = journal.approval_status();
        assert_eq!(approvals.requests, 2);
        assert_eq!(approvals.decisions, 2);
        assert_eq!(approvals.receipts, 2);
        assert_eq!(approvals.pending, 1);
        assert_eq!(approvals.unacked, 1);
        assert_eq!(approvals.applied, 1);
        assert_eq!(approvals.stale, 1);
        assert_eq!(approvals.orphan_decisions, 1);
        assert_eq!(approvals.orphan_receipts, 1);

        let mut status = journal.status();
        status.approvals = Some(approvals);
        let json = serde_json::to_value(status).unwrap();
        assert_eq!(json["approvals"]["unacked"], 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 检查点必须自带「未结算的铃」——否则截断会把「结果铃丢了」这件事悄悄变成
    /// 一次重派发，而 journal 存在的意义正是防止这一点。
    #[test]
    fn rotate_keeps_unsettled_bells_reconcilable() {
        let dir = scratch("inflight");
        let path = dir.join("journal.jsonl");
        let journal = Journal::create(&path).unwrap();
        seed(&journal, 2);
        journal
            .rotate(0)
            .unwrap()
            .expect("rotation to checkpoint only");
        assert_eq!(journal.len(), 1, "live file is a bare checkpoint");

        let report = reconcile(&journal.replay());
        assert_eq!(
            report.in_flight.len(),
            1,
            "the one unanswered bell survives a full rotation"
        );
        assert_eq!(report.in_flight[0].bell.iteration, 2);
        assert_eq!(report.state.stage, Some(Stage::Dev));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn rotate_declines_when_there_is_nothing_new_to_fold() {
        let dir = scratch("noop");
        let path = dir.join("journal.jsonl");
        let journal = Journal::create(&path).unwrap();
        seed(&journal, 1);
        assert!(journal.rotate(5).unwrap().is_none(), "keep >= len");
        assert!(journal.rotate(0).unwrap().is_some());
        assert!(
            journal.rotate(0).unwrap().is_none(),
            "a live file that is only a checkpoint has nothing to archive"
        );
        assert!(path.metadata().unwrap().len() > 0, "journal stays readable");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 一条记录不值得检查点：折它会让活文件变长（检查点那一行比被它替换的记录更贵）。
    /// 守卫放在库里，不指望调用方给的阈值刚好。
    #[test]
    fn rotate_refuses_a_degenerate_prefix() {
        let dir = scratch("degenerate");
        let path = dir.join("journal.jsonl");
        let journal = Journal::create(&path).unwrap();
        seed(&journal, 1); // 3 records
        assert!(
            journal.rotate(2).unwrap().is_none(),
            "a one-record prefix must not be folded"
        );
        assert_eq!(journal.len(), 3, "nothing moved, nothing archived");
        assert!(
            !dir.join("journal.seg0-1.jsonl").exists(),
            "no segment was written for a degenerate fold"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// rotate 全程持住 append 锁的意义就在这条断言上：并发追加与旋转交错进行，
    /// 每条记录必须「要么进了归档段，要么还活在活文件里」，不重复也不丢。
    /// rename 前后释放锁会留一个写进旧 inode 的窗口——那个窗口就是这条测试。
    #[test]
    fn concurrent_appends_survive_rotation() {
        use std::sync::Arc;
        let dir = scratch("concurrent");
        let path = dir.join("journal.jsonl");
        let journal = Arc::new(Journal::create(&path).unwrap());

        let mut writers = Vec::new();
        for t in 0..4u64 {
            let journal = Arc::clone(&journal);
            writers.push(std::thread::spawn(move || {
                for i in 0..10u64 {
                    journal
                        .append(&JournalRecord::StageWritten {
                            task: "demo".into(),
                            stage: Stage::Dev,
                            iteration: t * 10 + i,
                        })
                        .unwrap();
                }
            }));
        }
        let rotator = {
            let journal = Arc::clone(&journal);
            std::thread::spawn(move || {
                let mut rounds = 0;
                while rounds < 40 {
                    let _ = journal.rotate(2);
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    rounds += 1;
                }
            })
        };

        for w in writers {
            w.join().unwrap();
        }
        rotator.join().unwrap();

        let all = journal.replay_all();
        let written: Vec<u64> = all
            .iter()
            .filter_map(|r| match r {
                JournalRecord::StageWritten { iteration, .. } => Some(*iteration),
                _ => None,
            })
            .collect();
        assert_eq!(written.len(), 40, "every append survived");
        let unique: std::collections::BTreeSet<u64> = written.into_iter().collect();
        assert_eq!(unique.len(), 40, "no record was written twice");
        assert!(
            staged_leftovers(&dir).is_empty(),
            "the staged file is consumed by the rename"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 崩溃窗口的幂等性：段名由「归档区间」决定，所以「段已写好、rename 前崩溃」
    /// 后重试会覆盖同一个文件，而不是把同一段前缀归档两次（那会让 replay_all 把
    /// 预算重复计一遍）。这里直接造出那个崩溃残留来验证。
    #[test]
    fn a_crashed_rotation_is_retried_idempotently() {
        let dir = scratch("crash");
        let path = dir.join("journal.jsonl");
        let journal = Journal::create(&path).unwrap();
        seed(&journal, 4); // 15 records, iteration 4 unanswered
        // residue of a rotation that died before the rename: the segment exists,
        // the live file is untouched, and a stale staged file is lying around
        write_records(&dir.join("journal.seg0-10.jsonl"), &journal.replay()[..10]).unwrap();
        write_records(&dir.join("journal.seg0-10.jsonl.new"), &[]).unwrap();
        assert_eq!(journal.len(), 15);

        let rotated = journal.rotate(5).unwrap().expect("retried rotation");
        assert_eq!(
            rotated.archived_to,
            dir.join("journal.seg0-10.jsonl"),
            "the same range overwrites the same segment"
        );
        assert_eq!(
            replay_path(&dir.join("journal.seg0-10.jsonl")).len(),
            10,
            "the retry rewrote exactly that range"
        );
        assert!(staged_leftovers(&dir).is_empty());
        // replay_all must not double-count the budget: 4 dispatches x 0.5
        let all = reconcile(&journal.replay_all());
        let spent = all.state.spent.get("harness-rd").copied().unwrap_or(0.0);
        assert!((spent - 2.0).abs() < 1e-9, "{spent}");
        assert_eq!(all.in_flight.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    fn staged_leftovers(dir: &Path) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Vec::new();
        };
        entries
            .flatten()
            .filter_map(|e| e.file_name().to_str().map(str::to_string))
            .filter(|name| name.ends_with(".new"))
            .collect()
    }

    /// 检查点是 journal 的一种记录：序列化往返必须保住未结算的铃与账本。
    #[test]
    fn checkpoint_record_roundtrips() {
        let checkpoint = Checkpoint {
            stage: Some(Stage::Test),
            iteration: 3,
            spent: [("harness-qa".to_string(), 1.5)].into_iter().collect(),
            in_flight: vec![records::InFlightBell {
                task: "demo".into(),
                to: "harness-qa".into(),
                bell: bell(3),
            }],
            ..Checkpoint::default()
        };
        let record = JournalRecord::Snapshot {
            covers: 9,
            checkpoint: checkpoint.clone(),
        };
        let line = serde_json::to_string(&record).unwrap();
        assert!(line.contains("\"type\":\"snapshot\""), "{line}");
        let back: JournalRecord = serde_json::from_str(&line).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), line);
        let report = reconcile(&[record]);
        assert_eq!(report.in_flight.len(), 1);
        assert_eq!(report.state.stage, Some(Stage::Test));
        assert_eq!(report.state.iteration, 3);
    }

    /// 不变式 5 的可执行形式：在线投影必须与「把整个活文件重放一遍」逐次相等。
    /// 这条断言是投影增量化唯一的安全带——否则预算停损和巡检读的是两份真相。
    #[test]
    fn projection_equals_a_full_replay_after_every_append() {
        let dir = scratch("projection");
        let path = dir.join("journal.jsonl");
        let journal = Journal::create(&path).unwrap();
        seed(&journal, 3);
        journal
            .append(&JournalRecord::ApprovalRequested {
                task: "demo".into(),
                peer: "harness-rd".into(),
                req: "a-1".into(),
                command: "cargo test".into(),
            })
            .unwrap();
        journal
            .append(&JournalRecord::ApprovalDecided {
                req: "a-1".into(),
                decision: "allow".into(),
                by: "harness-sa".into(),
            })
            .unwrap();
        journal
            .append(&JournalRecord::BellReceived {
                task: "demo".into(),
                from: "harness-rd".into(),
                bell: bell(3),
            })
            .unwrap();

        let online = journal.projection();
        let replayed = reconcile(&journal.replay());
        assert_eq!(online.state, replayed.state, "budget/stage/iteration agree");
        assert_eq!(
            online.in_flight, replayed.in_flight,
            "unsettled bells agree"
        );
        assert_eq!(journal.live_records(), journal.replay().len());
        assert!(journal.file_bytes() > 0);

        // a rotation replaces the file underneath: the projection follows it
        journal.rotate(2).unwrap();
        let after = journal.projection();
        let after_replay = reconcile(&journal.replay());
        assert_eq!(after.state, after_replay.state);
        assert_eq!(after.in_flight, after_replay.in_flight);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `create` 打开的是追加模式：旧 journal 里已经花掉的钱和没结算的铃都必须
    /// 被读回来，否则跨运行保留账本（`KESTREL_DEMO_KEEP_JOURNAL=1`）会变成
    /// 「预算归零」，停损形同虚设。
    #[test]
    fn create_seeds_the_projection_of_an_existing_journal() {
        let dir = scratch("resume");
        let path = dir.join("journal.jsonl");
        {
            let journal = Journal::create(&path).unwrap();
            journal
                .append(&JournalRecord::BellSent {
                    task: "demo".into(),
                    to: "harness-rd".into(),
                    bell: bell(1),
                })
                .unwrap();
            journal
                .append(&JournalRecord::reserved(
                    "demo",
                    "harness-rd",
                    Stage::Dev,
                    1,
                    0.5,
                    false,
                ))
                .unwrap();
        }
        let reopened = Journal::create(&path).unwrap();
        let projection = reopened.projection();
        assert_eq!(
            projection.in_flight.len(),
            1,
            "unsettled bell survives reopen"
        );
        assert_eq!(
            projection.state.spent.get("harness-rd").copied(),
            Some(0.5),
            "the stop-loss still sees yesterday's spend"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
