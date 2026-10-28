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

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;

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
    /// The name comes from the record RANGE it covers, so retrying a rotation
    /// that crashed before the rename overwrites the same file instead of
    /// archiving the same prefix twice — which would double-count the budget in
    /// `replay_all`.
    fn segments(&self) -> Vec<PathBuf> {
        let Some(dir) = self.path.parent() else {
            return vec![];
        };
        let Some(stem) = self.path.file_stem().and_then(|s| s.to_str()) else {
            return vec![];
        };
        let prefix = format!("{stem}.seg");
        let mut found: Vec<(u64, u64, PathBuf)> = Vec::new();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return vec![];
        };
        for entry in entries.map_while(Result::ok) {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(rest) = name.strip_prefix(&prefix) else {
                continue;
            };
            let Some(range) = rest.strip_suffix(".jsonl") else {
                continue;
            };
            let Some((from, to)) = range.split_once('-') else {
                continue;
            };
            if let (Ok(a), Ok(b)) = (from.parse::<u64>(), to.parse::<u64>()) {
                found.push((a, b, entry.path()));
            }
        }
        found.sort();
        found.into_iter().map(|(_, _, p)| p).collect()
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
        for usd in [0.5, 0.75] {
            j.append(&JournalRecord::BudgetSpent {
                peer: "harness-rd".into(),
                usage: crate::provider::wire::Usage::default(),
                usd,
            })
            .unwrap();
        }
        let records = Journal::open(&path).unwrap().replay();
        let reconciled = records::reconcile(&records);
        let spent = reconciled
            .state
            .spent_usd
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
                .append(&JournalRecord::BudgetSpent {
                    peer: "harness-rd".into(),
                    usage: Default::default(),
                    usd: 0.5,
                })
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
        let spent = all
            .state
            .spent_usd
            .get("harness-rd")
            .copied()
            .unwrap_or(0.0);
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
        let spent = all
            .state
            .spent_usd
            .get("harness-rd")
            .copied()
            .unwrap_or(0.0);
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
            spent_usd: [("harness-qa".to_string(), 1.5)].into_iter().collect(),
            in_flight: vec![records::InFlightBell {
                task: "demo".into(),
                to: "harness-qa".into(),
                bell: bell(3),
            }],
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
                .append(&JournalRecord::BudgetSpent {
                    peer: "harness-rd".into(),
                    usage: Default::default(),
                    usd: 0.5,
                })
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
            projection.state.spent_usd.get("harness-rd").copied(),
            Some(0.5),
            "the stop-loss still sees yesterday's spend"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
