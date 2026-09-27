//! Journal: SA-side write-ahead truth source (plan §2.9).
//! Rule 1 「先写盘再拉铃」由调用序保证：append(StageWritten) → append(BellSent)
//! → bus.notify()。恢复 = replay → 对账 → 重生成 state 投影。
//! state.json 与 journal 的关系：journal 是事件真相，state.json 是投影。

pub mod records;

pub use records::{JournalRecord, ReconcileReport, StateSnapshot, ThinBell, Verdict};

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

pub struct Journal {
    file: Mutex<File>,
    path: std::path::PathBuf,
}

use std::sync::Mutex;

impl Journal {
    pub fn create(path: &Path) -> anyhow::Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
            path: path.to_path_buf(),
        })
    }

    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
            path: path.to_path_buf(),
        })
    }

    /// Append one record and fsync — the ONLY way records enter the journal.
    pub fn append(&self, record: &JournalRecord) -> anyhow::Result<()> {
        let mut file = self.file.lock().unwrap();
        writeln!(file, "{}", serde_json::to_string(record)?)?;
        file.flush()?;
        file.sync_all()?;
        Ok(())
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
}
