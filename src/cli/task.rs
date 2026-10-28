//! Task identity on the wire and on disk — the single source of truth for how a
//! task id becomes a name.
//!
//! 一个 hub 上可以并存多条 pipeline（多任务并行）。task id 必须同时出现在门铃
//! payload、worker 的工件文件名、SA 的 journal 记录里，而且三处拼法必须一致：
//! 「两端各自写一遍 `format!`」正是 design §9.11④ 记过的那类 bug（SA 的读盘巡检
//! 永不命中，stage 全绿是假的）。所以命名规则只在这里定义一次。

use std::path::{Path, PathBuf};

use serde_json::Value;

/// Task id used when a bell carries none (single-pipeline runs).
pub const DEFAULT_TASK: &str = "default";

/// Longest task/session slug before it gets a uniqueness suffix.
const SLUG_MAX: usize = 40;

/// Task id of a bell payload: `task_id`, else the default. A bell that only
/// carries the human-readable `task` text still lands in the default namespace
/// instead of inventing one from prose.
pub fn task_of(payload: &Value) -> String {
    payload["task_id"]
        .as_str()
        .filter(|t| !t.trim().is_empty())
        .unwrap_or(DEFAULT_TASK)
        .to_string()
}

/// Filesystem-safe, deterministic slug for a task id or a session address.
///
/// 这些字符串来自另一个进程（SA 发的铃、CLI 传的 session 名），却要成为文件名的一部
/// 分：`/` 与 `..` 绝不能穿过工件目录去动别人的文件，所以非 `[A-Za-z0-9._-]` 一律
/// 变成 `-`（折叠连续分隔符），`.` / `..` 这两个特殊名换成带哈希的名字。中文任务名
/// 因此退化成 `demo-LRU-cache` 这种形状——可读性次要，唯一与不可穿越是硬要求。
pub fn slug(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.trim().chars() {
        let kept = if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            ch
        } else {
            '-'
        };
        if kept == '-' && out.ends_with('-') {
            continue;
        }
        out.push(kept);
    }
    let out = out.trim_matches('-');
    if out.is_empty() || out == "." || out == ".." {
        // the two names that would let a filename become a path
        return format!("task-{:08x}", fnv1a(raw) as u32);
    }
    if out.len() <= SLUG_MAX {
        return out.to_string();
    }
    // only ASCII was pushed, so byte slicing is char-safe here
    let head = out[..SLUG_MAX].trim_end_matches('-');
    format!("{head}-{:08x}", fnv1a(raw) as u32)
}

/// `{session}-{task}-{stage}-iter{n}.json` — the contract both ends share.
pub fn artifact_name(session: &str, task_id: &str, stage: &str, iteration: u64) -> String {
    format!(
        "{}-{}-{}-iter{n}.json",
        slug(session),
        slug(task_id),
        slug(stage),
        n = iteration
    )
}

/// Where the SA looks for that artifact.
pub fn artifact_path(
    dir: &Path,
    session: &str,
    task_id: &str,
    stage: &str,
    iteration: u64,
) -> PathBuf {
    dir.join(artifact_name(session, task_id, stage, iteration))
}

/// FNV-1a: a couple of lines, no dependency, stable across runs and platforms.
fn fnv1a(text: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn task_id_is_read_from_the_bell_with_a_fallback() {
        assert_eq!(task_of(&json!({ "task_id": "lru" })), "lru");
        assert_eq!(task_of(&json!({ "task_id": "   " })), DEFAULT_TASK);
        assert_eq!(task_of(&json!({ "task": "自由文本" })), DEFAULT_TASK);
    }

    /// 名字里的路径分隔与 `..` 都被中和：文件名不能变成通往别人的目录的出口。
    #[test]
    fn slugs_cannot_escape_the_artifacts_dir() {
        for raw in [
            "../../etc/passwd",
            "..",
            ".",
            "/absolute/path",
            "a/../../b",
            "",
            "   ",
            "中文任务名",
        ] {
            let s = slug(raw);
            assert!(!s.is_empty(), "{raw:?}");
            assert!(!s.contains('/'), "{raw:?} -> {s}");
            assert_ne!(s, "..", "{raw:?} -> {s}");
            assert_ne!(s, ".", "{raw:?} -> {s}");
        }
        assert_eq!(slug("demo: 实现一个 LRU cache"), "demo-LRU-cache");
        assert_eq!(
            slug("harness-rd"),
            "harness-rd",
            "plain names are untouched"
        );
    }

    #[test]
    fn long_ids_stay_unique_and_bounded() {
        let a = format!("{}-one", "x".repeat(SLUG_MAX));
        let b = format!("{}-two", "x".repeat(SLUG_MAX));
        let (sa, sb) = (slug(&a), slug(&b));
        assert_ne!(sa, sb, "a shared prefix must not collide");
        assert!(sa.len() <= SLUG_MAX + 10, "{sa}");
        assert_eq!(slug(&a), sa, "deterministic");
    }

    /// SA 与 worker 各自拼同一个函数：命名漂移曾经让 stage 假绿（§9.11④）。
    #[test]
    fn artifact_name_is_the_shared_contract() {
        assert_eq!(
            artifact_name("harness-rd", "demo", "dev", 1),
            "harness-rd-demo-dev-iter1.json"
        );
        // two tasks on one worker never write the same file
        assert_ne!(
            artifact_name("harness-rd", "task-a", "dev", 1),
            artifact_name("harness-rd", "task-b", "dev", 1)
        );
        // a task-prefixed session address on a second pipeline stays distinct
        assert_eq!(
            artifact_name("t2:harness-rd", "t2", "dev", 1),
            "t2-harness-rd-t2-dev-iter1.json"
        );
        assert!(
            artifact_path(Path::new("/tmp/a"), "s", "t", "dev", 2).ends_with("s-t-dev-iter2.json")
        );
    }
}
