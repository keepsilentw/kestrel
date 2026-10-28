//! The evidence contract between a worker's answer and the SA's verdict.
//!
//! A text verdict is a CLAIM; the files it points at are the evidence. `serve`
//! lifts the paths out of the answer and writes them into the artifact, then
//! `demo-sa` verifies them on disk — design §10 二期「判定从工件文字升级到
//! 工件 + 证据文件」的最小形态：`PASS` 不再自证，它必须指出盘上可查的东西。

use std::path::Path;

/// Cap on judged paths per artifact: a model that dumps a whole directory
/// listing must not turn one verdict into a filesystem walk.
pub const MAX_EVIDENCE_PATHS: usize = 12;

/// Lift evidence paths out of a worker's final answer.
///
/// 1. an explicit `evidence:` line is the machine-readable contract and wins;
/// 2. otherwise absolute-path tokens in the answer are collected — the SA
///    verifies them on disk, so a guessed path costs a re-dispatch, not a
///    false pass.
pub fn evidence_from_answer(text: &str) -> Vec<String> {
    let mut explicit: Vec<String> = Vec::new();
    for line in text.lines() {
        let trimmed = line
            .trim()
            .trim_start_matches(['-', '*', '#', '>', ' '])
            .trim_start_matches('*')
            .trim();
        let Some(rest) = strip_prefix_ci(trimmed, "evidence:") else {
            continue;
        };
        explicit.extend(path_tokens(rest));
    }
    let picked = if explicit.is_empty() {
        path_tokens(text)
    } else {
        explicit
    };
    dedup_cap(picked)
}

/// Verify claimed evidence on disk. One issue per unusable path; an empty
/// result means every claimed path is there and carries content.
pub fn check_evidence(paths: &[String]) -> Vec<String> {
    let mut issues = Vec::new();
    for p in paths.iter().take(MAX_EVIDENCE_PATHS) {
        let path = Path::new(p);
        match std::fs::metadata(path) {
            Err(_) => issues.push(format!("evidence missing: {p}")),
            Ok(md) if md.is_dir() => {
                if std::fs::read_dir(path).is_ok_and(|mut d| d.next().is_none()) {
                    issues.push(format!("evidence dir is empty: {p}"));
                }
            }
            Ok(md) if md.len() == 0 => issues.push(format!("evidence file is empty: {p}")),
            Ok(_) => {}
        }
    }
    issues
}

fn dedup_cap(paths: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for p in paths {
        if !out.contains(&p) {
            out.push(p);
        }
        if out.len() >= MAX_EVIDENCE_PATHS {
            break;
        }
    }
    out
}

/// Split a span into path-looking tokens: whitespace/quote/bracket separated,
/// trailing prose punctuation stripped, absolute-only, never a URL.
fn path_tokens(span: &str) -> Vec<String> {
    span.split([
        ' ', '\t', ',', ';', '"', '\'', '(', ')', '[', ']', '{', '}', '|',
    ])
    .map(|t| t.trim_matches(|c: char| ".:!?、）。*`".contains(c)))
    .filter(|t| t.starts_with('/') && t.len() > 1 && !t.contains("://") && !t.ends_with('/'))
    .map(|t| t.to_string())
    .collect()
}

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.get(..prefix.len())?;
    if head.eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_evidence_line_wins_over_prose() {
        let answer = "DONE: 实现完成（临时文件 /tmp/scratch.txt 已清理）\n\
                      evidence: /tmp/wt/lru.py, /tmp/wt/test_lru.py\n";
        assert_eq!(
            evidence_from_answer(answer),
            vec![
                "/tmp/wt/lru.py".to_string(),
                "/tmp/wt/test_lru.py".to_string()
            ]
        );
    }

    #[test]
    fn absolute_paths_are_collected_when_no_contract_line_exists() {
        let answer = "PASS — 审阅了 /tmp/wt/src/cache.rs 与 /tmp/wt/tests/t.rs";
        let got = evidence_from_answer(answer);
        assert!(got.contains(&"/tmp/wt/src/cache.rs".to_string()), "{got:?}");
        assert!(got.contains(&"/tmp/wt/tests/t.rs".to_string()), "{got:?}");
    }

    #[test]
    fn urls_and_relative_paths_are_not_evidence() {
        assert!(
            evidence_from_answer("see https://example.com/a.py and src/lib.rs and / alone")
                .is_empty()
        );
    }

    #[test]
    fn markdown_punctuation_does_not_reach_the_path() {
        let answer = "evidence: `/tmp/wt/a.py`; `/tmp/wt/b.py` (新增)";
        assert_eq!(
            evidence_from_answer(answer),
            vec!["/tmp/wt/a.py".to_string(), "/tmp/wt/b.py".to_string()]
        );
    }

    #[test]
    fn evidence_check_reports_missing_empty_and_ok() {
        let dir = std::env::temp_dir().join(format!("kestrel-ev-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let full = dir.join("full.txt");
        let empty = dir.join("empty.txt");
        std::fs::write(&full, "content").unwrap();
        std::fs::write(&empty, "").unwrap();
        let missing = dir.join("nope.txt");

        let issues = check_evidence(&[
            full.display().to_string(),
            empty.display().to_string(),
            missing.display().to_string(),
        ]);
        assert_eq!(issues.len(), 2, "{issues:?}");
        assert!(
            issues
                .iter()
                .any(|i| i.starts_with("evidence file is empty")),
            "{issues:?}"
        );
        assert!(
            issues.iter().any(|i| i.starts_with("evidence missing")),
            "{issues:?}"
        );
        assert!(
            check_evidence(&[dir.display().to_string()]).is_empty(),
            "a non-empty dir is evidence"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn path_cap_bounds_the_walk() {
        let many: Vec<String> = (0..30)
            .map(|i| format!("evidence: /tmp/wt/f{i}.py"))
            .collect();
        let answer = many.join("\n");
        assert_eq!(evidence_from_answer(&answer).len(), MAX_EVIDENCE_PATHS);
    }
}
