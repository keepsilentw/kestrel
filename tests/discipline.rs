//! Static discipline gates (plan §5 质量闸门):
//! - every `tokio::spawn` in engine code is wrapped in AbortOnDropHandle,
//!   except lines explicitly marked as fire-and-forget.
//! - the size / test counts quoted in README, design and the smoke report are
//!   recomputed from the tree and compared against each doc's canonical
//!   `**Metrics**` line. Those figures used to be copied by hand and drifted
//!   (design §9.19), so a number in a doc is now a value a machine checks.

use std::fs;

const SCAN_DIRS: &[&str] = &["src/core", "src/tools", "src/provider"];

#[test]
fn spawns_are_wrapped_or_marked_fire_and_forget() {
    let mut checked = 0;
    for dir in SCAN_DIRS {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(dir);
        for entry in std::fs::read_dir(&root).expect("dir readable") {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let text = fs::read_to_string(&path).unwrap();
            for (i, line) in text.lines().enumerate() {
                if line.contains("tokio::spawn(") {
                    checked += 1;
                    let wrapped = line.contains("AbortOnDropHandle");
                    // marked fire-and-forget / managed: marker on this line, the
                    // one above, or the one below (comment may sit next to the spawn)
                    let lines: Vec<&str> = text.lines().collect();
                    let marked = [i.saturating_sub(1), i, (i + 1).min(lines.len() - 1)]
                        .iter()
                        .any(|&j| {
                            let l = lines[j].to_lowercase();
                            l.contains("fire-and-forget") || l.contains("managed:")
                        });
                    assert!(
                        wrapped || marked,
                        "{path:?}:{} bare tokio::spawn without AbortOnDropHandle or \
                         fire-and-forget/managed marker",
                        i + 1
                    );
                }
            }
        }
    }
    assert!(
        checked >= 3,
        "gate scanned too few spawns ({checked}); did sources move?"
    );
}

/* ------------------------------------------------------------------------ *
 * Metrics gate.
 *
 * README.md, docs/design.md and docs/smoke-report.md all quote how big the
 * tree is and how many tests it holds. Those numbers were maintained by hand
 * in three places and had already drifted (design §9.19 records 137 quoted
 * against 130 actually run). A number nobody recomputes is a number that goes
 * stale, so the docs now carry ONE canonical line each, and this test
 * recomputes it from the filesystem.
 *
 * When this fails: copy the `expected` string from the message over the stale
 * line, and grep the docs for the old figure if any prose still repeats it.
 * ------------------------------------------------------------------------ */

/// Docs that quote size or test counts.
const METRIC_DOCS: &[&str] = &["README.md", "docs/design.md", "docs/smoke-report.md"];
/// Docs that additionally list the per-file integration split.
const INTEGRATION_DOCS: &[&str] = &["docs/design.md", "docs/smoke-report.md"];
const METRICS_MARK: &str = "**Metrics**";
const INTEGRATION_MARK: &str = "**Integration**";

#[derive(Debug, PartialEq)]
struct Counts {
    src_files: usize,
    src_lines: usize,
    test_files: usize,
    test_lines: usize,
    lib: usize,
    bin: usize,
    integration: usize,
    /// (测试文件名, 该文件的 #[test] 数)，按名排序，用于拼 INTEGRATION 行。
    per_file: Vec<(String, usize)>,
}

impl Counts {
    fn total(&self) -> usize {
        self.lib + self.bin + self.integration
    }

    fn metrics_line(&self) -> String {
        format!(
            "{METRICS_MARK} — src {} files / {} lines · tests {} files / {} lines · {} tests \
             (lib {} + bin {} + integration {})",
            self.src_files,
            self.src_lines,
            self.test_files,
            self.test_lines,
            self.total(),
            self.lib,
            self.bin,
            self.integration
        )
    }

    fn integration_line(&self) -> String {
        let parts: Vec<String> = self
            .per_file
            .iter()
            .map(|(name, n)| format!("{name} {n}"))
            .collect();
        format!("{INTEGRATION_MARK} — {}", parts.join(" / "))
    }
}

/// `#[test]`, `#[tokio::test]` and any `#[tokio::test(...)]` spelling; comment
/// lines never count (a doc example must not inflate a gate).
fn is_test_attr(line: &str) -> bool {
    let s = line.trim();
    if s.starts_with("//") {
        return false;
    }
    s == "#[test]" || s == "#[tokio::test]" || (s.starts_with("#[tokio::test") && s.ends_with(']'))
}

fn read(path: &std::path::Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Recurses so `src/bus/` and a future nested module both get scanned.
fn rs_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in
            fs::read_dir(&current).unwrap_or_else(|e| panic!("read {}: {e}", current.display()))
        {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// Walks the working tree (not `git ls-files`) so the gate also holds from a
/// source tarball or a worktree without an index.
fn compute_counts() -> Counts {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = rs_files(&root.join("src"));
    let tests_dir = root.join("tests");
    // tests/*.rs only: tests/common/mod.rs is a shared helper module, and the
    // fixtures dir holds SSE/JSONL, not test functions.
    let tests: Vec<std::path::PathBuf> = {
        let mut entries: Vec<_> = fs::read_dir(&tests_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("rs"))
            .collect();
        entries.sort();
        entries
    };
    let all_test_files = rs_files(&tests_dir);

    let lines = |paths: &[std::path::PathBuf]| -> usize {
        paths.iter().map(|p| read(p).lines().count()).sum()
    };
    let per_file: Vec<(String, usize)> = tests
        .iter()
        .map(|p| {
            (
                p.file_stem().unwrap().to_string_lossy().into_owned(),
                read(p).lines().filter(|l| is_test_attr(l)).count(),
            )
        })
        .collect();

    Counts {
        src_files: src.len(),
        src_lines: lines(&src),
        test_files: all_test_files.len(),
        test_lines: lines(&all_test_files),
        lib: src
            .iter()
            .filter(|p| p.file_name().and_then(|e| e.to_str()) != Some("main.rs"))
            .map(|p| read(p).lines().filter(|l| is_test_attr(l)).count())
            .sum(),
        bin: src
            .iter()
            .filter(|p| p.file_name().and_then(|e| e.to_str()) == Some("main.rs"))
            .map(|p| read(p).lines().filter(|l| is_test_attr(l)).count())
            .sum(),
        integration: per_file.iter().map(|(_, n)| n).sum(),
        per_file,
    }
}

/// A canonical line must appear EXACTLY once per doc and match what the tree
/// says. Only lines that BEGIN with the mark (after an optional `>` blockquote
/// prefix) are treated as canonical, so prose that merely mentions the line is
/// not mistaken for one.
fn check_canonical_line(doc: &str, mark: &str, expected: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(doc);
    let text = read(&path);
    let found: Vec<&str> = text
        .lines()
        .map(|line| line.trim_start_matches(['>', ' ']))
        .filter(|line| line.starts_with(mark))
        .map(|line| line.trim_end())
        .collect();
    assert_eq!(
        found.len(),
        1,
        "{doc}: expected exactly one `{mark}` line (the counts need ONE source), \
         found {}\n  expected: {expected}",
        found.len()
    );
    assert_eq!(
        found[0], expected,
        "{doc}: stale `{mark}` line.\n  found:    {}\n  expected: {expected}",
        found[0]
    );
}

#[test]
fn metrics_lines_match_the_tree() {
    let counts = compute_counts();
    // 自检：扫描器搬家或漏目录时会静默变成 0，那比数字过期更糟
    assert!(
        counts.src_files > 40 && counts.total() > 150,
        "the scanner found too little: {counts:?}"
    );
    assert_eq!(
        counts.integration,
        counts.per_file.iter().map(|(_, n)| n).sum::<usize>(),
        "integration split adds up"
    );

    let metrics = counts.metrics_line();
    for doc in METRIC_DOCS {
        check_canonical_line(doc, METRICS_MARK, &metrics);
    }
    let integration = counts.integration_line();
    for doc in INTEGRATION_DOCS {
        check_canonical_line(doc, INTEGRATION_MARK, &integration);
    }
}
