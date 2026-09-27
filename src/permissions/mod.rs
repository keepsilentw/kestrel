//! Permission engine: policy = data (pi tool_call-block 形), three-state
//! decisions over a JSON prefix table (codex execpolicy semantics, minus the
//! Starlark DSL — phase 2). v1 is fail-closed: unparseable -> Ask.

pub mod tokenizer;

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Decision {
    Allow,
    Ask,
    Deny,
}

impl Decision {
    /// Monotone combine: the most restrictive wins.
    pub fn max(self, other: Decision) -> Decision {
        if self >= other { self } else { other }
    }
}

/// One pattern token: an exact word or a set of alternatives
/// (`["git", ["pull", "push"]]` matches `git pull` / `git push`).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Segment {
    One(String),
    AnyOf(Vec<String>),
}

impl Segment {
    fn matches(&self, word: &str) -> bool {
        match self {
            Segment::One(w) => w == word,
            Segment::AnyOf(vs) => vs.iter().any(|v| v == word),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrefixRule {
    pub pattern: Vec<Segment>,
    pub decision: Decision,
}

/// JSON policy file shape (loaded at startup; no hot reload in v1).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyTable {
    pub default: Decision,
    #[serde(default)]
    pub rules: Vec<PrefixRule>,
    /// Never allowed even if a rule says so: interpreter prefixes
    /// (codex BANNED_PREFIX_SUGGESTIONS — never固化 `bash -c` as allow).
    #[serde(default)]
    pub forbidden_prefixes: Vec<Vec<String>>,
}

impl Default for PolicyTable {
    fn default() -> Self {
        Self {
            default: Decision::Ask,
            rules: vec![],
            forbidden_prefixes: vec![],
        }
    }
}

pub struct PermissionEngine {
    pub table: PolicyTable,
}

impl PermissionEngine {
    pub fn new(table: PolicyTable) -> Self {
        Self { table }
    }

    pub fn from_json_file(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let table: PolicyTable = serde_json::from_str(&text)?;
        Ok(Self::new(table))
    }

    /// Evaluate an already-parsed word list against the prefix table.
    fn evaluate_words(&self, words: &[String]) -> Decision {
        for banned in &self.table.forbidden_prefixes {
            if prefix_matches_words(banned, words) {
                return Decision::Deny;
            }
        }
        for rule in &self.table.rules {
            if prefix_matches(&rule.pattern, words) {
                return rule.decision;
            }
        }
        self.table.default
    }

    /// Overall decision for a whole command line: parse-fail -> Ask (fail-closed);
    /// per-segment decisions combine monotonically.
    pub fn evaluate_command(&self, cmd: &str) -> Decision {
        match tokenizer::split_segments(cmd) {
            None => Decision::Ask,
            Some(segments) => segments
                .iter()
                .map(|w| self.evaluate_words(w))
                .fold(Decision::Allow, |acc, d| acc.max(d)),
        }
    }

    /// True when every segment was explicitly allowed by a rule (never by
    /// default) — precondition for bypassing the sandbox (codex exec_policy.rs:440).
    pub fn all_segments_explicitly_allowed(&self, cmd: &str) -> bool {
        let Some(segments) = tokenizer::split_segments(cmd) else {
            return false;
        };
        !segments.is_empty()
            && segments.iter().all(|w| {
                !self
                    .table
                    .forbidden_prefixes
                    .iter()
                    .any(|b| prefix_matches_words(b, w))
                    && self
                        .table
                        .rules
                        .iter()
                        .find(|r| prefix_matches(&r.pattern, w))
                        .map(|r| r.decision == Decision::Allow)
                        .unwrap_or(false)
            })
    }

    /// File-write path check for write-class tools: inside any writable root,
    /// minus read-only islands.
    pub fn path_writable(&self, roots: &[PathBuf], islands: &[PathBuf], path: &Path) -> bool {
        let path = canon_lexical(path);
        let in_root = roots.iter().any(|r| path.starts_with(canon_lexical(r)));
        if !in_root {
            return false;
        }
        !islands.iter().any(|i| path.starts_with(canon_lexical(i)))
    }
}

/// Canonicalize resolving symlinks even when the final components don't
/// exist yet: walk up to the nearest existing ancestor, then re-join.
/// Necessary on macOS where /tmp is a symlink to /private/tmp.
fn canon_lexical(p: &Path) -> PathBuf {
    if let Ok(c) = p.canonicalize() {
        return c;
    }
    let mut missing: Vec<std::ffi::OsString> = Vec::new();
    let mut cur = p.to_path_buf();
    loop {
        match (cur.parent(), cur.file_name()) {
            (Some(parent), Some(name)) => {
                missing.push(name.to_owned());
                if let Ok(c) = parent.canonicalize() {
                    let mut out = c;
                    for m in missing.iter().rev() {
                        out.push(m);
                    }
                    return out;
                }
                cur = parent.to_path_buf();
            }
            _ => return p.to_path_buf(),
        }
    }
}

fn prefix_matches_words(pattern: &[String], words: &[String]) -> bool {
    pattern.len() <= words.len() && pattern.iter().zip(words).all(|(p, w)| p == w)
}

fn prefix_matches(pattern: &[Segment], words: &[String]) -> bool {
    pattern.len() <= words.len() && pattern.iter().zip(words).all(|(p, w)| p.matches(w))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> PermissionEngine {
        PermissionEngine::new(PolicyTable {
            default: Decision::Ask,
            rules: vec![
                PrefixRule {
                    pattern: vec![Segment::One("rg".into())],
                    decision: Decision::Allow,
                },
                PrefixRule {
                    pattern: vec![
                        Segment::One("git".into()),
                        Segment::AnyOf(vec!["pull".into(), "fetch".into(), "status".into()]),
                    ],
                    decision: Decision::Allow,
                },
                PrefixRule {
                    pattern: vec![Segment::One("git".into()), Segment::One("push".into())],
                    decision: Decision::Ask,
                },
                PrefixRule {
                    pattern: vec![Segment::One("rm".into())],
                    decision: Decision::Deny,
                },
            ],
            forbidden_prefixes: vec![vec!["bash".into()], vec!["python".into()]],
        })
    }

    #[test]
    fn allow_ask_deny_matrix() {
        let e = engine();
        assert_eq!(e.evaluate_command("rg foo"), Decision::Allow);
        assert_eq!(e.evaluate_command("git status"), Decision::Allow);
        assert_eq!(e.evaluate_command("git pull --rebase"), Decision::Allow);
        assert_eq!(e.evaluate_command("git push origin main"), Decision::Ask);
        assert_eq!(e.evaluate_command("rm -rf x"), Decision::Deny);
        assert_eq!(e.evaluate_command("cargo test"), Decision::Ask); // default
    }

    #[test]
    fn unparseable_fails_closed_to_ask() {
        let e = engine();
        assert_eq!(e.evaluate_command("rg foo > out.txt"), Decision::Ask);
        assert_eq!(e.evaluate_command("echo $HOME"), Decision::Ask);
        assert_eq!(e.evaluate_command("echo `id`"), Decision::Ask);
        assert_eq!(e.evaluate_command("a; b"), Decision::Ask);
    }

    #[test]
    fn forbidden_prefix_beats_rules() {
        let e = PermissionEngine::new(PolicyTable {
            default: Decision::Deny,
            rules: vec![PrefixRule {
                pattern: vec![Segment::One("bash".into())],
                decision: Decision::Allow,
            }],
            forbidden_prefixes: vec![vec!["bash".into()]],
        });
        assert_eq!(e.evaluate_command("bash -c 'anything'"), Decision::Deny);
    }

    #[test]
    fn monotone_combine_across_segments() {
        let e = engine();
        // first segment allowed, second hits default ask -> overall ask
        assert_eq!(e.evaluate_command("rg foo && cargo test"), Decision::Ask);
        // one deny poisons the whole line
        assert_eq!(e.evaluate_command("rg foo && rm x"), Decision::Deny);
    }

    #[test]
    fn explicit_allow_gate_for_sandbox_bypass() {
        let e = engine();
        assert!(e.all_segments_explicitly_allowed("rg foo"));
        assert!(!e.all_segments_explicitly_allowed("cargo test")); // default ask
        assert!(!e.all_segments_explicitly_allowed("rg foo > x")); // unparseable
    }

    #[test]
    fn path_writable_respects_roots_and_islands() {
        let e = PermissionEngine::new(PolicyTable::default());
        let tmp = std::env::temp_dir().join(format!("kestrel-test-{}", std::process::id()));
        std::fs::create_dir_all(tmp.join("ws/.git")).unwrap();
        let roots = vec![tmp.join("ws")];
        let islands = vec![tmp.join("ws/.git")];
        assert!(e.path_writable(&roots, &islands, &tmp.join("ws/a.txt")));
        assert!(!e.path_writable(&roots, &islands, &tmp.join("other/a.txt")));
        assert!(!e.path_writable(&roots, &islands, &tmp.join("ws/.git/config")));
        std::fs::remove_dir_all(&tmp).ok();
    }
}
