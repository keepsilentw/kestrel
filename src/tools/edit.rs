//! edit tool: multi-edit exact-string replacement (pi-style, NOT apply-patch).
//! Each `old` must match exactly once after the advancing cursor: zero matches
//! ("not found") and several ("not unique") are both rejections, because a
//! first-match guess is how the wrong occurrence gets rewritten. CRLF/BOM
//! normalization fallback rewrites normalized. The tool refuses to touch a file
//! the model has not read at exactly this version (`read_state`), and echoes the
//! rewritten lines back so the model can check where the edit landed.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::read_state::path_key;
use super::{Prepared, Reject, Tool, ToolCall, ToolCtx, ToolError, ToolOutcome, ToolUpdate};

pub struct Edit;

/// Byte range of one replacement inside the *result* text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Applied {
    pub start: usize,
    pub end: usize,
}

/// Cap on the echoed excerpt: a large rewrite must not flood the context.
const MAX_ECHO_BYTES: usize = 2_000;
/// Context lines echoed above/below each replacement.
const ECHO_CONTEXT: usize = 2;
/// Body lines echoed before the middle is elided.
const ECHO_BODY_LINES: usize = 6;

/// One old->new replacement pair.
fn parse_edits(args: &Value) -> Result<(String, Vec<(String, String)>), Reject> {
    let Some(path) = args["path"].as_str() else {
        return Err(Reject::RespondToModel {
            message: "edit requires a string `path`".into(),
        });
    };
    let Some(edits) = args["edits"].as_array() else {
        return Err(Reject::RespondToModel {
            message: "edit requires an `edits` array of {old, new}".into(),
        });
    };
    let mut out = Vec::new();
    for e in edits {
        let Some(old) = e["old"].as_str() else {
            return Err(Reject::RespondToModel {
                message: "edit entry requires `old`".into(),
            });
        };
        let Some(new) = e["new"].as_str() else {
            return Err(Reject::RespondToModel {
                message: "edit entry requires `new`".into(),
            });
        };
        out.push((old.to_string(), new.to_string()));
    }
    Ok((path.to_string(), out))
}

/// Apply edits left to right with an advancing cursor. Each `old` must occur
/// **exactly once** in the remaining text; returns the result plus the byte
/// range of every replacement inside it (for the echo).
pub fn apply_edits(
    content: &str,
    edits: &[(String, String)],
) -> Result<(String, Vec<Applied>), String> {
    let mut cursor = 0usize;
    let mut out = String::new();
    let mut applied = Vec::with_capacity(edits.len());
    for (i, (old, new)) in edits.iter().enumerate() {
        if old.is_empty() {
            return Err(format!("edit {i}: empty `old`"));
        }
        let hay = &content[cursor..];
        let matches = hay.matches(old.as_str()).count();
        if matches == 0 {
            // Two traps worth naming: an anchor pointing at text another edit in
            // this batch produced, and one sitting behind the advancing cursor.
            let hint = if applied
                .iter()
                .any(|a: &Applied| out[a.start..a.end].contains(old.as_str()))
            {
                " (it matches text an earlier edit in this batch produced; anchors \
                 resolve against the original file — use a separate edit call)"
            } else if out.contains(old.as_str()) {
                " (it sits before an earlier edit's region; edits are applied \
                 front-to-back — reorder them)"
            } else {
                ""
            };
            return Err(format!(
                "edit {i}: old text not found after cursor: {:?}{hint}",
                abbrev(old)
            ));
        }
        if matches > 1 {
            return Err(format!(
                "edit {i}: old text is not unique — {matches} matches after cursor; \
                 include more surrounding context: {:?}",
                abbrev(old)
            ));
        }
        let start = cursor + hay.find(old.as_str()).expect("counted exactly one match");
        out.push_str(&content[cursor..start]);
        let from = out.len();
        out.push_str(new);
        applied.push(Applied {
            start: from,
            end: out.len(),
        });
        cursor = start + old.len();
    }
    out.push_str(&content[cursor..]);
    Ok((out, applied))
}

/// Line-numbered excerpt of the rewritten regions: the model's self-check for
/// "did this land where I meant it to". Byte-capped (MAX_ECHO_BYTES).
fn echo(applied_text: &str, regions: &[Applied]) -> String {
    let lines: Vec<&str> = applied_text.split('\n').collect();
    let line_of = |byte: usize| applied_text[..byte].matches('\n').count();
    let mut out = String::new();
    for (n, r) in regions.iter().enumerate() {
        let first = line_of(r.start);
        let last = line_of(r.end).max(first);
        let from = first.saturating_sub(ECHO_CONTEXT);
        let to = (last + ECHO_CONTEXT).min(lines.len().saturating_sub(1));
        out.push_str(&format!("\n--- edit {} (line {}) ---\n", n + 1, first + 1));
        let long_body = last.saturating_sub(first) > ECHO_BODY_LINES;
        let head_end = first + ECHO_BODY_LINES / 2;
        let tail_start = last.saturating_sub(ECHO_BODY_LINES / 2);
        let mut elided = false;
        for (idx, line) in lines[from..=to].iter().enumerate() {
            let lineno = from + idx;
            if long_body && lineno > head_end && lineno < tail_start {
                if !elided {
                    out.push_str("        ...\n");
                    elided = true;
                }
                continue;
            }
            out.push_str(&format!("{:>6}| {line}\n", lineno + 1));
            if out.len() > MAX_ECHO_BYTES {
                out.push_str("        ... (echo truncated)\n");
                return out;
            }
        }
    }
    out
}

fn abbrev(s: &str) -> String {
    if s.len() > 40 {
        format!("{}...", &s[..40])
    } else {
        s.to_string()
    }
}

fn normalize(s: &str) -> String {
    s.trim_start_matches('\u{feff}').replace("\r\n", "\n")
}

#[async_trait]
impl Tool for Edit {
    fn name(&self) -> &str {
        "edit"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "edits": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": { "old": { "type": "string" }, "new": { "type": "string" } },
                        "required": ["old", "new"]
                    }
                }
            },
            "required": ["path", "edits"]
        })
    }

    async fn prepare(&self, call: &ToolCall, ctx: &ToolCtx) -> Result<Prepared, Reject> {
        let (path, edits) = parse_edits(&call.args)?;
        let p = Path::new(&path);
        let p = if p.is_absolute() {
            p.to_path_buf()
        } else {
            ctx.cwd.join(p)
        };
        if !ctx
            .engine
            .path_writable(&ctx.fs.writable_roots, &ctx.fs.read_only_within, &p)
        {
            return Err(Reject::Blocked {
                reason: format!("{path}: outside writable roots for role {}", ctx.role),
            });
        }
        // Read-before-edit: the model may only rewrite a version it has seen.
        // Both refusals are model-facing, so the loop continues and the model
        // can fix itself (read it, or re-read after an external change).
        let key = path_key(&ctx.cwd, &p);
        let current = tokio::fs::read_to_string(&p)
            .await
            .map_err(|e| Reject::RespondToModel {
                message: format!("edit: cannot read {}: {e}", p.display()),
            })?;
        ctx.reads
            .check(&key, &current)
            .map_err(|message| Reject::RespondToModel { message })?;
        Ok(Prepared(json!({
            "path": p.display().to_string(),
            "key": key.display().to_string(),
            "edits": edits.iter().map(|(o, n)| json!({"old": o, "new": n})).collect::<Vec<_>>()
        })))
    }

    async fn execute(
        &self,
        prep: Prepared,
        ctx: &ToolCtx,
        _cancel: CancellationToken,
        _update: tokio::sync::mpsc::UnboundedSender<ToolUpdate>,
    ) -> Result<ToolOutcome, ToolError> {
        let path = prep.0["path"].as_str().unwrap_or_default().to_string();
        let key = PathBuf::from(prep.0["key"].as_str().unwrap_or_default());
        let edits: Vec<(String, String)> = prep.0["edits"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|e| {
                        (
                            e["old"].as_str().unwrap_or_default().into(),
                            e["new"].as_str().unwrap_or_default().into(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        let original = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| ToolError::Io(format!("{path}: {e}")))?;
        // Re-check against what prepare validated: another call in the same
        // batch may have rewritten the file in between (or the user did).
        ctx.reads.check(&key, &original).map_err(ToolError::Io)?;

        let (applied, regions) = match apply_edits(&original, &edits) {
            Ok(r) => r,
            Err(first_err) => {
                // Normalization fallback: BOM/CRLF differences between model
                // output and file; rewrite the file normalized (pi edit-diff).
                let norm_content = normalize(&original);
                let norm_edits: Vec<(String, String)> = edits
                    .iter()
                    .map(|(o, n)| (normalize(o), normalize(n)))
                    .collect();
                match apply_edits(&norm_content, &norm_edits) {
                    Ok(r) => r,
                    Err(_) => return Err(ToolError::Io(first_err)),
                }
            }
        };

        tokio::fs::write(&path, &applied)
            .await
            .map_err(|e| ToolError::Io(format!("{path}: {e}")))?;
        ctx.reads.record(key, &applied);
        Ok(ToolOutcome::text(format!(
            "applied {} edit(s) to {path} ({} -> {} bytes){}",
            edits.len(),
            original.len(),
            applied.len(),
            echo(&applied, &regions)
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(o, n)| (o.to_string(), n.to_string()))
            .collect()
    }

    #[test]
    fn multi_edit_sequential() {
        let content = "alpha\nbeta\ngamma\n";
        let (out, applied) =
            apply_edits(content, &pairs(&[("alpha", "ALPHA"), ("gamma", "GAMMA")])).unwrap();
        assert_eq!(out, "ALPHA\nbeta\nGAMMA\n");
        assert_eq!(applied.len(), 2);
        assert_eq!(&out[applied[0].start..applied[0].end], "ALPHA");
        assert_eq!(&out[applied[1].start..applied[1].end], "GAMMA");
    }

    #[test]
    fn overlap_rejected_via_cursor() {
        let content = "hello world";
        // second edit's old only exists inside the first edit's replaced range
        let err = apply_edits(
            content,
            &pairs(&[("hello world", "goodbye"), ("lo wo", "XX")]),
        )
        .unwrap_err();
        assert!(err.contains("not found"), "{err}");
    }

    #[test]
    fn ambiguous_old_is_rejected_not_guessed() {
        let content = "let x = 1;\nlet x = 1;\nlet y = 2;\n";
        let err = apply_edits(content, &pairs(&[("let x = 1;", "let x = 9;")])).unwrap_err();
        assert!(err.contains("not unique"), "{err}");
        assert!(err.contains("2 matches"), "{err}");
        // disambiguated with context: same intent, now unique
        let (out, _) = apply_edits(
            content,
            &pairs(&[("let x = 1;\nlet y = 2;", "let x = 9;\nlet y = 2;")]),
        )
        .unwrap();
        assert!(out.contains("let x = 9;"));
    }

    #[test]
    fn uniqueness_is_scoped_to_the_cursor() {
        // `dup` appears twice in the file but once after the cursor: allowed.
        let (out, _) = apply_edits(
            "dup\nmid\ndup\n",
            &pairs(&[("dup\nmid", "MID"), ("dup", "DUP")]),
        )
        .unwrap();
        assert_eq!(out, "MID\nDUP\n");
    }

    #[test]
    fn anchors_resolve_against_the_original_not_the_partial_result() {
        // Edit 2 targets text edit 1 produced: not found, and the message says
        // why instead of leaving the model to guess.
        let err = apply_edits(
            "one();\n",
            &pairs(&[("one();", "two();"), ("two();", "three();")]),
        )
        .unwrap_err();
        assert!(err.contains("not found"), "{err}");
        assert!(
            err.contains("an earlier edit in this batch produced"),
            "{err}"
        );

        // Edit 2 points behind the cursor: also named.
        let err = apply_edits(
            "a;\nkeep;\nb;\n",
            &pairs(&[("keep;\nb;\n", "KEEP;\nB;\n"), ("a;", "A;")]),
        )
        .unwrap_err();
        assert!(
            err.contains("sits before an earlier edit's region"),
            "{err}"
        );
    }

    #[test]
    fn crlf_bom_fallback() {
        let content = "\u{feff}fn a() {\r\n    x();\r\n}\r\n";
        let edits = pairs(&[("fn a() {\n    x();\n}", "fn a() {\n    y();\n}")]);
        let out = apply_edits(content, &edits).unwrap_err(); // exact fails on CRLF/BOM
        assert!(out.contains("not found"));
        // normalized path succeeds
        let norm = normalize(content);
        let norm_edits: Vec<(String, String)> = edits
            .iter()
            .map(|(o, n)| (normalize(o), normalize(n)))
            .collect();
        let (out2, _) = apply_edits(&norm, &norm_edits).unwrap();
        assert!(out2.contains("fn y") || out2.contains("y();"));
    }

    #[test]
    fn empty_old_rejected() {
        let err = apply_edits("abc", &pairs(&[("", "x")])).unwrap_err();
        assert!(err.contains("empty"), "{err}");
    }

    #[test]
    fn echo_reports_line_number_with_context() {
        let content = "a\nb\nc\nd\ne\n";
        let (out, applied) = apply_edits(content, &pairs(&[("c", "C")])).unwrap();
        let e = echo(&out, &applied);
        assert!(e.contains("--- edit 1 (line 3) ---"), "{e}");
        assert!(e.contains("     3| C"), "{e}");
        assert!(e.contains("     2| b"), "{e}"); // context above
        assert!(e.contains("     4| d"), "{e}"); // context below
    }

    #[test]
    fn echo_elides_a_long_body() {
        let body: String = (0..40).map(|i| format!("// line {i}\n")).collect();
        let (out, applied) = apply_edits("head\n", &pairs(&[("head\n", body.as_str())])).unwrap();
        let e = echo(&out, &applied);
        assert!(e.contains("// line 0"), "{e}");
        assert!(e.contains("// line 39"), "{e}");
        assert!(e.contains("..."), "{e}");
        assert!(!e.contains("// line 20"), "middle should be elided: {e}");
    }

    #[test]
    fn echo_is_capped_on_a_wide_batch() {
        // The cap matters when many regions each carry context, not when one
        // region is long (that case is handled by elision).
        let content: String = (0..200).map(|i| format!("line-{i}\n")).collect();
        let edits: Vec<(String, String)> = (0..100)
            .map(|i| (format!("line-{i}\n"), format!("line-{i}-touched\n")))
            .collect();
        let (out, applied) = apply_edits(&content, &edits).unwrap();
        assert_eq!(applied.len(), 100);
        let e = echo(&out, &applied);
        assert!(e.contains("echo truncated"), "{e}");
        assert!(
            e.len() <= MAX_ECHO_BYTES + 128,
            "echo not capped: {}",
            e.len()
        );
    }
}
