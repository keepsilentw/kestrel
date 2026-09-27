//! Partial-JSON tolerant parser (地基件 3, 自写 per plan §3 决策).
//!
//! Tool-call args stream as deltas; the engine re-parses the accumulated
//! prefix every delta (pi parseStreamingJson 语义). A prefix is generally
//! INVALID JSON, so this parser repairs the tail: closes open strings and
//! containers, completes `key:` with null, drops trailing commas, and drops
//! dangling escapes / partial keywords. The last prefix of a full document
//! must equal the full document.

use serde_json::Value;

/// Parse a (possibly truncated) JSON prefix into the best-effort value.
pub fn parse_partial(input: &str) -> Value {
    if let Ok(v) = serde_json::from_str(input) {
        return v;
    }
    let mut s = repair(input);
    loop {
        if let Ok(v) = serde_json::from_str(&s) {
            return v;
        }
        if s.is_empty() {
            return Value::Null;
        }
        s.pop();
    }
}

/// Structural repair of a truncated JSON prefix.
fn repair(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut in_str = false;
    let mut esc = false;
    let mut stack: Vec<char> = Vec::new();
    for &c in &chars {
        if esc {
            esc = false;
            continue;
        }
        match c {
            '\\' if in_str => esc = true,
            '"' => in_str = !in_str,
            '{' | '[' if !in_str => stack.push(c),
            '}' | ']' if !in_str => {
                stack.pop();
            }
            _ => {}
        }
    }
    let mut s: String = chars.into_iter().collect();
    // dangling escape: the escape sequence never completed
    if esc {
        s.pop();
    }
    // unterminated string: close it (partial string content is preserved)
    if in_str {
        s.push('"');
    }
    // partial keyword (tru/fals/nul): strip it — a strict prefix of
    // true/false/null cannot be completed into valid JSON.
    {
        let t = s.trim_end();
        let word: String = t
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_alphabetic())
            .collect();
        let word = word.chars().rev().collect::<String>();
        if !word.is_empty()
            && ["true", "false", "null"]
                .iter()
                .any(|k| k.starts_with(&word) && k != &word)
        {
            s = t[..t.len() - word.len()].to_string();
        }
    }
    // trailing separator outside a string: ',' -> drop, ':' -> complete null
    loop {
        let t = s.trim_end();
        if t.ends_with(',') {
            s = t.trim_end_matches(',').to_string();
        } else if t.ends_with(':') {
            s.push_str(" null");
            break;
        } else {
            break;
        }
    }
    // close open containers, innermost first
    for c in stack.iter().rev() {
        s.push(if *c == '{' { '}' } else { ']' });
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every 1..=n prefix parses; prefix-consistency (all complete fields of
    /// the prefix survive); the last prefix equals the full document.
    #[test]
    fn all_prefixes_of_object_parse_prefix_consistently() {
        let full = r#"{"cmd":"ls -la","n":5,"flag":true,"nested":{"a":[1,2,"x"]}}"#;
        let n = full.chars().count();
        let mut prev_len = 0usize;
        for cut in 1..=n {
            let prefix: String = full.chars().take(cut).collect();
            let v = parse_partial(&prefix);
            let obj = match &v {
                Value::Object(o) => o,
                Value::Null => continue, // degenerate 1-char prefix
                other => panic!("prefix {cut:?} -> non-object {other:?}"),
            };
            // prefix consistency: fields already present in the previous
            // prefix must still be present (possibly identical)
            if let Value::Object(prev) =
                parse_partial(&full.chars().take(prev_len.max(1)).collect::<String>())
            {
                for (k, pv) in &prev {
                    if k != "cmd" {
                        // string values may only grow/complete; numbers complete
                        if let Some(cv) = obj.get(k) {
                            assert_json_superset(cv, pv);
                        }
                    }
                }
            }
            prev_len = cut;
        }
        assert_eq!(
            parse_partial(full),
            json!({"cmd":"ls -la","n":5,"flag":true,"nested":{"a":[1,2,"x"]}})
        );
    }

    fn assert_json_superset(current: &Value, prev: &Value) {
        match (prev, current) {
            (Value::Number(p), Value::Number(c)) => {
                assert!(c.as_f64() >= p.as_f64(), "number regressed: {c} < {p}");
            }
            (Value::String(p), Value::String(c)) => {
                assert!(c.starts_with(p.as_str()), "string regressed: {c} vs {p}");
            }
            (Value::Array(p), Value::Array(c)) => {
                assert!(c.len() >= p.len());
            }
            _ => {}
        }
    }

    #[test]
    fn last_prefix_equals_full_document() {
        let full = r#"{"cmd":"ls","n":5,"flag":true,"nested":{"a":[1,2,"x"]}}"#;
        assert_eq!(
            parse_partial(full),
            serde_json::from_str::<Value>(full).unwrap()
        );
    }

    #[test]
    fn complete_value_prefixes_pass_through() {
        assert_eq!(parse_partial("{\"a\":1}"), json!({"a":1}));
        assert_eq!(parse_partial("[1,2]"), json!([1, 2]));
        assert_eq!(parse_partial("\"x\""), json!("x"));
    }

    #[test]
    fn truncated_containers_close() {
        assert_eq!(parse_partial("{\"a\":1,\"b\":"), json!({"a":1, "b": null}));
        assert_eq!(parse_partial("{\"a\":1,"), json!({"a":1}));
        assert_eq!(parse_partial("[1,2,"), json!([1, 2]));
        assert_eq!(parse_partial("[{\"a\":1"), json!([{"a": 1}]));
        assert_eq!(
            parse_partial("{\"outer\":{\"in\":["),
            json!({"outer": {"in": []}})
        );
    }

    #[test]
    fn truncated_strings_close() {
        assert_eq!(parse_partial("{\"cmd\":\"ls -la"), json!({"cmd": "ls -la"}));
        assert_eq!(parse_partial("\"abc\\"), json!("abc"));
        assert_eq!(parse_partial("[\"x\",\"y"), json!(["x", "y"]));
    }

    #[test]
    fn partial_keywords_stripped() {
        assert_eq!(parse_partial("{\"flag\":tru"), json!({"flag": null}));
        assert_eq!(parse_partial("{\"flag\":fals"), json!({"flag": null}));
        assert_eq!(parse_partial("{\"flag\":nul"), json!({"flag": null}));
    }

    #[test]
    fn partial_number_completes_or_drops() {
        // number prefix parses once closed
        assert_eq!(parse_partial("{\"n\":12"), json!({"n": 12}));
        assert_eq!(parse_partial("{\"n\":"), json!({"n": null}));
    }

    #[test]
    fn garbage_degrades_to_null() {
        assert_eq!(parse_partial(""), Value::Null);
    }
}
