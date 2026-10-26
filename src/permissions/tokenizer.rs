//! Conservative word-only command tokenizer (codex shell-command discipline):
//! accepts plain words, single/double-quoted literals and the safe connectors
//! `&& || |`; ANYTHING else (redirection, `$`, backticks, subshells, `;`,
//! newlines, unbalanced quotes, escapes at end) fails the whole parse —
//! fail-closed, no interpretation, no guessing (tree-sitter-bash is phase 2).

/// Split a command line into word-list segments (split on `&&`/`||`/`|`).
/// `None` = unparseable under the conservative grammar -> caller must Ask.
pub fn split_segments(cmd: &str) -> Option<Vec<Vec<String>>> {
    let mut segments: Vec<Vec<String>> = vec![Vec::new()];
    let mut cur = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = cmd.chars().peekable();

    fn flush(cur: &mut String, segments: &mut [Vec<String>]) {
        if !cur.is_empty() {
            segments.last_mut().unwrap().push(std::mem::take(cur));
        }
    }

    while let Some(c) = chars.next() {
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '$' if !in_single => return None, // expansion, also inside double quotes
            '`' => return None,
            '\\' => {
                if in_single {
                    cur.push('\\');
                } else {
                    // dangling escape -> unparseable, fail closed
                    cur.push(chars.next()?);
                }
            }
            '|' if !in_single && !in_double => {
                flush(&mut cur, &mut segments);
                if chars.peek() == Some(&'|') {
                    chars.next();
                }
                segments.push(Vec::new());
            }
            '&' if !in_single && !in_double => {
                if chars.peek() == Some(&'&') {
                    chars.next();
                    flush(&mut cur, &mut segments);
                    segments.push(Vec::new());
                } else {
                    return None; // background job, not in the safe grammar
                }
            }
            ';' | '<' | '>' | '(' | ')' | '\n' if !in_single && !in_double => return None,
            c if c.is_whitespace() && !in_single && !in_double => {
                flush(&mut cur, &mut segments);
            }
            c => cur.push(c),
        }
    }
    if in_single || in_double {
        return None; // unbalanced quotes
    }
    flush(&mut cur, &mut segments);
    let nonempty: Vec<Vec<String>> = segments.into_iter().filter(|s| !s.is_empty()).collect();
    if nonempty.is_empty() {
        None
    } else {
        Some(nonempty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_words_ok() {
        assert_eq!(
            split_segments(Some("rg foo bar").into_iter().collect::<String>().as_str()),
            Some(vec![vec!["rg".into(), "foo".into(), "bar".into()]])
        );
    }

    #[test]
    fn single_quoted_literal_ok() {
        assert_eq!(
            split_segments("grep 'a|b' file"),
            Some(vec![vec!["grep".into(), "a|b".into(), "file".into()]])
        );
    }

    #[test]
    fn connectors_split_segments() {
        assert_eq!(
            split_segments("git add . && git commit -m 'msg'"),
            Some(vec![
                vec!["git".into(), "add".into(), ".".into()],
                vec!["git".into(), "commit".into(), "-m".into(), "msg".into()],
            ])
        );
    }

    #[test]
    fn unsafe_constructs_fail() {
        for cmd in [
            "echo $HOME",
            "echo `id`",
            "cat a > b",
            "cat < b",
            "echo $(id)",
            "a; b",
            "sleep 1 &",
            "(cd x; ls)",
            "echo 'unbalanced",
            "echo \"unbalanced",
            "echo \"has $var inside\"",
            "\\",
        ] {
            assert!(split_segments(cmd).is_none(), "expected None for {cmd:?}");
        }
    }
}
