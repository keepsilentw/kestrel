//! The evidence contract between a worker's answer and the SA's verdict.
//!
//! A text verdict is a CLAIM; the files it points at are the evidence. `serve`
//! lifts the claims out of the answer and writes them into the artifact, then
//! `demo-sa` verifies them on disk — design §10 二期「判定从工件文字升级到
//! 工件 + 证据文件」：`PASS` 不再自证，它必须指出盘上可查的东西。
//!
//! 这一层再加一格（§10 最后一条新边界）：**证据有类型**。指向一个文件只证明「这里有
//! 东西」；声称「这是测试日志」则多了一个可以机械核对的属性——日志里应当有测试汇总行，
//! 声称 PNG 的文件开头应当真是 PNG 的魔数。类型把「我看过」变成「这句话能在盘上被
//! 反驳」，也正是它能被 stage 要求的原因：`test` 这一关必须有一份可核对的测试日志。

use std::path::Path;

/// 证据类型。`File` 是「只承诺存在且有内容」的那一档（也就是升级前的全部语义），
/// 其余每档都比它多一个可机械核对的属性。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum EvidenceKind {
    /// 存在 + 非空（裸路径的缺省类型）。
    File,
    /// 测试/构建日志：UTF-8 文本，且含可识别的汇总行（cargo / pytest / jest / go test）。
    TestLog,
    /// 统一 diffs：`diff --git` 或 `--- / +++ / @@` 的组合。
    Diff,
    /// 截图：文件头是真的图片魔数，且大小不像占位。
    Screenshot,
    /// 不认识的类型声明。不猜：判定层把它当成一条 issue（fail-closed）。
    Unknown,
}

impl EvidenceKind {
    /// 线名（写进工件 JSON、stage 要求表里也用这个名字）。
    pub fn as_str(&self) -> &'static str {
        match self {
            EvidenceKind::File => "file",
            EvidenceKind::TestLog => "test_log",
            EvidenceKind::Diff => "diff",
            EvidenceKind::Screenshot => "screenshot",
            EvidenceKind::Unknown => "unknown",
        }
    }

    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "file" | "path" | "artifact" => EvidenceKind::File,
            "test_log" | "testlog" | "log" => EvidenceKind::TestLog,
            "diff" | "patch" => EvidenceKind::Diff,
            "screenshot" | "shot" | "image" | "png" => EvidenceKind::Screenshot,
            _ => EvidenceKind::Unknown,
        }
    }

    /// 未知类型无法核对，也就不能算证据。
    pub fn is_verifiable(&self) -> bool {
        *self != EvidenceKind::Unknown
    }
}

/// 一条证据声明：路径 + 声称的类型。
#[derive(Clone, Debug, PartialEq)]
pub struct Evidence {
    pub path: String,
    pub kind: EvidenceKind,
}

impl Evidence {
    pub fn file(path: impl Into<String>) -> Self {
        Evidence {
            path: path.into(),
            kind: EvidenceKind::File,
        }
    }
}

/// 写进工件 JSON 的形状：`{"path": ..., "kind": ...}`。
/// 手写而不是 derive，是为了让线名只有一个来源（`as_str`）——读侧的
/// `EvidenceKind::parse` 与写侧都走它，derive 会造出第二份定义。
impl serde::Serialize for Evidence {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut out = serializer.serialize_struct("Evidence", 2)?;
        out.serialize_field("path", &self.path)?;
        out.serialize_field("kind", self.kind.as_str())?;
        out.end()
    }
}

/// Cap on judged paths per artifact: a model that dumps a whole directory
/// listing must not turn one verdict into a filesystem walk.
pub const MAX_EVIDENCE_PATHS: usize = 12;

/// Lift evidence out of a worker's final answer.
///
/// 1. a typed line wins: `EVIDENCE test_log /tmp/wt/pytest.log`（`EVIDENCE <kind>
///    <path>`，也接受 `evidence: a, b` 这种旧写法 = `file`）；
/// 2. 没有契约行时退回收集答案里的绝对路径，一律按 `file` 处理——SA 在盘上核对，
///    猜错路径的代价是回派，不是假通过。
pub fn evidence_from_answer(text: &str) -> Vec<Evidence> {
    let mut explicit: Vec<Evidence> = Vec::new();
    for line in text.lines() {
        let trimmed = line
            .trim()
            .trim_start_matches(['-', '*', '#', '>', ' '])
            .trim_start_matches('*')
            .trim();
        let Some(rest) =
            strip_prefix_ci(trimmed, "evidence:").or_else(|| strip_prefix_ci(trimmed, "evidence "))
        else {
            continue;
        };
        explicit.extend(parse_evidence_span(rest));
    }
    let picked = if explicit.is_empty() {
        path_tokens(text).into_iter().map(Evidence::file).collect()
    } else {
        explicit
    };
    dedup_cap(picked)
}

/// 一行契约的正文：`test_log /tmp/a.log /tmp/b.log` 或 `/tmp/a.py, /tmp/b.py`。
/// 首个 token 是认识的类型名 => 整行按该类型；否则整行都是路径（旧写法）。
fn parse_evidence_span(rest: &str) -> Vec<Evidence> {
    // 首个 token 与其余部分按第一个空白切开：head 被剥过反引号/逗号，它的长度和
    // rest 里的位置对不上，用字节偏移会切进下一个 token。
    let (head_raw, tail) = match rest.split_once(char::is_whitespace) {
        Some((head, tail)) => (head, tail),
        None => (rest, ""),
    };
    let head = head_raw.trim_matches(|c: char| "`*,".contains(c));
    if !head.is_empty() && !head.starts_with('/') {
        let kind = EvidenceKind::parse(head);
        if kind != EvidenceKind::Unknown {
            return path_tokens(tail)
                .into_iter()
                .map(|path| Evidence { path, kind })
                .collect();
        }
    }
    path_tokens(rest).into_iter().map(Evidence::file).collect()
}

/// 一次核对的产出：不能成立的条目 + **确实成立的那些类型**（stage 要求要看后者，
/// 不能只看「有条目没报错」——一个 `file` 档的证据永远满足不了 `test_log` 的要求）。
#[derive(Debug, Default, PartialEq)]
pub struct EvidenceReport {
    pub issues: Vec<String>,
    pub verified_kinds: Vec<EvidenceKind>,
}

/// 按类型核对证据。`issues` 为空意味着每一条都按它**声称的类型**在盘上成立。
pub fn check_evidence(items: &[Evidence]) -> EvidenceReport {
    let mut report = EvidenceReport::default();
    for item in items.iter().take(MAX_EVIDENCE_PATHS) {
        let path = Path::new(&item.path);
        let Ok(md) = std::fs::metadata(path) else {
            report
                .issues
                .push(format!("evidence missing: {}", item.path));
            continue;
        };
        if md.is_dir() {
            // 目录只能证明「这里有东西」：它承载不了任何更强的类型声明
            if item.kind != EvidenceKind::File {
                report.issues.push(format!(
                    "evidence {} is a directory, cannot be a {}",
                    item.path,
                    item.kind.as_str()
                ));
                continue;
            }
            if std::fs::read_dir(path).is_ok_and(|mut d| d.next().is_none()) {
                report
                    .issues
                    .push(format!("evidence dir is empty: {}", item.path));
            } else {
                report.verified_kinds.push(item.kind);
            }
            continue;
        }
        if md.len() == 0 {
            report
                .issues
                .push(format!("evidence file is empty: {}", item.path));
            continue;
        }
        let kind_issue = match item.kind {
            EvidenceKind::File => None,
            EvidenceKind::TestLog => match read_tail(path, SPAN_BYTES) {
                Ok(bytes) => {
                    let text = String::from_utf8_lossy(&bytes);
                    if find_marker(&text, TEST_SUMMARY_MARKERS).is_some() {
                        None
                    } else {
                        Some(format!(
                            "evidence {} is not a test log (no summary line: {})",
                            item.path,
                            marker_list(TEST_SUMMARY_MARKERS)
                        ))
                    }
                }
                Err(e) => Some(format!("evidence {} unreadable: {e}", item.path)),
            },
            EvidenceKind::Diff => match read_head(path, SPAN_BYTES) {
                Ok(bytes) => {
                    let text = String::from_utf8_lossy(&bytes);
                    if looks_like_diff(&text) {
                        None
                    } else {
                        Some(format!(
                            "evidence {} is not a diff (no `diff --git` or `--- / +++ / @@` markers)",
                            item.path
                        ))
                    }
                }
                Err(e) => Some(format!("evidence {} unreadable: {e}", item.path)),
            },
            EvidenceKind::Screenshot => match read_head(path, 16) {
                Ok(bytes) if image_format(&bytes).is_some() => {
                    if md.len() < MIN_IMAGE_BYTES {
                        Some(format!(
                            "evidence {} is too small to be a screenshot ({} byte(s))",
                            item.path,
                            md.len()
                        ))
                    } else {
                        None
                    }
                }
                Ok(bytes) => Some(format!(
                    "evidence {} is not the claimed image: header {} is not PNG/JPEG/GIF/WebP",
                    item.path,
                    hex_preview(&bytes)
                )),
                Err(e) => Some(format!("evidence {} unreadable: {e}", item.path)),
            },
            // 认不出来的类型声明不猜：它既不是 file 档（worker 没承诺存在性之外的东西
            // 吗？承诺了别的），也不是任何可核对的档 —— 直接算一条 issue。
            EvidenceKind::Unknown => Some(format!(
                "evidence {} claims an unknown kind (only {})",
                item.path, KNOWN_KINDS
            )),
        };
        match kind_issue {
            Some(issue) => report.issues.push(issue),
            None => report.verified_kinds.push(item.kind),
        }
    }
    report
}

/// stage 要求是否齐全：每种被要求的类型都要有一条**核对通过**的证据。
/// 返回缺的那些（空 = 齐全）。
///
/// `file` 是下界而不是并列的一档：任何一条通过更强的类型核对的证据，都已经过了
/// 「在盘上、有内容」这一关（那段检查在类型分派之前就跑完了）。把它当成并列类型的话，
/// 「要求 `file` 的表」就会拒绝一份货真价实的测试日志——而提示词正在教 worker 那么写。
pub fn missing_requirements(
    verified: &[EvidenceKind],
    required: &[EvidenceKind],
) -> Vec<EvidenceKind> {
    required
        .iter()
        .filter(|kind| {
            if **kind == EvidenceKind::File {
                verified.is_empty()
            } else {
                !verified.contains(kind)
            }
        })
        .cloned()
        .collect()
}

/// 一次读的上限：日志可能很大，汇总行在结尾，diff 的特征在开头，都只看一段。
const SPAN_BYTES: u64 = 64 * 1024;
/// 声称是截图的文件至少要这么大：带 PNG 头的 20 字节不是截图。
const MIN_IMAGE_BYTES: u64 = 1024;

const TEST_SUMMARY_MARKERS: &[&str] = &[
    "test result:", // cargo
    "passed",       // pytest / jest 的汇总行
    "failed",
    "Tests:", // jest
    "PASS:",  // pytest -k 单条
    "FAIL:",
    "BUILD SUCCESS", // maven / gradle
    "BUILD FAILURE",
    "no tests ran", // pytest 空跑——仍然是「跑过测试」的证据
];

const KNOWN_KINDS: &str = "file | test_log | diff | screenshot";

fn find_marker<'a>(text: &str, markers: &[&'a str]) -> Option<&'a str> {
    markers.iter().find(|m| text.contains(**m)).copied()
}

fn marker_list(markers: &[&str]) -> String {
    // 报错里只举几个例子，太长没人读
    format!("{} …", markers[..3].join(", "))
}

fn looks_like_diff(text: &str) -> bool {
    text.contains("diff --git")
        || (text.contains("--- ") && text.contains("+++ ") && text.contains("@@"))
}

fn image_format(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0x89, b'P', b'N', b'G']) {
        Some("png")
    } else if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        Some("jpeg")
    } else if bytes.starts_with(b"GIF8") {
        Some("gif")
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(&b"WEBP"[..]) {
        Some("webp")
    } else {
        None
    }
}

fn hex_preview(bytes: &[u8]) -> String {
    let hex: Vec<String> = bytes.iter().take(8).map(|b| format!("{b:02x}")).collect();
    if hex.is_empty() {
        "(empty)".to_string()
    } else {
        hex.join(" ")
    }
}

fn read_head(path: &Path, cap: u64) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut buf = Vec::new();
    file.by_ref().take(cap).read_to_end(&mut buf)?;
    Ok(buf)
}

/// 尾部一段：测试汇总行在结尾，从头读满 64 KiB 可能正好错过它。
fn read_tail(path: &Path, cap: u64) -> std::io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let start = len.saturating_sub(cap);
    file.seek(SeekFrom::Start(start))?;
    let mut buf = Vec::new();
    file.take(len - start).read_to_end(&mut buf)?;
    Ok(buf)
}

fn dedup_cap(items: Vec<Evidence>) -> Vec<Evidence> {
    let mut out: Vec<Evidence> = Vec::new();
    for item in items {
        // 同一路径可以以不同身份出现两次（既是 diff 又是 test_log 的情况极少，
        // 但去重按 (路径, 类型) 才不会把两条不同的承诺合成一条）
        if !out
            .iter()
            .any(|seen| seen.path == item.path && seen.kind == item.kind)
        {
            out.push(item);
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

    fn scratch(tag: &str, name: &str, body: &[u8]) -> Evidence {
        let dir = std::env::temp_dir().join(format!("kestrel-ev-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        Evidence {
            path: path.display().to_string(),
            kind: EvidenceKind::File,
        }
    }

    #[test]
    fn explicit_evidence_line_wins_over_prose() {
        let answer = "DONE: 实现完成（临时文件 /tmp/scratch.txt 已清理）\n\
                      evidence: /tmp/wt/lru.py, /tmp/wt/test_lru.py\n";
        let got = evidence_from_answer(answer);
        assert_eq!(
            got.iter()
                .map(|e| (e.path.as_str(), e.kind))
                .collect::<Vec<_>>(),
            vec![
                ("/tmp/wt/lru.py", EvidenceKind::File),
                ("/tmp/wt/test_lru.py", EvidenceKind::File),
            ]
        );
    }

    /// 类型化声明：`EVIDENCE <kind> <path...>`。这是 worker 能表达「我看过的是一
    /// 份测试日志」的唯一通道，也是 stage 要求得以核对的前提。
    #[test]
    fn typed_evidence_lines_carry_their_kind() {
        let answer = "EVIDENCE test_log /tmp/wt/pytest.log /tmp/wt/cargo.log\n\
                      evidence diff /tmp/wt/change.diff\n\
                      EVIDENCE screenshot /tmp/wt/shot.png\n";
        let got = evidence_from_answer(answer);
        assert_eq!(got.len(), 4, "{got:?}");
        assert_eq!(got[0].kind, EvidenceKind::TestLog);
        assert_eq!(got[1].path, "/tmp/wt/cargo.log");
        assert_eq!(got[2].kind, EvidenceKind::Diff);
        assert_eq!(got[3].kind, EvidenceKind::Screenshot);
    }

    /// 模型会把类型名包在 markdown 里（`` `test_log` ``）：剥掉装饰之后仍然按类型走。
    #[test]
    fn decorated_kind_names_still_parse() {
        let got = evidence_from_answer("EVIDENCE `test_log` /tmp/wt/a.log\n");
        assert_eq!(
            got.iter()
                .map(|e| (e.path.as_str(), e.kind))
                .collect::<Vec<_>>(),
            vec![("/tmp/wt/a.log", EvidenceKind::TestLog)],
            "{got:?}"
        );
    }

    /// 认不出的类型名不当成类型，也不能把整行丢掉：那串还是路径，按 `file` 收。
    #[test]
    fn unknown_kind_word_falls_back_to_paths_not_nothing() {
        let got = evidence_from_answer("evidence: wat /tmp/wt/a.py");
        assert_eq!(
            got.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(),
            vec!["/tmp/wt/a.py"],
            "{got:?}"
        );
    }

    #[test]
    fn same_path_under_two_kinds_is_two_claims() {
        let answer = "EVIDENCE test_log /tmp/wt/run.log\nEVIDENCE diff /tmp/wt/run.log\n";
        let got = evidence_from_answer(answer);
        assert_eq!(got.len(), 2, "{got:?}");
    }

    /// 类型化核对的主角：声称是测试日志，就得有测试汇总行——「文件存在」这件事
    /// 证明不了它跑过测试。
    #[test]
    fn test_log_must_actually_summarize_a_test_run() {
        let good = scratch(
            "log",
            "cargo.log",
            b"running 12 tests\ntest result: ok. 12 passed; 0 failed\n",
        );
        let mut claim = good.clone();
        claim.kind = EvidenceKind::TestLog;
        let rep = check_evidence(&[claim]);
        assert!(rep.issues.is_empty(), "{:?}", rep.issues);
        assert_eq!(rep.verified_kinds, vec![EvidenceKind::TestLog]);

        let bogus = scratch("log", "note.txt", b"DONE: everything is fine, trust me\n");
        let mut claim = bogus.clone();
        claim.kind = EvidenceKind::TestLog;
        let rep = check_evidence(&[claim]);
        assert_eq!(rep.issues.len(), 1, "{:?}", rep.issues);
        assert!(rep.issues[0].contains("not a test log"), "{:?}", rep.issues);
        assert!(rep.verified_kinds.is_empty(), "没通过就不能算这类证据");
    }

    #[test]
    fn diff_must_look_like_a_diff() {
        let real = scratch(
            "diff",
            "change.diff",
            b"diff --git a/lru.py b/lru.py\n--- a/lru.py\n+++ b/lru.py\n@@ -1,3 +1,5 @@\n",
        );
        let mut claim = real.clone();
        claim.kind = EvidenceKind::Diff;
        assert!(check_evidence(&[claim]).issues.is_empty());

        let fake = scratch("diff", "list.txt", b"lru.py\ntest_lru.py\n");
        let mut claim = fake.clone();
        claim.kind = EvidenceKind::Diff;
        let rep = check_evidence(&[claim]);
        assert_eq!(rep.issues.len(), 1, "{:?}", rep.issues);
        assert!(rep.issues[0].contains("not a diff"), "{:?}", rep.issues);
    }

    /// 截图看文件头：扩展名是谁都写得出来的，魔数才是这张图存在过的痕迹。
    #[test]
    fn screenshot_checks_the_magic_bytes_and_the_size() {
        let mut png = vec![0x89u8, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n'];
        png.extend(std::iter::repeat_n(0u8, 4096));
        let shot = scratch("shot", "real.png", &png);
        let mut claim = shot.clone();
        claim.kind = EvidenceKind::Screenshot;
        assert!(check_evidence(&[claim]).issues.is_empty());

        let forged = scratch("shot", "fake.png", b"not an image at all, but named .png\n");
        let mut claim = forged.clone();
        claim.kind = EvidenceKind::Screenshot;
        let rep = check_evidence(&[claim]);
        assert_eq!(rep.issues.len(), 1, "{:?}", rep.issues);
        assert!(
            rep.issues[0].contains("header") && rep.issues[0].contains("PNG/JPEG/GIF/WebP"),
            "{:?}",
            rep.issues
        );

        let tiny = {
            let mut bytes = vec![0x89u8, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n'];
            bytes.extend(std::iter::repeat_n(0u8, 8));
            scratch("shot", "tiny.png", &bytes)
        };
        let mut claim = tiny.clone();
        claim.kind = EvidenceKind::Screenshot;
        let rep = check_evidence(&[claim]);
        assert!(rep.issues[0].contains("too small"), "{:?}", rep.issues);
    }

    /// 认不出的类型不猜：它既过不了任何一档核对，也不能顶替 stage 的要求。
    #[test]
    fn unverifiable_kind_is_an_issue_not_a_pass() {
        let file = scratch("unknown", "a.txt", b"content");
        let mut claim = file.clone();
        claim.kind = EvidenceKind::Unknown;
        let rep = check_evidence(&[claim]);
        assert_eq!(rep.issues.len(), 1, "{:?}", rep.issues);
        assert!(rep.issues[0].contains("unknown kind"), "{:?}", rep.issues);
    }

    /// stage 要求看的是「通过的类型」，不是「声明过的类型」：一份假 PNG 不能满足
    /// `screenshot` 的要求，否则要求就退化成了措辞比赛。
    #[test]
    fn requirements_are_filled_by_verified_kinds_only() {
        let good = scratch("req", "ok.png", &{
            let mut v = vec![0x89u8, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n'];
            v.extend(std::iter::repeat_n(0u8, 2048));
            v
        });
        let mut shot = good.clone();
        shot.kind = EvidenceKind::Screenshot;
        let rep = check_evidence(&[shot]);
        let required = [EvidenceKind::Screenshot, EvidenceKind::TestLog];
        let missing = missing_requirements(&rep.verified_kinds, &required);
        assert_eq!(missing, vec![EvidenceKind::TestLog], "{rep:?}");
        assert!(missing_requirements(&[EvidenceKind::File, EvidenceKind::TestLog], &[]).is_empty());
    }

    /// 回归护栏：`file` 要求是**下界**，不是并列的一档。一份核对通过的测试日志当然
    /// 也在盘上、也有内容，所以它必须能满足 `file` 那一格——否则「要求表写 `file`」加
    /// 「提示词教 worker 写 `EVIDENCE test_log`」这两件好事凑在一起，就会把 pipeline
    /// 打进无限回派。反向不成立：普通文件顶不出更强的那一格。
    #[test]
    fn a_verified_stronger_kind_also_fills_the_file_floor() {
        let log = scratch(
            "floor",
            "cargo.log",
            b"running 1 test\ntest result: ok. 1 passed; 0 failed\n",
        );
        let mut claim = log.clone();
        claim.kind = EvidenceKind::TestLog;
        let rep = check_evidence(&[claim]);
        assert!(
            missing_requirements(&rep.verified_kinds, &[EvidenceKind::File]).is_empty(),
            "{rep:?}"
        );
        assert_eq!(
            missing_requirements(&rep.verified_kinds, &[EvidenceKind::Diff]),
            vec![EvidenceKind::Diff]
        );
        assert_eq!(
            missing_requirements(&[], &[EvidenceKind::File]),
            vec![EvidenceKind::File],
            "一条都没通过时，下界也不满足"
        );
    }

    #[test]
    fn absolute_paths_are_collected_when_no_contract_line_exists() {
        let answer = "PASS — 审阅了 /tmp/wt/src/cache.rs 与 /tmp/wt/tests/t.rs";
        let got = evidence_from_answer(answer);
        let paths: Vec<&str> = got.iter().map(|e| e.path.as_str()).collect();
        assert!(paths.contains(&"/tmp/wt/src/cache.rs"), "{got:?}");
        assert!(paths.contains(&"/tmp/wt/tests/t.rs"), "{got:?}");
        assert!(got.iter().all(|e| e.kind == EvidenceKind::File));
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
        let got = evidence_from_answer(answer);
        assert_eq!(
            got.iter()
                .map(|e| (e.path.as_str(), e.kind))
                .collect::<Vec<_>>(),
            vec![
                ("/tmp/wt/a.py", EvidenceKind::File),
                ("/tmp/wt/b.py", EvidenceKind::File),
            ]
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

        let items = [
            Evidence::file(full.display().to_string()),
            Evidence::file(empty.display().to_string()),
            Evidence::file(missing.display().to_string()),
        ];
        let issues = check_evidence(&items).issues;
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
            check_evidence(&[Evidence::file(dir.display().to_string())])
                .issues
                .is_empty(),
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

    /// 写侧（工件 JSON）与读侧（`evidence_from_artifact`）之间只有一份线名定义：
    /// 序列化出去的类型字符串必须能被 `parse` 原样读回来。
    #[test]
    fn serialized_kind_parses_back() {
        for kind in [
            EvidenceKind::File,
            EvidenceKind::TestLog,
            EvidenceKind::Diff,
            EvidenceKind::Screenshot,
        ] {
            let item = Evidence {
                path: "/tmp/wt/x".into(),
                kind,
            };
            let line = serde_json::to_string(&item).unwrap();
            let back: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert_eq!(back["kind"].as_str().unwrap(), kind.as_str(), "{line}");
            assert_eq!(EvidenceKind::parse(back["kind"].as_str().unwrap()), kind);
        }
        assert_eq!(
            serde_json::to_string(&Evidence::file("/tmp/a")).unwrap(),
            "{\"path\":\"/tmp/a\",\"kind\":\"file\"}"
        );
    }
}
