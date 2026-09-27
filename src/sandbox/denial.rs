//! Sandbox-denial heuristics (codex denial.rs semantics): exit 2/126/127 or
//! SIGSYS, or a stderr signature. Distinguishing "sandbox refused" from
//! "command failed" drives the escalation loop.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitStatus {
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

pub fn looks_like_sandbox_denied(status: ExitStatus, stderr: &str) -> bool {
    if matches!(status.signal, Some(SIGSYS)) {
        return true;
    }
    if matches!(status.code, Some(2) | Some(126) | Some(127)) {
        return true;
    }
    stderr_signatures()
        .iter()
        .any(|s| stderr.to_lowercase().contains(s))
}

const SIGSYS: i32 = 12; // macOS signal number for SIGSYS

fn stderr_signatures() -> Vec<&'static str> {
    vec![
        "operation not permitted",
        "sandbox",
        "not allowed",
        "permission denied", // weak but useful last resort
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_hit() {
        for code in [2, 126, 127] {
            assert!(looks_like_sandbox_denied(
                ExitStatus {
                    code: Some(code),
                    signal: None
                },
                ""
            ));
        }
        assert!(!looks_like_sandbox_denied(
            ExitStatus {
                code: Some(1),
                signal: None
            },
            ""
        ));
    }

    #[test]
    fn sigsys_hits() {
        assert!(looks_like_sandbox_denied(
            ExitStatus {
                code: None,
                signal: Some(SIGSYS)
            },
            ""
        ));
    }

    #[test]
    fn stderr_signature_hits() {
        assert!(looks_like_sandbox_denied(
            ExitStatus {
                code: Some(1),
                signal: None
            },
            "touch: /etc/passwd: Operation not permitted"
        ));
        assert!(!looks_like_sandbox_denied(
            ExitStatus {
                code: Some(1),
                signal: None
            },
            "no such file"
        ));
    }
}
