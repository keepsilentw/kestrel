//! bash tool: policy-gated, sandbox-wrapped shell execution with process-group
//! kill on cancel and sandbox-denial detection feeding the escalation loop.

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use super::{Prepared, Reject, Tool, ToolCall, ToolCtx, ToolError, ToolOutcome, ToolUpdate};
use crate::sandbox::SpawnCmd;
use crate::sandbox::denial::{self, ExitStatus};

pub struct Bash;

/// Per-stream ceiling on captured bytes. A command may write gigabytes; the
/// kernel keeps the head, drains the rest without storing it (so the child never
/// blocks on a full pipe), and says out loud that it truncated. This bounds what
/// lands in the tool result, the session tree and the next request — the RSS
/// guard can only react after the allocation, never prevent it.
const MAX_CAPTURED_BYTES: usize = 1024 * 1024;

/// Read at most `cap` bytes into memory while draining the pipe to EOF.
/// Returns `(kept, total_written)`: `total_written > kept.len()` means truncated.
async fn capture_capped<R>(mut pipe: R, cap: usize) -> (Vec<u8>, u64)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut kept = Vec::new();
    let mut total: u64 = 0;
    let mut buf = [0u8; 16 * 1024];
    loop {
        match pipe.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                total = total.saturating_add(n as u64);
                let room = cap.saturating_sub(kept.len());
                if room > 0 {
                    kept.extend_from_slice(&buf[..n.min(room)]);
                }
            }
            Err(_) => break,
        }
    }
    (kept, total)
}

/// Turn the captured head into the text the model sees, appending an explicit
/// marker when bytes were dropped. A silent cut would read like a short log.
fn capped_text(bytes: Vec<u8>, total: u64) -> String {
    let truncated = total > bytes.len() as u64;
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    if truncated {
        text.push_str(&format!(
            "\n[output truncated: kept the first {} of {} bytes]",
            bytes.len(),
            total
        ));
    }
    text
}

#[async_trait]
impl Tool for Bash {
    fn name(&self) -> &str {
        "bash"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "command": { "type": "string", "description": "shell command to run" },
                "output_path": {
                    "type": "string",
                    "description": "optional ABSOLUTE path inside your writable roots: the kernel writes this run's stdout+stderr (plus the exit line) there. Use it to land a verifiable test log — do NOT write `> file` in the command, that is not a command the permission engine can judge."
                }
            },
            "required": ["command"]
        })
    }

    fn mode(&self) -> super::ExecMode {
        super::ExecMode::Sequential // bash writes state; exclusive gate
    }

    async fn prepare(&self, call: &ToolCall, ctx: &ToolCtx) -> Result<Prepared, Reject> {
        let Some(cmd) = call.args["command"].as_str() else {
            return Err(Reject::RespondToModel {
                message: "bash requires a string `command`".into(),
            });
        };
        // `output_path` is I/O plumbing, not part of the command the approver
        // judges — which is why it is a tool argument and not shell redirection:
        // `pytest -q > log` fails the tokenizer, and fail-closed turns that into
        // Ask -> deny, so a worker could never land a test log (design §9.32④).
        // It is checked with the *write* rule: inside a writable root, outside
        // any read-only island. Out-of-bounds is Blocked, not asked: approval
        // cannot widen a role's write scope (that's the sandbox's job).
        let output_path = match call.args.get("output_path") {
            None | Some(Value::Null) => None,
            Some(Value::String(p)) if p.trim().is_empty() => None,
            Some(Value::String(p)) => {
                let raw = std::path::Path::new(p);
                let p = if raw.is_absolute() {
                    raw.to_path_buf()
                } else {
                    ctx.cwd.join(raw)
                };
                if !ctx
                    .engine
                    .path_writable(&ctx.fs.writable_roots, &ctx.fs.read_only_within, &p)
                {
                    return Err(Reject::Blocked {
                        reason: format!(
                            "bash output_path {p:?} is outside writable roots for role {}",
                            ctx.role
                        ),
                    });
                }
                Some(p)
            }
            Some(other) => {
                return Err(Reject::RespondToModel {
                    message: format!("bash `output_path` must be a string path, got {other}"),
                });
            }
        };
        // Policy gate: Deny -> Blocked; Ask -> reverse approval (or headless
        // resolution inside the orchestrator); Allow -> proceed. On the
        // post-approval re-prepare pass the Ask has already been granted.
        match ctx.engine.evaluate_command(cmd) {
            crate::permissions::Decision::Deny => Err(Reject::Blocked {
                reason: format!("command denied by policy: {cmd}"),
            }),
            crate::permissions::Decision::Ask if !ctx.reprepare_approved => {
                Err(Reject::NeedsApproval {
                    prompt: format!("run command?\n  {cmd}"),
                    detail: json!({ "command": cmd }),
                })
            }
            _ => Ok(Prepared(json!({
                "command": cmd,
                "output_path": output_path.map(|p| p.display().to_string()),
            }))),
        }
    }

    async fn execute(
        &self,
        prep: Prepared,
        ctx: &ToolCtx,
        cancel: CancellationToken,
        _update: tokio::sync::mpsc::UnboundedSender<ToolUpdate>,
    ) -> Result<ToolOutcome, ToolError> {
        let Some(cmd) = prep.0["command"].as_str().map(str::to_string) else {
            return Err(ToolError::Io("prepared command missing".into()));
        };
        // Defense in depth: policy must have allowed this in prepare (or the
        // orchestrator granted approval on the re-prepare pass).
        if !ctx.reprepare_approved
            && ctx.engine.evaluate_command(&cmd) != crate::permissions::Decision::Allow
        {
            return Err(ToolError::Io(
                "command not allowed (policy changed mid-flight)".into(),
            ));
        }

        let spawn = SpawnCmd {
            // `cmd` stays alive after the spawn: the landed log quotes it, so the
            // file says which command produced it (a log without a command line is
            // not checkable evidence).
            argv: vec!["/bin/bash".into(), "-c".into(), cmd.clone()],
            cwd: ctx.cwd.clone(),
        };
        let wrapped = if ctx.unsandboxed {
            spawn
        } else {
            ctx.sandbox.wrap(&spawn, &ctx.fs)
        };

        let mut command = tokio::process::Command::new(&wrapped.argv[0]);
        command
            .args(&wrapped.argv[1..])
            .current_dir(&wrapped.cwd)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .process_group(0) // own group: cancel kills grandchildren too
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|e| ToolError::Io(e.to_string()))?;
        let pid = child.id();
        let mut stdout_pipe = child.stdout.take();
        let mut stderr_pipe = child.stderr.take();
        let read_out = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            match stdout_pipe.take() {
                Some(pipe) => capture_capped(pipe, MAX_CAPTURED_BYTES).await,
                None => (Vec::new(), 0),
            }
        }));
        let read_err = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            match stderr_pipe.take() {
                Some(pipe) => capture_capped(pipe, MAX_CAPTURED_BYTES).await,
                None => (Vec::new(), 0),
            }
        }));

        let status = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                kill_group(pid);
                let _ = child.wait().await; // reap after group kill
                return Err(ToolError::Cancelled);
            }
            st = child.wait() => st,
        };
        let status = status.map_err(|e| ToolError::Io(e.to_string()))?;
        let (stdout_bytes, stdout_total) = read_out.await.unwrap_or_default();
        let (stderr_bytes, stderr_total) = read_err.await.unwrap_or_default();
        let stdout = capped_text(stdout_bytes, stdout_total);
        let stderr = capped_text(stderr_bytes, stderr_total);

        let exit = ExitStatus {
            code: status.code(),
            signal: std::os::unix::process::ExitStatusExt::signal(&status),
        };
        // Land the output *before* judging it: the run that failed is the one
        // whose log you want. Parent dirs are not created (same rule as the
        // write tool — fail loudly instead of inventing a tree).
        let log_note = match prep.0.get("output_path").and_then(Value::as_str) {
            None => String::new(),
            Some(path) => {
                let log = format!(
                    "command: {cmd}\nexit: {exit:?}\n--- stdout ---\n{stdout}--- stderr ---\n{stderr}"
                );
                match tokio::fs::write(path, log).await {
                    Ok(()) => format!("\n[output written to {path}]"),
                    Err(e) => format!(
                        "\n[output NOT written to {path}: {e} — the file your evidence \
                         points at does not exist]"
                    ),
                }
            }
        };
        if !status.success() {
            // Sandbox refusal -> escalate via the orchestrator (not a plain failure).
            if !ctx.unsandboxed && denial::looks_like_sandbox_denied(exit, &stderr) {
                return Err(ToolError::SandboxDenied(stderr.trim().to_string()));
            }
            return Ok(ToolOutcome {
                content: vec![crate::provider::wire::ToolContent::Text {
                    text: format!("exit {exit:?}\nstdout:\n{stdout}\nstderr:\n{stderr}{log_note}"),
                }],
                is_error: true,
                terminate: false,
            });
        }
        Ok(ToolOutcome::text(format!("{stdout}{stderr}{log_note}")))
    }
}

fn kill_group(pid: Option<u32>) {
    if let Some(pid) = pid {
        // negative pid = process group (we created one via process_group(0))
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
}
