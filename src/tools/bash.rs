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

#[async_trait]
impl Tool for Bash {
    fn name(&self) -> &str {
        "bash"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "command": { "type": "string", "description": "shell command to run" }
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
            _ => Ok(Prepared(json!({ "command": cmd }))),
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
            argv: vec!["/bin/bash".into(), "-c".into(), cmd],
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
            let mut buf = Vec::new();
            if let Some(p) = stdout_pipe.as_mut() {
                let _ = p.read_to_end(&mut buf).await;
            }
            buf
        }));
        let read_err = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(p) = stderr_pipe.as_mut() {
                let _ = p.read_to_end(&mut buf).await;
            }
            buf
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
        let stdout = String::from_utf8_lossy(&read_out.await.unwrap_or_default()).into_owned();
        let stderr = String::from_utf8_lossy(&read_err.await.unwrap_or_default()).into_owned();

        let exit = ExitStatus {
            code: status.code(),
            signal: std::os::unix::process::ExitStatusExt::signal(&status),
        };
        if !status.success() {
            // Sandbox refusal -> escalate via the orchestrator (not a plain failure).
            if !ctx.unsandboxed && denial::looks_like_sandbox_denied(exit, &stderr) {
                return Err(ToolError::SandboxDenied(stderr.trim().to_string()));
            }
            return Ok(ToolOutcome {
                content: vec![crate::provider::wire::ToolContent::Text {
                    text: format!("exit {exit:?}\nstdout:\n{stdout}\nstderr:\n{stderr}"),
                }],
                is_error: true,
                terminate: false,
            });
        }
        Ok(ToolOutcome::text(format!("{stdout}{stderr}")))
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
