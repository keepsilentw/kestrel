//! write tool: create/overwrite a file. Path must be inside a writable root
//! (permission engine fs check); runs inside the sandbox when used by bash,
//! but its own file IO is engine-side so prepare enforces the boundary.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::read_state::path_key;
use super::{Prepared, Reject, Tool, ToolCall, ToolCtx, ToolError, ToolOutcome, ToolUpdate};

pub struct Write;

#[async_trait]
impl Tool for Write {
    fn name(&self) -> &str {
        "write"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "file path to write" },
                "content": { "type": "string", "description": "full file content" }
            },
            "required": ["path", "content"]
        })
    }

    async fn prepare(&self, call: &ToolCall, ctx: &ToolCtx) -> Result<Prepared, Reject> {
        let Some(path) = call.args["path"].as_str() else {
            return Err(Reject::RespondToModel {
                message: "write requires a string `path`".into(),
            });
        };
        let Some(content) = call.args["content"].as_str() else {
            return Err(Reject::RespondToModel {
                message: "write requires a string `content`".into(),
            });
        };
        let p = std::path::Path::new(path);
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
        Ok(Prepared(
            json!({ "path": p.display().to_string(), "content": content }),
        ))
    }

    async fn execute(
        &self,
        prep: Prepared,
        ctx: &ToolCtx,
        _cancel: CancellationToken,
        _update: tokio::sync::mpsc::UnboundedSender<ToolUpdate>,
    ) -> Result<ToolOutcome, ToolError> {
        let path = prep.0["path"].as_str().unwrap_or_default().to_string();
        let content = prep.0["content"].as_str().unwrap_or_default();
        // Parent dir is NOT created explicitly: fail loudly instead.
        tokio::fs::write(&path, content)
            .await
            .map_err(|e| ToolError::Io(format!("{path}: {e}")))?;
        // The model authored this version, so it counts as seen.
        ctx.reads
            .record(path_key(&ctx.cwd, std::path::Path::new(&path)), content);
        Ok(ToolOutcome::text(format!(
            "wrote {} bytes to {path}",
            content.len()
        )))
    }
}

#[allow(unused)]
fn _typecheck(_: Arc<dyn Tool>) {}
