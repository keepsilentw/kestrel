//! read tool: file content reader (reads are unrestricted in v1; the
//! sandbox also grants broad file-read*).

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::read_state::path_key;
use super::{Prepared, Reject, Tool, ToolCall, ToolCtx, ToolError, ToolOutcome, ToolUpdate};

const MAX_BYTES: u64 = 256 * 1024;

pub struct Read;

#[async_trait]
impl Tool for Read {
    fn name(&self) -> &str {
        "read"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "file path to read" }
            },
            "required": ["path"]
        })
    }

    async fn prepare(&self, call: &ToolCall, _ctx: &ToolCtx) -> Result<Prepared, Reject> {
        let Some(path) = call.args["path"].as_str() else {
            return Err(Reject::RespondToModel {
                message: "read requires a string `path`".into(),
            });
        };
        Ok(Prepared(json!({ "path": path })))
    }

    async fn execute(
        &self,
        prep: Prepared,
        ctx: &ToolCtx,
        _cancel: CancellationToken,
        _update: tokio::sync::mpsc::UnboundedSender<ToolUpdate>,
    ) -> Result<ToolOutcome, ToolError> {
        let Some(path) = prep.0["path"].as_str() else {
            return Err(ToolError::Io("prepared path missing".into()));
        };
        let meta = tokio::fs::metadata(path)
            .await
            .map_err(|e| ToolError::Io(format!("{}: {e}", path)))?;
        if meta.len() > MAX_BYTES {
            return Err(ToolError::Io(format!(
                "{path}: file larger than {MAX_BYTES} bytes"
            )));
        }
        let text = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| ToolError::Io(format!("{}: {e}", path)))?;
        // The model now holds this version: edits may be based on it.
        ctx.reads
            .record(path_key(&ctx.cwd, std::path::Path::new(path)), &text);
        Ok(ToolOutcome::text(text))
    }
}
