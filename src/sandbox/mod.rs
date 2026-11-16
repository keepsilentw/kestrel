//! Sandbox layer: Seatbelt on macOS and bubblewrap on Linux, using the same
//! filesystem policy seam.

pub mod bubblewrap;
pub mod denial;
pub mod seatbelt;
pub use seatbelt::Seatbelt;

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};

/// Filesystem view granted to a sandboxed child.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FsPolicy {
    #[serde(default)]
    pub writable_roots: Vec<PathBuf>,
    /// Read-only islands INSIDE writable roots (e.g. `<worktree>/.git`).
    #[serde(default)]
    pub read_only_within: Vec<PathBuf>,
    #[serde(default)]
    pub network: NetMode,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetMode {
    #[default]
    None,
    /// Only outbound to these localhost ports (local egress proxy).
    ProxyPorts(Vec<u16>),
}

/// The fully-resolved command a tool wants to run.
#[derive(Clone, Debug)]
pub struct SpawnCmd {
    pub argv: Vec<String>,
    pub cwd: PathBuf,
}

/// Sandbox implementations rewrite the argv into a wrapped form.
pub trait Sandbox: Send + Sync {
    fn wrap(&self, cmd: &SpawnCmd, fs: &FsPolicy) -> SpawnCmd;
}

/// No-op wrapper used for escalations ("脱沙箱重试") and non-macOS builds.
pub struct Unsandboxed;

impl Sandbox for Unsandboxed {
    fn wrap(&self, cmd: &SpawnCmd, _fs: &FsPolicy) -> SpawnCmd {
        cmd.clone()
    }
}

pub fn platform_sandbox() -> std::sync::Arc<dyn Sandbox> {
    static SANDBOX: OnceLock<Arc<dyn Sandbox>> = OnceLock::new();
    SANDBOX
        .get_or_init(|| {
            #[cfg(target_os = "macos")]
            {
                Arc::new(seatbelt::Seatbelt)
            }
            #[cfg(target_os = "linux")]
            {
                if bubblewrap::Bubblewrap::available() {
                    Arc::new(bubblewrap::Bubblewrap)
                } else {
                    tracing::warn!(
                        "bubblewrap (bwrap) is unavailable; Linux commands fail closed until approved"
                    );
                    Arc::new(bubblewrap::FailClosedLinux)
                }
            }
            #[cfg(not(any(target_os = "macos", target_os = "linux")))]
            {
                Arc::new(Unsandboxed)
            }
        })
        .clone()
}
