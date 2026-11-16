//! Linux sandbox wrapper using bubblewrap.
//!
//! The policy intentionally supports only a fully isolated network. A future
//! proxy-only mode needs a mechanism that enforces destination ports; sharing
//! the host network without that mechanism would be a silent weakening.

use std::process::Command;

use super::{FsPolicy, NetMode, Sandbox, SpawnCmd};

pub struct Bubblewrap;

impl Bubblewrap {
    pub fn available() -> bool {
        Command::new("bwrap")
            .arg("--version")
            .output()
            .is_ok_and(|output| output.status.success())
    }

    fn wrap_argv(cmd: &SpawnCmd, fs: &FsPolicy) -> SpawnCmd {
        if matches!(fs.network, NetMode::ProxyPorts(_)) {
            return fail_closed(cmd, "linux sandbox does not support proxy-only network");
        }

        let mut argv = vec![
            "bwrap".to_string(),
            "--die-with-parent".to_string(),
            "--new-session".to_string(),
            "--unshare-all".to_string(),
            "--ro-bind".to_string(),
            "/".to_string(),
            "/".to_string(),
            "--dev".to_string(),
            "/dev".to_string(),
            "--proc".to_string(),
            "/proc".to_string(),
        ];

        for root in &fs.writable_roots {
            argv.push("--bind".to_string());
            argv.push(root.to_string_lossy().into_owned());
            argv.push(root.to_string_lossy().into_owned());
        }
        for island in &fs.read_only_within {
            // A read-only bind after the writable root restores the island's
            // protection even though the parent root is writable.
            argv.push("--ro-bind-try".to_string());
            argv.push(island.to_string_lossy().into_owned());
            argv.push(island.to_string_lossy().into_owned());
        }
        argv.push("--chdir".to_string());
        argv.push(cmd.cwd.to_string_lossy().into_owned());
        argv.push("--".to_string());
        argv.extend(cmd.argv.iter().cloned());
        SpawnCmd {
            argv,
            cwd: cmd.cwd.clone(),
        }
    }
}

impl Sandbox for Bubblewrap {
    fn wrap(&self, cmd: &SpawnCmd, fs: &FsPolicy) -> SpawnCmd {
        Self::wrap_argv(cmd, fs)
    }
}

pub struct FailClosedLinux;

impl Sandbox for FailClosedLinux {
    fn wrap(&self, cmd: &SpawnCmd, _fs: &FsPolicy) -> SpawnCmd {
        fail_closed(cmd, "linux sandbox unavailable: install bubblewrap (bwrap)")
    }
}

fn fail_closed(cmd: &SpawnCmd, reason: &'static str) -> SpawnCmd {
    let script = format!("printf '%s\\n' '{reason}' >&2; exit 126");
    SpawnCmd {
        argv: vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            script,
            "kestrel-sandbox".to_string(),
        ],
        cwd: cmd.cwd.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn fs(network: NetMode) -> FsPolicy {
        FsPolicy {
            writable_roots: vec![Path::new("/work").into()],
            read_only_within: vec![Path::new("/work/.git").into()],
            network,
        }
    }

    #[test]
    fn bwrap_wraps_roots_and_islands_in_order() {
        let wrapped = Bubblewrap.wrap(
            &SpawnCmd {
                argv: vec!["touch".into(), "x".into()],
                cwd: Path::new("/work").into(),
            },
            &fs(NetMode::None),
        );
        assert_eq!(wrapped.argv[0], "bwrap");
        assert!(wrapped.argv.iter().any(|arg| arg == "--unshare-all"));
        assert!(
            wrapped
                .argv
                .windows(3)
                .any(|args| args[0] == "--bind" && args[1] == "/work" && args[2] == "/work")
        );
        assert!(
            wrapped
                .argv
                .windows(3)
                .any(|args| args[0] == "--ro-bind-try"
                    && args[1] == "/work/.git"
                    && args[2] == "/work/.git")
        );
        assert_eq!(wrapped.argv.last().unwrap(), "x");
    }

    #[test]
    fn unsupported_network_mode_fails_closed() {
        let wrapped = Bubblewrap.wrap(
            &SpawnCmd {
                argv: vec!["curl".into()],
                cwd: Path::new("/work").into(),
            },
            &fs(NetMode::ProxyPorts(vec![8080])),
        );
        assert_eq!(wrapped.argv[0], "/bin/sh");
        assert!(wrapped.argv[2].contains("proxy-only network"));
        assert!(wrapped.argv[2].contains("exit 126"));
    }
}
