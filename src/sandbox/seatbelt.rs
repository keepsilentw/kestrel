//! Seatbelt (macOS sandbox-exec) wrapper.
//!
//! Profile shape mirrors codex seatbelt.rs (study-permissions §3):
//! `(deny default)` baseline + fork/exec/read exemptions, parameterized
//! writable roots, `file-write-unlink` denies on ancestor dirs and read-only
//! islands (rename-escape defense), network only via explicit loopback
//! outbound allows. Executable is trusted only at /usr/bin/sandbox-exec.

use std::fmt::Write as _;
use std::path::Path;

use super::{FsPolicy, NetMode, Sandbox, SpawnCmd};

pub struct Seatbelt;

const BASE_POLICY: &str = include_str!("policies/base_policy.sbpl");
const NETWORK_POLICY: &str = include_str!("policies/network_policy.sbpl");

impl Seatbelt {
    /// Compose the full profile text for a policy. Pure function — golden
    /// snapshot tests target this.
    pub fn compose_policy(fs: &FsPolicy) -> String {
        let mut p = String::new();
        p.push_str(BASE_POLICY);

        // Writable roots -> parameterized write grants.
        for (i, _) in fs.writable_roots.iter().enumerate() {
            let _ = writeln!(
                p,
                "(allow file-write* (subpath (param \"WRITABLE_ROOT_{i}\")))"
            );
        }
        // /dev literals always writable.
        p.push_str("(allow file-write* (literal \"/dev/null\") (literal \"/dev/stdout\") (literal \"/dev/stderr\") (literal \"/dev/tty\"))\n");

        // Rename-escape defense: deny unlink (and data rewrites) on every
        // ancestor directory of each writable root, plus read-only islands.
        let mut deny_paths: Vec<String> = Vec::new();
        for root in &fs.writable_roots {
            for anc in ancestors(root) {
                deny_paths.push(anc);
            }
        }
        for island in &fs.read_only_within {
            deny_paths.push(island.to_string_lossy().into_owned());
        }
        deny_paths.sort();
        deny_paths.dedup();
        for (i, _) in deny_paths.iter().enumerate() {
            let _ = writeln!(
                p,
                "(deny file-write-unlink (subpath (param \"DENY_UNLINK_{i}\")))"
            );
        }

        // Network: default-deny comes from the baseline; ProxyPorts adds
        // explicit loopback outbound grants only.
        match &fs.network {
            NetMode::None => p.push_str("(deny network-outbound)\n"),
            NetMode::ProxyPorts(ports) => {
                p.push_str(NETWORK_POLICY);
                for port in ports {
                    let _ = writeln!(
                        p,
                        "(allow network-outbound (remote ip \"localhost:{port}\"))"
                    );
                }
            }
        }
        p
    }

    fn wrap_argv(&self, cmd: &SpawnCmd, fs: &FsPolicy) -> SpawnCmd {
        let policy = Self::compose_policy(fs);
        let mut argv = vec![
            "/usr/bin/sandbox-exec".to_string(),
            "-p".to_string(),
            policy,
        ];
        for (i, root) in fs.writable_roots.iter().enumerate() {
            argv.push(format!("-DWRITABLE_ROOT_{i}={}", root.display()));
        }
        let mut deny_paths: Vec<String> = Vec::new();
        for root in &fs.writable_roots {
            deny_paths.extend(ancestors(root));
        }
        for island in &fs.read_only_within {
            deny_paths.push(island.to_string_lossy().into_owned());
        }
        deny_paths.sort();
        deny_paths.dedup();
        for (i, path) in deny_paths.iter().enumerate() {
            argv.push(format!("-DDENY_UNLINK_{i}={path}"));
        }
        argv.push("--".to_string());
        argv.extend(cmd.argv.iter().cloned());
        SpawnCmd {
            argv,
            cwd: cmd.cwd.clone(),
        }
    }
}

impl Sandbox for Seatbelt {
    fn wrap(&self, cmd: &SpawnCmd, fs: &FsPolicy) -> SpawnCmd {
        self.wrap_argv(cmd, fs)
    }
}

fn ancestors(path: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = path;
    while let Some(parent) = cur.parent() {
        if parent == Path::new("/") || parent.as_os_str().is_empty() {
            break;
        }
        out.push(parent.to_string_lossy().into_owned());
        cur = parent;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_policy_snapshot() {
        let fs = FsPolicy {
            writable_roots: vec![Path::new("/tmp/ws").into()],
            read_only_within: vec![Path::new("/tmp/ws/.git").into()],
            network: NetMode::None,
        };
        let policy = Seatbelt::compose_policy(&fs);
        let expected = r#"; base policy: deny default, exec/read exemptions, write grants are appended
(version 1)
(deny default)
(allow process-fork)
(allow process-exec*)
(allow file-read*)
(allow sysctl-read)
(allow mach-lookup)
(allow system-socket)
(allow signal (target self))
(allow file-write* (subpath (param "WRITABLE_ROOT_0")))
(allow file-write* (literal "/dev/null") (literal "/dev/stdout") (literal "/dev/stderr") (literal "/dev/tty"))
(deny file-write-unlink (subpath (param "DENY_UNLINK_0")))
(deny file-write-unlink (subpath (param "DENY_UNLINK_1")))
(deny network-outbound)
"#;
        assert_eq!(policy, expected);
    }

    #[test]
    fn wrap_argv_shape() {
        let fs = FsPolicy {
            writable_roots: vec![Path::new("/tmp/ws").into()],
            read_only_within: vec![],
            network: NetMode::ProxyPorts(vec![13659]),
        };
        let sb = Seatbelt;
        let wrapped = sb.wrap(
            &SpawnCmd {
                argv: vec!["touch".into(), "x".into()],
                cwd: Path::new("/tmp/ws").into(),
            },
            &fs,
        );
        assert_eq!(wrapped.argv[0], "/usr/bin/sandbox-exec");
        assert_eq!(wrapped.argv[1], "-p");
        assert!(wrapped.argv[2].contains("(deny default)"));
        assert!(
            wrapped
                .argv
                .iter()
                .any(|a| a == "-DWRITABLE_ROOT_0=/tmp/ws")
        );
        assert!(wrapped.argv.iter().any(|a| a == "-DDENY_UNLINK_0=/tmp"));
        assert_eq!(wrapped.argv.last().unwrap(), "x");
        assert!(
            wrapped
                .argv
                .windows(2)
                .any(|w| w[0] == "--" && w[1] == "touch")
        );
        // proxy port grant present
        assert!(wrapped.argv[2].contains("localhost:13659"));
    }
}
