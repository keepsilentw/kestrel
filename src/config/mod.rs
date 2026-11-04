//! Role configuration (harness/roles/<role>.toml): one file per role pins
//! permissions, sandbox policy, bus address and the backend it runs on.
//!
//! Relative paths in a role file (`policy_file`, `prompt_file`, `write_roots`,
//! `read_only_islands`) resolve against the **role file's own directory**: a
//! role living in `harness/roles/` reaches its sibling tree as
//! `../systems/<role>.md`. Getting this wrong used to fail silently in `-p`
//! mode and only surface when starting a worker.

use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Deserialize;

use crate::permissions::PolicyTable;
use crate::sandbox::{FsPolicy, NetMode};

/// Backend reference from the role file (design §6: 一角色一后端).
#[derive(Debug, Clone, Deserialize)]
pub struct LlmRef {
    pub provider: String,
    pub model: String,
}

#[derive(Debug, Deserialize)]
pub struct RoleConfig {
    pub role: String,
    #[serde(default)]
    pub peer_id: Option<String>,
    /// Bus socket this role's peer dials (`~` expanded by the caller).
    #[serde(default)]
    pub bus_socket: Option<String>,
    #[serde(default)]
    pub permissions: PermissionsSection,
    #[serde(default)]
    pub system: SystemSection,
    /// Provider/model this role runs on; CLI flags override.
    #[serde(default)]
    pub llm: Option<LlmRef>,
}

#[derive(Debug, Default, Deserialize)]
pub struct PermissionsSection {
    #[serde(default)]
    pub policy_file: Option<PathBuf>,
    /// Approval policy for Ask-class tool calls: `"none"` (default: deny) |
    /// `"all"` (approve, then the unsandboxed retry) | `"bus"` (ask a peer over
    /// the bus — see `approver`). A worker runs headless, so this is where a
    /// role opts into the escalation loop.
    #[serde(default)]
    pub approve: Option<String>,
    /// Bus peer/session that answers `approve = "bus"` requests. Absent means
    /// "ask whoever rang this bell" — the SA in the star topology.
    #[serde(default)]
    pub approver: Option<String>,
    /// How long `approve = "bus"` waits for the approver before denying the
    /// call itself (fail closed). Default 120s.
    #[serde(default)]
    pub approve_timeout_secs: Option<u64>,
    #[serde(default)]
    pub write_roots: Vec<PathBuf>,
    #[serde(default)]
    pub read_only_islands: Vec<PathBuf>,
    #[serde(default)]
    pub sandbox: SandboxSection,
}

#[derive(Debug, Default, Deserialize)]
pub struct SandboxSection {
    /// "none" | "proxy:<port>[,<port>...]"
    #[serde(default)]
    pub network: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct SystemSection {
    #[serde(default)]
    pub prompt_file: Option<PathBuf>,
}

impl RoleConfig {
    /// Approval mode for a headless run. Absent/unknown values fall back to
    /// deny — fail closed, and never silently wider than the role intended.
    pub fn approve_mode(&self) -> crate::tools::ApproveMode {
        match self.permissions.approve.as_deref() {
            Some("all") => crate::tools::ApproveMode::HeadlessAllow,
            Some("bus") => crate::tools::ApproveMode::Bus {
                timeout: self.approve_timeout(),
            },
            _ => crate::tools::ApproveMode::HeadlessDeny,
        }
    }

    /// Deadline for a bus approval: the role's `approve_timeout_secs`, else the
    /// kernel default.
    pub fn approve_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.permissions
                .approve_timeout_secs
                .unwrap_or(crate::tools::ApproveMode::DEFAULT_BUS_TIMEOUT.as_secs()),
        )
    }

    /// Bus address of the peer allowed to answer this role's approvals.
    pub fn approver(&self) -> Option<&str> {
        self.permissions.approver.as_deref()
    }

    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read role config {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parse role config {}", path.display()))
    }

    /// Writable roots / islands are canonicalized against the role file's dir
    /// when relative, after `$VAR` / `$VAR:-default` expansion — the role file
    /// is shared, the dispatch worktree is not.
    pub fn fs_policy(&self, base: &Path) -> anyhow::Result<FsPolicy> {
        let p = &self.permissions;
        let network = match p.sandbox.network.as_deref() {
            None | Some("none") => NetMode::None,
            Some(s) if s.starts_with("proxy:") => {
                let ports = s["proxy:".len()..]
                    .split(',')
                    .filter_map(|x| x.trim().parse::<u16>().ok())
                    .collect::<Vec<_>>();
                if ports.is_empty() {
                    anyhow::bail!("sandbox.network {s:?}: no valid ports");
                }
                NetMode::ProxyPorts(ports)
            }
            Some(other) => anyhow::bail!("unknown sandbox.network value: {other:?}"),
        };
        Ok(FsPolicy {
            writable_roots: p
                .write_roots
                .iter()
                .map(|r| canon(base, r))
                .collect::<anyhow::Result<Vec<_>>>()?,
            read_only_within: p
                .read_only_islands
                .iter()
                .map(|r| canon(base, r))
                .collect::<anyhow::Result<Vec<_>>>()?,
            network,
        })
    }

    pub fn policy_table(&self, base: &Path) -> anyhow::Result<PolicyTable> {
        match &self.permissions.policy_file {
            Some(f) => {
                let engine_path = canon(base, f)?;
                crate::permissions::PermissionEngine::from_json_file(&engine_path)
                    .with_context(|| format!("load policy table {}", engine_path.display()))
                    .map(|e| e.table)
            }
            None => Ok(PolicyTable::default()),
        }
    }

    pub fn system_prompt(&self, base: &Path) -> anyhow::Result<String> {
        match &self.system.prompt_file {
            Some(f) => {
                let p = canon(base, f)?;
                std::fs::read_to_string(&p)
                    .with_context(|| format!("read system prompt {}", p.display()))
            }
            None => Ok(String::new()),
        }
    }
}

/// kestrel home: $KESTREL_HOME or $HOME/.kestrel
pub fn kestrel_home() -> PathBuf {
    if let Ok(h) = std::env::var("KESTREL_HOME") {
        return PathBuf::from(h);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    Path::new(&home).join(".kestrel")
}

fn canon(base: &Path, p: &Path) -> anyhow::Result<PathBuf> {
    let expanded = expand_env_path(p, &|name| std::env::var(name).ok())?;
    let joined = if expanded.is_absolute() {
        expanded
    } else {
        base.join(expanded)
    };
    let joined = joined.canonicalize().unwrap_or(joined);
    Ok(joined)
}

/// Expand a leading `$NAME` reference in a role path: `$NAME`, `$NAME/rest`,
/// `$NAME:-default`, or the braced `${NAME}` / `${NAME:-default}` followed by a
/// path. An unset variable with no default is a hard error naming the spelling:
/// a role that silently lost its write root would move the sandbox without
/// anyone deciding to.
///
/// The braced form exists because the bare one cannot express "default first,
/// then more path": in `$NAME:-default` the `:-` owns the REST of the string, so
/// `$WT:-/tmp/wt/.git` silently drops `.git` the moment `WT` is set — which left
/// the whole RD worktree inside its own read-only island and made the demo
/// unable to write a single file (design §9.32).
fn expand_env_path(raw: &Path, lookup: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<PathBuf> {
    let text = raw.to_string_lossy().into_owned();
    let (name, default, tail) = match text.strip_prefix("${") {
        Some(braced) => {
            let Some(end) = braced.find('}') else {
                anyhow::bail!("role path {text:?}: unterminated `${{...}}` reference");
            };
            let (head, rest) = braced.split_at(end);
            let rest = &rest[1..];
            let (name, default) = match head.split_once(":-") {
                Some((n, d)) => (n, Some(d.to_string())),
                None => (head, None),
            };
            (name, default, rest.to_string())
        }
        None => {
            let Some(rest) = text.strip_prefix('$') else {
                return Ok(raw.to_path_buf());
            };
            let name_len = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            let (name, after) = rest.split_at(name_len);
            match after.strip_prefix(":-") {
                Some(d) => (name, Some(d.to_string()), String::new()),
                None => (name, None, after.to_string()),
            }
        }
    };
    // A braced name is delimited by `}` rather than by `/`, so it needs its own
    // check: `${a.b}` must not be read as a variable named `a.b`.
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        anyhow::bail!("role path {text:?}: invalid variable name {name:?}");
    }
    if name.is_empty() {
        anyhow::bail!("role path {text:?}: empty variable name after '$'");
    }
    // An empty fallback (`$WT:-`) is not a declared default: a root of "" would
    // make every path on the machine a prefix match, so it stays a hard error.
    let default = default.filter(|d| !d.is_empty());
    let value = match (lookup(name), default) {
        (Some(v), _) => v,
        (None, Some(d)) => d,
        (None, None) => anyhow::bail!(
            "role path {text:?}: environment variable {name} is not set and declares no \
             default (`${name}:-<fallback>`)"
        ),
    };
    Ok(PathBuf::from(format!("{value}{tail}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Diagnostics must name the file that failed — a bare
    /// "No such file or directory (os error 2)" sent people hunting the wrong
    /// config layer (smoke 2026-10-26 finding H).
    #[test]
    fn load_error_names_the_file() {
        let path = std::path::Path::new("/tmp/kestrel-definitely-missing-role.toml");
        let err = RoleConfig::load(path).unwrap_err().to_string();
        assert!(
            err.contains("read role config")
                && err.contains("kestrel-definitely-missing-role.toml"),
            "{err}"
        );
    }

    #[test]
    fn approve_mode_is_fail_closed() {
        let bare: RoleConfig = toml::from_str("role = \"rd\"").unwrap();
        assert_eq!(bare.approve_mode(), crate::tools::ApproveMode::HeadlessDeny);
        let allow: RoleConfig =
            toml::from_str("role = \"rd\"\n[permissions]\napprove = \"all\"").unwrap();
        assert_eq!(
            allow.approve_mode(),
            crate::tools::ApproveMode::HeadlessAllow
        );
        let junk: RoleConfig =
            toml::from_str("role = \"rd\"\n[permissions]\napprove = \"yolo\"").unwrap();
        assert_eq!(junk.approve_mode(), crate::tools::ApproveMode::HeadlessDeny);
    }

    /// `approve = "bus"` asks a peer instead of guessing: the role names the
    /// approver and how long it may take. Silence is a DENY, never a hang.
    #[test]
    fn approve_bus_mode_carries_approver_and_deadline() {
        let role: RoleConfig = toml::from_str(
            "role = \"rd\"\n\
             [permissions]\n\
             approve = \"bus\"\n\
             approver = \"harness-sa\"\n\
             approve_timeout_secs = 5\n",
        )
        .unwrap();
        assert_eq!(
            role.approve_mode(),
            crate::tools::ApproveMode::Bus {
                timeout: std::time::Duration::from_secs(5)
            }
        );
        assert_eq!(role.approver(), Some("harness-sa"));

        // Absent timeout => kernel default, so a role cannot opt into bus mode
        // and silently wait forever.
        let plain: RoleConfig =
            toml::from_str("role = \"rd\"\n[permissions]\napprove = \"bus\"").unwrap();
        assert_eq!(
            plain.approve_timeout(),
            crate::tools::ApproveMode::DEFAULT_BUS_TIMEOUT
        );
        assert_eq!(plain.approver(), None);
    }

    /// Role paths are machine-independent: `$VAR` with a `:-fallback`, and an
    /// unset variable without a fallback is a named error rather than a
    /// silently relocated write root.
    #[test]
    fn role_paths_expand_env_references() {
        let table: std::collections::BTreeMap<String, String> =
            [("WT".to_string(), "/tmp/wt".to_string())]
                .into_iter()
                .collect();
        let lookup = |n: &str| table.get(n).cloned();
        assert_eq!(
            expand_env_path(&PathBuf::from("$WT"), &lookup).unwrap(),
            PathBuf::from("/tmp/wt")
        );
        assert_eq!(
            expand_env_path(&PathBuf::from("$WT/sub"), &lookup).unwrap(),
            PathBuf::from("/tmp/wt/sub")
        );
        assert_eq!(
            expand_env_path(&PathBuf::from("$MISSING:-/tmp/fallback"), &lookup).unwrap(),
            PathBuf::from("/tmp/fallback")
        );
        // a fallback may itself be a path: `:-` owns the rest of the string
        assert_eq!(
            expand_env_path(&PathBuf::from("$MISSING:-/tmp/wt/deep"), &lookup).unwrap(),
            PathBuf::from("/tmp/wt/deep")
        );
        assert_eq!(
            expand_env_path(&PathBuf::from("$WT:-/tmp/fallback"), &lookup).unwrap(),
            PathBuf::from("/tmp/wt"),
            "the environment wins over the fallback"
        );
        // ...and *only* then: the bare form cannot carry a path after the
        // fallback, so `$WT:-/tmp/wt/.git` used to lose `.git` whenever WT was
        // set. Braces delimit the reference so the tail survives either way.
        assert_eq!(
            expand_env_path(&PathBuf::from("${WT:-/tmp/fallback}/.git"), &lookup).unwrap(),
            PathBuf::from("/tmp/wt/.git"),
            "the environment wins, and the tail is not swallowed"
        );
        assert_eq!(
            expand_env_path(&PathBuf::from("${MISSING:-/tmp/wt}/.git"), &lookup).unwrap(),
            PathBuf::from("/tmp/wt/.git")
        );
        assert_eq!(
            expand_env_path(&PathBuf::from("${WT}/.git"), &lookup).unwrap(),
            PathBuf::from("/tmp/wt/.git")
        );
        // An empty fallback is not a declared default: a root of "" would match
        // every path on the machine, so both spellings stay hard errors.
        assert!(
            expand_env_path(&PathBuf::from("${MISSING:-}/x"), &lookup).is_err(),
            "an empty fallback does not open the root"
        );
        assert!(
            expand_env_path(&PathBuf::from("${WT"), &lookup).is_err(),
            "an unterminated brace is not guessed at"
        );
        assert!(
            expand_env_path(&PathBuf::from("${a.b}/x"), &lookup).is_err(),
            "a braced name is still an identifier"
        );
        // literal paths are untouched
        assert_eq!(
            expand_env_path(&PathBuf::from("systems/rd.md"), &lookup).unwrap(),
            PathBuf::from("systems/rd.md")
        );
        let err = expand_env_path(&PathBuf::from("$MISSING/deep"), &lookup)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("MISSING") && err.contains("not set"),
            "unset variable names itself: {err}"
        );
        assert!(
            expand_env_path(&PathBuf::from("$-x"), &lookup).is_err(),
            "empty variable name is not guessed at"
        );
    }

    /// The roles shipped with the demo are the ones people actually run, and an
    /// island that covers its own write root silently revokes it: RD spent three
    /// iterations unable to write a single file (design §9.32). The library now
    /// has the syntax to express "root, minus .git" — this makes sure the role
    /// file uses it.
    #[test]
    fn shipped_roles_do_not_island_away_their_own_write_root() {
        let roles = Path::new(env!("CARGO_MANIFEST_DIR")).join("harness/roles");
        for name in ["rd.toml", "qa.toml", "sa.toml"] {
            let path = roles.join(name);
            let cfg =
                RoleConfig::load(&path).unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
            let fs = cfg
                .fs_policy(&roles)
                .unwrap_or_else(|e| panic!("fs_policy {name}: {e}"));
            for root in &fs.writable_roots {
                for island in &fs.read_only_within {
                    assert!(
                        !root.starts_with(island),
                        "{name}: read-only island {island:?} covers its write root \
                         {root:?} — nothing in it is writable, which is not what an \
                         island is for"
                    );
                }
            }
        }
    }

    #[test]
    fn parse_role_toml() {
        let dir = std::env::temp_dir();
        let cfg: RoleConfig = toml::from_str(
            r#"
role = "rd"
peer_id = "harness-rd"
bus_socket = "/tmp/kestrel-test-bus.sock"

[llm]
provider = "deepseek-anthropic"
model = "deepseek-v4-flash"

[permissions]
policy_file = "rd.policy.json"
write_roots = ["/tmp/ws"]
read_only_islands = ["/tmp/ws/.git"]

[permissions.sandbox]
network = "proxy:13659"

[system]
prompt_file = "rd.md"
"#,
        )
        .unwrap();
        assert_eq!(cfg.role, "rd");
        let fs = cfg.fs_policy(&dir).unwrap();
        assert_eq!(fs.writable_roots, vec![PathBuf::from("/tmp/ws")]);
        assert_eq!(fs.network, NetMode::ProxyPorts(vec![13659]));
        assert_eq!(
            cfg.permissions.policy_file,
            Some(PathBuf::from("rd.policy.json"))
        );
        // backend + bus socket are what `serve` starts the worker from
        assert_eq!(
            cfg.bus_socket.as_deref(),
            Some("/tmp/kestrel-test-bus.sock")
        );
        let llm = cfg.llm.as_ref().expect("role declares a backend");
        assert_eq!(llm.provider, "deepseek-anthropic");
        assert_eq!(llm.model, "deepseek-v4-flash");
    }

    /// Relative role paths resolve against the role file's directory, and a
    /// declared-but-unreadable `prompt_file` is an error (not a silent empty
    /// system prompt — that mask cost a whole debugging session).
    #[test]
    fn role_relative_paths_and_missing_prompt() {
        let dir = std::env::temp_dir().join(format!("kestrel-role-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("roles")).unwrap();
        std::fs::create_dir_all(dir.join("systems")).unwrap();
        std::fs::write(dir.join("systems/p.md"), "you are p").unwrap();
        std::fs::write(
            dir.join("roles/p.policy.json"),
            r#"{"default": "ask", "rules": []}"#,
        )
        .unwrap();
        let role_path = dir.join("roles/p.toml");
        std::fs::write(
            &role_path,
            "role = \"p\"\n\
             [system]\n\
             prompt_file = \"../systems/p.md\"\n\
             [permissions]\n\
             policy_file = \"p.policy.json\"\n",
        )
        .unwrap();

        let cfg = RoleConfig::load(&role_path).unwrap();
        let base = role_path.parent().unwrap();
        assert_eq!(cfg.system_prompt(base).unwrap(), "you are p");
        assert_eq!(
            cfg.policy_table(base).unwrap().default,
            crate::permissions::Decision::Ask
        );

        // Same relative spelling as the old (broken) role files: resolves under
        // roles/ and must fail loudly.
        std::fs::write(
            &role_path,
            "role = \"p\"\n[system]\nprompt_file = \"systems/p.md\"\n",
        )
        .unwrap();
        let cfg = RoleConfig::load(&role_path).unwrap();
        assert!(cfg.system_prompt(base).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
