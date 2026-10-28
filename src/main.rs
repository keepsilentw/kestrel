//! kestrel binary. Modes: one-shot `-p` run (default), `serve` resident worker,
//! `demo-sa` scripted SA driver, `hub` bus process.

use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Args, Parser, Subcommand};
use kestrel::cli::run::{RunArgs, run_once};
use kestrel::config::RoleConfig;
use kestrel::provider::config::{KeyResolver, Registry};
use kestrel::provider::wire::{ModelMeta, WireId};

const DEFAULT_BUS_SOCKET: &str = "~/.kestrel/harness/bus.sock";

#[derive(Parser, Debug)]
#[command(name = "kestrel", version, about = "Minimal Rust harness kernel")]
struct Cli {
    /// One-shot prompt (headless run). Omit when using a subcommand.
    #[arg(short = 'p')]
    prompt: Option<String>,

    #[command(flatten)]
    backend: BackendArgs,

    /// Optional system prompt.
    #[arg(long, default_value = "")]
    system: String,

    /// Role TOML (permissions/sandbox/prompt/backend; see harness/roles/).
    #[arg(long)]
    role: Option<PathBuf>,

    /// How an Ask-class tool call is answered: none (deny) | all (allow once) |
    /// bus (ask the role's approver over the bus; `serve` only).
    #[arg(long, value_name = "POLICY")]
    approve: Option<String>,

    /// Built-in tools (bash/read/write/edit): opt-in — `--tools` on, `--tools false` off.
    #[arg(
        long,
        num_args = 0..=1,
        value_name = "BOOL",
        default_value_t = false,
        default_missing_value = "true",
        action = ArgAction::Set
    )]
    tools: bool,

    /// Continue an existing session tree instead of starting one: `last`, a
    /// session id or unique id prefix, or a path to a session `.jsonl`.
    #[arg(long, value_name = "SELECTOR")]
    resume: Option<String>,

    /// Overall timeout in seconds.
    #[arg(long, default_value_t = 300)]
    timeout_secs: u64,

    #[command(subcommand)]
    command: Option<Command>,
}

/// Backend selection, shared by the top-level run mode and `serve`.
#[derive(Args, Debug, Clone)]
struct BackendArgs {
    /// Provider id from ~/.kestrel/providers.toml.
    #[arg(long)]
    provider: Option<String>,

    /// Anthropic-compatible base URL (direct mode, no --provider).
    #[arg(long)]
    base_url: Option<String>,

    /// API key (`$VAR` | `$VAR:-default` | literal; direct mode).
    #[arg(long)]
    api_key: Option<String>,

    /// Model id (defaults to the role's [llm].model).
    #[arg(long)]
    model: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Resident worker: doorbell -> one turn -> result artifact + result bell.
    Serve(ServeCmd),
    /// Scripted SA driver for the 3-terminal demo (mechanical semantics only).
    DemoSa(DemoSaCmd),
    /// Bus hub: bind the UDS socket and route frames between peers.
    Hub(HubCmd),
}

#[derive(Args, Debug)]
struct ServeCmd {
    /// Bus session id (also the peer id registered with the hub).
    #[arg(long)]
    session: String,

    /// Bus unix socket path (defaults to the role's bus_socket).
    #[arg(long)]
    bus_socket: Option<String>,

    /// Role TOML: permissions, sandbox, prompt and backend.
    #[arg(long)]
    role: PathBuf,

    /// Result artifacts dir (the SA reads disk, not bells).
    #[arg(long, default_value = "artifacts")]
    artifacts_dir: PathBuf,

    /// How many tasks this worker hosts engines for, concurrently (one engine
    /// and one session tree per task). Over the ceiling a bell is refused with
    /// a visible error bell. 0 = no ceiling.
    #[arg(long, default_value_t = 4)]
    max_tasks: usize,

    #[command(flatten)]
    backend: BackendArgs,
}

#[derive(Args, Debug)]
struct DemoSaCmd {
    /// Task text dispatched to the workers.
    #[arg(long)]
    task: String,

    /// Bus unix socket path.
    #[arg(long, default_value = DEFAULT_BUS_SOCKET)]
    bus_socket: String,

    /// Journal (write-ahead truth source) path.
    #[arg(long, default_value = "journal.jsonl")]
    journal: PathBuf,

    /// Result artifacts dir (SA 巡检读盘为准).
    #[arg(long, default_value = "artifacts")]
    artifacts_dir: PathBuf,

    /// Budget ceiling for the whole pipeline, in the fixture USD accounting.
    /// It has to cover the declared retry policy, not just the happy path:
    /// 4 stages x 3 iterations x 0.5 = 6.0 (a fail 回环 is real now — an empty
    /// answer or a FAIL verdict re-dispatches instead of passing).
    #[arg(long, default_value_t = 6.0)]
    max_usd: f64,

    /// How long to wait for a worker's result bell before falling back to disk
    /// inspection. A resident worker can spend its whole round budget (8 tool
    /// rounds) on one bell, so this is not a 30s knob.
    #[arg(long, default_value_t = 300)]
    bell_timeout_secs: u64,

    /// The dispatch worktree RD writes into; published to every bell `ref` so
    /// QA verifies the same tree.
    #[arg(long)]
    worktree: Option<PathBuf>,

    /// Approval authority table for workers running `approve = "bus"`: a
    /// three-state prefix policy JSON (same format as a role policy file).
    /// Without it the SA denies every reverse request — fail closed.
    #[arg(long)]
    approvals: Option<PathBuf>,

    /// Fold the journal into a checkpoint once the live file holds this many
    /// records (the older prefix moves to `<journal>.seg<from>-<to>.jsonl`, never
    /// deleted). 0 keeps the original single-file, never-truncated behaviour.
    #[arg(long, default_value_t = 0)]
    rotate_after_records: usize,

    /// Byte ceiling for the live journal (either ceiling triggers a rotation;
    /// records vary in size, so a count alone can leave the file huge). 0 = off.
    #[arg(long, default_value_t = 0)]
    rotate_max_bytes: u64,

    /// Task namespace: stamped into every journal record, every bell and every
    /// artifact name, so several `demo-sa` processes can share one hub without
    /// reading each other's results. Keep it unique per pipeline.
    #[arg(long, default_value = "demo")]
    task_id: String,

    /// Peer the dev stage is dispatched to. A second pipeline points this at its
    /// own worker, e.g. `--rd-peer t2:harness-rd` with `serve --session
    /// t2:harness-rd`.
    #[arg(long, default_value = "harness-rd")]
    rd_peer: String,

    /// Peer the review/test/build stages are dispatched to.
    #[arg(long, default_value = "harness-qa")]
    qa_peer: String,
}

#[derive(Args, Debug)]
struct HubCmd {
    /// Bus unix socket path to bind.
    #[arg(long, default_value = DEFAULT_BUS_SOCKET)]
    bus_socket: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let approve = cli.approve.as_deref().map(parse_approve).transpose()?;

    // Project-layer discipline: providers may never be defined there (codex 纪律 1).
    let project_cfg = std::path::Path::new(".kestrel/kestrel.toml");
    if project_cfg.exists() {
        kestrel::provider::config::assert_no_provider_definitions(&std::fs::read_to_string(
            project_cfg,
        )?)?;
    }

    match cli.command {
        Some(Command::Hub(h)) => {
            return kestrel::cli::hub::run_hub(shellexpand_home(&h.bus_socket)).await;
        }
        Some(Command::Serve(s)) => {
            let role = RoleConfig::load(&s.role)?;
            let (model, api_key) = resolve_backend(&s.backend, Some(&role))?;
            // Socket precedence: explicit flag > role file > built-in default.
            let socket = match s.bus_socket.as_deref() {
                Some(p) => shellexpand_home(p),
                None => role
                    .bus_socket
                    .as_deref()
                    .map_or_else(|| shellexpand_home(DEFAULT_BUS_SOCKET), shellexpand_home),
            };
            return kestrel::cli::serve::serve(kestrel::cli::serve::ServeArgs {
                bus_socket: socket,
                session: s.session,
                role_file: s.role,
                model,
                api_key,
                artifacts_dir: s.artifacts_dir,
                max_tasks: s.max_tasks,
                approve: approve.map(|mode| with_role_deadline(mode, Some(&role))),
            })
            .await;
        }
        Some(Command::DemoSa(d)) => {
            return kestrel::cli::sa_demo::run_demo_sa(kestrel::cli::sa_demo::DemoSaArgs {
                bus_socket: shellexpand_home(&d.bus_socket),
                task: d.task,
                journal: d.journal,
                artifacts: d.artifacts_dir,
                per_dispatch_usd: 0.5,
                max_usd: d.max_usd,
                max_iterations: 3,
                bell_timeout: Duration::from_secs(d.bell_timeout_secs),
                worktree: d.worktree,
                approvals: d.approvals,
                rotate_after: d.rotate_after_records,
                rotate_max_bytes: d.rotate_max_bytes,
                task_id: d.task_id,
                rd_peer: d.rd_peer,
                qa_peer: d.qa_peer,
            })
            .await;
        }
        None => {}
    }

    // --- default mode: one-shot run.
    let Some(prompt) = cli.prompt else {
        anyhow::bail!(
            "pass -p <PROMPT> for a one-shot run, or use a subcommand: serve | demo-sa | hub"
        );
    };
    let role = match &cli.role {
        Some(path) => Some(RoleConfig::load(path)?),
        None => None,
    };
    // Approval precedence: runtime flag > role file > deny (fail closed).
    let approve = match (approve, role.as_ref()) {
        (Some(mode), r) => with_role_deadline(mode, r),
        (None, Some(r)) => r.approve_mode(),
        (None, None) => kestrel::tools::ApproveMode::HeadlessDeny,
    };
    // A one-shot process has no bus to ask on: fail at startup rather than
    // leaving every Ask-class call hanging until its deadline.
    if matches!(approve, kestrel::tools::ApproveMode::Bus { .. }) {
        anyhow::bail!(
            "approve = bus needs `serve`: a one-shot run has no bus to reach its approver \
             (use --approve none|all, or [permissions].approve in the role file)"
        );
    }
    let (model_meta, api_key) = resolve_backend(&cli.backend, role.as_ref())?;
    let code = run_once(RunArgs {
        prompt,
        model: model_meta,
        api_key,
        system: cli.system,
        timeout: Duration::from_secs(cli.timeout_secs),
        role_file: cli.role,
        approve,
        tools_enabled: cli.tools,
        resume: cli.resume,
    })
    .await?;
    std::process::exit(code);
}

/// `--approve none|all|bus` — how an Ask-class tool call gets answered:
/// deny, allow-once, or ask an approver peer over the bus.
fn parse_approve(value: &str) -> anyhow::Result<kestrel::tools::ApproveMode> {
    match value {
        "none" => Ok(kestrel::tools::ApproveMode::HeadlessDeny),
        "all" => Ok(kestrel::tools::ApproveMode::HeadlessAllow),
        "bus" => Ok(kestrel::tools::ApproveMode::Bus {
            timeout: kestrel::tools::ApproveMode::DEFAULT_BUS_TIMEOUT,
        }),
        other => anyhow::bail!("--approve must be none|all|bus, got {other:?}"),
    }
}

/// The runtime flag chooses the mode; the role file still owns the deadline it
/// declared, so `--approve bus` never silently drops a role's timing.
fn with_role_deadline(
    mode: kestrel::tools::ApproveMode,
    role: Option<&RoleConfig>,
) -> kestrel::tools::ApproveMode {
    match role {
        Some(r)
            if matches!(mode, kestrel::tools::ApproveMode::Bus { .. })
                && r.permissions.approve_timeout_secs.is_some() =>
        {
            kestrel::tools::ApproveMode::Bus {
                timeout: r.approve_timeout(),
            }
        }
        _ => mode,
    }
}

/// Resolve (model meta, api key) from CLI flags, falling back to the role's
/// `[llm]` reference — the role layer is where "一角色一后端" is declared
/// (design §6), so a role-only invocation needs no provider flags.
fn resolve_backend(
    backend: &BackendArgs,
    role: Option<&RoleConfig>,
) -> anyhow::Result<(ModelMeta, String)> {
    let role_llm = role.and_then(|r| r.llm.as_ref());
    let provider_id = backend
        .provider
        .clone()
        .or_else(|| role_llm.map(|l| l.provider.clone()));
    let model_id = backend
        .model
        .clone()
        .or_else(|| role_llm.map(|l| l.model.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!("no model: pass --model or declare [llm].model in the role file")
        })?;

    match provider_id {
        Some(pid) => {
            let reg_path = kestrel::config::kestrel_home().join("providers.toml");
            let registry = Registry::load(&reg_path)?;
            let meta = registry.model_meta(&pid, &model_id)?;
            let key = KeyResolver::new().resolve(&registry.providers[&pid].key)?;
            Ok((meta, key))
        }
        None => {
            let key = backend.api_key.as_deref().unwrap_or("");
            if key.is_empty() {
                anyhow::bail!("--api-key required (or use --provider with providers.toml)");
            }
            Ok((
                ModelMeta::minimal(
                    &model_id,
                    WireId::AnthropicMessages,
                    backend
                        .base_url
                        .as_deref()
                        .unwrap_or("https://api.anthropic.com"),
                ),
                resolve_key(key)?,
            ))
        }
    }
}

fn shellexpand_home(p: &str) -> PathBuf {
    if let Some(rest) = p.strip_prefix("~/").map(|rest| {
        std::env::var("HOME")
            .ok()
            .map(|home| PathBuf::from(home).join(rest))
    }) {
        return rest.unwrap_or_else(|| PathBuf::from(p));
    }
    PathBuf::from(p)
}

/// `$VAR` / `$VAR:-default` env reference, else literal (pi resolve-config-value semantics).
fn resolve_key(spec: &str) -> anyhow::Result<String> {
    if let Some(rest) = spec.strip_prefix('$') {
        let (name, default) = match rest.split_once(":-") {
            Some((n, d)) => (n, Some(d)),
            None => (rest, None),
        };
        match (std::env::var(name).ok(), default) {
            (Some(v), _) => Ok(v),
            (None, Some(d)) => Ok(d.to_string()),
            (None, None) => anyhow::bail!("env var {name} is not set"),
        }
    } else {
        Ok(spec.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{Cli, parse_approve, with_role_deadline};
    use clap::Parser;
    use kestrel::config::RoleConfig;
    use kestrel::tools::ApproveMode;

    #[test]
    fn tools_flag_is_opt_in_and_still_switchable_off() {
        let plain = Cli::try_parse_from(["kestrel", "-p", "hi"]).unwrap();
        assert!(!plain.tools, "tools default to off, as documented");
        assert!(
            Cli::try_parse_from(["kestrel", "-p", "hi", "--tools"])
                .unwrap()
                .tools
        );
        assert!(
            !Cli::try_parse_from(["kestrel", "-p", "hi", "--tools", "false"])
                .unwrap()
                .tools,
            "--tools false must be accepted (wrappers need to switch tools off)"
        );
    }

    #[test]
    fn approve_defaults_to_unset_so_the_role_can_decide() {
        let plain = Cli::try_parse_from(["kestrel", "-p", "hi"]).unwrap();
        assert!(plain.approve.is_none(), "no flag means the role decides");
        assert_eq!(parse_approve("none").unwrap(), ApproveMode::HeadlessDeny);
        assert_eq!(parse_approve("all").unwrap(), ApproveMode::HeadlessAllow);
        assert!(parse_approve("sometimes").is_err());
        assert_eq!(
            parse_approve("bus").unwrap(),
            ApproveMode::Bus {
                timeout: ApproveMode::DEFAULT_BUS_TIMEOUT
            }
        );
    }

    /// `--approve bus` overrides the mode, never the role's declared deadline.
    #[test]
    fn bus_mode_keeps_the_roles_deadline() {
        let role: RoleConfig = toml::from_str(
            "role = \"rd\"\n[permissions]\napprove = \"none\"\napprove_timeout_secs = 7\n",
        )
        .unwrap();
        assert_eq!(
            with_role_deadline(parse_approve("bus").unwrap(), Some(&role)),
            ApproveMode::Bus {
                timeout: std::time::Duration::from_secs(7)
            }
        );
        // no deadline declared => kernel default survives the override
        assert_eq!(
            with_role_deadline(parse_approve("bus").unwrap(), None),
            ApproveMode::Bus {
                timeout: ApproveMode::DEFAULT_BUS_TIMEOUT
            }
        );
        // headless modes are untouched by a bus deadline
        assert_eq!(
            with_role_deadline(ApproveMode::HeadlessAllow, Some(&role)),
            ApproveMode::HeadlessAllow
        );
    }

    #[test]
    fn resume_selector_is_captured() {
        let c = Cli::try_parse_from(["kestrel", "-p", "hi", "--resume", "last"]).unwrap();
        assert_eq!(c.resume.as_deref(), Some("last"));
        let plain = Cli::try_parse_from(["kestrel", "-p", "hi"]).unwrap();
        assert!(plain.resume.is_none());
    }
}
