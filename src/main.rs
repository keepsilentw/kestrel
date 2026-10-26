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

    /// Headless approval policy: none (deny Ask) | all (allow Ask).
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
                approve,
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
    let (model_meta, api_key) = resolve_backend(&cli.backend, role.as_ref())?;
    // Approval precedence: runtime flag > role file > deny (fail closed).
    let approve = match (approve, role.as_ref()) {
        (Some(mode), _) => mode,
        (None, Some(r)) => r.approve_mode(),
        (None, None) => kestrel::tools::ApproveMode::HeadlessDeny,
    };
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

/// `--approve none|all` — the headless answer to an Ask-class tool call.
fn parse_approve(value: &str) -> anyhow::Result<kestrel::tools::ApproveMode> {
    match value {
        "none" => Ok(kestrel::tools::ApproveMode::HeadlessDeny),
        "all" => Ok(kestrel::tools::ApproveMode::HeadlessAllow),
        other => anyhow::bail!("--approve must be none|all, got {other:?}"),
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
    use super::{Cli, parse_approve};
    use clap::Parser;
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
    }

    #[test]
    fn resume_selector_is_captured() {
        let c = Cli::try_parse_from(["kestrel", "-p", "hi", "--resume", "last"]).unwrap();
        assert_eq!(c.resume.as_deref(), Some("last"));
        let plain = Cli::try_parse_from(["kestrel", "-p", "hi"]).unwrap();
        assert!(plain.resume.is_none());
    }
}
