//! kestrel binary. Modes: one-shot `-p` run (default), `serve` resident worker,
//! `demo-sa` scripted SA driver, `hub` bus process.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
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
    #[arg(long, default_value = "none")]
    approve: String,

    /// Enable built-in tools (bash/read/write/edit).
    #[arg(long, default_value_t = true)]
    tools: bool,

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
    let approve = match cli.approve.as_str() {
        "none" => kestrel::tools::ApproveMode::HeadlessDeny,
        "all" => kestrel::tools::ApproveMode::HeadlessAllow,
        other => anyhow::bail!("--approve must be none|all, got {other:?}"),
    };

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
                max_usd: 2.0,
                max_iterations: 3,
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
    let code = run_once(RunArgs {
        prompt,
        model: model_meta,
        api_key,
        system: cli.system,
        timeout: Duration::from_secs(cli.timeout_secs),
        role_file: cli.role,
        approve,
        tools_enabled: cli.tools,
    })
    .await?;
    std::process::exit(code);
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
