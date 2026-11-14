//! kestrel binary. Modes: one-shot `-p` run (default), `serve` resident worker,
//! `demo-sa` scripted SA driver, `hub` bus process.

use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Args, Parser, Subcommand};
use kestrel::cli::run::{RunArgs, run_once};
use kestrel::config::RoleConfig;
use kestrel::journal::{Journal, JournalStatus};
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
    /// Move sealed journal segments into `<journal-stem>.segments/`.
    JournalArchive(JournalArchiveCmd),
    /// Summarize live records, sealed segments and staged-file residue.
    JournalStatus(JournalStatusCmd),
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

    /// How many tools one task may execute concurrently. This bounds the
    /// execute phase without changing source-order results or serial tools.
    /// 0 = no ceiling.
    #[arg(long, default_value_t = kestrel::tools::DEFAULT_MAX_PARALLEL_TOOLS)]
    max_parallel_tools: usize,

    /// How many tools all tasks in this worker may execute concurrently.
    /// 0 = no worker-wide ceiling.
    #[arg(long, default_value_t = kestrel::tools::DEFAULT_MAX_WORKER_TOOLS)]
    max_worker_tools: usize,

    /// Soft memory ceiling for the whole worker, in megabytes. Crossing it puts
    /// the worker into drain mode: new tasks are refused out loud, running ones
    /// finish. 0 = no ceiling.
    #[arg(long, default_value_t = 0)]
    max_rss_mb: u64,

    /// Idle time after which a task's engine is released so its slot frees up.
    /// The session tree survives and is reopened if the same task rings again,
    /// so this costs an engine restart, not the pipeline's history. 0 = never.
    #[arg(long, default_value_t = 1800)]
    task_idle_timeout_secs: u64,

    /// Atomically write a machine-readable worker status document.
    #[arg(long)]
    status_file: Option<PathBuf>,

    /// Status refresh interval in seconds.
    #[arg(long, default_value_t = 5)]
    status_interval_secs: u64,

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

    /// Budget ceiling for the whole pipeline, in the run's money unit.
    ///
    /// With a price table (`--roles-dir` together with a `cost` block for that
    /// model in providers.toml) this is a real bill: the stop-loss blocks a
    /// dispatch whose WORST-CASE cost (whole context window plus a full max_out
    /// answer) would cross it, then settles the actual tokens. Without one the
    /// ledger books `--nominal-cost` per dispatch, which is a fixture unit and not
    /// a bill — the SA prints that fact at startup rather than letting it pass as
    /// one. Either way it has to cover the retry policy, not just the happy path:
    /// 4 stages times 3 iterations times 0.5 is 6.0 (a fail 回环 is real now — an
    /// empty answer or a FAIL verdict re-dispatches instead of passing).
    /// The unit is whatever the price table declares per provider (`currency`);
    /// with no table in play it is `--currency` (design §9.44). The old
    /// `--max-usd` spelling still parses.
    #[arg(long = "max-cost", alias = "max-usd", default_value_t = 6.0)]
    max_cost: f64,

    /// Nominal per-dispatch charge, used only when a peer has no rate to settle
    /// against. Kept explicit as a flag because it is the difference between
    /// "the ceiling is a bill" and "the ceiling is a dispatch counter".
    #[arg(long = "nominal-cost", alias = "nominal-usd", default_value_t = 0.5)]
    nominal_cost: f64,

    /// The currency `--max-cost` / `--nominal-cost` are written in, used only
    /// when the price table declares none. A priced table always wins, and a
    /// table that mixes currencies in one run is refused at startup: a ledger
    /// sums one unit, not two.
    #[arg(long, default_value = "usd")]
    currency: String,

    /// Directory of role TOMLs. The ledger reads `[llm]` from them to learn which
    /// peer runs on which provider/model, then prices that model from
    /// providers.toml. Without it every dispatch stays nominal.
    #[arg(long)]
    roles_dir: Option<PathBuf>,

    /// Rate table (price per million tokens, in each provider's own `currency`).
    /// Defaults to the user layer file
    /// `~/.kestrel/providers.toml`, read only if it exists.
    #[arg(long)]
    prices: Option<PathBuf>,

    /// Per-stage evidence requirements, as JSON: `{"dev": ["diff"], "test":
    /// ["test_log"]}`. A required kind must be claimed by the worker AND verify
    /// as that kind on disk — a plain file cannot fill a `test_log` slot. Absent
    /// means existence-only checks (the pre-typing semantics); a malformed table
    /// is a start-up failure, never a silent "no requirements".
    #[arg(long)]
    evidence_spec: Option<PathBuf>,

    /// How long to wait for a worker's result bell before falling back to disk
    /// inspection. A resident worker can spend its whole round budget (8 tool
    /// rounds) on one bell, so this is not a 30s knob.
    #[arg(long, default_value_t = 300)]
    bell_timeout_secs: u64,

    /// The dispatch worktree RD writes into; published to every bell `ref` so
    /// QA verifies the same tree.
    #[arg(long)]
    worktree: Option<PathBuf>,

    /// Deterministic build command run by the SA at the build stage. Its
    /// kernel-generated log fills the `build_log` evidence requirement without
    /// asking the verifier model to execute the command.
    #[arg(long)]
    build_command: Option<String>,

    /// Additional trusted build command prefix, e.g. `zig build`. Repeat for
    /// multiple project-specific build systems. Built-in cargo/python/npm/go
    /// and related prefixes remain enabled.
    #[arg(long = "trusted-build-prefix", action = ArgAction::Append, value_parser = parse_trusted_build_prefix)]
    trusted_build_prefixes: Vec<String>,

    /// Approval authority table for workers running `approve = "bus"`: a
    /// three-state prefix policy JSON (same format as a role policy file).
    /// Without it the SA denies every reverse request — fail closed.
    #[arg(long)]
    approvals: Option<PathBuf>,

    /// Fold the journal into a checkpoint once the live file holds this many
    /// records (the older prefix moves to `<journal>.seg<from>-<to>.jsonl`, never
    /// deleted). Unset = the default policy (512 records); 0 turns this ceiling off
    /// explicitly.
    #[arg(long)]
    rotate_after_records: Option<usize>,

    /// Byte ceiling for the live journal (either ceiling triggers a fold; records
    /// vary in size — an approval record carries stderr — so a count alone cannot
    /// bound the file). Unset = 1 MiB; 0 = off.
    #[arg(long)]
    rotate_max_bytes: Option<u64>,

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

#[derive(Args, Debug)]
struct JournalArchiveCmd {
    /// Journal whose already-sealed segments should be moved. No records are
    /// deleted; `replay_all` continues to read the archive directory.
    #[arg(long, default_value = "journal.jsonl")]
    journal: PathBuf,

    /// Keep this many newest sealed segments in the journal directory. The
    /// remaining segments move into the archive. 0 = move every sealed segment.
    #[arg(long, default_value_t = 0)]
    keep: usize,

    /// Print the exact moves and collisions without renaming anything.
    #[arg(long)]
    dry_run: bool,

    /// Before moving, quarantine the non-authoritative copy of any duplicated
    /// segment range into `<journal-stem>.segments/duplicates/`. Nothing is
    /// deleted; without this a duplicated range blocks the move by design.
    #[arg(long)]
    resolve_duplicates: bool,
}

#[derive(Args, Debug)]
struct JournalStatusCmd {
    /// Journal whose live file and sealed segments should be summarized.
    #[arg(long, default_value = "journal.jsonl")]
    journal: PathBuf,

    /// Print each segment instead of only the aggregate counts.
    #[arg(long)]
    list: bool,

    /// Emit the complete status document as JSON. Segment details are always
    /// included in this mode.
    #[arg(long)]
    json: bool,

    /// Exit non-zero when duplicate segment ranges or staged residue are found.
    #[arg(long)]
    check: bool,

    /// Parse the records: compare each segment's real record count with the
    /// range in its name, and the live checkpoint's `covers` with the end of the
    /// chain. Slower than the metadata-only default, so it is opt-in.
    #[arg(long)]
    verify: bool,

    /// Replay approval audit records and include pending, decided, receipted and
    /// orphaned request counts.
    #[arg(long)]
    approvals: bool,
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
        Some(Command::JournalArchive(a)) => {
            let path = shellexpand_home(&a.journal.to_string_lossy());
            let journal = Journal::open(&path)?;
            if a.dry_run {
                let plan = journal.archive_plan(a.keep)?;
                let duplicates = journal.status().duplicate_segments;
                if a.resolve_duplicates {
                    println!(
                        "journal archive dry-run: {duplicates} duplicate copy/copies would \
                         be quarantined first"
                    );
                }
                println!("journal archive dry-run: {} planned move(s)", plan.len());
                for item in plan {
                    println!(
                        "{} seg{}-{} / {} byte(s) / {}{}",
                        if item.destination_exists {
                            "blocked"
                        } else {
                            "move"
                        },
                        item.from,
                        item.to,
                        item.bytes,
                        item.source.display(),
                        if item.destination_exists {
                            format!(" -> existing {}", item.destination.display())
                        } else {
                            format!(" -> {}", item.destination.display())
                        }
                    );
                }
                return Ok(());
            }
            let moved = journal.archive_segments(a.keep, a.resolve_duplicates)?;
            match journal.segment_archive_dir() {
                Some(dir) => println!(
                    "journal archive: moved {moved} sealed segment(s) into {}",
                    dir.display()
                ),
                None => println!("journal archive: moved {moved} sealed segment(s)"),
            }
            return Ok(());
        }
        Some(Command::JournalStatus(s)) => {
            let path = shellexpand_home(&s.journal.to_string_lossy());
            let journal = Journal::open(&path)?;
            let mut status = journal.status();
            if s.approvals {
                status.approvals = Some(journal.approval_status());
            }
            if s.verify {
                status.verification = Some(journal.verify());
            }
            if s.json {
                println!("{}", serde_json::to_string_pretty(&status)?);
                if s.check && !status.is_healthy() {
                    anyhow::bail!("{}", unhealthy_report(&status));
                }
                return Ok(());
            }
            println!(
                "journal: {} live record(s) / {} byte(s)",
                status.live_records, status.live_bytes
            );
            println!(
                "sealed: {} in root, {} archived, {} total / {} byte(s)",
                status.root_segments,
                status.archived_segments,
                status.root_segments + status.archived_segments,
                status.total_segment_bytes
            );
            println!(
                "integrity: {} duplicate range(s)",
                status.duplicate_segments
            );
            if status.coverage_gaps.is_empty() {
                println!("coverage chain: complete");
            } else {
                for gap in &status.coverage_gaps {
                    println!(
                        "coverage chain: records {}..{} are covered by no segment",
                        gap.expected_from, gap.found_from
                    );
                }
            }
            println!("staged residue: {} file(s)", status.staged_files);
            if let Some(verification) = &status.verification {
                if verification.problems.is_empty() {
                    println!(
                        "verify: {} segment(s) hold what their names promise, checkpoint covers \
                         {} record(s)",
                        verification.segments.len(),
                        verification.chain_end
                    );
                } else {
                    for problem in &verification.problems {
                        println!("verify: {problem}");
                    }
                }
            }
            if let Some(approvals) = &status.approvals {
                println!(
                    "approvals: {} request(s), {} decision(s), {} receipt(s), {} pending, {} unacked, {} applied, {} stale, {} orphan decision(s), {} orphan receipt(s)",
                    approvals.requests,
                    approvals.decisions,
                    approvals.receipts,
                    approvals.pending,
                    approvals.unacked,
                    approvals.applied,
                    approvals.stale,
                    approvals.orphan_decisions,
                    approvals.orphan_receipts
                );
            }
            if s.list {
                for segment in &status.segments {
                    println!(
                        "{} seg{}-{} / {} byte(s) / {}",
                        if segment.archived { "archived" } else { "root" },
                        segment.from,
                        segment.to,
                        segment.bytes,
                        segment.path.display()
                    );
                }
            }
            if s.check && !status.is_healthy() {
                anyhow::bail!("{}", unhealthy_report(&status));
            }
            return Ok(());
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
                max_parallel_tools: s.max_parallel_tools,
                max_worker_tools: s.max_worker_tools,
                max_rss_mb: s.max_rss_mb,
                task_idle_timeout: Duration::from_secs(s.task_idle_timeout_secs),
                status_file: s.status_file,
                status_interval: Duration::from_secs(s.status_interval_secs),
                approve: approve.map(|mode| with_role_deadline(mode, Some(&role))),
            })
            .await;
        }
        Some(Command::DemoSa(d)) => {
            // 三态：没给 = 默认策略；给 0 = 明确关闭；其余 = 自定义。两个上限各自
            // 独立解析，「封顶了条数」不等于「字节也要无限」。
            let (rotate_after, rotate_max_bytes) = kestrel::cli::sa_demo::resolve_rotation_policy(
                d.rotate_after_records,
                d.rotate_max_bytes,
            );
            return kestrel::cli::sa_demo::run_demo_sa(kestrel::cli::sa_demo::DemoSaArgs {
                bus_socket: shellexpand_home(&d.bus_socket),
                task: d.task,
                journal: d.journal,
                artifacts: d.artifacts_dir,
                per_dispatch_cost: d.nominal_cost,
                max_cost: d.max_cost,
                currency: d.currency,
                max_iterations: 3,
                // 与 max_iterations 同为编译期默认值（demo 的两个上限都不是钱的上限，
                // 钱由 --max-cost 管）：回跳额度见 design §9.32④。
                max_reworks: 2,
                bell_timeout: Duration::from_secs(d.bell_timeout_secs),
                worktree: d.worktree,
                build_command: d.build_command,
                trusted_build_prefixes: d.trusted_build_prefixes,
                approvals: d.approvals,
                rotate_after,
                rotate_max_bytes,
                task_id: d.task_id,
                rd_peer: d.rd_peer,
                qa_peer: d.qa_peer,
                roles_dir: d.roles_dir,
                prices: d.prices,
                evidence_spec: d.evidence_spec,
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

/// The one sentence `journal-status --check` refuses with. `--json` and the
/// human report call the same helper, because coverage gaps joined
/// `is_healthy()` on one branch only and `--check` went on refusing while
/// blaming duplicates and residue — a message that contradicted its own verdict.
fn unhealthy_report(status: &JournalStatus) -> String {
    let verified = match &status.verification {
        Some(verification) => format!("{} verify problem(s)", verification.problems.len()),
        // `--check` without `--verify` really has not looked at the records;
        // saying so beats implying a clean bill of health.
        None => "record-level verify not run".to_string(),
    };
    format!(
        "journal status is unhealthy: {} duplicate range(s), {} coverage gap(s), {} staged \
         residue file(s); {verified}",
        status.duplicate_segments,
        status.coverage_gaps.len(),
        status.staged_files
    )
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

fn parse_trusted_build_prefix(value: &str) -> Result<String, String> {
    let prefix = value.trim();
    if prefix.is_empty() {
        return Err("--trusted-build-prefix must not be empty".to_string());
    }
    if prefix.chars().any(char::is_control) {
        return Err(
            "--trusted-build-prefix must be one command prefix, not a control sequence".to_string(),
        );
    }
    Ok(prefix.to_string())
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
    use super::{
        Cli, JournalStatus, parse_approve, parse_trusted_build_prefix, unhealthy_report,
        with_role_deadline,
    };
    use clap::Parser;
    use kestrel::config::RoleConfig;
    use kestrel::journal::CoverageGap;
    use kestrel::tools::ApproveMode;

    /// `--check` must name every signal that made the journal unhealthy — the
    /// same sentence in both output modes.
    #[test]
    fn check_message_names_the_coverage_gap() {
        let status = JournalStatus {
            live_records: 0,
            live_bytes: 0,
            root_segments: 1,
            archived_segments: 0,
            duplicate_segments: 0,
            total_segment_bytes: 0,
            staged_files: 0,
            coverage_gaps: vec![CoverageGap {
                expected_from: 0,
                found_from: 5,
            }],
            segments: vec![],
            verification: None,
            approvals: None,
        };
        assert!(!status.is_healthy(), "a hole in the chain is not healthy");
        let text = unhealthy_report(&status);
        assert!(text.contains("1 coverage gap(s)"), "{text}");
        assert!(text.contains("0 duplicate range(s)"), "{text}");
        assert!(
            text.contains("verify not run"),
            "an un-run record check must not read as a passed one: {text}"
        );

        let verified = JournalStatus {
            verification: Some(kestrel::journal::Verification {
                chain_end: 9,
                checkpoint_covers: Some(10),
                segments: vec![],
                problems: vec!["the live checkpoint says 10, the chain ends at 9".into()],
            }),
            ..status.clone()
        };
        assert!(
            !verified.is_healthy(),
            "a verified disagreement is still unhealthy"
        );
        assert!(
            unhealthy_report(&verified).contains("1 verify problem(s)"),
            "{verified:?}"
        );
    }

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

    #[test]
    fn trusted_build_prefixes_are_trimmed_and_must_be_one_line() {
        assert_eq!(
            parse_trusted_build_prefix("  zig build  ").unwrap(),
            "zig build"
        );
        assert!(parse_trusted_build_prefix("   ").is_err());
        assert!(parse_trusted_build_prefix("zig build\nrm -rf /").is_err());
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
