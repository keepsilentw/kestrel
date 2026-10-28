//! Worker pool acceptance (§10 多任务并行): one `serve` process hosting one
//! engine per task, driven through the real bus with faux wires.
//!
//! 三条断言是这一项的实质：两个 task 各自落自己的工件、各自只看见自己的历史、
//! 超过上限时被显式拒绝（而不是排在一个看不见的队列里）。

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kestrel::bus::rpc::{BusClient, BusHub};
use kestrel::cli::pool::{EngineFactory, PoolConfig, PooledEngine, run_pool};
use kestrel::core::actor::{AgentHandle, EngineConfig};
use kestrel::core::id::SeqGen;
use kestrel::provider::faux::{FauxWire, stop_script};
use kestrel::provider::wire::{ModelMeta, NullTransport, ResolvedAuth, WireId};
use kestrel::sandbox::{FsPolicy, Unsandboxed};
use serde_json::{Value, json};

fn temp(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("kestrel-pool-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Harness {
    sa: BusClient,
    artifacts: std::path::PathBuf,
    wires: Arc<Mutex<HashMap<String, Arc<FauxWire>>>>,
    pool: tokio::task::JoinHandle<()>,
}

async fn harness(max_tasks: usize, tag: &str) -> Harness {
    let dir = temp(tag);
    let socket = dir.join("bus.sock");
    let artifacts = dir.join("artifacts");
    std::fs::create_dir_all(&artifacts).unwrap();
    // kept alive for the whole harness: dropping the hub would drop its accept task
    let _hub = BusHub::bind(&socket).unwrap();

    let worker = Arc::new(
        BusClient::connect(&socket, "harness-rd", Some("harness-rd"))
            .await
            .unwrap(),
    );
    let sa = BusClient::connect(&socket, "harness-sa", None)
        .await
        .unwrap();

    let wires: Arc<Mutex<HashMap<String, Arc<FauxWire>>>> = Arc::new(Mutex::new(HashMap::new()));
    let seen = Arc::clone(&wires);
    let factory: EngineFactory = Arc::new(move |task: &str| {
        let wire = Arc::new(FauxWire::new());
        // every task answers with its own name, so a crossed history is visible
        wire.push_turn_script(stop_script(&format!("answer for {task}")));
        seen.lock()
            .unwrap()
            .insert(task.to_string(), Arc::clone(&wire));
        let handle = AgentHandle::spawn(EngineConfig {
            provider: wire,
            model: ModelMeta::minimal("faux", WireId::AnthropicMessages, "faux://unused"),
            auth: ResolvedAuth::key("test"),
            transport: Arc::new(NullTransport),
            system: "role".into(),
            tools: vec![],
            max_rounds: 8,
            cwd: std::env::temp_dir(),
            role: "rd".into(),
            idgen: Arc::new(SeqGen::new()),
            fs: FsPolicy::default(),
            sandbox: Arc::new(Unsandboxed),
            engine: Arc::new(kestrel::permissions::PermissionEngine::new(
                Default::default(),
            )),
            approve: kestrel::tools::ApproveMode::HeadlessDeny,
            session: None,
        });
        let events = handle.take_event_rx().expect("single consumer");
        Ok(PooledEngine { handle, events })
    });

    let cfg = Arc::new(PoolConfig {
        session: "harness-rd".into(),
        role: "rd".into(),
        artifacts_dir: artifacts.clone(),
        approver: None,
        max_tasks,
    });
    let pool = tokio::spawn(async move {
        if let Err(e) = run_pool(worker, cfg, factory).await {
            println!("[debug] run_pool ended: {e:?}");
        }
    });
    // let the pump reach its first await on the bus
    tokio::time::sleep(Duration::from_millis(100)).await;
    Harness {
        sa,
        artifacts,
        wires,
        pool,
    }
}

fn bell(task: &str, stage: &str, text: &str) -> Value {
    json!({
        "role": "rd", "stage": stage, "iteration": 1,
        "task": text, "task_id": task, "ref": { "worktree": "/tmp/wt" }
    })
}

async fn read_artifact(dir: &std::path::Path, name: &str) -> Value {
    for _ in 0..80 {
        if let Ok(text) = std::fs::read_to_string(dir.join(name))
            && let Ok(v) = serde_json::from_str::<Value>(&text)
        {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no artifact {name} in {}", dir.display());
}

/// Two tasks, one process: distinct artifacts, distinct histories, both result
/// bells carry their own task id.
#[tokio::test]
async fn two_tasks_run_side_by_side_in_one_worker() {
    let h = harness(4, "two-tasks").await;
    h.sa.notify("harness-rd", bell("t1", "dev", "build the lru"));
    h.sa.notify("harness-rd", bell("t2", "review", "review the lru"));

    println!("[debug] pool finished? {}", h.pool.is_finished());
    let listing: Vec<String> = std::fs::read_dir(&h.artifacts)
        .map(|rd| {
            rd.flatten()
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect()
        })
        .unwrap_or_default();
    println!("[debug] artifacts: {listing:?}");
    let counts: Vec<(String, usize)> = h
        .wires
        .lock()
        .unwrap()
        .iter()
        .map(|(k, w)| (k.clone(), w.recorded_requests().len()))
        .collect();
    println!("[debug] per-task request counts: {counts:?}");
    let mut seen_tasks = Vec::new();
    for _ in 0..2 {
        let bell = tokio::time::timeout(Duration::from_secs(5), h.sa.recv_notify())
            .await
            .expect("no result bell")
            .unwrap();
        seen_tasks.push(bell.payload["task_id"].as_str().unwrap().to_string());
        assert!(
            bell.payload["result_ref"].is_string(),
            "a result bell must point at the artifact: {}",
            bell.payload
        );
    }
    seen_tasks.sort();
    assert_eq!(seen_tasks, vec!["t1".to_string(), "t2".to_string()]);

    let a1 = read_artifact(&h.artifacts, "harness-rd-t1-dev-iter1.json").await;
    let a2 = read_artifact(&h.artifacts, "harness-rd-t2-review-iter1.json").await;
    assert_eq!(a1["result"], "answer for t1", "{a1}");
    assert_eq!(a2["result"], "answer for t2", "{a2}");
    assert_eq!(a1["usage"]["output_tokens"], 5, "usage booked per task");

    // history isolation: each engine saw exactly its own bell
    let wires = h.wires.lock().unwrap();
    for (task, wire) in wires.iter() {
        let requests = wire.recorded_requests();
        let text = serde_json::to_string(&requests).unwrap();
        assert_eq!(requests.len(), 1, "{task} got exactly one turn");
        assert!(
            text.contains(if task == "t1" {
                "build the lru"
            } else {
                "review the lru"
            }),
            "{task} should see its own task text: {text}"
        );
        assert!(
            !text.contains(if task == "t1" {
                "review the lru"
            } else {
                "build the lru"
            }),
            "{task} must not see the other task's bell: {text}"
        );
    }
    drop(wires);
    h.pool.abort();
}

/// Over the task ceiling the worker REFUSES with a visible error bell: silently
/// queueing a third pipeline behind two engines would look like a lost bell to
/// the SA and burn its retry budget.
#[tokio::test]
async fn over_the_task_cap_a_bell_is_refused_out_loud() {
    let h = harness(1, "cap").await;
    h.sa.notify("harness-rd", bell("only", "dev", "first task"));
    let done = tokio::time::timeout(Duration::from_secs(5), h.sa.recv_notify())
        .await
        .expect("no result bell for the accepted task")
        .unwrap();
    assert_eq!(done.payload["task_id"], "only");

    h.sa.notify("harness-rd", bell("extra", "dev", "second task"));
    let refused = tokio::time::timeout(Duration::from_secs(5), h.sa.recv_notify())
        .await
        .expect("no refusal bell")
        .unwrap();
    assert_eq!(refused.payload["task_id"], "extra");
    assert!(
        refused.payload["error"]
            .as_str()
            .unwrap_or_default()
            .contains("cap"),
        "the refusal says why: {}",
        refused.payload
    );
    assert!(
        refused.payload.get("result_ref").is_none(),
        "a refusal is not a result"
    );
    h.pool.abort();
}

/// A bell with no `task_id` is not an error: it lands in the default namespace,
/// so a single-pipeline SA (which predates task ids) keeps working.
#[tokio::test]
async fn a_bell_without_a_task_id_uses_the_default_namespace() {
    let h = harness(4, "default-task").await;
    let mut payload = bell("ignored", "dev", "legacy bell");
    payload.as_object_mut().unwrap().remove("task_id");
    h.sa.notify("harness-rd", payload);
    let done = tokio::time::timeout(Duration::from_secs(5), h.sa.recv_notify())
        .await
        .expect("no result bell")
        .unwrap();
    assert_eq!(done.payload["task_id"], "default");
    let artifact = read_artifact(&h.artifacts, "harness-rd-default-dev-iter1.json").await;
    assert_eq!(artifact["result"], "answer for default");
    h.pool.abort();
}
