# kestrel

为星型多 Agent harness 写的 Rust harness kernel：引擎 ~2k 行 + 诚实 wire 层 ~1.5k + 权限/沙箱 ~2.5k + 会话 ~0.8k + 总线/journal ~1.5k。设计文档（中文）见本仓库 `docs/design.md`。

Status: M0-M4 complete（137 tests / clippy -D warnings clean）

## Quick start

```bash
# 注册 provider（一次性）：~/.kestrel/providers.toml
#   [providers.deepseek-anthropic]
#   base_url = "https://api.deepseek.com/anthropic"
#   api = "anthropic-messages"
#   key = "$DEEPSEEK_KEY"
#   [[providers.deepseek-anthropic.models]]
#   id = "deepseek-v4-flash"

cargo run -p kestrel -- -p "list files and summarize" \
  --provider deepseek-anthropic --model deepseek-v4-flash --tools

# 直连模式（不建 registry）
cargo run -p kestrel -- -p "hi" --base-url https://api.anthropic.com \
  --api-key "$ANTHROPIC_API_KEY" --model claude-sonnet-5

# 续接既有会话（append-only 树 + 无损压缩只在跨回合时才可能触发）
cargo run -p kestrel -- -p "continue the previous task" \
  --provider deepseek-anthropic --model deepseek-v4-flash --tools --resume last
```

## Build & test

```bash
cargo test          # 137 项；沙箱 e2e 需 macOS（真实 /usr/bin/sandbox-exec）
cargo fmt --check && cargo clippy --all-targets -- -D warnings   # 质量闸门
```

## Demo（星型三角色联跑）

```bash
cargo build && ./harness/run-demo.sh      # hub + RD + QA + demo-sa 四进程
```

可调项（env 或 `demo-sa` flag）：`--bell-timeout-secs`（等回铃，默认 300）、
`--max-usd`（预算停损，默认 6.0 = 4 stage × 3 iter × 0.5）、`--worktree`
（RD 的写入目录，广播进每颗门铃的 `ref`，QA 据此取证）。SA 的判定只读盘：
worker 文字结论里的 `PASS`/`DONE` 才算收敛，`FAIL`/空答案/无判定词一律回派。

## Configuration

三文件表：`~/.kestrel/providers.toml`（provider/key 唯一归属地，key 三态 `literal`/`$ENV`/`!cmd`）→ `harness/roles/<role>.toml`（权限/沙箱/提示词/预算）→ `<project>/.kestrel/kestrel.toml`（仅权限引用；**禁止定义 providers**）。优先级 runtime > role > project > user > env（详见 design.md §6）。

工具审批同样按这条优先级：`--approve all` > role 的 `[permissions].approve` >
`none`（拒绝）。`serve` 不再硬编码拒绝，worker 也能走「沙箱拒绝 → 审批 → 脱沙箱重试」。

## Sessions

每次运行落一个 append-only JSONL 树（`~/.kestrel/sessions/<cwd-slug>/`）。`-p`
默认新建，`--resume last|<session-id>|<path>` 续接；`serve` 一个 worker 一棵树、
一个常驻引擎——门铃是一个接一个的回合，历史、read-before-edit 台账与压缩检查点
跨派发存活。

## Module map

```
src/
  core/       actor(Op/Event) + run_turn + steering + 取消树        # 引擎
  provider/   wire trait + anthropic/openai_chat + compat + partial_json + retry + registry
  tools/      bash/read/write/edit + orchestrator(并行门闸) + 审批反向请求
  permissions/ 三态前缀表 + word-only tokenizer（fail-closed）
  sandbox/    Seatbelt 包裹（4 sbpl 模板）+ denial 检测 + 升级循环
  session/    append-only 树 JSONL + 无损 compaction（firstKeptEntryId）
  bus/        帧协议(4B 长度+strict 信封) + 哑 RPC 路由 + sender identity
  journal/    write-ahead 真相源 + 崩溃对账 + 预算账本
  cli/        -p 单发 / serve 常驻 worker / demo-sa 脚本化 SA / hub 总线进程
  config/     角色加载 + KESTREL_HOME
harness/      三角色 demo：roles/*.toml + systems/*.md + run-demo.sh
tests/        fixtures/*.sse|*.jsonl|*.events.jsonl + 验收测试 + 纪律闸门
```

## Design

- 设计文档（中文）：`docs/design.md`
- 测试报告：`docs/smoke-report.md`
- 实现规划与调研语料（codex / pi study notes、三方资料库）属开发过程资料，未随本仓库发布

## Non-goals

TUI / MCP / 插件 / OAuth / Linux 沙箱 / 全 family provider 通用性 / prompt 级权限——二期清单见 design.md §10。
