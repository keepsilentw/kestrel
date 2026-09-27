# kestrel

为星型多 Agent harness 写的 Rust harness kernel：引擎 ~2k 行 + 诚实 wire 层 ~1.5k + 权限/沙箱 ~2.5k + 会话 ~0.8k + 总线/journal ~1.5k。设计文档（中文）见本仓库 `docs/design.md`。

Status: M0-M4 complete（121 tests / clippy -D warnings clean）

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
```

## Build & test

```bash
cargo test          # 100 项；沙箱 e2e 需 macOS（真实 /usr/bin/sandbox-exec）
cargo fmt --check && cargo clippy --all-targets -- -D warnings   # 质量闸门
```

## Configuration

三文件表：`~/.kestrel/providers.toml`（provider/key 唯一归属地，key 三态 `literal`/`$ENV`/`!cmd`）→ `harness/roles/<role>.toml`（权限/沙箱/提示词/预算）→ `<project>/.kestrel/kestrel.toml`（仅权限引用；**禁止定义 providers**）。优先级 runtime > role > project > user > env（详见 design.md §6）。

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
