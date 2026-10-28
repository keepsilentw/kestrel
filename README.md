# kestrel

为星型多 Agent harness 写的 Rust harness kernel：引擎 1.1k 行 + wire 层 2.6k + 工具/权限/沙箱 2.3k + 会话 0.9k + 总线/journal 1.9k + CLI/配置 2.7k（`src` 49 文件 11.4k 行，`tests` 8 文件 2.3k 行）。设计文档（中文）见本仓库 `docs/design.md`。

Status: M0-M4 complete + 二期开工（`ApproveMode::Bus`、SA 判定含证据核对、bus 停车补投与自动重连、journal 快照/截断与在线投影、多任务 task 命名空间、openai-responses wire、worker 池）——188 tests，fmt / clippy `-D warnings` / test 三闸门全绿（人工执行：本仓库不挂 CI）

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
cargo test          # 188 项；沙箱 e2e 需 macOS（真实 /usr/bin/sandbox-exec）
cargo fmt --check && cargo clippy --all-targets -- -D warnings   # 质量闸门
```

> 没有 CI 兜底：上面两条命令是**每次改动都要人肉跑的闸门**，跑不过就不算完成。
> 总线测试要在真 UDS 上 bind socket：容器/严格沙箱里会以 `EPERM` 失败，不是回归。

## Demo（星型三角色联跑）

```bash
cargo build && ./harness/run-demo.sh      # hub + RD + QA + demo-sa 四进程
```

可调项（env 或 `demo-sa` flag）：`--bell-timeout-secs`（等回铃，默认 300）、
`--max-usd`（停损上限，默认 6.0 = 4 stage × 3 iter × 0.5；`usd` 是本 fixture 的**名义**
计费单位，每次派发固定 0.5，与 token 无关——真实 token 另记进 `usage_reported`）、
`--worktree`（RD 的写入目录，广播进每颗门铃的 `ref`，QA 据此取证；`KESTREL_WORKTREE`
同时喂给 role 的 `write_roots`）、`--approvals`（SA 的审批授权表，见下）。

SA 的判定只读盘，且分三层：通路（有答案、未 abort、worker 侧 stop 非 error）→
语义（文字结论里的 `PASS`/`DONE`）→ **证据**（工件 `evidence` 数组点名的绝对路径必须
在盘上存在且有内容）。`FAIL`/空答案/无判定词/给不出可查文件，一律回派。

## Configuration

三文件表：`~/.kestrel/providers.toml`（provider/key 唯一归属地，key 三态 `literal`/`$ENV`/`!cmd`）→ `harness/roles/<role>.toml`（权限/沙箱/提示词/后端/审批人；路径支持 `$VAR` 与 `$VAR:-fallback`）→ `<project>/.kestrel/kestrel.toml`（仅权限引用；**禁止定义 providers**）。优先级 runtime > role > project > user > env（详见 design.md §6）。预算不在 role 里声明——上限由 SA 的 journal 账本执行。

工具审批按同一条优先级：`--approve all|none|bus` > role 的 `[permissions].approve` >
`none`（拒绝）。`bus` 是反向请求：worker 把审批请求（带 tool + args + 可判定的命令）
发给 role 的 `[permissions].approver`（缺省 = 敲这颗铃的一方，即 SA），SA 只按自己的
授权表（`harness/roles/sa.approvals.json`，与 role policy 同格式的三态前缀表）判定——
**只有显式 allow 才放行**，Ask/Deny/无表/命令为空一律 deny；`approve_timeout_secs`
（默认 120s）内无应答同样 deny，审批不会把回合挂死。请求与裁决都先落 journal
（`approval_requested` / `approval_decided`），「谁放宽了沙箱」崩溃后可复现。
`-p` 单发模式没有总线可问：`approve = "bus"` 在启动时直接报错，不静默降级。

`api` 三值：`anthropic-messages` / `openai-completions` / `openai-responses`。第三条是 §10 二期补上的 wire（`/v1/responses` + `response.*` 事件族），只有 fixture 级验证，**尚未真端点实测**（见 design.md §9.24）。`--base-url` 直连模式仍固定用 anthropic wire。

## Sessions

每次运行落一个 append-only JSONL 树（`~/.kestrel/sessions/<cwd-slug>/`）。`-p`
默认新建，`--resume last|<session-id>|<path>` 续接。`serve` 的归属单位是 task：一个
进程为每个 task 各起一台常驻引擎、各开一棵树，门铃是那台引擎上一个接一个的回合——
历史、read-before-edit 台账与压缩检查点跨派发存活，但**不跨 task 串台**。

## Bus

`hub` 是唯一的总线进程，其余角色各连一条 UDS。断线处理是有限度的诚实：worker
自动重连（100ms 起退避，单窗口 10s）；hub 对「注册过但不在线」的 session 把
Notify 停车（每 session 32 帧、全局 256 帧，drop-oldest 并 warn），等它重新 Hello
时按序补投；从未注册过的地址立刻回 `unknown session`。RPC 与「已经写进随后死掉的
socket」的帧**不重放**——重复铃由 SA 侧的 write-ahead 对账（journal + 读盘巡检）
消化，而不是靠总线猜「大概没送到」。

**多任务并行**：一个 hub 上可以并存多条 pipeline。用带前缀的 session 地址隔开路由
（`serve --session t2:harness-rd`），再用 `demo-sa --task-id t2 --rd-peer
t2:harness-rd --journal t2.jsonl` 告诉 SA 打哪一对 worker；task id 会盖进 journal
记录、门铃、工件名与结果铃四处，命名契约由 `cli/task.rs` 一处算出，两端不会各拼一遍。
`serve` 的并行单位是 task：`--max-tasks`（缺省 4）之内，每个 task 一台常驻引擎、一棵
会话树，task 之间真并发。超过上限的门铃被**显式拒绝**（回一颗 error 铃），不排在看不
见的队列里——静默排队在 SA 眼里就是「铃丢了」。单活跃槽没动：它是取消、steering 与
压缩时序的地基，并行靠加引擎而不是放宽槽位。

## Journal 截断

`demo-sa --rotate-after-records N` / `--rotate-max-bytes B`（都默认 0 = 不截断）：活文件
超过 N 条记录或 B 字节时，把
较旧的前缀折成一个 `snapshot` 检查点并搬进归档段 `journal.seg<from>-<to>.jsonl`（名字就是
它覆盖的记录区间），活文件
只剩「检查点 + 最近若干条」。检查点里带着**未结算的铃**和每个 peer 的花费，所以巡检、
崩溃对账与预算停损的结论与不截断时完全一致；完整审计读 `replay_all()`（段 + 活文件）。
轮转顺序是崩溃安全的（归档 fsync → 新活文件 fsync → rename 原子替换），任何一步断电，
盘上都还有一份可读的 journal。**这不是删除**：段文件不自动清理，session 树也永不截断。

投影是在线维护的：`Journal` 每追加一条就把折叠推进一步，`projection()`（stage /
iteration / 每 peer 花费 / 未结算的铃）、`live_records()`、`file_bytes()` 都是 O(1)，
所以预算停损不再每次派发重放整个文件。在线与重放由**同一个** `Folder` 折叠产生，
测试逐条断言两者相等——否则「停损读缓存、巡检读重放」会分裂成两份真相。

## Module map

```
src/
  core/       actor(Op/Event) + run_turn + steering + 取消树        # 引擎
  provider/   wire trait + anthropic / openai_chat / openai_responses + compat + partial_json
              + retry + registry
  tools/      bash/read/write/edit + orchestrator(并行门闸 + 审批反向请求/超时)
  permissions/ 三态前缀表 + word-only tokenizer（fail-closed）
  sandbox/    Seatbelt 包裹（4 sbpl 模板）+ denial 检测 + 升级循环
  session/    append-only 树 JSONL + 无损 compaction（firstKeptEntryId）
  bus/        帧协议(4B 长度+strict 信封) + 哑 RPC 路由 + sender identity
              + 离线 session 停车补投 / 客户端自动重连（RPC 不跨重连）
  journal/    write-ahead 真相源 + 崩溃对账 + 在线投影 + 预算/用量/审批账本
              + snapshot 检查点与归档段轮转（截断不丢未结算的铃）
  cli/        -p 单发 / serve 常驻 worker / demo-sa 脚本化 SA / hub 总线进程
              + pool.rs（worker 池：按 task 路由，一 task 一引擎一树）
              + evidence.rs（证据契约：从结论提路径 + 盘上核对）
              + task.rs（task 命名空间：工件名契约 + slug 防路径穿越）
  config/     角色加载（含 $VAR 路径展开）+ KESTREL_HOME
harness/      三角色 demo：roles/*.toml + roles/sa.approvals.json + systems/*.md + run-demo.sh
tests/        fixtures/*.sse|*.jsonl|*.events.jsonl + 验收测试 + 纪律闸门
```

## Design

- 设计文档（中文）：`docs/design.md`
- 测试报告：`docs/smoke-report.md`
- 实现规划与调研语料（codex / pi study notes、三方资料库）属开发过程资料，未随本仓库发布

## Non-goals

TUI / MCP / 插件 / OAuth / Linux 沙箱 / 全 family provider 通用性 / prompt 级权限——二期清单见 design.md §10。
