# kestrel

为星型多 Agent harness 写的 Rust harness kernel：引擎 + wire 层 + 工具/权限/沙箱 + 会话 + 总线/journal + CLI/配置。设计文档（中文）见本仓库 `docs/design.md`。

**Metrics** — src 51 files / 14968 lines · tests 9 files / 3077 lines · 224 tests (lib 174 + bin 4 + integration 46)

Status: M0-M4 complete + 二期开工（`ApproveMode::Bus`、SA 判定含证据核对、bus 停车补投与自动重连、journal 快照/截断与在线投影、多任务 task 命名空间、openai-responses wire、worker 池、预算按 token 结算的价格表、journal 轮转的默认策略、按类型核对的证据与 stage 要求表、审批请求的 at-least-once 投递与按 req 幂等）——fmt / clippy `-D warnings` / test 三闸门全绿（人工执行：本仓库不挂 CI）。上面那行数字由 `tests/discipline.rs` 与源码树对照校验：改完代码不更新它，闸门就红。

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
cargo test          # 数量见上方 Metrics 行；沙箱 e2e 需 macOS（真实 /usr/bin/sandbox-exec）
cargo fmt --check && cargo clippy --all-targets -- -D warnings   # 质量闸门
```

> 没有 CI 兜底：上面两条命令是**每次改动都要人肉跑的闸门**，跑不过就不算完成。
> 总线测试要在真 UDS 上 bind socket：容器/严格沙箱里会以 `EPERM` 失败，不是回归。

## Demo（星型三角色联跑）

```bash
cargo build && ./harness/run-demo.sh      # hub + RD + QA + demo-sa 四进程
```

可调项（env 或 `demo-sa` flag）：`--bell-timeout-secs`（等回铃，默认 300）、
`--max-usd`（停损上限，默认 6.0）、`--roles-dir`/`--prices`（预算价格表，见下）、
`--evidence-spec`（每个 stage 要哪类证据，见下）、
`--worktree`（RD 的写入目录，广播进每颗门铃的 `ref`，QA 据此取证；`KESTREL_WORKTREE`
同时喂给 role 的 `write_roots`）、`--approvals`（SA 的审批授权表，见下）。

### 预算：账单还是名义值

`--max-usd` 管的是钱，但钱从哪来要说得出口。账本查两张表：`--roles-dir` 里的 role
文件给出「哪个 peer 跑在哪个 provider/model 上」，`providers.toml` 的
`[[providers.<id>.models]] cost` 给出「那个 model 每百万 token 多少钱」。**两端都有**
=> 派发前先按该模型的最坏情况（整窗输入 + 满 `max_out` 输出）预留额度做停损，结果铃
回来后按实际 token 结算换掉预留；**任何一端缺席** => 每次派发记 `--nominal-usd`
（默认 0.5）的名义值，SA 开工即印出哪些 peer 没有价，绝不把「没人填价」记成「这次免费」。
`priced` 标记与金额一起落 journal，所以事后能复现「这笔钱当时按什么算」——这是不变式 5
（journal 是真相）对钱的延伸。把 `cost` 写成 `{input = 0.0, output = 0.0}` 不算「免费」，
等同没填：按 0 记账的停损等于没有停损。

SA 的判定只读盘，且分三层：通路（有答案、未 abort、worker 侧 stop 非 error）→
语义（文字结论里的 `PASS`/`DONE`）→ **证据**（工件 `evidence` 数组点名的绝对路径必须
在盘上存在且有内容）。`FAIL`/空答案/无判定词/给不出可查文件，一律回派。

### 证据的类型化

「有个文件」只证明有东西。worker 可以按类型声明证据：`EVIDENCE test_log /abs/path/pytest.log`
（可用类型 `file` / `test_log` / `diff` / `screenshot`，缺省 `file`），而类型是 **SA 能在盘上
反驳的那句话**：`test_log` 里要能找到测试汇总行（`test result:` / `passed` / `Tests:` …），
`diff` 要有 `diff --git` 或 `--- / +++ / @@`，`screenshot` 的文件头要真是 PNG/JPEG/GIF/WebP
且不小于 1 KiB。认不出的类型名不猜——它连 `file` 档都不算。

`--evidence-spec` 指向的表把「这一关要哪类证据」变成数据：`{"test": ["test_log"], "dev": ["file"]}`。
要求看的是**核对通过**的类型，所以一份普通文件顶不出 `test_log` 那一格。表读不出来或要求了
认不出的类型 => **启动即失败**（坏表当成「没有要求」就是 fail-open；永远满足不了的要求会让
pipeline 无限回派）。`harness/roles/sa.evidence.json` 缺省四个 stage 都只要 `file`，也就是
类型化之前的语义——收紧哪一格是定标准的人的决定，不由 demo 替它默认。QA 写范围为零这条纪律
没动：它指认盘上已有的那份，不需要为了「声明过」而先造一个文件。

## Configuration

三文件表：`~/.kestrel/providers.toml`（provider/key/费率 的唯一归属地，key 三态 `literal`/`$ENV`/`!cmd`）→ `harness/roles/<role>.toml`（权限/沙箱/提示词/后端/审批人；路径支持 `$VAR` 与 `$VAR:-fallback`）→ `<project>/.kestrel/kestrel.toml`（仅权限引用；**禁止定义 providers**）。优先级 runtime > role > project > user > env（详见 design.md §6）。预算上限不在 role 里声明——由 SA 的 journal 账本执行；单价跟着 provider 住，同一条「唯一归属地」纪律。

工具审批按同一条优先级：`--approve all|none|bus` > role 的 `[permissions].approve` >
`none`（拒绝）。`bus` 是反向请求：worker 把审批请求（带 tool + args + 可判定的命令）
发给 role 的 `[permissions].approver`（缺省 = 敲这颗铃的一方，即 SA），SA 只按自己的
授权表（`harness/roles/sa.approvals.json`，与 role policy 同格式的三态前缀表）判定——
**只有显式 allow 才放行**，Ask/Deny/无表/命令为空一律 deny；`approve_timeout_secs`
（默认 120s）内无应答同样 deny，审批不会把回合挂死。请求与裁决都先落 journal
（`approval_requested` / `approval_decided`），「谁放宽了沙箱」崩溃后可复现。
`-p` 单发模式没有总线可问：`approve = "bus"` 在启动时直接报错，不静默降级。

**沉默**和**没送到**是两件事（design.md §9.30）。总线故意不重放「写进随后死掉的 socket」的帧
（§9.21），所以审批请求在应用层按 at-least-once 投递：worker 把在途请求连同帧本体一起留着，
按窗口的三分之一重发**同一个 `req` id**（节奏在 `ApproveMode::approval_retry_period()`，上限
`MAX_APPROVAL_SENDS = 3`——hub 的停车窗按 session 计数，猛发会把别人的帧挤掉）。审批者按 `req`
幂等：答过的请求原样重发上次那个批复，不重判、也不再落第二对记录；SA 重启后台账从 journal
播种，仍然认得「这个 req 我答过了」。worker 也去重：同一个 `req` 的第二份 `allow` 只记一次、
只唤醒一次工具。语义没动——到点仍是 deny，只是那现在意味着「问了三次都没人答」。也没假装做全：
批复**迟到**（回合结束即关闭在途请求）仍停在超时 deny，跨进程重启的 ack 仍是 §10 上的一格。

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

活文件有**默认上限**：512 条记录 / 1 MiB（`demo-sa --rotate-after-records N` /
`--rotate-max-bytes B`；没传 = 用默认，传 `0` = 明确关掉那一个条件）。触顶时把较旧的前缀
折成一个 `snapshot` 检查点并搬进归档段 `journal.seg<from>-<to>.jsonl`（名字就是它覆盖的记录
区间），活文件只剩「检查点 + 最近若干条」，于是长期在上限的 1/2~1 之间浮动。折叠只发生在
**stage 结算边界**，判据是两个 O(1) 读（投影条数 + metadata 字节数）。

阈值不是猜派发节奏，而是给「崩溃对账与丢铃巡检要从活文件里读多少」定一条上界：一次完整派发
约 7 条记录，512 条 ≈ 最近七十次；字节上限是因为记录大小不齐（`approval_requested` 能带着
stderr）。检查点里带着**未结算的铃**与每个 peer 的花费（含未结算的预算预留），所以巡检、崩溃
对账与预算停损的结论与不截断时完全一致——`the_default_rotation_policy_changes_no_conclusion`
就是拿两条 journal 跑同一条 pipeline 逐 stage 对照这件事。完整审计读 `replay_all()`（段 + 活文件）。
轮转顺序是崩溃安全的（归档 fsync → 新活文件 fsync → rename 原子替换），任何一步断电，盘上都还有
一份可读的 journal。**这不是删除**：段文件不自动清理，session 树也永不截断。

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
  tools/      bash/read/write/edit + orchestrator(并行门闸 + 审批反向请求/超时/重发节奏)
  permissions/ 三态前缀表 + word-only tokenizer（fail-closed）
  sandbox/    Seatbelt 包裹（4 sbpl 模板）+ denial 检测 + 升级循环
  session/    append-only 树 JSONL + 无损 compaction（firstKeptEntryId）
  bus/        帧协议(4B 长度+strict 信封) + 哑 RPC 路由 + sender identity
              + 离线 session 停车补投 / 客户端自动重连（RPC 不跨重连）
  journal/    write-ahead 真相源 + 崩溃对账 + 在线投影 + 预算/用量/审批账本
              + snapshot 检查点与归档段轮转（截断不丢未结算的铃）
  cli/        -p 单发 / serve 常驻 worker / demo-sa 脚本化 SA / hub 总线进程
              + pool.rs（worker 池：按 task 路由，一 task 一引擎一树；在途审批请求的
                at-least-once 重发也在这层：按 req 去重、重发的是同一份帧）
              + evidence.rs（证据契约：类型化声明 + 按类型在盘上核对 + stage 要求齐全）
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
