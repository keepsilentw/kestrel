# kestrel

为星型多 Agent harness 写的 Rust harness kernel：引擎 + wire 层 + 工具/权限/沙箱 + 会话 + 总线/journal + CLI/配置。设计文档（中文）见本仓库 `docs/design.md`。

**Metrics** — src 51 files / 19338 lines · tests 9 files / 3704 lines · 269 tests (lib 211 + bin 5 + integration 53)

Status: M0-M4 complete + 二期主体落地。三期于 2026-11-10 开工：worker 池的并发执行护栏同时提供每 task 的 `--max-parallel-tools`（缺省 4）和 worker 全局的 `--max-worker-tools`（缺省 8），任一为 `0` 时显式关闭对应上限；两级限制都不改变源序回写、串行工具写锁和取消语义（design §9.46 / §9.48）。journal 的已封闭段可移入 `<journal>.segments/`，`replay_all()` 同时读取两处，移动而非删除历史（§9.47）；`journal-archive --keep/--dry-run` 与 `journal-status --json/--check/--list/--approvals` 把归档的预览、执行、库存与审批链都变成可查项。`serve --status-file` 原子发布活动 task、未决审批与各级上限（§9.52），`serve --max-rss-mb` 给整个 worker 一条内存**软**上限：peak RSS 越线后进入 drain，新 task 被显式拒绝、在跑的跑完（§9.53）；`serve --task-idle-timeout-secs` 让空闲 task 退休引擎、释放 `--max-tasks` 的 slot，同一 task 再响时重开同一棵会话树（§9.55）。名义成本分类已细到 `peer × reason`（§9.49），判定 issue 已改为自带 `Artifact | Turn` 归因的类型（§9.50）。剩下的未完成边界：正在跑的引擎的内存回收（现在只有准入闸门）、强制 kill 式硬上限、归档段压缩、跨回合审批槽位、三期真端点联跑。二期已落地 `ApproveMode::Bus`、SA 判定含证据核对、bus 停车补投与自动重连、journal 快照/截断与在线投影、多任务 task 命名空间、openai-responses wire、worker 池、预算按 token 结算的价格表、journal 轮转的默认策略、按类型核对的证据与 stage 要求表、审批请求的 at-least-once 投递与按 req 幂等、验证 FAIL 回跳写者、bash `output_path` 交出可核对的 test_log。2026-11-05 最后一次真实联跑四关一次通过，QA 切到 GLM 的 OpenAI 兼容表面后报账为 `0.1806 cny, of which 0.0000 nominal`（§9.45、smoke-report §33）。2026-11-10 的三期改动都过了 fmt / clippy `-D warnings` / test 三道人工闸门，尚未做真端点联跑。上面那行数字由 `tests/discipline.rs` 与源码树对照校验。

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
`--max-cost`（停损上限，默认 40 个本次币种）、`--currency`（名义记账的币种，默认 `usd`；
demo 传 `cny`）、`--roles-dir`/`--prices`（预算价格表，见下）、
`--evidence-spec`（每个 stage 要哪类证据，见下）、
`--worktree`（RD 的写入目录，广播进每颗门铃的 `ref`，QA 据此取证；`KESTREL_WORKTREE`
同时喂给 role 的 `write_roots`）、`--approvals`（SA 的审批授权表，见下）。worker 侧的资源
护栏走脚本已有的那套三态语义：**没设 env 就不传 flag**，交给 `serve` 的默认值 —
`KESTREL_DEMO_MAX_TASKS` / `KESTREL_DEMO_MAX_PARALLEL_TOOLS` /
`KESTREL_DEMO_MAX_WORKER_TOOLS` / `KESTREL_DEMO_MAX_RSS_MB` /
`KESTREL_DEMO_STATUS_INTERVAL`；`KESTREL_DEMO_STATUS_DIR` 设了才写状态文件，并按 session
分成 `harness-rd.json` / `harness-qa.json`。

### 预算：账单还是名义值

`--max-cost` 管的是钱，但钱从哪来要说得出口。账本查两张表：`--roles-dir` 里的 role
文件给出「哪个 peer 跑在哪个 provider/model 上」，`providers.toml` 的
`[[providers.<id>.models]] cost` 给出「那个 model 每百万 token 多少钱」。**两端都有**
=> 派发前先按该模型的最坏情况（整窗输入 + 满 `max_out` 输出）预留额度做停损，结果铃
回来后按实际 token 结算换掉预留；**任何一端缺席** => 每次派发记 `--nominal-cost`
（默认 4）的名义值，SA 开工即印出哪些 peer 没有价，绝不把「没人填价」记成「这次免费」。
**有价但读数不可信**（端点报了 output 却不报 input，BigModel 就是这个形状）=> 结算照做（预留得配对
掉），但按**名义**落账并当场打印理由：一张少掉整个输入侧的「账单」比名义值更危险，因为它看起来是
算出来的。那个字符估算只用来决定「要不要压上下文」，绝不当钱的来源（§9.36）。
这一格 2026-11-05 被走通了：同一颗模型、同一份价目表，换到它自己的 **OpenAI 兼容表面**
（`api = "openai-completions"`，`base_url` 指 `/api/paas/v4`）就报 input usage 了，报账里的名义值
当场归零（§9.45、smoke-report §33）。换 wire 不是省钱手段，是让读数配得上账单。

收尾那一行把「说不清的钱」按来源分开报（§9.37）：
`ledger total: 0.8000 cny, of which 0.7000 nominal (under-reported 0.7000)`。

**钱的单位是声明出来的，不是写在名字里的**（design §9.44）：`providers.toml` 每个
provider 可以写 `currency = "cny"`，账本读它并当场印出来——所以一份中文价目表可以
原样落进配置里，不必先换算成美元（换算一次就多一个会过期的数字，和一个没人复核的
汇率）。同一次运行里**混两种币种直接启动失败**：账本是要加总的，「2 元 + 0.3 美元」
不是一个数。没有价目表时单位来自 `--currency`（默认 `usd`）。journal 里的金额字段
因此叫 `cost` 而不是 `usd`（旧行 `usd` 仍可读，`old_usd_keyed_lines_still_replay` 钉住）。
`no rate` 是**该补价**，`under-reported` 是**该修读数或换端点**——两种「不省钱」的下一步完全不同，
不该挤在同一个数字里。
零就写零：`f64` 的 `sum()` 对**空集合**给的是 `-0.0`，所以名义账全清白时那一行一度印成
`of which -0.0000 nominal`，读起来像「倒欠了钱」。这行字的职责是让人读得懂，渲染由 `money()` 兜住：
显示精度以下按零处理，**真负数照原样印**——那是折叠出了 bug，不该被格式掩盖（§9.45④）。
`priced` 标记与金额一起落 journal，所以事后能复现「这笔钱当时按什么算」——这是不变式 5
（journal 是真相）对钱的延伸。把 `cost` 写成 `{input = 0.0, output = 0.0}` 不算「免费」，
等同没填：按 0 记账的停损等于没有停损。
收尾那行的 token 两列来自**读数本身**（`UsageReported` 参与折叠，只填用量、不动 stage 投影），
不来自结算（§9.42）：没有费率 => 没有结算 => 过去照样把一次烧掉两万多 token 的运行印成
`tokens in 0 / out 0`。结算是钱的来源，读数是用量的来源，两者分开才不会让「没算成账单」
看起来像「没花东西」。

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
pipeline 无限回派）。`harness/roles/sa.evidence.json` 把 `dev` 提到 `["file", "test_log"]`：
写者必须自己交出「真跑过测试」的盘上形状，验证者才有东西可反驳；其余三关仍只要 `file`——
收紧哪一格是定标准的人的决定。QA 写范围为零这条纪律没动：它指认盘上已有的那份，不需要为了
「声明过」而先造一个文件。

那份形状从哪来——bash 工具的 `output_path` 参数（绝对路径，内核按 write 的规则核它落在写根内、
不在只读岛，把 stdout/stderr 连 `exit:` 状态一起写进去；跑挂的那次**也**落盘，因为最想看日志的
正是这一次）。不要把 shell 重定向给他：`cmd > log` 整条在权限层的 tokenizer 里解析失败 → Ask →
被拒（fail-closed 是对的），而 `output_path` 是 I/O 形状，不是判定对象。越界是 Blocked 而不是
Ask——审批不能放宽角色的写范围。`test_log` 的汇总表认 cargo / pytest / jest / maven /
**标准库 unittest**（`Ran N tests`、`FAILED (failures=…)`）：demo 的沙箱 `network = none`，
第三方 runner 装不上，这条不是可选项。

`test_log` 还带**结论**：内核落的日志里有那行 `exit:`，SA 读它，也数 `N passed`/`N failed`/
`failures=`。非零退出、被信号杀死、或数出失败条数，都是一句 issue——盘上的失败可以顶回 worker
那句 `DONE`。数不出来的字段留 `None`（缺席不是 0），读出来的结论随判定一起落 journal。

结论还是**状态**：投影按 `(task, stage)` 存最后一次提交的结论（覆盖，不是追加；什么都没数
出来的提交会把它撤掉），并且跨 journal 轮转存活。两个读者是收尾报账
（`[sa] test conclusion [demo] Dev: green — exit 0, ran 27, 27 passed / 0 failed`，读投影不重读
工件）与返工铃（`ref.rework.last_test`，`null` 明说「你上关什么都没数出来」）。预算停损刻意
不读它：那本账记的是钱。

证据的**路径形状**在写侧就把住（§9.39）：`serve` 把答案折进工件时，路径 token 会在全角/半角括号、
书名号、引号处切断——`` `…/unittest.log`（其内含汇总行） `` 这种写法过去会被当成一个不存在的文件名，
把真的测试判成 `evidence missing`。切掉不是静默宽容：`evidence_warnings` 跟着工件进
`Verdict.notes` 落 journal，SA 现场打印 `note: worker-side shape warning: …`。判定端另留了一层兜底
（`resolve_claim`：字面路径不存在时只认「切掉括注后是个文件」的前缀，不认目录）。

## Configuration

三文件表：`~/.kestrel/providers.toml`（provider/key/费率 的唯一归属地，key 三态 `literal`/`$ENV`/`!cmd`）→ `harness/roles/<role>.toml`（权限/沙箱/提示词/后端/审批人；路径支持 `$VAR`、`$VAR:-fallback`，要再接子路径就写 `${VAR:-fallback}/sub`——裸写时 `:-` 拥有整条后缀）→ `<project>/.kestrel/kestrel.toml`（仅权限引用；**禁止定义 providers**）。优先级 runtime > role > project > user > env（详见 design.md §6）。预算上限不在 role 里声明——由 SA 的 journal 账本执行；单价跟着 provider 住，同一条「唯一归属地」纪律。

验证关判 FAIL 时，铃**回跳给写者**（`ref.rework` 带着失败原因与那份判定工件的路径），而不是
再派一次验证者——QA 没有写范围，再给它一颗铃只会得到同一个结论。回跳有额度（`max_reworks`，
缺省 2），用尽就显式失败，不静默放宽验收标准。

但**先归因再路由**（§9.40）：`empty result` / `worker aborted` / `worker stop:` 这类「验证者自己那一
回合没交出东西」的 FAIL，说的是 QA 的哑火，不是产物的缺陷——那种情况**原地重派 QA、不扣返工额度**。
只有产物类的 FAIL（缺文件、类型不符、要求没满足）才回跳写者。把 QA 的哑火算到 RD 头上，实测换来
两轮辩护 + 撞上限。

归因是**判定自带的字段**（`Verdict.attribution: Artifact | Turn`，§9.41），不是路由去读的措辞：
分类在判定产出的那一刻做完并随判定落 journal，改一句 issue 文案不会悄悄改掉路由；分不出来源时默认落在
`Artifact`（更贵的一侧）。「工件根本没落地」也算 `Turn`——那一关的产出没出来，不能拿去定罪被审的产物。

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

回执把最后那段模糊消掉（design.md §9.31）：worker 对**每一份**批复都回一句
`approval_receipt{req, applied, reason}`——`applied: true` 是「唤醒了还在等的工具」，
`applied: false` 是「收到了，但这格已经过期」。回执落 journal（`approval_acked`），台账因此
能分出「判过却没有回执」的批复，SA 重启时对它们各补投一次。顺带修掉一个会谎报的缺陷：
审批窗口到点时等待者曾留在台账里，后来那份批复「发送成功」，工件就把一次从未生效的批准
记成 `allow`；现在到点即撤摊、工件写 `expired / applied:false`。**没生效的批准不许记成生效**，
因为 journal 与工件会互相印证一个假结论。仍不做的事：迟到批复不会在下一次同样调用上重新
兑现（那等于把一次 allow 变成长期通行证，要放宽只能改授权表）；跨回合挂起的审批槽位仍在 §10。

同一颗 `req` 的**两张矛盾回执**按事实强弱合并，不按先到先得（§9.42）：at-least-once 让 SA 去重时
把批复重投一次，第一份唤醒了工具（工件记 `applied:true`），第二份只换回 `applied:false (no open
request)`。台账存 `req -> applied`，`true` 覆盖 `false` 并**补一条更正记录**（journal 不抹旧行），
`false` 永远不能把生效过的降级——同一句纪律的两面。矛盾的第二张仍然不落，但要把矛盾印出来：
它是「批复被投了两次」唯一的现场线索。重启播种走同一个合并函数，所以账上读回来的是最强那条。

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
会话树，task 之间真并发。每个 task 内部再用 `--max-parallel-tools`（缺省 4，`0` =
无上限）限制同时执行的工具；整个 worker 再用 `--max-worker-tools`（缺省 8，`0` = 无上限）
封顶所有 task 的工具执行总数。两级限制只收窄执行阶段，不改变串行 prepare、源序回写、
串行工具写锁或取消语义。超过 task 上限的门铃被**显式拒绝**（回一颗 error 铃），不排在
看不见的队列里——静默排队在 SA 眼里就是「铃丢了」。单活跃槽没动：它是取消、steering 与
压缩时序的地基，并行靠加引擎而不是放宽槽位。

`serve --status-file /path/worker.json --status-interval-secs 5` 会周期性原子写入运行状态：
session、role、排序后的活动 task、未决审批数、各级上限、`peak_rss_bytes` / `draining` 和
`retirements`。状态写入失败只 warn，不会拖垮 worker；状态文件是观测，不是调度真相源。

`serve --max-rss-mb N`（缺省 `0` = 不设限）给整个 worker 一条内存**软**上限：每次派铃前读
一次 `getrusage` 的 peak RSS（`ru_maxrss`，macOS 字节 / Linux KB，换算在库里做），越线就进入
drain——新 task 照旧被显式拒绝成 error 铃，已经起好的引擎继续跑完。这里刻意不用 `setrlimit`
之类的硬上限：那会把一次超限变成别人工具调用里随机的分配失败，而 drain 至少说得出「这颗铃
为什么没干成」。它是准入闸门，不是内存回收。

`serve --task-idle-timeout-secs N`（缺省 1800，`0` = 永不退休）让空闲 task 的引擎**退休**，
把 slot 还给 `--max-tasks`。退休释放引擎，不释放历史：worker 记下 task ↔ 会话树的路径，同一个
task 再响时重开**同一棵**树（history 与 read-before-edit 台账从 `prompt_snapshot()` 重建），
代价是一次引擎重启，不是记忆清零。退休是提议-批准两拍——worker 到点只递 `RetireRequested`，
只有 pump 能丢 sender，而 mpsc 会先把已排队的门铃送达再让 `recv()` 拿到 `None`；worker 自己
跳出循环就是丢铃窗口。

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

已封闭段可以手动移出 journal 根目录：

```bash
cargo run -- journal-archive --journal journal.jsonl --keep 2
```

程序把 `<stem>.seg<from>-<to>.jsonl` 原子移动到 `<stem>.segments/`。这不是删除：
`replay_all()` 会按区间同时读取根目录和归档目录；重复执行返回移动 0 个，目标同名文件
存在时会在移动任何文件之前整体报错。`--keep 2` 把最新两个已封闭段留在根目录，只移动更旧的；
缺省 `--keep 0` 移动全部。归档只治理目录，不压缩存储，磁盘占用不会因此下降。

加 `--dry-run` 可以先用同一份归档计划查看将被移动的段与冲突，不执行任何 rename：

```bash
cargo run -- journal-archive --journal journal.jsonl --keep 2 --dry-run
```

归档前后可以查看不重放历史的库存摘要：

```bash
cargo run -- journal-status --journal journal.jsonl --list
```

默认输出活文件记录数/字节数、根目录与归档段数量、段总字节和残留 `.new` 文件数；
`--list` 再逐段列出区间、位置和大小；`--json` 输出包含段明细的稳定 JSON 文档，便于监控脚本消费。
`--check` 在发现重复区间或 `.new` 残留时以非零状态退出，可直接用于健康检查。
`--approvals` 会重放审批审计记录，输出请求、裁决、回执以及 pending / unacked / applied /
stale / orphan 计数；与 JSON 输出可以组合使用。

库存里还包含**归档链完整性**：链必须从记录 0 开始、后一段的起点等于前一段的终点，
断口以 `coverage_gaps` 报出（`--check` 会把断口也算作不健康）。少一段比多一段更危险——
`replay_all()` 不会报错，它只是少读。重复区间的出口是 `journal-archive --resolve-duplicates`：
先把 `replay_all()` 不读的那一份移入 `<stem>.segments/duplicates/`，再照常移动；
仍然不删除任何字节，目标已存在则在移动任何东西之前拒绝。

`--verify` 是更硬的一层：它读记录，把段名宣称的区间、段文件里真实的条数、活文件检查点的
`covers` 三者对齐，任何一处不吻合就进 `verification.problems` 并让 `--check` 拒绝。默认不跑
（`status()` 承诺只看 metadata）；`--check` 的句子会显式说明 record-level verify not run，
不把「没查」写成「查过且干净」。

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
