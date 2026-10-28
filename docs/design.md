# kestrel 设计文档：Rust harness kernel

> 状态：M0-M4 已实现并通过测试（181/181）；§10 二期已落地七项——`ApproveMode::Bus`、
> 「判定 = 工件 + 证据文件」、bus 重连重放（停车补投 + 客户端重连）、journal 快照/截断语义、
> 多任务并行（task 命名空间 + 前缀寻址）、openai-responses wire（**真端点未实测**，§9.24）、
> journal 投影增量化与轮转触发（§9.25）。
> 质量闸门保持人工执行（本仓库不挂 CI，见 §9.19⑥）。
> 实现规模：src 50 文件 12,139 行 + tests 8 文件 2,284 行（plan 预算 ~10-13k 行内）。
> 输入：实现规划（plan.md）与调研语料（codex / pi study notes、三方资料库 references.md）——均为开发过程资料，未随本仓库发布。
> 本文沉淀实现后的最终设计；与 plan 的偏差在 §9 逐条记录。

## 1 定位与非目标

**定位**：为星型多 Agent harness（SA 协调者 + RD 写者 + QA 验证者，多进程各接异构 LLM 后端）提供的 Rust 引擎内核。无头/极简 CLI：`-p` 单发（默认模式）、`serve` 常驻 worker、`demo-sa` 脚本化 SA、`hub` 总线进程；worker 的后端/总线地址来自 role 文件（`[llm]`、`bus_socket`），CLI flag 可覆盖。

**非目标**（二期清单见 §10）：TUI、MCP、插件/扩展系统、OAuth 订阅登录、Linux 沙箱、openai-responses wire、tree-sitter-bash 命令解析、execpolicy DSL。

## 2 总体架构

```mermaid
flowchart TD
    subgraph 进程[kestrel 进程（每角色一个）]
        CLI[cli: -p / serve] --> ACTOR
        ACTOR["core actor<br/>Op/Event 双通道 + 单活跃槽"]
        ACTOR --> TURN["turn 状态机<br/>run_turn 采样循环"]
        TURN --> ORCH["tools orchestrator<br/>prepare 串行 / execute 并发<br/>RwLock 门闸 + 源序回写"]
        ORCH --> SANDBOX["sandbox: Seatbelt 包裹<br/>denial 检测 → 升级循环"]
        TURN --> PERM["permissions: 三态前缀表<br/>word-only tokenizer fail-closed"]
        TURN --> SESSION["session 树 JSONL<br/>无损 compaction"]
        TURN --> WIRE["provider wire<br/>anthropic / openai-completions / openai-responses<br/>+ compat 位 + partial-json + retry"]
        SESSION --> TREE[("session 文件<br/>~/.kestrel/sessions/")]
    end
    subgraph 总线[bus（M4）：UDS 哑 RPC]
        HUB["BusHub<br/>Hello 注册 / Notify 路由<br/>sender identity 校验"]
        JOURNAL[("journal.jsonl<br/>write-ahead 真相源")]
    end
    SA[SA 进程] -- "BellSent 先写盘再拉铃" --> JOURNAL
    SA -- Notify --> HUB --> RD[RD 进程]
    HUB --> QA[QA 进程]
    RD -. "工件写盘 + 结果回铃" .-> HUB
```

核心循环为 codex 形 actor（Op/Event 双通道、单活跃槽、token 树取消）；会话与压缩为 pi 形（append-only 树 + first_kept 无损检查点）；权限接口为 pi 形（`tool_call` 前置策略）、执行兜底为 codex 形（Seatbelt 默认包裹 + denial 升级）。

## 3 三方血统映射

| 组件 | 血统 | 蓝本锚点 |
|---|---|---|
| Op/Event actor、取消三件套、length 整批 fail | Codex | study-core-loop §1-2/§5 |
| `prepare_next_turn` 轮间快照（压缩+换模型）、steering 队列 | pi | study-pi-agent §1 |
| Tool trait prepare/execute 拆分、`tool_call` block 钩子 | pi | study-pi-infra §2 |
| RwLock 并行门闸、FuturesOrdered 源序 | Codex | study-core-loop §3 |
| Seatbelt 模板 + rename 逃逸防护 + denial 升级 | Codex | study-permissions §3/§5 |
| 三态前缀表 + 危险前缀禁令 | Codex（execpolicy 简化版） | study-permissions §2 |
| session 树 + firstKeptEntryId 无损压缩 | pi | study-pi-agent §4 |
| provider 双轴注册表 + compat 位 + key 三态 | pi | study-pi-ai §1-3 |
| provider 项目层禁入、无隐式 env 继承 | Codex | study-provider-context §3 |
| 帧协议（长度前缀 + strict 信封 + opaque 载荷）+ 路由三元组 | pi protocol | study-pi-infra §4 |
| journal write-ahead、状态靠重放定义 | Codex rollout + harness v3 §5 | codex-study-notes §6.3 |

## 4 关键类型与五条不变式

关键签名见 plan §2（`WireFormat`/`Op`/`EventMsg`/`Tool`/`PermissionEngine`/`Sandbox`/`Entry`/`Envelope`/`JournalRecord`），实现位于 `src/provider/wire.rs`、`src/core/{ops,events,actor,turn}.rs`、`src/tools/{mod,orchestrator}.rs`、`src/permissions/mod.rs`、`src/sandbox/{mod,seatbelt}.rs`、`src/session/{entries,store}.rs`、`src/bus/{frame,rpc,identity}.rs`、`src/journal/{records,mod}.rs`。

实现强制的不变式（都有测试背书）：

1. **取消三件套**：每个 await 过 `or_cancel`；`tokio::spawn` 必须包 `AbortOnDropHandle`（`tests/discipline.rs` 静态闸门，fire-and-forget/managed 两类豁免标记）；`ResponseStream` drop 通知上游。
2. **fail-closed**：tokenizer 解析不了 → Ask；未设角色/无策略 → Ask 或 Deny；QA 写范围为零；审批者沉默、回复不可解析、或 SA 没有授权表 → Deny（`approve_timeout_secs`，默认 120s）。
3. **只追加**：session 永不截断；journal 的**活文件**可以变短，但只允许在旧前缀已被折成 `snapshot` 检查点并归档成段之后——历史不消失，只是搬出活文件（§9.22）。回滚/恢复 = 重放；半写容忍（残行 skip）。
4. **sender identity**：Hello 声明 + 帧内 sender 强一致；伪装者断连；bus 来源 item 渲染带 `<bell from>` 标注。
5. **journal 是真相**：StageWritten → BellSent → notify 的调用序强制 write-ahead；state 投影从 journal 重生成。投影可以**增量维护**（`Folder` 每追加一条走一步，避免每次派发都重放整个文件），但增量与全量必须是同一次折叠——`projection_equals_a_full_replay_after_every_append` 断言两者逐次相等，否则「预算停损读的是缓存、巡检读的是重放」会分成两份真相。

## 5 安全模型

双层：策略层（JSON 三态前缀表，policy = 数据，决策 Allow/Ask/Deny；`all_segments_explicitly_allowed` 才允许免沙箱）× OS 层（Seatbelt `(deny default)` + 参数化 writable_roots + 祖先 `file-write-unlink` 防 rename 逃逸 + 网络全禁仅留 loopback proxy）。沙箱失败经 exit 2/126/127/SIGSYS/stderr 特征判定 → ApprovalRequest → 批准后脱沙箱重试一次。**诚实边界**：v1 威胁模型是防手滑防漂移（同机同用户），命令解析是保守 word-only（`&&`/管道分段评估，其余结构一律判失败），不防恶意。

审批的第三种落点（`ApproveMode::Bus`，§10 二期首项）：Ask 不再只有「预先 all / 预先 none」两个答案，worker 可以把请求发给一个审批 peer。三个约束让它不会变成后门：

1. **请求带数据不带心情**：`approval_request` 帧携带 `tool` + `args` + 可判定的 `command`（`approval_detail` 把工具名与参数折进载荷），审批者判的是命令行本身，不是一段措辞。
2. **审批者的权力也是一张表**：SA 用 `roles/sa.approvals.json`（与 role policy 同格式的三态前缀表）判定，只有显式 Allow 放行；Ask/Deny/命令为空/没有表 → deny。协调者不能放宽角色策略里没写过的东西。
3. **沉默 = 拒绝**：`approve_timeout_secs`（默认 120s）到点由 worker 自己 deny，审批不会把回合挂死；总线断开时立刻 deny 全部在途请求，不在死 socket 上空转。

`-p` 单发模式没有总线可问，声明 `approve = "bus"` 启动即报错（不静默降级成 none）。审批的两个动作都进 journal（`approval_requested` → `approval_decided`，先写盘再回铃），所以「谁把沙箱放宽了」在崩溃后仍可复现——不变式 5 对安全事件的延伸。

## 6 配置与多后端

| 层 | 来源 | 可定 provider/key |
|---|---|---|
| runtime | CLI flag | 是 |
| role | `harness/roles/<role>.toml` | 引用 + 权限/沙箱 |
| project | `.kestrel/kestrel.toml` | **否（启动即报错）** |
| user | `~/.kestrel/providers.toml` | 唯一归属地（key 三态：字面量 / `$ENV` / `!cmd`） |
| env | 仅被显式 `$VAR` 引用 | 无隐式 ANTHROPIC_* 回退 |

每角色一个 TOML = 「一角色一后端」：SA→qwen3.8-max、RD→deepseek-v4-flash、QA→glm-5.3-flash（示例见 `harness/roles/`）。对照 CC 方案（harness-v3 §6.2 的 settings 优先级对抗 + 钉全集仪式）此处 provider 归属在类型层唯一，无需仪式。

工具审批走同一条优先级链：`--approve all|none|bus`（runtime）> role 的 `[permissions].approve` > `none`（拒绝，fail-closed）。此前 `serve` 硬编码 HeadlessDeny，worker 永远走不到「沙箱拒绝 → 审批 → 脱沙箱重试」这条回路；现在 role 可以声明三态（demo 里 RD = `bus` 且 `approver = "harness-sa"`，QA 保持缺省拒绝）。runtime 只覆盖**模式**，不覆盖 role 声明的 `approve_timeout_secs`（`with_role_deadline`）——一个 CLI flag 不该顺手把审批等待改成无限。

role 文件的路径字段（`write_roots`/`read_only_islands`/`policy_file`/`prompt_file`）支持 `$VAR` 与 `$VAR:-fallback`：role 是共享文件，把某台机器的绝对路径钉进 `write_roots` 等于把沙箱写范围钉死在那台机器上（demo 的 worktree 由 `run-demo.sh` 导出 `KESTREL_WORKTREE`，于是 SA 广播的 `ref` 与 RD 能写的目录同源）。变量未设且无 fallback 是**带拼写的硬错误**——静默丢掉一个写根会悄悄改变沙箱边界。

预算不写进 role：`[budget].max_usd` 曾在三份 role 里各有一份，而代码中没有任何读者——花钱的上限由 SA 的 journal 账本执行（`demo-sa --max-usd`），单点权威，与「provider 归属唯一」是同一条理由。

会话归属：`-p` 一次调用一个 append-only 树，默认新建，`--resume last|<id>|<path>` 续接；`serve` 一个 worker 一棵树 + 一个常驻引擎——每颗门铃是同一棵树上的下一个回合，因此历史、read-before-edit 台账与压缩检查点跨派发存活（`AgentHandle::spawn` 从树上重建 history/history_ids，并把 ctx_tokens 预置为重建上下文的估算值，否则恢复出的大会话要先采样一次才可能触发压缩）。

## 7 里程碑与验收（已全部达成）

| M | 内容 | 测试 |
|---|---|---|
| M0 | actor 循环 + SSE 归一化 + 取消 + steering + 最小 anthropic wire（2.4k 行） | 20 项：fixture 续轮/length 整批 fail/中断 100ms/steering 注入/spawn 闸门 |
| M1 | 4 工具 + 权限引擎 + Seatbelt + 三态表（1.8k 行） | 24 项：并发墙钟/沙箱 e2e×3（真实 sandbox-exec）/rename 逃逸/三态矩阵/edit golden/sbpl golden/错误回灌 |
| M2 | session 树 + 无损压缩（0.8k 行） | 15 项：golden JSONL/半写/fork/压缩触发+merge/开关模型落盘/resume |
| M3 | 诚实 wire 层：双 wire + compat + partial-json + thinking 回放 + retry + registry（1.5k 行） | 23 项：registry/金身回显/双 wire 归一/前缀穷举/重试矩阵/compat flip |
| M4 | 哑 RPC 总线 + journal + 三角色 demo（1.5k 行） | 13 项：帧 strict/路由/identity/注入面/崩溃对账/预算 |

**遗留人工项**：三端点 LLM 实测已全部完成（DeepSeek/BigModel 见 `smoke-report.md` §2.1-2.2，Qwen 见 §2.3；compat 偏差逐条落 compat 位，plan §7.1），其中 Qwen 暴露的 wire 层缺陷与修复见 §9.9。三角色联跑已跑通（`harness/run-demo.sh`，见 §9.11）。demo 任务下 RD/QA 曾在轮预算内收敛不到文字答复（工件 `stop` 显示 `round budget exhausted`）——那是任务/提示词层面的现象而非通路缺陷；自 §9.13 起 SA 只按工件内容判定（空结果=fail+回派），角色 prompt 也写了硬性收敛纪律，「pipeline complete」不再可能由空答案支撑。

## 8 测试策略

faux provider 第一天进 trait（`WireFormat` 抽象 ByteStream + `render_request` 纯函数）→ 零真 key 覆盖全部验收；三层测试（单元/组件 golden/e2e）；determinism 经 `IdGen`/`Clock` 注入；沙箱 e2e 仅 macOS（`#[cfg(target_os="macos")]`）；闸门 = `cargo fmt --check` + `clippy -D warnings` + 全测试，**人工执行、每次改动都跑**：本仓库不挂 CI（2026-10-28 试过 `ci.yml`，同日按决定移除，见 §9.19⑥），所以这道闸门没有兜底机制——rust/clippy 1.98 那次破功就是因为只靠人记得跑。

181 项 = lib 单测 138 + bin 单测（CLI 形状）4 + 集成 39（core_loop 5 / m1_tools 8 / m2_session 6 / m4_bus 13 / sandbox_e2e 4 / wire_anthropic 2 / discipline 1）。计数以实跑为准，改测试时同步这一行：此前 README / design / 报告三处各写一个数（137 / 137 / 121），实跑只有 130，漂移本身记在 §9.19。总线测试需要真 UDS `bind`，在严格沙箱或容器里以 `EPERM` 失败，属环境限制而非回归。

## 9 与 plan 的偏差（诚实台账）

1. `Op::SwitchModel` v1 携带具体 spec（plan 为 provider id）——registry 落地后改为 id 解析已排入 M4 收尾，当前由调用侧解析。
2. `ensure_done` 适配器放 wire 层而非循环层（流尾恒一个 Done 的保证对双 wire 复用）。
3. compat 位收敛为 3 个有实际行为的 flag（`max_tokens_field`/`tolerate_unknown_finish`/`stream_include_usage`），plan 草案的 reasoning/thinking 格式位在 openai-chat wire 上以「不回发」策略落地（"Armin is wrong" 原则），未引入未用 flag。
4. partial_json 实现约 90 行核心（plan 估 250）：repair + 逐字符 pop 收敛足够覆盖测试矩阵。
5. demo-sa 是脚本化状态机（无 LLM 派发智能）——验证机械语义；智能派发为二期。
6. 里程碑行数落在预算下沿：引擎薄于预估（pi 佐证的 2k 结论），wire 层 1.5k 而非 4-5k——因为 v1 只钉两个 wire 且 compat 只实现有行为的位；三期端点实测可能上修。
7. **BigModel（glm-5.3-flash）实测三项偏差（2026-09-10 smoke）**：① CC 风格的 `[1m]` 模型后缀网关不认（报 1211 模型不存在），kestrel 用裸 id——该后缀是 CC 侧的上下文标记，若需 1M 上下文应映射为 beta header 而非模型名（二期 compat 位）；② 认证双头通吃：x-api-key 与 `Authorization: Bearer` 同时发送即通过（已实装，pi anthropic.ts 同法）；③ `message_start` 不回 input usage（`input_tokens=0`），usage 低报会推迟压缩触发——**已修**：ctx_tokens 记账改为「reported input 为 0 时用 TurnContext 字符估算兜底」（turn.rs + 回归测试 compaction_triggers_on_estimated_context）。
8. **DeepSeek（deepseek-v4-flash-vision-exp）实测（2026-09-10 smoke，双场景通过）**：对话流式与 bash 工具回合全通；usage 上报**完整**（input 565/output 21）——证实 input=0 低报为 BigModel 特有，§9.7③ 的估算兜底按端点自动分工正确；认证同走双头；cc-switch 配置中的 `[1m]` 后缀未测（用裸 id 通过）。
9. **Qwen（百炼 Token Plan）实测（2026-09-17 smoke）暴露 wire 层缺陷并已修**：openai 表面的流式 usage 恒为 0。根因不是端点低报（该端点上报完整），而是 `parse_stream` 一见 `finish_reason` 就终止，而百炼把 usage 放在其**之后**的 `choices: []` chunk——消费循环在 `Done` 处 break，尾部 usage 永远读不到。修法：终止推迟到 `[DONE]`/流尾，`finish_reason` 只记入 `pending_stop`；顺带把「有 `[DONE]` 无 `finish_reason`」判为 `Stop`（原判 `Aborted`，会让 turn 循环丢弃模型输出）。**影响面**：所有 usage 后置的 OpenAI 兼容网关——此前从未被真实端点跑过（DeepSeek/BigModel 走的是 anthropic wire）。回归测试 4 项；同轮修掉 thinking-only 回合的空 `message_done`。详见 `smoke-report.md` §6。
10. **Qwen 端点形态（2026-09-17 smoke）**：百炼 Token Plan 主机同时提供 anthropic 兼容（`/apps/anthropic`，供 SA 角色）与 openai 兼容（`/compatible-mode`，供 Codex）两个表面，两侧凭据实测**互通**；`/anthropic`、`/v1/messages`、`/compatible-mode/v1/messages` 均 404。openai 表面须置 compat 位 `stream_include_usage = true`，否则全程 `"usage": null`。provider 定义见 `~/.kestrel/providers.toml`（qwen-anthropic / bailian-qwen / deepseek-anthropic / zhipu-anthropic 四条，key 均以 `!cmd` 引用 cc-switch DB，不落明文）。
11. **三角色 demo 首次跑通（2026-09-17）**：`harness/run-demo.sh` 此前从未跑通，补齐四处：① 总线 hub 进程缺失——`BusHub::bind` 只在测试里被调用，新增 `hub` 子命令（脚本先起 hub 再起 worker，轮询 socket 就绪）；② CLI 形状不符——`serve`/`demo-sa` 改为子命令，`-p`/`--model` 改为可选（`-p` 单发保持向后兼容），`--rd-role`/`--rd-model` 这两个 demo 用不到的参数删除；③ role 的 `[llm]`/`bus_socket` 是死配置——`RoleConfig` 现在解析二者，worker 的后端与总线地址由角色声明（design §6 的"一角色一后端"首次真正落地）；④ 工件命名不一致——serve 写 `{session}-{route.session}-iter{n}.json`，SA 找的是 `{target}-{stage}-iter{n}.json`，"读盘为准"巡检永不命中，改为 `{session}-{stage}-iter{n}.json`。
    调试中另暴露两个真 bug：**role 相对路径基准错**（`harness/roles/rd.toml` 写 `prompt_file = "systems/rd.md"`，代码按 role 文件所在目录解析，实际文件在兄弟目录 `systems/`；`-p` 侧把这个错误静默吞掉回退成空 system prompt，所以只有 worker 起不来）与 **工具批次历史按调用交替提交**（见 §9.12）。工件新增 `stop` 字段，空结果从此可自解释。
12. **并行工具批次必须成组提交（2026-09-17）**：`outcomes_to_history` 原按「每个调用一对 assistant/user」交替写历史，导致第二个 `tool_use` 落在没有 thinking 块的 assistant 消息里。DeepSeek 在 thinking 模式下直接 400（`The content[].thinking in the thinking mode must be passed back to the API`），而同样的历史一次只有一个工具调用时不会触发——所以三端点 smoke 全绿也没暴露它。最小复现：交替形状 400 / Anthropic 惯用的「一个 assistant 装全部 tool_use、一个 user 装全部 tool_result」200。修法：改为 `outcomes_to_history` 批量提交（全部 tool_use 在前、全部 tool_result 在后），openai wire 相应把同批 `tool_calls` 合并进一条 assistant 消息。

13. **一期标称但没接线的四处（2026-10-26 冒烟复盘，已修）**：
    ① `serve` 在每颗门铃里 `AgentHandle::spawn` 且传 `session: None`——worker 跨铃无历史、`~/.kestrel/sessions` 里连一个 worker 会话文件都没有，「常驻」名不副实。改为一个 worker 一个常驻引擎 + 一棵会话树（回归测试 `resumed_store_carries_history_into_the_next_turn`）。
    ② 会话树与无损压缩在 CLI 上**不可达**：`SessionStore::open/prompt_items` 只被测试调用，`-p` 没有续接口，而 `should_compact` 只在回合起点检查（`-p` 恰好只有一个回合）。新增 `--resume last|<id>|<path>`（配套 `SessionStore::list/resolve`），spawn 时从树上重建 history/history_ids 与 read-before-edit 台账，并把 `ctx_tokens` 预置为重建上下文的估算值——否则恢复出的大会话要先生成一轮才可能触发压缩（回归 `resume_compacts_before_the_first_sample`）。
    ③ `--tools` 是 `default_value_t = true` 的 bool flag：只能开不能关，`--tools false` 报「unrecognized subcommand」，而 README/help 把它写成 opt-in。改为默认关 + `--tools[=BOOL]`（`num_args = 0..=1` + `default_missing_value`）。
    ④ `serve` 硬编码 `ApproveMode::HeadlessDeny`，worker 走不到「沙箱拒绝 → 审批 → 脱沙箱重试」这条回路。新增 role 键 `[permissions].approve`（非法值一律回落为拒绝）+ `--approve` 覆盖，优先级 runtime > role > 拒绝。
14. **demo 的 pass 是假的（2026-10-26 冒烟复盘，已修）**：`sa_demo` 判 stage 收敛只看工件是否存在，不读 `result`/`stop`，于是 RD 那份 `result:""` + `stop:"round budget exhausted"` 的工件也能让四个 stage 全绿并打出「pipeline complete」。现在判定只读盘（`verdict_from_artifact`：空结果、aborted、`stop.type == "error"` 均为 fail），fail 走真实回派环——iter 自增、journal 落 `pass:false` 的 `result_committed`。demo 预算 2.0 只够 4 次派发（一次回派即触顶），默认上调为 4.0 并开放 `--max-usd`。两个角色 prompt 同时补了硬性收敛纪律（最后一次回合必须留给人工可读的文字结论；同类工具失败最多再试一次），否则「修好判定」只会把联跑变成常红。
15. **闸门与诊断（2026-10-26 冒烟复盘，已修）**：rust/clippy 1.98 新增 `clippy::question_mark` 让 `cargo clippy --all-targets -- -D warnings` 破功（tokenizer 的悬空转义分支）；配置类错误只吐 `No such file or directory (os error 2)` 而不说是哪个文件——role config 读/解析、policy 表、system prompt、provider registry 四处全部带上路径。design/README 的规模数字按实测修正。
16. **「读盘为准」缺了指针本身（2026-10-26 修判定后暴露）**：`serve` 只把铃 payload 的 `task` 塞进 prompt，`check` 与 `ref`（artifacts/worktree 路径）被丢掉，worker 拿不到验证对象——QA 连续三轮只能回「证据不足（缺 ref.worktree）」。修法：`bell_prompt()` 把 task/check/iteration/ref 一并交给 worker（铃仍是薄消息，但指针必须可读），demo-sa 的 `ref` 补 `--worktree` 广播 RD 的写入目录。修完判定才真正有意义：worker 自述 `PASS`/`DONE` 为收敛，`FAIL`/`BLOCKED`/无判定词为 fail 并回派（`verdict_marker_decides_the_stage`）。
17. **联跑的计时与预算假设过期（2026-10-26）**：SA 原先硬编码 180s 等回铃、2.0 USD 预算，二者都是「一次派发 = 一次冷引擎」时代的数字。常驻引擎 + 8 轮工具回合 + 真网关（BigModel 单轮实测 48-77s）下 180s 必然误判为「铃丢了」；一次诚实回派也会直接触顶。改为 `--bell-timeout-secs`（默认 300）与 `--max-usd`（默认 6.0 = 4 stage × 3 iter × 0.5，让预算覆盖声明的重试策略），run-demo.sh 可用 `KESTREL_DEMO_BELL_TIMEOUT` / `KESTREL_DEMO_MAX_USD` 覆盖。
18. **判定变严之后，demo 自身的矛盾才暴露（2026-10-26）**：① 角色 prompt 要求 RD/QA「把结果写进 artifacts」，可 RD 的写范围只有 worktree、QA 写范围为零——它们每轮都在撞自己角色的墙；结论改为「文字回答，由 `serve` 落成工件」。② QA 没有目录发现手段（策略只放 `ls`/`rg`/`cat`/`cargo`/`python3 -m`），又没有 RD 产出清单，只能猜文件名，三轮都只能回「证据不足」；因此 RD 的 `DONE` 必须逐条列出新建/修改的绝对路径与测试入口，QA 按清单取证（读 RD 工件 → 按路径读源码 → 单条已允许的命令执行），并且 `python3 -m` 进入两侧允许表（此前 QA 的允许表里没有测试运行器，`test` stage 在结构上不可能拿到执行证据）。③ 命令拼接纪律：`&&`/管道分段求值，任一段不在允许表里整条被拒——prompt 里写明用单条命令，避免把策略失败读成实现失败。这三条都不是内核缺陷，但只有先把 SA 判定做实，它们才会被看见（此前「工件存在即 pass」把它们全盖住了）。

19. **文档与配置的漂移（2026-10-28 修正）**：① 测试数三处不一致——README/design 写 137、报告写 121、实跑 130；② README 的规模拆解（wire ~1.5k、总线+journal ~1.5k）与实测（provider 2.58k、bus+journal 1.08k）不符；③ journal 的 `budget_spent` 记 `usage: 0` 却写 `usd: 0.5`，读起来像按 token 计费，实际是 `per_dispatch_usd` 的固定名义扣费——口径改为「fixture 名义停损」，真实 token 由结果铃带回并另记 `usage_reported`；④ 三份 role 的 `[budget].max_usd` 没有任何读者，删除；⑤ RD 的 `write_roots` 硬编码 `/tmp/kestrel-worktree`，改为 `$KESTREL_WORKTREE:-…`；⑥ 闸门只靠人记得跑（1.98 那次 clippy 破功，§9.15）——同日补过 `.github/workflows/ci.yml`（ubuntu + macOS），随后按决定移除：**这个仓库不挂 CI**，闸门回到「每次改动人工跑 fmt/clippy -D/test，跑不过不算完成」，代价是这类失真只能靠人兜。数字与配置写错的代价不是难看，而是让人不再核对它们。

20. **二期开工：审批反向请求 + 证据层（2026-10-28）**：`ApproveMode::Bus` 落地（§5）。三个关键取舍：请求载荷必须带 `tool` + `args`（`approval_detail`），否则审批者只能对措辞表态；审批者自己的权力也来自一张三态前缀表（`roles/sa.approvals.json`，只有显式 Allow 放行）；`Waited::TimedOut` 与总线断开都由 worker 侧 deny——一期的 `Bus` 分支是无期限等待，一个不答的 SA 能把 worker 的回合永久挂住，这本身就是可用性缺陷。SA 判定加第三层「证据核对」（`cli/evidence.rs`）：`serve` 从结论取绝对路径（优先机器可读的 `evidence:` 行）写进工件，`verdict_from_artifact` **只在 worker 声称收敛时**核对这些路径存在且非空，FAIL 不额外举证——证据层是为了防止「把 verify 说成通过」，不是给拒绝增加文书工作。同时 `serve` 在回合结束清空在途请求，迟到的批复不会被下一个回合误读。尚未做：按文件类型核对证据（截图/测试产物）、`-p` 模式的本地审批通道。审批请求与批复都是 Notify，跨断线投递由 §9.21 负责，但迟到的批复仍被 worker 丢弃——语义停在超时 deny，不会变成静默放行。

21. **bus 重连重放（2026-10-28，部分落地）**：总线原先「断线即静默丢帧」，`serve` 还会因 `recv_notify` 报错而退出——hub 重启 = 整个拓扑报废。现在三件事分开处理：
    ① hub 断线只清连接表，**保留 session 归属**，于是「注册过但不在线」与「从未注册」可区分：前者的 Notify 进停车队列（每 session 32 帧、全局 256 帧，drop-oldest 并 warn），重新 Hello 时按序补投；后者仍立刻回 `unknown session` Error——把拼错的 session 名静默停车等于让铃永远没人听见。
    ② 客户端 `recv_notify` 自动重连（100ms 起指数退避，单窗口 10s），`Drop` 显式 abort 两个 pump 任务，让 hub 立即看到 EOF 并开始停车；worker 不再因 hub 重启而退出。
    ③ **RPC 不跨重连**：Response 的路由属于那条死连接，所以 `request` 在无连接时 fail fast，Request 帧也不进重放队列。同理，已经写进「随后死掉的 socket」的帧不重放——hub 可能已经处理过它，重复铃由 SA 侧 write-ahead 对账消化（§5 不变式 5），而不是靠总线猜「大概没送到」。审批批复因此可能因重连而迟到并被 worker 丢弃（`open` 已在回合结束时清空）——语义仍是超时 deny，不会变成静默放行。
    未做：跨 hub 进程重启的停车表恢复（停车只在一个 hub 生命周期内有效）、按 peer 的可靠投递确认。`run-demo.sh` 里等注册的那一拍仍然需要。

22. **journal 快照/截断语义（2026-10-28）**：不变式 3 原话是「journal 永不截断」，实现上等于把一个只增不减的文件当成真相本体。现在拆开三件事：
    ① `JournalRecord::Snapshot { covers, checkpoint }`——检查点折的是「到此刻为止的投影」：stage / iteration / 每 peer 花费 / **未结算的铃清单**。最后这项是关键：截断若丢掉 in_flight，一次「worker 写完工件但死在拉铃前」就会被 SA 误读成「从没派发过」而重烧一轮预算，正是 journal 要防的事。
    ② `rotate(keep)` 把新于 `keep` 之外的前缀折成检查点并搬到归档段 `<stem>.seg<from>-<to>.jsonl`；段文件+活文件 = 完整历史（`replay_all()`），审计链不断。活文件的 `replay()` 里出现 Snapshot 时，折叠**从检查点重启**而不是继续累加，否则预算会被重复计一次。
    ③ 轮转顺序即安全性：全程持住 append 锁 → 段落盘 fsync → 新活文件写 `.new` 并 fsync → `rename` 原子替换 → 通过已持有的 guard 换 fd（先放锁再换会有「写进旧 inode」的窗口）。**段名取「覆盖的记录区间」而不是计数器**，`covers` 因此是累计值：这样「段已写好、rename 前崩溃」后重试会覆盖同一个文件，而不是把同一段前缀归档第二份——后者会让 `replay_all` 把预算重复计一遍。测试：`a_crashed_rotation_is_retried_idempotently`（手工造出崩溃残留）与 `concurrent_appends_survive_rotation`（4 线程并发追加 × 40 轮 rotate，断言 40 条记录不重不丢、无 `.new` 残留）。
    语义边界：**这不是删除**。段文件不自动清理，`session` 树仍完全不截断；`demo-sa --rotate-after-records` 默认 0（不轮转），因为阈值该由派发节奏定，不该由库猜。

23. **多任务并行（2026-10-28）**：一个 hub 上并存多条 pipeline，前提不是并发而是**命名空间不串台**。原先 `demo-sa` 把 task 写死成 `"demo"`，工件名 `{session}-{stage}-iter{n}.json`：两条 pipeline 打同一个 worker 名、同一个 stage 时，后一条会读到前一条的工件并把它判成 pass——这是 §9.11④「两端各拼一遍文件名」那类 bug 的升级版。现在四件事：
    ① `cli/task.rs` 是唯一一份命名契约：`artifact_name(session, task, stage, iteration)`——SA 找文件与 worker 写文件调同一个函数，`serve` 不再自己 `format!`。
    ② task id 三处同源：journal 每条记录、门铃 payload（`task_id`）、工件名与结果铃（worker 原样带回，SA 不会认错回执）。没带 `task_id` 的铃落进 `default` 命名空间，而不是从自由文本 `task` 里猜一个出来。
    ③ slug 把任意文本变成安全的文件名片段：非 `[A-Za-z0-9._-]` 一律变 `-`（中文任务名退化成 `demo-LRU-cache`），`/` 被消灭，`.` 与 `..` 换成带 FNV-1a 后缀的名字，超长截断也保留唯一性。**task id 来自另一个进程却要变成文件名，它不是可信任的输入**。
    ④ 寻址用前缀：`serve --session t2:harness-rd` 配 `demo-sa --task-id t2 --rd-peer t2:harness-rd --journal t2.jsonl`。session id 本来就是 opaque 字符串，hub 不改一行代码就能把两条 pipeline 路由隔开；`--rd-peer/--qa-peer` 让 SA 知道该打哪一对 worker。
    边界要说清：这一项解决的是「并存不串台」，不是「一个引擎同时服务两个 task」。单活跃槽（§4 不变式）依旧，同一 worker 收到两颗铃仍是一回合一回合地跑；真并发需要 worker 池，仍在 §10。

24. **openai-responses wire（2026-10-28，第三条 v1 wire）**：`api = "openai-responses"` → `POST /v1/responses`。它与 chat 的差三处，不是兼容位能糊过去的：系统提示走顶层 `instructions`；历史是 `input` 项列表（`message` / `function_call` / `function_call_output`），因此**并行工具批次天然成组**——§9.12 那条「一个 assistant 装全部 tool_use」的形状约束是 chat/anthropic 的事，这条 wire 上不存在；usage 搭在终止事件 `response.completed` 的 `response.usage` 里，所以 §9.9「finish_reason 之后还有 usage」的延后终止补丁在这里没有对应问题（终止事件本身就是唯一的 usage 载体）。
    映射规则要说清：`response.incomplete` + `incomplete_details.reason == "max_output_tokens"` → **`Length`，不软成 Stop**，否则 §4 的「length 整批 fail」纪律在这条 wire 上静默失效；`response.failed` / 顶层 `error` → `Error`（`server_error` / `rate_limit_error` 标 retryable，交给 retry 层）；流尾没有终止事件 → `Aborted`，且**不吐出已积累的 tool call**（回合会丢弃 partials）。
    取舍延续 §9.3：只发有行为依据的字段——`store: false`（会话树才是真相源，不让服务端另存一份）、`max_output_tokens`、扁平 tools + `tool_choice`；`reasoning.effort` 没实测就不发。思考内容仍是不透明 blob：blob 本身就是 Responses 的 `reasoning` 项（带 `encrypted_content`）时原样回显，anthropic 形状的 blob 直接跳过。
    **诚实边界**：这条 wire 只有金身请求体 + fixture 事件流验证（7 项），**没有跑过任何真端点**——本会话既无 OpenAI 凭据也无网络。它与 §9.7-9.10 那三条实测过的 wire 不同级：usage 字段命名、`call_id` 与 `item.id` 的实际填法、reasoning 项的回显要求，都可能像 Qwen 那轮（§9.9）一样暴露结构偏差。真端点实测已随 model-in-the-loop evals 排在 §10。

25. **journal 投影增量化 + 轮转触发（2026-10-28）**：不变式 5 说「state 投影从 journal 重生成」，实现里它是**每次派发都把活文件读一遍再折一次**——O(历史) 的开销挂在最热的路径上（预算停损 + 丢铃巡检都要它）。现在：
    ① 折叠逻辑抽成唯一的 `Folder`（`apply` 一条 / `report` 取投影），`reconcile(records)` 只是把它跑一遍。在线投影与全量重放因此**不可能各写一套**；`projection_equals_a_full_replay_after_every_append` 逐条断言两者相等（含轮转后重新播种）。
    ② `Journal` 持一份在线投影：`append` 顺带推进一步（锁序固定 file → projection，与 `rotate` 一致），`projection()` / `live_records()` / `file_bytes()` 都是 O(1)。SA 的预算停损与丢铃巡检改读投影。
    ③ 轮转触发 `rotation_due(records, bytes, cap_records, cap_bytes)`：两个上限任一超限即折，0 关闭该条件。字节上限是必要的——记录大小不齐（一条 `approval_requested` 带着 stderr），只按条数封顶挡不住文件长大。保留新近一半，于是活文件在上限的 1/2~1 之间浮动。**退化情形由库自己挡**：`rotate` 在前缀不足 2 条原始记录时返回 `None`——折一条记录进检查点会让活文件变长（检查点那行比它替换的记录更贵），这种守卫不能指望调用方给的阈值刚好。
    ④ 顺带修掉一个既有缺陷：`Journal::create` 开的是追加模式，却把投影当空的。配合 `KESTREL_DEMO_KEEP_JOURNAL=1`（跨运行续账本）时，**昨天花掉的钱与没结算的铃在重启后被忘掉**，停损形同虚设。现在 `create`/`open` 都从盘上播种（`create_seeds_the_projection_of_an_existing_journal`）。
    仍不做的事：库自己决定阈值。`--rotate-after-records` / `--rotate-max-bytes` 默认 0（不截断），策略归派发节奏的主人（SA/人），这条留在 §10。

## 10 二期清单

已落地（2026-10-28）：~~`ApproveMode::Bus`~~（授权表 + 超时 fail-closed + journal 审计，§9.20）、~~SA 判定升级到「工件 + 证据文件」~~（绝对路径存在性/非空核对，§9.20）、~~bus 重连重放~~（离线 session 停车补投 + 客户端自动重连；RPC 与「写入即死」的帧不重放，§9.21）、~~journal 快照/截断语义~~（检查点 + 归档段 + 崩溃安全轮转，§9.22）、~~多任务并行~~（task 命名空间三处同源 + 前缀寻址，§9.23）、~~openai-responses wire~~（第三条 v1 wire，金身 + fixture 验证，**真端点未实测**，§9.24）、~~journal 投影增量化与轮转触发~~（在线投影 = 全量重放 + 条数/字节双上限，§9.25）。

仍在清单上：tree-sitter-bash 命令解析、execpolicy DSL（Starlark）、landlock/bwrap（Linux 沙箱）、TUI（行级 diff 形制，pi-tui 蓝本）、MCP 客户端、model-in-the-loop evals（含 openai-responses 的真端点实测，§9.24）、审批批复的可靠投递（请求与批复都是 Notify：§9.21 的停车补投已覆盖「审批者当时不在线」，剩下的缺口是批复迟到——回合结束即丢弃，语义停在超时 deny）、journal 轮转的**默认策略**（机制已齐：`rotate` + 条数/字节双上限 + O(1) 触发，§9.25；阈值仍由调用方给，库不猜派发节奏）、一个引擎内真正并发地服务多个 task（单活跃槽是刻意的不变式：今天多 task 在同一 worker 上串行，真并行靠各自的 worker 实例）。

新划出的两条边界：`BudgetSpent` 的价格表（有单价表之后 usd 才能真正按 token 结算，而不是 per-dispatch 名义值）；证据的类型化（`evidence` 从「路径存在」升级到「测试产物/截图按 stage 要求齐全」）。
