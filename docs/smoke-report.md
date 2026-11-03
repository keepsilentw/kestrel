# kestrel 真实端点测试报告

> 日期：2026-09-09/10 首轮；2026-09-17 补 Qwen 一轮。对象：kestrel MVP（M0-M4）。
> 范围：真实 LLM 端点 smoke（plan §4-M3 验收项 7 / §4-M0 验收项 6）。
> 凭据来源：BigModel 与 DeepSeek 取自 `~/.claude/settings.json` / cc-switch DB（报告中脱敏）；
> Qwen 取自 cc-switch 的百炼 Token Plan 条目（codex 与 claude 两侧凭据实测互通）。

## 1. 自动化测试基线（cargo test）

**Metrics** — src 51 files / 15374 lines · tests 9 files / 3204 lines · 228 tests (lib 177 + bin 4 + integration 47)
**Integration** — core_loop 5 / discipline 2 / m1_tools 8 / m2_session 6 / m4_bus 15 / m5_pool 5 / sandbox_e2e 4 / wire_anthropic 2

| 组 | 覆盖 |
|---|---|
| 单测（lib） | wire 归一（含 openai-responses）/ partial_json / retry / registry / 权限矩阵 / sbpl golden / session 树 / journal 对账、轮转与在线投影 / 证据契约 / 审批授权表 / task 命名空间 / worker 池路由表 / **预算价格表与预留—结算配对**（§13）、**证据类型化与 stage 要求**（§15） |
| 单测（bin：CLI 形状） | `--tools` 三态、`--approve` 三值（none / all / bus）、`--resume` 选择子、bus 模式保留 role 期限 |
| 集成（逐文件分组见上面的 Integration 行） | 引擎验收 / 工具并发与审批超时 / 真实 Seatbelt / golden JSONL / 总线路由、审批往返、停车补投、重连重放、多 pipeline 并存、取消安全、截断后巡检与崩溃对账 / worker 池真并发与历史隔离 / **文档数字不再靠人抄**（discipline） |
| 合计 | 见 Metrics 行；2026-10-28 那次实跑全部通过，`fmt --check` 与 `clippy -D warnings` 干净（本仓库不挂 CI，闸门靠人工每次执行） |

> 上面两行的数字**不再由人抄写**：`tests/discipline.rs::metrics_lines_match_the_tree` 会把源码树重算一遍并与 README / design / 本报告对照，忘了同步就转红。历史上这张表漂移过三次（101 → 121 → 137，实跑 130，见 design.md §9.19）——没有 CI 兜底时，唯一防线就是把「同步」变成闸门。
> 总线测试要真 UDS `bind`：在严格沙箱/容器内以 `EPERM` 失败，属环境限制而非回归。

## 2. 端点 smoke 结果

### 2.1 BigModel（智谱）—— `https://open.bigmodel.cn/api/anthropic`

| 场景 | 结果 | 关键证据 |
|---|---|---|
| 纯对话流式 | **通过** | thinking/text delta 流式 → `message_done("成功")` → `turn_complete(stop=stop)` |
| bash 工具回合 | **通过** | 模型发起 bash 调用 → orchestrator 派发 → **Seatbelt 沙箱内执行** → 结果回灌 → 模型报告 `kestrel-ok` → 收敛 |

### 2.2 DeepSeek —— `https://api.deepseek.com/anthropic`

| 场景 | 结果 | 关键证据 |
|---|---|---|
| 纯对话流式 | **通过** | 文本流式 → `turn_complete`；**usage 完整**（input 565 / output 21） |
| bash 工具回合 | **通过** | 沙箱内 `echo deepseek-ok` → 回灌 → 报告 → 收敛（input 98 / output 112） |

### 2.3 Qwen（百炼 Token Plan）—— 2026-09-17

端点两个表面（同一份 Token Plan，credential 互通）：

| 表面 | base_url | wire |
|---|---|---|
| Anthropic 兼容 | `https://token-plan.cn-beijing.maas.aliyuncs.com/apps/anthropic` | `anthropic-messages` |
| OpenAI 兼容 | `https://token-plan.cn-beijing.maas.aliyuncs.com/compatible-mode` | `openai-completions` |

| 场景 | wire | 结果 | 关键证据 |
|---|---|---|---|
| 纯对话流式 | anthropic | **通过** | `message_done("kestrel-ok")` → `turn_complete(stop=stop)`；usage 218/26 |
| bash 工具回合 | anthropic | **通过** | `tool_begin` → 沙箱内 `echo qwen-anthropic-ok` → `tool_end` 回灌 → 报告 → 收敛；usage 466/72 |
| 纯对话流式 | openai | **通过** | usage 1188/255（修复前恒为 0，见 §6） |
| bash 工具回合 | openai | **通过** | 沙箱内 `echo qwen-ok` → 回灌 → 报告 → 收敛 |
| Seatbelt 生效 | — | **通过** | 策略显式 Allow 的 `touch /tmp/...` 被拒（`Operation not permitted`）；升级分支返回 `sandbox denied (headless)`，文件未创建 |

同轮顺带验证 `deepseek-anthropic`（usage 503/2）与 `zhipu-anthropic`（input 0 / output 3，即 §3-3 的 BigModel 低报复现）两个 provider 条目在 kestrel 内可用。

## 3. compat 偏差发现（已落 design.md §9 台账）

| # | 端点 | 偏差 | 处置 |
|---|---|---|---|
| 1 | BigModel | `[1m]` 模型名后缀报 1211 模型不存在（CC 侧标记） | kestrel 用裸 id；1M 上下文应走 beta header（二期） |
| 2 | BigModel/DeepSeek | 认证方式差异（x-api-key vs Bearer） | 双头同发（x-api-key + Bearer），已实装，两端点均通过 |
| 3 | BigModel 特有 | `message_start` 不回 input usage（`input_tokens=0`） | **已修**：ctx_tokens 记账在 reported input=0 时以 TurnContext 字符估算兜底（回归测试 `compaction_triggers_on_estimated_context`）；DeepSeek 上报完整、兜底自动不激活，按端点分工正确 |
| 4 | 百炼（Qwen） | anthropic 表面挂在 `/apps/anthropic`；`/anthropic`、`/v1/messages`、`/compatible-mode/v1/messages` 全部 404 | provider 基址钉 `/apps/anthropic`（`~/.kestrel/providers.toml`） |
| 5 | 百炼（Qwen）openai 表面 | 不发 `stream_options.include_usage` 时，每个 chunk 都是 `"usage": null`，全程无真实 usage | compat 位 `stream_include_usage = true` |
| 6 | 百炼（Qwen）openai 表面 | 真实 usage 在 `finish_reason` **之后**的 `choices: []` chunk 才下发 | **已修**：wire 层改为在流终结符处才收敛 `Done`（见 §6），否则整个 usage 账目归零 |
| 7 | 百炼 / BigModel | `[1m]`/`[1M]` 后缀复现 1211 模型不存在（`glm-5.3-flash[1m]` 实测 400） | 与 §3-1 同因；kestrel 一律用裸 id |

## 4. 与自动化测试的交叉验证价值

- 沙箱 e2e 在真实网关下复现通过：工具执行路径（wire → orchestrator → Seatbelt → 回灌）端到端成立
- usage 低报问题只有真机才能暴露（faux fixture 天然上报准确值）——印证 plan §7.1「端点实测不可压缩」的判断
- 权限分类器（同走 BigModel 端点）在测试中出现过一次超时，与 kestrel 无关，但提醒了无人值守场景的依赖单点

## 5. 遗留

1. DeepSeek/BigModel 的 `[1m]` 后缀可接受性未测（用裸 id 不阻塞）
2. 多轮长会话下的压缩触发真机验证（依赖 input usage 上报准确性，BigModel 需观察估算兜底的实际表现）
3. 三角色联跑**已跑通**（2026-09-17，见 §7）：hub + RD/QA worker + demo-sa 四进程，四个 stage（dev/review/test/build）全部 pass，journal 的 write-ahead 顺序与 `{session}-{stage}-iter{n}.json` 工件命名均验证成立。遗留：demo 任务下 RD/QA 常在 8 轮预算内收敛不到文字答复（工件 `stop` 显示 `round budget exhausted`），是任务/提示词层面的现象，不是通路缺陷
4. Qwen anthropic 表面的 `thinking` 块 `signature` 恒为空串——回放语义未经多轮工具回合验证（§6 修复只覆盖 openai 表面）

## 6. 2026-09-17 修复：openai 表面流式 usage 归零

**症状**：`bailian-qwen`（openai-completions）下每个 `turn_complete` 的 usage 恒为 `input=0 / output=0`，而同一模型的 anthropic 表面上报正常。

**根因**（非端点低报，是 wire 层终止时机）：百炼把真实 usage 放在 `finish_reason` 之后的独立 chunk 里——

```
data: {..."choices":[{"delta":{...},"finish_reason":"stop"}],"usage":null}
data: {...  "choices":[],  "usage":{"prompt_tokens":63,...}}   <- 真实 usage
data: [DONE]
```

原 `parse_stream` 一见 `finish_reason` 就 push `WireEvent::Done`，`turn.rs` 的消费循环在 `Done` 处 break 并丢弃流，尾部 usage 永远读不到。

**修法**：`parse_stream` 改为 `unfold` 驱动，`finish_reason` 只记录进 `pending_stop`（顺带冲刷工具调用），唯一 `Done` 延迟到 `[DONE]` / 流尾才发。附带修正两处终止语义：`[DONE]` 但无 `finish_reason` 判 `Stop`（原为 `Aborted`，会让 turn 循环丢掉模型输出），无 `[DONE]` 的裸 EOF 仍判 `Aborted`（保留原契约）。

**影响面**：所有把 usage 放在 `finish_reason` 之后的 OpenAI 兼容网关，此前从未被真实端点跑过（DeepSeek/BigModel 的 smoke 走的是 anthropic wire）。

**回归测试**：`usage_after_finish_reason_is_kept`、`eof_without_done_marker_keeps_finish_reason`、`done_marker_without_finish_reason_is_stop`、`truncated_stream_without_markers_is_aborted`（`src/provider/openai_chat.rs`）。

**同时修掉**：`turn.rs` 在 thinking-only 回合会发一个 `text:""` 的 `message_done`（无可见文本时不再发该事件）。

## 7. 2026-09-17 三角色联跑与并行工具批次修复

### 7.1 联跑结果

`harness/run-demo.sh`：hub + `serve`(harness-rd) + `serve`(harness-qa) + `demo-sa` 四进程。

```
[hub] listening on ~/.kestrel/harness/bus.sock
[sa] dev iter1: pass / review iter1: pass / test iter1: pass / build iter1: pass
[sa] pipeline complete
```

- journal 顺序成立：`stage_written` → `bell_sent` → `budget_spent` → `bell_received` → `result_committed`（write-ahead 不变式 5）
- 工件按 `{session}-{stage}-iter{n}.json` 落盘，SA 的读盘巡检能命中（此前两端命名不一致，巡检永不命中）
- 四个 stage 的 worker 都真实调用了 LLM；工件新增 `stop` 字段，空结果可自解释

### 7.2 暴露的 wire 缺陷：并行工具批次的历史形状

**症状**：RD/QA worker 在第二个回合收到 `400 invalid_request_error: The content[].thinking in the thinking mode must be passed back to the API`。

**根因**：`outcomes_to_history` 按「每个调用一对 assistant/user」交替提交，两个工具调用产生

```
assistant: [thinking, tool_use(c1)]   user: [tool_result(c1)]
assistant: [tool_use(c2)]             user: [tool_result(c2)]
```

第二条 assistant 消息带着 `tool_use` 却没有 thinking 块。DeepSeek 在 thinking 模式下对此直接 400。最小复现（同一份真实 thinking/工具调用，只改形状）：

| 请求形状 | 结果 |
|---|---|
| 「每调用一对」交替 | **400** |
| 一个 assistant 装全部 `tool_use`、一个 user 装全部 `tool_result` | 200 |

**为什么三端点 smoke 没抓到**：smoke 的每次工具回合都只有一个工具调用，交替与成组是同一个形状。

**修法**：历史改为批量提交（`outcomes_to_history`：全部 `tool_use` 在前，全部 `tool_result` 在后）；openai wire 相应把同批 `tool_calls` 合并进一条 assistant 消息，避免连续两条 assistant 消息。回归测试 2 项（`anthropic::parallel_tool_batch_renders_grouped`、`openai_chat::render_messages_merges_parallel_tool_calls`）。

## 8. 2026-10-26 全量冒烟与修复回归

对象：一期 M0-M4 全部已实现功能。工装：`cargo` 闸门 + 本地零成本假端点（Anthropic 形流式，可切 text/loop/flaky/slow 模式）+ 一份**独立实现的 Node 总线客户端**（反证帧协议互操作）+ 真实三端点。假端点层覆盖工具编排 / Seatbelt / 审批 / 超时 / retry / 会话；真端点层只用于 wire 归一、usage 记账与四进程联跑。

### 8.1 修复前基线

121/121 通过、`fmt --check` 干净；`clippy --all-targets -- -D warnings` **失败**（rust/clippy 1.98 新增 `question_mark`）。真实端点：Qwen anthropic usage 218/22、Qwen openai usage 564/24、DeepSeek 两个 bash 调用成组回合收敛；Seatbelt 三态（root 内允许 / root 外拒绝 / 审批后脱沙箱）；总线路由、未知 session 报错、伪造 sender 断连；四进程联跑 4 stage 全 pass。

### 8.2 发现的问题与处置

| # | 问题 | 处置 |
|---|---|---|
| A | clippy 1.98 新 lint 破闸门（tokenizer 悬空转义分支） | 改 `chars.next()?`，闸门恢复干净 |
| B | `serve` 每颗门铃新建引擎且 `session: None`——worker 跨铃无历史，`sessions/` 里连一个 worker 会话文件都没有 | 一个 worker 一个常驻引擎 + 一棵会话树；`Fatal` 只终止回合，不终止 worker |
| C | 会话树/无损压缩在 CLI 上不可达（`open`/`prompt_items` 只被测试调用，`-p` 无续接口，压缩只在回合起点检查） | 新增 `--resume last\|<id>\|<path>` + `SessionStore::list/resolve`；spawn 时重建 history/history_ids 与 read 台账，并把 `ctx_tokens` 预置为恢复上下文的估算值 |
| D | `--tools` 默认开且关不掉（`--tools false` 报 unrecognized subcommand），文档写成 opt-in | 默认关 + `--tools[=BOOL]`（`num_args=0..=1` + `default_missing_value`） |
| E | `serve` 硬编码 `HeadlessDeny`，worker 走不到「拒绝 → 审批 → 脱沙箱重试」 | role 键 `[permissions].approve`（非法值回落拒绝）+ `--approve` 覆盖，runtime > role > 拒绝 |
| F | `demo-sa` 判 pass 只看工件存在：`result:""` + `stop:"round budget exhausted"` 也能四绿 | `verdict_from_artifact` + `parse_verdict`：通路层（空结果 / abort / stop=error）与语义层（worker 自述 PASS/DONE vs FAIL/BLOCKED，允许前置铺垫、否定判定优先）都要过；fail 走真实回派，journal 落 `pass:false` |
| G | design/README 规模数字过期（写 45 文件 ~7,800 行） | 按实测修正 src/tests 文件与行数，状态行测试数同步 |
| H | 配置错误只吐 `No such file or directory (os error 2)`，不说是哪个文件 | role config 读/解析、policy 表、system prompt、provider registry 全部带上路径 |
| I | （修 F 后暴露）`serve` 只把铃 payload 的 `task` 给 worker，`check`/`ref` 被丢——QA 拿不到验证对象，连判三轮「证据不足」 | `bell_prompt()` 把 task/check/iteration/ref 一并交出去；demo-sa 新增 `--worktree` 把 RD 的写入目录广播进 `ref` |
| J | （修 F 后暴露）SA 硬编码 180s 等铃 / 2.0 USD 预算，是「一次派发 = 一次冷引擎」时代的数字：常驻引擎 + 8 轮工具 + BigModel 单轮实测 48-77s 必然误判；一次诚实回派即触顶 | `--bell-timeout-secs`（默认 300）、`--max-usd`（默认 6.0 = 4 stage × 3 iter × 0.5，覆盖声明的重试策略）；`run-demo.sh` 可用 `KESTREL_DEMO_BELL_TIMEOUT` / `KESTREL_DEMO_MAX_USD` 覆盖 |
| K | （修 F 后暴露）角色 prompt 与权限互相矛盾：要求 RD/QA「把结果写进 artifacts」，但二者写范围各只有 worktree / 空；QA 的允许命令里没有测试运行器，`review` 永远缺 test 证据 | 结论改为「文字回答，由 `serve` 落成工件」；两侧策略允许 `python3 -m`（跑被测代码），`bash`/`sh`/`python`/`rm` 仍禁止 |

### 8.3 修复后回归

- 自动化：130/130 通过（当时实跑计数；本报告原先写 137/137 与 lib 102 + 集成 35，属数字漂移，复核见 §1）；新增 resume/压缩可达、铃文本、判定解析、CLI flag 形状等回归；`fmt --check` 与 `clippy -D warnings` 干净。
- 假端点：`--tools` 三态（缺省请求无 tools 字段 / `--tools` 带 / `--tools false` 不带）；`--resume last` 后请求历史从 1 条变 3 条；role 声明 `approve="all"` 时无需 CLI 即可走审批重试；缺文件错误带出文件名。
- 常驻 worker：一次运行两个回合写进**同一个**会话文件，`compaction` 检查点落盘（`tokens_before` 658 → 1258，第二次带 `seed` 合并）。
- 真端点：Qwen anthropic / Qwen openai 的 usage 记账不变；DeepSeek 两个并行 bash 调用仍成组收敛。
- 联跑：判定改读工件后，`dev iter1` 如实报 `fail (no verdict / round budget exhausted)` 并回派，后续 iter 靠常驻历史收敛——「pipeline complete」不再可能由空答案支撑。

### 8.4 遗留

1. 联跑是否绿取决于 worker 当次能否收敛；判定严格之后，失败路径（回派 → iter 上限 → 非零退出）是真实行为，不是回归缺陷。任务/提示词层面的收敛调优仍属 demo 打磨。
2. ~~`ApproveMode::Bus`（worker 把审批反向请求发给 SA/真人）未接线~~ —— 2026-10-28 接线（§9）：请求带 `tool`+`args`+命令，SA 按 `roles/sa.approvals.json` 判定，超时/断开由 worker deny。**尚未在真端点联跑里验证**（只过了单测与总线级 e2e）。
3. ~~SA 判定只吃 `result` 文字与 `aborted`/`stop`~~ —— 2026-10-28 加了第三层：`serve` 从结论提绝对路径写进工件 `evidence`，SA 核对这些路径存在且非空（§9）。按类型核对证据（截图/测试产物）仍在清单上。

## 9. 2026-10-28 二期开工：审批反向请求 + 证据层

本轮没有新端点接入，两项能力在既有三端点之外验证（真端点联跑待人工执行 `harness/run-demo.sh`）：

| 项 | 验证方式 | 结果 |
|---|---|---|
| `ApproveMode::Bus` 数据形状 | `tests/m4_bus.rs::approval_request_is_answered_by_the_sas_authority`：worker 发请求 → SA 用授权表判 → 回铃 → worker 解析 | 通过；非 `allow` 的回复一律读成 deny |
| 审批超时 fail-closed | `tests/m1_tools.rs::bus_approval_without_answer_denies_on_deadline`：无审批者应答 | 120ms 期限到点回 is_error，回合不挂死 |
| SA 授权表 | `sa_demo.rs::approval_authority_is_a_table_not_a_mood` | 显式 Allow 放行；Ask/Deny/空命令/无表全 deny |
| 证据层 | `cli/evidence.rs` 6 项 + `converged_answer_without_checkable_evidence_is_fail` | 「PASS 但给不出文件」与「文件不在盘上」都判 fail 并回派 |
| role 路径 `$VAR` 展开 | `config::role_paths_expand_env_references` | `$WT`、`$WT/sub`、`$MISSING:-fallback` 正确；未设且无 fallback 报出拼写 |
| journal 审计扩展 | `records::approval_and_usage_records_are_audit_only` | 审批/用量记录不落进「在途铃」投影，回放不误判 |

| 离线 session 停车补投 | `m4_bus::bells_for_an_offline_session_are_parked_and_redelivered_in_order` | worker 重启后按派发顺序收到两颗铃，sender 身份保留 |
| 错地址不静默停车 | `m4_bus::never_registered_session_still_answers_error` | 从未注册的 session 立刻回 `unknown session` Error |
| 断线重连与重放边界 | `m4_bus::dead_socket_reconnects_and_replays_only_unsent_frames` | 重连带 re-Hello；未送出的帧补发，已写入死 socket 的帧不补；RPC 立即失败 |
| journal 截断不改变投影 | `journal::rotate_archives_the_prefix_without_changing_the_projection` + `m4_bus::rotation_does_not_hide_an_in_flight_bell` | 折叠活文件与折叠「段+活文件」给出同一 state / in_flight；预算不被重复计 |
| 检查点自带未结算的铃 | `journal::rotate_keeps_unsettled_bells_reconcilable` | 全量轮转后（活文件只剩检查点）巡检仍看见那颗未回的铃 |
| 轮转的崩溃安全顺序 | `journal::concurrent_appends_survive_rotation`（4 线程并发追加 × 40 轮 rotate） | 40 条记录不重不丢，无 `.new` 残留 |
| 崩溃后重试的幂等性 | `journal::a_crashed_rotation_is_retried_idempotently` | 段名取覆盖区间：「段已写好、rename 前崩溃」的残留被同一个区间覆盖，预算不被重复计 |
| 两条 pipeline 并存不串台 | `m4_bus::two_pipelines_share_one_hub_without_cross_talk` | 前缀 session 各自收到自己的铃；同 stage 同 iter 的两个 task 工件名不同；未注册的 pipeline 地址仍回 Error |
| 工件名两端同源且防路径穿越 | `cli::task` 4 项（slug 中和 `..` / `/` / 中文 / 超长，`artifact_name` 契约，`task_of` 回落 default） | task id 来自另一个进程，也不成为文件名攻击面 |
| SA 侧 task 与 peer 接线 | `sa_demo::task_namespace_and_peer_wiring` | journal / 门铃 / 工件三处同一个 task id；`--rd-peer` / `--qa-peer` 决定派发对象 |

真端点侧的已知空洞：`--max-usd` 仍是 per-dispatch 名义值（无价格表），真实 token 现在只进
`usage_reported` 记录，不参与停损判断。

## 10. 2026-10-28 openai-responses wire：只有 fixture 级验证

`api = "openai-responses"`（`POST /v1/responses` + `response.*` 事件族）新增，7 项单测：

| 项 | 用例 | 结果 |
|---|---|---|
| 请求形状（顶层 `instructions` / `input` 项 / 扁平 tools / `store:false`） | `openai_responses::request_shape_is_the_responses_api` | 通过；并行调用天然是独立 input 项，无 chat 那条成组要求（§9.12 的约束不外溢） |
| thinking blob 分流 | 同上 | Responses 形状的 `reasoning` 项原样回显；anthropic 形状跳过 |
| 文本 + usage | `text_stream_with_usage` | usage 搭终止事件，input/output/cached 三项对上 |
| 工具批次归一 | `function_call_stream_becomes_a_tool_batch` | begin/args/end 按 `output_index` 成组；`call_id` 取事件的 `call_id`，不是 `item.id` |
| token 上限映射 | `incomplete_from_token_cap_maps_to_length` | `max_output_tokens` → `Length`，「整批 fail」纪律在这条 wire 上仍生效 |
| 失败与限流 | `failed_and_error_events_are_terminal_errors` | `response.failed` / `error` → Error；rate limit 标 retryable 交给重试层 |
| 流被截断 | `eof_mid_stream_is_aborted_and_drops_calls` | Aborted 且不交出已积累的 tool call |

**这条 wire 没跑过任何真端点**：本会话没有 OpenAI 凭据也没有网络。它与 §2 那三条实测 wire
不同级，风险点写在 design §9.24——usage 字段命名、`call_id` 与 `item.id` 的实际填法、
reasoning 项的回显要求，都要等真端点定案（§9.9 的 Qwen 教训就是这么暴露的）。registry 侧
只有 `config.rs` 的解析测试证明 `api = "openai-responses"` 能被选中并落到会话头部。

## 11. 2026-10-28 journal 在线投影与轮转触发

不变式 5 原先的实现是「每次派发都把活文件读一遍再折一次」，O(历史) 挂在预算停损与丢铃
巡检这两条热路径上。现在折叠逻辑只有 `Folder` 一份（`reconcile` 是它的批处理外壳），
`Journal` 持一份在线投影随 append 推进。

| 项 | 用例 | 结果 |
|---|---|---|
| 在线投影 == 全量重放 | `journal::projection_equals_a_full_replay_after_every_append`（含轮转后重新播种） | 逐次相等：state 与 in_flight 都不分裂 |
| 跨运行续账本 | `journal::create_seeds_the_projection_of_an_existing_journal` | 旧文件里的花费与未结算铃被读回（修掉「重启即归零」） |
| 检查点清掉陈旧回执键 | `records::checkpoint_clears_stale_receipt_keys` | 轮转后复用同一 (role, stage, iteration) 的铃不会被误判成已结算 |
| 轮转触发（纯函数） | `sa_demo::rotation_trigger_accepts_either_ceiling` | 条数或字节任一超限即折；0 关闭；少于 2 条不折 |
| 退化前缀由库挡住 | `journal::rotate_refuses_a_degenerate_prefix` | 前缀不足 2 条原始记录时 `rotate` 返回 `None`，不写段、活文件不变长 |
| O(1) 读 | `live_records()` 走投影、`file_bytes()` 走 metadata | 触发判断不再重放文件 |

**顺带修掉的既有缺陷**：`Journal::create` 以追加模式打开，却把投影当空的——配合
`KESTREL_DEMO_KEEP_JOURNAL=1`（跨运行保留账本做崩溃恢复演练）时，昨天花掉的钱和不
结算的铃在重启后被忘掉，`--max-usd` 停损形同虚设。现在 `create` / `open` 都从盘上播种。

轮转阈值本身仍由调用方给（`--rotate-after-records` / `--rotate-max-bytes`，默认 0 = 不截断）：
库不猜派发节奏，这条策略留在 §10。

## 12. 2026-10-28 worker 池与两个「取消安全」缺陷

`serve` 改成按 task 路由的池（一 task 一引擎一树），faux 引擎 + 真总线验证：

| 项 | 用例 | 结果 |
|---|---|---|
| 两个 task 真并发、各自落工件 | `m5_pool::two_tasks_run_side_by_side_in_one_worker` | 通过；两份工件、两颗结果铃各带自己的 task id |
| 历史隔离 | 同上（反向断言 `recorded_requests()`） | t1 的请求里只有 t1 的铃，反之亦然 |
| 超上限显式拒绝 | `m5_pool::over_the_task_cap_a_bell_is_refused_out_loud` | 回一颗 error 铃且不带 `result_ref`，不静默排队 |
| 无 task id 的旧门铃 | `m5_pool::a_bell_without_a_task_id_uses_the_default_namespace` | 落进 `default` 命名空间，单管线行为不变 |
| 路由与审批归属（纯表） | `pool::tests` 3 项 | 上限拒绝、批复按 req 定向不广播、请求表随回合关闭回收 |
| **回归：select! 取消破坏连接** | `m4_bus::a_cancelled_inbound_poll_leaves_the_connection_usable` | 修复后通过；**变异验证**：摘掉 RAII 归还 → 该测试转红 |

修掉的两个缺陷都属于「被 `tokio::select!` 取消的 future 留下坏状态」这一类：
其一，`take_inbox` 取出接收器后被 drop，连接从此每次读都以 `inbox already polled`
失败（上一版 `serve` 的审批循环是同一种形状，同样中招）；其二，竞争同时就绪时被丢弃的
分支会连带丢掉那一帧——取消安全不等于不丢帧，所以泵改用专职 reader 任务读总线。

## 13. 2026-11-03 预算价格表（usd 按 token 结算）与文档数字闸门

design §10 第一条新边界落地：`CostPerMTok` 早在 M3 就进了 registry，但**没有任何读者**——
一个没人读的价格字段比没有价格更危险，它让人以为 `--max-usd` 在算钱。本轮把「名义值停损」
换成「预留 → 结算」的两段式账本，并把查不到价的情形显式标成名义值（详见 design §9.27）。

| 项 | 用例 | 结果 |
|---|---|---|
| 账单算到每个 token 桶（含缓存读写） | `wire::tests::usage_cost_uses_every_bucket` | 通过；未声明缓存费率时按 input 计价，缓存 token 不会掉出总额 |
| 预留 = 整窗输入 + 满 `max_out` | `wire::tests::reservation_is_the_context_window_plus_one_full_answer` | 通过（128k/8k @ 1.0/4.0 → 0.160768） |
| 空价表 ≠ 免费 | `wire::tests::zero_rate_is_not_a_declared_price`、`config::tests::model_cost_only_answers_for_declared_rates` | 通过；`{0.0,0.0}`、未知 provider、未声明 model 一律 `None` |
| 结算替换预留 | `records::tests::settlement_replaces_the_reservation` | 通过；`spent_usd` 从 0.5 变 0.12，`tokens` 留档，配对键消耗 |
| 名义值可分离报告 | `records::tests:unpriced_reservation_stays_on_the_books_as_nominal` / `settling_a_nominal_reservation_clears_the_nominal_slice` | 通过；`nominal_usd` 随结算退回，不出自相矛盾的账 |
| 找不到预留的结算丢弃 | `records::tests::settlement_without_a_reservation_is_ignored`、`settlement_pairs_by_iteration_not_just_stage` | 通过；重复投递不把一次派发烧成两笔钱 |
| stage 结账收掉配对键（账本有界） | `records::tests::committed_stage_prunes_pairing_but_keeps_the_money` | 通过；按 (task, stage) 收敛，不跨 task 串台 |
| **检查点带未结算预留的 JSON 往返** | `records::tests::checkpoint_with_unsettled_reservations_roundtrips_through_json` | 通过；同一个测试还直接断言「元组 key 的 map 序列化成 JSON 会失败」——因为**空 map 恰好能过**，只有非空往返 + 这条反断言才钉得住这个形状（`Checkpoint.reserved` 因此存数组而不是 map） |
| 价格来源两端可缺 | `sa_demo::tests::price_ledger_prices_a_peer_from_role_then_registry` / `an_unpriced_backend_books_nominal_not_zero` | 通过；role→model→费率拼装，缺任一端退回名义值 |
| 在线投影 == 全量重放（含新账本字段） | `journal::tests::projection_equals_a_full_replay_after_every_append` | 通过；预留/结算/名义/token 四类状态都不分裂 |
| 文档数字不再人抄 | `discipline::metrics_lines_match_the_tree` | 通过；三份文档各只有一行 `**Metrics**`（design/报告另有 `**Integration**`），与源码树重算值逐字符对照 |

**验证状态**：`cargo check --all-targets`、`cargo clippy --all-targets -- -D warnings`、
`cargo fmt --check` 本轮已跑并干净；**`cargo test` 全量三闸门按仓库惯例仍由人工执行**
（本仓库不挂 CI，见 §1 与 design §9.19⑥）。

顺带留下的两处判断，记下来免得日后被当成疏漏：
① `--max-usd` 的默认值 6.0 仍是「名义时代」的数字——按价结算后每次派发的预留远小于 0.5，
预算因此几乎不会触发。要复现停损行为，请把 `--max-usd` 调小（或删掉 `cost` 退回名义记账），
这条属于 §10 的「默认策略」范畴，库不替人猜上限。
② 缓存写费率缺省按 input 计，是**轻微低估**（厂商通常加价 ~25%）；缓存读缺省按 input 计则是
高估。两者都写进 `Usage::cost_usd` 的文档注释里，需要精确就在 `providers.toml` 填
`cache_read` / `cache_write`。

## 14. 2026-11-03 journal 轮转的默认策略（活文件有界）

§10 上那条「机制已齐、阈值仍由调用方给」收口：缺省从「永不截断」改成「活文件 512 条 / 1 MiB
双上限」，三态 CLI（没给 = 默认，`0` = 明确关闭，其余 = 自定义）。阈值的来历不是派发节奏，而是
「崩溃对账与丢铃巡检要从活文件里读多少东西」的上界（详见 design §9.28）。

| 项 | 用例 | 结果 |
|---|---|---|
| 默认策略与不截断**结论逐字段相等** | `m4_bus::the_default_rotation_policy_changes_no_conclusion` | 通过；12 轮 × 4 stage 同一条 pipeline 跑两条 journal，每个 stage 结算后投影与未结算的铃都相等，活文件不越过上限，`replay_all()` 折出来 == 不截断的那份 |
| 策略真的反复触发 | 同上（`rotations >= 3`、`segments >= 3`） | 通过；不是「测试里没人轮转」的空过 |
| 生产与测试同一条路径 | `sa_demo::rotate_at_boundary` 被测试直接调用 | 通过；测试不复制触发条件（复制了就会「测试绿、生产不轮转」） |
| 三态解析、两个上限各自独立 | `sa_demo::tests::rotation_policy_is_three_state_per_ceiling` | 通过；只封顶条数时字节上限仍跟默认 |
| 默认值的量级护栏 | 同上 | 通过；512 条不至于天天折文件，1 MiB 不至于挡不住增长 |

**验证状态**：`cargo fmt --check`、`cargo clippy --all-targets -- -D warnings`、`cargo test`
本轮全部实跑（见 §1 的 Metrics 行——它由 `tests/discipline.rs` 对照源码树校验，本轮就被它抓到
过一次漂移：新增测试后文档仍写着 203）。沙箱内 UDS `bind` 会以 `EPERM` 失败，需在真机跑
`cargo test`，属环境限制（§1 已注明）。

一处**故意没做**：归档段自身仍然不治理（不压缩、不清理）。默认策略落地后长运行会攒出很多段，
这条被单列进 §10，因为「顺手删段」会撞上 §9.22 那条「这不是删除」的语义边界。

## 15. 2026-11-03 证据的类型化（按类型核对 + stage 要求齐全）

§10 最后一条新边界落地：判定层此前只问「路径在不在盘上、有没有内容」，于是 `PASS` 只要在句子里
塞一个存在的路径就能过。现在证据有类型，类型是**能在盘上被反驳的那句话**（详见 design §9.29）。

| 项 | 用例 | 结果 |
|---|---|---|
| 声称 `test_log` 就要有测试汇总行 | `evidence::tests::test_log_must_actually_summarize_a_test_run` | 通过；`test result: ok. 12 passed` 过，写着「trust me」的 txt 被反驳 |
| 声称 `diff` 就要有 diff 特征 | `evidence::tests::diff_must_look_like_a_diff` | 通过；`diff --git` 或 `--- / +++ / @@` 至少一组 |
| 截图看魔数与大小，不看扩展名 | `evidence::tests::screenshot_checks_the_magic_bytes_and_the_size` | 通过；改名 txt 被拒，带 PNG 头的 8 字节占位被拒 |
| 认不出的类型不猜 | `evidence::tests::unverifiable_kind_is_an_issue_not_a_pass` | 通过；它连 `file` 档都不算 |
| 类型化声明的解析 | `evidence::tests::typed_evidence_lines_carry_their_kind` / `unknown_kind_word_falls_back_to_paths_not_nothing` / `same_path_under_two_kinds_is_two_claims` | 通过；`EVIDENCE <kind> <path...>`，认不出类型名时整行仍按路径收，不丢声明 |
| 写读同源（线名只有一份） | `evidence::tests::serialized_kind_parses_back` | 通过；`Serialize` 手写而非 derive，避免第二组线名 |
| 判定层按类型核 + 要求齐全 | `sa_demo::tests::a_claimed_kind_is_verified_not_trusted` / `a_plain_file_cannot_fill_a_typed_requirement` | 通过；要求看的是**核对通过**的类型，普通文件顶不出 `test_log` 那一格 |
| 老工件形状仍可用 | `sa_demo::tests::legacy_artifact_shapes_still_parse` | 通过；裸路径数组、缺 `kind` 的对象、正文提取都按 `file` 走 |
| `file` 是下界，不是并列档（回归） | `evidence::tests::a_verified_stronger_kind_also_fills_the_file_floor` | 通过；核对通过的 `test_log` 满足 `file` 要求，反向不成立，全都没通过时下界也不满足 |
| 要求表坏 = 硬失败 | `sa_demo::tests::evidence_spec_parses_stages_and_fails_closed_on_unknown_kind` | 通过；非 JSON、认不出的类型名都拒绝启动（当成「没有要求」就是 fail-open） |

**验证状态**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test`
本轮全部实跑，计数见 §1 的 Metrics 行（由 `tests/discipline.rs` 对照源码树校验）。

**没做的事，说清楚**：`harness/roles/sa.evidence.json` 缺省四个 stage 只要 `file`，也就是
类型化之前的语义——**默认行为不变**。把 `test`/`build` 改成 `test_log` 的价值要靠真端点联跑来验
（模型会不会照契约写 `EVIDENCE test_log <路径>`、写的日志里有没有真汇总行），离线阶段拿 demo 默认
为它背书就是把主张当证据。类型化本身已经生效：worker 一旦声明类型，那份声明就会被按类型核对，
不需要等要求表收紧。

另一处刻意分工：`test_log` 只证明「有一份像测试日志的东西」，**不证明测试通过**
（`0 passed; 14 failed` 同样合格）。结论仍由判定词给，形状由类型管——要证据自带结论，
就得再开一格（解析 runner 退出码 / 结构化输出并把过与败写进 journal），这条记在 §10。

## 16. 2026-11-03 审批批复的可靠投递（at-least-once + 按 req 幂等）

§10 上「批复的可靠投递」这一格收口了一半：总线仍然不猜「大概没送到」（§9.21 那条线不动），
但审批改由应用层按 at-least-once 投递——代价不对称这件事得做完（重发一份帧 vs 一次静默 deny
之后整次回派）。详见 design §9.30。

| 项 | 用例 | 结果 |
|---|---|---|
| 重发把批复拿回来（真总线 + 真 worker 池） | `m5_pool::an_unanswered_approval_request_is_resent_and_answered_once` | 通过；测试侧故意不答第一份请求，worker 重发后拿到批复，工具跑成、结果铃回来 |
| 重发的是同一颗请求 | 同上（断言两份 `approval_request` 的 `req` 与 `command` 完全相同） | 通过；发的是存着的原件，不是重新拼一遍 |
| 重复批复只生效一次 | 同上（SA 连答两遍 `allow`，工件 `approvals` 长度为 1） | 通过；同一个 req 的第二份 allow 不再唤醒工具 |
| 最后一次重发必须早于 deny | `tools::tests::approval_retries_land_before_the_deny` | 通过；对 5s / 60s / 120s 三种窗口断言 `2 × period < timeout` |
| 节奏两端有夹子 | `tools::tests::approval_retry_period_is_clamped_at_both_ends` | 通过；60ms 窗口 → 50ms 下限，1h → 30s 上限，demo 的 60s → 20s |
| 审批者按 req 幂等 | `sa_demo::tests::approval_ledger_dedups_by_req_and_survives_a_restart` | 通过；重发不再落第二对 `approval_requested`/`approval_decided`，答案原样重发；重启后从 journal 播种仍认得该 req |
| 播种的读法有界 | `sa_demo::tests::approval_ledger_dedups_by_req_and_survives_a_restart`（`seed` 走 `replay()`） | 通过；只读活文件。比活文件更老的 req 不可能再被重发；真收到迟到副本也只是多一条记录，第二次唤醒由 worker 的在途表挡住 |
| 「请求已落盘、批复未落」的崩溃点 | 同上（半写 journal） | 通过；不再记一次请求，但必须去判 |

**没做的事**：这不是 exactly-once，也不是跨进程重启的可靠投递。回合结束即关闭在途请求
（§9.26② 的路由表有界），所以**批复迟到**的语义仍停在超时 deny；「SA 下班、明早再批」需要
ack + 跨回合的审批槽位，那条留在 §10。播种只解决 req 的幂等，不做「重启后主动重答所有未决
请求」——那等于把审批改成 pull 模式，与 §9.21「总线不猜」是两种取舍。

**验证状态**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test`
本轮全部实跑（计数见 §1 的 Metrics 行）。沙箱内 UDS `bind` 会以 `EPERM` 失败，`m4_bus` /
`m5_pool` 需要在真机跑（§1 已注明）。

## 17. 2026-11-03 批复的回执：没生效的批准不许记成生效

§9.30 让请求送得到，这一格回答「批复到底生效了没有」。此前 SA 答完就忘、worker 回合结束就关单，
于是「批复丢了」与「批了但没人听」在账上无法区分（详见 design §9.31）。

| 项 | 用例 | 结果 |
|---|---|---|
| 到点撤摊：后来的批复不能算生效 | `core::approvals::tests::abandon_makes_a_late_reply_report_no_waiter` | 通过；`abandon` 返回真值即「当时还有等待者」，撤摊后 `complete` 只能说 no |
| 到点要广播「这一格已过期」 | `m1_tools::bus_approval_without_answer_denies_on_deadline`（扩展） | 通过；`ApprovalExpired{req, reason}` 发出、台账里不再有等待者、重复批复不再被当作生效 |
| 工件不谎报生效（真 worker 池 + 真总线） | `m5_pool::an_expired_approval_is_never_booked_as_applied` | 通过；窗口 400ms 内问了 ≥2 次，SA 全程不答：回执 `applied:false` 且 reason 含 timed out，工件记 `decision:"expired"` / `applied:false`，回合照常收敛 |
| 生效与重复各回一份回执 | `m5_pool::an_unanswered_approval_request_is_resent_and_answered_once`（扩展） | 通过；两份 `allow` 换回两张回执，一张 applied=true、一张 applied=false（pump 找不到在途请求也要回执，不静默丢） |
| 回执线形严格 | `serve::tests::approval_receipt_wire_shape_roundtrips` / `unreadable_receipts_are_not_applied_or_recorded` | 通过；少 `req` 或少 `applied` 都不当回执——要落审计链的东西不能靠猜 |
| 回执落账 + 未回执的补投集合 | `sa_demo::tests::approval_ledger_dedups_by_req_and_survives_a_restart`（扩展） | 通过；`unacked()` 找得出来、回执一到就离开集合、重启读回 `acked` 且不再补投 |
| 审计记录不动状态 | `journal::records::tests::approval_and_usage_records_are_audit_only`（含新记录） | 通过；`ApprovalAcked` 序列化往返 + 不结算任何铃 |

**顺带修掉的缺陷**（这才是这一格的主要价值）：`ApproveMode::Bus` 到点时只返回错误、**等待者仍留在台账里**。
后来那份批复于是「发送成功」，CLI 就把一次从未生效的批准记成 `{"decision":"allow"}` 写进工件——
journal 与工件会互相印证一个假结论。现在到点即撤摊并广播过期事件，工件写 `expired/applied:false`。

**验证状态**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 本轮实跑。
`m4_bus` / `m5_pool` 需要真 UDS `bind`（沙箱内 EPERM，§1 已注明）。

**没做**：`applied:false` 的迟到批复不会在下一次同样调用上被重新兑现（那等于把一次 allow 变成长期
通行证；要放宽只能改 `sa.approvals.json` 那一格）。跨回合挂起的审批槽位（「SA 下班、明早再批」）
仍在 §10。
