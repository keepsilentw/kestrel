# kestrel 真实端点测试报告

> 日期：2026-09-09/10 首轮；2026-09-17 补 Qwen 一轮。对象：kestrel MVP（M0-M4）。
> 范围：真实 LLM 端点 smoke（plan §4-M3 验收项 7 / §4-M0 验收项 6）。
> 凭据来源：BigModel 与 DeepSeek 取自 `~/.claude/settings.json` / cc-switch DB（报告中脱敏）；
> Qwen 取自 cc-switch 的百炼 Token Plan 条目（codex 与 claude 两侧凭据实测互通）。

## 1. 自动化测试基线（cargo test）

**Metrics** — src 51 files / 17338 lines · tests 9 files / 3383 lines · 249 tests (lib 196 + bin 4 + integration 49)
**Integration** — core_loop 5 / discipline 2 / m1_tools 9 / m2_session 6 / m4_bus 15 / m5_pool 6 / sandbox_e2e 4 / wire_anthropic 2

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

1. ~~联跑是否绿取决于 worker 当次能否收敛~~ —— 2026-11-04 一轮把「跑不起来」的三处根因拔掉后收敛了（§18）：剩下 dev iter1 那一次「8 回合内没给判定词」是模型行为，不是通路缺陷；失败路径（回派 → iter 上限 → 非零退出）仍是真实行为。
2. ~~`ApproveMode::Bus`（worker 把审批反向请求发给 SA/真人）未接线~~ —— 2026-10-28 接线（§9）：请求带 `tool`+`args`+命令，SA 按 `roles/sa.approvals.json` 判定，超时/断开由 worker deny。~~**尚未在真端点联跑里验证**~~ —— 2026-11-04 真端点联跑验证（§18）：5 请求 / 5 判定 / 5 回执成对落 journal，被拒的命令确实没跑，工件里的 `decision`/`applied` 与实际一致。
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

## 18. 2026-11-04 真端点联跑：十一项二期改动的第一次端到端验证

对象：`harness/run-demo.sh` 四进程（hub + RD=DeepSeek `deepseek-v4-flash` + QA=BigModel
`glm-5.3-flash` + demo-sa），凭据由 `~/.kestrel/providers.toml` 经 `!sqlite3` 从 cc-switch 取。
这一轮的意义不在于「又跑了一次」，而在于 §9.20–§9.31 那十一项**只有 faux 与总线级 e2e 覆盖**，
而报告自己在 §8.4② 写着「尚未在真端点联跑里验证」——欠了 7 天的账。

前两次都没收敛，且都不是「模型不稳」：

| 现象 | 根因 | 修法 |
|---|---|---|
| RD 三轮写不进 worktree（六个候选路径全 `outside writable roots`） | `rd.toml` 的只读岛写成 `$KESTREL_WORKTREE:-…/.git`，而 `:-` 拥有整条后缀：env 一设，岛就展开成 worktree 本身，`.git` 消失 → 整个工作树只读 | 库里补 `${NAME:-default}` 花括号形式（此后能表达「默认值再拼路径」；空 fallback 仍硬失败），role 改用它；`shipped_roles_do_not_island_away_their_own_write_root` 读真实 role 文件断言岛不覆盖写根（design §9.32①） |
| Review 连续三轮诚实 FAIL：`import pytest` → `ModuleNotFoundError` | role 的 `sandbox.network = "none"` ⇒ pip 永远装不上 ⇒ 这条验收条件在本环境**不可能**满足，而回派只是重复同一个 FAIL | `systems/rd.md` 增「测试只用这台机器上真有的东西」（标准库 `unittest` / `cargo test`；bash 的 cwd 已是 worktree，`cd X && …` 整条按最严一段判会被拒），两份提示词的 `pytest.log` 例子换 `unittest.log`（§9.32②） |

修完的一轮（`pipeline complete`，退出码 0）：

| 检查 | 实测 |
|---|---|
| 阶段收敛 | dev iter1 fail（8 回合内无判定词，模型行为）→ dev iter2 **pass** → review / test / build 各 iter1 **pass** |
| journal 真相源 | 45 条：`stage_written`/`bell_sent`/`budget_spent`/`bell_received`/`result_committed`/`usage_reported` 各 5，write-ahead 顺序成立 |
| 审批闭环（§9.31） | `approval_requested` 5 / `approval_decided` 5 / `approval_acked` 5 成对；被拒命令确实没跑，工件 `decision:"deny", applied:true` 与回执一致 |
| 重发与幂等（§9.30） | 现场抓到一颗 re-sent：重复批复换回 `applied:false` 且台账标重，不落第二对请求/判定 |
| 轮转默认策略（§9.28） | 45 < 512 ⇒ 无归档段（`journal.seg*.jsonl` 为空），活文件未被截断——与「默认值不是关闭」一致 |
| task 命名空间（§9.23） | 工件名 `harness-rd-demo-dev-iter2.json` / `harness-qa-demo-review-iter1.json` 带 task 前缀 |
| 预算（§9.27） | 开工即打印 `budget stays NOMINAL for harness-rd,harness-qa`；结算 2.5000 usd 全 nominal（`providers.toml` 未填 `cost`）——「没人填价」没被记成「这次免费」 |
| 判定可信度独立复核 | 我另跑 `python3 -m unittest discover -s /tmp/kestrel-worktree` → `Ran 26 tests in 0.011s / OK`，与 QA 声明一致，不是引用 RD 的旧输出 |

**闸门**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 本轮实跑全绿
（178 lib + 4 bin + 47 integration）。`**Metrics**` 行先红后绿：新增两个测试后忘了同步文档，
`discipline::metrics_lines_match_the_tree` 直接把正确的那一行打出来——这道闸门第一次按设计拦住我本人。

**仍开着**（design §10 新添两格）：失败回派仍打给同一个 verifier（缺陷常在写者产物里，验证者无法
自证式返工）；重定向 `>` 在 tokenizer 里解析失败 ⇒ 整条 Ask ⇒ 被拒，于是 worker 拿不到 stdout，
`test_log` 这类证据在 demo 里落不成盘。dev iter1 那次「轮预算耗尽无判定词」是模型行为，与 §7 的
遗留说法一致。

## 19. 2026-11-04 第三轮联跑：证据带上结论，以及一次「声明档位」的翻车

§9.34 落地后（`read_test_run` 读 `exit:` 行 + 数 `N passed`/`N failed`/`failures=`，
`Verdict.test_runs` 随判定落 journal），用最终二进制连跑两轮。

**第一轮没过，而且原因值得记**：`dev iter2`/`iter3` 都是
`fail (no verified test_log evidence: this stage requires one)`。盘上那份 `unittest.log`
本身是好的（`exit: ExitStatus { code: Some(0), … }`、32 例全过），问题是 RD 这次只把它写进
`evidence:` 那一行——而那一行的档位是 `file`。SA 只核对文件**声称的那种东西**，所以那一格
没人顶。不是新层误判（同一份日志按 `test_log` 声明时逐条通过），是提示词留了个歧义：
「证据可以声明类型」听起来像可选项。修法是把话写死（`systems/rd.md`：要交测试日志必须**另起
一行** `EVIDENCE test_log <绝对路径>`，同一个路径在两处各出现一次不算重复），并把这次的后果
原样引进提示词。

**第二轮（收敛）**：

| 检查 | 实测 |
|---|---|
| 阶段 | dev iter1 pass → review / test / build 各 iter1 pass → `pipeline complete`，4 颗门铃、2.0000 usd 全 nominal |
| 现场打印的结论 | 每一关都打 `[sa] <stage> iter1: test log /tmp/kestrel-worktree/unittest.log -> exit 0, ran 32, 32 passed / 0 failed` |
| 结论落账 | journal 27 条；四条 `result_committed` 的 `verdict.test_runs` 都是 `{exit_code: 0, exit_signal: null, passed: 32, failed: 0, ran: 32}`——事后能问「那一次到底过了几条」 |
| 计数从哪来 | unittest 只打 `Ran 32 tests` + 一行 `OK`，不打 passed 计数：按它自己的语义补（`unittest_ok_line` 只认整行 `OK` / `OK (…)`，不按 `contains` 匹配正文里的两个字母） |
| 独立复核 | `python3 -m unittest test_lru_cache` → `Ran 32 tests in 0.008s / OK`，与落账的 32/0 一致 |
| 反驳能力 | 单元层已钉：`a_done_claim_with_a_failing_test_log_is_rebutted`（同一份工件，日志换成 exit 1 就 FAIL）、`a_landed_test_log_carries_its_exit_status`（含 `signal: Some(9)` 那条：没有退出码也算没过）。真端点上这次没有跑挂的日志可反驳——不假装它发生过 |

**闸门**：`cargo fmt --check` / `clippy -D warnings` / `cargo test` 全绿，234 tests
（lib 182 + bin 4 + integration 48）。本轮新加 4 项：`read_test_run` 的退出状态与计数、
`unittest` 的 `OK` 行推断、`verdict_test_runs_roundtrip_and_old_lines_still_replay`（老
journal 行没有该字段仍可读，缺席读成空而不是 0）、以及上面那句反驳。

**踩到的解析坑（值得留在台账里）**：`key_number_i64` 起初写成「往后扫第一个数字」，于是
`code: None, signal: Some(9)` 把 9 读成退出码——把「没有读数」记成「退出码 9」。现在只吃
key 紧跟着的那个值（剥 `:`/`=`/空白与 `Some(`），读不到就是 `None`。这行代码的 bug 恰好是
这一格最反对的那类错误：把缺席当结论。

## 20. 2026-11-04 第四轮联跑：结论进了投影，并且有人读它

§9.35（`StateSnapshot.tests` + `Checkpoint.tests` + 两个读者）落地后，再跑一轮真端点。

| 检查 | 实测 |
|---|---|
| 阶段 | dev iter1 fail（8 回合内无判定词）→ dev iter2 pass → review / test / build 各 iter1 pass → `pipeline complete`，5 颗门铃、2.5000 usd 全 nominal、journal 33 条 |
| 落账的形状 | 5 条 `result_committed` 里，**失败那条 `test_runs` 是空的**（`no PASS/DONE/FAIL verdict…`，什么都没数出来），另外四条各带 `{passed: 27, failed: 0, exit_code: 0}` |
| 投影的覆盖语义 | 收尾只打印 **4** 行结论而不是 5 行：dev 那关被 iter2 那份通过覆盖掉了 iter1 那份空结论——状态回答「现在是什么」，历史仍在那五条记录里 |
| 读者之一（报账） | `[sa] test conclusion [demo] Dev: green — exit 0, ran 27, 27 passed / 0 failed`（Review/Test/Build 同）；读的是 `journal.projection()`，与预算停损同一份状态 |
| 读者之二（返工铃） | `ref.rework.last_test = {green, runs}`；本轮没走到返工（四关一次过），所以这一条是**单元级**验证（`the_test_conclusion_is_state_not_just_payload` + §23.2 那轮的实弹路由），不假装现场发生过 |
| 独立复核 | `python3 -m unittest test_lru_cache` → `Ran 27 tests in 0.002s / OK`，与投影里那 27/0 一致 |

**闸门**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 实跑全绿，
235 tests（lib 183 + bin 4 + integration 48）。新增 1 项：`the_test_conclusion_is_state_not_just_payload`
——覆盖 / 撤掉 / 轮转往返三件事在同一次折叠里验，因为在线投影与 `reconcile` 共用同一个 `Folder`。
`**Metrics**` 又被拦红一次（加了测试与实现），照它给的数字回写。

**轮转没在现场被触发**（33 条 < 默认 512），检查点带结论这件事只有单元级覆盖（构造检查点 →
JSON 往返 → 重放等于在线）。要现场验它得跑到 512 条记录，那是另一轮的量级，不在今天这一格内。

## 21. 2026-11-04 第五轮联跑（带费率表）：账单与「顶不起账单的读数」第一次同场分岔

前置：`--prices` 指向一份**仓库外**的实验件（provider/model 与 `cost` 齐全；单价是任意取整的
合成数字 `input 0.2 / output 0.8` per MTok，**不是任何市场报价**；`key` 是占位符——SA 只从这里
读 cost，拨号仍走 worker 自己的 `~/.kestrel/providers.toml`）。`run-demo.sh` 为此新增
`KESTREL_DEMO_PRICES`（没设 = 不传 flag，走用户层默认）。

| 时刻 | SA 打印 | 这一行走的是哪条路 |
|---|---|---|
| dev iter1 | `settled 0.0175 usd for 55228 in / 8117 out tokens` | DeepSeek 报了 input ⇒ 按 token 结算成真账单（§9.27 那条路径第一次在真端点上算出钱，不再停在名义值） |
| dev iter2 | `settled 0.0016 usd for 3180 in / 1226 out tokens` | 同上，随后 `test log … exit 0, ran 34, 34 passed / 0 failed` → pass |
| review iter1 | `harness-qa reported 0 input tokens with 8197 out — the gateway under-reports (§9.7), so this books the reservation 0.0320 usd as NOMINAL instead of a bill that would read too cheap` | BigModel 的 0 输入读数顶不起账单：结算照做（配对键消耗）、钱按预留落、名义那一份跟着涨 |
| test / build | 同上一条（`4453 out` / `1684 out`） | 同一判断在每一关都独立成立，不是一次性的 |
| 收尾 | `ledger harness-qa: 0.0960 usd ; tokens in 0 / out 14334` ; `ledger harness-rd: 0.0192 usd ; tokens in 58408 / out 9343` ; `ledger total: 0.1152 usd, of which 0.0960 nominal` | 两本账分开的样子第一次看得见：rd 那 0.0192 是**算出来的**，qa 那 0.0960 是**说不清的**；token 读数即使不可信也照样留档 |

`pipeline complete`，退出码 0；开工首行不再有 `budget stays NOMINAL for …`（两端都有价了），
分岔发生在结算那一刻而不是开工那一刻——这正是 §9.36 想钉的位置。

**闸门**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 实跑全绿，
237 tests（lib 185 + bin 4 + integration 48）。本轮新增 2 项：`a_zero_input_reading_is_not_a_bill`
（三种读数：0 输入有输出 = 少报；报了输入 = 不算少报；什么都没报 ≠ 少报，那条走「没有价」）、
`booking_a_priced_reservation_back_as_nominal_keeps_the_money`（钱留在账上、名义那份跟着涨、配对键
消耗掉不被二次结算烧穿）。

**账本上看不见的东西**（design §10 已记）：`nominal_usd` 现在混着两个来源——没填价、与读数不可信。
收尾只说 `of which 0.0960 nominal`，理由活在逐次派发的打印与 journal 历史里。要分开报就得给
`BudgetSettled` 加一个名义来源标记并让它参与折叠，那是账本形状的改变，不顺手做。

**没做的事**：没有改动任何人的 `~/.kestrel/providers.toml`（单价跟着 provider 住，那是用户的配置层）；
费率实验件写在仓库外，跑完即删，docs 里留下它的形状与那句「单价是合成数字，不是市场报价」。

## 22. 2026-11-04 第六轮联跑（带费率表）：说不清的钱，现在说得出为什么说不清

§9.37（`NominalReason` + 投影里的 `nominal_by_reason` + 检查点携带）落地后再跑一轮，费率实验件同 §21
（仓库外、合成单价、跑完即删）。

| 检查 | 实测 |
|---|---|
| 阶段 | dev iter2 pass → review / test / build 各 iter1 pass → `pipeline complete` |
| 账单那一份 | `dev iter2: settled 0.0021 usd for 4330 in / 1578 out tokens`；`ledger harness-rd: 0.0207 usd ; tokens in 53279 / out 12507` |
| 说不清那一份 | 三关都是 `reported 0 input tokens with 3156/2408/1587 out — … books the reservation 0.0320 usd as NOMINAL`；`ledger harness-qa: 0.0960 usd ; tokens in 0 / out 7151` |
| **新的那一行** | `ledger total: 0.1167 usd, of which 0.0960 nominal (under-reported 0.0960)` |
| 结论状态 | 四关各打印 `test log … -> exit 0, ran 53, 53 passed / 0 failed`（§9.35 那本账还在报） |

这一行值钱在哪：`no rate` 与 `under-reported` 的下一步动作完全不同——前者是去 `providers.toml`
补价，后者是去修读数或换端点，**补价根本不解决问题**。合并成一个数字时，读账的人只能猜；分开报之后
不需要猜。分类与总额的一致性由 `nominal_money_says_which_reason_it_is` 直接断言
（`sum(by_reason) == sum(nominal_usd)`），分类不是装饰。

老记录的读法也钉住了：没有来源字段的 `BudgetSettled{priced:false}` 一律算 `NoRate`，**不猜成
「少报」**——猜了就等于用今天的知识改写历史账本。

**闸门**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 实跑全绿，
238 tests（lib 186 + bin 4 + integration 48）。新增 1 项 `nominal_money_says_which_reason_it_is`
（两个来源分开、与总额一致、老行按 NoRate、检查点 JSON 往返后重放相等），并把 §21 那条
`booking_a_priced_reservation_back_as_nominal_keeps_the_money` 升级为带来源标记的形状。

**没做**：分类只到「来源」这一层，还没细到 **peer × 来源**（看不出是哪个 peer 说不清，本轮靠逐次派发的
打印看得出来）；以及**没有把费率实验件放进仓库**——单价跟着 provider 住，仓库里不该有一份假价。

## 23. 2026-11-04 第二轮联跑：`test_log` 第一次可交，FAIL 路由第一次回跳

> 编号说明：本节记的是**第二轮**联跑，物理位置却排在最后——是我给它选错了插入锚点，不是内容
> 后补。按发生顺序它应排在 §18 与 §19 之间；内容一字未改，编号与内部小节号已按物理位置改成 23。

§18 之后接着落地 design §9.33（bash `output_path` + 验证 FAIL 回跳写者 + unittest 汇总行）。
这一节的规矩和 §18 一样：**只写盘上真发生过的**。两轮，各证一件事。

### 23.1 第一轮：默认证据表把 `dev` 提到 `file+test_log`

`roles/sa.evidence.json` 从此要求写者交出「真跑过测试」的盘上形状。开工首行即打印
`evidence requirements: build:file, dev:file+test_log, review:file, test:file`。

| 检查 | 实测 |
|---|---|
| 阶段 | dev iter1 fail（8 回合内无判定词，模型行为）→ dev iter2 pass → review / test / build 各 iter1 pass → `pipeline complete`，5 颗门铃，2.5000 usd 全 nominal |
| `output_path` 落盘 | worktree 里真出现 `unittest.log` / `doctest.log`，文件头是内核写的 `command: python3 -m unittest test_lru_cache -v` + `exit: ExitStatus { code: Some(0), signal: None }` + `--- stdout ---` |
| 按类型核对 | dev 工件的 `evidence` 里两份都是 `kind: "test_log"` 且核对通过（`Ran 25 tests in 0.004s / OK` 命中新加的 `Ran ` 标记；`4 passed.` 命中既有的 `passed`）。**没有 shell 重定向参与**：RD 试 `> log` 那类命令仍会被 tokenizer 拒，这次它走的是工具参数 |
| 独立复核 | `python3 -m unittest discover -s /tmp/kestrel-worktree` → `Ran 25 tests / OK`，与工件声明一致 |

### 23.2 第二轮：故意让验证过不了，看路由往哪儿派

临时把 review 提到 `["diff"]`（`KESTREL_DEMO_EVIDENCE_SPEC=<tmp>/kestrel-evidence-diff.json ./harness/run-demo.sh`，
表内容 `{"dev":["file","test_log"],"review":["diff"],"test":["file"],"build":["file"]}`，写在 repo 外：
这是一次性的实验条件，不是要提交的验收标准。dev 盘上已有源文件与日志，所以它会先 pass——
目的只有一个：制造一次**验证类 stage 的 FAIL**）。

| 时刻 | SA 打出来的那一行 | 为什么这行是新的 |
|---|---|---|
| review iter1 | `fail (no verified diff evidence: this stage requires one) -> rework by harness-rd (1 credit(s) left)` | 回派对象是**写者**，不再是刚判 FAIL 的 QA；额度显式打印 |
| dev iter2 | `fail (empty result, worker stop: round budget exhausted) -> re-dispatch` | 返工铃打给 RD 后它自己没做成——这时才该原地重派（`rework_target(Dev) == None`） |
| dev iter3 | `pass` | 返工确实被接住（`ref.rework` 进了铃，RD 去试 `make_diff.py` / `changes.diff`，被审批拒了几条命令后交了活） |
| review iter2 | `fail (...) -> rework by harness-rd (0 credit(s) left)` | 第二次回跳，额度归零 |
| 结束 | `Error: [sa] iteration cap reached for Dev` | 显式失败、非零退出——**没有**静默放宽那条 diff 要求 |

这一轮证的是**路由发生**，不是返工必然收敛：RD 两次没能交出符合 `diff` 形状的文件（一次轮预算
耗尽、一次空结果），所以最终仍判失败。这是诚实结果，也顺带挖出一个缺陷——返工派发吃的是写者
自己的 iter 预算，而 `iteration cap reached for Dev` 把原因藏了起来（design §9.33⑦）。上限现在会
说明「其中几次是验证者要来的返工」。

### 19.3 闸门

`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 本轮实跑全绿：
231 tests（lib 179 + bin 4 + integration 48）。新增三项：`a_verifier_fail_routes_back_to_the_writer`
（路由规则，不需要总线）、`bash_output_path_lands_a_log_inside_the_write_root`（落盘 / 越界 Blocked /
父目录不猜 / 落进的文件要能过 `test_log` 那一档核对）、unittest 汇总行的两条断言。`**Metrics**` /
`**Integration**` 两行今天被 `discipline` 拦红三次（加测试、再加测试、改完源码行数变了），每次都照它
给的数字回写——这道闸门拦的是写代码的人自己。

**用最终二进制在干净工作树上复跑了一遍**（23.1/23.2 跑的是改动过程中的构建）：dev iter1 fail（8 回合内
无判定词）→ dev iter2 pass → review / test / build 各 iter1 pass → `pipeline complete`；journal 42 条
（`stage_written`/`bell_sent`/`budget_spent`/`bell_received`/`result_committed`/`usage_reported` 各 5，审批
三件套各 4），RD 工件的 `test_log` 那一格核对通过，盘上 `unittest.log` 写的是 `Ran 37 tests in 0.003s /
OK`——我独立重跑 `unittest discover` 得到同一句，不是引用谁的输出。

**当时的下一步**：`exit:` 那一行已经在日志里了，SA 还没有去读它——`test_log` 只证明形状、不证明结论。
这句话在本节写下时是实情，随后由 §19（第三轮）走完：现在 `read_test_run` 会读那行 `exit:`，
跑挂的日志能顶回那句 `DONE`，计数随判定落账（design §9.34）。留在这里是因为它就是当时的状态，
不要把它改成好像早就做完的样子。

## 24. 2026-11-04 第七、八轮联跑：轮转第一次在现场发生，以及一个白烧预算的假 FAIL

把 §9.28 的默认阈值临时压到 24 条（`KESTREL_DEMO_ROTATE_AFTER=24`），目的是让**截断这件事在真端点上
真的发生一次**——在此之前，轮转路径只有合成数据单测覆盖过。带费率表（同 §21 那份仓库外的合成单价件）。

### 24.1 第七轮：现场抓出一个假 FAIL

轮转确实发生了（`journal rotated: 15 record(s) archived into harness/journal.seg0-15.jsonl, live file
now 16 record(s)`），但 pipeline 最终非零退出：

```
review iter1: fail (evidence missing: /tmp/kestrel-worktree/unittest.log（其内含汇总行) -> rework by harness-rd
Error: [sa] iteration cap reached for Dev — 1 of those iterations were rework dispatches asked for
by a verifier that cannot fix the artifact itself
```

那份日志在盘上、`exit 0`、76 例全过。根因是 worker 把中文括注粘在路径后面（没有空格），抽取器把整串
当成文件名——`evidence missing` 在那一刻既准确又错误。代价实测：一次派发（0.032 usd 名义预留）
+ 一次返工额度 + dev 的 iteration 预算，最后以失败收场（design §9.38）。

### 24.2 第八轮：修完之后，轮转发生在 pipeline 中间而账一分不差

| 检查 | 实测 |
|---|---|
| 阶段 | dev iter1 pass → review pass → **test iter1 中途轮转** → test pass → build pass → `pipeline complete` |
| 轮转位置 | 归档 15 条 / 活文件 16 条起算；段名 `journal.seg0-15.jsonl` 就是它覆盖的区间 |
| 归档内容 | 段内 15 条：`stage_written`/`bell_sent`/`budget_spent`/`bell_received`/`usage_reported`/`budget_settled` 各 1 + 审批三件套各 3 |
| 活文件 | 首条是 `snapshot (covers 15)`，其后 22 条含 4 条 `result_committed`（**一条不少**） |
| 检查点带的东西 | `spent_usd {harness-rd: 0.00445}`（dev 那笔已在归档前缀里结算过）、`in_flight 0`、`reserved []` |
| 账对得上 | `ledger total: 0.1004 usd, of which 0.0960 nominal (under-reported 0.0960)` = 0.0044（dev 按 token 算的账单）+ 3 × 0.0320（QA 三关按名义） |
| 结论跨轮转 | 四关各打印 `test log … exit 0, ran 76, 76 passed / 0 failed`；收尾四行 `test conclusion … green` |

这次折叠切在 dev 的 `result_committed` **之前**（那条落在活文件里），所以检查点里 `tests` 是空的——
不是丢了结论：结论在活文件的那四条 `result_committed` 里，投影 = 检查点 + 活文件，收尾才打印得出四行。
这一点先前我差点读成缺陷，是数了段内/活文件里的 `result_committed` 条数（合计 4）才确认无恙。

**认账修复没能在现场复现**：第八轮 RD 没有再粘括注（四条判定的 `notes` 全为空），所以 §9.38 那个
`resolve_claim` 目前只有单元级覆盖（`an_annotation_glued_to_the_path_is_repaired_and_said_out_loud`
的三个分支：认账、前缀不存在不认、前缀是目录不认）。现场那一次是事故本身，不是验证。

**闸门**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 实跑全绿，
239 tests（lib 187 + bin 4 + integration 48）。`**Metrics**` 这次被拦红两次（分类账、认账）并回写。

**顺带修的文档账**：§19–§23 的编号此前被我插错位置（第二轮那节落到文件末尾、§22 空号），本轮按物理
顺序重编并在 §23 留下说明；§23 结尾那句「`exit:` 还没读」也改成「当时如此，随后由 §19 走完」，
不把它改写成好像早就做完的样子。

## 25. 2026-11-04 第九、十轮联跑：写侧把住路径形状，以及一次归因错误付出的两轮预算

### 25.1 第九轮：写侧修复生效，但暴露了路由的归因错误

§9.39 落地（`path_tokens` 认得全角括号 + `evidence_warnings` 进工件）后跑一轮。dev iter1 的现场：

```
[sa] dev iter1: note: worker-side shape warning: evidence claim /tmp/…/README.md`（API was read as path /tmp/…/README.md
[sa] dev iter1: note: worker-side shape warning: evidence claim /tmp/…/unittest.log`（测试输出落盘：`Ran was read as path /tmp/…/unittest.log
[sa] dev iter1: pass
```

RD 依然在粘全角括注（这是模型的真实习惯，不是我构造出来的），但**这一关过了**——§9.38 那种白烧一次
派发的假 FAIL 没有重现。写侧修复由此从「只有单测」升级为「现场见过」。

同一轮后半段露出我自己的一个错判：

```
[sa] review iter1: fail (empty result, worker stop: round budget exhausted) -> rework by harness-rd (1 credit(s) left)
[sa] dev iter2: fail (verdict: 2. `worker stop: round budget exhausted`…) -> re-dispatch
[sa] dev iter3: fail (verdict: DONE: 第三次收到同一份返工单…) -> re-dispatch
Error: [sa] iteration cap reached for Dev — 1 of those iterations were rework dispatches asked for by a verifier…
```

`empty result` 是 **QA 那一回合没交出东西**，产物根本没被可信地看过。我那条「验证 FAIL 就回跳写者」
把它当成产物的罪，于是 RD 花两轮替一个它改不动的东西辩护，最后撞 dev 的上限。这就是 §9.40：先归因
（回合类 issue vs 产物类 issue），再决定路由，回合类**原地重派、不扣返工额度**。

### 25.2 第十轮：归因修完之后，一轮干净的收敛

| 检查 | 实测 |
|---|---|
| 阶段 | dev iter1 pass → review pass → test pass → build pass → `pipeline complete`，4 颗门铃 |
| 写侧认账 | test iter1 打出两条 `note: worker-side shape warning: evidence claim … was read as path …`，而这一关照样 pass |
| 结论 | 四关各打印 `test log … exit 0, ran 22, 22 passed / 0 failed`；收尾四行 `test conclusion … green` |
| 报账 | `ledger total: 2.0000 usd, of which 2.0000 nominal (no rate 2.0000)` —— §9.37 的分类账现场第二次露面，这次是 `no rate`（本轮没带费率表），与 §22 那次的 `under-reported` 是两种不同的下一步 |

**归因修复本身没在现场复现**：第十轮 QA 没有哑火，所以那条新分支只有单元覆盖
（`a_broken_turn_is_not_evidence_about_the_artifact` 钉住两侧形状）。这一句必须留着，免得读的人以为
「跑过了就是验证过了」。

**闸门**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 实跑全绿，
243 tests（lib 190 + bin 4 + integration 49）。本轮新增 3 项：
`a_full_width_annotation_is_cut_at_the_write_side`（写侧切 + 报警，且散文里的括号不报警）、
`a_glued_annotation_is_cut_before_it_reaches_the_artifact`（m5_pool：真 worker 把工件写成干净路径 +
带 `evidence_warnings`）、`a_worker_side_shape_warning_rides_the_verdict`（写侧警告跟着判定落账，
并且**顺带抓到一个真 bug**：`notes = report.notes` 是赋值，会覆盖掉先合并进来的写侧警告，改成 `extend`）。
`**Metrics**` / `**Integration**` 各被拦红一次并回写（lib 187→190，m5_pool 5→6）。

**当时没做的一层**：归因还住在路由里，读的是 issue 的文本前缀。这一格随后由 §26 走完
（design §9.41）——保留这句原文，是为了留下「当时的边界画在哪里」。

## 26. 2026-11-04 第十一轮联跑：归因从「读措辞」提成「判定自带字段」

§9.41 落地：`Verdict.attribution: Attribution{Artifact|Turn}`（`#[serde(default)]`，旧 journal 行
照旧可读），分类在判定产出的那一刻做完并随判定落账，路由只读这个字段。

| 检查 | 实测 |
|---|---|
| 阶段 | dev iter1 fail → dev iter2 pass → review / test / build 各 iter1 pass → `pipeline complete` |
| 结论 | 四关各打印 `test log … exit 0, ran 27, 27 passed / 0 failed`；收尾四行 `test conclusion … green` |
| 报账 | `ledger total: 2.5000 usd, of which 2.5000 nominal (no rate 2.5000)`（§9.37 的分类账，本轮没有费率表，所以来源全是 `no rate`） |
| 归因的新家 | `result_committed.verdict.attribution` —— 事后能从 journal 直接问出「那一次 SA 为什么回跳/为什么原地重派」，而不是靠当时的源码去反推措辞 |
| 老账本 | `verdict_test_runs_roundtrip_and_old_lines_still_replay` 里加了一条断言：**没有归因字段的老行落在 `Artifact`**（更贵的默认），不猜成 `Turn` 把这一关放过去 |

为什么这件事值得单独一轮：上一版分类住在路由里，读的是 `"empty result"` / `"worker stop:"` 这类**字符串
前缀**。那意味着改一句 issue 的文案就能悄悄改掉路由，而没有任何测试会为此变红——正确性寄生在措辞上。
`the_verdict_carries_its_own_attribution` 因此测的是**产出端**（用真工件长出真 verdict），而不是那个辅助函数。

**没在现场复现的部分**：第十一轮没有出现回合类 FAIL（QA 每关都交出了东西），所以 `Turn` 分支这一轮
仍未在真端点上走过，只有单元覆盖。这条与 §25 末尾那句是同一件事——路由规则本身在 §25 第一轮之后
一直没被现场触发过，我把它当作**未验证**而不是「跑过很多轮所以没问题」。

**闸门**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 实跑全绿，
243 tests（lib 190 + bin 4 + integration 49）。净新增 0 项：`a_broken_turn_is_not_evidence_about_the_artifact`
（测辅助函数的文本匹配）被 `the_verdict_carries_its_own_attribution`（测产出端的真 verdict）替掉，
外加老 journal 行默认值的一条断言。`**Metrics**` 又被拦红一次并回写（src 16762→16848 行）。

**新露出的一层**（design §10）：`issues` 还是 `Vec<String>`，所以新加一类 issue 忘了判归因会**静默落在
`Artifact`**。要根治得让 issue 自己带归因变体（`JudgmentIssue::Artifact | Turn`），把「忘了标」变成
编译不过去的事。

## 27. 2026-11-05 第十二轮联跑：四关一次过，报账上两处「说不清」被抓出来

一个全新的 worktree（不复用上一次的产物，免得 RD 去评自己昨天写的那份），其余全走默认：
`--evidence-spec roles/sa.evidence.json`、`--approvals`、`--nominal-usd 0.5`、`--max-usd 6.0`、轮转走默认策略。

| 检查 | 实测 |
|---|---|
| 阶段 | dev / review / test / build **各 iter1 一次过** → `pipeline complete`（本轮没有 FAIL，所以 §9.40/§9.41 的归因与回跳都没被触发） |
| 结论 | 四关各打印 `test log … exit 0, ran 28, 28 passed / 0 failed`；收尾四行 `test conclusion … green` |
| 形状警告 | 四份工件的 `evidence_warnings` **全空**（§9.39 的写侧切分这轮没被触发，因为 RD 给的是裸路径） |
| 审批 | 2 请求 / 2 deny：`ls -l … && tail -5 …` 这种复合命令在 tokenizer 里判不出来 → Ask → SA 只认显式 allow → deny（fail-closed 正确）；RD 改用自己的读工具继续，回合没被挂住 |
| 报账 | `ledger total: 2.0000 usd, of which 2.0000 nominal (no rate 2.0000)`——两端都没填价，`priced` 全 false |
| journal | 30 条（`budget_spent` 4 / `usage_reported` 4 / `result_committed` 4，`attribution` 全是 `artifact`），远低于默认上限，轮转未触发 |

**抓出来的第一件事**：收尾那行印的是 `tokens in 0 / out 0`，而同一份 journal 里躺着
`input 5646 / output 16894`。根因在钱的形状吞掉了用量——没有费率 => `settle()` 返回 `None` =>
根本不落 `BudgetSettled`（§9.36 那条分支刻意如此），而 token 恰好只在结算配对成功时才累加。
名义记账本身没错，错在「没结算」把用量一起抹了（design §9.42①②）。

**抓出来的第二件事**：同一颗 `req` 的两张**互相矛盾**的回执。journal 落下
`approval_acked{req:0581d66b…, applied:false}`，而 RD 的工件对同一颗 req 记 `applied:true`。
顺序是：deny #1 唤醒了工具（工件 true + 回执 true）→ 那颗 at-least-once 的重发被 SA 去重后
**重投**了同一份 deny → 第二份没有等待者，只换回 `applied:false (no open request)` → 而 `note_ack`
先到先得，于是 `false` 落盘、`true` 被当重复丢弃，哪张赢取决于帧到达顺序（design §9.42③④）。
这一条正好撞在 §9.31 立的那句「journal 与工件会互相印证」上——它们这次互相打脸。

## 28. 2026-11-05 第十三轮联跑（测试费率表）：两段式结算第一次在真端点落地

两处修复的源码已就位，这轮用 `KESTREL_DEMO_PRICES` 指了一份**测试费率表**（价钱是刻意编的整数：
验的是形状，不是价钱；仓库外文件，`providers.toml` 本身没动）。

| 检查 | 实测 |
|---|---|
| 阶段 | dev / review / test / build 各 iter1 pass → `pipeline complete`（`ran 22, 22 passed / 0 failed`） |
| 结算（第一次现场跑通） | `dev iter1: settled 0.0066 usd for 5265 in / 4633 out tokens`——§9.27 的预留→结算两段式，journal 里 `budget_settled` 带着 `priced:true` 与那份读数 |
| 读数不可信那条 | QA 三关走进 §9.36 的另一支：`harness-qa reported 0 input tokens with 5168 out — … books the reservation 0.1161 usd as NOMINAL instead of a bill that would read too cheap` |
| 报账 | `ledger harness-qa: 0.3484 usd ; tokens in 0 / out 8508`、`ledger harness-rd: 0.0066 usd ; tokens in 5265 / out 4633`、`ledger total: 0.3550 usd, of which 0.3484 nominal (under-reported 0.3484)`——**混合来源第一次同场**，且 §9.37 那句「下一步该做什么」直接可读：该修读数/换端点，不是该补价 |
| 形状警告又现场复现 | `dev iter1: note: worker-side shape warning: evidence claim …/unittest.log（测试日志，落盘） was read as path …/unittest.log`，判定照样 pass（§9.39 写侧切分 + 警告跟着 verdict 落账） |
| 审批 | 1 请求 / 1 deny / 1 回执成对（`applied:true`），本轮没有出现矛盾回执 |

**这两轮的修复各自没有被现场证明**，要说清：§9.42②（用量来自读数）在本轮走不到——两端都有价，
每条派发都产生了结算；上一轮虽然是 bug 现场，跑的仍是修复前的代码。§9.42④（回执按强弱合并）
本轮只有一颗请求，没有第二张矛盾回执。两者都由单测钉住：`a_nominal_run_reports_the_tokens_it_read`、
`a_contradicting_receipt_upgrades_without_downgrading`（升级 / 不降级 / 重启读回最强那条，三个方向都断）。

**闸门**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 实跑全绿，
245 tests（lib 192 + bin 4 + integration 49）。净新增 2 项，`src` 16848→17043 行；`**Metrics**` 又被
拦红一次并回写三处文档（§9.19 那条闸门照常做事）。两条环境说明：总线测试要真 UDS `bind`、
`key_sources_resolve` 要写 `/tmp`，在严格沙箱里都会以 `EPERM` 失败，须在沙箱外执行这轮 `cargo test`。

## 29. 2026-11-05 第十四轮联跑（真实费率）：`no rate` 这一格从报账里消失

§28 用的是刻意编的整数费率（只验形状）。这一轮把它换成**官方页当天读到的真实单价**，写进用户层
`~/.kestrel/providers.toml`（费率跟 provider 住、不跟仓库住；来源、抓取日期、USD/CNY 换算与「取峰时价」
的理由都记在文件注释与 design §9.43）。走默认价格表路径（不传 `--prices`），全新 worktree。

| 检查 | 实测 |
|---|---|
| 阶段 | dev / review / test / build 各 iter1 pass → `pipeline complete`（`ran 19, 19 passed / 0 failed`） |
| 真账单 | `dev iter1: settled 0.0065 usd for 5218 in / 4086 out tokens`——按 Flash 峰时价复算 0.30×5218 + 1.20×4086（每百万）= 0.0064686，与印出来的数一致；预留 0.0581 先落盘、结算换掉它，`budget_spent` 与 `budget_settled` 各 4 条成对 |
| 开工那行 | 上一轮启动时印 `budget stays NOMINAL for harness-rd,harness-qa`，本轮**没有这一行**：两个被派发的 peer 都查得到价 |
| 网关少报 | QA 三关全走进 §9.36 的第二条分支：`harness-qa reported 0 input tokens with 6464 out — … books the reservation 0.0222 usd as NOMINAL instead of a bill that would read too cheap` |
| 报账 | `ledger harness-rd: 0.0065 usd ; tokens in 5218 / out 4086`、`ledger harness-qa: 0.0667 usd ; tokens in 0 / out 10628`、`ledger total: 0.0732 usd, of which 0.0667 nominal (under-reported 0.0667)` |
| 名义值的来源 | 分类账只剩 `under-reported`——**`no rate` 归零**。QA 那 0.0667 = 3 × 0.0222 预留额，即每一关按最坏情况落账，而不是按少掉整个输入侧的读数落账 |
| 审批闭环 | 2 请求 / 2 deny / 2 回执 `applied:true`，journal 与 RD 工件对同一颗 req 取值一致；本轮没出现矛盾回执，§9.42④ 的升级路径仍只有单测覆盖 |

**这一轮证明不了什么**：§9.42②（用量来自读数）需要一次「某个 peer 没有价、因而不产生结算」的运行，
而真实费率恰好把那条分支变成了默认配置下的不可达路径。要现场证明它，得用一份只给 RD 填价的表跑一轮
（design §9.42⑥ 末尾已记下这件事）。

**闸门**：本轮无源码变更，`**Metrics**` 两行不变（245 tests / src 17043 行），`discipline` 复跑通过。

## 30. 2026-11-05 第十五轮联跑（只给 RD 填价）：§9.42② 的现场证明

§29 之后，「某个 peer 没有价、因而不产生结算」在默认配置下已经不可达。这一轮把表换成
`prices-rd-only.toml`（RD 用真实峰时价 + 官方 1M/384K，QA 的 `cost` 整块删掉），专门走那条分支。

| 检查 | 实测 |
|---|---|
| 阶段 | dev / review / test / build 各 iter1 pass → `pipeline complete`（`ran 25, 25 passed / 0 failed`） |
| 开工警告 | `budget stays NOMINAL for harness-qa`（只列 QA 一个 peer——RD 有价） |
| 有价那一侧 | `dev iter1: settled 0.0071 usd for 4704 in / 4733 out tokens`（0.30×4704 + 1.20×4733 每百万 = 0.00709） |
| 没价那一侧 | QA 三关全部 `harness-qa has no rate — the reservation stays as the bill (nominal)` |
| journal | `budget_spent` 4 条（rd 1 + qa 3）、`budget_settled` **只有 1 条**（rd）——QA 三次派发根本没有结算记录 |
| **这一轮要看的** | `ledger harness-qa: 1.5000 usd (nominal backend: no rate) ; tokens in 0 / out 8503`。三条 `usage_reported`（4532 + 1885 + 2086 = 8503）在没有结算的情况下**照样进了用量列**；修复前这里是 `tokens in 0 / out 0`（§27 那一次的现场） |
| 报账 | `ledger total: 1.5071 usd, of which 1.5000 nominal (no rate 1.5000)` |
| 预留形状 | RD 的预留按官方 1M/384K 是 0.7608，结算 0.0071 换掉它；QA 无价 => 每次 0.5 名义 |

**仍未在现场出现过的一件事**：§9.42④（同一颗 req 的矛盾回执按强弱合并）。这一轮只有 1 颗审批请求、
1 张回执，没有第二份批复可投。那条路径要的是「审批窗口里恰好卡上一次重发」的时序，配表达不到，
所以它的证明仍然只有 `a_contradicting_receipt_upgrades_without_downgrading` 这一条单测。

**闸门**：本轮无源码变更，`**Metrics**` 两行不变（245 tests / src 17043 行）。

## 31. 2026-11-05 第十六轮联跑（直接记人民币）：钱的单位端到端换成 `cny`

§9.44 把换算整段拿掉：两份价目表按厂商原文写（DeepSeek 元、智谱 元），provider 声明
`currency = "cny"`，账本读它并当场印它。demo 的默认值也换成同币种（`--nominal-cost 4`、
`--max-cost 40`、`--currency cny`）。走默认价格表（用户层 `providers.toml`），全新 worktree。

| 检查 | 实测 |
|---|---|
| 阶段 | dev / review / test / build 各 iter1 pass → `pipeline complete`（`ran 16, 16 passed / 0 failed`） |
| 真账单 | `dev iter1: settled 0.0401 cny for 3458 in / 4143 out tokens`——按峰时元价复算 2×3458 + 8×4143（每百万）= 0.04006 元，与印出的数一致，且**没有任何一步经过汇率** |
| 预留形状 | RD 一次派发预留 5.072 元（1M×2 + 384K×8），QA 预留 1.1584 元（1M×0.8 + 128K×2.8）——官方 1M/384K 与 1M/128K 的窗口在这里第一次参与停损 |
| 网关少报 | QA 三关全部 `books the reservation 1.1584 cny as NOMINAL instead of a bill that would read too cheap` |
| 报账 | `ledger harness-rd: 0.0401 cny ; tokens in 3458 / out 4143`、`ledger harness-qa: 3.4752 cny ; tokens in 0 / out 9642`、`ledger total: 3.5153 cny, of which 3.4752 nominal (under-reported 3.4752)`（3.4752 = 3 × 1.1584） |
| 开工那行 | 没有 `budget stays NOMINAL`——两个被派发的 peer 都有价，且价目表的币种被 `--currency` 之外的路径（表声明）覆盖，正是 §9.44② 要的优先级 |
| 审批 | 1 请求 / 1 deny / 1 回执成对（`applied:true`），本轮无矛盾回执 |

**没被这一轮证明的**：MiMo（SA 后端）**仍未实测**——demo 不派发 SA，`providers.toml` 里那条只被
`harness/roles/sa.toml` 引用；它的 `cost` 也仍留空（官方标价查不到，§9.44⑥），所以那格是「名义 +
未验证」两件事叠在一起。第一次实测要么 `-p` 单发，要么等 SA 变成真协调者。

**闸门**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 实跑全绿，
247 tests（lib 194 + bin 4 + integration 49）。本轮源码变更是单位改名与两个新测试
（`the_money_unit_comes_from_the_table_and_never_gues`、`old_usd_keyed_lines_still_replay`），
`src` 17043→17251 行；`**Metrics**` 又被拦红一次并回写三处文档。

## 32. 2026-11-05 MiMo（小米）实测两轮 + 官方 元 价目表

§31 留下的那一格（SA 后端换了 MiMo 但没跑过）在本节补上。两次都是 `-p` 单发，不经过总线与 journal。

| 场景 | 结果 | 关键证据 |
|---|---|---|
| 纯对话流式 | **通过** | `thinking_delta` 流 → `text_delta` → `message_done("kestrel-mimo-ok")` → `turn_complete(stop=stop)`，`usage in=17 / out=23` |
| bash 工具回合 | **通过** | `tool_begin{call_id, name:"bash", args:{command:"echo kestrel-mimo-tool-ok"}}` → Seatbelt 沙箱内执行 → `tool_end(is_error:false, output "kestrel-mimo-tool-ok\n")` → 模型回灌后报告原文 → `turn_complete in=534 / out=68` |
| usage 形状 | **两端都报** | 与 §9.7/§2.1 的 BigModel（`message_start` 不回 input）不同，MiMo 的 input/output 都有——所以它不会掉进 §9.36 的「读数不可信 => 按名义落账」分支 |
| 端点 | `https://api.xiaomimimo.com/anthropic`，`api = "anthropic-messages"`，model `mimo-v2.6-flash`；凭据仍按 `!sqlite3` 引用 cc-switch 的「小米」条目（唯一归属地不变） |

**定价（已查到并记进 `providers.toml`）**：`https://mimo.mi.com/pricing` 是服务端渲染正文，curl 读得到
——MiMo-V2.6-Flash 元/1M tokens：输入（缓存未命中）**1**、缓存命中 **0.02**、输出 **2**，峰谷不分档。
同页另一条运维事实：**MiMo-V2.5 / V2.5-Pro 已于 2026-10-21 下线，旧名不再解析**（所以 model id 必须留在 V2.6）。
失败过的入口一并记下，避免下次重走：`platform.mimo.xiaomi.com`、`www.xiaomimimo.com` 不解析；
`mimo.xiaomi.com/pricing`、`api.xiaomimimo.com/pricing` 404；`platform.xiaomimimo.com/pricing` 与
`mimo.mi.com/docs/price/pay-as-you-go` 只有 JS 壳（正文 84 字符），因此 MiMo 的
`context_window` / `max_out` **仍不声明**（§9.44⑦）。

**仍未覆盖**：MiMo 没有走过星型联跑那条路（门铃 / 工件 / 证据核对 / journal 账本）——demo 不派发 SA，
`harness/roles/sa.toml` 只是把后端指向它。这一格要等 SA 变成真协调者才谈得上。

**闸门**：本轮只改用户层配置与文档，无源码变更；`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` /
`cargo test` 复跑全绿，247 tests（lib 194 + bin 4 + integration 49），`**Metrics**` 两行不变。

## 33. 2026-11-05 第七轮联跑：QA 换 OpenAI 兼容表面，名义值归零

`harness/roles/qa.toml` 的 `[llm]` 从 `zhipu-anthropic` 换成 `zhipu-openai`（同一颗 GLM-5.3-Flash，
`api = "openai-completions"`，`base_url = https://open.bigmodel.cn/api/paas/v4`）。换之前先用 `-p` 单发
确认这条表面回 input usage：`turn_complete usage in=24 / out=72`。全新 worktree，其余全默认。

| 检查 | 实测 |
|---|---|
| 阶段 | dev / review / test / build 各 iter1 pass → `pipeline complete`（`ran 15, 15 passed / 0 failed`） |
| QA 第一次拿到真账单 | `ledger harness-qa: 0.1455 cny ; tokens in 126841 / out 6778 ; cache read 108800`——输入侧有数了，缓存命中第一次进账单（0.23 元/百万那一档） |
| RD | `settled 0.0351 cny for 4071 in / 3373 out tokens`；预留 5.072 元（1M×1 + 128K×2 形状，峰时价）被结算换掉 |
| 名义值 | `ledger total: 0.1806 cny, of which 0.0000 nominal`，**后面那对括号整段消失**（`no rate` 与 `under-reported` 两个来源都没有了）——design §9.36 那种「说不清」第一次清零 |
| 钱少了 20 倍 | §31 那轮 `3.5153 cny`（其中 3.4752 说不清）→ 这轮 `0.1806 cny` 全清白。价格没变，是终于按实际读数算而不是按最坏情况猜 |
| 形状坑 | `/api/paas/v4` 这根已经带版本号，旧 `chat_url` 会拼成 `/v4/v1/chat/completions`（实测 404）。改成认三种形状并加测试 |
| 打印坑 | 第一版收尾行印的是 `of which -0.0000 nominal`——`f64::sum()` 对**空集合**给 `-0.0`。`money()` 把显示精度以下按零渲染，真负数照印（§9.45④） |
| 审批 | 本轮 0 请求（RD 没试图跑策略外的命令） |

**同一天早先那次被我自己打断的运行里，§9.42④ 的升级路径第一次在现场发生**。下面两行是当时从 stdout
抄录的原文；那一轮的日志与 journal 都已被后续运行和清理临时文件抹掉，所以**这是一条不可复算的记录**：

```
[sa] approval receipt dc2e618e-…: applied=false (no open request dc2e618e-…)
[sa] approval receipt dc2e618e-…: applied=true (the waiting tool was woken) — upgrades the earlier applied=false for the same req
```
这正是 §27 抓到的那个形状（at-least-once 重投 → 两张矛盾回执），差别是这次账本按事实强弱合并并且把矛盾说出来了。
**这条记录的限度要说清**：它来自一次被我提前终止的运行（我用 `head` 截管道，SIGPIPE 打断了 demo），
现场只剩 stdout 的两行，没有可复算的 journal；合并逻辑本身的证明仍是
`a_contradicting_receipt_upgrades_without_downgrading`。下一次联跑如果遇到同样的时序，值得留着 journal 再看一次。

**闸门**：`cargo fmt --check` / `cargo clippy --all-targets -- -D warnings` / `cargo test` 全绿，
249 tests（lib 196 + bin 4 + integration 49）。净新增 2 项（`chat_url_accepts_a_versioned_root_and_a_full_path`、
`a_zero_ledger_line_does_not_read_as_a_debt`），`src` 17251→17338 行；`**Metrics**` 又被拦红一次并回写三处。
