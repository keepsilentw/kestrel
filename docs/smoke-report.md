# kestrel 真实端点测试报告

> 日期：2026-09-09/10 首轮；2026-09-17 补 Qwen 一轮。对象：kestrel MVP（M0-M4）。
> 范围：真实 LLM 端点 smoke（plan §4-M3 验收项 7 / §4-M0 验收项 6）。
> 凭据来源：BigModel 与 DeepSeek 取自 `~/.claude/settings.json` / cc-switch DB（报告中脱敏）；
> Qwen 取自 cc-switch 的百炼 Token Plan 条目（codex 与 claude 两侧凭据实测互通）。

## 1. 自动化测试基线（cargo test）

| 组 | 数量 | 覆盖 |
|---|---|---|
| 单测（lib） | 115 | wire 归一 / partial_json / retry / registry / 权限矩阵 / sbpl golden / session 树 / journal 对账 / 证据契约 / 审批授权表 |
| 单测（bin：CLI 形状） | 4 | `--tools` 三态、`--approve` 三值（none / all / bus）、`--resume` 选择子、bus 模式保留 role 期限 |
| 集成（core_loop 5 / m1_tools 8 / sandbox_e2e 4 / m2_session 6 / m4_bus 11 / wire_anthropic 2 / discipline 1） | 37 | 引擎验收 / 工具并发与审批超时 / 真实 Seatbelt / golden JSONL / 总线路由、审批往返、停车补投、重连重放与崩溃对账 |
| 合计 | **156**，全部通过；`fmt --check` 与 `clippy -D warnings` 干净，三者已在 `.github/workflows/ci.yml`（ubuntu + macOS）执行 | |

> 计数以 2026-10-28 实跑为准。历史上这张表漂移过三次（101 → 121 → 137，实跑 130），
> §8.3 里那句「137/137」也是其中之一——数字一旦需要人肉同步就会失真，现在由 CI 兜底，
> 本行随 `cargo test` 输出更新（漂移记录见 design.md §9.19）。
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

真端点侧的已知空洞：`--max-usd` 仍是 per-dispatch 名义值（无价格表），真实 token 现在只进
`usage_reported` 记录，不参与停损判断。
