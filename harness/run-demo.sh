#!/usr/bin/env bash
# harness demo: SA + RD + QA 星型拓扑的最小可跑闭环（design §2）。
#
# 拓扑：hub（唯一总线进程）+ RD/QA 两个 worker（各按自己 role 的 [llm] 接后端）
#       + demo-sa（脚本化 SA 状态机，无 LLM 派发智能）。
#
# 前置：~/.kestrel/providers.toml 已配置 harness/roles/*.toml 里 [llm] 引用的 provider。
# 环境变量：
#   KESTREL_BIN               kestrel 可执行文件（默认 target/debug/kestrel）
#   KESTREL_BUS_SOCKET        总线 socket 路径
#   KESTREL_ARTIFACTS         工件目录
#   KESTREL_WORKTREE          RD 的写入目录（role 的 write_roots 引用同一个变量）
#   KESTREL_TEST_KILL_AFTER   崩溃注入点：bell_sent | result_received（见 sa_demo.rs）
#   KESTREL_DEMO_KEEP_JOURNAL 置 1 则保留上一轮 journal/工件（崩溃恢复演练用）
#   KESTREL_DEMO_ROTATE_AFTER 活文件超过这么多条记录就折成检查点并归档旧前缀
#     （不设 = 走 SA 的默认策略 512 条；显式设 0 = 关闭这一条）
#   KESTREL_DEMO_ROTATE_BYTES 活文件超过这么多字节也折（不设 = 默认 1 MiB；0 = 关闭）
#   KESTREL_DEMO_TASK / KESTREL_DEMO_BELL_TIMEOUT / KESTREL_DEMO_MAX_COST
#                             停损上限，单位是这次运行的钱的币种（见 --currency，
#                             价格表声明了币种时以表为准，design §9.44）
#   KESTREL_DEMO_CURRENCY     名义记账的币种（默认 cny；两个被派发的 peer 都有
#                             价时它只是备用声明）
#   KESTREL_DEMO_PRICES       费率表（默认用 ~/.kestrel/providers.toml）。仓库外的
#                             实验文件，用来验证「按 token 结算」这条路径——单价跟着
#                             provider 住，不把费率数字钉进 demo。
#   KESTREL_DEMO_MAX_TASKS / KESTREL_DEMO_MAX_PARALLEL_TOOLS /
#   KESTREL_DEMO_MAX_WORKER_TOOLS / KESTREL_DEMO_MAX_RSS_MB
#                             worker 的三级资源护栏（task 数 / 单 task 工具并发 /
#                             全 worker 工具并发 / 内存软上限）。**没设就不传 flag**，
#                             让 serve 用自己的默认值（4 / 4 / 8 / 0 = 不设限）——
#                             和轮转上限同一套三态语义，脚本不替默认值做决定。
#                             RSS 那条是软上限：越线后新 task 被显式拒绝成 error 铃，
#                             已经起好的引擎继续跑完（design §9.53）。
#   KESTREL_DEMO_STATUS_DIR   设了才给每个 worker 传 --status-file（该目录下
#                             harness-rd.json / harness-qa.json）；不设 = 不写状态文件。
#   KESTREL_DEMO_STATUS_INTERVAL  状态刷新秒数（不设 = serve 的默认 5）。
#
# 用法：./run-demo.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
SOCKET="${KESTREL_BUS_SOCKET:-$HOME/.kestrel/harness/bus.sock}"
ARTIFACTS="${KESTREL_ARTIFACTS:-$HERE/artifacts}"
# RD 的工作树导出给子进程：role 文件的 write_roots 读的是同一个 $KESTREL_WORKTREE，
# 否则「SA 广播的 ref」与「RD 能写的目录」会各自解析成不同路径。
WORKTREE="${KESTREL_WORKTREE:-/tmp/kestrel-worktree}"
export KESTREL_WORKTREE="$WORKTREE"

BIN="${KESTREL_BIN:-$ROOT/target/debug/kestrel}"
if [ ! -x "$BIN" ]; then
  if command -v kestrel >/dev/null 2>&1; then
    BIN="$(command -v kestrel)"
  else
    echo "[demo] no kestrel binary at $BIN — run: cargo build" >&2
    exit 1
  fi
fi

# journal 是 write-ahead 真相源，只追加不截断（design §5 不变式 5），因此上一轮的
# 花费会被重放：预算 名义 4×4 阶段 = 16 个币种单位，直接再跑就可能撞 --max-cost 停损。
# 默认清空重来；
# 崩溃恢复演练需要跨进程续读，用 KESTREL_DEMO_KEEP_JOURNAL=1 保留。
if [ "${KESTREL_DEMO_KEEP_JOURNAL:-0}" != "1" ]; then
  rm -f "$HERE/journal.jsonl"
  mkdir -p "$ARTIFACTS"
  rm -f "$ARTIFACTS"/*.json
fi
mkdir -p "$ARTIFACTS"
# RD 的沙箱写范围（rd.toml:[permissions].write_roots，也是它的 cwd）——demo 前置。
mkdir -p "$WORKTREE"
rm -f "$SOCKET"

PIDS=()
cleanup() {
  for p in ${PIDS[@]:-}; do kill "$p" 2>/dev/null || true; done
}
trap cleanup EXIT

# --- 0. hub：唯一的总线进程（Hello 注册 / Notify 路由 / sender identity 校验）
"$BIN" hub --bus-socket "$SOCKET" &
PIDS+=($!)
for _ in $(seq 1 50); do [ -S "$SOCKET" ] && break; sleep 0.1; done
if [ ! -S "$SOCKET" ]; then
  echo "[demo] hub failed to bind $SOCKET" >&2
  exit 1
fi

# --- 1. worker ×2：角色决定权限/沙箱/提示词与后端（一角色一后端）
# 资源护栏：没设环境变量就**不传 flag**，走 serve 的默认值（与下面的 ROTATE_ARGS 同理）。
RESOURCE_ARGS=()
[ -z "${KESTREL_DEMO_MAX_TASKS+set}" ] || RESOURCE_ARGS+=(--max-tasks "$KESTREL_DEMO_MAX_TASKS")
[ -z "${KESTREL_DEMO_MAX_PARALLEL_TOOLS+set}" ] || RESOURCE_ARGS+=(--max-parallel-tools "$KESTREL_DEMO_MAX_PARALLEL_TOOLS")
[ -z "${KESTREL_DEMO_MAX_WORKER_TOOLS+set}" ] || RESOURCE_ARGS+=(--max-worker-tools "$KESTREL_DEMO_MAX_WORKER_TOOLS")
[ -z "${KESTREL_DEMO_MAX_RSS_MB+set}" ] || RESOURCE_ARGS+=(--max-rss-mb "$KESTREL_DEMO_MAX_RSS_MB")
[ -z "${KESTREL_DEMO_STATUS_INTERVAL+set}" ] || RESOURCE_ARGS+=(--status-interval-secs "$KESTREL_DEMO_STATUS_INTERVAL")

# 状态文件按 session 分开：两个 worker 写同一个路径就会互相覆盖，读到的「几个 task」
# 就成了别人的数。
STATUS_RD=()
STATUS_QA=()
if [ -n "${KESTREL_DEMO_STATUS_DIR:-}" ]; then
  mkdir -p "$KESTREL_DEMO_STATUS_DIR"
  STATUS_RD+=(--status-file "$KESTREL_DEMO_STATUS_DIR/harness-rd.json")
  STATUS_QA+=(--status-file "$KESTREL_DEMO_STATUS_DIR/harness-qa.json")
fi

"$BIN" serve --session harness-rd --bus-socket "$SOCKET" \
  --role "$HERE/roles/rd.toml" --artifacts-dir "$ARTIFACTS" \
  ${RESOURCE_ARGS[@]+"${RESOURCE_ARGS[@]}"} ${STATUS_RD[@]+"${STATUS_RD[@]}"} &
PIDS+=($!)
"$BIN" serve --session harness-qa --bus-socket "$SOCKET" \
  --role "$HERE/roles/qa.toml" --artifacts-dir "$ARTIFACTS" \
  ${RESOURCE_ARGS[@]+"${RESOURCE_ARGS[@]}"} ${STATUS_QA[@]+"${STATUS_QA[@]}"} &
PIDS+=($!)

# 总线上没有"谁已注册"的查询接口，也没有就绪握手：worker 在 Hello 后即可被寻址，
# 这里给一拍等注册完成。停车补投只救「注册过之后掉线」的 session，首次注册前
# 的铃仍会被 hub 判为 unknown session 退回 Error——那正是拼错地址该有的信号。
sleep 1

# --- 2. SA：脚本化状态机，验证 write-ahead 顺序、崩溃对账、fail 回环与预算停损。
#     --worktree 把 RD 的写入目录广播进每颗门铃的 ref，QA 才是在验同一棵树；
#     fail 回环是真的（空结果/FAIL 结论都会回派），所以预算给到 8 次派发。
#     --roles-dir 让账本知道每个 peer 跑在哪个 provider/model 上：providers.toml
#     里那个 model 写了 cost，停损就是按 token 结算的账单；没写就退回 --nominal-cost
#     的名义记账（SA 开工时把这件事印出来，不含糊过去）。钱的单位不写在 flag 名里
#     （§9.44）：`--max-cost 40 --nominal-cost 4` 是 40/4 个「本次币种」，币种由
#     价格表声明的 `currency` 决定，报账那一行会把它印出来。
#     --evidence-spec 是「每个 stage 必须拿出哪类证据」的表（`roles/sa.evidence.json`）。
#     缺省那份只要求 `file`，也就是类型化之前的语义；把某一格改成 `test_log` / `diff` /
#     `screenshot` 之后，worker 光说「跑过了」不管用——那份文件必须真经得起按类型核对。
#     用 KESTREL_DEMO_EVIDENCE_SPEC 换表，设为空串则不传（回到「不要求任何类型」）。
# 轮转上限：没设环境变量就**不传 flag**，让 SA 走默认策略（512 条 / 1 MiB）；显式设 0
# 才是关闭。写成 `${X:-0}` 会把「没设」当成「关掉」，默认策略就此变成死代码——这正是
# 这条清单上「库不猜派发节奏」曾经的位置，现在阈值有了来历，脚本不再替它选 0。
ROTATE_ARGS=()
[ -z "${KESTREL_DEMO_ROTATE_AFTER+set}" ] || ROTATE_ARGS+=(--rotate-after-records "$KESTREL_DEMO_ROTATE_AFTER")
[ -z "${KESTREL_DEMO_ROTATE_BYTES+set}" ] || ROTATE_ARGS+=(--rotate-max-bytes "$KESTREL_DEMO_ROTATE_BYTES")

# 证据要求表：默认用 roles/sa.evidence.json；显式设成空串 = 不传（无类型要求）。
# 写成 `[ -n ... ] && arr+=(...)` 在 set -e 下会在空值时把脚本带走，所以用 if。
EVIDENCE_SPEC="${KESTREL_DEMO_EVIDENCE_SPEC-$HERE/roles/sa.evidence.json}"
EVIDENCE_ARGS=()
if [ -n "$EVIDENCE_SPEC" ]; then
  EVIDENCE_ARGS+=(--evidence-spec "$EVIDENCE_SPEC")
fi

# 没设这个变量就**不传** --prices：让 SA 走用户层那份 providers.toml（存在才读）。
PRICE_ARGS=()
[ -z "${KESTREL_DEMO_PRICES+set}" ] || PRICE_ARGS+=(--prices "$KESTREL_DEMO_PRICES")

KESTREL_TEST_KILL_AFTER="${KESTREL_TEST_KILL_AFTER:-}" \
  "$BIN" demo-sa --bus-socket "$SOCKET" --task "${KESTREL_DEMO_TASK:-demo: 实现一个 LRU cache}" \
  --journal "$HERE/journal.jsonl" --artifacts-dir "$ARTIFACTS" \
  --worktree "$WORKTREE" \
  --approvals "$HERE/roles/sa.approvals.json" \
  ${EVIDENCE_ARGS[@]+"${EVIDENCE_ARGS[@]}"} \
  --roles-dir "$HERE/roles" \
  --currency "${KESTREL_DEMO_CURRENCY:-cny}" \
  --nominal-cost "${KESTREL_DEMO_NOMINAL:-4}" \
  ${PRICE_ARGS[@]+"${PRICE_ARGS[@]}"} \
  ${ROTATE_ARGS[@]+"${ROTATE_ARGS[@]}"} \
  --bell-timeout-secs "${KESTREL_DEMO_BELL_TIMEOUT:-300}" --max-cost "${KESTREL_DEMO_MAX_COST:-40}"
