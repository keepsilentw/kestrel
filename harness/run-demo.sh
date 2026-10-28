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
#                             （默认 0 = 不截断，journal 始终是单文件）
#   KESTREL_DEMO_TASK / KESTREL_DEMO_BELL_TIMEOUT / KESTREL_DEMO_MAX_USD
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
# 花费会被重放：预算 0.5×4 阶段 = 2.0 = max_usd，直接再跑会立刻停损。默认清空重来；
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
"$BIN" serve --session harness-rd --bus-socket "$SOCKET" \
  --role "$HERE/roles/rd.toml" --artifacts-dir "$ARTIFACTS" &
PIDS+=($!)
"$BIN" serve --session harness-qa --bus-socket "$SOCKET" \
  --role "$HERE/roles/qa.toml" --artifacts-dir "$ARTIFACTS" &
PIDS+=($!)

# 总线上没有"谁已注册"的查询接口，也没有就绪握手：worker 在 Hello 后即可被寻址，
# 这里给一拍等注册完成。停车补投只救「注册过之后掉线」的 session，首次注册前
# 的铃仍会被 hub 判为 unknown session 退回 Error——那正是拼错地址该有的信号。
sleep 1

# --- 2. SA：脚本化状态机，验证 write-ahead 顺序、崩溃对账、fail 回环与预算停损。
#     --worktree 把 RD 的写入目录广播进每颗门铃的 ref，QA 才是在验同一棵树；
#     fail 回环是真的（空结果/FAIL 结论都会回派），所以预算给到 8 次派发。
KESTREL_TEST_KILL_AFTER="${KESTREL_TEST_KILL_AFTER:-}" \
  "$BIN" demo-sa --bus-socket "$SOCKET" --task "${KESTREL_DEMO_TASK:-demo: 实现一个 LRU cache}" \
  --journal "$HERE/journal.jsonl" --artifacts-dir "$ARTIFACTS" \
  --worktree "$WORKTREE" \
  --approvals "$HERE/roles/sa.approvals.json" \
  --rotate-after-records "${KESTREL_DEMO_ROTATE_AFTER:-0}" \
  --bell-timeout-secs "${KESTREL_DEMO_BELL_TIMEOUT:-300}" --max-usd "${KESTREL_DEMO_MAX_USD:-6.0}"
