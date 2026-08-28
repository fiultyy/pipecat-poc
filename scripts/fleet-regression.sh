#!/usr/bin/env bash
# fleet-regression — fleet 语音编排回归入口（默认离线幂等冒烟；--live 追加真网关验收）。
#
# 用法:
#   scripts/fleet-regression.sh           # 离线冒烟: pytest 清单 + rt_voice_app 自检 + maestro 自检清扫
#   scripts/fleet-regression.sh --live    # 在离线冒烟之后追加: 把 /tmp 下两个验收探针拷入 scripts/
#                                         #   并执行(backend 残留绑定→换新→新席位; ws brief→cleanup),
#                                         #   仅当 fleet 有在册席位才做 brief→cleanup, 收尾 fleet 回空。
#
# 约定:
#   - maestro 命令缺失记 MISSING(过渡期容忍, 不计失败); 存在但自检失败记 FAIL。
#   - exit 0 = 无 FAIL; 1 = 存在 FAIL; 2 = 用法错误。重复运行结果一致:
#     默认模式只读(pytest 用例相互独立); --live 每轮 seed→cleanup, fleet 终态回空。
set -u

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$REPO/.venv/bin/python"
MAESTRO_BIN="${MAESTRO_BIN:-$HOME/.dsh/maestro/bin}"
GW_ENV_FILE="${GW_ENV_FILE:-$HOME/.config/voice-gateway/env}"
FLEET_JSON="${FLEET_JSON:-$HOME/.dsh/maestro/fleet.json}"

PASS_N=0 FAIL_N=0 MISS_N=0
ROWS=()

row() {  # row <PASS|FAIL|MISSING|SKIP> <name> <detail>
  ROWS+=("$(printf '%-7s %-40s %s' "$1" "$2" "$3")")
  case "$1" in
    PASS) PASS_N=$((PASS_N + 1)) ;;
    FAIL) FAIL_N=$((FAIL_N + 1)) ;;
    MISSING|SKIP) MISS_N=$((MISS_N + 1)) ;;
  esac
}

run_check() {  # run_check <name> <timeout_s> <cmd> [args...]
  local name="$1" tmo="$2"; shift 2
  local log rc
  log="$(mktemp /tmp/fleet-regression.XXXXXX)"
  timeout "$tmo" "$@" >"$log" 2>&1
  rc=$?
  if [ "$rc" -eq 0 ]; then
    row PASS "$name" "$(tail -n 1 "$log" | cut -c1-100)"
    rm -f "$log"
  else
    # 失败时保留日志供取证(路径附在说明列); 成功即删, 重复运行不留残迹。
    if [ "$rc" -eq 124 ]; then
      row FAIL "$name" "timeout after ${tmo}s, log=$log"
    else
      row FAIL "$name" "exit=$rc: $(tail -n 1 "$log" | cut -c1-60) log=$log"
    fi
  fi
}

# maestro_check <tolerant:0|1> <bin-name> <args...>
# tolerant=1 的命令(过渡期名单) exit=2 视为 selftest 未实现, 记 MISSING 不计失败。
maestro_check() {
  local tolerant="$1" name="$2"; shift 2
  if [ ! -f "$MAESTRO_BIN/$name" ]; then
    row MISSING "$name $*" "不在 $MAESTRO_BIN (过渡期容忍)"
    return
  fi
  local log rc
  log="$(mktemp /tmp/fleet-regression.XXXXXX)"
  timeout 180 "$MAESTRO_BIN/$name" "$@" >"$log" 2>&1
  rc=$?
  if [ "$rc" -eq 0 ]; then
    row PASS "$name $*" "$(tail -n 1 "$log" | cut -c1-100)"
    rm -f "$log"
  elif [ "$rc" -eq 2 ] && [ "$tolerant" -eq 1 ]; then
    row MISSING "$name $*" "存在但 selftest 未实现(exit=2), 过渡期容忍"
    rm -f "$log"
  elif [ "$rc" -eq 124 ]; then
    row FAIL "$name $*" "timeout after 180s, log=$log"
  else
    row FAIL "$name $*" "exit=$rc: $(tail -n 1 "$log" | cut -c1-60) log=$log"
  fi
}

fleet_seats() {  # 输出 fleet.json 在册席位 code(空格分隔)
  "$PY" - "$FLEET_JSON" <<'EOF'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
print(" ".join(sorted((d.get("fleet") or {}).keys())))
EOF
}

offline_smoke() {
  local tests=(
    tests/test_qwen_realtime_tool_result_position.py
    tests/test_rt_voice_app.py
    tests/test_rt_head_tools.py
    tests/test_rt_orchestrator.py
    tests/test_rt_transcript.py
    tests/test_rt_reconnect.py
    tests/test_rt_session_store.py
    tests/test_rt_head_registry.py
    tests/test_rt_fleet_registry.py
    tests/test_rt_dsh_lane.py
    tests/test_registry.py
    tests/test_rt_gateway.py
  )
  # env 消毒: 测试必须对环境变量Hermetic——ambient 网关 env（liaison/AUTO_NEW/
  # DSH_PORT）会让 head-tool 用例走真实 liaison 分支甚至真实 spawn 会话。
  run_check "pytest fleet 清单" 900 env \
    -u VOICE_LIAISON_SESSION -u VOICE_LIAISON_STATE -u VOICE_LIAISON_AUTO_NEW \
    -u VOICE_GATEWAY_TOKEN -u DSH_PORT \
    "$PY" -m pytest -q "${tests[@]}" \
    --deselect tests/test_rt_dsh_lane.py::test_live_dais_roundtrip

  run_check "rt_voice_app --selftest" 120 \
    "$PY" "$REPO/examples/realtime-provider-poc/rt_voice_app.py" --selftest

  maestro_check 0 fleet-touch --selftest
  maestro_check 0 flowc selftest
  maestro_check 0 verify-report --selftest
  # 过渡期容忍名单(后 4+3): 缺失或 selftest 未实现均记 MISSING 不计失败。
  maestro_check 1 session-send --selftest
  maestro_check 1 session-spawn --selftest
  maestro_check 1 cb-send --selftest
  maestro_check 1 fleet-list --selftest
  maestro_check 1 liaison-state --selftest
  maestro_check 1 session-archived --selftest
  maestro_check 1 fleet-release --selftest
  maestro_check 0 dev-sync.sh --verify
  # release-check 校验 repo 检出(host/packages、plugins),必须用检出副本跑;
  # 镜像副本按设计不含 host/(dev-sync EXCLUDES),从镜像跑必挂检查 1/6/7。
  local RELEASE_CHECK="${MAESTRO_REPO:-$HOME/tools/maestro-preset}/bin/release-check.sh"
  run_check "release-check.sh --selftest" 180 "$RELEASE_CHECK" --selftest
}

live_acceptance() {
  local f ok=1
  for f in fleet_e2e_backend.py fleet_accept_ws.py; do
    if [ -f "/tmp/$f" ]; then
      cp -f "/tmp/$f" "$REPO/scripts/$f"
      row PASS "live: 拷贝 $f" "-> scripts/$f"
    else
      row FAIL "live: 拷贝 $f" "/tmp/$f 不存在"
      ok=0
    fi
  done
  [ "$ok" -eq 1 ] || return 0

  if [ ! -f "$GW_ENV_FILE" ]; then
    row FAIL "live: 网关凭据" "缺 $GW_ENV_FILE"
    return 0
  fi
  set -a; . "$GW_ENV_FILE"; set +a
  export VOICE_LIAISON_AUTO_NEW=1

  run_check "live: backend 残留绑定探针" 300 \
    "$PY" "$REPO/scripts/fleet_e2e_backend.py"

  local seats
  seats="$(fleet_seats)"
  if [ -z "$seats" ]; then
    row SKIP "live: ws brief→cleanup" "fleet 无在册席位, 跳过(已回空)"
    return 0
  fi
  run_check "live: ws fleet.brief" 120 \
    "$PY" "$REPO/scripts/fleet_accept_ws.py" brief
  local code
  for code in $seats; do
    run_check "live: ws fleet.cleanup $code" 120 \
      "$PY" "$REPO/scripts/fleet_accept_ws.py" cleanup "$code"
  done

  seats="$(fleet_seats)"
  if [ -z "$seats" ]; then
    row PASS "live: fleet 回空" "fleet.json 无在册席位"
  else
    row FAIL "live: fleet 回空" "残留席位: $seats"
  fi
}

main() {
  cd "$REPO"
  case "${1:-}" in
    "") offline_smoke ;;
    --live)
      offline_smoke
      live_acceptance
      ;;
    -h|--help)
      sed -n '2,14p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "usage: $0 [--live]" >&2
      exit 2
      ;;
  esac

  echo
  echo "==================== fleet-regression 汇总 ===================="
  printf '%-7s %-40s %s\n' "结果" "检查项" "说明"
  local r
  for r in "${ROWS[@]}"; do echo "$r"; done
  echo "---------------------------------------------------------------"
  echo "PASS=$PASS_N  FAIL=$FAIL_N  MISSING/SKIP=$MISS_N (MISSING/SKIP 不计失败)"
  [ "$FAIL_N" -eq 0 ] && exit 0 || exit 1
}

main "$@"
