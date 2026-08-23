#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Live V5/V6: the full voice-head chain over real GLM + real dais (W1.6),
now against the REAL dsh-liaison agent (VO-006; KG 06 §3).

GLM text mode (Q2 decision: DASHSCOPE_API_KEY absent — documented fallback
path). The head is a GLM chat loop with the four rt_head_tools wired onto
a real DshBackend (lane B) against the real dais bus; the orchestrator
side is no longer the in-script stand-in player — it is a genuinely
incubated dsh-liaison session (plugin incubate RPC, role=liaison,
mailbox=agent_liaison), so F4 delivery reaches a real agent turn and F10
finals come back from real agent work.

  V5  voice(=text) -> dispatch_intent -> dais run -> F4 mailbox delivery
      + DSHMSG push wakeup -> liaison first action = mailbox snapshot
      drain -> real work -> F10 done reply ([ref:] envelope + FINAL_PREFIX
      verbatim + credentials verbatim) -> phase-2 re-injection -> head
      broadcast with the credential marker VERBATIM.
  V6  in-flight run + barge-in question -> query_status (pending>=1,
      run still alive) -> cancel_run -> canceled receipt, no late
      broadcast.

Head-side zero-drift (KG 06 §3.1 four-row checklist): the only change is
the ``orchestrator_handle`` CONFIG VALUE (stand-in ``session_vhlive`` →
real ``agent_liaison``); rt_dsh_backend / rt_dsh_lane / rt_head_tools /
rt_orchestrator / src/pipecat are untouched.
"""

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from openai import AsyncOpenAI

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from rt_dsh_backend import DshBackend
from rt_dsh_lane import DaisLane
from rt_env import glm_credentials
from rt_head_tools import (
    DSH_TOOLS_DOCTRINE,
    cancel_run_tool,
    dsh_head_tools,
    dispatch_intent_tool,
    query_status_tool,
    remain_silent_tool,
)
from rt_orchestrator import FINAL_PREFIX

# ---------------------------------------------------------------------------
# Doctrine — liaison 真身落位（VO-006；KG 06 §1.6 唤醒模型 + §3.1 对照表）
# ---------------------------------------------------------------------------
# dsh 会话是按回合执行的 agent，不是守护进程。本脚本的对接协议：
#   1. F4 投递 = dais 邮箱正文（send_intent → agent_liaison 邮箱，读即消费）
#      + 推唤醒（session-send DSHMSG 单行信令 → 触发 liaison 会话新回合；
#      信令只携带 run_id 线索，正文一律走邮箱）。
#   2. liaison 回合首动作 = check-messages agent_liaison --timeout-ms 快照
#      排空邮箱取正文（doctrine 固化；推唤醒仅触发回合，不承载正文）。
#   3. F10 终稿 = liaison 每 ref 恰好一条 done 回信（[ref:] 信封前缀 +
#      FINAL_PREFIX 逐字符 + 凭证逐字），投 voice-head 邮箱由 _phase2 收割；
#     受理回执是 head 本地 dispatch 即时生成的（phase-1），真身不另发
#      中间回执——带 ref 的非终稿信会污染 phase-2 匹配（live 探测事实）。
#   4. head 侧零改动：orchestrator_handle 从替身换真身只是配置值；
#      三过滤/FINAL_PREFIX/凭证格式等协议常量原样（KG 06 §3.1 四项对照）。
# ---------------------------------------------------------------------------

LIAISON_MAILBOX = "agent_liaison"       # 真身配置值（原替身: session_vhlive）
LIAISON_PROFILE = "vh-liaison"
HEAD = "voice-head"
GLM_MODEL = "glm-5.3"
V5_CRED = "R-V5-9901"

PLUGIN_DIR = Path("~/.dsh/plugins/a2a-profile-server").expanduser()
SESSION_SEND = Path("~/.dsh/maestro/bin/session-send").expanduser()
FLEET_PATH = Path("~/.dsh/maestro/fleet.json").expanduser()

# 追加节：通信操作规约（live 探测验证过的可执行细节——具体 CLI 命令、
# 每 ref 恰好一条终稿、不另发中间回执）。投影产物（场景=编排对接，
# 经 Projector 生成并过三门）末尾追加本节；协议字面量（FINAL_PREFIX/
# [ref:]/【凭证…】）逐字内嵌，不改写（G5 不漂移）。
LIAISON_PROTO_APPENDIX = f"""
### 通信操作规约（最高优先级，逐字执行）

1. 回合首动作：运行 `~/.local/bin/dais orchestration check-messages agent_liaison --timeout-ms 2000`，
   快照排空你的邮箱取正文。唤醒推送（回合首行的 DSHMSG 信令）只代表"有新任务"，
   任务正文一律以邮箱快照为准。
2. 从正文解析 `[ref:<ref>]` 前缀、任务描述、必须逐字回显的【凭证…】标记；
   从唤醒信令解析 `run=<run_id>`（回信命令要用）。
3. 完成任务后，用恰好一条 dais 回信回复上游（每 ref 只回这一条终稿；此前不要另发
   受理确认或任何中间消息——上游的受理回执由它自己即时生成，不需要你发）：
   `~/.local/bin/dais orchestration send-message <run_id> agent_liaison voice-head --message-type status --subject done --body '<回信体>'`
   回信体 = `[ref:<ref>] "Agent Final Message":` 前缀行，随后一个空行，再接终稿正文；
   前缀逐字符照抄；终稿正文必须包含来件中的全部【凭证…】标记，逐字不改、不丢、不加。
4. 回信发出后本回合即结束：不要轮询等待回音，不要向其他 handle 发消息，
   在没有任务正文时不要编造任务或回信。
"""

TOOL_FNS = {
    "dispatch_intent_tool": dispatch_intent_tool,
    "query_status_tool": query_status_tool,
    "cancel_run_tool": cancel_run_tool,
    "remain_silent_tool": remain_silent_tool,
}


class LiveParams:
    """FunctionCallParams shim: same app_resources contract as the pipecat
    pipeline, capturing the result_callback payload."""

    def __init__(self, backend):
        self.app_resources = {"dsh_backend": backend}
        self.result = None

    async def result_callback(self, value):
        self.result = value

def tool_schemas() -> list[dict]:
    from pipecat.adapters.schemas.direct_function import DirectFunctionWrapper

    return [
        {
            "type": "function",
            "function": {
                "name": s.name,
                "description": s.description,
                "parameters": {"type": "object", "properties": s.properties,
                               "required": s.required or []},
            },
        }
        for s in (DirectFunctionWrapper(fn).to_function_schema() for fn in dsh_head_tools())
    ]


def verdict(ok, label, detail=""):
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))
    return ok


async def head_turn(glm, messages, params, max_tools: int = 4) -> tuple[str, list[dict]]:
    """One GLM head turn: run tool loop until spoken content. Returns
    (content, tool_calls) where tool_calls records {name, args, result}."""
    calls: list[dict] = []
    for _ in range(max_tools):
        resp = await glm.chat.completions.create(
            model=GLM_MODEL, messages=messages, tools=tool_schemas(),
        )
        msg = resp.choices[0].message
        if not msg.tool_calls:
            return (msg.content or "").strip(), calls
        messages.append(msg.model_dump(exclude_none=True))
        for tc in msg.tool_calls:
            fn = TOOL_FNS[tc.function.name]
            args = json.loads(tc.function.arguments or "{}")
            params.result = None
            await fn(params, **args)
            result = params.result
            if not isinstance(result, str):
                result = json.dumps(result, ensure_ascii=False)
            calls.append({"name": tc.function.name, "args": args, "result": result})
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})
    return "", calls


async def bus_healthy(lane: DaisLane) -> bool:
    try:
        await lane.check_status()
        return True
    except Exception:
        return False


# ---- real dsh-liaison incubation (plugin incubate RPC; VO-006 step 1) ----

def _boot_incubator() -> tuple[subprocess.Popen, int]:
    """Boot the a2a-profile-server plugin HTTP face on an ephemeral port with
    the real dsh incubator wired (profile store at the canonical incubated
    root, so fleet/profile artifacts land where the ecosystem expects)."""
    script = (
        "(async () => {"
        "const { createHttpServer } = await import('./http-server.js');"
        "const { createTaskStore } = await import('./task-store.js');"
        "const { createProfileStore } = await import('./profile-store.js');"
        "const { getIncubator } = await import('./incubators/index.js');"
        "const { homedir } = await import('node:os');"
        "const { join } = await import('node:path');"
        "const { mkdirSync } = await import('node:fs');"
        "const state = join(homedir(), '.dsh/plugins/a2a-profile-server/state/live-vo006');"
        "mkdirSync(state, { recursive: true });"
        "const tasks = createTaskStore(join(state, 'tasks.jsonl'));"
        "const profiles = createProfileStore(join(homedir(), '.dsh/profiles/incubated'));"
        "const incubate = getIncubator('dsh');"
        "const http = createHttpServer({ tasks, profiles, incubate, token: '' });"
        "console.log('READY ' + await http.start(0));"
        "})().catch(e => { console.error('BOOTFAIL ' + (e?.stack ?? e)); process.exit(1); })"
    )
    proc = subprocess.Popen(
        ["node", "--input-type=module", "-e", script],
        cwd=PLUGIN_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if "READY" in line:
            return proc, int(line.split()[-1])
        if "BOOTFAIL" in line or proc.poll() is not None:
            break
    proc.kill()
    raise RuntimeError("incubator harness did not become ready")


def _rpc_incubate(port: int, agents_md: str) -> dict:
    """One incubate RPC over loopback (proxy-stripped urllib)."""
    import urllib.request

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "incubate",
        "params": {
            "name": LIAISON_PROFILE,
            "targets": ["dsh-liaison"],
            "role": "liaison",
            "project": "voice-head",
            "mailbox": LIAISON_MAILBOX,
            "projection": {
                "agents_md": agents_md,
                "profile_json": {"agent_role": "liaison", "scenario": "编排对接"},
                "description": "voice-head 主对接联络 agent（VO-006 真身落位）",
            },
        },
    }
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/",
        data=json.dumps(payload, ensure_ascii=False).encode(),
        headers={"Content-Type": "application/json"},
    )
    with opener.open(req, timeout=180) as resp:
        out = json.loads(resp.read())
    if "error" in out:
        raise RuntimeError(f"incubate RPC error: {out['error']}")
    return out["result"]


async def incubate_liaison() -> dict:
    """Real incubation through the plugin (scenario projection → gates →
    incubate RPC); returns the dsh receipt.

    The agents_md is a REAL projection (Projector over the context-files
    kernel, scenario=编排对接, role=liaison → ROLE_TEMPLATES doctrine
    appended by the projector itself), plus the operational appendix so
    the live agent has the exact CLI contract (mailbox drain command,
    single done-reply per ref, verbatim prefix/credentials). The plugin's
    incubateDsh then prepends its own role doctrine section.
    """
    from rt_projector import Projector
    from rt_projection_gates import run_gates

    proj = await Projector().project(
        "编排对接：作为语音编排头与外部执行体系之间的常驻对接联络 agent，"
        "接收语音头的语义任务指令，收敛为稳定指令后跟踪执行并回传终稿；"
        "工作在消息邮箱与终端命令环境中，不直接面向终端用户。", role="liaison")
    agents_md = proj.agents_md.rstrip() + "\n" + LIAISON_PROTO_APPENDIX
    report = run_gates(agents_md)
    if not report.passed:
        raise RuntimeError(f"liaison agents_md gate violations: {report.violations}")

    proc, port = _boot_incubator()
    try:
        result = _rpc_incubate(port, agents_md)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    receipts = result["receipts"]
    bad = [r for r in receipts if r.get("error")]
    if bad:
        raise RuntimeError(f"incubate receipts failed: {bad}")
    receipt = receipts[0]
    assert receipt["target"] == "dsh-liaison", receipt
    return receipt


async def wakeup(code: str, ref: str, run_id: str) -> None:
    """DSHMSG push wakeup: session-send injects a machine-parseable first
    line into the liaison session, triggering a fresh agent turn (KG 06 §2.2).
    session-send goes over the maestro loopback, not the dais CLI bus —
    no bus-lock interplay with lane calls."""
    body = f"VO-006 wake: mailbox has a task for you; run={run_id}; reply to voice-head per doctrine"
    proc = await asyncio.create_subprocess_exec(
        str(SESSION_SEND), HEAD, code, "steer", ref, body,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
    if proc.returncode != 0 or b"accepted=True" not in out:
        raise RuntimeError(f"session-send wakeup failed: {out.decode(errors='replace')!r}")


async def dispatch_intent_live(params, raw_intent: str):
    """Head tool wrapper (wiring, not head logic): the real liaison chain.

    1. semantic hardening: pin the phase-2 credential marker into the intent
       so the real agent has a verbatim marker to echo back (stand-in V5 got
       it from the scripted reply body; a real agent can only echo what the
       intent carries);
    2. call the untouched head tool (create run + F4 mailbox delivery with
       the [ref:] envelope — the backend generates the phase-1 receipt
       locally and immediately);
    3. DSHMSG push wakeup so the liaison session starts a real agent turn.
    """
    intent = raw_intent.rstrip() + f" 终稿必须逐字回显受理凭证标记【凭证{V5_CRED}】。"
    await dispatch_intent_tool(params, intent)
    receipt = params.result
    if isinstance(receipt, str):
        receipt = json.loads(receipt)
    if receipt.get("status") == "accepted":
        await wakeup(params.app_resources["liaison_code"],
                     receipt["ref"], receipt["run_id"])


TOOL_FNS["dispatch_intent_tool"] = dispatch_intent_live


# ---- doctrine behavioral check: first action of a wakeup turn ----

def _wakeup_turn_actions(session_id: str, ref: str) -> list[dict]:
    """Loopback session.history: tool calls of the turn whose DSHMSG wakeup
    line carries ``ref``. The wakeup turn is located by the ref (turn
    numbering varies: the PROFILE-INJECT prompt does not always get its
    own turn). Skill loads (e.g. dais-orchestration) precede the drain as
    preparation, so the doctrine check below looks at the first EXECUTED
    bash action, not the literal first tool call."""
    import urllib.request

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    fleet = json.loads(FLEET_PATH.read_text())
    port = str(fleet.get("port", 3080))
    wire = json.dumps({
        "type": "client-request", "rpcId": str(time.time_ns()),
        "method": "session.history",
        "payload": {"sessionId": session_id, "maxMessages": 4000},
    }).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/session.history",
        data=wire, headers={"content-type": "application/json"},
    )
    with opener.open(req, timeout=60) as resp:
        result = json.loads(resp.read())["result"]
    if not result.get("ok"):
        raise RuntimeError(f"session.history failed: {result.get('error')}")

    turn_calls: dict[int, list[dict]] = {}
    cur_turn = 0
    wake_turn = None
    for e in result["value"]["events"]:
        ev = e.get("event", e)
        t, d = ev.get("type"), ev.get("data", {})
        if t == "turn/start":
            cur_turn = d.get("turn", cur_turn)
        elif t in ("user/message", "agent/inbox/spliced"):
            for msg in d.get("inserted") or [d]:
                for part in msg.get("content") or []:
                    text = part.get("text", "")
                    if (text.startswith("DSHMSG]") and f'"{ref}"' in text
                            and wake_turn is None):
                        wake_turn = cur_turn
        elif t == "tool/call":
            turn_calls.setdefault(cur_turn, []).append(
                {"name": d.get("name"), "arguments": d.get("arguments", "")})
    return turn_calls.get(wake_turn or -1, [])


async def main() -> int:
    # 环境适配：宿主 shell 常带 SOCKS 代理（ALL_PROXY=socks5://…）而 httpx 未装
    # socksio，构造客户端即 ImportError；GLM 端点国内直连，剥掉 SOCKS 变量即可
    # （同 incubation-wizard 方案）。
    for _k in ("ALL_PROXY", "all_proxy"):
        os.environ.pop(_k, None)
    key, base_url = glm_credentials()
    glm = AsyncOpenAI(api_key=key, base_url=base_url)

    lane = DaisLane(default_timeout_s=20)
    if not await bus_healthy(lane):
        print("[SKIP] dais bus unresponsive (daemon wedge; restart resident dais)")
        return 0

    # ---- incubate the real dsh-liaison (plugin RPC; kept registered) ----
    print("== incubate: real dsh-liaison via plugin ==")
    receipt = await incubate_liaison()
    code = receipt["code"]
    fleet_ent = json.loads(FLEET_PATH.read_text())["fleet"][code]
    ok_inc = verdict(
        receipt["mailbox"] == LIAISON_MAILBOX and receipt["role"] == "liaison"
        and receipt["project"] == "voice-head",
        "孵化回执（真身 dsh-liaison）",
        f"code={code} mailbox={receipt['mailbox']} role={receipt['role']} project={receipt['project']}")
    ok_inc &= verdict(
        fleet_ent.get("mailbox") == LIAISON_MAILBOX and fleet_ent.get("role") == "liaison"
        and fleet_ent.get("project") == "voice-head" and "profile_version" in fleet_ent,
        "fleet 五键登记",
        f"sessionId={fleet_ent['sessionId'][:18]}…")
    if not ok_inc:
        return 1

    print("== V5: voice(text) -> dais run -> real liaison -> broadcast ==")

    finals: list[tuple[str, str]] = []

    async def on_final(ref, message):
        finals.append((ref, message))

    # head 侧唯一变化：orchestrator_handle 指向真身邮箱（配置值；逻辑 diff=0）
    backend = DshBackend(lane=lane, orchestrator_handle=LIAISON_MAILBOX,
                         head_handle=HEAD, on_final=on_final,
                         await_timeout_s=540, poll_s=1.0)
    params = LiveParams(backend)
    params.app_resources["liaison_code"] = code

    # --- V5: dispatch -> (F4 + wakeup) -> real agent work -> F10 -> broadcast ---
    messages_v5: list[dict] = [
        {"role": "system", "content": DSH_TOOLS_DOCTRINE},
        {"role": "user", "content":
            "帮我调研 WebGPU 在生产环境的采用情况，办好了把凭证号念给我。"},
    ]
    ack, calls5 = await head_turn(glm, messages_v5, params)
    dispatches = [c for c in calls5 if c["name"] == "dispatch_intent_tool"]
    ok5 = verdict(bool(dispatches), "head 调用 dispatch_intent",
                  f"tools={[c['name'] for c in calls5]}")
    ref = None
    if dispatches:
        receipt5 = json.loads(dispatches[0]["result"])
        ref = receipt5.get("ref", "")
        ok5 &= verdict(receipt5.get("status") == "accepted"
                       and receipt5["run_id"].startswith("run_"),
                       "phase-1 受理回执（head 本地即时生成）",
                       f"ref={ref} run={receipt5.get('run_id')}")
        ok5 &= verdict(ref and ref in ack, "回执 ref 原样出现在口语回执", f"ack={ack[:80]!r}")

    ok5 &= verdict(not finals, "phase-1 阶段终稿未到（两阶段时序）")

    # real agent turn budget: live-probed ~40-70s typical, but a live GLM
    # agent turn can stall past 5min under load — allow up to ~9min
    for _ in range(540):
        if finals:
            break
        await asyncio.sleep(1.0)
    ok5 &= verdict(bool(finals), "phase-2 终稿到达（真身 F10 回信）")
    if finals and ref:
        fref, fmsg = finals[0]
        ok5 &= verdict(fref == ref and fmsg.startswith(FINAL_PREFIX),
                       "终稿 = FINAL_PREFIX + done body（[ref:] 信封三过滤命中）",
                       f"final[:60]={fmsg[:60]!r}")
        ok5 &= verdict(f"【凭证{V5_CRED}】" in fmsg, "终稿凭证逐字回显")
        messages_v5.append({"role": "user", "content": finals[0][1]})
        broadcast, _ = await head_turn(glm, messages_v5, params)
        if f"【凭证{V5_CRED}】" not in broadcast:
            # live sampling variance: the model occasionally re-acks the
            # phase-1 receipt instead of reading the final aloud. One
            # corrective re-turn (contract check below stays verbatim-strict).
            messages_v5.append({"role": "assistant", "content": broadcast})
            messages_v5.append({"role": "user", "content": "终稿已送达。请按 doctrine 播报终稿全文，逐字保留其中全部【凭证…】标记。"})
            broadcast, _ = await head_turn(glm, messages_v5, params)
        ok5 &= verdict(f"【凭证{V5_CRED}】" in broadcast,
                       "播报含凭证标记（逐字）", f"broadcast={broadcast[:80]!r}")

        wake_calls = _wakeup_turn_actions(receipt["sessionId"], ref)
        exec_calls = [c for c in wake_calls if c["name"] in ("bash", "shell", "exec")]
        first_exec = exec_calls[0] if exec_calls else None
        ok5 &= verdict(
            first_exec is not None
            and "check-messages" in first_exec["arguments"]
            and LIAISON_MAILBOX in first_exec["arguments"],
            "liaison 回合首动作 = 邮箱快照排空（doctrine 行为断言）",
            f"calls={[c['name'] for c in wake_calls][:4]} "
            f"first_exec={(first_exec or {}).get('arguments', '')[:70]!r}")
    print(f"V5 {'PASS' if ok5 else 'FAIL'}")
    v5_ok = ok5

    # --- V6: barge-in query + cancel while the real agent is in flight ---
    print("== V6: interrupt query + cancel ==")
    finals.clear()
    messages_v6: list[dict] = [
        {"role": "system", "content": DSH_TOOLS_DOCTRINE},
        {"role": "user", "content": "再帮我查一下 WebAssembly 生态报告，先办着。"},
    ]
    ack6, calls6a = await head_turn(glm, messages_v6, params)
    dispatches6 = [c for c in calls6a if c["name"] == "dispatch_intent_tool"]
    ok6 = verdict(bool(dispatches6), "V6 dispatch 受理（F4 已投真身）")
    ref6 = json.loads(dispatches6[0]["result"])["ref"] if dispatches6 else None

    # barge-in: user interrupts with a status question; run keeps going
    messages_v6.append({"role": "user", "content": "等一下——现在都进行得怎么样了？"})
    status_reply, calls6b = await head_turn(glm, messages_v6, params)
    queries = [c for c in calls6b if c["name"] == "query_status_tool"]
    pending_seen = False
    if queries:
        data = json.loads(queries[0]["result"])
        pending_seen = ref6 in data.get("pending", [])
    ok6 &= verdict(bool(queries), "打断后 head 走 query_status",
                   f"tools={[c['name'] for c in calls6b]}")
    ok6 &= verdict(pending_seen, "run 在打断后仍在途（不打断远程执行）")

    # cancel by voice
    messages_v6.append({"role": "user", "content": "算了，把它取消了吧。"})
    cancel_reply, calls6c = await head_turn(glm, messages_v6, params)
    cancels = [c for c in calls6c if c["name"] == "cancel_run_tool"]
    canceled_ok = False
    if cancels:
        data = json.loads(cancels[0]["result"])
        canceled_ok = data.get("status") == "canceled" and bool(data.get("run_id"))
    ok6 &= verdict(bool(cancels) and canceled_ok, "cancel_run 取消成功",
                   f"result={cancels[0]['result'][:60] if cancels else 'none'}")

    await asyncio.sleep(1.5)
    late = [f for f in finals if f[0] == ref6]
    ok6 &= verdict(not late, "取消后无迟到终稿播报")
    print(f"V6 {'PASS' if ok6 else 'FAIL'}")

    print(f"liaison code={code} mailbox={LIAISON_MAILBOX} "
          f"sessionId={receipt['sessionId']} (kept registered in fleet)")
    return 0 if (ok_inc and v5_ok and ok6) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
