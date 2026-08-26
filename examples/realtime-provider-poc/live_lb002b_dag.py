#
# SPDX-License-Identifier: BSD 2-Clause License.
#

"""Live LB-002-B: DAG production chain + observe WS plane (gen3, 2026-08-26).

Acceptance (docs/plans/lb-002-dais-plane.md §2 LB-002-B + gen3 handoff §6.1):

  one intent -> real GLM head splits it into >=2 DEPENDENT subtasks
  (dispatch_plan) -> dais create-task --dep -> dependency-wave workers
  bound to REAL dais terminal sessions (start-worker --session, dais
  be8d9cf3 D-04 first live use) -> daemon block settlement
  (worker_done) -> wave walker aggregates ONE final -> phase-2
  re-injection -> head broadcast with the credential marker VERBATIM.

The whole orchestration runs against a REAL VoiceGateway (:8765) whose
EventBus is the shared spine: an observe WS client (session.start
{observe:true, topics:[...]}) captures the b-dag orch.* events plus
bridge.msg / tickets.snapshot topic frames over the real wire, tee'd to
docs/kg/evidence/lb-002b-events.jsonl (同屏落盘).

GLM text mode (Q2 decision: DASHSCOPE_API_KEY absent — documented
fallback). The head runs in-process against the gateway's bus; the
head_provider seam stays stubbed (a voice session opening in this run
is a bug, not a feature).

Usage: .venv/bin/python examples/realtime-provider-poc/live_lb002b_dag.py
"""

import asyncio
import json
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

from openai import AsyncOpenAI

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))

from rt_dsh_backend import DshBackend  # noqa: E402
from rt_dsh_lane import DaisLane  # noqa: E402
from rt_env import glm_credentials  # noqa: E402
from rt_gateway import DEFAULT_TOPIC_SOURCES, VoiceGateway  # noqa: E402
from rt_head_tools import (  # noqa: E402
    DSH_TOOLS_DOCTRINE,
    dsh_head_tools,
    dispatch_plan_tool,
)
from rt_orchestrator import FINAL_PREFIX  # noqa: E402

POC_ROOT = HERE.parent.parent
DAIS = Path("~/.local/bin/dais").expanduser()
EVIDENCE_DIR = POC_ROOT / "docs" / "kg" / "evidence"
BRIDGE_INBOX = Path("~/.dsh/maestro/bridge/inbox.log").expanduser()
GLM_MODEL = "glm-5.3"
GW_PORT = 8765
OBSERVE_TOPICS = [
    "orch.dispatch", "orch.ack", "orch.progress", "orch.done",
    "bridge.msg", "tickets.snapshot",
]

INTENT = (
    "帮我建立本仓库 pipecat 的规模基线：第一步数出 src/pipecat/frames/frames.py "
    "里有多少个帧类定义；第二步在第一步基础上数出 src/pipecat/processors/ 目录"
    "所有 .py 文件里的类定义总数，并与第一步的数字对比。办好了把凭证号念给我。"
)


def verdict(ok, label, detail=""):
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))
    return ok


# ---------------------------------------------------------------------------
# head turn (live_v5_v6_dsh.py pattern, verbatim discipline)
# ---------------------------------------------------------------------------

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
        for s in (DirectFunctionWrapper(fn).to_function_schema()
                  for fn in dsh_head_tools())
    ]


TOOL_FNS = {fn.__name__: fn for fn in dsh_head_tools()}


async def head_turn(glm, messages, params, max_tools: int = 4) -> tuple[str, list[dict]]:
    """One GLM head turn: run tool loop until spoken content."""
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


# ---------------------------------------------------------------------------
# dais plane helpers
# ---------------------------------------------------------------------------

def dais_cli(*args: str, timeout_s: float = 30.0) -> str:
    """One-shot dais orchestration CLI call (provisioning path; the DAG
    itself goes through DaisLane like production)."""
    proc = subprocess.run(
        [str(DAIS), "orchestration", *args],
        capture_output=True, text=True, timeout=timeout_s)
    if proc.returncode != 0:
        raise RuntimeError(f"dais {args[0]} exit={proc.returncode}: "
                           f"{proc.stderr.strip()[:200]}")
    return proc.stdout


def provision_worker(n: int) -> str:
    """Open a fresh dais terminal in this repo; return its session mailbox."""
    out = dais_cli("new-terminal", str(POC_ROOT), "--cwd", str(POC_ROOT))
    m = re.search(r"session_[0-9a-f]+", out)
    if not m:
        raise RuntimeError(f"no session handle in new-terminal output: {out!r}")
    return m.group(0)


async def bus_healthy(lane: DaisLane) -> bool:
    try:
        await lane._run("check-messages", "voice-head", "--timeout-ms", "500")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[SKIP] dais bus unresponsive: {e}")
        return False


# ---------------------------------------------------------------------------
# observe WS client
# ---------------------------------------------------------------------------

async def observe_client(token: str, frames: list, ready: asyncio.Event,
                         stop_at: asyncio.Event | None = None):
    """Connect, auth, start an observe session; tee every frame to `frames`."""
    import websockets

    uri = f"ws://127.0.0.1:{GW_PORT}/ws"
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"t": "auth", "token": token}))
        reply = json.loads(await ws.recv())
        assert reply.get("t") == "auth.ok", reply
        await ws.send(json.dumps({"t": "session.start", "observe": True,
                                  "topics": OBSERVE_TOPICS}))
        reply = json.loads(await ws.recv())
        assert reply.get("t") == "session.started", reply
        assert reply.get("observe") is True
        ready.set()
        recv_task = asyncio.create_task(_observe_recv(ws, frames))
        if stop_at is not None:
            await stop_at.wait()
            recv_task.cancel()
            await asyncio.gather(recv_task, return_exceptions=True)
            await ws.close()


async def _observe_recv(ws, frames: list):
    import websockets

    try:
        async for msg in ws:
            t0 = time.time()
            try:
                data = json.loads(msg)
            except ValueError:
                data = {"t": "_raw", "raw": str(msg)[:200]}
            frames.append({"recv_ts": t0, "frame": data})
    except asyncio.CancelledError:
        pass
    except websockets.exceptions.ConnectionClosed:
        pass


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

async def main() -> int:
    for _k in ("ALL_PROXY", "all_proxy"):
        os.environ.pop(_k, None)
    key, base_url = glm_credentials()
    glm = AsyncOpenAI(api_key=key, base_url=base_url)

    lane = DaisLane(default_timeout_s=30)
    if not await bus_healthy(lane):
        return 1

    print("== provision: two real dais worker terminals ==")
    workers = [provision_worker(i) for i in range(2)]
    print(f"  workers: {workers}")

    token = os.environ.get("VOICE_GATEWAY_TOKEN") or f"lb002b-{uuid.uuid4().hex[:8]}"
    os.environ["VOICE_GATEWAY_TOKEN"] = token

    async def _no_voice_session(session):
        raise RuntimeError("LB-002-B run must not open a voice session")

    gateway = VoiceGateway(
        port=GW_PORT, host="127.0.0.1", token=token,
        head_provider=_no_voice_session, lane=lane,
        topic_sources=DEFAULT_TOPIC_SOURCES,
    )
    gw_task = asyncio.create_task(gateway.run_forever())
    await asyncio.sleep(1.0)  # tailers prime + server bind

    frames: list[dict] = []
    obs_ready = asyncio.Event()
    obs_stop = asyncio.Event()
    obs_task = asyncio.create_task(
        observe_client(token, frames, obs_ready, obs_stop))
    for _ in range(50):
        if obs_ready.is_set():
            break
        await asyncio.sleep(0.1)
    ok_obs = verdict(obs_ready.is_set(), "observe 会话建立（WS 真实链路）",
                     f"topics={OBSERVE_TOPICS}")
    if not ok_obs:
        obs_stop.set()
        obs_task.cancel()
        gw_task.cancel()
        for w in workers:
            try:
                dais_cli("close-terminal", w)
            except RuntimeError:
                pass
        return 1

    print("== B1: one intent -> GLM head split -> dispatch_plan ==")
    finals: list[tuple[str, str]] = []

    async def on_final(ref, message):
        finals.append((ref, message))

    backend = DshBackend(lane=lane, bus=gateway.bus, on_final=on_final,
                         dag_workers=workers, await_timeout_s=420,
                         poll_s=1.0, poll_max_s=8.0)
    params = LiveParams(backend)

    messages = [
        {"role": "system", "content": DSH_TOOLS_DOCTRINE},
        {"role": "user", "content": INTENT},
    ]
    t_head0 = time.time()
    ack, calls1 = await head_turn(glm, messages, params)
    plans = [c for c in calls1 if c["name"] == "dispatch_plan_tool"]
    ok1 = verdict(bool(plans), "head 调用 dispatch_plan（真 GLM 拆分决策）",
                  f"tools={[c['name'] for c in calls1]}")
    receipt = None
    head_tasks = []
    if plans:
        receipt = json.loads(plans[0]["result"])
        head_tasks = plans[0]["args"].get("subtasks_json") or "[]"
        try:
            head_tasks = json.loads(head_tasks)
        except ValueError:
            head_tasks = []
        ok1 &= verdict(receipt.get("status") == "accepted"
                       and str(receipt.get("run_id", "")).startswith("run_"),
                       "phase-1 受理回执", f"ref={receipt.get('ref')} "
                       f"run={receipt.get('run_id')} tasks={receipt.get('tasks')}")
        ok1 &= verdict(len(head_tasks) >= 2, "一意图拆 ≥2 子任务",
                       f"split={json.dumps(head_tasks, ensure_ascii=False)[:160]}")
        has_dep = any(t.get("deps") for t in head_tasks
                      if isinstance(t, dict))
        ok1 &= verdict(has_dep, "拆分携带依赖（deps 非空）")
        ok1 &= verdict(bool(ack) and receipt["ref"] in ack
                       and str(receipt.get("tasks")) in ack,
                       "口语回执含 ref 与任务数（逐字）", f"ack={ack[:90]!r}")
    print(f"B1 {'PASS' if ok1 else 'FAIL'}")

    print("== B2: dependency waves -> worker_done -> aggregated final ==")
    ref = receipt["ref"] if receipt else None
    for _ in range(420):
        if finals:
            break
        await asyncio.sleep(1.0)
    ok2 = verdict(bool(finals), "phase-2 聚合终稿到达")
    final = finals[0][1] if finals else ""
    ok2 &= verdict(final.startswith(FINAL_PREFIX),
                   "终稿 = FINAL_PREFIX + 聚合正文")
    ok2 &= verdict(final.count("子任务") >= 2, "终稿含 ≥2 子任务行")
    cred = (receipt["credentials"][0] if receipt and receipt.get("credentials")
            else "")
    ok2 &= verdict(cred and cred in final, "终稿凭证逐字回显", f"cred={cred!r}")
    ok2 &= verdict(final.count("succeeded") >= len(head_tasks or []),
                   "全部子任务 settled succeeded")

    # dais-side state: the run's tasks completed (promote/settle chain)
    if receipt:
        status = json.loads(await backend.query_status())
        run_line = next((r for r in status.get("runs", [])
                         if receipt["run_id"] in r), "")
        ok2 &= verdict(bool(run_line), "dais check-status 见 run",
                       f"{run_line[:120]!r}")
    print(f"B2 {'PASS' if ok2 else 'FAIL'}")

    print("== B3: re-injection broadcast (credential verbatim) ==")
    ok3 = True
    if finals and ref:
        messages.append({"role": "user", "content": final})
        broadcast, _ = await head_turn(glm, messages, params)
        if cred and cred not in (broadcast or ""):
            messages.append({"role": "assistant", "content": broadcast})
            messages.append({"role": "user", "content":
                             "终稿已送达。请按 doctrine 播报终稿全文，"
                             "逐字保留其中全部【凭证…】标记。"})
            broadcast, _ = await head_turn(glm, messages, params)
        ok3 = verdict(cred and cred in (broadcast or ""),
                      "播报含凭证标记（逐字）", f"broadcast={broadcast[:90]!r}")
    else:
        ok3 = False
        verdict(False, "播报（终稿缺席，跳过）")
    print(f"B3 {'PASS' if ok3 else 'FAIL'}")

    print("== B4: observe WS plane captured the chain (同屏) ==")
    # live bridge.msg marker: a REAL bridge write inside the window
    marker = f"DSH-RE] {{\"type\":\"report\",\"from\":\"lb002b@live\",\
\"to\":\"orch1@session-<id>\",\"body\":\
\"[ref:{ref or '-'}] LB-002-B observe window marker\"}}"
    with open(BRIDGE_INBOX, "a", encoding="utf-8") as f:
        f.write(marker + "\n")
    await asyncio.sleep(5.0)  # tailer poll + WS flush

    orch_frames = [f for f in frames
                   if str(f["frame"].get("t", "")).startswith("orch.")]
    dispatches = [f for f in orch_frames if f["frame"].get("t") == "orch.dispatch"]
    progress = [f for f in orch_frames if f["frame"].get("t") == "orch.progress"]
    dones = [f for f in orch_frames if f["frame"].get("t") == "orch.done"]
    bridge = [f for f in frames if f["frame"].get("t") == "bridge.msg"]
    tickets = [f for f in frames if f["frame"].get("t") == "tickets.snapshot"]

    ok4 = verdict(any(f["frame"].get("lane") == "b-dag" for f in dispatches),
                  "observe 收到 orch.dispatch lane=b-dag（WS 真实帧）",
                  f"dispatch={len(dispatches)} progress={len(progress)} "
                  f"done={len(dones)}")
    ok4 &= verdict(any("LB-002-B observe window marker" in
                       json.dumps(f["frame"], ensure_ascii=False)
                       for f in bridge),
                   "observe 收到本窗 bridge.msg 实推行", f"bridge={len(bridge)}")
    ok4 &= verdict(bool(tickets), "observe 收到 tickets.snapshot 快照（缓存回放）",
                   f"tickets={len(tickets)}")

    # dependency wave order over the wire: every task with deps starts
    # only after its deps settled succeeded
    seq: list[tuple[int, str, str]] = []
    for f in progress:
        m = re.match(r"task (\d+)/(\d+) (worker \S+ started|settled \w+)",
                     str(f["frame"].get("note", "")))
        if m:
            seq.append((int(m.group(1)),
                        "start" if "started" in m.group(3) else "settled",
                        f["recv_ts"]))
    wave_ok = len(seq) >= 2 * max(1, len(head_tasks or [])) - 1
    deps_spec = {i + 1: (t.get("deps") or []) for i, t in
                 enumerate(head_tasks) if isinstance(t, dict)}
    for task_no, deps in deps_spec.items():
        if not deps:
            continue
        t_start = next((ts for no, ph, ts in seq
                        if no == task_no and ph == "start"), None)
        for d in deps:
            t_settled = next((ts for no, ph, ts in seq
                              if no == d + 1 and ph == "settled"), None)
            if t_start is None or t_settled is None or t_start < t_settled:
                wave_ok = False
    ok4 &= verdict(wave_ok and len(seq) >= 2,
                   "依赖波序（依赖任务在前置 settled 后才 start）",
                   f"seq={[(no, ph) for no, ph, _ in seq]}")

    # worker binding: start-worker carried --session for pooled tasks
    starts = [a for a in lane._call_log if a[2] == "start-worker"]
    bound = any("--session" in a and w in a
                for a in starts for w in workers)
    ok4 &= verdict(bound, "start-worker --session 绑定真实 worker 会话",
                   f"starts={len(starts)} workers={workers}")
    print(f"B4 {'PASS' if ok4 else 'FAIL'}")

    # ---- teardown + evidence ----
    obs_stop.set()
    await asyncio.sleep(0.5)
    obs_task.cancel()
    gw_task.cancel()
    for w in workers:
        try:
            dais_cli("close-terminal", w)
        except RuntimeError as e:
            print(f"  [warn] close-terminal {w}: {e}")

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    jsonl = EVIDENCE_DIR / "lb-002b-events.jsonl"
    with open(jsonl, "w", encoding="utf-8") as f:
        for fr in frames:
            f.write(json.dumps(fr, ensure_ascii=False) + "\n")

    total_ok = ok_obs and ok1 and ok2 and ok3 and ok4
    md = EVIDENCE_DIR / "lb-002b-dag-live.md"
    md.write_text(f"""# LB-002-B live 凭证（DAG 生产链 + observe 面）

- 日期: {time.strftime('%Y-%m-%d %H:%M:%S')}
- 结果: {'**ALL PASS**' if total_ok else '**FAIL**'}
  (observe={ok_obs} B1拆分={ok1} B2链路={ok2} B3播报={ok3} B4观察面={ok4})
- head: GLM {GLM_MODEL} 文本模式（Q2 定案回退；DASHSCOPE_API_KEY 缺席）
- run: {receipt.get('run_id') if receipt else '-'} ref: {ref}
- worker 会话（new-terminal 真身，start-worker --session 绑定）:
{chr(10).join('  - ' + w for w in workers)}
- head 拆分: {json.dumps(head_tasks, ensure_ascii=False)}
- 终稿: {(final[:400] + '…') if len(final) > 400 else final}
- WS observe 帧: orch.dispatch={len(dispatches)} orch.progress={len(progress)}
  orch.done={len(dones)} bridge.msg={len(bridge)} tickets.snapshot={len(tickets)}
- 波序: {seq}
- 事件流: lb-002b-events.jsonl（每帧含 recv_ts）
- 首次 live: start-worker --session（dais be8d9cf3 D-04 pane 绑定）
""", encoding="utf-8")
    print(f"== evidence: {md.relative_to(POC_ROOT)} ==")
    print(f"LB-002-B live {'PASS' if total_ok else 'FAIL'}")
    return 0 if total_ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
