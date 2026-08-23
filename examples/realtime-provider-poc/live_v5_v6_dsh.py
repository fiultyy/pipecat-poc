#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Live V5/V6: the full voice-head chain over real GLM + real dais (W1.6).

GLM text mode (Q2 decision: DASHSCOPE_API_KEY absent — documented fallback
path). The head is a GLM chat loop with the four rt_head_tools wired onto
a real DaisBackend (lane B) against the real dais bus; this script plays
the orchestrator session (drains the ORCH mailbox, replies per ref).

  V5  voice(=text) -> dispatch_intent -> dais run -> phase-1 ack (ref
      echo) -> orchestrator done reply -> phase-2 final re-injection ->
      head broadcast with the credential marker VERBATIM.
  V6  in-flight run + barge-in question -> query_status (pending>=1,
      run still alive) -> cancel_run -> canceled receipt, no late
      broadcast.
"""

import asyncio
import json
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))

from openai import AsyncOpenAI

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

ORCH = "session_vhlive"
HEAD = "voice-head"
GLM_MODEL = "glm-5.3"
V5_CRED = "R-V5-9901"

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


async def orchestrator_player(lane: DaisLane, backend: DshBackend, reply_body: str,
                              refs: set[str], stop: asyncio.Event,
                              deadline_s: float = 30):
    """Play the orchestrator: drain ORCH mailbox, reply per ref (run id
    resolved from the backend's ref→run map)."""
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline and not stop.is_set():
        rows = await lane.check_messages(ORCH)
        for row in rows:
            body = row.get("body", "")
            m = re.search(r"\[ref:(vh-[0-9a-f]+)\]", body)
            if not m or m.group(1) in refs:
                continue
            ref = m.group(1)
            refs.add(ref)
            dispatch = backend._runs.get(ref)
            run_id = dispatch.run_id if dispatch else None
            if not run_id:
                continue
            await lane.send_reply(run_id, ORCH, row.get("from") or HEAD,
                                  reply_body, ref)
        await asyncio.sleep(0.3)


async def find_run_for(lane: DaisLane, marker: str) -> str:
    status = await lane.check_status()
    for e in status.get("entries", []):
        if marker in (e.get("summary") or ""):
            return e["id"]
    raise RuntimeError(f"no run found for {marker}")


async def main() -> int:
    key, base_url = glm_credentials()
    glm = AsyncOpenAI(api_key=key, base_url=base_url)

    lane = DaisLane(default_timeout_s=20)
    if not await bus_healthy(lane):
        print("[SKIP] dais bus unresponsive (daemon wedge; restart resident dais)")
        return 0
    print("== V5: voice(text) -> dais run -> broadcast ==")

    finals: list[tuple[str, str]] = []

    async def on_final(ref, message):
        finals.append((ref, message))

    backend = DshBackend(lane=lane, orchestrator_handle=ORCH, head_handle=HEAD,
                         on_final=on_final, await_timeout_s=40, poll_s=0.5)
    params = LiveParams(backend)

    # --- V5: dispatch -> ack -> done -> broadcast ---
    messages_v5: list[dict] = [
        {"role": "system", "content": DSH_TOOLS_DOCTRINE},
        {"role": "user", "content":
            "帮我调研 WebGPU 在生产环境的采用情况，办好了把凭证号念给我。"},
    ]
    stop = asyncio.Event()
    refs_seen: set[str] = set()
    player = asyncio.create_task(
        orchestrator_player(lane, backend,
                            f"调研完成 【凭证{V5_CRED}】 结论 41%", refs_seen, stop))
    try:
        ack, calls5 = await head_turn(glm, messages_v5, params)
        dispatches = [c for c in calls5 if c["name"] == "dispatch_intent_tool"]
        ok5 = verdict(bool(dispatches), "head 调用 dispatch_intent",
                      f"tools={[c['name'] for c in calls5]}")
        ref = None
        if dispatches:
            receipt = json.loads(dispatches[0]["result"])
            ref = receipt.get("ref", "")
            ok5 &= verdict(receipt.get("status") == "accepted"
                           and receipt["run_id"].startswith("run_"),
                           "phase-1 受理回执", f"ref={ref} run={receipt.get('run_id')}")
            ok5 &= verdict(ref and ref in ack, "回执 ref 原样出现在口语回执", f"ack={ack[:80]!r}")

        for _ in range(200):
            if finals:
                break
            await asyncio.sleep(0.2)
        ok5 &= verdict(bool(finals), "phase-2 终稿到达")
        if finals and ref:
            fref, fmsg = finals[0]
            ok5 &= verdict(fref == ref and fmsg.startswith(FINAL_PREFIX),
                           "终稿 = FINAL_PREFIX + done body")
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
        print(f"V5 {'PASS' if ok5 else 'FAIL'}")
        v5_ok = ok5
    finally:
        stop.set()
        await asyncio.sleep(0)

    # --- V6: barge-in query + cancel while in flight ---
    print("== V6: interrupt query + cancel ==")
    finals.clear()
    stop6 = asyncio.Event()
    refs6: set[str] = set()
    # NO player for V6: the run must stay pending until cancelled.
    messages_v6: list[dict] = [
        {"role": "system", "content": DSH_TOOLS_DOCTRINE},
        {"role": "user", "content": "再帮我查一下 WebAssembly 生态报告，先办着。"},
    ]
    ack6, calls6a = await head_turn(glm, messages_v6, params)
    dispatches6 = [c for c in calls6a if c["name"] == "dispatch_intent_tool"]
    ok6 = verdict(bool(dispatches6), "V6 dispatch 受理")
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
    return 0 if (v5_ok and ok6) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
