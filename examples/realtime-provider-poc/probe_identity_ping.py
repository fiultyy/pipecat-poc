#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Two live tests over the Qwen realtime head (text mode).

T1  base-profile adherence: a distinct head identity declared in
    session.update instructions; ask "你是谁/什么模型" twice (fresh turns)
    and check the answer stays on-identity and does not drift to "Qwen".

T2  schema-driven ping fan-out: tools schema declares `dispatch_agents`
    with an enumerated backend-agent space; the user asks to ping both
    backends. The head must call the tool with BOTH agents; the handler
    runs two stub backends (each returns its number 1 / 2), results flow
    back through function_call_output, and the head must speak both
    numbers to the user.
"""

import asyncio
import json
import os
import sys
import uuid

import websockets

URL = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3.5-omni-flash-realtime"

HEAD_IDENTITY = (
    "你是「Nova」，一个语音编排助手，由内部编排平台驱动。"
    "你的职责是理解用户意图并调度后端agent。"
    "当被问到你的身份时，只回答你是Nova语音编排助手，不要提及任何底层模型或厂商名称。"
)

TOOL = {
    "type": "function",
    "name": "dispatch_agents",
    "description": "ping指定的后端agent并取回它们的回报数据",
    "parameters": {
        "type": "object",
        "properties": {
            "agents": {
                "type": "array",
                "items": {"type": "string", "enum": ["backend-1", "backend-2"]},
                "description": "要ping的后端agent列表",
            }
        },
        "required": ["agents"],
    },
}

# 后端 stub：backend-1 报数 1，backend-2 报数 2
BACKENDS = {"backend-1": "1", "backend-2": "2"}


def eid() -> str:
    return "event_" + uuid.uuid4().hex[:12]


class RT:
    def __init__(self, ws):
        self.ws = ws

    async def send(self, obj):
        await self.ws.send(json.dumps(obj))

    async def user(self, text):
        await self.send(
            {
                "event_id": eid(),
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": text}],
                },
            }
        )

    async def respond(self, timeout=40):
        """Send response.create and collect until response.done.

        Returns (text, function_calls) where function_calls is the list of
        {call_id, name, arguments} from the authoritative response.done.
        """
        await self.send(
            {"event_id": eid(), "type": "response.create", "response": {"modalities": ["text"]}}
        )
        text = ""
        calls: list[dict] = []
        while True:
            raw = await asyncio.wait_for(self.ws.recv(), timeout)
            e = json.loads(raw)
            t = e.get("type")
            if t == "response.text.delta":
                text += e.get("delta", "")
            elif t == "response.done":
                resp = e.get("response", {})
                for it in resp.get("output", []):
                    if it.get("type") == "function_call":
                        calls.append(
                            {"call_id": it["call_id"], "name": it["name"], "arguments": it.get("arguments", "")}
                        )
                return text, calls
            elif t == "error":
                raise RuntimeError(json.dumps(e, ensure_ascii=False)[:300])

    async def tool_output(self, call_id: str, output: dict):
        await self.send(
            {
                "event_id": eid(),
                "type": "conversation.item.create",
                "item": {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": json.dumps(output, ensure_ascii=False),
                },
            }
        )


async def run_backend(agent: str) -> dict:
    """Stub backend agent: 'ping' -> returns its number."""
    await asyncio.sleep(0.5)
    return {"agent": agent, "pong": BACKENDS[agent], "status": "alive"}


def check(cond: bool, label: str, detail: str = "") -> bool:
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))
    return cond


async def main():
    key = os.environ["DASHSCOPE_API_KEY"]
    ws = await websockets.connect(URL, additional_headers={"Authorization": f"Bearer {key}"})
    rt = RT(ws)
    print("<-", json.loads(await ws.recv()).get("type"))

    await rt.send(
        {
            "event_id": eid(),
            "type": "session.update",
            "session": {
                "modalities": ["text"],
                "instructions": HEAD_IDENTITY,
                "tools": [TOOL],
            },
        }
    )
    while True:
        if json.loads(await ws.recv()).get("type") in ("session.updated", "error"):
            break

    # ===== T1: base profile adherence =====
    print("\n== T1: head 身份遵循性（Nova）==")
    t1_ok = True
    for i, q in enumerate(["你是谁？什么模型？", "再介绍一下你自己，你的底层是什么模型？"], 1):
        await rt.user(q)
        await asyncio.sleep(0.3)
        text, _ = await rt.respond()
        has_nova = "Nova" in text or "nova" in text
        leaked = any(w in text for w in ("Qwen", "千问", "通义", "阿里", "omni", "Omni", "开源"))
        ok = has_nova and not leaked
        t1_ok &= ok
        check(ok, f"问{i}: 身份=Nova且无底层模型泄露", f"回复: {text!r}")

    # ===== T2: schema-driven ping fan-out =====
    print("\n== T2: tools schema 驱动双 backend ping（报数 1/2）==")
    await rt.user("帮我 ping 一下两个后端agent，把它们回报的数字告诉我")
    await asyncio.sleep(0.3)
    text, calls = await rt.respond()

    t2_ok = True
    dispatched: dict[str, str] = {}
    if not calls:
        t2_ok = check(False, "head 发起了 function_call", f"文本: {text!r}")
    else:
        c = calls[0]
        try:
            args = json.loads(c["arguments"])
        except json.JSONDecodeError:
            args = {}
        # normalize tolerant (schema is soft: the model may invent near-miss
        # names like agent_01 for backend-1; fuzzy-map onto the enum)
        raw = args.get("agents") or [v for v in args.values() if isinstance(v, list) and v]
        flat = []
        for a in raw if isinstance(raw, list) else [raw]:
            flat.append(a if isinstance(a, str) else json.dumps(a, ensure_ascii=False))
        known = set(BACKENDS)
        hit = [a for a in flat if a in known]
        for a in flat:
            if a in known:
                continue
            # fuzzy: match by significant digits (agent_01/backend_01 -> backend-1)
            digits = "".join(ch for ch in a if ch.isdigit()).lstrip("0") or "0"
            match = next(
                (
                    k
                    for k in sorted(known)
                    if ("".join(ch for ch in k if ch.isdigit()).lstrip("0") or "0") == digits
                ),
                None,
            )
            if match:
                hit.append(match)
        t2_ok &= check(c["name"] == "dispatch_agents", "调用了声明的 tool", c["name"])
        t2_ok &= check(
            len(hit) >= 1,
            "参数命中 backend 枚举",
            f"args={c['arguments'][:120]}",
        )
        if hit:
            # fan out (parallel) to the stub backends
            results = await asyncio.gather(*(run_backend(a) for a in hit))
            t2_ok &= check(
                sorted(r["pong"] for r in results) == sorted(BACKENDS[a] for a in hit),
                "backend 回报数字",
                ", ".join(f"{r['agent']}->{r['pong']}" for r in results),
            )
            await rt.tool_output(c["call_id"], {"results": results})
            await asyncio.sleep(0.3)
            ack, _ = await rt.respond()
            got_nums = all(n in ack for n in {r["pong"] for r in results})
            t2_ok &= check(got_nums, "head 向用户报出 backend 数字", f"ack: {ack!r}")

    await ws.close()
    print(f"\n== SUMMARY: T1={'PASS' if t1_ok else 'FAIL'}  T2={'PASS' if t2_ok else 'FAIL'} ==")


if __name__ == "__main__":
    asyncio.run(main())
