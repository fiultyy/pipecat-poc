#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Realtime (non-turn) behavior probe: streamed, fragmented text input.

Three short exchanges against the live Qwen3.5-Omni-Realtime endpoint, all
using conversation.item.create with MULTIPLE text fragments sent back-to-back
in the SAME turn — never waiting for a response between fragments — then one
response.create at the end. What this verifies:

  A. intra-turn streaming: the head sees fragments accumulate in the item
     (server does not need a "finished utterance" to accept input).
  B. multi-item context: two items pushed before one response.create —
     the reply must integrate BOTH (session-level memory, not turn memory).
  C. incremental follow-up: after the first response, a tiny fragment
     ("第二个呢？") referencing the prior exchange resolves correctly —
     conversational state persists across turns WITHOUT re-sending history.

No VAD, no audio, no waiting between fragments. Raw WebSocket, no pipecat.
"""

import asyncio
import json
import os
import sys
import uuid

import websockets

URL = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3.5-omni-flash-realtime"


def eid() -> str:
    return "event_" + uuid.uuid4().hex[:12]


class Session:
    def __init__(self):
        self.ws = None
        self.log: list[str] = []

    async def send(self, obj):
        await self.ws.send(json.dumps(obj))

    async def recv_until(self, wanted, timeout=25, collect_text=True):
        text = ""
        events: list[str] = []
        while True:
            try:
                raw = await asyncio.wait_for(self.ws.recv(), timeout)
            except asyncio.TimeoutError:
                return text, events, None
            e = json.loads(raw)
            t = e.get("type")
            events.append(t)
            if t == "error":
                return text, events, e
            if t == "response.text.delta" and collect_text:
                text += e.get("delta", "")
            if t in wanted:
                return text, events, e

    async def item_fragment(self, content: list[dict], item_id: str | None = None):
        """conversation.item.create with partial content; no waiting."""
        item = {"type": "message", "role": "user", "content": content}
        if item_id:
            item["id"] = item_id
        await self.send({"event_id": eid(), "type": "conversation.item.create", "item": item})

    async def respond(self) -> str:
        await self.send(
            {"event_id": eid(), "type": "response.create", "response": {"modalities": ["text"]}}
        )
        text, _, done = await self.recv_until({"response.done"})
        if done and done.get("type") == "error":
            raise RuntimeError(f"server error: {json.dumps(done, ensure_ascii=False)[:200]}")
        status = (done or {}).get("response", {}).get("status")
        if status and status != "completed":
            raise RuntimeError(f"response status: {status}")
        return text


async def main():
    key = os.environ["DASHSCOPE_API_KEY"]
    s = Session()
    s.ws = await websockets.connect(URL, additional_headers={"Authorization": f"Bearer {key}"})
    print("<-", json.loads(await s.ws.recv()).get("type"))

    await s.send(
        {
            "event_id": eid(),
            "type": "session.update",
            "session": {
                "modalities": ["text"],
                "instructions": "你是测试助手，回答保持一句话以内。",
            },
        }
    )
    await s.recv_until({"session.updated"}, collect_text=False)
    print("session ready\n")

    # --- A. intra-turn fragmented input (send all, never wait) ---
    print("== A: 单条消息拆4片连发（不等任何回复）==")
    await s.item_fragment([{"type": "input_text", "text": "记住"}])
    await s.item_fragment([{"type": "input_text", "text": "这三个数字："}])
    await s.item_fragment([{"type": "input_text", "text": "7, "}])
    await s.item_fragment([{"type": "input_text", "text": "42, 108。"}])
    await asyncio.sleep(0.3)
    text = await s.respond()
    print(f"   回复: {text!r}\n")

    # --- B. two items in one turn ---
    print("== B: 同一turn内两条独立item（一次回复需综合）==")
    await s.item_fragment([{"type": "input_text", "text": "第一条：法国的首都是哪？"}])
    await s.item_fragment([{"type": "input_text", "text": "第二条：日本的首都呢？"}])
    await asyncio.sleep(0.3)
    text = await s.respond()
    print(f"   回复: {text!r}\n")

    # --- C. incremental follow-up referencing prior turn ---
    print("== C: 极短追问依赖上文（'第二个呢？'）==")
    await s.item_fragment([{"type": "input_text", "text": "对了，刚才第二"}])
    await s.item_fragment([{"type": "input_text", "text": "个城市的美食推荐一个？"}])
    await asyncio.sleep(0.3)
    text = await s.respond()
    print(f"   回复: {text!r}\n")

    await s.ws.close()
    print("done")


if __name__ == "__main__":
    asyncio.run(main())
