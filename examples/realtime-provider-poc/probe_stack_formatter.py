#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Strategy 1+2 stack: v3-doctrine head + GLM-5-turbo formatter/orchestrator.

Architecture under test (the head is NOT the tool orchestrator anymore):

    user (text) -> qwen3.5-omni-flash-realtime head (v3 doctrine)
        head's ONLY tool: dispatch_intent(raw_intent: string)
            -> glm-5-turbo formatter (system prompt: receive intent, emit
               structured fan-out JSON, sync-block on canary backends,
               return results verbatim)
        -> function_call_output (formatter's verbatim JSON string)
        -> head relays to user (semantic summary)

Why: Omni's soft schema / verbatim decay (T3a) pushed schema pressure and
fidelity down a layer. The head keeps: hearing + intent extraction + user
rapport. GLM (text model, strong FC) owns: structuring, fan-out, faithful
relay. If glm-5-turbo can't hold it, escalate to glm-5.3 (1M ctx, thinking).

Measured with canaries through BOTH hops:
  C1  formatter emits the exact enum agent names (researcher/writer/coder)
  C2  formatter goal self-containment (inlines head-provided context)
  C3  formatter returns backend results VERBATIM in its tool_output string
  C4  head's user-facing ack carries the canary (end-to-end fidelity)
  C5  routing correctness across heterogeneous subtasks
"""

import asyncio
import json
import os
import sys
import uuid
from pathlib import Path

import websockets
from dotenv import load_dotenv
from loguru import logger
from openai import AsyncOpenAI

sys.path.insert(0, str(Path(__file__).parent))

load_dotenv()

URL = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3.5-omni-flash-realtime"
GLM_BASE_URL = os.environ.get("GLM_BASE_URL", "https://open.bigmodel.cn/api/coding/paas/v4")
GLM_FORMATTER = os.environ.get("GLM_FORMATTER_MODEL", "glm-5-turbo")

glm = AsyncOpenAI(api_key=os.environ["GLM_API_KEY"], base_url=GLM_BASE_URL)

HEAD_DOCTRINE = """# Role and Objective
你是「Nova」语音编排助手。你负责听懂用户、提取意图、调度执行、把结果讲给用户。你本人不执行调研、写作或编码任务。

# Tools
- 你只有一个工具 dispatch_intent：把用户的原始意图（含全部上下文与指代展开）作为一段文字传入。意图明确即调用，无需确认。
- 闲聊、问候、一句话可答的简单事实：直接回答，不调用工具。
- raw_intent 必须自包含：执行方看不到对话历史，所有"我们/刚才那个/上面说的"都要展开写全。

# After Tool Calls
- 工具返回的是执行方给你的完整结果原文。向用户转述时保留其中全部编号、标记、数字。
- 结果信息不足时如实说执行方未返回细节，可提议再试，不得自行补全。

# Personality and Tone
中文口语，简洁友好，不用 emoji 和 Markdown。"""

HEAD_TOOL = {
    "type": "function",
    "name": "dispatch_intent",
    "description": "把用户意图（自包含描述）交给执行编排层处理并取回结果",
    "parameters": {
        "type": "object",
        "properties": {
            "raw_intent": {
                "type": "string",
                "description": "用户意图的完整自包含描述，含所有上下文",
            }
        },
        "required": ["raw_intent"],
    },
}

FORMATTER_SYSTEM = """你是执行编排层。你接收一段用户意图描述，职责：
1. 解析意图，拆成子任务，每个子任务指定 agent 类型（只能是 researcher/writer/coder 之一）和自包含 goal；
2. 等待所有子任务执行完毕（执行方会把结果发给你）；
3. 把执行结果【逐字原样】作为你的最终回复返回——包括所有编号、标记、数字，一个字都不许改、不许丢、不许加。

当收到用户消息时，只输出子任务 JSON（不要执行结果）：{"subtasks": [{"agent": "...", "goal": "..."}]}
当收到以 [RESULTS] 开头的消息时，把其后内容逐字作为你的回复输出，不加任何前后缀。"""

CANARIES = {"researcher": "R-CANARY-7734", "writer": "W-CANARY-1188", "coder": "C-CANARY-9051"}


def eid() -> str:
    return "event_" + uuid.uuid4().hex[:12]


async def backend(agent: str, goal: str) -> dict:
    await asyncio.sleep(0.4)
    topic = (goal or "").strip()[:24]
    finding = {
        "researcher": f"调研结论（编号 R-CANARY-7734）：围绕「{topic}」的核心发现有二，其中第二点量化指标为 23%，置信度较高。",
        "writer": f"文案交付（编号 W-CANARY-1188）：围绕「{topic}」——'Guard Every Quiet Moment.'",
        "coder": f"代码交付（编号 C-CANARY-9051）：「{topic}」完成，12 行 pytest 全绿。",
    }[agent]
    return {"agent": agent, "canary": CANARIES[agent], "finding": finding}


async def formatter_turn(messages: list[dict]) -> str:
    """One GLM formatter turn; returns its content."""
    resp = await glm.chat.completions.create(
        model=GLM_FORMATTER,
        messages=messages,
        max_tokens=4096,
    )
    return (resp.choices[0].message.content or "").strip()


def verdict(ok: bool, label: str, detail: str = "", soft: bool = False) -> bool:
    mark = "PASS" if ok else ("WARN" if soft else "FAIL")
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))
    return ok or soft


async def main():
    key = os.environ["DASHSCOPE_API_KEY"]
    ws = await websockets.connect(URL, additional_headers={"Authorization": f"Bearer {key}"})
    print("<-", json.loads(await ws.recv()).get("type"))
    await ws.send(
        json.dumps(
            {
                "event_id": eid(),
                "type": "session.update",
                "session": {
                    "modalities": ["text"],
                    "instructions": HEAD_DOCTRINE,
                    "tools": [HEAD_TOOL],
                },
            }
        )
    )
    while True:
        if json.loads(await ws.recv()).get("type") in ("session.updated", "error"):
            break
    print(f"session ready | head=flash+v3 | formatter={GLM_FORMATTER}\n")

    async def send(o):
        await ws.send(json.dumps(o))

    async def respond(timeout=60):
        await send({"event_id": eid(), "type": "response.create", "response": {"modalities": ["text"]}})
        text, call = "", None
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout)
            e = json.loads(raw)
            t = e.get("type")
            if t == "response.text.delta":
                text += e.get("delta", "")
            elif t == "response.done":
                for it in e.get("response", {}).get("output", []):
                    if it.get("type") == "function_call":
                        call = it
                return text, call
            elif t == "error":
                raise RuntimeError(json.dumps(e, ensure_ascii=False)[:250])

    async def exchange(user_text: str) -> dict:
        """Full exchange; returns metrics dict."""
        m: dict = {"user": user_text}
        await send(
            {
                "event_id": eid(),
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": user_text}],
                },
            }
        )
        await asyncio.sleep(0.3)
        pre, call = await respond()
        m["preamble"] = pre
        if not call:
            m["direct"] = True
            m["ack"] = pre
            return m
        m["direct"] = False
        raw_intent = ""
        try:
            raw_intent = json.loads(call.get("arguments", "{}")).get("raw_intent", "")
        except json.JSONDecodeError:
            pass
        m["raw_intent"] = raw_intent

        # --- formatter hop: intent -> subtasks ---
        fmt_msgs = [{"role": "system", "content": FORMATTER_SYSTEM},
                    {"role": "user", "content": raw_intent}]
        fmt_out = await formatter_turn(fmt_msgs)
        m["formatter_plan"] = fmt_out[:200]
        try:
            plan = json.loads(fmt_out.removeprefix("```json").removeprefix("```").removesuffix("```").strip())
            subs = plan.get("subtasks", [])
        except json.JSONDecodeError:
            subs = []
        m["subtasks"] = subs
        # C1 enum + C5 routing
        m["c1_enum_ok"] = all(s.get("agent") in CANARIES for s in subs) if subs else False

        # --- run backends (parallel) ---
        results = list(await asyncio.gather(*(backend(s["agent"], s["goal"]) for s in subs))) if subs else []

        # --- formatter hop: results -> verbatim return ---
        results_blob = json.dumps({"results": results}, ensure_ascii=False)
        fmt_msgs.append({"role": "assistant", "content": fmt_out})
        fmt_msgs.append({"role": "user", "content": f"[RESULTS] {results_blob}"})
        fmt_final = await formatter_turn(fmt_msgs)
        m["fmt_final"] = fmt_final
        # C3 formatter verbatim
        m["c3_fmt_verbatim"] = all(c in fmt_final for c in [r["canary"] for r in results]) if results else False

        # --- back to head ---
        await send(
            {
                "event_id": eid(),
                "type": "conversation.item.create",
                "item": {
                    "type": "function_call_output",
                    "call_id": call["call_id"],
                    "output": fmt_final,
                },
            }
        )
        await asyncio.sleep(0.3)
        ack, _ = await respond()
        m["ack"] = ack
        m["results"] = results
        # C4 end-to-end canary
        m["c4_ack_canary"] = all(r["canary"] in ack for r in results) if results else False
        return m

    score = {}

    # ---- seed context for self-containment ----
    print("== C2: head raw_intent 自包含 ==")
    await send(
        {
            "event_id": eid(),
            "type": "conversation.item.create",
            "item": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "背景：我们在做智能家居项目Aurora，主打独居老人监护。记住。"}],
            },
        }
    )
    await asyncio.sleep(0.3)
    await respond()
    m = await exchange("帮我调研一下我们产品在养老院的落地机会")
    c2 = not m["direct"] and any(k in m.get("raw_intent", "") for k in ("Aurora", "老人", "监护", "养老"))
    verdict(not m["direct"], "head 调用了 dispatch_intent", f"raw_intent={m.get('raw_intent','')[:60]!r}")
    verdict(c2, "C2 raw_intent 内联上下文", f"intent={m.get('raw_intent','')[:80]!r}")
    verdict(m.get("c1_enum_ok", False), "C1 formatter 枚举精确", f"plan={m.get('formatter_plan','')[:90]}")
    verdict(m.get("c3_fmt_verbatim", False), "C3 formatter 逐字回传", f"fmt_final={m.get('fmt_final','')[:80]!r}")
    verdict(m.get("c4_ack_canary", False), "C4 head ack 端到端含金丝雀", f"ack={m.get('ack','')[:90]!r}")
    score["C2"] = c2
    score["C1"] = m.get("c1_enum_ok", False)
    score["C3"] = m.get("c3_fmt_verbatim", False)
    score["C4"] = m.get("c4_ack_canary", False)

    # ---- routing through both hops ----
    print("\n== C5: 异构任务路由（research/writer/coder）==")
    m2 = await exchange("三件事：调研WebGPU现状；写一句英文slogan；写一个Python快速排序")
    kinds = [s.get("agent") for s in m2.get("subtasks", [])]
    c5 = sorted(kinds) == sorted(CANARIES)
    verdict(c5, "C5 formatter 三类路由正确", f"kinds={kinds}")
    verdict(m2.get("c4_ack_canary", False), "C5 端到端金丝雀（3只）",
            f"ack={m2.get('ack','')[:100]!r}", soft=True)
    score["C5"] = c5

    # ---- history-decay stress: repeat fidelity check late in session ----
    print("\n== C4b: 历史衰减压力（本会话已多轮后）==")
    m3 = await exchange("再调研一下站立办公对腰椎的影响")
    verdict(m3.get("c3_fmt_verbatim", False), "C3b formatter 仍逐字", f"final={m3.get('fmt_final','')[:70]!r}")
    verdict(m3.get("c4_ack_canary", False), "C4b head ack 仍含金丝雀", f"ack={m3.get('ack','')[:80]!r}")
    score["C4b"] = m3.get("c4_ack_canary", False)

    await ws.close()
    print("\n== SUMMARY ==")
    for k in sorted(score):
        print(f"  {k}: {'PASS' if score[k] else 'FAIL'}")
    print(f"\n  formatter model: {GLM_FORMATTER}")


if __name__ == "__main__":
    asyncio.run(main())
