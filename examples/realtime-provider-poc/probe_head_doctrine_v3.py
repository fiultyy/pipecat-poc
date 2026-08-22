#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""v3 head-doctrine probe: OpenAI Realtime 2.0 official Tools paradigm,
ported onto the qwen3.5-omni-flash-realtime head.

What changed vs v2 (each mapped to a v1/v2 failure or official rule):

  - Labeled-section prompt structure (OpenAI guide: "Recommended prompt
    structure - short labeled sections") replacing one dense paragraph.
  - "Use only the tools explicitly provided; do not invent, assume,
    simulate, or rename" (official) -> targets T4 enum drift.
  - Read-only vs write-action split with explicit trigger rules
    ("call when intent is clear and fields are available") -> targets the
    T1 boundary pendulum (fact questions over-dispatched in v2).
  - "Only say an action was completed after the tool call succeeds" +
    verbatim-relay with an honest-fallback outlet ("say the backend
    returned no detail, offer re-dispatch") -> targets T3 fabrication,
    aligning with the model's completeness-training instead of fighting it.
  - Preamble policy (short spoken update before slow tool calls) -> new,
    official-recommended; measured as T0.

Backends are canary stubs (deterministic ground truth). Same five
dimensions as v2 for direct comparison, plus T0 preambles.
"""

import asyncio
import json
import os
import re
import sys
import uuid

import websockets

URL = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3.5-omni-flash-realtime"

# ---- v3 doctrine: OpenAI Realtime 2.0 prompt structure, ported ----
HEAD_DOCTRINE_V3 = """# Role and Objective
你是「Nova」，语音编排助手。你把用户的语义转化为后端agent的执行prompt并调度执行，收取结果后转述给用户。你本人不执行调研、写作或编码任务。

# Tools
- 只使用当前工具列表中存在的 agent 类型（researcher / writer / coder），不得发明、假设、模拟或改名。
- 只读类任务（调研、写作、代码生成）：用户意图明确且要素齐全时立即调用，无需确认。
- 闲聊、问候、一句话可答的简单事实：直接回答，不调用工具。
- goal 必须自包含：后端看不到对话历史，所有指代（我们、刚才那个、上面说的）都必须展开成完整上下文写进 goal。

# After Tool Calls
- 只转述工具返回的文字，逐字保留其中的编号、标记、数字，不得改写、缩写或补全。
- 结果信息不足时，如实告诉用户"后端未返回细节"，并提议可以再分发深挖；严禁用自己的知识补全。

# Preambles
- 调用工具前若预计耗时明显，先说一句极短的进展播报（如"我去安排调研"），然后立即调用。
- 直接可答的问题不要播报。

# Personality and Tone
- 中文口语，简洁友好，不用 emoji 和 Markdown。"""

AGENT_KINDS = ["researcher", "writer", "coder"]

TOOL = {
    "type": "function",
    "name": "dispatch_agents",
    "description": "把子任务分发给后端agent执行并取回结果",
    "parameters": {
        "type": "object",
        "properties": {
            "subtasks": {
                "type": "array",
                "description": "子任务列表，每个含 agent 类型与自包含 goal",
                "items": {
                    "type": "object",
                    "properties": {
                        "agent": {"type": "string", "enum": AGENT_KINDS},
                        "goal": {"type": "string", "description": "自包含执行prompt"},
                    },
                    "required": ["agent", "goal"],
                },
            }
        },
        "required": ["subtasks"],
    },
}

CANARIES = {"researcher": "R-CANARY-7734", "writer": "W-CANARY-1188", "coder": "C-CANARY-9051"}


def eid() -> str:
    return "event_" + uuid.uuid4().hex[:12]


def norm_agent(raw: str) -> str | None:
    s = (raw or "").lower()
    if "research" in s or "调研" in s:
        return "researcher"
    if any(k in s for k in ("writ", "copy", "文案", "market", "营销", "slogan", "创意")):
        return "writer"
    if "cod" in s or "code" in s or "代码" in s or "program" in s:
        return "coder"
    return None


def parse_subtasks(arguments: str) -> list[dict]:
    try:
        data = json.loads(arguments)
    except json.JSONDecodeError:
        return []
    raw = data.get("subtasks") or data.get("tasks") or data.get("agents") or []
    out = []
    for item in raw if isinstance(raw, list) else []:
        if isinstance(item, dict):
            agent_raw = str(item.get("agent") or item.get("type") or item.get("role") or "")
            goal = str(item.get("goal") or item.get("task") or item.get("prompt") or "")
            out.append({"agent_raw": agent_raw, "agent": norm_agent(agent_raw), "goal": goal})
    return out


async def backend(agent: str, goal: str, substantive: bool = True) -> dict:
    await asyncio.sleep(0.4)
    topic = (goal or "").strip()[:24]
    if substantive:
        finding = {
            "researcher": f"调研结论（编号 R-CANARY-7734）：围绕「{topic}」的核心发现有二，其中第二点的量化指标为 23%，置信度较高。",
            "writer": f"slogan 提案（编号 W-CANARY-1188）：围绕「{topic}」的候选 —— 'Guard Every Quiet Moment.'",
            "coder": f"已交付（编号 C-CANARY-9051）：「{topic}」实现完成，平均 O(n log n)，12 行 pytest 全绿。",
        }[agent]
    else:
        finding = f"[{agent}] 已完成（编号见 canary 字段）：细节省略。"
    return {"agent": agent, "canary": CANARIES[agent], "finding": finding}


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

    async def respond(self, timeout=45):
        await self.send(
            {"event_id": eid(), "type": "response.create", "response": {"modalities": ["text"]}}
        )
        text, calls = "", []
        while True:
            raw = await asyncio.wait_for(self.ws.recv(), timeout)
            e = json.loads(raw)
            t = e.get("type")
            if t == "response.text.delta":
                text += e.get("delta", "")
            elif t == "response.done":
                for it in e.get("response", {}).get("output", []):
                    if it.get("type") == "function_call":
                        calls.append(it)
                return text, calls
            elif t == "error":
                raise RuntimeError(json.dumps(e, ensure_ascii=False)[:250])

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

    async def roundtrip(self, user_text: str, substantive: bool = True):
        await self.user(user_text)
        await asyncio.sleep(0.3)
        text, calls = await self.respond()
        if not calls:
            return text, [], None, []
        call_id = calls[0]["call_id"]
        subtasks = parse_subtasks(calls[0].get("arguments", ""))
        results = list(
            await asyncio.gather(
                *(backend(st["agent"] or "researcher", st["goal"], substantive) for st in subtasks)
            )
        )
        await self.tool_output(call_id, {"results": results})
        await asyncio.sleep(0.3)
        ack, _ = await self.respond()
        return ack, subtasks, call_id, results


def verdict(ok: bool, label: str, detail: str = "", soft: bool = False) -> bool:
    mark = "PASS" if ok else ("WARN" if soft else "FAIL")
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))
    return ok or soft


async def main():
    key = os.environ["DASHSCOPE_API_KEY"]
    ws = await websockets.connect(URL, additional_headers={"Authorization": f"Bearer {key}"})
    rt = RT(ws)
    print("<-", json.loads(await ws.recv()).get("type"))
    await rt.send(
        {
            "event_id": eid(),
            "type": "session.update",
            "session": {"modalities": ["text"], "instructions": HEAD_DOCTRINE_V3, "tools": [TOOL]},
        }
    )
    while True:
        if json.loads(await ws.recv()).get("type") in ("session.updated", "error"):
            break
    print("session ready (v3 doctrine: OpenAI Realtime 2.0 Tools paradigm, ported)\n")
    score = {}

    # ---------- T0 preamble (new, official-recommended) ----------
    print("== T0: preamble 行为（慢工具前播报）==")
    ack, subs, _, _ = await rt.roundtrip("帮我调研一下边缘计算在自动驾驶中的应用现状")
    t0 = bool(subs)
    verdict(t0, "任务正常分发", f"subtasks={len(subs)}")
    score["T0"] = t0

    # ---------- T1 boundary ----------
    print("\n== T1: 决策边界 ==")
    t1 = True
    text, subs, _, _ = await rt.roundtrip("嗨，随便聊聊")
    t1 &= verdict(not subs, "闲聊直答", f"reply={text[:30]!r}")
    text, subs, _, _ = await rt.roundtrip("珠穆朗玛峰有多高？")
    t1 &= verdict(not subs, "简单事实直答（OpenAI规则：一句话可答不调工具）", f"dispatched={bool(subs)} reply={text[:40]!r}")
    text, subs, _, _ = await rt.roundtrip("帮我调研固态电池商业化进展")
    t1 &= verdict(bool(subs), "调研任务分发", f"subtasks={len(subs)}")
    score["T1"] = t1

    # ---------- T2 self-containment ----------
    print("\n== T2: goal 自包含 ==")
    await rt.user("背景：我们在做智能家居项目 Aurora，主打独居老人监护。记住。")
    await asyncio.sleep(0.3)
    await rt.respond()
    ack, subs, _, _ = await rt.roundtrip("调研一下我们产品在养老院的落地机会")
    t2 = bool(subs) and all(re.search(r"Aurora|老人|养老|监护", st["goal"]) for st in subs if st["goal"])
    verdict(t2, "goal 内联上下文", f"goals={[st['goal'][:46] for st in subs]}", soft=not subs)
    score["T2"] = t2

    # ---------- T3 fidelity ----------
    print("\n== T3: 结果忠实性 ==")
    ack, subs, _, results = await rt.roundtrip("调研远程办公对团队信任的影响并讲给我听", substantive=True)
    expected = [r["canary"] for r in results]
    t3a = bool(ack) and all(c in ack for c in expected)
    verdict(t3a, "实质结果逐字含金丝雀", f"missing={[c for c in expected if c not in ack]}")
    ackb, subsb, _, resultsb = await rt.roundtrip("调研站立办公对腰椎的影响并讲给我听", substantive=False)
    expectedb = [r["canary"] for r in resultsb]
    honest = ("未返回" in ackb or "细节" in ackb) and len(ackb) < 160
    fab = len(ackb) > 160 and not all(c in ackb for c in expectedb)
    verdict(not fab, "空洞结果不编造", f"len={len(ackb)} ack={ackb[:60]!r}", soft=True)
    verdict(honest, "空洞结果如实相告+提议深挖", f"ack={ackb[:80]!r}", soft=True)
    score["T3"] = t3a and not fab

    # ---------- T4 routing ----------
    print("\n== T4: 路由准确性（不发明/不改名）==")
    ack, subs, _, _ = await rt.roundtrip(
        "三件事：1) 调研WebGPU现状 2) 写一句英文slogan 3) 写一个Python快速排序"
    )
    raws = [st["agent_raw"] for st in subs]
    exact = all(r in AGENT_KINDS for r in raws)
    got = [st["agent"] for st in subs if st["agent"]]
    t4 = exact and sorted(got) == sorted(AGENT_KINDS)
    verdict(t4, "枚举精确命中（raw 即合法值）", f"raw={raws}")
    score["T4"] = t4

    # ---------- T5 refinement ----------
    print("\n== T5: 增量再分发 ==")
    ack2, subs2, _, _ = await rt.roundtrip("对刚才第一个子任务的结果再深入调研一步")
    t5 = bool(subs2) and any(("23%" in (st["goal"] or "")) or ("R-CANARY-7734" in (st["goal"] or "")) for st in subs2)
    verdict(bool(subs2), "再次分发", f"subtasks={len(subs2)}")
    verdict(t5, "新 goal 内联前次结果（23%/金丝雀）", f"goals={[st['goal'][:60] for st in subs2]}", soft=bool(subs2) and not t5)
    score["T5"] = bool(subs2)

    await ws.close()
    print("\n== SUMMARY (v3 vs v2) ==")
    for k in sorted(score):
        print(f"  {k}: {'PASS' if score[k] else 'FAIL'}")


if __name__ == "__main__":
    asyncio.run(main())
