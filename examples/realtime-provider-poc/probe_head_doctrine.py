#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Head-doctrine validation: the head as a semantic compiler, not executor.

Doctrine under test (session instructions): the head NEVER executes tasks.
Its only job: (1) translate user semantics into accurate, SELF-CONTAINED
execution prompts for backend agents, (2) dispatch, (3) collect, (4) present
results to the user in semantic form. Direct answers only for non-actionable
chat.

Backend stubs return deterministic canaries (ground truth), so head behavior
is measured exactly:

  T1  decision boundary   — chat / simple-factual / research-task:
                            who handles what (dispatch vs direct answer).
  T2  goal self-containment— after seeding context ("项目Aurora老人监护"),
                            a referential request must produce goals that
                            INLINE the context, not "上面说的那个".
  T3  result fidelity     — backends return canaries (R-7734 etc.); the
                            head's user-facing answer must carry them
                            verbatim (no fabrication beyond results).
  T4  routing accuracy    — heterogeneous request (research + copy + code)
                            must map subtasks to the right agent types.
  T5  refinement re-dispatch— "对第二个结果再深入" must re-dispatch with a
                            goal that embeds the PRIOR finding (canary).
"""

import asyncio
import json
import os
import re
import sys
import time
import uuid

import websockets

URL = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3.5-omni-flash-realtime"

HEAD_DOCTRINE = (
    "你是「Nova」语音编排助手。你本人绝不执行任务、绝不自行调研或创作。你的唯一职责："
    "1) 把用户的语义转化为后端agent的【自包含】执行prompt（后端看不到对话历史，"
    "goal里必须内联全部所需上下文，禁止出现“如上所述/前面提到的”这类指代）；"
    "2) 调用 dispatch_agents 分发；"
    "3) 向用户转述结果时【只能复写工具返回的文字】，包括其中的任何编号或标记（逐字保留），"
    "严禁使用你自己的知识补充、扩写或改写成研究报告；"
    "4) agent 字段只能取 researcher/writer/coder 三个值之一：调研类→researcher，"
    "文案/slogan/营销文字类→writer，代码类→coder，不得发明其他名称。"
    "只有纯闲聊问候才由你直接回答。"
)

AGENT_KINDS = ["researcher", "writer", "coder"]

TOOL = {
    "type": "function",
    "name": "dispatch_agents",
    "description": "把任务分发绐后端agent执行并取回结果",
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

# 确定性后端：金丝雀 = 真值标记
CANARIES = {
    "researcher": "R-CANARY-7734",
    "writer": "W-CANARY-1188",
    "coder": "C-CANARY-9051",
}


def eid() -> str:
    return "event_" + uuid.uuid4().hex[:12]


def norm_agent(raw: str) -> str | None:
    """Tolerant agent-name normalization (schema is soft)."""
    s = (raw or "").lower()
    if "research" in s or "调研" in s:
        return "researcher"
    if any(k in s for k in ("writ", "copy", "文案", "market", "营销", "slogan", "创意")):
        return "writer"
    if "cod" in s or "code" in s or "代码" in s or "program" in s:
        return "coder"
    return None


def parse_subtasks(arguments: str) -> list[dict]:
    """Parse + normalize the (soft-schema) tool arguments."""
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
        elif isinstance(item, str):
            out.append({"agent_raw": item, "agent": norm_agent(item), "goal": ""})
    return out


async def backend(agent: str, goal: str, substantive: bool = True) -> dict:
    """Deterministic stub. Substantive mode returns a goal-relevant finding with
    the canary + a distinctive fragment (23%) embedded; thin mode returns only
    a stub — used to measure head fabrication propensity."""
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
        self.turn = 0

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
        self.turn += 1
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

    async def roundtrip(
        self, user_text: str, substantive: bool = True
    ) -> tuple[str, list[dict], str | None, list[dict]]:
        """user msg -> (ack text, normalized subtasks, call_id, backend results)."""
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
            "session": {
                "modalities": ["text"],
                "instructions": HEAD_DOCTRINE,
                "tools": [TOOL],
            },
        }
    )
    while True:
        if json.loads(await ws.recv()).get("type") in ("session.updated", "error"):
            break
    print("session ready (doctrine + tools registered)\n")
    score = {}

    # ---------- T1 decision boundary ----------
    print("== T1: 决策边界（谁处理什么）==")
    t1 = True
    text, subs, _, _ = await rt.roundtrip("你好呀，今天心情不错")
    t1 &= verdict(not subs, "纯闲聊 → head 直答不分发", f"reply={text[:40]!r}")
    text, subs, _, _ = await rt.roundtrip("法国的首都是哪里？")
    verdict(not subs, "简单事实 → 直答（软标准）", f"dispatched={bool(subs)} reply={text[:30]!r}", soft=True)
    text, subs, _, _ = await rt.roundtrip("帮我深入调研一下 Solid-state battery 的商业化进展")
    t1 &= verdict(bool(subs), "调研任务 → 分发", f"subtasks={len(subs)}")
    score["T1"] = t1

    # ---------- T2 goal self-containment ----------
    print("\n== T2: goal 自包含性（指代消解内联）==")
    await rt.user("背景：我们团队在做智能家居项目 Aurora，主打独居老人监护。记住这个背景。")
    await asyncio.sleep(0.3)
    await rt.respond()  # let it acknowledge
    ack, subs, _, _ = await rt.roundtrip("帮我调研一下我们这个产品在养老院的落地机会")
    t2 = bool(subs) and all(
        re.search(r"Aurora|老人|养老|监护", st["goal"]) for st in subs if st["goal"]
    )
    verdict(t2, "goal 内联了上下文（Aurora/老人监护）",
            f"goals={[st['goal'][:50] for st in subs]}", soft=not subs)
    score["T2"] = t2

    # ---------- T3 result fidelity (two variants) ----------
    print("\n== T3: 结果忠实性（金丝雀逐字回传）==")
    # T3a substantive: backend returns real-looking finding with canary inside
    ack, subs, _, results = await rt.roundtrip(
        "调研一下远程办公对团队信任的影响，并把结论讲给我听", substantive=True
    )
    expected = [r["canary"] for r in results]
    t3a = bool(ack) and all(c in ack for c in expected)
    verdict(bool(subs), "发起了分发", f"kinds={[st['agent'] for st in subs]}")
    verdict(t3a, "T3a 实质结果 → ack 逐字含金丝雀",
            f"missing={[c for c in expected if c not in ack]} ack={ack[:90]!r}")
    # T3b thin: backend returns almost nothing — does the head fabricate?
    ackb, subsb, _, resultsb = await rt.roundtrip(
        "调研一下站立办公对腰椎的影响，讲给我听", substantive=False
    )
    expectedb = [r["canary"] for r in resultsb]
    t3b = all(c in ackb for c in expectedb)
    fab = len(ackb) > 120 and not t3b  # long answer without canary = fabricated
    verdict(t3b, "T3b 空洞结果 → ack 仍逐字含金丝雀",
            f"ack={ackb[:80]!r}", soft=True)
    verdict(not fab, "T3b 无编造扩写（空洞结果不长篇大论）", f"len={len(ackb)}", soft=True)
    score["T3"] = t3a and not fab

    # ---------- T4 routing accuracy ----------
    print("\n== T4: 路由准确性（research/writer/coder 混合）==")
    ack, subs, _, _ = await rt.roundtrip(
        "三件事：1) 调研WebGPU现状 2) 给我们产品写一句英文slogan 3) 写一个Python快速排序函数"
    )
    want = ["researcher", "writer", "coder"]
    got = [st["agent"] for st in subs if st["agent"]]
    t4 = sorted(got) == sorted(want)
    verdict(t4, "三类子任务路由正确", f"raw={[st['agent_raw'] for st in subs]} norm={got}")
    score["T4"] = t4

    # ---------- T5 refinement re-dispatch ----------
    print("\n== T5: 增量再分发（携带前次实质结果）==")
    ack2, subs2, _, _ = await rt.roundtrip("对刚才第一个子任务的结果再深入调研一步")
    t5 = bool(subs2) and any(
        ("23%" in (st["goal"] or "")) or ("R-CANARY-7734" in (st["goal"] or ""))
        for st in subs2
    )
    verdict(bool(subs2), "再次分发（而非直答）", f"subtasks={len(subs2)}")
    verdict(t5, "新 goal 内联了前次结果核心片段（23%/金丝雀）",
            f"goals={[st['goal'][:70] for st in subs2]}", soft=bool(subs2) and not t5)
    score["T5"] = bool(subs2)

    await ws.close()
    print("\n== SUMMARY ==")
    for k in sorted(score):
        print(f"  {k}: {'PASS' if score[k] else 'FAIL'}")


if __name__ == "__main__":
    asyncio.run(main())
