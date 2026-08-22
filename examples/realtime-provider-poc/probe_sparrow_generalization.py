#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Sparrow-generalization probe: does the verbatim-relay rule generalize?

The prior canary clause named R-CANARY explicitly, so the head may be
pattern-matching that specific token rather than following the general
"identifiers must survive verbatim" rule. This probe measures:

  ARM A (bare)   : markers embedded plainly in the backend finding text.
  ARM B (marked) : the same markers wrapped in 【凭证…】 delimiters, with
                   the doctrine defining the convention once.

Doctrine in BOTH arms states the rule generically (编号/链接/电话/金额/
序列号 must survive verbatim) and names NO concrete marker format.

Marker batteries (novel every round, never seen in the doctrine):
  order id, tracking url, phone, money amount, serial, short code.

Measures, per arm:
  - survival rate by marker type (does the rule generalize beyond the
    enumerated categories?)
  - survival by round (history decay curve within the session)
  - a control: an identifier category NOT in the doctrine's list
    (a quoted product-name string, a time "14:37:22") to test whether
    "credential-ness" generalizes or only the listed categories do.

The head is the variable; dispatch_intent's handler returns results
directly (formatter layer already proven faithful).
"""

import asyncio
import json
import os
import sys
import uuid

import websockets

sys.path.insert(0, ".")
from dotenv import load_dotenv

load_dotenv()

URL = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3.5-omni-flash-realtime"

DOCTRINE_BASE = """# Role and Objective
你是「Nova」语音编排助手。你听懂用户、提取意图、调用 dispatch_intent、把执行层返回的结果讲给用户。

# Tools
- 只有一个工具 dispatch_intent：把用户意图（自包含，指代全部展开）传入。意图明确即调用。
- 凡需要查询或获取具体信息的请求（订单、文件、链接、电话、金额、设备、记录等）都必须调用 dispatch_intent 交给执行层，不得自行回答，也不得以"无法查询"推脱。
- 闲聊、问候、一句话可答的常识问题直接回答。

# After Tool Calls（最高优先级规则）
- 执行结果中的标识信息（编号、链接、电话号码、金额、序列号等）必须原样出现在你给用户的回复里，一个字符都不能改。这些是用户核对任务的凭证，丢失即事故。
- 除此之外不添加执行层没提到的任何事实细节。

# Personality and Tone
中文口语，简洁友好，不用 Markdown。"""

DOCTRINE_MARKED_EXTRA = """
- 结果中凡以【凭证…】围起来的内容属于必回显凭证，回复里必须包含【凭证…】及其内部原文。"""

TOOL = {
    "type": "function",
    "name": "dispatch_intent",
    "description": "把用户意图交给执行层处理并取回结果",
    "parameters": {
        "type": "object",
        "properties": {"raw_intent": {"type": "string", "description": "自包含意图描述"}},
        "required": ["raw_intent"],
    },
}

# marker battery per round: (label, marker, category-in-doctrine?)
ROUNDS = [
    ("订单号", "ORD-88213-ZK", True, "帮我查一下我上周下的订单"),
    ("链接", "https://res.nova.io/d/9f8a2c", True, "帮我把那份报告的下载链接找出来给我"),
    ("电话", "138-0013-9000", True, "帮我查一下客服回拨电话是多少"),
    ("金额", "¥1,299.50", True, "帮我确认一下这笔订单要付多少钱"),
    ("序列号", "SN_QX778899", True, "帮我查一下设备序列号"),
    ("时间戳", "14:37:22", False, "帮我看一下上次任务完成的时间点"),
]


debug_round = 0


def eid() -> str:
    return "event_" + uuid.uuid4().hex[:12]


def make_finding(label: str, marker: str, marked: bool) -> str:
    if marked:
        return f"执行完成。凭证类别：{label}，【凭证{marker}】，请用户核对。"
    return f"执行完成。{label}为 {marker}，请用户核对。"


def verdict(ok, label, detail="", soft=False):
    mark = "PASS" if ok else ("WARN" if soft else "FAIL")
    print(f"    [{mark}] {label}" + (f" — {detail}" if detail else ""))
    return ok


async def run_arm(arm_name: str, marked: bool):
    key = os.environ["DASHSCOPE_API_KEY"]
    ws = await websockets.connect(URL, additional_headers={"Authorization": f"Bearer {key}"})
    doctrine = DOCTRINE_BASE + (DOCTRINE_MARKED_EXTRA if marked else "")
    await ws.recv()
    await ws.send(
        json.dumps(
            {
                "event_id": eid(),
                "type": "session.update",
                "session": {
                    "modalities": ["text"],
                    "instructions": doctrine,
                    "tools": [TOOL],
                },
            }
        )
    )
    while True:
        if json.loads(await ws.recv()).get("type") in ("session.updated", "error"):
            break

    async def send(o):
        if os.environ.get("SPARROW_DEBUG") and o.get("type") == "conversation.item.create":
            print("      [dbg OUT]", json.dumps(o, ensure_ascii=False)[:220])
        await ws.send(json.dumps(o))

    async def respond(timeout=60):
        await send({"event_id": eid(), "type": "response.create", "response": {"modalities": ["text"]}})
        text, call = "", None
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout)
            e = json.loads(raw)
            t = e.get("type")
            if os.environ.get("SPARROW_DEBUG") and debug_round <= 1 and t != "response.text.delta":
                print("      [dbg IN]", t)
            if t == "response.text.delta":
                text += e.get("delta", "")
            elif t == "response.done":
                for it in e.get("response", {}).get("output", []):
                    if it.get("type") == "function_call":
                        call = it
                if os.environ.get("SPARROW_DEBUG"):
                    print(f"      [dbg r{debug_round}] outputs=", [(it.get('type'), (it.get('name') or '')[:20]) for it in e.get('response', {}).get('output', [])], "| text=", repr(text[:60]))
                return text, call

    print(f"\n== ARM {arm_name}（{'【凭证】格式化标记' if marked else '裸文本嵌入'}）==")
    stats = []  # (round_idx, label, marker, survived, in_doctrine)
    global debug_round
    for i, (label, marker, listed, ask) in enumerate(ROUNDS, 1):
        debug_round = i
        if os.environ.get("SPARROW_DEBUG"):
            print(f"      [dbg] ask={ask!r}")
        await send(
            {
                "event_id": eid(),
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": ask}],
                },
            }
        )
        await asyncio.sleep(0.3)
        _, call = await respond()
        if not call:
            print(f"    [FAIL] round{i}({label}) head 未调用工具")
            stats.append((i, label, marker, False, listed))
            continue
        finding = make_finding(label, marker, marked)
        await send(
            {
                "event_id": eid(),
                "type": "conversation.item.create",
                "item": {
                    "type": "function_call_output",
                    "call_id": call["call_id"],
                    "output": json.dumps({"results": [{"finding": finding}]}, ensure_ascii=False),
                },
            }
        )
        await asyncio.sleep(0.3)
        ack, _ = await respond()
        survived = marker in ack
        stats.append((i, label, marker, survived, listed))
        state = "存活" if survived else "丢失"
        verdict(survived, f"round{i} {label}（{'教义列举类' if listed else '未列举类'}）",
                f"{marker!r} {state} | ack={ack[:60]!r}")
    await ws.close()
    return stats


def summarize(name, stats):
    listed = [s for s in stats if s[4]]
    unlisted = [s for s in stats if not s[4]]
    def rate(ss): return f"{sum(1 for s in ss if s[3])}/{len(ss)}"
    print(f"\n  {name}: 教义列举类存活 {rate(listed)} | 未列举类(时间戳) {rate(unlisted)} | 总体 {rate(stats)}")
    print(f"    按轮次: {[('R%d' % s[0], 'Y' if s[3] else 'N') for s in stats]}")


async def main():
    a = await run_arm("A", marked=False)
    b = await run_arm("B", marked=True)
    print("\n== SUMMARY ==")
    summarize("ARM A 裸文本", a)
    summarize("ARM B 格式化标记", b)


if __name__ == "__main__":
    asyncio.run(main())
