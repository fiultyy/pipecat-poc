#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Orchestration layer for the voice PoC — the research conclusions, made code.

Layered contract (validated live over eight probe rounds):

  head (qwen3.5-omni-flash-realtime, v3 doctrine)
    tools: dispatch_intent(raw_intent)  — single string arg, no schema
           pressure on the head (strategy 2)
  formatter (GLM text model — glm-5-turbo suffices, verified)
    receives raw_intent, emits enum-exact subtask JSON (C1 verified),
    then echoes backend results VERBATIM under the [RESULTS] protocol
    (C3 verified, no decay under history)
  backends (canary stubs here; LM Studio / GLM-5.3 agents in production)
    deterministic findings with credential markers
  head ack: doctrine's receipt-echo rule (canary marks MUST appear
    verbatim — sparrow-generalized: 12/12 across novel marker types)

Also ports the Codex-inspired conventions:
  - ``【凭证…】`` delimiters (machine-parsable credential echo, arm B)
  - ``"Agent Final Message":`` prefix for final vs intermediate results
  - ``[SAY]`` / ``[KNOW]`` channel prefixes (degraded Speakable/Commentary)
  - remain_silent tool (no-op so the head can choose silence politely)
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from openai import AsyncOpenAI

AGENT_KINDS = ("researcher", "writer", "coder")

FORMATTER_SYSTEM = """你是执行编排层。你接收一段用户意图描述，职责：
1. 解析意图，拆成子任务，每个子任务指定 agent 类型（只能是 researcher/writer/coder 之一）和自包含 goal；
2. 等待子任务执行（执行方会把结果发给你）；
3. 把执行结果【逐字原样】作为你的最终回复返回——包括所有编号、标记、数字，一个字都不许改、不许丢、不许加。

当收到用户消息时，只输出子任务 JSON（不要执行结果）：{"subtasks": [{"agent": "...", "goal": "..."}]}
当收到以 [RESULTS] 开头的消息时，把其后内容逐字作为你的回复输出，不加任何前后缀。"""

FINAL_PREFIX = '"Agent Final Message":\n\n'


@dataclass
class BackendResult:
    agent: str
    finding: str
    canary: str | None = None


BackendFn = Callable[[str, str], Awaitable[BackendResult]]


def make_credential(value: str, kind: str = "id") -> str:
    """Wrap a value in the machine-parsable credential convention (arm B)."""
    return f"【凭证{value}】"


def extract_credentials(text: str) -> list[str]:
    """Pull credential values back out of a (possibly spoken) ack."""
    return re.findall(r"【凭证(.+?)】", text)


class Normalizer:
    """Defensive argument normalization — schema softness is a constant.

    The head/formatter may emit near-miss enum values (agent_001,
    backend_1, marketing_agent); production handlers must not trust
    names. Digit-normalized matching + synonym maps (probe-verified
    across three rounds).
    """

    SYNONYMS = {
        "researcher": ("research", "调研", "researcher"),
        "writer": ("writ", "copy", "文案", "market", "营销", "slogan", "创意", "writer"),
        "coder": ("cod", "code", "代码", "program", "coder"),
    }

    @classmethod
    def agent(cls, raw: str | None) -> str | None:
        s = (raw or "").lower()
        for canonical, keys in cls.SYNONYMS.items():
            if any(k in s for k in keys):
                return canonical
        digits = "".join(ch for ch in s if ch.isdigit()).lstrip("0")
        if digits and digits in ("1", "2", "3"):
            return AGENT_KINDS[int(digits) - 1]
        return None

    @classmethod
    def subtasks(cls, arguments: str | dict) -> list[dict]:
        """Parse tool arguments tolerantly: any of the known key spellings,
        any of the item field spellings; agents normalized; goals kept."""
        data = arguments if isinstance(arguments, dict) else {}
        if isinstance(arguments, str):
            try:
                data = json.loads(arguments)
            except json.JSONDecodeError:
                return []
        raw = None
        for key in ("subtasks", "tasks", "agents", "items"):
            if isinstance(data.get(key), list):
                raw = data[key]
                break
        out = []
        for item in raw or []:
            if isinstance(item, dict):
                agent_raw = str(
                    item.get("agent") or item.get("type") or item.get("role") or ""
                )
                goal = str(item.get("goal") or item.get("task") or item.get("prompt") or "")
                if agent := cls.agent(agent_raw):
                    out.append({"agent": agent, "goal": goal})
        return out


class Formatter:
    """GLM-backed two-turn formatter: intent -> plan; results -> verbatim."""

    def __init__(self, client: AsyncOpenAI, model: str = "glm-5-turbo"):
        self._client = client
        self.model = model

    async def plan(self, raw_intent: str) -> list[dict]:
        msgs = [
            {"role": "system", "content": FORMATTER_SYSTEM},
            {"role": "user", "content": raw_intent},
        ]
        resp = await self._client.chat.completions.create(
            model=self.model, messages=msgs, max_tokens=4096
        )
        raw = (resp.choices[0].message.content or "").strip()
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return []
        items = data.get("subtasks") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return []
        # Normalize agents but never invent: unrecognizable kinds drop out.
        out = []
        for item in items:
            if isinstance(item, dict):
                if agent := Normalizer.agent(str(item.get("agent", ""))):
                    out.append({"agent": agent, "goal": str(item.get("goal", ""))})
        return out

    async def relay(self, raw_intent: str, results: list[BackendResult]) -> str:
        """Formatter turn 2: results in, verbatim string out (C3 contract)."""
        blob = json.dumps(
            {"results": [{"agent": r.agent, "finding": r.finding} for r in results]},
            ensure_ascii=False,
        )
        msgs = [
            {"role": "system", "content": FORMATTER_SYSTEM},
            {"role": "user", "content": raw_intent},
            {"role": "assistant", "content": json.dumps(
                {"subtasks": [{"agent": r.agent, "goal": ""} for r in results]},
                ensure_ascii=False,
            )},
            {"role": "user", "content": f"[RESULTS] {blob}"},
        ]
        resp = await self._client.chat.completions.create(
            model=self.model, messages=msgs, max_tokens=8192
        )
        return (resp.choices[0].message.content or "").strip()


@dataclass
class Orchestrator:
    """Owns the dispatch_intent / remain_silent tool handlers for the head.

    The head never sees schema: dispatch_intent takes one string. This
    class plans via the formatter, fans out to backends in parallel,
    collects results, and returns the formatter's verbatim relay as the
    tool output (the head then speaks it under the receipt-echo doctrine).
    """

    formatter: Formatter
    backend_fn: BackendFn
    history: list[dict[str, Any]] = field(default_factory=list)

    async def dispatch_intent(self, raw_intent: str) -> str:
        subtasks = await self.formatter.plan(raw_intent)
        if not subtasks:
            return json.dumps(
                {"status": "clarify", "note": "无法拆解该意图，请补充信息"},
                ensure_ascii=False,
            )
        results = list(
            await asyncio.gather(
                *(self.backend_fn(s["agent"], s["goal"]) for s in subtasks)
            )
        )
        self.history.append(
            {"intent": raw_intent, "subtasks": subtasks,
             "results": [{"agent": r.agent, "finding": r.finding} for r in results]}
        )
        relay = await self.formatter.relay(raw_intent, results)
        return f"{FINAL_PREFIX}{relay}"


# ---- head-facing tool wrappers (docstrings become the tool schema) ----

HEAD_TOOLS_DOCTRINE = """# Role and Objective
你是「Nova」语音编排助手。你听懂用户、提取意图、调用工具、把执行层返回的结果讲给用户。

# Tools
- dispatch_intent：把用户意图（自包含，指代全部展开）交给执行层。凡需要查询或执行具体事务的请求必须调用，不得推脱。
- remain_silent：当最好的回应是不说话时调用（如控制消息后的确认），无用户可见效果。
- 闲聊、问候、一句话可答的常识直接回答。

# After Tool Calls（最高优先级规则）
- 执行结果中的标识信息（编号、链接、电话、金额、序列号、【凭证…】等）必须原样出现在你给用户的回复里，一个字符都不能改。丢失即事故。
- 结果以 "Agent Final Message" 开头时为最终结果；否则是进展更新，简要提及即可。
- 除此之外不添加执行层没提到的任何事实细节。

# Personality and Tone
中文口语，简洁友好，不用 Markdown。"""


async def dispatch_intent_tool(params, raw_intent: str):
    """把用户意图交给执行编排层处理并取回结果。

    Args:
        raw_intent: 用户意图的完整自包含描述，含所有上下文。
    """
    orch: Orchestrator = params.app_resources["orchestrator"]
    result = await orch.dispatch_intent(raw_intent)
    await params.result_callback(result)


async def remain_silent_tool(params):
    """当最好的回应是不说话时调用此工具；无用户可见效果。"""
    await params.result_callback({"status": "silent"})


def head_tools() -> list:
    return [dispatch_intent_tool, remain_silent_tool]
