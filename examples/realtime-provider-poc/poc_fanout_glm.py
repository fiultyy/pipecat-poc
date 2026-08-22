#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Full PoC loop: Qwen-Omni head -> submit_intent -> 2x GLM-5.3 agents.

Verifies the orchestration path end-to-end against real clouds:

    mic/text -> Qwen3.5-Omni-Realtime head (DashScope WS)
      -> function call submit_intent(subtasks=[{goal}, {goal}])
        -> asyncio.gather: 2 GLM-5.3 agents in parallel (z.ai coding plan,
           OpenAI-compatible), each returning a schema-constrained JSON
        -> results -> function_call_output -> head speaks the ack

Env: DASHSCOPE_API_KEY, GLM_API_KEY (coding plan key),
     GLM_BASE_URL (default https://open.bigmodel.cn/api/coding/paas/v4).
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from openai import AsyncOpenAI

sys.path.insert(0, str(Path(__file__).parent))

from providers import RealtimeHeadConfig, RealtimeProvider, create_realtime_head

from pipecat.frames.frames import EndFrame, LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.processors.frame_processor import Frame, FrameDirection, FrameProcessor
from pipecat.services.llm_service import FunctionCallParams
from pipecat.workers.runner import WorkerRunner

load_dotenv()

GLM_BASE_URL = os.environ.get("GLM_BASE_URL", "https://open.bigmodel.cn/api/coding/paas/v4")
GLM_MODEL = os.environ.get("GLM_MODEL", "glm-5.3")

AGENT_SCHEMA = {
    "type": "object",
    "properties": {
        "agent": {"type": "string"},
        "goal": {"type": "string"},
        "finding": {"type": "string", "description": "one-sentence research finding"},
        "confidence": {"type": "number", "description": "0.0-1.0"},
    },
    "required": ["agent", "goal", "finding", "confidence"],
}

glm_client = AsyncOpenAI(api_key=os.environ["GLM_API_KEY"], base_url=GLM_BASE_URL)

EVENTS: list[str] = []
TOOL_DONE = asyncio.Event()
FANOUT_RESULTS: list[dict] = []


class Timeline(FrameProcessor):
    def __init__(self):
        super().__init__()
        self.texts: list[str] = []

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        text = getattr(frame, "text", None)
        if text and text.strip():
            self.texts.append(text)
        await self.push_frame(frame, direction)


def ts() -> str:
    return f"{time.monotonic() - T0:6.2f}s"


T0 = time.monotonic()


async def glm_agent(name: str, goal: str) -> dict:
    """One orchestrator agent: GLM-5.3 with a schema-constrained reply."""
    t = time.monotonic()
    resp = await glm_client.chat.completions.create(
        model=GLM_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    f"你是执行agent「{name}」。针对给定目标做一句话调研并输出 JSON，"
                    f"严格遵守此 schema：{json.dumps(AGENT_SCHEMA, ensure_ascii=False)}。"
                    "只输出 JSON，不要其他文字。"
                ),
            },
            {"role": "user", "content": goal},
        ],
        max_tokens=2048,
    )
    raw = resp.choices[0].message.content or ""
    # tolerate markdown fences
    raw = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = {"agent": name, "goal": goal, "finding": raw[:120], "confidence": 0.0}
    dt = time.monotonic() - t
    EVENTS.append(f"{ts()} agent[{name}] done in {dt:.2f}s -> {json.dumps(data, ensure_ascii=False)[:110]}")
    return data


async def submit_intent(params: FunctionCallParams, subtasks_json: str):
    """Head tool: intent schema -> parallel GLM agents -> results back."""
    EVENTS.append(f"{ts()} submit_intent called: {subtasks_json[:120]}")
    intent = json.loads(subtasks_json)
    goals = [s["goal"] for s in intent.get("subtasks", [])]
    agents = [f"agent-{i+1}" for i in range(len(goals))]
    EVENTS.append(f"{ts()} fan-out to {len(goals)} GLM agents (parallel)")
    results = await asyncio.gather(*(glm_agent(a, g) for a, g in zip(agents, goals)))
    FANOUT_RESULTS.extend(results)
    EVENTS.append(f"{ts()} fan-in complete, returning ack")
    await params.result_callback(
        {"status": "done", "results": [r["finding"] for r in results]}
    )


async def main():
    head = create_realtime_head(
        RealtimeHeadConfig(
            provider=RealtimeProvider.QWEN,
            system_instruction=(
                "你是语音编排助手。用户提出调研任务时必须调用 submit_intent 工具"
                '（subtasks_json 形如 {"subtasks":[{"goal":"..."},...]}，拆2个子任务），'
                "拿到结果后用一两句话汇总确认。"
            ),
            tools=[submit_intent],
            text_only=True,
        )
    )
    logger.info(f"head={type(head).__name__} | agents={GLM_MODEL} @ {GLM_BASE_URL}")

    context = LLMContext(tools=[submit_intent])
    context.add_message(
        {"role": "user", "content": "调用submit_intent：分别调研 咖啡因对睡眠的影响 和 每日步数对心血管的好处"}
    )
    aggregators = LLMContextAggregatorPair(context)
    timeline = Timeline()
    worker = PipelineWorker(
        Pipeline([aggregators.user(), head, timeline, aggregators.assistant()]),
        cancel_on_idle_timeout=False,
    )
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)

    async def drive():
        await asyncio.sleep(2.0)
        EVENTS.append(f"{ts()} LLMRunFrame -> head")
        await worker.queue_frame(LLMRunFrame())
        # wait for the whole tool round (agents + ack)
        for _ in range(180):
            if FANOUT_RESULTS and timeline.texts and TOOL_DONE.is_set():
                break
            if FANOUT_RESULTS and len(timeline.texts) > 5:
                TOOL_DONE.set()
            await asyncio.sleep(1.0)
        await asyncio.sleep(2.0)
        await worker.queue_frame(EndFrame())

    await asyncio.wait_for(asyncio.gather(runner.run(), drive()), timeout=240)

    print("\n== TIMELINE ==")
    for e in EVENTS:
        print(" ", e)
    print("\n== AGENT RESULTS (schema-constrained) ==")
    for r in FANOUT_RESULTS:
        print(" ", json.dumps(r, ensure_ascii=False))
    print("\n== HEAD ACK ==")
    print(" ", "".join(timeline.texts))


if __name__ == "__main__":
    asyncio.run(main())
