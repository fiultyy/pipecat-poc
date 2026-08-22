#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Text-input PoC run: typed prompt -> head -> submit_intent -> GLM fan-out.

The head (Qwen3.5-Omni-Realtime, text-only output) receives a text user
message instead of audio, still goes through submit_intent, fans out to two
GLM-5.3 agents in parallel, and speaks (as text) the final ack.
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
glm = AsyncOpenAI(api_key=os.environ["GLM_API_KEY"], base_url=GLM_BASE_URL)

PROMPT = sys.argv[1] if len(sys.argv) > 1 else (
    "帮我调研一下：深度工作对程序员产出的影响，以及站立办公对健康的影响"
)

T0 = time.monotonic()
EVENTS: list[str] = []
FANOUT: list[dict] = []


def ts() -> str:
    return f"{time.monotonic() - T0:6.2f}s"


class TextTap(FrameProcessor):
    def __init__(self):
        super().__init__()
        self.texts: list[str] = []

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        text = getattr(frame, "text", None)
        if text and text.strip():
            self.texts.append(text)
        await self.push_frame(frame, direction)


async def glm_agent(name: str, goal: str) -> dict:
    t = time.monotonic()
    resp = await glm.chat.completions.create(
        model=GLM_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    f"你是执行agent「{name}」。对目标做一句话调研，只输出JSON："
                    '{"agent":"...","goal":"...","finding":"一句话结论","confidence":0.0-1.0}'
                ),
            },
            {"role": "user", "content": goal},
        ],
        max_tokens=2048,
    )
    raw = (resp.choices[0].message.content or "").strip()
    raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = {"agent": name, "goal": goal, "finding": raw[:120], "confidence": 0.0}
    EVENTS.append(
        f"{ts()} agent[{name}] done {time.monotonic()-t:.2f}s "
        f"confidence={data.get('confidence')}"
    )
    return data


async def submit_intent(params: FunctionCallParams, subtasks_json: str):
    EVENTS.append(f"{ts()} submit_intent <- head 调用")
    EVENTS.append(f"{ts()}   args: {subtasks_json[:160]}")
    intent = json.loads(subtasks_json)
    goals = [s["goal"] for s in intent.get("subtasks", [])]
    EVENTS.append(f"{ts()} fan-out: {len(goals)} 个 GLM agent 并行启动")
    results = await asyncio.gather(*(glm_agent(f"agent-{i+1}", g) for i, g in enumerate(goals)))
    FANOUT.extend(results)
    EVENTS.append(f"{ts()} fan-in 完成，回传结果")
    await params.result_callback({"status": "done", "results": [r["finding"] for r in results]})


async def main():
    logger.info(f"head=qwen3.5-omni-flash-realtime(text-only) agents={GLM_MODEL}")
    logger.info(f"PROMPT: {PROMPT}")

    head = create_realtime_head(
        RealtimeHeadConfig(
            provider=RealtimeProvider.QWEN,
            system_instruction=(
                "你是编排助手。收到调研类请求必须调用 submit_intent 工具"
                '（subtasks_json 形如 {"subtasks":[{"goal":"..."},{"goal":"..."}]}），'
                "拿到结果后用一两句话汇总。闲聊直接回答。"
            ),
            tools=[submit_intent],
            text_only=True,
        )
    )

    context = LLMContext(tools=[submit_intent])
    context.add_message({"role": "user", "content": PROMPT})
    aggregators = LLMContextAggregatorPair(context)
    tap = TextTap()
    worker = PipelineWorker(
        Pipeline([aggregators.user(), head, tap, aggregators.assistant()]),
        cancel_on_idle_timeout=False,
    )
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)

    async def drive():
        await asyncio.sleep(2.0)
        EVENTS.append(f"{ts()} 发送用户文本 prompt")
        await worker.queue_frame(LLMRunFrame())
        # wait for fan-out + final ack
        for _ in range(240):
            if FANOUT and len(tap.texts) > 3:
                await asyncio.sleep(2.5)
                break
            await asyncio.sleep(1.0)
        await worker.queue_frame(EndFrame())

    await asyncio.wait_for(asyncio.gather(runner.run(), drive()), timeout=300)

    print("\n== TIMELINE ==")
    for e in EVENTS:
        print(" ", e)
    print("\n== AGENT RESULTS ==")
    for r in FANOUT:
        print(" ", json.dumps(r, ensure_ascii=False))
    print("\n== HEAD ACK ==")
    print(" ", "".join(tap.texts))


if __name__ == "__main__":
    asyncio.run(main())
