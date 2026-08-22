#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Live cloud E2E for QwenOmniRealtimeLLMService (needs DASHSCOPE_API_KEY).

Runs the real pipecat service against the real DashScope endpoint:
factory -> service -> pipeline -> frames, including a submit_intent-style
tool call roundtrip. Text-only modality (no audio I/O on this box).
"""

import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent))

from providers import RealtimeHeadConfig, RealtimeProvider, create_realtime_head

from pipecat.frames.frames import (
    EndFrame,
    LLMContextFrame,
    LLMFullResponseEndFrame,
    LLMRunFrame,
    TextFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import Frame, FrameDirection, FrameProcessor
from pipecat.services.llm_service import FunctionCallParams
from pipecat.workers.runner import WorkerRunner

load_dotenv()

TOOL_CALLED = asyncio.Event()
TOOL_ARGS: dict = {}


class TextPrinter(FrameProcessor):
    def __init__(self):
        super().__init__()
        self.texts: list[str] = []
        self.full_response_ended = asyncio.Event()

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, TextFrame) and frame.text.strip():
            self.texts.append(frame.text)
            logger.info(f"HEAD TEXT: {frame.text}")
        if isinstance(frame, LLMFullResponseEndFrame):
            self.full_response_ended.set()
        await self.push_frame(frame, direction)


async def submit_intent(params: FunctionCallParams, subtasks_json: str):
    """PoC head tool: intent schema -> agent fan-out (stub) -> ack."""
    global TOOL_ARGS
    TOOL_ARGS = json.loads(subtasks_json)
    TOOL_CALLED.set()
    goals = [s["goal"] for s in TOOL_ARGS.get("subtasks", [])]
    results = [f"[{g[:30]}] done" for g in goals]
    await params.result_callback({"ack": "agents finished", "results": results})


async def main():
    service = create_realtime_head(
        RealtimeHeadConfig(
            provider=RealtimeProvider.QWEN,
            system_instruction=(
                "你是语音助手。闲聊正常回答。用户提出多步任务时，调用 submit_intent 工具，"
                'subtasks_json 形如 {"subtasks":[{"goal":"..."}]}，然后用一句话确认结果。'
            ),
            tools=[submit_intent],
            text_only=True,
        )
    )
    logger.info(f"service={type(service).__name__} url={service.base_url}")

    printer = TextPrinter()
    worker = PipelineWorker(Pipeline([service, printer]), cancel_on_idle_timeout=False)
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)

    context = LLMContext(tools=[submit_intent])
    context.add_message({"role": "user", "content": "调用submit_intent工具，安排两个子任务：研究披萨健康度、研究奶茶健康度"})

    async def drive():
        await asyncio.sleep(1.5)
        await worker.queue_frame(LLMContextFrame(context))
        await worker.queue_frame(LLMRunFrame())
        # wait for tool call + final text
        try:
            await asyncio.wait_for(TOOL_CALLED.wait(), timeout=30)
            logger.info(f"TOOL CALLED with: {json.dumps(TOOL_ARGS, ensure_ascii=False)}")
        except asyncio.TimeoutError:
            logger.error("tool was never called")
        try:
            await asyncio.wait_for(printer.full_response_ended.wait(), timeout=15)
            await asyncio.sleep(1.0)  # drain
        except asyncio.TimeoutError:
            logger.warning("no LLMFullResponseEndFrame")
        await worker.queue_frame(EndFrame())

    await asyncio.wait_for(asyncio.gather(runner.run(), drive()), timeout=90)
    await printer.done if hasattr(printer, "done") else None

    print("\n== LIVE E2E SUMMARY ==")
    print(f"tool called: {TOOL_CALLED.is_set()}")
    print(f"tool args:   {json.dumps(TOOL_ARGS, ensure_ascii=False)}")
    print(f"head texts:  {''.join(printer.texts)!r}")


if __name__ == "__main__":
    asyncio.run(main())
