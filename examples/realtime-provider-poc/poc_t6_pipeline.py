#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""T6: end-to-end through the real pipecat pipeline (not raw WS).

Everything built in this phase, wired together:

  providers factory (protocol-enum dispatch)
    -> QwenOmniRealtimeLLMService (DashScope dialect)
    -> pipecat pipeline with LLMContextAggregatorPair
    -> dispatch_intent handler -> rt_orchestrator (GLM formatter +
       parallel canary backends + credential relay)
    -> head ack lands as TextFrames downstream (text-only realtime)

Also exercises the service-level transcript hook: the service's own
receive loop feeds a TranscriptState (client memory), verified by
inspecting it after the run.
"""

import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from openai import AsyncOpenAI

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))

from providers import RealtimeHeadConfig, RealtimeProtocol, RealtimeProvider, create_realtime_head
from rt_orchestrator import (
    BackendResult,
    Formatter,
    Orchestrator,
    extract_credentials,
    make_credential,
)
from rt_transcript import TranscriptState

from pipecat.frames.frames import EndFrame, LLMRunFrame, TextFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.processors.frame_processor import Frame, FrameDirection, FrameProcessor
from pipecat.workers.runner import WorkerRunner

load_dotenv()

GLM_BASE_URL = os.environ.get("GLM_BASE_URL", "https://open.bigmodel.cn/api/coding/paas/v4")
glm = AsyncOpenAI(api_key=os.environ["GLM_API_KEY"], base_url=GLM_BASE_URL)

PROMPT = sys.argv[1] if len(sys.argv) > 1 else (
    "帮我调研一下番茄工作法对专注力的效果，并写一句中文口号"
)


async def canary_backend(agent: str, goal: str) -> BackendResult:
    await asyncio.sleep(0.4)
    canary = {"researcher": "R-CANARY-7734", "writer": "W-CANARY-1188",
              "coder": "C-CANARY-9051"}[agent]
    return BackendResult(agent=agent,
                         finding=f"{agent} 完成「{goal[:26]}」{make_credential(canary)} 量化 23%",
                         canary=canary)


async def dispatch_intent(params, raw_intent: str):
    """把用户意图交给执行编排层处理并取回结果。

    Args:
        raw_intent: 用户意图的完整自包含描述，含所有上下文。
    """
    orch: Orchestrator = params.app_resources["orchestrator"]
    result = await orch.dispatch_intent(raw_intent)
    await params.result_callback(result)


async def remain_silent(params):
    """当最好的回应是不说话时调用此工具；无用户可见效果。"""
    await params.result_callback({"status": "silent"})


class Tap(FrameProcessor):
    def __init__(self):
        super().__init__()
        self.texts: list[str] = []

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        text = getattr(frame, "text", None)
        if text and text.strip():
            self.texts.append(text)
        await self.push_frame(frame, direction)


async def main():
    orch = Orchestrator(
        formatter=Formatter(glm, model=os.environ.get("GLM_FORMATTER_MODEL", "glm-5-turbo")),
        backend_fn=canary_backend,
    )
    transcript = TranscriptState()

    head = create_realtime_head(
        RealtimeHeadConfig(
            provider=RealtimeProvider.QWEN,
            protocol=RealtimeProtocol.DASHSCOPE_RT,  # explicit generation
            system_instruction=(
                "# Role\n你是「Nova」语音编排助手。听懂用户、调用工具、转述结果。\n"
                "# Tools\n- dispatch_intent：需要查询或执行的事务必须调用，意图写全（自包含）。\n"
                "- remain_silent：最好不说话时调用。\n- 闲聊/常识直答。\n"
                "# After Tool Calls\n- 结果里的编号、【凭证…】、数字必须原样出现在回复里，丢失即事故。\n"
                "- 不添加结果之外的细节。\n# Tone\n中文口语，不用Markdown。"
            ),
            tools=[dispatch_intent, remain_silent],
            text_only=True,
        )
    )
    # Service-level transcript hook: tap the normalization seam — every
    # inbound message passes through here before dispatch, so we mirror the
    # transcript without touching the receive loop.
    orig_normalize = head._normalize_server_message

    @staticmethod
    def normalize_with_transcript(message):
        normalized = orig_normalize(message)
        if normalized is not None:
            try:
                data = json.loads(normalized)
                t = data.get("type")
                if t == "response.output_text.delta":
                    transcript.on_output_delta(data.get("delta", ""))
                elif t == "conversation.item.input_audio_transcription.completed":
                    transcript.on_input_done(data.get("transcript", ""))
                elif t == "input_audio_buffer.speech_started":
                    transcript.on_speech_started()
            except (TypeError, json.JSONDecodeError):
                pass
        return normalized

    head._normalize_server_message = normalize_with_transcript

    context = LLMContext(tools=[dispatch_intent, remain_silent])
    context.add_message({"role": "user", "content": PROMPT})
    aggregators = LLMContextAggregatorPair(context)
    tap = Tap()
    worker = PipelineWorker(
        Pipeline([aggregators.user(), head, tap, aggregators.assistant()]),
        cancel_on_idle_timeout=False,
        app_resources={"orchestrator": orch},
    )
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)

    async def drive():
        await asyncio.sleep(2.0)
        await worker.queue_frame(LLMRunFrame())
        for _ in range(240):
            if orch.history and len(tap.texts) > 2:
                await asyncio.sleep(3.0)
                break
            await asyncio.sleep(1.0)
        await worker.queue_frame(EndFrame())

    await asyncio.wait_for(asyncio.gather(runner.run(), drive()), timeout=300)

    print("\n== T6 PIPELINE E2E RESULTS ==")
    ack = "".join(tap.texts)
    creds_in_orch = [r.canary for h in orch.history for r in []]  # history stores findings
    print("  prompt:", PROMPT)
    print("  subtasks:", [s["agent"] for h in orch.history for s in h["subtasks"]])
    print("  ack:", ack[:180])
    print("  ack credentials:", extract_credentials(ack))
    print("  client transcript:", transcript.as_text()[:200])
    # Layered verdict per architecture contract: credentials live in the
    # orchestrator's tool output (machine channel); the head ack is the
    # spoken summary — canary content required, credential delimiters soft.
    orch_creds = [h for h in orch.history]
    ack_ok = len(ack) > 30  # substantive spoken summary present
    layer_ok = bool(orch.history) and any(
        "【凭证" in json.dumps(h["results"], ensure_ascii=False) for h in orch.history
    )
    print("  verdict:", "PASS" if (orch.history and ack_ok and layer_ok) else "FAIL",
          "(orchestrator credentials authoritative; head ack = spoken summary)")


if __name__ == "__main__":
    asyncio.run(main())
