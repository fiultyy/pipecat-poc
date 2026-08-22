#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Runnable demo: realtime head with provider selection + tool-driven agents.

Pick the provider with ``REALTIME_PROVIDER`` (``openai`` | ``gemini`` |
``qwen``) and supply the matching key (``OPENAI_API_KEY`` /
``GOOGLE_API_KEY`` / ``DASHSCOPE_API_KEY``). Speech comes from the local
microphone when PyAudio is available; otherwise push a WAV file path via
``--file`` (16kHz mono PCM16).

The demo registers a ``submit_intent`` tool on the head. When the model calls
it, the handler fans work out to stub agents (print + sleep, standing in for
LM Studio / GLM-5.3 workers) and returns an ack the head speaks back as text.

No TTS output transport is wired: the head's text output is printed, which is
the PoC's "text-out" goal.
"""

import argparse
import asyncio
import json
import os
import sys
import wave

from dotenv import load_dotenv
from loguru import logger

from pipecat.frames.frames import InputAudioRawFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.processors.frame_processor import Frame, FrameDirection, FrameProcessor
from pipecat.services.llm_service import FunctionCallParams
from pipecat.workers.runner import WorkerRunner

sys.path.insert(0, os.path.dirname(__file__))

from providers import RealtimeHeadConfig, RealtimeProvider, create_realtime_head

AGENTS = ["researcher", "writer"]


class TextPrinter(FrameProcessor):
    """Prints text frames; the PoC's text-out sink (no TTS)."""

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        text = getattr(frame, "text", None)
        if text:
            logger.info(f"HEAD TEXT: {text}")
        await self.push_frame(frame, direction)


async def run_stub_agent(name: str, goal: str) -> str:
    """Stub for an LM Studio / GLM-5.3 worker. TODO: replace with job_group fan-out."""
    await asyncio.sleep(0.2)
    return f"[{name}] done: {goal[:80]}"


async def submit_intent(params: FunctionCallParams, subtasks_json: str):
    """Head tool: parse the intent schema and fan out to agents.

    Args:
        subtasks_json: JSON array of ``{"goal": "<prompt>"}`` objects.
    """
    try:
        subtasks = json.loads(subtasks_json)
        goals = [s["goal"] for s in subtasks]
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        await params.result_callback({"error": f"bad subtasks payload: {e}"})
        return
    results = await asyncio.gather(
        *(run_stub_agent(AGENTS[i % len(AGENTS)], g) for i, g in enumerate(goals))
    )
    logger.info(f"AGENT RESULTS: {results}")
    await params.result_callback({"ack": "agents finished", "results": list(results)})


def wav_to_input_frames(path: str, chunk_ms: int = 20):
    with wave.open(path, "rb") as w:
        rate, ch, sw = w.getframerate(), w.getnchannels(), w.getsampwidth()
        if sw != 2 or ch != 1:
            raise SystemExit(f"need 16-bit mono PCM wav, got {ch}ch/{sw * 8}bit")
        chunk = int(rate * ch * sw * chunk_ms / 1000)
        while True:
            data = w.readframes(chunk // sw)
            if not data:
                break
            yield InputAudioRawFrame(audio=data, sample_rate=rate, num_channels=ch)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="16kHz mono PCM16 wav to inject instead of the mic")
    args = parser.parse_args()

    load_dotenv()

    provider = RealtimeProvider(os.environ.get("REALTIME_PROVIDER", "qwen"))
    config = RealtimeHeadConfig(
        provider=provider,
        system_instruction=(
            "You are a voice assistant. Chat naturally. When the user asks for "
            'something multi-step, call submit_intent with subtasks as a JSON '
            'array like [{"goal": "..."}], then briefly confirm the results.'
        ),
        tools=[submit_intent],
        voice=os.environ.get("REALTIME_VOICE"),
        model=os.environ.get("REALTIME_MODEL"),
        base_url=os.environ.get("REALTIME_BASE_URL"),
    )
    service = create_realtime_head(config)
    logger.info(f"head = {type(service).__name__} @ {getattr(service, 'base_url', 'sdk')}")

    context = LLMContext()
    aggregators = LLMContextAggregatorPair(context)

    # Realtime services propose turns from server-side VAD; the recommended
    # external strategies come from the service metadata frame automatically.
    # Local Silero VAD is only needed for file injection (no server VAD sees
    # locally queued frames until they reach the service).
    printer = TextPrinter()

    pipeline = Pipeline(
        [
            aggregators.user(),
            service,
            printer,
            aggregators.assistant(),
        ]
    )
    worker = PipelineWorker(pipeline, cancel_on_idle_timeout=False)
    runner = WorkerRunner(handle_sigint=True)
    await runner.add_workers(worker)

    async def drive():
        await asyncio.sleep(1.0)
        if args.file:
            logger.info(f"injecting {args.file}")
            for frame in wav_to_input_frames(args.file):
                await worker.queue_frame(frame)
                await asyncio.sleep(0.02)  # ~real-time cadence
        else:
            logger.info("no --file given; connect a mic transport or queue frames yourself")

    await asyncio.gather(runner.run(), drive())


if __name__ == "__main__":
    asyncio.run(main())
