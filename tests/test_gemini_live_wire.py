#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Wire-level test for the Gemini Live provider path.

Replaces ``google.genai.Client`` on the service with a fake whose
``aio.live.connect()`` returns a scripted ``AsyncSession``. The connection
task, config assembly, event dispatch and frame pushing all run the real
pipecat ``GeminiLiveLLMService`` code; only the Google socket is faked. This
proves the factory's settings serialize into the correct
``LiveConnectConfig`` and server messages become pipeline frames.
"""

import asyncio
import sys
from pathlib import Path

import pytest
from google.genai import types as gt

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from providers import RealtimeHeadConfig, RealtimeProvider, create_realtime_head  # noqa: E402

from pipecat.frames.frames import (  # noqa: E402
    EndFrame,
    LLMContextFrame,
    LLMFullResponseStartFrame,
    TextFrame,
)
from pipecat.pipeline.pipeline import Pipeline  # noqa: E402
from pipecat.pipeline.worker import PipelineWorker  # noqa: E402
from pipecat.processors.aggregators.llm_context import LLMContext  # noqa: E402
from pipecat.processors.frame_processor import Frame, FrameDirection, FrameProcessor  # noqa: E402
from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService  # noqa: E402
from pipecat.workers.runner import WorkerRunner  # noqa: E402


class Collector(FrameProcessor):
    def __init__(self):
        super().__init__()
        self.frames: list[Frame] = []
        self.done = asyncio.Event()

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        self.frames.append(frame)
        if isinstance(frame, EndFrame):
            self.done.set()
        await self.push_frame(frame, direction)


class FakeGeminiSession:
    """Scripted google-genai AsyncSession."""

    def __init__(self, events: list[gt.LiveServerMessage]):
        self._events = events
        self.sent: list[tuple] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def receive(self):
        async def gen():
            for e in self._events:
                yield e
            # A real session blocks waiting for the next message; mirror that
            # so the service's receive loop parks instead of busy-spinning.
            await asyncio.Event().wait()

        return gen()

    async def send_client_content(self, **kw):
        self.sent.append(("client_content", kw))

    async def send_realtime_input(self, **kw):
        self.sent.append(("realtime_input", kw))

    async def send_tool_response(self, **kw):
        self.sent.append(("tool_response", kw))

    async def send_input(self, **kw):
        self.sent.append(("input", kw))

    async def close(self):
        self.sent.append(("close", None))


class FakeGeminiLive:
    def __init__(self):
        self.captured_model = None
        self.captured_config = None
        self.session = FakeGeminiSession(events=[])

    def connect(self, *, model, config):
        """google-genai's connect() returns an async CM directly (not a coroutine)."""
        self.captured_model = model
        self.captured_config = config
        self.session = FakeGeminiSession(events=SCRIPTED_EVENTS)
        return self.session


class FakeAio:
    def __init__(self):
        self.live = FakeGeminiLive()


class FakeGeminiClient:
    def __init__(self):
        self.aio = FakeAio()


# Server script: a TEXT-modality model turn, then the turn completes.
SCRIPTED_EVENTS = [
    gt.LiveServerMessage(
        server_content=gt.LiveServerContent(
            model_turn=gt.Content(parts=[gt.Part(text="Hello from Gemini")]),
        )
    ),
    gt.LiveServerMessage(
        server_content=gt.LiveServerContent(turn_complete=True),
        usage_metadata=gt.UsageMetadata(
            prompt_token_count=5, response_token_count=3, total_token_count=8
        ),
    ),
]


@pytest.mark.asyncio
async def test_gemini_provider_wire_level():
    service = create_realtime_head(
        RealtimeHeadConfig(
            provider=RealtimeProvider.GEMINI,
            api_key="g-test",
            system_instruction="You are Gemini Live.",
            model="gemini-2.5-flash-live",
            voice="Charon",
            text_only=True,
        )
    )
    assert isinstance(service, GeminiLiveLLMService)

    fake_client = FakeGeminiClient()
    service._client = fake_client  # swap the transport before the pipeline starts

    collector = Collector()
    worker = PipelineWorker(Pipeline([service, collector]), cancel_on_idle_timeout=False)
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)

    context = LLMContext()
    context.add_message({"role": "user", "content": "hello"})

    async def drive():
        await asyncio.sleep(1.0)  # connect + session_ready + scripted events
        await worker.queue_frame(LLMContextFrame(context))
        await asyncio.sleep(1.5)
        await worker.queue_frame(EndFrame())

    await asyncio.wait_for(asyncio.gather(runner.run(), drive()), timeout=15)
    await collector.done.wait()

    # --- the factory's settings serialized into the wire config ---
    live = fake_client.aio.live
    assert live.captured_model == "gemini-2.5-flash-live"
    cfg = live.captured_config
    assert gt.Modality.TEXT in cfg.generation_config.response_modalities, (
        f"text_only must map to TEXT modality, got {cfg.generation_config.response_modalities}"
    )
    voice = (
        cfg.generation_config.speech_config.voice_config.prebuilt_voice_config.voice_name
    )
    assert voice == "Charon", f"voice not serialized: {voice}"

    # --- server messages became downstream frames (real handler code) ---
    types_seen = [type(f) for f in collector.frames]
    texts = [getattr(f, "text", "") for f in collector.frames]
    assert any(t == "Hello from Gemini" for t in texts), f"text frame missing: {texts}"
    assert LLMFullResponseStartFrame in types_seen
