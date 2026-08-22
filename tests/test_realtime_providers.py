#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Tests for the realtime provider factory and the OpenAI/Gemini providers.

- Factory: provider -> service class mapping, unified config injection
  (system prompt, voice, text-only modality, model), api-key resolution.
- OpenAI Realtime: end-to-end against a local fake WebSocket server in the
  OpenAI dialect (nested audio config, ``response.output_audio.delta``),
  proving the factory's ``base_url`` path.
- Gemini Live: construction-level (the service connects lazily via the
  google-genai SDK, so no network is touched at construction time).
"""

import asyncio
import base64
import json
import sys
from pathlib import Path

import pytest
import websockets

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from providers import RealtimeHeadConfig, RealtimeProvider, create_realtime_head  # noqa: E402

from pipecat.frames.frames import (  # noqa: E402
    EndFrame,
    LLMContextFrame,
    TextFrame,
    TranscriptionFrame,
    TTSAudioRawFrame,
)
from pipecat.pipeline.pipeline import Pipeline  # noqa: E402
from pipecat.pipeline.worker import PipelineWorker  # noqa: E402
from pipecat.processors.aggregators.llm_context import LLMContext  # noqa: E402
from pipecat.processors.frame_processor import Frame, FrameDirection, FrameProcessor  # noqa: E402
from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService  # noqa: E402
from pipecat.services.openai.realtime.llm import OpenAIRealtimeLLMService  # noqa: E402
from pipecat.services.qwen.realtime import QwenOmniRealtimeLLMService  # noqa: E402
from pipecat.workers.runner import WorkerRunner  # noqa: E402

FAKE_PCM_CHUNK = base64.b64encode(b"\x01\x00" * 240).decode()

_EID = 0


def _evt(**kw) -> dict:
    global _EID
    _EID += 1
    return {"event_id": f"oevt_{_EID}", **kw}


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


class FakeOpenAIRealtimeServer:
    """Fake OpenAI Realtime endpoint (OpenAI dialect)."""

    def __init__(self):
        self.received: list[dict] = []
        self.session_update: dict | None = None

    async def handler(self, ws):
        await ws.send(json.dumps(_evt(type="session.created", session={"id": "sess_o"})))
        try:
            async for raw in ws:
                msg = json.loads(raw)
                self.received.append(msg)
                t = msg.get("type")
                if t == "session.update":
                    self.session_update = msg["session"]
                    await ws.send(json.dumps(_evt(type="session.updated", session=msg["session"])))
                elif t == "response.create":
                    await self._respond(ws)
        except websockets.ConnectionClosed:
            pass

    async def _respond(self, ws):
        rid = {"response_id": "resp_1", "item_id": "item_1", "output_index": 0, "content_index": 0}
        await ws.send(
            json.dumps(
                _evt(type="response.output_audio_transcript.delta", **rid, delta="Hello world")
            )
        )
        await ws.send(json.dumps(_evt(type="response.output_audio.delta", **rid, delta=FAKE_PCM_CHUNK)))
        await ws.send(
            json.dumps(
                _evt(
                    type="response.done",
                    response={
                        "id": "resp_1",
                        "object": "realtime.response",
                        "status": "completed",
                        "status_details": {},
                        "output": [],
                        "conversation_id": "conv_1",
                        "usage": {
                            "total_tokens": 3,
                            "input_tokens": 2,
                            "output_tokens": 1,
                            "input_token_details": {"cached_tokens": 0, "audio_tokens": 0},
                            "output_token_details": {"audio_tokens": 0},
                        },
                    },
                )
            )
        )


@pytest.mark.asyncio
async def test_openai_provider_end_to_end_via_factory():
    server = FakeOpenAIRealtimeServer()
    async with websockets.serve(server.handler, "127.0.0.1", 8766):
        service = create_realtime_head(
            RealtimeHeadConfig(
                provider=RealtimeProvider.OPENAI,
                api_key="sk-test",
                base_url="ws://127.0.0.1:8766/v1/realtime",
                system_instruction="Be brief.",
                voice="alloy",
            )
        )
        assert isinstance(service, OpenAIRealtimeLLMService)
        assert "?model=" in service.base_url

        collector = Collector()
        worker = PipelineWorker(Pipeline([service, collector]), cancel_on_idle_timeout=False)
        runner = WorkerRunner(handle_sigint=False)
        await runner.add_workers(worker)

        context = LLMContext()
        context.add_message({"role": "user", "content": "hi"})

        async def drive():
            await asyncio.sleep(0.5)
            await worker.queue_frame(LLMContextFrame(context))
            # Wait for the response round-trip, then let events drain.
            for _ in range(100):
                if any(m.get("type") == "response.create" for m in server.received):
                    break
                await asyncio.sleep(0.1)
            await asyncio.sleep(2.0)
            await worker.queue_frame(EndFrame())

        await asyncio.wait_for(asyncio.gather(runner.run(), drive()), timeout=15)
        await collector.done.wait()

    # OpenAI dialect keeps nested audio.output.voice (no flattening).
    assert server.session_update is not None
    assert "modalities" not in server.session_update  # untouched by any rewrite
    assert server.session_update["audio"]["output"]["voice"] == "alloy"
    assert server.session_update.get("instructions") == "Be brief."
    # Incoming OpenAI-dialect events produced frames.
    types = [type(f) for f in collector.frames]
    assert TTSAudioRawFrame in types
    assert any(getattr(f, "text", "") == "Hello world" for f in collector.frames)


def test_gemini_provider_construction():
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
    assert service._settings.model == "gemini-2.5-flash-live"
    assert service._settings.voice == "Charon"
    assert service._settings.modalities == "TEXT"


def test_qwen_provider_construction_via_factory():
    service = create_realtime_head(
        RealtimeHeadConfig(
            provider=RealtimeProvider.QWEN,
            api_key="sk-test",
            system_instruction="Speak Chinese.",
            model="qwen3.5-omni-plus-realtime",
            voice="Cherry",
            workspace_id="ws-1",
        )
    )
    assert isinstance(service, QwenOmniRealtimeLLMService)
    assert service.base_url.endswith("model=qwen3.5-omni-plus-realtime")
    sp = service._settings.session_properties
    assert sp.instructions == "Speak Chinese."
    assert sp.audio.output.voice == "Cherry"
    assert service._workspace_id == "ws-1"


def test_factory_defaults_and_key_resolution(monkeypatch):
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-env")
    service = create_realtime_head(RealtimeHeadConfig(provider=RealtimeProvider.QWEN))
    assert isinstance(service, QwenOmniRealtimeLLMService)
    assert service.api_key == "sk-env"
    assert "qwen3.5-omni-flash-realtime" in service.base_url

    monkeypatch.delenv("DASHSCOPE_API_KEY")
    with pytest.raises(ValueError, match="DASHSCOPE_API_KEY"):
        create_realtime_head(RealtimeHeadConfig(provider=RealtimeProvider.QWEN))


def test_provider_str_values():
    assert RealtimeProvider("qwen") is RealtimeProvider.QWEN
    assert {p.value for p in RealtimeProvider} == {"openai", "gemini", "qwen"}
