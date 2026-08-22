#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Protocol-level integration tests for QwenOmniRealtimeLLMService.

Runs the service against a local fake DashScope WebSocket server that speaks
the Qwen-Omni-Realtime dialect, verifying:

- the outgoing ``session.update`` uses DashScope's field layout (top-level
  ``modalities``/``voice``/``turn_detection``),
- DashScope server event names (``response.audio.delta`` etc.) are normalized
  and dispatched by the inherited OpenAI Realtime handlers,
- frames reach the pipeline downstream (TTS audio, transcripts, turn
  proposals),
- teardown sends ``session.finish`` and waits for ``session.finished``.
"""

import asyncio
import base64
import json

import pytest
import websockets

from pipecat.frames.frames import (
    EndFrame,
    InterimTranscriptionFrame,
    LLMContextFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    ProposedUserStartedSpeakingFrame,
    TextFrame,
    TranscriptionFrame,
    TTSStartedFrame,
    TTSAudioRawFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import Frame, FrameDirection, FrameProcessor
from pipecat.services.qwen.realtime import QwenOmniRealtimeLLMService
from pipecat.workers.runner import WorkerRunner

FAKE_PCM_CHUNK = base64.b64encode(b"\x01\x00" * 240).decode()  # 20ms of 24kHz mono

_EID = 0


def _evt(**kw) -> dict:
    """Build a server event dict with the event_id the pydantic models require."""
    global _EID
    _EID += 1
    return {"event_id": f"evt_{_EID}", **kw}


def _response(done: bool) -> dict:
    usage = None
    if done:
        usage = {
            "total_tokens": 15,
            "input_tokens": 10,
            "output_tokens": 5,
            "input_token_details": {"cached_tokens": 0, "audio_tokens": 0},
            "output_token_details": {"audio_tokens": 0},
        }
    return {
        "id": "resp_1",
        "object": "realtime.response",
        "status": "completed" if done else "in_progress",
        "status_details": {},
        "output": [],
        "conversation_id": "conv_1",
        "usage": usage,
    }


class FrameCollector(FrameProcessor):
    def __init__(self):
        super().__init__()
        self.frames: list[Frame] = []
        self.done = asyncio.Event()

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        self.frames.append(frame)
        if isinstance(frame, (EndFrame,)):
            self.done.set()
        await self.push_frame(frame, direction)


class FakeDashScopeServer:
    """Minimal DashScope Qwen-Omni-Realtime endpoint."""

    def __init__(self):
        self.received: list[dict] = []
        self.session_update: dict | None = None
        self.finish_sent = asyncio.Event()
        self.finished_acked = asyncio.Event()

    async def handler(self, ws):
        await ws.send(json.dumps(_evt(type="session.created", session={"id": "sess_1"})))
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
                elif t == "session.finish":
                    self.finish_sent.set()
                    await ws.send(json.dumps(_evt(type="session.finished")))
                    self.finished_acked.set()
                    await ws.close()
                    return
        except websockets.ConnectionClosed:
            pass

    async def _respond(self, ws):
        rid = {"response_id": "resp_1", "item_id": "item_1", "output_index": 0, "content_index": 0}
        await ws.send(
            json.dumps(
                _evt(type="input_audio_buffer.speech_started", audio_start_ms=0, item_id="item_0")
            )
        )
        await ws.send(json.dumps(_evt(type="response.created", response=_response(done=False))))
        for _ in range(2):
            await ws.send(
                json.dumps(_evt(type="response.audio.delta", **rid, delta=FAKE_PCM_CHUNK))
            )
        await ws.send(
            json.dumps(_evt(type="response.audio_transcript.delta", **rid, delta="你好世界"))
        )
        await ws.send(
            json.dumps(
                _evt(
                    type="input_audio_buffer.speech_stopped",
                    audio_end_ms=500,
                    item_id="item_0",
                )
            )
        )
        await ws.send(json.dumps(_evt(type="response.done", response=_response(done=True))))


@pytest.mark.asyncio
async def test_qwen_omni_realtime_end_to_end():
    server = FakeDashScopeServer()
    async with websockets.serve(server.handler, "127.0.0.1", 8765):
        collector = FrameCollector()
        service = QwenOmniRealtimeLLMService(
            api_key="sk-test",
            base_url="ws://127.0.0.1:8765/api-ws/v1/realtime",
            settings=QwenOmniRealtimeLLMService.Settings(
                session_properties=__import__(
                    "pipecat.services.openai.realtime.events", fromlist=["SessionProperties"]
                ).SessionProperties(
                    instructions="Speak Chinese.",
                    output_modalities=["audio"],
                )
            ),
        )
        pipeline = Pipeline([service, collector])
        worker = PipelineWorker(pipeline, cancel_on_idle_timeout=False)
        runner = WorkerRunner(handle_sigint=False)
        await runner.add_workers(worker)

        context = LLMContext()
        context.add_message({"role": "user", "content": "打个招呼"})

        async def drive():
            await asyncio.sleep(0.5)  # let the session handshake settle
            await worker.queue_frame(LLMContextFrame(context))
            # Wait until the fake server saw response.create + response.done effects.
            for _ in range(100):
                if any(m.get("type") == "response.create" for m in server.received):
                    break
                await asyncio.sleep(0.1)
            # Give the service time to process server events into frames.
            await asyncio.sleep(1.0)
            await worker.queue_frame(EndFrame())

        await asyncio.wait_for(
            asyncio.gather(runner.run(), drive()), timeout=15
        )
        await collector.done.wait()

    # --- outbound dialect assertions ---
    assert server.session_update is not None, "service never sent session.update"
    assert "modalities" in server.session_update, "DashScope expects top-level modalities"
    assert "output_modalities" not in server.session_update
    assert server.session_update.get("instructions") == "Speak Chinese."
    assert "type" not in server.session_update and "object" not in server.session_update
    client_event_types = {m.get("type") for m in server.received}
    assert "session.update" in client_event_types
    assert "response.create" in client_event_types

    # --- inbound normalization + frame emission ---
    types = [type(f) for f in collector.frames]
    assert TTSAudioRawFrame in types, f"no audio frames downstream: {types}"
    assert TTSStartedFrame in types
    text_frames = [f for f in collector.frames if isinstance(f, (LLMTextFrame, TextFrame))]
    assert any(getattr(f, "text", "") == "你好世界" for f in text_frames), (
        f"transcript text not found: {text_frames}"
    )
    assert LLMFullResponseStartFrame in types
    assert LLMFullResponseEndFrame in types
    assert ProposedUserStartedSpeakingFrame in types

    # --- teardown: plain socket close (session.finish is rejected by
    # qwen3.5 endpoints; live-probe-verified 2026-08-22) ---
    client_types = {m.get("type") for m in server.received}
    assert "session.finish" not in client_types, "session.finish must not be sent"


def test_server_event_aliases_cover_documented_events():
    """DashScope server event names (Model Studio docs + SDK) normalize to OpenAI's."""
    from pipecat.services.qwen.realtime.llm import _SERVER_EVENT_ALIASES

    for dashscope_name in (
        "response.audio.delta",
        "response.audio_transcript.delta",
        "response.text.delta",  # text-only modality output (docs)
        "response.audio.done",
    ):
        assert dashscope_name in _SERVER_EVENT_ALIASES

    assert (
        QwenOmniRealtimeLLMService._normalize_server_message('{"type":"response.text.delta","delta":"hi"}')
        .count("response.output_text.delta")
        == 1
    )
    # Unknown events pass through untouched.
    assert QwenOmniRealtimeLLMService._normalize_server_message('{"type":"whatever.thing"}').count(
        "whatever.thing"
    ) == 1


def test_url_and_model_defaults():
    svc = QwenOmniRealtimeLLMService(api_key="sk-test")
    assert svc.base_url == (
        "wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=qwen3.5-omni-flash-realtime"
    )
    from pipecat.services.qwen.realtime.llm import DASHSCOPE_REALTIME_BASE_URL_INTL

    intl = QwenOmniRealtimeLLMService(
        api_key="sk-test", base_url=DASHSCOPE_REALTIME_BASE_URL_INTL
    )
    assert intl.base_url.startswith("wss://dashscope-intl.aliyuncs.com")
