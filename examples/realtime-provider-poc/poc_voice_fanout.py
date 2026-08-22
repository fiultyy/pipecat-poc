#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Full voice PoC: mp3/wav audio -> Qwen-Omni head -> GLM-5.3 agent fan-out.

Audio in (any libsndfile-readable file: mp3/wav/flac...) is resampled to
16k mono PCM16, streamed into the pipeline as InputAudioRawFrames; DashScope
server VAD segments the speech, the head hears the request, calls
submit_intent, two GLM-5.3 agents run in parallel, results flow back, and the
head's spoken reply (audio + transcript) is captured.
"""

import asyncio
import base64
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import soundfile as sf
from dotenv import load_dotenv
from loguru import logger
from openai import AsyncOpenAI

sys.path.insert(0, str(Path(__file__).parent))

from providers import RealtimeHeadConfig, RealtimeProvider, create_realtime_head

from pipecat.frames.frames import (
    EndFrame,
    InputAudioRawFrame,
    InterimTranscriptionFrame,
    TranscriptionFrame,
)
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

T0 = time.monotonic()
EVENTS: list[str] = []
FANOUT_RESULTS: list[dict] = []


def ts() -> str:
    return f"{time.monotonic() - T0:6.2f}s"


def load_audio_frames(path: str, target_sr: int = 16000, realtime: bool = True):
    """Decode any libsndfile format -> list of InputAudioRawFrame (100ms)."""
    data, sr = sf.read(path, dtype="float32", always_2d=False)
    if data.ndim > 1:
        data = data.mean(axis=1)
    if sr != target_sr:
        n_out = int(len(data) * target_sr / sr)
        idx = np.arange(n_out) * sr / target_sr
        i0 = np.minimum(idx.astype(int), len(data) - 2)
        frac = idx - i0
        data = data[i0] * (1 - frac) + data[i0 + 1] * frac
    # pad: 300ms head silence (VAD prefix), 1000ms tail (VAD stop)
    data = np.concatenate(
        [np.zeros(int(0.3 * target_sr)), data, np.zeros(int(1.0 * target_sr))]
    ).astype("float32")
    pcm = (np.clip(data, -1, 1) * 32767).astype("<i2").tobytes()
    chunk = int(target_sr * 2 * 0.1)
    return [
        InputAudioRawFrame(audio=pcm[i : i + chunk], sample_rate=target_sr, num_channels=1)
        for i in range(0, len(pcm), chunk)
    ], len(data) / target_sr


class Monitor(FrameProcessor):
    """Captures transcripts, head text, and bot audio."""

    def __init__(self):
        super().__init__()
        self.user_asr: list[str] = []
        self.head_text: list[str] = []
        self.audio_bytes = 0
        self.finished = asyncio.Event()

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, TranscriptionFrame) and frame.text.strip():
            self.user_asr.append(frame.text)
            EVENTS.append(f"{ts()} ASR FINAL: {frame.text!r}")
        elif isinstance(frame, InterimTranscriptionFrame) and frame.text.strip():
            EVENTS.append(f"{ts()} ASR interim: {frame.text!r}")
        else:
            text = getattr(frame, "text", None)
            if text and text.strip():
                self.head_text.append(text)
        if type(frame).__name__ == "TTSAudioRawFrame":
            self.audio_bytes += len(frame.audio)
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
    EVENTS.append(f"{ts()} agent[{name}] done {time.monotonic()-t:.2f}s")
    return data


async def submit_intent(params: FunctionCallParams, subtasks_json: str):
    EVENTS.append(f"{ts()} submit_intent: {subtasks_json[:100]}")
    intent = json.loads(subtasks_json)
    goals = [s["goal"] for s in intent.get("subtasks", [])]
    results = await asyncio.gather(
        *(glm_agent(f"agent-{i+1}", g) for i, g in enumerate(goals))
    )
    FANOUT_RESULTS.extend(results)
    EVENTS.append(f"{ts()} fan-in done -> ack")
    await params.result_callback({"status": "done", "results": [r["finding"] for r in results]})


async def main():
    audio_path = sys.argv[1] if len(sys.argv) > 1 else "~/桌面/Recording 4.mp3"
    frames, dur = load_audio_frames(audio_path)
    logger.info(f"audio {audio_path}: {dur:.1f}s -> {len(frames)} frames(100ms) @16k")

    head = create_realtime_head(
        RealtimeHeadConfig(
            provider=RealtimeProvider.QWEN,
            system_instruction=(
                "你是语音编排助手。听到调研类请求必须调用 submit_intent 工具"
                '（subtasks_json 形如 {"subtasks":[{"goal":"..."},...]}，拆2个子任务），'
                "拿到结果后用一两句话汇总。闲聊则直接回答。"
            ),
            tools=[submit_intent],
            text_only=True,  # 语音进、文本出：省掉音频输出 token（300元/M）
        )
    )

    context = LLMContext(tools=[submit_intent])
    aggregators = LLMContextAggregatorPair(context)
    monitor = Monitor()
    worker = PipelineWorker(
        Pipeline([aggregators.user(), head, monitor, aggregators.assistant()]),
        cancel_on_idle_timeout=False,
    )
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)

    async def drive():
        await asyncio.sleep(2.0)
        EVENTS.append(f"{ts()} streaming {len(frames)} audio frames")
        for f in frames:
            await worker.queue_frame(f)
            await asyncio.sleep(0.1)  # real-time cadence for server VAD
        # wait for the full conversation to settle
        for _ in range(240):
            if FANOUT_RESULTS and monitor.head_text and len(monitor.head_text) > 3:
                await asyncio.sleep(3.0)
                break
            await asyncio.sleep(1.0)
        await worker.queue_frame(EndFrame())

    await asyncio.wait_for(asyncio.gather(runner.run(), drive()), timeout=300)

    print("\n== TIMELINE ==")
    for e in EVENTS:
        print(" ", e)
    print("\n== RESULTS ==")
    print("  用户语音(ASR):", "".join(monitor.user_asr))
    for r in FANOUT_RESULTS:
        print("  agent:", json.dumps(r, ensure_ascii=False)[:150])
    print("  head 回复文本:", "".join(monitor.head_text))
    print(f"  head 回复语音: {monitor.audio_bytes/48000:.1f}s PCM16@24k")


if __name__ == "__main__":
    asyncio.run(main())
