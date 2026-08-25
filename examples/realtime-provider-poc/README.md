# Realtime Provider PoC

Three live (realtime) voice providers behind one thin selection layer:

| Provider | Service | Endpoint |
|---|---|---|
| OpenAI Realtime | `OpenAIRealtimeLLMService` (pipecat) | `wss://api.openai.com/v1/realtime` |
| Gemini Live | `GeminiLiveLLMService` (pipecat, google-genai SDK) | Google (SDK-managed) |
| Qwen-Omni Realtime | `QwenOmniRealtimeLLMService` (**new**, this PoC) | `wss://dashscope.aliyuncs.com/api-ws/v1/realtime` (CN) / `dashscope-intl...` (SG) |

## Layout

- `providers.py` — thin factory: `create_realtime_head(RealtimeHeadConfig)` →
  the pipecat realtime service for the selected provider, with a unified
  config surface (system prompt, voice, model, tools, text-only modality,
  endpoint override, DashScope workspace).
- `bot.py` — runnable demo: provider chosen via `REALTIME_PROVIDER`, a
  `submit_intent` tool that fans out to stub agents, text output printed
  (no TTS — the PoC's text-out goal).
- `../../tests/test_qwen_omni_realtime.py` — protocol-level test of the Qwen
  service against a fake DashScope WS server (dialect assertions both ways).
- `../../tests/test_realtime_providers.py` — factory tests for all three
  providers + OpenAI end-to-end against a fake OpenAI WS server.

## The Qwen service

`pipecat.services.qwen.realtime.QwenOmniRealtimeLLMService` extends pipecat's
`OpenAIRealtimeLLMService`: the DashScope Qwen-Omni-Realtime WebSocket API is
an OpenAI Realtime dialect, so only the differences are adapted (verified
against the `dashscope` SDK 1.27.0, `dashscope/audio/qwen_omni/omni_realtime.py`):

- URL & auth: `wss://dashscope[|-intl].aliyuncs.com/api-ws/v1/realtime?model=...`,
  `Authorization: Bearer <DASHSCOPE_API_KEY>`, optional `X-DashScope-WorkSpace`.
- Outgoing `session.update`: `modalities`/`voice`/`turn_detection`/
  `input_audio_transcription` move to the top level of the session object.
- Incoming events: `response.audio.delta` → `response.output_audio.delta`
  (and transcript/audio `.done`), plus `session.finished`.
- Teardown sends `session.finish` before closing the socket.

Not yet verified against the real DashScope cloud (no API key in this
environment); the fake-server tests cover the protocol shape from the SDK
source. First live run checklist:

1. `DASHSCOPE_API_KEY=... REALTIME_PROVIDER=qwen python bot.py --file x.wav`
2. Watch for `session.updated` errors — field mismatches surface there.
3. If the intl site is wanted: `REALTIME_BASE_URL=wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime`.

## Usage

```bash
# tests (no keys needed)
.venv/bin/python -m pytest tests/test_qwen_omni_realtime.py tests/test_realtime_providers.py

# live demo (needs the provider's key)
REALTIME_PROVIDER=qwen DASHSCOPE_API_KEY=sk-... python bot.py --file sample.wav
REALTIME_PROVIDER=openai OPENAI_API_KEY=sk-... python bot.py
REALTIME_PROVIDER=gemini GOOGLE_API_KEY=... python bot.py
```

Factory:

```python
from providers import RealtimeHeadConfig, RealtimeProvider, create_realtime_head

service = create_realtime_head(RealtimeHeadConfig(
    provider=RealtimeProvider.QWEN,
    system_instruction="You are a helpful voice assistant.",
    tools=[submit_intent],          # head tool → fan-out to agents
    text_only=False,
))
```

## rt_voice_app.py · 超轻桌面语音客户端（无浏览器引擎）

stdlib tkinter 画布 + sounddevice 原生采集 + numpy FFT + aiohttp WS，替代
`web/index.html` 的浏览器形态，直连 rt-gateway（帧协议 §1 逐字对齐）：

```bash
.venv/bin/pip install sounddevice            # 唯一新增依赖（PortAudio 绑定）
.venv/bin/python examples/realtime-provider-poc/rt_voice_app.py \
    --url ws://127.0.0.1:8765/ws --token "$VOICE_GATEWAY_TOKEN"
```

- 频谱显示：24 根对数分频柱（rfft，30fps）+ RMS 电平条（dBFS 标尺）
- 输入开关：**静音语义**——开关只停采集与上行，WS 会话保持；断线 2s 自动
  重连（gateway resume 槽续接 transcript）
- 事件面板：orch.dispatch/progress/done、auth/session 回执；下行二进制音频
  直送扬声器
- 自检：`--selftest` 无设备起退；gateway 侧回环验证用
  `rt_gateway.py --echo --token t-demo`（mic→网关→扬声器）
