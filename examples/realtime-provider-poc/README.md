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
