#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Qwen-Omni-Realtime LLM service for the DashScope Realtime API.

The DashScope Qwen-Omni-Realtime WebSocket API speaks an OpenAI Realtime
dialect: most client/server event names and payloads are identical
(``session.update``, ``input_audio_buffer.append``, ``response.create``,
``response.done``, ``conversation.item.*``, function calling, ...). This
service therefore extends :class:`OpenAIRealtimeLLMService` and only adapts
the dialect differences, verified against the ``dashscope`` SDK
(``dashscope/audio/qwen_omni/omni_realtime.py``) and the Alibaba Cloud
Model Studio "Realtime API" reference:

- Endpoint: ``wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=...``
  (China/Beijing) or ``wss://dashscope-intl.aliyuncs.com/api-ws/v1/realtime``
  (Singapore). Auth: ``Authorization: Bearer <DASHSCOPE_API_KEY>``, optional
  ``X-DashScope-WorkSpace`` header.
- Outgoing ``session.update``: modalities/voice/turn_detection/input_audio_
  transcription live at the top level of the session object (OpenAI nests
  them under ``audio.output`` / ``audio.input``).
- Incoming events: ``response.audio.delta`` / ``response.audio_transcript.
  delta`` (OpenAI: ``response.output_audio.*``); ``session.finished`` is
  emitted after a graceful ``session.finish``.
- Graceful teardown sends ``session.finish`` before closing the socket.
"""

from pipecat.services.qwen.realtime.llm import QwenOmniRealtimeLLMService

__all__ = ["QwenOmniRealtimeLLMService"]
