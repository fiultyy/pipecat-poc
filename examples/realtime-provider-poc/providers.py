#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Thin provider-selection layer over the three realtime (live) providers.

``create_realtime_head(config)`` instantiates the pipecat realtime service for
the selected provider with a unified configuration surface:

- OpenAI Realtime  -> ``OpenAIRealtimeLLMService``
- Gemini Live      -> ``GeminiLiveLLMService``
- Qwen-Omni        -> ``QwenOmniRealtimeLLMService`` (DashScope)

All three are pipecat ``RealtimeService``-style LLM services: server-side VAD
turn proposals, streaming audio/text responses, and function calling (the
``submit_intent``-drives-agents pattern), so a pipeline built around one
provider runs against any other by changing only the config.
"""

import os
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from pipecat.services.llm_service import LLMService


class RealtimeProvider(StrEnum):
    OPENAI = "openai"
    GEMINI = "gemini"
    QWEN = "qwen"


class RealtimeProtocol(StrEnum):
    """Protocol generation selector (Codex's RealtimeEventParser pattern).

    A protocol generation is a client-side parser/adapter choice, not a
    model choice: the same provider may expose more than one wire dialect,
    and the harness picks which one to speak.

    OPENAI_RT: standard OpenAI Realtime events (nested audio config,
        ``response.output_audio.delta`` names).
    DASHSCOPE_RT: the DashScope dialect of the same protocol (flat
        session fields, short event names) — what
        QwenOmniRealtimeLLMService adapts.
    """

    OPENAI_RT = "openai-rt"
    DASHSCOPE_RT = "dashscope-rt"


_PROVIDER_PROTOCOLS = {
    RealtimeProvider.OPENAI: RealtimeProtocol.OPENAI_RT,
    RealtimeProvider.GEMINI: RealtimeProtocol.OPENAI_RT,  # SDK-managed wire
    RealtimeProvider.QWEN: RealtimeProtocol.DASHSCOPE_RT,
}

_PROVIDER_CLASSES: dict[tuple[RealtimeProvider, RealtimeProtocol], str] = {
    (RealtimeProvider.OPENAI, RealtimeProtocol.OPENAI_RT): "OpenAIRealtimeLLMService",
    (RealtimeProvider.GEMINI, RealtimeProtocol.OPENAI_RT): "GeminiLiveLLMService",
    (RealtimeProvider.QWEN, RealtimeProtocol.DASHSCOPE_RT): "QwenOmniRealtimeLLMService",
}


_PROVIDER_ENV_KEYS = {
    RealtimeProvider.OPENAI: "OPENAI_API_KEY",
    RealtimeProvider.GEMINI: "GOOGLE_API_KEY",
    RealtimeProvider.QWEN: "DASHSCOPE_API_KEY",
}


@dataclass
class RealtimeHeadConfig:
    """Unified configuration for any realtime head provider.

    Parameters:
        provider: Which provider to instantiate.
        model: Provider model name; None uses the provider default.
        api_key: API key; None reads the provider's environment variable
            (``OPENAI_API_KEY`` / ``GOOGLE_API_KEY`` / ``DASHSCOPE_API_KEY``).
        system_instruction: System prompt for the voice head.
        voice: Provider-specific voice name (e.g. "alloy" / "Charon" / "Cherry").
        tools: pipecat tools (plain async functions, ``FunctionSchema`` or
            ``DirectFunction``) advertised to the model for function calling.
        language: BCP-47 language hint (Gemini `language`, Qwen transcription).
        base_url: Endpoint override. For Qwen pass the intl endpoint
            (``DASHSCOPE_REALTIME_BASE_URL_INTL``) to target Singapore.
        workspace_id: DashScope workspace ID (X-DashScope-WorkSpace header).
        text_only: Request text-only output modality (no TTS audio).
        turn_detection: Provider-native turn-detection config object
            (OpenAI/Qwen ``events.TurnDetection``-style dict), or False to
            disable server VAD (locally-driven turns).
        extra: Provider-native overrides merged last, keyed by provider
            (e.g. ``{"openai": {"session_properties": ...}}``).
    """

    provider: RealtimeProvider
    model: str | None = None
    protocol: RealtimeProtocol | None = None  # None = provider default
    api_key: str | None = None
    system_instruction: str | None = None
    voice: str | None = None
    tools: list[Any] = field(default_factory=list)
    language: str | None = None
    base_url: str | None = None
    workspace_id: str | None = None
    text_only: bool = False
    turn_detection: Any = None
    extra: dict[str, dict] = field(default_factory=dict)

    def resolved_api_key(self) -> str:
        if self.api_key:
            return self.api_key
        key = os.environ.get(_PROVIDER_ENV_KEYS[self.provider], "")
        if not key:
            raise ValueError(
                f"No API key for {self.provider.value}: set api_key= or "
                f"{_PROVIDER_ENV_KEYS[self.provider]}"
            )
        return key


def create_realtime_head(config: RealtimeHeadConfig) -> LLMService:
    """Instantiate the pipecat realtime service for the configured provider.

    Protocol generation is resolved before dispatch: the provider's
    default applies unless ``config.protocol`` overrides it, and the
    (provider, protocol) pair must be a known combination.

    Args:
        config: Unified head configuration.

    Returns:
        A realtime LLM service instance ready to drop into a pipeline.
    """
    protocol = config.protocol or _PROVIDER_PROTOCOLS[config.provider]
    if (config.provider, protocol) not in _PROVIDER_CLASSES:
        raise ValueError(
            f"Unsupported combination: provider={config.provider.value} "
            f"protocol={protocol.value}. Known: "
            f"{[(p.value, q.value) for p, q in _PROVIDER_CLASSES]}"
        )
    match config.provider:
        case RealtimeProvider.OPENAI:
            return _create_openai(config)
        case RealtimeProvider.GEMINI:
            return _create_gemini(config)
        case RealtimeProvider.QWEN:
            return _create_qwen(config)
    raise ValueError(f"Unknown provider: {config.provider}")


def _create_openai(config: RealtimeHeadConfig) -> LLMService:
    from pipecat.services.openai.realtime import events
    from pipecat.services.openai.realtime.llm import OpenAIRealtimeLLMService

    audio_kwargs: dict[str, Any] = {}
    if config.voice:
        audio_kwargs["output"] = events.AudioOutput(voice=config.voice)
    if config.turn_detection is not None:
        td = None if config.turn_detection is False else config.turn_detection
        audio_kwargs["input"] = events.AudioInput(turn_detection=td)

    sp_kwargs: dict[str, Any] = {}
    if audio_kwargs:
        sp_kwargs["audio"] = events.AudioConfiguration(**audio_kwargs)
    if config.text_only:
        sp_kwargs["output_modalities"] = ["text"]
    if config.tools:
        # Fallback until an LLMContext arrives; context tools override these.
        sp_kwargs["tools"] = config.tools

    sp_kwargs.update(config.extra.get("openai", {}))

    settings_kwargs: dict[str, Any] = {"session_properties": events.SessionProperties(**sp_kwargs)}
    if config.system_instruction:
        settings_kwargs["system_instruction"] = config.system_instruction
    if config.model:
        settings_kwargs["model"] = config.model

    return OpenAIRealtimeLLMService(
        api_key=config.resolved_api_key(),
        **({"base_url": config.base_url} if config.base_url else {}),
        settings=OpenAIRealtimeLLMService.Settings(**settings_kwargs),
    )


def _create_gemini(config: RealtimeHeadConfig) -> LLMService:
    from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService

    settings_kwargs: dict[str, Any] = {}
    if config.system_instruction:
        settings_kwargs["system_instruction"] = config.system_instruction
    if config.model:
        settings_kwargs["model"] = config.model
    if config.voice:
        settings_kwargs["voice"] = config.voice
    if config.language:
        settings_kwargs["language"] = config.language
    if config.text_only:
        from pipecat.services.google.gemini_live.llm import GeminiModalities

        settings_kwargs["modalities"] = GeminiModalities.TEXT

    settings_kwargs.update(config.extra.get("gemini", {}))

    kwargs: dict[str, Any] = {
        "api_key": config.resolved_api_key(),
        "settings": GeminiLiveLLMService.Settings(**settings_kwargs),
    }
    if config.tools:
        kwargs["tools"] = config.tools
    return GeminiLiveLLMService(**kwargs)


def _create_qwen(config: RealtimeHeadConfig) -> LLMService:
    from pipecat.services.openai.realtime import events
    from pipecat.services.qwen.realtime import QwenOmniRealtimeLLMService
    from pipecat.services.qwen.realtime.llm import DASHSCOPE_REALTIME_BASE_URL_CN

    sp_kwargs: dict[str, Any] = {}
    audio_kwargs: dict[str, Any] = {}
    if config.voice:
        audio_kwargs["output"] = events.AudioOutput(voice=config.voice)
    if config.turn_detection is not None:
        td = None if config.turn_detection is False else config.turn_detection
        audio_kwargs["input"] = events.AudioInput(turn_detection=td)
    if audio_kwargs:
        sp_kwargs["audio"] = events.AudioConfiguration(**audio_kwargs)
    if config.text_only:
        sp_kwargs["output_modalities"] = ["text"]
    if config.tools:
        # Fallback until an LLMContext arrives; context tools override these.
        sp_kwargs["tools"] = config.tools

    sp_kwargs.update(config.extra.get("qwen", {}))

    settings_kwargs: dict[str, Any] = {"session_properties": events.SessionProperties(**sp_kwargs)}
    if config.system_instruction:
        settings_kwargs["system_instruction"] = config.system_instruction
    if config.model:
        settings_kwargs["model"] = config.model

    kwargs: dict[str, Any] = {
        "api_key": config.resolved_api_key(),
        "base_url": config.base_url or DASHSCOPE_REALTIME_BASE_URL_CN,
        "settings": QwenOmniRealtimeLLMService.Settings(**settings_kwargs),
    }
    if config.workspace_id:
        kwargs["workspace_id"] = config.workspace_id
    return QwenOmniRealtimeLLMService(**kwargs)
