#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for the protocol-generation selector in the provider factory."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from providers import (  # noqa: E402
    RealtimeHeadConfig,
    RealtimeProtocol,
    RealtimeProvider,
    _PROVIDER_CLASSES,
    _PROVIDER_PROTOCOLS,
    create_realtime_head,
)


def test_every_provider_has_default_protocol():
    for provider in RealtimeProvider:
        assert provider in _PROVIDER_PROTOCOLS
        assert _PROVIDER_PROTOCOLS[provider] in RealtimeProtocol


def test_protocol_enum_values():
    assert RealtimeProtocol.OPENAI_RT.value == "openai-rt"
    assert RealtimeProtocol.DASHSCOPE_RT.value == "dashscope-rt"


def test_unknown_combination_rejected():
    with pytest.raises(ValueError, match="Unsupported combination"):
        create_realtime_head(
            RealtimeHeadConfig(
                provider=RealtimeProvider.GEMINI,
                protocol=RealtimeProtocol.DASHSCOPE_RT,  # gemini can't speak dashscope
                api_key="g",
            )
        )


def test_known_combinations_table():
    assert _PROVIDER_CLASSES[
        (RealtimeProvider.QWEN, RealtimeProtocol.DASHSCOPE_RT)
    ] == "QwenOmniRealtimeLLMService"
    assert _PROVIDER_CLASSES[
        (RealtimeProvider.OPENAI, RealtimeProtocol.OPENAI_RT)
    ] == "OpenAIRealtimeLLMService"


def test_qwen_explicit_protocol_still_builds(monkeypatch):
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-x")
    svc = create_realtime_head(
        RealtimeHeadConfig(
            provider=RealtimeProvider.QWEN,
            protocol=RealtimeProtocol.DASHSCOPE_RT,  # explicit == default
        )
    )
    assert type(svc).__name__ == "QwenOmniRealtimeLLMService"


def test_tools_honored_for_every_provider(monkeypatch):
    """The unified config surface advertises tools on all providers."""

    async def a_tool(params):
        """demo"""

    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-x")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-x")
    monkeypatch.setenv("GOOGLE_API_KEY", "sk-x")
    for provider in RealtimeProvider:
        svc = create_realtime_head(RealtimeHeadConfig(provider=provider, tools=[a_tool]))
        if provider is RealtimeProvider.GEMINI:
            tools = svc._tools_from_init
        else:
            tools = svc._settings.session_properties.tools
        assert tools, f"{provider.value}: config.tools silently dropped"
