#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Tool-result positioning on the Qwen-Omni realtime service.

Realtime tool results must reach the model through the native
``function_call_output`` channel — never packed into the seeded user
message, where the payload reads as user text and the model relays it
aloud (whiteboard bodies, full briefs) instead of applying the doctrine.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipecat.processors.aggregators.llm_context import LLMContext  # noqa: E402
from pipecat.services.qwen.realtime.llm import _ToolPairFreeContextView  # noqa: E402


def _ctx(messages):
    ctx = LLMContext()
    ctx.set_messages(list(messages))
    return ctx


def test_view_hides_tool_pairs_keeps_text_messages():
    """tool 结果/assistant 工具调用对被隐藏；user/assistant 纯文本原样保留。"""
    ctx = _ctx([
        {"role": "assistant", "tool_calls": [
            {"id": "call_1", "type": "function",
             "function": {"name": "read_whiteboard_tool", "arguments": "{}"}}]},
        {"role": "tool", "content": '{"status":"ok","body":"全文"}', "tool_call_id": "call_1"},
        {"role": "user", "content": "Agent Final Message: 结论"},
        {"role": "assistant", "content": "已受理"},
    ])
    view = _ToolPairFreeContextView(ctx)
    kept = view.get_messages()
    assert kept == [
        {"role": "user", "content": "Agent Final Message: 结论"},
        {"role": "assistant", "content": "已受理"},
    ]


def test_view_forwards_tools_and_all_messages_passthrough():
    """tools 属性透传；无 tool 对时视图消息与原上下文一致。"""
    ctx = _ctx([
        {"role": "user", "content": "问题"},
        {"role": "assistant", "content": "回答"},
    ])
    view = _ToolPairFreeContextView(ctx)
    assert view.tools is ctx.tools
    assert view.get_messages() == ctx.get_messages()


def test_view_hides_async_tool_envelopes():
    """async 工具信封（带 tool_call_id 的非标准消息）同样隐藏。"""
    ctx = _ctx([
        {"role": "tool", "tool_call_id": "call_async", "kind": "final",
         "content": '{"result":"x"}'},
        {"role": "user", "content": "追问题"},
    ])
    assert _ToolPairFreeContextView(ctx).get_messages() == [
        {"role": "user", "content": "追问题"}]


@pytest.mark.asyncio
async def test_first_context_sends_tool_result_natively(monkeypatch):
    """首个上下文帧（=首个工具回合）必须走原生 function_call_output 通道，
    而不是依赖 packed 重注入（user 位）携带结果。"""
    from pipecat.services.qwen.realtime.llm import QwenOmniRealtimeLLMService

    svc = QwenOmniRealtimeLLMService.__new__(QwenOmniRealtimeLLMService)
    calls = []

    async def _fake_process(send_new_results):
        calls.append(("process", send_new_results))
        # 模拟父类行为：有新结果被发送时会自行触发响应
        return None

    async def _fake_create():
        calls.append(("create",))

    monkeypatch.setattr(svc, "_process_completed_function_calls", _fake_process)
    monkeypatch.setattr(svc, "_create_response", _fake_create)

    # 首帧带 tool 结果对 → 原生发送（True），不再额外 create（process 已触发）
    svc._context = None
    ctx = _ctx([
        {"role": "assistant", "tool_calls": [
            {"id": "c1", "type": "function",
             "function": {"name": "t", "arguments": "{}"}}]},
        {"role": "tool", "content": "结果", "tool_call_id": "c1"},
    ])
    await svc._handle_context(ctx)
    assert calls == [("process", True)]

    # 首帧无 tool 结果（如网关注入终稿）→ 原生发送（无果可发）+ 显式 create
    calls.clear()
    svc._context = None
    ctx2 = _ctx([{"role": "user", "content": "Agent Final Message: 终稿"}])
    await svc._handle_context(ctx2)
    assert calls == [("process", True), ("create",)]


@pytest.mark.asyncio
async def test_seeding_uses_tool_pair_free_view(monkeypatch):
    """setup 重注入经 _ToolPairFreeContextView：tool 对不再进 packed user 项。"""
    from pipecat.services.qwen.realtime.llm import QwenOmniRealtimeLLMService

    svc = QwenOmniRealtimeLLMService.__new__(QwenOmniRealtimeLLMService)
    svc._api_session_ready = True
    svc._name = "qwen-test"  # logger f-string formats the service repr
    svc._context = _ctx([
        {"role": "assistant", "tool_calls": [
            {"id": "c1", "type": "function",
             "function": {"name": "t", "arguments": "{}"}}]},
        {"role": "tool", "content": '{"body":"全文"}', "tool_call_id": "c1"},
    ])
    svc._llm_needs_conversation_setup = True
    svc._messages_added_manually = {}
    svc._run_llm_when_api_session_ready = False

    sent = []

    class _Evt:
        def __init__(self, item):
            self.item = item

    class _Adapter:
        def get_messages_for_logging(self, context):
            return context.get_messages()

        def get_llm_invocation_params(self, context):
            from pipecat.adapters.services.open_ai_realtime_adapter import (
                OpenAIRealtimeLLMAdapter,
            )
            return OpenAIRealtimeLLMAdapter().get_llm_invocation_params(context)

    monkeypatch.setattr(svc, "get_llm_adapter", lambda: _Adapter())

    async def _send_update():
        pass

    async def _send_client_event(evt):
        sent.append(evt)

    async def _push_frame(*a, **k):
        pass

    async def _noop(*a, **k):
        pass

    monkeypatch.setattr(svc, "_send_session_update", _send_update)
    monkeypatch.setattr(svc, "send_client_event", _send_client_event)
    monkeypatch.setattr(svc, "push_frame", _push_frame)
    monkeypatch.setattr(svc, "start_processing_metrics", _noop)
    monkeypatch.setattr(svc, "start_ttfb_metrics", _noop)
    monkeypatch.setattr(svc, "_get_enabled_modalities", lambda: ["text"])

    # response.create 事件也要被吞掉（send_client_event 已记录）
    await svc._create_response()

    # 唯一被 seed 的内容不含 tool 载荷；response.create 照发
    bodies = [getattr(e, "item", None) for e in sent]
    assert svc._llm_needs_conversation_setup is False
    assert not any(getattr(b, "type", None) == "function_call_output" for b in bodies)
    # 空过滤后无消息可 seed：只剩 response.create 一件出站事件
    assert len(sent) == 1
