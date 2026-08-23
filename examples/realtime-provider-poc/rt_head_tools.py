#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Head tool surface over DshBackend (WS1 W1.4; docs/kg/01-ws1-head-dsh.md §5).

Four tools, docstring-as-schema (same convention as rt_orchestrator):

- ``dispatch_intent(raw_intent)`` — phase-1 receipt now; the phase-2
  final arrives later as a context re-injection carrying the
  ``"Agent Final Message":`` prefix.
- ``query_status()`` — spoken-friendly aggregation of dais runs.
- ``cancel_run(ref_or_run)`` — cancel by voice.
- ``remain_silent()`` — polite no-op.

All handlers resolve the backend from ``params.app_resources["dsh_backend"]``
so the pipeline wiring stays a single dict entry.
"""

from __future__ import annotations

from rt_dsh_backend import DshBackend

DSH_TOOLS_DOCTRINE = """# Role and Objective
你是「Nova」语音编排助手，对接 dsh 编排会话。你听懂用户、提取意图、调用工具、把执行层返回的信息讲给用户。

# Tools
- dispatch_intent：把用户意图（自包含，指代全部展开）交给编排层分派。受理回执会立即返回。
- query_status：查询当前编排任务的状态摘要。
- cancel_run：取消一个编排任务，参数用回执里的 ref（vh-…）或 run_id。
- remain_silent：当最好的回应是不说话时调用（如控制消息后的确认），无用户可见效果。
- 闲聊、问候、一句话可答的常识直接回答。

# After Tool Calls（最高优先级规则）
- dispatch_intent 的回执是受理凭证：ref、【凭证…】等标识必须原样出现在你的回复里，一个字符都不能改。丢失即事故。
- 回执不是最终结果；终稿稍后以 "Agent Final Message" 开头送达，届时再完整播报。
- 编排状态/取消结果里的编号、任务数等标识同样原样转述，不添加执行层没提到的任何事实细节。

# Personality and Tone
中文口语，简洁友好，不用 Markdown。"""


async def dispatch_intent_tool(params, raw_intent: str):
    """把用户意图交给 dsh 编排层分派；立即返回受理回执，终稿稍后送达。

    Args:
        raw_intent: 用户意图的完整自包含描述，含所有上下文。
    """
    backend: DshBackend = params.app_resources["dsh_backend"]
    await params.result_callback(await backend.dispatch(raw_intent))


async def query_status_tool(params):
    """查询当前编排任务的状态摘要。"""
    backend: DshBackend = params.app_resources["dsh_backend"]
    await params.result_callback(await backend.query_status())


async def cancel_run_tool(params, ref_or_run: str):
    """取消一个编排任务。

    Args:
        ref_or_run: 受理回执里的 ref（vh-…）或 run_id。
    """
    backend: DshBackend = params.app_resources["dsh_backend"]
    await params.result_callback(await backend.cancel(ref_or_run))


async def remain_silent_tool(params):
    """当最好的回应是不说话时调用此工具；无用户可见效果。"""
    await params.result_callback({"status": "silent"})


def dsh_head_tools() -> list:
    """The four tool functions, ready for LLMContext(tools=...)."""
    return [dispatch_intent_tool, query_status_tool, cancel_run_tool, remain_silent_tool]
