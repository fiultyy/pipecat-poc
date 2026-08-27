#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Head tool surface over DshBackend (WS1 W1.4; docs/kg/01-ws1-head-dsh.md §5).

Five tools, docstring-as-schema (same convention as rt_orchestrator):

- ``dispatch_intent(raw_intent)`` — phase-1 receipt now; the phase-2
  final arrives later as a context re-injection carrying the
  ``"Agent Final Message":`` prefix.
- ``dispatch_plan(objective, subtasks_json)`` — dependency-split DAG
  dispatch (lane b-dag); one intent becomes ≥2 dependent subtasks and
  ONE aggregated final arrives later the same way.
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
- dispatch_intent：把用户意图（自包含，指代全部展开）交给编排层分派。受理回执会立即返回。凡用户没有明确要求分步执行的意图，一律用这个。
- dispatch_plan：仅当用户明确要求分步、且步骤之间有先后依赖（如"先…再…"、"第一步…第二步基于第一步…"）时调用。后一步用到前一步结果的，必须在前一步条目的 deps 里写上前一步的下标。subtasks_json 是 JSON 数组，每项含 spec（自包含子任务描述）、deps（前置子任务的下标数组，从 0 起，无依赖可省略）、command（真实完成该子任务工作的 shell 结算块，在仓库根目录执行）。
- query_status：查询当前编排任务的状态摘要。
- cancel_run：取消一个编排任务，参数用回执里的 ref（vh-…）或 run_id。
- remain_silent：当最好的回应是不说话时调用（如控制消息后的确认），无用户可见效果。
- 闲聊、问候、一句话可答的常识直接回答。

# After Tool Calls（最高优先级规则）
- dispatch_intent / dispatch_plan 返回的回执必须逐字转述给用户：ref、credentials 里的完整凭证标记、note 原文，一个字符都不能少。不得缩写、不得用省略号、不得替换成别的写法。
- 回执不是最终结果；终稿稍后以 "Agent Final Message" 开头送达，届时再完整播报。
- 编排状态/取消结果里的编号、任务数等标识同样原样转述，不添加执行层没提到的任何事实细节。

# Personality and Tone
中文口语，简洁友好，不用 Markdown。"""


class DoctrineSource:
    """Head system-instruction source: env-pointed file, built-in fallback.

    ``VOICE_HEAD_DOCTRINE=/path/to/file.md`` points the head at an external
    doctrine file (edit behavior without a code change; takes effect on the
    next head build, i.e. client reconnect — no gateway restart needed).
    Unset, unreadable, or blank falls back to ``DSH_TOOLS_DOCTRINE`` with a
    stderr warning: a bad doctrine config must never keep the voice head
    from starting.
    """

    ENV_KEY = "VOICE_HEAD_DOCTRINE"

    def __init__(self, env=None, default: str = DSH_TOOLS_DOCTRINE, warn=None):
        import os
        import sys

        self._env = env if env is not None else os.environ
        self._default = default
        self._warn = warn or (lambda msg: print(f"rt_head_tools: {msg}",
                                                file=sys.stderr))

    def load(self) -> str:
        """Return the effective doctrine text for one head build."""
        path = (self._env.get(self.ENV_KEY) or "").strip()
        if not path:
            return self._default
        try:
            with open(path, encoding="utf-8") as fh:
                text = fh.read()
        except OSError as e:
            self._warn(f"{self.ENV_KEY}={path} unreadable ({e}); "
                       "using built-in doctrine")
            return self._default
        if not text.strip():
            self._warn(f"{self.ENV_KEY}={path} is blank; using built-in doctrine")
            return self._default
        return text


async def dispatch_intent_tool(params, raw_intent: str, profile: str | None = None):
    """把用户意图交给 dsh 编排层分派；立即返回受理回执，终稿稍后送达。

    Args:
        raw_intent: 用户意图的完整自包含描述，含所有上下文。
        profile: 可选。库内 profile 名——绑到在飞编排会话穿衣（人格），
            需要编排会话以特定人格处理本次意图时才传。
    """
    backend: DshBackend = params.app_resources["dsh_backend"]
    await params.result_callback(await backend.dispatch(raw_intent, profile=profile))


async def dispatch_plan_tool(params, objective: str, subtasks_json: str):
    """把一个多步且分步有依赖的意图拆成子任务 DAG 交给编排层；立即返回受理回执，聚合终稿稍后送达。

    Args:
        objective: 总目标的完整自包含描述。
        subtasks_json: 子任务 JSON 数组字符串，每项形如 {"spec": "自包含子任务描述", "deps": [前置子任务下标], "command": "真实完成该子任务的 shell 结算块"}；deps 为本数组内前置子任务的下标（从 0 起），无依赖可省略；command 在仓库根目录执行。例：[{"spec":"先统计A","command":"grep -c x a.txt"},{"spec":"再基于A统计B并对比","deps":[0],"command":"grep -rc y b/"}]。凡后一步要以前一步结果为输入，必须写 deps。
    """
    backend: DshBackend = params.app_resources["dsh_backend"]
    if not isinstance(subtasks_json, str):
        # live drift: the model may pass the array itself instead of the
        # documented JSON string spelling
        import json

        subtasks_json = json.dumps(subtasks_json, ensure_ascii=False)
    await params.result_callback(
        await backend.dispatch_plan(objective, subtasks_json))


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
    """The five tool functions, ready for LLMContext(tools=...)."""
    return [dispatch_intent_tool, dispatch_plan_tool, query_status_tool,
            cancel_run_tool, remain_silent_tool]
