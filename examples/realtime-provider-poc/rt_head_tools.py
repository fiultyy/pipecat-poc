#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Head tool surface over DshBackend (WS1 W1.4; docs/kg/01-ws1-head-dsh.md §5).

Seven tools, docstring-as-schema (same convention as rt_orchestrator):

- ``dispatch_intent(raw_intent)`` — phase-1 receipt now; the phase-2
  final arrives later as a context re-injection carrying the
  ``"Agent Final Message":`` prefix.
- ``dispatch_plan(objective, subtasks_json)`` — dependency-split DAG
  dispatch (lane b-dag); one intent becomes ≥2 dependent subtasks and
  ONE aggregated final arrives later the same way.
- ``query_status()`` — spoken-friendly aggregation of dais runs.
- ``read_body(ref_or_no, max_chars=None, from_tail=False)`` — one final
  body from the read-only session store (full ref or spoken task no).
- ``list_bodies(limit=None)`` — recent store index (no bodies).
- ``cancel_run(ref)`` — cancel by voice.
- ``remain_silent()`` — polite no-op.

All handlers resolve the backend from ``params.app_resources["dsh_backend"]``
and the session store from ``params.app_resources["voice_store"]`` so the
pipeline wiring stays a single dict. The store is strictly read-only here
(``_store_bridge`` in rt_gateway remains the only write path); a missing
or unusable store degrades to an error form instead of raising.
"""

from __future__ import annotations

import asyncio

from rt_dsh_backend import DshBackend

DSH_TOOLS_DOCTRINE = """# Persona and Role
你是「Nova」，任务助手：精炼表述、状态优先、正文引用详情栏。你听懂用户、提取意图、调用工具；执行层的长正文不经过你的嘴，落在会话详情栏，用户要时你才取、才讲。

# Tools
- dispatch_intent：把用户意图（自包含，指代全部展开）交给编排层分派。凡用户没有明确要求分步执行的意图，一律用这个。
- dispatch_plan：仅当用户明确要求分步、且步骤之间有先后依赖（如"先…再…"、"第一步…第二步基于第一步…"）时调用。后一步用到前一步结果的，必须在前一步条目的 deps 里写上前一步的下标。subtasks_json 是 JSON 数组，每项含 spec（自包含子任务描述）、deps（前置子任务的下标数组，从 0 起，无依赖可省略）、command（真实完成该子任务工作的 shell 结算块，在仓库根目录执行）。
- query_status：查询当前编排任务的状态摘要。
- read_body：用户要看某条任务的终稿正文、或追问任务输出细节时调用。ref_or_no 用你上下文里的完整 ref（vh-…）；用户念"任务N"编号时用编号 N。正文可能很长：用 max_chars 限定返回长度、from_tail 取尾部（终稿结论常在尾），按需分段读取。
- list_bodies：用户问"都有什么任务/什么状态"时调用，列出最近任务的台账索引（编号、状态、标题、字数，无正文）；要看哪条正文再用 read_body 取。
- cancel_run：取消一个编排任务，参数用回执里的 ref（vh-…）。用户说"取消刚才那个/第一个调研"时，由你从上下文里的回执解析出 ref，不让用户念编号。
- remain_silent：当最好的回应是不说话时调用（如控制消息后的确认），无用户可见效果。
- 闲聊、问候、一句话可答的常识直接回答。

# After Tool Calls（最高优先级规则）
- 受理回执（status=accepted）：用一句话讲状态和极简摘要，如"已受理，转对接人执行，详情栏可看进度"。不念 ref、不念凭证、不念 run_id、不复述回执里的 JSON 字段。
- 终稿与完成通报：无论以 "Agent Final Message" 开头的全文注入、还是以 [编排通报] 开头的 JSON 消息到达，都只用一句话讲状态与要点，如"调研完成了：采纳方案B，全文在详情栏"。不整段播报正文；用户追问细节时再展开。
- read_body/list_bodies：结果按用户所问讲，不整段倒正文；长文先讲结构与要点，用户要哪段再用 max_chars/from_tail 分段取、逐段展开。查无（miss）就说目前没有这条任务，台账不可用（error）就说详情暂不可用，不编造内容。
- 编号协议：默认不念编号。仅当用户要核对、或同时有多个任务需要区分时，念 ref 的前三位（如"编号 3f7"）；工具调用一律使用你上下文里的完整 ref，与念法无关。
- 不添加执行层没有的事实；转述终稿正文要忠实，长文先讲结构与要点，用户要求再逐段展开。

# Personality and Tone
中文口语，短句优先，简洁友好，不用 Markdown。"""


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


async def cancel_run_tool(params, ref: str):
    """取消一个编排任务。

    Args:
        ref: 受理回执里的 ref（vh-…）。
    """
    backend: DshBackend = params.app_resources["dsh_backend"]
    await params.result_callback(await backend.cancel(ref))


async def remain_silent_tool(params):
    """当最好的回应是不说话时调用此工具；无用户可见效果。"""
    await params.result_callback({"status": "silent"})


# ---- 台账只读两件套（KG 14 §2.3 七件套，PR4）----

_READ_INDEX_SCAN = 500  # 按 no 解析时的索引扫描深度（对齐 LRU 上限）


async def _lookup_body_rec(store, ref_or_no) -> dict | None:
    """台账取一条完整条目：ref 直查；整型/纯数字串按 no 先在最近
    索引里解析出 ref 再查全文（store 无按 no 直查面）。"""
    if isinstance(ref_or_no, int) and not isinstance(ref_or_no, bool):
        no: int | None = int(ref_or_no)
    else:
        text = str(ref_or_no).strip()
        no = int(text) if text.isdigit() else None
    if no is None:
        return await asyncio.to_thread(store.get, str(ref_or_no).strip())
    rows = await asyncio.to_thread(store.list, _READ_INDEX_SCAN)
    ref = next((r.get("ref") for r in rows if r.get("no") == no), None)
    return await asyncio.to_thread(store.get, ref) if ref else None


async def read_body_tool(params, ref_or_no, max_chars=None, from_tail=False):
    """读取一条编排任务的终稿正文（只读台账，不改任何数据）。

    Args:
        ref_or_no: 目标任务——上下文里的完整 ref（vh-…），或用户念的
            编号 no（如"任务2"里的 2）。
        max_chars: 可选。正文最多返回的字数；超长时截断并标记
            truncated 与 returned_chars，其余部分需要时再分段取。
        from_tail: 可选。True 时返回正文尾部 max_chars 字（终稿结论
            常在尾部）。
    """
    store = params.app_resources.get("voice_store")
    if store is None:
        await params.result_callback({"status": "error", "reason": "台账不可用"})
        return
    try:
        rec = await _lookup_body_rec(store, ref_or_no)
    except Exception as e:  # noqa: BLE001 — C 降级：读不到就说读不到，不炸
        await params.result_callback({"status": "error", "reason": f"台账读取失败：{e}"})
        return
    if rec is None:
        await params.result_callback({"status": "miss", "ref_or_no": ref_or_no})
        return
    body = rec.get("body") or ""
    out = {
        "status": "ok",
        "ref": rec.get("ref"),
        "no": rec.get("no"),
        "task_status": rec.get("status"),
        "title": rec.get("title"),
        "chars": len(body),
        "body": body,
    }
    if max_chars is not None:
        try:  # live drift: the model may pass "500"/None-ish strings
            n = max(int(max_chars), 0)
        except (TypeError, ValueError):
            n = None
        if n is not None and len(body) > n:
            out["body"] = body[len(body) - n:] if from_tail else body[:n]
            out["truncated"] = True
            out["returned_chars"] = n
    await params.result_callback(out)


async def list_bodies_tool(params, limit=None):
    """列出最近的编排任务台账索引（无正文）。

    Args:
        limit: 可选。最多返回条数，缺省 10、上限 20。
    """
    store = params.app_resources.get("voice_store")
    if store is None:
        await params.result_callback({"status": "error", "reason": "台账不可用"})
        return
    n = 10 if limit is None else min(max(int(limit), 1), 20)
    try:
        rows = await asyncio.to_thread(store.list, n)
    except Exception as e:  # noqa: BLE001 — C 降级
        await params.result_callback({"status": "error", "reason": f"台账读取失败：{e}"})
        return
    keys = ("no", "ref", "status", "title", "summary", "chars", "ts")
    await params.result_callback(
        {"status": "ok", "items": [{k: r.get(k) for k in keys} for r in rows]})


def dsh_head_tools() -> list:
    """The seven tool functions, ready for LLMContext(tools=...)."""
    return [dispatch_intent_tool, dispatch_plan_tool, query_status_tool,
            read_body_tool, list_bodies_tool, cancel_run_tool,
            remain_silent_tool]
