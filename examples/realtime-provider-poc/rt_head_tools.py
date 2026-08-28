#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Head tool surface over DshBackend (WS1 W1.4; docs/kg/01-ws1-head-dsh.md §5).

Twelve tools, docstring-as-schema (same convention as rt_orchestrator):

- ``dispatch_intent(raw_intent)`` — phase-1 receipt now; the phase-2
  final arrives later as a context re-injection carrying the
  ``"Agent Final Message":`` prefix.
- ``dispatch_plan(objective, subtasks_json)`` — dependency-split DAG
  dispatch (lane b-dag); one intent becomes ≥2 dependent subtasks and
  ONE aggregated final arrives later the same way.
- ``query_status()`` — local status summary: ledger rows ∪ in-flight runs
  (same state source as head.compact); never dispatches work.
- ``read_body(ref_or_no, max_chars=None, from_tail=False)`` — one final
  body from the read-only session store (full ref or spoken task no).
- ``list_bodies(limit=None)`` — recent store index (no bodies).
- ``cancel_run(ref)`` — cancel by voice.
- ``remain_silent()`` — polite no-op.
- ``find_files/grep_files/read_file`` — read-only workspace lookups
  (glob / regex content search / windowed file read).
- ``edit_file/write_file`` — literal text edits and whole-file writes,
  confined to the workspace root.

All handlers resolve the backend from ``params.app_resources["dsh_backend"]``
and the session store from ``params.app_resources["voice_store"]`` so the
pipeline wiring stays a single dict. The store is strictly read-only here
(``_store_bridge`` in rt_gateway remains the only write path); a missing
or unusable store degrades to an error form instead of raising.

File tools resolve the workspace root from
``params.app_resources["workspace_root"]`` (rt_gateway sets it from
``VOICE_WORKSPACE`` or the repo root). Every path is resolved inside that
root — escapes are rejected, not clamped; lookups cap lines/matches/scan
breadth so a spoken request can never pull an unbounded reply.
"""

from __future__ import annotations

import asyncio
import fnmatch
import os
import re
from pathlib import Path

from rt_conversation_items import state_snapshot
from rt_dsh_backend import DshBackend

DSH_TOOLS_DOCTRINE = """# Persona and Role
你是「Nova」，任务助手：精炼表述、状态优先、正文引用详情栏。你听懂用户、提取意图、调用工具；执行层的长正文不经过你的嘴，落在会话详情栏，用户要时你才取、才讲。

# Tools
- dispatch_intent：把用户意图（自包含，指代全部展开）交给编排层分派。凡用户没有明确要求分步执行的意图，一律用这个。
- dispatch_plan：仅当用户明确要求分步、且步骤之间有先后依赖（如"先…再…"、"第一步…第二步基于第一步…"）时调用。后一步用到前一步结果的，必须在前一步条目的 deps 里写上前一步的下标。subtasks_json 是 JSON 数组，每项含 spec（自包含子任务描述）、deps（前置子任务的下标数组，从 0 起，无依赖可省略）、command（真实完成该子任务工作的 shell 结算块，在仓库根目录执行）。
- query_status：用户问"现在什么状态/进行到哪了/有几个任务"时调用，返回任务状态汇总（编号、状态、摘要，运行中带时长）。本地查询，不打扰对接人、不产生新任务。
- read_body：用户要看某条任务的终稿正文、或追问任务输出细节时调用。ref_or_no 用你上下文里的完整 ref（vh-…）；用户念"任务N"编号时用编号 N。正文可能很长：用 max_chars 限定返回长度、from_tail 取尾部（终稿结论常在尾），按需分段读取。
- list_bodies：用户问"都有什么任务/什么状态"时调用，列出最近任务的台账索引（编号、状态、标题、字数，无正文）；要看哪条正文再用 read_body 取。
- find_files：按通配模式在工作区找文件路径（如"哪个文件叫 X""列出所有 markdown 文件"）。
- grep_files：按正则搜工作区文件内容（如"谁调用了 process_frame"），可限定子目录（path）与文件名过滤（include）。
- read_file：读某个文件的一段内容；超长文件用 offset/limit 分段，先看结构再定位到段。
- edit_file：改文件中的一处文字——old_string 必须与文件现有内容完全一致；多处相同且确要全改才用 replace_all，否则换更长的 old_string 精确定位。
- write_file：整文件新建或整体重写，仅在用户明确要求时用。
- cancel_run：取消一个编排任务，参数用回执里的 ref（vh-…）。用户说"取消刚才那个/第一个调研"时，由你从上下文里的回执解析出 ref，不让用户念编号。
- remain_silent：当最好的回应是不说话时调用（如控制消息后的确认），无用户可见效果。
- 闲聊、问候、一句话可答的常识直接回答。

# After Tool Calls（最高优先级规则）
- 受理回执（status=accepted）：只回一个状态，如"已受理"或"任务2已受理"，说完即止。不加解释、不追加任何尾巴（去向、进度提示都不加）。不念 ref、不念凭证、不念 run_id、不复述回执里的 JSON 字段。
- 终稿与完成通报：无论以 "Agent Final Message" 开头的全文注入、还是以 [编排通报] 开头的 JSON 消息到达，都只回一个状态，如"任务2完成了"、"完成了"或"任务2失败了"。不播报正文、不讲要点；用户追问时再讲。
- 连续工具调用（如 list_bodies 后再 read_body）：中间步骤不出声；全部取到所需信息后一次性作答。
- 文件工具（find_files/grep_files/read_file/edit_file/write_file）：读取类是中间步骤不出声，取到后按用户所问一句话作答；edit_file/write_file 完成只回一个状态（如"改好了"，可带一句改了什么），失败只说原因（如"没找到这处"），不念文件内容、不倒 diff。找不到就说没找到，不编造。
- query_status/read_body/list_bodies：状态问句先一句 counts（如"2个完成，1个在跑"）；read_body/list_bodies 结果按用户所问讲，不整段倒正文，长文先讲结构与要点，用户要哪段再用 max_chars/from_tail 分段取、逐段展开。查无（miss）就说目前没有这条任务，台账不可用（error/note）就说详情暂不可用，不编造内容。
- 编号协议：单任务时不念编号；多任务并存或用户要核对时，用「任务N」（N 是回执/通报/台账里的编号）区分。工具调用一律使用你上下文里的完整 ref，与念法无关。
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
    """查询编排任务的状态汇总（本地台账+运行态；不产生新任务）。

    返回 {"status":"ok","tasks":[…],"counts":{…}}：task 形如
    {"no","ref","status","summary","chars"}（终态，来自台账）或
    {"ref","status":"running","elapsed_s"}（在飞，来自运行登记）；
    台账不可用时仅返回运行态并附 note。
    """
    import asyncio
    import time

    backend: DshBackend = params.app_resources["dsh_backend"]
    store = params.app_resources.get("voice_store")
    note = None
    rows: list[dict] = []
    if store is not None:
        try:
            rows = await asyncio.to_thread(store.list, 20)
        except Exception:  # noqa: BLE001 — degrade to running-only
            note = "台账不可用，仅显示运行中任务"
    else:
        note = "台账不可用，仅显示运行中任务"
    running = [{"ref": ref, "ts": getattr(d, "ts", 0) or 0}
               for ref, d in getattr(backend, "_runs", {}).items()]
    snap = state_snapshot(rows, running, now=time.time())
    out = {"status": "ok", "tasks": snap["tasks"], "counts": snap["counts"]}
    if note:
        out["note"] = note
    await params.result_callback(out)


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


# ---- 工作区文件五件套（基础 agent 工具；root 越界拒绝、量限封顶）----

_SKIP_DIRS = {".git", ".hg", ".svn", "node_modules", "__pycache__",
              ".venv", "venv", ".mypy_cache", ".pytest_cache", ".ruff_cache",
              ".tox", ".idea", ".vscode", "dist", "build", "target"}
_FIND_DEFAULT, _FIND_MAX, _FIND_SCAN = 50, 200, 20000
_GREP_DEFAULT, _GREP_MAX, _GREP_SCAN = 30, 100, 5000
_GREP_FILE_MAX_BYTES = 2 * 1024 * 1024
_READ_DEFAULT_LINES, _READ_MAX_LINES, _READ_MAX_LINE_CHARS = 300, 1000, 1000
_WRITE_MAX_CHARS = 65536


def _workspace_root(params) -> Path | None:
    root = (getattr(params, "app_resources", None) or {}).get("workspace_root")
    if not root:
        return None
    try:  # resolve once: escape checks compare against the real root
        return Path(root).resolve()
    except OSError:
        return None


def _resolve_ws_path(root: Path, rel) -> Path | None:
    """Resolve ``rel`` under ``root``; None when blank or escaping root."""
    text = str(rel or "").strip()
    if not text:
        return None
    try:
        p = (root / text).resolve()
    except OSError:
        return None
    if p == root or root not in p.parents:
        return None
    return p


def _to_int(value, default: int, lo: int, hi: int) -> int:
    """Coerce a model-supplied numeric arg (may arrive as string) to bounds."""
    n = default
    if value is not None:
        try:
            n = int(value)
        except (TypeError, ValueError):
            n = default
    return min(max(n, lo), hi)


def _walk_ws(root: Path, base: Path | None):
    """os.walk under root (or a subdir), pruning junk/dot dirs; yields paths."""
    for dirpath, dirnames, filenames in os.walk(base or root):
        dirnames[:] = [d for d in sorted(dirnames)
                       if d not in _SKIP_DIRS and not d.startswith(".")]
        for name in sorted(filenames):
            yield Path(dirpath, name)


def _rel(root: Path, p: Path) -> str:
    try:
        return p.relative_to(root).as_posix()
    except ValueError:
        return str(p)


async def find_files_tool(params, pattern: str, limit=None):
    """按通配模式在工作区找文件路径（只读，不改任何数据）。

    Args:
        pattern: glob 模式。含 "/" 时按相对路径整体匹配（如
            "src/**/*.py"）；不含 "/" 时只匹配文件名（如 "*.md"）。
        limit: 可选。最多返回条数，缺省 50、上限 200。
    """
    root = _workspace_root(params)
    if root is None:
        await params.result_callback({"status": "error", "reason": "工作区不可用"})
        return
    n = _to_int(limit, _FIND_DEFAULT, 1, _FIND_MAX)

    def run() -> dict:
        hits: list[str] = []
        scanned = 0
        capped = False
        for p in _walk_ws(root, None):
            scanned += 1
            if scanned > _FIND_SCAN:
                capped = True
                break
            rel = _rel(root, p)
            ok = (fnmatch.fnmatch(rel, pattern)
                  if "/" in pattern else fnmatch.fnmatch(p.name, pattern))
            if ok:
                hits.append(rel)
                if len(hits) >= n:
                    break
        hits.sort()
        out = {"status": "ok", "files": hits, "count": len(hits)}
        if capped:
            out["note"] = "工作区过大，仅扫描了部分文件"
        return out

    await params.result_callback(await asyncio.to_thread(run))


async def grep_files_tool(params, pattern: str, path=None, include=None, limit=None):
    """按正则在工作区文件内容里搜索匹配行（只读，不改任何数据）。

    Args:
        pattern: 正则表达式，按行匹配（如 "def process_frame"）。
        path: 可选。只搜这个子目录或文件（相对工作区根）。
        include: 可选。文件名通配过滤（如 "*.py"）。
        limit: 可选。最多返回匹配数，缺省 30、上限 100；二进制与
            解码失败文件自动跳过。
    """
    root = _workspace_root(params)
    if root is None:
        await params.result_callback({"status": "error", "reason": "工作区不可用"})
        return
    n = _to_int(limit, _GREP_DEFAULT, 1, _GREP_MAX)
    base = _resolve_ws_path(root, path) if path else None
    if path and base is None:
        await params.result_callback({"status": "error", "reason": "路径越界",
                                      "path": str(path)})
        return
    if path and not base.exists():
        await params.result_callback({"status": "miss", "path": str(path)})
        return
    try:
        rx = re.compile(pattern)
    except re.error as e:
        await params.result_callback(
            {"status": "error", "reason": f"正则无效：{e}"})
        return

    def run() -> dict:
        matches: list[dict] = []
        scanned = 0
        truncated = False
        files = [base] if base is not None and base.is_file() else _walk_ws(root, base)
        for p in files:
            scanned += 1
            if scanned > _GREP_SCAN:
                truncated = True
                break
            if include and not fnmatch.fnmatch(p.name, include):
                continue
            try:
                if p.stat().st_size > _GREP_FILE_MAX_BYTES:
                    continue
                raw = p.read_bytes()
            except OSError:
                continue
            if b"\x00" in raw:
                continue
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                continue
            rel = _rel(root, p)
            for i, line in enumerate(text.splitlines(), 1):
                if rx.search(line):
                    matches.append({"path": rel, "line": i,
                                    "text": line.strip()[:200]})
                    if len(matches) >= n:
                        return {"status": "ok", "matches": matches,
                                "count": len(matches), "truncated": True}
        return {"status": "ok", "matches": matches, "count": len(matches),
                **({"note": "文件过多，仅扫描了部分文件"} if truncated else {})}

    await params.result_callback(await asyncio.to_thread(run))


async def read_file_tool(params, path: str, offset=None, limit=None):
    """读工作区一个文件的一段内容（带行号；只读）。

    Args:
        path: 相对工作区根的文件路径。
        offset: 可选。起始行号（从 1 起），缺省 1。
        limit: 可选。最多返回行数，缺省 300、上限 1000；超长单行
            截断到 1000 字符。
    """
    root = _workspace_root(params)
    if root is None:
        await params.result_callback({"status": "error", "reason": "工作区不可用"})
        return
    p = _resolve_ws_path(root, path)
    if p is None:
        await params.result_callback({"status": "error", "reason": "路径越界",
                                      "path": str(path)})
        return
    start = _to_int(offset, 1, 1, 10 ** 9)
    n = _to_int(limit, _READ_DEFAULT_LINES, 1, _READ_MAX_LINES)

    def run() -> dict:
        try:
            raw = p.read_bytes()
        except FileNotFoundError:
            return {"status": "miss", "path": str(path)}
        except OSError as e:
            return {"status": "error", "reason": f"读取失败：{e}"}
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            return {"status": "error", "reason": "非 UTF-8 文本文件",
                    "path": str(path)}
        lines = text.splitlines()
        sel = lines[start - 1:start - 1 + n]
        cap = _READ_MAX_LINE_CHARS
        out_lines = [f"{start + i}: {ln[:cap]}{'…' if len(ln) > cap else ''}"
                     for i, ln in enumerate(sel)]
        return {"status": "ok", "path": _rel(root, p),
                "total_lines": len(lines), "offset": start,
                "returned": len(sel), "lines": out_lines}

    await params.result_callback(await asyncio.to_thread(run))


async def edit_file_tool(params, path: str, old_string: str,
                         new_string: str, replace_all=False):
    """把文件中一处文字替换为另一段文字（精确匹配，写入文件）。

    Args:
        path: 相对工作区根的文件路径。
        old_string: 要被替换的原文，必须与文件现有内容完全一致；
            默认要求全文件唯一，多处匹配时返回错误。
        new_string: 替换后的新文字（可为空串，即删除该段）。
        replace_all: 可选。True 时替换全部匹配处，缺省 False。
    """
    root = _workspace_root(params)
    if root is None:
        await params.result_callback({"status": "error", "reason": "工作区不可用"})
        return
    p = _resolve_ws_path(root, path)
    if p is None:
        await params.result_callback({"status": "error", "reason": "路径越界",
                                      "path": str(path)})
        return
    if not old_string:
        await params.result_callback(
            {"status": "error", "reason": "old_string 不能为空"})
        return

    def run() -> dict:
        try:
            raw = p.read_bytes()
        except FileNotFoundError:
            return {"status": "miss", "path": str(path)}
        except OSError as e:
            return {"status": "error", "reason": f"读取失败：{e}"}
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            return {"status": "error", "reason": "非 UTF-8 文本文件",
                    "path": str(path)}
        count = text.count(old_string)
        if count == 0:
            return {"status": "miss", "path": str(path)}
        if count > 1 and not replace_all:
            return {"status": "error",
                    "reason": f"old_string 有 {count} 处匹配；换更长的原文精确定位，或确认全改时用 replace_all"}
        new_text = text.replace(old_string, new_string) if replace_all \
            else text.replace(old_string, new_string, 1)
        try:
            p.write_text(new_text, encoding="utf-8")
        except OSError as e:
            return {"status": "error", "reason": f"写入失败：{e}"}
        return {"status": "ok", "path": _rel(root, p),
                "replacements": count if replace_all else 1,
                "bytes": len(new_text.encode("utf-8"))}

    await params.result_callback(await asyncio.to_thread(run))


async def write_file_tool(params, path: str, content: str):
    """整文件写入（新建或整体重写；会按需创建父目录）。

    Args:
        path: 相对工作区根的文件路径。
        content: 完整文件内容（UTF-8 文本，上限 65536 字符）。
    """
    root = _workspace_root(params)
    if root is None:
        await params.result_callback({"status": "error", "reason": "工作区不可用"})
        return
    p = _resolve_ws_path(root, path)
    if p is None:
        await params.result_callback({"status": "error", "reason": "路径越界",
                                      "path": str(path)})
        return
    if not isinstance(content, str):
        content = str(content)
    if len(content) > _WRITE_MAX_CHARS:
        await params.result_callback(
            {"status": "error",
             "reason": f"内容超过 {_WRITE_MAX_CHARS} 字符上限"})
        return

    def run() -> dict:
        created = not p.exists()
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content, encoding="utf-8")
        except OSError as e:
            return {"status": "error", "reason": f"写入失败：{e}"}
        return {"status": "ok", "path": _rel(root, p), "chars": len(content),
                "created": created}

    await params.result_callback(await asyncio.to_thread(run))


def dsh_head_tools() -> list:
    """The twelve tool functions, ready for LLMContext(tools=...)."""
    return [dispatch_intent_tool, dispatch_plan_tool, query_status_tool,
            read_body_tool, list_bodies_tool, cancel_run_tool,
            remain_silent_tool,
            find_files_tool, grep_files_tool, read_file_tool,
            edit_file_tool, write_file_tool]
