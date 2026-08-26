#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for rt_voice_app ONE-client additions (KG 12):

- ObserveLink handshake: auth → observe session.start → session.started
  echoes topics; observe flag present, no media path.
- Pure render helpers: fleet_rows / orch_tree_lines / turn_line.
"""

import asyncio
import json
import queue
import sys
from pathlib import Path

import aiohttp
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_voice_app import (  # noqa: E402
    ObserveLink,
    PushToTalk,
    fleet_rows,
    orch_tree_lines,
    turn_line,
)


class _FakeWs:
    """Server-side stand-in recording client sends, feeding canned replies.

    ``closed`` mirrors link close: once set, iteration ends (the worker's
    reconnect loop observes the stop event and exits)."""

    def __init__(self, replies: list[dict]):
        self._replies = list(replies)
        self.sent: list[dict] = []
        self.closed = False
        self._q: asyncio.Queue = asyncio.Queue()
        for r in self._replies:
            self._q.put_nowait((aiohttp.WSMsgType.TEXT, r))

    async def send_json(self, obj):  # coroutine like aiohttp's
        self.sent.append(obj)
        # auth → auth.ok; observe session.start → session.started echo
        if obj.get("t") == "auth":
            self._q.put_nowait((aiohttp.WSMsgType.TEXT, {"t": "auth.ok", "session_id": "s-obs1"}))
        elif obj.get("t") == "session.start" and obj.get("observe"):
            self._q.put_nowait((aiohttp.WSMsgType.TEXT, {
                "t": "session.started", "session_id": "s-obs1",
                "topics": ["orch.dispatch", "fleet.snapshot"],
            }))

    async def close(self):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.closed:
            raise _Stop
        if self._q.empty():
            # no more canned frames: park until the test closes us (a real
            # ws blocks awaiting the server, never spins)
            while not self.closed and self._q.empty():
                await asyncio.sleep(0.01)
            if self.closed and self._q.empty():
                raise _Stop
        kind, payload = self._q.get_nowait()
        return _Msg(kind, payload)


class _Stop(Exception):
    """Iteration-end sentinel: StopAsyncIteration raised inside a coroutine
    turns into RuntimeError under asyncio (PEP 479 kin); ObserveLink treats
    any exception as a connection drop and re-enters its wait loop."""


class _Msg:
    def __init__(self, kind, payload):
        self.type = kind  # aiohttp.WSMsgType enum
        self.data = json.dumps(payload)  # str like a real text frame


class _FakeSessionCtx:
    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, *exc):
        return False


class _FakeHttp:
    def __init__(self, ws):
        self._ws = ws

    def ws_connect(self, url, max_msg_size=0):
        # plain method like aiohttp's: returns a context manager whose
        # __aenter__/__aexit__ are coroutines (async-with protocol)
        return _FakeSessionCtx(self._ws)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def test_fleet_rows_parses_snapshot():
    frame = {"t": "fleet.snapshot", "fleet": {
        "<seat>": {"sessionId": "session-x", "alias": "webgui", "node": "voice-head",
                 "role": "orchestrator", "status": "active"},
        "<seat>": {"sessionId": "session-y", "alias": "liaison", "node": "voice-head",
                 "role": "liaison", "status": "active"},
    }}
    rows = fleet_rows(frame)
    assert rows == [
        ("20d0", "webgui", "voice-head", "orchestrator", "active"),
        ("9b95", "liaison", "voice-head", "liaison", "active"),
    ]
    assert fleet_rows({"t": "fleet.snapshot"}) == []          # no fleet key
    assert fleet_rows({"fleet": "corrupt"}) == []             # non-dict


def test_orch_tree_lines_builds_run_ref_tree():
    frames = [
        {"t": "orch.dispatch", "run_id": "run_aa", "ref": "vh-1"},
        {"t": "orch.progress", "run_id": "run_aa"},
        {"t": "orch.dispatch", "run_id": "run_bb", "ref": "vh-2"},
        {"t": "orch.done", "run_id": "run_aa"},
    ]
    lines = orch_tree_lines(frames)
    assert lines == ["✓ run_aa", "   └ vh-1", "… run_bb", "   └ vh-2"]


def test_turn_line_labels_and_detail():
    assert turn_line({"phase": "user_text", "detail": "调研完成"}) == "💬 user_text 调研完成"
    assert turn_line({"phase": "tool_call"}) == "🔧 tool_call"
    assert turn_line({"phase": "unknown_phase"}) == "· unknown_phase"


def test_push_totalk_edges_only():
    ptt = PushToTalk()
    assert ptt.press() == "start"        # 按下 → 开麦
    assert ptt.press() is None           # 按住重复事件 → 不动作
    assert ptt.release() == "stop"       # 松开 → 闭麦
    assert ptt.release() is None         # 无持有时松开 → 不动作
    assert ptt.press() == "start"        # 可再次进入


@pytest.mark.asyncio
async def test_observe_link_handshake(monkeypatch):
    """auth → session.start{observe:true} → session.started echoes topics;
    subsequent event frames land in the obs queue verbatim."""
    ws = _FakeWs([{"t": "orch.dispatch", "run_id": "run_zz", "ref": "vh-9"}])
    obs: queue.Queue = queue.Queue()
    states: list[str] = []
    link = ObserveLink("ws://x/ws", "tok", obs, states.append)

    monkeypatch.setattr(
        "aiohttp.ClientSession",
        lambda *a, **k: _FakeHttp(ws),
    )
    task = asyncio.create_task(link._worker())
    for _ in range(200):
        await asyncio.sleep(0.01)
        if not obs.empty() and "open" in states:
            break
    link._stop.set()
    ws.closed = True
    await asyncio.wait_for(task, timeout=5)

    start = next(s for s in ws.sent if s.get("t") == "session.start")
    assert start["observe"] is True
    assert link.topics == ["orch.dispatch", "fleet.snapshot"]
    assert "open" in states
    f = obs.get_nowait()
    assert f.get("t") == "orch.dispatch" and f.get("ref") == "vh-9"
