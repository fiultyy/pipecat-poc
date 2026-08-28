#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for rt_voice_app ONE-client additions (KG 12 + KG 14 §2.4):

- ObserveLink handshake: auth → observe session.start → session.started
  echoes topics; observe flag present, no media path. Outbound request face
  (body.get / fleet.cleanup) rides the same ws; in-session ``error`` frames
  are per-request (fed to the obs queue) instead of dropping the link.
- Pure render helpers: fleet_rows / orch_tree_lines / turn_line plus the
  detail-tab helpers (rows from push, ref-dedup merge, notice lines, notify
  label, ack gray line, error line) and the tab-consolidation helpers
  (cleanup_request frame, cleanup_result_line summary, tickets_text), plus
  the fleet brief helpers (request frame, per-seat status lines).
- Tab assembly smoke (real tkinter widgets, no network, no mainloop):
  detail/task tab and the fleet cleanup control face, plus the fleet brief
  face (button, read-only panel, result rendering).
"""

import asyncio
import json
import queue
import sys
import time
from pathlib import Path

import aiohttp
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_voice_app import (  # noqa: E402
    App,
    DETAIL_COLS,
    FLEET_EVENT_KEEP,
    FLEET_STALE_S,
    ObserveLink,
    PENDING_TIMEOUT_S,
    PushToTalk,
    VAD_TAIL_STEP_MS,
    body_list_more_request,
    chars_mag,
    cleanup_request,
    cleanup_result_line,
    detail_ack_line,
    detail_error_line,
    detail_rows_from_push,
    detail_ts_map,
    error_identity,
    fleet_brief_lines,
    fleet_brief_request,
    fleet_rows,
    head_list_request,
    head_names_from_list,
    head_switch_line,
    head_switch_request,
    last_seen_label,
    liaison_bind_request,
    liaison_line,
    liaison_result_line,
    liaison_unbind_request,
    merge_detail_rows,
    notice_line_from_push,
    orch_tree_lines,
    pending_error_line,
    pending_send_fail_line,
    pending_timeout_line,
    replay_notice_line,
    run_cancel_request,
    run_cancel_result_line,
    tickets_text,
    turn_label_with_notify,
    turn_line,
    vad_tail_plan,
    whiteboard_set_line,
    whiteboard_set_request,
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
        else:
            # post-handshake client frames (body.get …) → test-scripted replies
            reply = self.on_send(obj)
            if reply is not None:
                self._q.put_nowait((aiohttp.WSMsgType.TEXT, reply))

    def on_send(self, obj):
        """Hook for per-test reply scripting; return a frame dict or None."""
        return None

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


@pytest.mark.asyncio
async def test_observe_link_body_get_roundtrip(monkeypatch):
    """body.get 经观测连接发出（握手先行）；body.item/error 回包进 obs 队列；
    会话内 error 是请求级不断链（worker 存活、无 error 态）。"""
    ws = _FakeWs([])

    def on_send(obj):
        if obj.get("t") != "body.get":
            return None
        ref = obj.get("ref")
        if ref == "vh-miss":
            return {"t": "error", "code": "body_miss",
                    "msg": f"no stored body for ref {ref}"}
        return {"t": "body.item", "ref": ref, "title": "t", "text": "全文正文",
                "chars": 4, "ts": 1.0}

    ws.on_send = on_send
    obs: queue.Queue = queue.Queue()
    states: list[str] = []
    link = ObserveLink("ws://x/ws", "tok", obs, states.append)
    link.send_request({"t": "body.get", "ref": "vh-1"})  # 连接前入队：握手后发

    monkeypatch.setattr(
        "aiohttp.ClientSession",
        lambda *a, **k: _FakeHttp(ws),
    )
    task = asyncio.create_task(link._worker())

    async def _until(pred, timeout=5.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            while not obs.empty():
                f = obs.get_nowait()
                if pred(f):
                    return f
            await asyncio.sleep(0.01)
        return None

    item = await _until(lambda f: f.get("t") == "body.item" and f.get("ref") == "vh-1")
    assert item is not None and item["text"] == "全文正文"

    link.send_request({"t": "body.get", "ref": "vh-miss"})
    err = await _until(lambda f: f.get("t") == "error" and f.get("code") == "body_miss")
    assert err is not None
    assert not task.done()                     # 请求级 error 不断链
    assert not any(s.startswith("error") for s in states)

    link._stop.set()
    ws.closed = True
    await asyncio.wait_for(task, timeout=5)

    assert [m.get("t") for m in ws.sent[:2]] == ["auth", "session.start"]
    gets = [m for m in ws.sent if m.get("t") == "body.get"]
    assert [g.get("ref") for g in gets] == ["vh-1", "vh-miss"]


@pytest.mark.asyncio
async def test_observe_link_fleet_cleanup_roundtrip(monkeypatch):
    """fleet.cleanup 经观测连接发出（握手先行）；fleet.cleanup.result
    回包按 req_id 关联进 obs 队列——send_request 出站请求面往返验证。"""
    ws = _FakeWs([])

    def on_send(obj):
        if obj.get("t") != "fleet.cleanup":
            return None
        return {"t": "fleet.cleanup.result", "req_id": obj.get("req_id"),
                "results": [{"id": i, "ok": True} for i in obj.get("ids")]}

    ws.on_send = on_send
    obs: queue.Queue = queue.Queue()
    states: list[str] = []
    link = ObserveLink("ws://x/ws", "tok", obs, states.append)
    link.send_request(cleanup_request(["20d0", "9b95"], "end", "r-clean-1"))

    monkeypatch.setattr(
        "aiohttp.ClientSession",
        lambda *a, **k: _FakeHttp(ws),
    )
    task = asyncio.create_task(link._worker())

    deadline = time.monotonic() + 5.0
    got = None
    while time.monotonic() < deadline:
        while not obs.empty():
            f = obs.get_nowait()
            if f.get("t") == "fleet.cleanup.result":
                got = f
        if got is not None:
            break
        await asyncio.sleep(0.01)

    link._stop.set()
    ws.closed = True
    await asyncio.wait_for(task, timeout=5)

    assert got is not None and got["req_id"] == "r-clean-1"
    assert [r["id"] for r in got["results"]] == ["20d0", "9b95"]
    sent = [m for m in ws.sent if m.get("t") == "fleet.cleanup"]
    assert sent == [{"t": "fleet.cleanup", "ids": ["20d0", "9b95"],
                     "mode": "end", "req_id": "r-clean-1"}]


# ---- KG 14 §2.4 详情页签 / 迷你通知行 / 回合 notify 相 纯函数 ----


def test_detail_rows_from_push_shapes():
    ts = 1700000000.0
    when = time.strftime("%H:%M:%S", time.localtime(ts))
    single = {"t": "body.push", "ref": "vh-a", "no": 3, "status": "done",
              "title": "标题一", "summary": "摘要", "chars": 1234,
              "inline": "全文", "ts": ts}
    assert detail_rows_from_push(single) == [(when, "3", "vh-a", "标题一", "1234", "done")]
    replay = {"t": "body.push", "items": [
        {"ref": "vh-b", "no": 1, "status": "done", "title": "b", "chars": 9, "ts": ts},
        {"no": 8, "status": "done", "title": "无 ref 跳过"},
        "corrupt",
        {"ref": "vh-c", "status": "failed", "title": "c", "chars": 0},
    ], "ts": ts}
    assert detail_rows_from_push(replay) == [
        (when, "1", "vh-b", "b", "9", "done"),
        ("", "", "vh-c", "c", "0", "failed"),
    ]
    # 缺 status 键 → 空串（旧网关帧向后兼容）
    assert detail_rows_from_push({"ref": "vh-d", "no": 2, "chars": 1, "ts": ts}) == \
        [(when, "2", "vh-d", "", "1", "")]
    assert detail_rows_from_push({}) == []
    assert detail_rows_from_push({"items": "corrupt"}) == []
    assert detail_rows_from_push(None) == []


def test_merge_detail_rows_dedup_by_ref():
    prev = [("10:00:00", "1", "vh-a", "旧标题", "5", "done"),
            ("10:01:00", "2", "vh-b", "b", "6", "done")]
    # 同 ref 更新：原位换值（含 time/status——行显示最新推送态）、行不跳动
    merged = merge_detail_rows(prev, [("10:02:00", "1", "vh-a", "新标题", "90", "failed")])
    assert merged == [("10:02:00", "1", "vh-a", "新标题", "90", "failed"),
                      ("10:01:00", "2", "vh-b", "b", "6", "done")]
    # 新 ref 追加尾部
    merged2 = merge_detail_rows(merged, [("10:03:00", "7", "vh-z", "z", "1", "cancelled")])
    assert len(merged2) == 3 and \
        merged2[-1] == ("10:03:00", "7", "vh-z", "z", "1", "cancelled")


def test_notice_line_from_push():
    f = {"t": "body.push", "ref": "vh-x", "no": 2, "status": "done",
         "summary": "采纳方案B并整理正文", "chars": 1834}
    assert notice_line_from_push(f) == "📣 #2 done 采纳方案B并整理正文（1.8k字）"
    # 无 no → 用 ref；无 summary → 退 title
    assert notice_line_from_push({"ref": "vh-y", "status": "failed",
                                  "title": "失败标题", "chars": 512}) == \
        "📣 vh-y failed 失败标题（512字）"
    assert notice_line_from_push({"ref": "vh-z"}) == "📣 vh-z ? （0字）"
    assert notice_line_from_push({}) == ""
    assert notice_line_from_push(None) == ""


def test_replay_notice_line():
    assert replay_notice_line({"items": [{}, {}]}) == "📣 台账回放 2 条（详情页签查看）"
    assert replay_notice_line({"items": []}) == ""
    assert replay_notice_line({"ref": "vh-live"}) == ""   # 单条帧不是回放


def test_chars_mag():
    assert chars_mag(0) == "0"
    assert chars_mag(None) == "0"
    assert chars_mag("x") == "0"
    assert chars_mag(999) == "999"
    assert chars_mag(1000) == "1.0k"
    assert chars_mag(1834) == "1.8k"
    assert chars_mag(4096) == "4.1k"
    assert chars_mag(38000) == "38k"


def test_turn_label_with_notify_and_line():
    assert turn_label_with_notify("notify") == "📣"
    assert turn_label_with_notify("user_text") == "💬"
    assert turn_label_with_notify("mystery") == "·"
    # notify 无 detail → 回退显示 ref（通报帧只带 conv_id/phase/ref）
    assert turn_line({"phase": "notify", "ref": "vh-9", "conv_id": "s"}) == \
        "📣 notify → vh-9"
    assert turn_line({"phase": "notify", "ref": "vh-9", "detail": "通报文本"}) == \
        "📣 notify 通报文本"
    assert turn_line({"phase": "tool_call"}) == "🔧 tool_call"   # 旧行为不变


def test_detail_ack_line_conditions():
    seen = {"vh-1", "vh-2"}
    push = {"t": "body.push", "ref": "vh-1", "no": 1, "status": "done", "chars": 3}
    assert detail_ack_line(push, seen) == "└ 已入详情 vh-1"
    assert detail_ack_line({"ref": "vh-x"}, seen) is None     # 回合页没见过该 ref
    assert detail_ack_line({"ref": "vh-1"}, set()) is None    # notify 集为空
    assert detail_ack_line({"no": 1}, seen) is None           # 无 ref
    assert detail_ack_line(None, seen) is None


def test_detail_error_line():
    assert detail_error_line({"t": "error", "code": "body_miss",
                              "msg": "no stored body for ref vh-x"}) == \
        "⚠ body_miss no stored body for ref vh-x"
    assert detail_error_line({"code": "body_miss"}) == "⚠ body_miss"
    assert detail_error_line(None) == "⚠ 未知错误"


# ---- 页签整合 + 席位清理控制面：纯函数 ----


def test_cleanup_request_frame():
    assert cleanup_request(["20d0", "9b95"], "end", "r-1") == \
        {"t": "fleet.cleanup", "ids": ["20d0", "9b95"],
         "mode": "end", "req_id": "r-1"}
    assert cleanup_request(["a1b2"], "release", "r-2")["mode"] == "release"
    assert cleanup_request([], "release", "r-3") == \
        {"t": "fleet.cleanup", "ids": [], "mode": "release", "req_id": "r-3"}
    assert cleanup_request([20, 30], "release", "r-4")["ids"] == ["20", "30"]  # str 归一
    assert cleanup_request(None, "end", "r-5")["ids"] == []


def test_cleanup_result_line():
    ok = {"t": "fleet.cleanup.result", "req_id": "r-1", "results": [
        {"id": "20d0", "ok": True}, {"id": "9b95", "ok": True}]}
    assert cleanup_result_line(ok) == "🧹 清理 2/2 成功"
    # 失败明细 + active-liaison 备注（ok 条目不阻断）
    mixed = {"results": [
        {"id": "20d0", "ok": True},
        {"id": "aaaa", "ok": False, "error": "not_found"},
        {"id": "9b95", "ok": True, "note": "active-liaison"},
    ]}
    line = cleanup_result_line(mixed)
    assert "2/3" in line
    assert "aaaa(not_found)" in line
    assert "9b95:active-liaison" in line
    # 畸形回包 → 占位行，不抛
    assert cleanup_result_line({}) == "⚠ 清理回包不可读"
    assert cleanup_result_line({"results": []}) == "⚠ 清理回包不可读"
    assert cleanup_result_line({"results": "corrupt"}) == "⚠ 清理回包不可读"
    assert cleanup_result_line({"results": ["x", None]}) == "⚠ 清理回包不可读"
    assert cleanup_result_line(None) == "⚠ 清理回包不可读"


def test_tickets_text_tail_window():
    assert tickets_text({"t": "tickets.snapshot", "text": "# T\n- [ ] 项"}) == "# T\n- [ ] 项"
    assert tickets_text({"content": "无 text 退 content"}) == "无 text 退 content"
    assert tickets_text({"text": ""}) == ""
    assert tickets_text({}) == ""
    assert tickets_text(None) == ""
    long = "x" * 3000
    out = tickets_text({"text": long})
    assert len(out) == 2000 and out == "x" * 2000   # 尾部窗口


def test_detail_tab_assembly_smoke():
    """详情页签装配冒烟（真 tkinter、无网络、无 mainloop）：假 body.push
    进表按 ref 去重、notify 灰行、inline/回填缓存直渲染。无显示环境跳过。"""
    try:
        import tkinter as tk

        root = tk.Tk()
    except Exception as e:  # noqa: BLE001 — headless 环境
        pytest.skip(f"no display for tkinter: {e}")
    root.withdraw()
    app = None
    try:
        app = App(root, "ws://127.0.0.1:8765/ws", "", False)
        root.update()
        app._selftest_probe()
        app._drain_obs()          # 确定性收割（不等 after 节拍）
        root.update()

        assert app._notify_refs == {"vh-self2"}
        # iid=ref；无 ref 条目未入表；回放批先入、增量追加
        assert list(app.detail_tree.get_children()) == ["vh-self1", "vh-self2"]
        assert app.detail_tree.item("vh-self1", "values")[2] == "vh-self1"

        # inline 缓存直渲染（不触网：观测连接未开也不走 body.get）
        app.detail_tree.selection_set("vh-self2")
        app._on_detail_select()
        assert app.detail_body.get("1.0", "end").strip() == "inline 正文"

        # body.item 已回填缓存 → 直渲染
        app.detail_tree.selection_set("vh-self1")
        app._on_detail_select()
        assert app.detail_body.get("1.0", "end").strip() == "全文正文"

        turn_text = app.turn_log.get("1.0", "end")
        assert "📣 notify → vh-self2" in turn_text
        assert turn_text.count("└ 已入详情 vh-self2") == 1
    finally:
        if app is not None and getattr(app, "ptt_listener", None):
            app.ptt_listener.stop()
        root.destroy()


def test_task_fleet_tabs_assembly_smoke():
    """任务页（台账+正文+tickets 面板）/编排页 bridge 行/席位多选+清理按钮
    装配冒烟（真 tkinter、无网络、无 mainloop）。空选择与观测未开两条
    早退路径不弹确认框——无显示环境可安全驱动。无显示环境跳过。"""
    try:
        import tkinter as tk

        root = tk.Tk()
    except Exception as e:  # noqa: BLE001 — headless 环境
        pytest.skip(f"no display for tkinter: {e}")
    root.withdraw()
    app = None
    try:
        app = App(root, "ws://127.0.0.1:8765/ws", "", False)
        root.update()

        # 页签序：语音/编排/回合/席位/白板/任务（消息/票板、详情页已整合）
        assert [app.nb.tab(t, "text").strip() for t in app.nb.tabs()] == \
            ["语音", "编排", "回合", "席位", "白板", "任务"]

        # bridge.msg → 编排页底部独立小面板（ScrolledText 内嵌一层 plain
        # Frame，取 master.master 判真实挂载页）
        assert app.bridge_log.master.master is app.orch_tab
        app.obs_q.put({"t": "bridge.msg", "line": "bridge 增量行内容"})

        # tickets.snapshot → 任务页下半面板（覆写渲染，面板挂任务页下栏）
        assert app.tickets_log.master.master is app.task_bottom
        app.obs_q.put({"t": "tickets.snapshot", "text": "# 票板\n- [ ] 项一"})

        # fleet.cleanup.result → 席位页结果摘要行
        app.obs_q.put({"t": "fleet.cleanup.result", "req_id": "r-sm",
                       "results": [{"id": "20d0", "ok": True}]})
        app._drain_obs()
        root.update()

        assert "bridge 增量行内容" in app.bridge_log.get("1.0", "end")
        assert "# 票板" in app.tickets_log.get("1.0", "end")
        assert app.fleet_note_var.get() == "🧹 清理 1/1 成功"

        # 席位表多选 + 清理双按钮已装配
        assert str(app.fleet_tree.cget("selectmode")) == "extended"
        assert app.fleet_release_btn.winfo_exists()
        assert app.fleet_end_btn.winfo_exists()

        # 空选择点击 → 提示早退（不弹确认框）
        app._fleet_cleanup("end")
        assert app.fleet_note_var.get() == "未选中席位"

        # 有选择但观测未开 → 提示早退（不弹确认框、不触网）
        app._render_fleet({"fleet": {
            "<seat>": {"sessionId": "s-a", "alias": "webgui", "node": "voice-head",
                     "role": "orchestrator", "status": "active"}}})
        root.update()
        app.fleet_tree.selection_set(app.fleet_tree.get_children()[0])
        app._fleet_cleanup("release")
        assert "观测连接未开" in app.fleet_note_var.get()
    finally:
        if app is not None and getattr(app, "ptt_listener", None):
            app.ptt_listener.stop()
        root.destroy()


# ---- head 配置面（PR8：head.list/head.switch 纯函数）----


def test_head_request_frames():
    assert head_list_request("r-1") == {"t": "head.list", "req_id": "r-1"}
    assert head_switch_request("echo", "r-2") == \
        {"t": "head.switch", "name": "echo", "req_id": "r-2"}


def test_head_switch_line():
    ok = {"t": "head.switch.result", "ok": True, "active": "echo",
          "note": "下一次语音连接生效"}
    assert head_switch_line(ok) == "head → echo（下一次语音连接生效）"
    assert head_switch_line({"ok": True, "active": "nova", "note": "已是激活 head"}) \
        == "head → nova（已是激活 head）"
    assert head_switch_line({"ok": True, "active": "x"}) == "head → x"  # 无 note
    assert head_switch_line({"ok": False, "reason": "pinned"}).startswith("⚠")
    assert head_switch_line({}).startswith("⚠")


def test_head_names_from_list():
    frame = {"t": "head.list.result", "active": "nova", "file_backed": True,
             "profiles": [{"name": "nova", "label": "Nova·任务助手", "active": True},
                          {"name": "echo", "label": "", "active": False}]}
    rows, active, backed = head_names_from_list(frame)
    assert rows == [("nova", "Nova·任务助手"), ("echo", "echo")]  # 空 label 回落 name
    assert active == "nova" and backed is True
    single = {"active": "default", "file_backed": False,
              "profiles": [{"name": "default", "active": True}]}
    rows2, active2, backed2 = head_names_from_list(single)
    assert rows2 == [("default", "default")] and active2 == "default" and backed2 is False
    # 畸形帧不抛：空表 + 空 active
    assert head_names_from_list({}) == ([], "", False)
    assert head_names_from_list({"profiles": "corrupt"}) == ([], "", False)


def test_connection_bar_stays_visible_above_tabs():
    """回归：连接控制条（连接/观测/结束会话）是窗口级 chrome——先于
    Notebook pack，固定窗高下页签内容再高也不得把它挤没（PR8 head 行
    曾把它压到 0 高）。无显示环境跳过。"""
    try:
        import tkinter as tk
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"no tkinter: {e}")
    try:
        root = tk.Tk()
    except Exception as e:  # noqa: BLE001 — headless 环境
        pytest.skip(f"no display for tkinter: {e}")
    app = None
    try:
        root.geometry("980x640")  # 与生产同参：固定窗高才复现历史挤压
        app = App(root, "ws://127.0.0.1:8765/ws", "", False)
        root.update()
        # 固定窗高（与生产同参）：三钮映射可见、正高度、位于 Notebook 之上
        for btn in (app.conn_btn, app.obs_btn, app.end_btn):
            assert btn.winfo_ismapped(), f"{btn['text']} 未渲染"
            assert btn.winfo_height() > 0, f"{btn['text']} 被压到 0 高"
        assert app.conn_btn.winfo_rooty() <= app.nb.winfo_rooty()
        # 顶栏是 root 首个 pack 的子控件
        assert root.pack_slaves()[0] is app.conn_btn.master
    finally:
        if app is not None:
            try:
                app.root.destroy()
            except Exception:  # noqa: BLE001
                pass


def test_fleet_rows_real_tailer_payload_shape():
    """回归（席位删除坏因）：实线形帧载荷是 {"fleet": <fleet.json 全文>}，
    席位表嵌在 doc["fleet"]——旧解析只读一层，表里只有一条垃圾行，UI
    无法选中真实席位。两级解析 + 扁平合成形兼容都要罩住。"""
    real = {"t": "fleet.snapshot", "fleet": {
        "port": 3080, "defaultWorkspaceId": "60312e7a-ae74",
        "fleet": {
            "<seat>": {"sessionId": "session-3103", "node": "node-<redacted>",
                     "role": "worker", "status": "active"},
            "db05": {"sessionId": "session-db05", "node": "vh-head-liaison",
                     "role": "worker", "preset": "maestro", "status": "active"},
        }}}
    rows = fleet_rows(real)
    assert rows == [
        ("3103", "", "node-<redacted>", "worker", "active"),
        ("db05", "", "vh-head-liaison", "worker", "active"),
    ]
    # 扁平合成形（旧单测/文档形）继续可用
    flat = {"fleet": {"<seat>": {"node": "x", "role": "worker", "status": "active"}}}
    assert fleet_rows(flat) == [("3103", "", "x", "worker", "active")]
    # 畸形不抛
    assert fleet_rows({}) == [] and fleet_rows({"fleet": "corrupt"}) == []
    assert fleet_rows({"fleet": {"fleet": "corrupt"}}) == []


# ---- 席位简报（fleet.brief 纯函数 + 席位页签装配）----


def test_fleet_brief_request_frame():
    assert fleet_brief_request("r-1") == {"t": "fleet.brief", "req_id": "r-1"}


def test_fleet_brief_lines_seats_and_idle_buckets():
    """一行一席：在跑/没跑、title 空省段、live=false 尾注会话已死且无
    闲置段、idle 分钟/小时/天三档（含档位边界）。"""
    result = {"t": "fleet.brief.result", "req_id": "r-1", "seats": [
        {"id": "db05", "node": "vh-head-liaison", "role": "worker",
         "status": "active", "live": True, "running": True,
         "title": "封装统一client", "task": "running", "idle_s": 300},
        {"id": "3103", "node": "node-<redacted>", "role": "worker",
         "status": "active", "live": True, "running": False,
         "title": "", "task": "", "idle_s": 7200},
        {"id": "20d0", "node": "voice-head", "role": "orchestrator",
         "status": "released", "live": False, "running": False,
         "title": "", "task": "", "idle_s": None},
    ]}
    assert fleet_brief_lines(result) == [
        "db05 · vh-head-liaison · 在跑 · 封装统一client · 5分钟前动过",
        "3103 · node-<redacted> · 没跑 · 2小时前",
        "20d0 · voice-head · 没跑 (会话已死)",
    ]
    # idle 三档边界：分钟含 0 与 59:59；小时 1h/23:59:59；天 1d/3d
    edges = {"seats": [
        {"id": "x1", "node": "n", "live": True, "running": True, "idle_s": 0},
        {"id": "x2", "node": "n", "live": True, "running": True, "idle_s": 3599},
        {"id": "x3", "node": "n", "live": True, "running": True, "idle_s": 3600},
        {"id": "x4", "node": "n", "live": True, "running": True, "idle_s": 86399},
        {"id": "x5", "node": "n", "live": True, "running": True, "idle_s": 86400},
        {"id": "x6", "node": "n", "live": True, "running": True, "idle_s": 3 * 86400},
    ]}
    assert fleet_brief_lines(edges) == [
        "x1 · n · 在跑 · 0分钟前动过",
        "x2 · n · 在跑 · 59分钟前动过",
        "x3 · n · 在跑 · 1小时前",
        "x4 · n · 在跑 · 23小时前",
        "x5 · n · 在跑 · 1天前",
        "x6 · n · 在跑 · 3天前",
    ]


def test_fleet_brief_lines_note_and_malformed():
    # note（loopback 降级仅席位表）首行前插 ⚠
    degraded = {"seats": [
        {"id": "db05", "node": "n", "live": True, "running": True,
         "title": "t", "task": "", "idle_s": 30}],
        "note": "dsh 状态不可达，仅席位表"}
    assert fleet_brief_lines(degraded) == [
        "⚠ dsh 状态不可达，仅席位表",
        "db05 · n · 在跑 · t · 0分钟前动过",
    ]
    # 空席位表合法；非 dict 条目与不可读 idle 值防御性跳过
    assert fleet_brief_lines({"seats": []}) == []
    assert fleet_brief_lines({"seats": [], "note": "x"}) == ["⚠ x"]
    assert fleet_brief_lines({"seats": ["x", None]}) == []
    assert fleet_brief_lines({"seats": [
        {"id": "x", "node": "n", "live": True, "running": True,
         "idle_s": "corrupt"}]}) == ["x · n · 在跑"]
    # 畸形回包 → 一行占位，不抛
    assert fleet_brief_lines({}) == ["⚠ 简报回包不可读"]
    assert fleet_brief_lines({"seats": "corrupt"}) == ["⚠ 简报回包不可读"]
    assert fleet_brief_lines(None) == ["⚠ 简报回包不可读"]


class _StubObsLink:
    """观测连接替身：存活检查恒真、send_request 只记录出站帧（不触网）。"""

    def __init__(self):
        self.sent: list[dict] = []
        self._thread = self            # 存活检查走 obs_link._thread.is_alive()

    def is_alive(self):
        return True

    def send_request(self, req: dict):
        self.sent.append(req)


def test_fleet_brief_tab_assembly_smoke():
    """席位简报面装配冒烟（真 tkinter、无网络、无 mainloop）：简报按钮+
    只读小面板挂在席位页；观测未开提示早退；stub 连接下出站帧形状与
    req_id 唯一；fleet.brief.result 入 obs_q 后 _drain_obs 渲染摘要行与
    逐行。无显示环境跳过。"""
    try:
        import tkinter as tk

        root = tk.Tk()
    except Exception as e:  # noqa: BLE001 — headless 环境
        pytest.skip(f"no display for tkinter: {e}")
    root.withdraw()
    app = None
    try:
        app = App(root, "ws://127.0.0.1:8765/ws", "", False)
        root.update()

        # 简报按钮在清理条上；只读小面板挂席位页（内嵌一层 plain Frame）
        assert app.fleet_brief_btn.winfo_exists()
        assert str(app.fleet_brief_btn.cget("text")) == "简报"
        assert app.fleet_brief_log.master.master is app.fleet_tab
        assert str(app.fleet_brief_log.cget("state")) == "disabled"
        assert int(app.fleet_brief_log.cget("height")) == 6

        # 观测未开 → 提示早退（不触网）
        app._fleet_brief()
        assert "观测连接未开" in app.fleet_note_var.get()

        # stub 连接：出站 fleet.brief 帧 + req_id 唯一（seq 递增）
        app.obs_link = _StubObsLink()
        app._fleet_brief()
        app._fleet_brief()
        reqs = [r for r in app.obs_link.sent if r.get("t") == "fleet.brief"]
        assert len(reqs) == 2
        assert all(set(r.keys()) == {"t", "req_id"} for r in reqs)
        assert reqs[0]["req_id"] != reqs[1]["req_id"]
        assert app.fleet_note_var.get() == "已请求席位简报…"

        # 回包入 obs_q → _drain_obs：摘要行 + 面板逐行
        app.obs_q.put({"t": "fleet.brief.result", "req_id": reqs[1]["req_id"],
                       "seats": [
                           {"id": "db05", "node": "vh-head-liaison",
                            "role": "worker", "status": "active", "live": True,
                            "running": True, "title": "封装统一client",
                            "task": "running", "idle_s": 300},
                           {"id": "3103", "node": "node-<redacted>",
                            "role": "worker", "status": "active", "live": True,
                            "running": False, "title": "", "task": "",
                            "idle_s": 7200},
                           {"id": "20d0", "node": "voice-head",
                            "role": "orchestrator", "status": "released",
                            "live": False, "running": False, "title": "",
                            "task": "", "idle_s": None},
                       ]})
        app._drain_obs()
        root.update()
        assert app.fleet_note_var.get() == "3 席位 · 1 在跑"
        panel = app.fleet_brief_log.get("1.0", "end")
        assert "db05 · vh-head-liaison · 在跑 · 封装统一client · 5分钟前动过" in panel
        assert "3103 · node-<redacted> · 没跑 · 2小时前" in panel
        assert "20d0 · voice-head · 没跑 (会话已死)" in panel

        # 畸形回包 → 摘要行降级文案 + 面板占位行
        app.obs_q.put({"t": "fleet.brief.result", "req_id": "r-x",
                       "seats": "corrupt"})
        app._drain_obs()
        root.update()
        assert app.fleet_note_var.get() == "席位简报回包不可读"
        assert "⚠ 简报回包不可读" in app.fleet_brief_log.get("1.0", "end")
    finally:
        if app is not None and getattr(app, "ptt_listener", None):
            app.ptt_listener.stop()
        root.destroy()


# ---- 白板（PR10：协作交互输入面纯函数 + 白板页签装配）----


def test_whiteboard_set_request_frame():
    assert whiteboard_set_request("正文", "r-1") == \
        {"t": "whiteboard.set", "text": "正文", "req_id": "r-1"}
    # 非 str 强转（与网关 str 校验对齐：UI 只发 str，这里兜底）
    assert whiteboard_set_request(123, "r-2")["text"] == "123"
    assert whiteboard_set_request("", "r-3")["text"] == ""


def test_whiteboard_set_line():
    ok = {"t": "whiteboard.set.result", "req_id": "r-1", "ok": True, "chars": 42}
    assert whiteboard_set_line(ok) == "✓ 白板已同步（42 字）"
    over = {"t": "whiteboard.set.result", "req_id": "r-2", "ok": False,
            "reason": "内容超过 65536 字符上限（当前 65537）"}
    assert whiteboard_set_line(over) == "⚠ 白板同步失败：内容超过 65536 字符上限（当前 65537）"
    assert whiteboard_set_line({"ok": False}) == "⚠ 白板同步失败：未知原因"
    assert whiteboard_set_line({}) == "⚠ 白板回包不可读"
    assert whiteboard_set_line(None) == "⚠ 白板回包不可读"


def test_whiteboard_tab_assembly_smoke(monkeypatch):
    """白板页装配冒烟（真 tkinter、无网络、无 mainloop）：可编辑文本区即
    白板本体，无手动同步钮；观测未开提示早退；stub 连接下推送帧带整段
    文本、req_id 唯一；防抖窗口内连续改动合并成一帧末版全文；
    whiteboard.set.result 入 obs_q 后 _drain_obs 渲染结果行。
    无显示环境跳过。"""
    try:
        import tkinter as tk

        root = tk.Tk()
    except Exception as e:  # noqa: BLE001 — headless 环境
        pytest.skip(f"no display for tkinter: {e}")
    root.withdraw()
    app = None
    try:
        app = App(root, "ws://127.0.0.1:8765/ws", "", False)
        root.update()

        # 可编辑文本区（默认 normal）已装配；无手动同步钮（自动同步面）
        assert str(app.wb_text.cget("state")) == "normal"
        assert not hasattr(app, "wb_sync_btn")
        app.wb_text.insert("1.0", "白板正文\n第二行")
        assert app.wb_text.get("1.0", "end-1c") == "白板正文\n第二行"

        # 观测未开 → 推送早退记提示（不触网）
        app._whiteboard_push()
        assert "观测连接未开" in app.whiteboard_note_var.get()

        # stub 连接：推送 whiteboard.set 帧（整段文本）+ req_id 唯一
        app.obs_link = _StubObsLink()
        app._whiteboard_push()
        app.wb_text.delete("1.0", "end")
        app.wb_text.insert("1.0", "第二版")
        app._whiteboard_push()
        reqs = [r for r in app.obs_link.sent if r.get("t") == "whiteboard.set"]
        assert [r["text"] for r in reqs] == ["白板正文\n第二行", "第二版"]
        assert all(set(r.keys()) == {"t", "text", "req_id"} for r in reqs)
        assert reqs[0]["req_id"] != reqs[1]["req_id"]
        assert app.whiteboard_note_var.get() == "白板同步中（3 字）…"

        # 回包入 obs_q → _drain_obs：结果行
        app.obs_q.put({"t": "whiteboard.set.result", "req_id": reqs[1]["req_id"],
                       "ok": True, "chars": 3})
        app._drain_obs()
        root.update()
        assert app.whiteboard_note_var.get() == "✓ 白板已同步（3 字）"

        # 防抖：窗口内两次改动合并一帧、推末版全文（钳短窗口注入）
        monkeypatch.setattr("rt_voice_app.WB_SYNC_DEBOUNCE_MS", 40)
        app.wb_text.delete("1.0", "end")
        app.wb_text.insert("1.0", "改一")
        app.wb_text.insert("end", "改二")
        root.update()                     # 排程 <<Modified>> → 防抖定时器
        assert app._wb_push_job is not None
        for _ in range(20):               # 走完 40ms 窗口
            root.update()
            time.sleep(0.01)
        assert app._wb_push_job is None
        merged = [r for r in app.obs_link.sent
                  if r.get("t") == "whiteboard.set" and r["text"] == "改一改二"]
        assert len(merged) == 1

        # 失败回包 → 原因行
        app.obs_q.put({"t": "whiteboard.set.result", "req_id": "r-x",
                       "ok": False, "reason": "内容超过 65536 字符上限（当前 70000）"})
        app._drain_obs()
        root.update()
        assert app.whiteboard_note_var.get() == \
            "⚠ 白板同步失败：内容超过 65536 字符上限（当前 70000）"
    finally:
        if app is not None and getattr(app, "ptt_listener", None):
            app.ptt_listener.stop()
        root.destroy()


def test_vad_tail_plan_constraints():
    """尾静音计划的两条硬约束：总长 ≥1000ms（服务端 VAD 句尾阈值实测
    >700ms）；任意 1s 滑窗 ≤64KB（网关限速，步进 50ms=32KB/s）。"""
    plan = vad_tail_plan()
    assert [d for d, _ in plan] == [50 * (k + 1) for k in range(30)]
    assert all(n == 1600 for _, n in plan)          # 50ms @16k s16le
    total_ms = sum(n for _, n in plan) * 1000 // (2 * 16000)
    assert total_ms >= 1000
    # 1s 滑窗内最大字节数（步进投递的码率上界）
    events = sorted((d, n) for d, n in plan)
    worst = max(
        sum(n for d, n in events if win <= d < win + 1000)
        for win, _ in events
    )
    assert worst <= 65536
    assert vad_tail_plan(0) == []
    assert vad_tail_plan(700, 0) == []


class _ManualAfter:
    """``root.after``/``after_cancel`` 的确定性替身：只登记 (delay, fn) 不
    排程，测试按 id 手动驱动回调——尾静音节奏断言全走 delay 序列与手动
    fire，不看墙钟（并发负载下不抖）。"""

    def __init__(self):
        self.scheduled: dict = {}
        self.delays: list[int] = []
        self.cancelled: list = []
        self._n = 0

    def __call__(self, delay, fn):
        self._n += 1
        tid = f"after-{self._n}"
        self.scheduled[tid] = fn
        self.delays.append(delay)
        return tid

    def cancel(self, tid):
        self.cancelled.append(tid)
        self.scheduled.pop(tid, None)

    def fire(self, tid):
        fn = self.scheduled.pop(tid, None)
        if fn is not None:
            fn()


def _manual_after(monkeypatch, root, app):
    """把 root.after/after_cancel 换成 _ManualAfter 并返回它。"""
    sched = _ManualAfter()
    monkeypatch.setattr(root, "after", sched)
    monkeypatch.setattr(root, "after_cancel", sched.cancel)
    return sched


def test_send_vad_tail_pacing_and_cancel_on_press(monkeypatch):
    """松键 → 尾静音只排程不落块；手动驱动步进回调验证节奏契约：

    - 不打块：首块非同步投递（排程后 tx 仍空）；
    - 步进 delay 全为 VAD_TAIL_STEP_MS、每步一块 1600 字节；
    - 不打爆：计划步数上限 30，走完后无可驱动回调、队列不再增长；
    - 可取消：再按下 → after_cancel 待发定时器、无新块投递。
    """
    root, app = _tk_app()
    try:
        sched = _manual_after(monkeypatch, root, app)
        app.state_var.set("open")
        base = len(sched.delays)

        app._send_vad_tail()
        assert app._tail_after is not None
        assert app.tx.qsize() == 0                       # 首块 50ms 后才投
        assert sched.delays[base:] == [VAD_TAIL_STEP_MS]  # 首个 delay 即步长

        steps = 0
        while app._tail_after is not None:               # 手动驱动整条步进链
            sched.fire(app._tail_after)
            steps += 1
            assert app.tx.qsize() == min(steps, 30)      # 每个投递步恰一块
            assert all(len(chunk) == 1600 for chunk in app.tx.queue)
            assert sched.delays[base:] == [VAD_TAIL_STEP_MS] * min(steps + 1, 31)
        assert steps == 31                               # 30 个投递步 + 1 个收尾步
        assert app.tx.qsize() == 30                      # 步数上限（1500ms/50ms）
        sched.fire("after-nothing")                      # 计划走完：无可驱动回调
        assert app.tx.qsize() == 30                      # 队列不再增长

        # 取消语义：新尾巴排程后立即按下 → 定时器撤销、零块投递
        while not app.tx.empty():
            app.tx.get_nowait()
        base2 = len(sched.delays)
        app._send_vad_tail()
        tail_id = app._tail_after
        app.ptt.held = True                              # 预持有：press 不开真实麦克风
        app._ptt_press()
        assert app._tail_after is None                   # 尾巴已取消
        assert sched.cancelled == [tail_id]
        sched.fire(tail_id)                              # 已取消的定时器不可再驱动
        assert app.tx.qsize() == 0                       # 无任何新块
        assert sched.delays[base2:] == [VAD_TAIL_STEP_MS]  # 取消后无新排程
    finally:
        _teardown(root, app)


def test_send_vad_tail_step_exception_chain_continues(monkeypatch):
    """(d) _send_vad_tail._step 步进体抛异常 → 记「尾静音步进异常已跳过」
    并继续重排：计划 30 步全部试投、链走完自然结束（_tail_after 归
    None）。确定性驱动（_ManualAfter），不看墙钟。"""
    class _BoomTx:
        calls = 0

        def put_nowait(self, _chunk):
            type(self).calls += 1
            raise RuntimeError("boom")

    root, app = _tk_app()
    try:
        sched = _manual_after(monkeypatch, root, app)
        app.tx = _BoomTx()
        app.state_var.set("open")
        base = len(sched.delays)
        app._send_vad_tail()
        assert app._tail_after is not None
        steps = 0
        while app._tail_after is not None:
            sched.fire(app._tail_after)
            steps += 1
            assert sched.delays[base:] == [VAD_TAIL_STEP_MS] * min(steps + 1, 31)
        assert steps == 31                               # 30 投递步 + 1 收尾步
        assert _BoomTx.calls == 30                       # 异常不断链：投递步全试投
        assert "尾静音步进异常已跳过" in app.log.get("1.0", "end")
    finally:
        _teardown(root, app)


def test_render_heads_does_not_kill_tick():
    """回归：head.list.result 渲染曾用 __init__ 局部名 ttk → NameError 在
    _tick 内抛出，_tick 不再重排，频谱/电平/事件流全体死掉（观测一开就
    触发）。渲染必须成功且 _tick 存活。"""
    try:
        import tkinter as tk

        root = tk.Tk()
    except Exception as e:  # noqa: BLE001 — headless 环境
        pytest.skip(f"no display for tkinter: {e}")
    root.withdraw()
    app = None
    try:
        app = App(root, "ws://127.0.0.1:8765/ws", "", False)
        app.obs_q.put({"t": "head.list.result", "active": "nova", "file_backed": True,
                       "profiles": [{"name": "nova", "label": "Nova·任务助手", "active": True},
                                    {"name": "echo", "label": "Echo·副本", "active": False}]})
        app._drain_obs()                          # 修复前：NameError: ttk
        assert set(app.head_buttons) == {"nova", "echo"}
        assert app.head_var.get() == "nova"
        assert app.head_note_var.get() == "切换对下一次语音连接生效"
        # _tick 泵继续活着：异常路径也不该断（模拟一帧坏数据后再跑一拍）
        app.obs_q.put({"t": "head.list.result", "boom": object()})  # type: ignore[dict-item]
        app._tick()                               # 不抛即存活
        root.update()
    finally:
        if app is not None and getattr(app, "ptt_listener", None):
            app.ptt_listener.stop()
        root.destroy()


def test_spectrum_draws_from_latest_block():
    """频谱契约：_on_audio 置 latest → _tick 画 24 根柱；响亮信号出高柱、
    静音回 2px 基线。经 Canvas 项内省判定，不依赖像素截屏。"""
    try:
        import tkinter as tk

        root = tk.Tk()
    except Exception as e:  # noqa: BLE001 — headless 环境
        pytest.skip(f"no display for tkinter: {e}")
    root.withdraw()
    import numpy as np

    app = None
    try:
        app = App(root, "ws://127.0.0.1:8765/ws", "", False)
        app.canvas.config(width=600, height=240)
        root.update()

        def bar_heights():
            app._tick()
            root.update()
            return [round(app.canvas.coords(i)[3] - app.canvas.coords(i)[1])
                    for i in app.canvas.find_all()]

        t = np.arange(800) / 16000
        app.latest = (np.sin(2 * np.pi * 440 * t) * 20000).astype(np.int16)
        loud = bar_heights()
        assert len(loud) == 24
        assert max(loud) > 20, f"loud tone must raise bars, got {loud}"

        app.latest = np.zeros(800, dtype=np.int16)
        silent = bar_heights()
        assert set(silent) == {2}, f"silence must fall to baseline, got {silent}"
    finally:
        if app is not None and getattr(app, "ptt_listener", None):
            app.ptt_listener.stop()
        root.destroy()


# ---- P1：req_id 贯穿（在途登记 / error 关联回填 / 超时 / 发送失败） ----


def _tk_app():
    """装配真 App（无网络、无 mainloop）；无显示环境跳过。"""
    try:
        import tkinter as tk

        root = tk.Tk()
    except Exception as e:  # noqa: BLE001 — headless 环境
        pytest.skip(f"no display for tkinter: {e}")
    root.withdraw()
    app = App(root, "ws://127.0.0.1:8765/ws", "", False)
    root.update()
    return root, app


def _teardown(root, app):
    if app is not None:
        app._cancel_vad_tail()
        if getattr(app, "ptt_listener", None):
            app.ptt_listener.stop()
    root.destroy()


def test_error_with_req_id_resolves_pending():
    """(a) error 帧带 req_id 且命中 pending → 登记清除 + 按 kind 回填提示
    （清理 → 席位提示行；body.get → 详情右栏 + 在途态清空）。"""
    root, app = _tk_app()
    try:
        app.obs_link = _StubObsLink()
        app._pending_send(cleanup_request(["20d0"], "end", "r-err-1"), "fleet.cleanup")
        app.fleet_note_var.set("已发清理请求（end · 1 个席位）…")
        assert "r-err-1" in app._pending

        app.obs_q.put({"t": "error", "req_id": "r-err-1", "code": "internal",
                       "msg": "fleet.json write failed"})
        app._drain_obs()
        assert app._pending == {}
        assert app.fleet_note_var.get() == "清理失败：internal fleet.json write failed"

        # body.get 命中：右栏回填「拉取失败：…」且在途 ref 清空
        app._pending_send({"t": "body.get", "ref": "vh-e", "req_id": "r-err-2"}, "body.get")
        app._detail_pending = "vh-e"
        app._render_detail_body("（拉取中 vh-e …）")
        app.obs_q.put({"t": "error", "req_id": "r-err-2", "code": "body_miss",
                       "msg": "no stored body for ref vh-e"})
        app._drain_obs()
        assert app._pending == {}
        assert app._detail_pending is None
        assert app.detail_body.get("1.0", "end").strip() == \
            "拉取失败：body_miss no stored body for ref vh-e"
    finally:
        _teardown(root, app)


def test_error_without_req_id_keeps_body_pending():
    """(b) error 帧不带 req_id（或 req_id 未命中）→ 只进编排页日志行：
    body.get 在途态保持、详情右栏不被写入、pending 不清。"""
    root, app = _tk_app()
    try:
        app.obs_link = _StubObsLink()
        app._pending_send({"t": "body.get", "ref": "vh-x", "req_id": "r-body-1"}, "body.get")
        app._detail_pending = "vh-x"
        app._render_detail_body("（拉取中 vh-x …）")

        app.obs_q.put({"t": "error", "code": "body_miss",
                       "msg": "no stored body for ref vh-x"})
        app._drain_obs()
        assert app._detail_pending == "vh-x"                 # 在途保持
        assert "r-body-1" in app._pending                    # 登记保持
        assert app.detail_body.get("1.0", "end").strip() == "（拉取中 vh-x …）"
        assert "body_miss" in app.orch_log.get("1.0", "end")  # 只进日志行

        # req_id 存在但未命中（别的请求/旧回包）→ 同样不动在途与右栏
        app.obs_q.put({"t": "error", "req_id": "r-unknown", "code": "x", "msg": "y"})
        app._drain_obs()
        assert app._detail_pending == "vh-x"
        assert "r-body-1" in app._pending
        assert app.detail_body.get("1.0", "end").strip() == "（拉取中 vh-x …）"
    finally:
        _teardown(root, app)


def test_pending_timeout_sweep_fills_notes():
    """(c) 在途请求超 PENDING_TIMEOUT_S → _pending_sweep（_tick 内调用）
    清登记并回填「<中文名>超时无响应」；未超时不动。"""
    root, app = _tk_app()
    try:
        app.obs_link = _StubObsLink()
        app._pending_send(cleanup_request(["20d0"], "release", "r-t1"), "fleet.cleanup")
        app.fleet_note_var.set("已发清理请求（release · 1 个席位）…")
        app._pending_send({"t": "body.get", "ref": "vh-t", "req_id": "r-t2"}, "body.get")
        app._detail_pending = "vh-t"
        app._render_detail_body("（拉取中 vh-t …）")

        app._pending_sweep()                                 # 时刻未超：不动
        assert set(app._pending) == {"r-t1", "r-t2"}
        assert app.fleet_note_var.get().startswith("已发清理请求")

        # 注入未来时钟：全部超时
        app._pending_sweep(now=time.monotonic() + PENDING_TIMEOUT_S + 0.1)
        assert app._pending == {}
        assert app.fleet_note_var.get() == "清理超时无响应"
        assert app._detail_pending is None
        assert app.detail_body.get("1.0", "end").strip() == "拉取超时无响应"

        # _tick 集成：直改 ts 为过期值，一帧 UI 泵内完成清扫
        app._pending_send(fleet_brief_request("r-t3"), "fleet.brief")
        app._pending["r-t3"]["ts"] = time.monotonic() - PENDING_TIMEOUT_S - 1
        app._pending_send(head_switch_request("nova", "r-t4"), "head.switch")
        app._tick()
        assert "r-t3" not in app._pending
        assert app.fleet_note_var.get() == "席位简报超时无响应"
        assert "r-t4" in app._pending                          # 未超时者保留
    finally:
        _teardown(root, app)


def test_pending_line_builders():
    """kind → 回填行的三形：error/超时/发送失败（含未知 kind 防御）。"""
    err = {"code": "body_miss", "msg": "no stored body for ref vh-x"}
    assert pending_error_line("body.get", err) == \
        "拉取失败：body_miss no stored body for ref vh-x"
    assert pending_error_line("fleet.brief", {"msg": "boom"}) == "席位简报失败：boom"
    assert pending_error_line("fleet.cleanup", {}) == "清理失败：未知错误"
    assert pending_timeout_line("fleet.cleanup") == "清理超时无响应"
    assert pending_timeout_line("head.switch") == "head 切换超时无响应"
    assert pending_timeout_line("body.get") == "拉取超时无响应"
    assert pending_send_fail_line("whiteboard.set") == "白板同步失败：发送失败（链路断开）"
    assert pending_timeout_line("mystery") == "mystery超时无响应"


@pytest.mark.asyncio
async def test_req_pump_send_failure_reports_req_id():
    """_req_pump 发送失败（连接断）→ `_send_failed` 帧进 obs 队列（带受影响
    req_id），不再静默丢；无 req_id 的请求只退出不发帧。"""
    class _DeadWs:
        async def send_json(self, obj):
            raise ConnectionResetError("dead")

    obs: queue.Queue = queue.Queue()
    link = ObserveLink("ws://x/ws", "tok", obs, lambda _s: None)
    ready = asyncio.Event()
    ready.set()
    link.req.put({"t": "fleet.brief", "req_id": "r-dead-1"})
    link.req.put({"t": "body.get", "ref": "vh-noid"})
    pump = asyncio.create_task(link._req_pump(_DeadWs(), ready))
    await asyncio.wait_for(asyncio.shield(pump), timeout=5)   # 泵发送失败即返回
    frames = []
    while not obs.empty():
        frames.append(obs.get_nowait())
    assert frames == [{"_send_failed": "r-dead-1"}]


def test_send_failed_frame_fills_note():
    """`_send_failed` 回填：受影响 req_id 按 kind 落「发送失败（链路断开）」
    并清登记；未知 req_id 只记编排页日志行。"""
    root, app = _tk_app()
    try:
        app.obs_link = _StubObsLink()
        app._pending_send(cleanup_request(["20d0"], "end", "r-d1"), "fleet.cleanup")
        app._pending_send({"t": "body.get", "ref": "vh-d", "req_id": "r-d2"}, "body.get")
        app._detail_pending = "vh-d"
        app._render_detail_body("（拉取中 vh-d …）")

        app.obs_q.put({"_send_failed": "r-d1"})
        app.obs_q.put({"_send_failed": "r-d2"})
        app._drain_obs()
        assert app._pending == {}
        assert app.fleet_note_var.get() == "清理失败：发送失败（链路断开）"
        assert app._detail_pending is None
        assert app.detail_body.get("1.0", "end").strip() == "拉取失败：发送失败（链路断开）"

        app.obs_q.put({"_send_failed": "r-nobody"})
        app._drain_obs()
        assert "发送失败（链路断开）" in app.orch_log.get("1.0", "end")
    finally:
        _teardown(root, app)


# ---- P1：after 循环加固 + liaison 常显 + 席位移出提示 + status 列 ----


def test_whiteboard_push_exception_note(monkeypatch):
    """(e) _whiteboard_push 抛异常 → note 回填「白板同步失败」，不永停
    「同步中」，且无出站帧。"""
    root, app = _tk_app()
    try:
        app.obs_link = _StubObsLink()

        def _boom(*_a, **_k):
            raise RuntimeError("wb boom")

        monkeypatch.setattr(app.wb_text, "get", _boom)
        app._whiteboard_push()
        assert app.whiteboard_note_var.get() == "白板同步失败"
        assert app.obs_link.sent == []
    finally:
        _teardown(root, app)


def test_liaison_line_states():
    """(f) liaison 三态 + 畸形防御：无绑定 / 席位码 / 席位码（已归档）。"""
    assert liaison_line({"bound": False, "code": None, "sessionId": None,
                         "archived": False}) == "对接席位：无绑定"
    assert liaison_line({"bound": True, "code": "3103", "sessionId": "s-1",
                         "archived": False}) == "对接席位：3103"
    assert liaison_line({"bound": True, "code": "db05", "sessionId": "s-2",
                         "archived": True}) == "对接席位：db05（已归档）"
    assert liaison_line(None) == "对接席位：未知"
    assert liaison_line("corrupt") == "对接席位：未知"


def test_liaison_var_refresh_on_brief():
    """(f) 席位页常显行：brief.result 带 liaison 即刷新；旧网关（无字段）
    不覆盖现有显示；初始态「未知」。"""
    root, app = _tk_app()
    try:
        assert app.liaison_var.get() == "对接席位：未知"

        app.obs_q.put({"t": "fleet.brief.result", "req_id": "r-l1", "seats": [],
                       "liaison": {"bound": True, "code": "db05",
                                   "sessionId": "s-1", "archived": True}})
        app._drain_obs()
        assert app.liaison_var.get() == "对接席位：db05（已归档）"

        app.obs_q.put({"t": "fleet.brief.result", "req_id": "r-l2", "seats": [],
                       "liaison": {"bound": False, "code": None,
                                   "sessionId": None, "archived": False}})
        app._drain_obs()
        assert app.liaison_var.get() == "对接席位：无绑定"

        # 旧网关回包无 liaison 字段 → 保持上次显示
        app.obs_q.put({"t": "fleet.brief.result", "req_id": "r-l3", "seats": []})
        app._drain_obs()
        assert app.liaison_var.get() == "对接席位：无绑定"
    finally:
        _teardown(root, app)


def test_fleet_snapshot_removal_note():
    """(g) snapshot 前后 diff：上次存在的席位码消失 → 事件区一条「席位
    <code> 已移出」（去重键 removed:<code>：同码反复移出归并一条刷新到
    最新位）；首帧无基线不 diff；席位表随之刷新。"""
    root, app = _tk_app()
    try:
        seat = {"sessionId": "s-a", "node": "voice-head",
                "role": "worker", "status": "active"}
        app.obs_q.put({"t": "fleet.snapshot", "fleet": {"<seat>": seat, "<seat>": seat}})
        app._drain_obs()
        assert app.fleet_event_log.get("1.0", "end").strip() == ""  # 首帧无基线

        app.obs_q.put({"t": "fleet.snapshot", "fleet": {"<seat>": seat}})
        app._drain_obs()
        assert app.fleet_event_log.get("1.0", "end") == "▸ 席位 9b95 已移出\n"
        assert len(app.fleet_tree.get_children()) == 1

        # 席位回来再消失：同码归并一条（不堆积重复行），另一码新增一条；
        # 两码同帧移出按码序入史，后入者（9b95）为最新
        app.obs_q.put({"t": "fleet.snapshot", "fleet": {"<seat>": seat, "<seat>": seat}})
        app._drain_obs()
        app.obs_q.put({"t": "fleet.snapshot", "fleet": {}})
        app._drain_obs()
        assert app.fleet_event_log.get("1.0", "end") == \
            "▸ 席位 9b95 已移出\n席位 20d0 已移出\n"
        assert len(app.fleet_tree.get_children()) == 0
    finally:
        _teardown(root, app)


def test_detail_status_column():
    """(h) 任务表 status 列：DETAIL_COLS 含 status；行数据带 status 显示
    （如 cancelled）；缺 status 键 → 空串不崩。"""
    assert "status" in DETAIL_COLS
    assert DETAIL_COLS.index("chars") < DETAIL_COLS.index("status")
    root, app = _tk_app()
    try:
        now = time.time()
        app.obs_q.put({"t": "body.push", "ref": "vh-s1", "no": 1, "status": "cancelled",
                       "title": "已取消任务", "chars": 5, "ts": now})
        app.obs_q.put({"t": "body.push", "ref": "vh-s2", "no": 2, "title": "无状态",
                       "chars": 6, "ts": now})              # 缺 status 键
        app._drain_obs()
        root.update()
        si = DETAIL_COLS.index("status")
        assert app.detail_tree.item("vh-s1", "values")[si] == "cancelled"
        assert app.detail_tree.item("vh-s2", "values")[si] == ""
        assert list(app.detail_tree.get_children()) == ["vh-s1", "vh-s2"]
    finally:
        _teardown(root, app)


# ---- body.item 回包清在途：误清防护（发起序号校验） ----


def test_body_item_old_reply_clears_only_corresponding_pending():
    """同 ref 两次在途（发起序 1 旧、2 新），旧回包先到 → 仅清对应者 b1；
    同 ref 的更新在途 b2 保留（超时/错误回填保护不丢）；重复旧回包是
    no-op；纯 ref 形回包按 FIFO 清最老一条。"""
    root, app = _tk_app()
    try:
        app.obs_link = _StubObsLink()
        app._pending_send({"t": "body.get", "ref": "vh-dup", "req_id": "b1"},
                          "body.get", seq=1)
        app._pending_send({"t": "body.get", "ref": "vh-dup", "req_id": "b2"},
                          "body.get", seq=2)
        app._pending_send({"t": "body.get", "ref": "vh-other", "req_id": "b3"},
                          "body.get", seq=3)

        # 旧回包（req_id 命中 seq=1 者）→ 只清 b1，b2/b3 在途保持
        app.obs_q.put({"t": "body.item", "ref": "vh-dup", "req_id": "b1",
                       "text": "旧文"})
        app._drain_obs()
        assert set(app._pending) == {"b2", "b3"}
        assert app.detail_bodies["vh-dup"] == "旧文"     # 全文照常缓存

        # 重复旧回包（同 req_id 再到，未命中）→ 幂等 no-op，不动其余在途
        app.obs_q.put({"t": "body.item", "ref": "vh-dup", "req_id": "b1",
                       "text": "旧文"})
        app._drain_obs()
        assert set(app._pending) == {"b2", "b3"}

        # 新回包 → 清 b2；纯 ref 形（无 req_id）→ FIFO 清最老一条（b3）
        app.obs_q.put({"t": "body.item", "ref": "vh-dup", "req_id": "b2",
                       "text": "新文"})
        app._drain_obs()
        assert set(app._pending) == {"b3"}
        app.obs_q.put({"t": "body.item", "ref": "vh-other", "text": "另文"})
        app._drain_obs()
        assert app._pending == {}
    finally:
        _teardown(root, app)


# ---- 席位事件历史（去重键 + 最近 8 条裁剪） ----


def test_fleet_event_history_trim_and_dedup():
    """事件历史：超 FLEET_EVENT_KEEP 条裁剪稳定——重复补投「存留集」后
    顺序与渲染同值（第二次 no-op）；同一事件重复到达按去重键归并不
    堆积。"""
    root, app = _tk_app()
    try:
        for i in range(10):                              # 10 条不同主体事件
            app._fleet_event_add(f"removed:code{i}", f"席位 code{i} 已移出")
        assert [e["key"] for e in app._fleet_events] == \
            [f"removed:code{i}" for i in range(2, 10)]   # 只留最近 8 条
        first = app.fleet_event_log.get("1.0", "end")
        assert first.splitlines()[0] == "▸ 席位 code9 已移出"   # 最新置顶标 ▸
        assert "code0" not in first and "code1" not in first

        # 幂等：重复补投存留的 8 条 → 事件序与渲染同值（no-op）
        for i in range(2, 10):
            app._fleet_event_add(f"removed:code{i}", f"席位 code{i} 已移出")
        assert [e["key"] for e in app._fleet_events] == \
            [f"removed:code{i}" for i in range(2, 10)]
        assert app.fleet_event_log.get("1.0", "end") == first

        # 同一事件重复到达：归并一条、刷新到最新位（不堆积重复行）
        for _ in range(3):
            app._fleet_event_add("brief", "3 席位 · 1 在跑")
        assert [e for e in app._fleet_events if e["key"] == "brief"] == \
            [{"key": "brief", "line": "3 席位 · 1 在跑"}]
        assert app.fleet_event_log.get("1.0", "end").splitlines()[0] == \
            "▸ 3 席位 · 1 在跑"
        assert FLEET_EVENT_KEEP == 8
    finally:
        _teardown(root, app)


# ---- 任务历史翻页（body.list_more） ----


def test_body_list_more_request_frame():
    assert body_list_more_request(123.5, "r-1") == \
        {"t": "body.list_more", "req_id": "r-1", "before_ts": 123.5, "limit": 50}
    assert body_list_more_request(7, "r-2", 20)["limit"] == 20
    assert body_list_more_request("9", "r-3")["before_ts"] == 9.0   # float 归一


def test_detail_ts_map_shapes():
    assert detail_ts_map({"ref": "vh-a", "ts": 1700000000.0}) == \
        {"vh-a": 1700000000.0}
    batch = {"items": [{"ref": "vh-b", "ts": 1}, {"ref": "vh-c"},   # 无 ts 跳过
                        {"ts": 9}]}                                  # 无 ref 跳过
    assert detail_ts_map(batch) == {"vh-b": 1.0}
    assert detail_ts_map({}) == {}
    assert detail_ts_map(None) == {}


def test_task_list_more_pagination_and_eof():
    """翻页游标=表内最旧 ts；回包行追加（ref+no 同行原位同值，不增行）；
    同 before_ts 重复拉取行集不增不减；eof 后按钮置灰、再点无请求。"""
    root, app = _tk_app()
    try:
        # 观测未开 → 提示早退；开了但无行 → 提示早退
        app._task_list_more()
        assert "观测连接未开" in app.task_note_var.get()
        app.obs_link = _StubObsLink()
        app._task_list_more()
        assert app.task_note_var.get() == "暂无可翻页的台账行"

        # 种子两行（ts 700/800）→ 游标取 min=700
        app.obs_q.put({"t": "body.push", "ref": "vh-a", "no": 1, "status": "done",
                       "title": "a", "chars": 1, "ts": 700})
        app.obs_q.put({"t": "body.push", "ref": "vh-b", "no": 2, "status": "done",
                       "title": "b", "chars": 2, "ts": 800})
        app._drain_obs()
        root.update()
        assert app._oldest_detail_ts() == 700.0

        page = {"t": "body.list_more.result", "eof": False, "items": [
            {"ref": "vh-c", "no": 3, "status": "done", "title": "c",
             "chars": 3, "ts": 750},
            {"ref": "vh-d", "no": 4, "status": "done", "title": "d",
             "chars": 4, "ts": 780}]}

        # 首次拉取：帧形状（before_ts=700/limit=50）+ 行追加
        app._task_list_more()
        reqs = [r for r in app.obs_link.sent if r.get("t") == "body.list_more"]
        assert len(reqs) == 1
        assert reqs[0] == {"t": "body.list_more", "req_id": reqs[0]["req_id"],
                           "before_ts": 700.0, "limit": 50}
        app.obs_q.put(page | {"req_id": reqs[0]["req_id"]})
        app._drain_obs()
        root.update()
        assert list(app.detail_tree.get_children()) == ["vh-a", "vh-b", "vh-c", "vh-d"]
        assert app.task_note_var.get() == "追加 2 行"
        assert str(app.task_more_btn.cget("state")) == "normal"

        # 幂等：同 before_ts（游标仍 700）重复拉取同一页 → 行集不增不减
        rows_once = list(app.detail_tree.get_children())
        app._task_list_more()
        reqs2 = [r for r in app.obs_link.sent if r.get("t") == "body.list_more"]
        assert len(reqs2) == 2 and reqs2[1]["before_ts"] == 700.0
        app.obs_q.put(page | {"req_id": reqs2[1]["req_id"]})
        app._drain_obs()
        root.update()
        assert list(app.detail_tree.get_children()) == rows_once
        assert [tuple(app.detail_tree.item(i, "values")) for i in rows_once] == \
            [tuple(app.detail_tree.item(i, "values")) for i in rows_once]

        # eof：按钮置灰 + 提示到底；再点（直接调）无请求发出
        app._task_list_more()
        reqs3 = [r for r in app.obs_link.sent if r.get("t") == "body.list_more"]
        assert len(reqs3) == 3
        app.obs_q.put({"t": "body.list_more.result", "req_id": reqs3[2]["req_id"],
                       "items": [], "eof": True})
        app._drain_obs()
        root.update()
        assert str(app.task_more_btn.cget("state")) == "disabled"
        assert app.task_note_var.get() == "历史已到底"
        n_sent = len(app.obs_link.sent)
        app._task_list_more()
        assert len(app.obs_link.sent) == n_sent           # 无请求发出
    finally:
        _teardown(root, app)


# ---- 席位陈旧指示（快照标签）+ last_seen 列 ----


def test_last_seen_label():
    epoch = 1700000000
    assert last_seen_label(epoch) == \
        time.strftime("%H:%M:%S", time.localtime(epoch))
    assert last_seen_label(None) == "—"
    assert last_seen_label("") == "—"
    assert last_seen_label("12:00:00Z") == "12:00:00Z"    # 文本原样
    assert last_seen_label(True) == "—"


def test_fleet_stale_label_and_last_seen():
    """快照标签：阈内常显「快照 HH:MM:SS」；注入时钟超 FLEET_STALE_S 切
    「（可能陈旧）」；重收快照复位。last_seen 列 brief 回填、缺省「—」。"""
    root, app = _tk_app()
    try:
        assert app.fleet_snap_var.get() == "快照 —"       # 未收快照占位
        seat = {"sessionId": "s-a", "node": "voice-head",
                "role": "worker", "status": "active"}
        app.obs_q.put({"t": "fleet.snapshot", "fleet": {"<seat>": seat}})
        app._drain_obs()
        snap = app._fleet_snap_ts
        assert snap is not None
        when = time.strftime("%H:%M:%S", time.localtime(snap))

        app._tick(now=snap + FLEET_STALE_S)               # 阈界（含）不陈旧
        assert app.fleet_snap_var.get() == f"快照 {when}"
        app._tick(now=snap + FLEET_STALE_S + 0.001)       # 注入时钟推进 → 陈旧
        assert app.fleet_snap_var.get() == f"快照 {when}（可能陈旧）"

        # 重收快照复位：陈旧标记消失
        app.obs_q.put({"t": "fleet.snapshot", "fleet": {"<seat>": seat}})
        app._drain_obs()
        snap2 = app._fleet_snap_ts
        when2 = time.strftime("%H:%M:%S", time.localtime(snap2))
        app._tick(now=snap2 + 1)
        assert app.fleet_snap_var.get() == f"快照 {when2}"
        assert "可能陈旧" not in app.fleet_snap_var.get()

        # last_seen 列：快照行缺省「—」；brief 回填数值时刻（重绘后行
        # iid 重新分配，按当前 children 取）
        li = ("code", "alias", "node", "role", "status", "last_seen").index(
            "last_seen")
        assert app.fleet_tree.item(app.fleet_tree.get_children()[0],
                                   "values")[li] == "—"
        app.obs_q.put({"t": "fleet.brief.result", "req_id": "r-ls", "seats": [
            {"id": "3103", "node": "voice-head", "role": "worker",
             "status": "active", "live": True, "running": True,
             "title": "t", "task": "", "idle_s": 5, "last_seen": snap2},
            {"id": "99aa", "node": "x", "live": False, "running": False}]})
        app._drain_obs()
        assert app.fleet_tree.item(app.fleet_tree.get_children()[0],
                                   "values")[li] == \
            time.strftime("%H:%M:%S", time.localtime(snap2))
        # 同值 brief 重放 → 列值不变（幂等）
        app.obs_q.put({"t": "fleet.brief.result", "req_id": "r-ls2", "seats": [
            {"id": "3103", "node": "voice-head", "role": "worker",
             "status": "active", "live": True, "running": True,
             "title": "t", "task": "", "idle_s": 5, "last_seen": snap2}]})
        app._drain_obs()
        assert app.fleet_tree.item(app.fleet_tree.get_children()[0],
                                   "values")[li] == \
            time.strftime("%H:%M:%S", time.localtime(snap2))
    finally:
        _teardown(root, app)


# ---- liaison 运维入口（unbind / bind） ----


def test_liaison_request_frames():
    assert liaison_unbind_request("r-1") == {"t": "liaison.unbind", "req_id": "r-1"}
    assert liaison_bind_request("3103", "r-2") == \
        {"t": "liaison.bind", "code": "3103", "req_id": "r-2"}
    assert liaison_bind_request(20, "r-3")["code"] == "20"    # str 归一


def test_liaison_result_line_states():
    assert liaison_result_line({"t": "liaison.result", "req_id": "r-1",
                                "op": "unbind", "ok": True,
                                "was_bound": False}) == "对接解绑：本就无绑定"
    assert liaison_result_line({"op": "unbind", "ok": True,
                                "was_bound": True}) == "对接解绑：已解绑"
    bind_ok = {"op": "bind", "ok": True,
               "liaison": {"bound": True, "code": "3103", "sessionId": "s-1",
                           "archived": False}}
    assert liaison_result_line(bind_ok) == "对接绑定：3103"
    archived = {"op": "bind", "ok": True,
                "liaison": {"bound": True, "code": "db05", "sessionId": "s-2",
                            "archived": True}}
    assert liaison_result_line(archived) == "对接绑定：db05（已归档）"
    assert liaison_result_line({"op": "bind", "ok": False,
                                "error": "seat-not-found"}) == \
        "⚠ 对接绑定失败：seat-not-found"
    assert liaison_result_line({"op": "unbind", "ok": False}) == \
        "⚠ 对接解绑失败：未知错误"
    assert liaison_result_line({}) == "⚠ 对接回包不可读"
    assert liaison_result_line(None) == "⚠ 对接回包不可读"


def test_liaison_ops_tab_assembly(monkeypatch):
    """席位页对接运维装配：双按钮；未选席位/观测未开早退；确认门可注入；
    出站帧形状；回包按 op 回填事件区（未绑定解绑→「本就无绑定」不报错、
    bind ok:false→error 原文、重复回包不堆积）。"""
    root, app = _tk_app()
    try:
        assert app.liaison_unbind_btn.winfo_exists()
        assert app.liaison_bind_btn.winfo_exists()

        app._liaison_unbind()                            # 观测未开 → 早退
        assert "观测连接未开" in app.fleet_note_var.get()
        app.obs_link = _StubObsLink()
        app._liaison_bind_selected()                     # 未选席位 → 早退
        assert app.fleet_note_var.get() == "未选中席位"

        monkeypatch.setattr("tkinter.messagebox.askyesno", lambda *a, **k: True)
        seat = {"sessionId": "s-a", "node": "voice-head",
                "role": "worker", "status": "active"}
        app.obs_q.put({"t": "fleet.snapshot", "fleet": {"<seat>": seat}})
        app._drain_obs()
        app.fleet_tree.selection_set(app.fleet_tree.get_children()[0])

        app._liaison_unbind()
        unbind_reqs = [r for r in app.obs_link.sent if r.get("t") == "liaison.unbind"]
        assert len(unbind_reqs) == 1 and set(unbind_reqs[0].keys()) == {"t", "req_id"}
        app._liaison_bind_selected()
        bind_reqs = [r for r in app.obs_link.sent if r.get("t") == "liaison.bind"]
        assert bind_reqs == [{"t": "liaison.bind", "code": "3103",
                              "req_id": bind_reqs[0]["req_id"]}]

        # 未绑定时解绑 → 事件区/提示「本就无绑定」，常显行同步无绑定
        app.obs_q.put({"t": "liaison.result", "req_id": unbind_reqs[0]["req_id"],
                       "op": "unbind", "ok": True, "was_bound": False})
        app._drain_obs()
        assert app.fleet_note_var.get() == "对接解绑：本就无绑定"
        assert app.liaison_var.get() == "对接席位：无绑定"
        assert "对接解绑：本就无绑定" in app.fleet_event_log.get("1.0", "end")
        # 同 req_id 重复回包 → 事件区不重复堆积
        app.obs_q.put({"t": "liaison.result", "req_id": unbind_reqs[0]["req_id"],
                       "op": "unbind", "ok": True, "was_bound": False})
        app._drain_obs()
        assert app.fleet_event_log.get("1.0", "end").count("本就无绑定") == 1

        # bind ok:false → 事件区 error 原文（置顶最新）
        app.obs_q.put({"t": "liaison.result", "req_id": bind_reqs[0]["req_id"],
                       "op": "bind", "ok": False, "error": "seat-not-found"})
        app._drain_obs()
        assert app.fleet_event_log.get("1.0", "end").splitlines()[0] == \
            "▸ ⚠ 对接绑定失败：seat-not-found"
        assert app.fleet_note_var.get() == "⚠ 对接绑定失败：seat-not-found"

        # bind ok → 常显行刷新 + 事件
        app.obs_q.put({"t": "liaison.result", "req_id": "r-bind-2",
                       "op": "bind", "ok": True,
                       "liaison": {"bound": True, "code": "3103",
                                   "sessionId": "s-9", "archived": False}})
        app._drain_obs()
        assert app.liaison_var.get() == "对接席位：3103"
        assert "对接绑定：3103" in app.fleet_event_log.get("1.0", "end")
    finally:
        _teardown(root, app)


# ---- 任务取消入口（run.cancel） ----


def test_run_cancel_request_and_result_lines():
    assert run_cancel_request("vh-1", "r-1") == \
        {"t": "run.cancel", "ref": "vh-1", "req_id": "r-1"}
    ok = {"t": "run.cancel.result", "req_id": "r-1", "ref": "vh-1",
          "ok": True, "state": "cancelled"}
    assert run_cancel_result_line(ok) == "已取消 vh-1（cancelled）"
    assert run_cancel_result_line({"req_id": "r-2", "ref": "vh-1",
                                   "ok": True}) == "已取消 vh-1（cancelled）"
    assert run_cancel_result_line({"req_id": "r-3", "ref": "vh-x", "ok": False,
                                   "error": "unknown-ref"}) == \
        "⚠ 取消失败：unknown-ref"
    assert run_cancel_result_line({"ok": False}) == "⚠ 取消失败：未知错误"
    assert run_cancel_result_line({}) == "⚠ 取消回包不可读"
    assert run_cancel_result_line(None) == "⚠ 取消回包不可读"


def test_task_cancel_flow_idempotent(monkeypatch):
    """取消流：确认门 → run.cancel{ref}；ok 回包行状态转 cancelled、按钮
    置灰；同一 ref 连续第二次取消不再发底层取消（重复 ok 回包终态不
    变）；ok:false 显示 error 原文且行状态不动。"""
    root, app = _tk_app()
    try:
        app._task_cancel()                               # 未选中 → 提示
        assert app.task_note_var.get() == "未选中任务行"

        app.obs_q.put({"t": "body.push", "ref": "vh-run", "no": 1,
                       "status": "running", "title": "t", "chars": 5,
                       "ts": 1700000000})
        app._drain_obs()
        root.update()
        app.detail_tree.selection_set("vh-run")
        app._task_cancel()                               # 观测未开 → 提示
        assert "观测连接未开" in app.task_note_var.get()

        app.obs_link = _StubObsLink()
        monkeypatch.setattr("tkinter.messagebox.askyesno", lambda *a, **k: True)
        si = DETAIL_COLS.index("status")
        assert str(app.task_cancel_btn.cget("state")) == "normal"

        app._task_cancel()                               # 首次取消 → 发帧
        reqs = [r for r in app.obs_link.sent if r.get("t") == "run.cancel"]
        assert len(reqs) == 1 and reqs[0]["ref"] == "vh-run"
        app.obs_q.put({"t": "run.cancel.result", "req_id": reqs[0]["req_id"],
                       "ref": "vh-run", "ok": True, "state": "cancelled"})
        app._drain_obs()
        root.update()
        assert app.detail_tree.item("vh-run", "values")[si] == "cancelled"
        assert str(app.task_cancel_btn.cget("state")) == "disabled"
        assert app.task_note_var.get() == "已取消 vh-run（cancelled）"

        # 幂等：第二次取消（同一 ref）→ 不重复发；迟到同形 ok 回包不改终态
        app._task_cancel()
        assert len([r for r in app.obs_link.sent
                    if r.get("t") == "run.cancel"]) == 1
        assert app.task_note_var.get() == "vh-run 已取消"
        app.obs_q.put({"t": "run.cancel.result", "req_id": "r-late",
                       "ref": "vh-run", "ok": True, "state": "cancelled"})
        app._drain_obs()
        assert app.detail_tree.item("vh-run", "values")[si] == "cancelled"
        assert str(app.task_cancel_btn.cget("state")) == "disabled"

        # ok:false（unknown-ref）→ error 原文，行状态不动、按钮可再点
        app.obs_q.put({"t": "body.push", "ref": "vh-run2", "no": 2,
                       "status": "running", "title": "t2", "chars": 6,
                       "ts": 1700000060})
        app._drain_obs()
        root.update()
        app.detail_tree.selection_set("vh-run2")
        app._task_cancel()
        reqs2 = [r for r in app.obs_link.sent if r.get("t") == "run.cancel"]
        assert len(reqs2) == 2
        app.obs_q.put({"t": "run.cancel.result", "req_id": reqs2[1]["req_id"],
                       "ref": "vh-run2", "ok": False, "error": "unknown-ref"})
        app._drain_obs()
        assert app.task_note_var.get() == "⚠ 取消失败：unknown-ref"
        assert app.detail_tree.item("vh-run2", "values")[si] == "running"
        assert str(app.task_cancel_btn.cget("state")) == "normal"
    finally:
        _teardown(root, app)


# ---- error 帧新契约（type/message 优先，兜底 code/msg） ----


def test_error_identity_prefers_type_message():
    new = {"t": "error", "type": "error", "req_id": "r-1", "message": "body miss"}
    assert error_identity(new) == ("error", "body miss")
    old = {"t": "error", "code": "body_miss", "msg": "no stored body"}
    assert error_identity(old) == ("body_miss", "no stored body")
    both = {"t": "error", "type": "error", "message": "新消息",
            "code": "body_miss", "msg": "旧消息"}
    assert error_identity(both) == ("error", "新消息")   # 新字段优先
    assert error_identity({}) == ("", "")
    assert error_identity(None) == ("", "")
    # 渲染行同口径（右栏提示 / pending 回填）
    assert detail_error_line(new) == "⚠ error body miss"
    assert pending_error_line("body.get", new) == "拉取失败：error body miss"
    assert detail_error_line({}) == "⚠ 未知错误"
    assert pending_error_line("fleet.cleanup", {}) == "清理失败：未知错误"
