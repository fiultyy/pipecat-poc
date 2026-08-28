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
    ObserveLink,
    PushToTalk,
    chars_mag,
    cleanup_request,
    cleanup_result_line,
    detail_ack_line,
    detail_error_line,
    detail_rows_from_push,
    fleet_brief_lines,
    fleet_brief_request,
    fleet_rows,
    head_list_request,
    head_names_from_list,
    head_switch_line,
    head_switch_request,
    merge_detail_rows,
    notice_line_from_push,
    orch_tree_lines,
    replay_notice_line,
    tickets_text,
    turn_label_with_notify,
    turn_line,
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
    assert detail_rows_from_push(single) == [(when, "3", "vh-a", "标题一", "1234")]
    replay = {"t": "body.push", "items": [
        {"ref": "vh-b", "no": 1, "status": "done", "title": "b", "chars": 9, "ts": ts},
        {"no": 8, "status": "done", "title": "无 ref 跳过"},
        "corrupt",
        {"ref": "vh-c", "status": "failed", "title": "c", "chars": 0},
    ], "ts": ts}
    assert detail_rows_from_push(replay) == [
        (when, "1", "vh-b", "b", "9"),
        ("", "", "vh-c", "c", "0"),
    ]
    assert detail_rows_from_push({}) == []
    assert detail_rows_from_push({"items": "corrupt"}) == []
    assert detail_rows_from_push(None) == []


def test_merge_detail_rows_dedup_by_ref():
    prev = [("10:00:00", "1", "vh-a", "旧标题", "5"), ("10:01:00", "2", "vh-b", "b", "6")]
    # 同 ref 更新：原位换值（含 time——行显示最新推送态）、行不跳动
    merged = merge_detail_rows(prev, [("10:02:00", "1", "vh-a", "新标题", "90")])
    assert merged == [("10:02:00", "1", "vh-a", "新标题", "90"),
                      ("10:01:00", "2", "vh-b", "b", "6")]
    # 新 ref 追加尾部
    merged2 = merge_detail_rows(merged, [("10:03:00", "7", "vh-z", "z", "1")])
    assert len(merged2) == 3 and merged2[-1] == ("10:03:00", "7", "vh-z", "z", "1")


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


def test_whiteboard_tab_assembly_smoke():
    """白板页装配冒烟（真 tkinter、无网络、无 mainloop）：可编辑文本区+
    同步按钮；观测未开提示早退；stub 连接下出站帧带整段文本、req_id
    唯一；whiteboard.set.result 入 obs_q 后 _drain_obs 渲染结果行。
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

        # 可编辑文本区（默认 normal）+ 同步按钮已装配
        assert app.wb_sync_btn.winfo_exists()
        assert str(app.wb_text.cget("state")) == "normal"
        app.wb_text.insert("1.0", "白板正文\n第二行")
        assert app.wb_text.get("1.0", "end-1c") == "白板正文\n第二行"

        # 观测未开 → 提示早退（不触网）
        app._whiteboard_sync()
        assert "观测连接未开" in app.whiteboard_note_var.get()

        # stub 连接：出站 whiteboard.set 帧（整段文本）+ req_id 唯一
        app.obs_link = _StubObsLink()
        app._whiteboard_sync()
        app.wb_text.delete("1.0", "end")
        app.wb_text.insert("1.0", "第二版")
        app._whiteboard_sync()
        reqs = [r for r in app.obs_link.sent if r.get("t") == "whiteboard.set"]
        assert [r["text"] for r in reqs] == ["白板正文\n第二行", "第二版"]
        assert all(set(r.keys()) == {"t", "text", "req_id"} for r in reqs)
        assert reqs[0]["req_id"] != reqs[1]["req_id"]
        assert app.whiteboard_note_var.get() == "已发同步请求（3 字）…"

        # 回包入 obs_q → _drain_obs：结果行
        app.obs_q.put({"t": "whiteboard.set.result", "req_id": reqs[1]["req_id"],
                       "ok": True, "chars": 3})
        app._drain_obs()
        root.update()
        assert app.whiteboard_note_var.get() == "✓ 白板已同步（3 字）"

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
