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
  (cleanup_request frame, cleanup_result_line summary, tickets_text).
- Tab assembly smoke (real tkinter widgets, no network, no mainloop):
  detail/task tab and the fleet cleanup control face.
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

        # 页签序：语音/编排/回合/席位/任务（消息/票板、详情页已整合）
        assert [app.nb.tab(t, "text").strip() for t in app.nb.tabs()] == \
            ["语音", "编排", "回合", "席位", "任务"]

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
