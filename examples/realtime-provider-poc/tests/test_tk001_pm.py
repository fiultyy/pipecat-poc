#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-001 · tk 客户端 PM 协议接入测试（<internal-repo> spec-tk-client §TK-001）.

替身验证门：假 SSE 源 + 临时 pm.port + 进程内真 rt_gateway（同 GW-002
夹具），零真实服务依赖。真服务联合门在编排者排期后跑（rt_gate_tk001）。

- 纯函数面：退避曲线、pm.res/pm.event/横幅文案、会话配置 JSON（可丢）、
  pm 帧 id 自增不重放
- ObserveLink 集成面：会话就绪自动 pm.sub；pm.req 透传往返；gateway
  重启 → 指数退避重连 → 自动重订（新 id、不重放已确认请求）；上游
  事件流断 → 延迟自动重订（同连接恢复）
"""

from __future__ import annotations

import asyncio
import json
import os
import queue
import re
import sys
import tempfile
import time
import unittest
from pathlib import Path

POC_DIR = Path(__file__).resolve().parents[1]
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import aiohttp  # noqa: E402
from aiohttp import web  # noqa: E402

import rt_gateway  # noqa: E402
import rt_voice_app as rva  # noqa: E402

TOKEN = "t-tk001"
KIND = "fleet"


class FakePMSSE:
    """替身 pm-host-service：GET /subscribe → SSE（快照先行+增量）+
    GET /health（pm.req 透传目标）。结构同 test_gw002_pm。"""

    def __init__(self) -> None:
        self.snapshot: list[dict] = []
        self.subscribers: list[asyncio.Queue] = []
        self.active = 0
        self.tickets_payload: dict = {
            "op": "tickets", "count": 2, "cache": "miss", "degraded": False,
            "note": "", "signature": "fake-sig-1",
            "tickets": [
                {"ticket_id": "T-A", "title": "进行中票", "state": "running",
                 "deps": "[\"T-B\"]", "lease_owner": "w-1",
                 "refs": "{\"evidence\": \"docs/a.md\"}", "outcome": None,
                 "updated_at": "2026-08-29T10:00:00+00:00"},
                {"ticket_id": "T-B", "title": "已合并票", "state": "merged",
                 "deps": "[]", "lease_owner": None, "refs": "{}",
                 "outcome": "已收口", "updated_at": "2026-08-29T09:00:00+00:00"},
            ],
        }
        self.fleet_payload: dict = {
            "op": "fleet", "count": 2, "degraded": False,
            "sessionJoined": False, "note": "",
            "seats": [
                {"code": "0699", "sessionId": "s-0699", "role": "worker",
                 "node": "gw-002", "preset": "maestro",
                 "spawnedAt": "2026-08-28T17:05:11+00:00",
                 "status": "active", "session": None},
                {"code": "b9be", "sessionId": "s-b9be", "role": "worker",
                 "node": "pm-007", "preset": "maestro",
                 "spawnedAt": "2026-08-28T16:41:02+00:00",
                 "status": "active", "session": None},
            ],
        }
        self.trace_payload: dict = {
            "op": "trace", "sessionId": "s-fake", "signature": "fake-ts-1",
            "totalLines": 4, "parseFailures": 0, "logTruncated": False,
            "filter": {"type": None, "tool": None, "text": None,
                       "seqFrom": None, "seqTo": None},
            "matched": {"entries": 3, "chars": 800, "payload_chars": 800,
                        "seq_range": [1, 9], "type_histogram": {}},
            "folded": True, "budget": 20000,
            "dropped": {"entries": 1, "chars": 500},
            "entries": [
                {"type": "trace.compact", "reason": "threshold",
                 "threshold": 20000,
                 "dropped": {"entries": 1, "chars": 500},
                 "kept": {"entries": 2, "chars": 300}, "seq_range": [1, 9]},
                {"type": "turn/start", "seq": 5, "time": 0,
                 "data": {"turn": 2}},
                {"type": "tool/call", "seq": 9, "time": 0,
                 "data": {"turn": 2, "name": "bash", "command": "ls"}},
            ],
        }

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/subscribe", self.handle)
        app.router.add_get("/health", self.handle_health)
        # 票板全量（TK-002）+ 席位舰全量（TK-003）：双路由——现行网关透传
        # GET /{op}，真服 v0.7.0 实际在 /op/*（G3 映射后统一走 /op/*），
        # 都备好
        app.router.add_get("/tickets", self.handle_tickets)
        app.router.add_get("/op/tickets", self.handle_tickets)
        app.router.add_get("/fleet", self.handle_fleet)
        app.router.add_get("/op/fleet", self.handle_fleet)
        app.router.add_get("/trace", self.handle_trace)
        app.router.add_get("/op/trace", self.handle_trace)
        return app

    async def handle_trace(self, _request: web.Request) -> web.Response:
        return web.json_response(self.trace_payload)

    async def handle_tickets(self, _request: web.Request) -> web.Response:
        return web.json_response(self.tickets_payload)

    async def handle_fleet(self, _request: web.Request) -> web.Response:
        return web.json_response(self.fleet_payload)

    def publish(self, kind: str, msgid: str, replay: bool = False) -> dict:
        ev = {"kind": kind, "msgid": msgid, "path": "/fake", "replay": replay}
        self.snapshot.append(ev)
        for q in list(self.subscribers):
            q.put_nowait(ev)
        return ev

    async def handle_health(self, _request: web.Request) -> web.Response:
        return web.json_response({"service": "fake-pm", "ok": True})

    async def handle(self, request: web.Request) -> web.StreamResponse:
        kinds = set(filter(None, request.query.get("kinds", "").split(",")))
        resp = web.StreamResponse(
            headers={"content-type": "text/event-stream"})
        await resp.prepare(request)
        q: asyncio.Queue = asyncio.Queue()
        self.active += 1
        try:
            for ev in self.snapshot:  # 快照回放先行
                if ev["kind"] in kinds:
                    await resp.write(f"data: {json.dumps(ev)}\n\n".encode())
            self.subscribers.append(q)
            while True:
                try:
                    ev = await asyncio.wait_for(q.get(), 0.2)
                except asyncio.TimeoutError:
                    try:
                        await resp.write(b": keepalive\n\n")
                        continue
                    except (ConnectionError, RuntimeError):
                        break
                await resp.write(f"data: {json.dumps(ev)}\n\n".encode())
        except asyncio.CancelledError:
            raise
        except (ConnectionError, RuntimeError):
            pass
        finally:
            self.active -= 1
            if q in self.subscribers:
                self.subscribers.remove(q)
        return resp


class Inbox:
    """obs 队列收集器：pump 进列表，按谓词等待（tk 线程外读不消费已收帧）。"""

    def __init__(self, q: "queue.Queue[dict]") -> None:
        self.q = q
        self.frames: list[dict] = []

    def pump(self) -> None:
        while True:
            try:
                self.frames.append(self.q.get_nowait())
            except queue.Empty:
                return

    async def wait(self, pred, timeout: float = 8.0, what: str = "frame"):
        deadline = time.monotonic() + timeout
        while True:
            self.pump()
            for f in self.frames:
                if pred(f):
                    return f
            if time.monotonic() > deadline:
                raise AssertionError(
                    f"{what} 未到达（{timeout}s）；已收帧：{self.frames!r}"[:2000])
            await asyncio.sleep(0.05)


def is_subscribed_res(frame: dict) -> bool:
    data = frame.get("data") if isinstance(frame, dict) else None
    return frame.get("t") == "pm.res" and isinstance(data, dict) \
        and "subscribed" in data


def is_pm_sub_accepted(frame: dict, kinds=("fleet",)) -> bool:
    """pm.sub 受理形（带 subscribed、非退订回包、非 already no-op）。"""
    data = frame.get("data") if isinstance(frame, dict) else None
    return frame.get("t") == "pm.res" and isinstance(data, dict) \
        and data.get("subscribed") == list(kinds) \
        and "was_subscribed" not in data and "note" not in data


class FakeWS:
    """_pm_subscribe 直测用的最小 ws 替身（只收 send_json）。"""

    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send_json(self, obj: dict) -> None:
        self.sent.append(obj)


class Tk001PureTest(unittest.TestCase):
    """纯函数面：退避 / 文案 / 配置 / id 自增。"""

    def test_reconnect_delay_exponential_with_cap(self):
        self.assertEqual(rva.reconnect_delay_s(1), 2.0)
        self.assertEqual(rva.reconnect_delay_s(2), 4.0)
        self.assertEqual(rva.reconnect_delay_s(3), 8.0)
        self.assertEqual(rva.reconnect_delay_s(5), 30.0)   # 32 封顶 30
        self.assertEqual(rva.reconnect_delay_s(6), 30.0)
        self.assertEqual(rva.reconnect_delay_s(0), 2.0)    # ≥1 归一

    def test_pm_error_of_shapes(self):
        self.assertEqual(rva.pm_error_of(
            {"error": {"code": "pm_down", "message": "dead"}}), ("pm_down", "dead"))
        self.assertEqual(rva.pm_error_of({"error": "boom"}), ("boom", ""))
        self.assertEqual(rva.pm_error_of({"data": {}}), ("", ""))
        self.assertEqual(rva.pm_error_of("junk"), ("", ""))

    def test_pm_is_degraded_family(self):
        for code in ("pm_down", "pm_unreachable", "pm_timeout",
                     "pm_unavailable", "pm_status", "pm_internal"):
            self.assertTrue(rva.pm_is_degraded(code), code)
        self.assertFalse(rva.pm_is_degraded("bad_request"))
        self.assertFalse(rva.pm_is_degraded(""))

    def test_pm_res_line_forms(self):
        self.assertEqual(
            rva.pm_res_line({"id": "a", "data": {"subscribed": ["fleet"]}}),
            "✓ pm.res[a] 已订阅 fleet")
        self.assertEqual(
            rva.pm_res_line({"id": "a",
                             "data": {"subscribed": [], "note": "already-subscribed"}}),
            "✓ pm.res[a] 已订阅 —（already-subscribed）")
        line = rva.pm_res_line({"id": "a", "data": {"ok": 1}})
        self.assertTrue(line.startswith("✓ pm.res[a] {"))
        self.assertEqual(
            rva.pm_res_line({"id": "a",
                             "error": {"code": "pm_down", "message": "x"}}),
            "⚠ pm.res[a] pm_down x")
        self.assertEqual(
            rva.pm_res_line({"id": "a",
                             "data": {"subscribed": [], "was_subscribed": True}}),
            "✓ pm.res[a] 已退订")
        self.assertEqual(
            rva.pm_res_line({"id": "a",
                             "data": {"subscribed": [], "was_subscribed": False}}),
            "✓ pm.res[a] 本就无订阅")
        self.assertEqual(rva.pm_res_line(None), "⚠ PM 回包不可读")

    def test_pm_event_line(self):
        now = 1700000000.0
        line = rva.pm_event_line({"seq": 7, "source": "fleet", "kind": "fleet",
                                  "path": "/f.json", "replay": True}, now=now)
        self.assertIn("#7 fleet/fleet /f.json · 回放", line)
        live = rva.pm_event_line({"seq": 1, "source": "tickets", "kind": "tickets",
                                  "path": "", "replay": False}, now=now)
        self.assertNotIn("回放", live)
        long = rva.pm_event_line({"seq": 1, "source": "s", "kind": "k",
                                  "path": "x" * 100}, now=now)
        self.assertLessEqual(len(long), 100)

    def test_banner_lines(self):
        self.assertEqual(rva.pm_banner_degraded_line("pm_down", "refused"),
                         "⚠ PM 降级（pm_down）：refused")
        self.assertEqual(rva.pm_banner_degraded_line("", ""),
                         "⚠ PM 降级（未知）：PM 服务不可用")
        self.assertEqual(rva.pm_banner_ok_line(["fleet", "tickets"]),
                         "PM 正常 · 已订阅 fleet，tickets")
        self.assertEqual(rva.pm_banner_ok_line([]), "PM 正常 · 已订阅 —")

    def test_session_config_roundtrip_and_fallbacks(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "cfg.json"
            self.assertEqual(rva.load_session_config(p), {})  # 缺文件
            rva.save_session_config("ws://127.0.0.1:9999/ws", p)
            self.assertEqual(rva.load_session_config(p),
                             {"url": "ws://127.0.0.1:9999/ws"})
            p.write_text("not json{", encoding="utf-8")       # 损坏
            self.assertEqual(rva.load_session_config(p), {})
            p.write_text('{"url": 42}', encoding="utf-8")     # 字段不合法
            self.assertEqual(rva.load_session_config(p), {})

    def test_pm_frame_ids_autoincrement_never_reused(self):
        link = rva.ObserveLink("ws://x", "tok", queue.Queue(), lambda s: None,
                               pm_kinds=("fleet", "tickets"))
        ws = FakeWS()

        async def scenario():
            await link._pm_subscribe(ws)
            await link._pm_subscribe(ws)

        asyncio.run(scenario())
        self.assertEqual(len(ws.sent), 2)
        ids = [f["id"] for f in ws.sent]
        self.assertEqual(len(set(ids)), 2)                 # 每帧全新 id
        self.assertTrue(all(i.startswith("pm-") for i in ids))
        for f in ws.sent:
            self.assertEqual(f["t"], "pm.sub")
            self.assertEqual(f["kinds"], ["fleet", "tickets"])
        seqs = [int(re.sub(r"^pm-\d+-", "", i)) for i in ids]
        self.assertEqual(seqs, [1, 2])                     # 自增序号单调


class Tk001LinkTest(unittest.IsolatedAsyncioTestCase):
    """ObserveLink 集成面（进程内真 rt_gateway + 假 SSE 源）。"""

    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.port_file = Path(self._tmp.name) / "pm.port"
        self._old_env = os.environ.get("PM_HOST_PORT_FILE")
        os.environ["PM_HOST_PORT_FILE"] = str(self.port_file)
        rt_gateway._reset_pm_port_cache()
        self.fake = FakePMSSE()
        self._fake_runner = await self._serve(self.fake.app())
        self.port_file.write_text(
            json.dumps({"service": "fake-pm", "port": self._fake_port}))
        self.addAsyncCleanup(self._stop_fake)
        self.links: list[rva.ObserveLink] = []

    async def asyncTearDown(self) -> None:
        for link in self.links:
            link.close()
        if self._old_env is None:
            os.environ.pop("PM_HOST_PORT_FILE", None)
        else:
            os.environ["PM_HOST_PORT_FILE"] = self._old_env
        rt_gateway._reset_pm_port_cache()
        self._tmp.cleanup()

    async def _serve(self, app: web.Application) -> web.AppRunner:
        runner = web.AppRunner(app, handler_cancellation=True)
        await runner.setup()
        # shutdown_timeout 有界：SSE 流是活流，缺省 60s 会拖死 cleanup
        site = web.TCPSite(runner, "127.0.0.1", 0, shutdown_timeout=0.5)
        await site.start()
        self._fake_port = runner.addresses[0][1]
        return runner

    async def _stop_fake(self) -> None:
        await self._fake_runner.cleanup()

    async def _start_gateway(self, port: int) -> rt_gateway.VoiceGateway:
        gw = rt_gateway.VoiceGateway(port=port, token=TOKEN,
                                     head_provider=None, topic_sources=None)
        await gw.start()
        self.addAsyncCleanup(gw.stop)
        return gw

    def _link(self, gw: rt_gateway.VoiceGateway,
              pm_kinds=("fleet",)) -> tuple[rva.ObserveLink, Inbox]:
        q: "queue.Queue[dict]" = queue.Queue()
        link = rva.ObserveLink(f"ws://127.0.0.1:{gw.port}/ws", TOKEN, q,
                               lambda s: None, pm_kinds=pm_kinds)
        self.links.append(link)
        link.start()
        return link, Inbox(q)

    @staticmethod
    async def _free_port() -> int:
        import socket

        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    async def test_ready_auto_sub_and_pm_req_roundtrip(self):
        self.fake.publish(KIND, "snap-1", replay=True)      # 订阅即快照回放
        gw = await self._start_gateway(0)
        _link, inbox = self._link(gw)
        sub = await inbox.wait(is_subscribed_res, what="自动订阅 pm.res")
        self.assertEqual(sub["data"]["subscribed"], ["fleet"])
        snap = await inbox.wait(
            lambda f: f.get("t") == "pm.event" and f.get("replay") is True,
            what="快照回放 pm.event")
        self.assertEqual(snap["kind"], KIND)
        _link.send_request({"t": "pm.req", "id": "pmq-t-1",
                            "op": "health", "params": {}})
        res = await inbox.wait(
            lambda f: f.get("t") == "pm.res" and f.get("id") == "pmq-t-1",
            what="pm.req 回包")
        self.assertIn("data", res)                          # 透传成功非 error
        self.assertEqual(res["data"]["ok"], True)

    async def test_gateway_restart_auto_resubscribe_no_replay(self):
        port = await self._free_port()
        gw = await self._start_gateway(port)
        link, inbox = self._link(gw)
        sub1 = await inbox.wait(is_subscribed_res, what="首次自动订阅")
        link.send_request({"t": "pm.req", "id": "pmq-keep-1",
                           "op": "health", "params": {}})
        await inbox.wait(
            lambda f: f.get("t") == "pm.res" and f.get("id") == "pmq-keep-1",
            what="重启前已确认请求")
        # gateway 重启：先拔客户端连接（真实断线等价），旧实例随之下线，
        # 新实例同端口顶上——客户端视角=连接死+服务回归
        asyncio.run_coroutine_threadsafe(
            link._ws.close(), link._loop).result(3)
        await gw.stop()
        await self._start_gateway(port)
        sub2 = await inbox.wait(
            lambda f: is_subscribed_res(f) and f is not sub1,
            timeout=15.0, what="重启后自动重订")
        self.assertEqual(sub2["data"]["subscribed"], ["fleet"])
        self.assertNotIn("note", sub2["data"])              # 非同订 no-op
        seq = lambda fid: int(re.sub(r"^pm-\d+-", "", str(fid)))  # noqa: E731
        self.assertGreater(seq(sub2["id"]), seq(sub1["id"]))  # id 自增不复用
        await asyncio.sleep(1.5)                            # 静默窗：重放会在此露头
        inbox.pump()
        subs = [f for f in inbox.frames if is_subscribed_res(f)]
        self.assertEqual(len(subs), 2)                      # 每连接恰好一订
        self.assertEqual(  # 已确认 pm.req 不重放：重启后无第二份回包
            sum(1 for f in inbox.frames
                if f.get("t") == "pm.res" and f.get("id") == "pmq-keep-1"), 1)

    async def test_upstream_stream_break_auto_resubscribe(self):
        old_delay = rva.PM_RESUB_DELAY_S
        rva.PM_RESUB_DELAY_S = 0.5
        try:
            gw = await self._start_gateway(0)
            _link, inbox = self._link(gw)
            sub1 = await inbox.wait(is_subscribed_res, what="首次自动订阅")
            await self._stop_fake()                         # 上游事件流断
            brk = await inbox.wait(
                lambda f: f.get("t") == "error"
                and f.get("code") in ("pm_sub_failed", "pm_sub_ended"),
                timeout=10.0, what="断流 error 帧")
            self.assertIn(brk.get("code"), ("pm_sub_failed", "pm_sub_ended"))
            self.fake = FakePMSSE()                         # 上游恢复
            self._fake_runner = await self._serve(self.fake.app())
            self.port_file.write_text(
                json.dumps({"service": "fake-pm", "port": self._fake_port}))
            rt_gateway._reset_pm_port_cache()
            sub2 = await inbox.wait(
                lambda f: is_pm_sub_accepted(f) and f is not sub1,
                timeout=10.0, what="同连接自动重订受理")
            seq = lambda fid: int(re.sub(r"^pm-\d+-", "", str(fid)))  # noqa: E731
            self.assertGreater(seq(sub2["id"]), seq(sub1["id"]))
            self.fake.publish(KIND, "live-1")               # 新泵真活：增量可达
            ev = await inbox.wait(
                lambda f: f.get("t") == "pm.event" and f.get("msgid") == "live-1",
                what="重订后增量事件")
            self.assertEqual(ev["kind"], KIND)
        finally:
            rva.PM_RESUB_DELAY_S = old_delay

    async def test_no_auto_sub_without_pm_kinds(self):
        gw = await self._start_gateway(0)
        q: "queue.Queue[dict]" = queue.Queue()
        states: list[str] = []
        link = rva.ObserveLink(f"ws://127.0.0.1:{gw.port}/ws", TOKEN, q,
                               lambda s: states.append(s), pm_kinds=None)
        self.links.append(link)
        link.start()
        deadline = time.monotonic() + 8
        while "open" not in states and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        self.assertIn("open", states)                       # 会话已开
        inbox = Inbox(q)
        await asyncio.sleep(1.5)                            # 静默窗
        inbox.pump()
        self.assertFalse([f for f in inbox.frames
                          if f.get("t") == "pm.res"])       # 无 pm 帧


if __name__ == "__main__":
    unittest.main()
