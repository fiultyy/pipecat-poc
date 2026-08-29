#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""GW-002 · pm.event 回流订阅测试（<internal-repo> spec-gateway §GW-002）.

替身验证门：假 SSE 源（替 PM-007 op=subscribe，快照先行+增量队列）+
临时 pm.port，零真实服务依赖。真服务联调门在 PM-007 收口后由编排者
排联合验证（gateway 侧端点/query 假设见 rt_gateway.PM_SUBSCRIBE_PATH）。

- 门1：订阅建立/取消干净（无泄漏：上游流归零、泵任务出册、_pm_sub 清空；
  同 id 重放只回一次 res；同 kinds 重订 no-op 不新起上游流；白名单过滤）
- 门2：多客户端同事件互不干扰 + 断线重连快照兜底（重连快照先行补齐离线
  事件，存活客户端序列无缺口）
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import tempfile
import unittest
from pathlib import Path

POC_DIR = Path(__file__).resolve().parents[1]
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import aiohttp  # noqa: E402
from aiohttp import web  # noqa: E402

import rt_gateway  # noqa: E402
TOKEN = "t-gw002"


class FakePMSSE:
    """替身 pm-host-service 事件源：GET /subscribe → SSE（快照先行+增量）.

    - ``snapshot``：累积事件史 = 快照回放源（流建立时按该流 kinds 过滤发）
    - ``publish()``：向所有活动流投增量（不过滤 kinds——白名单过滤的
      服务侧职责由网关侧防御过滤覆盖测试）
    - ``active``：当前活动 SSE 流数（无泄漏门的权威计数）
    """

    def __init__(self) -> None:
        self.snapshot: list[dict] = []
        self.subscribers: list[asyncio.Queue] = []
        self.active = 0
        self.consumers: list[str] = []
        self.eof = asyncio.Event()  # 置位→活动流干净收尾（EOF；泵侧退场测试钩子）

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/subscribe", self.handle)
        return app

    def publish(self, kind: str, msgid: str, payload: dict | None = None) -> dict:
        ev: dict = {"kind": kind, "msgid": msgid}
        if payload is not None:
            ev["payload"] = payload
        self.snapshot.append(ev)
        for q in list(self.subscribers):
            q.put_nowait(ev)
        return ev

    async def handle(self, request: web.Request) -> web.StreamResponse:
        self.consumers.append(request.query.get("consumer", ""))
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
                    if self.eof.is_set():
                        break  # 本流干净收尾 → 订阅方读到 EOF
                    try:
                        await resp.write(b": keepalive\n\n")  # SSE 注释行保活
                        continue
                    except (ConnectionError, RuntimeError):
                        break  # 对端已断
                await resp.write(f"data: {json.dumps(ev)}\n\n".encode())
        except asyncio.CancelledError:
            raise  # handler_cancellation：网关侧断开 → 服务侧流收尾
        except (ConnectionError, RuntimeError):
            pass
        finally:
            self.active -= 1
            if q in self.subscribers:
                self.subscribers.remove(q)
        return resp


class GW002PMSubTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.port_file = Path(self._tmp.name) / "pm.port"
        self._old_env = os.environ.get("PM_HOST_PORT_FILE")
        os.environ["PM_HOST_PORT_FILE"] = str(self.port_file)
        rt_gateway._reset_pm_port_cache()
        self.fake = FakePMSSE()
        runner = web.AppRunner(self.fake.app(), handler_cancellation=True)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        self._runner = runner
        self.port = runner.addresses[0][1]
        self.port_file.write_text(json.dumps({"service": "fake-pm",
                                              "port": self.port}))
        self.addAsyncCleanup(runner.cleanup)

    async def asyncTearDown(self) -> None:
        if self._old_env is None:
            os.environ.pop("PM_HOST_PORT_FILE", None)
        else:
            os.environ["PM_HOST_PORT_FILE"] = self._old_env
        rt_gateway._reset_pm_port_cache()
        self._tmp.cleanup()

    # ---- 夹具 ----

    async def _start_gateway(self) -> rt_gateway.VoiceGateway:
        gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                     topic_sources=None)
        await gw.start()
        self.addAsyncCleanup(gw.stop)
        return gw

    async def _connect(self, gw: rt_gateway.VoiceGateway):
        http = aiohttp.ClientSession()
        ws = await http.ws_connect(f"ws://127.0.0.1:{gw.port}/ws")
        await ws.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        msg = await asyncio.wait_for(ws.receive(), 5)
        assert msg.json()["t"] == "auth.ok"
        self.addAsyncCleanup(ws.close)
        self.addAsyncCleanup(http.close)
        return ws

    async def _send(self, ws, frame: dict) -> None:
        await ws.send_str(json.dumps(frame))

    async def _recv(self, ws, timeout: float = 5.0) -> dict:
        msg = await asyncio.wait_for(ws.receive(), timeout)
        return msg.json()

    async def _drain(self, ws, window: float = 1.0) -> list[dict]:
        frames: list[dict] = []
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive(), window)
            except (asyncio.TimeoutError, TimeoutError):
                return frames
            if msg.type != aiohttp.WSMsgType.TEXT:
                return frames
            frames.append(msg.json())

    async def _wait_active_zero(self, deadline: float = 3.0) -> bool:
        end = asyncio.get_event_loop().time() + deadline
        while asyncio.get_event_loop().time() < end:
            if self.fake.active == 0:
                return True
            await asyncio.sleep(0.05)
        return self.fake.active == 0

    def _server_session(self, gw: rt_gateway.VoiceGateway):
        return next(iter(gw._active))

    # ---- 门1：订阅建立/取消干净（无泄漏）----

    async def test_gate1_subscribe_teardown_clean_no_leak(self) -> None:
        gw = await self._start_gateway()
        ws = await self._connect(gw)
        sess = self._server_session(gw)

        # 建立：pm.res 确认 + 上游流建立 + consumer 契约
        await self._send(ws, {"t": "pm.sub", "id": "s1",
                              "kinds": ["ticket.changed", "fleet.heartbeat"]})
        res = await self._recv(ws)
        self.assertEqual(res, {"t": "pm.res", "id": "s1",
                               "data": {"subscribed": ["ticket.changed",
                                                       "fleet.heartbeat"]}})
        await asyncio.sleep(0.2)
        self.assertEqual(self.fake.active, 1)
        self.assertTrue(self.fake.consumers[0].startswith("gw-s-"))

        # 同 id 重放：只回一次 res（GW-001 语义），不新起流
        await self._send(ws, {"t": "pm.sub", "id": "s1",
                              "kinds": ["ticket.changed"]})
        await self._send(ws, {"t": "ping"})
        frames = await self._drain(ws)
        self.assertEqual([f for f in frames if f.get("id") == "s1"], [])
        self.assertIn("pong", [f["t"] for f in frames])

        # 同 kinds 重订（新 id）：幂等 no-op，泵不动、上游流仍 1 条
        await self._send(ws, {"t": "pm.sub", "id": "s2",
                              "kinds": ["ticket.changed", "fleet.heartbeat"]})
        res2 = await self._recv(ws)
        self.assertEqual(res2["data"]["note"], "already-subscribed")
        await asyncio.sleep(0.2)
        self.assertEqual(self.fake.active, 1)

        # 白名单过滤：黑名单 kind 的增量被网关防御层拦下
        self.fake.publish("other.kind", "x-1")
        await self._send(ws, {"t": "ping"})
        self.assertNotIn("pm.event", [f["t"] for f in await self._drain(ws)])

        # 增量透传：msgid/payload 原样保留
        self.fake.publish("ticket.changed", "t-1", {"ref": "TK-1"})
        ev = await self._recv(ws)
        self.assertEqual(ev, {"t": "pm.event", "kind": "ticket.changed",
                              "msgid": "t-1", "payload": {"ref": "TK-1"}})

        # 取消：pm.res + 上游流归零 + 会话侧订阅态清空、泵任务终结
        await self._send(ws, {"t": "pm.unsub", "id": "u1"})
        res3 = await self._recv(ws)
        self.assertEqual(res3, {"t": "pm.res", "id": "u1",
                                "data": {"subscribed": [], "was_subscribed": True}})
        self.assertTrue(await self._wait_active_zero())
        self.assertIsNone(sess._pm_sub)  # 泵经 done-callback 出册，订阅态清空
        self.fake.publish("ticket.changed", "t-2")
        await self._send(ws, {"t": "ping"})
        self.assertNotIn("pm.event", [f["t"] for f in await self._drain(ws)])

        # 重复 unsub：幂等 no-op
        await self._send(ws, {"t": "pm.unsub", "id": "u2"})
        res4 = await self._recv(ws)
        self.assertEqual(res4["data"]["was_subscribed"], False)

    async def test_gate1b_disconnect_cleans_stream(self) -> None:
        gw = await self._start_gateway()
        http = aiohttp.ClientSession()
        ws = await http.ws_connect(f"ws://127.0.0.1:{gw.port}/ws")
        await ws.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        await self._recv(ws)
        await self._send(ws, {"t": "pm.sub", "id": "d1", "kinds": ["flow.step"]})
        await self._recv(ws)
        await asyncio.sleep(0.2)
        self.assertEqual(self.fake.active, 1)
        await ws.close()  # 断开 → teardown → 泵取消 → 上游流关
        self.assertTrue(await self._wait_active_zero())
        await http.close()

    # ---- 门2：多客户端互不干扰 + 重连快照兜底 ----

    async def test_gate2_multi_client_isolation_and_reconnect_snapshot(self) -> None:
        gw = await self._start_gateway()
        self.fake.publish("ticket.changed", "t-old-1", {"ref": "TK-0"})

        ws_a = await self._connect(gw)
        ws_b = await self._connect(gw)  # 同 token 上限 2 会话，恰好占满
        for wid, w in (("a", ws_a), ("b", ws_b)):
            await self._send(w, {"t": "pm.sub", "id": f"sub-{wid}",
                                 "kinds": ["ticket.changed"]})
            res = await self._recv(w)
            self.assertEqual(res["t"], "pm.res")
            # 快照先行：订阅即得存量事件（此时史里只有 t-old-1）
            self.assertEqual((await self._recv(w))["msgid"], "t-old-1")
        await asyncio.sleep(0.2)
        self.assertEqual(self.fake.active, 2)  # 每客户端一条独立上游流

        # 同一事件：两客户端各得一份拷贝，msgid 一致
        self.fake.publish("ticket.changed", "t-live-1", {"n": 1})
        ev_a, ev_b = await self._recv(ws_a), await self._recv(ws_b)
        self.assertEqual(ev_a["msgid"], "t-live-1")
        self.assertEqual(ev_b["msgid"], "t-live-1")

        # A 断线：B 完全不受扰，序列无缺口
        await ws_a.close()
        self.fake.publish("ticket.changed", "t-live-2", {"n": 2})
        ev = await self._recv(ws_b)
        self.assertEqual(ev["msgid"], "t-live-2")
        self.fake.publish("ticket.changed", "t-live-3", {"n": 3})
        self.assertEqual((await self._recv(ws_b))["msgid"], "t-live-3")

        # A 重连重订：快照回放先行，离线事件一个不丢、先于新增量
        ws_a2 = await self._connect(gw)
        self.addAsyncCleanup(ws_a2.close)
        await self._send(ws_a2, {"t": "pm.sub", "id": "sub-a2",
                                 "kinds": ["ticket.changed"]})
        await self._recv(ws_a2)  # pm.res
        got = []
        while len(got) < 4:  # 快照: t-old-1, t-live-1..3（服务侧回放，gateway 只搬运）
            got.append((await self._recv(ws_a2))["msgid"])
        self.assertEqual(got, ["t-old-1", "t-live-1", "t-live-2", "t-live-3"])
        # 重连后新事件双端齐发，B 序列继续无缺口
        self.fake.publish("ticket.changed", "t-live-4", {"n": 4})
        self.assertEqual((await self._recv(ws_a2))["msgid"], "t-live-4")
        self.assertEqual((await self._recv(ws_b))["msgid"], "t-live-4")
        await ws_b.close()

    # ---- 校验面 ----

    async def test_sub_validation_and_discovery_fail(self) -> None:
        self.port_file.unlink()  # 发现失败路径
        rt_gateway._reset_pm_port_cache()
        gw = await self._start_gateway()
        ws = await self._connect(gw)
        sess = self._server_session(gw)

        await self._send(ws, {"t": "pm.sub", "kinds": ["x"]})  # 无 id → 共用错误帧
        self.assertEqual((await self._recv(ws))["t"], "error")
        await self._send(ws, {"t": "pm.sub", "id": "v1", "kinds": []})
        self.assertEqual((await self._recv(ws))["error"]["code"], "bad_request")
        await self._send(ws, {"t": "pm.sub", "id": "v2", "kinds": ["a/b"]})
        self.assertEqual((await self._recv(ws))["error"]["code"], "bad_request")
        await self._send(ws, {"t": "pm.sub", "id": "v3",
                              "kinds": ["k"] * (rt_gateway.PM_SUB_MAX_KINDS + 1)})
        self.assertEqual((await self._recv(ws))["error"]["code"], "bad_request")
        await self._send(ws, {"t": "pm.sub", "id": "v4", "kinds": ["flow.step"]})
        res = await self._recv(ws)
        self.assertEqual(res["error"]["code"], "pm_unavailable")
        self.assertIsNone(sess._pm_sub)  # 失败订阅零残留
        # 校验/失败同样消耗 id：同 id 重放被窗吸收
        await self._send(ws, {"t": "pm.sub", "id": "v4", "kinds": ["flow.step"]})
        await self._send(ws, {"t": "ping"})
        self.assertEqual([f for f in await self._drain(ws)
                          if f.get("id") == "v4"], [])


class HF003DeadPumpSameKindsResubTest(unittest.IsolatedAsyncioTestCase):
    """HF-003 门：泵死后同 kinds 重订重建泵，推送恢复（死→恢复 ×2）。

    死泵态=订阅在册（``_pm_sub`` 同 kinds）但泵任务已终结（上游连接
    失败退场）。修复前同 kinds 重订命中 already-subscribed no-op，泵
    永不重建；修复后重订即重建，快照回放+增量推送恢复。
    """

    KINDS = ["ticket.changed"]

    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.port_file = Path(self._tmp.name) / "pm.port"
        self._old_env = os.environ.get("PM_HOST_PORT_FILE")
        os.environ["PM_HOST_PORT_FILE"] = str(self.port_file)
        # 先指死口（无监听）：首订泵起即败，落进死泵态
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        self.pm_port = sock.getsockname()[1]
        sock.close()
        self.port_file.write_text(json.dumps({"service": "fake-pm",
                                              "port": self.pm_port}))
        rt_gateway._reset_pm_port_cache()
        self.fake = FakePMSSE()
        self._runner: web.AppRunner | None = None

    async def asyncTearDown(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
        if self._old_env is None:
            os.environ.pop("PM_HOST_PORT_FILE", None)
        else:
            os.environ["PM_HOST_PORT_FILE"] = self._old_env
        rt_gateway._reset_pm_port_cache()
        self._tmp.cleanup()

    async def _fake_up(self) -> None:
        runner = web.AppRunner(self.fake.app(), handler_cancellation=True)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", self.pm_port)
        await site.start()
        self._runner = runner

    async def _recv(self, ws, timeout: float = 5.0) -> dict:
        msg = await asyncio.wait_for(ws.receive(), timeout)
        return msg.json()

    async def _sub(self, ws, sub_id: str) -> None:
        await ws.send_str(json.dumps({"t": "pm.sub", "id": sub_id,
                                      "kinds": self.KINDS}))

    async def _wait_pump_done(self, sess, deadline: float = 3.0) -> None:
        end = asyncio.get_event_loop().time() + deadline
        while asyncio.get_event_loop().time() < end:
            if sess._pm_sub is not None and sess._pm_sub["pump"].done():
                return
            await asyncio.sleep(0.05)
        self.fail("pump task did not finish (dead-pump state not reached)")

    async def _resub_and_expect_restore(self, gw, ws, sess, n: int,
                                        expect_snapshot: list[str]) -> None:
        await self._sub(ws, f"r{n}")
        res = await self._recv(ws)
        self.assertEqual(res["data"]["subscribed"], self.KINDS)
        self.assertNotEqual(res["data"].get("note"), "already-subscribed")
        for msgid in expect_snapshot:  # 快照回放先行
            self.assertEqual((await self._recv(ws))["msgid"], msgid)
        self.fake.publish("ticket.changed", f"live-{n}", {"round": n})
        self.assertEqual((await self._recv(ws))["msgid"], f"live-{n}")

    async def test_dead_pump_same_kinds_resub_restores_push(self) -> None:
        gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                     topic_sources=None)
        await gw.start()
        self.addAsyncCleanup(gw.stop)
        http = aiohttp.ClientSession()
        ws = await http.ws_connect(f"ws://127.0.0.1:{gw.port}/ws")
        await ws.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        await self._recv(ws)
        self.addAsyncCleanup(ws.close)
        self.addAsyncCleanup(http.close)
        sess = next(iter(gw._active))

        # 周期1 死法：上游死口——订阅受理 → 泵异步连败退场（在册+已终结）
        await self._sub(ws, "d1")
        self.assertEqual((await self._recv(ws))["data"]["subscribed"],
                         self.KINDS)
        self.assertEqual((await self._recv(ws))["code"], "pm_sub_failed")
        await self._wait_pump_done(sess)

        # 同 kinds 重订（上游已复活）：重建泵，快照+增量推送恢复
        await self._fake_up()
        self.fake.publish("ticket.changed", "snap-1", {"round": 1})
        await self._resub_and_expect_restore(gw, ws, sess, 1, ["snap-1"])

        # 周期2 死法：活流干净 EOF——泵退场（pm_sub_ended）
        self.fake.eof.set()
        self.assertEqual((await self._recv(ws))["code"], "pm_sub_ended")
        await self._wait_pump_done(sess)
        self.fake.eof.clear()

        # 同 kinds 重订再次恢复：快照含周期1 全史（snap-1, live-1）
        await self._resub_and_expect_restore(gw, ws, sess, 2,
                                             ["snap-1", "live-1"])


if __name__ == "__main__":
    unittest.main()
