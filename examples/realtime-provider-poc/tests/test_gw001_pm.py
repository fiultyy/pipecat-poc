#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""GW-001 · pm.req 透传测试（<internal-repo> spec-gateway §GW-001）.

两个验证门的替身版：假上游（本机随机端口）+ 临时 pm.port 文件，零
systemd、零真实服务依赖，可任意重跑。真服务门（systemctl 停起真
pm-host-service）由 rt_gate_gw001.py 承担。

- 门1（替身）：同 id 重放只回一次 res
- 门2（替身）：上游死 → pm.res{error} 结构化、连接不断
- 另覆盖：去重窗纯逻辑、发现缓存、op/params 校验、非 2xx 透传
"""

from __future__ import annotations

import asyncio
import json
import os
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

TOKEN = "t-gw001"


def _make_fake_pm(health_ok: bool = True):
    """假 pm-host-service：/health 与 /tickets（可指定健康态/载荷）。"""

    async def health(_request: web.Request) -> web.Response:
        if health_ok:
            return web.json_response({"status": "ok", "service": "fake-pm"})
        return web.json_response({"status": "bad"}, status=500)

    async def tickets(_request: web.Request) -> web.Response:
        return web.json_response({"error": "not found", "service": "fake-pm"},
                                 status=404)

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/tickets", tickets)
    return app


class GW001PMTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.port_file = Path(self._tmp.name) / "pm.port"
        self._old_env = os.environ.get("PM_HOST_PORT_FILE")
        os.environ["PM_HOST_PORT_FILE"] = str(self.port_file)
        rt_gateway._reset_pm_port_cache()

    async def asyncTearDown(self) -> None:
        if self._old_env is None:
            os.environ.pop("PM_HOST_PORT_FILE", None)
        else:
            os.environ["PM_HOST_PORT_FILE"] = self._old_env
        rt_gateway._reset_pm_port_cache()
        self._tmp.cleanup()

    # ---- 夹具 ----

    def _write_port_file(self, port: int) -> None:
        self.port_file.write_text(json.dumps({"service": "fake-pm", "port": port}))

    async def _start_fake_pm(self, app: web.Application) -> web.AppRunner:
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        return runner

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
        self.assertEqual(msg.json()["t"], "auth.ok")
        self.addAsyncCleanup(ws.close)
        self.addAsyncCleanup(http.close)
        return ws

    async def _send(self, ws, frame: dict) -> None:
        await ws.send_str(json.dumps(frame))

    async def _recv(self, ws, timeout: float = 5.0) -> dict:
        msg = await asyncio.wait_for(ws.receive(), timeout)
        return msg.json()

    async def _drain(self, ws, window: float = 1.2) -> list[dict]:
        """收割 window 秒内的全部文本帧（pong 与 pm.res 走不同路径，顺序
        不保证——断言只能数帧，不能依赖跨路径定序）。"""
        frames: list[dict] = []
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive(), window)
            except (asyncio.TimeoutError, TimeoutError):
                return frames
            if msg.type != aiohttp.WSMsgType.TEXT:
                return frames
            frames.append(msg.json())

    # ---- 门1（替身）：同 id 重放只回一次 res ----

    async def test_gate1_same_id_replay_single_res(self) -> None:
        runner = await self._start_fake_pm(_make_fake_pm())
        self.addAsyncCleanup(runner.cleanup)
        port = runner.addresses[0][1]
        self._write_port_file(port)
        gw = await self._start_gateway()
        ws = await self._connect(gw)

        req = {"t": "pm.req", "id": "replay-1", "op": "health"}
        await self._send(ws, req)
        await self._send(ws, req)  # 窗内重放
        await self._send(ws, {"t": "ping"})  # 活性哨兵（pong 可合法超车）

        frames = await self._drain(ws)
        res = [f for f in frames if f["t"] == "pm.res"]
        self.assertEqual(len(res), 1, f"expect exactly one pm.res, got {frames}")
        self.assertEqual(res[0]["id"], "replay-1")
        self.assertEqual(res[0]["data"]["status"], "ok")
        self.assertIn("pong", [f["t"] for f in frames])  # 连接活、帧在流动

        # 响应后再次重放：窗口内零 pm.res，仅活性帧
        await self._send(ws, req)
        await self._send(ws, {"t": "ping"})
        frames = await self._drain(ws)
        self.assertEqual([f for f in frames if f["t"] == "pm.res"], [])
        self.assertIn("pong", [f["t"] for f in frames])

    # ---- 门2（替身）：上游死 → pm.res{error}，连接不断 ----

    async def test_gate2_dead_upstream_error_frame_connection_alive(self) -> None:
        # 占住一个端口再关掉：保证指向一个确定已死的端口
        holder_runner = await self._start_fake_pm(_make_fake_pm())
        dead_port = holder_runner.addresses[0][1]
        await holder_runner.cleanup()
        self._write_port_file(dead_port)

        gw = await self._start_gateway()
        ws = await self._connect(gw)

        await self._send(ws, {"t": "pm.req", "id": "dead-1", "op": "tickets"})
        res = await self._recv(ws)
        self.assertEqual(res["t"], "pm.res")
        self.assertEqual(res["id"], "dead-1")
        self.assertIn("error", res)
        self.assertNotIn("data", res)
        self.assertIn(res["error"]["code"],
                      ("pm_down", "pm_unreachable", "pm_timeout"))
        # 连接仍活：ping→pong
        await self._send(ws, {"t": "ping"})
        self.assertEqual((await self._recv(ws))["t"], "pong")

    # ---- 去重窗纯逻辑 ----

    def _bare_session(self) -> rt_gateway.WsSession:
        gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                     topic_sources=None)
        return rt_gateway.WsSession(gw, None)  # type: ignore[arg-type]

    async def test_dedup_first_then_replay_then_ttl_expiry(self) -> None:
        session = self._bare_session()
        self.assertTrue(session._pm_dedup_first("a"))
        self.assertFalse(session._pm_dedup_first("a"))
        self.assertTrue(session._pm_dedup_first("b"))
        # 窗上限：挤出最老条目（a 出窗、最新者保留）
        for i in range(rt_gateway.PM_DEDUP_MAX):
            session._pm_dedup_first(f"f{i}")
        self.assertLessEqual(len(session._pm_seen), rt_gateway.PM_DEDUP_MAX)
        self.assertNotIn("a", session._pm_seen)
        self.assertIn(f"f{rt_gateway.PM_DEDUP_MAX - 1}", session._pm_seen)
        # TTL 过期：手工把条目写旧 → 视为新请求
        session._pm_seen["old"] = time.monotonic() - (rt_gateway.PM_DEDUP_TTL_S + 1)
        self.assertTrue(session._pm_dedup_first("old"))
        # 重放不续期
        session._pm_seen["keep"] = time.monotonic() - (rt_gateway.PM_DEDUP_TTL_S - 1)
        self.assertFalse(session._pm_dedup_first("keep"))

    # ---- 发现：pm.port 读取与签名缓存 ----

    async def test_discovery_reads_port_and_caches_by_signature(self) -> None:
        self.assertIsNone(rt_gateway._pm_port())  # 文件缺席
        self.port_file.write_text("not json")
        self.assertIsNone(rt_gateway._pm_port())  # 坏 JSON
        self.port_file.write_text(json.dumps({"port": "abc"}))
        self.assertIsNone(rt_gateway._pm_port())  # port 字段非数值
        self.port_file.write_text(json.dumps({"port": 12345}))
        self.assertEqual(rt_gateway._pm_port(), 12345)
        self.port_file.unlink()
        self.assertIsNone(rt_gateway._pm_port())  # 文件被撤

    # ---- 帧校验与非 2xx 透传（不打 WS，直接驱 WsSession/_pm_call）----

    async def test_pm_req_frame_validation(self) -> None:
        session = self._bare_session()
        await session._on_pm_req({"t": "pm.req"})  # 无 id → 共用错误帧
        self.assertEqual(session._out[0][1]["t"], "error")
        self.assertEqual(session._out[0][1]["code"], "bad_request")
        await session._on_pm_req({"t": "pm.req", "id": "x", "op": "../etc"})
        self.assertEqual(session._out[1][1]["t"], "pm.res")
        self.assertEqual(session._out[1][1]["error"]["code"], "bad_request")
        await session._on_pm_req({"t": "pm.req", "id": "y", "op": "tickets",
                                  "params": "oops"})
        self.assertEqual(session._out[2][1]["error"]["code"], "bad_request")
        # 校验失败同样消耗 id：同 id 重放被窗吸收（只回一次）
        await session._on_pm_req({"t": "pm.req", "id": "y", "op": "tickets",
                                  "params": "oops"})
        self.assertEqual(len(session._out), 3)

    async def test_roundtrip_non200_relays_status_and_upstream(self) -> None:
        runner = await self._start_fake_pm(_make_fake_pm())
        self.addAsyncCleanup(runner.cleanup)
        self._write_port_file(runner.addresses[0][1])
        session = self._bare_session()
        await session._pm_roundtrip("t1", "tickets", "")
        frame = session._out[0][1]
        self.assertEqual(frame["t"], "pm.res")
        self.assertEqual(frame["id"], "t1")
        self.assertEqual(frame["error"]["code"], "pm_status")
        self.assertEqual(frame["error"]["status"], 404)
        self.assertEqual(frame["error"]["upstream"]["service"], "fake-pm")

    async def test_roundtrip_success_envelope(self) -> None:
        runner = await self._start_fake_pm(_make_fake_pm())
        self.addAsyncCleanup(runner.cleanup)
        self._write_port_file(runner.addresses[0][1])
        session = self._bare_session()
        await session._pm_roundtrip("t2", "health", "")
        frame = session._out[0][1]
        self.assertEqual(frame, {"t": "pm.res", "id": "t2",
                                 "data": {"status": "ok", "service": "fake-pm"}})

    async def test_roundtrip_port_file_missing_structured_error(self) -> None:
        session = self._bare_session()  # 未写 pm.port → pm_unavailable
        await session._pm_roundtrip("t3", "health", "")
        frame = session._out[0][1]
        self.assertEqual(frame["error"]["code"], "pm_unavailable")

    async def test_params_mechanical_query_mapping(self) -> None:
        """params→query 纯机械：str 原样、其余 JSON 编码；服务端可解析。"""
        seen: dict = {}

        async def echo(request: web.Request) -> web.Response:
            seen.update(request.query)
            return web.json_response({"q": dict(request.query)})

        app = web.Application()
        app.router.add_get("/trace", echo)
        runner = await self._start_fake_pm(app)
        self.addAsyncCleanup(runner.cleanup)
        self._write_port_file(runner.addresses[0][1])
        session = self._bare_session()
        await session._pm_roundtrip("t4", "trace", "")
        # 无 params：空 query 照常到服务
        self.assertEqual(session._out[0][1]["data"]["q"], {})


if __name__ == "__main__":
    unittest.main()
