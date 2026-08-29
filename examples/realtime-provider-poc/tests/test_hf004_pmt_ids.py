#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""HF-004 · pmt- 拉取 id 跨流唯一 + tickets/trace 双流并发零丢帧.

缺陷：tickets/trace 双流共用 ``pmt-`` 前缀但各持独立计数器，同毫秒且
序号相等时 id 相同 → 网关 (client, id) 去重窗把后到帧静默丢弃，该请求
永远无回包。修复：双流共用一个计数器（同前缀跨流唯一）。

门（×2）：真网关 + 真客户端链路（ObserveLink），time.time 钉死同一毫秒
（撞号最恶劣条件），交替并发拉 tickets/trace 各 K 次 → 发出 id 全唯一
+ pm.res 全数回包（零丢帧）。App 以 __new__ 桩化（被测方法不触 UI 件）。
"""

from __future__ import annotations

import asyncio
import os
import queue
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
from tests.test_tk001_pm import FakePMSSE, Inbox, TOKEN  # noqa: E402

K = 30  # 每轮每流拉取次数


class HF004PmtIdTest(unittest.IsolatedAsyncioTestCase):
    """同前缀双流 id 唯一性 + 并发零丢帧（真网关真链路）。"""

    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.port_file = Path(self._tmp.name) / "pm.port"
        self._old_env = os.environ.get("PM_HOST_PORT_FILE")
        os.environ["PM_HOST_PORT_FILE"] = str(self.port_file)
        rt_gateway._reset_pm_port_cache()
        self.fake = FakePMSSE()
        runner = web.AppRunner(self.fake.app(), handler_cancellation=True)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0, shutdown_timeout=0.5)
        await site.start()
        self._runner = runner
        self.port_file.write_text(
            __import__("json").dumps({"port": runner.addresses[0][1]}))
        self.addAsyncCleanup(runner.cleanup)

    async def asyncTearDown(self) -> None:
        if self._old_env is None:
            os.environ.pop("PM_HOST_PORT_FILE", None)
        else:
            os.environ["PM_HOST_PORT_FILE"] = self._old_env
        rt_gateway._reset_pm_port_cache()
        self._tmp.cleanup()

    async def test_dual_stream_same_ms_ids_unique_zero_loss(self) -> None:
        gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                     topic_sources=None)
        await gw.start()
        self.addAsyncCleanup(gw.stop)
        q: "queue.Queue[dict]" = queue.Queue()
        link = rva.ObserveLink(f"ws://127.0.0.1:{gw.port}/ws", TOKEN, q,
                               lambda s: None, pm_kinds=("tickets",))
        self.addAsyncCleanup(link.close)
        link.start()
        inbox = Inbox(q)
        await inbox.wait(
            lambda f: f.get("t") == "pm.res"
            and isinstance(f.get("data"), dict)
            and f["data"].get("subscribed") == ["tickets"],
            8, "订阅受理（会话就绪标志）")

        app = rva.App.__new__(rva.App)  # 桩化：被测方法只触下列属性
        app.obs_link = link
        app._pmt_seq = 0
        app._pending = {}
        app._tickets_fetch_job = None
        sent: list[str] = []
        orig_send = link.send_request

        def send_record(req: dict) -> None:
            sent.append(str(req.get("id")))
            orig_send(req)

        link.send_request = send_record
        params = rva.trace_query_params("s-fake", "", "", "", "", "")

        real_time = rva.time.time
        for rnd in (1, 2):  # ×2：两轮独立计验（计数器跨轮续增仍唯一）
            rva.time.time = lambda: 1788000000.0  # 钉死同毫秒（撞号最恶劣条件）
            try:
                for _ in range(K):
                    app._tickets_fetch()
                    app._trace_fetch(params=params)
            finally:
                rva.time.time = real_time

            # 发出面：2K 帧、id 全唯一（同前缀跨流 + 跨轮）
            want = 2 * K
            self.assertEqual(len(sent), rnd * want)
            self.assertEqual(len(set(sent)), rnd * want,
                             f"round {rnd}: pmt- id 撞号")

            # 回包面：pm.res 全数回包（零丢帧；撞号帧会被网关去重窗吞掉）
            deadline = time.monotonic() + 15
            while True:
                inbox.pump()
                got = {f["id"] for f in inbox.frames
                       if f.get("t") == "pm.res"
                       and str(f.get("id", "")).startswith("pmt-")}
                if len(got) >= rnd * want or time.monotonic() > deadline:
                    break
                await asyncio.sleep(0.1)
            self.assertEqual(len(got), rnd * want,
                             f"round {rnd}: 丢帧 {rnd * want - len(got)}")
            self.assertEqual(got, set(sent))


if __name__ == "__main__":
    unittest.main()
