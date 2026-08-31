#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""WSP-001 · auth.ok 增量字段 proto 测试（spec-ws-protocol-v1.md §0/§3）.

打版条款落地验证（只增不改）：
- 门1：auth.ok 携带 proto == "v1"（帧内自证版本，免连接期再协商）
- 门2：老字段零变——键集恰为 {t, session_id, proto}，t/session_id 语义与
  打版前一致（t=="auth.ok"，session_id=="s-"+8hex），既有字段顺序不变
- 门3：错误路径零变——错 token 仍 error{auth}+close；未鉴权发帧仍
  error{unauthorized}+close（proto 字段不改变鉴权语义）
替身版：内存 VoiceGateway（port=0 随机口），零 systemd 依赖，可任意重跑。
"""

from __future__ import annotations

import asyncio
import json
import sys
import unittest
from pathlib import Path

POC_DIR = Path(__file__).resolve().parents[1]
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import aiohttp  # noqa: E402

import rt_gateway  # noqa: E402

TOKEN = "t-wsp001"


class WSP001ProtoTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.gw = rt_gateway.VoiceGateway(port=0, token=TOKEN,
                                          head_provider=None,
                                          topic_sources=None)
        await self.gw.start()
        self.addAsyncCleanup(self.gw.stop)

    async def _ws(self):
        http = aiohttp.ClientSession()
        ws = await http.ws_connect(f"ws://127.0.0.1:{self.gw.port}/ws")
        self.addAsyncCleanup(ws.close)
        self.addAsyncCleanup(http.close)
        return ws

    async def _auth(self, ws, token: str = TOKEN) -> dict:
        await ws.send_str(json.dumps({"t": "auth", "token": token}))
        msg = await asyncio.wait_for(ws.receive(), 5)
        return msg.json()

    # ---- 门1：proto 字段存在且为 "v1" ----

    async def test_gate1_auth_ok_carries_proto_v1(self) -> None:
        ws = await self._ws()
        frame = await self._auth(ws)
        self.assertEqual(frame["t"], "auth.ok")
        self.assertEqual(frame.get("proto"), rt_gateway.PROTO_VERSION)
        self.assertEqual(frame["proto"], "v1")

    # ---- 门2：老字段零变（键集精确 + 形态不变）----

    async def test_gate2_auth_ok_old_fields_unchanged(self) -> None:
        ws = await self._ws()
        frame = await self._auth(ws)
        self.assertEqual(set(frame.keys()), {"t", "session_id", "proto"})
        self.assertTrue(frame["session_id"].startswith("s-"))
        self.assertEqual(len(frame["session_id"]), 10)  # "s-" + 8 hex
        # 常量可 imported 且唯一权威（spec §0：WSP-001 生效标志）
        self.assertEqual(rt_gateway.PROTO_VERSION, "v1")

    # ---- 门3：错误路径零变（鉴权语义不受新字段影响）----

    async def test_gate3_auth_error_paths_unchanged(self) -> None:
        # 错 token：error{auth} + close
        ws = await self._ws()
        frame = await self._auth(ws, token="wrong-token")
        self.assertEqual(frame["t"], "error")
        self.assertEqual(frame["code"], "auth")
        msg = await asyncio.wait_for(ws.receive(), 5)
        self.assertEqual(msg.type, aiohttp.WSMsgType.CLOSE)
        # 未鉴权先发 pm.req：error{unauthorized} + close
        ws2 = await self._ws()
        await ws2.send_str(json.dumps({"t": "pm.req", "id": 1, "op": "tickets"}))
        msg = await asyncio.wait_for(ws2.receive(), 5)
        self.assertEqual(msg.json()["code"], "unauthorized")
        msg = await asyncio.wait_for(ws2.receive(), 5)
        self.assertEqual(msg.type, aiohttp.WSMsgType.CLOSE)


if __name__ == "__main__":
    unittest.main()
