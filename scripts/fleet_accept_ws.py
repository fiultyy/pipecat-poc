"""fleet 验收探针（ws 控制面）：fleet.brief / fleet.cleanup。
用法:
  python fleet_accept_ws.py brief
  python fleet_accept_ws.py cleanup <seat_code>   # mode=end
输出逐帧 JSON（关键帧），供验收比对契约字段。
"""
import asyncio
import json
import os
import sys

import aiohttp

URL = "ws://127.0.0.1:8765/ws"
TOKEN = os.environ.get("VOICE_GATEWAY_TOKEN", "")


async def run(mode, arg=None):
    async with aiohttp.ClientSession() as http:
        async with http.ws_connect(URL) as ws:
            await ws.send_str(json.dumps({"t": "auth", "token": TOKEN}))
            print("auth:", (await ws.receive_json()).get("t"))
            await ws.send_str(json.dumps({"t": "session.start", "observe": True}))
            print("start:", (await ws.receive_json()).get("t"))

            if mode == "brief":
                await ws.send_str(json.dumps({"t": "fleet.brief", "req_id": "acc-brief"}))
            elif mode == "cleanup":
                await ws.send_str(json.dumps(
                    {"t": "fleet.cleanup", "ids": [arg], "mode": "end",
                     "req_id": "acc-cleanup"}))
            else:
                raise SystemExit(f"unknown mode {mode}")

            # 监听有限窗口，抓目标 result 帧
            want = {"brief": "fleet.brief.result", "cleanup": "fleet.cleanup.result"}[mode]
            try:
                while True:
                    msg = await asyncio.wait_for(ws.receive(), timeout=20)
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            print("ws closed:", msg)
                            break
                        continue
                    d = msg.json()
                    t = d.get("t")
                    if t == want:
                        print(want, "=>", json.dumps(d, ensure_ascii=False))
                        break
                    if t == "error":
                        print("error =>", json.dumps(d, ensure_ascii=False))
                        if d.get("req_id"):
                            break
            except asyncio.TimeoutError:
                print("TIMEOUT waiting", want)
            await ws.send_str(json.dumps({"t": "session.end"}))


asyncio.run(run(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None))
