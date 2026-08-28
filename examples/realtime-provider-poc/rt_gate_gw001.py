#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""GW-001 验证门（真服务版，<internal-repo> spec-gateway §GW-001）.

门1  同 id 重放只回一次 res（真 pm-host-service，op=health）
门2  pm-host-service 停（systemctl --user stop 模拟）→ pm.res{error}
     结构化、连接不断；测后 systemctl --user start 恢复

前置：pm-host-service active 且 ~/.dsh/maestro/pm.port 可读。整门幂等
可重复跑（收口=整门 ×2 全绿）；finally 恒恢复服务，中途失败不留死服务。

用法：rt_gate_gw001.py [--skip-systemd]
  --skip-systemd：门2 改为不碰 systemd（仅替身端口已死路径），供无
  systemd 权限的环境冒烟；真门必须不带此开关跑。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import aiohttp  # noqa: E402

import rt_gateway  # noqa: E402

TOKEN = "gw001-live-gate"
UNIT = "pm-host-service"
REPLAY_ID = "gw001-live-gate-replay"
DEAD_ID = "gw001-live-gate-dead"
RESTORED_ID = "gw001-live-gate-restored"

results: list[tuple[str, bool, str]] = []


def gate(name: str, ok: bool, detail: str) -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")


def systemctl(*args: str) -> None:
    subprocess.run(["systemctl", "--user", *args, UNIT], check=True)


def real_health(port: int, timeout: float = 2.0) -> dict | None:
    """直读 pm.port 端口探真服务（绕开网关缓存，独立口径）。"""
    try:
        with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/health", timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:  # noqa: BLE001 — 探活只回答活/死
        return None


def read_pm_port() -> int:
    doc = json.loads(Path(rt_gateway._pm_port_path()).read_text())
    return int(doc["port"])


async def wait_service_back(deadline_s: float = 20.0) -> bool:
    """systemctl start 后等服务真起来（新 pm.port + /health 200）。"""
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        rt_gateway._reset_pm_port_cache()
        port = rt_gateway._pm_port()
        if port is not None and real_health(port) is not None:
            return True
        await asyncio.sleep(0.5)
    return False


async def pin_dead_discovery(attempts: int = 4) -> tuple[int, str] | None:
    """停服并把 pm.port 残留快照钉住，返回确死的 (port, 冻结文件路径)。

    部署常态失败面=「文件残留旧端口 + 连接拒绝」。本机有外部 repair/升级
    进程可能在门2 途中把服务拉回并换写 pm.port（实测竞态）——把停服瞬间
    的 pm.port 快照冻结到副本、发现面指向副本，死上游路径即确定性复现。
    快照端口若探活（已被拉回）则再停一轮重试。
    """
    for _ in range(attempts):
        systemctl("stop")
        await asyncio.sleep(0.3)
        frozen_dir = tempfile.mkdtemp(prefix="gw001-gate-")
        frozen = Path(frozen_dir) / "pm.port"
        frozen.write_text(Path(rt_gateway._pm_port_path()).read_text())
        port = int(json.loads(frozen.read_text())["port"])
        if real_health(port) is None:
            return port, str(frozen)
    return None


class LiveGate:
    def __init__(self, gw: rt_gateway.VoiceGateway) -> None:
        self.gw = gw
        self.http: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None

    async def connect(self) -> None:
        self.http = aiohttp.ClientSession()
        self.ws = await self.http.ws_connect(f"ws://127.0.0.1:{self.gw.port}/ws")
        await self.ws.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        msg = await asyncio.wait_for(self.ws.receive(), 5)
        assert msg.json()["t"] == "auth.ok", msg.json()

    async def send(self, frame: dict) -> None:
        await self.ws.send_str(json.dumps(frame))

    async def recv(self, timeout: float = 8.0) -> dict:
        msg = await asyncio.wait_for(self.ws.receive(), timeout)
        assert msg.type == aiohttp.WSMsgType.TEXT, msg
        return msg.json()

    async def drain(self, window: float = 1.2) -> list[dict]:
        """pong 可合法超车 pm.res（内联回包 vs 独立任务）——数帧不断言顺序。"""
        frames: list[dict] = []
        while True:
            try:
                msg = await asyncio.wait_for(self.ws.receive(), window)
            except (asyncio.TimeoutError, TimeoutError):
                return frames
            if msg.type != aiohttp.WSMsgType.TEXT:
                return frames
            frames.append(msg.json())

    async def close(self) -> None:
        if self.ws is not None:
            await self.ws.close()
        if self.http is not None:
            await self.http.close()


async def run(skip_systemd: bool) -> int:
    port0 = rt_gateway._pm_port()
    if port0 is None or real_health(port0) is None:
        print("FAIL 前置: pm-host-service 不在（pm.port 不可读或 /health 不通）")
        return 1
    print(f"前置 ok: real pm-host-service on 127.0.0.1:{port0}")

    gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                 topic_sources=None)
    await gw.start()
    live = LiveGate(gw)
    try:
        await live.connect()

        # ---- 门1: 同 id 重放只回一次 res（真服务 health）----
        req = {"t": "pm.req", "id": REPLAY_ID, "op": "health"}
        await live.send(req)
        await live.send(req)  # 窗内重放
        await live.send({"t": "ping"})
        frames = await live.drain()
        res = [f for f in frames if f["t"] == "pm.res"]
        gate("门1a 同id重放唯一res",
             len(res) == 1 and res[0]["id"] == REPLAY_ID
             and res[0].get("data", {}).get("status") == "ok",
             f"pm.res×{len(res)} data={res[0].get('data') if res else None}")
        gate("门1b 连接活性", "pong" in [f["t"] for f in frames],
             f"frames={[f['t'] for f in frames]}")
        await live.send(req)
        await live.send({"t": "ping"})
        frames = await live.drain()
        gate("门1c 响应后重放仍零res",
             not [f for f in frames if f["t"] == "pm.res"]
             and "pong" in [f["t"] for f in frames],
             f"frames={[f['t'] for f in frames]}")

        # ---- 门2: 服务停 → error 帧，连接不断；finally 恢复 ----
        if skip_systemd:
            print("（--skip-systemd：门2 跳过真停起，仅输出门1）")
        else:
            pinned = await pin_dead_discovery()
            gate("门2a 服务确已停（发现面钉住残留快照）",
                 pinned is not None,
                 f"dead port {pinned[0]} @ {pinned[1]}" if pinned else "外部拉回竞态未能钉住")
            if pinned is not None:
                dead_port, frozen = pinned
                os.environ["PM_HOST_PORT_FILE"] = frozen
                rt_gateway._reset_pm_port_cache()
                try:
                    await live.send({"t": "pm.req", "id": DEAD_ID, "op": "health"})
                    dead = await live.recv()
                    gate("门2b error帧结构化",
                         dead.get("t") == "pm.res" and dead.get("id") == DEAD_ID
                         and "error" in dead and "data" not in dead,
                         json.dumps(dead, ensure_ascii=False)[:200])
                    gate("门2c 连接未断",
                         dead.get("error", {}).get("code")
                         in ("pm_down", "pm_unreachable", "pm_timeout"),
                         f"code={dead.get('error', {}).get('code')}")
                    await live.send({"t": "ping"})
                    alive = await live.recv()
                    gate("门2d 停服后连接活性", alive.get("t") == "pong", str(alive))
                finally:
                    os.environ.pop("PM_HOST_PORT_FILE", None)
                    rt_gateway._reset_pm_port_cache()
    finally:
        if not skip_systemd:
            subprocess.run(["systemctl", "--user", "start", UNIT], check=False)
            ok = await wait_service_back()
            print(f"[{'PASS' if ok else 'FAIL'}] 恢复: pm-host-service "
                  f"{'back on ' + str(rt_gateway._pm_port()) if ok else 'NOT BACK'}")
            if ok:
                # 恢复后全链路再走一遍：新端口发现 + 透传 + 新 id
                await live.send({"t": "pm.req", "id": RESTORED_ID, "op": "health"})
                back = await live.recv()
                gate("门2e 恢复后透传自愈",
                     back.get("t") == "pm.res" and back.get("id") == RESTORED_ID
                     and back.get("data", {}).get("status") == "ok",
                     json.dumps(back, ensure_ascii=False)[:160])
        await live.close()
        await gw.stop()

    failed = [r for r in results if not r[1]]
    print(f"\n== GW-001 live gates: {len(results) - len(failed)}/{len(results)} green ==")
    return 1 if failed else 0


def main() -> int:
    ap = argparse.ArgumentParser("rt_gate_gw001")
    ap.add_argument("--skip-systemd", action="store_true")
    args = ap.parse_args()
    return asyncio.run(run(args.skip_systemd))


if __name__ == "__main__":
    raise SystemExit(main())
