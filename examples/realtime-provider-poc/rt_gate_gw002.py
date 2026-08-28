#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""GW-002 联合门（真服务版）：真 pm-host-service /subscribe v0.7.0 端到端.

真实契约（编排者已核，冒烟复核一致）：SSE 事件帧
``{t:"pm.event", seq, msgid, source, kind, path, replay}``；快照 epoch 回放
≤50（replay:true）→增量（replay:false）；同 consumer 单活流（顶替）；
缺 consumer→400。事件触发源=touch ~/.dsh/maestro/fleet.json（mtime-only，
零内容变更）；kinds 过滤在服务侧，网关白名单为防御层。

门（整门幂等可重复，×2 收口）：
  ① 订阅建立/取消干净（unsub/断开零泄漏，泵 cancel、订阅态清空、连接活）
  ② 事件转发不丢序不重复（以服务侧 msgid/seq 判；touch ×3）
  ③ 多客户端同事件互不扰（同 touch 双端各得一份同 msgid）
  ④ 断线重连→快照+增量无缝（离线期 touch 事件由快照补齐；流内无重；
     replay:true→false 单调；全量 msgid 并集无漏）
  ⑤ GW-001 回归：同 id 重放只回一次 res；死服（systemctl stop）→
     pm.res{error} 不崩连接；finally 恢复+恢复后透传自愈

后台存在 tickets 等无关流量，所有集合断言用包含式（触发集 ⊆ 收到集），
序断言用 seq 严格递增；不 commit 留编排者。
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import aiohttp  # noqa: E402

import rt_gateway  # noqa: E402

TOKEN = "gw002-live-gate"
UNIT = "pm-host-service"
FLEET = Path("~/.dsh/maestro/fleet.json").expanduser()
KIND = "fleet"  # 冒烟实证的真 kind；触发源 fleet.json touch

results: list[tuple[str, bool, str]] = []


def gate(name: str, ok: bool, detail: str) -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")


def systemctl(*args: str) -> None:
    subprocess.run(["systemctl", "--user", *args, UNIT], check=True)


def real_health(timeout: float = 2.0) -> dict | None:
    port = rt_gateway._pm_port()
    if port is None:
        return None
    try:
        with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/health", timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:  # noqa: BLE001
        return None


class Client:
    """WS 客户端 + 后台收帧泵（帧入本地队列，主流程按需取）。"""

    def __init__(self, name: str, gw: rt_gateway.VoiceGateway) -> None:
        self.name = name
        self.gw = gw
        self.http: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.q: asyncio.Queue = asyncio.Queue()
        self.reader: asyncio.Task | None = None
        self.session_id: str | None = None  # auth.ok 签发，匹配服务端 WsSession

    async def connect(self) -> None:
        self.http = aiohttp.ClientSession()
        self.ws = await self.http.ws_connect(f"ws://127.0.0.1:{self.gw.port}/ws")
        await self.ws.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        msg = await asyncio.wait_for(self.ws.receive(), 5)
        body = msg.json()
        assert body["t"] == "auth.ok", body
        self.session_id = body["session_id"]
        self.reader = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        while True:
            msg = await self.ws.receive()
            if msg.type != aiohttp.WSMsgType.TEXT:
                return
            await self.q.put(json.loads(msg.data))

    async def send(self, frame: dict) -> None:
        await self.ws.send_str(json.dumps(frame))

    async def next(self, timeout: float = 8.0) -> dict:
        return await asyncio.wait_for(self.q.get(), timeout)

    async def drain(self, window: float = 1.0) -> list[dict]:
        out = []
        while True:
            try:
                out.append(await asyncio.wait_for(self.q.get(), window))
            except (asyncio.TimeoutError, TimeoutError):
                return out

    async def pm_events(self, window: float) -> list[dict]:
        return [f for f in await self.drain(window) if f.get("t") == "pm.event"]

    async def close(self) -> None:
        if self.reader is not None:
            self.reader.cancel()
            self.reader = None
        if self.ws is not None:
            await self.ws.close()
            self.ws = None
        if self.http is not None:
            await self.http.close()
            self.http = None


def touch_fleet() -> None:
    subprocess.run(["touch", str(FLEET)], check=True)


async def wait_service_back(deadline_s: float = 25.0) -> bool:
    end = time.monotonic() + deadline_s
    while time.monotonic() < end:
        rt_gateway._reset_pm_port_cache()
        if real_health() is not None:
            return True
        await asyncio.sleep(0.5)
    return False


async def wait_active_count(gw: rt_gateway.VoiceGateway, n: int,
                            deadline: float = 5.0) -> bool:
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        if len(gw._active) == n:
            return True
        await asyncio.sleep(0.05)
    return len(gw._active) == n


def server_session(gw: rt_gateway.VoiceGateway, client: Client):
    """按 auth.ok 的 session_id 找到服务端 WsSession（断言内部状态用）。"""
    for s in gw._active:
        if s.session_id == client.session_id:
            return s
    return None


async def skip_snapshot(c: Client) -> list[dict]:
    """吃掉订阅快照回放（replay:true 帧），直到首个增量边界.

    快照 burst 会先于任何 touch 到达并入队——不先清到 replay:false 边界，
    后续断言会消费到陈旧 epoch 帧（首轮实跑实证）。新启服务 epoch 可能为
    空（无快照无增量）——先自触发一次 touch 保证有流；边界帧（构造上必
    先于 ② 的 touch）一并消费丢弃。
    """
    snap: list[dict] = []
    touch_fleet()
    end = time.monotonic() + 12.0
    while time.monotonic() < end:
        ev = await c.next()
        if ev.get("t") != "pm.event":
            continue
        if ev.get("replay") is False:
            return snap  # 边界增量已消费（必先于 ② 触发）
        snap.append(ev)
    raise AssertionError(f"{c.name}: snapshot boundary not reached in 12s")


async def run() -> int:
    rt_gateway._reset_pm_port_cache()
    if real_health() is None:
        print("FAIL 前置: pm-host-service 不在")
        return 1
    print(f"前置 ok: pm-host-service v{real_health().get('version')} "
          f"port={rt_gateway._pm_port()}")

    gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                 topic_sources=None)
    await gw.start()
    a = b = a2 = None
    try:
        # ---- ⑤a GW-001 回归：同 id 重放只回一次 res（真服务 health）----
        a = Client("A", gw)
        await a.connect()
        req = {"t": "pm.req", "id": "jg-gw001-replay", "op": "health"}
        await a.send(req)
        await a.send(req)
        await a.send({"t": "ping"})
        frames = await a.drain(1.2)
        res = [f for f in frames if f.get("t") == "pm.res"]
        gate("⑤a 同id重放唯一res",
             len(res) == 1 and res[0].get("data", {}).get("status") == "ok",
             f"pm.res×{len(res)}")
        gate("⑤b 连接活性", "pong" in [f.get("t") for f in frames], "")

        # ---- ① 订阅建立：A/B 各一条上游流（同 token 上限 2 并发）----
        b = Client("B", gw)
        await b.connect()
        for c in (a, b):
            await c.send({"t": "pm.sub", "id": f"sub-{c.name}",
                          "kinds": [KIND]})
            r = await c.next()
            gate(f"①sub-{c.name} pm.res",
                 r.get("t") == "pm.res" and r.get("data", {}).get("subscribed") == [KIND],
                 json.dumps(r, ensure_ascii=False)[:120])
        await asyncio.sleep(0.5)
        gate("①双客户端各持订阅",
             server_session(gw, a)._pm_sub is not None
             and server_session(gw, b)._pm_sub is not None
             and not server_session(gw, a)._pm_sub["pump"].done(),
             "A/B _pm_sub 在册且泵活")
        for c in (a, b):  # 清快照回放，对齐到增量边界
            snap = await skip_snapshot(c)
            print(f"  ({c.name} snapshot×{len(snap)} drained)")

        # ---- ②/③ 触发 ×3：不丢序不重复 + 多端互不扰 ----
        seen_a: list[dict] = []
        seen_b: list[dict] = []
        for i in range(3):
            touch_fleet()
            ea = await a.next()
            eb = await b.next()
            while ea.get("t") != "pm.event":
                ea = await a.next()
            while eb.get("t") != "pm.event":
                eb = await b.next()
            seen_a.append(ea)
            seen_b.append(eb)
            await asyncio.sleep(0.3)
        ids_a = [e["msgid"] for e in seen_a]
        ids_b = [e["msgid"] for e in seen_b]
        seqs_a = [e["seq"] for e in seen_a]
        gate("②touch×3 全达 A(增量)", len(set(ids_a)) == 3
             and all(e.get("replay") is False for e in seen_a), f"{ids_a}")
        gate("②A 序不乱(seq 严格递增)", seqs_a == sorted(seqs_a), f"{seqs_a}")
        gate("②B 同步全达", len(set(ids_b)) == 3
             and all(e.get("replay") is False for e in seen_b), f"{ids_b}")
        gate("③多端同事件同 msgid",
             all(ea["msgid"] == eb["msgid"] for ea, eb in zip(seen_a, seen_b)),
             f"A∩B={sorted(set(ids_a) & set(ids_b))}")
        gate("③帧形契约(t/kind/replay)",
             all(e.get("t") == "pm.event" and e.get("kind") == KIND
                 and isinstance(e.get("replay"), bool) for e in seen_a),
             f"sample={json.dumps(seen_a[0], ensure_ascii=False)[:140]}")

        # ---- ①取消干净：unsub A → 泵终结、订阅态清空、连接仍活 ----
        sess_a = server_session(gw, a)
        await a.send({"t": "pm.unsub", "id": "unsub-a"})
        r = await a.next()
        while r.get("t") == "pm.event":  # 跳过后台增量，直到回执
            r = await a.next()
        gate("①unsub pm.res",
             r.get("t") == "pm.res" and r.get("data", {}).get("was_subscribed") is True,
             json.dumps(r, ensure_ascii=False)[:120])
        for _ in range(40):
            if sess_a._pm_sub is None:
                break
            await asyncio.sleep(0.05)
        gate("①取消零泄漏", sess_a._pm_sub is None, f"_pm_sub={sess_a._pm_sub}")
        await a.send({"t": "ping"})
        pong = await a.next()
        while pong.get("t") == "pm.event":
            pong = await a.next()
        gate("①连接未断", pong.get("t") == "pong", "")
        for _ in await a.pm_events(0.8):  # 清在飞增量，再做零投递断言
            pass
        touch_fleet()
        leaked = [f for f in await a.pm_events(1.2)]
        gate("①取消后零投递", leaked == [], f"leaked={leaked}")

        # ---- ④ 断线重连：快照+增量无缝 ----
        await a.close()
        gate("④A 断开到位", await wait_active_count(gw, 1), f"active={len(gw._active)}")
        missed_ids = []
        for i in range(2):
            touch_fleet()
            await asyncio.sleep(1.0)
        pre_ids = set(ids_a)  # 断线前已见
        # B 侧补看错过的 2 条（也证明 B 未受 A 断开影响；replay:false=真增量）
        for _ in range(2):
            eb = await b.next()
            while eb.get("t") != "pm.event" or eb.get("replay") is not False:
                eb = await b.next()
            missed_ids.append(eb["msgid"])
        gate("④B 跨 A 断线无缺口", len(set(missed_ids)) == 2, f"{missed_ids}")

        a2 = Client("A2", gw)
        await a2.connect()
        await a2.send({"t": "pm.sub", "id": "sub-a2", "kinds": [KIND]})
        r = await a2.next()
        while r.get("t") == "pm.event":
            r = await a2.next()
        gate("④A2 重订受理", r.get("t") == "pm.res", json.dumps(r)[:100])
        snap, inc, saw_inc = [], [], False
        ok_monotone = True
        end = time.monotonic() + 10.0  # 有界读：快照补齐即出循环
        while time.monotonic() < end:
            try:
                ev = await asyncio.wait_for(a2.q.get(), 1.0)
            except (asyncio.TimeoutError, TimeoutError):
                if set(missed_ids) <= {e["msgid"] for e in snap}:
                    break
                continue
            if ev.get("t") != "pm.event":
                continue
            if ev.get("replay") is False:
                saw_inc = True
            elif saw_inc:
                ok_monotone = False  # replay:true 复现于增量后 → 违单调
            (inc if ev.get("replay") is False else snap).append(ev)
            if saw_inc and set(missed_ids) <= {e["msgid"] for e in snap}:
                break
        snap_ids = [e["msgid"] for e in snap]
        gate("④快照补齐离线事件", set(missed_ids) <= set(snap_ids),
             f"missed={missed_ids} ⊆ snap({len(snap_ids)})")
        gate("④快照先行+replay 单调",
             ok_monotone and bool(snap) and all(e.get("replay") is True for e in snap),
             f"snap×{len(snap)} inc×{len(inc)}")
        gate("④A2 流内无重复", len(snap_ids) == len(set(snap_ids)), "")
        # 存活流证明：再 touch 一次，A2 必收一条 replay:false 增量
        touch_fleet()
        while True:
            ev = await a2.next()
            if ev.get("t") == "pm.event" and ev.get("replay") is False:
                break
        inc.append(ev)
        gate("④A2 增量续流(replay:false)", ev["msgid"] not in set(snap_ids),
             f"inc={ev['msgid']}")
        union = pre_ids | set(missed_ids) | set(snap_ids) | {e["msgid"] for e in inc}
        gate("④全量无漏", set(ids_a) <= union and set(missed_ids) <= union,
             f"union={len(union)}")

        # ---- 清理：双端 unsub/断开零残留 ----
        await b.send({"t": "pm.unsub", "id": "unsub-b"})
        await b.next()
        await b.close()
        await a2.close()
        gate("清退: 会话全收", await wait_active_count(gw, 0, 5), f"active={len(gw._active)}")

        # ---- ⑤c GW-001 死服回归（排他窗口内，快进快出，finally 恢复）----
        systemctl("stop")
        await asyncio.sleep(0.4)
        gate("⑤d 服务确已停", real_health() is None, "")
        dead_client = Client("D", gw)
        await dead_client.connect()
        await dead_client.send({"t": "pm.req", "id": "jg-dead", "op": "health"})
        dead = await dead_client.next()
        gate("⑤e 死服 error 帧",
             dead.get("t") == "pm.res" and "error" in dead
             and dead.get("error", {}).get("code") in ("pm_down", "pm_unreachable", "pm_timeout"),
             json.dumps(dead, ensure_ascii=False)[:160])
        await dead_client.send({"t": "ping"})
        gate("⑤f 死服连接不断", (await dead_client.next()).get("t") == "pong", "")
        await dead_client.close()
    finally:
        for c in (a, b, a2):
            if c is not None:
                await c.close()
        subprocess.run(["systemctl", "--user", "start", UNIT], check=False)
        ok = await wait_service_back()
        print(f"[{'PASS' if ok else 'FAIL'}] 恢复: pm-host-service "
              f"{'v' + str((real_health() or {}).get('version')) if ok else 'NOT BACK'}")
        if ok:
            c = Client("R", gw)
            await c.connect()
            await c.send({"t": "pm.req", "id": "jg-restored", "op": "health"})
            back = await c.next()
            gate("⑤g 恢复后透传自愈",
                 back.get("t") == "pm.res"
                 and back.get("data", {}).get("status") == "ok",
                 json.dumps(back, ensure_ascii=False)[:120])
            await c.close()
        await gw.stop()

    failed = [r for r in results if not r[1]]
    print(f"\n== GW-002 joint gates: {len(results) - len(failed)}/{len(results)} green ==")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
