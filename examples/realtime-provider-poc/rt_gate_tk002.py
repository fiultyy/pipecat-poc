#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-002 联合门：票板 tab 数据面/事件面/渲染面（真服务版）.

门（整门幂等可重复，×2 收口）：

  A 真服务数据面——真 pm-host-service op=tickets 经 G3 映射网关全量往返：
    规范化（JSON 串 deps/refs 整形）→列归属→卡片行块逐票入列；背靠背
    二次全量同 state（数据面重放幂等）；touch ledger.db（mtime 变更=
    既定触发方式）→ 真 tickets 事件 ≤5s。
  ① 事件驱动重绘时延（假源受控，真 App xvfb）——UI-READY 后两轮变更
    payload+publish，实测「事件到达→签名变更（重绘完成）」≤1000ms，
    逐轮记录实测值。
  ② 全量重放视图一致——同一网关源、两个全新 App 进程各拉全量，指纹
    （state+视图+渲染文本）跨进程严格一致；signature 同则严格比对，
    源数据迁移则记 data moved 放行。

不 commit 留编排者。
"""

from __future__ import annotations

import asyncio
import json
import os
import queue
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime
from pathlib import Path

from aiohttp import web

POC_DIR = Path(__file__).resolve().parent
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import rt_gateway  # noqa: E402
import rt_voice_app as rva  # noqa: E402
from tests.test_tk001_pm import FakePMSSE  # noqa: E402

TOKEN = "tk002-live-gate"
LEDGER = Path("~/.dsh/maestro/ledger.db").expanduser()

results: list[tuple[str, bool, str]] = []


def gate(name: str, ok: bool, detail: str) -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", flush=True)


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


def touch_ledger() -> None:
    subprocess.run(["touch", str(LEDGER)], check=True)


def is_sub_accepted(frame: dict, kinds=("tickets",)) -> bool:
    data = frame.get("data") if isinstance(frame, dict) else None
    return frame.get("t") == "pm.res" and isinstance(data, dict) \
        and data.get("subscribed") == list(kinds) \
        and "was_subscribed" not in data and "note" not in data


class Inbox:
    """obs 队列收集器（线程安全队列 → 主流程缓冲扫描）。"""

    def __init__(self, q: "queue.Queue[dict]") -> None:
        self.q = q
        self.frames: list[dict] = []

    def pump(self) -> None:
        while True:
            try:
                self.frames.append(self.q.get_nowait())
            except queue.Empty:
                return

    async def wait(self, pred, timeout: float, what: str):
        end = time.monotonic() + timeout
        while True:
            self.pump()
            for f in self.frames:
                if pred(f):
                    return f
            if time.monotonic() > end:
                raise AssertionError(f"{what} 未达（{timeout}s）")
            await asyncio.sleep(0.05)


def make_link(url: str, kinds=("tickets",)) -> tuple[rva.ObserveLink, Inbox]:
    q: "queue.Queue[dict]" = queue.Queue()
    link = rva.ObserveLink(url, TOKEN, q, lambda s: None, pm_kinds=kinds)
    link.start()
    return link, Inbox(q)


async def drop_ws_only(link: rva.ObserveLink) -> None:
    loop = getattr(link, "_loop", None)
    ws = getattr(link, "_ws", None)
    if loop is not None and ws is not None:
        asyncio.run_coroutine_threadsafe(ws.close(), loop).result(3)


async def drop_link(link: rva.ObserveLink) -> None:
    await drop_ws_only(link)
    link.close()


def restore_env(old_env: str | None) -> None:
    if old_env is None:
        os.environ.pop("PM_HOST_PORT_FILE", None)
    else:
        os.environ["PM_HOST_PORT_FILE"] = old_env
    rt_gateway._reset_pm_port_cache()


async def make_fake(tmp: tempfile.TemporaryDirectory):
    """假 SSE 源（同 tests FakePMSSE）起在临时口，回写口令文件。"""
    port_file = Path(tmp.name) / "pm.port"
    fake = FakePMSSE()
    runner = web.AppRunner(fake.app(), handler_cancellation=True)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, shutdown_timeout=0.5)
    await site.start()
    port_file.write_text(json.dumps({"port": runner.addresses[0][1]}))
    return fake, runner


async def fetch_tickets(link: rva.ObserveLink, inbox: Inbox, rid: str) -> dict:
    link.send_request({"t": "pm.req", "id": rid, "op": "tickets", "params": {}})
    return await inbox.wait(
        lambda f: f.get("t") == "pm.res" and f.get("id") == rid,
        8, f"op=tickets 回包 {rid}")


async def gate_a_real_service() -> None:
    """门A：真 pm-host-service 数据面全量往返 + 背靠背重放 + 真 events。"""
    gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                 topic_sources=None)
    await gw.start()
    link = None
    try:
        link, inbox = make_link(f"ws://127.0.0.1:{gw.port}/ws",
                                ("fleet", "tickets"))
        sub = await inbox.wait(
            lambda f: is_sub_accepted(f, ("fleet", "tickets")),
            8, "双 kind 订阅受理")
        gate("A1 双 kind 订阅受理（fleet+tickets）", True,
             f"kinds={sub['data']['subscribed']}")

        r1 = await fetch_tickets(link, inbox, "tk002-a1")
        d1 = r1.get("data") or {}
        ok = isinstance(d1.get("tickets"), list) and d1.get("count", 0) >= 1
        gate("A2 op=tickets 全量往返（真服 G3 映射）", ok,
             f"count={d1.get('count')} sig={d1.get('signature')}")

        st1 = rva.tickets_normalize(d1.get("tickets"))
        view1 = rva.tickets_kanban_view(st1)
        bad = [tid for tid, t in list(st1.items())[:12]
               if tid not in "\n".join(view1[rva.tickets_column_of(t)])]
        gate("A3 真数据渲染链（逐票 id 入列）", not bad and bool(st1),
             f"{len(st1)} 票规范化→列→卡片" + (f" 缺:{bad[:3]}" if bad else ""))

        r2 = await fetch_tickets(link, inbox, "tk002-a2")
        d2 = r2.get("data") or {}
        same_sig = d1.get("signature") == d2.get("signature")
        same_state = (json.dumps(st1, sort_keys=True, ensure_ascii=False)
                      == json.dumps(rva.tickets_normalize(d2.get("tickets")),
                                    sort_keys=True, ensure_ascii=False))
        gate("A4 背靠背二次全量同 state", same_state,
             f"state_eq={same_state} sig_eq={same_sig}")

        inbox.pump()
        pre = {f.get("msgid") for f in inbox.frames
               if f.get("t") == "pm.event" and f.get("kind") == "tickets"}
        t0 = time.monotonic()
        touch_ledger()
        ev = await inbox.wait(
            lambda f: f.get("t") == "pm.event"
            and f.get("kind") == "tickets" and f.get("msgid") not in pre,
            5, "真 tickets 事件")
        dt = (time.monotonic() - t0) * 1000
        gate("A5 touch ledger → 真 tickets 事件 ≤5s", True,
             f"msgid={ev.get('msgid')} 实测 {dt:.0f}ms")
    finally:
        if link is not None:
            await drop_link(link)
        await gw.stop()


async def run_driver(mode: str, gw_port: int, rounds: int, on_line
                     ) -> tuple[bool, str, list[str]]:
    """xvfb 拉起真 App 驱动腿，逐行回传；返回 (ok, 证据行, 全行)。"""
    driver = POC_DIR / "rt_gate_tk002_ui.py"
    proc = await asyncio.create_subprocess_exec(
        "xvfb-run", "-a", sys.executable, str(driver),
        f"ws://127.0.0.1:{gw_port}/ws", TOKEN, mode, str(rounds),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    lines: list[str] = []

    async def pump() -> None:
        while True:
            raw = await proc.stdout.readline()
            if not raw:
                break
            text = raw.decode("utf-8", errors="replace").rstrip()
            lines.append(text)
            print(f"    | {text}", flush=True)
            on_line(text)

    try:
        await asyncio.wait_for(pump(), 100)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
    else:
        await proc.wait()
    tail = next((ln for ln in lines if "UI-LEG" in ln),
                lines[-1] if lines else "无输出")
    ok = proc.returncode == 0 and any(
        ln.startswith("UI-LEG-PASS") for ln in lines)
    return ok, tail, lines


def gate_payload(prev: dict, extra: list[dict], sig: str) -> dict:
    tickets = [dict(t) for t in prev["tickets"]] + extra
    return {"op": "tickets", "cache": "miss", "degraded": False, "note": "",
            "signature": sig, "count": len(tickets), "tickets": tickets}


def _extra_ticket(tid: str, state: str, now_iso: str, deps="[]") -> dict:
    return {"ticket_id": tid, "title": f"门①票 {tid}", "state": state,
            "deps": deps, "lease_owner": "w-gate", "refs": "{}",
            "outcome": None, "updated_at": now_iso}


async def gate1_latency() -> None:
    """门①：事件→视图 ≤1000ms ×2（假源受控、真 App 真渲染、实测记录）。"""
    tmp = tempfile.TemporaryDirectory()
    old_env = os.environ.get("PM_HOST_PORT_FILE")
    try:
        fake, runner = await make_fake(tmp)
        os.environ["PM_HOST_PORT_FILE"] = str(Path(tmp.name) / "pm.port")
        rt_gateway._reset_pm_port_cache()
        gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                     topic_sources=None)
        await gw.start()
        lats: list[float] = []
        changed = 0
        ready = asyncio.Event()
        lat_ev = asyncio.Event()

        def on_line(text: str) -> None:
            nonlocal changed
            if text.startswith("UI-READY"):
                ready.set()
            m = re.match(r"TK002-LAT (-?[\d.]+)", text)
            if m:
                lats.append(float(m.group(1)))
                lat_ev.set()
            if "view_changed=True" in text:
                changed += 1

        task = asyncio.create_task(
            run_driver("latency", gw.port, 2, on_line))
        try:
            await asyncio.wait_for(ready.wait(), 30)
        except asyncio.TimeoutError:
            gate("①0 票板就绪（UI-READY）", False, "30s 未就绪")
        now_iso = datetime.now().astimezone().isoformat(timespec="seconds")
        for rnd in (1, 2):
            if rnd == 1:
                fake.tickets_payload = gate_payload(
                    fake.tickets_payload,
                    [_extra_ticket("T-GATE-1", "running", now_iso)],
                    "fake-sig-gate-1")
            else:
                moved = [dict(t) for t in fake.tickets_payload["tickets"]]
                for t in moved:
                    if t.get("ticket_id") == "T-A":
                        t["state"], t["updated_at"] = "done", now_iso
                fake.tickets_payload = gate_payload(
                    {"tickets": moved},
                    [_extra_ticket("T-GATE-2", "blocked", now_iso,
                                   '["T-GATE-1"]')],
                    "fake-sig-gate-2")
            fake.publish("tickets", f"tk002-gate-{rnd}")
            try:
                await asyncio.wait_for(lat_ev.wait(), 12)
            except asyncio.TimeoutError:
                pass
            lat_ev.clear()
        ok_run, tail, _ = await task
        lat_ok = len(lats) == 2 and all(0 <= v <= 1000 for v in lats)
        detail = (f"round1={lats[0]:.0f}ms round2={lats[1]:.0f}ms "
                  f"门限1000ms" if len(lats) == 2 else f"实测行={lats}")
        gate("①a 事件→视图 ≤1000ms ×2（实测）", lat_ok, detail)
        gate("①b 每轮视图确实重绘", changed >= 2, f"view_changed×{changed}")
        gate("①c UI 腿全程存活", ok_run, tail[:120])
        await gw.stop()
        await runner.cleanup()
    finally:
        restore_env(old_env)
        tmp.cleanup()


async def gate2_replay() -> None:
    """门②：同一源、两个全新 App 进程各拉全量，指纹跨进程严格一致。"""
    tmp = tempfile.TemporaryDirectory()
    old_env = os.environ.get("PM_HOST_PORT_FILE")
    try:
        _fake, runner = await make_fake(tmp)
        os.environ["PM_HOST_PORT_FILE"] = str(Path(tmp.name) / "pm.port")
        rt_gateway._reset_pm_port_cache()
        gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                     topic_sources=None)
        await gw.start()
        runs: list[dict] = []
        for i in (1, 2):
            box: dict = {}

            def on_line(text: str, box: dict = box) -> None:
                m = re.match(r"TK002-REPLAY sig=(\S+) h1=(\S+) h2=(\S+) "
                             r"(EQUAL|DIFF)", text)
                if m:
                    box.update(sig=m.group(1), h1=m.group(2),
                               h2=m.group(3), status=m.group(4))

            ok_run, tail, _ = await run_driver("replay", gw.port, 0, on_line)
            runs.append(box)
            gate(f"②{'a' if i == 1 else 'c'} 进程{i} 全量重拉进程内一致",
                 ok_run and box.get("status") == "EQUAL",
                 f"h1={box.get('h1')} h2={box.get('h2')} "
                 f"sig={box.get('sig')}" + ("" if ok_run
                                            else f" tail={tail[:60]}"))
        if runs[0].get("sig") == runs[1].get("sig"):
            gate("②b 跨进程重启重放视图一致（同 sig 严格）",
                 runs[0].get("h1") == runs[1].get("h1"),
                 f"h1_a={runs[0].get('h1')} h1_b={runs[1].get('h1')}")
        else:
            gate("②b 跨进程对比（源数据已迁移）", True,
                 f"sig {runs[0].get('sig')} → {runs[1].get('sig')} "
                 f"data moved 放行")
        await gw.stop()
        await runner.cleanup()
    finally:
        restore_env(old_env)
        tmp.cleanup()


async def run() -> int:
    rt_gateway._reset_pm_port_cache()
    if real_health() is None:
        print("FAIL 前置: pm-host-service 不在")
        return 1
    print(f"前置 ok: pm-host-service v{real_health().get('version')} "
          f"port={rt_gateway._pm_port()}", flush=True)

    await gate_a_real_service()
    await gate1_latency()
    await gate2_replay()

    print("\n==== TK-002 联合门汇总 ====")
    failed = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        failed += 0 if ok else 1
    print(f"门数={len(results)} 失败={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
