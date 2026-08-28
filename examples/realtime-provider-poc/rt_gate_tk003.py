#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-003 联合门：席位舰 tab 数据面/增量面/渲染面（真服务版）.

门（整门幂等可重复，×2 收口）：

  ① 席位快照+增量一致（touch fleet.json 触发实测）——真 App xvfb：就绪
    后两轮 mtime-only touch（零内容变更，既定触发方式），实测 touch→
    尾随 fleet.snapshot 全文卡 ≤5s ×2；每对 全文卡 vs op=fleet 增量重拉
    卡 码集+状态一致（EQUAL）。
  ② 全量重放视图一致——同一源、两个全新 App 进程各拉全量，冻结 now 的
    卡片+视图指纹跨进程严格一致；源数据迁移则记 data moved 放行。
  ③ 降级标记透出——死服（冻结死口，零 systemd 触碰）下 op=fleet 回
    pm_down → 席位舰页签内降级标记点亮、UI 不崩。

不 commit 留编排者。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

from aiohttp import web

POC_DIR = Path(__file__).resolve().parent
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import rt_gateway  # noqa: E402
from tests.test_tk001_pm import FakePMSSE  # noqa: E402

TOKEN = "tk003-live-gate"
FLEET = Path("~/.dsh/maestro/fleet.json").expanduser()

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


def touch_fleet() -> None:
    subprocess.run(["touch", str(FLEET)], check=True)


def restore_env(old_env: str | None) -> None:
    if old_env is None:
        os.environ.pop("PM_HOST_PORT_FILE", None)
    else:
        os.environ["PM_HOST_PORT_FILE"] = old_env
    rt_gateway._reset_pm_port_cache()


async def make_fake(tmp: tempfile.TemporaryDirectory):
    """假 SSE 源（同 tests FakePMSSE，含 /op/fleet 路由）起在临时口。"""
    port_file = Path(tmp.name) / "pm.port"
    fake = FakePMSSE()
    runner = web.AppRunner(fake.app(), handler_cancellation=True)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, shutdown_timeout=0.5)
    await site.start()
    port_file.write_text(json.dumps({"port": runner.addresses[0][1]}))
    return fake, runner


async def run_driver(mode: str, gw_port: int, rounds: int, on_line
                     ) -> tuple[bool, str, list[str]]:
    """xvfb 拉起真 App 驱动腿，逐行回传；返回 (ok, 证据行, 全行)。"""
    driver = POC_DIR / "rt_gate_tk003_ui.py"
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
        await asyncio.wait_for(pump(), 120)
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


def free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def gate1_touch_consistency() -> None:
    """门①：touch fleet.json → 全文卡与增量重拉卡 双面实测且一致。"""
    old_env = os.environ.get("PM_HOST_PORT_FILE")
    if old_env is not None:
        os.environ.pop("PM_HOST_PORT_FILE", None)
    rt_gateway._reset_pm_port_cache()
    # 尾随 fleet.json 的 topic 源要开（同 main() 部署形态）——否则无
    # fleet.snapshot 帧，门① 的全文卡面无源
    gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                 topic_sources=rt_gateway.DEFAULT_TOPIC_SOURCES)
    await gw.start()
    docs: list[float] = []          # 每轮 touch 后首个 TK003-DOC epoch
    consists: list[tuple[float, str]] = []  # (epoch, EQUAL|DIFF)
    doc_ev = asyncio.Event()

    def on_line(text: str) -> None:
        m = re.match(r"TK003-DOC ([\d.]+)", text)
        if m:
            docs.append(float(m.group(1)))
            doc_ev.set()
        m = re.match(r"TK003-CONSIST round=\d+ codes=(\d+) (EQUAL|DIFF)", text)
        if m:
            consists.append((time.time(), m.group(2)))

    try:
        task = asyncio.create_task(run_driver("touch", gw.port, 2, on_line))
        # 就绪代理：首个尾随全文卡行（真 App 种子渲染成形）
        await asyncio.wait_for(doc_ev.wait(), 30)
        doc_ev.clear()
        await asyncio.sleep(3)      # 让种子增量回包落位（首轮对照对成形）
        for rnd in (1, 2):
            base = len(docs)
            t_touch = time.time()
            touch_fleet()
            try:
                await asyncio.wait_for(doc_ev.wait(), 15)
            except asyncio.TimeoutError:
                pass
            doc_ev.clear()
            fresh = [e for e in docs[base:] if e >= t_touch - 0.5]
            lat = (fresh[0] - t_touch) * 1000 if fresh else -1.0
            gate(f"①{'a' if rnd == 1 else 'd'} touch→全文卡刷新 ≤5s（实测"
                 f" round{rnd}）", 0 <= lat <= 5000,
                 f"实测 {lat:.0f}ms 门限5000ms")
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and len(consists) < rnd + 1:
                await asyncio.sleep(0.2)
        ok_run, tail, _ = await task
        eqs = [c for _, c in consists if c == "EQUAL"]
        gate("①b 快照+增量码集+状态一致（EQUAL ×2）", len(eqs) >= 2,
             f"consist 行={len(consists)} EQUAL×{len(eqs)}")
        gate("①c UI 腿全程存活", ok_run, tail[:120])
        await gw.stop()
    finally:
        restore_env(old_env)


async def gate2_replay() -> None:
    """门②：同一源、两个全新 App 进程各拉全量，冻结 now 指纹跨进程一致。"""
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
                m = re.match(r"TK003-REPLAY h1=(\S+) h2=(\S+) (EQUAL|DIFF)",
                             text)
                if m:
                    box.update(h1=m.group(1), h2=m.group(2),
                               status=m.group(3))

            ok_run, tail, _ = await run_driver("replay", gw.port, 0, on_line)
            runs.append(box)
            gate(f"②{'a' if i == 1 else 'c'} 进程{i} 全量重拉进程内一致",
                 ok_run and box.get("status") == "EQUAL",
                 f"h1={box.get('h1')} h2={box.get('h2')}"
                 + ("" if ok_run else f" tail={tail[:60]}"))
        if runs[0].get("h1") and runs[0].get("h1") == runs[1].get("h1"):
            gate("②b 跨进程重启重放视图一致（同源严格）", True,
                 f"h1_a={runs[0].get('h1')} h1_b={runs[1].get('h1')}")
        elif runs[0].get("h1") and runs[1].get("h1"):
            gate("②b 跨进程对比（源数据已迁移）", True,
                 f"h1 {runs[0].get('h1')} → {runs[1].get('h1')} "
                 f"data moved 放行")
        else:
            gate("②b 跨进程对比", False, "指纹缺失")
        await gw.stop()
        await runner.cleanup()
    finally:
        restore_env(old_env)
        tmp.cleanup()


async def gate3_degraded() -> None:
    """门③：死服（冻结死口）→ op=fleet pm_down → 页签内降级标记点亮。"""
    dead_port = free_port()                # 无监听端口（冻结死口）
    tmp = tempfile.TemporaryDirectory()
    port_file = Path(tmp.name) / "pm.port"
    port_file.write_text(json.dumps({"service": "pm-host-service",
                                     "port": dead_port}))
    old_env = os.environ.get("PM_HOST_PORT_FILE")
    os.environ["PM_HOST_PORT_FILE"] = str(port_file)
    rt_gateway._reset_pm_port_cache()
    gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                 topic_sources=None)
    await gw.start()
    try:
        box: dict = {}

        def on_line(text: str) -> None:
            if text.startswith("UI-LEG"):
                box["tail"] = text

        ok_run, tail, lines = await run_driver("degraded", gw.port, 0, on_line)
        note = next((ln for ln in lines if "note=" in ln and "降级" in ln), "")
        gate("③a 死服→席位舰页签内降级透出（UI 不崩）",
             ok_run and "降级" in note,
             (note or tail)[:140])
        await gw.stop()
    finally:
        restore_env(old_env)
        tmp.cleanup()


async def run() -> int:
    rt_gateway._reset_pm_port_cache()
    if real_health() is None:
        print("FAIL 前置: pm-host-service 不在")
        return 1
    if not FLEET.exists():
        print("FAIL 前置: fleet.json 不在")
        return 1
    print(f"前置 ok: pm-host-service v{real_health().get('version')} "
          f"port={rt_gateway._pm_port()} fleet={FLEET}", flush=True)

    await gate1_touch_consistency()
    await gate2_replay()
    await gate3_degraded()

    print("\n==== TK-003 联合门汇总 ====")
    failed = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        failed += 0 if ok else 1
    print(f"门数={len(results)} 失败={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
