#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-004 联合门：轨迹视图 tab（真服务版，0699 会话=真实大样本）.

门（整门幂等可重复，×2 收口）：

  ① 20k 字符级 session 滚动不卡顿（阈值实测记录）——真 App xvfb 拉真
    0699 全量轨迹（10.9M 字符级 log，服务端 head.compact 折叠到 20k
    字符预算），实测渲染 ≤2000ms、整文本滚动扫掠 ≤1000ms、渲染字符
    数 ≤21k（20k 字符级实证）。
  ② 折叠摘要行可展开——主区出现折叠摘要行 → 展开按钮续查被折叠头部
    → 展开区非空 ≤20s（实测）。
  ③ 过滤器重放同视图——seq_range 取不可变历史窗（200 seq 窄窗）同参
    连拉两次：渲染文本指纹严格一致（会话在长、sig 可动，历史窗不动）。

不 commit 留编排者。
"""

from __future__ import annotations

import asyncio
import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import rt_gateway  # noqa: E402

TOKEN = "tk004-live-gate"
FLEET = Path("~/.dsh/maestro/fleet.json").expanduser()
SELF_CODE = "0699"

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


def self_session_id() -> str:
    """0699 会话 id（fleet.json 席位表）=真实大样本。"""
    data = json.loads(FLEET.read_text())
    return str(data["fleet"][SELF_CODE]["sessionId"])


def probe_trace_lock(sid: str) -> int:
    """门快照锁定样本（修 B）：现时 max seq（type=turn/start 投影的
    seq_range 上界，小响应）——此后所有拉取 seqTo=lock，冻结在既有
    历史，会话转录随后续写不进窗，膨胀免疫。探测失败 → 0（不锁）。"""
    try:
        port = rt_gateway._pm_port()
        q = urllib.parse.quote(sid, safe="")
        with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/op/trace?sessionId={q}"
                f"&type=turn/start", timeout=8) as resp:
            d = json.loads(resp.read())
        sr = (d.get("matched") or {}).get("seq_range") or []
        return int(sr[1]) if len(sr) == 2 and isinstance(sr[1], int) else 0
    except Exception:  # noqa: BLE001 — 锁探测失败不阻塞，退化为不锁
        return 0


async def run_driver(mode: str, gw_port: int, sid: str, lock: int
                     ) -> tuple[bool, str, list[str]]:
    """xvfb 拉起真 App 驱动腿，逐行回传；返回 (ok, 证据行, 全行)。"""
    driver = POC_DIR / "rt_gate_tk004_ui.py"
    proc = await asyncio.create_subprocess_exec(
        "xvfb-run", "-a", sys.executable, str(driver),
        f"ws://127.0.0.1:{gw_port}/ws", TOKEN, mode, sid, str(lock),
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

    try:
        await asyncio.wait_for(pump(), 150)
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


async def gate1_perf(gw_port: int, sid: str, lock: int) -> None:
    """门①：20k 字符级渲染+滚动 实测阈值（真 App 真滚动扫掠，锁窗样本）。"""
    ok_run, tail, lines = await run_driver("perf", gw_port, sid, lock)
    rend = [float(m.group(1)) for x in lines
            if (m := re.match(r"TK004-RENDER ([\d.]+)", x))]
    scrl = [float(m.group(1)) for x in lines
            if (m := re.match(r"TK004-SCROLL ([\d.]+)", x))]
    pays = [int(m.group(1)) for x in lines
            if (m := re.match(r"TK004-PAYLOAD (\d+)", x))]
    r = rend[0] if rend else -1.0
    s = scrl[0] if scrl else -1.0
    p = pays[0] if pays else -1
    gate("①a 折叠后渲染 ≤2000ms（实测）", 0 <= r <= 2000,
         f"实测 {r:.0f}ms 门限2000ms")
    gate("①b 20k 字符级滚动扫掠 ≤1000ms（实测）", 0 <= s <= 1000,
         f"实测 {s:.0f}ms 门限1000ms")
    gate("①c 实发投影为 20k 字符级（served entries ≤ 21000）",
         0 <= p <= 21000, f"实测 served payload {p} 字符（预算 20000）")
    gate("①d UI 腿全程存活", ok_run, tail[:120])


async def gate2_expand(gw_port: int, sid: str, lock: int) -> None:
    """门②：折叠摘要行可展开（锁窗样本：真被折叠头部的续查，膨胀免疫）。"""
    ok_run, tail, lines = await run_driver("expand", gw_port, sid, lock)
    m = next((re.match(r"TK004-EXPAND lines=(\d+) ms=([\d.]+)", x)
              for x in lines if x.startswith("TK004-EXPAND")), None)
    if m:
        n, ms = int(m.group(1)), float(m.group(2))
        gate("②a 折叠展开区非空 ≤20s（实测）", ok_run and n >= 1 and ms <= 20000,
             f"lines={n} 实测 {ms:.0f}ms 门限20000ms")
    else:
        gate("②a 折叠展开区非空 ≤20s（实测）", False, tail[:120])


async def gate3_replay(gw_port: int, sid: str, lock: int) -> None:
    """门③：过滤器重放同视图（不可变 seq 窗同参连拉，指纹严格一致）。"""
    ok_run, tail, lines = await run_driver("replay", gw_port, sid, lock)
    m = next((re.match(r"TK004-REPLAY (EQUAL|DIFF)", x)
              for x in lines if x.startswith("TK004-REPLAY")), None)
    gate("③a 过滤器重放同视图（不可变窗严格）",
         ok_run and m is not None and m.group(1) == "EQUAL",
         (next((x for x in lines if x.startswith("TK004-REPLAY")), tail)
          )[:140])


async def run() -> int:
    rt_gateway._reset_pm_port_cache()
    if real_health() is None:
        print("FAIL 前置: pm-host-service 不在")
        return 1
    sid = self_session_id()
    lock = probe_trace_lock(sid)
    print(f"前置 ok: pm-host-service v{real_health().get('version')} "
          f"port={rt_gateway._pm_port()} sid={sid} lock={lock}", flush=True)

    gw = rt_gateway.VoiceGateway(port=0, token=TOKEN, head_provider=None,
                                 topic_sources=None)
    await gw.start()
    try:
        await gate1_perf(gw.port, sid, lock)
        await gate2_expand(gw.port, sid, lock)
        await gate3_replay(gw.port, sid, lock)
    finally:
        await gw.stop()

    print("\n==== TK-004 联合门汇总 ====")
    failed = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        failed += 0 if ok else 1
    print(f"门数={len(results)} 失败={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
