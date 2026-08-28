#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-001 联合门（真服务版）：真 pm-host-service v0.7.0 端到端 tk 客户端.

客户端侧跑真实 ``rt_voice_app.ObserveLink``（TK-001 的传输面：就绪自动
pm.sub、id 自增、断线指数退避、断流自动重订），UI 面跑真实 ``App``（
xvfb 虚显，rt_gate_tk001_ui.py 腿）。门（整门幂等可重复，×2 收口）：

  ① gateway 重启自动恢复订阅——拔线+换 gateway 实例后 ObserveLink 指数
    退避重连、自动重发全新 id 的 pm.sub、touch 后增量事件恢复；已确认
    的 pm.req 不重放（全程恰一份回包）；每连接恰一订（无 already no-op）
  ② pm-host 死 → 降级横幅不崩 UI——死服模拟用「冻结死口」（临时
    pm.port 指向无监听端口，零 systemd 触碰）；传输腿：pm_sub_failed
    error 帧 + pm.req 回 pm.res{error:pm_down} + 非 PM 往返（head.list）
    不受扰；UI 腿：真 App 横幅「降级」点亮、pm_down 回填、root 存活。

后台存在 tickets 等无关流量，事件断言一律包含式；不 commit 留编排者。
"""

from __future__ import annotations

import asyncio
import json
import os
import queue
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import rt_gateway  # noqa: E402
import rt_voice_app as rva  # noqa: E402

TOKEN = "tk001-live-gate"
KIND = "fleet"
FLEET = Path("~/.dsh/maestro/fleet.json").expanduser()
KEEP_REQ_ID = "tk001-gate-keep-1"

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


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def pm_seq(frame_id) -> int:
    try:
        return int(str(frame_id).rsplit("-", 1)[-1])
    except ValueError:
        return -1


def is_sub_accepted(frame: dict, kinds=(KIND,)) -> bool:
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


def make_link(url: str) -> tuple[rva.ObserveLink, Inbox]:
    q: "queue.Queue[dict]" = queue.Queue()
    link = rva.ObserveLink(url, TOKEN, q, lambda s: None,
                           pm_kinds=(KIND,))
    link.start()
    return link, Inbox(q)


async def drop_ws_only(link: rva.ObserveLink) -> None:
    """只拔底层连接、链接续跑（重连面接管）——真实断线的客户端视角。"""
    loop = getattr(link, "_loop", None)
    ws = getattr(link, "_ws", None)
    if loop is not None and ws is not None:
        asyncio.run_coroutine_threadsafe(ws.close(), loop).result(3)


async def drop_link(link: rva.ObserveLink) -> None:
    """收尾：拔线并等线程退出。"""
    await drop_ws_only(link)
    link.close()


async def gate1_restart_recovery() -> None:
    """门①：gateway 重启 → 指数退避重连 → 自动重订恢复，不重放已确认请求。"""
    port = free_port()
    gw = rt_gateway.VoiceGateway(port=port, token=TOKEN, head_provider=None,
                                 topic_sources=None)
    await gw.start()
    link = None
    try:
        link, inbox = make_link(f"ws://127.0.0.1:{port}/ws")
        sub1 = await inbox.wait(is_sub_accepted, 8, "首次自动订阅受理")
        gate("①a 就绪自动 pm.sub 受理", True,
             f"id={sub1['id']} kinds={sub1['data']['subscribed']}")

        link.send_request({"t": "pm.req", "id": KEEP_REQ_ID,
                           "op": "health", "params": {}})
        keep = await inbox.wait(
            lambda f: f.get("t") == "pm.res" and f.get("id") == KEEP_REQ_ID,
            8, "已确认请求回包")
        gate("①b 透传往返（真服务 health）", "data" in keep,
             str(keep.get("data"))[:80])

        touch_fleet()
        ev1 = await inbox.wait(
            lambda f: f.get("t") == "pm.event", 12, "重启前事件")
        gate("①c 订阅事件流（真服务 fleet）", True,
             f"msgid={ev1.get('msgid')} replay={ev1.get('replay')}")
        pre = {f.get("msgid") for f in inbox.frames if f.get("t") == "pm.event"}

        # gateway 重启：拔线（链接续跑）→ 旧实例下线 → 新实例同端口顶上
        t_drop = time.monotonic()
        await drop_ws_only(link)
        await gw.stop()
        gw2 = rt_gateway.VoiceGateway(port=port, token=TOKEN,
                                      head_provider=None, topic_sources=None)
        await gw2.start()
        sub2 = await inbox.wait(
            lambda f: is_sub_accepted(f) and f is not sub1,
            25, "重启后自动重订受理")
        dt = time.monotonic() - t_drop
        gate("①d 重启后订阅自动恢复", True,
             f"id={sub2['id']}（{dt:.1f}s 内，含指数退避）")
        gate("①e 帧_id 自增不复用", pm_seq(sub2["id"]) > pm_seq(sub1["id"]),
             f"{sub1['id']} → {sub2['id']}")
        backoff = [f for f in inbox.frames if "_link" in f and "重连" in f["_link"]]
        gate("①f 断线指数退避在跑", bool(backoff),
             backoff[0]["_link"] if backoff else "无重连行")

        touch_fleet()
        fresh = await inbox.wait(
            lambda f: f.get("t") == "pm.event"
            and (f.get("msgid") not in pre or f.get("replay") is False),
            12, "重启后增量事件")
        gate("①g 事件流恢复（真服务增量）", True,
             f"msgid={fresh.get('msgid')} replay={fresh.get('replay')}")

        await asyncio.sleep(1.5)                        # 静默窗：重放会露头
        inbox.pump()
        keeps = [f for f in inbox.frames
                 if f.get("t") == "pm.res" and f.get("id") == KEEP_REQ_ID]
        gate("①h 已确认请求不重放", len(keeps) == 1,
             f"tk001-gate-keep-1 回包×{len(keeps)}")
        subs = [f for f in inbox.frames if is_sub_accepted(f)]
        noop = [f for f in inbox.frames
                if isinstance(f.get("data"), dict)
                and f["data"].get("note") == "already-subscribed"]
        gate("①i 每连接恰一订（无重放无 no-op）",
             len(subs) == 2 and not noop,
             f"受理×{len(subs)} no-op×{len(noop)}")
    finally:
        if link is not None:
            await drop_link(link)
        await gw.stop()


async def gate2_dead_pm_banner() -> None:
    """门②：pm-host 死（冻结死口，零 systemd 触碰）→ 降级横幅不崩 UI。"""
    dead_port = free_port()                             # 无监听端口
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
    link = None
    try:
        # ---- 传输腿：真 ObserveLink vs 死 PM 口 ----
        link, inbox = make_link(f"ws://127.0.0.1:{gw.port}/ws")
        sub = await inbox.wait(is_sub_accepted, 8, "死服下 pm.sub 受理")
        gate("②a 死服订阅受理（网关先认订、泵异步败）", True, f"id={sub['id']}")

        brk = await inbox.wait(
            lambda f: f.get("t") == "error"
            and f.get("code") in ("pm_sub_failed", "pm_sub_ended"),
            15, "断流 error 帧")
        banner = rva.pm_banner_degraded_line(
            str(brk.get("code")), str(brk.get("message") or brk.get("msg") or ""))
        gate("②b 断流降级映射（横幅文案非空）",
             "降级" in banner and rva.pm_is_degraded(str(brk.get("code"))),
             banner[:80])

        link.send_request({"t": "pm.req", "id": "tk001-gate-dead-1",
                           "op": "health", "params": {}})
        res = await inbox.wait(
            lambda f: f.get("t") == "pm.res" and f.get("id") == "tk001-gate-dead-1",
            15, "死服 pm.req 回包")
        code, msg = rva.pm_error_of(res)
        gate("②c pm.req 回 pm_down（pm_* 族→降级）",
             rva.pm_is_degraded(code),
             f"error={code} {msg}".strip()[:80])

        link.send_request({"t": "head.list",
                           "req_id": "tk001-gate-head-1"})
        head = await inbox.wait(
            lambda f: f.get("t") == "head.list.result"
            and f.get("req_id") == "tk001-gate-head-1",
            8, "非 PM 往返")
        gate("②d 非 PM 面不受扰（链路未毒化）", True,
             f"profiles={len(head.get('profiles') or [])}")

        # ---- UI 腿：真 App（xvfb）连同一死服网关 ----
        driver = POC_DIR / "rt_gate_tk001_ui.py"
        py = sys.executable
        env = dict(os.environ)
        proc = await asyncio.create_subprocess_exec(
            "xvfb-run", "-a", py, str(driver),
            f"ws://127.0.0.1:{gw.port}/ws", TOKEN,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            env=env)
        try:
            out = await asyncio.wait_for(proc.communicate(), 90)
        except asyncio.TimeoutError:
            proc.kill()
            out = await proc.communicate()
        text = (out[0] or b"").decode("utf-8", errors="replace")
        ok = proc.returncode == 0 and "UI-LEG-PASS" in text
        line = [ln for ln in text.splitlines() if "UI-LEG" in ln]
        gate("②e 真 App 降级横幅不崩 UI", ok,
             (line[-1] if line else text.strip()[:120])[:160])
    finally:
        if link is not None:
            await drop_link(link)
        await gw.stop()
        if old_env is None:
            os.environ.pop("PM_HOST_PORT_FILE", None)
        else:
            os.environ["PM_HOST_PORT_FILE"] = old_env
        rt_gateway._reset_pm_port_cache()
        tmp.cleanup()


async def run() -> int:
    rt_gateway._reset_pm_port_cache()
    if real_health() is None:
        print("FAIL 前置: pm-host-service 不在")
        return 1
    print(f"前置 ok: pm-host-service v{real_health().get('version')} "
          f"port={rt_gateway._pm_port()}", flush=True)

    await gate1_restart_recovery()
    await gate2_dead_pm_banner()

    print("\n==== TK-001 联合门汇总 ====")
    failed = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        failed += 0 if ok else 1
    print(f"门数={len(results)} 失败={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
