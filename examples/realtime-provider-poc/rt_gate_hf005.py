#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""HF-005 门：pm.fleet/pm.trace/pm.trace.expand 超时行可见（真 App xvfb）.

_pending_fill 的 kind→提示位映射缺 pm.fleet/pm.trace/pm.trace.expand：
这三类在途请求超时（或失败）时回填行落空——tab 静默。修复：补映射
（pm.fleet→席位舰页签状态行；pm.trace/pm.trace.expand→轨迹页签状态行）。

门：真 App 注入三类在途登记 → _pending_sweep 强制超时 → 三个提示位
逐类可见「<中文名>超时无响应」（×2：两轮独立注扫）。

自身分两层：缺 _HF005_UI_LEG 时为编排层（拉起 xvfb ui 腿并中继输
出/退出码）；置位时为 ui 腿（真 Tk 真 App）。
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

FAILS: list[str] = []


def check(name: str, ok: bool, detail: str) -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", flush=True)
    if not ok:
        FAILS.append(name)


def ui_leg() -> int:
    import time
    import tkinter as tk

    import rt_voice_app as rva

    root = tk.Tk()
    try:
        app = rva.App(root, "ws://127.0.0.1:1/ws", "t-hf005", False)
        sweep_at = time.monotonic() + rva.PENDING_TIMEOUT_S + 1.0

        def inject_and_sweep(kind: str) -> str:
            rid = f"hf005-{kind}"
            app._pending[rid] = {"kind": kind, "ts": time.monotonic(),
                                 "rid": rid}
            app._pending_sweep(now=sweep_at)
            return str(app.trace_note_var.get() if kind.startswith("pm.trace")
                       else app.fleet_ship_note_var.get())

        for rnd in (1, 2):  # ×2：两轮独立注扫，逐类可见
            got_fleet = inject_and_sweep("pm.fleet")
            check(f"r{rnd} pm.fleet 超时行落席位舰状态行",
                  "席位舰拉取超时无响应" in got_fleet, got_fleet[:80])
            got_trace = inject_and_sweep("pm.trace")
            check(f"r{rnd} pm.trace 超时行落轨迹状态行",
                  "轨迹拉取超时无响应" in got_trace, got_trace[:80])
            got_exp = inject_and_sweep("pm.trace.expand")
            check(f"r{rnd} pm.trace.expand 超时行落轨迹状态行",
                  "轨迹展开超时无响应" in got_exp, got_exp[:80])
    finally:
        root.destroy()
    return 0 if not FAILS else 1


def main() -> int:
    if os.environ.get("_HF005_UI_LEG") == "1":
        return ui_leg()
    env = dict(os.environ, _HF005_UI_LEG="1")
    proc = subprocess.run(
        ["xvfb-run", "-a", sys.executable, str(Path(__file__).resolve())],
        env=env, timeout=120)
    print(f"ui-leg exit={proc.returncode} fails={len(FAILS)}")
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
