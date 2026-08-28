#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""gate ② UI 腿（真 App 版）：真 Tk + 真 rt_voice_app.App + 真网关（死 PM 口）.

由 rt_gate_tk001.py 经 xvfb-run 拉起。验证门：pm-host-service 死 →
降级横幅点亮、透传 pm.req 回填 pm_down、UI 不崩（root 存活走完全程）。

输出契约：单行 ``UI-LEG-PASS <证据>``（成功）或 ``UI-LEG-FAIL <现场>``
（失败）；进程退出码随行。不经 on_close 落会话配置（root.destroy 直退，
不污染 ~/.rt_voice_app_session.json）。
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import rt_voice_app as rva  # noqa: E402


def main() -> int:
    url, token = sys.argv[1], sys.argv[2]

    import tkinter as tk
    from tkinter import ttk

    root = tk.Tk()
    try:
        ttk.Style(root).theme_use("clam")
    except Exception:
        pass
    app = rva.App(root, url, token, False)
    app.toggle_obs()                       # 真 ObserveLink：就绪即自动 pm.sub
    state = {"phase": "wait-degraded", "t0": time.monotonic()}

    def finish(ok: bool, detail: str) -> None:
        tag = "UI-LEG-PASS" if ok else "UI-LEG-FAIL"
        print(f"{tag} phase={state['phase']} {detail}", flush=True)
        root.destroy()

    def poll() -> None:
        banner = app.pm_banner_var.get()
        if time.monotonic() - state["t0"] > 30:
            finish(False, f"timeout banner={banner!r}")
            return
        if state["phase"] == "wait-degraded":
            if "降级" in banner:
                state["phase"] = "wait-pm-down"
                app.pm_op_var.set("health")     # 透传 pm.req（死服 → pm_down）
                app.pm_params_var.set("{}")
                app._pm_send()
        elif state["phase"] == "wait-pm-down":
            note = app.pm_note_var.get()
            if "pm_down" in note:
                ok = "降级" in banner and root.winfo_exists()
                finish(ok, f"banner={banner!r} note={note!r} ui_alive=True")
                return
        root.after(200, poll)

    root.after(1000, poll)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
