#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-002 UI 腿（真 App 版）：真 Tk + 真 rt_voice_app.App + 真网关.

由 rt_gate_tk002.py 经 xvfb-run 拉起（argv: url token mode [rounds]）。
两种模式：

  latency  票板就绪（UI-READY）后按门侧指令逐轮消费 tickets 事件，实测
           「事件到达 obs_q → signature 变更（重绘完成）」时延并逐轮打印
           ``TK002-LAT <ms>``；rounds 轮齐 → UI-LEG-PASS。
  replay   就绪后主动 _tickets_fetch() 二次全量，对齐 state+视图+渲染
           文本指纹（TK002-REPLAY ... EQUAL|DIFF）——门②的进程内一半，
           跨进程一致性由门进程对比两次独立运行的 h1。

输出契约：UI-READY / TK002-LAT / TK002-REPLAY / UI-LEG-PASS|FAIL 单行
证据；root.destroy 直退不经 on_close（不落会话配置）。
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent
if str(POC_DIR) not in sys.path:
    sys.path.insert(0, str(POC_DIR))

import rt_voice_app as rva  # noqa: E402


def main() -> int:
    url, token, mode = sys.argv[1], sys.argv[2], sys.argv[3]
    rounds = int(sys.argv[4]) if len(sys.argv) > 4 else 2

    import tkinter as tk
    from tkinter import ttk

    root = tk.Tk()
    try:
        ttk.Style(root).theme_use("clam")
    except Exception:
        pass
    app = rva.App(root, url, token, False)
    state = {"t_event": None, "n_event": 0, "n_snapshot": 0,
             "sig": None, "fp": None, "rounds": 0, "lats": []}

    orig_put = app.obs_q.put

    def timed_put(frame, *a, **kw):
        if isinstance(frame, dict):
            if frame.get("t") == "pm.event" and frame.get("kind") == "tickets":
                state["t_event"] = time.monotonic()      # 事件到达（线端）
                state["n_event"] += 1
            data = frame.get("data")
            if frame.get("t") == "pm.res" and isinstance(data, dict) \
                    and isinstance(data.get("tickets"), list):
                state["n_snapshot"] += 1                 # 全量回包计数
        return orig_put(frame, *a, **kw)

    app.obs_q.put = timed_put
    app.toggle_obs()

    def fingerprint() -> str:
        view = rva.tickets_kanban_view(app._pm_tickets)
        rendered = {k: w.get("1.0", "end-1c")
                    for k, w in app.tickets_cols.items()}
        payload = {"sig": app._pm_tickets_sig,
                   "state": [app._pm_tickets[k]
                             for k in sorted(app._pm_tickets)],
                   "view": view, "rendered": rendered}
        blob = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                          default=str)
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

    def finish(ok: bool, detail: str) -> None:
        print(("UI-LEG-PASS " if ok else "UI-LEG-FAIL ") + detail, flush=True)
        root.destroy()

    t0 = time.monotonic()

    def poll() -> None:
        if time.monotonic() - t0 > 60:
            finish(False, f"mode={mode} timeout rounds={state['rounds']} "
                          f"events={state['n_event']} "
                          f"snaps={state['n_snapshot']}")
            return
        sig = app._pm_tickets_sig
        if state["sig"] is None and sig is not None:
            state["sig"], state["fp"] = sig, fingerprint()
            print(f"UI-READY sig={sig} count={len(app._pm_tickets)} "
                  f"fp={state['fp']}", flush=True)
            if mode == "replay":
                app._tickets_fetch()                 # 门②：进程内二次全量
        elif mode == "latency" and sig is not None and sig != state["sig"]:
            now = time.monotonic()
            fp = fingerprint()
            lat = ((now - state["t_event"]) * 1000.0
                   if state["t_event"] is not None else -1.0)
            state["lats"].append(lat)
            state["rounds"] += 1
            print(f"TK002-LAT {lat:.0f} round={state['rounds']} "
                  f"event_no={state['n_event']} sig={sig} "
                  f"view_changed={fp != state['fp']}", flush=True)
            state["sig"], state["fp"] = sig, fp
            if state["rounds"] >= rounds:
                finish(True, f"mode=latency rounds={state['rounds']} "
                             f"max_ms={max(state['lats']):.0f}")
                return
        elif mode == "replay" and state["sig"] is not None \
                and state["n_snapshot"] >= 2 and "fp2" not in state:
            fp2 = fingerprint()
            state["fp2"] = fp2
            equal = fp2 == state["fp"]
            print(f"TK002-REPLAY sig={app._pm_tickets_sig} "
                  f"h1={state['fp']} h2={fp2} "
                  f"{'EQUAL' if equal else 'DIFF'}", flush=True)
            finish(equal, f"mode=replay h1={state['fp']} h2={fp2} "
                          f"sig={app._pm_tickets_sig}")
            return
        root.after(30, poll)

    root.after(1000, poll)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
