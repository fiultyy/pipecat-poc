#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-004 UI 腿（真 App 版）：真 Tk + 真 rt_voice_app.App + 真网关/真服.

由 rt_gate_tk004.py 经 xvfb-run 拉起（argv: url token mode sid lock）。
``lock`` = 门侧探测的固定 seq 上界（0=不锁）：所有拉取带 ``seqTo=lock``，
样本冻结在既有历史上——会话转录随后续写不进窗，膨胀免疫（门复验 B 修）。

  perf    锁窗拉全量（服务端 head.compact 折叠到 20k 字符级）→ 实测渲染
          时长（TK004-RENDER）、真实渲染文本（tag 着色轨迹时间线）整文
          扫掠时长与字符数（TK004-SCROLL / TK004-SCROLLCHARS）、实发
          载荷（TK004-PAYLOAD）。
  expand  锁窗拉到折叠 → _trace_expand()（被折叠头部续查）→ 展开区非空
          （TK004-EXPAND）。锁定窗整段被折叠（无保留区边界）时按上界
          逐次回退重锁（≤3 次）；应用层空态/失败行按 FAIL 判——不静默。
  replay  锁窗内取不可变 seq 窄窗（200 seq）同参连拉两次 → 渲染文本
          指纹严格一致（TK004-VIEW1/2 + TK004-REPLAY）。

输出契约：UI-READY / TK004-* / UI-LEG-PASS|FAIL 单行证据；root.destroy
直退不经 on_close（不落会话配置）。
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

SILENT_MARKS = ("⚠", "⊘ 无法展开", "（空）")


def main() -> int:
    url, token, mode, sid = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    lock = int(sys.argv[5]) if len(sys.argv) > 5 else 0

    import tkinter as tk
    from tkinter import ttk

    root = tk.Tk()
    try:
        ttk.Style(root).theme_use("clam")
    except Exception:
        pass
    app = rva.App(root, url, token, False)
    state = {"render_ms": -1.0, "renders": 0, "base": -1,
             "lock": lock, "attempts": 0, "phase": "main"}

    orig_render = app._render_trace

    def timed_render(*a, **kw):
        t0 = time.perf_counter()
        orig_render(*a, **kw)
        state["render_ms"] = (time.perf_counter() - t0) * 1000.0
        state["renders"] += 1

    app._render_trace = timed_render
    app.toggle_obs()

    def text_sha(widget) -> str:
        blob = widget.get("1.0", "end-1c")
        return hashlib.sha1(blob.encode()).hexdigest()[:16]

    def send_main():
        app.trace_from_var.set("")
        app.trace_to_var.set(str(state["lock"]) if state["lock"] else "")
        state["base"] = state["renders"]
        app._trace_fetch()

    def finish(ok: bool, detail: str) -> None:
        print(("UI-LEG-PASS " if ok else "UI-LEG-FAIL ") + detail, flush=True)
        root.destroy()

    app.trace_sid_var.set(sid)

    def poll() -> None:
        if time.monotonic() - t0 > 120:
            finish(False, f"mode={mode} timeout renders={state['renders']} "
                          f"phase={state['phase']}")
            return
        if state["phase"] == "main" and state["base"] < 0:
            send_main()
        elif state["phase"] == "main" and state["renders"] > state["base"]:
            if mode == "perf":
                state["phase"] = "done"
                data = app._trace_last or {}
                entries = data.get("entries")
                # 服务端实发载荷（head.compact 折叠后）= entries 序列化长度
                payload = len(json.dumps(entries, ensure_ascii=False,
                                         default=str))
                # 滚动实测走真实渲染路径：主区即 tag 着色轨迹时间线
                # （_render_trace 渲染产物），整文扫掠它——非代理合成窗
                root.update_idletasks()
                scroll_chars = len(app.trace_text.get("1.0", "end-1c"))
                t0s = time.perf_counter()
                for i in range(50):
                    app.trace_text.yview_moveto(i / 49.0)
                    app.trace_text.update_idletasks()
                scroll_ms = (time.perf_counter() - t0s) * 1000.0
                print(f"TK004-RENDER {state['render_ms']:.0f}", flush=True)
                print(f"TK004-SCROLL {scroll_ms:.0f}", flush=True)
                print(f"TK004-SCROLLCHARS {scroll_chars}", flush=True)
                print(f"TK004-PAYLOAD {payload}", flush=True)
                finish(True, f"mode=perf render_ms={state['render_ms']:.0f} "
                             f"scroll_ms={scroll_ms:.0f} "
                             f"scroll_chars={scroll_chars} "
                             f"payload={payload} lock={state['lock']}")
                return
            if mode == "expand":
                body = app.trace_text.get("1.0", "end-1c")
                if "可展开" not in body:
                    finish(False, "mode=expand 主区无折叠摘要行")
                    return
                params = rva.trace_expand_params(app._trace_last)
                if params is not None:
                    state["phase"] = "waiting"
                    state["t_expand"] = time.perf_counter()
                    app._trace_fetch(params, target="expand")
                else:
                    # 锁定窗整段被折叠（无保留区边界）→ 回退重锁再试
                    state["attempts"] += 1
                    sr = ((app._trace_last or {}).get("matched") or {}) \
                        .get("seq_range") or [0, 0]
                    lo, hi = (sr[0] or 0), (sr[1] or 0)
                    if state["attempts"] <= 3 and hi - 100000 > lo + 10:
                        state["lock"] = hi - 100000
                        print(f"TK004-RELOCK {state['lock']}", flush=True)
                        state["phase"] = "main"
                        send_main()
                    else:
                        finish(False, "mode=expand 展开无边界（可见空态，"
                                      "不静默）attempts="
                                      f"{state['attempts']}")
                        return
            elif mode == "replay":
                data = app._trace_last or {}
                sr = (data.get("matched") or {}).get("seq_range") or []
                if not (isinstance(sr, list) and len(sr) == 2
                        and isinstance(sr[0], int) and isinstance(sr[1], int)):
                    finish(False, f"mode=replay seq_range 缺失 sr={sr}")
                    return
                a, b = sr[0], min(sr[0] + 200, sr[1])
                app.trace_from_var.set(str(a))
                app.trace_to_var.set(str(b))
                state["phase"] = "win1"
                state["base"] = state["renders"]
                app._trace_fetch()                   # 窗拉 B（不可变历史窗）
        elif state["phase"] == "waiting":
            lines = [l for l in
                     app.trace_expand_text.get("1.0", "end-1c")
                     .splitlines() if l.strip()]
            if lines:
                state["phase"] = "done"
                ms = (time.perf_counter() - state["t_expand"]) * 1000.0
                first = lines[0].strip()
                ok = not first.startswith(SILENT_MARKS)
                print(f"TK004-EXPAND lines={len(lines)} ms={ms:.0f} "
                      f"first={first[:24]!r}", flush=True)
                finish(ok, f"mode=expand expand_lines={len(lines)} "
                           f"ms={ms:.0f}")
                return
        elif state["phase"] == "win1" and state["renders"] > state["base"]:
            state["phase"] = "win2"
            state["view1"] = text_sha(app.trace_text)
            print(f"TK004-VIEW1 {state['view1']}", flush=True)
            state["base"] = state["renders"]
            app._trace_fetch()                       # 同参重放 C
        elif state["phase"] == "win2" and state["renders"] > state["base"]:
            state["phase"] = "done"
            state["view2"] = text_sha(app.trace_text)
            print(f"TK004-VIEW2 {state['view2']}", flush=True)
            equal = state["view1"] == state["view2"]
            print(f"TK004-REPLAY {'EQUAL' if equal else 'DIFF'}",
                  flush=True)
            finish(equal, f"mode=replay view1={state['view1']} "
                          f"view2={state['view2']} "
                          f"{'EQUAL' if equal else 'DIFF'}")
            return
        root.after(30, poll)

    t0 = time.monotonic()
    root.after(1000, poll)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
