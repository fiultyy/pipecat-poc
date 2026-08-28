#!/usr/bin/env python3
#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""TK-003 UI 腿（真 App 版）：真 Tk + 真 rt_voice_app.App + 真网关.

由 rt_gate_tk003.py 经 xvfb-run 拉起（argv: url token mode [rounds]）。
三种模式：

  touch     席位舰就绪（UI-READY）后按门侧 touch fleet.json，逐对消费
            尾随 fleet.snapshot 全文卡与 op=fleet 增量重拉卡，逐对打印
            ``TK003-DOC/SEATS <epoch>`` + ``TK003-CONSIST``（码集+状态
            一致性）；初始对照对 + rounds 个 touch 对齐 → UI-LEG-PASS。
  replay    就绪后冻结 now 指纹，主动 _fleet_ship_fetch() 二次全量，
            对齐 卡片+视图 指纹（TK003-REPLAY ... EQUAL|DIFF）——门②
            进程内一半，跨进程一致性由门进程对比两次独立运行 h1。
  degraded  死服网关下等 op=fleet 回 pm_down → 页签内降级标记点亮
            （fleet_ship_note_var 含「降级」）→ UI-LEG-PASS。

输出契约：UI-READY / TK003-DOC / TK003-SEATS / TK003-CONSIST /
TK003-REPLAY / UI-LEG-PASS|FAIL 单行证据；root.destroy 直退不经
on_close（不落会话配置）。
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
    state = {"ready": False, "doc_n": 0, "seats_n": 0,
             "doc_cards": {}, "seats_cards": {},
             "done_pairs": set(), "consists": [],
             "t_frozen": None, "fp": None}

    orig_put = app.obs_q.put

    def timed_put(frame, *a, **kw):
        if isinstance(frame, dict):
            t = frame.get("t")
            if t == "fleet.snapshot":
                state["doc_cards"] = rva.fleet_ship_cards_doc(
                    frame.get("fleet"))
                state["doc_n"] += 1
                print(f"TK003-DOC {time.time():.3f} "
                      f"cards={len(state['doc_cards'])}", flush=True)
            elif t == "pm.res":
                d = frame.get("data")
                if isinstance(d, dict) and isinstance(d.get("seats"), list):
                    state["seats_cards"] = rva.fleet_ship_cards_seats(
                        d.get("seats"))
                    state["seats_n"] += 1
                    print(f"TK003-SEATS {time.time():.3f} "
                          f"cards={len(state['seats_cards'])}", flush=True)
        return orig_put(frame, *a, **kw)

    app.obs_q.put = timed_put
    app.toggle_obs()

    def fingerprint() -> str:
        now = state["t_frozen"] or time.time()   # 冻结 now：剔除墙钟漂移
        view = rva.fleet_ship_view(app._pm_fleet_cards, now)
        payload = {"cards": [app._pm_fleet_cards[k]
                             for k in sorted(app._pm_fleet_cards)],
                   "view": view}
        blob = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                          default=str)
        return hashlib.sha1(blob.encode()).hexdigest()[:16]

    def try_pair() -> None:
        """最新全文卡 vs 最新增量重拉卡：码集+状态一致（门①）。"""
        n = min(state["doc_n"], state["seats_n"])
        if n < 1 or n in state["done_pairs"]:
            return
        if not state["doc_cards"] or not state["seats_cards"]:
            return
        state["done_pairs"].add(n)
        d, s = state["doc_cards"], state["seats_cards"]
        codes_eq = set(d) == set(s)
        status_eq = all(d[c].get("status") == s[c].get("status")
                        for c in d if c in s)
        ok = codes_eq and status_eq
        state["consists"].append(ok)
        print(f"TK003-CONSIST round={n} codes={len(d)} "
              f"{'EQUAL' if ok else 'DIFF'}", flush=True)

    def finish(ok: bool, detail: str) -> None:
        print(("UI-LEG-PASS " if ok else "UI-LEG-FAIL ") + detail, flush=True)
        root.destroy()

    t0 = time.monotonic()

    def poll() -> None:
        if time.monotonic() - t0 > 60:
            finish(False, f"mode={mode} timeout docs={state['doc_n']} "
                          f"seats={state['seats_n']} "
                          f"pairs={len(state['done_pairs'])}")
            return
        if not state["ready"] and app._pm_fleet_cards:
            state["ready"] = True
            extra = ""
            if mode == "replay":
                state["t_frozen"] = time.time()
                state["fp"] = fingerprint()
                extra = f" fp={state['fp']}"
            print(f"UI-READY cards={len(app._pm_fleet_cards)}{extra}",
                  flush=True)
            if mode == "replay":
                app._fleet_ship_fetch()          # 门②：进程内二次全量
        try_pair()
        if mode == "touch" and len(state["done_pairs"]) >= rounds + 1:
            ok = bool(state["consists"]) and all(state["consists"])
            finish(ok, f"mode=touch pairs={len(state['done_pairs'])} "
                       f"consists={state['consists']}")
            return
        if mode == "replay" and state["fp"] is not None \
                and state["seats_n"] >= 2 and "fp2" not in state:
            fp2 = fingerprint()
            state["fp2"] = fp2
            equal = fp2 == state["fp"]
            print(f"TK003-REPLAY h1={state['fp']} h2={fp2} "
                  f"{'EQUAL' if equal else 'DIFF'}", flush=True)
            finish(equal, f"mode=replay h1={state['fp']} h2={fp2}")
            return
        if mode == "degraded":
            note = app.fleet_ship_note_var.get()
            if "降级" in note:
                finish(root.winfo_exists(),
                       f"mode=degraded note={note!r} ui_alive=True")
                return
        root.after(30, poll)

    root.after(1000, poll)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
