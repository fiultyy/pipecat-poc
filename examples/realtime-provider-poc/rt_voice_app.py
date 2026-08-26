#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""rt-voice · ONE 桌面客户端：语音面 + 编排观测（无浏览器引擎；KG 12）。

stdlib tkinter（Notebook 页签）+ sounddevice 原生采集 + numpy FFT + aiohttp WS，
直连 rt-gateway（ws://host:8765/ws）。双连接分面：

- 语音连接（现状保留）：PCM16LE/16k/mono 上行、扬声器下行、24 柱频谱 +
  RMS 电平；**按住说话**（全局热键默认 F9 或按住🎤按钮；🔒锁定=连续采集），
  静音语义只控采集上行，WS 会话保持；断线 2s 重连续接
- 观测连接（KG 11 §3 observe:true）：独立 WS，缺省订阅全部 topic——
  编排页（orch.* 任务树+时间线）、回合页（head.turn）、席位页
  （fleet.snapshot 表）、消息/票板页（bridge.msg + tickets.snapshot）

两连接同 token 计入网关并发上限 ≤2（即本客户端独占单 token 配额）。
帧协议与 rt_gateway.py §1 逐字对齐。

用法：
    python rt_voice_app.py [--url ws://127.0.0.1:8765/ws] [--token T]
    python rt_voice_app.py --selftest        # 无设备冒烟：UI 起即退
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import queue
import sys
import threading
import time
from pathlib import Path

import numpy as np
import sounddevice as sd

sys.path.insert(0, str(Path(__file__).parent))

RATE = 16000
BLOCK_MS = 50
BLOCK = RATE * BLOCK_MS // 1000          # 800 samples per callback
BARS = 24
DB_FLOOR = -60.0

LOG_SHORT = {
    "orch.dispatch": "派发", "orch.progress": "进展", "orch.done": "终稿",
    "auth.ok": "鉴权", "session.started": "会话", "session.ended": "结束",
    "gate.resolved": "闸解", "error": "错误",
}


class VoiceLink:
    """asyncio 侧：WS 会话机 + 音频上行泵 + 事件下行泵。

    与 tkinter 主线程只通过线程安全队列交换（tx: 采集块；rx: 事件行；
    spectrum: 最新块副本供 UI 取）。
    """

    def __init__(self, url: str, token: str, tx: "queue.Queue[bytes]",
                 rx: "queue.Queue[str]", on_state):
        import aiohttp

        self.url, self.token, self.tx, self.rx, self.on_state = url, token, tx, rx, on_state
        self.session_id: str | None = None
        self._aiohttp = aiohttp
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._out: sd.OutputStream | None = None

    # ---- lifecycle (called from tkinter thread) ----

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def close(self, graceful: bool):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    # ---- worker ----

    def _run(self):
        asyncio.run(self._worker())

    async def _worker(self):
        import aiohttp

        while not self._stop.is_set():
            try:
                self.on_state("connecting")
                async with aiohttp.ClientSession() as http:
                    async with http.ws_connect(self.url, max_msg_size=1 << 20) as ws:
                        await ws.send_json({"t": "auth", "token": self.token})
                        started = False
                        self.on_state("authing")
                        # reader task 收帧入队；主循环带超时轮询 _stop——
                        # close() 需要循环自己看到停（服务端不发帧时 receive
                        # 永park；直接 wait_for(ws.receive) 会踩 aiohttp 的
                        # cancel-半途状态坑，故帧经队列间接）
                        frames_q: asyncio.Queue = asyncio.Queue()

                        async def _reader():
                            async for m in ws:
                                await frames_q.put(m)

                        reader = asyncio.create_task(_reader())
                        pump = asyncio.create_task(self._pump_up(ws, lambda: started))
                        try:
                            while not self._stop.is_set():
                                try:
                                    msg = await asyncio.wait_for(
                                        frames_q.get(), timeout=0.5)
                                except asyncio.TimeoutError:
                                    continue
                                if msg.type == aiohttp.WSMsgType.BINARY:
                                    self._play(bytes(msg.data))
                                    continue
                                if msg.type != aiohttp.WSMsgType.TEXT:
                                    continue
                                data = json.loads(msg.data)
                                t = data.get("t", "")
                                if t == "auth.ok":
                                    self.session_id = data.get("session_id")
                                    await ws.send_json({"t": "session.start",
                                                        "session_id": self.session_id})
                                elif t == "session.started":
                                    started = True
                                    self.session_id = data.get("session_id") or self.session_id
                                    self.on_state("open")
                                elif t == "error":
                                    self.rx.put(f"[错误] {data.get('code')}: "
                                                f"{str(data.get('msg'))[:120]}")
                                else:
                                    kind = t.split(".")[0] if "." in t else t
                                    label = LOG_SHORT.get(t) or LOG_SHORT.get(kind, kind or "事件")
                                    body = json.dumps(data, ensure_ascii=False)
                                    self.rx.put(f"[{label}] {body[:200]}")
                        finally:
                            pump.cancel()
                            reader.cancel()
            except Exception as e:  # noqa: BLE001 — 断线重连面：任何异常都进退避
                self.rx.put(f"[链路] {type(e).__name__}: {str(e)[:100]}")
            if self._stop.wait(2.0):
                break
            self.rx.put("[链路] 2s 后重连…")
        self.on_state("closed")
        if self._out:
            try:
                self._out.stop()
                self._out.close()
            except Exception:
                pass

    async def _pump_up(self, ws, started):
        """tx 队列 → 二进制帧；200ms 合块（32KB/s 码率上限内）。"""

        while True:
            chunks: list[bytes] = []
            deadline = time.monotonic() + 0.2
            while time.monotonic() < deadline:
                try:
                    chunks.append(self.tx.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.01)
            if chunks and started():
                await ws.send_bytes(b"".join(chunks))

    def _play(self, pcm: bytes):
        try:
            if self._out is None:
                self._out = sd.OutputStream(samplerate=RATE, channels=1, dtype="int16")
                self._out.start()
            self._out.write(np.frombuffer(pcm, dtype=np.int16).reshape(-1, 1))
        except Exception:
            self._out = None  # 无输出设备时静默丢弃


class ObserveLink:
    """asyncio 侧：纯观测 WS 会话（KG 11 §3 observe:true——不建 head、
    拒媒体、缺省订阅全部 topic）。

    与语音连接生命周期解耦（观测可先于语音开；语音断不影响观测）。
    与 tkinter 只经 obs 队列交换（dict 帧，渲染在 UI 线程）。
    """

    def __init__(self, url: str, token: str, obs: "queue.Queue[dict]", on_state):
        self.url, self.token = url, token
        self.obs, self.on_state = obs, on_state
        self.session_id: str | None = None
        self.topics: list[str] | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def _run(self):
        asyncio.run(self._worker())

    async def _worker(self):
        import aiohttp

        while not self._stop.is_set():
            try:
                self.on_state("connecting")
                async with aiohttp.ClientSession() as http:
                    async with http.ws_connect(self.url, max_msg_size=1 << 21) as ws:
                        await ws.send_json({"t": "auth", "token": self.token})
                        self.on_state("authing")
                        async for msg in ws:
                            if self._stop.is_set():
                                break
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            data = json.loads(msg.data)
                            t = data.get("t", "")
                            if t == "auth.ok":
                                self.session_id = data.get("session_id")
                                await ws.send_json({"t": "session.start",
                                                    "observe": True,
                                                    "session_id": self.session_id})
                            elif t == "session.started":
                                self.topics = data.get("topics")
                                self.on_state("open")
                            elif t == "error":
                                self.obs.put({"_error": data})
                                self.on_state(f"error {data.get('code')}")
                                return
                            else:
                                self.obs.put(data)
            except Exception as e:  # noqa: BLE001 — 断线重连面
                self.obs.put({"_link": f"{type(e).__name__}: {str(e)[:100]}"})
            # asyncio-native wait (this coroutine owns the loop thread; a
            # threading.Event.wait here would block the loop — unlike
            # VoiceLink, which parks in its own thread)
            for _ in range(20):
                if self._stop.is_set():
                    break
                await asyncio.sleep(0.1)
            else:
                self.obs.put({"_link": "2s 后重连…"})
                continue
            break
        self.on_state("closed")


def fleet_rows(fleet: dict) -> list[tuple]:
    """fleet.snapshot payload → 表格行（code/alias/node/role/status）。

    payload 形如 {"fleet": {"<code>": {sessionId, alias, node, role,
    project, status, …}}}（FileTailer 全量快照，KG 11 §1）。
    """
    out = []
    entries = fleet.get("fleet") if isinstance(fleet, dict) else None
    if not isinstance(entries, dict):
        return out
    for code, e in sorted(entries.items()):
        if not isinstance(e, dict):
            continue
        out.append((code, e.get("alias", ""), e.get("node", ""),
                    e.get("role", ""), e.get("status", "")))
    return out


def orch_tree_lines(frames: list[dict]) -> list[str]:
    """orch.* 帧序列 → run→ref 任务树文本行（种子页三级树逻辑的纯函数化，
    便于单测；frames 顺序即到达序）。"""
    runs: dict[str, dict] = {}
    order: list[str] = []
    for f in frames:
        t = f.get("t", "")
        rid = f.get("run_id") or "—"
        if t == "orch.dispatch":
            node = runs.setdefault(rid, {"refs": [], "done": False})
            if rid not in order:
                order.append(rid)
            node["refs"].append(f.get("ref", "?"))
        elif t == "orch.done" and rid in runs:
            runs[rid]["done"] = True
    lines = []
    for rid in order:
        r = runs[rid]
        mark = "✓" if r["done"] else "…"
        lines.append(f"{mark} {rid}")
        for ref in r["refs"]:
            lines.append(f"   └ {ref}")
    return lines


def st_write(widget, line: str):
    """ScrolledText 追加一行并滚尾（state 恢复 disabled）。"""
    widget.config(state="normal")
    widget.insert("end", line.rstrip("\n") + "\n")
    widget.see("end")
    widget.config(state="disabled")


TURN_PHASE_LABEL = {
    "user_start": "🗣", "user_end": "·", "user_text": "💬",
    "assistant_start": "🤖", "assistant_end": "✅", "tool_call": "🔧",
    "interrupted": "⚡",
}


def turn_line(frame: dict) -> str:
    """head.turn 帧 → 单行呈现（phase 图标 + detail 截断）。"""
    phase = frame.get("phase", "?")
    icon = TURN_PHASE_LABEL.get(phase, "·")
    detail = str(frame.get("detail", "") or "")[:80]
    return f"{icon} {phase}{(' ' + detail) if detail else ''}"


class PushToTalk:
    """按住说话状态机：press/release → start/stop 动作。

    重复按下（按键重复/热键与按钮多入口并发）与无持有时的释放均不
    出动作——采集启停只跟随持有态边沿。
    """

    def __init__(self) -> None:
        self.held = False

    def press(self) -> str | None:
        if self.held:
            return None
        self.held = True
        return "start"

    def release(self) -> str | None:
        if not self.held:
            return None
        self.held = False
        return "stop"


class App:
    """tkinter 主线程：频谱画布 + 开关 + 事件面板。"""

    def __init__(self, root: "tkinter.Tk", url: str, token: str, selftest: bool,
                 ptt_key: str = "f9"):
        import tkinter as tk
        from tkinter import scrolledtext, ttk

        self.tk, self.ttk = tk, ttk
        self.root = root
        self.url, self.token = url, token
        self.ptt_key = ptt_key
        self.tx: queue.Queue[bytes] = queue.Queue(maxsize=64)
        self.rx: "queue.Queue[str]" = queue.Queue()
        self.latest = np.zeros(BLOCK, dtype=np.int16)
        self.sent_bytes = 0
        self.events = 0
        self.link = VoiceLink(url, token, self.tx, self.rx, self._set_state)
        self.obs_q: "queue.Queue[dict]" = queue.Queue()
        self.obs_link: ObserveLink | None = None
        self.orch_frames: list[dict] = []      # orch.* 帧序（树渲染源）
        self.stream: sd.InputStream | None = None

        root.title("rt-voice · ONE 桌面客户端（语音+观测）")
        root.geometry("980x640")
        root.minsize(780, 520)

        # ---- Notebook：语音页 + 观测四页签 ----
        self.nb = ttk.Notebook(root)
        self.nb.pack(fill="both", expand=True)

        voice_tab = ttk.Frame(self.nb, padding=0)
        self.nb.add(voice_tab, text=" 语音 ")

        # ---- 顶栏：地址 / token / 双连接开关 ----
        top = ttk.Frame(root, padding=(8, 6))
        top.pack(fill="x")
        ttk.Label(top, text="网关").pack(side="left")
        self.url_var = tk.StringVar(value=url)
        ttk.Entry(top, textvariable=self.url_var, width=34).pack(side="left", padx=(4, 10))
        ttk.Label(top, text="令牌").pack(side="left")
        self.token_var = tk.StringVar(value=token)
        ttk.Entry(top, textvariable=self.token_var, width=14, show="•").pack(side="left", padx=(4, 10))
        self.conn_btn = ttk.Button(top, text="连接", command=self.toggle_link, width=8)
        self.conn_btn.pack(side="left")
        self.state_var = tk.StringVar(value="未连接")
        ttk.Label(top, textvariable=self.state_var, foreground="#555").pack(side="left", padx=10)
        self.obs_btn = ttk.Button(top, text="观测", command=self.toggle_obs, width=8)
        self.obs_btn.pack(side="left")
        self.obs_state_var = tk.StringVar(value="观测未开")
        ttk.Label(top, textvariable=self.obs_state_var, foreground="#555").pack(side="left", padx=10)
        self.end_btn = ttk.Button(top, text="结束会话", command=self.end_session, width=10)
        self.end_btn.pack(side="right")

        # ---- 语音页：频谱 + 开关 + 事件面板 ----
        self.canvas = tk.Canvas(voice_tab, bg="#0b0f14", height=240, highlightthickness=0)
        self.canvas.pack(fill="both", expand=True, padx=8, pady=(6, 0))
        self.level_var = tk.StringVar(value="电平 ──────────")
        ttk.Label(voice_tab, textvariable=self.level_var, font=("monospace 10")).pack(anchor="w", padx=12)

        mid = ttk.Frame(voice_tab, padding=(8, 4))
        mid.pack(fill="x")
        self.ptt = PushToTalk()
        self.lock_var = tk.BooleanVar(value=False)
        self.mic_var = tk.StringVar(value=f"🎤 按住说话（热键 {ptt_key.upper()}）")
        self.mic_btn = tk.Button(mid, textvariable=self.mic_var,
                                 font=("system-ui 13",), bg="#1f6f43", fg="white",
                                 activebackground="#2a8f56", activeforeground="white",
                                 relief="flat", padx=18, pady=6, cursor="hand2")
        # 按钮即 PTT：按下开麦、松开闭麦（无 command——避免与按压语义双触发）
        self.mic_btn.bind("<ButtonPress-1>", lambda _e: self._ptt_press())
        self.mic_btn.bind("<ButtonRelease-1>", lambda _e: self._ptt_release())
        self.mic_btn.pack(side="left")
        ttk.Checkbutton(mid, text="🔒 锁定连续采集", variable=self.lock_var,
                        command=self._on_lock).pack(side="left", padx=10)
        self.stat_var = tk.StringVar(value="待连接")
        ttk.Label(mid, textvariable=self.stat_var).pack(side="left", padx=12)

        self.log = scrolledtext.ScrolledText(voice_tab, height=9, font=("monospace 9"),
                                             state="disabled", wrap="word")
        self.log.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        # ---- 观测页签 ----
        orch_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(orch_tab, text=" 编排 ")
        self.orch_tree = scrolledtext.ScrolledText(orch_tab, font=("monospace 9"),
                                                   state="disabled", height=8)
        self.orch_tree.pack(fill="both", expand=False)
        self.orch_log = scrolledtext.ScrolledText(orch_tab, font=("monospace 8"),
                                                  state="disabled", wrap="word")
        self.orch_log.pack(fill="both", expand=True)

        turn_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(turn_tab, text=" 回合 ")
        self.turn_log = scrolledtext.ScrolledText(turn_tab, font=("monospace 9"),
                                                  state="disabled", wrap="word")
        self.turn_log.pack(fill="both", expand=True)

        fleet_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(fleet_tab, text=" 席位 ")
        cols = ("code", "alias", "node", "role", "status")
        self.fleet_tree = ttk.Treeview(fleet_tab, columns=cols, show="headings", height=18)
        for c, w in zip(cols, (60, 110, 190, 90, 90)):
            self.fleet_tree.heading(c, text=c)
            self.fleet_tree.column(c, width=w, anchor="w")
        self.fleet_tree.pack(fill="both", expand=True)

        stream_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(stream_tab, text=" 消息/票板 ")
        self.bridge_log = scrolledtext.ScrolledText(stream_tab, font=("monospace 8"),
                                                    state="disabled", wrap="none", height=12)
        self.bridge_log.pack(fill="both", expand=True)
        self.tickets_log = scrolledtext.ScrolledText(stream_tab, font=("monospace 8"),
                                                     state="disabled", wrap="none", height=10)
        self.tickets_log.pack(fill="both", expand=True)

        root.protocol("WM_DELETE_WINDOW", self.on_close)
        self._draw_spectrum(np.zeros(BARS))
        self._tick()
        self._bind_ptt_hotkey(ptt_key)
        if not self.token:
            self.log_write("[提示] 未配置令牌——gateway 开启鉴权时会收到 auth 错误")
        if selftest:
            root.after(700, root.destroy)

    # ---- 连接 ----

    def toggle_link(self):
        if self.link._thread and self.link._thread.is_alive():
            self.link.close(graceful=False)
            self._set_state("未连接")
            self.conn_btn.config(text="连接")
        else:
            self.link.url = self.url_var.get().strip()
            self.link.token = self.token_var.get().strip()
            self.link.start()
            self.conn_btn.config(text="断开")

    # ---- 观测连接（与语音连接解耦；KG 11 §3 observe 会话） ----

    def toggle_obs(self):
        if self.obs_link and self.obs_link._thread and self.obs_link._thread.is_alive():
            self.obs_link.close()
            self.obs_link = None
            self.obs_state_var.set("观测未开")
            self.obs_btn.config(text="观测")
            return
        link = ObserveLink(self.url_var.get().strip(), self.token_var.get().strip(),
                           self.obs_q,
                           lambda s: self.root.after(0, lambda: self.obs_state_var.set(f"观测:{s}")))
        self.obs_link = link
        link.start()
        self.obs_btn.config(text="停观测")

    def _drain_obs(self):
        """收割观测帧 → 分面渲染（_tick 内调用，UI 线程）。"""
        while True:
            try:
                f = self.obs_q.get_nowait()
            except queue.Empty:
                break
            t = f.get("t", "")
            if t.startswith("orch."):
                self.orch_frames.append(f)
                if len(self.orch_frames) > 400:
                    self.orch_frames = self.orch_frames[-400:]
                self._render_orch()
                st_write(self.orch_log, f"{t} {json.dumps(f, ensure_ascii=False)[:160]}")
            elif t == "head.turn":
                st_write(self.turn_log, turn_line(f))
            elif t == "fleet.snapshot":
                self._render_fleet(f)
            elif t == "bridge.msg":
                st_write(self.bridge_log, str(f.get("line", ""))[:300])
            elif t == "tickets.snapshot":
                st_write(self.tickets_log, str(f.get("text", f.get("content", "")))[-2000:])
            elif "_error" in f:
                st_write(self.orch_log, f"[观测错误] {f['_error']}")
            elif "_link" in f:
                st_write(self.orch_log, f"[观测链路] {f['_link']}")

    def _render_orch(self):
        lines = orch_tree_lines(self.orch_frames)
        self.orch_tree.config(state="normal")
        self.orch_tree.delete("1.0", "end")
        self.orch_tree.insert("1.0", "\n".join(lines) or "（等待编排事件…）")
        self.orch_tree.config(state="disabled")

    def _render_fleet(self, frame: dict):
        rows = fleet_rows(frame)
        self.fleet_tree.delete(*self.fleet_tree.get_children())
        for row in rows:
            self.fleet_tree.insert("", "end", values=row)

    def end_session(self):
        """优雅收尾需要一帧 session.end——经由 tx 旁路不便，此处仅断链。

        gateway 对非优雅断开保留 resume 槽（重连带 session_id 续接），
      真正丢弃由服务端超时回收。"""
        self.link.close(graceful=False)
        self._set_state("已断开（可重连续接）")

    # ---- 采集（按住说话默认；锁定=连续采集，静音语义：WS 保持） ----

    def _ptt_press(self):
        if self.ptt.press() == "start":
            self._mic_start()

    def _ptt_release(self):
        if self.lock_var.get():
            return                      # 锁定连续采集：松键不停
        if self.ptt.release() == "stop":
            self._mic_stop()
            self._send_vad_tail()

    def _on_lock(self):
        if self.lock_var.get():
            self.ptt.press()            # 锁定即视为持续持有
            self._mic_start()
        elif not self.ptt.held:
            self._mic_stop()

    def _bind_ptt_hotkey(self, key: str):
        """全局热键（pynput，窗口无焦点也生效）；缺库退化窗口内绑定。"""
        want = key.lower()
        try:
            from pynput import keyboard

            def _on_press(k):
                if getattr(k, "name", "").lower() == want:
                    self.root.after(0, self._ptt_press)

            def _on_release(k):
                if getattr(k, "name", "").lower() == want:
                    self.root.after(0, self._ptt_release)

            self.ptt_listener = keyboard.Listener(on_press=_on_press,
                                                  on_release=_on_release)
            self.ptt_listener.start()
            self.log_write(f"[热键] 全局按住说话：{key.upper()}（pynput）")
        except Exception as e:  # noqa: BLE001 — 退化窗口内绑定
            self.ptt_listener = None
            self.root.bind(f"<KeyPress-{key.capitalize()}>",
                           lambda _e: self._ptt_press())
            self.root.bind(f"<KeyRelease-{key.capitalize()}>",
                           lambda _e: self._ptt_release())
            self.log_write(f"[热键] 窗口内按住说话：{key.upper()}（{e}）")

    def _mic_start(self):
        if self.stream:
            return
        try:
            self.stream = sd.InputStream(
                samplerate=RATE, channels=1, dtype="int16",
                blocksize=BLOCK, callback=self._on_audio)
            self.stream.start()
            self.mic_btn.config(bg="#1f6f43", activebackground="#2a8f56")
            self.mic_var.set(f"● 收音中（松开 {self.ptt_key.upper()} 结束）")
        except Exception as e:  # noqa: BLE001 — 无设备/权限要上屏而非崩
            self.log_write(f"[采集] 启动失败: {e}")

    def _mic_stop(self):
        if not self.stream:
            return
        self.stream.stop()
        self.stream.close()
        self.stream = None
        self.mic_btn.config(bg="#8a3f3f", activebackground="#a44f4f")
        self.mic_var.set(f"🎤 按住说话（热键 {self.ptt_key.upper()}）")
        while True:
            try:
                self.tx.get_nowait()
            except queue.Empty:
                break

    def _send_vad_tail(self):
        """松键后补 0.7s 静音：服务端 VAD 需尾随静音判定句尾并自动提交
        （PTT 下采集骤停不给尾巴，VAD 永远等不到句尾——18:54 无回应根因）。"""
        if self.state_var.get() != "open":
            return
        for _ in range(14):             # 14 × 50ms
            try:
                self.tx.put_nowait(b"\x00\x00" * 800)
            except queue.Full:
                break

    def _on_audio(self, data, frames, time_info, status):
        self.latest = data.copy()
        try:
            self.tx.put_nowait(bytes(data))
        except queue.Full:
            pass  # 背压：丢最新块（与 gateway 忙时丢最老互补）

    # ---- UI 泵 ----

    def _tick(self):
        spec = self._spectrum(self.latest)
        self._draw_spectrum(spec)
        rms = float(np.sqrt(np.mean((self.latest.astype(np.float32) / 32768) ** 2)))
        db = 20 * math.log10(rms + 1e-9)
        frac = max(0.0, min(1.0, (db - DB_FLOOR) / -DB_FLOOR))
        self.level_var.set(f"电平 {'█' * int(frac * 18):<18} {db:5.1f} dBFS")
        self.sent_bytes += 0
        self.stat_var.set(
            f"会话 {self.link.session_id or '-'} · 事件 {self.events} · "
            f"在采 {'是' if self.stream else '否'}")
        while True:
            try:
                line = self.rx.get_nowait()
            except queue.Empty:
                break
            self.events += 1
            self.log_write(line)
        self._drain_obs()
        self.root.after(33, self._tick)

    @staticmethod
    def _spectrum(block: np.ndarray) -> np.ndarray:
        x = block.astype(np.float32) / 32768
        if x.size == 0:
            return np.zeros(BARS)
        mag = np.abs(np.fft.rfft(x * np.hanning(x.size)))
        edges = np.unique(np.geomspace(1, max(2, mag.size), BARS + 1).astype(int))
        vals = np.zeros(BARS)
        for i in range(min(BARS, len(edges) - 1)):
            seg = mag[edges[i]:edges[i + 1]]
            if seg.size:
                v = 20 * math.log10(float(seg.mean()) + 1e-9)
                vals[i] = max(0.0, min(1.0, (v - DB_FLOOR) / -DB_FLOOR))
        return vals

    def _draw_spectrum(self, vals: np.ndarray):
        c = self.canvas
        c.delete("all")
        w = max(c.winfo_width(), 100)
        h = max(c.winfo_height(), 60)
        bw = w / BARS
        for i, v in enumerate(vals):
            bh = max(2, v * (h - 8))
            r, g, b = self._heat(v)
            c.create_rectangle(i * bw + 1, h - bh - 2, (i + 1) * bw - 1, h - 2,
                               fill=f"#{r:02x}{g:02x}{b:02x}", width=0)

    @staticmethod
    def _heat(v: float) -> tuple[int, int, int]:
        if v < 0.5:
            k = v / 0.5
            return int(30 + 100 * k), int(160 + 60 * k), 80
        k = (v - 0.5) / 0.5
        return int(130 + 120 * k), int(220 - 140 * k), int(80 - 40 * k)

    def log_write(self, line: str):
        self.log.config(state="normal")
        self.log.insert("end", line + "\n")
        self.log.see("end")
        self.log.config(state="disabled")

    def _set_state(self, s: str):
        self.state_var.set(s)

    def on_close(self):
        self.link.close(graceful=False)
        if self.obs_link:
            self.obs_link.close()
        if getattr(self, "ptt_listener", None):
            self.ptt_listener.stop()
        if self.stream:
            self.stream.stop()
            self.stream.close()
        self.root.destroy()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--url", default="ws://127.0.0.1:8765/ws")
    ap.add_argument("--token", default="")
    ap.add_argument("--selftest", action="store_true",
                    help="无设备冒烟：UI 起即退（CI/无显示环境验证布局）")
    ap.add_argument("--ptt-key", default="f9",
                    help="按住说话热键（pynput 键名，默认 f9；全局生效）")
    args = ap.parse_args()

    import tkinter as tk

    root = tk.Tk()
    try:
        from tkinter import ttk  # noqa: F401 — 触发主题可用性早失败
        ttk.Style(root).theme_use("clam")
    except Exception:
        pass
    App(root, args.url, args.token, args.selftest, ptt_key=args.ptt_key)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
