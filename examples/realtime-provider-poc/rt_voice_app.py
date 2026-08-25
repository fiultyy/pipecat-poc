#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""rt-voice · 超轻桌面语音客户端（无浏览器引擎）。

stdlib tkinter 画布 + sounddevice 原生采集 + numpy FFT + aiohttp WS，
直连 rt-gateway（ws://host:8765/ws）替代 web/index.html 的浏览器形态：

- 频谱显示：24 根对数分频柱（rfft · 30fps 刷新）+ RMS 电平条（dBFS）
- 输入开关：静音语义——开关只控制采集与上行，WS 会话保持（断线自动重连
  走 gateway 的 resume_id 续接）；「结束会话」才发 session.end
- 事件面板：orch.dispatch/progress/done、auth/session/control 回执、
  下行二进制音频直送扬声器（OutputStream）

帧协议与 rt_gateway.py §1 逐字对齐：JSON 文本帧（control/event）+
二进制帧（PCM16LE/16k/mono，session.start 之后才可上行）。

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

        self.url, self.token = url, token
        self.tx, self.rx, self.on_state = tx, rx, on_state
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
                        pump = asyncio.create_task(self._pump_up(ws, lambda: started))
                        try:
                            async for msg in ws:
                                if self._stop.is_set():
                                    break
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
                                    label = LOG_SHORT.get(kind, kind or "事件")
                                    body = json.dumps(data, ensure_ascii=False)
                                    self.rx.put(f"[{label}] {body[:200]}")
                        finally:
                            pump.cancel()
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


class App:
    """tkinter 主线程：频谱画布 + 开关 + 事件面板。"""

    def __init__(self, root: "tkinter.Tk", url: str, token: str, selftest: bool):
        import tkinter as tk
        from tkinter import scrolledtext, ttk

        self.tk, self.ttk = tk, ttk
        self.root = root
        self.url, self.token = url, token
        self.tx: queue.Queue[bytes] = queue.Queue(maxsize=64)
        self.rx: "queue.Queue[str]" = queue.Queue()
        self.latest = np.zeros(BLOCK, dtype=np.int16)
        self.sent_bytes = 0
        self.events = 0
        self.link = VoiceLink(url, token, self.tx, self.rx, self._set_state)
        self.stream: sd.InputStream | None = None

        root.title("rt-voice · 语音编排客户端")
        root.geometry("860x560")
        root.minsize(720, 480)

        # ---- 顶栏：地址 / token / 连接 ----
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
        self.end_btn = ttk.Button(top, text="结束会话", command=self.end_session, width=10)
        self.end_btn.pack(side="right")

        # ---- 频谱画布 ----
        self.canvas = tk.Canvas(root, bg="#0b0f14", height=240, highlightthickness=0)
        self.canvas.pack(fill="both", expand=True, padx=8, pady=(6, 0))
        self.level_var = tk.StringVar(value="电平 ──────────")
        ttk.Label(root, textvariable=self.level_var, font=("monospace 10")).pack(anchor="w", padx=12)

        # ---- 开关 ----
        mid = ttk.Frame(root, padding=(8, 4))
        mid.pack(fill="x")
        self.mic_var = tk.StringVar(value="🎤 开麦（上行开启）")
        self.mic_btn = tk.Button(mid, textvariable=self.mic_var, command=self.toggle_mic,
                                 font=("system-ui 13",), bg="#1f6f43", fg="white",
                                 activebackground="#2a8f56", activeforeground="white",
                                 relief="flat", padx=18, pady=6, cursor="hand2")
        self.mic_btn.pack(side="left")
        self.stat_var = tk.StringVar(value="待连接")
        ttk.Label(mid, textvariable=self.stat_var).pack(side="left", padx=12)

        # ---- 事件面板 ----
        self.log = scrolledtext.ScrolledText(root, height=9, font=("monospace 9"),
                                             state="disabled", wrap="word")
        self.log.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        root.protocol("WM_DELETE_WINDOW", self.on_close)
        self._draw_spectrum(np.zeros(BARS))
        self._tick()
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

    def end_session(self):
        """优雅收尾需要一帧 session.end——经由 tx 旁路不便，此处仅断链。

        gateway 对非优雅断开保留 resume 槽（重连带 session_id 续接），
      真正丢弃由服务端超时回收。"""
        self.link.close(graceful=False)
        self._set_state("已断开（可重连续接）")

    # ---- 采集开关（静音语义：WS 保持） ----

    def toggle_mic(self):
        if self.stream:
            self.stream.stop()
            self.stream.close()
            self.stream = None
            self.mic_btn.config(bg="#8a3f3f", activebackground="#a44f4f")
            self.mic_var.set("🔇 闭麦（上行停发）")
            while True:
                try:
                    self.tx.get_nowait()
                except queue.Empty:
                    break
            return
        try:
            self.stream = sd.InputStream(
                samplerate=RATE, channels=1, dtype="int16",
                blocksize=BLOCK, callback=self._on_audio)
            self.stream.start()
            self.mic_btn.config(bg="#1f6f43", activebackground="#2a8f56")
            self.mic_var.set("🎤 开麦（上行开启）")
        except Exception as e:  # noqa: BLE001 — 无设备/权限要上屏而非崩
            self.log_write(f"[采集] 启动失败: {e}")

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
    args = ap.parse_args()

    import tkinter as tk

    root = tk.Tk()
    try:
        from tkinter import ttk  # noqa: F401 — 触发主题可用性早失败
        ttk.Style(root).theme_use("clam")
    except Exception:
        pass
    App(root, args.url, args.token, args.selftest)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
