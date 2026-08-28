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
  编排页（orch.* 任务树+时间线+bridge.msg 原始行面板）、回合页
  （head.turn）、席位页（fleet.snapshot 表+清理控制面：多选释放/结束
  → fleet.cleanup，回包一行结果摘要）、任务页（body.push 台账：左表
  右正文，body.get{ref} 拉全文，KG 14 §2.4；下半 tickets.snapshot 全文
  面板）
- 语音页迷你通知行：body.push 到达一行（no/status/summary/chars 量级）；
  回合页 notify 相（📣）+ 同 ref body.push 追加灰行「└已入详情」

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
OUT_RATE = 24000  # 网关下行 TTS PCM 码率（Qwen-Omni realtime 头 24k）

# 播放线程哨兵：清空待播队列并断流（打断用）
_DROP_PLAYBACK = object()
BLOCK_MS = 50
BLOCK = RATE * BLOCK_MS // 1000          # 800 samples per callback
BARS = 24
DB_FLOOR = -60.0

LOG_SHORT = {
    "orch.dispatch": "派发", "orch.progress": "进展", "orch.done": "终稿",
    "auth.ok": "鉴权", "session.started": "会话", "session.ended": "结束",
    "gate.resolved": "闸解", "error": "错误", "body.push": "台账",
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
        self._play_q: "queue.Queue" = queue.Queue()
        self._play_thread: threading.Thread | None = None
        self._mute = False  # 打断后丢弃下行音频，直到新应答开始（worker 线程内读写）

    # ---- lifecycle (called from tkinter thread) ----

    def start(self):
        self._stop.clear()
        while True:  # 丢弃上次会话残留
            try:
                self._play_q.get_nowait()
            except queue.Empty:
                break
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        self._play_thread = threading.Thread(target=self._playback_worker, daemon=True)
        self._play_thread.start()

    def close(self, graceful: bool):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)
        if self._play_thread:
            self._play_thread.join(timeout=3)

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
                                    # reader 正常结束=连接被服务端关闭（无异常
                                    # 抛出），必须跳出重连，否则挂在死连接上
                                    if reader.done():
                                        break
                                    continue
                                if msg.type == aiohttp.WSMsgType.BINARY:
                                    if not self._mute:
                                        self._play_q.put(bytes(msg.data))
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
                                elif t == "head.turn":
                                    ph = data.get("phase")
                                    if ph in ("user_start", "interrupted"):
                                        self._mute = True  # 残余旧音频也拦下
                                        self._drop_playback()  # 打断：立刻闭嘴
                                    elif ph == "assistant_start":
                                        self._mute = False
                                    self.rx.put("[回合] "
                                                + json.dumps(data, ensure_ascii=False)[:200])
                                elif t == "body.push":
                                    # KG 14 §2.4 语音页迷你通知行：单条→一行轻通知，
                                    # 回放批（items）→一行摘要（全文去详情页签）
                                    if isinstance(data.get("items"), list):
                                        line = replay_notice_line(data)
                                    else:
                                        line = notice_line_from_push(data)
                                    if line:
                                        self.rx.put(f"[台账] {line}")
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

    def _playback_worker(self):
        """播放线程：独占 OutputStream。

        sounddevice 的 write 按设备缓冲节流阻塞——下行音频比实时快时
        会长期卡在 write 里，不能在收帧循环内直接播（打断事件会排在
        音频帧后面，永远轮不到处理）。收帧循环只入队，本线程消费；
        打断经哨兵让本线程自行清队列并断流。
        """
        out: sd.OutputStream | None = None
        while not self._stop.is_set():
            try:
                item = self._play_q.get(timeout=0.2)
            except queue.Empty:
                continue
            if item is _DROP_PLAYBACK:
                if out is not None:
                    try:
                        out.abort()  # 丢弃设备缓冲内未播样本，立即静音
                        out.close()
                    except Exception:
                        pass
                    out = None
                while True:  # 丢弃已在队列里的旧音频（含重复哨兵）
                    try:
                        self._play_q.get_nowait()
                    except queue.Empty:
                        break
                continue
            try:
                if out is None:
                    out = sd.OutputStream(samplerate=OUT_RATE, channels=1, dtype="int16")
                    out.start()
                out.write(np.frombuffer(item, dtype=np.int16).reshape(-1, 1))
            except Exception:
                if out is not None:
                    try:
                        out.close()
                    except Exception:
                        pass
                out = None  # 无输出设备等异常：丢块继续
        if out is not None:
            try:
                out.stop()
                out.close()
            except Exception:
                pass

    def _drop_playback(self):
        """打断：先清待播队列，再放哨兵让播放线程断流。

        哨兵不能先入队——FIFO 里它会排在旧音频后面，播放线程会把
        旧音频全部播完才轮到断流。清完队列后线程最多等当前 write
        返回（≤一个设备缓冲期）即静音。
        """
        while True:
            try:
                self._play_q.get_nowait()
            except queue.Empty:
                break
        self._play_q.put(_DROP_PLAYBACK)


class ObserveLink:
    """asyncio 侧：纯观测 WS 会话（KG 11 §3 observe:true——不建 head、
    拒媒体、缺省订阅全部 topic）。

    与语音连接生命周期解耦（观测可先于语音开；语音断不影响观测）。
    与 tkinter 只经 obs 队列交换（dict 帧，渲染在 UI 线程）。

    出站请求面（KG 14 §2.4）：``send_request`` 投递客户端帧（body.get 等）
    ——req 线程队列 → worker 内泵任务在会话开后发送；会话内的 ``error``
    回包（body_miss 等）是请求级错误，进 obs 队列按帧处理，不断链；
    握手期 error 仍视为致命（鉴权/协议失败）。
    """

    def __init__(self, url: str, token: str, obs: "queue.Queue[dict]", on_state):
        self.url, self.token = url, token
        self.obs, self.on_state = obs, on_state
        self.session_id: str | None = None
        self.topics: list[str] | None = None
        self.req: "queue.Queue[dict]" = queue.Queue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def send_request(self, req: dict):
        """线程安全投递客户端帧（UI 线程调用；worker 侧在会话开后发送）。"""
        self.req.put(req)

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

    async def _req_pump(self, ws, ready):
        """req 队列 → 文本帧；握手先行（session.started 前不发——auth 会被
        网关拒），50ms 轮询线程队列（tk 线程只 put 不碰 ws）。"""
        await ready.wait()
        while True:
            try:
                req = self.req.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.05)
                continue
            try:
                await ws.send_json(req)
            except Exception:  # noqa: BLE001 — 连接已断：外层 async for 随即退出重连
                return

    async def _worker(self):
        import aiohttp

        while not self._stop.is_set():
            try:
                self.on_state("connecting")
                async with aiohttp.ClientSession() as http:
                    async with http.ws_connect(self.url, max_msg_size=1 << 21) as ws:
                        await ws.send_json({"t": "auth", "token": self.token})
                        self.on_state("authing")
                        ready = asyncio.Event()
                        pump = asyncio.create_task(self._req_pump(ws, ready))
                        try:
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
                                    ready.set()
                                elif t == "error":
                                    if self.topics is not None:
                                        # 会话内请求级错误（body.get miss 等）：
                                        # 按普通帧下发，由请求方回填，不断链
                                        self.obs.put(data)
                                    else:
                                        self.obs.put({"_error": data})
                                        self.on_state(f"error {data.get('code')}")
                                        return
                                else:
                                    self.obs.put(data)
                        finally:
                            pump.cancel()
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


def st_write(widget, line: str, tags: "tuple[str, ...] | list[str]" = ()):
    """ScrolledText 追加一行并滚尾（state 恢复 disabled）；tags 上文本样式。"""
    text = line.rstrip("\n") + "\n"
    widget.config(state="normal")
    if tags:
        widget.insert("end", text, tuple(tags))
    else:
        widget.insert("end", text)
    widget.see("end")
    widget.config(state="disabled")


def st_set(widget, text: str):
    """ScrolledText 覆写渲染：整体替换内容——全文快照面板语义（文件原子
    替换触发整帧重发，追加会滚动累积重复）。"""
    widget.config(state="normal")
    widget.delete("1.0", "end")
    widget.insert("1.0", text)
    widget.config(state="disabled")


TURN_PHASE_LABEL = {
    "user_start": "🗣", "user_end": "·", "user_text": "💬",
    "assistant_start": "🤖", "assistant_end": "✅", "tool_call": "🔧",
    "interrupted": "⚡",
}


def turn_label_with_notify(phase: str) -> str:
    """head.turn phase → 显示图标；notify 相=📣（KG 14 §2.4 回合页）。"""
    if phase == "notify":
        return "📣"
    return TURN_PHASE_LABEL.get(phase, "·")


def turn_line(frame: dict) -> str:
    """head.turn 帧 → 单行呈现（phase 图标 + detail 截断；notify 无 detail
    时回退显示 ref——通报帧只带 conv_id/phase/ref，ref 即检索线索）。"""
    phase = frame.get("phase", "?")
    icon = turn_label_with_notify(phase)
    detail = str(frame.get("detail", "") or "")[:80]
    if not detail and phase == "notify" and frame.get("ref"):
        detail = f"→ {frame.get('ref')}"
    return f"{icon} {phase}{(' ' + detail) if detail else ''}"


def chars_mag(n) -> str:
    """字数 → 量级短写（512 / 1.2k / 38k）。"""
    try:
        n = int(n or 0)
    except (TypeError, ValueError):
        n = 0
    if n < 1000:
        return str(n)
    if n < 10000:
        return f"{n / 1000:.1f}k"
    return f"{n // 1000}k"


def notice_line_from_push(frame: dict) -> str:
    """body.push 单条帧 → 语音页迷你通知行（no/status/summary/chars 量级）；
    无 ref 的畸形帧返回 ""。"""
    if not isinstance(frame, dict) or not frame.get("ref"):
        return ""
    no = frame.get("no")
    who = f"#{no}" if no not in (None, "") else str(frame.get("ref"))
    summary = str(frame.get("summary") or frame.get("title") or "")[:40]
    return f"📣 {who} {frame.get('status', '?')} {summary}（{chars_mag(frame.get('chars'))}字）"


def replay_notice_line(frame: dict) -> str:
    """body.push 回放帧（items 批）→ 语音页单行摘要；非回放/空批返回 ""。"""
    items = frame.get("items") if isinstance(frame, dict) else None
    if not isinstance(items, list) or not items:
        return ""
    return f"📣 台账回放 {len(items)} 条（详情页签查看）"


DETAIL_COLS = ("time", "no", "ref", "title", "chars")
DETAIL_REF_I = DETAIL_COLS.index("ref")


def detail_rows_from_push(frame: dict) -> list[tuple]:
    """body.push 帧 → 详情表行 (time/no/ref/title/chars)。

    单条帧（ref/inline 形）出一行；回放帧（items 批、无 inline）按批
    出多行；无 ref 条目与非 dict 条目跳过。行序=帧内序（合并时去重）。
    """
    if not isinstance(frame, dict):
        return []
    items = frame.get("items")
    src = [e for e in items if isinstance(e, dict)] if isinstance(items, list) \
        else ([frame] if frame.get("ref") else [])
    rows = []
    for e in src:
        if not e.get("ref"):
            continue
        ts = e.get("ts")
        when = time.strftime("%H:%M:%S", time.localtime(ts)) \
            if isinstance(ts, (int, float)) else ""
        rows.append((when, str(e.get("no", "") if e.get("no") is not None else ""),
                     str(e.get("ref")), str(e.get("title", "") or "")[:28],
                     str(e.get("chars", 0))))
    return rows


def merge_detail_rows(prev: list[tuple], new: list[tuple]) -> list[tuple]:
    """详情表行按 ref 去重合并：已见 ref 原位更新值（行不跳动），
    新 ref 追加尾部。"""
    out = {r[DETAIL_REF_I]: r for r in prev}
    for r in new:
        out[r[DETAIL_REF_I]] = r
    return list(out.values())


def detail_ack_line(frame: dict, notify_refs) -> str | None:
    """body.push 帧 → 回合页灰行「└已入详情」；ref 未在回合页出现过
    （无 notify 相）则 None——灰行只标已通报过的任务。"""
    if not isinstance(frame, dict):
        return None
    ref = frame.get("ref")
    if not ref or ref not in set(notify_refs or ()):
        return None
    return f"└ 已入详情 {ref}"


def detail_error_line(frame: dict) -> str:
    """error 帧（body.get 失败回包）→ 详情右栏提示行。"""
    if not isinstance(frame, dict):
        return "⚠ 未知错误"
    parts = [str(frame.get("code") or "?"), str(frame.get("msg", "") or "")[:120]]
    return "⚠ " + " ".join(p for p in parts if p)


def tickets_text(frame: dict) -> str:
    """tickets.snapshot 帧 → 面板文本（全文快照，尾部 2000 字窗口）。

    源文件 tickets.md 原子替换即触发整帧重发，面板按覆写渲染
    （st_set），非增量追加。
    """
    if not isinstance(frame, dict):
        return ""
    return str(frame.get("text", frame.get("content", "")))[-2000:]


def cleanup_request(ids, mode: str, req_id: str) -> dict:
    """席位清理控制帧（fleet.cleanup）：ids=四码列表，mode=release（只出册）
    /end（经网关 loopback 真死会话再出册），req_id 关联回包
    fleet.cleanup.result。"""
    return {"t": "fleet.cleanup", "ids": [str(i) for i in (ids or [])],
            "mode": mode, "req_id": req_id}


def cleanup_result_line(result: dict) -> str:
    """fleet.cleanup.result 回包 → 单行结果摘要（成功数 + 失败/备注明细）。

    失败条目带 error（not_found 等）；ok 条目可带 note（active-liaison：
    该席位是当前 liaison 绑定，摘除后下个派发自动拉新属设计内行为）。
    """
    rows: list[dict] = []
    if isinstance(result, dict):
        items = result.get("results")
        if isinstance(items, list):
            rows = [r for r in items if isinstance(r, dict)]
    if not rows:
        return "⚠ 清理回包不可读"
    ok_n = sum(1 for r in rows if r.get("ok"))
    parts = [f"🧹 清理 {ok_n}/{len(rows)} 成功"]
    fails = [f"{r.get('id', '?')}({r.get('error', '?')})" for r in rows if not r.get("ok")]
    if fails:
        parts.append("失败 " + " ".join(fails))
    notes = [f"{r.get('id', '?')}:{r.get('note')}" for r in rows if r.get("note")]
    if notes:
        parts.append("备注 " + " ".join(notes))
    return "；".join(parts)


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
        # 详情页签状态（KG 14 §2.4）：表行（按 ref 去重）+ inline/拉取两级正文缓存
        self.detail_rows: list[tuple] = []
        self.detail_inline: dict[str, str] = {}    # ref → inline 轻通知全文
        self.detail_bodies: dict[str, str] = {}    # ref → body.get 拉取全文
        self._detail_pending: str | None = None    # 在途 body.get 的 ref
        self._notify_refs: set[str] = set()        # 回合页已见 notify 相的 ref
        self._cleanup_seq = 0                      # fleet.cleanup req_id 序号
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
        self.orch_tab = orch_tab
        self.nb.add(orch_tab, text=" 编排 ")
        self.orch_tree = scrolledtext.ScrolledText(orch_tab, font=("monospace 9"),
                                                   state="disabled", height=8)
        self.orch_tree.pack(fill="both", expand=False)
        self.orch_log = scrolledtext.ScrolledText(orch_tab, font=("monospace 8"),
                                                  state="disabled", wrap="word")
        self.orch_log.pack(fill="both", expand=True)
        # bridge.msg 原始行（inbox.log 增量）：编排页底部独立小面板，标注来源
        ttk.Label(orch_tab, text="bridge.msg · inbox.log 增量行",
                  foreground="#8a8a8a").pack(anchor="w", pady=(6, 0))
        self.bridge_log = scrolledtext.ScrolledText(orch_tab, font=("monospace 8"),
                                                    state="disabled", wrap="none", height=6)
        self.bridge_log.pack(fill="both", expand=False)

        turn_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(turn_tab, text=" 回合 ")
        self.turn_log = scrolledtext.ScrolledText(turn_tab, font=("monospace 9"),
                                                  state="disabled", wrap="word")
        self.turn_log.pack(fill="both", expand=True)
        self.turn_log.tag_configure("dim", foreground="#8a8a8a")  # 「└已入详情」灰行

        fleet_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(fleet_tab, text=" 席位 ")
        cols = ("code", "alias", "node", "role", "status")
        # 多选（extended）：清理动作按批处理所选席位
        self.fleet_tree = ttk.Treeview(fleet_tab, columns=cols, show="headings",
                                       height=18, selectmode="extended")
        for c, w in zip(cols, (60, 110, 190, 90, 90)):
            self.fleet_tree.heading(c, text=c)
            self.fleet_tree.column(c, width=w, anchor="w")
        self.fleet_tree.pack(fill="both", expand=True)
        # 清理控制面：释放=只出册；结束=经网关 loopback 真死会话再出册
        fleet_bar = ttk.Frame(fleet_tab)
        fleet_bar.pack(fill="x", pady=(6, 0))
        self.fleet_release_btn = ttk.Button(
            fleet_bar, text="释放选中", command=lambda: self._fleet_cleanup("release"))
        self.fleet_release_btn.pack(side="left")
        self.fleet_end_btn = ttk.Button(
            fleet_bar, text="结束选中", command=lambda: self._fleet_cleanup("end"))
        self.fleet_end_btn.pack(side="left", padx=(8, 0))
        self.fleet_note_var = tk.StringVar(value="")
        ttk.Label(fleet_tab, textvariable=self.fleet_note_var,
                  foreground="#555").pack(anchor="w", pady=(4, 0))

        # ---- 任务页签（详情整合 + tickets 全文）：上下 PanedWindow ----
        # 上=台账表（body.push 按 ref 去重）+右只读正文（body.get 拉全文，
        # KG 14 §2.4 原详情页全部内容，水平 Paned 沿用）；
        # 下=tickets.md 全文面板（tickets.snapshot 覆写渲染）
        task_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(task_tab, text=" 任务 ")
        task_pane = self.ttk.PanedWindow(task_tab, orient="vertical")
        task_pane.pack(fill="both", expand=True)
        task_top = ttk.Frame(task_pane)
        self.task_bottom = ttk.Frame(task_pane)
        task_pane.add(task_top, weight=3)
        task_pane.add(self.task_bottom, weight=2)
        detail_pane = self.ttk.PanedWindow(task_top, orient="horizontal")
        detail_pane.pack(fill="both", expand=True)
        detail_left = ttk.Frame(detail_pane)
        detail_right = ttk.Frame(detail_pane)
        detail_pane.add(detail_left, weight=3)
        detail_pane.add(detail_right, weight=4)
        self.detail_tree = ttk.Treeview(detail_left, columns=DETAIL_COLS,
                                        show="headings", height=10, selectmode="browse")
        for c, w in zip(DETAIL_COLS, (76, 44, 130, 280, 52)):
            self.detail_tree.heading(c, text=c)
            self.detail_tree.column(c, width=w, anchor="w")
        self.detail_tree.pack(fill="both", expand=True)
        self.detail_tree.bind("<<TreeviewSelect>>", self._on_detail_select)
        self.detail_body = scrolledtext.ScrolledText(detail_right,
                                                     font=("monospace 9"),
                                                     state="disabled", wrap="word")
        self.detail_body.pack(fill="both", expand=True)
        ttk.Label(self.task_bottom, text="tickets.md 全文（tickets.snapshot）",
                  foreground="#8a8a8a").pack(anchor="w")
        self.tickets_log = scrolledtext.ScrolledText(self.task_bottom,
                                                     font=("monospace 8"),
                                                     state="disabled", wrap="none")
        self.tickets_log.pack(fill="both", expand=True)

        root.protocol("WM_DELETE_WINDOW", self.on_close)
        self._draw_spectrum(np.zeros(BARS))
        self._tick()
        self._bind_ptt_hotkey(ptt_key)
        if not self.token:
            self.log_write("[提示] 未配置令牌——gateway 开启鉴权时会收到 auth 错误")
        if selftest:
            root.after(200, self._selftest_probe)
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
                if f.get("phase") == "notify" and f.get("ref"):
                    self._notify_refs.add(str(f["ref"]))
                st_write(self.turn_log, turn_line(f))
            elif t == "body.push":
                self._on_body_push(f)
            elif t == "body.item":
                self._on_body_item(f)
            elif t == "error":
                # 会话内请求级错误（body.get miss 等）：在途详情回填右栏
                if self._detail_pending:
                    self._render_detail_body(detail_error_line(f))
                    self._detail_pending = None
                else:
                    st_write(self.orch_log, f"[观测错误] {detail_error_line(f)}")
            elif t == "fleet.snapshot":
                self._render_fleet(f)
            elif t == "fleet.cleanup.result":
                self.fleet_note_var.set(cleanup_result_line(f))
            elif t == "bridge.msg":
                st_write(self.bridge_log, str(f.get("line", ""))[:300])
            elif t == "tickets.snapshot":
                st_set(self.tickets_log, tickets_text(f))
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

    # ---- 席位清理控制面（fleet.cleanup，经观测连接出站） ----

    def _fleet_cleanup(self, mode: str):
        """释放/结束所选席位：确认门 → observe send_request 发 fleet.cleanup。

        结束=经网关 loopback 真死会话再出册（不可恢复，确认文案明示）；
        释放=只出册不碰会话。回包 fleet.cleanup.result 渲染一行结果摘要；
        席位表靠 fleet.snapshot（fleet.json 原子写触发）自动刷新。
        """
        ids = [str(self.fleet_tree.item(i, "values")[0])
               for i in self.fleet_tree.selection()]
        if not ids:
            self.fleet_note_var.set("未选中席位")
            return
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            self.fleet_note_var.set("观测连接未开——开启「观测」后可清理席位")
            return
        from tkinter import messagebox

        if mode == "end":
            msg = (f"结束 {len(ids)} 个席位（{'、'.join(ids)}）？\n\n"
                   "结束将经网关真正终止会话进程——"
                   "通话立即断开、不可恢复；席位条目同时出册。")
            title, icon = "结束席位确认", "warning"
        else:
            msg = (f"释放 {len(ids)} 个席位（{'、'.join(ids)}）？\n\n"
                   "释放只把席位条目从 fleet.json 出册，"
                   "不触碰正在运行的会话。")
            title, icon = "释放席位确认", "question"
        if not messagebox.askyesno(title, msg, parent=self.root, icon=icon):
            return
        self._cleanup_seq += 1
        req_id = f"clean-{int(time.time() * 1000)}-{self._cleanup_seq}"
        self.fleet_note_var.set(f"已发清理请求（{mode} · {len(ids)} 个席位）…")
        self.obs_link.send_request(cleanup_request(ids, mode, req_id))

    # ---- 详情页签（KG 14 §2.4：表按 ref 去重，正文两级缓存+观测拉取） ----

    def _on_body_push(self, frame: dict):
        """body.push（单条/回放批）→ 入表去重 + inline 缓存 + 回合页灰行。"""
        rows = detail_rows_from_push(frame)
        if not rows:
            return
        self.detail_rows = merge_detail_rows(self.detail_rows, rows)
        self._render_detail()
        inline = frame.get("inline")
        if isinstance(inline, str) and inline and frame.get("ref"):
            self.detail_inline[str(frame["ref"])] = inline
        ack = detail_ack_line(frame, self._notify_refs)
        if ack:
            st_write(self.turn_log, ack, tags=("dim",))

    def _on_body_item(self, frame: dict):
        """body.item（body.get 回包）→ 全文缓存；正选中该 ref 则回填右栏。"""
        ref = str(frame.get("ref", "") or "")
        text = str(frame.get("text", "") or "")
        if not ref:
            return
        self.detail_bodies[ref] = text
        if self._detail_pending == ref:
            self._detail_pending = None
        if ref == self._selected_detail_ref():
            self._render_detail_body(text)

    def _render_detail(self):
        sel = self._selected_detail_ref()   # 增量重绘保住选中行
        self.detail_tree.delete(*self.detail_tree.get_children())
        for row in self.detail_rows:
            # iid=ref：去重已由 merge 保证，选中路径直接拿 ref
            self.detail_tree.insert("", "end", iid=row[DETAIL_REF_I], values=row)
        if sel and self.detail_tree.exists(sel):
            self.detail_tree.selection_set(sel)

    def _render_detail_body(self, text: str):
        self.detail_body.config(state="normal")
        self.detail_body.delete("1.0", "end")
        self.detail_body.insert("1.0", text)
        self.detail_body.config(state="disabled")

    def _selected_detail_ref(self) -> str:
        sel = self.detail_tree.selection()
        if not sel:
            return ""
        values = self.detail_tree.item(sel[0], "values")
        return str(values[2]) if values and len(values) > DETAIL_REF_I else ""

    def _on_detail_select(self, _e=None):
        """行选中 → inline/已拉取缓存直渲染，否则经观测连接 body.get{ref}。"""
        ref = self._selected_detail_ref()
        if not ref:
            return
        body = self.detail_bodies.get(ref) or self.detail_inline.get(ref)
        if body is not None:
            self._detail_pending = None
            self._render_detail_body(body)
            return
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            self._render_detail_body("（观测连接未开——开启「观测」后选择行可拉取正文）")
            return
        self._detail_pending = ref
        self._render_detail_body(f"（拉取中 {ref} …）")
        self.obs_link.send_request({"t": "body.get", "ref": ref})

    def _selftest_probe(self):
        """selftest 冒烟（无显示环境 CI）：假帧走真实渲染路径——回放批入表、
        notify 相记 ref、单条 push 入表+灰行、body.item 回填缓存；席位快照
        入表、bridge 行入编排页、tickets 覆写、清理回包摘要行。不触网。"""
        now = time.time()
        self.obs_q.put({"t": "body.push", "items": [
            {"ref": "vh-self1", "no": 1, "status": "done", "title": "回放标题",
             "summary": "回放摘要", "chars": 12, "ts": now - 60},
            {"ref": "", "no": 9, "status": "done", "title": "无 ref 跳过", "chars": 1},
        ], "ts": now})
        self.obs_q.put({"t": "head.turn", "phase": "notify", "ref": "vh-self2",
                        "conv_id": "s-self"})
        self.obs_q.put({"t": "body.push", "ref": "vh-self2", "no": 2, "status": "done",
                        "title": "直播标题", "summary": "直播摘要", "chars": 4096,
                        "inline": "inline 正文", "ts": now})
        self.obs_q.put({"t": "body.item", "ref": "vh-self1", "title": "回放标题",
                        "text": "全文正文", "chars": 4, "ts": now})
        self.obs_q.put({"t": "fleet.snapshot", "fleet": {
            "<seat>": {"sessionId": "s-a", "alias": "webgui", "node": "voice-head",
                     "role": "orchestrator", "status": "active"}}})
        self.obs_q.put({"t": "bridge.msg", "line": "selftest bridge 增量行"})
        self.obs_q.put({"t": "tickets.snapshot", "text": "# 票板\n- [ ] selftest 项"})
        self.obs_q.put({"t": "fleet.cleanup.result", "req_id": "self-clean-1",
                        "results": [{"id": "20d0", "ok": True,
                                     "note": "active-liaison"}]})

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
