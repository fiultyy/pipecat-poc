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
  → fleet.cleanup，回包一行结果摘要；「简报」拉 fleet.brief 席位一行
  一状态小面板；快照陈旧指示与 last_seen 列；对接运维 liaison.
  unbind/bind；事件历史小面板保留最近若干条）、任务页（body.push
  台账：左表右正文，body.get{ref} 拉全文，KG 14 §2.4；「拉取更多」
  body.list_more 历史翻页、选中行 run.cancel 取消；下半
  tickets.snapshot 全文面板）
- 语音页迷你通知行：body.push 到达一行（no/status/summary/chars 量级）；
  回合页 notify 相（📣）+ 同 ref body.push 追加灰行「└已入详情」
- 语音页 head 选择行（PR8）：观测开启即 head.list 拉配置表渲染单选钮；
  切换发 head.switch——激活单例，活会话不拆，下一次语音连接生效
- 白板页（协作交互输入面）：文本区即白板本体，改动防抖（600ms）自动
  经 whiteboard.set 推进网关全局白板，无手动同步步；语音侧让 head 调
  read_whiteboard 工具直接读当前内容（长文输入走眼不走嘴）
- PM 页签（TK-001，<internal-repo> spec-tk-client）：观测连接上 pm.sub 自动订阅
  （会话就绪即订、上游断流自动重订、帧 id 自增不重放已发帧）；降级横幅
  （pm_* 失败码/订阅断流点亮，订阅受理即熄灭）+ pm.req 透传控制台
  （op/params 原样交网关机械路由，ADR-004 客户端零业务）+ pm.res/
  pm.event 流水面板；断线重连指数退避 2s→30s；会话配置 JSON 只记
  网关地址（可丢）
- 票板页签（TK-002）：kanban by state（五列+未知态兜底列），卡片含
  deps 边/refs chips/lease_owner/outcome；数据面=pm.event(kind=tickets
  失效通知，无载荷) 防抖合并 → op=tickets 全量重拉 → state 整体替换 →
  渲染纯函数（state→view）覆写重绘 + ticket_events 侧栏
- 席位舰页签（TK-003）：fleet 卡片墙（短码/term_·准入态
  probing/verified/stale·preset·lastSeen 时长 + 租约到期倒计时 + 换代中
  瞬态）；数据面=op=fleet 全量 + fleet.kind 失效通知防抖重拉 + 尾随
  fleet.snapshot 全文双源共一卡模；stale 高亮阈值页签内可配
- 轨迹页签（TK-004）：turn 分组时间线（类型相着色）+ 事件类型/工具名
  过滤 + 文本搜索 + seq 跳转 + 折叠摘要展开；数据面=op=trace 拉取
  （过滤/折叠全在服务端，参数组合即幂等键，客户端纯渲染）

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
import re
import sys
import threading
import time
from datetime import datetime, timedelta
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
WB_SYNC_DEBOUNCE_MS = 600              # 白板改动 → 自动同步的防抖窗口
VAD_TAIL_MS = 1500                     # 松键尾静音总长：服务端 VAD 判句尾阈值实测 >700ms
VAD_TAIL_STEP_MS = 50                  # 步进投递：50ms/块=32KB/s，低于网关 64KB/s 限速
PENDING_TIMEOUT_S = 5.0                # 控制请求在途上限：超时清登记并回填提示
FLEET_STALE_S = 120                    # 席位快照陈旧阈值：距收帧超时标「可能陈旧」
FLEET_EVENT_KEEP = 8                   # 席位事件历史保留条数（事件区最新条标 ▸）
DEFAULT_GATEWAY_URL = "ws://127.0.0.1:8765/ws"
SESSION_CONFIG_PATH = Path.home() / ".rt_voice_app_session.json"  # 会话配置（可丢）
RECONNECT_BASE_S = 2.0                 # 断线重连退避基值（第 1 次）
RECONNECT_MAX_S = 30.0                 # 退避封顶：2,4,8,16,30,30…
PM_SUB_KINDS = ("fleet", "tickets")    # 就绪自动订阅的 PM 事件 kind（冒烟实证词汇）
PM_RESUB_DELAY_S = 10.0                # 上游事件流断（pm_sub_failed/ended）后的重订间隔
PM_BANNER_OFF = "PM 未接入（开启「观测」后自动订阅）"
TICKET_STATES = ("dispatched", "running", "blocked", "done", "merged")
TICKETS_REFETCH_DEBOUNCE_MS = 400      # tickets 事件→全量重拉的合并窗（事件只作失效通知）
FLEET_REFETCH_DEBOUNCE_MS = 400        # fleet 事件→全量重拉的合并窗（同票板语义）
FLEET_VERIFY_STALE_S = 120             # 席位舰 stale 判定缺省阈值（页签内可调）
FLEET_VERIFY_STATES = ("probing", "verified", "mismatch")  # 准入探测态原样透出
TRACE_BUDGET_CHARS = 20000             # 服务端 head.compact 阈值（展示参照）
# 轨迹行着色相：类型前缀 → 相（turn 琥珀/tool 蓝/step 灰/流紫/元深灰）
TRACE_TYPE_PHASES = (("turn/", "turn"), ("tool/", "tool"), ("step/", "step"),
                     ("agent/", "stream"), ("session", "meta"),
                     ("approval/", "meta"), ("permission/", "meta"),
                     ("sandbox/", "meta"))

LOG_SHORT = {
    "orch.dispatch": "派发", "orch.progress": "进展", "orch.done": "终稿",
    "auth.ok": "鉴权", "session.started": "会话", "session.ended": "结束",
    "gate.resolved": "闸解", "error": "错误", "body.push": "台账",
}

# 在途控制请求 kind → 提示中文名：error 回包/超时/发送失败按 kind 组装回填行
PENDING_KINDS = {
    "fleet.cleanup": "清理",
    "fleet.brief": "席位简报",
    "head.switch": "head 切换",
    "head.list": "head 配置",
    "body.get": "拉取",
    "body.list_more": "历史拉取",
    "whiteboard.set": "白板同步",
    "liaison.unbind": "对接解绑",
    "liaison.bind": "对接绑定",
    "run.cancel": "任务取消",
    "pm.req": "PM 请求",
    "pm.tickets": "票板拉取",
    "pm.fleet": "席位舰拉取",
    "pm.trace": "轨迹拉取",
    "pm.trace.expand": "轨迹展开",
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
        self._attempts = 0  # 连续失败次数（session.started 复位；退避指数源）
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
                                    self._attempts = 0
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
                                    etype, emsg = error_identity(data)
                                    self.rx.put(f"[错误] {etype or '?'}: {emsg}")
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
            self._attempts += 1
            if self._stop.wait(reconnect_delay_s(self._attempts)):
                break
            self.rx.put(f"[链路] {reconnect_delay_s(self._attempts):.0f}s 后重连…")
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

    PM 面（TK-001，<internal-repo> spec-tk-client）：``pm_kinds`` 给定即会话就绪
    自动发一帧 ``pm.sub``（id 自增，重连只发新 id、不重放已发帧）；
    上游事件流断（``pm_sub_failed``/``pm_sub_ended`` error 帧）延迟自动
    重订——GW-002 恢复语义=客户端重订，快照回放兜底。
    """

    def __init__(self, url: str, token: str, obs: "queue.Queue[dict]", on_state,
                 pm_kinds: "tuple[str, ...] | None" = None):
        self.url, self.token = url, token
        self.obs, self.on_state = obs, on_state
        self.session_id: str | None = None
        self.topics: list[str] | None = None
        self.pm_kinds = pm_kinds
        self.req: "queue.Queue[dict]" = queue.Queue()
        self._pm_seq = 0                              # pm 帧 id 自增序号
        self._attempts = 0                            # 连续失败次数（就绪复位）
        self._pm_resub_task: "asyncio.Task | None" = None
        self._ws = None                               # 活连接引用（close 时真关）
        self._loop: asyncio.AbstractEventLoop | None = None
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
            # 读循环 park 在 async for 上等帧，_stop 不会自己唤醒它——
            # 从这里跨线程真关 ws（对端/网关随即看到断链、handler 退出）
            if self._ws is not None and self._loop is not None \
                    and self._thread.is_alive():
                try:
                    asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)
                except RuntimeError:
                    pass  # loop 已停（线程自然退出中）
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
                rid = None
                if isinstance(req, dict):
                    rid = req.get("req_id")
                    if not isinstance(rid, str) or not rid:
                        rid = req.get("id")  # pm.* 帧的幂等键叫 id
                if isinstance(rid, str) and rid:
                    self.obs.put({"_send_failed": rid})  # UI 侧回填「发送失败」
                return

    async def _worker(self):
        import aiohttp

        while not self._stop.is_set():
            try:
                self.on_state("connecting")
                async with aiohttp.ClientSession() as http:
                    async with http.ws_connect(self.url, max_msg_size=1 << 21) as ws:
                        self._loop, self._ws = asyncio.get_running_loop(), ws
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
                                    self._attempts = 0
                                    if self.pm_kinds:
                                        await self._pm_subscribe(ws)
                                elif t == "error":
                                    if self.topics is not None:
                                        # 会话内请求级错误（body.get miss 等）：
                                        # 按普通帧下发，由请求方回填，不断链
                                        self.obs.put(data)
                                        if (self.pm_kinds and data.get("code")
                                                in ("pm_sub_failed", "pm_sub_ended")):
                                            # GW-002：上游事件流断/起流败——
                                            # 恢复=客户端延迟重订（快照回放兜底）
                                            if (self._pm_resub_task is not None
                                                    and not self._pm_resub_task.done()):
                                                self._pm_resub_task.cancel()
                                            self._pm_resub_task = asyncio.create_task(
                                                self._pm_resub_later(ws))
                                    else:
                                        self.obs.put({"_error": data})
                                        self.on_state(
                                            f"error {data.get('type') or data.get('code')}")
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
            self._attempts += 1
            delay = reconnect_delay_s(self._attempts)
            deadline = time.monotonic() + delay
            while not self._stop.is_set() and time.monotonic() < deadline:
                await asyncio.sleep(0.1)
            if self._stop.is_set():
                break
            self.obs.put({"_link": f"{delay:.0f}s 后重连…"})
        self.on_state("closed")

    async def _pm_subscribe(self, ws) -> None:
        """PM 事件自动订阅（TK-001，GW-002 帧集）：帧 id 自增——每次
        （重）连只发全新 id 的订阅帧，不重放任何已发过的帧（客户端侧
        幂等键；网关 (client,id) 去重窗只是兜底）。"""
        self._pm_seq += 1
        await ws.send_json({"t": "pm.sub",
                            "id": f"pm-{int(time.time() * 1000)}-{self._pm_seq}",
                            "kinds": list(self.pm_kinds or ())})

    async def _pm_resub_later(self, ws) -> None:
        """上游事件流断（pm_sub_failed/pm_sub_ended）→ 延迟自动重订；
        先 ``pm.unsub`` 再 ``pm.sub``——网关同 kinds 重订是 no-op（泵不动），
        死泵场景须先清在册订阅才能起新泵；连接已断则发送即败、静默退出
        ——由断线重连面接管（重连后会话就绪路径本就重发 pm.sub）。"""
        try:
            await asyncio.sleep(PM_RESUB_DELAY_S)
            self._pm_seq += 1
            await ws.send_json({"t": "pm.unsub",
                                "id": f"pm-{int(time.time() * 1000)}-{self._pm_seq}"})
            await self._pm_subscribe(ws)
        except asyncio.CancelledError:
            pass  # 新一轮失败接管了重订调度
        except Exception:  # noqa: BLE001 — 连接已断：外层重连面接管
            pass


def fleet_rows(fleet: dict) -> list[tuple]:
    """fleet.snapshot 帧 → 表格行（code/alias/node/role/status）。

    实线形（FileTailer snapshot）：帧载荷 = {"fleet": <fleet.json 全文>}，
    席位表再嵌一层 doc["fleet"]；兼容扁平合成形 {"fleet": {<code>: …}}。
    """
    out = []
    entries = fleet.get("fleet") if isinstance(fleet, dict) else None
    if isinstance(entries, dict) and isinstance(entries.get("fleet"), dict):
        entries = entries["fleet"]  # tailer 实形：载荷是整个 fleet.json 文档
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


DETAIL_COLS = ("time", "no", "ref", "title", "chars", "status")
DETAIL_REF_I = DETAIL_COLS.index("ref")


def detail_items(frame: dict) -> list[dict]:
    """台账帧（body.push 单条/回放批、body.list_more.result 翻页批）→
    条目 dict 列表：items 批取批内 dict 条目，无批取帧身（须带 ref）。"""
    if not isinstance(frame, dict):
        return []
    items = frame.get("items")
    if isinstance(items, list):
        return [e for e in items if isinstance(e, dict)]
    return [frame] if frame.get("ref") else []


def detail_rows_from_push(frame: dict) -> list[tuple]:
    """body.push / body.list_more.result 帧 → 详情表行 (time/no/ref/title/
    chars/status)。

    单条帧（ref/inline 形）出一行；批帧（items、无 inline）按批出多行；
    无 ref 条目跳过。行序=帧内序（合并时去重）；条目缺 status 键 →
    空串（旧网关帧向后兼容）。
    """
    rows = []
    for e in detail_items(frame):
        if not e.get("ref"):
            continue
        ts = e.get("ts")
        when = time.strftime("%H:%M:%S", time.localtime(ts)) \
            if isinstance(ts, (int, float)) else ""
        rows.append((when, str(e.get("no", "") if e.get("no") is not None else ""),
                     str(e.get("ref")), str(e.get("title", "") or "")[:28],
                     str(e.get("chars", 0)), str(e.get("status", "") or "")))
    return rows


def detail_ts_map(frame: dict) -> dict[str, float]:
    """台账帧 → ref → 原始 ts：历史翻页 before_ts 游标的数据源（表行只存
    格式化时刻，原始秒值在此单独留档）。"""
    out: dict[str, float] = {}
    for e in detail_items(frame):
        ref, ts = e.get("ref"), e.get("ts")
        if ref and isinstance(ts, (int, float)):
            out[str(ref)] = float(ts)
    return out


def merge_detail_rows(prev: list[tuple], new: list[tuple]) -> list[tuple]:
    """详情表行按 ref 去重合并：已见 ref 原位更新值（行不跳动），
    新 ref 追加尾部。"""
    out = {r[DETAIL_REF_I]: r for r in prev}
    for r in new:
        out[r[DETAIL_REF_I]] = r
    return list(out.values())


def body_list_more_request(before_ts: float, req_id: str, limit: int = 50) -> dict:
    """历史翻页控制帧（body.list_more）：before_ts=当前表时间上最旧行的
    原始 ts（游标），回包 body.list_more.result 追加更早的行。"""
    return {"t": "body.list_more", "req_id": req_id,
            "before_ts": float(before_ts), "limit": int(limit)}


def run_cancel_request(ref: str, req_id: str) -> dict:
    """任务取消控制帧（run.cancel）：按台账行 ref 取消编排任务，回包
    run.cancel.result。"""
    return {"t": "run.cancel", "ref": str(ref), "req_id": req_id}


def run_cancel_result_line(frame: dict) -> str:
    """run.cancel.result 回包 → 提示行：ok 带 state（缺省 cancelled）；
    ok:false 显示 error 原文（如 unknown-ref）。"""
    if not isinstance(frame, dict) or "ok" not in frame:
        return "⚠ 取消回包不可读"
    if not frame.get("ok"):
        return f"⚠ 取消失败：{frame.get('error') or '未知错误'}"
    state = str(frame.get("state") or "cancelled")
    return f"已取消 {frame.get('ref', '?')}（{state}）"


def detail_ack_line(frame: dict, notify_refs) -> str | None:
    """body.push 帧 → 回合页灰行「└已入详情」；ref 未在回合页出现过
    （无 notify 相）则 None——灰行只标已通报过的任务。"""
    if not isinstance(frame, dict):
        return None
    ref = frame.get("ref")
    if not ref or ref not in set(notify_refs or ()):
        return None
    return f"└ 已入详情 {ref}"


def error_identity(frame) -> tuple[str, str]:
    """error 帧 → (类型, 消息)：新契约 ``type``/``message`` 优先，兜底旧
    ``code``/``msg``（最终回落 ``t``）；无任何身份字段 → ("", "")，由
    调用方走各自的「未知错误」占位。消息截 120 字。"""
    if not isinstance(frame, dict):
        return "", ""
    etype = str(frame.get("type") or frame.get("code") or frame.get("t") or "")
    msg = str(frame.get("message") or frame.get("msg") or "")[:120]
    return etype, msg


def detail_error_line(frame: dict) -> str:
    """error 帧（body.get 失败回包）→ 详情右栏提示行。"""
    if not isinstance(frame, dict):
        return "⚠ 未知错误"
    etype, msg = error_identity(frame)
    return "⚠ " + (" ".join(p for p in (etype, msg) if p) or "未知错误")


def pending_error_line(kind: str, frame: dict) -> str:
    """命中的 error 帧 → 「<中文名>失败：type message」回填行。"""
    name = PENDING_KINDS.get(kind, kind or "请求")
    etype, msg = error_identity(frame)
    return f"{name}失败：{' '.join(p for p in (etype, msg) if p) or '未知错误'}"


def pending_timeout_line(kind: str) -> str:
    """在途超时 → 「<中文名>超时无响应」回填行。"""
    return f"{PENDING_KINDS.get(kind, kind or '请求')}超时无响应"


def pending_send_fail_line(kind: str) -> str:
    """req 泵发送失败 → 「<中文名>失败：发送失败（链路断开）」回填行。"""
    return f"{PENDING_KINDS.get(kind, kind or '请求')}失败：发送失败（链路断开）"


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


def fleet_brief_request(req_id: str) -> dict:
    """席位状态简报控制帧（fleet.brief）：网关 join fleet.json 与
    session.list 产出各席位一行状态，回包 fleet.brief.result。"""
    return {"t": "fleet.brief", "req_id": req_id}


def _brief_idle_label(idle_s) -> str:
    """idle 秒数 → 相对时间短语：分钟/小时/天三档；空/非数返 ""。"""
    try:
        s = float(idle_s)
    except (TypeError, ValueError):
        return ""
    s = max(0.0, s)
    if s < 3600:
        return f"{int(s // 60)}分钟前动过"
    if s < 86400:
        return f"{int(s // 3600)}小时前"
    return f"{int(s // 86400)}天前"


def last_seen_label(v) -> str:
    """brief 席位 ``last_seen`` 字段 → 席位行显示值：epoch 秒 → HH:MM:SS；
    缺省/空 →「—」；其他文本原样。"""
    if isinstance(v, bool):
        return "—"
    if isinstance(v, (int, float)):
        return time.strftime("%H:%M:%S", time.localtime(v))
    s = str(v or "").strip()
    return s if s else "—"


def fleet_brief_lines(result) -> list[str]:
    """fleet.brief.result 回包 → 席位简报文本行（一行一席）。

    行形「id · node · 在跑/没跑 · title · 闲置时长」：title 空省段；
    live=false（session.list 已无此会话）无闲置段、行尾标注会话已死；
    回包带 note（loopback 降级仅席位表）时首行前插 ⚠ 行；畸形回包
    一行占位不抛。
    """
    if not isinstance(result, dict) or not isinstance(result.get("seats"), list):
        return ["⚠ 简报回包不可读"]
    lines: list[str] = []
    if result.get("note"):
        lines.append(f"⚠ {result['note']}")
    for seat in result["seats"]:
        if not isinstance(seat, dict):
            continue
        parts = [str(seat.get("id", "?")), str(seat.get("node", "") or ""),
                 "在跑" if seat.get("running") else "没跑"]
        if seat.get("title"):
            parts.append(str(seat["title"]))
        idle = _brief_idle_label(seat.get("idle_s"))
        if idle:
            parts.append(idle)
        line = " · ".join(parts)
        if not seat.get("live"):
            line += " (会话已死)"
        lines.append(line)
    return lines


def liaison_line(liaison) -> str:
    """fleet.brief.result 顶层 liaison 字段 → 席位页常显状态行。

    契约：字段恒存在 ``{"bound": bool, "code": str|null,
    "sessionId": str|null, "archived": bool}``。bound=false → 无绑定；
    bound → 席位码（archived 追加「（已归档）」）。非 dict（防御）→
    未知；字段缺失（旧网关回包）由调用方保持原显示不覆盖。
    """
    if not isinstance(liaison, dict):
        return "对接席位：未知"
    if not liaison.get("bound"):
        return "对接席位：无绑定"
    code = str(liaison.get("code") or "?")
    suffix = "（已归档）" if liaison.get("archived") else ""
    return f"对接席位：{code}{suffix}"


def liaison_unbind_request(req_id: str) -> dict:
    """对接解绑控制帧（liaison.unbind）：回包 liaison.result 带
    ``{"op": "unbind", "ok", "was_bound"}``。"""
    return {"t": "liaison.unbind", "req_id": req_id}


def liaison_bind_request(code: str, req_id: str) -> dict:
    """对接绑定控制帧（liaison.bind）：code=目标席位码，回包
    liaison.result 带 ``{"op": "bind", "ok", "error"?, "liaison": {…}}``。"""
    return {"t": "liaison.bind", "code": str(code), "req_id": req_id}


def liaison_result_line(frame: dict) -> str:
    """liaison.result 回包 → 事件区一行：ok=false 显示 error 原文；
    unbind 且 was_bound=false →「本就无绑定」（非错误）；成功带 op 目标态。"""
    if not isinstance(frame, dict) or "ok" not in frame:
        return "⚠ 对接回包不可读"
    op = str(frame.get("op", "?"))
    if not frame.get("ok"):
        name = "解绑" if op == "unbind" else "绑定"
        return f"⚠ 对接{name}失败：{frame.get('error') or '未知错误'}"
    if op == "unbind":
        return "对接解绑：本就无绑定" if not frame.get("was_bound") else "对接解绑：已解绑"
    if isinstance(frame.get("liaison"), dict):
        return f"对接绑定：{liaison_line(frame['liaison']).split('：', 1)[-1]}"
    return "对接绑定：成功"


def head_list_request(req_id: str) -> dict:
    """head 配置查询控制帧：回包 head.list.result（配置表+当前激活）。"""
    return {"t": "head.list", "req_id": req_id}


def head_switch_request(name: str, req_id: str) -> dict:
    """head 切换控制帧：激活单例——只改下一次语音会话的 head 选择。"""
    return {"t": "head.switch", "name": str(name), "req_id": req_id}


def head_switch_line(frame: dict) -> str:
    """head.switch.result 回包 → 单行结果（切换目标 + 生效说明）。"""
    if not isinstance(frame, dict) or not frame.get("ok"):
        return f"⚠ head 切换失败：{(frame or {}).get('reason', '未知原因')}"
    line = f"head → {frame.get('active', '?')}"
    if frame.get("note"):
        line += f"（{frame['note']}）"
    return line


def head_names_from_list(frame: dict) -> tuple[list[tuple[str, str]], str, bool]:
    """head.list.result 回包 → ([(name,label)…], active, file_backed)。

    单头模式（file_backed=False）也回一行 default——UI 据此只显示
    「单头模式」不渲染切换钮。
    """
    profiles = frame.get("profiles") if isinstance(frame, dict) else None
    rows = [(str(p.get("name")), str(p.get("label") or p.get("name")))
            for p in profiles if isinstance(p, dict)] if isinstance(profiles, list) else []
    return rows, str(frame.get("active", "")), bool(frame.get("file_backed"))


def whiteboard_set_request(text, req_id: str) -> dict:
    """白板写入控制帧（whiteboard.set）：把本页文本整段存进网关全局白板
    （单例、跨会话存活、65536 字上限），req_id 关联回包
    whiteboard.set.result。"""
    return {"t": "whiteboard.set", "text": str(text), "req_id": req_id}


def whiteboard_set_line(result) -> str:
    """whiteboard.set.result 回包 → 单行结果（同步字数 / 失败原因）。"""
    if not isinstance(result, dict) or "ok" not in result:
        return "⚠ 白板回包不可读"
    if result["ok"]:
        return f"✓ 白板已同步（{result.get('chars', '?')} 字）"
    return f"⚠ 白板同步失败：{result.get('reason', '未知原因')}"


# ---- TK-001 PM 协议面（<internal-repo> spec-tk-client：纯函数，便于单测） ----

def reconnect_delay_s(attempt: int, base: float = RECONNECT_BASE_S,
                      cap: float = RECONNECT_MAX_S) -> float:
    """断线重连指数退避：第 attempt 次（1 起）等待 min(cap, base·2^(attempt-1))
    ——2,4,8,16,30,30…；连接成功即从 1 重数（调用方复位计数）。"""
    return min(cap, base * (2 ** (max(1, attempt) - 1)))


def pm_error_of(frame: dict) -> tuple[str, str]:
    """pm.res error → (code, message)：``error`` 为 ``{code, message}`` 形；
    无 error → ("", "")。message 截 120 字。"""
    err = frame.get("error") if isinstance(frame, dict) else None
    if isinstance(err, dict):
        return str(err.get("code", "")), str(err.get("message", ""))[:120]
    if isinstance(err, str):
        return err, ""
    return "", ""


def pm_is_degraded(code: str) -> bool:
    """pm_* 失败码族（pm_down/pm_unreachable/pm_timeout/pm_unavailable/
    pm_status/pm_bad_payload/pm_internal）= PM 底座故障 → 降级横幅；
    bad_request 等网关侧校验错不算降级。"""
    return str(code).startswith("pm_")


def pm_res_line(frame: dict) -> str:
    """pm.res → PM 流水行：订阅受理形出订阅清单，退订回包（带
    was_subscribed）出退订行，一般 data 出 JSON 截断，error 出
    「⚠ code message」。"""
    if not isinstance(frame, dict):
        return "⚠ PM 回包不可读"
    code, msg = pm_error_of(frame)
    if code:
        return f"⚠ pm.res[{frame.get('id')}] {code} {msg}".rstrip()
    data = frame.get("data")
    if isinstance(data, dict) and "subscribed" in data:
        if "was_subscribed" in data:                    # pm.unsub 回包
            return (f"✓ pm.res[{frame.get('id')}] 已退订"
                    if data["was_subscribed"]
                    else f"✓ pm.res[{frame.get('id')}] 本就无订阅")
        note = f"（{data['note']}）" if data.get("note") else ""
        kinds = "，".join(str(k) for k in data["subscribed"]) or "—"
        return f"✓ pm.res[{frame.get('id')}] 已订阅 {kinds}{note}"
    return f"✓ pm.res[{frame.get('id')}] {json.dumps(data, ensure_ascii=False)[:160]}"


def pm_event_line(frame: dict, now: float | None = None) -> str:
    """pm.event → 单行流水：时刻 #seq source/kind path（快照回放标 ·回放）。"""
    if not isinstance(frame, dict):
        return "⚠ PM 事件不可读"
    when = time.strftime("%H:%M:%S",
                         time.localtime(now if now is not None else time.time()))
    path = str(frame.get("path", "") or "")[:60]
    replay = " · 回放" if frame.get("replay") else ""
    return (f"{when} #{frame.get('seq', '?')} {frame.get('source', '?')}/"
            f"{frame.get('kind', '?')} {path}{replay}")


def pm_banner_degraded_line(code: str, msg: str) -> str:
    """降级横幅文案：pm_* 失败码 / 订阅断流（pm_sub_failed/ended）点亮。"""
    return f"⚠ PM 降级（{code or '未知'}）：{(msg or '').strip()[:80] or 'PM 服务不可用'}"


def pm_banner_ok_line(subscribed) -> str:
    """恢复横幅文案：pm.res{subscribed} 到达（订阅受理）即熄灭降级。"""
    kinds = ("，".join(str(k) for k in subscribed)
             if isinstance(subscribed, list) else "")
    return f"PM 正常 · 已订阅 {kinds or '—'}"


def load_session_config(path: Path | None = None) -> dict:
    """会话配置读取（可丢，TK-001 存储）：``{"url": <网关地址>}``；缺文件/
    损坏/字段不合法一律回 {}——配置丢失可接受，回落默认地址。"""
    try:
        data = json.loads(
            (path or SESSION_CONFIG_PATH).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if (isinstance(data, dict) and isinstance(data.get("url"), str)
            and data["url"].strip()):
        return {"url": data["url"].strip()}
    return {}


def save_session_config(url: str, path: Path | None = None) -> None:
    """会话配置落盘（best-effort）：关窗时记当前网关地址；失败静默（可丢）。"""
    try:
        (path or SESSION_CONFIG_PATH).write_text(
            json.dumps({"url": str(url)}, ensure_ascii=False), encoding="utf-8")
    except OSError:
        pass


# ---- TK-002 票板（渲染纯函数：state → view；事件驱动重绘的幂等键） ----

def tickets_normalize(raw) -> dict[str, dict]:
    """op=tickets 全量列表 → ``{ticket_id: 规范票卡}``。

    deps/refs 服务侧是 JSON 编码串（容错解包：坏串退化为分隔列表/空
    dict）；无 ticket_id 或非 dict 条目跳过。字段缺省不抛——畸形票保
    位渲染（零业务，只整形）。
    """
    out: dict[str, dict] = {}
    if not isinstance(raw, list):
        return out
    for e in raw:
        if not isinstance(e, dict):
            continue
        tid = str(e.get("ticket_id") or "").strip()
        if not tid:
            continue
        deps = e.get("deps")
        if isinstance(deps, str):
            try:
                deps = json.loads(deps)
            except ValueError:
                deps = [d.strip() for d in deps.split(",") if d.strip()]
        if not isinstance(deps, list):
            deps = []
        refs = e.get("refs")
        if isinstance(refs, str):
            try:
                refs = json.loads(refs)
            except ValueError:
                refs = {}
        if not isinstance(refs, dict):
            refs = {}
        out[tid] = {
            "id": tid,
            "title": str(e.get("title") or "").replace("\n", " "),
            "state": str(e.get("state") or "").strip(),
            "deps": [str(d) for d in deps],
            "lease_owner": e.get("lease_owner") or None,
            "refs": {str(k): str(v) for k, v in refs.items()},
            "outcome": e.get("outcome") or None,
            "updated_at": str(e.get("updated_at") or ""),
        }
    return out


def tickets_column_of(t: dict) -> str:
    """票卡 → kanban 列名：五态之外（含空）落「其他」兜底列。"""
    state = t.get("state", "")
    return state if state in TICKET_STATES else "其他"


def ticket_time_label(v: str) -> str:
    """ISO updated_at → 本地 ``MM-DD HH:MM``；解析失败原样截断 11 字。"""
    try:
        return datetime.fromisoformat(str(v)).astimezone().strftime("%m-%d %H:%M")
    except (TypeError, ValueError):
        return str(v)[:11]


def tickets_card_lines(t: dict) -> list[str]:
    """单票 → 卡片行块：id·时刻 / 标题 / ⛓deps 边 / 🏷refs chips /
    👤lease_owner / ✅outcome（空段省略）。"""
    lines = [f"{t['id']} · {ticket_time_label(t['updated_at'])}"]
    if t["title"]:
        lines.append(f"  {t['title'][:48]}")
    if t["deps"]:
        lines.append(f"  ⛓ {'，'.join(t['deps'])}"[:80])
    if t["refs"]:
        chips = " ".join(sorted(t["refs"]))[:60]
        lines.append(f"  🏷 {chips}")
    if t["lease_owner"]:
        lines.append(f"  👤 {t['lease_owner']}")
    if t["outcome"]:
        lines.append(f"  ✅ {str(t['outcome'])[:40]}")
    return lines


def tickets_kanban_view(state: dict[str, dict]) -> dict[str, list[str]]:
    """票卡 state → kanban 视图（纯函数，视图=state 的确定映射）：五列
    （加「其他」兜底列，仅非空时出现），列内 updated_at 降序、卡间空行。
    全量重放同 state 必得同视图（验证门②的幂等键）。"""
    by_col: dict[str, list[dict]] = {name: [] for name in TICKET_STATES}
    for t in state.values():
        by_col.setdefault(tickets_column_of(t), []).append(t)
    view: dict[str, list[str]] = {}
    for name, ts in by_col.items():
        cards = [tickets_card_lines(t)
                 for t in sorted(ts, key=lambda t: t["updated_at"],
                                 reverse=True)]
        block: list[str] = []
        for i, card in enumerate(cards):
            if i:
                block.append("")
            block.extend(card)
        if name == "其他" and not block:
            continue
        view[name] = block
    return view


def _iso_ts(v) -> float | None:
    """ISO 时间串 → epoch 秒；数字直通，解析失败/缺省 None。"""
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return datetime.fromisoformat(str(v)).timestamp()
    except (TypeError, ValueError):
        return None


def fleet_ship_card_of(code: str, e: dict) -> dict:
    """单席位条目（fleet.json 原条目或 op=fleet 投影 seat）→ 规范卡。

    两源一卡：尾随 fleet.snapshot（fleet.json 全文，带租约/准入字段）与
    op=fleet 投影（SEAT_KEYS+session join，无租约字段）共用一模型；缺的
    字段落 None/空，渲染层按「—」透出。"""
    status = str(e.get("status") or "")
    return {
        "code": str(code),
        "kind": str(e.get("kind") or "maestro"),
        "term": str(e.get("handle") or ""),
        "status": status,
        "preset": str(e.get("preset") or ""),
        "node": str(e.get("node") or ""),
        "role": str(e.get("role") or ""),
        "alias": str(e.get("alias") or ""),
        "last_seen": _iso_ts(e.get("lastSeenAt") or e.get("heartbeatAt")
                             or e.get("spawnedAt")),
        "lease_until": _iso_ts(e.get("leaseExpiresAt")),
        "lease_owner": str(e["owner"]) if e.get("owner") else None,
        "handover": bool(e.get("retiring"))
        or status in ("handover", "retiring"),
    }


def fleet_ship_cards_doc(doc) -> dict[str, dict]:
    """fleet.snapshot 帧/fleet.json 全文 → 码→卡（内层 fleet 键解包）。"""
    entries = doc.get("fleet") if isinstance(doc, dict) else None
    if isinstance(entries, dict) and isinstance(entries.get("fleet"), dict):
        entries = entries["fleet"]          # tailer 实形：载荷是 fleet.json 全文
    if not isinstance(entries, dict):
        return {}
    return {str(c): fleet_ship_card_of(c, e) for c, e in entries.items()
            if isinstance(e, dict)}


def fleet_ship_cards_seats(seats) -> dict[str, dict]:
    """op=fleet 投影 seats 列表 → 码→卡（session join 的 running 并入）。"""
    out: dict[str, dict] = {}
    if not isinstance(seats, list):
        return out
    for s in seats:
        if not isinstance(s, dict) or not s.get("code"):
            continue
        card = fleet_ship_card_of(str(s["code"]), s)
        session = s.get("session")
        if isinstance(session, dict):
            card["running"] = bool(session.get("running"))
        out[card["code"]] = card
    return out


def fleet_verify_of(card: dict, now: float, stale_s: float) -> str:
    """准入态透出：probing/verified/mismatch 原样；lastSeen 超（可配）阈值
    判 stale；无准入态且新鲜的 maestro 席落「—」。"""
    v = card.get("status", "")
    if v in FLEET_VERIFY_STATES:
        return v
    ls = card.get("last_seen")
    if ls is not None and now - ls > stale_s:
        return "stale"
    return "—"


def fleet_last_seen_label(last_seen, now: float) -> str:
    """lastSeen 时长（epoch 差→人读时长）；缺省「—」。"""
    if last_seen is None:
        return "—"
    d = max(0, int(now - last_seen))
    if d < 60:
        return f"{d}s前"
    if d < 3600:
        return f"{d // 60}m前"
    if d < 86400:
        return f"{d // 3600}h前"
    return f"{d // 86400}d前"


def fleet_lease_label(card: dict, now: float) -> str:
    """租约到期倒计时（fleet-touch claim 写 leaseExpiresAt/owner）。"""
    until = card.get("lease_until")
    if until is None:
        return ""
    left = int(until - now)
    owner = card.get("lease_owner") or ""
    who = f"{owner} " if owner else ""
    if left >= 0:
        return f"⏳租约 {who}还剩{left // 60}m{left % 60:02d}s"
    return f"⏳租约 {who}已过期{-left // 60}m{-left % 60:02d}s"


def fleet_ship_card_lines(card: dict, now: float,
                          stale_s: float = FLEET_VERIFY_STALE_S) -> list[str]:
    """单席位 → 卡片行块：短码·状态·preset / term_·准入态·lastSeen 时长 /
    节点·角色 / 租约倒计时 / 换代中瞬态（空段省略）。"""
    verify = fleet_verify_of(card, now, stale_s)
    mark = "⚠" if verify == "stale" else ("✓" if verify == "verified" else "·")
    lines = [f"{card['code']} · {card['status'] or '—'}"
             + (f" · {card['preset']}" if card["preset"] else "")]
    lines.append(f"term {card['term'] or '—'} · {mark}{verify} · lastSeen "
                 f"{fleet_last_seen_label(card['last_seen'], now)}")
    seg = " · ".join(x for x in (card["node"], card["role"]) if x)
    if seg:
        lines.append(seg)
    lease = fleet_lease_label(card, now)
    if lease:
        lines.append(lease)
    if card["handover"]:
        lines.append("⟳ 换代中")
    return lines


def fleet_ship_view(cards: dict[str, dict], now: float,
                    stale_s: float = FLEET_VERIFY_STALE_S) -> list[str]:
    """席位卡 state → 视图（纯函数）：码序稳定、卡间空行；同 state+now
    必得同视图（全量重放一致门的幂等键）。"""
    block: list[str] = []
    for i, code in enumerate(sorted(cards)):
        if i:
            block.append("")
        block.extend(fleet_ship_card_lines(cards[code], now, stale_s))
    return block


def trace_query_params(session_id, type_csv="", tool="", text="",
                       seq_from=None, seq_to=None) -> dict:
    """UI 输入 → op=trace 查询参数（空值剔除、seq 收敛 int）。

    参数组合即请求幂等键（spec §TK-004）：同组合必同参数，同参数服务端
    必同投影——本函数是组合到参数的唯一确定映射。"""
    p: dict = {}
    if session_id:
        p["sessionId"] = str(session_id)
    if type_csv:
        p["type"] = ",".join(x.strip() for x in str(type_csv).split(",")
                             if x.strip())
    if tool:
        p["tool"] = str(tool)
    if text:
        p["text"] = str(text)
    for k, v in (("seqFrom", seq_from), ("seqTo", seq_to)):
        if v not in (None, ""):
            try:
                p[k] = int(v)
            except (TypeError, ValueError):
                pass
    return p


def trace_params_key(params: dict) -> str:
    """参数组合 → 幂等键（sorted json 串）。"""
    return json.dumps(params, ensure_ascii=False, sort_keys=True)


def trace_expand_params(data) -> dict | None:
    """折叠快照 → 展开被折叠头部的续查参数（seqTo=保留区首条 seq-1，其余
    过滤条件原样）；无折叠/推不出边界 → None。被折叠头部自身超预算时
    服务端会再折叠——展开是逐层剥洋葱，客户端不做预算业务。"""
    if not isinstance(data, dict) or not data.get("folded"):
        return None
    entries = data.get("entries")
    if not isinstance(entries, list) or len(entries) < 2:
        return None
    seq = entries[1].get("seq") if isinstance(entries[1], dict) else None
    if not isinstance(seq, int):
        return None
    f = data.get("filter") if isinstance(data.get("filter"), dict) else {}
    sr = (data.get("matched") or {}).get("seq_range") or [None, None]
    base = trace_query_params(data.get("sessionId") or "", f.get("type") or "",
                              f.get("tool") or "", f.get("text") or "",
                              sr[0], seq - 1)
    return base if base.get("seqFrom") is not None else None


def trace_phase_of(entry: dict) -> str:
    """记录类型前缀 → 着色相（确定性；未知归 misc）。"""
    t = str(entry.get("type") or "?")
    for prefix, phase in TRACE_TYPE_PHASES:
        if t.startswith(prefix):
            return phase
    return "misc"


def trace_line_of(entry: dict) -> str:
    """单记录 → 时间线行：#seq · 类型[turnN] · 工具名 · 摘要（≤80 字符）。"""
    e = entry if isinstance(entry, dict) else {}
    d = e.get("data") if isinstance(e.get("data"), dict) else {}
    seq = e.get("seq")
    turn = d.get("turn")
    parts = [f"#{seq if isinstance(seq, int) else '?'}",
             str(e.get("type") or "?")
             + (f"[turn{turn}]" if isinstance(turn, int) else "")]
    name = d.get("name") or d.get("toolName") or ""
    if name:
        parts.append(str(name))
    brief = (d.get("title") or d.get("summary") or d.get("command")
             or d.get("arguments") or "")
    if not brief:
        brief = " ".join(str(v) for v in d.values()
                         if isinstance(v, (str, int, float)))
    brief = re.sub(r"\s+", " ", str(brief))[:80]
    if brief:
        parts.append(brief)
    return " · ".join(parts)


def trace_fold_line(s: dict) -> str:
    """trace.compact 折叠摘要 → 单行（可展开提示）。"""
    d = s.get("dropped") if isinstance(s.get("dropped"), dict) else {}
    k = s.get("kept") if isinstance(s.get("kept"), dict) else {}
    sr = s.get("seq_range")
    return (f"⊘ trace.compact 折叠：丢弃 {d.get('entries', '?')} 条/"
            f"{d.get('chars', '?')} 字符 · 保留 {k.get('entries', '?')} 条/"
            f"{k.get('chars', '?')} 字符 · seq范围 {sr} （可展开）")


def trace_groups(entries: list) -> list[tuple[str, list[dict]]]:
    """seq 序 entries → turn 分组（data.turn 编组；无 turn 归「· 前导」；
    trace.compact 摘要行归前导组）。"""
    groups: list[tuple[str, list[dict]]] = []
    cur_key: str | None = None
    cur_list: list[dict] = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        d = e.get("data") if isinstance(e.get("data"), dict) else {}
        turn = d.get("turn")
        key = (f"turn {turn}" if isinstance(turn, int)
               and str(e.get("type")) != "trace.compact" else "· 前导")
        if key != cur_key:
            cur_key = key
            cur_list = []
            groups.append((key, cur_list))
        cur_list.append(e)
    return groups


def trace_view(entries: list) -> list[tuple[str, str]]:
    """entries → [(相标签, 行)] 确定视图：同 entries 必同视图（过滤器重放
    同视图门的客户端幂等键）。"""
    view: list[tuple[str, str]] = []
    for key, es in trace_groups(entries):
        view.append(("group", f"── {key}（{len(es)} 条）"))
        for e in es:
            if str(e.get("type")) == "trace.compact":
                view.append(("fold", trace_fold_line(e)))
            else:
                view.append((trace_phase_of(e), trace_line_of(e)))
    return view


def vad_tail_plan(total_ms: int = VAD_TAIL_MS,
                  step_ms: int = VAD_TAIL_STEP_MS) -> list[tuple[int, int]]:
    """尾静音投递表：``[(距松键毫秒, 块字节数), …]``。

    服务端 VAD 按「收到的静音样本量」判句尾，但网关限制上行媒体
    ≤64KB/s（1s 滑窗）——大帧倾倒会撞限速断链，故按真实码率（32KB/s）
    步进投递。
    """
    if total_ms <= 0 or step_ms <= 0:
        return []
    return [(step_ms * (k + 1), 2 * RATE * step_ms // 1000)
            for k in range(total_ms // step_ms)]


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
        self._brief_seq = 0                        # fleet.brief req_id 序号
        self._head_seq = 0                         # head.switch req_id 序号
        self._wb_seq = 0                           # whiteboard.set req_id 序号
        self._body_seq = 0                         # body.get req_id 序号（按 ref 清在途的发起序）
        self._list_more_seq = 0                    # body.list_more req_id 序号
        self._cancel_seq = 0                       # run.cancel req_id 序号
        self._liaison_seq = 0                      # liaison.* req_id 序号
        self._pm_seq = 0                           # pm.req 控制台 id 序号（TK-001）
        self._pm_tickets: dict[str, dict] = {}     # 票板 state（id→规范票卡，TK-002）
        self._pm_tickets_sig = None                # 最近全量 signature（重放门参照）
        self._ticket_events: list[str] = []        # tickets 事件侧栏行（最新在尾）
        self._tickets_fetch_job: str | None = None  # 防抖全量重拉定时器
        self._pm_fleet_cards: dict[str, dict] = {}  # 席位舰 state（码→卡，TK-003）
        self._fleet_ship_fetch_job: str | None = None  # 防抖全量重拉定时器
        self._fleet_ship_seq = 0                   # 席位舰拉取 id 序号（pmf- 前缀独立域）
        self._fleet_ship_snap_ts: float | None = None  # 舰页收帧时刻（断流陈旧判定）
        self._pmt_seq = 0                          # pmt- 拉取 id 序号（tickets/trace 共用——同前缀跨流唯一，防同毫秒同序号撞 id 被网关去重窗静默丢帧）
        self._trace_last: dict | None = None       # 最近 op=trace 快照（展开参照）
        self._pending: dict[str, dict] = {}        # req_id → {kind, ts, rid[, ref, seq]} 在途
        self._fleet_codes: set[str] | None = None  # 上次 fleet.snapshot 席位码（移出 diff 源）
        self._fleet_rows_cache: list[tuple] = []   # 上次快照表行（brief 回填 last_seen 后重绘源）
        self._fleet_events: list[dict] = []        # 席位事件历史 [{key,line}]（去重键见 _fleet_event_add）
        self._fleet_last_seen: dict[str, str] = {}  # 席位码 → last_seen 显示值（brief 回填）
        self._fleet_snap_ts: float | None = None   # 上次 fleet.snapshot 收帧时刻（陈旧判定源）
        self._detail_ts: dict[str, float] = {}     # ref → 原始 ts（翻页 before_ts 游标源）
        self._list_more_eof = False                # body.list_more 翻到底（按钮置灰）
        self._wb_push_job: str | None = None       # 白板防抖自动同步定时器
        self.stream: sd.InputStream | None = None

        root.title("rt-voice · ONE 桌面客户端（语音+观测）")
        root.geometry("980x640")
        root.minsize(780, 520)

        # ---- 顶栏：地址 / token / 双连接开关 ----
        # （先于 Notebook pack：连接控制是窗口级 chrome，页签内容再高也
        # 不许把它挤没——历史上后 pack 在固定窗高下会被压缩到 0 高）
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

        # ---- Notebook：语音页 + 观测四页签 ----
        self.nb = ttk.Notebook(root)
        self.nb.pack(fill="both", expand=True)

        voice_tab = ttk.Frame(self.nb, padding=0)
        self.nb.add(voice_tab, text=" 语音 ")

        # ---- 语音页：head 选择行 + 频谱 + 开关 + 事件面板 ----
        head_row = ttk.Frame(voice_tab, padding=(10, 6))
        head_row.pack(fill="x")
        self.head_row = head_row
        ttk.Label(head_row, text="head").pack(side="left")
        self.head_var = tk.StringVar(value="")
        self.head_buttons: dict = {}
        self.head_note_var = tk.StringVar(value="（观测开启后显示 head 配置）")
        ttk.Label(head_row, textvariable=self.head_note_var,
                  foreground="#8a8a8a").pack(side="left", padx=10)

        self.canvas = tk.Canvas(voice_tab, bg="#0b0f14", height=240, highlightthickness=0)
        self.canvas.pack(fill="both", expand=True, padx=8, pady=(6, 0))
        self.level_var = tk.StringVar(value="电平 ──────────")
        ttk.Label(voice_tab, textvariable=self.level_var, font=("monospace 10")).pack(anchor="w", padx=12)

        mid = ttk.Frame(voice_tab, padding=(8, 4))
        mid.pack(fill="x")
        self.ptt = PushToTalk()
        self._tail_after: str | None = None   # 尾静音步进定时器（再按/关窗取消）
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
        self.fleet_tab = fleet_tab
        self.nb.add(fleet_tab, text=" 席位 ")
        # 顶部快照行：常显收帧时刻，距收帧超 FLEET_STALE_S 由 _tick 追加
        # 「（可能陈旧）」——观测断流时席位表停在旧态的可见提示
        self.fleet_snap_var = tk.StringVar(value="快照 —")
        ttk.Label(fleet_tab, textvariable=self.fleet_snap_var,
                  foreground="#555").pack(anchor="w")
        cols = ("code", "alias", "node", "role", "status", "last_seen")
        # 多选（extended）：清理动作按批处理所选席位
        self.fleet_tree = ttk.Treeview(fleet_tab, columns=cols, show="headings",
                                       height=18, selectmode="extended")
        for c, w in zip(cols, (60, 110, 190, 90, 90, 90)):
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
        # 简报按钮：只读拉席位状态（fleet.brief），无确认门
        self.fleet_brief_btn = ttk.Button(
            fleet_bar, text="简报", command=self._fleet_brief)
        self.fleet_brief_btn.pack(side="left", padx=(8, 0))
        # liaison 运维面：解绑当前对接 / 绑定到选中席位（均有确认门）
        self.liaison_unbind_btn = ttk.Button(
            fleet_bar, text="解绑对接", command=self._liaison_unbind)
        self.liaison_unbind_btn.pack(side="left", padx=(12, 0))
        self.liaison_bind_btn = ttk.Button(
            fleet_bar, text="绑定到选中席位", command=self._liaison_bind_selected)
        self.liaison_bind_btn.pack(side="left", padx=(8, 0))
        # liaison 常显行：数据来自 fleet.brief.result 顶层 liaison 字段；
        # 未收到过该字段（旧网关）保持「未知」
        self.liaison_var = tk.StringVar(value="对接席位：未知")
        ttk.Label(fleet_bar, textvariable=self.liaison_var,
                  foreground="#555").pack(side="left", padx=(12, 0))
        # 席位简报面板：fleet.brief.result 逐行渲染（一行一席，追加式）
        self.fleet_brief_log = scrolledtext.ScrolledText(
            fleet_tab, font=("monospace 8"), state="disabled", wrap="none", height=6)
        self.fleet_brief_log.pack(fill="x")
        # 席位事件区（小型历史面板）：清理/简报/移出/超时/对接变化保留
        # 最近 FLEET_EVENT_KEEP 条，最新条标 ▸（渲染置顶）
        ttk.Label(fleet_tab, text=f"席位事件（最近 {FLEET_EVENT_KEEP} 条，▸ 最新）",
                  foreground="#8a8a8a").pack(anchor="w", pady=(6, 0))
        self.fleet_event_log = scrolledtext.ScrolledText(
            fleet_tab, font=("monospace 8"), state="disabled", wrap="none", height=5)
        self.fleet_event_log.pack(fill="x")
        self.fleet_note_var = tk.StringVar(value="")
        ttk.Label(fleet_tab, textvariable=self.fleet_note_var,
                  foreground="#555").pack(anchor="w", pady=(4, 0))

        # ---- 白板页签（协作交互输入面）：文本区即白板，改动防抖自动同步 ----
        # 用户在此手打/粘贴长文（文档、日志、参考资料），文本区就是白板
        # 本体：改动停顿 600ms 即自动推进网关全局白板，语音侧让 head 调
        # read_whiteboard 直接读当前内容——长输入走眼不走嘴，无手动同步。
        wb_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(wb_tab, text=" 白板 ")
        self.whiteboard_note_var = tk.StringVar(
            value="输入即自动同步——语音侧可直接读（说：看一下白板）")
        ttk.Label(wb_tab, textvariable=self.whiteboard_note_var,
                  foreground="#555").pack(anchor="w")
        self.wb_text = scrolledtext.ScrolledText(wb_tab, font=("monospace 10"),
                                                 wrap="word")
        self.wb_text.pack(fill="both", expand=True)
        # <<Modified>> 是文本区标准变更信号：清标志 + 防抖重排一次推送
        self.wb_text.bind("<<Modified>>", self._wb_on_modified)

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
        # 任务操作条：历史翻页（body.list_more，eof 后置灰）+ 选中行取消
        # （run.cancel，确认门；已取消行置灰不重复发）
        task_bar = ttk.Frame(task_top)
        task_bar.pack(fill="x", pady=(0, 4))
        self.task_more_btn = ttk.Button(task_bar, text="拉取更多",
                                        command=self._task_list_more)
        self.task_more_btn.pack(side="left")
        self.task_cancel_btn = ttk.Button(task_bar, text="取消",
                                          command=self._task_cancel)
        self.task_cancel_btn.pack(side="left", padx=(8, 0))
        self.task_note_var = tk.StringVar(value="")
        ttk.Label(task_bar, textvariable=self.task_note_var,
                  foreground="#555").pack(side="left", padx=10)
        detail_pane = self.ttk.PanedWindow(task_top, orient="horizontal")
        detail_pane.pack(fill="both", expand=True)
        detail_left = ttk.Frame(detail_pane)
        detail_right = ttk.Frame(detail_pane)
        detail_pane.add(detail_left, weight=3)
        detail_pane.add(detail_right, weight=4)
        self.detail_tree = ttk.Treeview(detail_left, columns=DETAIL_COLS,
                                        show="headings", height=10, selectmode="browse")
        for c, w in zip(DETAIL_COLS, (76, 44, 130, 280, 52, 76)):
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

        # ---- PM 页签（TK-001：WS PM 协议接入——横幅 + 透传控制台 + 事件流水）----
        # 订阅面在 ObserveLink（会话就绪即自动 pm.sub）；此处只做渲染与
        # 透传输入，零业务（ADR-004）
        pm_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(pm_tab, text=" PM ")
        self.pm_banner_var = tk.StringVar(value=PM_BANNER_OFF)
        self.pm_banner = tk.Label(pm_tab, textvariable=self.pm_banner_var,
                                  anchor="w", padx=8, pady=4,
                                  bg="#e8e8e8", fg="#555")
        self.pm_banner.pack(fill="x")
        pm_bar = ttk.Frame(pm_tab)
        pm_bar.pack(fill="x", pady=(6, 0))
        ttk.Label(pm_bar, text="op").pack(side="left")
        self.pm_op_var = tk.StringVar()
        ttk.Entry(pm_bar, textvariable=self.pm_op_var, width=16).pack(side="left", padx=(4, 8))
        ttk.Label(pm_bar, text="params").pack(side="left")
        self.pm_params_var = tk.StringVar()
        ttk.Entry(pm_bar, textvariable=self.pm_params_var, width=38).pack(side="left", padx=(4, 8))
        ttk.Button(pm_bar, text="发送 pm.req", command=self._pm_send).pack(side="left")
        self.pm_note_var = tk.StringVar(value="")
        ttk.Label(pm_tab, textvariable=self.pm_note_var,
                  foreground="#555").pack(anchor="w", pady=(4, 0))
        ttk.Label(pm_tab, text="pm.res / pm.event 流水",
                  foreground="#8a8a8a").pack(anchor="w", pady=(6, 0))
        self.pm_log = scrolledtext.ScrolledText(pm_tab, font=("monospace 8"),
                                                state="disabled", wrap="word")
        self.pm_log.pack(fill="both", expand=True)

        # ---- 票板页签（TK-002：kanban by state——纯函数渲染、事件驱动重绘）----
        board_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(board_tab, text=" 票板 ")
        self.tickets_note_var = tk.StringVar(value="（PM 订阅受理后自动拉全量）")
        ttk.Label(board_tab, textvariable=self.tickets_note_var,
                  foreground="#555").pack(anchor="w")
        board_pane = ttk.Frame(board_tab)
        board_pane.pack(fill="both", expand=True)
        # 事件侧栏（右）：tickets 失效通知流水（无载荷，重拉由防抖合并）
        side = ttk.Frame(board_pane)
        side.pack(side="right", fill="y", padx=(6, 0))
        ttk.Label(side, text="票事件（最近 30 条）",
                  foreground="#8a8a8a").pack(anchor="w")
        self.ticket_event_log = scrolledtext.ScrolledText(
            side, font=("monospace 8"), state="disabled", wrap="none", width=34)
        self.ticket_event_log.pack(fill="both", expand=True)
        # kanban 列（左）：五态 + 「其他」兜底列（未知/空态不丢卡）
        self.tickets_cols: dict[str, scrolledtext.ScrolledText] = {}
        self.tickets_col_vars: dict[str, tk.StringVar] = {}
        for name in (*TICKET_STATES, "其他"):
            col = ttk.Frame(board_pane)
            col.pack(side="left", fill="both", expand=True, padx=(0, 4))
            var = tk.StringVar(value=f"{name} 0")
            ttk.Label(col, textvariable=var, foreground="#555").pack(anchor="w")
            txt = scrolledtext.ScrolledText(col, font=("monospace 8"),
                                            state="disabled", wrap="none",
                                            width=24)
            txt.pack(fill="both", expand=True)
            self.tickets_cols[name] = txt
            self.tickets_col_vars[name] = var

        # ---- 席位舰页签（TK-003：fleet 卡片墙——事件驱动刷新、租约倒计时、
        #      stale 高亮阈值可配、降级标记透出）----
        ship_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(ship_tab, text=" 席位舰 ")
        self.fleet_ship_note_var = tk.StringVar(
            value="（PM 订阅受理后自动拉全量）")
        ttk.Label(ship_tab, textvariable=self.fleet_ship_note_var,
                  foreground="#555").pack(anchor="w")
        ship_bar = ttk.Frame(ship_tab)
        ship_bar.pack(fill="x")
        self.fleet_ship_snap_var = tk.StringVar(value="舰快照 —")
        ttk.Label(ship_bar, textvariable=self.fleet_ship_snap_var,
                  foreground="#555").pack(side="left")
        ttk.Label(ship_bar, text="  stale阈值(s)",
                  foreground="#8a8a8a").pack(side="left")
        self.fleet_ship_stale_var = tk.StringVar(value=str(FLEET_VERIFY_STALE_S))
        ttk.Spinbox(ship_bar, from_=10, to=86400, width=7,
                    textvariable=self.fleet_ship_stale_var).pack(
            side="left", padx=(4, 0))
        self.fleet_ship_stale_var.trace_add(
            "write", lambda *_: self._render_fleet_ship())
        self.fleet_ship_text = scrolledtext.ScrolledText(
            ship_tab, font=("monospace 9"), state="disabled", wrap="none")
        self.fleet_ship_text.pack(fill="both", expand=True, pady=(4, 0))

        # ---- 轨迹页签（TK-004：turn 分组时间线——类型/工具过滤、文本搜索、
        #      seq 跳转、折叠展开；过滤折叠全在服务端，客户端纯渲染）----
        trace_tab = ttk.Frame(self.nb, padding=6)
        self.nb.add(trace_tab, text=" 轨迹 ")
        self.trace_sid_var = tk.StringVar(value="")
        self.trace_type_var = tk.StringVar(value="")
        self.trace_tool_var = tk.StringVar(value="")
        self.trace_text_var = tk.StringVar(value="")
        self.trace_from_var = tk.StringVar(value="")
        self.trace_to_var = tk.StringVar(value="")
        self.trace_seq_var = tk.StringVar(value="")
        trow = ttk.Frame(trace_tab)
        trow.pack(fill="x")
        for label, var, w in (("sessionId", self.trace_sid_var, 30),
                              ("type", self.trace_type_var, 12),
                              ("tool", self.trace_tool_var, 10),
                              ("text", self.trace_text_var, 14),
                              ("seqFrom", self.trace_from_var, 7),
                              ("seqTo", self.trace_to_var, 7)):
            ttk.Label(trow, text=label, foreground="#8a8a8a").pack(side="left")
            ttk.Entry(trow, textvariable=var, width=w).pack(
                side="left", padx=(2, 6))
        trow2 = ttk.Frame(trace_tab)
        trow2.pack(fill="x", pady=(4, 0))
        ttk.Button(trow2, text="拉取",
                   command=self._trace_fetch).pack(side="left")
        ttk.Button(trow2, text="展开折叠",
                   command=self._trace_expand).pack(side="left", padx=(6, 0))
        ttk.Label(trow2, text="seq跳转",
                  foreground="#8a8a8a").pack(side="left", padx=(12, 0))
        ttk.Entry(trow2, textvariable=self.trace_seq_var,
                  width=8).pack(side="left", padx=(2, 2))
        ttk.Button(trow2, text="跳", command=self._trace_jump).pack(side="left")
        self.trace_note_var = tk.StringVar(
            value="（填 sessionId 后拉取；参数组合即幂等键）")
        ttk.Label(trace_tab, textvariable=self.trace_note_var,
                  foreground="#555").pack(anchor="w", pady=(4, 0))
        self.trace_text = scrolledtext.ScrolledText(
            trace_tab, font=("monospace 8"), state="disabled", wrap="none")
        self.trace_text.pack(fill="both", expand=True)
        ttk.Label(trace_tab, text="折叠展开区",
                  foreground="#8a8a8a").pack(anchor="w", pady=(4, 0))
        self.trace_expand_text = scrolledtext.ScrolledText(
            trace_tab, font=("monospace 8"), state="disabled", wrap="none",
            height=8)
        self.trace_expand_text.pack(fill="both")
        for tag, fg in (("group", "#555555"), ("fold", "#8a3f3f"),
                        ("turn", "#b26a00"), ("tool", "#0a5bd3"),
                        ("step", "#666666"), ("stream", "#6a4fb2"),
                        ("meta", "#777777"), ("misc", "#222222")):
            self.trace_text.tag_configure(tag, foreground=fg)
            self.trace_expand_text.tag_configure(tag, foreground=fg)

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
                           lambda s: self.root.after(0, lambda: self.obs_state_var.set(f"观测:{s}")),
                           pm_kinds=PM_SUB_KINDS)
        self.obs_link = link
        link.start()
        self.obs_btn.config(text="停观测")
        # head 配置面：观测会话就绪即拉一次（req 泵等握手完成后发出）
        self._pending_send(head_list_request(f"head-list-{int(time.time() * 1000)}"),
                           "head.list")

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
                # 请求级错误按 req_id 关联回填（命中 pending 才动 UI）；
                # 无 req_id / 未命中只记编排页日志行——不碰 body.get 在途
                # 态、不写详情右栏（历史误投修复：任意 error 曾被投右栏）
                if f.get("code") in ("pm_sub_failed", "pm_sub_ended"):
                    # GW-002 上游事件流断/起流败（细分码仅旧字段携带）：
                    # 点亮降级横幅——ObserveLink 已在延迟自动重订
                    self._pm_degrade(str(f.get("code")),
                                     str(f.get("message") or f.get("msg") or ""))
                entry = self._pending_pop(f.get("req_id"))
                if entry is not None:
                    self._pending_fill(entry, pending_error_line(entry.get("kind", ""), f))
                else:
                    st_write(self.orch_log, f"[观测错误] {detail_error_line(f)}")
            elif t == "fleet.snapshot":
                self._render_fleet(f)
                self._on_fleet_ship_frame(f)
            elif t == "fleet.cleanup.result":
                self._pending_pop(f.get("req_id"))
                line = cleanup_result_line(f)
                self.fleet_note_var.set(line)
                self._fleet_event_add(f"fleet.cleanup:{f.get('req_id')}", line)
            elif t == "fleet.brief.result":
                self._pending_pop(f.get("req_id"))
                self._render_fleet_brief(f)
            elif t == "body.list_more.result":
                self._pending_pop(f.get("req_id"))
                self._on_list_more_result(f)
            elif t == "liaison.result":
                self._pending_pop(f.get("req_id"))
                self._on_liaison_result(f)
            elif t == "run.cancel.result":
                self._pending_pop(f.get("req_id"))
                self._on_run_cancel_result(f)
            elif t == "head.list.result":
                self._pending_pop(f.get("req_id"))
                self._render_heads(f)
            elif t == "whiteboard.set.result":
                self._pending_pop(f.get("req_id"))
                self.whiteboard_note_var.set(whiteboard_set_line(f))
            elif t == "head.switch.result":
                self._pending_pop(f.get("req_id"))
                line = head_switch_line(f)
                self.head_note_var.set(line)
                st_write(self.log, f"[head] {line}")
                if f.get("ok"):
                    self.head_var.set(str(f.get("active", "")))
            elif t == "bridge.msg":
                st_write(self.bridge_log, str(f.get("line", ""))[:300])
            elif t == "tickets.snapshot":
                st_set(self.tickets_log, tickets_text(f))
            elif t == "pm.res":
                self._on_pm_res(f)
            elif t == "pm.event":
                st_write(self.pm_log, pm_event_line(f))
                if f.get("kind") == "tickets":
                    self._on_tickets_event(f)
                elif f.get("kind") == "fleet":
                    self._on_fleet_ship_event(f)
            elif "_send_failed" in f:
                # req 泵发送失败（连接已断）：受影响 req_id 回填提示，不静默丢
                entry = self._pending_pop(f.get("_send_failed"))
                if entry is not None:
                    self._pending_fill(entry, pending_send_fail_line(entry.get("kind", "")))
                else:
                    st_write(self.orch_log, "[观测链路] 发送失败（链路断开）")
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
        """快照收帧：收帧时刻复位（陈旧判定源）；与上次快照 diff，消失的
        席位码进事件历史「席位 <code> 已移出」（首帧无基线不 diff）；
        全删重插刷新席位表（last_seen 列由 brief 回填）。"""
        rows = fleet_rows(frame)
        self._fleet_snap_ts = time.time()
        codes = {r[0] for r in rows}
        if self._fleet_codes is not None:
            for c in sorted(self._fleet_codes - codes):
                self._fleet_event_add(f"removed:{c}", f"席位 {c} 已移出")
        self._fleet_codes = codes
        self._fleet_rows_cache = rows
        self._render_fleet_tree()

    def _render_fleet_tree(self):
        """按缓存快照行重绘席位表：last_seen 取 brief 回填值，缺省「—」。"""
        self.fleet_tree.delete(*self.fleet_tree.get_children())
        for code, alias, node, role, status in self._fleet_rows_cache:
            self.fleet_tree.insert("", "end", values=(
                code, alias, node, role, status,
                self._fleet_last_seen.get(code, "—")))

    def _fleet_event_add(self, key: str, line: str):
        """席位事件历史入一条：同 key 重复到达不堆积（去重键=事件主体：
        ``removed:<code>`` / ``<kind>:<req_id>`` / ``brief`` / ``liaison``），
        只刷新到最新位；保留最近 FLEET_EVENT_KEEP 条后重渲染事件区。

        去重键按「事件主体」而非帧序设计：同 req_id 的重复回包、同席位的
        反复移出、反复拉的简报都归并为一条最新态，重放不放大历史。
        """
        self._fleet_events = [e for e in self._fleet_events if e["key"] != key]
        self._fleet_events.append({"key": key, "line": line})
        if len(self._fleet_events) > FLEET_EVENT_KEEP:
            self._fleet_events = self._fleet_events[-FLEET_EVENT_KEEP:]
        self._render_fleet_events()

    def _render_fleet_events(self):
        """事件区覆写渲染：最新条置顶并标 ▸。"""
        lines = [e["line"] for e in reversed(self._fleet_events)]
        if lines:
            lines[0] = "▸ " + lines[0]
        st_set(self.fleet_event_log, "\n".join(lines))

    def _fleet_stale_label(self, now: float | None = None) -> str:
        """席位页顶部快照标签：「快照 HH:MM:SS」；距收帧超 FLEET_STALE_S
        追加「（可能陈旧）」；从未收到快照 →「快照 —」。"""
        if self._fleet_snap_ts is None:
            return "快照 —"
        if now is None:
            now = time.time()
        when = time.strftime("%H:%M:%S", time.localtime(self._fleet_snap_ts))
        if now - self._fleet_snap_ts > FLEET_STALE_S:
            return f"快照 {when}（可能陈旧）"
        return f"快照 {when}"

    # ---- 在途控制请求登记（req_id → kind；错误/超时/发送失败按 kind 回填） ----

    def _pending_send(self, req: dict, kind: str, seq: int | None = None):
        """登记在途控制请求（req_id → kind+时刻+rid，body.get 另记 ref 与
        发起序号 seq）并经观测连接发出：回包/error/超时/发送失败都以
        req_id 关联回填；seq 供 body.item 按 ref 清在途时校验发起新旧。
        pm.* 帧无 req_id、幂等键叫 id——取 id 兜底。"""
        rid = req.get("req_id")
        if not isinstance(rid, str) or not rid:
            rid = req.get("id")
        if isinstance(rid, str) and rid:
            entry = {"kind": kind, "ts": time.monotonic(), "rid": rid}
            if kind == "body.get":
                entry["ref"] = str(req.get("ref") or "")
                entry["seq"] = seq if isinstance(seq, int) else 0
            self._pending[rid] = entry
        self.obs_link.send_request(req)

    def _pending_pop(self, req_id) -> dict | None:
        """按 req_id 清一条在途登记（无 req_id/未命中返 None，不动其他）。"""
        if not isinstance(req_id, str):
            return None
        return self._pending.pop(req_id, None)

    def _pending_clear_body(self, ref: str, before_seq: int | None = None):
        """body.item 回包按 ref 清在途（回包不带 req_id 的网关形）。

        误清防护：回包只清它对应的那次请求——``before_seq`` 已知（req_id
        命中的回包）时仅清发起序号 ≤ 它的同 ref 在途；未知（纯 ref 形）
        按 FIFO 只清最老一条。同 ref 更新请求的在途登记不被旧回包连带
        清掉（保住其超时/错误回填保护）。
        """
        matches = [(rid, e) for rid, e in self._pending.items()
                   if e.get("kind") == "body.get" and e.get("ref") == ref]
        if not matches:
            return
        if before_seq is None:
            matches = [min(matches, key=lambda p: p[1].get("seq") or 0)]
        else:
            matches = [(rid, e) for rid, e in matches
                       if (e.get("seq") or 0) <= before_seq]
        for rid, _e in matches:
            del self._pending[rid]

    def _pending_fill(self, entry: dict, line: str):
        """按 kind 把失败/超时行回填到发起时的提示位：席位/头部/白板/任务
        提示行各自覆盖；body.get 只在右栏仍显示该 ref 时回填；席位域
        （清理/简报/对接）的失败行同时进事件历史。"""
        kind = entry.get("kind")
        if kind == "body.get":
            if self._detail_pending == entry.get("ref"):
                self._detail_pending = None
                self._render_detail_body(line)
            return
        var = {"fleet.cleanup": self.fleet_note_var,
               "fleet.brief": self.fleet_note_var,
               "liaison.unbind": self.fleet_note_var,
               "liaison.bind": self.fleet_note_var,
               "head.switch": self.head_note_var,
               "head.list": self.head_note_var,
               "whiteboard.set": self.whiteboard_note_var,
               "body.list_more": self.task_note_var,
               "run.cancel": self.task_note_var,
               "pm.req": self.pm_note_var,
               "pm.tickets": self.pm_note_var,
               "pm.fleet": self.fleet_ship_note_var,
               "pm.trace": self.trace_note_var,
               "pm.trace.expand": self.trace_note_var}.get(kind)
        if var is not None:
            var.set(line)
        if kind in ("fleet.cleanup", "fleet.brief", "liaison.unbind", "liaison.bind"):
            self._fleet_event_add(f"{kind}:{entry.get('rid') or ''}", line)

    def _pending_sweep(self, now: float | None = None):
        """超 PENDING_TIMEOUT_S 的在途请求：清登记并回填「<中文名>超时
        无响应」（now 可注入，测试用；_tick 每帧扫）。"""
        if now is None:
            now = time.monotonic()
        for rid, entry in list(self._pending.items()):
            if now - entry.get("ts", now) <= PENDING_TIMEOUT_S:
                continue
            del self._pending[rid]
            self._pending_fill(entry, pending_timeout_line(entry.get("kind", "")))

    # ---- 席位控制面（fleet.cleanup 清理 / fleet.brief 简报，经观测连接出站） ----

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
        self._pending_send(cleanup_request(ids, mode, req_id), "fleet.cleanup")

    def _fleet_brief(self):
        """拉席位状态简报（fleet.brief）：只读动作无确认门，存活检查同
        清理面；回包 fleet.brief.result 渲染摘要行 + 简报面板逐行。"""
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            self.fleet_note_var.set("观测连接未开——开启「观测」后可拉取席位简报")
            return
        self._brief_seq += 1
        req_id = f"brief-{int(time.time() * 1000)}-{self._brief_seq}"
        self.fleet_note_var.set("已请求席位简报…")
        self._pending_send(fleet_brief_request(req_id), "fleet.brief")

    def _render_fleet_brief(self, frame: dict):
        """fleet.brief.result → 摘要行（总量/在跑数）+ 简报面板逐行追加；
        带顶层 liaison 字段时刷新席位页常显行（旧网关无字段不覆盖），
        变化时进事件历史；席位 last_seen 字段回填席位表列。"""
        if isinstance(frame, dict) and "liaison" in frame:
            line = liaison_line(frame.get("liaison"))
            if line != self.liaison_var.get():
                self.liaison_var.set(line)
                self._fleet_event_add("liaison", f"对接变化 → {line}")
        seats = frame.get("seats") if isinstance(frame, dict) else None
        if isinstance(seats, list):
            rows = [s for s in seats if isinstance(s, dict)]
            running = sum(1 for s in rows if s.get("running"))
            summary = f"{len(rows)} 席位 · {running} 在跑"
            self.fleet_note_var.set(summary)
            self._fleet_event_add("brief", summary)
            self._update_fleet_last_seen(rows)
        else:
            self.fleet_note_var.set("席位简报回包不可读")
        for line in fleet_brief_lines(frame):
            st_write(self.fleet_brief_log, line)

    def _update_fleet_last_seen(self, seats: list[dict]):
        """brief 席位 last_seen → 席位表列缓存（变化才重绘）。"""
        changed = False
        for seat in seats:
            code = str(seat.get("id", "") or "")
            if not code:
                continue
            label = last_seen_label(seat.get("last_seen"))
            if self._fleet_last_seen.get(code) != label:
                self._fleet_last_seen[code] = label
                changed = True
        if changed:
            self._render_fleet_tree()

    # ---- 白板（协作交互输入面：改动防抖自动同步，经观测连接出站）----

    def _wb_on_modified(self, _event=None):
        """文本区变更 → 清 <<Modified>> 标志 + 防抖重排一次自动同步
        （窗口内连续改动合并成一帧，推末版全文）。"""
        try:
            self.wb_text.edit_modified(False)
        except Exception:  # noqa: BLE001 — 关窗竞态下文本区已销毁
            return
        if self._wb_push_job is not None:
            self.root.after_cancel(self._wb_push_job)
        self._wb_push_job = self.root.after(
            WB_SYNC_DEBOUNCE_MS, self._whiteboard_push)

    def _whiteboard_push(self):
        """把白板页当前文本整段自动推进网关全局白板（无手动同步步）：
        空文本也推（清板语义）；观测未开只记提示不重试，重开观测后随
        下次改动补推。回包 whiteboard.set.result 渲染一行结果；推送面
        异常回填「白板同步失败」，不永停「同步中」。"""
        self._wb_push_job = None
        try:
            if not (self.obs_link and self.obs_link._thread
                    and self.obs_link._thread.is_alive()):
                self.whiteboard_note_var.set(
                    "观测连接未开——白板未同步（重开观测后改动即同步）")
                return
            text = self.wb_text.get("1.0", "end-1c")
            self._wb_seq += 1
            req_id = f"wb-{int(time.time() * 1000)}-{self._wb_seq}"
            self._pending_send(whiteboard_set_request(text, req_id), "whiteboard.set")
            self.whiteboard_note_var.set(f"白板同步中（{len(text)} 字）…")
        except Exception:  # noqa: BLE001 — 同步面异常不得永停「同步中」
            try:
                self.whiteboard_note_var.set("白板同步失败")
            except Exception:
                pass

    # ---- head 配置面（head.list/head.switch，经观测连接出站）----

    def _render_heads(self, frame: dict):
        """head.list.result → 单选钮行；单头模式只留说明不渲染钮。"""
        rows, active, file_backed = head_names_from_list(frame)
        for btn in self.head_buttons.values():
            btn.destroy()
        self.head_buttons.clear()
        if not file_backed:
            self.head_note_var.set("单头模式（无 profiles 文件）")
            self.head_var.set(active)
            return
        for name, label in rows:
            rb = self.ttk.Radiobutton(self.head_row, value=name,
                                      variable=self.head_var,
                                      text=label, command=lambda n=name: self._head_switch(n))
            rb.pack(side="left", padx=(6, 0))
            self.head_buttons[name] = rb
        self.head_var.set(active)
        self.head_note_var.set("切换对下一次语音连接生效")

    def _head_switch(self, name: str):
        """切激活 head：观测连接出站 head.switch；活会话不拆（单例语义）。"""
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            self.head_note_var.set("观测连接未开——开启「观测」后可切换 head")
            return
        self._head_seq += 1
        req_id = f"head-{int(time.time() * 1000)}-{self._head_seq}"
        self.head_note_var.set(f"切换 head → {name} …")
        self._pending_send(head_switch_request(name, req_id), "head.switch")

    # ---- 详情页签（KG 14 §2.4：表按 ref 去重，正文两级缓存+观测拉取） ----

    def _on_body_push(self, frame: dict):
        """body.push（单条/回放批）→ 入表去重 + 原始 ts 留档 + inline 缓存
        + 回合页灰行。"""
        rows = detail_rows_from_push(frame)
        if not rows:
            return
        self.detail_rows = merge_detail_rows(self.detail_rows, rows)
        self._detail_ts.update(detail_ts_map(frame))
        self._render_detail()
        inline = frame.get("inline")
        if isinstance(inline, str) and inline and frame.get("ref"):
            self.detail_inline[str(frame["ref"])] = inline
        ack = detail_ack_line(frame, self._notify_refs)
        if ack:
            st_write(self.turn_log, ack, tags=("dim",))

    def _on_body_item(self, frame: dict):
        """body.item（body.get 回包）→ 全文缓存；正选中该 ref 则回填右栏；
        清对应在途登记：req_id 命中按其发起序号校验清（不连带更新的同
        ref 在途）；纯 ref 形按 FIFO 清最老一条；带 req_id 但未命中
        （重复/迟到回包）不动其余在途（幂等 no-op）。"""
        ref = str(frame.get("ref", "") or "")
        text = str(frame.get("text", "") or "")
        if not ref:
            return
        rid = frame.get("req_id")
        popped = self._pending_pop(rid) if isinstance(rid, str) and rid else None
        if popped is not None:
            self._pending_clear_body(ref, before_seq=popped.get("seq"))
        elif not (isinstance(rid, str) and rid):
            self._pending_clear_body(ref)
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
        self._update_task_buttons()

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
        """行选中 → inline/已拉取缓存直渲染，否则经观测连接 body.get{ref}；
        同时按行状态刷新任务操作条按钮态。"""
        self._update_task_buttons()
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
        self._body_seq += 1
        self._pending_send({"t": "body.get", "ref": ref,
                            "req_id": f"body-{int(time.time() * 1000)}-{self._body_seq}"},
                           "body.get", seq=self._body_seq)

    # ---- 任务页操作条（历史翻页 body.list_more / 取消 run.cancel） ----

    def _oldest_detail_ts(self) -> float | None:
        """表内时间上最旧行的原始 ts（翻页 before_ts 游标）；无任何留档
        → None。"""
        return min(self._detail_ts.values()) if self._detail_ts else None

    def _row_status(self, ref: str) -> str:
        """按 ref 查表行 status 列值（无行/无值 → 空串）。"""
        si = DETAIL_COLS.index("status")
        for row in self.detail_rows:
            if row[DETAIL_REF_I] == ref:
                return str(row[si]) if len(row) > si else ""
        return ""

    def _update_task_buttons(self):
        """任务操作条按钮态：选中行已取消 → 取消键置灰（不重复发底层
        取消）；翻页到底 → 拉取更多置灰。"""
        ref = self._selected_detail_ref()
        self.task_cancel_btn.config(
            state="disabled" if ref and self._row_status(ref) == "cancelled"
            else "normal")
        self.task_more_btn.config(
            state="disabled" if self._list_more_eof else "normal")

    def _task_list_more(self):
        """历史翻页：以表内最旧行 ts 为游标发 body.list_more（limit 50），
        回包追加行按 ref 去重合并（同 ref+no 重复页原位同值、行不增）；
        eof 后按钮置灰、再点无请求发出。"""
        if self._list_more_eof:
            return
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            self.task_note_var.set("观测连接未开——开启「观测」后可拉取历史")
            return
        before_ts = self._oldest_detail_ts()
        if before_ts is None:
            self.task_note_var.set("暂无可翻页的台账行")
            return
        self._list_more_seq += 1
        req_id = f"more-{int(time.time() * 1000)}-{self._list_more_seq}"
        self.task_note_var.set("拉取历史中…")
        self._pending_send(body_list_more_request(before_ts, req_id),
                           "body.list_more")

    def _on_list_more_result(self, frame: dict):
        """body.list_more.result → 追加行（ref 去重合并，行集对同一
        before_ts 幂等）+ eof 置灰。"""
        rows = detail_rows_from_push(frame)
        if rows:
            self.detail_rows = merge_detail_rows(self.detail_rows, rows)
            self._detail_ts.update(detail_ts_map(frame))
            self._render_detail()
        if isinstance(frame, dict) and frame.get("eof"):
            self._list_more_eof = True
            self.task_note_var.set("历史已到底")
        else:
            self.task_note_var.set(f"追加 {len(rows)} 行")
        self._update_task_buttons()

    def _task_cancel(self):
        """取消选中任务（确认门）→ run.cancel{ref}；已取消行置灰早退
        （不重复发底层取消）；回包 ok:false 显示 error 原文。"""
        ref = self._selected_detail_ref()
        if not ref:
            self.task_note_var.set("未选中任务行")
            return
        if self._row_status(ref) == "cancelled":
            self.task_note_var.set(f"{ref} 已取消")
            return
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            self.task_note_var.set("观测连接未开——开启「观测」后可取消任务")
            return
        from tkinter import messagebox

        if not messagebox.askyesno(
                "取消任务确认",
                f"取消任务 {ref}？\n\n正在运行的编排任务将被终止，"
                "台账行状态转为 cancelled。", parent=self.root, icon="warning"):
            return
        self._cancel_seq += 1
        req_id = f"cancel-{int(time.time() * 1000)}-{self._cancel_seq}"
        self.task_note_var.set(f"取消请求已发（{ref}）…")
        self._pending_send(run_cancel_request(ref, req_id), "run.cancel")

    def _on_run_cancel_result(self, frame: dict):
        """run.cancel.result → 提示行；ok 时该行状态转 result.state（缺省
        cancelled）并重绘（取消键随选中态置灰）。"""
        line = run_cancel_result_line(frame)
        self.task_note_var.set(line)
        if isinstance(frame, dict) and frame.get("ok"):
            ref = str(frame.get("ref") or "")
            si = DETAIL_COLS.index("status")
            for i, row in enumerate(self.detail_rows):
                if row[DETAIL_REF_I] == ref:
                    state = str(frame.get("state") or "cancelled")
                    self.detail_rows[i] = row[:si] + (state,) + row[si + 1:]
                    break
            self._render_detail()
        self._update_task_buttons()

    # ---- liaison 运维面（unbind/bind，经观测连接出站，确认门） ----

    def _liaison_unbind(self):
        """解绑对接（确认门）→ liaison.unbind；回包 liaison.result 回填
        事件区（未绑定时 was_bound=false →「本就无绑定」不算错误）。"""
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            self.fleet_note_var.set("观测连接未开——开启「观测」后可操作对接")
            return
        from tkinter import messagebox

        if not messagebox.askyesno(
                "解绑对接确认",
                "解除当前对接（liaison）绑定？\n\n解除后下个任务派发"
                "将重新拉取对接席位。", parent=self.root, icon="question"):
            return
        self._liaison_seq += 1
        req_id = f"liaison-u-{int(time.time() * 1000)}-{self._liaison_seq}"
        self._pending_send(liaison_unbind_request(req_id), "liaison.unbind")

    def _liaison_bind_selected(self):
        """绑定到选中席位（确认门，单选语义取首个选中）→ liaison.bind
        {code}；回包 liaison.result 回填事件区（ok:false 显示 error 原文）。"""
        sel = self.fleet_tree.selection()
        if not sel:
            self.fleet_note_var.set("未选中席位")
            return
        code = str(self.fleet_tree.item(sel[0], "values")[0])
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            self.fleet_note_var.set("观测连接未开——开启「观测」后可操作对接")
            return
        from tkinter import messagebox

        if not messagebox.askyesno(
                "绑定对接确认",
                f"把对接（liaison）绑定到席位 {code}？\n\n"
                "此后任务派发优先派给该席位。", parent=self.root, icon="question"):
            return
        self._liaison_seq += 1
        req_id = f"liaison-b-{int(time.time() * 1000)}-{self._liaison_seq}"
        self._pending_send(liaison_bind_request(code, req_id), "liaison.bind")

    def _on_liaison_result(self, frame: dict):
        """liaison.result → 事件区一行 + 常显行按 op 同步（bind 带
        liaison 字段；unbind ok 即无绑定）。"""
        line = liaison_result_line(frame)
        self.fleet_note_var.set(line)
        self._fleet_event_add(f"liaison:{frame.get('req_id')}", line)
        if isinstance(frame, dict) and frame.get("ok"):
            if str(frame.get("op")) == "unbind":
                self.liaison_var.set("对接席位：无绑定")
            elif isinstance(frame.get("liaison"), dict):
                self.liaison_var.set(liaison_line(frame.get("liaison")))

    # ---- PM 页（TK-001：透传控制台 + 降级横幅 + 回包渲染；ADR-004 零业务） ----

    def _pm_send(self):
        """控制台透传 ``pm.req{op, params}``：op/params 原样交网关机械路由
        （ADR-002 只读 GET 由网关侧保证），客户端零业务；id 自增、在途
        登记——回包 pm.res / error / 超时 / 发送失败均按 id 回填本页。"""
        op = self.pm_op_var.get().strip()
        if not op:
            self.pm_note_var.set("op 不能为空")
            return
        raw = self.pm_params_var.get().strip() or "{}"
        try:
            params = json.loads(raw)
        except ValueError:
            self.pm_note_var.set("params 不是合法 JSON")
            return
        if not isinstance(params, dict):
            self.pm_note_var.set("params 必须是 JSON 对象")
            return
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            self.pm_note_var.set("观测连接未开——开启「观测」后可发 PM 请求")
            return
        self._pm_seq += 1
        rid = f"pmq-{int(time.time() * 1000)}-{self._pm_seq}"
        self.pm_note_var.set(f"已发 pm.req {op} …")
        self._pending_send({"t": "pm.req", "id": rid, "op": op,
                            "params": params}, "pm.req")

    def _on_pm_res(self, frame: dict):
        """pm.res 分发：error 带 pm_* 码 → 降级横幅；pm.sub 受理形（带
        subscribed 且非退订回包）→ 横幅恢复；每帧出流水行；控制台请求
        按 id 清在途回填。"""
        entry = self._pending_pop(frame.get("id"))
        code, msg = pm_error_of(frame)
        if code:
            if pm_is_degraded(code):
                self._pm_degrade(code, msg)
            if entry is not None:
                self._pending_fill(entry, f"PM 请求失败：{code} {msg}".rstrip())
                if entry.get("kind") == "pm.fleet":
                    # 席位舰页签内的降级透出（死服/断流时 tab 不静默）
                    self.fleet_ship_note_var.set(
                        f"⚠ 降级 {code} {str(msg)[:60]}")
                elif entry.get("kind") == "pm.trace.expand":
                    # 轨迹展开失败也不静默：展开区直接给失败行
                    st_set(self.trace_expand_text,
                           f"⚠ 展开失败：{code} {str(msg)[:60]}")
        else:
            data = frame.get("data")
            if isinstance(data, dict) and isinstance(data.get("tickets"), list):
                # op=tickets 全量回包：票板数据面（台面即显示，不再泼流水）
                self._on_tickets_snapshot(data)
                if entry is not None:
                    self._pending_fill(entry, "票板全量已更新")
                st_write(self.pm_log,
                         f"✓ pm.res[{frame.get('id')}] "
                         f"票板全量 {data.get('count', '?')} 票")
                return
            if isinstance(data, dict) and isinstance(data.get("seats"), list):
                # op=fleet 全量回包：席位舰数据面（台面即显示，不再泼流水）
                self._on_fleet_ship_snapshot(data)
                if entry is not None:
                    self._pending_fill(entry, "席位舰全量已更新")
                st_write(self.pm_log,
                         f"✓ pm.res[{frame.get('id')}] "
                         f"席位舰全量 {data.get('count', '?')} 席")
                return
            if isinstance(data, dict) and data.get("op") == "trace" \
                    and isinstance(data.get("entries"), list):
                # op=trace 回包：轨迹数据面（过滤/折叠已在服务端，客户端纯渲染）
                expand = (entry is not None
                          and entry.get("kind") == "pm.trace.expand")
                self._on_trace_snapshot(data, expand=expand)
                if entry is not None:
                    self._pending_fill(entry, "轨迹已更新")
                st_write(self.pm_log,
                         f"✓ pm.res[{frame.get('id')}] 轨迹 "
                         f"{(data.get('matched') or {}).get('entries', '?')} 条")
                return
            if isinstance(data, dict) and "subscribed" in data \
                    and "was_subscribed" not in data:
                self._pm_restore(data["subscribed"])    # pm.sub 受理
            if entry is not None:
                self._pending_fill(entry, "PM 请求完成")
        st_write(self.pm_log, pm_res_line(frame))

    def _pm_degrade(self, code: str, msg: str):
        """降级横幅：PM 底座故障可见化（不崩 UI、不断链）——熄灭只等
        pm.res{subscribed}（自动重订/恢复后的订阅受理）。"""
        self.pm_banner_var.set(pm_banner_degraded_line(code, msg))
        self.pm_banner.config(bg="#8a3f3f", fg="white")
        st_write(self.pm_log, f"[降级] {code} {msg}".rstrip())

    def _pm_restore(self, subscribed):
        """订阅受理 → 横幅恢复常态；含 tickets kind 时播种一次票板全量
        （首订播种/重订补拉共用，防抖定时器幂等）。"""
        self.pm_banner_var.set(pm_banner_ok_line(subscribed))
        self.pm_banner.config(bg="#1f6f43", fg="white")
        if (self._tickets_fetch_job is None
                and "tickets" in [str(k) for k in subscribed]):
            self._tickets_fetch_job = self.root.after(
                TICKETS_REFETCH_DEBOUNCE_MS, self._tickets_fetch)
        if (self._fleet_ship_fetch_job is None
                and "fleet" in [str(k) for k in subscribed]):
            self._fleet_ship_fetch_job = self.root.after(
                FLEET_REFETCH_DEBOUNCE_MS, self._fleet_ship_fetch)

    # ---- 票板（TK-002：失效通知 → 防抖全量重拉 → 纯函数重绘） ----

    def _on_tickets_event(self, frame: dict):
        """tickets 事件（服务侧只发作失效通知，无载荷）→ 侧栏一行 +
        防抖合并一次全量重拉（窗内连发只花一次 RTT）。"""
        self._ticket_events.append(pm_event_line(frame))
        if len(self._ticket_events) > 50:
            self._ticket_events = self._ticket_events[-50:]
        st_set(self.ticket_event_log, "\n".join(self._ticket_events[-30:]))
        if self._tickets_fetch_job is None:
            self._tickets_fetch_job = self.root.after(
                TICKETS_REFETCH_DEBOUNCE_MS, self._tickets_fetch)

    def _tickets_fetch(self):
        """全量重拉 op=tickets（观测链路在才拉；离线期事件丢弃——后续
        事件/重订自会再补）。"""
        self._tickets_fetch_job = None
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            return
        self._pmt_seq += 1
        self._pending_send(
            {"t": "pm.req", "op": "tickets", "params": {},
             "id": f"pmt-{int(time.time() * 1000)}-{self._pmt_seq}"},
            "pm.tickets")

    def _on_tickets_snapshot(self, data: dict):
        """全量回包 → state 整体替换 + signature 留档（重放一致门参照）
        → 重绘 + 状态行。"""
        self._pm_tickets = tickets_normalize(data.get("tickets"))
        self._pm_tickets_sig = data.get("signature")
        self._render_tickets()
        degraded = "⚠ " if data.get("degraded") else ""
        self.tickets_note_var.set(
            f"{degraded}票板 {data.get('count', len(self._pm_tickets))} 票 · "
            f"全量 {data.get('cache') or '拉取'} · "
            f"{time.strftime('%H:%M:%S')}")

    def _render_tickets(self):
        """票板重绘：state → view 纯函数，逐列覆写 + 列头计数。"""
        view = tickets_kanban_view(self._pm_tickets)
        counts: dict[str, int] = {}
        for t in self._pm_tickets.values():
            col = tickets_column_of(t)
            counts[col] = counts.get(col, 0) + 1
        for name, widget in self.tickets_cols.items():
            st_set(widget, "\n".join(view.get(name, [])))
            self.tickets_col_vars[name].set(f"{name} {counts.get(name, 0)}")

    # ---- 席位舰（TK-003：fleet 事件/尾随快照 → 卡片墙纯函数重绘） ----

    def _on_fleet_ship_frame(self, frame: dict):
        """尾随 fleet.snapshot（fleet.json 全文，带租约/准入字段）→ 舰页
        state 替换重绘（增量面第一源：touch fleet.json 即达）。"""
        self._pm_fleet_cards = fleet_ship_cards_doc(frame.get("fleet"))
        self._fleet_ship_snap_ts = time.time()
        self._render_fleet_ship()

    def _on_fleet_ship_event(self, frame: dict):
        """fleet 事件（失效通知，无载荷）→ 防抖合并一次 op=fleet 全量重拉。"""
        if self._fleet_ship_fetch_job is None:
            self._fleet_ship_fetch_job = self.root.after(
                FLEET_REFETCH_DEBOUNCE_MS, self._fleet_ship_fetch)

    def _fleet_ship_fetch(self):
        """全量重拉 op=fleet（观测链路在才拉；离线期事件丢弃）。"""
        self._fleet_ship_fetch_job = None
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            return
        self._fleet_ship_seq += 1
        self._pending_send(
            {"t": "pm.req", "op": "fleet", "params": {},
             "id": f"pmf-{int(time.time() * 1000)}-{self._fleet_ship_seq}"},
            "pm.fleet")

    def _on_fleet_ship_snapshot(self, data: dict):
        """op=fleet 全量回包 → 投影 seats 共一卡模替换 + 状态行（降级透出：
        degraded 标记与 sessionJoined 失败的「纯 fleet 视图」注记）。"""
        self._pm_fleet_cards = fleet_ship_cards_seats(data.get("seats"))
        self._fleet_ship_snap_ts = time.time()
        self._render_fleet_ship()
        degraded = "⚠ " if data.get("degraded") else ""
        self.fleet_ship_note_var.set(
            f"{degraded}席位舰 {data.get('count', len(self._pm_fleet_cards))} 席 · "
            f"全量 {time.strftime('%H:%M:%S')}"
            + ("" if data.get("sessionJoined") else " · 纯 fleet 视图"))

    def _render_fleet_ship(self, now: float | None = None):
        """舰页重绘：state→view 纯函数覆写；stale 阈值即时读页签可配值。"""
        try:
            stale_s = float(self.fleet_ship_stale_var.get())
        except (TypeError, ValueError):
            stale_s = FLEET_VERIFY_STALE_S
        now = time.time() if now is None else now
        st_set(self.fleet_ship_text, "\n".join(
            fleet_ship_view(self._pm_fleet_cards, now, stale_s)))

    def _fleet_ship_stale_label(self, now: float | None = None) -> str:
        """舰页顶部快照标签；距收帧超 FLEET_STALE_S 追加「（断流陈旧）」
        ——断流时页签内的降级指示。"""
        if self._fleet_ship_snap_ts is None:
            return "舰快照 —"
        if now is None:
            now = time.time()
        when = time.strftime("%H:%M:%S",
                             time.localtime(self._fleet_ship_snap_ts))
        if now - self._fleet_ship_snap_ts > FLEET_STALE_S:
            return f"舰快照 {when}（断流陈旧）"
        return f"舰快照 {when}"

    # ---- 轨迹视图（TK-004：op=trace 拉取 → turn 分组时间线纯渲染） ----

    def _trace_fetch(self, params: dict | None = None, target: str = "main"):
        """op=trace 拉取（params 缺省由 UI 过滤行组装；直给用于展开）。
        观测链路不在时只提示不拉（离线期请求丢弃）。"""
        if not (self.obs_link and self.obs_link._thread
                and self.obs_link._thread.is_alive()):
            self.trace_note_var.set("（观测链路未开）")
            return
        if params is None:
            params = trace_query_params(
                self.trace_sid_var.get().strip(), self.trace_type_var.get(),
                self.trace_tool_var.get(), self.trace_text_var.get(),
                self.trace_from_var.get(), self.trace_to_var.get())
        if not params.get("sessionId"):
            self.trace_note_var.set("（先填 sessionId）")
            return
        self._pmt_seq += 1
        kind = "pm.trace" if target == "main" else "pm.trace.expand"
        self._pending_send(
            {"t": "pm.req", "op": "trace", "params": params,
             "id": f"pmt-{int(time.time() * 1000)}-{self._pmt_seq}"}, kind)

    def _trace_jump(self):
        """seq 跳转：seqFrom=seqTo=输入 seq 的窄窗拉取（历史不可变窗）。"""
        seq = self.trace_seq_var.get().strip()
        if not seq:
            self.trace_note_var.set("（先填要跳的 seq）")
            return
        self.trace_from_var.set(seq)
        self.trace_to_var.set(seq)
        self._trace_fetch()

    def _trace_expand(self):
        """展开折叠：最近快照 → 续查参数（被折叠头部窗口）拉到展开区。

        快照无保留区边界（整段被折叠、只剩摘要行）时不静默：展开区直接
        给出可见的「无法展开」行（边界缺失也是结论）。"""
        params = trace_expand_params(self._trace_last)
        if params is None:
            st_set(self.trace_expand_text,
                   "⊘ 无法展开：折叠快照无保留区边界（全部被折叠）")
            self.trace_note_var.set("（无已折叠快照可展开——见展开区说明）")
            return
        self._trace_fetch(params, target="expand")

    def _on_trace_snapshot(self, data: dict, expand: bool = False):
        """快照 → 主区留档（展开参照）+ 纯渲染 + 状态行（幂等键回显）。"""
        if not expand:
            self._trace_last = data
            self._render_trace(data)
        else:
            self._render_trace(data, widget=self.trace_expand_text)
        m = data.get("matched") or {}
        self.trace_note_var.set(
            f"轨迹 {m.get('entries', '?')} 条 · payload "
            f"{m.get('payload_chars', '?')}/{TRACE_BUDGET_CHARS} 字符 · "
            f"key {trace_params_key(data.get('filter') or {})[:40]}…"
            + (" · 已折叠（可展开）" if data.get("folded") else ""))

    def _render_trace(self, data: dict, widget=None):
        """轨迹渲染：trace_view 纯函数输出逐行插 tag（类型相着色）；
        空投影不静默，给可见「（空）」行。"""
        widget = widget or self.trace_text
        view = trace_view(data.get("entries")) or [("group", "（空）")]
        widget.config(state="normal")
        widget.delete("1.0", "end")
        for tag, line in view:
            widget.insert("end", line + "\n", tag)
        widget.config(state="disabled")

    def _selftest_probe(self):
        """selftest 冒烟（无显示环境 CI）：假帧走真实渲染路径——回放批入表、
        notify 相记 ref、单条 push 入表+灰行、body.item 回填缓存；席位快照
        入表、bridge 行入编排页、tickets 覆写、清理回包摘要行、简报摘要+
        逐行；对接解绑/历史翻页 eof/取消回包各走一条；PM 订阅受理→横幅
        恢复、事件流水两行、pm_down→降级横幅；票板全量入 kanban（含
        未知态兜底列）、tickets 事件进侧栏；席位舰全文卡（租约/准入/换代
        瞬态）、投影 seats 卡、fleet 事件防抖；轨迹折叠快照入 turn 分组
        时间线。不触网。"""
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
        self.obs_q.put({"t": "fleet.brief.result", "req_id": "self-brief-1",
                        "seats": [
                            {"id": "20d0", "node": "voice-head",
                             "role": "orchestrator", "status": "active",
                             "live": True, "running": True,
                             "title": "selftest 标题", "task": "running",
                             "idle_s": 65},
                            {"id": "9b95", "node": "voice-head", "role": "liaison",
                             "status": "released", "live": False,
                             "running": False, "title": "", "task": "",
                             "idle_s": None},
                        ]})
        self.obs_q.put({"t": "liaison.result", "req_id": "self-liaison-1",
                        "op": "unbind", "ok": True, "was_bound": False})
        self.obs_q.put({"t": "body.list_more.result", "req_id": "self-more-1",
                        "items": [], "eof": True})
        self.obs_q.put({"t": "run.cancel.result", "req_id": "self-cancel-1",
                        "ref": "vh-self2", "ok": True, "state": "cancelled"})
        # PM 面（TK-001）：订阅受理→横幅恢复、快照/增量事件两行、
        # pm_down 回包→降级横幅（回包不可读形也走一条）
        self.obs_q.put({"t": "pm.res", "id": "self-pm-sub-1",
                        "data": {"subscribed": ["fleet", "tickets"]}})
        self.obs_q.put({"t": "pm.event", "seq": 1, "msgid": "self-m1",
                        "source": "fleet", "kind": "fleet",
                        "path": "/dsh/maestro/fleet.json", "replay": True})
        self.obs_q.put({"t": "pm.event", "seq": 2, "msgid": "self-m2",
                        "source": "tickets", "kind": "tickets",
                        "path": "", "replay": False})
        self.obs_q.put({"t": "pm.res", "id": "self-pmq-1",
                        "error": {"code": "pm_down", "message": "health probe failed"}})
        # 票板（TK-002）：全量入 kanban（含未知态兜底列）+ 事件进侧栏
        self.obs_q.put({"t": "pm.res", "id": "self-pmt-1", "data": {
            "op": "tickets", "count": 3, "cache": "hit", "degraded": False,
            "signature": "self-sig-1",
            "tickets": [
                {"ticket_id": "T-A", "title": "进行中票", "state": "running",
                 "deps": "[\"T-B\"]", "lease_owner": "w-self",
                 "refs": "{\"evidence\": \"docs/a.md\"}", "outcome": None,
                 "updated_at": "2026-08-29T10:00:00+00:00"},
                {"ticket_id": "T-B", "title": "已合并票", "state": "merged",
                 "deps": "[]", "lease_owner": None, "refs": "{}",
                 "outcome": "已收口", "updated_at": "2026-08-29T09:00:00+00:00"},
                {"ticket_id": "T-C", "title": "怪状态兜底票", "state": "weird",
                 "deps": "[]", "lease_owner": None, "refs": "{}",
                 "outcome": None, "updated_at": "2026-08-29T08:00:00+00:00"},
            ]}})
        self.obs_q.put({"t": "pm.event", "seq": 3, "msgid": "tickets:ledger.db:self",
                        "source": "ledger", "kind": "tickets",
                        "path": "maestro/ledger.db", "replay": False})
        # 席位舰（TK-003）：全文卡（租约/准入/换代瞬态）+ 投影 seats 卡
        # + fleet 事件（防抖重拉；selftest 无观测链路，拉取静默跳过）
        now_iso = datetime.now().astimezone().isoformat(timespec="seconds")
        fut_iso = (datetime.now().astimezone()
                   + timedelta(minutes=5)).isoformat(timespec="seconds")
        self.obs_q.put({"t": "fleet.snapshot", "fleet": {
            "0699": {"sessionId": "s-0699", "role": "worker", "node": "gw-002",
                     "preset": "maestro", "spawnedAt": now_iso,
                     "status": "active", "owner": "<orchestrator>",
                     "leaseExpiresAt": fut_iso},
            "t9ab": {"kind": "orca-terminal", "handle": "t-42",
                     "status": "probing", "alias": "dev",
                     "lastSeenAt": now_iso},
            "t0cd": {"kind": "orca-terminal", "handle": "t-7",
                     "status": "verified", "alias": "orch",
                     "lastSeenAt": now_iso, "retiring": True}}})
        self.obs_q.put({"t": "pm.res", "id": "self-pmf-1", "data": {
            "op": "fleet", "count": 1, "degraded": False,
            "sessionJoined": False, "note": "",
            "seats": [{"code": "0699", "sessionId": "s-0699",
                       "role": "worker", "node": "gw-002",
                       "preset": "maestro", "spawnedAt": now_iso,
                       "status": "active",
                       "session": {"running": True, "blank": False,
                                   "agentPreset": "maestro", "cwd": "/tmp",
                                   "title": "t"}}]}})
        self.obs_q.put({"t": "pm.event", "seq": 4, "msgid": "fleet:touch:self",
                        "source": "fleet", "kind": "fleet",
                        "path": "maestro/fleet.json", "replay": False})
        # 轨迹（TK-004）：折叠快照入时间线（折叠摘要行 + turn 分组相着色）
        self.obs_q.put({"t": "pm.res", "id": "self-pmtt-1", "data": {
            "op": "trace", "sessionId": "s-self", "signature": "self-ts-1",
            "totalLines": 5, "parseFailures": 0, "logTruncated": False,
            "filter": {"type": None, "tool": None, "text": None,
                       "seqFrom": None, "seqTo": None},
            "matched": {"entries": 3, "chars": 900, "payload_chars": 900,
                        "seq_range": [1, 6], "type_histogram": {}},
            "folded": True, "budget": TRACE_BUDGET_CHARS,
            "dropped": {"entries": 1, "chars": 600},
            "entries": [
                {"type": "trace.compact", "reason": "threshold",
                 "threshold": TRACE_BUDGET_CHARS,
                 "dropped": {"entries": 1, "chars": 600},
                 "kept": {"entries": 2, "chars": 300},
                 "seq_range": [1, 6]},
                {"type": "turn/start", "seq": 5, "time": 0,
                 "data": {"turn": 1}},
                {"type": "tool/call", "seq": 6, "time": 0,
                 "data": {"turn": 1, "name": "bash", "command": "echo hi"}},
            ]}})

    def end_session(self):
        """优雅收尾需要一帧 session.end——经由 tx 旁路不便，此处仅断链。

        gateway 对非优雅断开保留 resume 槽（重连带 session_id 续接），
      真正丢弃由服务端超时回收。"""
        self.link.close(graceful=False)
        self._set_state("已断开（可重连续接）")

    # ---- 采集（按住说话默认；锁定=连续采集，静音语义：WS 保持） ----

    def _ptt_press(self):
        self._cancel_vad_tail()       # 再按：新语音前不垫剩余尾静音
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
        self.latest = np.zeros(BLOCK, dtype=np.int16)   # 频谱回基线，不留末帧残影

    def _send_vad_tail(self):
        """松键后按 vad_tail_plan 步进补尾静音：服务端 VAD 判句尾需 >700ms
        静音（实测 700ms 不判、1000ms 判），且一次性倾倒大帧会撞网关
        64KB/s 限速断链——按真实码率投递两头都安全。"""
        if self.state_var.get() != "open":
            return
        self._cancel_vad_tail()
        plan = iter(vad_tail_plan())

        def _step():
            item = next(plan, None)
            if item is None:
                self._tail_after = None
                return
            try:
                self.tx.put_nowait(b"\x00" * item[1])
            except queue.Full:
                pass  # 背压：丢本块（与采集回调同语义）
            except Exception as e:  # noqa: BLE001 — 单步失败只跳过，链走完
                try:
                    self.log_write(f"[UI] 尾静音步进异常已跳过: {type(e).__name__}: {e}")
                except Exception:
                    pass
            self._tail_after = self.root.after(VAD_TAIL_STEP_MS, _step)

        self._tail_after = self.root.after(VAD_TAIL_STEP_MS, _step)

    def _cancel_vad_tail(self):
        if self._tail_after is not None:
            try:
                self.root.after_cancel(self._tail_after)
            except Exception:  # noqa: BLE001 — 定时器已失效
                pass
            self._tail_after = None

    def _on_audio(self, data, frames, time_info, status):
        self.latest = data.copy()
        try:
            self.tx.put_nowait(bytes(data))
        except queue.Full:
            pass  # 背压：丢最新块（与 gateway 忙时丢最老互补）

    # ---- UI 泵 ----

    def _tick(self, now: float | None = None):
        try:
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
            self._pending_sweep()
            # 席位快照陈旧判定（now 可注入，测试用；缺省墙钟）
            self.fleet_snap_var.set(
                self._fleet_stale_label(time.time() if now is None else now))
            self.fleet_ship_snap_var.set(
                self._fleet_ship_stale_label(
                    time.time() if now is None else now))
        except Exception as e:  # noqa: BLE001 — 单帧渲染失败只丢一帧，不杀 UI 泵
            try:
                self.log_write(f"[UI] 渲染异常已跳过: {type(e).__name__}: {e}")
            except Exception:
                pass
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
        save_session_config(self.url_var.get().strip())  # 会话配置（可丢，尽力写）
        if self._tickets_fetch_job is not None:
            try:
                self.root.after_cancel(self._tickets_fetch_job)
            except Exception:  # noqa: BLE001 — 定时器已失效
                pass
            self._tickets_fetch_job = None
        if self._fleet_ship_fetch_job is not None:
            try:
                self.root.after_cancel(self._fleet_ship_fetch_job)
            except Exception:  # noqa: BLE001 — 定时器已失效
                pass
            self._fleet_ship_fetch_job = None
        self._cancel_vad_tail()
        if self._wb_push_job is not None:
            try:
                self.root.after_cancel(self._wb_push_job)
            except Exception:  # noqa: BLE001 — 定时器已失效
                pass
            self._wb_push_job = None
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
    ap.add_argument("--url", default=None,
                    help="网关地址（缺省读会话配置 JSON，再回落 "
                         f"{DEFAULT_GATEWAY_URL}）")
    ap.add_argument("--token", default="")
    ap.add_argument("--selftest", action="store_true",
                    help="无设备冒烟：UI 起即退（CI/无显示环境验证布局）")
    ap.add_argument("--ptt-key", default="f9",
                    help="按住说话热键（pynput 键名，默认 f9；全局生效）")
    args = ap.parse_args()

    # 网关地址三级回落：CLI > 会话配置 JSON（可丢）> 默认
    url = args.url or load_session_config().get("url") or DEFAULT_GATEWAY_URL

    import tkinter as tk

    root = tk.Tk()
    try:
        from tkinter import ttk  # noqa: F401 — 触发主题可用性早失败
        ttk.Style(root).theme_use("clam")
    except Exception:
        pass
    App(root, url, args.token, args.selftest, ptt_key=args.ptt_key)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
