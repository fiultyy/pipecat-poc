#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""rt_gateway: 本地局域网 WS 语音网关（M4/WS4；docs/kg/04-ws4-gateway.md §0–§4）.

单进程 aiohttp（不依赖未装 extras——pipecat 仅在 CLI live 模式惰性导入）:

- ``VoiceGateway`` — web server：``/ws`` + ``/healthz`` + ``GET /`` 静态页
- ``WsSession``     — 每连接一个；control/media/event 三路复用（§1）
- ``TailReader``    — dais read-worker 增量尾读协程 → orch.progress（§2）
- ``FileTailer``    — maestro 侧文件观测源 → topic.*（KG 11 §2：fleet 快照 /
  inbox 行尾 / tickets 全文快照）
- ``TurnTrace``     — head pipeline 帧流 → head.turn 事件（KG 11 §1；纯逻辑，
  帧类注入，离线可测）

帧协议（§1）：JSON 文本帧（control/event）+ 二进制帧（raw PCM16LE/16k/mono，
推荐 20ms=640B）。握手序：``auth`` → ``auth.ok{session_id}`` →
``session.start{session_id?}``（旧 id = take_tail 重播种，§4）→ 媒体/事件双工。

安全基线（§4）：token 鉴权（``VOICE_GATEWAY_TOKEN``）、单 token 并发会话≤2、
media 码率上限（>64KB/s 断开）、音频不落盘、transcript 仅内存会话级。
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

from aiohttp import WSMsgType, web

from rt_dsh_lane import DaisLane, DaisLaneError

from rt_event_bus import EventBus
from rt_transcript import TranscriptEntry, TranscriptState

log = logging.getLogger("rt_gateway")

# ---- 协议/基线常量（KG 04 §1/§3/§4）----
PCM_CHUNK_BYTES = 640              # 20ms @ PCM16LE/16k/mono
PCM_RATE_LIMIT_BYTES = 64 * 1024   # >64KB/s 持续 → 断开
RATE_WINDOW_S = 1.0
SEND_QUEUE_LIMIT = 100             # 慢客户端：send queue >100 → 合并 progress
PROGRESS_KIND = "orch.progress"
AUDIO_QUEUE_MAX = 16               # ~320ms；忙时丢最老块（实时性>完整性）
MAX_SESSIONS_PER_TOKEN = 2

# orch.* 事件全集（KG 04 §3；显式订阅而非 wildcard——EventBus 的无 kinds
# 订阅会把同一 entry 双挂 "*" 造成双投递，见 VO-011 报告"发现"节）
ORCH_KINDS = (
    "orch.dispatch",
    "orch.ack",
    "orch.progress",
    "orch.gate",
    "orch.done",
    "orch.metrics",
)
# topic.* 观测事件全集（KG 11 §1；console P1 数据平面）
TOPIC_KINDS = (
    "head.turn",         # head 回合生命周期（TurnTrace，KG 11 §1 phase 表）
    "fleet.snapshot",    # ~/.dsh/maestro/fleet.json 字节变更 → 全量 JSON 快照
    "bridge.msg",        # ~/.dsh/maestro/bridge/inbox.log 增量行
    "tickets.snapshot",  # ~/.dsh/maestro/tickets.md 变更 → 全文快照（render 覆写非追加）
)
SUBSCRIBABLE_KINDS = ORCH_KINDS + TOPIC_KINDS
# voice 会话默认订阅：orch.* + 自己的 turn 轨迹（语音壳顺手显示，开销每回合数帧）
DEFAULT_VOICE_KINDS = ORCH_KINDS + ("head.turn",)
MAESTRO_DIR = Path(os.path.expanduser("~/.dsh/maestro"))
DEFAULT_TOPIC_SOURCES: dict[str, dict] = {
    "fleet.snapshot": {"path": str(MAESTRO_DIR / "fleet.json"), "mode": "snapshot", "parse": "json"},
    "bridge.msg": {
        "path": str(MAESTRO_DIR / "bridge" / "inbox.log"),
        "mode": "lines",
        "backlog": 8192,  # 起播回看窗：给 console 近期上下文，不重放全史
    },
    "tickets.snapshot": {"path": str(MAESTRO_DIR / "tickets.md"), "mode": "snapshot", "parse": "text"},
}
RESUME_TTL_S = 600.0
WEB_DIR = Path(__file__).parent / "web"

# head pipeline 需要的鸭子面（fake 见 tests；realtime 见 build_realtime_head）:
#   transcript: TranscriptState      — take_tail()/seed() 供断线重连重播种
#   async start() / async stop()
#   async push_audio(pcm: bytes)     — 可慢消费；忙时由 session 侧丢最老块
# 音频下行（TTS PCM）与事件上抛由工厂闭包经 session.send_audio / bus.emit 接线。


class EchoHead:
    """回环开发头：mic 音频原样回发（--echo 模式）。

    不引 pipecat/凭据即可打通网关全媒体路径（入帧→队列/pump→下行二进制），
    供本机浏览器 e2e 自检；生产路径用 ``build_realtime_head``。
    """

    def __init__(self) -> None:
        self.transcript = TranscriptState()
        self.session: "WsSession | None" = None

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def push_audio(self, pcm: bytes) -> None:
        if self.session is not None:
            self.session.send_audio(pcm)


def echo_head_provider(session: "WsSession") -> EchoHead:
    head = EchoHead()
    head.session = session
    return head


class TurnTrace:
    """head pipeline 帧流 → ``head.turn`` 事件（KG 11 §1 phase 表）。

    纯逻辑、帧类注入（build_realtime_head 传 pipecat 真类，单测传 fake
    类）——本模块保持不依赖 pipecat 导入。判定顺序敏感：Transcription /
    Interim 是 TextFrame 子类，必须先于 TextFrame 增量累积判定。
    """

    def __init__(self, ft: dict[str, type]) -> None:
        self._ft = ft
        self._text_buf: list[str] = []

    def on_frame(self, frame: Any) -> dict | None:
        """返回 head.turn payload（无 phase 键约束外的 ts/conv_id 由调用方补）。"""
        ft = self._ft
        if isinstance(frame, ft["user_start"]):
            return {"phase": "user_start"}
        if isinstance(frame, ft["user_end"]):
            return {"phase": "user_end"}
        if isinstance(frame, ft["user_text"]):
            return {"phase": "user_text", "detail": str(frame.text)[:200]}
        if isinstance(frame, ft["interim"]):
            return None  # 中间转录不上报（chatty；权威文本走 user_text）
        if isinstance(frame, ft["assistant_start"]):
            self._text_buf.clear()
            return {"phase": "assistant_start"}
        if isinstance(frame, ft["text"]):
            self._text_buf.append(str(frame.text))
            return None
        if isinstance(frame, ft["assistant_end"]):
            detail = "".join(self._text_buf).strip()[:200]
            self._text_buf.clear()
            ev: dict = {"phase": "assistant_end"}
            if detail:
                ev["detail"] = detail
            return ev
        if isinstance(frame, ft["tool_call"]):
            return {"phase": "tool_call", "detail": str(getattr(frame, "function_name", ""))}
        if isinstance(frame, ft["interrupted"]):
            return {"phase": "interrupted"}
        return None


class WsSession:
    """单连接会话：帧协议分派 + 出站队列（慢客户端合并）+ 音频背压。"""

    def __init__(self, gateway: "VoiceGateway", ws: web.WebSocketResponse, remote: str = "") -> None:
        self.gateway = gateway
        self.ws = ws
        self.remote = remote
        self.session_id: str | None = None      # auth.ok 签发的连接级 id
        self.conv_id: str | None = None         # 会话级 id（重连续接用，不变）
        self.authenticated = False
        self.started = False
        self.observe = False  # observe 会话：只订阅事件，不建 head pipeline、拒媒体
        self.graceful_end = False
        self.closed = False
        self._token: str | None = None
        self._pipeline: Any = None
        self._unsubscribe: Callable[[], None] | None = None
        self._tasks: list[asyncio.Task] = []
        # 出站：("text", dict) 序列化延迟到发送时 → 慢客户端可在队内原位合并
        self._out: deque[tuple[str, Any]] = deque()
        self._out_wake = asyncio.Event()
        self._progress_slot: dict | None = None  # 合并激活期间在队的那帧 progress
        self._close_sent = False
        # 入站音频
        self._audio_pending: deque[bytes] = deque()
        self._audio_wake = asyncio.Event()
        self._media_window: deque[tuple[float, int]] = deque()
        self._no_session_warned = False
        self.stats = {
            "audio_in_chunks": 0,
            "audio_dropped": 0,
            "events_sent": 0,
            "progress_merged": 0,
        }

    # ---- 主循环 ----

    async def run(self) -> None:
        """接收循环 + sender 任务；收尾由 handler 的 teardown 兜底。"""
        self._tasks.append(asyncio.create_task(self._sender()))
        try:
            async for msg in self.ws:
                if msg.type == WSMsgType.ERROR:
                    break
                await self.on_ws_message(msg)
        finally:
            self.closed = True

    async def on_ws_message(self, msg) -> None:
        """二进制→_on_audio；文本→按 t 分派（KG 04 §2）。"""
        if msg.type == WSMsgType.BINARY:
            await self._on_audio(bytes(msg.data))
            return
        if msg.type != WSMsgType.TEXT:
            return
        try:
            data = json.loads(msg.data)
        except ValueError:
            await self._send_error("bad_json", "text frame must be JSON")
            return
        if not isinstance(data, dict):
            await self._send_error("bad_json", "control frame must be a JSON object")
            return
        t = data.get("t")
        if t == "auth":
            await self._on_auth(data)
        elif not self.authenticated:
            await self._send_error("unauthorized", "auth first")
            await self._close()
        elif t == "session.start":
            await self._on_start(data)
        elif t == "session.end":
            await self._on_end(data)
        elif t == "ping":
            await self._reply({"t": "pong", "ts": time.time()})
        elif t == "gate.resolve":
            await self._on_gate_resolve(data)
        else:
            await self._send_error("bad_type", f"unknown t={t!r}")

    # ---- control: auth / start / end / gate.resolve ----

    async def _on_auth(self, data: dict) -> None:
        if self.authenticated:
            await self._send_error("bad_state", "already authenticated")
            return
        token = data.get("token") or ""
        if not self.gateway.check_token(token):
            await self._send_error("auth", "invalid token")
            await self._close()
            return
        if not self.gateway.reserve_session(token, self):
            await self._send_error(
                "concurrent_limit",
                f">{self.gateway.max_sessions_per_token} concurrent sessions per token",
            )
            await self._close()
            return
        self._token = token
        self.authenticated = True
        self.session_id = "s-" + uuid.uuid4().hex[:8]
        await self._reply({"t": "auth.ok", "session_id": self.session_id})

    async def _on_start(self, data: dict) -> None:
        if self.started:
            await self._send_error("bad_state", "session already started")
            return
        observe = bool(data.get("observe"))
        # topics 订阅参数（KG 11 §3）：显式列表校验后生效；缺省 observe=全部
        # 可订阅面 / voice=orch.* + head.turn（现行客户端零改动兼容）
        topics = data.get("topics")
        if topics is not None:
            if not isinstance(topics, list) or not all(isinstance(k, str) for k in topics):
                await self._send_error("bad_request", "topics must be a list of strings")
                return
            unknown = [k for k in topics if k not in SUBSCRIBABLE_KINDS]
            if unknown:
                await self._send_error(
                    "bad_request", f"unknown topics: {unknown}; valid: {list(SUBSCRIBABLE_KINDS)}"
                )
                return
        resume_id = data.get("session_id") or None
        entries: list[TranscriptEntry] = []
        reseeded = False
        if observe:
            kinds = tuple(topics) if topics is not None else SUBSCRIBABLE_KINDS
        else:
            if resume_id:
                parked = self.gateway.pop_resumable(resume_id)
                if parked is not None:
                    entries = parked
                    reseeded = True
            try:
                await self._build_pipeline(entries)
            except Exception as e:  # noqa: BLE001 — 握手失败要回错误帧而非裸断
                log.warning("pipeline build failed: %s", e)
                await self._send_error("internal", f"head pipeline: {e}"[:200])
                await self._close()
                return
            kinds = tuple(topics) if topics is not None else DEFAULT_VOICE_KINDS
        self.started = True
        self.observe = observe
        self.conv_id = resume_id if reseeded else self.session_id
        self._unsubscribe = self.gateway.bus.subscribe(self.event_sink, *kinds)
        self._tasks.append(asyncio.create_task(self._audio_pump()))
        await self._reply(
            {
                "t": "session.started",
                "session_id": self.conv_id,
                "reseeded": reseeded,
                "entries": len(entries),
                "observe": observe,
                "topics": list(kinds),
            }
        )
        # 快照类 topic 缓存回放：回执先于回放（客户端先知道会话已开，再收状态）
        for kind in kinds:
            cached = self.gateway.topic_cache.get(kind)
            if cached is not None:
                await self.event_sink(kind, dict(cached))

    async def _build_pipeline(self, reseed_entries: list[TranscriptEntry]) -> None:
        """经 gateway.head_provider 建每会话管线，并把重连 transcript 尾重播种。"""
        if self.gateway.head_provider is None:
            raise RuntimeError("no head_provider configured")
        pipeline = self.gateway.head_provider(self)
        if inspect.isawaitable(pipeline):
            pipeline = await pipeline
        for e in reseed_entries:
            pipeline.transcript.seed(e.role, e.text)
        self._pipeline = pipeline
        await pipeline.start()

    async def _on_end(self, data: dict) -> None:
        if not self.started:
            await self._send_error("bad_state", "session not started")
            return
        self.graceful_end = True
        if self.conv_id:
            self.gateway.discard_resumable(self.conv_id)  # 优雅结束不留续接槽
        await self._reply({"t": "session.ended", "session_id": self.conv_id})
        await self._stop_pipeline()
        await self._close()

    async def _on_gate_resolve(self, data: dict) -> None:
        """gate 弹层答案回传 → DaisLane.resolve_gate（KG 04 §2 静态页注）。"""
        gate_id = data.get("gate_id")
        resolution = data.get("resolution")
        if not gate_id or not resolution:
            await self._send_error("bad_request", "gate.resolve needs gate_id and resolution")
            return
        lane = self.gateway.lane
        if lane is None:
            await self._send_error("internal", "no lane configured")
            return
        try:
            await lane.resolve_gate(str(gate_id), str(resolution))
        except DaisLaneError as e:
            await self._send_error("lane", str(e)[:160])
            return
        await self._reply({"t": "gate.resolved", "gate_id": gate_id})

    # ---- media ----

    async def _on_audio(self, pcm: bytes) -> None:
        """上行二进制 PCM 入管线；背压=忙时丢最老块；码率超限断开（§1/§4）。"""
        if not self.started:
            if not self._no_session_warned:
                self._no_session_warned = True
                await self._send_error("no_session", "session.start required before media")
            return
        if self.observe:
            if not self._no_session_warned:
                self._no_session_warned = True
                await self._send_error("observe_media", "observe session has no media path")
            return
        now = time.monotonic()
        self._media_window.append((now, len(pcm)))
        while self._media_window and now - self._media_window[0][0] > RATE_WINDOW_S:
            self._media_window.popleft()
        if sum(n for _, n in self._media_window) > self.gateway.media_rate_limit:
            await self._send_error("rate", "media bitrate above 64KB/s")
            await self._close()
            return
        self.stats["audio_in_chunks"] += 1
        while len(self._audio_pending) >= AUDIO_QUEUE_MAX:
            self._audio_pending.popleft()  # 管线忙 → 丢最老（实时性>完整性）
            self.stats["audio_dropped"] += 1
        self._audio_pending.append(pcm)
        self._audio_wake.set()

    async def _audio_pump(self) -> None:
        while True:
            await self._audio_wake.wait()
            self._audio_wake.clear()
            while self._audio_pending:
                chunk = self._audio_pending.popleft()
                pipeline = self._pipeline
                if pipeline is None:
                    break
                try:
                    await pipeline.push_audio(chunk)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001 — 单块失败不杀 pump
                    log.warning("push_audio failed: %s", e)
                    break
            if self._close_sent:
                return

    def send_audio(self, pcm: bytes) -> None:
        """head TTS PCM → 下行二进制帧（管线闭包调用，非阻塞）。"""
        if not self._close_sent:
            self._out.append(("bytes", bytes(pcm)))
            self._out_wake.set()

    # ---- event 路（EventBus → event 帧）----

    async def event_sink(self, kind: str, payload: dict) -> None:
        """EventBus 订阅回调 → ``{t:kind, **payload, ts}`` 下发（KG 04 §2）。

        慢客户端：send queue 超过 SEND_QUEUE_LIMIT 后，orch.progress 各帧
        原位合并进队列中既有的一帧（lines 取 max、head 取最新）；dispatch/
        ack/done/gate/metrics 等关键帧永不丢、永不并。
        """
        ev = {"t": kind, **payload}
        ev.setdefault("ts", time.time())
        if kind == PROGRESS_KIND:
            slot = self._progress_slot
            if slot is not None:
                slot["lines"] = max(slot.get("lines", 0), ev.get("lines", 0) or 0)
                slot["head"] = ev.get("head") or slot.get("head")
                slot["ts"] = ev["ts"]
                self.stats["progress_merged"] += 1
                return
            if len(self._out) > SEND_QUEUE_LIMIT:
                self._progress_slot = ev
                self.stats["progress_merged"] += 1
        self._out.append(("text", ev))
        self._out_wake.set()

    # ---- 出站 sender ----

    async def _sender(self) -> None:
        while True:
            await self._out_wake.wait()
            self._out_wake.clear()
            while self._out:
                kind, payload = self._out.popleft()
                if payload is self._progress_slot:
                    self._progress_slot = None  # 离开合并模式（下一帧独立入队）
                try:
                    if kind == "text":
                        await self.ws.send_str(json.dumps(payload, ensure_ascii=False))
                        if str(payload.get("t", "")).startswith("orch."):
                            self.stats["events_sent"] += 1
                    else:
                        await self.ws.send_bytes(payload)
                except (ConnectionError, RuntimeError, asyncio.CancelledError):
                    return
            if self._close_sent:
                return

    async def _reply(self, obj: dict) -> None:
        self._out.append(("text", obj))
        self._out_wake.set()

    async def _send_error(self, code: str, msg: str) -> None:
        await self._reply({"t": "error", "code": code, "msg": msg})

    async def _close(self) -> None:
        """冲刷出站队列后关连接。"""
        if self._close_sent:
            return
        self._close_sent = True
        self._out_wake.set()
        self._audio_wake.set()
        for task in self._tasks:
            if task.done():
                continue
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                task.cancel()
        try:
            await self.ws.close()
        except Exception:  # noqa: BLE001 — 对端可能已断
            pass

    # ---- 生命周期收尾 ----

    async def _stop_pipeline(self) -> None:
        pipeline, self._pipeline = self._pipeline, None
        if pipeline is not None:
            try:
                await pipeline.stop()
            except Exception as e:  # noqa: BLE001
                log.warning("pipeline stop failed: %s", e)

    async def teardown(self) -> None:
        """连接结束：异常断开→take_tail 停靠续接槽；优雅结束→丢弃。"""
        if self.closed and self.started and not self.graceful_end:
            pipeline = self._pipeline
            if pipeline is not None and self.conv_id:
                entries = await pipeline.transcript.take_tail()
                if entries:
                    self.gateway.park_resumable(self.conv_id, entries)
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None
        await self._stop_pipeline()
        self.gateway.release_session(self)
        for task in self._tasks:
            if not task.done():
                task.cancel()
        if not self._close_sent:
            self._close_sent = True
            try:
                await self.ws.close()
            except Exception:  # noqa: BLE001
                pass


class TailReader:
    """dais read-worker 增量尾读协程；每活动 worker 一个（KG 04 §2）。

    cursor=0 起步；``read-worker --after`` 取增量 → 摘要成 orch.progress
    （dispatch_id + 累计行数 + 首行 head）；机器 cursor 从 STDERR 解析
    （rt_dsh_lane.read_worker 已归一，勿混入正文）；lane 错误（终态/未知
    ctx）或 ``stop`` 置位即退出。轮询走 DaisLane 单飞锁。
    """

    def __init__(
        self,
        lane: DaisLane,
        emit: Callable[[str, dict], Awaitable[None]],
        poll_s: float = 2.0,
        lines_per_read: int = 40,
        head_max_chars: int = 120,
    ) -> None:
        self.lane = lane
        self.emit = emit
        self.poll_s = poll_s
        self.lines_per_read = lines_per_read
        self.head_max_chars = head_max_chars

    async def follow(self, dispatch_id: str, stop: asyncio.Event | None = None) -> None:
        cursor = 0
        total_lines = 0
        while True:
            if stop is not None and stop.is_set():
                return
            try:
                tail, cursor2 = await self.lane.read_worker(
                    dispatch_id, after=cursor, lines=self.lines_per_read
                )
            except DaisLaneError as e:
                log.debug("tail reader %s exits: %s", dispatch_id, e)
                return
            text = tail.strip()
            if text:
                total_lines += text.count("\n") + 1
                head = next((ln for ln in text.splitlines() if ln.strip()), "")
                await self.emit(
                    "orch.progress",
                    {
                        "dispatch_id": dispatch_id,
                        "lines": total_lines,
                        "head": head[: self.head_max_chars],
                    },
                )
            cursor = cursor2
            if stop is not None and stop.is_set():
                return
            await asyncio.sleep(self.poll_s)


class FileTailer:
    """maestro 侧文件观测源 → topic.*（KG 11 §2）。

    - ``mode="lines"``：行尾读（inbox.log）。stat 轮询 size；只消费到最后一
      个完整 ``\\n``（写者未收行的残尾等下一轮）；变小=截断/轮转 → cursor
      归零。起始 cursor = ``max(0, size - backlog)``（回看窗，不重放全史）。
    - ``mode="snapshot"``：变更即全量（fleet.json / tickets.md——后者是
      ledger render 覆写非追加）。``(mtime_ns, size)`` 签名变更才读发；
      ``parse="json"`` 解析失败不更新签名（下轮重试），``"text"`` 原文行。
    - 文件缺席：静默等待（INFO 一次），出现后照常起播。

    快照 payload 同时写 ``cache``（gateway.topic_cache）——订阅回放用，
    log 行源不缓存（无"最新状态"语义）。
    """

    def __init__(
        self,
        kind: str,
        path: str,
        emit: Callable[[str, dict], Awaitable[None]],
        *,
        mode: str = "lines",
        parse: str | None = None,
        poll_s: float = 2.0,
        backlog: int = 0,
        cache: dict | None = None,
        line_max_chars: int = 2000,
    ) -> None:
        self.kind = kind
        self.path = path
        self.emit = emit
        self.mode = mode
        self.parse = parse
        self.poll_s = poll_s
        self.backlog = backlog
        self.cache = cache
        self.line_max_chars = line_max_chars

    async def follow(self, stop: asyncio.Event | None = None) -> None:
        cursor: int | None = None
        sig: tuple[int, int] | None = None
        warned = False
        while True:
            if stop is not None and stop.is_set():
                return
            try:
                st = os.stat(self.path)
            except FileNotFoundError:
                if not warned:
                    log.info("topic source %s: %s absent, waiting", self.kind, self.path)
                    warned = True
                cursor, sig = None, None
                await asyncio.sleep(self.poll_s)
                continue
            warned = False
            try:
                if self.mode == "snapshot":
                    now_sig = (st.st_mtime_ns, st.st_size)
                    if now_sig != sig:
                        with open(self.path, encoding="utf-8", errors="replace") as f:
                            raw = f.read()
                        if self.parse == "json":
                            payload = {"fleet": json.loads(raw)}
                        else:
                            payload = {"text": raw, "lines": len(raw.splitlines())}
                        sig = now_sig
                        if self.cache is not None:
                            self.cache[self.kind] = payload
                        await self.emit(self.kind, payload)
                else:
                    if cursor is None:
                        cursor = max(0, st.st_size - self.backlog)
                    if st.st_size < cursor:  # 截断/轮转 → 从头
                        cursor = 0
                    if st.st_size > cursor:
                        with open(self.path, "rb") as f:
                            f.seek(cursor)
                            data = f.read()
                        cut = data.rfind(b"\n")
                        if cut < 0:
                            await asyncio.sleep(self.poll_s)
                            continue  # 尚无完整行
                        off, complete = cursor, data[:cut]
                        cursor += cut + 1
                        for raw_line in complete.split(b"\n"):
                            line = raw_line.decode("utf-8", errors="replace")
                            if line.strip():
                                await self.emit(
                                    self.kind,
                                    {"line": line[: self.line_max_chars], "offset": off},
                                )
                            off += len(raw_line) + 1
            except (OSError, ValueError) as e:  # noqa: BLE001 — 单轮失败不杀尾读
                log.warning("topic source %s poll failed: %s", self.kind, e)
            if stop is not None and stop.is_set():
                return
            await asyncio.sleep(self.poll_s)


@dataclass
class VoiceGateway:
    """aiohttp web server + ws；每 WsSession 独占一条管线（会话级资源）。"""

    port: int = 8765
    head_provider: Callable[["WsSession"], Any] | None = None
    backend_factory: Callable[[], Any] | None = None
    host: str = "0.0.0.0"
    token: str | None = None
    bus: EventBus = field(default_factory=EventBus)
    lane: DaisLane | None = None
    # topic 观测源（KG 11 §2）：{kind: {path, mode, parse?, backlog?, poll_s?}}；
    # None=不启用（单测缺省）；main() 注入 DEFAULT_TOPIC_SOURCES
    topic_sources: dict[str, dict] | None = None
    max_sessions_per_token: int = MAX_SESSIONS_PER_TOKEN
    media_rate_limit: int = PCM_RATE_LIMIT_BYTES
    resume_ttl_s: float = RESUME_TTL_S

    def __post_init__(self) -> None:
        if self.token is None:
            self.token = os.environ.get("VOICE_GATEWAY_TOKEN", "")
        self._runner: web.AppRunner | None = None
        self._active: set[WsSession] = set()
        self._token_counts: dict[str, int] = {}
        self._resumable: dict[str, tuple[float, list[TranscriptEntry]]] = {}
        self.topic_cache: dict[str, dict] = {}   # 快照类 topic 最近 payload（订阅回放）
        self._tailers: list[asyncio.Task] = []

    def _start_tailers(self) -> None:
        for kind, cfg in (self.topic_sources or {}).items():
            tailer = FileTailer(
                kind,
                cfg["path"],
                self.bus.emit,
                mode=cfg.get("mode", "lines"),
                parse=cfg.get("parse"),
                poll_s=cfg.get("poll_s", 2.0),
                backlog=cfg.get("backlog", 0),
                cache=self.topic_cache if cfg.get("mode") == "snapshot" else None,
            )
            self._tailers.append(asyncio.create_task(tailer.follow()))

    def _stop_tailers(self) -> None:
        for task in self._tailers:
            task.cancel()
        self._tailers = []

    # ---- 安全基线（§4）----

    def check_token(self, token: str) -> bool:
        return bool(self.token) and token == self.token

    def reserve_session(self, token: str, session: WsSession) -> bool:
        if self._token_counts.get(token, 0) >= self.max_sessions_per_token:
            return False
        self._token_counts[token] = self._token_counts.get(token, 0) + 1
        self._active.add(session)
        return True

    def release_session(self, session: WsSession) -> None:
        self._active.discard(session)
        if session._token is not None:
            remain = self._token_counts.get(session._token, 1) - 1
            if remain > 0:
                self._token_counts[session._token] = remain
            else:
                self._token_counts.pop(session._token, None)

    # ---- 断线续接槽（§4 take_tail 重播种）----

    def park_resumable(self, conv_id: str, entries: list[TranscriptEntry]) -> None:
        self._prune_resumable()
        self._resumable[conv_id] = (time.time(), entries)

    def pop_resumable(self, conv_id: str) -> list[TranscriptEntry] | None:
        self._prune_resumable()
        hit = self._resumable.pop(conv_id, None)
        return hit[1] if hit else None

    def discard_resumable(self, conv_id: str) -> None:
        self._resumable.pop(conv_id, None)

    def _prune_resumable(self) -> None:
        now = time.time()
        for key in [k for k, (ts, _) in self._resumable.items() if now - ts > self.resume_ttl_s]:
            del self._resumable[key]

    # ---- HTTP ----

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        session = WsSession(self, ws, remote=request.remote or "")
        try:
            await session.run()
        finally:
            await session.teardown()
        return ws

    async def _handle_healthz(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                "ok": True,
                "service": "rt_gateway",
                "sessions_active": len(self._active),
                "resume_slots": len(self._resumable),
            }
        )

    async def _handle_index(self, request: web.Request) -> web.Response:
        return web.FileResponse(WEB_DIR / "index.html")

    # ---- 生命周期 ----

    async def start(self) -> None:
        """绑定监听（port=0 → 随机端口，实际值回写 self.port）。"""
        if not self.token:
            raise ValueError("token required: pass token=… or set VOICE_GATEWAY_TOKEN")
        app = web.Application()
        app.router.add_get("/ws", self._handle_ws)
        app.router.add_get("/healthz", self._handle_healthz)
        app.router.add_get("/", self._handle_index)
        app.router.add_static("/static/", WEB_DIR)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        if self.port == 0 and self._runner.addresses:
            self.port = self._runner.addresses[0][1]
        self._start_tailers()
        log.info("rt_gateway listening on ws://%s:%d/ws", self.host, self.port)

    async def stop(self) -> None:
        self._stop_tailers()
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def run_forever(self) -> None:
        await self.start()
        try:
            await asyncio.Event().wait()
        finally:
            await self.stop()


# ---- live 阶段（W4.1 e2e）：真 pipecat 头，惰性导入，单测不触达 ----


async def build_realtime_head(session: "WsSession", bus: EventBus, backend: Any):
    """真 Qwen realtime 头接到 ws 会话（形制=poc_t6_pipeline.py）。

    providers 工厂 + dsh_head_tools() 四件套 + transcript 镜像 tap；上行
    InputAudioRawFrame 入管线；下行 TTSAudioRawFrame → session.send_audio。
    注意：GLM 文本模式先行（Q2 定案 TTS 通道前，音频下行可能为空）。
    """
    from pipecat.frames.frames import (
        EndFrame,
        Frame,
        FunctionCallInProgressFrame,
        InputAudioRawFrame,
        InterimTranscriptionFrame,
        LLMFullResponseEndFrame,
        LLMFullResponseStartFrame,
        ProposedUserStartedSpeakingFrame,
        ProposedUserStoppedSpeakingFrame,
        InterruptionFrame,
        TextFrame,
        TranscriptionFrame,
        TTSAudioRawFrame,
    )
    from pipecat.pipeline.pipeline import Pipeline
    from pipecat.pipeline.worker import PipelineWorker
    from pipecat.processors.aggregators.llm_context import LLMContext
    from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
    from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
    from pipecat.workers.runner import WorkerRunner

    from providers import RealtimeHeadConfig, RealtimeProtocol, RealtimeProvider, create_realtime_head
    from rt_head_tools import DSH_TOOLS_DOCTRINE, dsh_head_tools

    transcript = TranscriptState()
    tools = dsh_head_tools()
    turn_trace = TurnTrace(
        {
            "user_start": ProposedUserStartedSpeakingFrame,
            "user_end": ProposedUserStoppedSpeakingFrame,
            "user_text": TranscriptionFrame,
            "interim": InterimTranscriptionFrame,
            "assistant_start": LLMFullResponseStartFrame,
            "assistant_end": LLMFullResponseEndFrame,
            "text": TextFrame,
            "tool_call": FunctionCallInProgressFrame,
            "interrupted": InterruptionFrame,
        }
    )

    class _Tap(FrameProcessor):
        """transcript 镜像 + TTS PCM → ws 下行 + head.turn 上报（KG 11 §1）。"""

        async def process_frame(self, frame: Frame, direction: FrameDirection) -> Frame:
            if isinstance(frame, InputAudioRawFrame):
                transcript.on_speech_started()
            elif isinstance(frame, TTSAudioRawFrame):
                session.send_audio(bytes(frame.audio))
            ev = turn_trace.on_frame(frame)
            if ev is not None:
                await bus.emit("head.turn", {"conv_id": session.conv_id, **ev})
            await self.push_frame(frame, direction)
            return frame

    head = create_realtime_head(
        RealtimeHeadConfig(
            provider=RealtimeProvider.QWEN,
            protocol=RealtimeProtocol.DASHSCOPE_RT,
            system_instruction=DSH_TOOLS_DOCTRINE,
            tools=tools,
            text_only=True,
        )
    )
    context = LLMContext(tools=tools)
    aggregators = LLMContextAggregatorPair(context)
    worker = PipelineWorker(
        Pipeline([aggregators.user(), head, _Tap(), aggregators.assistant()]),
        cancel_on_idle_timeout=False,
        app_resources={"dsh_backend": backend},
    )
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)
    run_task = asyncio.create_task(runner.run())

    class _RealtimeHeadAdapter:
        async def start(self) -> None:
            pass  # runner 已随构建启动

        async def stop(self) -> None:
            await worker.queue_frame(EndFrame())
            run_task.cancel()

        async def push_audio(self, pcm: bytes) -> None:
            await worker.queue_frame(
                InputAudioRawFrame(audio=pcm, sample_rate=16000, num_channels=1)
            )

    adapter = _RealtimeHeadAdapter()
    adapter.transcript = transcript
    return adapter


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser("rt_gateway", description="本地 WS 语音网关（KG 04）")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--loopback-only", action="store_true", help="绑 127.0.0.1（默认 0.0.0.0 局域网可达）")
    ap.add_argument("--token", default=None, help="缺省取 env VOICE_GATEWAY_TOKEN")
    ap.add_argument("--echo", action="store_true", help="回环头（无 pipecat/凭据，mic→网关→扬声器自检）")
    ap.add_argument(
        "--no-topics", action="store_true", help="停用 topic 观测源（fleet/bridge/tickets 尾读）"
    )
    args = ap.parse_args(argv)

    token = args.token or os.environ.get("VOICE_GATEWAY_TOKEN")
    if not token:
        print("rt_gateway: 需要 --token 或 VOICE_GATEWAY_TOKEN", file=sys.stderr)
        return 2

    async def _run() -> None:
        lane = DaisLane()
        if args.echo:

            async def provider(session: "WsSession"):  # noqa: E306 — 回环头
                return echo_head_provider(session)

        else:
            from rt_dsh_backend import DshBackend

            backend = DshBackend(lane=lane)

            async def provider(session: "WsSession"):  # noqa: E306 — live 头闭包
                return await build_realtime_head(session, gateway.bus, backend)

        gateway = VoiceGateway(
            port=args.port,
            host="127.0.0.1" if args.loopback_only else args.host,
            token=token,
            head_provider=provider,
            lane=lane,
            topic_sources=None if args.no_topics else DEFAULT_TOPIC_SOURCES,
        )
        # TailReader 接线：orch.dispatch 携带 dispatch_ids 时逐 worker 起尾读协程
        readers: dict[str, asyncio.Task] = {}

        async def _bridge(kind: str, payload: dict) -> None:
            if kind == "orch.dispatch":
                for did in payload.get("dispatch_ids", []):
                    stop = asyncio.Event()
                    reader = TailReader(lane, gateway.bus.emit, poll_s=2.0)
                    readers[did] = asyncio.create_task(reader.follow(did, stop))
            elif kind == "orch.done":
                for did, task in list(readers.items()):
                    task.cancel()

        gateway.bus.subscribe(_bridge)
        await gateway.run_forever()

    logging.basicConfig(level=logging.INFO)
    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
