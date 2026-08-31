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
- 台账接线（KG 14 §2.2 PR1）— ``init_store()`` 模块级 ``SessionStore`` 单例 +
  ``attach_store_bridge`` 唯一写入面（orch.dispatch→put、orch.done/orch.failed
  →update+emit ``body.push`` 轻通知）；``body.get``→``body.item``/``error
  body_miss``；轻通知索引入 ``topic_cache`` 回放（最近 50 条、无 body）
- 终稿投递形态（KG 14 §2.3；PR5 翻缺省）— ``VOICE_FINAL_MODE=split|fulltext``
  （缺省 ``split``）：split 注入 ``[编排通报]`` 单行 JSON（字段取台账，缺席降级从终稿
  本体算，不接指令句）；仅精确 ``fulltext`` 回旧全文串（免重启回退）
- 会话压缩（KG 14 §2.5，PR5）— ``ConversationLog`` 镜像服务端 items +
  ``ConversationCompactor`` 零 LLM 快照：每次 turn_idle 检查阈值
  （``VOICE_COMPACT_CHARS`` 缺省 20000、0 关），过线则在终稿注入锁内逐项
  ``conversation.item.delete``（open 工具对钉住）+ 单条 ``state.snapshot``
  user item 注入（tasks=store.list ∪ backend 运行登记−store），emit
  ``head.compact``；不换会话、doctrine 不动
- 席位清理控制帧（KG 14 §2.4）— ``fleet.cleanup{ids,mode}``：mode=end
  先经 dsh loopback 真死会话（已死容忍；硬失败该条标 failed、fleet.json
  条目保留），再 fleet.json 摘条目（fleet-touch 同款 flock 哨兵锁互斥下
  原子写）；mode=release 只摘条目。被摘席位若是当前 liaison 绑定 → 清绑
  （result 标 binding-cleared；下次派发经 AUTO_NEW 换新，不再续投旧席）。
  回包 ``fleet.cleanup.result{req_id,results,failed}``
- 席位状态简报（PR9）— ``fleet.brief``：fleet.json 席位表 join dsh
  ``session.list`` 活性（live=在列且未归档；running/title/task/idle_s）、
  顶层恒带 ``liaison`` 绑定对象（bound/code/sessionId/archived）；
  loopback 不可达降级纯席位表附 note，回包 ``fleet.brief.result``；
  并以 ``fleet_brief`` app resource 暴露给 head 工具；归档集带 30s
  TTL 模块级缓存（cleanup 归档/摘除成功即失效）

Additional control frames (W3):

- ``body.list_more{req_id, before_ts, limit?}`` — 台账索引向前翻页 →
  ``body.list_more.result{req_id, items, eof}``（items 与 body.push 索引
  行同构；只返回 ts 早于 before_ts 的行；eof=返回数<limit；同游标重放
  同结果）
- ``liaison.unbind{req_id}`` → ``liaison.result{op:"unbind", ok, was_bound}``
  （未绑定为幂等 no-op）
- ``liaison.bind{req_id, code}`` → ``liaison.result{op:"bind", ok, liaison}``
  — 仅允许绑到 fleet.json 在册且未归档席位；重复绑定同 code 幂等 no-op
- ``run.cancel{req_id, ref}`` → ``run.cancel.result{req_id, ref, ok,
  state?, error?}`` — 复用 ``backend.cancel`` 内部路径；未知 ref 报
  ``unknown-ref``，重复取消幂等返回 ``state:"cancelled"``
- PM 路由（GW-001，<internal-repo> spec-gateway §GW-001）— ``pm.req{id, op,
  params}`` 纯透传 pm-host-service（ADR-004 零业务：op 机械映射
  ``GET /op/<op>?params``（``health`` 除外，见 ``_pm_op_path``），
  ADR-002 只读 GET）→ ``pm.res{id, data|error}``；
  发现=``~/.dsh/maestro/pm.port`` 的 ``port`` 字段（签名缓存，服务重启换
  端口自动失效），传输失败以 ``GET /health`` 探活分级；超时/上游失败回
  ``pm.res{error}`` 结构化（不崩连接）；``(client, id)`` 去重窗=内存有界窗
  （60s TTL、≤512 条），窗内同 id 重放不再转发——同 id 只回一次 res
- PM 事件回流（GW-002，spec-gateway §GW-002）— ``pm.sub{id, kinds}``
  注册客户端 kinds 白名单 → 每客户端订阅起一条上游 SSE 泵
  （``GET /subscribe?consumer=<session_id>&kinds=<csv>``），服务侧快照
  回放先行、泵原序搬运 → ``pm.event{…服务侧事件原样}``（保留服务侧
  msgid/payload/快照标记，白名单防御性过滤）；``pm.unsub`` 或断开即
  cancel 泵关上游（零残留）；幂等键=订阅 ``(client, kinds)``（同 kinds
  重订 no-op），帧面沿用 GW-001 同 id 去重语义；无新持久化（游标归
  pm-host-service 消费者账，断线重连靠快照回放兜底）；上游流终止（非
  主动取消）回 ``pm_sub_ended`` 错误帧，恢复=客户端重订

帧协议（§1）：JSON 文本帧（control/event）+ 二进制帧（raw PCM16LE/16k/mono，
推荐 20ms=640B）。握手序：``auth`` → ``auth.ok{session_id}`` →
``session.start{session_id?}``（旧 id = take_tail 重播种，§4）→ 媒体/事件双工。

安全基线（§4）：token 鉴权（``VOICE_GATEWAY_TOKEN``）、单 token 并发会话≤2、
media 码率上限（>64KB/s 断开）、音频不落盘、transcript 仅内存会话级。
"""

from __future__ import annotations

import asyncio
import fcntl
import inspect
import json
import logging
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urlencode

from aiohttp import ClientSession, ClientTimeout, WSMsgType, web

from rt_dsh_lane import DaisLane, DaisLaneError

from rt_event_bus import EventBus
from rt_transcript import TranscriptEntry, TranscriptState

try:  # 协议常量单点定义（G5: no drift）；openai 缺席的回环部署照常起网关
    from rt_orchestrator import FINAL_PREFIX
except Exception:  # noqa: BLE001
    FINAL_PREFIX = '"Agent Final Message":\n\n'

log = logging.getLogger("rt_gateway")

# ---- 协议/基线常量（KG 04 §1/§3/§4）----
PCM_CHUNK_BYTES = 640              # 20ms @ PCM16LE/16k/mono
PCM_RATE_LIMIT_BYTES = 64 * 1024   # >64KB/s 持续 → 断开
RATE_WINDOW_S = 1.0
SEND_QUEUE_LIMIT = 100             # 慢客户端：send queue >100 → 合并 progress
PROGRESS_KIND = "orch.progress"
AUDIO_QUEUE_MAX = 16               # ~320ms；忙时丢最老块（实时性>完整性）
MAX_SESSIONS_PER_TOKEN = 2
# WS 协议帧集版本（WSP-001，spec-ws-protocol-v1.md §0 打版条款）：连接期
# 协商字段，auth.ok 帧内自证版本。只增不改——客户端容忍缺失=pre-freeze
# 行为；破坏性变更须 v2 并行期。
PROTO_VERSION = "v1"

# orch.* 事件全集（KG 04 §3；orch.failed 终点失败面见 KG 14 §2.2；显式订阅
# 而非 wildcard——EventBus 的无 kinds 订阅会把同一 entry 双挂 "*" 造成双投递，
# 见 VO-011 报告"发现"节）
ORCH_KINDS = (
    "orch.dispatch",
    "orch.ack",
    "orch.progress",
    "orch.gate",
    "orch.done",
    "orch.failed",
    "orch.metrics",
)
# 台账轻通知（KG 14 §2.2）：正文只走 body.push（裁决 #3）；形态
# {t,ref,no,status,title,summary,chars,inline|null,ts}，chars≤4096 附 inline
# 全文；索引入 topic_cache 回放（{t:"body.push",items:[…无 body],ts}）
BODY_PUSH_KIND = "body.push"
BODY_INDEX_LIMIT = 50               # 回放索引条数（兼掉 list 帧，裁决 #3）
BODY_INLINE_MAX_CHARS = 4096        # ≤ 此长度轻通知附 inline 全文
# body.list_more 翻页（W3）：缺省页大小与 rt_session_store.DEFAULT_LIST_LIMIT
# 同值（50）；上限封顶防一次拉穿 LRU 全库。
BODY_LIST_MORE_DEFAULT_LIMIT = 50
BODY_LIST_MORE_MAX_LIMIT = 200
CANCEL_ARTIFACT = "(已取消)"         # orch.done 的 cancel 语义标记 → status=cancelled
# split 完成通报前缀（KG 14 §2.3/裁决 #7）：注入载荷是纯数据、单行 JSON，
# 不接任何指令句——行为约定只在 doctrine，载荷嵌指令即漂移源
FINAL_NOTICE_PREFIX = "[编排通报] "
# topic.* 观测事件全集（KG 11 §1；console P1 数据平面）
TOPIC_KINDS = (
    "head.turn",         # head 回合生命周期（TurnTrace，KG 11 §1 phase 表）
    "head.compact",      # 会话压缩通报（KG 14 §2.5：{conv_id,before/after_chars,pinned,reason}）
    "fleet.snapshot",    # ~/.dsh/maestro/fleet.json 字节变更 → 全量 JSON 快照
    "bridge.msg",        # ~/.dsh/maestro/bridge/inbox.log 增量行
    "tickets.snapshot",  # ~/.dsh/maestro/tickets.md 变更 → 全文快照（render 覆写非追加）
    BODY_PUSH_KIND,      # 台账轻通知（KG 14 §2.2；bridge 发、索引回放，非文件源）
)
SUBSCRIBABLE_KINDS = ORCH_KINDS + TOPIC_KINDS
# voice 会话默认订阅：orch.* + 自己的 turn 轨迹（语音壳顺手显示，开销每回合数帧）
# + body.push 轻通知（KG 14 §2.6 PR4/裁决 #9：语音页迷你行自动收，不进 head 上下文）
DEFAULT_VOICE_KINDS = ORCH_KINDS + ("head.turn", BODY_PUSH_KIND)
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

# ---- PM 路由常量（GW-001，<internal-repo> spec-gateway §GW-001）----
# ADR-004 幂等枢纽零业务：pm.req 只做机械路由（op→路径、params→query），
# 不带任何 op 语义；pm-host-service 只读（ADR-002），GET 一个动词走到底。
PM_OP_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")  # op→URL 路径段的合法字符集
PM_REQ_TIMEOUT_S = 8.0                # 单次透传调用总预算
PM_PROBE_TIMEOUT_S = 2.0              # GET /health 探活预算
# (client, id) 去重窗：沿用现有内存窗范式（有界 OrderedDict + TTL，无持久化）
PM_DEDUP_TTL_S = 60.0
PM_DEDUP_MAX = 512
# ---- GW-002 事件回流常量（<internal-repo> spec-gateway §GW-002）----
# op=subscribe 由服务侧 PM-007 提供（收口前由替身 SSE 源验证）；端点路径与
# query 参数名是网关侧假设，联调以 PM-007 收口形态为准，只调此处不改泵体。
PM_SUBSCRIBE_PATH = "subscribe"
PM_KIND_RE = re.compile(r"[A-Za-z0-9_.-]{1,64}")
PM_SUB_MAX_KINDS = 32

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
    Interim 是 TextFrame 子类，必须先于文本增量累积判定。
    """

    def __init__(self, ft: dict[str, type]) -> None:
        self._ft = ft
        self._text_buf: list[str] = []

    def on_frame(self, frame: Any) -> dict | None:
        """返回 head.turn payload（无 phase 键约束外的 ts/conv_id 由调用方补）。"""
        ft = self._ft
        if isinstance(frame, ft["user_start"]):
            # 文本缓冲按用户回合清：一次应答内 assistant_start 会触发多次
            # （response 创建 + 每个 assistant item added），按 start 清会把
            # 已累积的应答文本抹掉。
            self._text_buf.clear()
            return {"phase": "user_start"}
        if isinstance(frame, ft["user_end"]):
            return {"phase": "user_end"}
        if isinstance(frame, ft["user_text"]):
            return {"phase": "user_text", "detail": str(frame.text)[:200]}
        if isinstance(frame, ft["interim"]):
            return None  # 中间转录不上报（chatty；权威文本走 user_text）
        if isinstance(frame, ft["assistant_start"]):
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
        # pm.req (client, id) 去重窗（GW-001）：id→首见 monotonic 时刻，
        # 有界内存窗，无持久化
        self._pm_seen: OrderedDict[str, float] = OrderedDict()
        # pm 订阅态（GW-002）：None=未订阅；{"kinds": tuple, "pump": Task}
        self._pm_sub: dict | None = None
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
        elif t == "body.get":
            await self._on_body_get(data)
        elif t == "body.list_more":
            await self._on_body_list_more(data)
        elif t == "fleet.cleanup":
            await self._on_fleet_cleanup(data)
        elif t == "fleet.brief":
            await self._on_fleet_brief(data)
        elif t == "liaison.unbind":
            await self._on_liaison_unbind(data)
        elif t == "liaison.bind":
            await self._on_liaison_bind(data)
        elif t == "run.cancel":
            await self._on_run_cancel(data)
        elif t == "whiteboard.set":
            await self._on_whiteboard_set(data)
        elif t == "head.list":
            await self._on_head_list(data)
        elif t == "head.switch":
            await self._on_head_switch(data)
        elif t == "pm.req":
            await self._on_pm_req(data)
        elif t == "pm.sub":
            await self._on_pm_sub(data)
        elif t == "pm.unsub":
            await self._on_pm_unsub(data)
        else:
            await self._send_error("bad_type", f"unknown t={t!r}",
                                   req_id=data.get("req_id"))

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
        await self._reply({"t": "auth.ok", "session_id": self.session_id,
                           "proto": PROTO_VERSION})

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
        req_id = data.get("req_id")
        gate_id = data.get("gate_id")
        resolution = data.get("resolution")
        if not gate_id or not resolution:
            await self._send_error("bad_request",
                                   "gate.resolve needs gate_id and resolution",
                                   req_id=req_id)
            return
        lane = self.gateway.lane
        if lane is None:
            await self._send_error("internal", "no lane configured",
                                   req_id=req_id)
            return
        try:
            await lane.resolve_gate(str(gate_id), str(resolution))
        except DaisLaneError as e:
            await self._send_error("lane", str(e)[:160], req_id=req_id)
            return
        await self._reply({"t": "gate.resolved", "gate_id": gate_id})

    async def _on_body_get(self, data: dict) -> None:
        """body.get{ref} → body.item 全文（KG 14 §2.2 裁决 #4；miss→body_miss）。"""
        req_id = data.get("req_id")
        ref = data.get("ref")
        if not ref:
            await self._send_error("bad_request", "body.get needs ref",
                                   req_id=req_id)
            return
        store = _get_store()
        rec = await _store_call(store.get, str(ref)) if store is not None else None
        if not isinstance(rec, dict) or not rec:
            await self._send_error("body_miss", f"no stored body for ref {ref}",
                                   req_id=req_id)
            return
        await self._reply({
            "t": "body.item",
            "ref": rec.get("ref") or ref,
            "title": rec.get("title") or "",
            "text": rec.get("body") or "",
            "chars": rec.get("chars") or 0,
            "ts": rec.get("updated_ts") or rec.get("ts") or time.time(),
        })

    async def _on_body_list_more(self, data: dict) -> None:
        """body.list_more{req_id,before_ts,limit?} → 台账索引翻页（W3）.

        ``before_ts`` 是游标：只返回台账 ``ts`` 严格早于该值的行（no 升序、
        去 body，行与 body.push 索引行同构——:func:`_index_entry`）。
        ``limit`` 缺省 :data:`BODY_LIST_MORE_DEFAULT_LIMIT`（与
        ``rt_session_store.DEFAULT_LIST_LIMIT`` 同值），1..200 钳制；
        ``eof``=返回数<limit（不足一页即到头）。纯读、同 before_ts 重放
        同结果（幂等）。store 未初始化/读失败 → internal 错误帧。
        """
        req_id = data.get("req_id")
        before_ts = data.get("before_ts")
        if isinstance(before_ts, bool) or not isinstance(before_ts, (int, float)):
            await self._send_error("bad_request",
                                   "body.list_more needs a numeric before_ts",
                                   req_id=req_id)
            return
        limit = BODY_LIST_MORE_DEFAULT_LIMIT
        if data.get("limit") is not None:
            try:
                limit = int(data["limit"])
            except (TypeError, ValueError):
                await self._send_error("bad_request",
                                       "body.list_more limit must be an integer",
                                       req_id=req_id)
                return
        limit = min(max(limit, 1), BODY_LIST_MORE_MAX_LIMIT)
        store = _get_store()
        if store is None:
            await self._send_error("internal", "session store unavailable",
                                   req_id=req_id)
            return
        rows = await _store_call(store.list, limit, before_ts=float(before_ts))
        if not isinstance(rows, list):
            await self._send_error("internal", "session store read failed",
                                   req_id=req_id)
            return
        await self._reply({
            "t": "body.list_more.result",
            "req_id": req_id,
            "items": [_index_entry(r) for r in rows],
            "eof": len(rows) < limit,
        })

    async def _on_fleet_cleanup(self, data: dict) -> None:
        """fleet.cleanup{ids,mode} → 逐 id 处理 → fleet.cleanup.result（KG 14 §2.4）.

        mode=end 先经 loopback 真死会话（已死容忍——正常清理场景，锁外
        发rpc）；硬失败（非 gone 分型）该条标 ``error:end-failed``、
        fleet.json 条目保留并计入汇总 ``failed`` 数。mode=release 只摘
        fleet.json 条目不碰会话；id 不存在该条 not_found 其余照处理。
        被摘席位（end 真死成功 / release 出册）若是当前 liaison 绑定 →
        删除 liaison.json（result 标 ``binding-cleared``；此后派发不再
        续投旧席——AUTO_NEW 开启时换新，未开启时按无绑定口径报错）。

        摘除的读-改-写全程走 :func:`_fleet_update`（fleet-touch 同款
        flock 哨兵锁互斥、锁内读→删→写，B1）；无有效摘除不触碰文件
        （免空写触发 fleet.snapshot 重发）。获锁超时 → 待摘条目全部
        ``{"ok": false, "error": "fleet-lock-timeout"}``、条目保留、计入
        failed（B2 如实回包；锁释放后重放同请求即成功）。end 归档成功
        （含 gone 分型）或条目实际摘除后立即失效归档集缓存（B3：后续
        brief 不再看到幽灵席位）。重复 cleanup 同输入：条目已摘 → 全
        not_found、文件零写，终态一致（幂等）。
        """
        req_id = data.get("req_id")
        ids = data.get("ids")
        mode = data.get("mode")
        if (mode not in FLEET_CLEANUP_MODES
                or not isinstance(ids, list) or not ids
                or not all(isinstance(i, str) and i for i in ids)):
            await self._send_error(
                "bad_request",
                f"fleet.cleanup needs non-empty string ids and mode in {FLEET_CLEANUP_MODES}",
                req_id=req_id,
            )
            return
        path = _fleet_path()
        try:
            with open(path, encoding="utf-8") as fh:
                fleet = json.load(fh)
        except (OSError, ValueError) as e:
            await self._send_error("internal", f"fleet.json unreadable: {e}"[:160],
                                   req_id=req_id)
            return
        entries = fleet.get("fleet") if isinstance(fleet, dict) else None
        if not isinstance(entries, dict):
            await self._send_error("internal", "fleet.json has no fleet table",
                                   req_id=req_id)
            return
        liaison_code = _liaison_bound_code()
        results: list[dict] = []
        removable: list[str] = []
        archived_any = False
        for fid in ids:
            entry = entries.get(fid)
            if not isinstance(entry, dict):
                results.append({"id": fid, "ok": False, "error": "not_found"})
                continue
            if mode == "end":
                sid = str(entry.get("sessionId") or "")
                if sid:
                    try:
                        await _dsh_api(SESSION_END_METHOD, {"sessionId": sid})
                        archived_any = True
                    except Exception as e:  # noqa: BLE001 — 会话真死失败：条目保留
                        if _session_gone(e):
                            log.debug("fleet.cleanup %s already gone: %s", sid, e)
                            archived_any = True  # 会话确不在，归档口径同已归档
                        else:
                            log.warning("fleet.cleanup end %s failed: %s", sid, e)
                            results.append({"id": fid, "ok": False,
                                            "error": "end-failed"})
                            continue  # 真死未确认 → 不摘册（残席可重试清理）
            results.append({"id": fid, "ok": True})
            removable.append(fid)
        removed: list[str] = []
        if removable:
            def _apply(fleet_now: dict) -> list[str]:
                table = fleet_now.get("fleet")
                removed_ids: list[str] = []
                if isinstance(table, dict):
                    for fid in removable:
                        if table.pop(fid, None) is not None:
                            removed_ids.append(fid)
                return removed_ids

            try:
                removed = await _fleet_update(path, _apply)
            except FleetLockTimeout:
                for item in results:
                    if item.get("ok"):
                        item.update(ok=False, error="fleet-lock-timeout")
                removed = []
            except (OSError, ValueError) as e:
                await self._send_error(
                    "internal", f"fleet.json write failed: {e}"[:160],
                    req_id=req_id)
                return
            for fid in removed:
                if fid == liaison_code:
                    for item in results:
                        if item.get("id") == fid and item.get("ok"):
                            item["note"] = "active-liaison"
                            if _liaison_bound_clear():
                                item["binding-cleared"] = True
        if removed or archived_any:
            _reset_archived_cache()
        await self._reply({
            "t": "fleet.cleanup.result",
            "req_id": req_id,
            "results": results,
            "failed": sum(1 for r in results
                          if r.get("error") in ("end-failed", "fleet-lock-timeout")),
        })

    async def _on_fleet_brief(self, data: dict) -> None:
        """fleet.brief → ``fleet.brief.result{req_id, seats[, note]}``（PR9）.

        payload 组装在模块级 :func:`_fleet_brief_payload`（与 head 工具的
        ``fleet_brief`` app resource 同一实现）；fleet.json 读不了由其抛出，
        此处回 internal 错误帧。
        """
        try:
            payload = await _fleet_brief_payload()
        except (OSError, ValueError) as e:
            await self._send_error("internal", f"fleet.json unreadable: {e}"[:160],
                                   req_id=data.get("req_id"))
            return
        await self._reply({
            "t": "fleet.brief.result",
            "req_id": data.get("req_id"),
            **payload,
        })

    async def _on_liaison_unbind(self, data: dict) -> None:
        """liaison.unbind{req_id} → liaison.result{op:"unbind",ok,was_bound}（W3）.

        删除当前 liaison 绑定文件；未绑定时 ``ok:true, was_bound:false``
        （幂等 no-op）。删除失败（文件在而删不掉）→ ``ok:false`` 附
        ``error``，绑定保持原样。
        """
        req_id = data.get("req_id")
        was_bound = os.path.exists(_liaison_state_path())
        ok, error = True, None
        if was_bound and not _liaison_bound_clear():
            # clear 返回 False 且文件仍不在了 → 并发删除，等效已解绑
            ok = os.path.exists(_liaison_state_path())
            if not ok:
                was_bound = True
            else:
                error = "liaison state clear failed"
        frame = {"t": "liaison.result", "req_id": req_id, "op": "unbind",
                 "ok": ok, "was_bound": was_bound}
        if error is not None:
            frame["error"] = error
        await self._reply(frame)

    async def _on_liaison_bind(self, data: dict) -> None:
        """liaison.bind{req_id,code} → liaison.result{op:"bind",…}（W3）.

        仅允许绑到 fleet.json 在册席位：code 不在 fleet 表（或条目无
        sessionId）→ ``ok:false``；目标会话已归档（workspace.list 归档集，
        带 TTL 缓存）→ ``ok:false`` 且 ``error`` 注明归档；成功写
        liaison.json（``{"code","sessionId","bound_at"}``）。重复绑定同
        code 且文件已同值 → 不重写文件（幂等 no-op），回包同值。失败帧
        的 ``liaison`` 对象回报操作后的当前绑定（未动即旧值/无绑定）。
        """
        req_id = data.get("req_id")
        code = data.get("code")
        if not isinstance(code, str) or not code.strip():
            await self._send_error("bad_request", "liaison.bind needs a code",
                                   req_id=req_id)
            return
        code = code.strip()

        def _frame(ok: bool, error: str | None, liaison: dict) -> dict:
            frame = {"t": "liaison.result", "req_id": req_id, "op": "bind",
                     "ok": ok, "liaison": liaison}
            if error is not None:
                frame["error"] = error
            return frame

        try:
            with open(_fleet_path(), encoding="utf-8") as fh:
                fleet = json.load(fh)
        except (OSError, ValueError) as e:
            await self._reply(_frame(False, f"fleet.json unreadable: {e}"[:160],
                                     await _liaison_view_after()))
            return
        table = fleet.get("fleet") if isinstance(fleet, dict) else None
        entry = table.get(code) if isinstance(table, dict) else None
        sid = str(entry.get("sessionId") or "") if isinstance(entry, dict) else ""
        if not sid:
            await self._reply(_frame(False, "code-not-in-fleet",
                                     await _liaison_view_after()))
            return
        archived = await _archived_session_ids()
        if archived is not None and sid in archived:
            await self._reply(_frame(False, f"target session archived: {sid}",
                                     await _liaison_view_after()))
            return
        current = _liaison_bound_state()
        if current.get("code") != code or current.get("sessionId") != sid:
            doc = {"code": code, "sessionId": sid, "bound_at": time.time()}
            try:
                await asyncio.to_thread(_write_json_atomic, _liaison_state_path(), doc)
            except OSError as e:
                await self._reply(_frame(False, f"liaison state write failed: {e}"[:160],
                                         await _liaison_view_after()))
                return
        await self._reply(_frame(True, None, {
            "bound": True, "code": code, "sessionId": sid, "archived": False,
        }))

    async def _on_run_cancel(self, data: dict) -> None:
        """run.cancel{req_id,ref} → run.cancel.result{req_id,ref,ok,state?}（W3）.

        复用 ``backend.cancel(ref)`` 现有内部路径（本地 phase-2 kill 是权威
        取消，``state`` 恒报 ``"cancelled"``）。ref 不在 backend ``_runs``
        运行登记 → ``ok:false, error:"unknown-ref"``；重复取消同一 ref →
        第二次仍 ``ok:true, state:"cancelled"``（幂等 no-op：pending 任务
        已死，路径内 ``done()`` 短路）。backend 未接线（echo 模式/未建
        live 头）→ internal 错误帧。
        """
        req_id = data.get("req_id")
        ref = data.get("ref")
        if not isinstance(ref, str) or not ref:
            await self._send_error("bad_request", "run.cancel needs a ref",
                                   req_id=req_id)
            return
        backend = _get_backend()
        if backend is None:
            await self._send_error("internal", "no dsh backend wired",
                                   req_id=req_id)
            return
        runs = getattr(backend, "_runs", None)
        if not isinstance(runs, dict) or ref not in runs:
            await self._reply({"t": "run.cancel.result", "req_id": req_id,
                               "ref": ref, "ok": False, "error": "unknown-ref"})
            return
        try:
            await backend.cancel(ref)
        except Exception as e:  # noqa: BLE001 — 取消失败如实回包
            await self._reply({"t": "run.cancel.result", "req_id": req_id,
                               "ref": ref, "ok": False, "error": str(e)[:160]})
            return
        await self._reply({"t": "run.cancel.result", "req_id": req_id,
                           "ref": ref, "ok": True, "state": "cancelled"})

    async def _on_whiteboard_set(self, data: dict) -> None:
        """whiteboard.set{text} → whiteboard.set.result（PR10）.

        客户端白板页签的整体同步：整段覆写全局白板（单用户单板，跨会话
        存活）。超上限拒绝并报差额——用户改短后重发即可；head 侧读取面
        是 :func:`_whiteboard_get`（app_resources 注入，read_whiteboard
        工具消费）。
        """
        text = data.get("text")
        if not isinstance(text, str):
            await self._send_error("bad_request", "whiteboard.set needs a string text",
                                   req_id=data.get("req_id"))
            return
        if len(text) > _WHITEBOARD_MAX_CHARS:
            await self._reply({
                "t": "whiteboard.set.result", "req_id": data.get("req_id"),
                "ok": False,
                "reason": (f"内容超过 {_WHITEBOARD_MAX_CHARS} 字符上限"
                           f"（当前 {len(text)}）"),
            })
            return
        _WHITEBOARD["text"] = text
        _WHITEBOARD["ts"] = time.time()
        await self._reply({
            "t": "whiteboard.set.result", "req_id": data.get("req_id"),
            "ok": True, "chars": len(text),
        })

    # ---- control: head 配置面（PR8：多 head 配置，激活单例）----

    async def _on_head_list(self, data: dict) -> None:
        """head.list → head.list.result：配置表 + 当前激活（只读）。"""
        reg = _head_registry()
        await self._reply({
            "t": "head.list.result",
            "req_id": data.get("req_id"),
            "active": reg.active,
            "file_backed": reg.file_backed,
            "env_pinned": reg.env_pinned,
            "profiles": reg.describe(),
        })

    async def _on_head_switch(self, data: dict) -> None:
        """head.switch{name} → 换激活 head；下一次语音会话生效。

        激活是单例且活会话不拆——切换只改下一次构建的选择并写回
        profiles 文件（原子写；写回失败时内存选择仍生效但不跨重启，
        结果帧如实说明）。单头模式（无 profiles 文件）与
        ``VOICE_HEAD_PROFILE`` 钉住时拒绝。
        """
        name = data.get("name")
        reg = _head_registry()
        if not isinstance(name, str) or not name.strip():
            await self._send_error("bad_request", "head.switch needs a name",
                                   req_id=data.get("req_id"))
            return
        name = name.strip()
        if not reg.file_backed:
            await self._send_error(
                "bad_request", "single-head mode: no profiles file",
                req_id=data.get("req_id"))
            return
        if reg.env_pinned:
            await self._send_error(
                "bad_request", "active head pinned by VOICE_HEAD_PROFILE",
                req_id=data.get("req_id"))
            return
        if not any(p.name == name for p in reg.profiles):
            await self._send_error(
                "bad_request",
                f"unknown head {name!r}; valid: {[p.name for p in reg.profiles]}",
                req_id=data.get("req_id"))
            return
        if reg.active == name:
            await self._reply({
                "t": "head.switch.result", "req_id": data.get("req_id"),
                "ok": True, "active": name, "note": "已是激活 head",
            })
            return
        reg.active = name
        try:
            with open(reg.path, encoding="utf-8") as fh:
                doc = json.load(fh)
            doc["active"] = name
            _write_json_atomic(reg.path, doc)
        except (OSError, ValueError) as e:
            await self._reply({
                "t": "head.switch.result", "req_id": data.get("req_id"),
                "ok": True, "active": name,
                "note": f"已切换，但写回失败（{e}）——重启后回旧值",
            })
            return
        log.info("head switched to %s (effective next session)", name)
        await self._reply({
            "t": "head.switch.result", "req_id": data.get("req_id"),
            "ok": True, "active": name, "note": "下一次语音连接生效",
        })

    # ---- control: PM 路由（GW-001：pm.req 纯透传 pm-host-service）----

    def _pm_dedup_first(self, key: str) -> bool:
        """``(client, id)`` 去重窗：True=首次（放行并记窗）、False=窗内重放。

        有界内存窗（:data:`PM_DEDUP_TTL_S`/:data:`PM_DEDUP_MAX`），无持久化；
        条目记首见时刻，重放不续期——窗过期后同 id 视为新请求。
        """
        now = time.monotonic()
        seen = self._pm_seen
        for stale in [k for k, ts in seen.items() if now - ts > PM_DEDUP_TTL_S]:
            del seen[stale]
        if key in seen:
            return False
        seen[key] = now
        while len(seen) > PM_DEDUP_MAX:
            seen.popitem(last=False)
        return True

    async def _on_pm_req(self, data: dict) -> None:
        """pm.req{id, op, params} → 透传 pm-host-service → pm.res（GW-001）.

        帧内 ``id``（客户端生成）是幂等键：去重窗 ``(client, id)`` 命中即静默
        丢弃——同 id 重放只回一次 res。HTTP 往返放独立任务跑，接收循环不被
        上游拖住（媒体帧照常流动）。上游超时/失败一律结构化成
        ``pm.res{error}``，绝不关连接。
        """
        pm_id = data.get("id")
        if (isinstance(pm_id, bool) or not isinstance(pm_id, (str, int))
                or (isinstance(pm_id, str) and not pm_id)):
            await self._send_error("bad_request", "pm.req needs a non-empty id")
            return
        if not self._pm_dedup_first(f"{pm_id}"):
            log.debug("pm.req id=%s in dedup window; dropped", pm_id)
            return
        op = data.get("op")
        if not isinstance(op, str) or not PM_OP_RE.fullmatch(op):
            await self._pm_reply(pm_id, error={
                "code": "bad_request",
                "message": "pm.req.op must match [A-Za-z0-9_-]{1,64}"})
            return
        params = data.get("params")
        if params is None:
            query = ""
        elif isinstance(params, dict):
            # 机械映射：str 原样，其余 JSON 编码（bool/None/嵌套结构无损）
            query = urlencode({
                str(k): v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
                for k, v in params.items()
            })
        else:
            await self._pm_reply(pm_id, error={
                "code": "bad_request", "message": "pm.req.params must be an object"})
            return
        task = asyncio.create_task(self._pm_roundtrip(pm_id, op, query))
        task.add_done_callback(self._tasks.remove)  # 往返完即出册，_tasks 不涨
        self._tasks.append(task)

    async def _pm_roundtrip(self, pm_id: str | int, op: str, query: str) -> None:
        """透传往返：成功 ``pm.res{id, data}``；任何失败 ``pm.res{id, error}``。"""
        try:
            data = await _pm_call(op, query)
        except asyncio.CancelledError:
            raise
        except PMUpstreamError as e:
            await self._pm_reply(pm_id, error=e.error)
        except Exception as e:  # noqa: BLE001 — 结构化兜底：绝不带崩连接
            await self._pm_reply(pm_id, error={"code": "pm_internal",
                                               "message": str(e)[:160]})
        else:
            await self._pm_reply(pm_id, data=data)

    async def _pm_reply(self, pm_id: str | int, data: Any = None,
                        error: dict | None = None) -> None:
        """pm.res 出站封套：``{t:"pm.res", id, data|error}``（ADR-006 帧集）。"""
        frame: dict = {"t": "pm.res", "id": pm_id}
        if error is not None:
            frame["error"] = error
        else:
            frame["data"] = data
        await self._reply(frame)

    # ---- control: PM 事件回流（GW-002：pm.sub/pm.unsub + SSE 泵）----

    @staticmethod
    def _pm_bad_kinds(kinds: Any) -> str | None:
        """kinds 白名单机械校验：非空串列表、词法合法、≤上限；违例回原因。"""
        if not isinstance(kinds, list) or not kinds:
            return "pm.sub needs a non-empty kinds list (pm.unsub to clear)"
        if len(kinds) > PM_SUB_MAX_KINDS:
            return f"pm.sub kinds exceeds {PM_SUB_MAX_KINDS}"
        for k in kinds:
            if not isinstance(k, str) or not PM_KIND_RE.fullmatch(k):
                return f"bad kind {k!r}: must match [A-Za-z0-9_.-]{{1,64}}"
        return None

    async def _on_pm_sub(self, data: dict) -> None:
        """pm.sub{id, kinds} → 订阅建立/替换 → pm.res（GW-002）.

        幂等键=订阅 ``(client, kinds)``：同 kinds 且泵在活时重订是 no-op
        （泵不动，只回 ok）；kinds 不同则 cancel 旧泵起新泵（替换语义）；
        同 kinds 但泵已死（上游失败/EOF 退场）时重订重建泵——恢复=重订，
        无需先 unsub。帧 id 沿用
        GW-001 去重窗——同 id 重放只回一次 res。发现失败同步快失败（该 id
        的唯一一帧 pm.res 即 error）；泵内异步失败走 ``pm_sub_failed``/
        ``pm_sub_ended`` 错误帧（非致命，不占 id、不崩连接）。
        """
        pm_id = data.get("id")
        if (isinstance(pm_id, bool) or not isinstance(pm_id, (str, int))
                or (isinstance(pm_id, str) and not pm_id)):
            await self._send_error("bad_request", "pm.sub needs a non-empty id")
            return
        if not self._pm_dedup_first(f"{pm_id}"):
            log.debug("pm.sub id=%s in dedup window; dropped", pm_id)
            return
        kinds = data.get("kinds")
        if reason := self._pm_bad_kinds(kinds):
            await self._pm_reply(pm_id, error={"code": "bad_request",
                                               "message": reason})
            return
        fresh = tuple(kinds)
        if (self._pm_sub is not None and self._pm_sub["kinds"] == fresh
                and not self._pm_sub["pump"].done()):
            await self._pm_reply(pm_id, data={"subscribed": list(fresh),
                                              "note": "already-subscribed"})
            return
        if _pm_port() is None:
            await self._pm_reply(pm_id, error={
                "code": "pm_unavailable",
                "message": f"pm.port unreadable: {_pm_port_path()}"})
            return
        await self._pm_sub_teardown()
        pump = asyncio.create_task(self._pm_event_pump(fresh))
        pump.add_done_callback(self._tasks.remove)
        self._tasks.append(pump)
        self._pm_sub = {"kinds": fresh, "pump": pump}
        await self._pm_reply(pm_id, data={"subscribed": list(fresh)})

    async def _on_pm_unsub(self, data: dict) -> None:
        """pm.unsub{id} → 取消订阅、关上游泵 → pm.res（幂等 no-op 容忍）。"""
        pm_id = data.get("id")
        if (isinstance(pm_id, bool) or not isinstance(pm_id, (str, int))
                or (isinstance(pm_id, str) and not pm_id)):
            await self._send_error("bad_request", "pm.unsub needs a non-empty id")
            return
        if not self._pm_dedup_first(f"{pm_id}"):
            log.debug("pm.unsub id=%s in dedup window; dropped", pm_id)
            return
        was = await self._pm_sub_teardown()
        await self._pm_reply(pm_id, data={"subscribed": [], "was_subscribed": was})

    async def _pm_sub_teardown(self) -> bool:
        """cancel 并等干泵任务（上游连接随任务内 async-with 关闭）→ 无泄漏。

        Returns:
            是否确有订阅被拆除。
        """
        sub, self._pm_sub = self._pm_sub, None
        if sub is None:
            return False
        pump = sub["pump"]
        if not pump.done():
            pump.cancel()
        try:
            await pump
        except asyncio.CancelledError:
            pass  # 泵被本函数取消：等到了干净退出即目的达成
        except Exception as e:  # noqa: BLE001 — 泵自吞一切，这里只兜底
            log.warning("pm event pump ended with error: %s", e)
        return True

    async def _pm_event_pump(self, kinds: tuple[str, ...]) -> None:
        """上游 SSE 消费泵（GW-002，每客户端订阅一条）.

        ``GET /subscribe?consumer=<session_id>&kinds=<csv>``：服务侧快照
        回放先行、泵按到达序逐帧搬运（``pm.event{…原样}``，保留服务侧
        msgid/payload/快照标记；kind 不在白名单的防御性跳过）。连接/读
        失败、流终止或任何异常 → ``pm_sub_failed``/``pm_sub_ended`` 错误
        帧（非致命）并退出——不做上游自动重试，恢复=客户端重订（快照
        回放兜底，游标归 pm-host-service 消费者账）。取消路径零输出。
        """
        port = _pm_port()
        consumer = f"gw-{self.session_id or uuid.uuid4().hex[:8]}"
        query = urlencode({"consumer": consumer, "kinds": ",".join(kinds)})
        url = f"http://127.0.0.1:{port}/{PM_SUBSCRIBE_PATH}?{query}"
        try:
            timeout = ClientTimeout(total=None, connect=PM_REQ_TIMEOUT_S)
            async with ClientSession(timeout=timeout) as http:
                async with http.get(url) as resp:
                    if resp.status != 200:
                        body = (await resp.text())[:200]
                        await self._send_error(
                            "pm_sub_failed",
                            f"upstream HTTP {resp.status}: {body}")
                        return
                    await self._pm_sse_consume(resp, kinds)
        except asyncio.CancelledError:
            raise  # unsub/断开：静默，不发任何帧
        except Exception as e:  # noqa: BLE001 — 结构化降级，绝不崩连接
            if not self.closed:
                await self._send_error("pm_sub_failed", str(e)[:160])
        else:
            # 流自然 EOF：仅当本泵仍是客户端在册订阅时告知（被替换/拆除的
            # 旧泵静默退场）——恢复路径=客户端重订（快照回放兜底）
            if (not self.closed and self._pm_sub is not None
                    and self._pm_sub["pump"] is asyncio.current_task()):
                await self._send_error(
                    "pm_sub_ended", "upstream event stream ended; re-subscribe")

    async def _pm_sse_consume(self, resp: Any, kinds: tuple[str, ...]) -> None:
        """SSE 事件循环：按空行分帧，``data:`` 行 JSON 解析 → 白名单 → 搬运。"""
        buf = b""
        async for chunk in resp.content.iter_any():
            buf += chunk
            while True:
                cut = buf.find(b"\n\n")
                if cut < 0:
                    if len(buf) > 1 << 20:  # 病态上游：无分帧的超长流，弃帧防涨
                        log.warning("pm event stream frame oversized; dropped")
                        buf = b""
                    break
                raw, buf = buf[:cut], buf[cut + 2:]
                data_lines = [
                    ln[5:].lstrip() for ln in raw.decode("utf-8",
                                                         errors="replace").split("\n")
                    if ln.startswith("data:")
                ]
                if not data_lines:
                    continue  # 注释/keepalive 行（":"开头）或空事件
                try:
                    ev = json.loads("\n".join(data_lines))
                except ValueError:
                    log.warning("pm event stream: non-JSON data frame dropped")
                    continue
                if not isinstance(ev, dict):
                    continue
                kind = ev.get("kind")
                if not isinstance(kind, str) or kind not in kinds:
                    continue
                frame = dict(ev)
                frame["t"] = "pm.event"  # 透传保留 msgid/payload/快照标记等
                await self._reply(frame)

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

    def drop_pending_audio(self) -> None:
        """打断时丢弃尚未下发的 TTS 音频（保留文本/事件帧）。"""
        if self._close_sent:
            return
        kept = deque(
            item for item in self._out if item[0] != "bytes" or item[1] is self._progress_slot
        )
        dropped = len(self._out) - len(kept)
        self._out.clear()
        self._out.extend(kept)
        if dropped:
            self.stats["audio_dropped_on_interrupt"] = (
                self.stats.get("audio_dropped_on_interrupt", 0) + dropped
            )

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

    async def _send_error(self, code: str, msg: str,
                          req_id: str | None = None) -> None:
        """错误帧（契约形态，与前端共享）：``{"type":"error",
        "req_id"?: str, "message": str}`` —— req_id 仅在收到时携带，供
        请求级错误回填关联。``t:"error"`` 是本应用 ws 帧路由键，永久保留
        （勿删）；旧字段 ``code``/``msg`` 与新字段同帧同发（web 控制台
        渲染按新字段优先、旧字段兜底；细分码仅旧字段携带）。"""
        frame = {"t": "error", "code": code, "msg": msg,
                 "type": "error", "message": msg}
        if req_id is not None:
            frame["req_id"] = req_id
        await self._reply(frame)

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
        await self._pm_sub_teardown()
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

    def active_conv_id(self) -> str | None:
        """任一在席语音会话的 conv_id（台账 conv_id 尽力回填用；可 None）。"""
        return next((s.conv_id for s in self._active if s.conv_id), None)

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


# ---- 台账接线（KG 14 §2.1/§2.2，PR1）----

_store: Any = None  # SessionStore 模块级单例（main() 实例化；None=不可用）


def init_store() -> Any:
    """实例化台账单例；main()（与单测）入口调用，幂等。

    路径由 :class:`rt_session_store.SessionStore` 自取 env ``VOICE_STORE_DB``、
    缺省 ``~/.local/state/voice-gateway/store.db``。失败只 stderr 告警并保持
    None——台账缺席不影响网关与派发链路（PR1 硬约束）。
    """
    global _store
    if _store is None:
        try:
            from rt_session_store import SessionStore

            _store = SessionStore()
            log.info("session store at %s", _store.path)
        except Exception as e:  # noqa: BLE001 — 台账是可选件
            print(f"rt_gateway: session store unavailable: {e}", file=sys.stderr)
    return _store


def _get_store() -> Any:
    """台账单例读取；不在此处创建（未初始化=不可用，防止单测触达真实库）。"""
    return _store


def _workspace_root() -> str:
    """文件工具的工作区根：``VOICE_WORKSPACE`` 覆写，缺省取仓库根。"""
    env = (os.environ.get("VOICE_WORKSPACE") or "").strip()
    if env:
        return env
    return str(Path(__file__).resolve().parents[2])


# ---- 白板（PR10：客户端文本输入区 ↔ head 读取工具的中转）----

_WHITEBOARD_MAX_CHARS = 65536
_WHITEBOARD: dict = {"text": "", "ts": 0.0}


def _whiteboard_get() -> dict:
    """head 工具读取面：当前白板内容 + 更新时间（app_resources 注入）。"""
    return {"text": _WHITEBOARD["text"], "ts": _WHITEBOARD["ts"]}


def _whiteboard_reset() -> None:
    """单测隔离：清空全局白板。"""
    _WHITEBOARD["text"] = ""
    _WHITEBOARD["ts"] = 0.0


# ---- head 配置注册表（PR8：多 head 配置，激活单例）----

_HEAD_REGISTRY: Any = None


def _head_registry() -> Any:
    """注册表惰性单例；首载打一行日志（文件位形 + 当前激活）。"""
    global _HEAD_REGISTRY
    if _HEAD_REGISTRY is None:
        from rt_head_registry import load_head_registry

        _HEAD_REGISTRY = load_head_registry()
        if _HEAD_REGISTRY.file_backed:
            log.info("head profiles: %d from %s (active=%s)",
                     len(_HEAD_REGISTRY.profiles), _HEAD_REGISTRY.path,
                     _HEAD_REGISTRY.active)
        else:
            log.info("head profiles: single-head mode (no profiles file)")
    return _HEAD_REGISTRY


def _reset_head_registry() -> None:
    """单测用：丢掉缓存的单例，下一次 _head_registry() 重载。"""
    global _HEAD_REGISTRY
    _HEAD_REGISTRY = None


def _profile_doctrine(profile: Any) -> str:
    """profile 的 doctrine 解析：inline → doctrine_file → 旧路径
    （``VOICE_HEAD_DOCTRINE`` 外置文件 → 内置常量）。坏文件只告警回落。"""
    if profile.doctrine:
        return profile.doctrine
    if profile.doctrine_file:
        try:
            with open(profile.doctrine_file, encoding="utf-8") as fh:
                text = fh.read()
            if text.strip():
                return text
            print(f"rt_gateway: doctrine_file {profile.doctrine_file} blank; "
                  "falling back", file=sys.stderr)
        except OSError as e:
            print(f"rt_gateway: doctrine_file {profile.doctrine_file} "
                  f"unreadable ({e}); falling back", file=sys.stderr)
    from rt_head_tools import DoctrineSource

    return DoctrineSource().load()


def _profile_turn_detection(profile: Any) -> dict | None:
    """profile 的 VAD 旋钮：任一设了才组装 ``server_vad``；全 None →
    旧路径（``VOICE_TURN_*`` env → 服务端默认）。"""
    td = {field: getattr(profile, attr) for attr, field in (
        ("turn_silence_ms", "silence_duration_ms"),
        ("turn_prefix_ms", "prefix_padding_ms"),
        ("turn_threshold", "threshold"),
    ) if getattr(profile, attr) is not None}
    if td:
        return {"type": "server_vad", **td}
    return _turn_detection_from_env()


async def _store_call(fn: Callable, *a, **kw) -> Any:
    """store 调用统一包装：``asyncio.to_thread`` 包裹（store 为同步 API，
    见其模块头约定）；任何失败只 stderr 告警、返回 None，绝不抛——
    写入失败不得影响派发链路（PR1 硬约束）。"""
    try:
        return await asyncio.to_thread(fn, *a, **kw)
    except Exception as e:  # noqa: BLE001
        print(
            f"rt_gateway: store {getattr(fn, '__name__', '?')} failed: {e}",
            file=sys.stderr,
        )
        return None


def _first_line(text: str | None, limit: int) -> str:
    """正文首非空行截 ``limit`` 字（title=16/summary=60 机械生成，零 LLM）。"""
    line = next((ln.strip() for ln in (text or "").splitlines() if ln.strip()), "")
    return line[:limit]


def _index_entry(rec: dict) -> dict:
    """回放索引条目：轻字段、无 body/inline（与 store.list 形态对齐）。"""
    return {k: rec.get(k) for k in ("ref", "no", "status", "title", "summary", "chars", "ts")}


def _remember_body_index(gateway: "VoiceGateway", rec: dict) -> None:
    """台账条目入 ``topic_cache[BODY_PUSH_KIND]`` 索引：按 ref 去重、只留
    最近 :data:`BODY_INDEX_LIMIT` 条（订阅回放，兼掉 list 帧——裁决 #3）。"""
    items = gateway.topic_cache.setdefault(BODY_PUSH_KIND, {}).setdefault("items", [])
    items[:] = [e for e in items if e.get("ref") != rec.get("ref")]
    items.append(_index_entry(rec))
    del items[:-BODY_INDEX_LIMIT]


async def _store_bridge(kind: str, payload: dict, gateway: "VoiceGateway") -> None:
    """台账唯一写入面（KG 14 §2.2；裁决 #8，backend 不感知 store）。

    - ``orch.dispatch`` → ``put(accepted)``：no 由 store 受理时分配并回填入
      台账，事件本身不改不发
    - ``orch.done`` → ``update(done|cancelled)`` + emit ``body.push`` 轻通知
      ——正文取载荷 ``body``（PR3），旧 ``artifact`` 键兜底；正文只走
      body.push（裁决 #3）。cancel 语义两形态：``status:"cancelled"``
      （backend 现行，裁决 #10）或旧哨兵 ``artifact="(已取消)"``（兼容保留）
    - ``orch.failed`` → ``update(failed)`` + emit ``body.push``（错误文本
      取 body/artifact/error/reason/note 之首个非空）

    store 调用全部经 ``_store_call`` 吞并；轻通知照发（字段尽力）。
    """
    if kind not in ("orch.dispatch", "orch.done", "orch.failed"):
        return
    store = _get_store()
    ref = payload.get("ref")
    if store is None or not ref:
        return
    ts = float(payload.get("ts") or time.time())
    if kind == "orch.dispatch":
        rec = await _store_call(store.put, {
            "ref": ref,
            "no": payload.get("no"),
            "status": "accepted",
            "run_id": payload.get("run_id"),
            "credentials": payload.get("credentials") or [],
            "summary": payload.get("summary") or "已受理，转对接人执行",
            "conv_id": gateway.active_conv_id(),
            "ts": ts,
        })
        if isinstance(rec, dict):
            _remember_body_index(gateway, rec)
        return
    if kind == "orch.failed":
        status = "failed"
        text = str(payload.get("body") or payload.get("artifact")
                   or payload.get("error") or payload.get("reason")
                   or payload.get("note") or "执行失败")
    else:
        text = str(payload.get("body") or payload.get("artifact") or "")
        status = ("cancelled"
                  if text == CANCEL_ARTIFACT or payload.get("status") == "cancelled"
                  else "done")
    title, summary = _first_line(text, 16), _first_line(text, 60)
    rec = await _store_call(store.update, ref, status=status, body=text,
                            title=title, summary=summary)
    src = rec if isinstance(rec, dict) else {}
    push = {
        "ref": ref,
        "no": src.get("no") if src else payload.get("no"),
        "status": status,
        "title": title,
        "summary": summary,
        "chars": src.get("chars", len(text)),
        "inline": text if len(text) <= BODY_INLINE_MAX_CHARS else None,
        "ts": ts,
    }
    await gateway.bus.emit(BODY_PUSH_KIND, push)
    if isinstance(rec, dict):
        _remember_body_index(gateway, rec)


async def attach_store_bridge(gateway: "VoiceGateway") -> Callable[[], None] | None:
    """把台账写入面挂上总线——main() 与单测共用同一条路径（唯一写入面）。

    挂接时把 ``store.list`` 最近 :data:`BODY_INDEX_LIMIT` 条播种进
    ``topic_cache``（重启后续接回放索引）。台账未初始化时 stderr 告警并
    返回 None（body.push/body.get 静默停用）。
    """
    store = _get_store()
    if store is None:
        print("rt_gateway: body store not initialized; body.push/body.get disabled",
              file=sys.stderr)
        return None
    rows = await _store_call(store.list, BODY_INDEX_LIMIT)
    if rows:
        gateway.topic_cache[BODY_PUSH_KIND] = {"items": [_index_entry(r) for r in rows]}

    async def _bridge(kind: str, payload: dict) -> None:
        await _store_bridge(kind, payload, gateway)

    return gateway.bus.subscribe(_bridge)


# ---- 席位清理控制面（KG 14 §2.4）：fleet.cleanup → loopback + fleet.json ----

FLEET_CLEANUP_MODES = ("release", "end")
# loopback 会话真死操作。dsh web API 面无 ``session.end`` 路由；
# ``workspace.archiveSession`` 是该面唯一的会话退役操作——对 live 会话
# （running 与否）生效，未知会话回 ``error.code="session-not-found"``
# （即"会话已不在"分型）。
SESSION_END_METHOD = "workspace.archiveSession"
# 已死会话分型标记：archiveSession 的 error.code / 错误 message 用词
_GONE_MARKERS = ("session-not-found", "no such session")


async def _dsh_api(method: str, payload: dict) -> Any:
    """POST 一条 RPC 到 dsh web loopback API（镜像 rt_dsh_backend._dsh_api
    的小实现——网关不持有 backend 实例，observe 链路也要能发）。"""

    def _call() -> Any:
        wire = {"type": "client-request", "rpcId": str(uuid.uuid4()),
                "method": method, "payload": payload}
        req = urllib.request.Request(
            f"http://127.0.0.1:{os.environ.get('DSH_PORT', '3080')}/api/{method}",
            data=json.dumps(wire).encode(),
            headers={"content-type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())["result"]
        if not result.get("ok"):
            raise RuntimeError(f"{method}: {result.get('error')}")
        return result["value"]

    return await asyncio.to_thread(_call)


def _session_gone(e: BaseException) -> bool:
    """loopback 会话操作错误是否为"会话已不在"（容忍分型，正常清理场景）。"""
    text = str(e).lower()
    return any(marker in text for marker in _GONE_MARKERS)


def _fleet_path() -> str:
    """fleet.json 路径；``MAESTRO_FLEET`` env 覆写（与 session-spawn 同一约定）。"""
    return os.path.expanduser(
        os.environ.get("MAESTRO_FLEET", str(MAESTRO_DIR / "fleet.json")))


# fleet.json 写锁（与 maestro bin/fleet-touch 同一锁对象）：哨兵文件 =
# $MAESTRO_STATE（缺省 $MAESTRO_HOME|~/.dsh 下 maestro/state）/
# <fleet.json basename>.lock —— 独立 inode，不随 os.replace 换代。
# 等待上限与自旋步长：fleet 状态是缓存性快照，拿不到锁放弃本次更新并
# 如实回包（FleetLockTimeout，B2）。
FLEET_LOCK_WAIT_S = 2.0
FLEET_LOCK_SPIN_S = 0.05


def _fleet_lock_path(fleet_path: str) -> str:
    """fleet-touch 的 flock 哨兵锁文件路径（完全同款解析，勿漂移）。"""
    state = os.environ.get("MAESTRO_STATE") or os.path.join(
        os.environ.get("MAESTRO_HOME", os.path.expanduser("~/.dsh")),
        "maestro", "state")
    return os.path.join(
        state, os.path.basename(os.path.abspath(fleet_path)) + ".lock")


class FleetLockTimeout(Exception):
    """fleet.json 写锁在 ``FLEET_LOCK_WAIT_S`` 预算内不可得.

    读-改-写未发生（未读未写，条目保持原样）；调用方按
    ``fleet-lock-timeout`` 口径如实回包（B2）。
    """


def _write_json_atomic(path: str, obj: dict) -> None:
    """tempfile + os.replace 原子写（镜像 session-spawn 范式：读者要么见
    旧文件要么见新文件，永不见半写截断态）。

    无锁——需要 fleet.json 写互斥的调用方走 :func:`_fleet_update`
    （锁内读-改-写全程，B1）。失败抛 OSError。
    """
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".fleet-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


async def _fleet_update(path: str, fn: Callable[[dict], Any]) -> Any:
    """fleet.json 锁内读-改-写唯一通道（B1：锁覆盖 读→fn→写 全程）.

    与 maestro bin/fleet-touch 的 flock 哨兵锁互斥：获锁 → 读 fleet.json →
    ``fn(fleet)``（就地修改；返回值透传给调用方）→ dict 有变更才原子写
    （无变更零写：免空写触发 fleet.snapshot 重发）→ 放锁。全程经
    ``asyncio.to_thread`` 执行——阻塞的 LOCK_NB 自旋与文件 IO 不占事件
    循环。``fn`` 抛错 → 不写、锁照放、异常透传（条目保持原样）。

    Returns:
        ``fn`` 的返回值。

    Raises:
        FleetLockTimeout: ``FLEET_LOCK_WAIT_S`` 内未获锁（未读未写）。
        OSError: 锁文件不可用 / fleet.json 读写失败。
        ValueError: fleet.json 不是合法 JSON。
    """

    def _run() -> Any:
        lock_path = _fleet_lock_path(path)
        os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
        lock_fh = open(lock_path, "a+", encoding="utf-8")
        try:
            deadline = time.monotonic() + FLEET_LOCK_WAIT_S
            while True:
                try:
                    fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        log.warning("fleet lock %s busy >%.1fs; skip update",
                                    lock_path, FLEET_LOCK_WAIT_S)
                        raise FleetLockTimeout(lock_path)
                    time.sleep(FLEET_LOCK_SPIN_S)
            with open(path, encoding="utf-8") as fh:
                fleet = json.load(fh)
            before = json.dumps(fleet, ensure_ascii=False, indent=2)
            result = fn(fleet)
            if json.dumps(fleet, ensure_ascii=False, indent=2) != before:
                _write_json_atomic(path, fleet)
            return result
        finally:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
            lock_fh.close()

    return await asyncio.to_thread(_run)


def _liaison_state_path() -> str:
    """liaison 绑定文件路径（``VOICE_LIAISON_STATE`` 覆写；与
    rt_dsh_backend.DshBackend._liaison_state_path 同一 env 口径——网关
    不持有 backend 实例，两侧镜像）。"""
    return os.path.expanduser(os.environ.get(
        "VOICE_LIAISON_STATE", "~/.local/state/voice-gateway/liaison.json"))


def _liaison_bound_state() -> dict:
    """liaison.json 绑定 ``{code, sessionId}``；缺席/坏文件→``{}``。"""
    try:
        with open(_liaison_state_path(), encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, ValueError):
        return {}
    if not isinstance(doc, dict):
        return {}
    out = {"code": str(doc.get("code") or ""),
           "sessionId": str(doc.get("sessionId") or "")}
    return out if (out["code"] or out["sessionId"]) else {}


def _liaison_bound_code() -> str:
    """当前 liaison 绑定四码（liaison.json 的 code；缺席/坏文件→空串）。"""
    return _liaison_bound_state().get("code") or ""


def _liaison_bound_clear() -> bool:
    """删除 liaison 绑定文件（绑定席位已被清理出册/归档：下次派发应
    AUTO_NEW 换新而非续投旧席）。返回是否确实删除；无残留与失败均
    False。与 rt_dsh_backend.DshBackend._liaison_bound_clear 同一
    VOICE_LIAISON_STATE 口径——网关不持有 backend 实例，此处为镜像。"""
    path = os.path.expanduser(os.environ.get(
        "VOICE_LIAISON_STATE", "~/.local/state/voice-gateway/liaison.json"))
    try:
        os.unlink(_liaison_state_path())
    except FileNotFoundError:
        return False
    except OSError as e:
        log.warning("liaison state clear failed: %s", e)
        return False
    return True


def _liaison_payload(bound: dict, archived: set[str] | None) -> dict:
    """liaison 绑定对象（fleet.brief 顶层与 liaison.result 共用形态）：
    ``{bound, code, sessionId, archived}``；归档核验降级（archived 为
    None）时 archived=False（不臆断）。"""
    sid = str(bound.get("sessionId") or "")
    return {"bound": bool(sid),
            "code": bound.get("code") or None,
            "sessionId": sid or None,
            "archived": bool(sid and archived is not None and sid in archived)}


async def _liaison_view_after() -> dict:
    """操作后的当前绑定视图（liaison.bind 失败帧用：绑定未动即旧值）。"""
    return _liaison_payload(_liaison_bound_state(), await _archived_session_ids())


# ---- 席位状态简报（PR9）：fleet.json join session.list 活性 ----

SESSION_LIST_METHOD = "session.list"
WORKSPACE_LIST_METHOD = "workspace.list"
# session.list loopback 预算：_dsh_api 自身 urlopen 30s 兜底太宽，简报是
# 交互面（用户在等回话），超时即降级纯席位表
BRIEF_DSH_TIMEOUT_S = 8.0
BRIEF_DSH_NOTE = "dsh 状态不可达，仅席位表"
# workspace.list（归档核验）不可达：live 口径退回「在列即活」并附注说明
BRIEF_ARCH_NOTE = "归档核验不可达，live 含可能已归档席位"


def _brief_task_status(long_task: Any) -> str:
    """longTask 投影里的任务状态字段（实测形态 ``{"task": {"phase": …},
    "roundsStarted": …}``，无长任务会话为 ``null``）。

    防御式取值：顶层与嵌套 ``task`` 各依 status/phase/state 序取首个非空
    串，取不到空串，绝不抛。
    """
    if not isinstance(long_task, dict):
        return ""
    for scope in (long_task, long_task.get("task")):
        if not isinstance(scope, dict):
            continue
        for key in ("status", "phase", "state"):
            value = scope.get(key)
            if isinstance(value, str) and value:
                return value
    return ""


ARCHIVED_CACHE_TTL_S = 30.0
# 归档集模块级缓存（B3）：{"at": 取得时刻（_archived_clock 口径）|None,
# "ids": set|None}——at=None 即空。失败（None 结果）不缓存：下一次调用
# 即重试。cleanup 归档成功/条目摘除后 _reset_archived_cache() 立即失效
# （后续 brief 不再看到幽灵席位）。
_ARCHIVED_CACHE: dict = {"at": None, "ids": None}
# 时钟注入面（单测翻 TTL 用）；缺省单调时钟
_archived_clock: Callable[[], float] = time.monotonic


def _reset_archived_cache() -> None:
    """立即失效归档集缓存（cleanup 归档/摘除成功后调用；单测隔离同用）。"""
    _ARCHIVED_CACHE["at"] = None
    _ARCHIVED_CACHE["ids"] = None


async def _archived_session_ids() -> set[str] | None:
    """归档会话集（workspace.list 的 archivedSessionIds）——网关侧等价
    helper，镜像 rt_dsh_backend.DshBackend._archived_session_ids。

    已知 wire 形态是顶层 ``{"items": […], "archivedSessionIds": [sid…]}``；
    递归遍历收集以容忍结构嵌套/演进。不可达 → None：调用方降级为不核验
    并附 note（核验失败不得阻断简报）。

    模块级缓存（B3）：成功结果缓存 :data:`ARCHIVED_CACHE_TTL_S`
    （时钟经 ``_archived_clock``，可注入）——窗口内重复调用零 loopback
    往返（brief 是交互面，逐次 workspace.list 过重）。失败不缓存。
    """
    now = _archived_clock()
    if (_ARCHIVED_CACHE["at"] is not None
            and now - _ARCHIVED_CACHE["at"] < ARCHIVED_CACHE_TTL_S):
        return _ARCHIVED_CACHE["ids"]
    try:
        value = await asyncio.wait_for(
            _dsh_api(WORKSPACE_LIST_METHOD, {}), timeout=BRIEF_DSH_TIMEOUT_S)
    except Exception as e:  # noqa: BLE001 — 降级信号，非致命
        log.warning("workspace.list archived check failed: %s", e)
        return None
    ids: set[str] = set()

    def _walk(node) -> None:
        if isinstance(node, dict):
            raw = node.get("archivedSessionIds")
            if isinstance(raw, list):
                ids.update(str(s) for s in raw if isinstance(s, str))
            for child in node.values():
                _walk(child)
        elif isinstance(node, list):
            for child in node:
                _walk(child)

    if isinstance(value, dict):
        _walk(value)
    _ARCHIVED_CACHE["at"] = _archived_clock()
    _ARCHIVED_CACHE["ids"] = ids
    return ids


# 席位口径 status 白名单（B4，词表实据 maestro bin/）：``active`` 是
# session-spawn 的出生值（bin/session-spawn）也是 fleet-touch 心跳的
# 缺省状态；``verified`` 是 fleet-probe 终端准入通过（bin/fleet-probe）。
# 词表其余——``probing``/``mismatch``（fleet-probe 探测中/回报不符）、
# ``stale``（fleet-probe --reverify 判失效）、``retired``（fleet-touch
# sweep --apply 退役）、运维经 fleet-touch --status 自设的任意值
# （idle/paused/…）及缺席——不占席位列，只计入顶层 ``inactive`` 数。
SEAT_STATUSES = ("active", "verified")


async def _fleet_brief_payload() -> dict:
    """席位状态简报 payload（PR9 + B4 席位口径过滤）：fleet.json join dsh.

    席位口径（B4）：fleet 条目 ``status`` ∈ :data:`SEAT_STATUSES`
    （``active``/``verified``，词表实据见其注释）才入 ``seats``；其余
    status 一律不占席位列，只计入顶层 ``inactive`` 数（恒在场——含
    dsh 降级形态）。

    join 口径：sessionId 出现在 session.list 条目且不在归档集 → live=true、
    running/title/task/idle_s 取该条目（title=projections.values.title 缺则
    空串、task=longTask 投影状态、idle_s=(now-updatedAt)/1000 下限 0，
    updatedAt 为毫秒纪元）；不在列表或已归档（workspace.list 的
    archivedSessionIds；归档席位留在 session.list 里但不得视为活——
    幽灵 worker 口径修正）→ live=false、running=false、title/task 空、
    idle_s=null。席位行透传 ``last_seen``=条目 ``lastSeenAt``，缺则
    ``heartbeatAt``，皆无则 null（ISO 串原样，fleet-touch 心跳口径）。

    顶层恒带 ``liaison`` 绑定对象：无绑定 ``{"bound": false, "code":
    null, "sessionId": null, "archived": false}``；有绑定填 code/sessionId，
    archived=绑定 sid 是否在归档集（核验降级时 false）。

    loopback 失败/超时（wait_for 8s）→ 降级 ``{seats:[纯席位字段],
    note, inactive, liaison}``；session.list 通而 workspace.list（归档
    核验）败 → live 口径退回「在列即活」附 BRIEF_ARCH_NOTE；
    fleet.json 读不了 → 抛（调用方回 internal 错误帧）。
    """
    path = _fleet_path()
    with open(path, encoding="utf-8") as fh:
        fleet = json.load(fh)
    entries = fleet.get("fleet") if isinstance(fleet, dict) else None
    if not isinstance(entries, dict):
        raise ValueError("fleet.json has no fleet table")

    def _seat(code: str, entry: dict) -> dict:
        seen = entry.get("lastSeenAt") or entry.get("heartbeatAt") or None
        return {"id": code, "node": str(entry.get("node") or ""),
                "role": str(entry.get("role") or ""),
                "status": str(entry.get("status") or ""),
                "last_seen": seen}

    codes = [code for code, entry in entries.items() if isinstance(entry, dict)]
    seat_codes = [c for c in codes
                  if str(entries[c].get("status") or "") in SEAT_STATUSES]
    inactive = len(codes) - len(seat_codes)
    bound = _liaison_bound_state()
    liaison_sid = str(bound.get("sessionId") or "")
    liaison = {"bound": bool(liaison_sid),
               "code": bound.get("code") or None,
               "sessionId": liaison_sid or None,
               "archived": False}
    try:
        value = await asyncio.wait_for(
            _dsh_api(SESSION_LIST_METHOD, {}), timeout=BRIEF_DSH_TIMEOUT_S)
        items = value.get("items") if isinstance(value, dict) else None
        by_sid = {str(it.get("sessionId")): it for it in (items or [])
                  if isinstance(it, dict) and it.get("sessionId")}
    except Exception as e:  # noqa: BLE001 — loopback 不可达 → 纯席位表降级
        log.warning("fleet.brief session.list failed: %s", e)
        return {"seats": [_seat(c, entries[c]) for c in seat_codes],
                "note": BRIEF_DSH_NOTE, "inactive": inactive, "liaison": liaison}

    archived = await _archived_session_ids()
    note = BRIEF_ARCH_NOTE if archived is None else None
    if liaison_sid and archived is not None and liaison_sid in archived:
        liaison["archived"] = True

    now_ms = time.time() * 1000
    seats: list[dict] = []
    for code in seat_codes:
        entry = entries[code]
        sid = str(entry.get("sessionId") or "")
        item = by_sid.get(sid) if sid else None
        if item is not None and archived is not None and sid in archived:
            item = None  # 在列但已归档：不可投，按不活上报
        seat = _seat(code, entry)
        if item is None:
            seat.update(live=False, running=False, title="", task="", idle_s=None)
        else:
            projections = item.get("projections")
            values = projections.get("values") if isinstance(projections, dict) else None
            values = values if isinstance(values, dict) else {}
            updated = item.get("updatedAt")
            idle_s = (max(0.0, (now_ms - updated) / 1000)
                      if isinstance(updated, (int, float)) else None)
            seat.update(live=True, running=bool(item.get("running")),
                        title=str(values.get("title") or ""),
                        task=_brief_task_status(values.get("longTask")),
                        idle_s=idle_s)
        seats.append(seat)
    payload = {"seats": seats, "inactive": inactive, "liaison": liaison}
    if note is not None:
        payload["note"] = note
    return payload


# ---- pm-host-service 路由面（GW-001，<internal-repo> spec-gateway）----
# 纯路由零业务（ADR-004）：发现（pm.port 的 port 字段）→ GET /<op>?params →
# pm.res。pm-host-service 只读（ADR-002），GET 一个动词走到底。

_PM_PORT_CACHE: dict = {"sig": None, "port": None}  # (mtime_ns,size)→port 签名缓存


def _reset_pm_port_cache() -> None:
    """单测隔离：丢弃 pm.port 签名缓存（下次 :func:`_pm_port` 重读文件）。"""
    _PM_PORT_CACHE.update(sig=None, port=None)


def _pm_port_path() -> str:
    """pm.port 端口文件路径；``PM_HOST_PORT_FILE`` env 覆写（单测指替身）。"""
    return os.path.expanduser(
        os.environ.get("PM_HOST_PORT_FILE", str(MAESTRO_DIR / "pm.port")))


def _pm_port() -> int | None:
    """服务发现：pm.port JSON 的 ``port`` 字段；缺席/坏文件/无字段→None.

    签名缓存（mtime_ns+size，FileTailer snapshot 同款）：服务重启换端口必
    重写 pm.port，签名变化自动失效。服务死时文件残留旧值——那是连接层的
    事（探活分级兜底），发现层不因文件在而误判服务活。
    """
    path = _pm_port_path()
    try:
        st = os.stat(path)
        sig = (st.st_mtime_ns, st.st_size)
    except OSError:
        _PM_PORT_CACHE.update(sig=None, port=None)
        return None
    if _PM_PORT_CACHE["sig"] == sig:
        return _PM_PORT_CACHE["port"]
    try:
        with open(path, encoding="utf-8") as fh:
            port = int(json.load(fh)["port"])
    except (OSError, ValueError, KeyError, TypeError):
        return None
    _PM_PORT_CACHE.update(sig=sig, port=port)
    return port


class _PMUnreachable(Exception):
    """pm-host-service 传输级失败（拒绝/超时/读断）——与非 2xx 分型开。"""


def _pm_op_path(op: str) -> str:
    """op → 上游路径：业务读一律 ``op/<op>`` 前缀，``health`` 除外（根下）.

    G3 热修（编排裁决 dae894c2）：真服 v0.7.0 业务端点全在 ``/op/`` 下，
    仅 ``/health`` 在根——纯机械前缀规则，ADR-004 零业务。"""
    return op if op == "health" else f"op/{op}"


def _pm_http_get(port: int, op: str, query: str, timeout: float) -> tuple[int, bytes]:
    """同步 GET（线程池里跑）：返回 (status, body)；非 2xx 经 HTTPError 把
    错误体一并读出透传；传输级失败折进 :class:`_PMUnreachable`。"""
    url = f"http://127.0.0.1:{port}/{_pm_op_path(op)}" + (f"?{query}" if query else "")
    req = urllib.request.Request(url, headers={"accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        with e:
            return e.code, e.read()
    except Exception as e:  # noqa: BLE001 — URLError/socket.timeout 等一律传输失败
        raise _PMUnreachable(str(e) or e.__class__.__name__) from e


class PMUpstreamError(Exception):
    """pm-host-service 调用失败的结构化载体（``pm.res{error}`` 的 error 值）."""

    def __init__(self, error: dict) -> None:
        super().__init__(str(error.get("message") or error.get("code") or "pm error"))
        self.error = error


async def _pm_probe(port: int) -> bool:
    """``GET /health`` 探活（短预算）；任何失败=False。"""
    try:
        status, _ = await asyncio.wait_for(
            asyncio.to_thread(_pm_http_get, port, "health", "", PM_PROBE_TIMEOUT_S),
            PM_PROBE_TIMEOUT_S + 1.0,
        )
    except Exception:  # noqa: BLE001 — 探活只回答活/死
        return False
    return status == 200


async def _pm_call(op: str, query: str) -> Any:
    """一次纯透传往返：``GET /<op>?<query>`` → 服务 JSON（GW-001）.

    2xx → 解析后的服务 JSON；非 2xx → ``pm_status``（status+upstream 原样
    附上）；传输失败 → ``GET /health`` 探活分级 ``pm_down``（探活也死）/
    ``pm_unreachable``（探活通但目标调用传输失败）；调用总预算
    :data:`PM_REQ_TIMEOUT_S` 超过 → ``pm_timeout``。全程只抛
    :class:`PMUpstreamError`（载荷即 pm.res 的 error）。
    """
    port = _pm_port()
    if port is None:
        raise PMUpstreamError({"code": "pm_unavailable",
                               "message": f"pm.port unreadable: {_pm_port_path()}"})
    try:
        status, raw = await asyncio.wait_for(
            asyncio.to_thread(_pm_http_get, port, op, query, PM_REQ_TIMEOUT_S),
            PM_REQ_TIMEOUT_S + 2.0,
        )
    except _PMUnreachable as e:
        healthy = await _pm_probe(port)
        raise PMUpstreamError({
            "code": "pm_down" if not healthy else "pm_unreachable",
            "message": str(e)[:160],
            "probe": "health-ok" if healthy else "health-failed",
        }) from e
    except asyncio.TimeoutError:
        raise PMUpstreamError({"code": "pm_timeout",
                               "message": f"upstream no reply within "
                                          f"{PM_REQ_TIMEOUT_S:.0f}s"}) from None
    if status != 200:
        try:
            upstream = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            upstream = raw.decode("utf-8", errors="replace")[:500]
        raise PMUpstreamError({"code": "pm_status", "status": status,
                               "message": f"upstream HTTP {status}",
                               "upstream": upstream})
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise PMUpstreamError({"code": "pm_bad_payload",
                               "message": "upstream 200 body is not JSON"}) from None


# ---- live 阶段（W4.1 e2e）：真 pipecat 头，惰性导入，单测不触达 ----


_live_heads: dict = {"head": None, "pending": []}  # 活 head + 无 head 期到达的终稿 (ref, 注入串)

# live DshBackend 槽（W3 ``run.cancel`` 控制帧的取消面）：build_realtime_head
# 接线时填入。backend 与 main() 同生命周期、跨语音会话存活，故不随单个
# head 的 stop 清除；echo 模式/未建 live 头时恒 None（run.cancel 回 internal）。
_live_backend: dict = {"backend": None}


def _get_backend() -> Any:
    """live DshBackend 单例读取（None=未接线：echo 模式/未建 live 头）。"""
    return _live_backend["backend"]

# 终稿注入串行锁：并发的终稿（phase-2 多路 + 会话补投）排队走同一个
# active-response 槽，否则后到的 response.create 撞前一个被服务端静默丢弃。
_final_inject_lock: asyncio.Lock = asyncio.Lock()


def _final_mode() -> str:
    """终稿投递形态（KG 14 裁决 #2；PR5 翻缺省 split）：仅 ``VOICE_FINAL_MODE``
    精确等于 ``"fulltext"`` 时走旧全文注入（回退路径），其余值（含未设）走
    台账通报——policy 只在 gateway，backend 不感知；改 env 即切形态，无需
    重启即回退。"""
    return "fulltext" if os.environ.get("VOICE_FINAL_MODE") == "fulltext" else "split"


def _turn_detection_from_env() -> dict | None:
    """服务端 VAD 旋钮（KG 14 之外的语音面调参）：任一 env 设了才组装
    ``server_vad`` 配置，否则 None→服务端默认（不猴急调节前保持原样）。

    - ``VOICE_TURN_SILENCE_MS``：静音多久判定说完（调大→head 不抢话）
    - ``VOICE_TURN_PREFIX_MS``：判定前的音频回带
    - ``VOICE_TURN_THRESHOLD``：0..1 触发灵敏度（调大→不易被噪音触发）
    """
    td: dict = {}
    for key, field, cast in (
        ("VOICE_TURN_SILENCE_MS", "silence_duration_ms", int),
        ("VOICE_TURN_PREFIX_MS", "prefix_padding_ms", int),
        ("VOICE_TURN_THRESHOLD", "threshold", float),
    ):
        raw = (os.environ.get(key) or "").strip()
        if not raw:
            continue
        try:
            td[field] = cast(raw)
        except ValueError:
            print(f"rt_gateway: {key}={raw} unparseable; ignored", file=sys.stderr)
    return {"type": "server_vad", **td} if td else None


async def _final_injection_text(ref: str, final: str) -> str:
    """组终稿注入串（fulltext/split 两形态；KG 14 §2.3，PR3）。

    - fulltext：``[编排终稿]`` 全文 + 播报指令——回退路径，字节级保持旧形。
    - split：``[编排通报]`` + 单行 JSON（no/ref/status/summary/chars），字段
      取台账——backend 先 emit ``orch.done`` 再调 ``on_final``，store 此时已
      终态。store 不可用或查不到时降级：ref 照旧、status/summary/chars 从
      final 本体算（剥 FINAL_PREFIX 后取首行 60 字与长度），no 省略，stderr
      告警。通报后不接任何指令句（裁决 #7）——播报行为只在 doctrine（PR2
      已有 ``[编排通报]`` 条款），载荷嵌指令即漂移源。

    pending 缓冲/补投在调用方，两形态共用（本函数返回时注入串已定形）。
    """
    if _final_mode() != "split":
        return (f"[编排终稿 {ref}] {final}\n"
                "请把上述终稿口语播报给用户：原样转述，不添加事实。")
    store = _get_store()
    rec = await _store_call(store.get, ref) if store is not None else None
    if isinstance(rec, dict):
        notice = {"no": rec.get("no"), "ref": ref, "status": rec.get("status"),
                  "summary": rec.get("summary"), "chars": rec.get("chars")}
    else:
        body = final[len(FINAL_PREFIX):] if final.startswith(FINAL_PREFIX) else final
        notice = {"ref": ref, "status": "done", "summary": _first_line(body, 60),
                  "chars": len(body)}
        print(f"rt_gateway: final {ref} notice degraded "
              "(store unavailable or ref miss); summary/chars from final body",
              file=sys.stderr)
    line = json.dumps(notice, ensure_ascii=False, separators=(",", ":"))
    return f"{FINAL_NOTICE_PREFIX}{line}"


async def _inject_final_when_idle(
    head: Any,
    text: str,
    *,
    idle_wait_s: float = 120.0,
    on_injected: Callable[[], Awaitable[None]] | None = None,
) -> bool:
    """终稿以新 user turn 注入；turn 进行中先排队等其结束（不打断用户）。

    ``head.turn_idle`` 是协议级回合信号（response.created 清 / response.done
    置）。无该信号的 head（echo 头、测试替身）直接注入，保持旧行为。等待超
    时降级为尽力注入。注入成功（response.create 已发）后调用 ``on_injected``
    （notify 相上报）再返回 True；失败路径不调用。注入失败返回 False，由
    调用方重排队不丢终稿。
    """
    from pipecat.services.openai.realtime import events as rt_events

    idle = getattr(head, "turn_idle", None)
    async with _final_inject_lock:
        if idle is not None and not idle.is_set():
            try:
                await asyncio.wait_for(idle.wait(), timeout=idle_wait_s)
            except asyncio.TimeoutError:
                pass  # 长独白：降级为尽力注入（同旧行为）
        try:
            await head.send_client_event(rt_events.ConversationItemCreateEvent(
                item=rt_events.ConversationItem(
                    type="message", role="user",
                    content=[rt_events.ItemContent(type="input_text", text=text)])))
            await head.send_client_event(rt_events.ResponseCreateEvent())
            if on_injected is not None:
                await on_injected()
            return True
        except Exception as e:  # noqa: BLE001 — 失败由调用方重排队
            print(f"rt_gateway: final inject failed: {e}", file=sys.stderr)
            return False


# ---- 会话压缩（KG 14 §2.5，PR5）：零 LLM 确定性快照 ----

COMPACT_KIND = "head.compact"
COMPACT_IDLE_WAIT_S = 30.0


def _compact_threshold() -> int:
    """压缩触发阈值：``VOICE_COMPACT_CHARS`` 覆写（缺省 20000 字），``0``
    关闭压缩；非法值回落缺省。"""
    try:
        return int(os.environ.get("VOICE_COMPACT_CHARS", ""))
    except ValueError:
        return 20_000


async def _send_item_delete(head: Any, item_id: str) -> None:
    """发一条 ``conversation.item.delete``：realtime 头有 typed 助手方法
    （qwen）优先，鸭子头（测试替身）回落裸事件。"""
    deleter = getattr(head, "delete_conversation_item", None)
    if deleter is not None:
        await deleter(item_id)
        return
    from pipecat.services.openai.realtime import events as rt_events

    await head.send_client_event(rt_events.ConversationItemDeleteEvent(item_id=item_id))


async def _run_compaction(
    head: Any,
    log: Any,
    compactor: Any,
    *,
    conv_id: str | None,
    backend: Any = None,
    emit: Callable[[str, dict], Awaitable[None]],
    idle_wait_s: float = COMPACT_IDLE_WAIT_S,
) -> bool:
    """执行一次压缩（kg/14 §2.5）。全程持终稿注入锁——不与终稿注入竞态。

    锁内先复检阈值（并发的 turn_idle 信号可能已压缩过）、再等 turn_idle，
    然后逐项 ``conversation.item.delete`` 非 pinned 项（open 工具对钉住；
    每删一项同步镜像，部分失败即中止，服务端/镜像不漂移）→ 注入单条
    ``state.snapshot`` user item（快照串单行 JSON；tasks=store.list() ∪
    backend 运行登记−store，running 带 elapsed_s；零 LLM）→ emit
    ``head.compact``。不建新会话、session.instructions/doctrine 不动。
    返回是否完成了一次压缩。
    """
    from pipecat.services.openai.realtime import events as rt_events

    async with _final_inject_lock:
        if not compactor.should_compact():
            return False
        idle = getattr(head, "turn_idle", None)
        if idle is not None and not idle.is_set():
            try:
                await asyncio.wait_for(idle.wait(), timeout=idle_wait_s)
            except asyncio.TimeoutError:
                print("rt_gateway: compact skipped (turn never went idle)",
                      file=sys.stderr)
                return False  # 不降级：下一轮 turn_idle 复检再试
        plan = compactor.compact_plan()
        store = _get_store()
        rows = await _store_call(store.list) if store is not None else []
        running = [{"ref": ref, "ts": getattr(d, "ts", 0)}
                   for ref, d in (getattr(backend, "_runs", None) or {}).items()]
        snapshot = compactor.state_snapshot(rows or [], running, now=time.time())
        text = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
        for item_id in plan["delete_ids"]:
            try:
                await _send_item_delete(head, item_id)
            except Exception as e:  # noqa: BLE001 — 部分失败中止，下轮重试
                print(f"rt_gateway: compact delete {item_id} failed: {e}",
                      file=sys.stderr)
                return False
            log.drop(item_id)
        snap_id = "snap-" + uuid.uuid4().hex[:8]
        try:
            await head.send_client_event(rt_events.ConversationItemCreateEvent(
                item=rt_events.ConversationItem(
                    id=snap_id, type="message", role="user",
                    content=[rt_events.ItemContent(type="input_text", text=text)])))
        except Exception as e:  # noqa: BLE001
            print(f"rt_gateway: compact snapshot inject failed: {e}", file=sys.stderr)
            return False
        log.add(snap_id, {"type": "message", "role": "user", "text": text})
        await emit(COMPACT_KIND, {
            "conv_id": conv_id,
            "before_chars": plan["before_chars"],
            "after_chars": log.text_chars(),
            "pinned": plan["pinned"],
            "reason": "threshold",
            "ts": time.time(),
        })
        return True


def _arm_compaction(
    head: Any,
    log: Any,
    compactor: Any,
    *,
    session: Any,
    backend: Any,
    emit: Callable[[str, dict], Awaitable[None]],
) -> None:
    """挂镜像/阈值两个 tap 到 realtime 头（kg/14 §2.5）。

    ``mirror_sink`` 喂 :class:`ConversationLog`（服务端 item 镜像，added/
    done 都进，item_id 去重更新）；``on_turn_idle`` 在 response.done 置位后
    检查阈值，过线才排压缩任务（锁序由 ``_run_compaction`` 保证）。conv_id
    取触发时点的 ``session.conv_id``（构建时会话尚未领 id）。头无这两个
    回调面（非 qwen provider）时属性照设、无人调用，零影响。
    """
    def _mirror(item: dict) -> None:
        log.add(item["item_id"], item)

    def _on_idle() -> None:
        if not compactor.should_compact():
            return
        asyncio.create_task(_run_compaction(
            head, log, compactor, conv_id=session.conv_id,
            backend=backend, emit=emit))

    head.mirror_sink = _mirror
    head.on_turn_idle = _on_idle


async def build_realtime_head(session: "WsSession", bus: EventBus, backend: Any):
    """真 Qwen realtime 头接到 ws 会话（形制=poc_t6_pipeline.py）。

    providers 工厂 + dsh_head_tools() 工具面 + observer（transcript 镜像、
    TTS 下发、head.turn）；上行 InputAudioRawFrame 入管线，下行
    TTSAudioRawFrame → session.send_audio。
    """
    from pipecat.frames.frames import (
        EndFrame,
        FunctionCallInProgressFrame,
        InputAudioRawFrame,
        InterimTranscriptionFrame,
        LLMFullResponseEndFrame,
        LLMFullResponseStartFrame,
        LLMTextFrame,
        UserStartedSpeakingFrame,
        UserStoppedSpeakingFrame,
        InterruptionFrame,
        TranscriptionFrame,
        TTSAudioRawFrame,
    )
    from pipecat.pipeline.pipeline import Pipeline
    from pipecat.pipeline.worker import PipelineWorker
    from pipecat.observers.base_observer import BaseObserver
    from pipecat.processors.aggregators.llm_context import LLMContext
    from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
    from pipecat.workers.runner import WorkerRunner

    from providers import RealtimeHeadConfig, RealtimeProtocol, RealtimeProvider, create_realtime_head
    from rt_conversation_items import ConversationCompactor, ConversationLog
    from rt_head_tools import dsh_head_tools

    transcript = TranscriptState()
    tools = dsh_head_tools()
    turn_trace = TurnTrace(
        {
            "user_start": UserStartedSpeakingFrame,
            "user_end": UserStoppedSpeakingFrame,
            "user_text": TranscriptionFrame,
            "interim": InterimTranscriptionFrame,
            "assistant_start": LLMFullResponseStartFrame,
            "assistant_end": LLMFullResponseEndFrame,
            # 只认 LLMTextFrame：realtime 头对同一段文本同时推 LLMTextFrame
            # 与 TTSTextFrame（都是 TextFrame 子类），宽匹配会双计。
            "text": LLMTextFrame,
            "tool_call": FunctionCallInProgressFrame,
            "interrupted": InterruptionFrame,
        }
    )

    class _HeadObserver(BaseObserver):
        """transcript 镜像 + TTS PCM → ws 下行 + head.turn 上报（KG 11 §1）。

        观察者而非管线 processor：自定义 processor 插在 realtime 头与
        assistant aggregator 之间时，其 process 任务不随 StartFrame 建立，
        TTS/文本帧会堆积在该 processor 队列无人消费（实测复现）；observer
        挂在 push 边上，不参与帧流。回合帧是双向广播的（上下行各一个实
        例，id 互为 broadcast_sibling_id），同帧跨多段 push 还有同一 id——
        把帧自身 id 与 sibling id 一起标记为已见，任何一条边再来即跳过，
        每帧只记一次；上行专属帧（如 TranscriptionFrame）不受影响。
        """

        def __init__(self) -> None:
            super().__init__()
            self._seen: OrderedDict[int, None] = OrderedDict()

        async def on_push_frame(self, data) -> None:
            frame = data.frame
            ids = {frame.id, frame.broadcast_sibling_id} - {None}
            if any(i in self._seen for i in ids):
                return
            for i in ids:
                self._seen[i] = None
            if len(self._seen) > 1024:  # 有界：只留近期 id
                for _ in range(256):
                    self._seen.popitem(last=False)
            if isinstance(frame, InputAudioRawFrame):
                transcript.on_speech_started()
            elif isinstance(frame, TTSAudioRawFrame):
                session.send_audio(bytes(frame.audio))
            ev = turn_trace.on_frame(frame)
            if ev is not None:
                if ev.get("phase") == "interrupted":
                    session.drop_pending_audio()  # 掐断未下发的旧应答音频
                await bus.emit("head.turn", {"conv_id": session.conv_id, **ev})

    observer = _HeadObserver()

    # 会话压缩（kg/14 §2.5）：镜像 + 阈值 tap；阈值读一次（env 覆写）。
    conv_log = ConversationLog()
    compactor = ConversationCompactor(conv_log, trigger_chars=_compact_threshold())

    head_profile = _head_registry().active_profile()
    head = create_realtime_head(
        RealtimeHeadConfig(
            provider=RealtimeProvider.QWEN,
            protocol=RealtimeProtocol.DASHSCOPE_RT,
            # 激活 head 的 profile 字段优先；None 字段回落旧 env 单头路径
            # （模型与音色，实测 plus 受理 14 个音色：Tina/Cherry/Serena/
            # Ethan/Lily/Griffin/Dana/Sandy/Yuanjia/Jada/Alex/Aria/Nofish/
            # Loongzai）
            model=head_profile.model or (os.environ.get("VOICE_HEAD_MODEL") or None),
            voice=head_profile.voice or (os.environ.get("VOICE_HEAD_VOICE") or None),
            # profile.doctrine → profile.doctrine_file → VOICE_HEAD_DOCTRINE
            # 外置文件 → 内置常量
            system_instruction=_profile_doctrine(head_profile),
            # profile VAD 旋钮 → VOICE_TURN_* env → 服务端默认
            turn_detection=_profile_turn_detection(head_profile),
            tools=tools,
        )
    )
    _arm_compaction(head, conv_log, compactor,
                    session=session, backend=backend, emit=bus.emit)

    if getattr(backend, "liaison_mode", False) or getattr(
            backend, "liaison_session", ""):
        # Liaison finals come back on the voice-head dais mailbox; re-inject
        # each as a text user turn so the head speaks it (DashScope accepts
        # conversation.item.create with input_text content). Finals can land
        # minutes later, after the arming session closed — inject into the
        # currently live head instead of the arming one (a dead head's send
        # silently no-ops, losing the final). Payload form per VOICE_FINAL_MODE
        # (fulltext legacy string / split store notice), shaped in
        # _final_injection_text — pending buffering below is mode-agnostic.
        # Injection success also emits head.turn phase="notify" (kg/14 §2.3).
        def _notify(ref: str) -> Callable[[], Awaitable[None]]:
            async def _emit_notify() -> None:
                await bus.emit("head.turn", {"conv_id": session.conv_id,
                                             "phase": "notify", "ref": ref})
            return _emit_notify

        async def _on_final(ref: str, final: str) -> None:
            text = await _final_injection_text(ref, final)
            head = _live_heads["head"]
            if head is None:
                # 无活会话（用户已断开）：终稿不丢，等下一个会话接入补投
                _live_heads["pending"].append((ref, text))
                print(f"rt_gateway: final {ref} buffered (no live head, "
                      f"total={len(_live_heads['pending'])})", file=sys.stderr)
                return
            if not await _inject_final_when_idle(head, text, on_injected=_notify(ref)):
                # 注入失败（head 恰在断开窗口等）：同样不丢，重排队
                _live_heads["pending"].append((ref, text))
                print(f"rt_gateway: final {ref} requeued (inject failed)",
                      file=sys.stderr)

        backend.on_final = _on_final

    context = LLMContext(tools=tools)
    aggregators = LLMContextAggregatorPair(context)
    worker = PipelineWorker(
        Pipeline([aggregators.user(), head, aggregators.assistant()]),
        cancel_on_idle_timeout=False,
        observers=[observer],
        app_resources={"dsh_backend": backend, "voice_store": _get_store(),
                       "workspace_root": _workspace_root(),
                       "fleet_brief": _fleet_brief_payload,
                       "whiteboard": _whiteboard_get},
    )
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)
    run_task = asyncio.create_task(runner.run())
    _live_heads["head"] = head  # 终稿注入目标切到本会话
    _live_backend["backend"] = backend  # run.cancel 控制帧的取消面（W3）
    # 无会话期缓冲的终稿：新会话就绪即补投（session.updated 后注入才生效）
    if _live_heads["pending"]:

        async def _flush_pending() -> None:
            await asyncio.sleep(2.0)  # 等 session.update 完成
            while _live_heads["pending"]:
                ref, text = _live_heads["pending"][0]
                if await _inject_final_when_idle(head, text, on_injected=_notify(ref)):
                    _live_heads["pending"].pop(0)
                else:
                    break  # head 不可用：剩余终稿留给下一个会话补投

        asyncio.create_task(_flush_pending())

    # Realtime 模式用户回合不推 context（event-only），工具 handler 注册
    # 若依赖首个 context 帧会晚于会话第一次函数调用——首个 dispatch 撞上
    # 未注册窗口拿到占位结果。启动即投 LLMSetToolsFrame：注册 handler +
    # 服务端 session.update 广告工具，赶在任何音频之前。
    from pipecat.frames.frames import LLMSetToolsFrame

    await worker.queue_frame(LLMSetToolsFrame(tools=tools))

    class _RealtimeHeadAdapter:
        async def start(self) -> None:
            pass  # runner 已随构建启动

        async def stop(self) -> None:
            if _live_heads.get("head") is head:
                _live_heads["head"] = None  # 陈旧引用会让终稿注入静默 no-op
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

            backend = DshBackend(
                lane=lane,
                liaison_session=os.environ.get("VOICE_LIAISON_SESSION") or "",
            )

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
        # DshBackend 自建私有 EventBus（default_factory）；不回填 gateway.bus
        # 的话 orch.* 全进无人订阅的总线，语音客户端观测面全瞎。
        if not args.echo:
            backend.bus = gateway.bus
        # 台账接线（KG 14 §2.2，PR1）：main() 实例化模块级 SessionStore 单例
        # （env VOICE_STORE_DB 覆写路径）；写入面=attach_store_bridge 总线订阅
        # （裁决 #8 单点）。store 缺席只 stderr 告警，派发链路不受影响。
        init_store()
        await attach_store_bridge(gateway)
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
