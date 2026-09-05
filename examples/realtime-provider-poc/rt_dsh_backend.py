#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""DshBackend: head tool semantics over the two lanes (WS1; docs/kg/
01-ws1-head-dsh.md §1/§5).

Two-phase response contract (docs/kg/05-contracts.md §2):

- phase 1 (immediate): acceptance receipt. Liaison deliveries are slim
  by default (``{"status","ref","summary"}``, kg/14 §2.3 — run_id and
  credentials stay in ``_runs``/orch.dispatch only);
  ``VOICE_RECEIPT_SLIM=0`` restores the legacy full form
  (run_id/credentials/note).
- phase 2 (on done):   ``"Agent Final Message":\n\n<done body>`` re-injected
  into the head context via the pending-result callback.

Interrupt semantics: local playback interruption never cancels the remote
run; explicit ``cancel()`` does.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict, dataclass, field
from typing import Awaitable, Callable

from rt_a2a_client import A2aClient, A2aError
from rt_dsh_lane import DaisLane
from rt_event_bus import EventBus
from rt_orchestrator import FINAL_PREFIX, BackendResult, make_credential

CLARIFY_NOTE = '{"status": "clarify", "note": "无法拆解该意图，请补充信息"}'

# lane B-orca worker defaults (live-probed 2026-08-24, orca app 1.4.185 +
# omp on GLM-5.3): the artifact-exactness choreography follows the
# conformance _orca_worker dance (render-proof completion signal).
ORCA_AGENT_DEFAULT = "omp"
ORCA_WORKER_BUDGET_S = 300.0
ORCA_WAIT_SLICE_MS = 15000
ORCA_TURN_SETTLE_S = 20.0        # output window before the one interrupt+resend

# ---- T0 A1/A4 + 探针裁决 wire 形态层 (seatB-cut3-2/3-3/3-4) ---------------
# OLD 宿主: POST /api/<method> 点号 RPC。NEW 宿主 (席A 运行时探针已裁决,
# T0 "运行时探针 (OQ3 裁决)" 节): 无点号兼容层, 斜杠三要素 —
#   ① path /api/<ns>/<verb> (method 字段同步斜杠形);
#   ② payload 包一层 {args:{request:<原payload>}};
#   ③ browser-session cookie 鉴权 (401 裸 unauthorized, 机器客户端无豁免)。
# 形态自适配 = 懒探测 + 进程级缓存: 默认点号先行, 路由层 404/405 (请求
# 未被任何 handler 接受)时翻转形态重发一次, 路由已通即缓存; 宿主换代际
# 重启后缓存形态同样失效翻转 → OLD/NEW 宿主任意顺序重启均通。应用层
# error (请求已被处理) 不触发翻转 — session.prompt 重发有副作用, 只有
# 确证"未处理"(404/405)才安全重试。401 (仅 slash 形态) = cookie 过期/
# secret 轮转 → 自铸重试一次, 重铸后仍 401 即响亮失败 (cut3-4 ④)。
_WIRE_DOT = "dot"
_WIRE_SLASH = "slash"
_wire_form: str | None = None  # None → 未探测, 点号先行
_dsh_cookie: str | None = None  # 进程级自铸 cookie 缓存 (401 时弃旧重铸)
# NEW workspace.list unary 不存在 (探针节 6: /api/workspace.list 与
# /api/workspace/list 均 404) — slash 形态下归档核验降级为 session/list
# 空 request (其 value 无 archivedSessionIds 字段 → 调用方递归 walk 得
# 空集, 核验退化 alive-only); dot 链 workspace.list 语义逐字节保留。
# 归档会话是否仍列于 session/list 待 T5 活体观察。
_WIRE_SLASH_ALIASES = {"workspace.list": "session.list"}


def _to_wire_method(method: str, form: str) -> str:
    """canonical 归一 + 形态展开: 首个 '/'→'.' 归一 (scripts/
    fleet_e2e_backend.py:58 已传斜杠形态), 再按 form 首 '.'→'/'
    (A4 四映射逐一等价 — 本 helper 调用面仅 session.*/workspace.*
    单点前缀方法, 不涉 A4 的 agentPresets/subagents 改名项)。"""
    dot = method.replace("/", ".", 1)
    if form == _WIRE_DOT:
        return dot
    dot = _WIRE_SLASH_ALIASES.get(dot, dot)
    return dot.replace(".", "/", 1)


def _b64url(raw: bytes) -> str:
    """base64url 无 padding (上游 encodeBase64Url 同款)。"""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _read_browser_session_secret() -> bytes:
    """$DSH_HOME/.credentials.yaml → client-connection/browser-session
    secret (32B)。固定 schema 最小解析 (零 PyYAML 依赖); 任何失败 →
    RuntimeError 显式报错, 不静默 (cut3-4 ②: 铸不出要响)。凭据只读
    不外传 — 本函数仅返回解码后的字节用于进程内 HMAC。"""
    home = os.environ.get("DSH_HOME") or os.path.expanduser("~/.dsh")
    path = os.path.join(home, ".credentials.yaml")
    try:
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
    except OSError as e:
        raise RuntimeError(
            f"dsh cookie mint: credentials unreadable at {path}: {e}") from e
    # schema (上游 browser-auth.ts STORED_SECRET_VERSION=1):
    #   records:
    #     client-connection/browser-session:
    #       kind: grant
    #       payload:
    #         version: 1
    #         secret: <base64url 32B>
    secret = None
    in_record = False
    in_payload = False
    for line in text.splitlines():
        if line.startswith("  client-connection/browser-session:"):
            in_record = True
            in_payload = False
            continue
        if in_record and line.startswith("    payload:"):
            in_payload = True
            continue
        if in_record and in_payload and line.lstrip().startswith("secret:"):
            secret = line.split("secret:", 1)[1].strip().strip("'\"")
            break
        if in_record and line and not line.startswith("    ") \
                and not line.startswith("  "):
            in_record = False  # 越出 records 条目, 防误读兄弟记录
    if not secret:
        raise RuntimeError(
            "dsh cookie mint: client-connection/browser-session secret "
            f"not found in {path}")
    try:
        raw = base64.urlsafe_b64decode(secret + "=" * (-len(secret) % 4))
    except Exception as e:  # noqa: BLE001 — 显式归因
        raise RuntimeError(f"dsh cookie mint: secret not base64url: {e}") from e
    if len(raw) != 32:
        raise RuntimeError(
            f"dsh cookie mint: secret must decode to 32B, got {len(raw)}")
    return raw


def _mint_dsh_cookie(port: str) -> str:
    """按 NEW client-connection/browser-auth.ts 实测契约自铸
    browser-session cookie (探针节 4 / OQ-T5 方案 a):
      name  = dsh-auth-<b64url(sha256(authority))>
      value = v1.<b64url(json payload)>.<b64url(hmac_sha256(secret, body))>
    authority = Host 头原样 = 127.0.0.1:<port>; HMAC 签的是 base64 后的
    body 串; payload = {version:1, authority, issuedAt, expiresAt} (ms)。
    TTL 30d 对齐上游 cookieMaxAgeDays 默认 (进程冷启重签, 实际寿命更短)。
    铸不出 (凭据缺失/格式坏) → RuntimeError, 上层不静默。"""
    authority = f"127.0.0.1:{port}"
    secret = _read_browser_session_secret()
    now_ms = time.time_ns() // 1_000_000
    payload = {"version": 1, "authority": authority,
               "issuedAt": now_ms, "expiresAt": now_ms + 30 * 24 * 3600 * 1000}
    body = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    mac = _b64url(hmac.new(secret, body.encode("ascii"),
                           hashlib.sha256).digest())
    name = "dsh-auth-" + _b64url(hashlib.sha256(
        authority.encode("ascii")).digest())
    return f"{name}=v1.{body}.{mac}"


async def _wire_dsh_api(method: str, payload: dict, call) -> dict:
    """wire 形态适配层 (dot↔slash 懒探测+缓存) — DshBackend._dsh_api 与
    模块级公共入口 dsh_api 共用 (seatB-cut3-2 引入, cut3-3 提为模块级
    单点, cut3-4 对齐 NEW 实测三要素)。``call``:
    (wire_method, payload, form, cookie) -> value 的同步出站函数。
    翻转边界: 仅路由层 404/405 (请求未被任何 handler 接受) 翻转重发
    一次; 应用层 error (请求已处理) 不翻转 — session.prompt 类副作用
    调用零双投。401 仅 slash 形态: 自铸重试一次。"""
    global _wire_form, _dsh_cookie
    form = _wire_form or _WIRE_DOT

    def _cookie_for(f: str) -> str | None:
        global _dsh_cookie
        if f != _WIRE_SLASH:
            return None
        if not _dsh_cookie:
            _dsh_cookie = _mint_dsh_cookie(
                os.environ.get("DSH_PORT", "3080"))
        return _dsh_cookie

    try:
        return await asyncio.to_thread(
            call, _to_wire_method(method, form), payload, form,
            _cookie_for(form))
    except urllib.error.HTTPError as exc:
        if exc.code == 401 and form == _WIRE_SLASH:
            # NEW 鉴权闸 (探针节 4): cookie 过期/secret 轮转的恢复通道。
            # 弃旧重铸 → 重发一次; 重铸本身失败 (凭据缺失/坏) 由
            # _mint_dsh_cookie 的 RuntimeError 显式穿透 (不静默)。
            _dsh_cookie = _mint_dsh_cookie(
                os.environ.get("DSH_PORT", "3080"))
            try:
                value = await asyncio.to_thread(
                    call, _to_wire_method(method, form), payload, form,
                    _dsh_cookie)
            except urllib.error.HTTPError as exc2:
                if exc2.code == 401:
                    raise RuntimeError(
                        "dsh auth: 401 persists after cookie re-mint "
                        "($DSH_HOME/.credentials.yaml secret stale or "
                        "authority mismatch)") from exc2
                raise
            return value
        if exc.code not in (404, 405):
            raise  # 5xx/403 等: 与 wire 形态无关, 原语义直抛
        alt = _WIRE_SLASH if form == _WIRE_DOT else _WIRE_DOT
        try:
            value = await asyncio.to_thread(
                call, _to_wire_method(method, alt), payload, alt,
                _cookie_for(alt))
        except urllib.error.HTTPError:
            raise exc  # 两形态均路由未注册 → 抛原 404 (方法名问题, 非形态)
        except Exception:
            # 应用层 error (result.ok=false): 路由已接受请求 — 该形态
            # 本身有效, 缓存后让业务错误穿透 (不吞不换)。
            _wire_form = alt
            raise
        _wire_form = alt
        return value


async def dsh_api(method: str, payload: dict) -> dict:
    """模块级公共入口: 无 backend 实件的调用方 (rt_gateway 镜像同化,
    seatB-cut3-3) 复用同一 wire 形态自适配 — 单点防两处演化分叉。"""
    return await _wire_dsh_api(method, payload, DshBackend._dsh_api_call)


def _report_phase2_death(task: asyncio.Task) -> None:
    """Tripwire: a dead phase-2 means the final is lost with no signal —
    surface it instead of an unretrieved-exception warning."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        print(f"[dsh-backend] phase-2 aborted: {exc!r}",
              file=sys.stderr, flush=True)


def _parse_dag_subtasks(raw: str) -> list:
    """Tolerantly parse a head-authored subtask JSON list.

    Accepts a bare array or a ``{"subtasks"|"tasks"|"items": [...]}``
    wrapper; per item the spec text, dep indices, settlement command,
    and worker session are read from any of their known spellings
    (same tolerance family as ``rt_orchestrator.SplitPlan.subtasks``).
    Returns ``[]`` on any structural failure; items without usable
    spec text are dropped.
    """
    import json

    if isinstance(raw, (list, dict)):
        data = raw
    else:
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            return []
    if isinstance(data, dict):
        items = next((data[k] for k in ("subtasks", "tasks", "items")
                      if isinstance(data.get(k), list)), None)
    elif isinstance(data, list):
        items = data
    else:
        return []
    out: list[DagTaskSpec] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        spec = str(item.get("spec") or item.get("goal") or item.get("task")
                   or item.get("description") or "").strip()
        if not spec:
            continue
        deps_raw = item.get("deps") or []
        if not isinstance(deps_raw, list):
            deps_raw = []
        deps = []
        for d in deps_raw:
            try:
                deps.append(int(d))
            except (ValueError, TypeError):
                continue
        command = item.get("command") or item.get("cmd") or None
        session = item.get("session") or item.get("worker") or None
        out.append(DagTaskSpec(spec=spec, deps=deps,
                               command=str(command) if command else None,
                               session=str(session) if session else None))
    return out


@dataclass
class DshDispatch:
    """Phase-1 acceptance receipt."""

    run_id: str | None
    task_id: str | None
    ref: str
    credentials: list[str] = field(default_factory=list)
    intent_seq: int = -1
    # Construction time ≈ dispatch time; the gateway's compaction snapshot
    # reads it for the ``elapsed_s`` of runs the store has no terminal row
    # for yet (kg/14 §2.5).
    ts: float = field(default_factory=time.time)


@dataclass
class DagTaskSpec:
    """One subtask in a dependency-ordered voice dispatch (lane B DAG).

    Args:
        spec: self-contained subtask description.
        deps: indices (into the same dispatch's task list) of prerequisite
            subtasks; a worker starts only after all deps settle
            ``succeeded`` — a failed dep skips the dependent task.
        command: shell block for daemon-driven settlement (exit 0 =
            worker_done succeeded); mutually informative with ``session``.
        session: bind the dispatch to a long-lived worker session's pane
            (``start-worker --session``, dais be8d9cf3) instead of a
            command block.
    """

    spec: str
    deps: list[int] = field(default_factory=list)
    command: str | None = None
    session: str | None = None


@dataclass
class DshBackend:
    """Implements the head tool surface: dispatch / query_status / cancel.

    Args:
        lane: dais CLI lane (lane B) — required for status/cancel even
            in lane-A mode.
        lane_a: optional A2A client; with ``lane_mode="a"`` dispatch goes
            through it (message/send) instead of the dais CLI.
        lane_mode: "b" (dais CLI, default) or "a" (A2A plugin lane).
        bus: event bus for WS4 forwarding.
        orchestrator_handle: dais handle of the orchestrator session
            (``session_<sid>``); the intent is addressed to it and done
            replies are expected from it.
        head_handle: the head's own mailbox handle (default
            ``voice-head``); phase-2 polls HERE — replies are addressed
            to the intent's sender.
        on_final: callback invoked with the phase-2 final message (the
            head pipeline wires this to context re-injection).
        await_timeout_s: phase-2 blocking budget per dispatch.
        poll_max_s: ceiling of the phase-2 poll backoff (consumption
            polls are write transactions on the daemon store; a flat
            cadence starves in-flight senders — see DaisLane.await_done).
        dag_workers: provisioned worker-session mailbox keys
            (``session_<sid>``); DAG tasks that carry no explicit
            ``session`` bind round-robin onto this pool, so the head
            splits semantically while execution binding stays with the
            plane.
        bind_session_id: in-flight dsh session (full ``session_<uuid>``)
            that a dispatch-carried profile is bound to via pool/spawn
            binding-mode (G4 dressing; dsh sessions only).
        receipt_slim: acceptance-receipt shape override for liaison
            deliveries; None (default) reads VOICE_RECEIPT_SLIM per call
            ("0" off, else on).
    """

    lane: DaisLane
    lane_a: A2aClient | None = None
    lane_mode: str = "b"
    lane_orca: object | None = None     # OrcaLane (typed loose: optional dep)
    orca_agent: str = ORCA_AGENT_DEFAULT
    orca_repo: str = ""                 # verbatim selector (path:<p>/id:/name:)
    bus: EventBus = field(default_factory=EventBus)
    orchestrator_handle: str = ""
    head_handle: str = "voice-head"
    on_final: Callable[[str, str], Awaitable[None]] | None = None
    await_timeout_s: float = 1800.0
    poll_s: float = 2.0
    poll_max_s: float = 8.0
    dag_workers: list[str] | None = None
    bind_session_id: str = ""
    # Dedicated liaison session (fleet 4-code or full sessionId): when set,
    # every head tool call is delivered into that session's turn — steer if
    # a turn is in flight (immediate activation), queue otherwise (drives a
    # fresh turn). Finals still return through the voice-head dais mailbox.
    liaison_session: str = ""
    # Steer waits for the liaison's current step to finish before the message
    # enters the next LLM call; when timing matters more than the in-flight
    # step's work, cancel the step on steer so the message drives a fresh
    # turn immediately.
    liaison_cancel_step: bool = True
    # Liaison auto-new lifecycle: with no usable liaison session (no alive
    # explicit target, no alive spawn binding), spawn a fresh one via
    # session-spawn + profile-envelope injection instead of waking an
    # archived session. None (default) defers to VOICE_LIAISON_AUTO_NEW
    # (production sets "1"; unset means off); True/False pin outright.
    liaison_auto_new: bool | None = None
    # Acceptance-receipt shape override (kg/14 #2): None (default) defers
    # to VOICE_RECEIPT_SLIM read at receipt-assembly time ("0" off, else
    # on — per-call read, no restart); True/False pin the shape outright.
    receipt_slim: bool | None = None
    _runs: dict[str, DshDispatch] = field(default_factory=dict)
    _pending: dict[str, asyncio.Task] = field(default_factory=dict)
    _bound: dict[str, dict] = field(default_factory=dict)

    # ---- liaison-session delivery (all head tools → one agent's turn) ----

    async def _dsh_api(self, method: str, payload: dict) -> dict:
        """POST one RPC to the dsh web host loopback API.

        seatB-cut3-2 wire 形态自适配: 调用点零改动 (点号/斜杠传入均经
        _to_wire_method 归一), 适配层与模块级公共入口 dsh_api 共用
        (seatB-cut3-3 同化) — 设计依据与安全边界见 _WIRE_* 块注释。
        payload 语义零改动 (mode:'queue' 等原样透传)。
        """
        return await _wire_dsh_api(method, payload, self._dsh_api_call)

    @staticmethod
    def _dsh_api_call(method: str, payload: dict, form: str,
                      cookie: str | None) -> dict:
        """单次 RPC 出站 (原 _dsh_api 内联 _call 的提取; staticmethod —
        零实例态, 供模块级 dsh_api 公共入口共用)。form=_WIRE_SLASH 时按
        NEW 实测契约 (探针节 2/3/4): payload 包一层 {args:{request:...}},
        附 cookie 头 (适配层已铸); dot 形态逐字节保持 OLD 语义 (cut3-4 ①)。
        payload 包装在出站层完成 — 适配层/调用点始终传业务原形。"""
        if form == _WIRE_SLASH:
            payload = {"args": {"request": payload}}
        headers = {"content-type": "application/json"}
        if form == _WIRE_SLASH:
            headers["cookie"] = cookie or ""
        wire = {"type": "client-request", "rpcId": str(uuid.uuid4()),
                "method": method, "payload": payload}
        req = urllib.request.Request(
            f"http://127.0.0.1:{os.environ.get('DSH_PORT', '3080')}/api/{method}",
            data=json.dumps(wire).encode(),
            headers=headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())["result"]
        if not result.get("ok"):
            raise RuntimeError(f"{method}: {result.get('error')}")
        return result["value"]

    def _liaison_sid(self) -> str:
        """Resolve the configured liaison target to a full sessionId."""
        key = self.liaison_session
        if key.startswith("session-"):
            return key
        with open(os.path.expanduser(
                os.environ.get("MAESTRO_FLEET", "~/.dsh/maestro/fleet.json"))) as fh:
            fleet = json.load(fh)
        entry = fleet.get("fleet", {}).get(key)
        if not entry:
            raise RuntimeError(f"liaison {key!r} not in fleet.json")
        return entry["sessionId"]

    # ---- liaison lifecycle: auto-new (no alive target -> fresh spawn) ----

    @property
    def liaison_mode(self) -> bool:
        """Liaison delivery armed: explicit target set OR auto-new on."""
        return bool(self.liaison_session) or self._liaison_auto_new()

    def _liaison_auto_new(self) -> bool:
        if self.liaison_auto_new is not None:
            return self.liaison_auto_new
        # Code default OFF (fanout lanes / unit tests untouched); production
        # arms it via VOICE_LIAISON_AUTO_NEW=1 in the gateway env.
        return os.environ.get("VOICE_LIAISON_AUTO_NEW", "0") == "1"

    @staticmethod
    def _liaison_state_path() -> str:
        return os.environ.get(
            "VOICE_LIAISON_STATE",
            os.path.expanduser("~/.local/state/voice-gateway/liaison.json"))

    def _liaison_bound_load(self) -> str:
        try:
            with open(self._liaison_state_path(), encoding="utf-8") as fh:
                return str(json.load(fh).get("sessionId") or "")
        except (OSError, ValueError):
            return ""

    def _liaison_bound_save(self, code: str, session_id: str) -> None:
        path = self._liaison_state_path()
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump({"code": code, "sessionId": session_id,
                           "spawned_at": time.time()}, fh)
        except OSError as e:  # binding is an optimization, never fatal
            print(f"rt_dsh_backend: liaison state save failed: {e}",
                  file=sys.stderr)

    def _liaison_bound_clear(self) -> bool:
        """Drop a stale liaison binding (bound session archived or removed
        from the fleet registry — otherwise the next dispatch would keep
        feeding the archived seat: ghost worker).

        Returns whether a file was actually removed; absent file and
        failures both return False (the binding is an optimization, and
        the next delivery's archived check retries the clear).
        """
        try:
            os.unlink(self._liaison_state_path())
        except FileNotFoundError:
            return False
        except OSError as e:
            print(f"rt_dsh_backend: liaison state clear failed: {e}",
                  file=sys.stderr)
            return False
        return True

    @staticmethod
    def _liaison_envelope(name: str, version, agents_md: str, mailbox: str) -> str:
        """Byte-shape mirrors a2a-profile-server incubators/real.js
        (injectionPrompt + roleDoctrine('liaison')) so a respawned session
        is indistinguishable from a pool-incubated one."""
        doctrine = "\n".join([
            "## Role Doctrine — liaison（对接 agent）",
            "",
            "- 语义收敛：上游语义指令 → 稳定指令（自包含、指代全展开、幂等可重放）。",
            "- 两阶段应答：先回受理回执 {status:\"accepted\", run_id, ref, credentials}；"
            "终稿以 \"Agent Final Message\": 前缀行起首。",
            "- 信封纪律：所有对外消息 body 以 [ref:<ref>] 前缀。",
            "- 凭证回显：【凭证…】逐字回显，不得改写。",
            f"- 回合首动作：check-messages {mailbox} --timeout-ms "
            "快照排空邮箱取正文（推唤醒仅触发回合，正文一律走邮箱）。",
            "",
        ])
        return (f"ORCA-CB] PROFILE-INJECT] {name}@v{version}\n"
                f"{doctrine}\n{agents_md}")

    def _liaison_fetch_profile(self) -> tuple[str, str, str]:
        """(envelope, name, version) from the a2a profile store; raises on
        any failure — a respawn without the liaison doctrine would produce
        garbage finals, so bootstrap failures must be loud."""
        port = os.environ.get("A2A_PROFILE_PORT", "8790")
        name = os.environ.get("VOICE_LIAISON_PROFILE", "vh-liaison")
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/",
            data=json.dumps({"jsonrpc": "2.0", "id": 1,
                             "method": "profiles/get",
                             "params": {"name": name}}).encode(),
            headers={"content-type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read()).get("result") or {}
        if "error" in result:
            raise RuntimeError(f"profiles/get {name}: {result['error']}")
        prof = result.get("profile") or result
        agents_md = str(prof.get("agentsMd") or "")
        if not agents_md.strip():
            raise RuntimeError(f"profile {name}: empty agentsMd")
        mailbox = str((prof.get("profile") or {}).get("mailbox")
                      or os.environ.get("VOICE_LIAISON_MAILBOX", "agent_liaison"))
        version = prof.get("version", 1)
        return (self._liaison_envelope(name, version, agents_md, mailbox),
                name, version)

    async def _liaison_spawn(self) -> str:
        """session-spawn a fresh liaison (birth + rename + fleet register),
        inject the profile envelope, persist the binding. Returns sessionId."""
        bin_path = os.environ.get(
            "VOICE_LIAISON_SPAWN_BIN",
            os.path.expanduser("~/.dsh/maestro/bin/session-spawn"))
        preset = os.environ.get("VOICE_LIAISON_PRESET", "maestro")
        node = os.environ.get("VOICE_LIAISON_NODE", "vh-head-liaison")
        purpose = os.environ.get("VOICE_LIAISON_PURPOSE", "voice-head对接")

        def _run() -> str:
            import subprocess
            out = subprocess.run([bin_path, preset, node, purpose],
                                 capture_output=True, text=True, timeout=60)
            if out.returncode != 0:
                raise RuntimeError(
                    f"session-spawn rc={out.returncode}: "
                    f"{(out.stderr or out.stdout).strip()[-160:]}")
            return out.stdout

        stdout = await asyncio.to_thread(_run)
        code = stdout.strip().splitlines()[-1].strip()
        if len(code) < 4 or code[:4].lower() != code[:4].lower() or not code[:4]:
            raise RuntimeError(f"session-spawn produced no code: {code[:120]}")
        fleet_path = os.path.expanduser(
            os.environ.get("MAESTRO_FLEET", "~/.dsh/maestro/fleet.json"))
        with open(fleet_path, encoding="utf-8") as fh:
            entry = json.load(fh).get("fleet", {}).get(code[:4])
        if not entry:
            raise RuntimeError(f"spawned code {code[:4]} missing from fleet.json")
        session_id = entry["sessionId"]
        envelope, _name, _ver = await asyncio.to_thread(self._liaison_fetch_profile)
        await self._dsh_api("session.prompt", {
            "sessionId": session_id, "mode": "queue",
            "content": [{"type": "text", "text": envelope}],
        })
        self._liaison_bound_save(code[:4], session_id)
        return session_id

    async def _archived_session_ids(self) -> set[str] | None:
        """Archived-session ids from ``workspace.list``.

        Known wire shape is a top-level ``{"items": […],
        "archivedSessionIds": [sessionId, …]}``; the walk recurses so
        nesting/shape drift stays tolerated. Unreachable loopback →
        None: the caller degrades to no verification — an archive-check
        failure must never block dispatch.
        """
        try:
            value = await self._dsh_api("workspace.list", {})
        except Exception as e:  # noqa: BLE001 — degrade signal, not fatal
            print(f"rt_dsh_backend: workspace.list archived check failed: {e}",
                  file=sys.stderr, flush=True)
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
        return ids

    async def _liaison_ensure_sid(self, sessions_value: dict) -> str:
        """Pick the liaison sessionId: an alive AND unarchived spawn
        binding first, then the alive explicit target; else auto-new
        spawn; else legacy explicit (prompt-the-configured-session,
        incl. waking archived ones) when auto-new is off.

        A bound sid that sits in the archived set is a dead binding
        (archived seats must not take dispatches — ghost workers): the
        stale state file is cleared and selection continues as if no
        binding existed. With auto-new off that dead-ends in an error
        naming the archived binding instead of waking it.
        """
        items = sessions_value.get("items", [])
        alive = {s.get("sessionId") for s in items}
        bound = self._liaison_bound_load()
        binding_archived = False
        if bound and bound in alive:
            archived = await self._archived_session_ids()
            if archived is not None and bound in archived:
                self._liaison_bound_clear()
                binding_archived = True
            else:
                return bound  # alive (archive check degraded → unverified)
        if self.liaison_mode:
            try:
                explicit = self._liaison_sid()
            except (OSError, RuntimeError):
                explicit = ""
            if explicit and explicit in alive:
                # Explicit targets deliberately MAY wake archived sessions:
                # an explicit config is an ops-chosen wake-up.
                return explicit
        if self._liaison_auto_new():
            return await self._liaison_spawn()
        if binding_archived:
            raise RuntimeError(
                "liaison 绑定会话已归档且 VOICE_LIAISON_AUTO_NEW 换新未开启")
        if self.liaison_mode:
            return self._liaison_sid()  # legacy: wake the configured session
        raise RuntimeError("no liaison session configured and auto-new off")

    async def _deliver_liaison(self, ref: str, body: str) -> str:
        """Drop one DSHMSG envelope into the liaison session's turn.

        Returns the delivery mode used: ``steer`` when a turn was in flight
        (the message joins the running turn immediately), ``queue`` when the
        session was idle (the message drives a fresh turn on it).

        Steer normally waits for the current step (one LLM call + its tool
        executions) to finish before the message enters the next request —
        latency is bounded by the step's remaining length. With
        ``liaison_cancel_step`` (default), steer first cancels the in-flight
        step: ``session.cancel`` aborts the running phase with keep-inbox,
        and the agent loop reclassifies the wake-up message as next-turn,
        starting a fresh turn immediately over the full history.
        """
        sessions = await self._dsh_api("session.list", {})
        sid = await self._liaison_ensure_sid(sessions)
        running = any(s.get("sessionId") == sid and s.get("running")
                      for s in sessions.get("items", []))
        if running and self.liaison_cancel_step:
            # abort the in-flight step; the queued wake-up then reclassifies
            # to next-turn and drives a fresh turn immediately
            await self._dsh_api("session.cancel", {"sessionId": sid})
            mode = "steer-cancel"
            prompt_mode = "queue"
        elif running:
            mode = prompt_mode = "steer"
        else:
            mode = prompt_mode = "queue"
        line = "DSHMSG]" + json.dumps({
            "from": self.head_handle, "to": self.liaison_session, "type": "ask",
            "ref": ref, "body": body[:8000],
            "msgid": str(uuid.uuid4()), "ts": int(time.time() * 1000),
        }, ensure_ascii=False)
        await self._dsh_api("session.prompt", {
            "sessionId": sid, "mode": prompt_mode,
            "content": [{"type": "text", "text": line}],
        })
        return mode

    def _receipt_slim(self) -> bool:
        """Whether liaison acceptance receipts take the slim form (kg/14 #2).

        ``receipt_slim`` pins the shape when set; otherwise the env is
        read fresh at each receipt assembly (no restart), matching the
        DoctrineSource philosophy.
        """
        if self.receipt_slim is not None:
            return self.receipt_slim
        return os.environ.get("VOICE_RECEIPT_SLIM", "1") != "0"

    async def _liaison_roundtrip(self, body: str, run_id: str | None,
                                 extra_receipt: dict | None = None) -> str:
        """Deliver to the liaison turn, arm the phase-2 wait, return receipt.

        The receipt is slim by default (kg/14 §2.3): status/ref/summary
        only (plus ``extra_receipt`` merges such as a plan's task count) —
        credentials and run_id stay on the observation plane
        (``_runs`` + orch.dispatch), never in the model-visible JSON.
        VOICE_RECEIPT_SLIM=0 restores the legacy full form.
        """
        ref = "vh-" + uuid.uuid4().hex[:8]
        dispatch = DshDispatch(run_id=run_id, task_id=None, ref=ref,
                               credentials=[make_credential(ref.upper())])
        self._runs[ref] = dispatch
        try:
            mode = await self._deliver_liaison(ref, body)
        except Exception as e:  # noqa: BLE001 — spawn/deliver failure is a
            # terminal, reportable condition: fail the dispatch loudly rather
            # than arming a phase-2 wait that can never complete
            await self.bus.emit("orch.failed", {
                "run_id": run_id, "ref": ref, "reason": f"liaison: {e}"})
            return json.dumps({"status": "failed", "ref": ref,
                               "summary": "对接会话不可用"}, ensure_ascii=False)
        if self._receipt_slim():
            receipt = {
                "status": "accepted", "ref": ref,
                "summary": "已受理，转对接人执行",
            }
        else:
            receipt = {
                "status": "accepted", "run_id": run_id, "ref": ref,
                "credentials": dispatch.credentials,
                "note": {"queue": "已转对接人（新回合），完成后播报",
                         "steer": "已转对接人（并入在飞回合），完成后播报",
                         "steer-cancel": "已转对接人（打断当前步立即执行），完成后播报"
                         }.get(mode, "已转对接人，完成后播报"),
            }
        if extra_receipt:
            receipt.update(extra_receipt)
        self._pending[ref] = asyncio.create_task(self._phase2(ref, dispatch))
        self._pending[ref].add_done_callback(_report_phase2_death)
        await self.bus.emit("orch.dispatch", {
            "run_id": run_id, "task_id": None, "ref": ref,
            "credentials": dispatch.credentials, "lane": "liaison", "mode": mode,
        })
        return json.dumps(receipt, ensure_ascii=False)

    # ---- head tool 1: dispatch_intent ----

    async def dispatch_plan(self, objective: str, subtasks_json: str) -> str:
        """Head-facing DAG entry: tolerant-parse a subtask JSON list.

        Accepts a bare JSON array or ``{"subtasks": [...]}`` (also
        ``tasks``/``items``); per item the spec text may sit under
        ``spec``/``goal``/``task``/``description``, deps under ``deps``
        (indices into the same list), the settlement command under
        ``command``/``cmd``, and an explicit worker session under
        ``session``/``worker``. Unparseable or spec-less input returns
        the clarify note.
        """
        import json

        tasks = _parse_dag_subtasks(subtasks_json)
        if not tasks:
            return CLARIFY_NOTE
        if self.liaison_mode:
            run_id = await self.lane.create_run(f"[voice-head-plan] {objective[:200]}")
            body = (f"PLAN {objective} run={run_id} || "
                    + json.dumps([asdict(t) for t in tasks],
                                 ensure_ascii=False))
            return await self._liaison_roundtrip(body, run_id, {"tasks": len(tasks)})
        return await self.dispatch_dag(objective, tasks)

    async def dispatch(self, raw_intent: str, profile: str | None = None) -> str:
        """Phase 1 acceptance now; phase 2 final re-injected on completion.

        ``profile`` (G4 dressing): bind the stored profile onto the
        configured in-flight dsh session (``bind_session_id``) via
        pool/spawn binding-mode BEFORE the intent goes out — the dressed
        session's final then carries the profile's persona traces.
        Binding is idempotent per (profile, session).
        """
        ref = "vh-" + uuid.uuid4().hex[:8]
        bound = await self._bind_profile(profile)
        if self.liaison_mode:
            run_id = await self.lane.create_run(f"[voice-head] {raw_intent[:200]}")
            body = f"INTENT {raw_intent} run={run_id}"
            if profile:
                body += f" profile={profile}"
            return await self._liaison_roundtrip(body, run_id)
        dispatch = await self._fanout(raw_intent, ref)

        if dispatch is None:
            return CLARIFY_NOTE

        receipt = {
            "status": "accepted",
            "run_id": dispatch.run_id,
            "ref": dispatch.ref,
            "credentials": dispatch.credentials,
            "note": "已受理，完成后播报",
        }
        if bound:
            receipt["profile"] = bound
        self._pending[ref] = asyncio.create_task(self._phase2(ref, dispatch))
        self._pending[ref].add_done_callback(_report_phase2_death)
        import json

        return json.dumps(receipt, ensure_ascii=False)

    async def _bind_profile(self, profile: str | None) -> dict | None:
        """Bind ``profile`` onto ``bind_session_id`` (pool/spawn
        binding-mode); returns the bind receipt summary or None when not
        applicable (no profile / no lane-a / no session / already bound).

        Failures are NOT swallowed silently — a dressing request that
        cannot be honored raises, so the head hears it instead of
        dispatching an undressed session under a dressed expectation.
        """
        if not profile or not self.bind_session_id or self.lane_a is None:
            return None
        key = f"{profile}@{self.bind_session_id}"
        if key in self._bound:
            return self._bound[key]
        receipt = await self.lane_a.pool_spawn(
            profile, strategy="binding-mode",
            binding_session_id=self.bind_session_id)
        summary = {"name": receipt.get("name", profile),
                   "version": receipt.get("version", ""),
                   "sessionId": receipt.get("sessionId", self.bind_session_id),
                   "injected": bool(receipt.get("injected"))}
        self._bound[key] = summary
        return summary

    async def dispatch_dag(self, objective: str, tasks: list[DagTaskSpec]) -> str:
        """Head tool: split one intent into a dependent task DAG (lane B).

        Creates the run + tasks (``--dep`` wired from spec indices), then
        a phase-2 walker starts workers in dependency waves — a worker
        starts only once its deps settled ``succeeded`` (dais-side state
        kept in step via promote-tasks) — matches each dispatch's
        worker_done settlement, and aggregates per-task outcomes plus
        terminal tails into ONE final message for the voice chain.
        """
        import json

        if not tasks:
            return CLARIFY_NOTE
        ref = "vh-" + uuid.uuid4().hex[:8]
        run_id = await self.lane.create_run(f"[voice-head] {objective[:200]}")
        task_ids: list[str] = []
        for t in tasks:
            deps = [task_ids[i] for i in t.deps if i < len(task_ids)]
            task_ids.append(await self.lane.create_task(run_id, t.spec[:2000],
                                                        deps=deps))
        credential = make_credential(ref.upper())
        dispatch = DshDispatch(run_id=run_id, task_id=None, ref=ref,
                               credentials=[credential])
        dispatch.dag = [
            {"task_id": tid, "deps": t.deps, "command": t.command,
             "session": t.session
             or (self.dag_workers[i % len(self.dag_workers)]
                 if self.dag_workers else None),
             "spec": t.spec}
            for i, (tid, t) in enumerate(zip(task_ids, tasks))
        ]  # type: ignore[attr-defined]
        dispatch.dag_ctx = {}                                  # type: ignore[attr-defined]
        self._runs[ref] = dispatch
        await self.bus.emit("orch.dispatch", {
            "run_id": run_id, "task_id": None, "ref": ref,
            "credentials": dispatch.credentials, "lane": "b-dag",
            "extra": json.dumps({"tasks": len(tasks)}, ensure_ascii=False),
        })
        self._pending[ref] = asyncio.create_task(self._phase2_dag(ref, dispatch))
        self._pending[ref].add_done_callback(_report_phase2_death)
        return json.dumps({
            "status": "accepted", "run_id": run_id, "ref": ref,
            "credentials": dispatch.credentials, "tasks": len(tasks),
            "note": "已按依赖拆分受理，完成后播报",
        }, ensure_ascii=False)

    async def _fanout(self, raw_intent: str, ref: str) -> DshDispatch | None:
        """Create run + intent message; returns None when unsplittable."""
        if self.lane_mode == "b-orca":
            return await self._fanout_orca(raw_intent, ref)
        if self.lane_mode == "a" and self.lane_a is not None:
            task_id = await self.lane_a.send(raw_intent, ref)
            credential = make_credential(ref.upper())
            dispatch = DshDispatch(run_id=task_id, task_id=None, ref=ref,
                                   credentials=[credential])
            self._runs[ref] = dispatch
            await self.bus.emit("orch.dispatch", {
                "run_id": None, "task_id": task_id, "ref": ref,
                "credentials": dispatch.credentials, "lane": "a",
            })
            return dispatch
        run_id = await self.lane.create_run(f"[voice-head] {raw_intent[:200]}")
        intent_seq = -1
        if not self.orchestrator_handle:
            # no orchestrator session yet — the run itself carries the intent
            task_id = await self.lane.create_task(run_id, raw_intent[:2000])
        else:
            intent_seq = await self.lane.send_intent(
                run_id, self.orchestrator_handle, raw_intent, ref,
                from_id=self.head_handle,
            )
            task_id = None
        credential = make_credential(ref.upper())
        dispatch = DshDispatch(run_id=run_id, task_id=task_id, ref=ref,
                               credentials=[credential], intent_seq=intent_seq)
        self._runs[ref] = dispatch
        await self.bus.emit("orch.dispatch", {
            "run_id": run_id, "task_id": task_id, "ref": ref,
            "credentials": dispatch.credentials, "lane": "b",
        })
        return dispatch

    async def _fanout_orca(self, raw_intent: str, ref: str) -> DshDispatch:
        """Lane B-orca fan-out (F7 routing: worktree/terminal deliveries).

        Spawns a scratch worktree with the configured agent (default omp)
        and a prompt that asks for one verbatim final line plus an artifact
        write — the artifact is the render-proof completion signal (the
        agent's answer text never enters the scrollback ring; live-probed
        2026-08-23). The worktree id doubles as the run handle.
        """
        if self.lane_orca is None:
            raise ValueError("lane_mode='b-orca' requires lane_orca=OrcaLane(...)")
        if not self.orca_repo:
            raise ValueError("lane_mode='b-orca' requires orca_repo selector")
        import json as _json

        credential = make_credential(ref.upper())
        body = f"{raw_intent} {credential}"
        artifact = f"final-{ref}.txt"
        prompt = (
            f"任务：{raw_intent}\n"
            f"完成后：①把下面这一行逐字写入当前工作目录的 {artifact} 文件（不要加任何其他内容）：\n"
            f"调研完成 {credential} 结论 41%\n"
            f"②同时用一行回复确认。"
        )
        created = await self.lane_orca.spawn_worktree(
            f"vh-{ref}", self.orca_repo, self.orca_agent, prompt, setup="skip")
        wt_id = created["worktreeId"]
        dispatch = DshDispatch(run_id=wt_id, task_id=created.get("terminalHandle"),
                               ref=ref, credentials=[credential])
        dispatch.artifact = artifact      # type: ignore[attr-defined]
        dispatch.prompt = prompt          # type: ignore[attr-defined]  (dance resend)
        self._runs[ref] = dispatch
        await self.bus.emit("orch.dispatch", {
            "run_id": wt_id, "task_id": None, "ref": ref,
            "credentials": dispatch.credentials, "lane": "b-orca",
            "extra": _json.dumps({"artifact": artifact, "agent": self.orca_agent}),
        })
        return dispatch

    async def _phase2(self, ref: str, dispatch: DshDispatch) -> None:
        """Await the done body and re-inject the final message.

        Transient lane errors (``database is locked`` under cross-process
        contention with the resident daemon) are retried until the
        overall budget expires. All terminal dead-ends (clean
        TimeoutError, lane errors until deadline, a lane-a task the
        server no longer knows) emit ``orch.failed``. A settled run
        emits ``orch.done`` carrying the raw final body (FINAL_PREFIX
        stripped) for the gateway's store bridge (kg/14 §2.2); the
        full final still re-enters the head via ``on_final`` only —
        the legacy ``artifact`` key stays gone (kg/14 #3).
        """
        from rt_dsh_lane import DaisLaneError

        deadline = asyncio.get_event_loop().time() + self.await_timeout_s
        body: str | None = None
        while body is None:
            budget = max(1.0, deadline - asyncio.get_event_loop().time())
            if (self.lane_mode == "b-orca"
                    and dispatch.run_id and "::" in dispatch.run_id):
                body = await self._await_orca(dispatch, budget)
                continue
            try:
                if (self.lane_a is not None and dispatch.run_id
                        and dispatch.run_id.startswith("t_")):
                    try:
                        body = await self.lane_a.await_done(
                            dispatch.run_id, timeout_s=budget)
                    except A2aError as exc:
                        # server no longer knows the task (restart/roll):
                        # its final is unknowable — settle the ref as
                        # failed and stop polling
                        await self.bus.emit("orch.failed", {
                            "ref": ref, "run_id": dispatch.run_id,
                            "reason": f"lane-a task gone: {str(exc)[:160]}"})
                        return
                    continue
                # Poll the HEAD's own mailbox: replies are addressed to the
                # intent's sender; polling the orchestrator handle would
                # self-match the intent we sent (live-probed 2026-08-23).
                body = await self.lane.await_done(
                    self.head_handle, ref, timeout_s=budget,
                    poll_s=self.poll_s, poll_max_s=self.poll_max_s,
                    after_seq=dispatch.intent_seq if dispatch.intent_seq >= 0 else None,
                    from_filter=self.orchestrator_handle or None,
                )
            except TimeoutError:
                # ts is stamped bus-wide (EventBus.emit setdefault)
                await self.bus.emit("orch.failed", {
                    "ref": ref, "run_id": dispatch.run_id,
                    "reason": "still running"})
                return
            except DaisLaneError:
                if asyncio.get_event_loop().time() >= deadline:
                    await self.bus.emit("orch.failed", {
                        "ref": ref, "run_id": dispatch.run_id,
                        "reason": "lane errors until deadline"})
                    return
                await asyncio.sleep(min(self.poll_s, 1.0))
        if body.startswith(FINAL_PREFIX):
            body = body[len(FINAL_PREFIX):]
        final = f"{FINAL_PREFIX}{body}"
        await self.bus.emit("orch.done", {"ref": ref, "run_id": dispatch.run_id,
                                          "body": body})
        if self.on_final:
            await self.on_final(ref, final)

    async def _phase2_dag(self, ref: str, dispatch: DshDispatch) -> None:
        """Walk the DAG in dependency waves; aggregate ONE final message.

        A worker starts only after its deps settled ``succeeded``; a
        failed dep skips the dependents (recorded, not hidden). Each
        settlement is matched via the dispatch's worker_done row, the
        terminal tail is kept as the task's contribution, and
        promote-tasks keeps dais-side readiness in step. Settlement
        observation shares the escalating-backoff discipline (D-17).
        Budget expiry with tasks still unsettled emits ``orch.failed``
        (same payload shape as ``_phase2``'s dead-ends).
        """
        from rt_dsh_lane import DaisLaneError

        dag: list[dict] = dispatch.dag          # type: ignore[attr-defined]
        ctx: dict[int, str] = dispatch.dag_ctx  # type: ignore[attr-defined]
        outcomes: dict[int, dict] = {}
        tails: dict[int, str] = {}
        deadline = asyncio.get_event_loop().time() + self.await_timeout_s
        delay = self.poll_s

        def _deps_done(i: int) -> bool:
            return all(d in outcomes for d in dag[i]["deps"])

        def _deps_ok(i: int) -> bool:
            return all(outcomes[d].get("outcome") == "succeeded"
                       for d in dag[i]["deps"])

        while len(outcomes) < len(dag):
            left = deadline - asyncio.get_event_loop().time()
            if left <= 0:
                await self.bus.emit("orch.failed", {
                    "ref": ref, "run_id": dispatch.run_id,
                    "reason": "still running"})
                return
            # wave start: ready tasks (deps settled succeeded) get workers
            for i, t in enumerate(dag):
                if i in outcomes or i in ctx:
                    continue
                if not _deps_done(i):
                    continue
                if _deps_ok(i):
                    try:
                        ctx[i] = await self.lane.start_worker(
                            t["task_id"], command=t["command"],
                            session=t["session"])
                        await self.bus.emit("orch.progress", {
                            "ref": ref,
                            "note": f"task {i + 1}/{len(dag)} worker "
                                    f"{ctx[i]} started"})
                        if t["command"]:
                            # block settlement needs the command to RUN in
                            # the bound terminal — inject it there. A failed
                            # injection strands the task (no command block
                            # → no settlement), so surface it on the
                            # progress plane instead of swallowing it.
                            try:
                                await self.lane.inject_prompt(ctx[i], t["command"])
                            except DaisLaneError as e:
                                await self.bus.emit("orch.progress", {
                                    "ref": ref,
                                    "note": f"task {i + 1} inject failed: "
                                            f"{str(e)[:120]}"})
                    except DaisLaneError:
                        await asyncio.sleep(min(delay, 1.0))
                else:
                    outcomes[i] = {"outcome": "skipped",
                                   "reason": "dependency failed"}
            if not ctx:
                break
            # observe settlements round-robin with small slices
            settled_any = False
            for i, handle in list(ctx.items()):
                slice_s = min(2.0, max(0.1, deadline - asyncio.get_event_loop().time()))
                try:
                    row = await self.lane.await_worker_done(
                        handle, timeout_s=slice_s, poll_s=0.5,
                        poll_max_s=2.0, run_id=dispatch.run_id,
                        task_id=dag[i]["task_id"])
                except TimeoutError:
                    continue
                except DaisLaneError:
                    continue  # transient lane errors retry next cycle
                outcomes[i] = row
                settled_any = True
                del ctx[i]
                try:
                    tail, _ = await self.lane.read_worker(handle, lines=6)
                    tails[i] = tail.strip()[-400:]
                except DaisLaneError:
                    tails[i] = ""
                try:
                    await self.lane.promote_tasks(dispatch.run_id or "")
                except DaisLaneError:
                    pass  # dais-side readiness is best-effort bookkeeping
                await self.bus.emit("orch.progress", {
                    "ref": ref,
                    "note": f"task {i + 1}/{len(dag)} settled "
                            f"{row.get('outcome', '?')}"})
            if not settled_any:
                await asyncio.sleep(min(delay, max(0.05, left)))
                delay = min(delay * 1.5, self.poll_max_s)

        parts = []
        for i, t in enumerate(dag):
            outcome = outcomes.get(i, {}).get("outcome", "unsettled")
            tail = tails.get(i, "")
            line = f"子任务{i + 1}：{t['spec'][:60]} → {outcome}"
            if tail:
                line += f"｜{tail}"
            parts.append(line)
        credential = (dispatch.credentials or [""])[0]
        body = "\n".join(parts) + f"\n【凭证{credential}】"
        final = f"{FINAL_PREFIX}{body}"
        await self.bus.emit("orch.done", {"ref": ref, "run_id": dispatch.run_id,
                                          "body": body})
        if self.on_final:
            await self.on_final(ref, final)

    async def _await_orca(self, dispatch: DshDispatch, budget_s: float) -> str | None:
        """Await the worktree artifact (render-proof) within one budget slice.

        Follows the conformance ``_orca_worker`` choreography in miniature:
        bounded tui-idle wait slices; completion decided ONLY by the
        artifact's exact content match (partial writes never satisfy);
        dialog flashes answered with ``1`` (bounded); ONE interrupt +
        prompt resend after the settle window passes with output but no
        artifact (a worker stuck thinking never completes otherwise).
        Teardown is owned by cancel()/the caller — this coroutine never
        removes the worktree. Returns the artifact body, or None when
        the slice budget expired (phase-2 loops back for another slice
        until await_timeout_s).
        """
        from pathlib import Path
        from rt_orca_lane import OrcaLaneError, OrcaLaneTimeout

        assert self.lane_orca is not None
        handle = dispatch.task_id
        wt_id = dispatch.run_id or ""
        artifact = getattr(dispatch, "artifact", None) or f"final-{dispatch.ref}.txt"
        prompt = getattr(dispatch, "prompt", "")
        final_path = Path(wt_id.split("::", 1)[1]) / artifact if "::" in wt_id else None
        expected = f"调研完成 {dispatch.credentials[0]} 结论 41%"
        now = asyncio.get_event_loop().time
        deadline = now() + budget_s
        dialogs = 0
        cursor = 0
        t_output = None
        danced = False

        async def lane_retry(note: str) -> None:
            # transient lane failure (CLI hang killed at subprocess bound,
            # ok:false busy, …): observable pause, retried until budget
            await self.bus.emit("orch.progress",
                                {"ref": dispatch.ref, "note": note[:160]})
            await asyncio.sleep(2.0)

        while now() < deadline:
            try:
                await self.lane_orca.wait(handle, what="tui-idle",
                                          timeout_ms=ORCA_WAIT_SLICE_MS)
            except OrcaLaneTimeout:
                pass  # bounded slice consumed — keep polling within budget
            except OrcaLaneError as exc:
                msg = str(exc)
                if not (msg.startswith("exit=1") and '"ok": true' in msg):
                    await lane_retry(f"lane retry (wait): {msg}")
                    continue
            if final_path is not None and final_path.exists():
                try:
                    content = final_path.read_text(encoding="utf-8").strip()
                except OSError:
                    content = ""  # mid-write/mid-remove race — next pass re-reads
                if content == expected:  # exact — partial writes never satisfy
                    return content
            try:
                page, cursor = await self.lane_orca.read(handle, cursor=cursor,
                                                         limit=200)
                if ("Enter to confirm" in page or "2. No, exit" in page) \
                        and dialogs < 5:
                    dialogs += 1
                    await self.lane_orca.send(handle, "1")
                    continue
                if page.strip() and t_output is None:
                    t_output = now()
                if (not danced and prompt and t_output is not None
                        and now() - t_output >= ORCA_TURN_SETTLE_S):
                    await self.lane_orca.interrupt(handle)
                    await asyncio.sleep(3)
                    await self.lane_orca.send(handle, prompt)
                    danced = True
            except OrcaLaneError as exc:
                await lane_retry(f"lane retry (read): {exc}")
        return None

    # ---- head tool 2: query_status ----

    async def query_status(self, run_id: str | None = None) -> str:
        """Spoken-friendly aggregation of dais status (+ pending refs).

        Pending dais dispatches (ctx_ handles) are additionally classified
        via ``scan-wait-blocked`` so the user hears WHERE a run is stuck,
        not just that it is."""
        from rt_dsh_lane import DaisLaneError

        if self.liaison_mode:
            body = f"STATUS run={run_id}" if run_id else "STATUS"
            return await self._liaison_roundtrip(body, run_id)
        status = await self.lane.check_status(run_id)
        lines = []
        for entry in status.get("entries", []):
            if "tasks" in entry:
                lines.append(f"{entry['id']}：{entry['tasks']} 个任务")
            else:
                lines.append(f"{entry['id']}：{entry.get('summary', '')}")
        pending = [d.ref for d in self._runs.values() if d.ref in self._pending
                   and not self._pending[d.ref].done()]
        if pending:
            lines.append(f"语音头待收终稿 {len(pending)} 项")
        for d in self._runs.values():
            if d.ref not in pending:
                continue
            handles = []
            tid = getattr(d, "task_id", None)
            if tid and str(tid).startswith("ctx_"):
                handles.append(str(tid))
            handles += [c for c in getattr(d, "dag_ctx", {}).values()]
            for handle in handles:
                try:
                    label = (await self.lane.scan_wait_blocked(handle)).strip()
                except DaisLaneError:
                    continue  # classification is best-effort surfacing
                if label and label.lower() not in ("none", "ok", ""):
                    lines.append(f"{d.ref}：卡在 {label}")
        import json

        return json.dumps({"runs": lines, "pending": pending}, ensure_ascii=False)

    async def resolve(self, gate_id: str, resolution: str) -> str:
        """Resolve a decision gate (head tool for "unblock it with X")."""
        await self.lane.resolve_gate(gate_id, resolution)
        await self.bus.emit("orch.progress",
                            {"note": f"gate {gate_id} → {resolution}"})
        import json

        return json.dumps({"status": "resolved", "gate": gate_id,
                           "resolution": resolution}, ensure_ascii=False)

    # ---- head tool 3: cancel ----

    async def cancel(self, ref_or_run: str) -> str:
        """Cancel by ref (vh-…) or run_id; kills pending phase-2 task."""
        dispatch = self._runs.get(ref_or_run)
        run_id = dispatch.run_id if dispatch else ref_or_run
        task = self._pending.get(ref_or_run) or self._pending.get(run_id)
        if task and not task.done():
            task.cancel()
        if self.liaison_mode:
            body = f"CANCEL {ref_or_run} run={run_id}"
            return await self._liaison_roundtrip(body, run_id)
        # live DAG dispatches fail individually (best-effort; the pending
        # kill above is the authoritative stop)
        for handle in list(getattr(dispatch, "dag_ctx", {}).values()):
            try:
                await self.lane.fail_dispatch(handle, "cancelled by voice user")
            except Exception:
                pass
        if self.lane_mode == "b-orca" and run_id and "::" in run_id:
            state = "canceled"
            try:
                await self.lane_orca.stop(run_id)  # type: ignore[union-attr]
                await self.lane_orca._run("worktree", "rm", "--worktree", run_id)  # type: ignore[union-attr]
            except Exception:
                pass  # teardown best-effort; residual listed by worktree_ps
        elif self.lane_a is not None and run_id.startswith("t_"):
            state = await self.lane_a.cancel(run_id)
        else:
            # fail-dispatch expects a ctx_ dispatch handle and rejects
            # run/task ids (exit 1 "not found", live-probed) — the local
            # pending-task kill above is the authoritative cancellation
            if run_id.startswith("ctx_"):
                await self.lane.fail_dispatch(run_id, "cancelled by voice user")
            state = "canceled"
        # kg/14 #3: no artifact text on the bus — the cancelled outcome
        # rides the status field (store maps it to status=cancelled)
        await self.bus.emit("orch.done", {"ref": ref_or_run, "run_id": run_id,
                                          "status": "cancelled"})
        import json

        return json.dumps({"status": state, "run_id": run_id}, ensure_ascii=False)

    # ---- PoC BackendFn adapter (keeps Orchestrator unit-test compatible) ----

    def as_backend_fn(self):
        """Wrap as ``(agent, goal) -> BackendResult`` for Orchestrator."""

        async def backend_fn(agent: str, goal: str) -> BackendResult:
            receipt = await self.dispatch(f"[{agent}] {goal}")
            import json

            data = json.loads(receipt)
            return BackendResult(agent=agent, finding=receipt,
                                 canary=(data.get("credentials") or [None])[0])

        return backend_fn
