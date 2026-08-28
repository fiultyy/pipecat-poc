#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""rt_gateway 单测（VO-011 ①③；KG 04 §1–§4）：帧协议/握手/背压/慢客户端合并/
断线重连续接/TailReader。全部 fake pipeline，不起真 head（G2 离线优先）。"""

import asyncio
import json
import sys
from pathlib import Path

import pytest
from aiohttp import ClientSession, WSMsgType

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_dsh_lane import DaisLane, DaisLaneError  # noqa: E402
from rt_event_bus import EventBus  # noqa: E402
from rt_gateway import (  # noqa: E402
    TailReader,
    VoiceGateway,
    echo_head_provider,
)
import rt_gateway  # noqa: E402
from rt_transcript import TranscriptState  # noqa: E402

TOKEN = "unit-token"


# ---- fake head pipeline（鸭子面：transcript/start/stop/push_audio）----


class FakeHead:
    """echo 头：上行 PCM 记账并原样回发。"""

    def __init__(self, session):
        self.transcript = TranscriptState()
        self.session = session
        self.received: list[bytes] = []
        self.started = False
        self.stopped = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def push_audio(self, pcm: bytes):
        self.received.append(pcm)
        self.session.send_audio(pcm)


class SlowHead(FakeHead):
    """push_audio 阻塞到 release 置位（制造管线忙，触发背压）。"""

    def __init__(self, session):
        super().__init__(session)
        self.release = asyncio.Event()

    async def push_audio(self, pcm: bytes):
        self.received.append(pcm)
        await self.release.wait()
        self.session.send_audio(pcm)


def make_provider(head_cls=FakeHead, heads: list | None = None):
    def provider(session):
        head = head_cls(session)
        if heads is not None:
            heads.append(head)
        return head

    return provider


class GatewayFixture:
    """起一个随机端口网关 + 便利方法。"""

    def __init__(self, **kwargs):
        self.heads: list = []
        kwargs.setdefault("port", 0)
        kwargs.setdefault("host", "127.0.0.1")
        kwargs.setdefault("token", TOKEN)
        kwargs.setdefault("head_provider", make_provider(FakeHead, self.heads))
        self.gw = VoiceGateway(**kwargs)
        self.url = None

    async def __aenter__(self):
        await self.gw.start()
        self.url = f"http://127.0.0.1:{self.gw.port}"
        return self

    async def __aexit__(self, *exc):
        await self.gw.stop()

    async def ws(self):
        session = ClientSession()
        ws = await session.ws_connect(self.url + "/ws")
        ws._session = session  # keep ref for close
        return ws

    def server_session(self):
        assert len(self.gw._active) == 1, "expected exactly one active session"
        return next(iter(self.gw._active))


async def recv_json(ws, timeout: float = 3.0) -> dict:
    msg = await asyncio.wait_for(ws.receive(), timeout)
    assert msg.type == WSMsgType.TEXT, f"expected text frame, got {msg.type}"
    return json.loads(msg.data)


async def recv_until(ws, pred, timeout: float = 5.0) -> dict:
    """收文本帧直到 pred 命中；orch 二进制帧（若混入）跳过。"""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        msg = await asyncio.wait_for(ws.receive(), max(0.05, deadline - asyncio.get_event_loop().time()))
        if msg.type != WSMsgType.TEXT:
            continue
        data = json.loads(msg.data)
        if pred(data):
            return data


async def drain_text(ws, timeout: float = 2.0) -> list[dict]:
    """把当前积压的文本帧全部取出（超时即止）。"""
    out = []
    try:
        while True:
            out.append(await recv_json(ws, timeout=timeout))
            timeout = 0.15  # 第一帧后短窗判断排空
    except asyncio.TimeoutError:
        return out


async def handshake(ws, token: str = TOKEN):
    await ws.send_str(json.dumps({"t": "auth", "token": token}))
    auth = await recv_json(ws)
    assert auth["t"] == "auth.ok" and auth["session_id"]
    await ws.send_str(json.dumps({"t": "session.start"}))
    started = await recv_json(ws)
    assert started["t"] == "session.started"
    return auth, started


async def close_ws(ws):
    await ws._session.close()


# ---- A. HTTP 面 + 帧协议/握手 ----


@pytest.mark.asyncio
async def test_healthz_and_static_index():
    async with GatewayFixture() as fx:
        async with ClientSession() as http:
            async with http.get(fx.url + "/healthz") as r:
                assert r.status == 200
                body = await r.json()
                assert body["ok"] is True and body["service"] == "rt_gateway"
                assert body["sessions_active"] == 0
            async with http.get(fx.url + "/") as r:
                assert r.status == 200
                assert "text/html" in r.headers["Content-Type"]
                html = await r.text()
                assert "AudioWorklet" in html or "audioWorklet" in html
                assert "gate.resolve" in html  # gate 弹层回传接线


@pytest.mark.asyncio
async def test_auth_handshake_sequence():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await ws.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        auth = await recv_json(ws)
        assert auth["t"] == "auth.ok"
        assert auth["session_id"].startswith("s-")
        await ws.send_str(json.dumps({"t": "session.start"}))
        started = await recv_json(ws)
        assert started["t"] == "session.started"
        assert started["session_id"] == auth["session_id"]
        assert started["reseeded"] is False and started["entries"] == 0
        assert fx.heads[0].started is True
        await close_ws(ws)


@pytest.mark.asyncio
async def test_auth_bad_token_rejected_and_closed():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await ws.send_str(json.dumps({"t": "auth", "token": "wrong"}))
        err = await recv_json(ws)
        assert err == {"t": "error", "code": "auth", "msg": "invalid token"}
        msg = await asyncio.wait_for(ws.receive(), 3.0)
        assert msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED)
        await close_ws(ws)


@pytest.mark.asyncio
async def test_control_before_auth_rejected():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await ws.send_str(json.dumps({"t": "session.start"}))
        err = await recv_json(ws)
        assert err["t"] == "error" and err["code"] == "unauthorized"
        msg = await asyncio.wait_for(ws.receive(), 3.0)
        assert msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED)
        await close_ws(ws)


@pytest.mark.asyncio
async def test_bad_json_and_unknown_type_error_frames():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str("not-json{")
        err = await recv_json(ws)
        assert err["code"] == "bad_json"
        await ws.send_str(json.dumps({"t": "wat"}))
        err = await recv_json(ws)
        assert err["code"] == "bad_type"
        await ws.send_str(json.dumps({"t": "ping"}))
        pong = await recv_json(ws)
        assert pong["t"] == "pong" and "ts" in pong
        await close_ws(ws)


@pytest.mark.asyncio
async def test_double_start_and_double_auth_bad_state():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str(json.dumps({"t": "session.start"}))
        err = await recv_json(ws)
        assert err["code"] == "bad_state"
        await ws.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        err = await recv_json(ws)
        assert err["code"] == "bad_state"
        await close_ws(ws)


@pytest.mark.asyncio
async def test_session_end_graceful_closes():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str(json.dumps({"t": "session.end"}))
        ended = await recv_json(ws)
        assert ended["t"] == "session.ended"
        msg = await asyncio.wait_for(ws.receive(), 3.0)
        assert msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED)
        head = fx.heads[0]
        for _ in range(40):
            if head.stopped:
                break
            await asyncio.sleep(0.05)
        assert head.stopped is True
        assert fx.gw._resumable == {}  # 优雅结束不留续接槽
        await close_ws(ws)


# ---- B. media 路：echo / 背压 / 码率 ----


@pytest.mark.asyncio
async def test_media_binary_roundtrip_echo():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        chunks = [bytes([i % 256]) * 640 for i in range(10)]
        for c in chunks:
            await ws.send_bytes(c)
        got = []
        for _ in chunks:
            msg = await asyncio.wait_for(ws.receive(), 3.0)
            assert msg.type == WSMsgType.BINARY
            got.append(bytes(msg.data))
        assert got == chunks  # echo 头逐字节回发（PCM16/16k/mono 透传）
        sess = fx.server_session()
        assert sess.stats["audio_in_chunks"] == 10 and sess.stats["audio_dropped"] == 0
@pytest.mark.asyncio
async def test_media_before_start_rejected():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await ws.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        await recv_json(ws)
        await ws.send_bytes(b"\x00" * 640)
        err = await recv_json(ws)
        assert err["code"] == "no_session"
        await close_ws(ws)




@pytest.mark.asyncio
async def test_media_backpressure_drop_oldest_when_busy():
    heads: list[SlowHead] = []
    async with GatewayFixture(head_provider=make_provider(SlowHead, heads)) as fx:
        ws = await fx.ws()
        await handshake(ws)
        head = heads[0]
        chunks = [bytes([i]) * 640 for i in range(40)]
        for c in chunks:
            await ws.send_bytes(c)
        await asyncio.sleep(0.4)  # pump 卡在第 1 块；队列封顶 16 丢最老
        sess = fx.server_session()
        assert sess.stats["audio_in_chunks"] == 40
        assert sess.stats["audio_dropped"] == 23  # chunk18 起每块丢 1（2..17 先填满 16）
        assert len(head.received) == 1  # 只有第 1 块被 push
        head.release.set()
        await asyncio.sleep(0.5)
        # 收到 = 第 1 块 + 队列里最新的 16 块（17..40 中前 16 块被丢，留 25..40）
        assert len(head.received) == 17
        assert head.received[0] == chunks[0]
        assert head.received[1] == chunks[24]
        await close_ws(ws)


@pytest.mark.asyncio
async def test_media_rate_limit_above_64kbps_kicks():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        err = None
        for _ in range(20):  # 20 × 8KB = 160KB ≫ 64KB/s 窗口
            await ws.send_bytes(b"\x01" * 8192)
            await asyncio.sleep(0.01)
            try:
                msg = await asyncio.wait_for(ws.receive(), 0.3)
            except asyncio.TimeoutError:
                continue
            if msg.type == WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("code") == "rate":
                    err = data
                    break
        assert err is not None and err["msg"].find("64KB/s") >= 0
        msg = await asyncio.wait_for(ws.receive(), 3.0)
        assert msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED)
        await close_ws(ws)


# ---- C. event 路：schema + 全序 ----


@pytest.mark.asyncio
async def test_event_bus_forwarding_schema_kg04_s3():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        await fx.gw.bus.emit(
            "orch.dispatch",
            {"run_id": "run_x", "ref": "vh-1", "tickets": ["LK-01"], "credentials": ["【凭证R-1】"]},
        )
        await fx.gw.bus.emit("orch.progress", {"dispatch_id": "ctx_x", "lines": 12, "head": "首行"})
        await fx.gw.bus.emit("orch.done", {"run_id": "run_x", "artifact": "终稿", "credentials": ["【凭证R-1】"]})
        got = await drain_text(ws)
        by_t = {g["t"]: g for g in got}
        assert set(by_t) >= {"orch.dispatch", "orch.progress", "orch.done"}
        d = by_t["orch.dispatch"]
        assert d["run_id"] == "run_x" and d["ref"] == "vh-1"
        assert d["tickets"] == ["LK-01"] and d["credentials"] == ["【凭证R-1】"]
        assert isinstance(d["ts"], float)
        p = by_t["orch.progress"]
        assert p["dispatch_id"] == "ctx_x" and p["lines"] == 12 and p["head"] == "首行"
        assert by_t["orch.done"]["artifact"] == "终稿"
        await close_ws(ws)


@pytest.mark.asyncio
async def test_event_total_order_preserved():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        seq = []
        for i in range(30):
            kind = ["orch.dispatch", "orch.ack", "orch.progress", "orch.done"][i % 4]
            seq.append(kind)
            await fx.gw.bus.emit(kind, {"i": i})
        got = [g for g in await drain_text(ws) if g["t"].startswith("orch.")]
        assert [g["t"] for g in got] == seq  # 全序：到达序 == 发射序
        assert [g["i"] for g in got] == list(range(30))
        await close_ws(ws)


# ---- D. 慢客户端：progress 合并，关键帧不丢 ----


@pytest.mark.asyncio
async def test_slow_client_merges_progress_keeps_critical():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        sess = fx.server_session()
        # 冻结 sender 的第一帧 → 出站队列堆积 → 触发慢客户端路径
        freeze = asyncio.Event()
        orig_send = sess.ws.send_str

        async def slow_send(data):
            await freeze.wait()
            await orig_send(data)

        sess.ws.send_str = slow_send

        total, critical_at = 200, 150
        for i in range(1, total + 1):
            await fx.gw.bus.emit(
                "orch.progress", {"dispatch_id": "ctx_x", "lines": i, "head": f"h{i}"}
            )
            if i == critical_at:
                await fx.gw.bus.emit("orch.dispatch", {"run_id": "run_x", "ref": "vh-9"})
            if i == total:
                await fx.gw.bus.emit("orch.done", {"run_id": "run_x", "artifact": "done"})

        # 帧 1..101 独立入队（帧 101 入队时 len(_out)=100 未过 >100 阈）；
        # 帧 102..200（99 帧）并入合并槽
        assert sess.stats["progress_merged"] == total - 101
        freeze.set()

        frames = []
        while len(frames) < 104:
            frames.append(await recv_json(ws, timeout=3.0))
        progress = [f for f in frames if f["t"] == "orch.progress"]
        dispatches = [f for f in frames if f["t"] == "orch.dispatch"]
        dones = [f for f in frames if f["t"] == "orch.done"]
        assert len(progress) == 102, "200 帧 progress 只应有 102 帧下行（101+合并槽）"
        assert [p["lines"] for p in progress[:101]] == list(range(1, 102))  # 前 101 帧原样
        assert progress[-1]["lines"] == total and progress[-1]["head"] == "h200"  # 合并槽=max/最新
        assert len(dispatches) == 1 and dispatches[0]["ref"] == "vh-9"  # dispatch 不丢不并
        assert len(dones) == 1 and dones[0]["artifact"] == "done"      # done 不丢不并
        # 顺序：合并槽在 dispatch 之前、dispatch 在 done 之前
        idx = {t: frames.index(next(f for f in frames if f["t"] == t))
               for t in ("orch.progress", "orch.dispatch", "orch.done")}
        assert idx["orch.dispatch"] < idx["orch.done"]
        await close_ws(ws)


# ---- E. gate.resolve 回传 ----


class _LaneStub:
    def __init__(self, error: str | None = None):
        self.calls: list[tuple[str, str]] = []
        self.error = error

    async def resolve_gate(self, gate_id, resolution):
        self.calls.append((gate_id, resolution))
        if self.error:
            raise DaisLaneError(self.error)


@pytest.mark.asyncio
async def test_gate_resolve_forwarded_to_lane():
    calls: list[str] = []

    async def runner(argv):
        calls.append(" ".join(argv[2:]))
        return ("ok\n", "")

    async with GatewayFixture(lane=DaisLane(runner=runner)) as fx:
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str(json.dumps({"t": "gate.resolve", "gate_id": "g_1", "resolution": "A"}))
        got = await recv_json(ws)
        assert got == {"t": "gate.resolved", "gate_id": "g_1"}
        assert any("resolve-gate g_1 A" in c for c in calls)
        await close_ws(ws)


@pytest.mark.asyncio
async def test_gate_resolve_lane_error_returns_error_frame():
    stub = _LaneStub(error="gate already resolved")
    async with GatewayFixture(lane=stub) as fx:
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str(json.dumps({"t": "gate.resolve", "gate_id": "g_2", "resolution": "B"}))
        err = await recv_json(ws)
        assert err["t"] == "error" and err["code"] == "lane"
        assert "already resolved" in err["msg"]
        await close_ws(ws)


@pytest.mark.asyncio
async def test_gate_resolve_missing_fields_bad_request():
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str(json.dumps({"t": "gate.resolve", "gate_id": "g_3"}))
        err = await recv_json(ws)
        assert err["code"] == "bad_request"
        await close_ws(ws)


# ---- F. 断线重连：take_tail 重播种 ----


@pytest.mark.asyncio
async def test_reconnect_resumes_transcript_via_take_tail():
    async with GatewayFixture() as fx:
        ws1 = await fx.ws()
        auth1, started1 = await handshake(ws1)
        conv = started1["session_id"]
        fx.heads[0].transcript.seed("user", "帮我调研番茄工作法")
        fx.heads[0].transcript.seed("assistant", "已受理 ref vh-1")
        await close_ws(ws1)  # 未经 session.end 的断开
        for _ in range(60):  # 等 teardown 停靠续接槽
            if fx.gw._resumable:
                break
            await asyncio.sleep(0.05)
        assert conv in fx.gw._resumable

        ws2 = await fx.ws()
        await ws2.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        auth2 = await recv_json(ws2)
        assert auth2["session_id"] != conv  # 新连接级 id
        await ws2.send_str(json.dumps({"t": "session.start", "session_id": conv}))
        started2 = await recv_json(ws2)
        assert started2["t"] == "session.started"
        assert started2["reseeded"] is True and started2["entries"] == 2
        assert started2["session_id"] == conv  # 会话级 id 续接
        text = fx.heads[1].transcript.as_text()
        assert "番茄工作法" in text and "vh-1" in text
        # 优雅结束 → 续接槽清除
        await ws2.send_str(json.dumps({"t": "session.end"}))
        await recv_json(ws2)
        await close_ws(ws2)
        assert fx.gw._resumable == {}


@pytest.mark.asyncio
async def test_graceful_end_leaves_no_resume_slot():
    async with GatewayFixture() as fx:
        ws1 = await fx.ws()
        _, started1 = await handshake(ws1)
        conv = started1["session_id"]
        fx.heads[0].transcript.seed("user", "hello")
        await ws1.send_str(json.dumps({"t": "session.end"}))
        await recv_json(ws1)
        await close_ws(ws1)
        for _ in range(40):
            if not fx.gw._active:
                break
            await asyncio.sleep(0.05)
        assert fx.gw._resumable == {}

        ws2 = await fx.ws()
        await ws2.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        auth2 = await recv_json(ws2)
        await ws2.send_str(json.dumps({"t": "session.start", "session_id": conv}))
        started2 = await recv_json(ws2)
        assert started2["reseeded"] is False and started2["entries"] == 0
        assert started2["session_id"] == auth2["session_id"]  # 旧 conv 无停靠 → 新 id
        await close_ws(ws2)


# ---- G. 安全基线：token / 并发 ----


@pytest.mark.asyncio
async def test_concurrent_sessions_per_token_capped_at_2():
    async with GatewayFixture() as fx:
        ws1, ws2 = await fx.ws(), await fx.ws()
        await handshake(ws1)
        await handshake(ws2)
        ws3 = await fx.ws()
        await ws3.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        err = await recv_json(ws3)
        assert err["code"] == "concurrent_limit"
        msg = await asyncio.wait_for(ws3.receive(), 3.0)
        assert msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED)
        await close_ws(ws3)
        # 释放一个名额后第三连接可入
        await close_ws(ws1)
        for _ in range(40):
            if len(fx.gw._active) == 1:
                break
            await asyncio.sleep(0.05)
        ws4 = await fx.ws()
        await ws4.send_str(json.dumps({"t": "auth", "token": TOKEN}))
        ok = await recv_json(ws4)
        assert ok["t"] == "auth.ok"
        await close_ws(ws2)
        await close_ws(ws4)


def test_token_from_env_and_start_guard(monkeypatch):
    monkeypatch.setenv("VOICE_GATEWAY_TOKEN", "env-tok")
    gw = VoiceGateway(port=0)
    assert gw.check_token("env-tok") and not gw.check_token("other") and not gw.check_token("")
    gw2 = VoiceGateway(port=0, token=None)
    gw2.token = ""  # 显式空 token = 未配置
    import asyncio as _a

    with pytest.raises(ValueError):
        _a.run(gw2.start())


def test_echo_head_provider_wiring():
    class _FakeWs:
        pass

    sess = type("S", (), {"gateway": None, "ws": _FakeWs(), "_close_sent": False, "_out": [], "_out_wake": None})()
    head = echo_head_provider(sess)
    assert isinstance(head.transcript, TranscriptState)


# ---- H. TailReader：DaisLane.read-worker 增量 → orch.progress ----


@pytest.mark.asyncio
async def test_tail_reader_incremental_progress_then_exit():
    outs = [
        ("line A\nline B\n", "cursor: 2\n"),   # 增量 2 行
        ("line C\n", "cursor: 3\n"),           # 增量 1 行（累计 3）
        ("", "cursor: 3\n"),                   # 无增量：不 emit
        ('{"error": "dispatch not found"}\n', "cursor: 3\n"),  # 软错误 → 终态退出
        ("SHOULD NOT BE READ\n", "cursor: 9\n"),
    ]
    pop = iter(outs)

    async def runner(argv):
        return next(pop)

    lane = DaisLane(runner=runner)
    events: list[dict] = []

    async def emit(kind, payload):
        assert kind == "orch.progress"
        events.append(payload)

    reader = TailReader(lane, emit, poll_s=0.01)
    await asyncio.wait_for(reader.follow("ctx_abc"), timeout=5.0)  # 软错误后返回
    assert len(events) == 2
    assert events[0] == {"dispatch_id": "ctx_abc", "lines": 2, "head": "line A"}
    assert events[1]["lines"] == 3 and events[1]["head"] == "line C"
    assert all(e["dispatch_id"] == "ctx_abc" for e in events)
    assert lane._call_log[0][2:] == ["read-worker", "ctx_abc", "--after", "0", "--lines", "40"]


@pytest.mark.asyncio
async def test_tail_reader_stop_event_exits():
    async def runner(argv):
        return ("work\n", "cursor: 1\n")

    lane = DaisLane(runner=runner)
    events: list[dict] = []

    async def emit(kind, payload):
        events.append(payload)

    stop = asyncio.Event()
    reader = TailReader(lane, emit, poll_s=0.01)
    task = asyncio.create_task(reader.follow("ctx_x", stop))
    await asyncio.sleep(0.08)
    stop.set()
    await asyncio.wait_for(task, timeout=2.0)
    assert events, "至少捕获一次增量后才 stop"


# ---- F. 终稿温和排队注入（turn_idle；A 语义：不打断在飞回合）----


class IdleFakeHead:
    """带协议级 turn_idle 信号的 head 替身，记录 send_client_event。"""

    def __init__(self):
        self.turn_idle = asyncio.Event()
        self.turn_idle.set()
        self.sent: list[str] = []

    async def send_client_event(self, event):
        self.sent.append(type(event).__name__)


@pytest.mark.asyncio
async def test_final_inject_waits_for_turn_idle():
    from rt_gateway import _inject_final_when_idle

    head = IdleFakeHead()
    head.turn_idle.clear()  # 用户正在对话（active response）
    task = asyncio.create_task(_inject_final_when_idle(head, "终稿A"))
    await asyncio.sleep(0.1)
    assert not head.sent, "turn 进行中不得注入"
    assert not task.done(), "注入必须挂起等待而非丢弃"

    head.turn_idle.set()  # 回合结束
    ok = await asyncio.wait_for(task, timeout=2.0)
    assert ok is True
    assert head.sent == ["ConversationItemCreateEvent", "ResponseCreateEvent"]


@pytest.mark.asyncio
async def test_final_inject_without_idle_signal_is_immediate():
    from rt_gateway import _inject_final_when_idle

    class PlainHead:  # echo 头/旧替身：无 turn_idle 属性
        def __init__(self):
            self.sent = []

        async def send_client_event(self, event):
            self.sent.append(type(event).__name__)

    head = PlainHead()
    ok = await asyncio.wait_for(_inject_final_when_idle(head, "终稿B"), timeout=2.0)
    assert ok is True and len(head.sent) == 2, "无信号 head 保持旧行为：直接注入"


@pytest.mark.asyncio
async def test_final_inject_failure_returns_false():
    from rt_gateway import _inject_final_when_idle

    class DeadHead(IdleFakeHead):
        async def send_client_event(self, event):
            raise RuntimeError("websocket closed")

    ok = await asyncio.wait_for(
        _inject_final_when_idle(DeadHead(), "终稿C"), timeout=2.0)
    assert ok is False, "注入失败必须返回 False 供调用方重排队"


@pytest.mark.asyncio
async def test_final_inject_serializes_concurrent_finals():
    from rt_gateway import _inject_final_when_idle

    head = IdleFakeHead()

    async def run(tag: str):
        return await _inject_final_when_idle(head, tag)

    t1 = asyncio.create_task(run("终稿1"))
    await asyncio.sleep(0.05)
    head.turn_idle.clear()  # 第一条注入后自己的 response 活跃
    t2 = asyncio.create_task(run("终稿2"))
    await asyncio.sleep(0.1)
    assert head.sent.count("ResponseCreateEvent") == 1, "第二条必须等第一条的回合结束"
    head.turn_idle.set()
    assert await asyncio.wait_for(asyncio.gather(t1, t2), timeout=2.0)
    assert head.sent.count("ResponseCreateEvent") == 2


# ---- I. 台账写入面 + body.get / body.push（KG 14 §2.2，PR1）----


@pytest.fixture(autouse=True)
def _isolate_store_singleton():
    """台账单例测试隔离：前置清空、后置关闭并复位（绝不触达真实库）。"""
    rt_gateway._store = None
    yield
    if rt_gateway._store is not None:
        try:
            rt_gateway._store.close()
        except Exception:
            pass
        rt_gateway._store = None


async def observe_handshake(ws, token: str = TOKEN) -> dict:
    await ws.send_str(json.dumps({"t": "auth", "token": token}))
    auth = await recv_json(ws)
    assert auth["t"] == "auth.ok"
    await ws.send_str(json.dumps({"t": "session.start", "observe": True}))
    started = await recv_json(ws)
    assert started["t"] == "session.started"
    return started


async def _open_store(fx, tmp_path, monkeypatch):
    """tmp 台账 + 挂唯一写入面（与 main() 同一条 attach_store_bridge 路径）。"""
    monkeypatch.setenv("VOICE_STORE_DB", str(tmp_path / "store.db"))
    store = rt_gateway.init_store()
    assert store is not None, "rt_session_store 必须可用（PR1 并行产物）"
    assert await rt_gateway.attach_store_bridge(fx.gw) is not None
    return store


@pytest.mark.asyncio
async def test_body_get_hit_after_dispatch_done(tmp_path, monkeypatch):
    body = "采纳方案B，收益约41%\n对比明细……"
    async with GatewayFixture() as fx:
        store = await _open_store(fx, tmp_path, monkeypatch)
        ws = await fx.ws()
        await handshake(ws)
        await fx.gw.bus.emit("orch.dispatch", {
            "run_id": "run_x", "ref": "vh-1",
            "credentials": ["【凭证R-1】"], "lane": "b",
        })
        await fx.gw.bus.emit("orch.done", {
            "ref": "vh-1", "run_id": "run_x", "artifact": body,
        })
        # PR4：body.push 进 DEFAULT_VOICE_KINDS——语音会话自动收轻通知
        push = await recv_until(ws, lambda d: d.get("t") == "body.push")
        assert push["ref"] == "vh-1" and push["status"] == "done"
        assert push["inline"] == body and push["chars"] == len(body)
        await ws.send_str(json.dumps({"t": "body.get", "ref": "vh-1"}))
        item = await recv_until(ws, lambda d: d.get("t") == "body.item")
        assert item["ref"] == "vh-1"
        assert item["title"] == body.splitlines()[0][:16]
        assert item["text"] == body and item["chars"] == len(body)
        assert isinstance(item["ts"], float)
        # 台账侧对账：dispatch 进账 accepted→done 覆写，no 全局自增首号
        rec = store.get("vh-1")
        assert rec["status"] == "done" and rec["body"] == body
        assert rec["summary"] == body.splitlines()[0][:60]
        assert rec["chars"] == len(body) and rec["no"] == 1
        assert rec["run_id"] == "run_x" and rec["credentials"] == ["【凭证R-1】"]
        left = await drain_text(ws)
        assert all(d.get("t") != "body.push" for d in left)  # 轻通知只此一条
        await close_ws(ws)


@pytest.mark.asyncio
async def test_body_get_miss_returns_body_miss_error(tmp_path, monkeypatch):
    async with GatewayFixture() as fx:
        await _open_store(fx, tmp_path, monkeypatch)
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str(json.dumps({"t": "body.get", "ref": "vh-nobody"}))
        err = await recv_json(ws)
        assert err["t"] == "error" and err["code"] == "body_miss"
        assert "vh-nobody" in err["msg"]
        await ws.send_str(json.dumps({"t": "body.get"}))
        err = await recv_json(ws)
        assert err["code"] == "bad_request"
        await close_ws(ws)


@pytest.mark.asyncio
async def test_body_push_live_frame_and_replay_to_new_observers(tmp_path, monkeypatch):
    body = "第一行结论\n第二行详情"
    async with GatewayFixture() as fx:
        await _open_store(fx, tmp_path, monkeypatch)
        ws = await fx.ws()
        started = await observe_handshake(ws)
        assert "body.push" in started["topics"]  # SUBSCRIBABLE_KINDS 扩容回显
        await fx.gw.bus.emit("orch.dispatch", {
            "run_id": "run_y", "ref": "vh-9", "credentials": ["【凭证R-9】"],
        })
        await fx.gw.bus.emit("orch.done", {
            "ref": "vh-9", "run_id": "run_y", "artifact": body,
        })
        push = await recv_until(ws, lambda d: d.get("t") == "body.push")
        assert push["ref"] == "vh-9" and push["no"] == 1 and push["status"] == "done"
        assert push["title"] == "第一行结论" and push["summary"] == "第一行结论"
        assert push["chars"] == len(body) and push["inline"] == body
        assert isinstance(push["ts"], float)

        # 第二个 observe 接入 → topic_cache 索引回放（{t,items,ts}，无 body/inline）
        ws2 = await fx.ws()
        await observe_handshake(ws2)
        replay = await recv_until(ws2, lambda d: d.get("t") == "body.push")
        assert isinstance(replay.get("items"), list) and replay["items"]
        entry = replay["items"][-1]
        assert entry["ref"] == "vh-9" and entry["no"] == 1 and entry["status"] == "done"
        assert set(entry) == {"ref", "no", "status", "title", "summary", "chars", "ts"}
        await close_ws(ws)
        await close_ws(ws2)


@pytest.mark.asyncio
async def test_body_push_cancel_failed_oversize_and_unknown_ref(tmp_path, monkeypatch):
    big = "结" * 5000
    async with GatewayFixture() as fx:
        store = await _open_store(fx, tmp_path, monkeypatch)
        ws = await fx.ws()
        await observe_handshake(ws)
        # cancel 语义：artifact="(已取消)" → status=cancelled
        await fx.gw.bus.emit("orch.dispatch", {"ref": "vh-a", "run_id": "r1"})
        await fx.gw.bus.emit("orch.done", {"ref": "vh-a", "artifact": "(已取消)"})
        p = await recv_until(ws, lambda d: d.get("t") == "body.push" and d["ref"] == "vh-a")
        assert p["status"] == "cancelled" and p["inline"] == "(已取消)"
        # orch.failed → status=failed，错误文本入台账正文
        await fx.gw.bus.emit("orch.dispatch", {"ref": "vh-b", "run_id": "r2"})
        await fx.gw.bus.emit("orch.failed", {"ref": "vh-b", "error": "lane dead"})
        p = await recv_until(ws, lambda d: d.get("t") == "body.push" and d["ref"] == "vh-b")
        assert p["status"] == "failed" and p["inline"] == "lane dead"
        assert p["summary"] == "lane dead"
        # 超长正文：inline=null（全文只入台账，body.get 取）
        await fx.gw.bus.emit("orch.dispatch", {"ref": "vh-c", "run_id": "r3"})
        await fx.gw.bus.emit("orch.done", {"ref": "vh-c", "artifact": big})
        p = await recv_until(ws, lambda d: d.get("t") == "body.push" and d["ref"] == "vh-c")
        assert p["inline"] is None and p["chars"] == 5000
        await ws.send_str(json.dumps({"t": "body.get", "ref": "vh-c"}))
        item = await recv_until(ws, lambda d: d.get("t") == "body.item")
        assert item["text"] == big and item["chars"] == 5000
        # 未见 dispatch 的 ref：台账无行（update miss），轻通知照发
        await fx.gw.bus.emit("orch.done", {"ref": "vh-ghost", "artifact": "迟到的终稿"})
        p = await recv_until(ws, lambda d: d.get("t") == "body.push" and d["ref"] == "vh-ghost")
        assert p["status"] == "done" and p["no"] is None
        assert store.get("vh-ghost") is None
        # 台账终态对账
        assert store.get("vh-a")["status"] == "cancelled"
        assert store.get("vh-b")["status"] == "failed"
        assert store.get("vh-c")["body"] == big
        await close_ws(ws)


@pytest.mark.asyncio
async def test_body_push_cancel_via_status_field(tmp_path, monkeypatch):
    """backend 现行 cancel 形态（orch.done 带 status="cancelled"、无
    artifact——裁决 #10）→ 台账/轻通知均落 cancelled，不误判 done。"""
    async with GatewayFixture() as fx:
        store = await _open_store(fx, tmp_path, monkeypatch)
        ws = await fx.ws()
        await observe_handshake(ws)
        await fx.gw.bus.emit("orch.dispatch", {"ref": "vh-cx", "run_id": "r9"})
        await fx.gw.bus.emit("orch.done", {
            "ref": "vh-cx", "run_id": "r9", "status": "cancelled"})
        p = await recv_until(ws, lambda d: d.get("t") == "body.push" and d["ref"] == "vh-cx")
        assert p["status"] == "cancelled"
        assert store.get("vh-cx")["status"] == "cancelled"
        await close_ws(ws)


# ---- J. split 终稿交付（KG 14 §2.2/§2.3，PR3）：bridge 落 body + 通报形态 ----


@pytest.mark.asyncio
async def test_body_push_from_done_body_field(tmp_path, monkeypatch):
    """orch.done 载荷 body（PR3）→ 台账正文/轻通知 inline 有值；body 优先于
    旧 artifact 键（裁决 #3 正文单通道，body.push 承载）。"""
    body = "采纳方案B，收益约41%\n对比明细……"
    assert len(body) <= rt_gateway.BODY_INLINE_MAX_CHARS  # inline 应有值
    async with GatewayFixture() as fx:
        store = await _open_store(fx, tmp_path, monkeypatch)
        ws = await fx.ws()
        await observe_handshake(ws)
        await fx.gw.bus.emit("orch.dispatch", {"ref": "vh-b1", "run_id": "r1"})
        await fx.gw.bus.emit("orch.done", {
            "ref": "vh-b1", "run_id": "r1", "body": body})
        p = await recv_until(ws, lambda d: d.get("t") == "body.push" and d["ref"] == "vh-b1")
        assert p["status"] == "done" and p["inline"] == body
        assert p["summary"] == body.splitlines()[0][:60]
        rec = store.get("vh-b1")
        assert rec["body"] == body and rec["chars"] == len(body)
        assert rec["title"] == body.splitlines()[0][:16]
        # body 优先：两键并存时旧 artifact 不生效
        await fx.gw.bus.emit("orch.dispatch", {"ref": "vh-b2", "run_id": "r2"})
        await fx.gw.bus.emit("orch.done", {
            "ref": "vh-b2", "run_id": "r2", "body": "新正文", "artifact": "旧通道"})
        p = await recv_until(ws, lambda d: d.get("t") == "body.push" and d["ref"] == "vh-b2")
        assert p["inline"] == "新正文" and store.get("vh-b2")["body"] == "新正文"
        await close_ws(ws)


def test_final_mode_env_parsing(monkeypatch):
    """PR5 翻缺省：未设/杂值一律 split；仅精确 "fulltext" 回旧全文。"""
    monkeypatch.delenv("VOICE_FINAL_MODE", raising=False)
    assert rt_gateway._final_mode() == "split"
    monkeypatch.setenv("VOICE_FINAL_MODE", "fulltext")
    assert rt_gateway._final_mode() == "fulltext"
    for value in ("split", "SPLIT", "FULLTEXT", "fulltext ", "0", "spoilt", ""):
        monkeypatch.setenv("VOICE_FINAL_MODE", value)
        assert rt_gateway._final_mode() == "split", value


def test_turn_detection_env_parsing(monkeypatch):
    """VAD 旋钮：全未设→None（服务端默认）；设了才组装 server_vad；
    非法值忽略不炸。"""
    for key in ("VOICE_TURN_SILENCE_MS", "VOICE_TURN_PREFIX_MS",
                "VOICE_TURN_THRESHOLD"):
        monkeypatch.delenv(key, raising=False)
    assert rt_gateway._turn_detection_from_env() is None
    monkeypatch.setenv("VOICE_TURN_SILENCE_MS", "800")
    assert rt_gateway._turn_detection_from_env() == {
        "type": "server_vad", "silence_duration_ms": 800}
    monkeypatch.setenv("VOICE_TURN_THRESHOLD", "0.7")
    monkeypatch.setenv("VOICE_TURN_PREFIX_MS", "junk")
    td = rt_gateway._turn_detection_from_env()
    assert td == {"type": "server_vad", "silence_duration_ms": 800,
                  "threshold": 0.7}


@pytest.mark.asyncio
async def test_final_fulltext_injection_byte_identical(monkeypatch):
    """回退路径字节级回归：VOICE_FINAL_MODE=fulltext 注入串与旧实现一字不差
    （不触台账）。"""
    monkeypatch.setenv("VOICE_FINAL_MODE", "fulltext")
    rt_gateway._store = None  # fulltext 不得依赖 store 可用性
    ref, final = "vh-f1", '"Agent Final Message":\n\n采纳方案B\n全文……'
    text = await rt_gateway._final_injection_text(ref, final)
    expected = (
        "[编排终稿 vh-f1] \"Agent Final Message\":\n\n采纳方案B\n全文……\n"
        "请把上述终稿口语播报给用户：原样转述，不添加事实。"
    )
    assert text == expected
    assert text.encode("utf-8") == expected.encode("utf-8")


@pytest.mark.asyncio
async def test_final_split_notice_from_store(tmp_path, monkeypatch):
    """split 通报：[编排通报] + 单行 JSON，字段取台账（backend 先 emit
    orch.done 再调 on_final 的时序前提）；串内无任何指令句（裁决 #7）。"""
    monkeypatch.setenv("VOICE_FINAL_MODE", "split")
    body = "采纳方案B，收益约41%\n对比明细……"
    final = f'"Agent Final Message":\n\n{body}'
    async with GatewayFixture() as fx:
        store = await _open_store(fx, tmp_path, monkeypatch)
        await fx.gw.bus.emit("orch.dispatch", {"ref": "vh-s1", "run_id": "r1"})
        await fx.gw.bus.emit("orch.done", {"ref": "vh-s1", "run_id": "r1", "body": body})
        text = await rt_gateway._final_injection_text("vh-s1", final)
        assert text.startswith(rt_gateway.FINAL_NOTICE_PREFIX)
        assert "\n" not in text  # 单行：pending 缓冲按行语义不受扰
        rec = store.get("vh-s1")
        payload = json.loads(text[len(rt_gateway.FINAL_NOTICE_PREFIX):])
        assert payload == {"no": rec["no"], "ref": "vh-s1", "status": "done",
                           "summary": rec["summary"], "chars": rec["chars"]}
        assert payload["no"] == 1  # no 来自 store 受理分配
        assert payload["summary"] == body.splitlines()[0][:60]
        assert payload["chars"] == len(body)
        for word in ("播报", "转述", "请把"):
            assert word not in text, f"通报不得嵌指令词：{word}"


@pytest.mark.asyncio
async def test_final_split_notice_degrades_without_record(tmp_path, monkeypatch, capsys):
    """降级路径：store 查不到（或不可用）→ no 省略、summary/chars 从 final
    本体算（剥 FINAL_PREFIX），stderr 告警；形态仍是纯数据通报。"""
    monkeypatch.setenv("VOICE_FINAL_MODE", "split")
    body = "降级正文首行，恰好超过不了六十字的限制\n第二行"
    final = f'"Agent Final Message":\n\n{body}'
    async with GatewayFixture() as fx:
        await _open_store(fx, tmp_path, monkeypatch)  # store 在、ref 无行
        text = await rt_gateway._final_injection_text("vh-ghost", final)
        payload = json.loads(text[len(rt_gateway.FINAL_NOTICE_PREFIX):])
        assert "no" not in payload
        assert payload == {"ref": "vh-ghost", "status": "done",
                           "summary": body.splitlines()[0][:60], "chars": len(body)}
        for word in ("播报", "转述", "请把"):
            assert word not in text
        assert "degraded" in capsys.readouterr().err

        # store 整体不可用：同一条降级路径（_store_call 之外的 None 分支）
        rt_gateway._store = None
        text = await rt_gateway._final_injection_text("vh-ghost2", final)
        payload = json.loads(text[len(rt_gateway.FINAL_NOTICE_PREFIX):])
        assert "no" not in payload and payload["ref"] == "vh-ghost2"
        assert payload["summary"] == body.splitlines()[0][:60]
        assert payload["chars"] == len(body)


# ---- K. live 头接线（KG 14 §2.3，PR4）：voice_store 进 app_resources ----


@pytest.mark.asyncio
async def test_realtime_head_wires_voice_store_into_app_resources(tmp_path, monkeypatch):
    """build_realtime_head 组 PipelineWorker 时 app_resources 同时携带
    dsh_backend 与 voice_store（=_get_store() 构建时快照，可能 None 由工具侧
    C 降级兜住）。providers 与 PipelineWorker/WorkerRunner 均以替身注入——
    真实 worker 生命周期不属于单测面（live 阶段惰性导入的既定边界）。"""
    import types

    from pipecat.pipeline import worker as worker_mod
    from pipecat.processors.frame_processor import FrameProcessor
    from pipecat.workers import runner as runner_mod

    wired: dict = {}

    class _PassthroughHead(FrameProcessor):
        async def process_frame(self, frame, direction):
            await self.push_frame(frame, direction)

    class _FakeWorker:
        def __init__(self, pipeline, **kwargs):
            wired["app_resources"] = kwargs.get("app_resources")
            wired["pipeline"] = pipeline

        async def queue_frame(self, frame):
            pass

    class _FakeRunner:
        def __init__(self, handle_sigint=False):
            pass

        async def add_workers(self, *workers):
            pass

        async def run(self):
            await asyncio.Event().wait()  # 挂起等 stop 侧 cancel

    def _fake_config(**kw):
        return types.SimpleNamespace(**kw)

    def _fake_create(config):
        return _PassthroughHead()

    fake_providers = types.ModuleType("providers")
    fake_providers.RealtimeProvider = types.SimpleNamespace(QWEN="qwen")
    fake_providers.RealtimeProtocol = types.SimpleNamespace(DASHSCOPE_RT="dashscope-rt")
    fake_providers.RealtimeHeadConfig = _fake_config
    fake_providers.create_realtime_head = _fake_create
    monkeypatch.setitem(sys.modules, "providers", fake_providers)
    monkeypatch.setattr(worker_mod, "PipelineWorker", _FakeWorker)
    monkeypatch.setattr(runner_mod, "WorkerRunner", _FakeRunner)
    # head 注册表隔离：不读真实 ~/.config/voice-gateway/heads.json
    monkeypatch.setenv("VOICE_HEAD_PROFILES", str(tmp_path / "no-heads.json"))
    rt_gateway._reset_head_registry()

    async def _noop(*a, **k):
        pass

    session = types.SimpleNamespace(conv_id="s-wire", send_audio=_noop,
                                    drop_pending_audio=_noop)
    backend = types.SimpleNamespace(liaison_session="")
    adapter = await rt_gateway.build_realtime_head(session, EventBus(), backend)
    await adapter.stop()

    res = wired["app_resources"]
    assert res["dsh_backend"] is backend
    assert res["voice_store"] is rt_gateway._get_store()


# ---- L. head.compact + notify 相 + 翻缺省缺省形态（KG 14 §2.5/§2.3，PR5）----


@pytest.mark.asyncio
async def test_final_split_is_default_without_env(tmp_path, monkeypatch):
    """翻缺省：未设 env 即 split 通报，字段取台账（store 有行时）。"""
    monkeypatch.delenv("VOICE_FINAL_MODE", raising=False)
    body = "缺省即分流\n第二行"
    final = f'"Agent Final Message":\n\n{body}'
    async with GatewayFixture() as fx:
        store = await _open_store(fx, tmp_path, monkeypatch)
        await fx.gw.bus.emit("orch.dispatch", {"ref": "vh-d0", "run_id": "r0"})
        await fx.gw.bus.emit("orch.done", {"ref": "vh-d0", "run_id": "r0", "body": body})
        text = await rt_gateway._final_injection_text("vh-d0", final)
        assert text.startswith(rt_gateway.FINAL_NOTICE_PREFIX)
        rec = store.get("vh-d0")
        payload = json.loads(text[len(rt_gateway.FINAL_NOTICE_PREFIX):])
        assert payload == {"no": rec["no"], "ref": "vh-d0", "status": "done",
                           "summary": rec["summary"], "chars": rec["chars"]}
        for word in ("播报", "转述", "请把"):
            assert word not in text


@pytest.mark.asyncio
async def test_final_default_split_degrades_without_store(monkeypatch):
    """翻缺省后的降级面：store 不可用 → 字段从终稿本体算，仍是纯数据通报。"""
    monkeypatch.delenv("VOICE_FINAL_MODE", raising=False)
    rt_gateway._store = None
    body = "降级本体"
    final = f'"Agent Final Message":\n\n{body}'
    text = await rt_gateway._final_injection_text("vh-d9", final)
    payload = json.loads(text[len(rt_gateway.FINAL_NOTICE_PREFIX):])
    assert payload == {"ref": "vh-d9", "status": "done",
                       "summary": "降级本体", "chars": len(body)}


def test_compact_threshold_env_parsing(monkeypatch):
    """VOICE_COMPACT_CHARS：缺省 20000、0 关、非法值回落缺省。"""
    monkeypatch.delenv("VOICE_COMPACT_CHARS", raising=False)
    assert rt_gateway._compact_threshold() == 20000
    monkeypatch.setenv("VOICE_COMPACT_CHARS", "5000")
    assert rt_gateway._compact_threshold() == 5000
    monkeypatch.setenv("VOICE_COMPACT_CHARS", "0")
    assert rt_gateway._compact_threshold() == 0
    monkeypatch.setenv("VOICE_COMPACT_CHARS", "abc")
    assert rt_gateway._compact_threshold() == 20000


class CompactFakeHead:
    """压缩路径 head 替身：turn_idle 置位 + 记录发出的客户端事件。"""

    def __init__(self):
        self.turn_idle = asyncio.Event()
        self.turn_idle.set()
        self.events: list = []

    async def send_client_event(self, event):
        self.events.append(event)


def _mirror_items(log):
    """喂一段过阈值的镜像（含一个在飞工具调用）。"""
    log.add("i1", {"type": "message", "role": "user", "text": "查一下任务2的正文"})
    log.add("i2", {"type": "function_call", "name": "read_body",
                   "call_id": "call_1", "arguments": '{"ref":"vh-1"}'})
    log.add("i3", {"type": "message", "role": "assistant", "text": "好的" * 500})


@pytest.mark.asyncio
async def test_compact_runs_deletes_snapshot_and_emits(tmp_path, monkeypatch):
    """过线压缩全链：非 pinned 逐项 delete → 单条 state.snapshot user item
    （tasks=store ∪ 运行登记−store，running 带 elapsed_s）→ head.compact。"""
    import time
    import types

    from rt_conversation_items import ConversationCompactor, ConversationLog

    async with GatewayFixture() as fx:
        await _open_store(fx, tmp_path, monkeypatch)
        rt_gateway._store.put({
            "ref": "vh-d1", "no": 1, "status": "done",
            "title": "完成稿", "summary": "完成稿摘要", "body": "完成稿全文",
        })
        log = ConversationLog()
        _mirror_items(log)
        compactor = ConversationCompactor(log, trigger_chars=100)
        assert compactor.should_compact()
        head = CompactFakeHead()
        backend = types.SimpleNamespace(_runs={
            "vh-r1": types.SimpleNamespace(ts=time.time() - 412.2)})
        emitted: list[tuple[str, dict]] = []

        async def emit(kind, payload):
            emitted.append((kind, payload))

        assert await rt_gateway._run_compaction(
            head, log, compactor, conv_id="s-c1", backend=backend,
            emit=emit) is True

        deletes = [e for e in head.events
                   if type(e).__name__ == "ConversationItemDeleteEvent"]
        assert [e.item_id for e in deletes] == ["i1", "i3"], "在飞 call_1 钉住不删"
        creates = [e for e in head.events
                   if type(e).__name__ == "ConversationItemCreateEvent"]
        assert len(creates) == 1
        snap_text = creates[0].item.content[0].text
        assert "\n" not in snap_text, "快照串必须单行"
        snap = json.loads(snap_text)
        assert snap["t"] == "state.snapshot"
        assert [t["ref"] for t in snap["tasks"]] == ["vh-d1", "vh-r1"]
        assert snap["tasks"][0] == {"no": 1, "ref": "vh-d1", "status": "done",
                                    "summary": "完成稿摘要", "chars": 5}
        assert snap["tasks"][1]["status"] == "running"
        assert 412 <= snap["tasks"][1]["elapsed_s"] <= 413
        assert snap["counts"] == {"done": 1, "running": 1}
        # 镜像 = 钉住项 + 快照项（与服务端同 id，echo 到来时原位更新）
        assert [i.item_id for i in log.items()] == ["i2", creates[0].item.id]
        assert log.items()[1].role == "user" and log.items()[1].text == snap_text
        # head.compact 事件形态（§2.5）
        assert len(emitted) == 1
        kind, payload = emitted[0]
        assert kind == "head.compact"
        assert payload["conv_id"] == "s-c1" and payload["reason"] == "threshold"
        assert payload["pinned"] == 1
        assert payload["before_chars"] > payload["after_chars"] > 0
        assert isinstance(payload["ts"], float)


@pytest.mark.asyncio
async def test_compact_prefers_typed_delete_helper(tmp_path, monkeypatch):
    """qwen 头的 typed 删除助手优先（_send_item_delete 分派）。"""
    from rt_conversation_items import ConversationCompactor, ConversationLog

    class TypedDeleteHead(CompactFakeHead):
        def __init__(self):
            super().__init__()
            self.deleted: list[str] = []

        async def delete_conversation_item(self, item_id):
            self.deleted.append(item_id)

    async with GatewayFixture() as fx:
        await _open_store(fx, tmp_path, monkeypatch)
        log = ConversationLog()
        _mirror_items(log)
        compactor = ConversationCompactor(log, trigger_chars=100)
        head = TypedDeleteHead()

        async def emit(kind, payload):
            pass

        assert await rt_gateway._run_compaction(
            head, log, compactor, conv_id="s-t1", emit=emit) is True
        assert head.deleted == ["i1", "i3"]
        assert not [e for e in head.events
                    if type(e).__name__ == "ConversationItemDeleteEvent"]


@pytest.mark.asyncio
async def test_compact_skips_below_threshold_or_disabled(tmp_path, monkeypatch):
    """锁内复检阈值：未过线 / 0 关闭 → 零发送零 emit。"""
    from rt_conversation_items import ConversationCompactor, ConversationLog

    async with GatewayFixture() as fx:
        await _open_store(fx, tmp_path, monkeypatch)
        log = ConversationLog()
        log.add("i1", {"type": "message", "role": "user", "text": "短会话"})
        head = CompactFakeHead()
        emitted: list = []

        async def emit(kind, payload):
            emitted.append((kind, payload))

        below = ConversationCompactor(log, trigger_chars=10_000)
        assert await rt_gateway._run_compaction(
            head, log, below, conv_id="s-c0", emit=emit) is False
        off = ConversationCompactor(log, trigger_chars=0)
        assert await rt_gateway._run_compaction(
            head, log, off, conv_id="s-c0", emit=emit) is False
        assert head.events == [] and emitted == []


@pytest.mark.asyncio
async def test_compact_aborts_when_turn_never_idle(tmp_path, monkeypatch):
    """turn 不空闲（等不到 idle）→ 放弃本轮不降级：零删除零 emit。"""
    from rt_conversation_items import ConversationCompactor, ConversationLog

    async with GatewayFixture() as fx:
        await _open_store(fx, tmp_path, monkeypatch)
        log = ConversationLog()
        _mirror_items(log)
        compactor = ConversationCompactor(log, trigger_chars=100)
        head = CompactFakeHead()
        head.turn_idle.clear()
        emitted: list = []

        async def emit(kind, payload):
            emitted.append((kind, payload))

        assert await rt_gateway._run_compaction(
            head, log, compactor, conv_id="s-b1", emit=emit,
            idle_wait_s=0.05) is False
        assert head.events == [] and emitted == []


@pytest.mark.asyncio
async def test_compact_waits_for_final_inject_lock(tmp_path, monkeypatch):
    """锁序：终稿注入锁被持有时压缩挂起，释放后才执行（不竞态）。"""
    from rt_conversation_items import ConversationCompactor, ConversationLog

    async with GatewayFixture() as fx:
        await _open_store(fx, tmp_path, monkeypatch)
        # 独立锁：模块级锁跨测试事件循环复用会绑定首个 loop
        monkeypatch.setattr(rt_gateway, "_final_inject_lock", asyncio.Lock())
        log = ConversationLog()
        _mirror_items(log)
        compactor = ConversationCompactor(log, trigger_chars=100)
        head = CompactFakeHead()
        emitted: list = []

        async def emit(kind, payload):
            emitted.append((kind, payload))

        async with rt_gateway._final_inject_lock:
            task = asyncio.create_task(rt_gateway._run_compaction(
                head, log, compactor, conv_id="s-l1", emit=emit))
            await asyncio.sleep(0.15)
            assert not task.done() and head.events == [], "压缩必须等终稿注入锁"
        assert await asyncio.wait_for(task, timeout=3.0) is True
        assert emitted and emitted[0][0] == "head.compact"


@pytest.mark.asyncio
async def test_arm_compaction_taps_mirror_and_turn_idle(tmp_path, monkeypatch):
    """build 侧挂的 tap：mirror_sink 喂镜像；on_turn_idle 过线才排压缩任务。"""
    import types

    from rt_conversation_items import ConversationCompactor, ConversationLog

    async with GatewayFixture() as fx:
        await _open_store(fx, tmp_path, monkeypatch)
        head = CompactFakeHead()
        log = ConversationLog()
        compactor = ConversationCompactor(log, trigger_chars=100)
        session = types.SimpleNamespace(conv_id="s-arm")
        emitted: list = []

        async def emit(kind, payload):
            emitted.append((kind, payload))

        rt_gateway._arm_compaction(head, log, compactor, session=session,
                                   backend=None, emit=emit)
        head.mirror_sink({"item_id": "i1", "type": "message", "role": "user",
                          "text": "问好", "name": None, "call_id": None})
        assert [i.item_id for i in log.items()] == ["i1"]
        head.on_turn_idle()  # 未过线：不排任务
        await asyncio.sleep(0.1)
        assert head.events == [] and emitted == []
        head.mirror_sink({"item_id": "i2", "type": "message", "role": "assistant",
                          "text": "长" * 200, "name": None, "call_id": None})
        assert compactor.should_compact()
        head.on_turn_idle()
        for _ in range(40):
            if emitted:
                break
            await asyncio.sleep(0.05)
        assert len(emitted) == 1 and emitted[0][0] == "head.compact"
        assert emitted[0][1]["conv_id"] == "s-arm"
        # 压缩后镜像落快照项，二次 on_turn_idle 不再触发
        head.on_turn_idle()
        await asyncio.sleep(0.1)
        assert len(emitted) == 1


@pytest.mark.asyncio
async def test_head_compact_topic_subscribable():
    """head.compact 入 SUBSCRIBABLE 面：observe 缺省订阅回显 + 帧下发。"""
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        started = await observe_handshake(ws)
        assert "head.compact" in started["topics"]
        await fx.gw.bus.emit("head.compact", {
            "conv_id": "s-x", "before_chars": 21340, "after_chars": 512,
            "pinned": 3, "reason": "threshold"})
        got = await recv_until(ws, lambda d: d.get("t") == "head.compact")
        assert got["conv_id"] == "s-x" and got["reason"] == "threshold"
        assert got["before_chars"] == 21340 and got["after_chars"] == 512
        assert got["pinned"] == 3 and isinstance(got["ts"], float)
        await close_ws(ws)


@pytest.mark.asyncio
async def test_final_inject_calls_on_injected_after_sends():
    """notify 钩子时序：create+response.create 发出后才调用（注入成功点）。"""
    head = IdleFakeHead()

    async def on_injected():
        head.sent.append("notify")

    ok = await rt_gateway._inject_final_when_idle(
        head, "终稿N", on_injected=on_injected)
    assert ok is True
    assert head.sent == ["ConversationItemCreateEvent", "ResponseCreateEvent", "notify"]


@pytest.mark.asyncio
async def test_final_inject_failure_skips_on_injected():
    class DeadHead(IdleFakeHead):
        async def send_client_event(self, event):
            raise RuntimeError("websocket closed")

    calls: list = []

    async def on_injected():
        calls.append(1)

    ok = await rt_gateway._inject_final_when_idle(
        DeadHead(), "终稿X", on_injected=on_injected)
    assert ok is False and calls == []


@pytest.mark.asyncio
async def test_notify_phase_head_turn_shape_reaches_observers():
    """notify 相 payload 经观测面可达：head.turn{phase:notify,ref} 全字段。"""
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await observe_handshake(ws)
        await fx.gw.bus.emit("head.turn", {
            "conv_id": "s-n1", "phase": "notify", "ref": "vh-n1"})
        got = await recv_until(
            ws, lambda d: d.get("t") == "head.turn" and d.get("phase") == "notify")
        assert got["conv_id"] == "s-n1" and got["ref"] == "vh-n1"
        assert isinstance(got["ts"], float)
        await close_ws(ws)


# ---- M. 席位清理控制面（KG 14 §2.4）：fleet.cleanup → fleet.cleanup.result ----
# 全程零网络：loopback rpc 以 mock 替身注入（mode=release 本就不发 rpc），
# fleet.json/liaison.json 指 tmp 路径（MAESTRO_FLEET / VOICE_LIAISON_STATE）。


FLEET_SEED = {
    "port": 3080,
    "defaultWorkspaceId": "ws-60312e7a",
    "fleet": {
        "aa01": {
            "sessionId": "session-<redacted>",
            "role": "worker", "node": "node-<redacted>", "preset": "standard",
            "spawnedAt": "2026-08-24T11:07:16.905913+00:00", "status": "active",
        },
        "bb02": {
            "sessionId": "session-<redacted>",
            "role": "worker", "node": "vh-head-liaison", "preset": "maestro",
            "spawnedAt": "2026-08-26T13:31:02.364158+00:00", "status": "active",
        },
        "cc03": {
            "sessionId": "session-<redacted>",
            "role": "supervisor", "node": "vh-m5-closeout-supervisor-000501",
            "spawnedAt": "2026-08-25T09:00:00.000000+00:00", "status": "active",
        },
    },
}


def _seed_fleet(tmp_path, monkeypatch, seed: dict | None = None) -> Path:
    path = tmp_path / "fleet.json"
    path.write_text(json.dumps(seed or FLEET_SEED, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    monkeypatch.setenv("MAESTRO_FLEET", str(path))
    # 缺省隔离 liaison 绑定（缺席文件→无 note）；需要 note 的测试自行覆写
    monkeypatch.setenv("VOICE_LIAISON_STATE", str(tmp_path / "absent-liaison.json"))
    return path


def _mock_rpc(monkeypatch, exc: BaseException | None = None) -> list[tuple]:
    """替身 loopback rpc：记录 (method, payload)；exc 非空则抛。"""
    calls: list[tuple] = []

    async def _fake(method: str, payload: dict):
        calls.append((method, dict(payload)))
        if exc is not None:
            raise exc
        return {"archivedSessionIds": [payload.get("sessionId")]}

    monkeypatch.setattr(rt_gateway, "_dsh_api", _fake)
    return calls


async def _fleet_cleanup(ws, ids: list, mode: str, req_id="q-1") -> dict:
    await ws.send_str(json.dumps(
        {"t": "fleet.cleanup", "ids": ids, "mode": mode, "req_id": req_id}))
    return await recv_until(ws, lambda d: d.get("t") == "fleet.cleanup.result")


@pytest.mark.asyncio
async def test_fleet_cleanup_release_removes_entries_without_rpc(tmp_path, monkeypatch):
    """mode=release：只摘条目不碰会话（零 loopback 调用）；原子写保留
    其余条目与顶层键原样、目标条目消失。"""
    path = _seed_fleet(tmp_path, monkeypatch)
    calls = _mock_rpc(monkeypatch)
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        got = await _fleet_cleanup(ws, ["aa01", "cc03"], "release")
        assert got["req_id"] == "q-1"
        assert got["results"] == [{"id": "aa01", "ok": True},
                                  {"id": "cc03", "ok": True}]
        await close_ws(ws)
    assert calls == [], "release 不得触达 loopback 会话面"
    after = json.loads(path.read_text(encoding="utf-8"))
    assert set(after["fleet"]) == {"bb02"}, "目标条目消失"
    assert after["fleet"]["bb02"] == FLEET_SEED["fleet"]["bb02"], "其余条目原样保留"
    assert after["port"] == 3080 and after["defaultWorkspaceId"] == "ws-60312e7a"
    assert not list(path.parent.glob(".fleet-*")), "原子写不留残留临时文件"


@pytest.mark.asyncio
async def test_fleet_cleanup_end_archives_session_then_removes(tmp_path, monkeypatch):
    """mode=end：逐 id 先 loopback 真死会话（{sessionId}）再摘条目。"""
    path = _seed_fleet(tmp_path, monkeypatch)
    calls = _mock_rpc(monkeypatch)
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        got = await _fleet_cleanup(ws, ["aa01", "bb02"], "end", req_id="q-end")
        assert got["req_id"] == "q-end"
        assert [r["id"] for r in got["results"]] == ["aa01", "bb02"]
        assert all(r["ok"] for r in got["results"])
        await close_ws(ws)
    assert calls == [
        ("workspace.archiveSession",
         {"sessionId": "session-<redacted>"}),
        ("workspace.archiveSession",
         {"sessionId": "session-<redacted>"}),
    ]
    after = json.loads(path.read_text(encoding="utf-8"))
    assert set(after["fleet"]) == {"cc03"}


@pytest.mark.asyncio
async def test_fleet_cleanup_end_tolerates_gone_session(tmp_path, monkeypatch):
    """会话已不在（session-not-found 分型）→ 容忍继续：条目照摘、ok:true。"""
    path = _seed_fleet(tmp_path, monkeypatch)
    _mock_rpc(monkeypatch, exc=RuntimeError(
        "workspace.archiveSession: {'code': 'session-not-found', "
        "'message': \"cannot archive session 'x': no such session\"}"))
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        got = await _fleet_cleanup(ws, ["aa01"], "end")
        assert got["results"] == [{"id": "aa01", "ok": True}]
        await close_ws(ws)
    assert "aa01" not in json.loads(path.read_text(encoding="utf-8"))["fleet"]


@pytest.mark.asyncio
async def test_fleet_cleanup_end_hard_rpc_failure_still_removes(tmp_path, monkeypatch):
    """session.end 失败不阻断摘条目（硬失败亦然——注册面与真死解耦）。"""
    path = _seed_fleet(tmp_path, monkeypatch)
    _mock_rpc(monkeypatch, exc=OSError("connection refused"))
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        got = await _fleet_cleanup(ws, ["bb02"], "end")
        assert got["results"] == [{"id": "bb02", "ok": True}]
        await close_ws(ws)
    after = json.loads(path.read_text(encoding="utf-8"))
    assert set(after["fleet"]) == {"aa01", "cc03"}


@pytest.mark.asyncio
async def test_fleet_cleanup_not_found_reports_and_rest_continue(tmp_path, monkeypatch):
    """id 不存在→该条 ok:false error:not_found，其余照处理。"""
    path = _seed_fleet(tmp_path, monkeypatch)
    calls = _mock_rpc(monkeypatch)
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        got = await _fleet_cleanup(ws, ["zz99", "aa01"], "end")
        assert got["results"] == [
            {"id": "zz99", "ok": False, "error": "not_found"},
            {"id": "aa01", "ok": True},
        ]
        await close_ws(ws)
    assert len(calls) == 1, "not_found 条目不触达 loopback"
    after = json.loads(path.read_text(encoding="utf-8"))
    assert set(after["fleet"]) == {"bb02", "cc03"}


@pytest.mark.asyncio
async def test_fleet_cleanup_all_not_found_writes_nothing(tmp_path, monkeypatch):
    """全 not_found（零有效摘除）→ 不触碰文件（内容字节不变、无临时文件）。"""
    path = _seed_fleet(tmp_path, monkeypatch)
    before = path.read_bytes()
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        got = await _fleet_cleanup(ws, ["xx00", "yy00"], "release")
        assert all(r["ok"] is False and r["error"] == "not_found"
                   for r in got["results"])
        await close_ws(ws)
    assert path.read_bytes() == before
    assert not list(path.parent.glob(".fleet-*"))


@pytest.mark.asyncio
async def test_fleet_cleanup_bad_request_shapes(tmp_path, monkeypatch):
    """ids 空/缺席/非串、mode 非法 → error bad_request；fleet.json 不动。"""
    path = _seed_fleet(tmp_path, monkeypatch)
    calls = _mock_rpc(monkeypatch)
    before = path.read_bytes()
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        for payload in (
            {"ids": [], "mode": "release"},
            {"mode": "release"},                       # ids 缺席
            {"ids": "aa01", "mode": "release"},        # 非列表
            {"ids": ["aa01", 2], "mode": "release"},   # 元素非串
            {"ids": ["aa01"], "mode": "kill"},
            {"ids": ["aa01"]},                         # mode 缺席
        ):
            await ws.send_str(json.dumps({"t": "fleet.cleanup", **payload}))
            err = await recv_json(ws)
            assert err["t"] == "error" and err["code"] == "bad_request", payload
        await close_ws(ws)
    assert calls == []
    assert path.read_bytes() == before


@pytest.mark.asyncio
async def test_fleet_cleanup_missing_fleet_file_internal(tmp_path, monkeypatch):
    """fleet.json 缺席/不可读 → error internal（非 bad_request）。"""
    monkeypatch.setenv("MAESTRO_FLEET", str(tmp_path / "nope" / "fleet.json"))
    _mock_rpc(monkeypatch)
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str(json.dumps(
            {"t": "fleet.cleanup", "ids": ["aa01"], "mode": "release"}))
        err = await recv_json(ws)
        assert err["t"] == "error" and err["code"] == "internal"
        await close_ws(ws)


@pytest.mark.asyncio
async def test_fleet_cleanup_liaison_note_on_bound_code(tmp_path, monkeypatch):
    """id 恰为当前 liaison 绑定（liaison.json code）→ 该条附
    note:active-liaison，不阻断摘除；无绑定文件时无 note。"""
    path = _seed_fleet(tmp_path, monkeypatch)
    liaison = tmp_path / "liaison.json"
    liaison.write_text(json.dumps({"code": "bb02",
                                   "sessionId": "session-<redacted>",
                                   "spawned_at": 1787889874.6}), encoding="utf-8")
    monkeypatch.setenv("VOICE_LIAISON_STATE", str(liaison))
    _mock_rpc(monkeypatch)
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        got = await _fleet_cleanup(ws, ["aa01", "bb02"], "release")
        assert got["results"] == [
            {"id": "aa01", "ok": True},
            {"id": "bb02", "ok": True, "note": "active-liaison"},
        ]
        await close_ws(ws)
    after = json.loads(path.read_text(encoding="utf-8"))
    assert set(after["fleet"]) == {"cc03"}, "note 不阻断摘除"

    # 绑定文件缺席 → 无 note（对其他席位的清理不带误注）
    path2 = tmp_path / "fleet2.json"
    path2.write_text(json.dumps(FLEET_SEED, ensure_ascii=False, indent=2),
                     encoding="utf-8")
    monkeypatch.setenv("MAESTRO_FLEET", str(path2))
    monkeypatch.setenv("VOICE_LIAISON_STATE", str(tmp_path / "absent-liaison.json"))
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        got = await _fleet_cleanup(ws, ["aa01"], "release")
        assert got["results"] == [{"id": "aa01", "ok": True}]
        await close_ws(ws)
    assert "aa01" not in json.loads(path2.read_text(encoding="utf-8"))["fleet"]


@pytest.mark.asyncio
async def test_fleet_cleanup_result_frame_shape_from_observe(tmp_path, monkeypatch):
    """客户端真实路径（observe 会话 send_request）+ 回包帧形态：键集恰为
    {t,req_id,results}、条目键集 {id,ok} / {id,ok,error}、顺序随 ids。"""
    _seed_fleet(tmp_path, monkeypatch)
    _mock_rpc(monkeypatch)
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await observe_handshake(ws)  # 客户端席位控制走 observe 连接
        await ws.send_str(json.dumps({"t": "fleet.cleanup", "ids": ["cc03", "zz99"],
                                      "mode": "end", "req_id": "clean-1"}))
        got = await recv_until(ws, lambda d: d.get("t") == "fleet.cleanup.result")
        assert set(got) == {"t", "req_id", "results"}
        assert got["req_id"] == "clean-1"
        assert [set(r) for r in got["results"]] == [{"id", "ok"}, {"id", "ok", "error"}]
        assert got["results"][0] == {"id": "cc03", "ok": True}
        assert got["results"][1] == {"id": "zz99", "ok": False, "error": "not_found"}
        await close_ws(ws)


# ---- M2. head 配置注册表（PR8：多 head 配置，激活单例）----

_HEAD_ENV_KEYS = ("VOICE_HEAD_MODEL", "VOICE_HEAD_VOICE", "VOICE_HEAD_DOCTRINE",
                  "VOICE_TURN_SILENCE_MS", "VOICE_TURN_PREFIX_MS",
                  "VOICE_TURN_THRESHOLD", "VOICE_HEAD_PROFILE")


def _fake_head_build_module(monkeypatch, wired: dict):
    """build_realtime_head 的 providers/worker 替身（同 wires 测试形制），
    额外把 RealtimeHeadConfig 收到的 kwargs 整包记进 wired["config"]。"""
    import types

    from pipecat.pipeline import worker as worker_mod
    from pipecat.processors.frame_processor import FrameProcessor
    from pipecat.workers import runner as runner_mod

    class _PassthroughHead(FrameProcessor):
        async def process_frame(self, frame, direction):
            await self.push_frame(frame, direction)

    class _FakeWorker:
        def __init__(self, pipeline, **kwargs):
            wired["app_resources"] = kwargs.get("app_resources")

        async def queue_frame(self, frame):
            pass

    class _FakeRunner:
        def __init__(self, handle_sigint=False):
            pass

        async def add_workers(self, *workers):
            pass

        async def run(self):
            await asyncio.Event().wait()

    def _fake_config(**kw):
        wired["config"] = kw
        return types.SimpleNamespace(**kw)

    fake_providers = types.ModuleType("providers")
    fake_providers.RealtimeProvider = types.SimpleNamespace(QWEN="qwen")
    fake_providers.RealtimeProtocol = types.SimpleNamespace(DASHSCOPE_RT="dashscope-rt")
    fake_providers.RealtimeHeadConfig = _fake_config
    fake_providers.create_realtime_head = lambda config: _PassthroughHead()
    monkeypatch.setitem(sys.modules, "providers", fake_providers)
    monkeypatch.setattr(worker_mod, "PipelineWorker", _FakeWorker)
    monkeypatch.setattr(runner_mod, "WorkerRunner", _FakeRunner)


async def _build_with_heads(tmp_path, monkeypatch, heads_doc, env_extra=None) -> dict:
    """隔离 env + 注册表后走一次 build_realtime_head，回 config kwargs。"""
    import types

    wired: dict = {}
    _fake_head_build_module(monkeypatch, wired)
    for k in _HEAD_ENV_KEYS:
        monkeypatch.delenv(k, raising=False)
    if heads_doc is not None:
        p = tmp_path / "heads.json"
        p.write_text(json.dumps(heads_doc, ensure_ascii=False), encoding="utf-8")
        monkeypatch.setenv("VOICE_HEAD_PROFILES", str(p))
    else:
        monkeypatch.setenv("VOICE_HEAD_PROFILES", str(tmp_path / "missing.json"))
    for k, v in (env_extra or {}).items():
        monkeypatch.setenv(k, v)
    rt_gateway._reset_head_registry()

    async def _noop(*a, **k):
        pass

    session = types.SimpleNamespace(conv_id="s-prof", send_audio=_noop,
                                    drop_pending_audio=_noop)
    backend = types.SimpleNamespace(liaison_session="")
    adapter = await rt_gateway.build_realtime_head(session, EventBus(), backend)
    await adapter.stop()
    rt_gateway._reset_head_registry()
    return wired["config"]


@pytest.mark.asyncio
async def test_head_build_profile_fields_override_env(tmp_path, monkeypatch):
    """激活 profile 的字段优先；占位副本（全 null）不设字段。"""
    cfg = await _build_with_heads(tmp_path, monkeypatch, {
        "active": "alpha",
        "profiles": [
            {"name": "alpha", "model": "m-profile", "voice": "v-profile",
             "doctrine": "# Persona inline", "turn_silence_ms": 900},
            {"name": "beta"},
        ],
    }, env_extra={"VOICE_HEAD_MODEL": "m-env", "VOICE_HEAD_VOICE": "v-env"})
    assert cfg["model"] == "m-profile" and cfg["voice"] == "v-profile"
    assert cfg["system_instruction"] == "# Persona inline"
    assert cfg["turn_detection"] == {"type": "server_vad",
                                     "silence_duration_ms": 900}


@pytest.mark.asyncio
async def test_head_build_null_fields_fall_back_to_env(tmp_path, monkeypatch):
    """全 null 副本 = 旧单头路径：model/voice/VAD 走 env，doctrine 走内置。"""
    cfg = await _build_with_heads(tmp_path, monkeypatch, {
        "active": "beta",
        "profiles": [{"name": "alpha", "model": "x"}, {"name": "beta"}],
    }, env_extra={"VOICE_HEAD_MODEL": "m-env", "VOICE_HEAD_VOICE": "v-env",
                  "VOICE_TURN_SILENCE_MS": "800"})
    assert cfg["model"] == "m-env" and cfg["voice"] == "v-env"
    assert cfg["turn_detection"] == {"type": "server_vad",
                                     "silence_duration_ms": 800}
    assert cfg["system_instruction"].startswith("# Persona")


@pytest.mark.asyncio
async def test_head_build_doctrine_file_with_blank_fallback(tmp_path, monkeypatch):
    d = tmp_path / "doctrine.md"
    d.write_text("DOCTRINE-FILE", encoding="utf-8")
    cfg = await _build_with_heads(tmp_path, monkeypatch, {
        "active": "a", "profiles": [{"name": "a", "doctrine_file": str(d)}]})
    assert cfg["system_instruction"] == "DOCTRINE-FILE"
    blank = tmp_path / "blank.md"
    blank.write_text("   \n", encoding="utf-8")
    cfg2 = await _build_with_heads(tmp_path, monkeypatch, {
        "active": "a", "profiles": [{"name": "a", "doctrine_file": str(blank)}]})
    assert cfg2["system_instruction"].startswith("# Persona")


async def _switch_heads_file(tmp_path, active="nova", pin=None):
    heads = tmp_path / "heads.json"
    heads.write_text(json.dumps({
        "active": active,
        "profiles": [{"name": "nova", "label": "N"}, {"name": "echo", "label": "E"}],
    }), encoding="utf-8")
    return heads


@pytest.mark.asyncio
async def test_head_list_and_switch_roundtrip(tmp_path, monkeypatch):
    """head.list/switch 全链路：切换写回文件、重复切换幂等、未知名 bad_request。"""
    heads = await _switch_heads_file(tmp_path)
    monkeypatch.setenv("VOICE_HEAD_PROFILES", str(heads))
    rt_gateway._reset_head_registry()
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str(json.dumps({"t": "head.list", "req_id": "q1"}))
        got = await recv_until(ws, lambda d: d.get("t") == "head.list.result")
        assert got["active"] == "nova" and got["file_backed"] is True
        assert got["env_pinned"] is False
        assert [p["name"] for p in got["profiles"]] == ["nova", "echo"]
        assert got["profiles"][0]["active"] is True

        await ws.send_str(json.dumps({"t": "head.switch", "name": "echo",
                                      "req_id": "q2"}))
        sw = await recv_until(ws, lambda d: d.get("t") == "head.switch.result")
        assert sw["ok"] is True and sw["active"] == "echo"
        assert "下一次" in sw["note"]
        assert json.loads(heads.read_text(encoding="utf-8"))["active"] == "echo"

        await ws.send_str(json.dumps({"t": "head.list", "req_id": "q3"}))
        got2 = await recv_until(
            ws, lambda d: d.get("t") == "head.list.result" and d.get("req_id") == "q3")
        assert got2["active"] == "echo"

        await ws.send_str(json.dumps({"t": "head.switch", "name": "echo",
                                      "req_id": "q4"}))
        sw2 = await recv_until(
            ws, lambda d: d.get("t") == "head.switch.result" and d.get("req_id") == "q4")
        assert sw2["ok"] is True and sw2["note"] == "已是激活 head"

        await ws.send_str(json.dumps({"t": "head.switch", "name": "ghost"}))
        err = await recv_until(ws, lambda d: d.get("t") == "error")
        assert err["code"] == "bad_request" and "ghost" in err["msg"]
        await close_ws(ws)
    rt_gateway._reset_head_registry()


@pytest.mark.asyncio
async def test_head_switch_rejected_in_single_head_mode(tmp_path, monkeypatch):
    """无 profiles 文件：head.list 报单头模式，head.switch 拒绝。"""
    monkeypatch.setenv("VOICE_HEAD_PROFILES", str(tmp_path / "missing.json"))
    rt_gateway._reset_head_registry()
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str(json.dumps({"t": "head.list", "req_id": "q1"}))
        got = await recv_until(ws, lambda d: d.get("t") == "head.list.result")
        assert got["file_backed"] is False and got["active"] == "default"
        await ws.send_str(json.dumps({"t": "head.switch", "name": "default"}))
        err = await recv_until(ws, lambda d: d.get("t") == "error")
        assert err["code"] == "bad_request" and "single-head" in err["msg"]
        await close_ws(ws)
    rt_gateway._reset_head_registry()


@pytest.mark.asyncio
async def test_head_switch_rejected_when_env_pinned(tmp_path, monkeypatch):
    """VOICE_HEAD_PROFILE 钉住：list 标 env_pinned，switch 拒绝。"""
    heads = await _switch_heads_file(tmp_path)
    monkeypatch.setenv("VOICE_HEAD_PROFILES", str(heads))
    monkeypatch.setenv("VOICE_HEAD_PROFILE", "nova")
    rt_gateway._reset_head_registry()
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str(json.dumps({"t": "head.list", "req_id": "q1"}))
        got = await recv_until(ws, lambda d: d.get("t") == "head.list.result")
        assert got["env_pinned"] is True and got["active"] == "nova"
        await ws.send_str(json.dumps({"t": "head.switch", "name": "echo"}))
        err = await recv_until(ws, lambda d: d.get("t") == "error")
        assert err["code"] == "bad_request" and "pinned" in err["msg"]
        await close_ws(ws)
    rt_gateway._reset_head_registry()


# ---- M3. 席位状态简报（PR9）：fleet.brief → _fleet_brief_payload ----
# 全程零网络：session.list 以 _dsh_api 替身注入（wire 形状 {items:[…]}），
# fleet.json/liaison.json 指 tmp 路径（复用 _seed_fleet）。


def _session_item(session_id, *, running=True, title="", long_task=None,
                  updated_ms=None) -> dict:
    """session.list 条目替身（真实 wire 形状；join 面外字段从简）。"""
    import time

    values: dict = {}
    if title:
        values["title"] = title
    if long_task is not None:
        values["longTask"] = long_task
    return {
        "sessionId": session_id,
        "updatedAt": (updated_ms if updated_ms is not None
                      else int(time.time() * 1000)),
        "running": running,
        "blank": False,
        "cwd": "/tmp/x",
        "agentPreset": "standard",
        "projections": {"asOfSeq": 7, "values": values},
    }


def _mock_session_list(monkeypatch, items) -> list[tuple]:
    """替身 loopback session.list：记录 (method, payload)、回 {items}。"""
    calls: list[tuple] = []

    async def _fake(method: str, payload: dict):
        calls.append((method, dict(payload)))
        return {"items": items}

    monkeypatch.setattr(rt_gateway, "_dsh_api", _fake)
    return calls


async def _fleet_brief(ws, req_id="b-1") -> dict:
    await ws.send_str(json.dumps({"t": "fleet.brief", "req_id": req_id}))
    return await recv_until(ws, lambda d: d.get("t") == "fleet.brief.result")


@pytest.mark.asyncio
async def test_fleet_brief_payload_join_live_seat_fields(tmp_path, monkeypatch):
    """join 口径：在列席位 live=true，running/title/task/idle_s 取条目；
    idle_s=(now-updatedAt)/1000（updatedAt 毫秒纪元）下限 0。"""
    import time

    _seed_fleet(tmp_path, monkeypatch)
    now_ms = int(time.time() * 1000)
    items = [
        _session_item("session-<redacted>",
                      running=True, title="封装统一client",
                      long_task={"task": {"phase": "executing"}},
                      updated_ms=now_ms - 90_000),
        _session_item("session-<redacted>",
                      running=False, title="",
                      long_task={"task": {"phase": "paused"}},
                      updated_ms=now_ms + 5_000),  # 时钟偏前 → 钳 0
    ]
    calls = _mock_session_list(monkeypatch, items)
    payload = await rt_gateway._fleet_brief_payload()
    assert calls == [("session.list", {})]
    assert set(payload) == {"seats"}
    seats = {s["id"]: s for s in payload["seats"]}
    aa01 = seats["aa01"]
    assert set(aa01) == {"id", "node", "role", "status", "live", "running",
                         "title", "task", "idle_s"}
    assert aa01["node"] == "node-<redacted>" and aa01["role"] == "worker"
    assert aa01["status"] == "active" and aa01["live"] is True
    assert aa01["running"] is True and aa01["title"] == "封装统一client"
    assert aa01["task"] == "executing"
    assert 89 <= aa01["idle_s"] <= 91
    bb02 = seats["bb02"]
    assert bb02["live"] is True and bb02["running"] is False
    assert bb02["title"] == "" and bb02["task"] == "paused"
    assert bb02["idle_s"] == 0


@pytest.mark.asyncio
async def test_fleet_brief_payload_dead_session_and_retired_seats(tmp_path, monkeypatch):
    """不在 session.list → live=false、running=false、title/task 空、
    idle_s=null；fleet status 已 inactive/released 的退役条目照常列出。"""
    import copy

    seed = copy.deepcopy(FLEET_SEED)
    seed["fleet"]["dd04"] = {
        "sessionId": "session-<redacted>",
        "role": "worker", "node": "vh-old", "status": "released",
        "spawnedAt": "2026-08-20T09:00:00.000000+00:00",
    }
    _seed_fleet(tmp_path, monkeypatch, seed)
    _mock_session_list(monkeypatch, [])  # dsh 无任何活会话
    payload = await rt_gateway._fleet_brief_payload()
    seats = {s["id"]: s for s in payload["seats"]}
    assert set(seats) == {"aa01", "bb02", "cc03", "dd04"}
    for code, entry in seed["fleet"].items():
        assert seats[code] == {
            "id": code, "node": entry["node"], "role": entry["role"],
            "status": entry["status"], "live": False, "running": False,
            "title": "", "task": "", "idle_s": None,
        }


@pytest.mark.asyncio
async def test_fleet_brief_payload_degrades_without_dsh(tmp_path, monkeypatch):
    """loopback 失败 → 降级 {seats:[纯席位字段], note}（席位键集恰四）。"""
    _seed_fleet(tmp_path, monkeypatch)

    async def _boom(method, payload):
        raise OSError("connection refused")

    monkeypatch.setattr(rt_gateway, "_dsh_api", _boom)
    payload = await rt_gateway._fleet_brief_payload()
    assert set(payload) == {"seats", "note"}
    assert payload["note"] == "dsh 状态不可达，仅席位表"
    assert payload["seats"] == [
        {"id": "aa01", "node": "node-<redacted>", "role": "worker",
         "status": "active"},
        {"id": "bb02", "node": "vh-head-liaison", "role": "worker",
         "status": "active"},
        {"id": "cc03", "node": "vh-m5-closeout-supervisor-000501",
         "role": "supervisor", "status": "active"},
    ]


@pytest.mark.asyncio
async def test_fleet_brief_payload_degrades_on_timeout(tmp_path, monkeypatch):
    """session.list 挂起 → wait_for 超时同走降级（钳短超时注入）。"""
    _seed_fleet(tmp_path, monkeypatch)
    monkeypatch.setattr(rt_gateway, "BRIEF_DSH_TIMEOUT_S", 0.05)

    async def _hang(method, payload):
        await asyncio.Event().wait()

    monkeypatch.setattr(rt_gateway, "_dsh_api", _hang)
    payload = await rt_gateway._fleet_brief_payload()
    assert payload["note"] == "dsh 状态不可达，仅席位表"
    assert {s["id"] for s in payload["seats"]} == {"aa01", "bb02", "cc03"}


@pytest.mark.asyncio
async def test_fleet_brief_payload_empty_fleet_table(tmp_path, monkeypatch):
    """空席位表 → {"seats": []}（loopback 照调，join 不短路）。"""
    _seed_fleet(tmp_path, monkeypatch, {"port": 3080, "fleet": {}})
    calls = _mock_session_list(monkeypatch, [])
    assert await rt_gateway._fleet_brief_payload() == {"seats": []}
    assert calls == [("session.list", {})]


@pytest.mark.asyncio
async def test_fleet_brief_payload_unreadable_fleet_raises(tmp_path, monkeypatch):
    """fleet.json 缺席/坏 JSON/无 fleet 表 → 抛（调用方回 internal 错误帧）。"""
    _mock_session_list(monkeypatch, [])
    monkeypatch.setenv("MAESTRO_FLEET", str(tmp_path / "nope" / "fleet.json"))
    with pytest.raises(OSError):
        await rt_gateway._fleet_brief_payload()
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    monkeypatch.setenv("MAESTRO_FLEET", str(bad))
    with pytest.raises(ValueError):
        await rt_gateway._fleet_brief_payload()
    notable = tmp_path / "notable.json"
    notable.write_text(json.dumps({"port": 3080}), encoding="utf-8")
    monkeypatch.setenv("MAESTRO_FLEET", str(notable))
    with pytest.raises(ValueError):
        await rt_gateway._fleet_brief_payload()


@pytest.mark.asyncio
async def test_fleet_brief_control_frame_roundtrip(tmp_path, monkeypatch):
    """fleet.brief 全链路（observe 连接）：回包键集 {t,req_id,seats}、
    req_id 回显、席位字段原样；在跑/死会话同帧可见。"""
    _seed_fleet(tmp_path, monkeypatch)
    item = _session_item("session-<redacted>",
                         running=True, title="db05 在跑封装统一client",
                         long_task={"task": {"phase": "executing"}})
    _mock_session_list(monkeypatch, [item])
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await observe_handshake(ws)  # 席位控制走 observe 连接（同 cleanup）
        got = await _fleet_brief(ws, req_id="brief-1")
        assert set(got) == {"t", "req_id", "seats"}
        assert got["req_id"] == "brief-1"
        seats = {s["id"]: s for s in got["seats"]}
        assert seats["aa01"]["live"] is True and seats["aa01"]["running"] is True
        assert seats["aa01"]["title"] == "db05 在跑封装统一client"
        assert seats["aa01"]["task"] == "executing"
        assert seats["bb02"]["live"] is False and seats["bb02"]["idle_s"] is None
        await close_ws(ws)


@pytest.mark.asyncio
async def test_fleet_brief_degraded_note_over_ws(tmp_path, monkeypatch):
    """loopback 不可达 → 回包带 note、席位纯四字段（降级形态上链路）。"""
    _seed_fleet(tmp_path, monkeypatch)

    async def _boom(method, payload):
        raise RuntimeError("session.list: down")

    monkeypatch.setattr(rt_gateway, "_dsh_api", _boom)
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await observe_handshake(ws)
        got = await _fleet_brief(ws, req_id="brief-2")
        assert set(got) == {"t", "req_id", "seats", "note"}
        assert got["note"] == "dsh 状态不可达，仅席位表"
        assert set(got["seats"][0]) == {"id", "node", "role", "status"}
        await close_ws(ws)


@pytest.mark.asyncio
async def test_fleet_brief_unreadable_fleet_internal_error_frame(tmp_path, monkeypatch):
    """fleet.json 读不了 → error internal 帧（非静默断流）。"""
    monkeypatch.setenv("MAESTRO_FLEET", str(tmp_path / "nope" / "fleet.json"))
    _mock_session_list(monkeypatch, [])
    async with GatewayFixture() as fx:
        ws = await fx.ws()
        await handshake(ws)
        await ws.send_str(json.dumps({"t": "fleet.brief", "req_id": "x"}))
        err = await recv_json(ws)
        assert err["t"] == "error" and err["code"] == "internal"
        await close_ws(ws)


@pytest.mark.asyncio
async def test_realtime_head_wires_fleet_brief_resource(tmp_path, monkeypatch):
    """build_realtime_head 的 app_resources 携带 fleet_brief callable
    （=模块级 _fleet_brief_payload，与 workspace_root 同形注入）。"""
    import types

    wired: dict = {}
    _fake_head_build_module(monkeypatch, wired)
    monkeypatch.setenv("VOICE_HEAD_PROFILES", str(tmp_path / "no-heads.json"))
    rt_gateway._reset_head_registry()

    async def _noop(*a, **k):
        pass

    session = types.SimpleNamespace(conv_id="s-brief", send_audio=_noop,
                                    drop_pending_audio=_noop)
    backend = types.SimpleNamespace(liaison_session="")
    adapter = await rt_gateway.build_realtime_head(session, EventBus(), backend)
    await adapter.stop()
    rt_gateway._reset_head_registry()

    res = wired["app_resources"]
    assert callable(res["fleet_brief"])
    assert res["fleet_brief"] is rt_gateway._fleet_brief_payload
