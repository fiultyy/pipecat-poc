#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Console P1 topic 数据平面测试（KG 11）。

覆盖：TurnTrace phase 映射（帧类注入）、FileTailer 行尾/快照两模式、
observe 会话握手（topics 回显/无 pipeline/拒媒体）、voice 会话默认订阅含
head.turn、topics 校验、快照缓存订阅回放。
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples" / "realtime-provider-poc"))

from rt_gateway import (  # noqa: E402
    DEFAULT_VOICE_KINDS,
    FileTailer,
    TurnTrace,
)

# ---- TurnTrace（纯逻辑，fake 帧类）----


class _TextFrame:
    def __init__(self, text):
        self.text = text


class _TranscriptionFrame(_TextFrame):
    pass


class _InterimFrame(_TextFrame):
    pass


class _F:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _ft():
    class _Base:
        pass

    class UserStart(_Base):
        pass

    class UserEnd(_Base):
        pass

    class AssistantStart(_Base):
        pass

    class AssistantEnd(_Base):
        pass

    class ToolCall(_Base):
        pass

    class Interrupted(_Base):
        pass

    return {
        "user_start": UserStart,
        "user_end": UserEnd,
        "user_text": _TranscriptionFrame,
        "interim": _InterimFrame,
        "assistant_start": AssistantStart,
        "assistant_end": AssistantEnd,
        "text": _TextFrame,
        "tool_call": ToolCall,
        "interrupted": Interrupted,
    }


def test_turn_trace_phase_table():
    ft = _ft()
    tr = TurnTrace(ft)
    assert tr.on_frame(ft["user_start"]()) == {"phase": "user_start"}
    assert tr.on_frame(ft["user_end"]()) == {"phase": "user_end"}
    assert tr.on_frame(ft["user_text"]("修正权威文本")) == {
        "phase": "user_text",
        "detail": "修正权威文本",
    }
    assert tr.on_frame(_InterimFrame("中间")) is None  # interim 不上报
    assert tr.on_frame(ft["assistant_start"]()) == {"phase": "assistant_start"}
    assert tr.on_frame(_F(text="x")) is None  # 未知帧类


def test_turn_trace_assistant_text_accumulates_to_end_detail():
    ft = _ft()
    tr = TurnTrace(ft)
    assert tr.on_frame(ft["assistant_start"]()) is not None
    assert tr.on_frame(_TextFrame("你好，")) is None
    assert tr.on_frame(_TextFrame("编排已派出。")) is None
    ev = tr.on_frame(ft["assistant_end"]())
    assert ev == {"phase": "assistant_end", "detail": "你好，编排已派出。"}
    # 缓冲清空：下一轮 assistant_end 无 detail
    tr.on_frame(ft["assistant_start"]())
    assert tr.on_frame(ft["assistant_end"]()) == {"phase": "assistant_end"}


def test_turn_trace_detail_truncated_200_and_transcription_ordering():
    ft = _ft()
    tr = TurnTrace(ft)
    long = "字" * 300
    assert tr.on_frame(ft["user_text"](long))["detail"] == "字" * 200
    # Transcription 是 TextFrame 子类：必须走 user_text 而非混入 assistant 缓冲
    tr.on_frame(ft["assistant_start"]())
    assert tr.on_frame(_TranscriptionFrame("用户说")) == {"phase": "user_text", "detail": "用户说"}
    assert tr.on_frame(ft["assistant_end"]()) == {"phase": "assistant_end"}


def test_turn_trace_tool_call_and_interrupted():
    ft = _ft()
    tr = TurnTrace(ft)
    call = ft["tool_call"]()
    call.function_name = "dsh_dispatch"
    assert tr.on_frame(call) == {"phase": "tool_call", "detail": "dsh_dispatch"}
    assert tr.on_frame(ft["interrupted"]()) == {"phase": "interrupted"}


# ---- FileTailer ----


async def _run_tailer(tailer: FileTailer, rounds: int = 1) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []

    async def emit(kind, payload):
        out.append((kind, dict(payload)))

    tailer.emit = emit
    stop = asyncio.Event()

    async def stopper():
        await asyncio.sleep(rounds * tailer.poll_s + 0.3)
        stop.set()

    await asyncio.gather(tailer.follow(stop), stopper())
    return out


@pytest.mark.asyncio
async def test_file_tailer_lines_incremental_offsets(tmp_path):
    f = tmp_path / "inbox.log"
    f.write_bytes(b"line-one\nline-two\n")
    # backlog=0 起播于 EOF：只看启动后新增行（尾读语义）；回看用大 backlog
    t = FileTailer("bridge.msg", str(f), None, mode="lines", poll_s=0.05, backlog=1024)
    out = await _run_tailer(t, rounds=3)
    kinds = [k for k, _ in out]
    assert kinds and set(kinds) == {"bridge.msg"}
    lines = [p["line"] for _, p in out]
    assert lines[:2] == ["line-one", "line-two"]
    # offset = 行首字节位（"line-one\n"=9B → 第二行 offset=9）
    by_line = {p["line"]: p["offset"] for _, p in out}
    assert by_line["line-one"] == 0 and by_line["line-two"] == 9


@pytest.mark.asyncio
async def test_file_tailer_lines_zero_backlog_starts_at_eof(tmp_path):
    f = tmp_path / "inbox.log"
    f.write_bytes(b"old-stuff\n")
    t = FileTailer("bridge.msg", str(f), None, mode="lines", poll_s=0.05)
    async def emit(kind, payload):  # noqa: E306
        emit.out.append((kind, dict(payload)))

    emit.out = []
    t.emit = emit
    stop = asyncio.Event()

    async def runner():
        task = asyncio.create_task(t.follow(stop))
        await asyncio.sleep(0.15)  # 两轮空转：存量不投
        assert emit.out == []
        with open(f, "a") as fh:
            fh.write("new-line\n")
        await asyncio.sleep(0.15)  # 新增行投递
        stop.set()
        await task

    await runner()
    assert [p["line"] for _, p in emit.out] == ["new-line"]


@pytest.mark.asyncio
async def test_file_tailer_lines_backlog_and_truncation(tmp_path):
    f = tmp_path / "log"
    f.write_bytes(b"x" * 100 + b"\nold\n")  # 105B；backlog=16 → 从 89 起（首行半行）
    t = FileTailer("bridge.msg", str(f), None, mode="lines", poll_s=0.05, backlog=16)
    out = await _run_tailer(t, rounds=1)
    lines = [p["line"] for _, p in out]
    assert lines[-1] == "old"  # 回看窗内完整行
    # 截断：文件变小 → cursor 归零重读新内容
    f.write_bytes(b"fresh\n")
    out2 = await _run_tailer(t, rounds=1)
    assert [p["line"] for _, p in out2] == ["fresh"]


@pytest.mark.asyncio
async def test_file_tailer_lines_incomplete_tail_waits(tmp_path):
    f = tmp_path / "log"
    f.write_bytes(b"done\nhalf-writt")  # 残尾无换行 → 等下一轮
    t = FileTailer("bridge.msg", str(f), None, mode="lines", poll_s=0.05, backlog=100)
    out = await _run_tailer(t, rounds=2)
    assert [p["line"] for _, p in out] == ["done"]


@pytest.mark.asyncio
async def test_file_tailer_snapshot_json_change_only(tmp_path):
    f = tmp_path / "fleet.json"
    f.write_text(json.dumps({"head": "20d0"}))
    cache: dict = {}
    t = FileTailer(
        "fleet.snapshot", str(f), None, mode="snapshot", parse="json", poll_s=0.05, cache=cache
    )
    out = await _run_tailer(t, rounds=2)
    assert len(out) == 1  # 未变更不重发
    assert out[0][1] == {"fleet": {"head": "20d0"}}
    assert cache["fleet.snapshot"] == {"fleet": {"head": "20d0"}}
    # 变更 → 新快照 + 缓存更新
    f.write_text(json.dumps({"head": "9b95"}))
    out2 = await _run_tailer(t, rounds=2)
    assert len(out2) == 1 and out2[0][1] == {"fleet": {"head": "9b95"}}
    assert cache["fleet.snapshot"] == {"fleet": {"head": "9b95"}}


@pytest.mark.asyncio
async def test_file_tailer_snapshot_bad_json_no_emit(tmp_path):
    f = tmp_path / "fleet.json"
    f.write_text("{broken")
    t = FileTailer("fleet.snapshot", str(f), None, mode="snapshot", parse="json", poll_s=0.05)
    out = await _run_tailer(t, rounds=2)
    assert out == []  # 解析失败：不投不发、签名不更新（下轮重试）


@pytest.mark.asyncio
async def test_file_tailer_snapshot_text_mode(tmp_path):
    f = tmp_path / "tickets.md"
    f.write_text("# 票面\nOF-001 ☑\n")
    t = FileTailer("tickets.snapshot", str(f), None, mode="snapshot", parse="text", poll_s=0.05)
    out = await _run_tailer(t, rounds=1)
    assert out[0][1]["text"] == "# 票面\nOF-001 ☑\n"
    assert out[0][1]["lines"] == 2


# ---- observe 会话 / 订阅协议（真网关 + 真 WS）----

TOKEN = "t-topics"


def _fixture_kwargs(tmp_path, **over):
    src = {
        "fleet.snapshot": {
            "path": str(tmp_path / "fleet.json"),
            "mode": "snapshot",
            "parse": "json",
            "poll_s": 0.05,
        },
        "bridge.msg": {
            "path": str(tmp_path / "inbox.log"),
            "mode": "lines",
            "poll_s": 0.05,
            "backlog": 64,
        },
    }
    kw = {"token": TOKEN, "topic_sources": src}
    kw.update(over)
    return kw


async def _handshake(ws, token=TOKEN, start_extra=None):
    from tests.test_rt_gateway import recv_json  # noqa: PLC0415 — 复用收帧器

    await ws.send_str(json.dumps({"t": "auth", "token": token}))
    auth = await recv_json(ws)
    assert auth["t"] == "auth.ok"
    await ws.send_str(json.dumps({"t": "session.start", **(start_extra or {})}))
    return await recv_json(ws)


@pytest.mark.asyncio
async def test_observe_session_subscribes_without_pipeline(tmp_path):
    from tests.test_rt_gateway import FakeHead, GatewayFixture, close_ws, make_provider, recv_json

    (tmp_path / "fleet.json").write_text(json.dumps({"code": "20d0"}))
    (tmp_path / "inbox.log").write_text("early-msg\n")
    heads: list = []
    fx_kw = _fixture_kwargs(tmp_path)
    fx_kw["head_provider"] = make_provider(FakeHead, heads)
    async with GatewayFixture(**fx_kw) as fx:
        ws = await fx.ws()
        started = await _handshake(ws, start_extra={"observe": True})
        assert started["t"] == "session.started"
        assert started["observe"] is True
        assert "head.turn" in started["topics"] and "orch.dispatch" in started["topics"]
        assert heads == []  # observe 不建 head pipeline
        # 快照缓存回放：连接即见 fleet 最新快照（无需等变更）
        got = await recv_json(ws, timeout=3.0)
        while got.get("t") != "fleet.snapshot":
            got = await recv_json(ws, timeout=3.0)
        assert got["fleet"] == {"code": "20d0"}
        await close_ws(ws)


@pytest.mark.asyncio
async def test_observe_session_rejects_media(tmp_path):
    from tests.test_rt_gateway import GatewayFixture, close_ws, recv_json, recv_until

    async with GatewayFixture(**_fixture_kwargs(tmp_path)) as fx:
        ws = await fx.ws()
        started = await _handshake(ws, start_extra={"observe": True})
        assert started["observe"] is True
        await ws.send_bytes(b"\x00" * 640)
        err = await recv_until(ws, lambda d: d.get("t") == "error", timeout=3.0)
        assert err["code"] == "observe_media"
        await close_ws(ws)


@pytest.mark.asyncio
async def test_topics_validation_rejects_unknown_kind(tmp_path):
    from tests.test_rt_gateway import GatewayFixture, close_ws, recv_until

    async with GatewayFixture(**_fixture_kwargs(tmp_path)) as fx:
        ws = await fx.ws()
        got = await _handshake(ws, start_extra={"topics": ["no.such"]})
        # 校验失败 → error 帧（bad_request），会话未开
        if got.get("t") != "error":
            got = await recv_until(ws, lambda d: d.get("t") == "error", timeout=3.0)
        assert got["code"] == "bad_request" and "no.such" in got["msg"]
        await close_ws(ws)


@pytest.mark.asyncio
async def test_voice_session_default_subscription_includes_head_turn(tmp_path):
    from tests.test_rt_gateway import GatewayFixture, close_ws, recv_until

    assert "head.turn" in DEFAULT_VOICE_KINDS
    async with GatewayFixture(**_fixture_kwargs(tmp_path)) as fx:
        ws = await fx.ws()
        started = await _handshake(ws)  # 普通语音会话
        assert started["observe"] is False
        assert "head.turn" in started["topics"] and "orch.progress" in started["topics"]
        await fx.gw.bus.emit("head.turn", {"conv_id": "s-x", "phase": "user_start"})
        ev = await recv_until(ws, lambda d: d.get("t") == "head.turn", timeout=3.0)
        assert ev["phase"] == "user_start" and ev["conv_id"] == "s-x"
        await close_ws(ws)


@pytest.mark.asyncio
async def test_observe_session_receives_live_bridge_lines(tmp_path):
    from tests.test_rt_gateway import GatewayFixture, close_ws, recv_until

    log = tmp_path / "inbox.log"
    log.write_text("history-1\n")
    async with GatewayFixture(**_fixture_kwargs(tmp_path)) as fx:
        ws = await fx.ws()
        await _handshake(ws, start_extra={"topics": ["bridge.msg"]})
        # 排空缓存/回放后追加新行 → 尾读投递
        await asyncio.sleep(0.2)
        with open(log, "a") as fh:
            fh.write("DSH-RE] {\"type\":\"report\"}\n")
        ev = await recv_until(ws, lambda d: d.get("t") == "bridge.msg", timeout=5.0)
        assert "DSH-RE]" in ev["line"]
        await close_ws(ws)
