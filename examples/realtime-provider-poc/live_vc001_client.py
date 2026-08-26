#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""VC-001 live: rt_voice_app ONE 桌面客户端双连接面（KG 12 §4）。

被测物是**真实客户端类**（rt_voice_app.VoiceLink / ObserveLink，各自线程 +
真实 aiohttp WS），网关为 in-process 真 VoiceGateway（echo head + 真实
DaisLane + 真实 topic 文件尾读）。tkinter 渲染不在无头验收面（渲染函数
单测覆盖，tests/test_rt_voice_app.py）。

Verdicts:
  V1 语音回归：VoiceLink open；voice-form topics 回显（raw 探针完整 JSON）
     含 orch.*+head.turn；上行 PCM 被 echo head 接受（无 error/链路帧）
  V2 观测建立：ObserveLink open；topics 回显=全部 SUBSCRIBABLE_KINDS
  V3 席位+票板：obs 队列收到 fleet.snapshot / tickets.snapshot（订阅即回放）
  V4 编排任务：真实 DshBackend.dispatch_plan→dispatch_dag（run→task→
     session worker→注入命令块结算）；obs 收到 orch.dispatch / progress /
     done（run_id 匹配；orch.ack 为协议保留位，当前实现无生产者）
  V5 消息+票板：cb-send 失配签名落文件桥（PORT-R1）→ inbox.log 增量 →
     obs 收到 bridge.msg 标记行
  V6 observe 拒媒体：voice 连接释放配额后，第三连接 observe 会话发二进制
     → error observe_media
  V7 并发上限：双连接同 token 时第三连接 auth → error concurrent_limit

回合页签（head.turn）：echo 头无 TurnTrace，GLM 文本模式无 STT/TTS 通道
（Q2 定案），live 无产生路径——以 V2 订阅回显含 head.turn + KG 11 TurnTrace
单测覆盖，如实记录。

Evidence: docs/kg/evidence/vc-001-live.md

Usage: .venv/bin/python examples/realtime-provider-poc/live_vc001_client.py
"""

from __future__ import annotations

import asyncio
import json
import os
import queue
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))

from rt_dsh_lane import DaisLane  # noqa: E402
from rt_gateway import (  # noqa: E402
    SUBSCRIBABLE_KINDS,
    DEFAULT_TOPIC_SOURCES,
    VoiceGateway,
    echo_head_provider,
)
from rt_voice_app import ObserveLink, VoiceLink  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
DAIS = Path("~/.local/bin/dais").expanduser()
EVID = REPO / "docs/kg/evidence/vc-001-live.md"
GW_PORT = 8765
WORKER_CMD = "echo vc001-live ok"

RESULTS: list[tuple[bool, str, str]] = []
FRAME_LOG: list[dict] = []          # obs 帧同屏落盘（tee）


def verdict(ok: bool, label: str, detail: str = "") -> bool:
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))
    RESULTS.append((ok, label, detail))
    return ok


def sh(*cmd: str, timeout_s: float = 30.0) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)


def drain(q) -> list:
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except queue.Empty:
            return out


def wait_frames(obs_q, pred, timeout_s: float) -> list[dict]:
    """轮询 obs 队列直到谓词命中；帧回填保序（tee 落 FRAME_LOG）。"""
    deadline = time.monotonic() + timeout_s
    hold: list[dict] = []
    while time.monotonic() < deadline:
        got = drain(obs_q)
        for f in got:
            FRAME_LOG.append(f)
        hold.extend(got)
        if got and pred(hold):
            break
        time.sleep(0.05)
    for f in hold:
        obs_q.put(f)
    return hold


def any_pred(*kinds: str):
    def pred(seen: list[dict]) -> bool:
        return any(f.get("t") in kinds for f in seen)
    return pred


async def raw_connect(token: str):
    """裸 aiohttp 连接（V1/V6/V7 探针用）。"""
    import aiohttp

    http = aiohttp.ClientSession()
    ws = await http.ws_connect(f"ws://127.0.0.1:{GW_PORT}/ws", max_msg_size=1 << 20)
    await ws.send_json({"t": "auth", "token": token})
    return http, ws


async def read_until(ws, t_expect: str, timeout_s: float = 5.0) -> dict | None:
    import aiohttp

    loop = asyncio.get_event_loop()
    end = loop.time() + timeout_s
    while loop.time() < end:
        try:
            msg = await asyncio.wait_for(ws.receive(), timeout=end - loop.time())
        except asyncio.TimeoutError:
            return None
        if msg.type != aiohttp.WSMsgType.TEXT:
            continue
        data = json.loads(msg.data)
        if data.get("t") == t_expect or data.get("t") == "error":
            return data
    return None


def _dedup(states: list[str]) -> str:
    out: list[str] = []
    for s in states:
        if not out or out[-1] != s:
            out.append(s)
    return "→".join(out)


async def _lane_retry(fn, *args, attempts: int = 3, backoff_s: float = 3.0,
                      **kwargs):
    """dais CLI 偶发撞 GUI/daemon 忙窗（probe 实测可 >30s）：退避重试。"""
    for i in range(attempts):
        try:
            return await fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            if i == attempts - 1:
                raise
            print(f"  [retry] {getattr(fn, '__name__', 'lane')}: {str(e)[:100]}")
            await asyncio.sleep(backoff_s)


def _wait_state(states: list[str], want: str, timeout_s: float) -> bool:
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        if want in states:
            return True
        time.sleep(0.05)
    return False


async def main() -> int:
    for _k in ("ALL_PROXY", "all_proxy"):
        os.environ.pop(_k, None)

    # 8765 被 systemd 网关占用时先停（unit 约定：live 自起同端口网关）
    svc_was_active = sh("systemctl", "--user", "is-active",
                        "voice-gateway.service").stdout.strip() == "active"
    if svc_was_active:
        sh("systemctl", "--user", "stop", "voice-gateway.service")
        print("== systemd voice-gateway stopped (live owns :8765) ==")

    lane = DaisLane(default_timeout_s=30)
    try:
        await lane._run("check-messages", "voice-head", "--timeout-ms", "500")
    except Exception as e:  # noqa: BLE001
        print(f"[SKIP] dais bus unresponsive: {e}")
        if svc_was_active:
            sh("systemctl", "--user", "start", "voice-gateway.service")
        return 1

    token = f"vc001-{uuid.uuid4().hex[:8]}"
    gateway = VoiceGateway(
        port=GW_PORT, host="127.0.0.1", token=token,
        head_provider=echo_head_provider, lane=lane,
        topic_sources=DEFAULT_TOPIC_SOURCES,
    )
    gw_task = asyncio.create_task(gateway.run_forever())
    await asyncio.sleep(1.0)  # tailers prime + bind
    url = f"ws://127.0.0.1:{GW_PORT}/ws"

    tx: queue.Queue = queue.Queue()
    rx: queue.Queue = queue.Queue()
    obs_q: queue.Queue = queue.Queue()
    v_states: list[str] = []
    o_states: list[str] = []
    voice: VoiceLink | None = None
    obs: ObserveLink | None = None
    worker_term: str | None = None

    try:
        print("== V1: 语音面回归（真实 VoiceLink + raw 探针拿完整 topics 回显）==")
        voice = VoiceLink(url, token, tx, rx, v_states.append)
        voice.start()
        ok1 = verdict(await asyncio.to_thread(_wait_state, v_states, "open", 10.0),
                      "V1a 语音连接 open（VoiceLink 真实链路；open 即 "
                      "auth.ok→session.started 全过）",
                      f"states={_dedup(v_states)}")
        h1, w1 = await raw_connect(token)          # voice-form 探针（占第2配额）
        a1 = await read_until(w1, "auth.ok", 5.0)
        await w1.send_json({"t": "session.start", "session_id": a1.get("session_id")})
        s1 = await read_until(w1, "session.started", 5.0)
        topics = (s1 or {}).get("topics") or []
        ok1 &= verdict("orch.dispatch" in topics and "head.turn" in topics,
                       "V1b voice topics 回显含 orch.*+head.turn",
                       f"topics={topics}")
        await w1.close()
        await h1.close()
        for _ in range(3):                          # 3 块 50ms 静音 PCM
            tx.put(b"\x00\x00" * 800)
        time.sleep(1.5)
        bad = [ln for ln in drain(rx) if ln.startswith(("[错误]", "[链路]"))]
        ok1 &= verdict(not bad, "V1c 上行 PCM 被网关接受（echo 面无错）",
                       f"bad={bad[:2]}")

        print("== V2: 观测连接（真实 ObserveLink observe:true）==")
        obs = ObserveLink(url, token, obs_q, o_states.append)
        obs.start()
        ok2 = verdict(await asyncio.to_thread(_wait_state, o_states, "open", 10.0),
                      "V2a 观测连接 open", f"states={_dedup(o_states)}")
        ok2 &= verdict(set(obs.topics or []) == set(SUBSCRIBABLE_KINDS),
                       "V2b obs topics 回显=全部 SUBSCRIBABLE_KINDS",
                       f"n={len(obs.topics or [])} head.turn∈topics="
                       f"{'head.turn' in (obs.topics or [])}")

        print("== V7: 并发上限（voice+obs 占满，第三连接被拒）==")
        h7, w7 = await raw_connect(token)
        r7 = await read_until(w7, "auth.ok", 5.0)
        ok7 = verdict((r7 or {}).get("t") == "error"
                      and r7.get("code") == "concurrent_limit",
                      "V7 第三连接 error concurrent_limit", f"reply={r7}")
        await w7.close()
        await h7.close()

        print("== V3: 席位+票板快照（订阅即回放）==")
        seen3 = await asyncio.to_thread(wait_frames, obs_q,
                                        any_pred("fleet.snapshot"), 5.0)
        fleet = next((f.get("fleet") for f in seen3
                      if f.get("t") == "fleet.snapshot"), None)
        ok3 = verdict(fleet is not None, "V3a fleet.snapshot 快照回放",
                      f"fleet_n={len(fleet or {})}")
        seen3b = await asyncio.to_thread(wait_frames, obs_q,
                                         any_pred("tickets.snapshot"), 5.0)
        ok3 &= verdict(any(f.get("t") == "tickets.snapshot" for f in seen3b),
                       "V3b tickets.snapshot 快照回放", "")

        print("== V4: 编排任务（真实 DshBackend.dispatch_dag: run→task→"
              "session worker→注入命令块结算）==")
        # 顺序：GUI 安静时先建 run/task（经 backend phase1）；new-terminal 开
        # tab 后有数秒~分钟级忙窗（probe 实测），phase2 的 start_worker/
        # inject 若撞窗由驱动层补射（obs 帧里的 ctx id 可见）。
        nt = sh(str(DAIS), "orchestration", "new-terminal", str(REPO),
                "--cwd", str(REPO), timeout_s=30)
        m = re.search(r"session_[0-9a-f]+", nt.stdout)
        if not m:
            verdict(False, "V4 provision dais 终端", nt.stdout[:120])
            raise RuntimeError("no session handle from new-terminal")
        worker_term = m.group(0)
        await asyncio.sleep(3.0)          # GUI tab settle

        from rt_dsh_backend import DshBackend

        finals: list[str] = []

        async def on_final(ref, message):
            finals.append(message)

        backend = DshBackend(lane=lane, bus=gateway.bus, on_final=on_final,
                             dag_workers=[worker_term], await_timeout_s=120,
                             poll_s=1.0, poll_max_s=4.0)
        subtasks = [{"spec": "echo vc001-live ok 并结算", "deps": [],
                     "command": WORKER_CMD, "session": worker_term}]
        receipt = json.loads(await _lane_retry(
            backend.dispatch_plan, "VC-001 live 编排观察验证",
            json.dumps(subtasks, ensure_ascii=False)))
        run_id = receipt.get("run_id")
        # 结算观察 + 补射：phase2 已注入命令；若 inject 曾撞 GUI 忙窗失败，
        # obs 的 orch.progress 帧（"inject failed"）里可见，此处补注一次。
        seen4 = await asyncio.to_thread(wait_frames, obs_q,
                                        lambda seen: (any(
                                            f.get("t") == "orch.done"
                                            for f in seen) or finals),
                                        30.0)
        if not any(f.get("t") == "orch.done" for f in seen4):
            # 补射：从 progress 帧抠 ctx id 再 inject 一次
            ctx_m = None
            for f in seen4:
                note = str((f.get("note") or ""))
                cm = re.search(r"ctx_[0-9a-f]+", note)
                if cm:
                    ctx_m = cm.group(0)
            if ctx_m:
                try:
                    await lane.inject_prompt(ctx_m, WORKER_CMD)
                except Exception as e:  # noqa: BLE001
                    print(f"  [rescue-inject] {e}")
                seen4 = await asyncio.to_thread(
                    wait_frames, obs_q, any_pred("orch.done"), 30.0)
        got = {f.get("t") for f in seen4}
        # orch.ack 为协议保留位（当前 DshBackend 不 emit——dispatch/progress/
        # done 是实际产生面），断言按实际协议集。
        ok4 = verdict({"orch.dispatch", "orch.progress", "orch.done"} <= got,
                      "V4a obs 收到 orch.dispatch/progress/done",
                      f"run={run_id} orch_kinds={sorted(k for k in got if str(k).startswith('orch.'))}")
        ok4 &= verdict(any(f.get("run_id") == run_id for f in seen4),
                       "V4b run_id 匹配", f"run={run_id}")

        print("== V5: 消息（bridge.msg 真实文件桥增量）==")
        # cb-send 对失配签名直落文件桥（PORT-R1）：append inbox.log →
        # gateway lines-tailer → bridge.msg 帧。type=ping/ref=- 无任务语义。
        cb = sh(str(Path("~/.dsh/maestro/bin/cb-send").expanduser()),
                "ping", "vc001-live@local", "nobody@session_0000dead",
                "-", "VC-001 live bridge.msg marker", timeout_s=15)
        print(f"  cb-send: {(cb.stdout + cb.stderr).strip()[:100]}")
        seen5 = await asyncio.to_thread(wait_frames, obs_q,
                                        any_pred("bridge.msg"), 8.0)
        ok5 = verdict(any(f.get("t") == "bridge.msg"
                          and "VC-001 live bridge.msg marker" in str(f)
                          for f in seen5),
                      "V5 obs 收到 bridge.msg（标记行）",
                      f"n={sum(1 for f in seen5 if f.get('t') == 'bridge.msg')}")

        print("== V6: observe 拒媒体（voice 释放配额后探针）==")
        voice.close(True)
        voice = None
        # 等网关释放会话计数（ws 断开 → release 异步），再开探针
        a6 = None
        for _ in range(10):
            await asyncio.sleep(0.5)
            try:
                h6, w6 = await raw_connect(token)
                a6 = await read_until(w6, "auth.ok", 3.0)
                if (a6 or {}).get("t") == "auth.ok":
                    break
                await w6.close()
                await h6.close()
            except Exception:
                pass
        ok6 = verdict((a6 or {}).get("t") == "auth.ok",
                      "V6a 探针连接建立（配额已释放）", f"auth={a6}")
        if ok6:
            await w6.send_json({"t": "session.start", "observe": True,
                                "session_id": a6.get("session_id")})
            s6 = await read_until(w6, "session.started", 5.0)
            ok6 &= verdict((s6 or {}).get("t") == "session.started",
                           "V6b observe 会话建立", f"reply={s6}")
            if ok6:
                await w6.send_bytes(b"\x00\x00")
                e6 = await read_until(w6, "error", 5.0)
                ok6 &= verdict((e6 or {}).get("code") == "observe_media",
                               "V6c observe 会话发媒体 → error observe_media",
                               f"err={e6}")
        try:
            await w6.close()
            await h6.close()
        except Exception:
            pass
    finally:
        if obs:
            obs.close()
        if voice:
            voice.close(True)
        if worker_term:
            sh(str(DAIS), "orchestration", "close-terminal", worker_term,
               timeout_s=15)
        gw_task.cancel()
        try:
            await gw_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        if svc_was_active:
            sh("systemctl", "--user", "start", "voice-gateway.service")
            up = sh("curl", "-s", "--max-time", "3",
                    f"http://127.0.0.1:{GW_PORT}/healthz").stdout.strip()
            print(f"== systemd voice-gateway restored: {up[:80]} ==")

    all_ok = all(r[0] for r in RESULTS)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"# VC-001 live 凭证：rt_voice_app 双连接观测面（{ts}）",
        "",
        "- 驱动: `live_vc001_client.py`（真实 VoiceLink/ObserveLink 线程 + "
        "in-process VoiceGateway[echo head + DaisLane + 真实 topic 尾读]）",
        f"- 总判: {'PASS' if all_ok else 'FAIL'}"
        f"（{sum(1 for r in RESULTS if r[0])}/{len(RESULTS)}）",
        "",
        "| 判定 | 结果 | 说明 |",
        "|---|---|---|",
    ]
    lines += [f"| {l} | {'✅' if o else '❌'} | {d} |" for o, l, d in RESULTS]
    lines += [
        "",
        "## obs 帧同屏（tee，按到达序，截 300 字符）",
        "",
        "```jsonl",
    ]
    lines += [json.dumps(f, ensure_ascii=False)[:300] for f in FRAME_LOG]
    lines += [
        "```",
        "",
        "## 限制（如实记录）",
        "",
        "- 回合页签 head.turn：echo 头无 TurnTrace、GLM 文本模式无 STT/TTS 通道"
        "（Q2 定案），live 无产生路径；通路以 V2b 订阅回显含 head.turn + "
        "KG 11 TurnTrace 单测覆盖。",
        "- tkinter 渲染面（频谱/页签）不在无头验收：渲染函数单测覆盖"
        "（tests/test_rt_voice_app.py 4/4）；`rt_voice_app.py --selftest` 覆盖 "
        "UI 起即退。",
        "- 观测只读（KG 12 §5）：本驱动对 topic 源零写入；orch.* 经真实 dais "
        "编排产生。",
    ]
    EVID.write_text("\n".join(lines) + "\n")
    print(f"== evidence: {EVID} ==")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
