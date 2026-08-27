#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for DshBackend two-phase contract (mocked lanes + bus)."""

import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_dsh_backend import DshBackend  # noqa: E402
from rt_dsh_lane import DaisLane, DaisLaneError  # noqa: E402
from rt_event_bus import EventBus  # noqa: E402


def make_lane(script: dict):
    """DaisLane mock: script maps subcommand → list of stdout replies.

    ``{ref}`` inside a reply is rendered from the ref most recently seen
    in a ``send-message`` ``--body`` — mirroring the orchestrator echoing
    the intent's ref in its replies.
    """
    import re

    state = {"ref": None}

    async def runner(argv):
        sub = argv[2]
        if sub == "send-message" and "--body" in argv:
            body = argv[argv.index("--body") + 1]
            m = re.search(r"\[ref:(vh-[0-9a-f]+)\]", body)
            if m:
                state["ref"] = m.group(1)
        replies = script.get(sub, [""])
        out = replies[0]
        if len(replies) > 1:
            script[sub] = replies[1:]
        if "{ref}" in out and state["ref"]:
            out = out.replace("{ref}", state["ref"])
        return (out, "")

    return DaisLane(runner=runner)


@pytest.fixture
def captured():
    return {"events": [], "finals": []}


@pytest.fixture
def backend(captured):
    script = {
        "create-run": ["run_<redacted>\n"],
        "create-task": ["task_cc33\n"],
        "send-message": ["enqueued seq=1\n"],
        "check-messages": ['seq=5 from=session_orch to=voice-head type=status body=[ref:{ref}] 调研完成 【凭证R-7734】 结论 23%\n'],
        "check-status": ["Run run_<redacted>: 2 tasks\n"],
        "scan-wait-blocked": ["wait-gate\n"],
        "resolve-gate": ["ok\n"],
        "fail-dispatch": ["ok\n"],
    }
    lane = make_lane(script)
    bus = EventBus()
    bus_state = captured

    async def sink(kind, payload):
        bus_state["events"].append((kind, payload))

    bus.subscribe(sink)
    finals = bus_state["finals"]

    async def on_final(ref, message):
        finals.append((ref, message))

    return DshBackend(lane=lane, bus=bus, orchestrator_handle="session_orch",
                      on_final=on_final, await_timeout_s=2), script


@pytest.mark.asyncio
async def test_dispatch_phase1_receipt(backend, captured):
    b, script = backend
    receipt = json.loads(await b.dispatch("调研 WebGPU 现状"))
    assert receipt["status"] == "accepted"
    assert receipt["run_id"] == "run_<redacted>"
    assert receipt["credentials"] and receipt["credentials"][0].startswith("【凭证")
    # event emitted
    kinds = [k for k, _ in captured["events"]]
    assert "orch.dispatch" in kinds
    await asyncio.sleep(0.1)
    # give phase-2 task a moment, then cancel pending to clean up
    for t in b._pending.values():
        t.cancel()


@pytest.mark.asyncio
async def test_two_phase_final_injection(backend, captured):
    b, script = backend
    script["check-messages"] = ['seq=5 from=session_orch to=voice-head type=status body=[ref:{ref}] 调研完成 【凭证R-7734】 结论 23%\n']

    async def run():
        receipt = json.loads(await b.dispatch("调研 WebGPU 现状"))
        ref = receipt["ref"]
        for _ in range(100):
            if captured["finals"]:
                break
            await asyncio.sleep(0.02)
        return ref

    ref = await run()
    assert captured["finals"], "phase-2 final never arrived"
    got_ref, message = captured["finals"][0]
    assert got_ref == ref
    assert message.startswith('"Agent Final Message":')
    assert "【凭证R-7734】" in message
    kinds = [k for k, _ in captured["events"]]
    assert "orch.done" in kinds
    # kg/14 #3: orch.done 无 artifact 键；PR3 起 body 键携带原始正文
    # （台账桥数据源），全文回注仍只经 on_final
    dones = [p for k, p in captured["events"] if k == "orch.done"]
    assert dones and all("artifact" not in p for p in dones)
    assert dones[0]["run_id"] == "run_<redacted>"
    assert dones[0]["body"] == "调研完成 【凭证R-7734】 结论 23%"
    assert not dones[0]["body"].startswith('"Agent Final Message"')


@pytest.mark.asyncio
async def test_query_status_aggregates(backend):
    b, _ = backend
    out = json.loads(await b.query_status())
    assert out["runs"] and "2 个任务" in out["runs"][0]


@pytest.mark.asyncio
async def test_cancel_by_ref(backend, captured):
    b, _ = backend
    script = backend[1]
    script["check-messages"] = ['(no messages)\n']  # keep phase-2 pending
    receipt = json.loads(await b.dispatch("可取消任务"))
    await asyncio.sleep(0.05)
    result = json.loads(await b.cancel(receipt["ref"]))
    assert result["status"] == "canceled"
    kinds = [k for k, _ in captured["events"]]
    assert kinds.count("orch.done") >= 1
    # kg/14 #3: 取消语义不再走 artifact="(已取消)"，由 status 字段承载
    dones = [p for k, p in captured["events"] if k == "orch.done"]
    assert dones and all("artifact" not in p for p in dones)
    assert dones[-1]["status"] == "cancelled"
    assert dones[-1]["ref"] == receipt["ref"]
    # cancel 形态不带 body（无终稿正文可落台账，正文面只有正常完成路径）
    assert "body" not in dones[-1]


@pytest.mark.asyncio
async def test_backend_fn_adapter(backend):
    b, _ = backend
    fn = b.as_backend_fn()
    result = await fn("researcher", "查 X")
    assert result.agent == "researcher"
    assert "accepted" in result.finding
    for t in b._pending.values():
        t.cancel()


# ---- G3: wait-blocked surfacing + gate resolve (LB-002-C consumer side) ----

@pytest.mark.asyncio
async def test_query_status_surfaces_wait_blocked(backend, captured):
    # pending ctx_ dispatch → scan-wait-blocked label lands in the lines
    from rt_dsh_backend import DshDispatch

    b, _ = backend
    ref = "vh-<redacted>"
    disp = DshDispatch(run_id="run_<redacted>", task_id="ctx_9f", ref=ref,
                       credentials=["【凭证X】"])

    async def noop(*a, **k):
        pass

    b._runs[ref] = disp
    b._pending[ref] = asyncio.get_event_loop().create_task(noop())
    out = json.loads(await b.query_status())
    assert any("卡在 wait-gate" in ln for ln in out["runs"]), out
    b._pending[ref].cancel()


@pytest.mark.asyncio
async def test_query_status_skips_scan_when_no_ctx_handle(backend, captured):
    from rt_dsh_backend import DshDispatch

    b, _ = backend
    ref = "vh-nohandle"
    disp = DshDispatch(run_id="run_<redacted>", task_id=None, ref=ref, credentials=[])

    async def noop(*a, **k):
        pass

    b._runs[ref] = disp
    b._pending[ref] = asyncio.get_event_loop().create_task(noop())
    out = json.loads(await b.query_status())
    assert not any("卡在" in ln for ln in out["runs"]), out
    b._pending[ref].cancel()


class _MockA2a:
    """Lane-a mock recording pool/spawn (binding) and send order."""

    def __init__(self):
        self.calls: list[tuple] = []

    async def send(self, raw_intent, ref="-", source="voice-head"):
        self.calls.append(("send", raw_intent, ref))
        return "task_mock01"

    async def pool_spawn(self, profile, *, strategy="binding-mode",
                         binding_session_id="", role=None, mailbox=None,
                         project=None):
        self.calls.append(("pool_spawn", profile, strategy,
                           binding_session_id))
        return {"target": "binding", "name": profile, "version": "v1",
                "sessionId": binding_session_id, "injected": True}


@pytest.mark.asyncio
async def test_dispatch_profile_dresses_before_send_idempotent(backend, captured):
    """G4: a dispatch-carried profile binds onto the in-flight session
    via pool/spawn binding-mode BEFORE the intent goes out; rebinding
    the same (profile, session) is a no-op."""
    b, _ = backend
    a2a = _MockA2a()
    b.lane_a = a2a
    b.lane_mode = "a"
    b.bind_session_id = "session_dressed01"

    receipt = json.loads(await b.dispatch("调研 X", profile="vh-liaison"))
    assert receipt["status"] == "accepted"
    assert receipt["profile"] == {"name": "vh-liaison", "version": "v1",
                                  "sessionId": "session_dressed01",
                                  "injected": True}
    # dressing strictly precedes the intent submission
    assert a2a.calls[0][:2] == ("pool_spawn", "vh-liaison")
    assert a2a.calls[0][2:] == ("binding-mode", "session_dressed01")
    assert a2a.calls[1][0] == "send"
    # second dispatch with the same profile: no rebind
    n_calls = len(a2a.calls)
    await b.dispatch("再调研 Y", profile="vh-liaison")
    binds = [c for c in a2a.calls if c[0] == "pool_spawn"]
    assert len(binds) == 1
    assert len(a2a.calls) == n_calls + 1
    for t in b._pending.values():
        t.cancel()


@pytest.mark.asyncio
async def test_dispatch_without_profile_never_binds(backend, captured):
    b, _ = backend
    a2a = _MockA2a()
    b.lane_a = a2a
    b.lane_mode = "a"
    b.bind_session_id = "session_dressed01"

    await b.dispatch("普通意图")
    assert all(c[0] != "pool_spawn" for c in a2a.calls)
    assert a2a.calls[0][0] == "send"
    for t in b._pending.values():
        t.cancel()


@pytest.mark.asyncio
async def test_resolve_gate_passthrough(backend, captured):
    b, _ = backend
    out = json.loads(await b.resolve("gate_1", "go"))
    assert out == {"status": "resolved", "gate": "gate_1", "resolution": "go"}
    assert any(e[0] == "orch.progress" and "gate_1" in e[1].get("note", "")
               for e in captured["events"])


# ---- LB-002-B: dependency-ordered DAG dispatch (lane b-dag) ----

def make_dag_lane(t1_outcome="succeeded"):
    """Stateful lane mock: two tasks, task 2 depends on task 1.

    Records start-worker order/flags; worker_done rows are per-dispatch
    one-shots (consumed on first read), mirroring mailbox semantics.
    """
    done = {
        "ctx_a1": [f'seq=1 from=w to=ctx_a1 type=worker_done body={{"task_id":"task_11","dispatch_id":"ctx_a1","outcome":"{t1_outcome}"}}'],
        "ctx_b2": ['seq=2 from=w to=ctx_b2 type=worker_done body={"task_id":"task_22","dispatch_id":"ctx_b2","outcome":"succeeded"}'],
    }
    starts: list[tuple] = []
    injects: list[tuple] = []
    create_task_calls: list[list[str]] = []

    async def runner(argv):
        sub = argv[2]
        if sub == "create-run":
            return ("run_beef\n", "")
        if sub == "create-task":
            create_task_calls.append(list(argv))
            spec = argv[4]
            return (f"task_{'11' if '甲' in spec else '22'}\n", "")
        if sub == "start-worker":
            flags = {argv[i]: argv[i + 1] for i in range(3, len(argv) - 1)
                     if argv[i].startswith("--")}
            starts.append((argv[3], flags.get("--command"),
                           flags.get("--session")))
            return ("ctx_a1\n" if argv[3] == "task_11" else "ctx_b2\n", "")
        if sub == "inject-prompt":
            injects.append((argv[3], argv[4]))
            return ("ok\n", "")
        if sub == "check-messages":
            rows = done.get(argv[3], [])
            return ((rows.pop(0) + "\n") if rows else ("no unread messages\n", ""), "")
        if sub == "read-worker":
            return (f"tail of {argv[3]}\n", "cursor: 9\n")
        if sub == "promote-tasks":
            return ("ok\n", "")
        return ("", "")

    return DaisLane(runner=runner), starts, create_task_calls


@pytest.mark.asyncio
async def test_dispatch_dag_dependency_waves_and_aggregate(captured):
    from rt_dsh_backend import DagTaskSpec

    lane, starts, ctc = make_dag_lane()
    bus = EventBus()

    async def sink(kind, payload):
        captured["events"].append((kind, payload))

    bus.subscribe(sink)

    async def on_final(ref, message):
        captured["finals"].append((ref, message))

    b = DshBackend(lane=lane, bus=bus, on_final=on_final, await_timeout_s=4,
                   poll_s=0.05, poll_max_s=0.2)
    receipt = json.loads(await b.dispatch_dag(
        "对比调研甲乙", [
            DagTaskSpec(spec="调研甲方案", command="echo A"),
            DagTaskSpec(spec="调研乙方案", deps=[0], command="echo B",
                        session="session_worker2"),
        ]))
    assert receipt["status"] == "accepted" and receipt["tasks"] == 2
    assert receipt["run_id"] == "run_beef"

    for _ in range(200):
        if captured["finals"]:
            break
        await asyncio.sleep(0.05)
    assert captured["finals"], "DAG final never arrived"
    ref, final = captured["finals"][0]
    assert ref == receipt["ref"]
    from rt_orchestrator import FINAL_PREFIX
    assert final.startswith(FINAL_PREFIX)
    assert "子任务1" in final and "succeeded" in final
    assert "子任务2" in final and "succeeded" in final
    assert "【凭证" in final
    # dependency wiring: task 2's create-task carries --dep task_11
    dep_calls = [c for c in ctc if "task_22" in c or c[4] == "调研乙方案"]
    assert any("--dep" in c and c[c.index("--dep") + 1] == "task_11"
               for c in dep_calls), ctc
    # wave order: task 1's worker started before task 2's
    assert [s[0] for s in starts] == ["task_11", "task_22"]
    # per-task worker flavor: command mode vs session-bound
    assert starts[0][1] == "echo A" and starts[0][2] is None
    assert starts[1][2] == "session_worker2"
    # execution wire: each command block is injected into its bound terminal
    # (block settlement fires only when the command actually runs there)
    injected = [(argv[3], argv[4])
                for argv in lane._call_log if argv[2] == "inject-prompt"]
    assert ("ctx_a1", "echo A") in injected
    assert ("ctx_b2", "echo B") in injected
    assert any(e[0] == "orch.dispatch" and e[1].get("lane") == "b-dag"
               for e in captured["events"])
    # kg/14 #3: DAG 聚合终稿的 orch.done 无 artifact；body = 原始聚合正文
    # （剥 FINAL_PREFIX），与 on_final 收到的 final 严格互补
    dones = [p for k, p in captured["events"] if k == "orch.done"]
    assert dones and all("artifact" not in p for p in dones)
    assert dones[0]["run_id"] == "run_beef"
    assert dones[0]["body"] == final[len(FINAL_PREFIX):]


@pytest.mark.asyncio
async def test_dispatch_dag_pool_binds_sessions_round_robin(captured):
    """dag_workers pool: tasks without explicit session bind round-robin."""
    from rt_dsh_backend import DagTaskSpec

    lane, starts, _ = make_dag_lane()
    bus = EventBus()

    async def sink(kind, payload):
        captured["events"].append((kind, payload))

    bus.subscribe(sink)

    async def on_final(ref, message):
        captured["finals"].append((ref, message))

    b = DshBackend(lane=lane, bus=bus, on_final=on_final, await_timeout_s=4,
                   poll_s=0.05, poll_max_s=0.2,
                   dag_workers=["session_w1", "session_w2"])
    await b.dispatch_dag("两步链", [
        DagTaskSpec(spec="调研甲方案", command="echo A"),
        DagTaskSpec(spec="调研乙方案", deps=[0], command="echo B",
                    session="session_explicit"),
    ])
    for _ in range(200):
        if captured["finals"]:
            break
        await asyncio.sleep(0.05)
    assert captured["finals"], "DAG final never arrived"
    sessions = [s[2] for s in starts]
    assert sessions == ["session_w1", "session_explicit"]


@pytest.mark.asyncio
async def test_dispatch_dag_failed_dep_skips_dependent(captured):
    from rt_dsh_backend import DagTaskSpec

    lane, starts, _ = make_dag_lane(t1_outcome="failed")
    bus = EventBus()

    async def sink(kind, payload):
        captured["events"].append((kind, payload))

    bus.subscribe(sink)

    async def on_final(ref, message):
        captured["finals"].append((ref, message))

    b = DshBackend(lane=lane, bus=bus, on_final=on_final, await_timeout_s=4,
                   poll_s=0.05, poll_max_s=0.2)
    receipt = json.loads(await b.dispatch_dag(
        "对比调研甲乙", [
            DagTaskSpec(spec="调研甲方案", command="echo A"),
            DagTaskSpec(spec="调研乙方案", deps=[0], command="echo B"),
        ]))
    for _ in range(200):
        if captured["finals"]:
            break
        await asyncio.sleep(0.05)
    assert captured["finals"], "DAG final never arrived"
    final = captured["finals"][0][1]
    assert "failed" in final and "skipped" in final
    # the dependent task never got a worker
    assert [s[0] for s in starts] == ["task_11"]


@pytest.mark.asyncio
async def test_dispatch_plan_liaison_body_is_json(backend):
    """Regression (2026-08-27 live): the liaison branch of dispatch_plan
    json.dumps'd DagTaskSpec dataclasses and raised TypeError — the tool
    died in 3ms and nothing ever reached the liaison session. With a
    liaison configured, the PLAN body must carry plain dicts."""
    b, script = backend
    b.liaison_session = "session-3499test"
    prompts: list[dict] = []

    async def fake_dsh_api(method, payload):
        if method == "session.prompt":
            prompts.append(payload)
        return {"items": []} if method == "session.list" else {}

    b._dsh_api = fake_dsh_api

    receipt = json.loads(await b.dispatch_plan(
        "生成对比报告",
        '[{"spec":"收集数据","command":"echo A"},'
        '{"spec":"分析差异","deps":[0],"command":"echo B"}]'))
    assert receipt["status"] == "accepted"
    assert receipt["tasks"] == 2
    assert prompts, "liaison turn never prompted"
    text = prompts[0]["content"][0]["text"]
    assert text.startswith("DSHMSG]"), text[:80]
    inner = json.loads(text[len("DSHMSG]"):])
    assert inner["type"] == "ask"
    plan_body = inner["body"]
    head, _, payload = plan_body.partition(" || ")
    assert head.startswith("PLAN 生成对比报告 run=run_<redacted>")
    tasks = json.loads(payload)
    assert tasks == [
        {"spec": "收集数据", "deps": [], "command": "echo A", "session": None},
        {"spec": "分析差异", "deps": [0], "command": "echo B", "session": None},
    ]
    for t in b._pending.values():
        t.cancel()


# ---- PR2 (kg/14 §2.3): liaison acceptance receipt slimming ----

def make_liaison_backend(**overrides):
    """Backend wired to a mock liaison session: ``_dsh_api`` faked (no
    fleet.json read, no loopback HTTP), the lane answers create-run only."""
    script = {"create-run": ["run_<redacted>\n"]}
    prompts: list[dict] = []

    async def fake_dsh_api(method, payload):
        if method == "session.prompt":
            prompts.append(payload)
        return {"items": []} if method == "session.list" else {}

    b = DshBackend(lane=make_lane(script), bus=EventBus(),
                   liaison_session="session-3499test", **overrides)
    b._dsh_api = fake_dsh_api
    return b, prompts


@pytest.mark.asyncio
async def test_liaison_receipt_slim_default(monkeypatch):
    """缺省 SLIM=1：受理回执仅 status/ref/summary —— run_id/credentials
    不进模型上下文，只留观测面（_runs + orch.dispatch）。"""
    monkeypatch.delenv("VOICE_RECEIPT_SLIM", raising=False)
    b, _ = make_liaison_backend()
    receipt = json.loads(await b.dispatch("调研 X"))
    assert set(receipt) == {"status", "ref", "summary"}
    assert receipt["status"] == "accepted"
    assert receipt["ref"].startswith("vh-")
    assert receipt["summary"] == "已受理，转对接人执行"
    # credentials 仍在进程内登记（cancel/对账锚点不丢）
    assert b._runs[receipt["ref"]].credentials[0] == \
        f"【凭证{receipt['ref'].upper()}】"
    for t in b._pending.values():
        t.cancel()


@pytest.mark.asyncio
async def test_liaison_receipt_full_when_slim_off(monkeypatch):
    """VOICE_RECEIPT_SLIM=0：逐字段回旧全形，字节级不变（回退路径）。"""
    monkeypatch.setenv("VOICE_RECEIPT_SLIM", "0")
    b, _ = make_liaison_backend()
    raw = await b.dispatch("调研 X")
    receipt = json.loads(raw)
    ref = receipt["ref"]
    expected = {
        "status": "accepted",
        "run_id": "run_<redacted>",
        "ref": ref,
        "credentials": [f"【凭证{ref.upper()}】"],
        "note": "已转对接人（新回合），完成后播报",  # session idle → queue
    }
    assert receipt == expected
    assert raw == json.dumps(expected, ensure_ascii=False)
    for t in b._pending.values():
        t.cancel()


@pytest.mark.asyncio
async def test_liaison_receipt_slim_switch_resolution(monkeypatch):
    """开关解析：字段缺省读 env（"0" 关、其余开）；构造参置位时钉死，
    不受 env 影响。"""
    b, _ = make_liaison_backend()
    monkeypatch.setenv("VOICE_RECEIPT_SLIM", "0")
    assert b._receipt_slim() is False
    monkeypatch.setenv("VOICE_RECEIPT_SLIM", "1")
    assert b._receipt_slim() is True
    monkeypatch.delenv("VOICE_RECEIPT_SLIM", raising=False)
    assert b._receipt_slim() is True  # 缺省 1
    pinned_on, _ = make_liaison_backend(receipt_slim=True)
    monkeypatch.setenv("VOICE_RECEIPT_SLIM", "0")
    assert pinned_on._receipt_slim() is True
    pinned_off, _ = make_liaison_backend(receipt_slim=False)
    monkeypatch.delenv("VOICE_RECEIPT_SLIM", raising=False)
    assert pinned_off._receipt_slim() is False


@pytest.mark.asyncio
async def test_liaison_receipt_slim_plan_carries_tasks(monkeypatch):
    """dispatch_plan 路径：slim 形并入 extra_receipt 的 tasks 数量。"""
    monkeypatch.delenv("VOICE_RECEIPT_SLIM", raising=False)
    b, prompts = make_liaison_backend()
    receipt = json.loads(await b.dispatch_plan(
        "生成对比报告",
        '[{"spec":"收集数据","command":"echo A"},'
        '{"spec":"分析差异","deps":[0],"command":"echo B"}]'))
    assert set(receipt) == {"status", "ref", "summary", "tasks"}
    assert receipt["tasks"] == 2
    assert prompts, "liaison turn never prompted"
    for t in b._pending.values():
        t.cancel()


@pytest.mark.asyncio
async def test_liaison_receipt_full_plan_keeps_tasks(monkeypatch):
    """SLIM=0 时 plan 回执沿用旧全形 + tasks 并入（机制不变）。"""
    monkeypatch.setenv("VOICE_RECEIPT_SLIM", "0")
    b, _ = make_liaison_backend()
    receipt = json.loads(await b.dispatch_plan(
        "生成对比报告",
        '[{"spec":"收集数据","command":"echo A"},'
        '{"spec":"分析差异","deps":[0],"command":"echo B"}]'))
    assert set(receipt) == {"status", "run_id", "ref", "credentials",
                            "note", "tasks"}
    assert receipt["tasks"] == 2
    assert receipt["run_id"] == "run_<redacted>"
    for t in b._pending.values():
        t.cancel()


# ---- kg/14：orch.done 载荷面（无 artifact、PR3 加 body）+ 终点失败面 ----

def make_phase2_backend(captured, check_messages_reply="", lane_error=False):
    """Backend purpose-built for direct ``_phase2`` drives: the lane only
    answers ``check-messages`` (fixed reply, or a DaisLaneError every
    call); the bus sink and on_final both record into ``captured``."""
    from rt_dsh_backend import DshBackend

    async def runner(argv):
        if argv[2] == "check-messages":
            if lane_error:
                raise DaisLaneError(
                    'exit=1 check-messages…: database is locked')
            return (check_messages_reply, "")
        return ("", "")

    bus = EventBus()

    async def sink(kind, payload):
        captured["events"].append((kind, payload))

    bus.subscribe(sink)

    async def on_final(ref, message):
        captured["finals"].append((ref, message))

    return DshBackend(lane=DaisLane(runner=runner), bus=bus,
                      on_final=on_final, poll_s=0.05, poll_max_s=0.2)


@pytest.mark.asyncio
async def test_phase2_done_carries_raw_body(captured):
    """PR3：orch.done 加 body 键（台账桥数据源）——值是剥掉
    FINAL_PREFIX 的原始正文；artifact 键仍不存在（裁决 #3），on_final
    全文回注行为不变。"""
    from rt_dsh_backend import DshDispatch

    ref = "vh-<redacted>"
    b = make_phase2_backend(
        captured,
        check_messages_reply=(
            f"seq=5 from=session_orch to=voice-head type=status "
            f"body=[ref:{ref}] 调研完成 【凭证R-AB12CD34】 结论 23%\n"))
    disp = DshDispatch(run_id="run_<redacted>", task_id=None, ref=ref,
                       credentials=["【凭证R-AB12CD34】"])
    await b._phase2(ref, disp)
    dones = [p for k, p in captured["events"] if k == "orch.done"]
    assert len(dones) == 1
    assert "artifact" not in dones[0]
    # 只剩 ref/run_id/body + 总线统一加盖的 ts
    assert set(dones[0]) == {"ref", "run_id", "body", "ts"}
    assert dones[0]["ref"] == ref and dones[0]["run_id"] == "run_<redacted>"
    assert dones[0]["body"] == "调研完成 【凭证R-AB12CD34】 结论 23%"
    assert captured["finals"], "on_final fulltext re-injection was lost"
    assert "调研完成" in captured["finals"][0][1]
    assert not any(k == "orch.failed" for k, _ in captured["events"])


@pytest.mark.asyncio
async def test_phase2_done_body_strips_final_prefix(captured):
    """邮箱回执自带 FINAL_PREFIX 时，orch.done.body 与 on_final 正文同为
    剥前缀后的原始正文——前缀是回注协议包装，不进台账。"""
    from rt_dsh_backend import DshDispatch
    from rt_orchestrator import FINAL_PREFIX

    ref = "vh-<redacted>"
    row = {"seq": 5, "from": "session_orch", "to": "voice-head",
           "type": "status",
           "body": f'[ref:{ref}] {FINAL_PREFIX}调研完成 结论 41%'}
    b = make_phase2_backend(captured,
                            check_messages_reply=json.dumps(row) + "\n")
    disp = DshDispatch(run_id="run_<redacted>", task_id=None, ref=ref,
                       credentials=[])
    await b._phase2(ref, disp)
    dones = [p for k, p in captured["events"] if k == "orch.done"]
    assert len(dones) == 1
    assert dones[0]["body"] == "调研完成 结论 41%"
    assert not dones[0]["body"].startswith('"Agent Final Message"')
    # on_final 收到的仍是 FINAL_PREFIX + 正文（回注协议不变）
    assert captured["finals"][0][1] == f"{FINAL_PREFIX}调研完成 结论 41%"


@pytest.mark.asyncio
async def test_phase2_dag_deadline_emits_orch_failed(captured):
    """DAG 预算耗尽仍有任务未结算 → orch.failed（对齐 _phase2 超时形态
    ref/run_id/reason），不再静默 return——ref 在台账落 status=failed
    而非永久悬置。"""
    from rt_dsh_backend import DshDispatch

    async def runner(argv):
        sub = argv[2]
        if sub == "start-worker":
            return ("ctx_deadbeef\n", "")
        return ("no unread messages\n" if sub == "check-messages" else "", "")

    bus = EventBus()

    async def sink(kind, payload):
        captured["events"].append((kind, payload))

    bus.subscribe(sink)

    async def on_final(ref, message):
        captured["finals"].append((ref, message))

    b = DshBackend(lane=DaisLane(runner=runner), bus=bus, on_final=on_final,
                   await_timeout_s=0.3, poll_s=0.05, poll_max_s=0.1)
    ref = "vh-dag0123"
    disp = DshDispatch(run_id="run_dag0123", task_id=None, ref=ref,
                       credentials=[])
    disp.dag = [{"task_id": "task_d1", "deps": [], "command": None,
                 "session": None, "spec": "调研甲"}]
    disp.dag_ctx = {}
    await b._phase2_dag(ref, disp)
    fails = [p for k, p in captured["events"] if k == "orch.failed"]
    assert len(fails) == 1
    assert fails[0]["ref"] == ref
    assert fails[0]["run_id"] == "run_dag0123"
    assert fails[0]["reason"] == "still running"
    assert isinstance(fails[0]["ts"], float)
    assert not any(k == "orch.done" for k, _ in captured["events"])
    assert not captured["finals"]


@pytest.mark.asyncio
async def test_phase2_lane_a_task_gone_emits_orch_failed(captured):
    """lane-a 任务被服务端遗忘（重启/回滚）→ orch.failed（终稿不可知，
    ref 落 failed），不再只发 progress 后静默 return。"""
    from rt_a2a_client import A2aError
    from rt_dsh_backend import DshDispatch

    class GoneA2a:
        async def await_done(self, task_id, timeout_s=600.0):
            raise A2aError(-2, f"task {task_id} failed: rolled")

    b = make_phase2_backend(captured)
    b.lane_a = GoneA2a()
    ref = "vh-la012345"
    disp = DshDispatch(run_id="t_mock01", task_id=None, ref=ref,
                       credentials=[])
    await b._phase2(ref, disp)
    fails = [p for k, p in captured["events"] if k == "orch.failed"]
    assert len(fails) == 1
    assert fails[0]["ref"] == ref
    assert fails[0]["run_id"] == "t_mock01"
    assert fails[0]["reason"].startswith("lane-a task gone")
    assert "rolled" in fails[0]["reason"]
    assert isinstance(fails[0]["ts"], float)
    assert not any(k == "orch.done" for k, _ in captured["events"])
    assert not captured["finals"]


@pytest.mark.asyncio
async def test_phase2_timeout_emits_orch_failed(captured):
    """邮箱一直空 → await_done TimeoutError → orch.failed(reason="still
    running")，不再静默 return（写入面据此落 status=failed）。"""
    from rt_dsh_backend import DshDispatch

    b = make_phase2_backend(captured, check_messages_reply="(no messages)\n")
    b.await_timeout_s = 0.2
    ref = "vh-to012345"
    disp = DshDispatch(run_id="run_to012345", task_id=None, ref=ref,
                       credentials=[])
    await b._phase2(ref, disp)
    fails = [p for k, p in captured["events"] if k == "orch.failed"]
    assert len(fails) == 1
    assert fails[0]["ref"] == ref
    assert fails[0]["run_id"] == "run_to012345"
    assert fails[0]["reason"] == "still running"
    assert isinstance(fails[0]["ts"], float)
    assert not any(k == "orch.done" for k, _ in captured["events"])
    assert not captured["finals"]


@pytest.mark.asyncio
async def test_phase2_lane_errors_until_deadline_emit_orch_failed(captured):
    """check-messages 每次 DaisLaneError 且撑到预算终点 → orch.failed
    (reason="lane errors until deadline")。"""
    from rt_dsh_backend import DshDispatch

    b = make_phase2_backend(captured, lane_error=True)
    b.await_timeout_s = 0.2
    ref = "vh-le012345"
    disp = DshDispatch(run_id="run_le012345", task_id=None, ref=ref,
                       credentials=[])
    await b._phase2(ref, disp)
    fails = [p for k, p in captured["events"] if k == "orch.failed"]
    assert len(fails) == 1
    assert fails[0]["ref"] == ref
    assert fails[0]["run_id"] == "run_le012345"
    assert fails[0]["reason"] == "lane errors until deadline"
    assert isinstance(fails[0]["ts"], float)
    assert not any(k == "orch.done" for k, _ in captured["events"])
    assert not captured["finals"]
