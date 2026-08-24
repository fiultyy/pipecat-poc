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
from rt_dsh_lane import DaisLane  # noqa: E402
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
    assert any(e[0] == "orch.dispatch" and e[1].get("lane") == "b-dag"
               for e in captured["events"])


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
