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
