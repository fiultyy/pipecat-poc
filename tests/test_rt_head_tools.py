#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for the four head tools over DshBackend (W1.4)."""

import asyncio
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_dsh_backend import DshBackend  # noqa: E402
from rt_dsh_lane import DaisLane  # noqa: E402
from rt_event_bus import EventBus  # noqa: E402
from rt_head_tools import (  # noqa: E402
    DSH_TOOLS_DOCTRINE,
    dsh_head_tools,
    cancel_run_tool,
    dispatch_intent_tool,
    query_status_tool,
    remain_silent_tool,
)


@dataclass
class FakeParams:
    """Minimal FunctionCallParams stand-in: resources + captured callback."""

    app_resources: dict = field(default_factory=dict)
    results: list = field(default_factory=list)

    async def result_callback(self, value):
        self.results.append(value)


def make_backend(**overrides):
    state = {"ref": None}
    script = {
        "create-run": ["run_<redacted>\n"],
        "send-message": ["enqueued seq=1\n"],
        "check-messages": [
            'seq=5 from=session_orch to=voice-head type=status '
            'body=[ref:{ref}] 调研完成 【凭证R-7734】 结论 23%\n'
        ],
        "check-status": ["Run run_<redacted>: 2 tasks\n"],
        "fail-dispatch": ["ok\n"],
    }

    async def runner(argv):
        sub = argv[2]
        if sub == "send-message" and "--body" in argv:
            body = argv[argv.index("--body") + 1]
            m = re.search(r"\[ref:(vh-[0-9a-f]+)\]", body)
            if m:
                state["ref"] = m.group(1)
        out = script.get(sub, [""])[0]
        if "{ref}" in out and state["ref"]:
            out = out.replace("{ref}", state["ref"])
        return (out, "")

    finals = []

    async def on_final(ref, message):
        finals.append((ref, message))

    backend = DshBackend(
        lane=DaisLane(runner=runner),
        bus=EventBus(),
        orchestrator_handle="session_orch",
        on_final=on_final,
        await_timeout_s=1,
        **overrides,
    )
    return backend, finals, script


@pytest.mark.asyncio
async def test_dispatch_intent_tool_returns_phase1_receipt():
    backend, finals, _ = make_backend()
    params = FakeParams(app_resources={"dsh_backend": backend})
    await dispatch_intent_tool(params, "调研 WebGPU 现状")
    receipt = json.loads(params.results[0])
    assert receipt["status"] == "accepted"
    assert receipt["ref"].startswith("vh-")
    assert receipt["credentials"][0].startswith("【凭证")
    for t in backend._pending.values():
        t.cancel()


@pytest.mark.asyncio
async def test_dispatch_intent_tool_phase2_final_flows_to_on_final():
    backend, finals, _ = make_backend()
    params = FakeParams(app_resources={"dsh_backend": backend})
    await dispatch_intent_tool(params, "调研 X")
    for _ in range(100):
        if finals:
            break
        await asyncio.sleep(0.02)
    assert finals and finals[0][1].startswith('"Agent Final Message":')


@pytest.mark.asyncio
async def test_query_status_tool():
    backend, _, _ = make_backend()
    params = FakeParams(app_resources={"dsh_backend": backend})
    await query_status_tool(params)
    out = json.loads(params.results[0])
    assert out["runs"] and "2 个任务" in out["runs"][0]


@pytest.mark.asyncio
async def test_cancel_run_tool():
    backend, _, script = make_backend()
    script["check-messages"] = ["(no messages)\n"]  # keep phase-2 pending
    params = FakeParams(app_resources={"dsh_backend": backend})
    await dispatch_intent_tool(params, "可取消任务")
    ref = json.loads(params.results[0])["ref"]
    await asyncio.sleep(0.05)
    await cancel_run_tool(params, ref)
    out = json.loads(params.results[-1])
    assert out["status"] == "canceled"


@pytest.mark.asyncio
async def test_remain_silent_tool():
    params = FakeParams()
    await remain_silent_tool(params)
    assert params.results == [{"status": "silent"}]


def test_dsh_head_tools_registry():
    names = {fn.__name__ for fn in dsh_head_tools()}
    assert names == {
        "dispatch_intent_tool", "query_status_tool",
        "cancel_run_tool", "remain_silent_tool",
    }


def test_doctrine_covers_all_tools_and_two_phase_rule():
    for name in ("dispatch_intent", "query_status", "cancel_run", "remain_silent"):
        assert name in DSH_TOOLS_DOCTRINE
    assert "【凭证" in DSH_TOOLS_DOCTRINE
    assert "Agent Final Message" in DSH_TOOLS_DOCTRINE
