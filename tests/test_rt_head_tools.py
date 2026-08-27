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
    dispatch_plan_tool,
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


@pytest.mark.asyncio
async def test_dispatch_plan_tool_parses_and_dispatches_dag():
    import asyncio as _aio

    from rt_dsh_backend import DagTaskSpec

    captured: dict = {}

    async def fake_dispatch_dag(objective, tasks):
        captured["objective"] = objective
        captured["tasks"] = tasks
        return '{"status": "accepted", "tasks": 2}'

    backend, _, _ = make_backend()
    backend.dispatch_dag = fake_dispatch_dag  # type: ignore[method-assign]
    params = FakeParams(app_resources={"dsh_backend": backend})
    subtasks = [{"spec": "统计帧类", "command": "grep -c 'class.*Frame' a.py"},
                {"spec": "统计处理器", "deps": [0], "cmd": "grep -rc b"}]
    await dispatch_plan_tool(
        params, "对比两数",
        json.dumps(subtasks, ensure_ascii=False))
    receipt = json.loads(params.results[0])
    assert receipt["status"] == "accepted"
    assert captured["objective"] == "对比两数"
    tasks = captured["tasks"]
    assert all(isinstance(t, DagTaskSpec) for t in tasks)
    assert tasks[0].deps == [] and tasks[0].command.startswith("grep")
    assert tasks[1].deps == [0] and tasks[1].command == "grep -rc b"


@pytest.mark.asyncio
async def test_dispatch_plan_tool_clarify_on_garbage():
    backend, _, _ = make_backend()
    params = FakeParams(app_resources={"dsh_backend": backend})
    await dispatch_plan_tool(params, "目标", "不是JSON")
    assert json.loads(params.results[0])["status"] == "clarify"
    for t in backend._pending.values():
        t.cancel()


def test_dsh_head_tools_registry():
    names = {fn.__name__ for fn in dsh_head_tools()}
    assert names == {
        "dispatch_intent_tool", "dispatch_plan_tool", "query_status_tool",
        "cancel_run_tool", "remain_silent_tool",
    }


def test_doctrine_covers_all_tools_and_two_phase_rule():
    for name in ("dispatch_intent", "dispatch_plan", "query_status",
                 "cancel_run", "remain_silent"):
        assert name in DSH_TOOLS_DOCTRINE


def test_doctrine_four_section_shape():
    headers = ("# Persona and Role", "# Tools", "# After Tool Calls",
               "# Personality and Tone")
    positions = [DSH_TOOLS_DOCTRINE.find(h) for h in headers]
    assert all(p >= 0 for p in positions)
    assert positions == sorted(positions)


def test_doctrine_persona_section_keywords():
    persona = DSH_TOOLS_DOCTRINE.split("# Tools")[0]
    assert "Nova" in persona
    assert "任务助手" in persona
    assert "精炼表述" in persona and "状态优先" in persona
    assert "详情栏" in persona


def test_doctrine_numbering_protocol():
    assert "默认不念编号" in DSH_TOOLS_DOCTRINE
    assert "前三位" in DSH_TOOLS_DOCTRINE
    assert "完整 ref" in DSH_TOOLS_DOCTRINE
    # cancel_run 条款：head 从上下文回执解析 ref，不让用户念编号
    assert "由你从上下文" in DSH_TOOLS_DOCTRINE
    assert "不让用户念编号" in DSH_TOOLS_DOCTRINE


def test_doctrine_receipt_rule_no_verbatim_relay():
    after = DSH_TOOLS_DOCTRINE.split("# After Tool Calls")[1]
    # 受理回执一句话；不念凭证语义（防 SLIM=0 全形回退时照念）
    assert "不念凭证" in after
    assert "不念 ref" in after
    assert "不念 run_id" in after
    assert "不复述" in after and "JSON" in after
    # 终稿/通报通用条款：罩住全文注入与 [编排通报] JSON 两种载荷
    assert "Agent Final Message" in after
    assert "[编排通报]" in after
    assert "一句话" in after
    # 旧逐字转述条款已废除
    assert "逐字" not in DSH_TOOLS_DOCTRINE
    assert "一个字符" not in DSH_TOOLS_DOCTRINE


def test_doctrine_no_pr4_tools_yet():
    # read_body/list_bodies 属 PR4，本 PR 不入 doctrine
    assert "read_body" not in DSH_TOOLS_DOCTRINE
    assert "list_bodies" not in DSH_TOOLS_DOCTRINE
