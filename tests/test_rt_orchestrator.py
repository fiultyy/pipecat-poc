#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for the orchestration layer (offline; formatter faked)."""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_orchestrator import (
    AGENT_KINDS,
    BackendResult,
    Formatter,
    Normalizer,
    Orchestrator,
    extract_credentials,
    make_credential,
)


# ---- Normalizer: the schema-softness defense ----

def test_normalizer_exact():
    assert Normalizer.agent("researcher") == "researcher"


def test_normalizer_near_miss_names():
    # the three live-observed drift shapes
    assert Normalizer.agent("agent_01") == "researcher"
    assert Normalizer.agent("backend_2") == "writer"
    assert Normalizer.agent("marketing_agent") == "writer"
    assert Normalizer.agent("coding_agent") == "coder"
    assert Normalizer.agent("research_agent") == "researcher"


def test_normalizer_digits_only():
    assert Normalizer.agent("001") == "researcher"  # leading zero stripped
    assert Normalizer.agent("3") == "coder"


def test_normalizer_unknown_is_none():
    assert Normalizer.agent("chef") is None


def test_normalizer_subtasks_key_variants():
    args = json.dumps({"tasks": [{"type": "research", "task": "查天气"}]})
    subs = Normalizer.subtasks(args)
    assert subs == [{"agent": "researcher", "goal": "查天气"}]
    # field drift: prompt instead of goal
    args2 = {"agents": [{"role": "coder", "prompt": "写快排"}]}
    assert Normalizer.subtasks(args2) == [{"agent": "coder", "goal": "写快排"}]
    # unparseable -> empty
    assert Normalizer.subtasks("not json") == []


def test_normalizer_never_invents():
    subs = Normalizer.subtasks({"subtasks": [{"agent": "chef", "goal": "做饭"}]})
    assert subs == []  # unknown kind dropped, not coerced


# ---- credentials convention ----

def test_credential_roundtrip():
    wrapped = make_credential("ORD-88213-ZK")
    assert wrapped == "【凭证ORD-88213-ZK】"
    assert extract_credentials(f"订单号是{wrapped}请核对") == ["ORD-88213-ZK"]


def test_extract_multiple_credentials():
    text = "【凭证A-1】和【凭证B-2】都已交付"
    assert extract_credentials(text) == ["A-1", "B-2"]


# ---- Orchestrator with a fake formatter ----

class FakeFormatter(Formatter):
    def __init__(self, plan=None, relay=None):
        super().__init__(client=None, model="fake")
        self._plan = plan or []
        self._relay = relay or ""
        self.calls = []

    async def plan(self, raw_intent):
        self.calls.append(("plan", raw_intent))
        return self._plan

    async def relay(self, raw_intent, results):
        self.calls.append(("relay", len(results)))
        return self._relay


async def backend(agent, goal):
    return BackendResult(agent=agent, finding=f"[{agent}] done {goal}",
                         canary=f"X-{agent}-1")


def test_orchestrator_parallel_fanout_and_relay():
    fmt = FakeFormatter(
        plan=[{"agent": "researcher", "goal": "a"}, {"agent": "coder", "goal": "b"}],
        relay='{"results": [...verbatim...]}',
    )
    orch = Orchestrator(formatter=fmt, backend_fn=backend)
    out = asyncio.run(orch.dispatch_intent("做两件事"))
    assert out.startswith('"Agent Final Message":\n\n')
    assert "verbatim" in out
    # history recorded
    assert len(orch.history) == 1
    assert len(orch.history[0]["results"]) == 2


def test_orchestrator_unsplittable_intent_clarifies():
    fmt = FakeFormatter(plan=[])
    orch = Orchestrator(formatter=fmt, backend_fn=backend)
    out = asyncio.run(orch.dispatch_intent("嗯..随便"))
    assert "clarify" in out


def test_relay_preserves_credentials():
    # formatter relay output feeding head ack: credentials must survive as text
    relay = json.dumps(
        {"results": [{"agent": "researcher",
                      "finding": f"编号 {make_credential('R-7734')} 结论 23%"}]},
        ensure_ascii=False,
    )
    fmt = FakeFormatter(plan=[{"agent": "researcher", "goal": "x"}], relay=relay)
    orch = Orchestrator(formatter=fmt, backend_fn=backend)
    out = asyncio.run(orch.dispatch_intent("查"))
    assert extract_credentials(out) == ["R-7734"]


if __name__ == "__main__":
    fns = [v for k, v in list(globals().items()) if k.startswith("test_")]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"  [PASS] {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  [FAIL] {fn.__name__}: {e}")
    sys.exit(1 if failed else 0)
