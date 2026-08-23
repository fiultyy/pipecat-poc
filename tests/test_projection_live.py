#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""W3.2 live smoke: a real GLM projection must pass all three gates.

Skipped unless GLM credentials resolve (rt_env chain); keeps the suite
green on machines without ~/.dsh/zhipu.env.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_env import glm_credentials  # noqa: E402
from rt_orchestrator import FINAL_PREFIX  # noqa: E402
from rt_projection_gates import run_gates  # noqa: E402
from rt_projector import Projector  # noqa: E402

pytestmark = pytest.mark.asyncio

SCENARIO = "探索型 MVP 开发 agent：快速验证想法、代码逻辑优先、忽略繁文缛节"


def _glm_available() -> bool:
    try:
        glm_credentials()
        return True
    except RuntimeError:
        return False


async def test_live_projection_passes_gates():
    if not _glm_available():
        pytest.skip("GLM credentials not present (rt_env chain)")
    # projector-level gate warm-retry is in place since VO-001: a returned
    # projection is guaranteed gate-clean by project() itself (≤2 warm
    # re-projections on gate failure), so no test-level re-projection here.
    proj = await Projector().project(SCENARIO)
    assert proj.priors and proj.priors[0] == "coding"
    # live-output tolerant: the description must carry trigger examples, in any
    # of the formats the projector emits (labeled list, numbered, or 「」 quotes)
    assert proj.description, "description must be non-empty"
    assert any(m in proj.description for m in ("触发示例", "示例", "①", "「")), proj.description[:120]
    report = run_gates(proj.agents_md)
    assert report.passed, f"gate violations: {report.violations}"
    assert "# AGENTS.md" in proj.agents_md
    assert proj.profile_json.get("agent_role") == "worker"


async def test_live_projection_role_smoke():
    """VO-001 live role smoke (1 case): liaison doctrine rides a real GLM call."""
    if not _glm_available():
        pytest.skip("GLM credentials not present (rt_env chain)")
    proj = await Projector().project("编排对接联络 agent：翻译上游意图为稳定指令", role="liaison")
    assert proj.profile_json.get("agent_role") == "liaison"
    md = proj.agents_md
    # 四条款随产物固化（KG 06 §1.2 liaison 行）
    assert "幂等可重放" in md and "{status:accepted, run_id, ref, credentials}" in md
    assert FINAL_PREFIX.strip() in md and "[ref:" in md and "【凭证" in md
    report = run_gates(md)
    assert report.passed, f"gate violations: {report.violations}"
