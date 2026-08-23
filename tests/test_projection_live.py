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
    # one re-projection on gate failure = live sampling variance tolerance
    # (spec'd projector-level warm retry lands with VO-001; see docs/tickets.md)
    report = None
    proj = None
    for attempt in range(2):
        proj = await Projector().project(SCENARIO)
        report = run_gates(proj.agents_md)
        if report.passed:
            break
    assert proj.priors and proj.priors[0] == "coding"
    # live-output tolerant: the description must carry trigger examples, in any
    # of the formats the projector emits (labeled list, numbered, or 「」 quotes)
    assert proj.description, "description must be non-empty"
    assert any(m in proj.description for m in ("触发示例", "示例", "①", "「")), proj.description[:120]
    report = run_gates(proj.agents_md)
    assert report.passed, f"gate violations: {report.violations}"
    assert "# AGENTS.md" in proj.agents_md
