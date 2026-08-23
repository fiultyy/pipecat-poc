#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for the WS3 projector and its three acceptance gates."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_projection_gates import gate1_terminology, gate2_catastrophe, gate3_completeness, run_gates  # noqa: E402
from rt_projector import PRESET_NAMES, Projector, ProjectionError  # noqa: E402

GOOD_MD = """# AGENTS.md

> 一次探索型 MVP 开发助手。

## Agent Behavior
快速验证想法，逻辑优先。

### Mission
1. 理解需求 2. 最小实现

### How you work
- 小步快跑

### MUST
- 每个结论标注证据强度（已证/推测）

### MUST NOT
- 禁止删除生产数据或泄露敏感凭据；不可逆动作必须先确认

### Style
简洁口语。

### Output
- 代码 + 一段说明
"""


# ---- W3.1: sources & template ----

def test_six_sources_exist():
    present = Projector.check_sources()
    missing = [k for k, ok in present.items() if not ok]
    assert not missing, f"kernel sources missing: {missing}"


def test_template_contains_scenario_placeholder_and_iron_rules():
    text = Projector._load_template(object.__new__(Projector))
    assert "<scenario>" in text
    assert "铁律" in text
    assert "框架术语零暴露" in text or "一个都不许出现" in text


def test_nearest_priors():
    p = object.__new__(Projector)
    assert "research" in p._nearest_priors("帮我做一个技术调研并对比方案")
    assert "debug" in p._nearest_priors("排查线上故障修 bug")
    assert p._nearest_priors("随便一个模糊事情") == ["coding"]  # fallback prior


def test_build_prompt_substitutes_scenario():
    p = object.__new__(Projector)
    prompt, priors = p.build_prompt("写一个监控脚本的 agent")
    assert "写一个监控脚本的 agent" in prompt
    assert "<scenario>" not in prompt
    assert "输出契约" in prompt
    assert priors


def test_preset_names_match_kernel():
    assert len(PRESET_NAMES) == 7


# ---- W3.2: parse robustness ----

def test_parse_strips_fences():
    raw = '```json\n{"agents_md": "x", "vector19": {}, "description": "d"}\n```'
    data = Projector._parse(raw)
    assert data["agents_md"] == "x"


def test_parse_raises_on_garbage():
    with pytest.raises(ValueError):
        Projector._parse("这不是 JSON")


@pytest.mark.asyncio
async def test_project_retries_then_raises():
    calls = {"n": 0}

    class FakeCompletions:
        @staticmethod
        async def create(**_):
            calls["n"] += 1
            raise RuntimeError("network down")

    class FakeChat:
        completions = FakeCompletions()

    class FakeGLM:
        chat = FakeChat()

    proj = object.__new__(Projector)
    proj._glm = FakeGLM()
    proj._model = "fake"
    with pytest.raises(ProjectionError):
        await proj.project("任何场景")
    # temperature warm-up retry happened
    assert calls["n"] >= 2


@pytest.mark.asyncio
async def test_project_parses_structured_output():
    import json

    payload = {"agents_md": GOOD_MD, "vector19": {"evidence": 0.9}, "description": "触发描述"}

    class FakeCompletions:
        @staticmethod
        async def create(**_):
            class R:
                pass

            r = R()
            c = type("Choice", (), {})()
            c.message = type("Msg", (), {})()
            c.message.content = "```json\n" + json.dumps(payload, ensure_ascii=False) + "\n```"
            r.choices = [c]
            return r

    class FakeChat:
        completions = FakeCompletions()

    class FakeGLM:
        chat = FakeChat()

    proj = object.__new__(Projector)
    proj._glm = FakeGLM()
    proj._model = "fake"
    result = await proj.project("探索型 MVP 开发 agent")
    assert result.agents_md == GOOD_MD
    assert result.profile_json == {"evidence": 0.9}
    assert result.priors  # priors recorded on the projection


# ---- W3.3: gates ----

def test_gate1_passes_clean_md():
    assert gate1_terminology(GOOD_MD) == []


@pytest.mark.parametrize("leak", [
    "按 19 维向量推理", "D7 强触发", "w=0.9 的准则", "阈值 0.85", "走泛化算法",
    "落在潜空间区域", "参考 BEHAVIOR-SPACE", "场景 profile 为 coding",
])
def test_gate1_catches_framework_leaks(leak):
    assert gate1_terminology(leak) != [], f"leak not caught: {leak}"


def test_gate1_exempts_legitimate_profile_word():
    assert gate1_terminology("维护用户配置档案，定期同步") == []


def test_gate2_requires_catastrophe_floor():
    assert gate2_catastrophe(GOOD_MD) == []
    stripped = GOOD_MD.replace("禁止删除生产数据或泄露敏感凭据；不可逆动作必须先确认", "保持谨慎")
    assert gate2_catastrophe(stripped) != []


def test_gate3_requires_all_sections():
    assert gate3_completeness(GOOD_MD) == []
    broken = GOOD_MD.replace("### Output", "### 交付物")
    assert gate3_completeness(broken) != []


def test_run_gates_aggregates():
    report = run_gates(GOOD_MD)
    assert report.passed and not report.violations
    bad = run_gates("# 没有章节\n只有 D1 泛化算法\n")
    assert not bad.passed
    assert set(bad.violations) >= {"gate1_terminology", "gate3_completeness"}
