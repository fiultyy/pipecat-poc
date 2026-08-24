#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Unit tests for the WS3 projector and its three acceptance gates."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "realtime-provider-poc"))

from rt_orchestrator import FINAL_PREFIX  # noqa: E402  (G5: 协议常量对拍锚)
from rt_projection_gates import gate1_terminology, gate2_catastrophe, gate3_completeness, run_gates  # noqa: E402
from rt_projector import PRESET_NAMES, ROLE_TEMPLATES, Projector, ProjectionError  # noqa: E402

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
    assert result.profile_json == {"evidence": 0.9, "agent_role": "worker"}
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

# ---- VO-001: 17th dimension agent_role ----

def _payload(md: str, vector19: dict | None = None) -> str:
    """Fenced JSON output-contract payload for the fake GLM."""
    body = {"agents_md": md, "vector19": vector19 or {}, "description": "触发描述"}
    return "```json\n" + json.dumps(body, ensure_ascii=False) + "\n```"


def _mock_projector(outputs):
    """Projector whose GLM yields queued payloads (last repeats); records create() kwargs."""
    calls: list[dict] = []

    class FakeCompletions:
        @staticmethod
        async def create(**kwargs):
            calls.append(kwargs)
            out = outputs[min(len(calls) - 1, len(outputs) - 1)]
            if isinstance(out, Exception):
                raise out
            r = type("R", (), {})()
            c = type("Choice", (), {})()
            c.message = type("Msg", (), {})()
            c.message.content = out
            r.choices = [c]
            return r

    class FakeChat:
        completions = FakeCompletions()

    class FakeGLM:
        chat = FakeChat()

    proj = object.__new__(Projector)
    proj._glm = FakeGLM()
    proj._model = "fake"
    return proj, calls


def test_role_templates_cover_four_roles_worker_empty():
    assert set(ROLE_TEMPLATES) == {"liaison", "manager", "queen", "worker", "supervisor"}
    assert ROLE_TEMPLATES["worker"] == ""  # 现行零变化（回归锚）


def test_role_templates_pass_gate1_themselves():
    for role, text in ROLE_TEMPLATES.items():
        assert gate1_terminology(text) == [], (role, gate1_terminology(text))


def test_liaison_template_pins_kg06_clauses():
    t = ROLE_TEMPLATES["liaison"]
    # ① 语义→稳定指令收敛契约 ② 两阶段应答 ③ [ref:] 信封 ④ 凭证逐字回显
    assert "稳定指令" in t and "自包含" in t and "幂等可重放" in t
    assert "{status:accepted, run_id, ref, credentials}" in t
    assert FINAL_PREFIX.strip() in t and repr(FINAL_PREFIX) in t  # 协议常量逐字内嵌（G5 不漂移）
    assert "[ref:" in t
    assert "【凭证" in t and "逐字回显" in t


def test_manager_template_pins_kg06_clauses():
    t = ROLE_TEMPLATES["manager"]
    # ① 域职责 ② A/B 车道选择 ③ --dep 拆分 ④ worker_done 等待 ⑤ 异常上抛
    assert "域职责" in t
    assert "车道选择" in t and "orca" in t and "dais" in t
    assert "--dep" in t
    assert "worker_done" in t
    assert "resolve-gate" in t and "scan-wait-blocked" in t and "supervisor" in t


@pytest.mark.asyncio
async def test_project_role_liaison_product_carries_four_clauses():
    proj, _ = _mock_projector([_payload(GOOD_MD)])
    result = await proj.project("对接联络 agent", role="liaison")
    md = result.agents_md
    assert md.startswith(GOOD_MD.rstrip())  # 基底投影在前，doctrine 追加在后
    assert ROLE_TEMPLATES["liaison"] in md
    assert "幂等可重放" in md and "{status:accepted, run_id, ref, credentials}" in md
    assert FINAL_PREFIX.strip() in md and "[ref:" in md and "【凭证" in md
    assert result.profile_json["agent_role"] == "liaison"


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["liaison", "manager", "supervisor"])
async def test_project_role_products_pass_three_gates(role):
    proj, _ = _mock_projector([_payload(GOOD_MD)])
    result = await proj.project("任意场景", role=role)
    report = run_gates(result.agents_md)
    assert report.passed, report.violations
    assert result.profile_json["agent_role"] == role


@pytest.mark.asyncio
async def test_project_role_worker_is_current_pipeline_verbatim():
    proj, _ = _mock_projector([_payload(GOOD_MD, {"evidence": 0.9})])
    default = await proj.project("探索型 MVP 开发 agent")  # 缺省即 worker
    proj2, _ = _mock_projector([_payload(GOOD_MD, {"evidence": 0.9})])
    explicit = await proj2.project("探索型 MVP 开发 agent", role="worker")
    assert default.agents_md == GOOD_MD  # 产物与现行全等（回归锚）
    assert (explicit.agents_md, explicit.profile_json, explicit.priors) == (
        default.agents_md, default.profile_json, default.priors)
    assert default.profile_json["agent_role"] == "worker"


@pytest.mark.asyncio
async def test_agent_role_traced_in_profile_only():
    proj, _ = _mock_projector([_payload(GOOD_MD)])
    result = await proj.project("对接联络 agent", role="manager")
    assert result.profile_json["agent_role"] == "manager"  # 落键（第 17 维可追溯）
    assert "agent_role" not in result.agents_md  # 不进正文/术语面


@pytest.mark.asyncio
async def test_project_rejects_unknown_role_before_any_call():
    proj, calls = _mock_projector([_payload(GOOD_MD)])
    with pytest.raises(ValueError, match="unknown agent_role"):
        await proj.project("任意场景", role="hero")
    assert calls == []  # 校验先于任何 GLM 调用


@pytest.mark.asyncio
async def test_project_warm_retries_on_gate_failure():
    leaky = GOOD_MD + "\n按 19 维向量推理\n"  # gate1 必炸（术语暴露）
    proj, calls = _mock_projector([_payload(leaky), _payload(GOOD_MD)])
    result = await proj.project("探索型 MVP 开发 agent")
    assert result.agents_md == GOOD_MD
    assert len(calls) == 2  # 门失败升温重投影 1 次后过门
    assert calls[0]["temperature"] == 0.0 and calls[1]["temperature"] == 0.2


@pytest.mark.asyncio
async def test_project_raises_after_gate_retry_budget():
    broken = GOOD_MD.replace("### Output", "### 交付物")  # gate3 必炸（缺章节）
    proj, calls = _mock_projector([_payload(broken)])
    with pytest.raises(ProjectionError, match="gate violations"):
        await proj.project("探索型 MVP 开发 agent")
    assert len(calls) == 3  # 1 次首发 + ≤2 次升温重投影
