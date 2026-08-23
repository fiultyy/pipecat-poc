#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Scenario → AGENTS.md projector (WS3; docs/kg/03-ws3-spawn-projection.md).

Projects a natural-language scenario onto the behavior space (context-files,
read-only) through the standardized spawn prompt, producing a pure-natural-
language AGENTS.md plus machine-readable profile metadata. Two iron rules
travel with every projection (spawnAgentPrompt.md §1): zero framework
terminology in the artifact, and a non-negotiable catastrophe floor.

VO-001 adds the 17th dimension ``agent_role`` (KG 06 §1.1–1.2): role
doctrine clauses are pinned verbatim via ROLE_TEMPLATES so every role
product passes the three gates unchanged.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from openai import AsyncOpenAI

from rt_orchestrator import FINAL_PREFIX  # protocol constant, embedded verbatim (G5: no drift)
from rt_projection_gates import run_gates
from rt_env import glm_credentials

CF = Path("~/文档/context-files").expanduser()

SOURCES = {
    "meta_prompt": CF / "spawnAgentPrompt.md",
    "kernel": CF / "skills/agents-md-generator/references/behavior-space-core.md",
    "sop": CF / "skills/agents-md-generator/references/projection-sop.md",
    "profiles": CF / "skills/agents-md-generator/references/scenario-profiles.md",
    "template": CF / "skills/agents-md-generator/assets/AGENTS-template.md",
    "space": CF / "BEHAVIOR-SPACE.md",
}

# 七场景预设名（BEHAVIOR-SPACE.md §四 表头），供先验检索匹配。
PRESET_NAMES = ("coding", "debug", "research", "release", "group-chat", "long-term", "security")

PRESET_KEYWORDS = {
    "coding": ("代码", "编码", "实现", "编程", "开发", "coding", "feature"),
    "debug": ("调试", "排障", "修 bug", "bug", "debug", "故障"),
    "research": ("调研", "研究", "检索", "research", "文献", "对比"),
    "release": ("发布", "上线", "release", "部署", "运维", "变更"),
    "group-chat": ("群聊", "协作", "团队", "社交", "群", "chat", "沟通"),
    "long-term": ("长期", "驻留", "自主", "long-term", "陪伴", "常驻"),
    "security": ("安全", "审计", "风控", "security", "合规", "渗透"),
}

# 第 17 维 agent_role 的 doctrine 模板（KG 06 §1.2 表条款逐条固化）。
# worker=空串：现行通用投影零变化（回归锚）。liaison/manager/supervisor
# 在产物尾部追加本段；模板条款文本自身必须过 gate1（术语零暴露同样
# 约束模板自身）。协议字面量（FINAL_PREFIX/[ref:]/【凭证…】）一律按
# 现行常量逐字内嵌，不得改写（G5 不漂移）。
ROLE_TEMPLATES = {
    "liaison": f"""### 角色契约：对接联络（liaison）
以下四条是对外沟通铁条款，优先级高于场景行为准则：
1. 语义收敛：把上游口语化意图收敛成稳定指令——自包含（离开对话历史仍可独立执行）、指代全部展开（不留"它/上面/刚才"类悬空指代）、幂等可重放（同一意图收敛结果恒定）。
2. 两阶段应答：
   - 第一阶段·受理回执：收到指令即刻回执，形如 {{status:accepted, run_id, ref, credentials}}，其中 ref 与 credentials 逐字取自来件；
   - 第二阶段·终稿：汇总完成后回终稿，回复 body 必须以前缀 {FINAL_PREFIX!r} 开头（逐字符原样照抄，含结尾换行），前缀之后接终稿正文。
3. 信封规则：一切对外消息的 body 前加 [ref:<来件ref>] 前缀；ref 逐跳透传，不改写、不丢弃。
4. 凭证纪律：来件中的【凭证…】标记必须逐字回显——不改、不丢、不加。""",
    "manager": """### 角色契约：域管理（manager）
以下五条是编排铁条款，优先级高于场景行为准则：
1. 域职责边界：只受理本域内的稳定指令；域外需求原样上抛给来件方，不越界受理、不私下扩权。
2. 车道选择：终端/工作树类任务（需真实终端与代码检出）走 orca 车道；消息 DAG/轻量 fan-out 类任务（纯消息往返即可完成）走 dais 车道。
3. 拆分与依赖：把稳定指令拆成子任务清单，每个子任务携带 --dep 依赖表；依赖未收齐的子任务不得派发。
4. 完成等待：以 worker_done 块匹配等待各子任务完成（免轮询）；全部依赖收齐后再做域汇总回信。
5. 异常上抛：gate 阻塞→调 resolve-gate 处置；疑似卡死→调 scan-wait-blocked 排查；仍超时→原样上抛 supervisor，不静默吞掉、不无限重试。""",
    "worker": "",  # 现行通用投影，零变化
    "supervisor": """### 角色契约：监护（supervisor，预留）
你是监护员：只做三件事——受理下级上抛的超时与异常、决定升级或降级处置、留痕回告；不代跑下级任务，不改写下级指令。""",
}


class ProjectionError(RuntimeError):
    """Raised when the model output cannot be parsed or passes no gates."""


@dataclass
class Projection:
    agents_md: str
    profile_json: dict = field(default_factory=dict)
    description: str = ""
    scenario: str = ""
    priors: list[str] = field(default_factory=list)


class Projector:
    """Scenario → AGENTS.md projector over the context-files kernel."""

    def __init__(self, glm: AsyncOpenAI | None = None, model: str | None = None):
        if glm is None:
            key, base_url = glm_credentials()
            glm = AsyncOpenAI(api_key=key, base_url=base_url)
        self._glm = glm
        self._model = model or __import__("os").environ.get("GLM_PROJECTOR_MODEL", "glm-5-turbo")

    # ---- sources ----

    @staticmethod
    def check_sources() -> dict[str, bool]:
        """Existence map of the six read-only kernel sources (W3.1 gate)."""
        return {k: p.exists() for k, p in SOURCES.items()}

    def _load_template(self) -> str:
        """Load the meta prompt and keep its scenario placeholder verbatim.

        The caller substitutes ``<scenario>`` via ``build_prompt`` so the
        template text itself stays the single source of truth.
        """
        return SOURCES["meta_prompt"].read_text(encoding="utf-8")

    def _nearest_priors(self, scenario: str) -> list[str]:
        """Keyword-overlap retrieval of the closest §四 preset names."""
        s = scenario.lower()
        hits = [
            name for name, keys in PRESET_KEYWORDS.items()
            if any(k in s for k in keys)
        ]
        return hits[:3] or ["coding"]  # 兜底先验：绝大多数场景含 coding 成分

    # ---- prompt assembly ----

    def build_prompt(self, scenario: str, answers: list[str] | None = None) -> str:
        """Meta prompt + <scenario> substitution + priors + clarifications.

        Output contract (JSON): {agents_md, vector19, description}.
        """
        template = self._load_template()
        filled = template.replace("<scenario>", scenario.strip() or "<空场景>")
        priors = self._nearest_priors(scenario)
        extras = [
            "先验参考（内部推理用，不得泄漏进产物）：",
            *(f"- {name}（BEHAVIOR-SPACE §四 预设）" for name in priors),
        ]
        if answers:
            extras.append("澄清问答：" + json.dumps(answers, ensure_ascii=False))
        extras.append(
            '输出契约（严格遵守）：只输出一个 JSON 对象 '
            '{"agents_md": "<AGENTS.md 全文>", "vector19": {<维度名: 0-1>}, '
            '"description": "<一句话触发描述+3个触发示例>"}，不要输出其它文本。'
        )
        return filled + "\n\n---\n" + "\n".join(extras), priors

    # ---- GLM call ----

    async def _call_glm(self, prompt: str, temperature: float = 0.0) -> str:
        resp = await self._glm.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": "你是 AGENTS.md 投影器，严格遵守 spawnAgentPrompt 两条铁律。"},
                {"role": "user", "content": prompt},
            ],
            temperature=temperature,
            max_tokens=8192,
        )
        return (resp.choices[0].message.content or "").strip()

    @staticmethod
    def _parse(raw: str) -> dict:
        """Strip code fences and parse the JSON output contract."""
        text = raw.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
            text = re.sub(r"\n?```\s*$", "", text)
        return json.loads(text)

    # ---- pipeline ----

    async def phase_a(self, scenario: str) -> list[str]:
        """Clarifying questions; empty when the scenario is self-contained.

        Heuristic: scenarios shorter than 12 chars or missing a verb-ish
        action word are treated as under-specified.
        """
        s = scenario.strip()
        if len(s) < 12 or not any(k in s for k in ("做", "写", "查", "调", "管", "析", "build", "make")):
            return [
                "这个 agent 主要交付什么类型的产物？",
                "它在什么环境里工作（项目/终端/群聊）？",
            ]
        return []

    async def project(self, scenario: str, *, answers: list[str] | None = None,
                      role: str = "worker", max_retries: int = 2) -> Projection:
        """Full pipeline: prompt → GLM → parse → role doctrine → gates → Projection.

        ``role`` is the 17th dimension (KG 06 §1.1): worker keeps the current
        pipeline verbatim; liaison/manager/supervisor append their ROLE_TEMPLATES
        doctrine to the artifact. All roles must pass the three gates before
        returning — gate failures warm-retry (temperature 0.2, ≤ max_retries)
        exactly like parse/transport failures, then raise ProjectionError.
        """
        if role not in ROLE_TEMPLATES:
            raise ValueError(
                f"unknown agent_role {role!r}; expected one of {sorted(ROLE_TEMPLATES)}"
            )
        prompt, priors = self.build_prompt(scenario, answers)
        temperature = 0.0
        last_err: Exception | None = None
        for _ in range(max_retries + 1):
            try:
                data = self._parse(await self._call_glm(prompt, temperature))
                agents_md = data["agents_md"]
                doctrine = ROLE_TEMPLATES[role]
                if doctrine:
                    agents_md = agents_md.rstrip() + "\n\n" + doctrine
                report = run_gates(agents_md)
                if not report.passed:
                    raise ValueError(f"gate violations: {report.violations}")
                profile_json = dict(data.get("vector19", {}))
                profile_json["agent_role"] = role  # traceable, never in the artifact
                return Projection(
                    agents_md=agents_md,
                    profile_json=profile_json,
                    description=data.get("description", ""),
                    scenario=scenario,
                    priors=priors,
                )
            except (json.JSONDecodeError, KeyError, RuntimeError, ValueError) as e:
                # JSONDecodeError/KeyError: malformed contract; RuntimeError:
                # transport failure (openai SDK); ValueError: gate violations
                # (raised above) — all warm-retry once per failure.
                last_err = e
                temperature = 0.2
        raise ProjectionError(f"projection failed after retries: {last_err}")
