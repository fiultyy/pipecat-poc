#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Scenario → AGENTS.md projector (WS3; docs/kg/03-ws3-spawn-projection.md).

Projects a natural-language scenario onto the behavior space (context-files,
read-only) through the standardized spawn prompt, producing a pure-natural-
language AGENTS.md plus machine-readable profile metadata. Two iron rules
travel with every projection (spawnAgentPrompt.md §1): zero framework
terminology in the artifact, and a non-negotiable catastrophe floor.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from openai import AsyncOpenAI

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
                      max_retries: int = 2) -> Projection:
        """Full pipeline: prompt → GLM → parse → Projection.

        Retries once per failure with a warmer temperature (0.2) before
        raising ProjectionError.
        """
        prompt, priors = self.build_prompt(scenario, answers)
        temperature = 0.0
        last_err: Exception | None = None
        for _ in range(max_retries + 1):
            try:
                data = self._parse(await self._call_glm(prompt, temperature))
                return Projection(
                    agents_md=data["agents_md"],
                    profile_json=data.get("vector19", {}),
                    description=data.get("description", ""),
                    scenario=scenario,
                    priors=priors,
                )
            except (json.JSONDecodeError, KeyError, RuntimeError, ValueError) as e:
                # JSONDecodeError/KeyError: malformed contract; RuntimeError:
                # transport failure (openai SDK surfaces those); both retry.
                last_err = e
                temperature = 0.2
        raise ProjectionError(f"projection failed after retries: {last_err}")
