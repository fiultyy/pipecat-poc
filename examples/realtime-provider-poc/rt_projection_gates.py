#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Three automated acceptance gates for projected AGENTS.md (WS3; docs/kg/
03-ws3-spawn-projection.md §3).

gate1 — zero framework terminology (spawnAgentPrompt.md §1 iron rule 1)
gate2 — non-negotiable catastrophe floor (iron rule 2)
gate3 — structural completeness against the §4 skeleton

A projection must pass all three before it may land in the ProfileStore.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# Framework tokens that must never appear in the artifact. Word-boundary
# anchored where the token is alphanumeric-ish; profile gets an exemption
# phrase list because plain language may legitimately use the word.
FORBIDDEN_PATTERNS = [
    r"19\s*维", r"16\s*维", r"三维十九", r"向量空间",
    r"\bD(?:1[0-6]|[1-9])\b", r"\bM[123]\b", r"\bP(?:10|[1-9])\b",
    r"w\s*=\s*[\d.]+", r"≥\s*0\.[78]", r"\b0\.85\b", r"\b0\.70\b",
    r"泛化算法", r"潜空间", r"投影(器|坐标系)", r"权重(计算|表)",
    r"BEHAVIOR-?SPACE", r"spawnAgentPrompt", r"准则向量", r"场景\s*profile",
]

# Natural-language phrasings that legitimately contain the word "profile"
# (e.g. "维护用户配置档案") — exempted from the bare-word check.
PROFILE_EXEMPTIONS = ("配置档案", "个人档案", "用户档案", "数据档案")

REQUIRED_SECTIONS = ("Agent Behavior", "Mission", "How you work", "MUST", "MUST NOT", "Style", "Output")

# Catastrophe floor: the artifact must carry at least one explicit
# prohibition touching irreversible/catastrophic actions.
CATASTROPHE_TOPIC = r"(生产|线上|敏感|密钥|凭[证据]|不可逆|删除|销毁|破坏|泄露|数据)"
CATASTROPHE_PROHIBITION = r"(MUST\s*NOT|禁止|绝不|不得|不许|不可以|严禁)"


@dataclass
class GateReport:
    passed: bool = True
    violations: dict[str, list[str]] = field(default_factory=dict)

    def add(self, gate: str, items: list[str]) -> None:
        if items:
            self.passed = False
            self.violations.setdefault(gate, []).extend(items)


def gate1_terminology(md: str) -> list[str]:
    """Iron rule 1 lint: return the list of forbidden-token hits."""
    hits: list[str] = []
    for pattern in FORBIDDEN_PATTERNS:
        for m in re.finditer(pattern, md, flags=re.IGNORECASE):
            hits.append(f"{pattern} → …{md[max(0, m.start() - 12):m.end() + 12]}…")
    # bare "profile": allow only inside exemption phrases
    for m in re.finditer(r"profile", md, flags=re.IGNORECASE):
        window = md[max(0, m.start() - 4):m.end() + 4]
        if not any(x in window for x in PROFILE_EXEMPTIONS):
            hits.append(f"bare 'profile' → …{window}…")
    return hits


def gate2_catastrophe(md: str) -> list[str]:
    """Iron rule 2: catastrophe floor clause must be present.

    Passes when a prohibition phrase co-occurs with a catastrophe topic
    within one line.
    """
    for line in md.splitlines():
        if re.search(CATASTROPHE_PROHIBITION, line) and re.search(CATASTROPHE_TOPIC, line):
            return []
    return ["缺少灾难底线条款（禁止语 + 不可逆/敏感主题未共现）"]


def gate3_completeness(md: str) -> list[str]:
    """§4 skeleton presence: every required section must exist as a heading."""
    missing = [s for s in REQUIRED_SECTIONS if not re.search(rf"^#+\s*{re.escape(s)}\b", md, flags=re.M)]
    return [f"缺少章节: {s}" for s in missing]


def run_gates(md: str) -> GateReport:
    """Run all three gates; empty violations everywhere = pass."""
    report = GateReport()
    report.add("gate1_terminology", gate1_terminology(md))
    report.add("gate2_catastrophe", gate2_catastrophe(md))
    report.add("gate3_completeness", gate3_completeness(md))
    return report
