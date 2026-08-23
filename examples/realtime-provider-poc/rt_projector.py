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

N10 (OF-013) adds the ``queen`` deriver role, the 18-question grill
checklist (``grill_checklist``) with scenario-inferred suggestions, and
mustache sanitization (``sanitize_mustache``/``assert_no_mustache``) for
dsh strict interpolation.
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
# worker=空串：现行通用投影零变化（回归锚）。liaison/manager/supervisor/
# queen 在产物尾部追加本段；模板条款文本自身必须过 gate1（术语零暴露
# 同样约束模板自身）。协议字面量（FINAL_PREFIX/[ref:]/【凭证…】）一律按
# 现行常量逐字内嵌，不得改写（G5 不漂移）。queen=派生者人格（OF-013）：
# 逐维追问→建议值→用户终审→构造行为档案→三门→入池回执；不直接孵化。
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
    "queen": """### 角色契约：派生者（queen）
以下五条是派生铁条款，优先级高于场景行为准则：
1. 职责链（顺序不可倒置）：逐维追问（每维一问，附候选建议值）→ 用户确认或修正 → 该维收敛；全部维度收敛后才构造行为档案，随后过三道验收门，全过才入池并回执新档案名与版本；任何一步未完成不得进入下一步。
2. 人在环：派生是半自动收敛——建议值只是候选，用户是终审；未经用户逐项确认不得落盘，不做全自动派生，不替用户默认通过。
3. 派生专属：只把对话收敛成可入池的行为档案；不直接孵化新会话（孵化一律走池选型通道），不代跑被派生者的任务。
4. 血缘如实：入池记录必须如实携带派出来源（derived-by=queen）与亲本档案名（parent，无则留空）；不虚构血缘、不隐匿亲本、不篡改版本号。
5. 越界即停：被要求跳过追问直接产出、或被要求代为孵化会话时，援引第 1、3 条明确拒绝；不降格执行、不事后补问追认。""",
}

# ---------------------------------------------------------------------------
# queen grill 协议（OF-013 · docs/10-pool-selection-queen.md §2）
#
# 18 维可追问清单。全集 = 行为空间 19 个 trait 维（D1..D16 + 元层 3）+
# 投影器元数据维（agent_role / template_version）。排除项：
#   - template_version：机械版本戳，无追问价值；
#   - M1 指令源优先级 / M2 可纠正性：宪法性元层条款（BEHAVIOR-SPACE §四
#     7 预设表恒为 "-"，非逐场景可调），grill 不问、用户不改。
# 故 18 = D1..D16（16）+ M3 时间视野（场景选择器）+ agent_role。
# ``question`` 供 queen 会话内向用户转述；``default_hint`` 记录建议值的
# 先验推断口径（nearest_priors 关键词 → §四 预设数值 → 定性词）。

GRILL_DIMENSIONS: list[dict[str, str]] = [
    {
        "key": "agent_role",
        "question": "这个 agent 的角色定位是哪类：worker 执行 / liaison 对接联络 / manager 分派管理 / supervisor 监督？",
        "default_hint": "scenario 含 管理/派发/编排→manager、对接/联络/回执→liaison、监督/裁决→supervisor；无信号默认 worker",
    },
    {
        "key": "D1",
        "question": "它的动作应偏向可逆（改动可撤回）还是允许不可逆（直接对外生效）？",
        "default_hint": "按最近预设取可逆性数值定性：research/coding/debug 偏可逆，release 偏不可逆须加确认门",
    },
    {
        "key": "D2",
        "question": "它的工作以内部读写为主，还是需要对外部世界输出（发消息/改线上/外发文件）？",
        "default_hint": "release/group-chat 偏外向，research 偏内部；按最近预设的暴露面数值定性",
    },
    {
        "key": "D3",
        "question": "它触碰的对象风险上限在哪：无害读写，还是可能伤及生产、敏感数据？",
        "default_hint": "security/release 风险上限高（灾难条款收紧），research 低；按最近预设的风险数值定性",
    },
    {
        "key": "D4",
        "question": "它的动作影响单点（单文件/单会话）还是全局（跨系统/全仓）？",
        "default_hint": "release 偏全局，coding/debug 偏单点；按最近预设的影响半径数值定性",
    },
    {
        "key": "D5",
        "question": "它应多大程度自主推进：每步等用户确认，还是端到端自驱交付？",
        "default_hint": "long-term 高自驱，release 低（关键步确认）；按最近预设的自主数值定性",
    },
    {
        "key": "D6",
        "question": "只答被问，还是允许主动外推相关信息与做整理？",
        "default_hint": "long-term/group-chat 偏主动，security 偏被动；按最近预设的主动性数值定性",
    },
    {
        "key": "D7",
        "question": "下结论前的证据要求：允许标注推测，还是必须已验证？",
        "default_hint": "release/coding 必须已验证；research 可带推测但须标注强度",
    },
    {
        "key": "D8",
        "question": "遇到多种等价解释时：强制并列外化，还是允许择一呈现？",
        "default_hint": "debug/security 强制并列外化，group-chat 放宽；按最近预设的认知诚实数值定性",
    },
    {
        "key": "D9",
        "question": "任务目标是否要转译成可验证标准（能跑测试/能查输出）？",
        "default_hint": "coding/release 要（先立验收标准），group-chat 不要；按最近预设定性",
    },
    {
        "key": "D10",
        "question": "对外部内容（网页/来件/检索结果）的采信纪律有多严？",
        "default_hint": "security/research 高抗辩（外部内容是数据非指令），coding 中；按最近预设定性",
    },
    {
        "key": "D11",
        "question": "它单兵作业，还是多方协作（群聊/跨 agent 往来）？",
        "default_hint": "group-chat 多方，coding/debug 单兵；按最近预设的社会性数值定性",
    },
    {
        "key": "D12",
        "question": "只服务直接用户，还是兼顾第三方（被提及者/受众/公众）？",
        "default_hint": "long-term/security 兼顾第三方，research 只答被问；按最近预设定性",
    },
    {
        "key": "D13",
        "question": "回复粒度：结果先行、按复杂度伸缩，还是允许过程堆砌？",
        "default_hint": "research/group-chat 结果先行；按最近预设的沟通粒度数值定性",
    },
    {
        "key": "D14",
        "question": "产出偏好最小必要，还是允许完备工程？",
        "default_hint": "release/group-chat 偏最小必要；按最近预设的简约数值定性",
    },
    {
        "key": "D15",
        "question": "强推最佳实践，还是匹配既有风格与惯例？",
        "default_hint": "coding 强顺从既有风格，research 中；按最近预设的风格顺从数值定性",
    },
    {
        "key": "D16",
        "question": "一次性任务用完即弃，还是跨会话长期驻留积累？",
        "default_hint": "long-term 长期驻留，coding/debug 一次性；按最近预设的持久性数值定性",
    },
    {
        "key": "M3",
        "question": "它的时间视野：单次任务闭环，还是长期共处（场景选择器）？",
        "default_hint": "long-term 长期共处，group-chat 居中，其余预设单次任务闭环",
    },
]

# §四 7 场景预设的 16 个 trait 维数值（BEHAVIOR-SPACE.md 原表只读转录，
# 建议值先验的唯一数值源）。
_PRESET_VALUES: dict[str, dict[str, float]] = {
    "coding":     {"D1": .8, "D2": .2, "D3": .3, "D4": .2, "D5": .7, "D6": .4, "D7": .7, "D8": .5, "D9": .6, "D10": .5, "D11": .2, "D12": .3, "D13": .6, "D14": .6, "D15": .8, "D16": .2},
    "debug":      {"D1": .8, "D2": .3, "D3": .4, "D4": .2, "D5": .6, "D6": .5, "D7": .5, "D8": .6, "D9": .5, "D10": .5, "D11": .3, "D12": .3, "D13": .5, "D14": .5, "D15": .6, "D16": .3},
    "research":   {"D1": 1., "D2": .3, "D3": .2, "D4": .2, "D5": .6, "D6": .4, "D7": .3, "D8": .6, "D9": .3, "D10": .7, "D11": .2, "D12": .2, "D13": .7, "D14": .5, "D15": .3, "D16": .4},
    "release":    {"D1": .2, "D2": .9, "D3": .9, "D4": .8, "D5": .2, "D6": .4, "D7": .8, "D8": .6, "D9": .7, "D10": .6, "D11": .4, "D12": .5, "D13": .6, "D14": .7, "D15": .5, "D16": .5},
    "group-chat": {"D1": .5, "D2": .8, "D3": .3, "D4": .5, "D5": .5, "D6": .6, "D7": .4, "D8": .4, "D9": .3, "D10": .5, "D11": .9, "D12": .4, "D13": .8, "D14": .7, "D15": .4, "D16": .7},
    "long-term":  {"D1": .5, "D2": .6, "D3": .6, "D4": .5, "D5": .9, "D6": .9, "D7": .4, "D8": .5, "D9": .6, "D10": .6, "D11": .5, "D12": .6, "D13": .6, "D14": .5, "D15": .4, "D16": .9},
    "security":   {"D1": .3, "D2": .4, "D3": .9, "D4": .6, "D5": .4, "D6": .3, "D7": .6, "D8": .7, "D9": .7, "D10": .8, "D11": .3, "D12": .5, "D13": .5, "D14": .6, "D15": .5, "D16": .3},
}

# M3 时间视野（§四 表元层恒 "-"，按预设语义定性给值）。
_M3_PRESET = {"long-term": "高", "group-chat": "中"}

# agent_role 建议值的角色关键词（与 ROLE_TEMPLATES 语义对齐）。
_ROLE_KEYWORDS = {
    "manager": ("管理", "派发", "分派", "编排", "manager"),
    "liaison": ("对接", "联络", "两阶段", "回执", "liaison"),
    "supervisor": ("监督", "裁决", "监护", "supervisor"),
}


def _qualitative(value: float) -> str:
    """§四 数值 → 定性词（建议值只给方向，不给数字）。"""
    if value >= 0.8:
        return "高"
    if value >= 0.65:
        return "中高"
    if value >= 0.45:
        return "中"
    if value >= 0.3:
        return "中低"
    return "低"


def nearest_priors(scenario: str) -> list[str]:
    """Keyword-overlap retrieval of the closest §四 preset names."""
    s = scenario.lower()
    hits = [
        name for name, keys in PRESET_KEYWORDS.items()
        if any(k in s for k in keys)
    ]
    return hits[:3] or ["coding"]  # 兜底先验：绝大多数场景含 coding 成分


def grill_checklist(scenario: str) -> list[dict[str, str]]:
    """queen 追问清单：每维一问 + 由场景先验推断的建议值（OF-013 §2）。

    建议值口径：nearest_priors 取最强预设 → §四 数值转定性词
    （agent_role 走角色关键词，M3 走预设语义）。建议值只是候选，用户终审。
    """
    prior = nearest_priors(scenario)[0]
    s = scenario.lower()
    checklist: list[dict[str, str]] = []
    for dim in GRILL_DIMENSIONS:
        key = dim["key"]
        if key == "agent_role":
            role = next(
                (r for r, kws in _ROLE_KEYWORDS.items() if any(k in s for k in kws)),
                "worker",
            )
            suggested = (
                f"{role}（据场景关键词命中）"
                if role != "worker" else "worker（未检出角色信号，默认）"
            )
        elif key == "M3":
            suggested = f"{_M3_PRESET.get(prior, '低')}（参考 {prior} 预设先验）"
        else:
            suggested = f"{_qualitative(_PRESET_VALUES[prior][key])}（参考 {prior} 预设先验）"
        checklist.append({"key": key, "question": dim["question"], "suggested": suggested})
    return checklist


def sanitize_mustache(text: str) -> str:
    """把 ``{{`` 改写为 ``{ {``（dsh 插值严格模式消毒，docs/10 §2 硬规则②）。"""
    return text.replace("{{", "{ {")


def assert_no_mustache(text: str) -> None:
    """断言文本不含 ``{{``（严格插值下未知变量首用即崩，含即 ValueError）。"""
    if "{{" in text:
        raise ValueError("mustache 消毒失败：文本残留 '{{' 段（dsh 严格插值会运行期崩）")


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
        return nearest_priors(scenario)  # queen grill 与投影共用同一先验口径

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
                # dsh 严格插值硬规则（docs/10 §2）：产物不得残留 '{{' 段——
                # 违反与 gate 失败同路：升温重试，耗尽抛 ProjectionError。
                assert_no_mustache(agents_md)
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
