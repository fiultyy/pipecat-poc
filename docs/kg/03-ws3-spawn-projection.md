# N3 · WS3 context-files 投影 → AGENTS.md（方法级，建成 + W5.1 扩第 17 维）

> 上游：`docs/plans/voice-orchestration-head-plan.md§1.1`（v2）· 索引：[00-INDEX.md](00-INDEX.md)
> 原料**只读**：`~/文档/context-files/`；产物写入 N2 ProfileStore（不回写原料）。
> 状态：M1 完成（Projector + 三门 22/22；真 GLM 投影冒烟过三门）；W5.1 增第 17 维 `agent_role`（N6§1.1/§1.2）。

## 0. 对接总图

```
scenario（自然语言）+ role（W5.1：liaison/manager/worker/supervisor）
  └─ Projector.project()              〔loc:examples/realtime-provider-poc/rt_projector.py:159〕
       ├─ Phase A 采集（answers 可选注入）
       ├─ Phase B 投影：spawnAgentPrompt 模板 + 内核 references → GLM（zhipu.env）
       ├─ 三道质量门（gate1/2/3）      〔loc:examples/realtime-provider-poc/rt_projection_gates.py〕
       └─ 输出 Projection{agents_md, profile_json, description}
            └─ 交 N2：incubate RPC → ProfileStore.save → 孵化器（dsh/dsh-liaison/dsh-manager/omp/claude）
dsh 侧入口：向导 skill（W3.4 待建：场景+role 选型→投影→三门→选目标→incubate）+ incubate RPC
```

## 1. Projector（核心类，已落地）

`〔loc:examples/realtime-provider-poc/rt_projector.py:63→Projector〕`

```python
SOURCES = {                       # :27  六文件只读引用（check_sources() 存在性断言）
  "meta_prompt": "~/文档/context-files/spawnAgentPrompt.md",
  "kernel": .../references/behavior-space-core.md,
  "sop": .../references/projection-sop.md,
  "profiles": .../references/scenario-profiles.md,
  "template": .../assets/AGENTS-template.md,
  "space": "~/文档/context-files/BEHAVIOR-SPACE.md",   # §四:112 七场景先验 / §五:136 检查表
}

@dataclass
class Projection:                 # :55
    agents_md: str                # 纯自然语言（零框架术语）
    profile_json: dict            # {scenario, vector19, generalization, template}（W5.1 增 agent_role）
    description: str              # 触发描述（含 3 例）

class Projector:
    def __init__(self, glm: AsyncOpenAI | None = None, model: str | None = None): ...  # :66
    @staticmethod
    def check_sources() -> dict[str, bool]: ...                                         # :76
    def _load_template(self) -> str: ...                                                # :80
    def _nearest_priors(self, scenario) -> list[str]: ...   # 关键词重合度先验检索            # :88
    def build_prompt(self, scenario, answers=None) -> str: ...                          # :99
    async def _call_glm(self, prompt, temperature=0.0) -> str: ...                      # :122
    @staticmethod
    def _parse(raw: str) -> dict: ...                       # 剥 ```json 围栏               # :135
    async def phase_a(self, scenario) -> list[str]: ...                                  # :145
    async def project(self, scenario, *, answers=None) -> Projection: ...               # :159
```

**W5.1 扩展签名**（`〔new:...rt_projector.py→ROLE_TEMPLATES/project(role=)〕`，方法级见 N6§1.1）：

```python
async def project(self, scenario: str, *, answers=None, role: str = "worker") -> Projection:
    # role ∈ {liaison, manager, worker, supervisor}；worker = 现行流水线零变化；
    # liaison/manager 追加 role 模板段落；profile_json 增 "agent_role"（第 17 维，可追溯不进术语面）
```

## 2. GLM 调用契约（建成态）

`build_prompt`（:99）产出 system=模板全文（含铁律）+ user=`<scenario>` + 先验 + 澄清问答；`_call_glm`（:122）temperature=0 结构化输出；`_parse`（:135）剥围栏后 `json.loads`，失败重试 1 次（升温 0.2），再失败抛 `ProjectionError`（:50）。两条铁律原样随行（`〔doc:spawnAgentPrompt.md§1:24〕`）：**框架术语零暴露；灾难底线恒 CAN NOT**——agent 间协议产物同样过门。

## 3. 三道质量门（建成态）

`〔loc:examples/realtime-provider-poc/rt_projection_gates.py〕`

```python
CATASTROPHE_TOPIC = r"(生产|线上|敏感|密钥|凭[证据]|不可逆|删除|销毁|破坏|泄露|数据)"      # :39
CATASTROPHE_PROHIBITION = r"(MUST\s*NOT|禁止|绝不|不得|不许|不可以|严禁)"                  # :40

@dataclass
class GateReport: ...                                                                       # :44
def gate1_terminology(md) -> list[str]    # 术语 lint（FORBIDDEN_TOKENS 命中即违规；词边界控误报）
def gate2_catastrophe(md) -> bool         # 禁令词与灾难主题词共现 ≥1 行
def gate3_completeness(p, md) -> list[str] # 对照 BEHAVIOR-SPACE §五检查表（表驱动缺项列表）
def run_gates(p) -> GateReport            # 三门串联；fail → 升温重投影（≤2）→ 人审队列
```

三门同时挂在 N2 `incubate` RPC 前置（`gatesFn`，`〔loc:~/.dsh/plugins/a2a-profile-server/http-server.js:106-109〕`）——W5.1 role 模板产物照常过门。

## 4. dsh skill 入口（人/编排 agent 面）

- 向导 skill（W3.4，待建）`〔new:~/.agents/skills/incubation-wizard/SKILL.md〕`：引导 dsh agent 走"**场景选型 + role 选型** → 投影 → 三门报告随产物回显 → 选孵化目标（dsh/dsh-liaison/dsh-manager/omp/claude）→ incubate"；参数 `--scenario --name --role --targets --model`；
- head 语音路径：N2 `incubate` RPC（A2aClient.incubate :100）。

## 5. 数据流契约（与 N2 的接口）

```python
# Projector 产物 → N2 incubate RPC params（W5.1 后增 role/project/mailbox）
{ "name": "<slug>", "targets": ["dsh"], "role": "worker", "project": "<域>", "mailbox": "agent_<name>",
  "projection": { "agents_md": "...", "profile_json": {...}, "description": "..." } }
# 反向：{profile:{name, version}, receipts:[...]}；Projector 侧不落盘（单一事实源在 ProfileStore）
```

## 6. 实施与验证序列

| 步 | 交付 | 状态/验证 |
|---|---|---|
| W3.1 Projector 骨架 | SOURCES 六文件 + check_sources | ✅ `tests/test_rt_projector.py` |
| W3.2 GLM 流水线 | build_prompt/_call_glm/_parse + 重试 | ✅ 同上 + `tests/test_projection_live.py`（真 GLM 冒烟过三门） |
| W3.3 三门 | gate1/2/3 正反例 | ✅ 22/22（projector+gates 合计） |
| W3.4 向导 skill | 场景+role 选型→…→incubate | ⬜ 待建（M3+；dogfood 于 M5） |
| W5.1a 第 17 维 | ROLE_TEMPLATES + project(role=) | ⬜ N6§5（liaison/manager 模板过三门为完成判定） |
