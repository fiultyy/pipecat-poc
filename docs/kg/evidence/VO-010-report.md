# VO-010 · 孵化向导 skill 壳（W3.4）报告

- 票：`docs/tickets.md` VO-010 · 规范：`docs/plans/impl-specs.md` §VO-010 · 依据：KG 02§7 W3.4 行、KG 03§4
- 交付物：`~/.agents/skills/incubation-wizard/SKILL.md`（2.8 KB）+ 附属驱动脚本 `wizard.py`（7.4 KB，可执行）
- **判定：PASS**（skill 壳 + 驱动脚本全链冒烟绿；验收①的"dsh 会话内发现性"由编排者复验，本报告 D 节给触发示例）

---

## A. skill 结构与关键段落原文

### A.1 文件结构

```
~/.agents/skills/incubation-wizard/
├── SKILL.md    # frontmatter(name/description 触发词) + 六步流程 + 决策表 + 回显契约
└── wizard.py   # 驱动脚本：参数校验→投影→三门复核回显→incubate RPC→回执回显
```

### A.2 frontmatter 原文（验收①发现性）

```yaml
---
name: incubation-wizard
description: >-
  孵化向导：引导 agent 走全流程——场景选型 + role 选型
  （liaison/manager/worker/supervisor）→ AGENTS.md 投影 → 三门报告回显 →
  选孵化目标（dsh/dsh-liaison/dsh-manager/omp/claude/dry）→ incubate →
  回执（name+version+receipts）回显。Use when 用户要求"孵化/新建/派生一个
  agent"、"生成 AGENTS.md 投影"、"跑一下孵化向导"、"incubate"、"spawn
  agent"、"建子代理/对接员/管理员/监督员"，或编排链路需要为某场景产出可孵化
  profile 时。参数 --scenario --name --role --targets --model。
---
```

### A.3 六步链路原文（验收②一步不漏，role 为必经步）

```
①场景选型 → ②role 选型 → ③投影 → ④三门报告回显 → ⑤选孵化目标 → ⑥incubate → 回执回显
（①②⑤由本 skill 决策表引导、经参数传给 wizard.py；③④⑥由 wizard.py 执行）
```

role 决策表（SKILL.md §② 原文摘录）：

| role | 用在 | 一句话判据 |
|---|---|---|
| `worker` | 执行单件任务 | 默认；无跨 agent 协调职责 |
| `liaison` | 对接联络 | 需要语义收敛、两阶段回复、[ref:] 回执、对外凭证回显 |
| `manager` | 分派管理 | 需要车道选择、--dep 派发、worker_done 回收、异常上抛 |
| `supervisor` | 监督 | 长期盯质量门/回归，只裁不停手 |

并固化 role-target 冲突拒收规则（`--role manager --targets dsh-liaison` → 客户端预检报错，与插件 `-32602` 同源）。

### A.4 回执回显契约原文（验收③）

```
== 孵化回执 ==
  name:    research-webgpu
  version: 1
  receipt: {"target":"dry","name":"research-webgpu","version":1,"note":"recorded only"}
INCUBATION-RECEIPT]{"name":"research-webgpu","version":1,"receipts":[...]}
```

SKILL.md 明令"把 name + version + receipts 原样回显给调用方，不要转述或截断"；末行 `INCUBATION-RECEIPT]` 单行 JSON 供机器解析。

### A.5 wizard.py 要点

- 参数：`--scenario --name --role --targets --model`（票面五参，`--targets` 逗号多值，缺省 `dry` 安全档）。
- 客户端预检：role 合法值、targets 合法集（dsh/dsh-liaison/dsh-manager/omp/claude/dry）、role-target 蕴含冲突（与插件 `ROLE_TARGETS` 同源，G5 不漂移）。
- 投影：复用主仓 `examples/realtime-provider-poc/rt_projector.py`（`Projector.project(scenario, role=)`，内部三门失败升温重试 ≤2）；`check_sources` 缺料即退码 1。
- 三门回显：对终产物逐门复核 `run_gates()`（gate1 术语零暴露 / gate2 灾难底线 / gate3 结构完整，PASS/FAIL+violation 逐行）。
- incubate：JSON-RPC POST `http://127.0.0.1:8790/`（env `A2A_PROFILE_PORT`/`A2A_PROFILE_TOKEN` 可覆写）；只调用不拉起服务，不可达退码 3。
- 环境适配：剥 `ALL_PROXY/all_proxy`（宿主 SOCKS 代理 + httpx 无 socksio 会 ImportError；GLM 端点国内直连）。
- 退出码：0 成功 / 1 参数缺料 / 2 投影三门失败 / 3 端点不可达 / 4 RPC 错误。

## B. 验收对照（impl-specs VO-010 ①–③）

| 验收 | 交付 | 证据 |
|---|---|---|
| ① skill 可被 dsh 会话发现并调用 | frontmatter name/description 带足触发词（孵化/incubate/AGENTS.md 投影/spawn agent/子代理/对接员/管理员/监督员），落位标准 skills 目录 `~/.agents/skills/`（与 dais-orchestration 等在用 skill 同目录同形制） | A.2；dsh 会话内复验由编排者执行，触发示例见 D |
| ② 全流程一步不漏（含 role） | SKILL.md 六步链路 ①–⑥ 显式编号，②role 选型为独立必经步+决策表；wizard.py `--role` 必传链：参数→`project(role=)`→doctrine 注入→`profile_json.agent_role` | A.3/A.5；C 中两链冒烟 `agent_role=worker`/`agent_role=liaison` 实测落键 |
| ③ 产物回执（name+version+receipts）回显 | wizard 末尾回执块 + `INCUBATION-RECEIPT]` 机器行；SKILL.md 回显契约段 | A.4；C 两链实测回执/version 递增 |

## C. 冒烟记录（2026-08-23，真 GLM + 真插件，dry 目标）

服务起停：`node -e "activate({port:8790})"` 一次性拉起 → 冒烟 → kill（不留进程）。

**C.1 worker 链**（`--scenario "调研 WebGPU…" --name wizard-smoke-vo010 --role worker --targets dry`）：

```
agents_md: 1676 chars  agent_role=worker
gate1 术语零暴露 PASS / gate2 灾难底线 PASS / gate3 结构完整 PASS
INCUBATION-RECEIPT]{"name":"wizard-smoke-vo010","version":1,"receipts":[{"target":"dry","name":"wizard-smoke-vo010","version":1,"note":"recorded only"}]}
wizard_rc=0
```

**C.2 liaison 链**（同 name 重投影，验 role 维度 + 版本化）：

```
agents_md: 1395 chars  agent_role=liaison
gate1/2/3 全 PASS
INCUBATION-RECEIPT]{"name":"wizard-smoke-vo010","version":2,"receipts":[{"target":"dry","name":"wizard-smoke-vo010","version":2,"note":"recorded only"}]}
wizard_rc=0
```

**C.3 参数面**：`--role manager --targets dsh-liaison` → 预检报错"role manager 与目标 dsh-liaison 冲突（dsh-liaison 蕴含 role=liaison）"✓；`--help` 参数面五参齐全 ✓。

**C.4 清理**：冒烟 profile 记录（`~/.dsh/profiles/incubated/wizard-smoke-vo010`）已删；8790 监听已停（`ss` 确认 clean）；无残留进程；skill 目录仅 SKILL.md + wizard.py。

## D. dsh 会话内触发示例（验收①编排者复验用）

一行触发（dsh 会话内对 agent 说）：

```
用孵化向导把"调研 WebGPU 在实时语音管线中的可行性并输出结论摘要"孵化成 worker agent，name=research-webgpu，targets=dry
```

预期：agent 命中 `incubation-wizard` skill → 走六步 → 执行 `python3 ~/.agents/skills/incubation-wizard/wizard.py --scenario … --name research-webgpu --role worker --targets dry` → 回显三门报告与 `INCUBATION-RECEIPT]` 回执。

## E. 红线自查

- 未碰 `src/pipecat/` ✓；未改插件目录任何文件（仅只读引用+一次性 activate 冒烟）✓；未 push、未 git commit ✓；无后台进程/临时文件残留（C.4）✓。

---

PASS;报告:docs/kg/evidence/VO-010-report.md;测试:skill壳+触发示例;备注:worker/liaison双链全绿,dry回执回显,8790起停即净
