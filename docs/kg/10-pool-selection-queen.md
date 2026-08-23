# Voice-Orchestration KG · 10 池选型与派生规划（pool-selection & queen-derivation）

> 版本 v1 · 2026-08-24 · 来源：用户两节点指示（当日裁决级输入）
> 性质：**规划态**。票据 OF-012..OF-015（延续 09 编号），落地排 W6 余量窗口。
> 依据链：本文 → `docs/kg/02-ws2-a2a-profile.md`（池建成态）+ `06-ws5-agent-ecosystem.md`（W5）+ `09-orch-hardening-plan.md`（OF 序列）。

## 0. 两节点与底座差距

| 节点 | 用户要求 | 底座现状（2026-08-24 核实） | 差距 |
|---|---|---|---|
| **节点1** 池选型 spawn | 新 session 创建时把 agent base profile（dsh 格式）抽象出来供池选 spawn；带策略：默认 / fanout-sub / binding-mode | `incubate` 单发：scenario→投影→session-spawn（fixed preset）；无选型面、无策略参数 | ①缺"从池选已有 profile 复用"通道（现行只能新建）②缺 spawn 策略语义 ③base profile（preset.yml+cordis 格式）与池 profile（19 维+AGENTS.md）两套格式未打通 |
| **节点2** queen agent | init 一个 queen base profile：按维度与用户 grill（追问）后创建新 profile 进池 | wizard 有 scenario+role 两问；无 queen 角色、无逐维 grill 交互、无"派生入池"闭环 | ①queen=第 18 维新 role（派生者）②grill 协议=维度清单驱动的多轮问答 ③派生产物走 ProfileStore.save 版本化入池 |

**格式事实**（本轮实测钉死）：dsh base profile = `~/.dsh/.agent-presets/<name>/`（preset.yml 元信息 + agent.cordis.yml 组合面 + plugins/skills 资产）。池 profile = `<root>/<name>/`（AGENTS.md + profile.json{scenario,vector19} + meta.json 血缘）。**打通点 = 派生器**：池 profile → 生成 dsh preset 目录（cordis persona 段内嵌 AGENTS.md 投影）→ `~/.dsh/.agent-presets/` 落位 → session.create {agentPreset} 原生可用。

## 1. OF-012 · 池选型 spawn（profile resolve + strategy）

**范围**：`http-server.js` 增 `pool/spawn` RPC：
- 入参 `{ profile: <name|`*new*`>, scenario?, role?, targets?, strategy, binding? }`
- **选型面**：`profile=*new*` → 走现行投影新建；`profile=<name>` → `profiles/get` 复用（AGENTS.md+vector19 读出重注入，版本钉死 meta.version）
- **策略三型**：
  - `default`：单 session 孵化（现行语义）
  - `fanout-sub`：N 个 worker sub-session 扇出（复用 VO-007 manager 群语义：同 profile 注入 N 实例，各自 mailbox 后缀唯一化）
  - `binding-mode`：绑定已有 session（不新孵——`{binding: {sessionId}}` 注入 profile 到在飞会话 = 车道A"穿衣"路径泛化）
- 落点：pool-registry（http-server 三分支扩第四支）+ incubators/real.js 扩 spawn 策略分派
- **验收**：①复用已有 profile 孵化，AGENTS.md 与库内逐字节一致；②fanout-sub 3 实例各自 mailbox 唯一+fleet 登记；③binding-mode 注入在飞 session 后 doctrine 生效（回合首动作可验）；④`*new*` 与现行 incubate 输出同构（回归）；⑤strategy 缺省=default 兼容
- 量：中 · 依赖：无（VO-007/012 已铺好 fanout 与穿衣先例）· 文件域：插件 http-server.js + incubators/real.js

## 2. OF-013 · queen base profile + grill 派生协议

**范围**：
- **queen role 落位**：rt_projector ROLE_TEMPLATES 增 `queen`（第 17 维扩值：派生者人格=维度追问→profile 构造→三门→入池）；wizard --role 增 queen 选项
- **grill 协议**（queen 的 doctrine 核心）：18 维清单（19 维中 grill 适用的维度——排除 template_version 等机械维）驱动多轮问答：每维 1 问+候选建议值（可从 scenario 预投影推断默认），用户确认/修正 → 全维收敛后构造 profile
- **派生入池闭环**：queen 收敛结果 → ProfileStore.save（lineage 记 `derived-by: queen,<parent-profile>?`）→ 三门 → 回执含新 profile 名+版本
- **dsh 格式导出**（打通节点1 格式差）：`pool/export` RPC——池 profile → 生成 `~/.dsh/.agent-presets/<name>/`（preset.yml 从 profile.json 生成 + agent.cordis.yml persona 段嵌 AGENTS.md 全文 + 软链 plugins/skills 资产自 maestro 模板）→ dsh 原生 agentPreset 即刻可用
- **验收**：①queen 孵化真跑：grill ≥3 维问答 → 派生 profile 入池（三门过）；②lineage 血缘可溯；③export 后 `session.create {agentPreset:<derived>}` 原生起会话；④revalidate 对派生物有效；⑤queen 不越权（只能派生 profile，不能直接 spawn——spawn 走 OF-012）
- **GUI 通路实证**（2025 实测，loopback `agentPreset.list`）：新会话屏原生有 preset 选择 chip（`AgentPresetSeat`，workspace 选择器旁，菜单项=name+description）；roster 每次 list 调用实时扫 `~/.dsh/.agent-presets/`——落一个目录**即刻出现**（trust=user，无需重启/重载）；只写 preset.yml 缺 agent.cordis.yml 会标 `broken`（"directory still occupies the id"）→ export 必须两文件齐写。即 export 一步就把派生 profile 送进 GUI 原生列表，零 UI 改动
- **dsh 格式契约核对**（源码级，`dsh-agent-presets`/`dsh-system-prompt`/`dsh-agent-instructions`/`dsh-persona` 四包 + maestro 实例，2025-08-24）：
  - **prompt 分层**（关键认知）：①SystemPrompt 注册表 = sections（有序静态）+ contexts（动态快照）+ variables + tool providers，scoped 层遮蔽全局；基础段仅 `harness:identity`(order -100) 与 `deployment:persona`(order 0，base 发行版默认空)。②**persona row（`@deepseek-ai/dsh-persona`）是 preset 改身份的唯一机制**——scope-only，config `{text(必填), complete?, includeRuntimeContext?}`；`complete:true`=独占段接管全 prompt（仅允许一个）。③**AGENTS.md 是另一通道**（`dsh-agent-instructions`）：workspace 链 `AGENTS.md`/`CLAUDE.md`+`.local` 叠层 + 用户全局 `~/.dsh/AGENTS.md`，以持久上下文基线消息注入（"Instructions from: <path>"），maxBytes 65536，fs 触碰实时对账；**不入 preset**。→ N10 的"persona 段嵌 AGENTS.md 全文"架构上正确：派生身份随 preset 走（任意 cwd 生效、遮蔽部署 persona、无字节预算），写 workspace AGENTS.md 反而是错误通道（作用域错+预算截断+与仓库自身 AGENTS.md 冲突）
  - **mount 契约**：组合=插件行列表（Include 子树挂 agent scope，会话级生命周期）；行不可用（等服务等）即 mount 失败；**任何行向 ROOT realm 发服务即拒绝**（必须 `isolate` realm 或移宿主组合）——池侧新增插件行若引用须守此则；裸包名从 harness base 解析（生成 preset 可直接引 `@deepseek-ai/dsh-*`）；preset 子树永不回写
  - **export 硬规则**（生成器必守，均会静默/运行期炸）：①**id slug**：目录名必须 `^[a-z0-9][a-z0-9-]*$`，不符则 discovery 静默跳过（中文/下划线/大写全灭）→ queen 命名策略 `queen-<scenario>-<slug>`；②**`{{` 消毒**：插值是严格模式——文本中任何 `{{x}}` 形段（x 不匹配已注册变量 `model`/`cwd` 等）在首次 model step 即 throw（未知/畸形变量皆炸，代码围栏不豁免）→ 投影器生成时断言输出无 `{{`，grill 用户答案含 `{{` 则改写为 `{ {`；③**生成行只用纯 YAML**（loader 方言含 `!!js` 函数标签是超集）——平台条件行（bash/pwsh `disabled: !!js ...`）从 maestro **原文整段拷贝**，不重排不重序列化；④preset.yml 仅显文本 `{name, description, order?}`（order 控制 picker 排序），坏损只降级显示不阻断 mount
- 量：中-大 · 依赖：OF-012（选型面先行，queen 产物经池选型消费）· 文件域：rt_projector.py + wizard + 插件 export RPC

## 3. 排期与依赖

```
OF-012（池选型 spawn）──→ OF-013（queen+grill+export）
   （先行：选型/策略面）        （消费面：派生入池+dsh 格式打通）
```

两票均在 W6 余量窗口（OF-004/009 之后或并行——文件域不相交：OF-012 动插件 JS、OF-013 动 python 投影器+插件 RPC，与持有票零冲突）。

## 4. 非目标（裁决）

- 不做 queen 自动派生（必须用户 grill 在环——半自动收敛，人是终审）
- 不做池 profile 反向同步进 dsh preset（单向：池→dsh 导出；dsh preset 改动不回流池）
- 不做多 queen 协商派生（单 queen 会话内闭环）
