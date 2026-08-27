# Voice-Orchestration KG · 00 索引（逻辑知识图入口）

> 版本 v2 · 2026-08-23 · 上游方案：`docs/plans/voice-orchestration-head-plan.md`（v2 架构修正版）· 渲染报告：`docs/reports/voice-orchestration-head-report.html`
> 本目录是方案的**方法级展开**：每个对接块给到函数签名 + 真实代码锚点（loc）+ 数据契约 + 调用链。
> v2 修正：dsh = 宿主；dais/orca = 双 ADE 车道；孵化池/编排 skill/主对接 agent 挂 dsh；frp 移出架构；新增 N6（W5 agent 生态）。

## 1. 指针规范

| 前缀 | 含义 | 例 |
|---|---|---|
| `〔loc:路径:行→符号〕` | **已存在**代码锚点（本轮实测验证过行号） | `〔loc:examples/realtime-provider-poc/rt_orchestrator.py:191→Orchestrator.dispatch_intent〕` |
| `〔new:路径→符号〕` | **待建**代码位（行号为落点参考） | `〔new:examples/realtime-provider-poc/rt_dsh_backend.py→DshBackend.dispatch〕` |
| `〔doc:路径§节〕` | 文档锚点 | `〔doc:~/.dsh/maestro/orch-loop.md§七步〕` |
| `〔cli:命令〕` | CLI 契约（skill/实测来源） | `〔cli:dais orchestration create-run〕` |

家目录缩写：`~` = `~`。工作区缩写：POC = `~/workspace-claw-02/pipecat-poc`。

## 2. 节点注册表（KG 文档）

| 节点 | 文档 | 内容 |
|---|---|---|
| N1 | [01-ws1-head-dsh.md](01-ws1-head-dsh.md) | **车道A · dais ADE**（已建成）：head→DshBackend→DaisLane 双到达路径与监控，方法级 |
| N2 | [02-ws2-a2a-profile.md](02-ws2-a2a-profile.md) | A2A 孵化池插件（主体建成）：六 RPC + ProfileStore + 三孵化器 + 执行桥 + W5.1/W5.2 扩展 |
| N3 | [03-ws3-spawn-projection.md](03-ws3-spawn-projection.md) | context-files 投影流水线（建成）+ 三道质量门 + 第 17 维 agent_role |
| N4 | [04-ws4-gateway.md](04-ws4-gateway.md) | 本地 WS 网关三路复用协议 + 事件汇总（frp 已移出架构） |
| N5 | [05-contracts.md](05-contracts.md) | 跨线数据契约（JSON schema）、凭据地图、测试矩阵 |
| N6 | [06-ws5-agent-ecosystem.md](06-ws5-agent-ecosystem.md) | W5 dsh agent 生态：孵化扩展 v2 + 内部通信层 + liaison 落位 + manager 群（方法级） |
| N7 | [07-laneb-orca-ade.md](07-laneb-orca-ade.md) | 车道B · orca ADE：OrcaLane 封装 + A/B 语义映射与 conformance 对拍 + DshBackend b-orca 主干（方法级，建成） |
| N8 | [08-defects-ledger.md](08-defects-ledger.md) | **编排链缺陷台账**（复盘实录）：dais 面/插件/relay 监控/live 预算/环境坑，现行对策+建议修复锚点 |
| N9 | [09-orch-hardening-plan.md](09-orch-hardening-plan.md) | **W6 编排链加固方案**（OF-001..010 票草案）：主题 A 信封 v2/租约/steer 两段/凭证 + 主题 B tickets DAG/watchd/checkpoint/longtask 绑定 + 缺陷直修 dais 守卫/live 纪律，含依赖图与 VO-012 后排期 |
| N10 | [10-pool-selection-queen.md](10-pool-selection-queen.md) | **池选型与派生 ✅已收口**（OF-012/013）：pool/spawn 选型面+策略 + queen grill 派生 + 池→dsh preset 导出；live E2E 全链过（§5）；实施域=~/tools/maestro-preset（HEAD ac9877c）+ 回流 199182d |
| N11 | [11-console-observation-p1.md](11-console-observation-p1.md) | **Console 观测面 P1 ✅topic 数据平面**：head.turn/fleet.snapshot/bridge.msg/tickets.snapshot 四源 + FileTailer（行尾/快照）+ TurnTrace（帧→phase）+ observe 会话/topics 订阅协议（P1.5 cmd 路由、P2 SPA 待开） |
| N12 | [12-voice-client-one.md](12-voice-client-one.md) | **rt-voice ONE 桌面客户端**：语音面 + 观测面双连接分面、Notebook 页签（rt_voice_app 建成） |
| N13 | [13-head-persona-receipt-slimming.md](13-head-persona-receipt-slimming.md) | **Head 人格配置化与回执精简化（子设计附录，落地形态以 N14 裁决为准）**：回执 status+summary、凭证移出模型上下文、[编排通报] 一句话终稿、read_body 第 6 工具、模糊编号协议、VOICE_RECEIPT_SLIM/VOICE_FINAL_MODE 回退开关 |
| N14 | [14-unified-callback-split.md](14-unified-callback-split.md) | **回调分流统一框架（A/B/C/D 合并裁决稿，PR1–PR5 ✅收口）**：rt_session_store（key=ref+SQLite 落盘）、orch.done 载 body 直载台账（`_store_bridge` 唯一写入面）、body.push 单 topic+body.get 帧、VOICE_FINAL_MODE=fulltext\|split 终稿分流（缺省 split，[编排通报] 纯数据 JSON+store 降级，精确 fulltext 回退）、VOICE_RECEIPT_SLIM 回执瘦身、工具面七件套（read_body ref_or_no/max_chars/from_tail+ok/miss/error 三形态+C 降级、list_bodies 索引、cancel_run ref-only、工具只读）、notify 相（head.turn phase=notify 含 pending 补投）、详情页第 6 页签+语音页迷你通知行+回合页 📣/「└已入详情」、head.compact 零 LLM 压缩（llm.py mirror_sink/on_turn_idle tap+delete_conversation_item、events.py item.delete 上游已备零改动、删非 pinned+state.snapshot 单 item、VOICE_COMPACT_CHARS 阈值、_final_inject_lock 锁序）——五面（store/观测/工具/显示/压缩）全部落地 |

## 3. 边表（对接依赖图，按修正架构）

```
局域网客户端 ──WS control/media/event──▶ N4 网关 ──pipecat 帧──▶ head 管线（工具四件套）
head ──dispatch_intent──▶ N1 DshBackend ──DaisLane create-run+send-message[ref:]──▶ dais 邮箱
dais 邮箱 ──F4/F10 两阶段（受理回执/FINAL_PREFIX 终稿）──▶ N6 liaison（orchestrator_handle 指向）
N6 liaison ──F6 router agents/send（DSHMSG 推 + 邮箱投）──▶ manager 群（N6）
N6 manager ──F7 编排 skill 选车道──▶ 车道A dais ADE（N1，live✅）/ 车道B orca ADE（N7，设计态）
N2 孵化池 ──incubate RPC──▶ 三孵化器 + 执行桥 + dsh-liaison/dsh-manager 目标（W5.1）
N3 Projector ──profile 原料（16+3 维 + agent_role）──▶ N2 incubate（H1→H5 供给线）
N3 ──内核数据──▶ context-files（只读）
N4 ──事件源──▶ N1（backend 埋点）+ dais read-worker 尾读协程
N5 ←全部（schema/凭据/测试的单一事实源）
```

## 4. 关键存量锚点速查（全 KG 复用）

### 4.1 POC 侧（全部已读验证；v2 增车道A 已建模块）

| 符号 | loc |
|---|---|
| `Orchestrator.dispatch_intent` / `.history` | `〔loc:examples/realtime-provider-poc/rt_orchestrator.py:191/:178→Orchestrator〕` |
| `FINAL_PREFIX` / `make_credential`/`extract_credentials` | `〔loc:...rt_orchestrator.py:48/:61/:66〕` |
| `HEAD_TOOLS_DOCTRINE` / `head_tools()` | `〔loc:...rt_orchestrator.py:213/:246〕` |
| **DshBackend**（lane_mode:68 / orchestrator_handle:70 / head_handle:71） | `〔loc:...rt_dsh_backend.py:45→DshBackend；dispatch:80/_fanout:100/_phase2:134/query_status:179/cancel:198〕` |
| **DaisLane**（单飞锁:47/:54） | `〔loc:...rt_dsh_lane.py:32→DaisLane；create_run:87/create_task:95/start_worker:106/send_intent:119/send_reply:134/check_messages:146/await_done:168/check_status:217/read_worker:225/fail_dispatch:254/resolve_gate:262〕` |
| **dsh_head_tools / DSH_TOOLS_DOCTRINE** | `〔loc:...rt_head_tools.py:74/:24〕` |
| **A2aClient**（send:57/get:68/cancel:76/await_done:83/incubate:100） | `〔loc:...rt_a2a_client.py:34→A2aClient〕` |
| **Projector**（SOURCES:33/build_prompt:131/_call_glm:154/project:191） | `〔loc:...rt_projector.py:95→Projector〕` |
| live V5/V6（bus_healthy:118/orchestrator_player:126/main:158） | `〔loc:...live_v5_v6_dsh.py〕` |
| `TranscriptState`（含 `take_tail:97`） | `〔loc:examples/realtime-provider-poc/rt_transcript.py:42〕` |
| `run_with_reconnect` / `ReconnectState` | `〔loc:examples/realtime-provider-poc/rt_reconnect.py:71/:39→run_with_reconnect/ReconnectState〕` |
| `create_realtime_head` / `RealtimeHeadConfig` | `〔loc:examples/realtime-provider-poc/providers.py:123/:73→create_realtime_head/RealtimeHeadConfig〕` |
| T6 管线宿主 `main` / `Tap` | `〔loc:examples/realtime-provider-poc/poc_t6_pipeline.py:100/:87〕` |
| `QwenOmniRealtimeLLMService`（方言层） | `〔loc:src/pipecat/services/qwen/realtime/llm.py:59〕` |

### 4.2 dsh / maestro 侧

| 符号 | loc |
|---|---|
| 派发一体化 `dispatch-ticket` | `〔loc:~/.dsh/maestro/bin/dispatch-ticket:2→usage 文档串〕`（读票→组契约→terminal send→ledger 落账；`ORCA_CLI_COMMAND` 默认 `orca-ide`） |
| 回调投递 `cb-send` | `〔loc:~/.dsh/maestro/bin/cb-send:2→usage cb-send <type> <from> <to> <ref> <body>〕`（信封单行 JSON `{"type","from","to","body"}`，ref 折进 body 前缀 `[ref:<ref>] `） |
| 会话孵化 `session-spawn` | `〔loc:~/.dsh/maestro/bin/session-spawn:2→usage session-spawn <preset> <node> <purpose>〕`（session.create {workspaceId, agentPreset}；DSH_PORT 3080；rename `ORCH/<node>-<code>·<preset>·<purpose>(active)`；fleet.json 原子登记） |
| **跨会话直发 `session-send`**（W5.2 推送模式底座） | `〔loc:~/.dsh/maestro/bin/session-send:4→usage session-send [--msgid <id>] <from> <to> <type> <ref> <body>；:6→type: ping|pong|done|ask|steer|nack|ack；:11→信封 v2(OF-001) DSHMSG]{from,to,type,ref,body,msgid,ts}（OG5 老键只增不改）；:21→env DSH_PORT/MAESTRO_FLEET；:32→resolve(key,fleet)〕`（loopback session.prompt 注入；steer 闸 OF-002 属主租约） |
| 账本 `ledger` | `〔loc:~/.dsh/maestro/bin/ledger:2→usage ledger node/event/review/status/project〕`（DB=`~/.dsh/maestro/ledger.db`） |
| gen3 七步闭环 | `〔doc:~/.dsh/maestro/orch-loop.md§全链七步〕` |
| 插件激活骨架 | `〔loc:~/.dsh/plugins/host-callback-bridge/index.js:205→apply(ctx)〕`、`〔loc:...index.js:109→activate(options)〕` |
| HTTP 受理面（TYPES 白名单） | `〔loc:~/.dsh/plugins/host-callback-bridge/http-intake.js:31→TYPES=['ack','done','ask','report','ping','status']〕`、`〔loc:...http-intake.js:84→validate(payload)〕` |
| 会话回合注入 API | `〔loc:~/.dsh/plugins/host-callback-bridge/loopback-sink.js:76→POST /api/session.prompt〕`（mode:'queue'，`accepted:true` 即入列） |
| dsh 系统注入位 | `〔loc:~/.dsh/APPEND_SYSTEM.md〕` + `〔loc:~/.dsh/settings.yaml→agent-presets.default=maestro / agent-default-model=glm-5.3@zhipu〕` |

### 4.3 编排器 CLI 与孵化池插件（建成）

| 面 | 契约 |
|---|---|
| dais（18 子命令，运行中） | `〔cli:dais orchestration send-message/check-messages/inject-prompt/create-run/create-task/start-worker/assign/mark-ready/fail-dispatch/promote-tasks/transition-worker/check-status/read-worker/scan-wait-blocked/answer/create-gate/resolve-gate/expire-gate〕`；总线 `〔loc:~/.local/state/dais/warp.sqlite〕`；runtime 探测 CLI-liveness 兜底（dais-runtime.json 已不落地，`〔loc:examples/realtime-provider-poc/rt_probe_m0.py→probe_dais_runtime〕`） |
| orca（v1.4.185 运行中） | 一律 `orca-cli`/`orca-ide`（**裸 `orca`=`/usr/bin/orca` GNOME 屏读器，禁用**）；车道B 命令面开工前先拉版本匹配指南（M0 探针 245ms 已验证可调起） |
| **a2a-profile-server 插件**（主体建成） | `〔loc:~/.dsh/plugins/a2a-profile-server/http-server.js:66→createHttpServer〕`（message/send:72 / tasks/get:86 / tasks/cancel:92 / incubate:102 / profiles/list:137 / profiles/get:141）；`〔loc:...profile-store.js:30→createProfileStore〕`；`〔loc:...incubators/real.js:59→incubateDsh/:90→incubateOmp/:123→incubateClaude〕`；执行桥 `〔loc:...executors/dais.js:80→createDaisExecutor〕` |

### 4.4 context-files（只读原料）

| 内容 | loc |
|---|---|
| 16+3 维空间 / 13 字段模板 / 泛化算法 / 7 场景 profile / 完备性检查表 / P1–P10 | `〔doc:~/文档/context-files/BEHAVIOR-SPACE.md§一:26 §二:74 §三:94 §四:112 §五:136 §六:148(P1:151…P10:855)〕` |
| 投影 meta-prompt（输入/纪律/内核/SOP6步/输出骨架/交付） | `〔doc:~/文档/context-files/spawnAgentPrompt.md§0:14 §1:24 §2:37 §3:90 §4:106 §5:137〕` |
| 生成器 skill（Phase A 采集 + Phase B 投影） | `〔loc:~/文档/context-files/skills/agents-md-generator/SKILL.md〕` + `assets/AGENTS-template.md` + `references/{projection-sop,behavior-space-core,scenario-profiles}.md` |

### 4.5 孵化目标格式（实测样例）

| 目标 | 格式锚点 |
|---|---|
| dsh | `session-spawn <preset> <node> <purpose>` + `APPEND_SYSTEM.md` 注入 |
| omp | `〔loc:~/.config/opencode/oh-my-opencode.json:3→agents 键（<name>={model,temperature} 条目形制）〕` + 目标项目根 `AGENTS.md` |
| claude | `〔loc:~/.claude/agents/research-analyst.md→YAML frontmatter(name/description/model/color)+正文=system prompt〕` |

## 5. 维护规则

1. 行号漂移：实现层改动后须同步本 KG 的 loc（grep 符号名刷新行号）；
2. 新增对接块：先在对应 N 文档登记（含签名+契约+调用链），再动代码；
3. `〔new:〕` 落地后即刻转为 `〔loc:〕`（实现即归档）。
