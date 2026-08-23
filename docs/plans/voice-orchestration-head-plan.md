# 语音编排头（Voice Orchestration Head）方案 v2（架构修正版）

> 版本 v2 · 2026-08-23 · 按用户裁决修正：**dsh = 宿主；dais/orca = 双 ADE 双车道；孵化池/编排 skill/主对接 agent 三件挂在 dsh；frp 移出架构**
> 工作基座：`~/workspace-claw-02/pipecat-poc`（head 侧）+ `~/.dsh`（宿主）+ `~/.local/bin`（dais CLI）
> 渲染报告（权威细化）：`docs/reports/voice-orchestration-head-report.html` · 方法级展开：`docs/kg/00-INDEX.md` → `06-ws5-agent-ecosystem.md`
> v1 → v2 变更：架构图重画（双车道）；WS1 归位为车道A（已建成）；新增 W5（dsh agent 生态）；WS4 收缩为本地局域网网关（frp 删除）；里程碑重排

## 0. 目标总览（修正架构）

```
局域网语音客户端（浏览器 mic→AudioWorklet→PCM16/16k）
    │ WebSocket 三路复用 control/media/event（F1↑/F12↓）        ← WS4（本地网关，无公网穿透）
    ▼
rt_gateway（pipecat 管线宿主）
    │ pipecat 帧管线（进程内，F2）
    ▼
语音编排头（GLM 文本模式先行，Q2 定案）
    │ 工具四件套 dispatch_intent/query_status/cancel_run/remain_silent（F3）
    ▼
DshBackend（两阶段应答：受理回执 + "Agent Final Message": 终稿）
    │ DaisLane：create-run + send-message [ref:vh-…] 语义指令（F4）
    ▼
dais 邮箱（收件人语义，读即消费）
    ▼
dsh 宿主（dsh-web GUI · agent 生态）
    ├─ ① 主对接 agent dsh-liaison（W5.3）：语义指令 → 稳定指令 → 按域分发；终稿汇总回信 head（F5/F6/F9/F10）
    ├─ ② manager 群 dsh-manager × N 域（W5.4）：拆子任务 + 选车道 + 监控 + 异常上抛（F7/F8/C3）
    ├─ 内部通信层 router（W5.2）：推=session-send DSHMSG 注入 · 拉=dais 邮箱；scope=project 互通 + journal 审计
    └─ ③ A2A 孵化池插件（主体建成）：持久 profile list + incubate（三孵化器 + 执行桥）+ router 三 RPC + 向导 skill（H3/H4）
         ▲ 孵化供给线：H1 Projector 投影（16+3 维 + 第17维 agent_role）→ H2 三门 → H3 incubate → H4 session-spawn → H5 arm
    ▼ 双 ADE 车道（F7/F8）
    ├─ 车道A · dais ADE（live✅）：18 子命令 · sqlite 邮箱总线 · 任务 DAG · gate 看护
    └─ 车道B · orca ADE（待建）：OrcaLane 封装 orca-cli · worktree/终端/handoff
```

**组件定位与依赖**（细节见报告 §2.1 组件清单 / §2.2 指向图）：

| 层 | 组件 | 状态 |
|---|---|---|
| L0 接入 | rt_gateway（WsSession/EventBus 订阅/TailReader） | 待建（M4） |
| L1 语音头 | head 管线（工具四件套+doctrine）· DshBackend · DaisLane | **建成**（M3，live V5/V6） |
| L2 dsh 宿主 | 主对接 agent · manager 群 · router 通信层 · 孵化池插件 | 孵化池主体建成；其余 W5.1–W5.4 待建 |
| L3 车道 | 车道A dais ADE（live✅）· 车道B orca ADE（待建） | A 全链 PASS |
| L4 底座 | WS3 投影架构（Projector 16+3 维 + 三门 + 第17维 agent_role） | 建成（M1） |

工作线重定位：**原 WS1 = 车道A 全链（已建成）**；**原 WS2 = 孵化池插件（主体建成，向导 skill 待建）**；**原 WS3 = 投影底座（建成，W5.1 扩第 17 维）**；**新增 W5 = dsh agent 生态（对接 agent + manager 群 + 内部通信）**；**原 WS4 = 本地网关（frp 移除）**。

## 1. 现状盘点（2026-08-23 实证）

### 1.1 已具备

| 组件 | 状态 | 关键事实 |
|---|---|---|
| 车道A 全链 | ✅ live V5/V6 | DaisLane（18 子命令 async 封装 + 单飞锁 + 收件箱缓冲 + live 两行式解析）→ DshBackend（两阶段应答、A/B lane_mode、防自匹配三过滤、瞬时锁重试）→ 工具四件套（DSH_TOOLS_DOCTRINE）→ 真 GLM head + 真 dais 总线全链 PASS；车道内双到达路径（CLI 直达 vs 孵化池执行桥 `executors/dais.js`）conformance 同结果 |
| 主对接 agent 协议 | ✅ 替身验证 | live V5/V6 中 `orchestrator_player`（session_vhlive）= 对接 agent 替身：两阶段契约（受理回执 + FINAL_PREFIX 终稿）、`[ref:]` 信封、凭证逐字回显全链已验——W5.3 只换真身，协议零改动 |
| A2A 孵化池插件 | ✅ 主体 | `~/.dsh/plugins/a2a-profile-server/`：六 RPC（message/send · tasks/get · tasks/cancel · incubate · profiles/list · profiles/get）+ ProfileStore 版本化 + 三真实孵化器（dsh session-spawn / omp 备份写 / claude frontmatter）冒烟 3/3 + 执行桥 dais.js；node selftest 14/14；**创建向导 skill 壳待建（W3.4）** |
| WS3 投影架构 | ✅ M1 | Projector（BEHAVIOR-SPACE 16+3 维 + 7 场景先验 + spawnAgentPrompt 模板）+ 三门（术语零暴露 / 灾难底线恒 CAN NOT / 七章节完整）；真 GLM 投影冒烟过三门 |
| 测试基线 | ✅ 94/94 | 13 文件全绿（`.venv/bin/python -m pytest tests/test_rt_*.py tests/test_live_v5_v6.py tests/test_m0_probe.py tests/test_projection_live.py tests/test_incubators_real.py -q`） |
| 通信底座 | ✅ 已存在 | maestro `session-send`（DSHMSG 单行信封经 loopback session.prompt 注入 = 推送模式）；dais 邮箱（有界快照轮询 = 拉取模式）——W5.2 直接复用，零新概念 |
| GLM 凭据 | ✅ | `~/.dsh/zhipu.env`（head 文本模式 / Projector / 编排 agent） |

### 1.2 缺口

| 缺口 | 影响 | 对策 |
|---|---|---|
| W5.1–W5.4 全部待建 | 对接 agent/manager/router 未落位 | 见 §5 方法级工作包（KG 06） |
| 车道B orca ADE 未封装 | manager 无 B 车道可派 | OrcaLane 封装 + A/B conformance 对拍（§4） |
| 孵化池向导 skill 壳 | dsh agent 无法自助孵化 | W3.4：场景选型→投影→三门→选目标→incubate |
| rt_gateway 未建 | 局域网语音入口缺失 | M4：三路复用帧协议 + orch.* 事件汇总（§6） |
| DASHSCOPE_API_KEY 缺失 | live 语音模态 | Q2 定案：GLM 文本模式先行；Qwen 路径已探通待 key |

## 2. 系统流程（F/H/C/E 编号，与报告 §2.3–2.6 同源）

| 流 | 编号 | 逐跳摘要 |
|---|---|---|
| 主任务流 | F1–F12 | 客户端→gateway（F1）→head（F2）→dispatch_intent（F3）→dais 邮箱投对接 agent（F4）→语义收敛为稳定指令（F5）→router agents/send 分发 manager（F6）→编排 skill 选车道派发（F7）→worker_done 回 manager（F8）→域汇总回对接（F9）→终稿回 head 邮箱（F10）→_phase2 三过滤注入（F11）→播报+事件（F12） |
| 孵化流 | H1–H5 | 向导 skill→Projector（含 role 选型，H1）→三门（H2）→incubate RPC（H3）→session-spawn（H4）→arm 注入 doctrine + 通信契约、心跳后 serving（H5） |
| 取消流 | C1–C4 | 语音打断只清本地（C1）→cancel_run 杀本地 phase-2（C2）→可选 DSHMSG{steer} 通知车道（C3）→迟到终稿丢弃（C4） |
| 事件流 | E1–E3 | DshBackend→EventBus（orch.dispatch/progress/done）→gateway 订阅转 event 帧（E2）→客户端面板（E3） |

**防自匹配三过滤**（F11 权威语义，V5 实测钉死）：`[ref:]` 命中 ∧ `seq > intent_seq` ∧ `from == orchestrator_handle`。

## 3. 车道A · dais ADE（已建成，M3 归位）

原 WS1 全部产出归位为车道A，双到达路径均 conformance 验证：

| 模块 | 落点 | 状态 |
|---|---|---|
| DaisLane | `examples/realtime-provider-poc/rt_dsh_lane.py:32` | ✅ 18 子命令封装；`asyncio.Lock` 单飞（:47/:54）；`--timeout-ms` 有界快照；_inbox 缓冲 |
| DshBackend | `examples/realtime-provider-poc/rt_dsh_backend.py:45` | ✅ lane_mode a/b（:68）；两阶段 dispatch（:80）/_phase2（:134）；cancel（:198） |
| 工具四件套 | `examples/realtime-provider-poc/rt_head_tools.py:74` | ✅ DSH_TOOLS_DOCTRINE（:24）；docstring 即 schema |
| 执行桥 | `~/.dsh/plugins/a2a-profile-server/executors/dais.js:80` | ✅ message/send→真 dais 总线（create-run→send-message→有界轮询→completed artifact） |
| live 验证 | `examples/realtime-provider-poc/live_v5_v6_dsh.py` | ✅ V5 六验 / V6 五验（orchestrator_player 扮演对接 agent） |

**dais 总线 live 语义**（实测钉死，方法级见 KG 01§2 与报告 §6）：check-messages = 收件人邮箱读即消费；无 flags 空箱无限阻塞；`--wait` 只捕捉窗口期新到；`--timeout-ms N`（无 --wait）= 有界快照（轮询一律用它 + 自管 sleep）；并发调用在总线锁互饿 → 进程内单飞 + 跨进程留 sleep 间隙；daemon 偶发楔死 → 健康探测 + 单次重试 + skip。

## 4. 车道B · orca ADE（待建，M3+；方法级见 KG 07，命令面已实测探明）

- **OrcaLane**（`〔new:examples/realtime-provider-poc/rt_orca_lane.py→class OrcaLane〕`）：封装 orca-ide CLI——`worktree create --agent --prompt`（spawn≈车道A 三连+投递合体）、`terminal read/wait --for exit|tui-idle --timeout-ms`（有界等待，纪律对齐 dais）、`terminal send --text/--enter/--interrupt`（应答/干预）、`worktree ps`（汇总）；硬编码 `orca-ide`，**严禁裸 `orca`**（GNOME 屏读器，R1）；M0 探针已验证可调起（245ms）。
- **编排 skill 车道B**：`orchestration`/`orca-cli` skill 供 dsh agent 引导；与 `dais-orchestration` skill 并列为双车道入口。
- **A/B conformance 对拍**：同一意图双车道执行 → 终稿均为 `FINAL_PREFIX + 同 done body`、凭证约定一致（终稿通道不分会道，统一走 dais 邮箱链回传；方法论复用 `tests/test_rt_conformance.py`）。
- **分派策略**（随 W5.4 manager 落地）：长驻终端任务/工作树级交付 → orca；消息总线 DAG/轻量 fan-out → dais。

## 5. W5 · dsh 内 agent 生态（新增工作包，方法级见 KG 06）

原则：对接 agent 与 manager 级 agent 同样是孵化池产物（prompt 来自 WS3 投影）；dsh 从"单会话容器"升级为"可孵化、可互通、可续命的 agent 生态底座"。

| 包 | 内容 | 关键落点 |
|---|---|---|
| W5.1 孵化能力扩展 | BEHAVIOR-SPACE 增第 17 维 `agent_role ∈ {liaison,manager,worker,supervisor}`；liaison/manager 专属投影模板；新增 dsh-liaison/dsh-manager 孵化目标（incubate RPC 扩 role/project/mailbox 参数，默认句柄 `agent_<name>`）；fleet 元数据扩 role/project/mailbox/profile_version/spawned_at + 重启 reattach（会话不存在→retired 孤儿检测）；生命周期 spawn→arm→ready→serving→retire（心跳 = 邮箱 ping 或 router 探活；退休保留 profile 可复活） | `rt_projector.py` SOURCES+role 模板；`incubators/real.js` + `http-server.js` incubate |
| W5.2 内部通信层 | 信封与既有契约同构 `{"from","to","ref","type","body"}`；推 = `session-send` DSHMSG 注入（唤醒/低时延）；拉 = dais 邮箱有界快照（正文/大负载）；路由器三 RPC `agents/registry`（在册 agent）·`agents/send`（推注入或邮箱投递→ack）·`agents/inbox`（未读列表）；scope=project 内全互通、跨 project 显式授权；router 只注入固定信封格式通知（不注入任意指令）；全量路由消息进 journal 审计（复用 task-store JSONL 形制） | `a2a-profile-server/http-server.js` 三 RPC + journal |
| W5.3 主对接 agent 落位 | dsh-liaison 模板孵化（场景=编排对接，过三门）；邮箱句柄接管 head 的 `DshBackend.orchestrator_handle`——V5/V6 替身协议原样移交，**head 侧零改动**；职责链：语义指令→稳定指令（agent + 自包含 goal + deps，幂等可重放）→按域分发 manager→汇总终稿→回信 head | 孵化池 + liaison 模板 |
| W5.4 manager 群 + live V7 | 每项目域一个 dsh-manager（调研/代码/文档…按 project profile 孵化）；收稳定指令→拆子任务+依赖→选车道→派发→等 worker_done→汇总回对接；异常路径：gate 阻塞（resolve-gate）/ wait-blocked（scan-wait-blocked）/ 超时上抛 supervisor（role 预留）；**live V7**：head→对接→manager→dais 车道全链一次，凭证逐跳逐字 | manager 模板 + `live_v7`（扩 validate 序列） |

W5 验证：单测（三 RPC mock 路由 / fleet 读写与 reattach 孤儿检测 / role 模板过三门）+ live V7 + conformance（推注入 vs 邮箱拉取双投递同结果；A/B 车道对拍）。

## 6. WS4 · 本地 WS 网关（frp 已移出架构）

> 用户裁决：**frp 部分移除，不在架构里搞**（Q1 关闭）。网关收缩为本地局域网语音入口。

独立进程 `rt_gateway.py`（aiohttp + websockets，不依赖未装 extras）：

- **单 WS 连接三路复用**：`control`（auth{token}/session.start/session.end/ping）；`media`（PCM16LE/16k/mono 双向二进制）；`event`（orch.dispatch/ack/progress/gate/done/metrics 全下行）。
- **编排事件来源**：EventBus.subscribe（DshBackend 埋点）+ TailReader 协程（dais read-worker --after cursor 增量尾读）。
- **静态页**：mic 采集 + 播放 + run/ticket/worker 三级树面板 + gate 弹层。
- **断线重连**：客户端退避重连 + session_id 续接（TranscriptState.take_tail 重播种，PoC V2 已验证）。
- 方法级设计见 KG 04（帧协议/类签名/事件 schema/安全基线）。

## 7. 里程碑与顺序（按修正架构重排）

```
M0 探针矩阵          ✅ dais 177ms / orca-ide 245ms 可调起；A2A fake 9/9
M1 WS3 投影底座      ✅ Projector+三门 22/22；真 GLM 冒烟过三门
M2 孵化池插件        ✅ 主体（六 RPC + 版本化 + 三孵化器 3/3 + 执行桥）；向导 skill 壳待建
M3 车道A dais ADE    ✅ 全链 live V5/V6 + 双路径 conformance（GLM 文本模式，Q2 定案）
W5.1 孵化能力扩展    待建（第17维 role / liaison+manager 模板 / incubate 扩参 / fleet+reattach / 生命周期）
W5.2 内部通信层      待建（router 三 RPC / DSHMSG 推 + 邮箱拉 / scope + journal）
W5.3 对接 agent 落位 待建（替身协议移交，head 零改动）
W5.4 manager 群+V7   待建（全链一次 + 双模式 conformance）
M3+ 车道B orca ADE   待建（OrcaLane + A/B 对拍 + 分派策略随 W5.4）
M3+ 向导 skill 壳    待建（W3.4：场景+role 选型→投影→三门→incubate）
M4 本地网关          待建（rt_gateway 三路复用 + orch.* 汇总；局域网入口）
M5 收口              待建（全量回归 + dogfood：向导自举"监督员"→对接 agent 编排真实 A/B fan-out）
```

依赖：W5.1→W5.2→W5.3→W5.4 串行（通信层依赖孵化目标，落位依赖通信层，V7 依赖 manager）；车道B 与 W5.4 的分派策略耦合；M4 独立可并行。

## 8. 风险登记（v2 更新）

| # | 风险 | 等级 | 对策 |
|---|---|---|---|
| R1 | 裸 `orca` = GNOME 屏读器 | 高 | 全部封装层硬编码 `orca-cli`/`orca-ide`；lint 禁裸 orca |
| R2 | dais 能力面绑定 GUI 进程；daemon 偶发楔死（futex 持锁） | 中 | 健康探测+单次重试+skip；进程内单飞锁；有界快照轮询；runtime 死→`dais serve` headless 降级（仅 pull 路径） |
| R3 | ~~frps 公网服务器缺失~~ | 已关闭 | frp 移出架构（Q1 关闭）；网关本地化 |
| R4 | 语音打断 × 长任务语义混乱 | 中 | 两阶段 + cancel 权威点=杀本地 phase-2 + 迟到终稿丢弃（V6 已验） |
| R5 | 投影产物术语泄漏 / 灾难底线缺失 | 中 | 三门自动验收 + 首批人审（已建成，W5.1 role 模板照常过门） |
| R6 | 内部通信打破会话隔离的越权面 | 中 | scope=project 受控互通；router 只注入固定信封格式；全量 journal 审计 |
| R7 | 对接 agent 语义收敛不稳定（口语→稳定指令漂移） | 中 | liaison 模板固化收敛契约；稳定指令幂等可重放；V7 逐跳断言 |
| R8 | head 云凭据缺失（DashScope） | 中 | Q2 定案：GLM 文本模式先行；Qwen 路径已探通 |

## 9. 验证策略

1. **单测**（现有 94/94 基线，13 文件）：车道A 封装 / 两阶段 / 工具 schema / 投影三门 / 孵化池跨语言一致 / conformance 状态机。
2. **live 序列**：V5/V6 ✅（车道A 全链）；**V7**（W5.4：head→对接→manager→车道→终稿逐跳回传，凭证逐字）；V8 调整为局域网网关时延基线（首音延迟/RTT/事件时延）。
3. **conformance**：车道内双路径对拍 ✅；W5 增推/拉双投递对拍；车道B 建成后 A/B 对拍。
4. **dogfood**（M5）：向导自举"监督员"profile → 对接 agent 编排一次真实 A/B 双车道 fan-out。

## 10. 决策记录（开放问题收口）

| # | 问题 | 状态 |
|---|---|---|
| Q1 | frps 公网部署 | **已关闭**：frp 移出架构，网关本地化 |
| Q2 | head 语音凭据 | **已定案**：GLM 文本模式先行；Qwen realtime 待 key |
| Q3 | A2A 互通深度 | **已定案**：纯内部契约（最小子集，版本化 agent-card） |
