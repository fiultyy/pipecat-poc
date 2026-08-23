# 实施规范聚合（impl-specs）· W5 + 车道B + 向导 + 网关 + 收口

> 版本 v1 · 2026-08-23 · 由 plan v2（§3–§7）+ KG 04/06/07 + 报告 §4 聚合而成，是**实施票的单一规范源**。
> 票板：`docs/tickets.md`（VO-xxx，v4 票制）。每票"验证目标"逐条引用本文件对应节的验收编号。
> 依据链：票 → 本文件 §包 → KG 方法级（loc/new 锚点）→ plan/报告（架构裁决）。
> 顺序：VO-001→012 串行为主；VO-008/010/011 可与 W5 线并行（见依赖列）。

## 全局验证门（本仓 lane）

- **G1 回归绿**：全量 pytest 基线（13 文件 94 收集）不引入新失败；live 用例必须带宿主掉线 skip（`_dais_plane_up` 模式）。
- **G2 离线优先**：新模块先 mock 单测；live 冒烟独立用例独立标记，不与离线断言混编。
- **G3 范围分治**：改动仅落 `examples/realtime-provider-poc/`、`tests/`、`docs/`、`~/.dsh/plugins/a2a-profile-server/`（对应包）；不碰 pipecat 框架 `src/`、不改 maestro bin/ 既有工具。
- **G4 文档同步**：实现落地的 `〔new:〕` 即刻转 `〔loc:〕`（KG 维护规则 3）；账本（`docs/kg/evidence/ledger-carryover-round6.md`）每包完成即更新；done body 固定格式。
- **G5 协议不漂移**：两阶段契约/`[ref:]` 信封/三过滤/FINAL_PREFIX/凭证逐字——任何包不得改动既有协议常量与语义（对拍测试即守卫）。

---

## VO-001 · Projector 第 17 维 agent_role（W5.1a）

| 项 | 内容 |
|---|---|
| 依据 | KG 06§1.1–1.2；KG 03§1；plan §5 W5.1 |
| 范围 | `rt_projector.py`：增 `ROLE_TEMPLATES`（liaison/manager/worker/supervisor 四键，worker=现行零变化）；`project()` 增 `role="worker"` 参数；`Projection.profile_json` 增 `"agent_role"` 字段。**不改**三门逻辑（role 模板产物照常过门） |
| 验收 | ① `project(role="liaison")` 产物含收敛契约/两阶段/`[ref:]`/凭证回显四条款（KG 06§1.2 liaison 行）；② `role="manager"` 含车道选择/`--dep` 拆分/worker_done 等待/异常上抛四条款；③ `role="worker"` 输出与现行签名完全一致（回归锚）；④ role 产物过三门（gate1 术语零暴露对模板自身条款同样生效）；⑤ `profile_json.agent_role` 正确落键且不进 agents_md 正文 |
| 验证形式 | 自动 = `tests/test_rt_projector.py` 扩 role 参数化用例（mock GLM）；live = `tests/test_projection_live.py` 增 role 冒烟 1 例 |
| 量/依赖 | 小 · 无（**纯离线，可立即开工**） |

## VO-002 · incubate 扩参 + dsh-liaison/dsh-manager 孵化目标（W5.1b）

| 项 | 内容 |
|---|---|
| 依据 | KG 06§1.3–1.4；KG 02§3/§5.1 |
| 范围 | 插件 `http-server.js` incubate 分支：params 增 `role/project/mailbox`（向后兼容缺省）；targets 合法值增 `dsh-liaison`/`dsh-manager`。`incubators/real.js` `incubateDsh` ctx 增 role 三元组，注入体 = role doctrine 段 + agentsMd，返回 `target:'dsh-'+role` + mailbox + fleet 扩展登记项 |
| 验收 | ① 缺参调用行为与现行完全一致（回归）；② `targets:["dsh-liaison"], role:"liaison", mailbox:"agent_liaison"` → receipts 含 `sessionId/code/mailbox/role/project`；③ role 非法值 → `-32602`；④ 三门前置照常拦截（gate fail → `-32000`）；⑤ node selftest 扩参用例绿 |
| 验证形式 | 自动 = selftest.mjs 扩（mock session-spawn）；live = 真 session-spawn 冒烟 1 例（宿主在线时） |
| 量/依赖 | 中 · VO-001（模板来源） |

## VO-003 · fleet 扩展 + registry/reattach + 生命周期（W5.1c）

| 项 | 内容 |
|---|---|
| 依据 | KG 06§1.5–1.6（含唤醒模型修正） |
| 范围 | 新文件 `~/.dsh/plugins/a2a-profile-server/registry.js`：fleet 登记项扩 role/project/mailbox/profile_version/spawned_at；`reattach()`（loopback session.list 比对，孤儿→retired+journal）；生命周期状态机 spawn→arm→ready→serving→retired（探活一律 router 侧驱动；**dsh 会话非常驻轮询者**——推唤醒 + 回合首拉取） |
| 验收 | ① fleet 读写含扩展五键（temp 副本，原子性）；② reattach：会话在→reattached；会话失→retired + journal `{op:"orphan"}`；③ 状态机全迁移可达且非法迁移拒绝；④ 心跳=router `session.list` 探活，无 agent 自发轮询假设 |
| 验证形式 | 自动 = `tests/test_rt_fleet_registry.py`（new，mock loopback/fleet） |
| 量/依赖 | 中 · VO-002（登记项形制） |

## VO-004 · router 三 RPC + scope + journal（W5.2a）

| 项 | 内容 |
|---|---|
| 依据 | KG 06§2.4–2.5；N5§4（DSHMSG 信封） |
| 范围 | 插件 `http-server.js` 增三分支：`agents/registry`（在册+状态+心跳）/ `agents/send`（scope 校验→轻载 session-send DSHMSG 推注入、重载 dais 邮箱投→ack）/ `agents/inbox`（只读快照）；`router-journal.jsonl` 全量审计（task-store JSONL 形制）。**只注入固定信封格式，不注入任意指令** |
| 验收 | ① 三 RPC 契约测试绿（签名/schema）；② 同 project 互通、跨 project 拒绝（`-32000 scope`）；③ 推/投双模式按载荷大小分流且 journal 记 `delivered:"push|mailbox"`；④ inbox 只读不消费；⑤ 全量路由消息 journal 可回放 |
| 验证形式 | 自动 = `tests/test_rt_router.py`（new，mock session-send/dais） |
| 量/依赖 | 中 · VO-003（registry 状态源） |

## VO-005 · 推/拉双投递 conformance（W5.2b）

| 项 | 内容 |
|---|---|
| 依据 | KG 06§2.2–2.3；N1§2 总线语义 |
| 范围 | 扩 `tests/test_rt_conformance.py`：同一消息 DSHMSG 推注入 vs dais 邮箱投递，收方视角同结果（信封可解析、ref 不丢、凭证逐字）；跨进程留 sleep 间隙（总线锁纪律） |
| 验收 | ① 双模式收方解析出同一 `{from,to,ref,type,body}`；② 推注入回合首行机器可解析；③ 邮箱路径读即消费语义确认；④ 宿主掉线确定性 skip |
| 验证形式 | live（真 session-send + 真 dais 邮箱；离线 mock 部分并入 VO-004） |
| 量/依赖 | 小 · VO-004；**需 dais 编排面在线** |

## VO-006 · liaison 落位（W5.3，替身移交）

| 项 | 内容 |
|---|---|
| 依据 | KG 06§3（含 3.1 零改动对照表） |
| 范围 | 真孵化 dsh-liaison（场景=编排对接，mailbox=`agent_liaison`）；head 侧 `DshBackend.orchestrator_handle` 指向它；**head 代码 diff=0**（对照表四项逐条核） |
| 验收 | ① head→liaison F4 投递、F10 终稿回信全链一次（两阶段时序正确）；② 受理回执/终稿/FINAL_PREFIX/凭证与替身 V5 输出同构；③ head 侧 git diff 仅配置（handle 值），无逻辑改动；④ liaison 回合首动作=邮箱快照排空（doctrine 固化） |
| 验证形式 | live（扩 live_v5_v6 场景：替身换真身） |
| 量/依赖 | 中 · VO-005；**需 dais 在线 + 会话孵化** |

## VO-007 · manager 群 + live V7（W5.4）

| 项 | 内容 |
|---|---|
| 依据 | KG 06§4（V7 六步场景）；plan §5 W5.4 |
| 范围 | dsh-manager 孵化（每域一个）；head→liaison→manager→dais 车道→逐跳回传全链；异常路径（gate/wait-blocked/超时上抛）入 doctrine |
| 验收 | V7 六步全过：①真孵化两 agent（fleet 登记）②F4→liaison 收 ③F6 router 分发 manager（推唤醒+邮箱正文）④F7 车道派发+worker_done ⑤F9/F10 终稿逐跳回传 ⑥断言全链 ref 不丢+凭证逐字+两阶段时序 |
| 验证形式 | live = `tests/test_live_v7.py`（new） |
| 量/依赖 | 中-大 · VO-006；**需 dais 在线** |

## VO-008 · OrcaLane 封装 + 单测（车道B B.1–B.2）

| 项 | 内容 |
|---|---|
| 依据 | KG 07§1–2（命令面已实测探明） |
| 范围 | `rt_orca_lane.py`：status/spawn_worktree/terminal_list/read/wait(--for exit\|tui-idle, 必带 timeout)/send/interrupt/stop/worktree_ps；全 `--json`；BIN 硬编码 `orca-ide`（R1：禁裸 orca）。开工首步 `orca-ide skills get orca-cli` 钉死 read 增量游标语义 |
| 验收 | ① 语义映射表（KG 07§2）全覆盖；② 全方法有界超时无裸等；③ `--json` 解析 + OrcaLaneError 形制对齐 DaisLaneError；④ 单测 mock CLI 输出全绿 |
| 验证形式 | 自动 = `tests/test_rt_orca_lane.py`（new）；探针 = skills get 记录入 evidence |
| 量/依赖 | 中 · 无（**可与 W5 线并行**） |

## VO-009 · 车道B live 冒烟 + A/B 对拍（B.3–B.4）

| 项 | 内容 |
|---|---|
| 依据 | KG 07§2 对拍判定 |
| 范围 | 真 spawn 1 worktree（codex/claude）→ wait → read 全程有界；A/B conformance：同一意图双车道 → 终稿均 `FINAL_PREFIX+同 body`、凭证一致（终稿通道不分会道，统一邮箱链回传） |
| 验收 | ① live 冒烟全程无裸等无死等；② A/B 对拍同终稿；③ 掉线/不可用确定性 skip |
| 验证形式 | live（扩 test_rt_conformance lane 工厂参数化） |
| 量/依赖 | 中 · VO-008；orca 宿主在线 |

## VO-010 · 孵化向导 skill 壳（W3.4）

| 项 | 内容 |
|---|---|
| 依据 | KG 02§7 W3.4 行；KG 03§4 |
| 范围 | `~/.agents/skills/incubation-wizard/SKILL.md`：引导 dsh agent 走"场景选型+role 选型→投影→三门报告回显→选孵化目标（dsh/dsh-liaison/dsh-manager/omp/claude）→incubate"；参数 `--scenario --name --role --targets --model` |
| 验收 | ① skill 可被 dsh 会话发现并调用；② 全流程一步不漏（含 role）；③ 产物回执（name+version+receipts）回显给调用 agent |
| 验证形式 | 冒烟 = dsh 会话内走一遍（M5 dogfood 前置） |
| 量/依赖 | 小 · VO-002（role/incubate 参数就绪）；**可并行** |

## VO-011 · rt_gateway 本地网关（M4，W4.1–W4.3）

| 项 | 内容 |
|---|---|
| 依据 | KG 04§1–4 |
| 范围 | `rt_gateway.py`：VoiceGateway/WsSession（control/media/event 三路复用）+ TailReader + 静态页（mic/播放/三级树面板/gate 弹层）；EventBus 订阅转 event 帧；断线重连续接（take_tail 重播种）；局域网安全基线（token/并发≤2/码率上限） |
| 验收 | ① 帧协议单测绿（握手/背压/慢客户端合并）；② 本机浏览器 e2e：mic→head→下行音频；③ dispatch/progress/done 全序到达；④ 局域网另一设备连通 + orch.metrics 三指标（首音/RTT/事件时延） |
| 验证形式 | 自动 = `tests/test_rt_gateway.py`（new）；本机 e2e + 局域网联调 |
| 量/依赖 | 中-大 · 无（**独立可并行**；EventBus 已建成 `rt_event_bus.py:20`） |

## VO-012 · M5 收口 dogfood

| 项 | 内容 |
|---|---|
| 依据 | plan §7 M5；报告 §10 |
| 范围 | 全量回归；dogfood：向导自举"监督员"profile → 对接 agent 编排一次真实 A/B 双车道 fan-out |
| 验收 | ① 13+ 文件全绿（含 V7/V8/车道B 用例）；② dogfood 全链一次成功（监督员真孵化+真派发+终稿回传）；③ 文档终态（KG 全 loc 化、账本收口） |
| 验证形式 | 全量 + dogfood 记录入 evidence |
| 量/依赖 | 中 · VO-007/009/010/011 全部 |

---

## 附：环境前置（排票时钉死；round 10 复核后更新）

1. **dais 编排面**：✅ 已恢复（2026-08-23 13:23 新二进制 GUI；runtime.json+L2 socket 在位；18 命令+v2 project/worktree/new-terminal 可用）。已验证路径：邮箱总线全功能、new-terminal→inject→pane 执行→read 回读。**注意**：`start-worker --command` 的 pane 自动绑定未闭环（dispatch 停 pending）——VO-007 的 manager 派发用 new-terminal+inject+mailbox 已验证路径；start-worker 自动绑定留作 dais 侧观察项。**契约变化**：read-worker 失败现为 exit-0+JSON error 软错误（DaisLane 已归一化）。
2. **GLM 凭据**：`~/.dsh/zhipu.env` 已有（文本模式全链已验；live 用例带采样容差重试各 1 次）。
3. **orca 宿主**：v1.4.185 在线（VO-008 探针已验 245ms）。
