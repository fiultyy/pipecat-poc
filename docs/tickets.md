# voice-orchestration 票板(v4 规范,与 ~/.dsh/maestro/tickets.md 同构)

> lane: 本仓语音编排头系统(车道A/W5/车道B/网关)。规范源:`docs/plans/impl-specs.md`(票的验证目标逐条引用其验收编号)。
> **派发执行:`docs/plans/dispatch-plan.md`(orca worktree 无竞争并行 + 合并管理;Wave 0 基线→三 worktree 并行 + P 链主线串行)**。
> 状态:☐ 待派 → ◐ 进行中 → ☑ 完成(审查通过)。回报物:`docs/kg/evidence/<REF>-report.md`。
> 全局验证门 G1–G5 与环境前置见 `docs/plans/impl-specs.md`(G1 回归绿/G2 离线优先/G3 范围分治/G4 文档同步/G5 协议不漂移)。

---

### VO-001 Projector 第 17 维 agent_role ☑

- **目标/方案**:投影器支持角色维度——`rt_projector.py` 增 `ROLE_TEMPLATES`(liaison/manager/worker/supervisor,worker=现行零变化)与 `project(role=)` 参数;`profile_json` 增 `agent_role` 键(可追溯,不进术语面)。liaison/manager 模板条款内容 = KG 06§1.2 表。
- **路径**:`examples/realtime-provider-poc/rt_projector.py`;`tests/test_rt_projector.py` 扩;`tests/test_projection_live.py` 增 role 冒烟。
- **验证目标**(impl-specs VO-001 ①–⑤):①liaison 产物含收敛契约/两阶段/[ref:]/凭证回显四条款 ②manager 含车道选择/--dep/worker_done/异常上抛 ③worker 与现行输出全等 ④role 产物过三门 ⑤agent_role 落键不入正文。
- **验证形式**:自动=参数化 mock 用例;live=role 冒烟 1 例。
- **量**:小 · **依赖**:无 · **派发**:本仓 code(纯离线,可立即开工)
- **回报物**:evidence/VO-001-report.md · **done body**:`<判定>;报告:evidence/VO-001-report.md;测试:<N>项全绿;备注:<≤40字>`

### VO-002 incubate 扩参 + dsh-liaison/dsh-manager 孵化目标 ☑

- **目标/方案**:孵化池 incubate RPC 增 `role/project/mailbox` 参数(缺省=现行语义,向后兼容);targets 新合法值 `dsh-liaison`/`dsh-manager`(复用 incubateDsh,注入体=role doctrine 段+agentsMd,fleet 扩展登记)。
- **路径**:`~/.dsh/plugins/a2a-profile-server/http-server.js`(incubate 分支)、`incubators/real.js`(incubateDsh);`selftest.mjs` 扩。
- **验证目标**(VO-002 ①–⑤):①缺参回归一致 ②liaison 孵化回执含 sessionId/code/mailbox/role/project ③非法 role→-32602 ④三门拦截照常 ⑤selftest 绿。
- **验证形式**:自动=selftest(mock session-spawn);live=真孵化冒烟 1 例(dais 在线时)。
- **量**:中 · **依赖**:VO-001 · **派发**:本仓 code(插件 js)
- **回报物**:evidence/VO-002-report.md · **done body**:同上格式

### VO-003 fleet 扩展 + registry/reattach + 生命周期 ☑

- **目标/方案**:新 `registry.js`——fleet 登记项扩五键(role/project/mailbox/profile_version/spawned_at);`reattach()`(loopback session.list 比对,孤儿→retired+journal);状态机 spawn→arm→ready→serving→retire。**唤醒模型钉死**:dsh 会话非常驻轮询者;推唤醒(DSHMSG 注入触发回合)+回合首拉取(邮箱快照排空);探活一律 router 侧驱动。
- **路径**:`~/.dsh/plugins/a2a-profile-server/registry.js`(新);`tests/test_rt_fleet_registry.py`(新)。
- **验证目标**(VO-003 ①–④):①fleet 五键原子读写 ②reattach 孤儿检测+journal ③状态机全迁移+非法迁移拒绝 ④无 agent 自发轮询假设。
- **验证形式**:自动=mock loopback/fleet 单测。
- **量**:中 · **依赖**:VO-002 · **派发**:本仓 code
- **回报物**:evidence/VO-003-report.md · **done body**:同上格式

### VO-004 router 三 RPC + scope + journal ☑

- **目标/方案**:插件增 `agents/registry`(在册+状态+心跳)/`agents/send`(scope 校验→轻载 session-send DSHMSG 推注入、重载 dais 邮箱投→ack)/`agents/inbox`(只读快照);`router-journal.jsonl` 全量审计。**红线:只注入固定信封格式,不注入任意指令**(G5 同源)。
- **路径**:`~/.dsh/plugins/a2a-profile-server/http-server.js`(三分支)+router-journal;`tests/test_rt_router.py`(新)。
- **验证目标**(VO-004 ①–⑤):①三 RPC 契约 ②同 project 通/跨 project 拒(-32000 scope) ③双模式分流+journal 记 delivered ④inbox 只读不消费 ⑤journal 可回放。
- **验证形式**:自动=mock session-send/dais 单测。
- **量**:中 · **依赖**:VO-003 · **派发**:本仓 code
- **回报物**:evidence/VO-004-report.md · **done body**:同上格式

### VO-005 推/拉双投递 conformance ☑

- **目标/方案**:同一消息双投递模式对拍——DSHMSG 推注入(session-send 底座)vs dais 邮箱投递,收方视角同结果;跨进程 sleep 间隙(总线锁纪律)。
- **路径**:`tests/test_rt_conformance.py` 扩。
- **验证目标**(VO-005 ①–④):①双模式同信封解析 ②推注入回合首行机器可解析 ③读即消费语义 ④掉线确定性 skip。
- **验证形式**:live(真 session-send+真邮箱)。
- **量**:小 · **依赖**:VO-004;dais 编排面在线(环境前置 1) · **派发**:本仓 code
- **回报物**:evidence/VO-005-report.md · **done body**:同上格式

### VO-006 liaison 落位(替身协议移交) ☑

- **目标/方案**:真孵化 dsh-liaison(mailbox=agent_liaison);head 的 `DshBackend.orchestrator_handle` 指向真身;**head 代码 diff=0**(KG 06§3.1 对照表四项逐条核)。
- **路径**:live_v5_v6 场景扩(替身换真身);head 侧仅配置值。
- **验证目标**(VO-006 ①–④):①F4/F10 全链一次 ②回执/终稿与替身 V5 同构 ③head diff 仅配置 ④回合首动作=邮箱快照排空。
- **验证形式**:live。
- **量**:中 · **依赖**:VO-005;dais 在线 · **派发**:本仓 code + dsh 会话
- **回报物**:evidence/VO-006-report.md · **done body**:同上格式

### VO-007 manager 群 + live V7 ☑（低载窗全绿+晚峰环境敏感已钉死 E 节；当前文件完整 live 复验=VO-012 dogfood 天然承载）

- **目标/方案**:dsh-manager 孵化(每域一个);head→liaison→manager→dais 车道→逐跳回传;异常路径(gate/wait-blocked/超时上抛)入 doctrine;分派策略随本票落地(终端/工作树→orca 占位,消息 DAG→dais 实跑)。
- **路径**:`tests/test_live_v7.py`(新);manager 模板产物。
- **验证目标**(V7 六步):①真孵化两 agent ②F4→liaison 收 ③F6 router 分发(推唤醒+邮箱正文) ④F7 派发+worker_done ⑤F9/F10 逐跳回传 ⑥全链 ref 不丢+凭证逐字+两阶段时序。
- **验证形式**:live。
- **量**:中–大 · **依赖**:VO-006;dais 在线 · **派发**:本仓 code + dsh 会话
- **回报物**:evidence/VO-007-report.md · **done body**:同上格式

### VO-008 OrcaLane 封装 + 单测(车道B B.1–B.2) ☑

- **目标/方案**:`rt_orca_lane.py` 全方法(status/spawn_worktree/terminal_list/read/wait/send/interrupt/stop/worktree_ps);全 --json;BIN 硬编码 `orca-ide`(**R1:严禁裸 orca**);全方法有界超时。开工首步 `orca-ide skills get orca-cli` 钉死 read 增量游标。
- **路径**:`examples/realtime-provider-poc/rt_orca_lane.py`(新);`tests/test_rt_orca_lane.py`(新)。
- **验证目标**(VO-008 ①–④):①KG 07§2 语义映射全覆盖 ②无裸等 ③--json 解析+错误形制对齐 DaisLaneError ④mock 单测全绿。
- **验证形式**:自动=mock CLI;探针=skills get 入 evidence。
- **量**:中 · **依赖**:无(**可与 W5 线并行**) · **派发**:本仓 code
- **回报物**:evidence/VO-008-report.md · **done body**:同上格式

### VO-009 车道B live 冒烟 + A/B 对拍(B.3–B.4) ☑

- **目标/方案**:真 spawn 1 worktree→wait→read 全程有界;A/B conformance:同一意图双车道→终稿均 FINAL_PREFIX+同 body、凭证一致(终稿通道不分会道,统一 dais 邮箱链回传)。
- **路径**:`tests/test_rt_conformance.py` 扩(lane 工厂参数化)。
- **验证目标**(VO-009 ①–③):①live 冒烟无死等 ②A/B 同终稿 ③不可用确定性 skip。
- **验证形式**:live。
- **量**:中 · **依赖**:VO-008;orca 宿主在线 · **派发**:本仓 code
- **回报物**:evidence/VO-009-report.md · **done body**:同上格式

### VO-010 孵化向导 skill 壳(W3.4) ☑

- **目标/方案**:`~/.agents/skills/incubation-wizard/SKILL.md` 引导 dsh agent:场景选型+role 选型→投影→三门报告回显→选孵化目标→incubate;参数 --scenario --name --role --targets --model。
- **路径**:skill 文件(新)。
- **验证目标**(VO-010 ①–③):①dsh 会话可发现调用 ②流程含 role 一步不漏 ③回执回显。
- **验证形式**:冒烟=dsh 会话内走一遍(M5 前置)。
- **量**:小 · **依赖**:VO-002(**可并行**) · **派发**:dsh agent(skill)
- **回报物**:evidence/VO-010-report.md · **done body**:同上格式

### VO-011 rt_gateway 本地网关(M4) ☑

- **目标/方案**:三路复用帧协议(control/media/event)+WsSession+TailReader+静态页(mic/播放/三级树/gate 弹层);EventBus 订阅转 event 帧;断线续接(take_tail 重播种);局域网安全基线。frp 已移出架构(Q1 关闭),仅本地。
- **路径**:`examples/realtime-provider-poc/rt_gateway.py`(新);`tests/test_rt_gateway.py`(新);静态页。
- **验证目标**(VO-011 ①–④):①帧协议单测(握手/背压/慢客户端合并) ②本机 e2e mic→head→音频 ③事件全序到达 ④局域网连通+orch.metrics 三指标。
- **验证形式**:自动=单测;本机 e2e+局域网联调。
- **量**:中–大 · **依赖**:无(**独立可并行**;EventBus 已建成) · **派发**:本仓 code
- **回报物**:evidence/VO-011-report.md · **done body**:同上格式

### VO-012 M5 收口 dogfood ☐

- **目标/方案**:全量回归+自举闭环:向导自举"监督员"profile→对接 agent 编排一次真实 A/B 双车道 fan-out;文档终态(KG 全 loc 化、账本收口)。
- **验证目标**(VO-012 ①–③):①全量绿(含 V7/车道B/网关用例) ②dogfood 全链一次 ③文档终态。
- **验证形式**:全量+dogfood 入 evidence。
- **量**:中 · **依赖**:VO-007/009/010/011 · **派发**:编排(dogfood 本体)
- **回报物**:evidence/VO-012-report.md · **done body**:同上格式

---

## 派发顺序建议

```
立即可开(纯离线,无环境依赖): VO-001 → VO-002 → VO-003 → VO-004(串行链);并行: VO-008、VO-011、VO-010(待002)
dais 编排面恢复后: VO-005 → VO-006 → VO-007(live 链)
orca 冒烟: VO-009(待 008)
收口: VO-012
```
