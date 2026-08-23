# Handoff · W6 编排链加固规划（orch-fix-plan）

> 来源：编排者 session-20d0a3ee（VO 波次主责）；受让：orch-fix-plan（maestro）
> 日期：2026-08-23 晚。**你只做规划，不做修复执行。**

## 1. 使命

以 `docs/kg/08-defects-ledger.md`（缺陷台账 N8，17 条 D-01..D-15 + 已修存档）为基础，叠加下面两条**结构性主题**（来自用户与编排者的复盘对话，此前只在对话里，本文件是其唯一落盘），产出 W6 加固方案：

- **`docs/kg/09-orch-hardening-plan.md`**：主题→工作包分解（impl-specs 风格：范围/锚点/验收编号/量/依赖/文件域），含票草案（建议编号 OF-001..，与 VO 系列隔离）
- 更新 `docs/kg/00-INDEX.md` 注册 N9

## 2. 结构性主题（用户裁决级输入，权威）

### 主题 A：多主责任 agent 无契约软隔离
现状=文件域有机械守卫（rebase 冲突），agent 域只有君子协定。底层真相：`session-send` 的安全模型=知道 sessionId；`/api/session.prompt` 不验证 `from`；DSHMSG 自报家门；收方无法拒绝/审计/证明能力。事故实录（全部 2026-08-23）：
- 编排者起重复 gate → 两 pytest 踩同一邮箱（无租约无锁）
- relay 持有与编排者相同原语，理论上可 steer 任何 agent（无 ACL 区分编排者/信使）
- steer 是 fire-and-forget：`sent:True` 只=排进队列 ≠被接受；two-phase 只覆盖任务消息(ack/done)，不覆盖控制消息(steer/interrupt)
- agent 的 transient dais 击落常驻 GUI（主责任体改共享运行时无隔离边界）
讽刺点：插件 router 层有 scope+journal 契约，脚下 session-send 敞开——契约建在了上面一层。

规划应覆盖（编排者已想的方向，可挑战可扩展）：
1. 信封 `msgid` + 收方去重窗口（顺带解决 relay 回声 D-10）
2. fleet.json `owner` 租约字段：终端/邮箱登记属主主责任体+心跳；非属主 steer 拒绝或记冲突
3. 控制消息纳入 two-phase（steer 也要 ack：已读/正忙排队）
4. 中期：loopback API 能力凭证（spawn 发 per-session token，注入必携且与 from 绑定）

### 主题 B：长时任务无高级编排能力
现状=波次状态在 LLM 上下文（易失：已 compact 一次、longtask 服务端坏过一次）；编排者回合制=回合间隙无滴答（VO-007 挂死 2h 由用户发现而非 watchdog）；relay 固定寿命先于长票到期需手工 re-arm；编排者之上无升级路径（V7 worker 有超时上抛，"谁看 watchdog"无答案）。

规划应覆盖：
1. 任务 DAG 数据化：ticket 状态机（dispatched→running→blocked→done→merged）+依赖+租约落 JSON/SQLite；tickets.md 退化为渲染视图
2. watchdog 出走回合制：relay 泛化为事件守护（文件落地/进程 CPU+mtime 陈旧/SLA 超时/租约到期→DSHMSG）
3. 检查点机器可读：每轮 wave 追加 JSON checkpoint

### 明确不过度设计（用户语境下的编排者判断，可复核）
- 多主编排者协商（抢占/让渡/合并决策权）：现阶段租约+journal 记冲突即可
- TUI 文本注入传输（send=打字，agent 在 bash 面时语义未定义）：长期换结构化通道，短期不动

## 3. 边界（红线）

- **只规划不执行**：不派发修复 agent、不碰 dais/插件运行时、不改任何 src/examples/tests 代码
- 仓库内只新建 `docs/kg/09-*` 与 INDEX 一行注册；**不碰** `tests/test_live_v7.py`、`examples/realtime-provider-poc/live_v5_v6_dsh.py`（VO-007 在飞，正被另一 agent 修改）
- 不接管 VO 波次编排（主责仍是 session-20d0a3ee）；不往 relay <seat> 发消息（它在值守）
- 你的 cwd=pipecat-poc 仓（workspace 70f21d59）

## 4. 环境事实（免重探）

- 缺陷台账：`docs/kg/08-defects-ledger.md`（D-01..D-15，每条含现行对策+建议锚点）
- 过程台账：`docs/kg/evidence/ledger-carryover-round6.md`（round 14-16=事故实录细节）
- 票制先例：`docs/tickets.md`（v4 格式）；spec 先例：`docs/plans/impl-specs.md`；派发先例：`docs/plans/dispatch-plan.md`（Lane 设计+合并五步）
- 回程通道：`~/.dsh/maestro/bin/session-send <你的code> session-<id> done plan-done '<body≤200字>'`（DSHMSG 直投编排者回合队列，编排者会被唤醒）
- dais 平面健康（check-status 可用）；GLM creds 见 `~/.dsh/zhipu.env`
- relay 存在：fleet code <seat>（第二期值守 VO-007 报告+merge），勿动

## 5. 交付与回报

1. `docs/kg/09-orch-hardening-plan.md`：主题 A/B + 台账高优缺陷（D-01/02/08/09/12/13）→ 工作包（建议 6-10 个 OF 票），每包：范围/验收编号/文件域/依赖/量级；含总依赖图与排期建议（哪些可在 VO-012 后并行）
2. INDEX 注册 N9
3. 完成即回程 `done plan-done <一句话+文件路径>`；编排者验收后合入 git（你不做 git commit）
