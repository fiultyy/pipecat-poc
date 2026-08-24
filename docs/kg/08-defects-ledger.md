# Voice-Orchestration KG · 08 编排链缺陷台账

> 版本 v1 · 2026-08-23 · 来源：VO-001→010 执行过程复盘（Wave 0–2 实录）
> 性质：**已观察缺陷 + 现行对策 + 建议修复锚点**。修复前排期参考本表；修复合入后在此标 ✅。
> 缩写：POC = `~/workspace-claw-02/pipecat-poc`；证据多在 `evidence/` 与 ledger。

## 1. dais 编排面

### D-01 【高→主体解】orchestration feature 静默丢失
- 现象：不带 `--features orchestration` 重建后，GUI 正常启动但编排面整体消失（无 RPC/无 dais-runtime.json/无邮箱），启动零告警。VO-005 agent 的一次重建即击落全平面 ≈2.5h。
- 根因：orchestration 为非默认 cargo feature；产物缺失只能靠 `strings 二进制 | grep "not enabled in this build"` 计数判别。
- 现行对策（已落地，W6/OF-008 ①）：`~/.local/bin/dais-build`（构建→strings 断言计数=0→安装→构建报告含错误形制快照，兼收 D-03 契约基线）；wrapper 头注释指路禁裸 cargo build。selftest 24/24（〔doc:~/.dsh/maestro/reports/OF-008-report.md〕）。**真构建全链已验**（2026-08-23：16m release 构建，sentinel=0 PASS，原位安装，平面 canary 存活；报告 〔doc:~/.local/state/dais/build-report.md〕）。③消费侧 WARN（rt_probe_m0/rt_dsh_lane 醒目告警）deferred 至 VO-012 后。

### D-02 【高→已解】GUI 单实例脆弱 + 看护拉起双实例
- 现象：①测试/agent spawn 的 transient dais 实例可击落常驻 GUI（VO-005 残留事故）；②重启窗口出现自动拉起实例与手工 nohup 并存（双实例 287105/302881）。
- 根因：无实例锁或锁不保护常驻进程；看护拉起与人工重启无协调。
- 现行对策（已落地，W6/OF-008 ②）：wrapper 实例锁 `~/.local/state/dais/instance.lock`（flock+pid+boot-id，锁 fd 随 exec 传入 GUI，退出自动释放；后起者退出并提示持有者；`--force` 覆盖；boot-id 变化陈旧锁让位）——agent transient spawn 走同一 PATH wrapper 同样被守卫；orchestration 等 CLI 子命令显式零取锁（监督通道豁免）。selftest 24/24。

### D-03 【中】CLI 软错误契约漂移
- 现象：重建后 read-worker 从 exit-1 改为 exit-0+JSON `{"error":...}`。
- 现行对策：〔loc:examples/realtime-provider-poc/rt_dsh_lane.py→DaisLane.read_worker〕双形态归一为 DaisLaneError。W6/OF-008 起 `dais-build` 构建报告含 read-worker 错误形制探测快照=版本化契约基线落盘起点。
- 建议：dais 侧把错误形制写入版本化契约文档，跨重建守恒。

### D-04 【中】start-worker pane 自动绑定未完成
- 现象：dispatch 受理成功但 `dispatch_contexts.assignee` 停 None/pending；`assign` 报 GUI 成功库行不更新。
- 现行对策：V7/manager 派发走已验证路径（new-terminal+inject+mailbox）。〔doc:evidence/ledger-carryover-round6.md§round10〕
- 建议：dais 侧修复 assign 落库；修前保持绕行路径。

### D-05 【低】run 注册表只增不清
- 现象：check-status runs 167→262 持续累积，无归档。
- 建议：GC/归档策略（按时间或终态）。

## 2. 孵化插件（a2a-profile-server）

### D-06 【中】插件域非 git
- 现象：`~/.dsh/plugins/a2a-profile-server` 无版本控制 → 无回滚、无 diff 审查；派发被迫 Lane P 主线串行。
- 建议：init git（本地仓即可）或纳入 dotfiles 管理；worktree 化后可并行。

### D-07 【低→已解】fleet.json 陈旧 active 累积
- 现象：大量历史 vh-vh-smoke-probe 条目 status=active 未 retire；VO-003 状态机（spawn→arm→ready→serving→retire）只管新增。
- 现行对策（已落地，W6/OF-002）：`fleet-touch sweep`（lastSeenAt/heartbeatAt 陈旧>N 天 active→retired，`--dry-run` 默认 `--apply` 才动真）+ 属主租约三动作 claim/heartbeat/release（flock 原子）+ steer 闸（owner≠from 拒绝 exit 4 + `fleet-conflicts.jsonl` 冲突审计）。selftest 33/33（〔doc:~/.dsh/maestro/reports/OF-002-report.md〕）。存量 sweep 待 GM 窗口对真实 fleet 跑一次 `--dry-run` 复核后 apply。

## 3. 回程与监控

### D-08 【高→已解】cb-send 文件桥不能唤醒回合制编排者
- 现象：cb-send 降级文件桥后仅在编排者**下一回合**被消费 → "30s 无响应"结构性不可达。
- 现行对策（已落地）：**vo-relay 模式**——`session-spawn maestro` 落 fleet，relay 用 `session-send <relay> <编排者完整sessionId>` 直投编排者回合队列=推唤醒。〔doc:~/.dsh/maestro/bin/session-send〕
- W6/OF-003 尾巴收口：maestro-bridge skill 主叙事已切换为"编排者可达性=直投完整 sessionId（推唤醒）"，cb-send 降级备胎；steer 两段式契约（ack/nack）同步落 orch-loop + dispatch-ticket 模板。live steer 往返冒烟留首次生产使用时验证。

### D-09 【高】relay 看守面无进程活性 + 固定寿命中途到期
- 现象：①relay 只看守文件事件（报告落地/merge），VO-007 pytest 挂死 2h（CPU 0.1%/ep_poll）零告警；②60 轮寿命在长票未完时先到期，需手工 re-arm（今晚实录）。
- 现行对策（W6/OF-006 ①② 已落地）：`bin/event-watchd` 常驻守护——文件面（glob+位点推进防回声）+ 进程面（CPU 阈值+日志 mtime 陈旧度**双条件与**判定，单条件不误报）+ 自续期（有活动票顺延寿命）+ alerts.log 升级终点；③SLA/④租约两面留 patch 位。selftest 27×2（worker 双绿；GM 侧 26/27=RENEW 轮询窗口在本机常态负载下偏紧，放宽 15s 已入补丁批）。relay 实迁 watchd 与真值守待 GM 窗口。〔doc:~/.dsh/maestro/reports/OF-006-report.md〕

### D-10 【低→已解】relay 事件回声
- 现象：已由编排者处理完的事件（合并后）仍回报（VO-009/merge 回声）。
- 根因：无消费位点；回报不推进基线。
- 现行对策（已落地，W6/OF-001）：信封 v2 增 msgid+ts（重发保号）；收方去重窗口 `msg-dedup`（60s (from,msgid) 去重）；relay 契约=事件回报后原子推进 `reports.base`/`git.base`（〔loc:~/.dsh/maestro/orch-fleet-conventions.md〕）。selftest 25/25（〔doc:~/.dsh/maestro/reports/OF-001-report.md〕，commit 1ebc155）。

### D-11 【约束】omp TUI alt-screen 帧不含响应文本
- 现象：`terminal read` 看不到 agent 回复文本。
- 现行对策（纪律化）：三源监控 = `~/.omp/logs` agent_end + worktree `git status` + 报告文件落地。〔doc:plans/dispatch-plan.md§纪律2〕

## 4. live 测试预算与并发

### D-12 【高】轮询预算未从实测延迟分布推导 ✅（OF-009，2026-08-24）
- 现象：VO-007 manager 回收轮询 60 次×~6s≈360s/域，而 VO-006 实测真实 agent 回合"典型 40–70s、偶发 >5min"——零余量 → 门禁打回实录（文档域 WITNESS=None）。
- 修复：impl-specs **G6** 固化公式 `预算 ≥ 实测 P95×2`，预算常量/实测记录/租约告警阈值三处同文件注释互指（P95 基线 VO-006-report）；dispatch-plan 派发模板同步 live 纪律段。证据：evidence/OF-009-report.md §A③。

### D-13 【中】live 测试无并发锁 ✅（OF-009，2026-08-24）
- 现象：两个 pytest 实例同邮箱并发互踩（误起重复 gate 需手工清理）。
- 修复：`tests/live_lock.py` flock 域租约（dais-bus/orca-host/邮箱域）+ conftest `@pytest.mark.live` 自动按域取锁（setup→teardown）；`DSH_LIVE_LOCK=skip` 让路跳过留痕；8 个 live 用例已标记。双进程演练不重叠 + skip 确定性：evidence/OF-009-report.md §A①。

### D-14 【中】live 瞬态容忍语义留在测试层
- 现象：V5 播报凭证漏报一次靠测试内重试容忍（组合跑复现一次后未再现）。
- 建议：broadcast 凭证重试语义下沉 DshBackend（一次自动补发），测试只断言终态。

## 5. 环境坑（多 agent 重复踩）

### D-15 【中】回环代理坑无共享帮手
- 现象：urllib 不认宿主 NO_PROXY 的 CIDR 条目、httpx 需剥 SOCKS——VO-010 与 VO-006 各自重新发现，各写各的解法。
- 建议：conftest/插件 README 提供 `no_proxy_env()` 统一帮手 + 坑位文档一处收录。

## 6. W6 加固收口记录（2026-08-23，OF 系列 8/10 done）

> 方案：`docs/kg/09-orch-hardening-plan.md`（N9）；maestro 域 commits `2f220c4..4652658`（9 个）；报告 `~/.dsh/maestro/reports/OF-00x-report.md` ×8；GM=orch-fix-plan(e858) 会话。

**主题 A（多主契约软隔离）W1 车道收口**：信封 v2（msgid+ts 只增键，`1ebc155`）→ fleet 属主租约+steer 闸+冲突审计（`28fa9d8`）→ steer 两段式契约+可达性主叙事切换（`260c597`）。v2 信封在 W6 全程回程通道实弹（6 例 done 均自带 msgid）。OF-004（loopback 凭证）按方案 §9 持有在中期窗口。

**主题 B（长时任务编排）W2+W3 车道收口**：tickets DAG（状态机 7 态/12 合法边，非法迁移实测拦截；`b013d64`）→ wave 检查点（SIGKILL×3 原子性实证；`439d9f0`）→ event-watchd ①②面（`a19558d`，③④留 patch 位）→ longtask 单向投影绑定（`4652658`；真实激活=收口时 10 票全量投影至 `state/longtask-carryover.md`）。**波次状态已搬出 LLM 上下文**：ticket 面（ledger.db）+检查点面（wave-checkpoints.jsonl）+承接面（longtask-carryover.md）三层数据化。

**C 组**：dais 构建断言+实例锁（`934cccc`；真构建 16m PASS sentinel=0）。

**遗留批（tracked）**：OF-006③SLA/④租约补丁+RENEW 窗口 15s；OF-007④ render 头部摘要联动；relay 实迁 watchd+一轮真值守；fleet 存量 sweep apply（dry-run 复核后）；OF-009（VO-012 后）；OF-004（中期）。

## 7. 已修复（存档）

- ✅ EventBus 通配订阅双重投递：〔loc:examples/realtime-provider-poc/rt_event_bus.py:26→subscribe〕kinds-or-star 单注册路径，exactly-once 复验（VO-011 agent 发现，main 已合）。
- ✅ VO-007 无界等待挂死：v7_main 全局 watchdog 840s + shell 900 壳 + 轮询预算整改（本台账 D-12 关联）。

## 8. 长时模式编排能力自检（2025-08-24，实证=session-20d0a3ee 自身）

`session.list` 证实本会话 `agentPreset: "long-task"`；对照组合文件 + 本会话活工具面：

**进程内多代理编排 ✔（组合内生，工具面一致）**：`delegation` 组（isolate realm workflowEngine）含 subagent(spawn)/subagent_fork(fork)（continuable 可跨轮召回）+ subagent-control（send_message/interrupt_agent/list_agents）+ workflow-worker-thread/tool-workflow（JS fan-out 编排脚本）+ tool-ralph(64 轮)；另有 tool-jobs（后台作业三件套）、tool-goal + tool-long-task（双台账）、tool-todo/ask-user。实证：本会话（70 turns/1105 steps）完成 VO 12/12（goal-9a721634 complete）。

**跨会话/跨平面编排 ✖（组合内生为 0）**：①persona 是一行裸文本（"You are a coding agent..."），零编排 doctrine（dispatch 握手/预算裁决/red lines 全靠外置 orch-index）；②预设目录仅 2 文件——无 skills/（skill-filesystem 未配 customSkillDirs，orchestration/orca-cli/maestro-bridge 全不在面）、无 plugins/ 行（maestro 有 message-bridge/orca-callback pump/session-purge/workspace-unarchive 4 行，long-task 0 行）；③codex/claude-code provider 显式 disabled（生产不装）。本会话历史里的跨会话编排（relay <seat>/supervisor <seat>/e858 handoff）全部走 host 级外置桥（`~/.dsh/maestro/bin/session-send`、孵化插件 subprocess），非 preset 能力。

**判定**：长时模式=单会话编排完备、舰队编排靠环境外挂——即 §5 复盘"long-task 缺高级编排"的结构性根源：桥是 ambient 的（知道即能用），不是 contractual 的（组合声明）。N10 的 queen 派生若要产出"编排型"profile，delegation 组是正确基底，跨会话面需显式补插件行+skills。

**更新溯源（补充）**：delegation 组是 2026-08-22 更新带来的——该日 10:01 装 dsh rc.8，21:39:26–57 三 preset（liangshen/maestro/long-task）agent.cordis.yml 同窗重写，maestro 与 long-task 的 delegation 组行集**完全一致**（含同为 disabled 的 codex/claude-code 行）。即更新后两 preset 的进程内编排面已对齐，maestro 的独占差异仅剩：4 个本地插件行（message-bridge/orca-callback pump/workspace-unarchive/session-purge）+ skills/ 目录 + persona 编排 doctrine。

## 9. D-14 · long-task 工具读取路径校验 bug + 台账悬空（2026-08-24，用户问责触发）

**现象**：`get_long_task` 恒报 `long-task change open question closes unknown checkpoint 5`；读路径死锁。盘上投影（storages/session_projcache.json）状态本体完好（6 checkpoints，seq 1-6），但 openQuestions 两项引用 `closesCheckpoint 5/4`——校验按"change"口径拒绝；宿主内存为权威，projcache 手改被覆写（实测 seq 318901→319397 回滚）。无独立服务存储落盘。

**根因**：宿主 rc.8 long-task 服务的 change 校验缺陷（引用编号口径错位），上游 bug。

**处置**：
- 不在运行宿主内存上做手术（会话自身活在 pid 16893 里，重启需用户裁决）。
- **台账判定性关闭**（声明式）：longtask-670ac697 目标=VO 四线波次，事实上已完成——M0-M5 六检查点全达成（VO 12/12、dogfood VO-012、goal-9a721634 平行 goal 已正规 complete、终态 commit bc27d7e）。两项 openQuestions 均已被事实了结（Q1 frp→用户裁定移除；Q2 DashScope key→GLM 路径交付未用）。该台账作废，不再尝试工具路径关闭。
- **跨波次权威账转移**（结构性修复）：ledger.db tickets（机械态）+ longtask-carryover.md（单向投影，N10 期间 checkpoint 1-10，本轮已读回渲染）+ pipecat-poc KG——三处一致性已核。波次状态此后不依赖 goal/long-task 任一会话内工具。

**责任记录（指挥者）**：①VO 波次开了 goal-9a721634 却留 long-task 台账悬空，两会话内状态机并行、后者失养——工具选择错误；②N10 结案核验信了"临时会话全清"未做 fleet 全量对账（28a2/4eeb 漏网，本轮已清账+purge）；③longtask-carryover 消费侧（读回渲染）从未执行至本轮——自立的规矩自己先违。三项均已矫正。
