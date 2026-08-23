# Voice-Orchestration KG · 08 编排链缺陷台账

> 版本 v1 · 2026-08-23 · 来源：VO-001→010 执行过程复盘（Wave 0–2 实录）
> 性质：**已观察缺陷 + 现行对策 + 建议修复锚点**。修复前排期参考本表；修复合入后在此标 ✅。
> 缩写：POC = `~/workspace-claw-02/pipecat-poc`；证据多在 `evidence/` 与 ledger。

## 1. dais 编排面

### D-01 【高】orchestration feature 静默丢失
- 现象：不带 `--features orchestration` 重建后，GUI 正常启动但编排面整体消失（无 RPC/无 dais-runtime.json/无邮箱），启动零告警。VO-005 agent 的一次重建即击落全平面 ≈2.5h。
- 根因：orchestration 为非默认 cargo feature；产物缺失只能靠 `strings 二进制 | grep "not enabled in this build"` 计数判别。
- 现行对策：wrapper `~/.local/bin/dais` 头注释写明构建命令；事故后人工 strings 校验。
- 建议：构建脚本一体化（build+strings 断言）；或 runtime 侧对"消费方在而平面不在"打启动 WARN。〔evidence:ledger round 15〕

### D-02 【高】GUI 单实例脆弱 + 看护拉起双实例
- 现象：①测试/agent spawn 的 transient dais 实例可击落常驻 GUI（VO-005 残留事故）；②重启窗口出现自动拉起实例与手工 nohup 并存（双实例 287105/302881）。
- 根因：无实例锁或锁不保护常驻进程；看护拉起与人工重启无协调。
- 现行对策：票面红线禁 agent spawn dais；发现双实例手工杀后起者。
- 建议：runtime 锁文件（pid+boot-id 校验，后起者退出并提示持有者）。

### D-03 【中】CLI 软错误契约漂移
- 现象：重建后 read-worker 从 exit-1 改为 exit-0+JSON `{"error":...}`。
- 现行对策：〔loc:examples/realtime-provider-poc/rt_dsh_lane.py→DaisLane.read_worker〕双形态归一为 DaisLaneError。
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

### D-07 【低】fleet.json 陈旧 active 累积
- 现象：大量历史 vh-vh-smoke-probe 条目 status=active 未 retire；VO-003 状态机（spawn→arm→ready→serving→retire）只管新增。
- 建议：存量 sweep + 心跳 lastSeenAt 判活自动 retire。

## 3. 回程与监控

### D-08 【高→已解】cb-send 文件桥不能唤醒回合制编排者
- 现象：cb-send 降级文件桥后仅在编排者**下一回合**被消费 → "30s 无响应"结构性不可达。
- 现行对策（已落地）：**vo-relay 模式**——`session-spawn maestro` 落 fleet，relay 用 `session-send <relay> <编排者完整sessionId>` 直投编排者回合队列=推唤醒。〔doc:~/.dsh/maestro/bin/session-send〕
- 建议：把"编排者可达性=直投 sessionId"写进 maestro-bridge skill 文档，取代 cb-send 叙事。

### D-09 【高】relay 看守面无进程活性 + 固定寿命中途到期
- 现象：①relay 只看守文件事件（报告落地/merge），VO-007 pytest 挂死 2h（CPU 0.1%/ep_poll）零告警；②60 轮寿命在长票未完时先到期，需手工 re-arm（今晚实录）。
- 现行对策：编排者穿插人工三信号抽查（进程 etime/CPU、omp 日志 mtime、终端 spinner）；re-arm 靠新 mission 注入。
- 建议：relay 增加第三看守面（agent 进程 CPU 阈值 + 日志 mtime 陈旧度 >N 分钟即回报 stuck）；寿命改"到期前若有在飞票则自动续期"。

### D-10 【低】relay 事件回声
- 现象：已由编排者处理完的事件（合并后）仍回报（VO-009/merge 回声）。
- 根因：无消费位点；回报不推进基线。
- 建议：回报后原子推进 `reports.base`/`git.base`。

### D-11 【约束】omp TUI alt-screen 帧不含响应文本
- 现象：`terminal read` 看不到 agent 回复文本。
- 现行对策（纪律化）：三源监控 = `~/.omp/logs` agent_end + worktree `git status` + 报告文件落地。〔doc:plans/dispatch-plan.md§纪律2〕

## 4. live 测试预算与并发

### D-12 【高】轮询预算未从实测延迟分布推导
- 现象：VO-007 manager 回收轮询 60 次×~6s≈360s/域，而 VO-006 实测真实 agent 回合"典型 40–70s、偶发 >5min"——零余量 → 门禁打回实录（文档域 WITNESS=None）。
- 现行对策：轮询提到 90 次对齐 `await_timeout_s=540`；shell 900 壳不破。
- 建议：doctrine 固化推导公式 预算 ≥ 实测 P95×2；预算常量与实测记录同文件注释互指。〔doc:evidence/VO-006-report.md〕

### D-13 【中】live 测试无并发锁
- 现象：两个 pytest 实例同邮箱并发互踩（误起重复 gate 需手工清理）。
- 建议：live 用例引入租约（flock 文件 per mailbox 域）或标记 `pytest -p no:xdist`+串行守卫。

### D-14 【中】live 瞬态容忍语义留在测试层
- 现象：V5 播报凭证漏报一次靠测试内重试容忍（组合跑复现一次后未再现）。
- 建议：broadcast 凭证重试语义下沉 DshBackend（一次自动补发），测试只断言终态。

## 5. 环境坑（多 agent 重复踩）

### D-15 【中】回环代理坑无共享帮手
- 现象：urllib 不认宿主 NO_PROXY 的 CIDR 条目、httpx 需剥 SOCKS——VO-010 与 VO-006 各自重新发现，各写各的解法。
- 建议：conftest/插件 README 提供 `no_proxy_env()` 统一帮手 + 坑位文档一处收录。

## 6. 已修复（存档）

- ✅ EventBus 通配订阅双重投递：〔loc:examples/realtime-provider-poc/rt_event_bus.py:26→subscribe〕kinds-or-star 单注册路径，exactly-once 复验（VO-011 agent 发现，main 已合）。
- ✅ VO-007 无界等待挂死：v7_main 全局 watchdog 840s + shell 900 壳 + 轮询预算整改（本台账 D-12 关联）。
