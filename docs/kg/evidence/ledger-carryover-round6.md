# Long-task ledger carry-over（账本损坏承接件）

> 生成于 round 6 收口（2026-08-23），**round 8 更新**。原账本 `longtask-<id>`
> 服务端状态损坏：存量开放问题引用了不存在的检查点5，导致读写全部被校验拦截
> （`Error: long-task change open question closes unknown checkpoint 5`，连整体覆写、
> `get_long_task`、人类回合的 `create_long_task` 均拒）。
> **本文件即本会话权威账本**，每轮更新。恢复路径：在新会话用 `create_long_task` 重建。

## Objective（不变）

落地语音编排头四线系统：WS1 pipecat head 对接 dsh 编排 agent（经 dais/orca fan-out+监控）、
WS2 A2A 插件+base profile 持久化孵化（dsh/omp/claude）、WS3 context-files 投影 spawn-agent 插件、
WS4 本地 WS 网关（局域网语音入口；frp 公网穿透已移出架构——round 7 用户裁决）。方案：`docs/plans/voice-orchestration-head-plan.md`

## Checkpoints

| # | 陈述 | verifiedBy |
|---|---|---|
| 01 | M0 探针矩阵：dais/orca-ide 可从 pipecat venv 调起 | tests/test_m0_probe.py 9/9；docs/kg/evidence/m0-probe.md |
| 02 | M1 WS3：Projector+三门落地；真 GLM 投影冒烟过三门 | tests/test_rt_projector.py 22/22；tests/test_projection_live.py PASS |
| 03 | M2 WS2：插件六RPC+版本化 ProfileStore+三真实孵化器真冒烟 | node selftest 14/14；tests/test_rt_a2a_client.py 5/5；tests/test_incubators_real.py 3/3 |
| 04 | M3 W1.2-W1.5：DaisLane/DshBackend/四件套/车道A+执行桥+live A/B conformance；总线 live 语义五条固化 KG §2 | 全任务回归 93/93；tests/test_rt_conformance.py 离线+live |
| 05 | **M3 W1.6 完成（round 7）**：live V5（语音→dais run→播报全链）+ V6（打断+问询+取消）PASS；Q2 定案=GLM 文本模式先行 | tests/test_live_v5_v6.py PASS（真 GLM+真 dais）；docs/kg/evidence/m3-live-v5v6.md；回归 94/94（13 文件） |
| 06 | M4 WS4：rt_gateway 三路复用帧协议+orch.* 事件汇总（本地局域网语音入口；frp 公网穿透已移出架构） | 待做 |
| 07 | M5 收口：全量绿+dogfood（孵化监督员 profile 自举真实 fan-out） | 待做 |
| 08 | **round 8：实施文档方法级对齐**——plan v2 + KG 00-06 全量对齐修正架构（双 ADE 车道/W5.1-W5.4 方法级/frp 移除/建成项 new→loc 归档）；KG 04 重命名去 frp；新增 KG 06（W5 方法级：第17维/incubate 扩参/fleet+reattach/生命周期/router 三 RPC/session-send 推+邮箱拉/liaison 移交表/V7 场景） | 本轮 diff；loc 锚点逐条抽检；回归 88P+6S=94 收集 0 失败（dais 宿主掉线 → 探活 skip 化） |
| 09 | **round 8 补：规划完整性收口**——新增 KG 07 车道B 方法级（OrcaLane 全方法签名，orca-ide 命令面本轮实测探明：worktree create --agent --prompt / terminal wait --for exit\|tui-idle --timeout-ms / send --interrupt；A/B 语义映射表+对拍判定）；修 N5 EventBus 误标（实际已建成 rt_event_bus.py:20）；KG 06 唤醒模型钉死（dsh 会话非常驻轮询者：推唤醒+回合首拉取+router 侧探活） | orca-ide --help 逐命令实测；回归 88P+6S 复验绿 |
| 10 | **round 9：spec 聚合 + 排票**——`docs/plans/impl-specs.md`（12 包实施规范：范围/锚点/验收编号/量/依赖/全局验证门 G1-G5/环境前置）；`docs/tickets.md`（VO-001..VO-012，v4 票制与 maestro 同构，验证目标逐条引用 impl-specs 验收编号，含派发顺序：离线链 001→004 可即开，live 链 005→007 待 dais）；**dais 掉线根因定 位**：wrapper（~/.local/bin/dais）注释明示 orchestration 非默认 feature，现行构建未带 flag 重建 → "orchestration is not enabled in this build"；修复构建 `cargo build --release -p warp --features orchestration` 已后台启动，完成后需重启 dais | 两文档落盘；~/.dsh/maestro/tickets.md 票制对照（v4 字段齐全） |
| 11 | **round 11：派票执行方案落盘**——`docs/plans/dispatch-plan.md`（orca worktree 无竞争并行：冲突面矩阵 12 票×文件域、Lane A/B/C worktree（001/008/011）+ Lane P 主线串行（插件非 git：002→003→004→005→006→007）、派发协议命令级模板、合并五步协议（rebase 域隔离零冲突预期/ff-only/全量门禁/原子回滚/worktree rm）、Wave 0 基线；事实核查：main 25 路径未提交、插件目录非 git） | dispatch-plan.md + tickets.md 头部指针；三事实 shell 实测 |
| 12 | **round 12：执行体定为 omp + 派发机制全链实测**——用户裁决"默认用 omp（oh-my-pi），orca bash 直接输入 omp 启动"（非 codex）。实测链全通：repo add --path 注册（repoId=b58d1d62）→ worktree create（落点 ~/orca/workspaces/pipecat-poc/\<name\>；--agent 旗标只识内置不识 omp）→ terminal create --command "omp"（TUI 起：pyright LSP+5 MCP 挂载）→ wait tui-idle → send --text --enter 提交 → GLM-5.3 真回包（~/.omp/logs agent_end hasText:true 铁证）→ worktree rm 清理。**关键发现**：alt-screen 帧捕获不显示响应文本 → 监控三源纪律（omp 日志 agent_end + worktree git status + 报告落盘；terminal read 仅辅助）。dispatch-plan 升 v2（omp 纪律三条+W0.2/0.3 完结）。另清理：broken opencode-dev 死链 symlinks ×2 删除 | dispatch-plan v2 diff；omp log agent_end 行；探针 worktree 建删往返 |
| 13 | **round 13：Wave 0 完结（基线就绪）**——预提交门禁 94/94 全绿后两条逻辑提交落 main：`63c252b`（车道A 全部代码+13 测试文件）+ `eca36eb`（KG/plan/tickets/report/evidence 全文档），工作树 0 未提交，**不 push**。src/ 无改动（qwen 方言层已在 HEAD）。基线验证：从新 HEAD 建 worktree → 票面三关键文件（tickets.md/dispatch-plan.md/rt_projector.py）齐 → 主区 venv 跑 worktree 测试 22/22 绿 → worktree rm 清理。**派发前置全部就绪** | git log 两提交；worktree 建删往返+22/22 绿 |

## Core（live，至多两条）

1. round 8 完成：**实施文档方法级对齐**——`docs/plans/voice-orchestration-head-plan.md`
   重写 v2（修正架构图/车道A 归位/§5 W5 工作包/WS4 去 frp/里程碑重排 M0✅→M3✅→W5→车道B→M4→M5/
   决策记录 Q1-Q3 全收口）；KG 六文档对齐（00 索引+边图+锚点速查、01 车道A 建成态
   new→loc、02 孵化池建成态+W5 扩展位、03 投影+第17维、04 重命名 04-ws4-gateway.md 去 frp、
   05 契约含 DSHMSG 信封/测试矩阵实测 13 文件、**新增 06 W5 方法级**：Projector 第17维
   ROLE_TEMPLATES、incubate 扩参签名、fleet 扩展 schema+reattach 算法+生命周期状态机、
   router 三 RPC 签名+scope+journal、liaison 替身移交对照表、manager 循环+V7 六步场景、
   W5.1a-W5.4 实施验证序列）。loc 锚点逐条抽检（DshBackend:45/:34/:70/:157-160、
   gates 正则:39-40、http-server gatesFn:106-109、session-send:4/:6/:10/:13/:22/:47）。
2. round 8 附带：测试探活健壮化（dais 宿主面掉线时确定性 skip 而非失败）——
   test_live_v5_v6 增 `_dais_plane_up()` skipif；test_m0_probe dais 两参数同款；
   test_rt_dsh_lane live 冒烟加 bus 预探；test_projection_live 描述断言改多格式容错
   （修 `a and b or c` 优先级脆弱）。回归 88 passed + 6 skipped = 94 收集，0 失败。

## Open questions

- Q1：**已关闭（round 7 用户裁决：frp 公网穿透移出架构，WS4 仅做本地局域网 ws 网关，不再需要 VPS 信息）**。
- Q2：**已定案（round 7）**——DASHSCOPE_API_KEY 缺失，GLM 文本模式先行；
  语音模态待 key 补给后按 providers factory 切换（QWEN realtime 路径已探通）。

## Next（round 7 末架构修正后重排）

**用户修正（2026-08-23，权威）**：双 ADE 双车道 = **dais ADE（车道A）/ orca ADE（车道B）**，
经两个编排 skill（`dais-orchestration` / `orchestration`·orca-cli）引导 dsh-web GUI 中的
agent 编排。**dsh 是宿主**，挂三件：① **A2A 孵化池插件**（基于 WS3 多维架构；持久 list；
创建向导 skill 引导 dsh agent 创建）② **编排器 skill**（指向 A/B 车道 ADE 执行）
③ **主对接 agent**（head 语义指令 → 稳定指令 → dsh 各 manager 级 agent）。

既有工作归位：DaisLane/DshBackend/四件套/live V5V6 = **车道A 全链**（车道内双到达路径：
CLI 直达 vs 孵化池执行桥，conformance 同结果）；`session_vhlive` 测试替身 = **主对接 agent
的替身**（两阶段契约+[ref:] 信封即其线协议，已验证）。

修正后待办（顺序）——round 7 末新增 W5（用户裁决：对接 agent 与 manager 级 agent
**也经孵化池供 prompt**（dsh 扩展孵化能力），且这些 agent **在 dsh 内部通信、打破会话隔离**）：

- **W5.1 孵化能力扩展（incubation v2）**：Projector 第 17 维 agent_role
  （liaison/manager/worker/supervisor 各自投影模板，三门照常）；插件新增 dsh-liaison /
  dsh-manager 孵化目标（incubate 扩 role/project/mailbox 参数）；fleet 元数据扩展
  （role/project/mailbox/profile_version）+ 重启 reattach + 孤儿检测；生命周期
  spawn→arm→ready→serving→retire（心跳探活，retire 后 profile 保留可复活）
- **W5.2 内部通信层（打破隔离）**：现成底座 = maestro `session-send`
  （`DSHMSG]{from,to,type,ref,body}` 单行信封经 loopback session.prompt 注入目标会话=推送）
  + dais 邮箱（agent 持 skill 有界快照轮询=拉取，语义已钉死）；插件内路由器三 RPC
  （agents/registry、agents/send、agents/inbox）；scope=project 内全互通、跨 project 显式
  授权、只注入固定信封格式不注入任意指令、全量 journal 审计
- **W5.3 主对接 agent 落位**：dsh-liaison 模板孵化；邮箱句柄接管 head 的
  DshBackend.orchestrator_handle（V5/V6 替身协议原样移交，head 零改动）
- **W5.4 manager 群 + live V7**：dsh-manager 孵化（选车道：终端/工作树→orca，
  消息 DAG/轻量→dais）；head→对接→manager→dais 全链一次（凭证逐跳逐字）；
  推/拉双模式 conformance
- 车道B orca ADE：OrcaLane 封装 orca-cli + 分派策略 + dais↔orca conformance
- 孵化池创建向导 skill 壳（W3.4，含 role 选型）
- M4 WS4：rt_gateway 三路复用帧协议 + orch.* 事件汇总（本地 ws 服务；
  **frp 公网穿透已移出架构——round 7 用户裁决，Q1 关闭**）
- M5 收口：全量绿 + dogfood（向导自举监督员 → 对接 agent 真实 A/B fan-out）

**环境状态（round 10：dais 重启后全面复核）**：新二进制 GUI 13:23 起运行
（pid 1445663），`dais-runtime.json` + L2 socket 在位；18 子命令 + v2
project/worktree/new-terminal 全部可用。**分层实证**：①邮箱总线全功能（94/94 中
全部 live 用例）；②new-terminal→session 邮箱→inject-prompt→pane 真执行→read
回显手动闭环验证通过；③`start-worker --command` dispatch 受理成功但 pane 自动
绑定未完成（dispatch_contexts.assignee 停 None/pending，`assign` 报 GUI 侧成功
但库行未更新）——**V7/manager 派发走已验证路径**（new-terminal+inject+mailbox），
start-worker 自动绑定留作 dais 侧观察项，不阻塞 VO-001→006。**重建带来的契约
变化**：read-worker 失败从 exit-1 改为 exit-0+JSON `{"error":...}`（软错误）——
DaisLane.read_worker 已归一化两种形态为 DaisLaneError（live 测试抓出并修复）。
另两处 live 容差：V5 播报凭证漏报一次纠正重试；projection 三门绊一次重投影
（projector 级暖重试属 VO-001 范围，票面已记）。全量 94/94 绿。

报告：docs/reports/voice-orchestration-head-report.html（v4：§2 重构为组件规划+流程指向
细化——§2.1 组件清单表（5 层 × 子组件 chips × 提供接口/依赖/状态）、§2.2 指向图
（编号边 F/H/C/E）、§2.3 主任务流 F1-F12 逐跳（指向/通道协议/载荷语义）、§2.4 孵化流
H1-H5、§2.5 取消流 C1-C4、§2.6 事件流 E1-E3）
