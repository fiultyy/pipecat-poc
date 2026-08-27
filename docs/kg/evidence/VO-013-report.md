# VO-013 对话账：steer-cancel 时效链 + 编排面五面复测（2026-08-26 夜）

- 范围: rt_dsh_backend / rt_gateway / rt_head_tools（本仓）+ host-callback-bridge / worker 供给链（maestro-preset，另仓收口）
- 用户裁定链: 「maestro 的消息必须到达时 cancel 当前 step，时效性非常重要」→ sink cancel 档 → host 重启装载 → 五面复测全绿

## 1. liaison steer-cancel（本仓 d229fd0）

- `_deliver_liaison`: 对接人在飞时 `session.cancel`（keepInbox abort）→ prompt mode=queue →
  agent loop 把唤醒重类为 next-turn 立即开新回合（wakingAfterAbort 语义）。
- 回执 note 三态: queue「新回合」/ steer「并入在飞回合」/ steer-cancel「打断当前步立即执行」。
- live 证据: dispatch#2 撞在飞对接人 → mode=steer-cancel，回执 7.3s 播报（不等 ~45s 终稿），
  ref/凭证逐字。
- 顺手修: `backend.bus` 从未接 `gateway.bus`（default_factory 私有总线）→ orch.* 观测面全瞎，已接线。
- doctrine 修: 占位符「【凭证…】」被 flash 鹦鹉学舌 → 改「回执逐字转述」规则。
- 遗留: flash 回执保真概率性（同轮一条幻觉假 ref、一条逐字全对）——prose 化回执待裁定。

## 2. 桥直发 sink cancel 档（maestro-preset 7119f2a，装点已齐平）

- 根因: loopback-sink 恒 `mode:'queue'`——通知 parked 在目标回合后一条一回合慢放
  （实测 ack 滞后 7-8 分钟；「回调不发/worker 不拉/乱飞堆加」三症状同源）。
- 修: deliver 前 session.list 查在飞 → session.cancel + queue（与 liaison 同语义）。
  list/cancel 瞬时失败退化纯 queue。文件桥与 HTTP intake 共用 sink 一处生效。
- selftest 37/37（T13 锁 cancel→prompt 顺序）。host 重启后 live 二次实测：消息秒级打断在飞 step。

## 3. 编排面五面复测（重启后全绿）

| 面 | 通路 | 证据 |
| --- | --- | --- |
| 桥直发 | cb-send→intake 46855→sink→steer-cancel | diag-011108 秒级打断；ping 秒达 |
| dais 邮箱 | send-message→check-messages | seq837 回环 |
| 语音全环 | head dispatch→对接人→回执/终稿 | 双模式；seq838/839 read=1+缓冲 |
| dais 正面 worker | new-terminal→start-worker→inject omp→inject 任务 | DAIS_PLANE_OK done 秒回 |
| 终稿回投 | done→邮箱→phase-2→活 head/缓冲 | 无 live head 时 buffering 不丢 |

## 4. worker 供给链诊断结论（修正版）

- **dais 正面链本身通**。此前四次全败是操作者把 Orca 终端错认成 dais 面板
  （列表碰巧有同名终端），对错误对象下了「断链」结论——宣判早于证据。
- 真实弱点仅两处、皆轻: ① new-terminal 间歇空输出（重试即过）；
  ② worker-up harness 注入被 title 读取失败挡掉后**静默跳过仍报 ok**（失败报成功类缺陷）。
- orca-send 适配器: 信封/传输层通（窄腰 40 exports 加载正常，装点三处一致零漂移），
  但 `run:<dais-id>` 跨注册表不互认（orca run 表 ≠ dais run 表）——跨面引用需桥或收敛单注册表。
- `/usr/bin/orca` 是 GNOME 屏幕阅读器同名异物；Orca CLI 实际入口
  `~/.config/orca/linux-orca-cli-shim/orca`。

## 5. 教训（写给后续会话）

- 文档/skill 指的路（start-worker 供给链）撞墙 ≥2 次即退回第一性：终端+harness+直注就是最小链。
- 「omp idle」标题在场 = 最简假设 10 秒可测，别锚定官方链。
- 判「链路死」前必须先排除「对象认错」；面板归属以 PTY 注册进程为准（dais GUI vs Orca IDE）。

## 遗留清单（未做）

- worker-up 两处加固（注入硬失败化 + new-terminal 空输出重试）——待裁定。
- orca/dais run 注册表互认桥——架构级待裁定。
- flash 回执 prose 化（保真概率性）——待裁定。
- audit P1/P2（B2/B3/B4/B5、max_tokens 等）见 dashscope-dialect-audit.md。
- 测试残留: 3 个诊断终端、dais GUI 诊断 tab、run_<redacted> 尾任务、seq840 ping（read=0）、
  网关缓冲 2 条旧终稿。
