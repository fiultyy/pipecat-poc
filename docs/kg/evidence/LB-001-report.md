# LB-001 报告 · 车道B 主干收口（DshBackend lane_mode="b-orca" 生产形态）

> 2026-08-24 21:40–22:05 CST · 交付 commit `464c7b2`（2 文件，+245/−3）
> 范围：`examples/realtime-provider-poc/rt_dsh_backend.py`（b-orca 分支 + 加固）、`tests/test_rt_conformance.py::test_live_lane_b_orca_backend_mainline`
> 规范源：KG 06 §1.2（F7 路由：工作树/终端交付 → orca 车道）· KG 07 §2（A/B 语义映射）

## A. 交付物

**生产形态主干**：head 的 `DshBackend` 以 `lane_mode="b-orca"` 直接驱动真 orca 工作树（此前 live 用例走的是测试侧 `_orca_worker` 编舞替身）。

| 交付 | 锚点 | 内容 |
|---|---|---|
| 派发 | `rt_dsh_backend.py→DshBackend._fanout_orca` | spawn 临时工作树 `vh-{ref}`（agent 默认 **omp**，用户裁定 2026-08-24；`ORCA_AGENT` 可覆盖）；工件契约：worker 逐字写 `final-{ref}.txt` = `调研完成 {凭证} 结论 41%`；`dispatch.artifact/.prompt` 透传 |
| 收终稿 | `→DshBackend._await_orca` | **渲染证明完成判定**：agent 答文本永不进 scrollback 环 → 仅工件**逐字全等**算完成（部分写入不满足）；有界 tui-idle 切片（15s）+ 对话框应答 "1"（≤5）+ **一次** interrupt+重发（settle 窗 20s 后） |
| 取消 | `→DshBackend.cancel` | b-orca 分支：杀本地 phase-2 + `terminal stop` + `worktree rm` |
| 事件 | `orch.dispatch`（带 artifact/agent extra）/ `orch.progress` / `orch.done` | WS4 转发面不变 |

**回归纪律**（红线）：live 用例 teardown 断言无残留（stop + rm + `worktree ps` 复核）；失败 attempt 先取消孤儿 phase-2 再 teardown。

## B. 根因与加固（序列跑 520s 双超时 → 三缺陷）

现象：单跑 13–20s 绿；全文件序列跑两次 attempt 均满 240s 超时（~520s），而失败工作树里工件**总是存在且正确** → 排除宿主负载，定位为结构性静默死亡。

1. **孤儿 phase-2**（测试侧）：失败 attempt 直接 teardown，不取消 `backend._pending[ref]` → 孤儿任务在整个重试期每 ~15s 对**已删除**的工作树发 wait/read CLI 子进程，饿死重试轮。修：失败路径先 `task.cancel()` 再 teardown。
2. **后端缺编舞**：`_await_orca` 无 interrupt+重发（`_orca_worker` 有，全文件 268s 能过的功臣），且瞬态 lane 错误（CLI 挂死被杀 / ok:false busy）直接 raise → **后台任务静默死亡 = 终稿无声丢失**。修：settle 窗后一次 interrupt+重发；瞬态错误预算内重试并以 `orch.progress` 可观测。
3. **lane-A 静默死亡**（tripwire 首捕实录，2026-08-24 21:57 全文件跑）：
   `[dsh-backend] phase-2 aborted: A2aError('a2a error -32602: unknown taskId: t_3973ecc9')`
   —— lane-A attempt 重试间端口复用，上一 attempt 泄漏的 phase-2 轮询到新 server → task 不存在 → raise 死亡。此前完全不可见。修：`unknown taskId` 类终态错误发 progress 后**停止轮询**（终稿不可知），并给 `_pending` 任务挂 done-callback tripwire（stderr 报告死亡，杜绝 unretrieved-exception 静默）。

## C. 验证矩阵（全部本机实测）

| 轮次 | 命令/形态 | 结果 |
|---|---|---|
| 单跑 | `pytest tests/test_rt_conformance.py::test_live_lane_b_orca_backend_mainline` | **1 passed 20.14s** |
| 子集序列（smoke→A/B[dais,orca]→主干） | `-k "lane_b_smoke or ab_lane_final or mainline"` | **4 passed 67.78s** |
| 全文件序列（加固前） | `pytest tests/test_rt_conformance.py` | 1F/6P **520.86s**（双 attempt 超时；tripwire 捕 lane-A 死亡） |
| 全文件序列（加固后 #1） | 同上 | **7 passed 111.34s** |
| 全文件序列（加固后 #2，lane-A 修复后） | 同上 | **7 passed 94.64s · tripwire 零输出** |
| 离线回归（12 文件，`-m "not live"`） | `pytest tests/test_rt_*.py -q -m "not live"`（除 conformance） | **151 passed 56.92s** |
| 残留复核 | `ls ~/orca/workspaces/pipecat-poc/vh-* vo009-*` | 空（无工作树/进程残留） |

worker 工件逐字正确的直接证据（失败轮遗留工作树抽查）：`调研完成 【凭证VH-2125806F】 结论 41%` 等，内容与期望全等。

## D. 遗留与后续

- 全链 E2E（语音→head→双车道 fan-out→回执→WS 播报）未做（Phase 4 前置）。
- WS4 gateway/frp live 证据未采（frp 已移出架构，公网穿透需求待用户定夺）。
- `test_dual_delivery_parity` 逐字节 wire 断言仍属 DSHMSG v1 契约遗留（VO-012 E 节记录，非本票范围）。
- 20+ 未 push 提交（maestro-preset 17 / POC 7 / maestro 2+）待用户批准。
