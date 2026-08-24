# N7 · 车道B · orca ADE：OrcaLane 封装（方法级，建成：VO-008 全方法 + VO-009 live+A/B 对拍 + LB-001 主干收口）

> 上游：`docs/plans/voice-orchestration-head-plan.md§4`（v2）· 索引：[00-INDEX.md](00-INDEX.md)
> 状态：**建成**——VO-008 全方法 + VO-009 live/A-B 对拍 + LB-001 主干收口（DshBackend `lane_mode="b-orca"` 生产形态，证据 [evidence/LB-001-report.md](evidence/LB-001-report.md)）。
> R1 纪律：二进制硬编码 `orca-ide`（`/usr/bin/orca-ide`，内部自称 `orca <command>`）；**严禁裸 `orca`**（`/usr/bin/orca` = GNOME 屏读器）。

## 0. 对接总图

```
manager（N6§4）/ 编排 skill（orchestration / orca-cli）
   └─ OrcaLane〔loc:examples/realtime-provider-poc/rt_orca_lane.py:41→OrcaLane〕──subprocess──▶ orca-ide CLI
        ├─ spawn：worktree create --agent --prompt（工作树级交付）
        ├─ 监控：terminal read / wait --for exit|tui-idle（有界）
        ├─ 干预：terminal send --text/--enter/--interrupt
        └─ 汇总：worktree ps（跨工作树编排摘要）
与车道A conformance 对拍：同一意图 A/B 双车道执行 → 终稿均 FINAL_PREFIX+同 body（N5§6 方法论复用）
```

## 1. OrcaLane 类设计

`〔loc:examples/realtime-provider-poc/rt_orca_lane.py:41→class OrcaLane〕`（✅ VO-008 建成，25 测；live 冒烟+A/B 对拍=VO-009）

```python
class OrcaLane:
    """orca-ide CLI 的 async 封装；形制对齐 DaisLane（_run + 每方法=1个CLI契约）。
    全部命令加 --json 走机器可读输出；不共享 DaisLane 的总线锁（不同执行面）。"""

    BIN = "orca-ide"          # 硬编码；禁裸 orca（R1）

    async def _run(self, *args: str, timeout_s: float | None = None) -> dict | str:
        """subprocess + asyncio.wait_for；--json 输出自动 json.loads；
        失败抛 OrcaLaneError（带 stdout/stderr 截断），形制对齐 DaisLaneError。"""

    # ---- 探活 ----
    async def status(self) -> dict:
        """〔cli:orca-ide status〕app/runtime/graph 三态就绪；live 测试前置探针（对齐 bus_healthy）。"""

    # ---- spawn（车道B 的 create-run+start-worker 等价物）----
    async def spawn_worktree(self, name: str, repo: str, agent: str,
                             prompt: str, *, base_branch: str | None = None,
                             setup: str = "run") -> dict:
        """〔cli:orca-ide worktree create --name --repo id:<id>|name:<n>|path:<p>
        --agent <agentId> --prompt <text> --setup run|skip|inherit --json〕
        一次调用 = 建工作树 + 起 agent + 注入初始 prompt（≈车道A create-run/create-task/
        start-worker 三连 + send-message 的合体）。agent id 来自 worktree create 文档面
        （codex/claude 系）；返回 {worktreeId, terminalHandle?}。"""

    # ---- 监控（车道B 的 read-worker/worker_done 等价物）----
    async def terminal_list(self, worktree: str | None = None) -> list[dict]:
        """〔cli:orca-ide terminal list〕活终端句柄表（路由用）。"""
    async def read(self, terminal: str, *, lines: int = 40) -> str:
        """〔cli:orca-ide terminal read〕有界尾读（≈read-worker --after；增量游标语义以
        skills get orca-cli 指南为准，开工时钉死）。"""
    async def wait(self, terminal: str, *, what: str = "exit",
                   timeout_ms: int = 30000) -> dict:
        """〔cli:orca-ide terminal wait --terminal <h> --for exit|tui-idle
        --timeout-ms <ms> --json〕有界等待（纪律对齐 dais：必带 timeout，绝不裸等）。"""

    # ---- 干预（车道B 的 answer/interrupt 等价物）----
    async def send(self, terminal: str, text: str, *, enter: bool = True) -> None:
        """〔cli:orca-ide terminal send --terminal --text --enter〕注入输入/应答提示。"""
    async def interrupt(self, terminal: str) -> None:
        """〔cli:orca-ide terminal send --interrupt〕本地中断（≈answer --interrupt）。"""
    async def stop(self, worktree: str) -> None:
        """〔cli:orca-ide terminal stop〕停工作树全部终端（manager 超时上抛前的兜底）。"""

    # ---- 汇总 ----
    async def worktree_ps(self) -> dict:
        """〔cli:orca-ide worktree ps〕跨工作树编排摘要（≈check-status 聚合）。"""
```

## 2. A/B 车道语义映射表（conformance 对拍基准）

| 语义 | 车道A dais | 车道B orca | 备注 |
|---|---|---|---|
| 派发 | create-run→create-task(--dep)→start-worker + send-message | worktree create --agent --prompt | B 无显式 DAG；跨工作树依赖归 manager 编排 |
| 有界等待 | check-messages --timeout-ms / worker_done 块匹配 | terminal wait --for exit\|tui-idle --timeout-ms | 双方纪律：必带界，绝不裸等 |
| 尾读 | read-worker --after cursor | terminal read | B 的增量游标语义开工时钉死 |
| 应答 | answer --text/--enter/--interrupt | terminal send --text/--enter/--interrupt | 旗标面同构 |
| 状态汇总 | check-status --run-id | worktree ps | |
| 终稿回信 | send-reply → 邮箱（[ref:] 信封） | 同车道A（回信统一走 dais 邮箱） | **终稿通道不分会道**：B 的产物也经 manager→liaison→head 邮箱链回传 |
| 取消 | fail-dispatch（仅 ctx_）/ 杀本地 phase-2 | terminal stop / send --interrupt | 远端语义与 V6 结论一致：本地打断≠远端取消 |

**对拍判定**：同一意图分投 A/B → 两边终稿均为 `FINAL_PREFIX + done body`、凭证【凭证…】逐字一致（复用 `tests/test_rt_conformance.py` 框架，lane 工厂参数化）。

## 3. 实施与验证序列

| 步 | 交付 | 验证 | 完成判定 |
|---|---|---|---|
| B.1 | `orca-ide skills get orca-cli` 拉指南 + read 增量语义钉死 | ✅ round 8-9 探针（KG 07 建文即果） | 命令面与 §1 表一致或修订表 ✅ |
| B.2 | OrcaLane 全方法（status/spawn/read/wait/send/ps） | ✅ VO-008 `tests/test_rt_orca_lane.py` 25 测（mock CLI --json 输出） | 语义映射表全覆盖 ✅ |
| B.3 | live 冒烟：真 spawn 1 worktree（codex/claude）→ wait → read | ✅ VO-009 `test_live_lane_b_smoke`（文件产物契约 13.5-27s；host 掉线确定性 skip） | 全程有界超时无裸等 ✅ |
| B.4 | A/B conformance 对拍 | ✅ VO-009 扩 `tests/test_rt_conformance.py`（lane 工厂参数化） | 双车道同终稿（统一 dais 邮箱链回传）✅ |
| B.5 | 分派策略落 manager 模板（N6§1.2） | ✅ VO-007 manager appendix 第 3 条（车道选择断言入 V7 六步） | 车道选择断言入 V7 ✅ |
| B.6 | 主干收口：DshBackend `lane_mode="b-orca"`（§4） | ✅ LB-001 `test_live_lane_b_orca_backend_mainline`（live 双租约；全文件序列 7P 94–111s，单跑 20s） | 派发→工件终稿→teardown 零残留 ✅ |

## 4. 主干收口：DshBackend b-orca（LB-001，commit `464c7b2`）

head 工具面直接驱动真工作树（不再借测试侧 `_orca_worker` 编舞替身）：

- **派发**〔loc:examples/realtime-provider-poc/rt_dsh_backend.py→DshBackend._fanout_orca〕：spawn `vh-{ref}` 临时工作树（agent 默认 omp，用户裁定 2026-08-24；`ORCA_AGENT` 可覆盖）；**工件契约** = worker 逐字写 `final-{ref}.txt`（`调研完成 {凭证} 结论 41%`）——agent 答文本永不进 scrollback 环，完成判定只认工件**逐字全等**（渲染证明）。
- **收终稿**〔loc:…→DshBackend._await_orca〕：有界 tui-idle 切片（15s）+ 对话框应答 ≤5 + settle 窗 20s 后**一次** interrupt+重发；瞬态 lane 错误（CLI 挂死/ok:false busy）预算内重试并上 `orch.progress`——phase-2 **永不静默死亡**（`_pending` 任务挂 done-callback tripwire，死亡报 stderr）。
- **lane-A 对称纪律**：A2A task 被服务端遗忘（-32602 unknown taskId）→ progress 后停轮询（终稿不可知，不空转到预算耗尽）。
- **取消**〔loc:…→DshBackend.cancel〕：杀本地 phase-2 + `terminal stop` + `worktree rm`。
- 测试侧红线：失败 attempt 先取消孤儿 phase-2 再 teardown（孤儿会整轮对已删工作树发 wait/read CLI 调饿死重试轮）；teardown 断言工作树/进程零残留。

