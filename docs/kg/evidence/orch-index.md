# 编排索引 · orch-index（摘要索引 → 文档库）

> 用途：**任何会话（含编排者被 compact/交接后的继任者）两跳重建全部态势**。
> 恢复协议：goal prompt → 本文件（一屏态势）→ 按需读指向文档（全文细节）。
> **状态：VO 波次已收口（12/12 ☑，2026-08-24 00:4x，终态提交 `b93f936`）**。本文件转为终态存档；新波次（W6 余量 OF-004/009 tests 面）另起索引。

## 1. 终态快照（收口态）

| 线 | 终态 |
|---|---|
| **VO-012** M5 收口 dogfood | **☑ `b93f936`**（dogfood 全链 494s 双 WITNESS：supervisor <seat> 真孵化→F4/F6→A/B 双车道真执行→F10 FINAL_PREFIX+凭证逐字→播报；信封 v2 parity 断言由编排者修为 OG5 子集语义；门禁=连续 2×156P+1S 全绿 + live 2P） |
| **VO-007** | ☑ `8845164`（低载窗 4/4 全绿+晚峰敏感 E 节钉死；VO-012 dogfood 已晚峰复验 447.9s 绿——实例锁+防挂死体系见效） |
| **W6 加固** | 方案 `b51bf05`/`cb45e85`；**执行波 8/10 ☑**（OF-001/2/3/5/6/7/8/10，maestro 域 2f220c4..4652658）；OF-004 ◐中期持有、OF-009 tests 面 ◐待窗口；记账 `44c5e02` |
| **relay <seat>** 第三期 | 值守中（收口后无新事件将自然到期自灭；无需 re-arm） |
| 票板 | **VO 12/12 ☑**（docs/tickets.md）· 门禁口径 `timeout 1500 壳`（VO-012 E 节教训） |

## 2. 交付物地图（文档库）

| 要什么 | 读哪 |
|---|---|
| 全票执行报告 | `docs/kg/evidence/VO-001..012-report.md`（12 份） |
| 缺陷台账（17 条含修复态） | `docs/kg/08-defects-ledger.md` |
| W6 加固方案+执行态 | `docs/kg/09-orch-hardening-plan.md` |
| 过程台账（round 1-17） | `docs/kg/evidence/ledger-carryover-round6.md` |
| 票面/验收/派发协议 | `docs/tickets.md` · `docs/plans/impl-specs.md` · `docs/plans/dispatch-plan.md` |
| 编排经验（relay 模式/门禁纪律/晚峰预算） | 本文件 §4-5 + VO-007/012 报告 E 节 |

## 3. 回程与唤醒通道（唯一权威，收口后仍有效）

- 任何 agent → 编排者：`~/.dsh/maestro/bin/session-send <code> session-<id> done <ref> '<body>'`（DSHMSG v2 直投=推唤醒；**勿用 cb-send**）
- 编排者 → 任何 agent：session-send `<code>` + 自包含 mission 简报

## 4. 红线总表（压缩版，新波次沿用）

不 push · agent 不 git commit（编排者统一合并）· 不 spawn dais 实例（实例锁已就位=`~/.local/state/dais/instance.lock`）· live 测试同邮箱域禁并发实例 · 门禁=timeout 壳内全量复跑全绿才合并（≥2 次，防 laneB 类瞬态）· 晚峰（21:30 后）live 链预算 ×1.5 余量。

## 5. 已知陷阱（新会话必读）

omp TUI 帧无响应文本（三源监控：`~/.omp/logs` agent_end + git status + 报告落地）· live 轮询预算 ≥实测 P95×2（D-12）· urllib 回环剥代理（D-15）· dais 重建必用 `dais-build`（strings 断言，D-01 已解）· TUI 缓冲不保留 verdict（用 agent 状态文件问询，VO-012 先例）· agent auto-compaction 可静默 45min+（R 态 CPU 40% = 在算，非挂死）。
