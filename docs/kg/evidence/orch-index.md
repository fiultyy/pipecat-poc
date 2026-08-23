# 编排索引 · orch-index（摘要索引 → 文档库）

> 用途：**任何会话（含编排者被 compact/交接后的继任者）两跳重建全部态势**。
> 恢复协议：goal prompt → 本文件（一屏态势）→ 按需读指向文档（全文细节）。
> 更新纪律：每条线状态变化（合并/打回/到期/新派）**当回合**更新本文件对应行。

## 1. 活跃线（heartbeat 视角）

| 线 | 状态 | 下一个事件 | 完成时动作 |
|---|---|---|---|
| **VO-012** 收口 dogfood | **已派发**（term_0e694d5c，23:4x；末票。晚峰策略=离线全量先行+live 择低载窗补跑，壳 1500） | VO-012-report.md 落地 → relay 唤醒 | 门禁→merge→☑→12/12 终态 |
| ~~VO-007~~ | **☑ 已合并 `8845164`**（低载窗 4/4 全绿+晚峰敏感 E 节钉死；离线回归 84P 复验；当前文件完整 live 复验=VO-012 天然承载） | — | — |
| ~~W6 规划+执行~~ | 方案合入 `b51bf05`/`cb45e85`；**执行波 8/10 ☑**（OF-001/2/3/5/6/7/8/10，maestro 域 2f220c4..4652658；OF-004/009 按计划持有）；记账入 `44c5e02` | — | — |
| **relay <seat>** 第三期 | **23:48 re-arm**（第二期 40/40 到期按设计自灭；第三期看守 VO-012 报告+merge，30 轮预算） | VO-012 报告/merge 回投；30 轮到期 | 到期若 VO-012 未完 → re-arm |

> 最近核实：23:45（VO-007 ☑ 11/12 / VO-012 已派 / W6 8/10 执行波记账完成）。maestro-bridge skill 描述已含 session-send 直投+steer 两段式=OF-003 落地痕迹。下轮 goal 先重新核实再动作。

> 最近核实：22:31（W6 合入 b51bf05 / VO-007 整改落地待重跑 / dais 新实例健康 / relay 22 轮）。注意：e858 INDEX N9 与我编辑撞车过一次（它先落），多主并发写共享文件需 OF-001 msgid+OF-005 数据化的又一个实录。下轮 goal 先重新核实再动作。

## 2. 票板快照

VO 10/12 ☑（001-006,008,009,010,011）。余：**007 在飞、012 待派**。
门禁口径：`timeout 900 .venv/bin/python -m pytest tests/test_rt_*.py tests/test_live_v5_v6.py -q`（live 串行防邮箱互踩）。

## 3. 文档库地图（全文在哪）

| 要什么 | 读哪 |
|---|---|
| 缺陷台账 17 条（D-01..15） | `docs/kg/08-defects-ledger.md` |
| W6 加固规划（待 e858 产出） | `docs/kg/09-orch-hardening-plan.md`（暂缺） |
| W6 使命书（主题A/B 全文） | `docs/kg/evidence/handoff-w6-hardening.md` |
| 票面/验收/派发协议 | `docs/tickets.md` · `docs/plans/impl-specs.md` · `docs/plans/dispatch-plan.md` |
| 过程台账（事故实录） | `docs/kg/evidence/ledger-carryover-round6.md`（cp 01-16） |
| 各票执行报告 | `docs/kg/evidence/VO-XXX-report.md` |
| relay 使命 | `/tmp/vo-relay-brief2.txt`（第二期）；fleet.json code=<seat> |

## 4. 回程与唤醒通道（唯一权威）

- 任何 agent → 编排者：`~/.dsh/maestro/bin/session-send <code> session-<id> done <ref> '<body>'`（DSHMSG 直投回合队列=推唤醒；**勿用 cb-send**，落文件桥不可达）
- 编排者 → 任何 agent：session-send `<code>` + 完整 mission 简报（自包含，勿引用对话）

## 5. 红线总表（压缩版）

在飞文件勿碰：`tests/test_live_v7.py`、`examples/realtime-provider-poc/live_v5_v6_dsh.py`（VO-007 agent 所有）· 不 push · agent 不 git commit（编排者统一合并）· 不 spawn dais 实例 · live 测试同邮箱域禁止并发实例 · relay <seat> 勿动。

## 6. 已知陷阱（新会话必读）

omp TUI 帧无响应文本（三源监控：`~/.omp/logs` agent_end + git status + 报告落地）· live 轮询预算须 ≥实测 P95×2（D-12）· urllib 回环须剥代理（D-15）· dais 重建必带 `--features orchestration`（D-01，strings 判别）。
