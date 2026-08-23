# 编排索引 · orch-index（摘要索引 → 文档库）

> 用途：**任何会话（含编排者被 compact/交接后的继任者）两跳重建全部态势**。
> 恢复协议：goal prompt → 本文件（一屏态势）→ 按需读指向文档（全文细节）。
> 更新纪律：每条线状态变化（合并/打回/到期/新派）**当回合**更新本文件对应行。

## 1. 活跃线（heartbeat 视角）

| 线 | 状态 | 下一个事件 | 完成时动作 |
|---|---|---|---|
| **VO-007** manager群+V7 | **三连跑第 2 轮完成（22:59），清理阶段中**（bg_2=sleep300→purge 残留 3dbc/0944；终端 verdict 被 TUI 滚出视野，以 agent 最终报告为准）。报告仍 21:37 旧版勿合并 | 清理完→第 3 轮或收报告→报告更新→relay 唤醒 | 门禁(timeout 900 壳复跑)→ merge→☑→派 VO-012 |
| **W6 规划** | **☑ 已验收合入 `b51bf05`**（09 方案 227 行 9 票 + INDEX N9 由 e858 自注 + 缺陷台账 08 + handoff 使命书 + orch-index）。e858 会话可 retire | — | 次波执行按 09 §9：VO-012 前只做 Wave 0 + OF-009 docs |
| **relay <seat>** 第二期 | 值守中（22/40 轮） | VO-007 报告更新/merge 回投；40 轮到期 | 到期若 VO-007 未完 → re-arm |
| VO-012 | 待派（前置=VO-007☑） | — | 派发主区 omp（收口 dogfood 票） |

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
