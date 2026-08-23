# VO-003 证据：fleet 扩展 + registry/reattach + 生命周期

- 日期：2026-08-23（round 7）
- 票：`docs/tickets.md` VO-003 · 规范：`docs/plans/impl-specs.md` VO-003 · 方法级：KG 06 §1.5（fleet schema + reattach 算法）§1.6（状态机迁移表 + 唤醒模型）
- 范围（两处，越界零改动）：`~/.dsh/plugins/a2a-profile-server/registry.js`（新建）、`tests/test_rt_fleet_registry.py`（新建，主仓）。VO-002 已落改动未触碰；未 git commit。

## 改动摘要

**registry.js**（新，导出 `createRegistry({fleetPath, journalPath?, loopback})`）：

| 面 | 实现 |
|---|---|
| fleet 读写 | `readFleet/writeFleet`（tmp+rename 原子写，形制对齐 profile-store/maestro fleet 纪律）；`updateEntry(code, fields)` 合并写扩展五键（`FLEET_EXT_KEYS` 导出对账），不破坏既有键，unknown code 抛错 |
| reattach | 遍历 fleet `role≠worker` 条目（无 role 键 = VO-002 前老 worker 条目，跳过）→ loopback `session.list`（`{items:[{sessionId}]}` 契约形制）比对：在 → 内存表登记 `{code, sessionId, mailbox, role, project, state:"reattached", lastHeartbeat:null}`；失 → fleet 条目标 `state:"retired"`（既有键不动）+ journal `{ts, op:"orphan", code, sessionId}` |
| 生命周期 | `transition(code, to)` 按 `LIFECYCLE` 表校验（spawn→arm→ready→serving→retired；reattached→serving/retired 为恢复态出口），非法抛 `illegal transition X → Y`（形制对齐 task-store）；成功 journal `{ts, op:"lifecycle", code, from, to}`；→retired 同步落 fleet.state（持久标记，profile 保留可复活） |
| 心跳 | `heartbeat()` = router 侧 loopback `session.list` 一次探活内存表：alive → 更新 `lastHeartbeat`、`arm→ready` 晋升（§1.6 首次心跳）；dead → 只报告不动状态（孤儿判定归 reattach/显式 retire）。**模块零定时器零自发轮询**（唤醒模型钉死：dsh 会话非常驻轮询者，探活一律 router 侧外部驱动） |
| journal | `router-journal.jsonl`（默认 fleet 同目录）：task-store JSONL 形制（`{ts, op, ...}` append-only + 1MB 轮转）；本票只记 orphan/lifecycle 事件 |

**tests/test_rt_fleet_registry.py**（新，5 用例）：node 子进程 driver `import` 真实 registry.js（非契约镜像）跑四场景 + 静态断言；mock loopback 记录全部调用；fleet/journal 落 tmp_path。

## A. 测试输出原文

```
$ .venv/bin/python -m pytest tests/test_rt_fleet_registry.py -q
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.0.0
rootdir: ~/workspace-claw-02/pipecat-poc
configfile: pyproject.toml
plugins: anyio-4.14.0, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=function, asyncio_default_test_loop_scope=function
collected 5 items

tests/test_rt_fleet_registry.py .....                                        [100%]

============================== 5 passed in 0.45s ==============================
```

## B. 回归（现行不破坏）

```
$ cd ~/.dsh/plugins/a2a-profile-server && node selftest.mjs   →   22 passed, 0 failed
$ .venv/bin/python -m pytest tests/test_rt_a2a_client.py tests/test_incubators_real.py tests/test_rt_fleet_registry.py -q
tests/test_incubators_real.py ...                                        [ 61%]
tests/test_rt_fleet_registry.py .....                                    [100%]
============================== 13 passed in 2.43s ==============================
```

## C. 验证目标对照（impl-specs VO-003 ①–④）

| # | 验收 | 证据 |
|---|---|---|
| ① | fleet 读写含扩展五键（temp 副本，原子性） | `test_fleet_ext_keys_atomic_rw`：五键合并落盘、既有键（sessionId/title）与旁条目保留、无 `.tmp-` 残留、unknown code 拒绝 |
| ② | reattach：会话在→reattached；会话失→retired + journal `{op:"orphan"}` | `test_reattach_orphan_and_live`：liaison 登记 `state:"reattached"` 全键视图；manager 孤儿→fleet `state:"retired"` + journal 行含 code/sessionId/ts；worker 与无 role 老条目零副作用 |
| ③ | 状态机全迁移可达且非法迁移拒绝 | `test_lifecycle_transitions`：spawn→arm→ready→serving→retired 全链 + journal from/to 序列；spawn→ready / retired→serving / arm→serving 三路非法抛 `illegal transition`；unknown agent 抛错；→retired 落 fleet 标记；reattached→serving 合法 |
| ④ | 心跳=router `session.list` 探活，无 agent 自发轮询假设 | `test_heartbeat_router_driven`：每次 heartbeat 恰一次 `session.list`、arm→ready 晋升且不重复、死会话只报告不改态、150ms 观察窗零自发调用；`test_no_agent_side_polling_by_design`：源码零 setInterval/setTimeout |

## D. 设计决策与遗留

- **reattached 态**：KG §1.5 规定 reattach 登记用 `state:"reattached"`（非状态机五态）；按"等效 ready"处理，出口 = serving（首次投递，VO-004 消费）/retired——已在 LIFECYCLE 表注明。
- **heartbeat 对死会话只报告不动状态**：孤儿判定职责归 reattach（启动）与显式 retire（退场）；运行中探活不越权改态，避免与 §1.6 触发表冲突。
- **无 role 键老条目按 worker 跳过**：VO-002 前的存量 fleet 条目即 worker 语义，不入 reattach 范围。
- **registry 未接 index.js 装配**：本票范围仅 registry.js + 测试；插件启动时执行 `reattach()` 与 loopback 注入留 VO-004（router 三 RPC 挂载时一并接线，journal 同文件扩 `op:"route"` 行）。
- **内存表 + fleet/journal 持久**：agents 内存表由 reattach/register 重建（KG §1.5"插件启动时执行"即恢复路径）；VO-004 `agents/registry` RPC 直接消费 `agents()` 视图。

done PASS;报告:docs/kg/evidence/VO-003-report.md;测试:5项全绿;备注:registry零定时器,探活全router侧驱动;接线留VO-004
