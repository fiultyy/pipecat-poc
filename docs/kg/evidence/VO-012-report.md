# VO-012 报告 · M5 收口 dogfood（末票，12/12 收口）

> 2026-08-24 00:0x–00:3x CST（晚峰窗口）· 规范源：docs/tickets.md §VO-012、docs/plans/impl-specs.md §VO-012、plan §7 M5 / 报告 §10
> 范围（只许改/建三处，越界零改动）：docs/kg/ 全域文档终态整理、本报告（新）、docs/reports/voice-orchestration-head-report.html（M5 节附 dogfood 结果）
> 代码零改动：src/examples/tests 全部只读（票面红线"发现代码问题→报告记录，不动手"）

## A. 全量回归输出原文

### A.0 命令与轮次

票面命令 = `timeout 1500 .venv/bin/python -m pytest tests/test_rt_*.py tests/test_live_v5_v6.py -q`（13 rt 文件 + live_v5_v6 一并），`tests/test_live_v7.py` 单独跑（串行保护 dais 总线锁与 GLM 容量，E 节既往教训）。晚峰窗口执行（23:38 起）——live 无环境性超时，无需按 E 节先例择窗补跑。

### A.1 离线全量（13 文件，2026-08-23 23:38）

```
$ timeout 1500 .venv/bin/python -m pytest tests/test_rt_a2a_client.py tests/test_rt_conformance.py tests/test_rt_dsh_backend.py tests/test_rt_dsh_lane.py tests/test_rt_fleet_registry.py tests/test_rt_gateway.py tests/test_rt_head_tools.py tests/test_rt_orca_lane.py tests/test_rt_orchestrator.py tests/test_rt_projector.py tests/test_rt_reconnect.py tests/test_rt_router.py tests/test_rt_transcript.py -q
============================== 1 failed, 156 passed in 103.98s (0:01:43) =============================

FAILED tests/test_rt_conformance.py::test_dual_delivery_parity - AssertionError: push wire line drifted:
  'DSHMSG]{"from": "voice-head-a2a", "to": "f5c9", "type": "steer", "ref": "vh-dual-2e9f5a", "body": "双投递对拍 TOKEN-DUAL-2e9f5a 【凭证R-DUAL-2e9f5a】", "msgid": "<id>", "ts": 1787499332857}'
  'DSHMSG]{"from": "voice-head-a2a", "to": "f5c9", "type": "steer", "ref": "vh-dual-2e9f5a", "body": "双投递对拍 TOKEN-DUAL-2e9f5a 【凭证R-DUAL-2e9f5a】"}'
（前 131 字符逐字相同 = from/to/type/ref/body 五元组语义全同；差异仅为尾键 msgid/ts）
```

复跑定罪（3.86s 确定性失败，dais 面健康）：

```
$ timeout 300 .venv/bin/python -m pytest tests/test_rt_conformance.py::test_dual_delivery_parity -q --tb=line
============================== 1 failed in 3.86s =============================
```

**判定**：非代码缺陷、非环境缺陷——`session-send` 已升 **DSHMSG 信封 v2（OF-001，maestro 域已执行）**：落史行自动追加 `msgid`（uuid4）与 `ts`（epoch ms），自述"老键全保留、只增不改（OG5）；老消费者按 ']'+json.loads 解析，忽略未知键即兼容"。本用例 2026-08-23 VO-005 时代写的**逐字节** wire 断言属 v1 契约遗留；v2 语义兼容（五元组全同、收方解析 `json.loads` 忽略未知键）。用例内**语义断言（push_tuple == pull_tuple == envelope、凭证逐字、读即消费）全部通过**，仅 `push_line == line` / `pull_line == line` 两处逐字节断言失败。处置：按票面红线记录不动手；修订建议见 E 节。

### A.2 live V5/V6（补齐票面命令清单，23:52）

```
$ timeout 1500 .venv/bin/python -m pytest tests/test_live_v5_v6.py -q
============================= test session starts ==============================
collected 2 items

tests/test_live_v5_v6.py ..                                              [100%]

======================== 2 passed in 215.70s (0:03:35) =========================
```

### A.3 live V7（23:57–00:05，晚峰窗口全绿）

```
$ timeout 1500 .venv/bin/python -m pytest tests/test_live_v7.py -q
============================= test session starts ==============================
collected 4 items

tests/test_live_v7.py ....                                               [100%]

======================== 4 passed in 447.94s (0:07:27) =========================
```

（对照 VO-007 E 节：晚峰曾以尾部超时告终；本轮晚峰全绿——dais 平面 OF-008 实例锁后总线稳定 + V7 防挂死体系（预算钳制/GLM timeout=90 单次重试/poll_s=2）吸收了慢尾。）

**汇总：离线 156P+1F（信封 v2 演进项，见 E）+ live 6P；除该演进项外全绿。**

## B. dogfood 全链（票面②；驱动=/tmp/vo012_dogfood.py，repo 零改动，只读复用 harness 导出面）

```
== d0: incubation-wizard 自举 supervisor（targets=dsh role=supervisor）==
[wizard rc=0 158s]
  | == 三门报告（回显）==
  |   gate1 术语零暴露      PASS
  |   gate2 灾难底线       PASS
  |   gate3 结构完整       PASS
  | == 孵化回执 ==
  |   name:    m5-closeout-supervisor-000501
  |   version: 1
  |   receipt: {"target": "dsh-supervisor", "name": "m5-closeout-supervisor-000501", "version": 1, "code": "6305", "sessionId": "session-<id>", "mailbox": "agent_m5-closeout-supervisor-000501", "role": "supervisor", "project": "", "preset": "maestro", "marker": "vh-m5-closeout-supervisor-000501"}
  | INCUBATION-RECEIPT]{...同上 receipts...}
  [PASS] supervisor fleet 登记（向导自举真孵化） — code=<seat> sessionId=session-63052cab-2…
== d1: incubate liaison/manager → boot router harness ==
  [PASS] 孵化回执（dogfood liaison） — code=<seat> mailbox=agent_liaison_m5_000501
  [PASS] liaison fleet 登记 — sessionId=session-e9aeac03-9…
  [PASS] 孵化回执（dogfood manager） — code=<seat> mailbox=agent_df_mgr_000501
  [PASS] manager fleet 登记 — sessionId=session-bb0d16b2-f…
  [router harness] port=39709 (grants: cross-project F6)
  [PASS] router registry 在册（supervisor + liaison + manager） — agents=66
== d2: F4 head -> liaison (semantic intent, real GLM) ==
  [PASS] head 调用 dispatch_intent — tools=['dispatch_intent_tool']
  [PASS] phase-1 受理回执 — ref=vh-<redacted> run=run_<redacted>
  [PASS] 回执 ref 在口语回执 — ack='办好了，任务已经交给编排体系。受理凭证号念给你：ref 是 vh-<redacted>，run_id 是 run_<redacted>，凭证'
  [PASS] phase-1 终稿未到（两阶段时序）
== d3/d4: chain watch (liaison F6 -> manager dual-lane dispatch) ==
  [PASS] manager 车道B 派发（orca worktree create） — name=vo012-dfb-000501
  [PASS] manager 车道A 派发（session-spawn + WITNESS 注入）
  [PASS] router journal：F6 推唤醒 push 留痕 — rows=2
== d5: phase-2 final (liaison F10 → head) ==
  [PASS] phase-2 终稿到达（真身 F10 回信） — wait=126s
  [PASS] 终稿 = FINAL_PREFIX + done body — final[:80]='"Agent Final Message":\n\n"Agent Final Message":\nM5 收口 dogfood 域终稿（来源：manager agen'
  [PASS] 双车道 WITNESS 时间戳逐字回传（真执行凭证） — WITNESS-DF-A-1787501746904822124 / WITNESS-DF-B-1787501754421267409
  [PASS] 终稿凭证逐字回显
  [PASS] 终稿含监督员会话码（dogfood 自举凭证）
  [PASS] 播报含凭证/引用标记 — broadcast='M5 收口 dogfood 办好了。先念受理凭证：ref 是 vh-<redacted>，标记【凭证VH-501163E9】，任务号 run_<redacted>…'
== RESULT: PASS (494s) ==
  [teardown] router harness stopped
```

router-journal（state/live-vo012/）F6 推唤醒原文：

```
{"ts":1787501451528,"op":"route","from":"agent_liaison_m5_000501","to":"agent_df_mgr_000501","type":"steer","ref":"vh-<redacted>","delivered":"push"}
{"ts":1787501593324,"op":"route","from":"agent_liaison_m5_000501","to":"agent_df_mgr_000501","type":"steer","ref":"vh-<redacted>","delivered":"push"}
```

### B.1 六步语义对照

| dogfood 步 | 证据 | 核对 |
|---|---|---|
| 向导自举监督员（role=supervisor 场景=M5 收口监督） | wizard rc=0 158s：三门 PASS 回显 + INCUBATION-RECEIPT（target=dsh-supervisor）+ fleet code=<seat> role=supervisor | ✅ |
| liaison 真实编排 | 真孵化 e9ae（mailbox 唯一后缀防误命中）→ F4 邮箱正文 + DSHMSG 唤醒 → 回合首排空（appendix 第 1 条）→ F6 双投递（dais 正文 + router push×2 journal） | ✅ |
| 车道A = dais 消息 DAG | manager bash 流含 create-run/create-task/start-worker + session-spawn wk-dfa + session-send WITNESS 注入；worker 真执行回投（WITNESS-DF-A-1787501746904822124 为 `date +%s%N` 真值） | ✅ |
| 车道B = orca 终端/worktree | worktree create vo012-dfb-000501（agent=claude，repo=path 主 checkout，setup=skip）→ 有界 tui-idle 切片 + final.txt 文件产物契约（WITNESS-DF-B-1787501754421267409 = worktree 内 `date +%s%N` 真值）→ manager 侧 stop+rm 清理 | ✅ |
| 终稿回传核验（FINAL_PREFIX+凭证+ref） | F10 = FINAL_PREFIX 前缀 + [ref:vh-<redacted>] + 【凭证R-M5-1201】逐字 + 双 WITNESS 逐字 + 监督员码 6305；播报回合含 ref | ✅ |
| 全链真实执行 | 4 个真 GLM agent 回合（head×2 + liaison×2 + manager×1）+ 1 个 fleet 工厂 worker + 1 个 orca worktree claude agent；无替身无 mock | ✅ |

### B.2 驱动工程事实（复用与新增）

- 时序铁律（V7 同款）：**孵化在先、router harness 后启**（reattach 需见全 fleet）；wizard 端点须为带 incubate 面的 `_boot_incubator` 临时实例（`boot_router_harness` 的 http face 只挂 router 无 incubate）。
- **跨 project F6 需显式 grants**（liaison project=voice-head-m5 → manager project=lane-df；`boot_router_harness(grants=[{from,to,ts}])`）——VO-007 双域恰好同 mailbox 域内互通未暴露此路径，本轮为 grants 授权路径的首次 live 实证。
- 观察面用全量 `session.history`（maxMessages=4000）——harness `bash_trace` 封装只取尾 10 条，观察全回合命令流不够。

## C. 文档终态（票面③）

- **loc 漂移修齐（9 处）**：00-INDEX（Orchestrator:189→178、Projector:63→95 及 SOURCES:27→33/build_prompt:99→131/_call_glm:122→154/project:159→191、session-send v2 全套 :4/:6/:11/:21/:32）；03（project:159→191、gatesFn:106-109→306-307/:248）；05（Projector:63→95）；06（SOURCES:27→33、project:159→191、session-send §2.2 锚表 v2 化）。
- **new→loc 归档（18 处）**：05 模块表六行（rt_gateway 三类、OrcaLane:41、ROLE_TEMPLATES:60、registry.js:45、createRouter:79、向导 SKILL.md）+ 里程碑表 M2/M4/M5 与 W5/车道B 行；06 W5.1a–W5.4 全序列行 + registry/heartbeat/router 三处 + ROLE_TEMPLATES 注释锚；07 B.1–B.5 全行 + 总图/类锚 + 标题转"建成态"；03 W3.4/W5.1a 行 + W5.1 签名锚 + 向导入口 new→loc；01 总图 OrcaLane + doctrine 落位行 + 尾注后续项勾销；02 表三行（W3.4/W5.1b/c/W5.2）。09（OF 规划域）保持 new 语义不动（规划未执行项）。
- **锚点回归验证**：更新后 15 个关键锚全部命中（rt_projector 60/95/191/33、rt_orchestrator 178、rt_orca_lane 41、rt_gateway 488/102/433、registry.js 45/125/160、http-server 79/306、session-send 32）。
- **ledger round 17 收口行**：`docs/kg/evidence/ledger-carryover-round6.md:35`（12/12 全 ☑、M0→M5 里程碑全通、回归+dogfood+文档终态+teardown 四段）。
- **report.html**：§M5 节更新（状态 待做→完成 + dogfood 结果摘要）+ 里程碑总览 M5 行同步。

## D. 验收①–③逐条

| # | impl-specs 验收 | 证据 | 判定 |
|---|---|---|---|
| ① | 13+ 文件全绿（含 V7/车道B/网关用例） | A 节：离线 156P（13 rt 文件全绿除 1F）+ live_v5_v6 2P + live_v7 4P；唯一 1F 为信封 v2（OF-001，OG5 合规演进）击穿 VO-005 时代逐字节断言——语义断言全过、非代码缺陷、红线记录不动手（E 节） | ✅（带 E 节演进项） |
| ② | dogfood 全链一次成功（监督员真孵化+真派发+终稿回传） | B 节：supervisor <seat> 真孵化（三门 PASS）→ F4 ref=vh-<redacted> → F6 push×2 → A/B 双车道真执行（双 WITNESS 纳秒时间戳）→ F10 FINAL_PREFIX+凭证+监督员码逐字 → 播报；494s PASS | ✅ |
| ③ | 文档终态（KG 全 loc 化、账本收口） | C 节：9 漂移修齐 + 18 new→loc + 锚点回归验证 + ledger round 17 | ✅ |

## E. 偏差与发现（本票钉死）

1. **DSHMSG 信封 v2 vs VO-005 逐字节断言**（A.1 唯一 1F 的完整定性）：`~/.dsh/maestro/bin/session-send` 已按 OF-001 升 v2（自动 msgid/ts、`--msgid` 重发保号、msg-dedup 助手、OG5 老键只增不改）。VO-005 用例的 `push_line == line` 逐字节契约属 v1 时代。**修订建议（不动手，留编排者裁量）**：断言改为"剥 DSHMSG 前缀后 json.loads，五元组子集断言 + 容忍 msgid/ts 附加键"——即与 OG5 消费者语义对齐；或改构造预期行时同样注入 msgid/ts。同文件内其余断言（五元组相等、凭证逐字、读即消费）已按该语义写，无需动。
2. **时序铁律**：registry 内存表仅 reattach/register 填充——harness 必须后于孵化启动（B.2）；驱动首轮把 boot 放孵化前导致 registry 断言失败，已按 V7 时序修正。
3. **跨 project grants 首次 live 实证**（B.2）：VO-004 的 scope 模型（同 project 互通/跨 project 显式授权）在跨域场景真实工作。
4. **晚峰表现改善**：V7 晚峰 447.9s 全绿（对照 VO-007 E 节晚峰各轮尾部超时）——dais 实例锁（OF-008）+ V7 防挂死体系共同见效；live 无需择窗补跑。
5. **teardown 忙闸**：wk-dfa worker 会话 purge 撞 409（事件日志 5min 内写入，V7 同款已知）；settle 后补清（F 节复验）。

## F. 残留核验（红线：不留进程/worktree/终端残留）

| 项 | 结果 |
|---|---|
| orca worktree（vo012-dfb-*） | 0（manager 侧 rm + 驱动兜底 sweep 双保险） |
| git worktree | 0 |
| 端口 39709 / 8790 | 0（router harness 与 wizard incubator 均已停） |
| 残留进程（vo012/wizard/live-vo012） | 0 |
| wk-dfa worker 会话 | 忙闸 409 → settle 后补 purge ✅（registry ok / disk rm ok / projcache ok / fleet removed 2313） |
| 孵化 agent（supervisor <seat> / liaison <seat> / manager <seat>） | 按 fleet 保留（红线） |

## G. 范围与红线自查

- 只改 docs/kg/（00/01/02/03/04/05/06/07 + evidence/ledger + 本报告）与 docs/reports/voice-orchestration-head-report.html；**src/examples/tests 零改动**（dogfood 驱动在 /tmp，repo 外）。
- 未 push、未 git commit（编排者统一合并）；未 spawn dais 实例；relay fleet code <seat> 未动；协议常量零触碰（驱动全部 import 复用 FINAL_PREFIX/[ref:]/【凭证】既有定义，G5）。

---
done body:
`通过;报告:docs/kg/evidence/VO-012-report.md;测试:156P+1F(信封v2演进项E节)+live6P全绿;备注:dogfood全链494s双WITNESS,KG全loc化,12/12收口`
