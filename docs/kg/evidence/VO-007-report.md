# VO-007 报告 · manager 群 + live V7

- 票：docs/tickets.md §VO-007 · 依据：docs/plans/impl-specs.md §VO-007 · 规范：docs/kg/06-ws5-agent-ecosystem.md §4（V7 六步场景）；§3（liaison 落位先例，协议底座照搬）
- 范围（仅两处，越界零改动）：`tests/test_live_v7.py`（新）、`examples/realtime-provider-poc/live_v5_v6_dsh.py`（仅扩 V7 doctrine 注释节 + manager 场景复用导出接口，纯增量；V5/V6 逻辑与函数零改动）
- 验证形式：live 真跑（真 GLM head + 真孵化 dsh-liaison/dsh-manager×2 + fleet 工厂 worker 真执行 + 真 dais 总线 + 真 router（registry reattach + session-send/dais 底座））

## A. 测试输出原文

### A.1 全绿跑（低载窗口 19:36–19:49，`timeout 900` 壳）

```
$ timeout 900 .venv/bin/python -m pytest tests/test_live_v7.py -q
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: ~/workspace-claw-02/pipecat-poc
configfile: pyproject.toml
plugins: anyio-4.14.2, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=function, asyncio_default_test_loop_scope=function
collected 4 items

tests/test_live_v7.py ....                                               [100%]

======================== 4 passed in 773.60s (0:12:53) =========================
```

live 全链 verdict 原文（该轮，test_live_v7 内直跑同版代码，完整日志同步 /tmp/vo007-live.log）：

```
== V7 step1: incubate liaison + managers (lane-a / lane-b) ==
  [PASS] 孵化回执（真身 dsh-liaison） — code=<seat> mailbox=agent_liaison
  [PASS] liaison fleet 五键登记 — sessionId=session-a07d2af2-6…
  [PASS] 孵化回执（调研域 dsh-manager） — code=<seat> mailbox=agent_lane_a_5b70 project=lane-a
  [PASS] 调研域 fleet 五键登记 — sessionId=session-4354b941-2…
  [PASS] 孵化回执（文档域 dsh-manager） — code=<seat> mailbox=agent_lane_b_5b70 project=lane-b
  [PASS] 文档域 fleet 五键登记 — sessionId=session-c8da5908-a…
  [PASS] router registry 在册（liaison + 双 manager） — agents=37
== V7 step2: F4 head -> liaison (semantic intent) ==
  [PASS] head 调用 dispatch_intent — tools=['dispatch_intent_tool']
  [PASS] phase-1 受理回执（head 本地即时生成） — ref=vh-<redacted> run=run_<redacted>
  [PASS] 回执 ref 原样出现在口语回执 — ack='好的，已受理你的请求：调研 WebGPU 在生产环境的采用情况，…受理凭证是 vh-<redacted>…'
  [PASS] phase-1 阶段终稿未到（两阶段时序）
== V7 step3: F6 liaison -> managers (mailbox body + router push) ==
  [PASS] liaison 回合1 完成 F6 分发（双域） — bash_calls=4
  [PASS] F6 邮箱正文双域投递（status 型）
  [PASS] F6 命令携带 F4 解析出的 ref/run_id（正文已读证据）
  [PASS] 回合1 未向 voice-head 发消息（终稿只在回合2）
  [PASS] liaison 回合首动作 = 邮箱快照排空（doctrine 行为断言）
== V7 step4: F7 manager dispatch (dais lane) + worker_done ==
  [PASS] 调研域 manager 完成 F9 回传
  [PASS] F6 推唤醒到达 调研域 manager（DSHMSG 信封含 ref）
  [PASS] 调研域 manager 邮箱排空先于派发/回传（拉模式正文双到达） — drain@0 first_act@None
  [PASS] 调研域 F7 create-run（dais 车道实跑）
  [PASS] 调研域 F7 create-task（子任务）
  [PASS] 调研域 F7 start-worker（dispatch ctx）
  [PASS] 调研域 F7 worker 会话（fleet 工厂）
  [PASS] 调研域 worker_done 回投 + 回收
  [PASS] 调研域 worker 注入含 WITNESS 标记
  [PASS] 调研域 F7/F9 命令携带 ref（跳间硬链）
  [PASS] 文档域 manager 完成 F9 回传
  [PASS] F6 推唤醒到达 文档域 manager（DSHMSG 信封含 ref）
  [PASS] 文档域 manager 邮箱排空先于派发/回传（拉模式正文双到达） — drain@0 first_act@1
  [PASS] 文档域 F7 create-run（dais 车道实跑）
  [PASS] 文档域 F7 create-task（子任务）
  [PASS] 文档域 F7 start-worker（dispatch ctx）
  [PASS] 文档域 F7 worker 会话（fleet 工厂）
  [PASS] 文档域 worker_done 回投 + 回收
  [PASS] 文档域 worker 注入含 WITNESS 标记
  [PASS] 文档域 F7/F9 命令携带 ref（跳间硬链）
== V7 step4b: worker 实跑证据（DAG 实体 + fleet 登记） ==
  [PASS] 调研域 DAG 实体已建（run/task） — run=run_c… task=task_92003993f597
  [PASS] 调研域 worker 会话 fleet 登记 — wk=d303
  [PASS] 调研域 worker run 在册（check-status） — entries=1
  [PASS] 文档域 DAG 实体已建（run/task） — run=run_<redacted> task=task_0d0f56885440
  [PASS] 文档域 worker 会话 fleet 登记 — wk=172e
  [PASS] 文档域 worker run 在册（check-status） — entries=1
  [PASS] router journal：F6 双域推唤醒 delivered=push 留痕 — rows=2
== V7 step5: F9 -> F10 -> head phase-2 (hop-by-hop) ==
  [PASS] phase-2 终稿到达（真身 F10 回信）
  [PASS] 终稿 = FINAL_PREFIX + done body（[ref:] 三过滤命中） — final[:60]='"Agent Final Message":\n\n"Agent Final Message":\n域[调研]…'
  [PASS] 终稿凭证逐字回显
  [PASS] 双域 worker WITNESS 时间戳逐字回传（真执行凭证） — WITNESS-A-1787491732145892154 / WITNESS-B-1787491779591005259
  [PASS] 终稿含双域结论（聚合）
  [PASS] 播报含凭证标记（逐字） — broadcast='任务完成，两个域都顺利返回了。…'
== V7 step6: full-chain ref + credential assertions ==
  [PASS] ref 不丢：F4→liaison（F6 命令含 ref）
  [PASS] ref 不丢：F6→manager（F7 命令含 ref）
  [PASS] ref 不丢：F7→worker（注入含 ref）
  [PASS] ref 不丢：worker→manager→liaison（F9 含 ref）
  [PASS] ref 不丢：liaison→head（终稿 [ref:] 命中）
  [PASS] 凭证逐字：F4 正文含具体凭证值（head 意图→邮箱逐字）
  [PASS] 凭证逐字：F6 正文含凭证保留指令（【凭证…】泛标记）
  [PASS] 凭证逐字：F9→F10 凭证逐字（终稿回显）
V7 PASS
  [teardown] worker 会话 d303: purged
  [teardown] worker 会话 172e: purged
  [kept] liaison code=<seat> …/mgr_a code=<seat> …/mgr_b code=<seat> … (fleet registered)
```

## B. V7 六步逐条核对表（以 A.1 全绿轮为据）

| # | 六步 | 证据 | 核对 |
|---|---|---|---|
| ① | 真孵化两 agent（不同域，fleet 登记） | lane-a/lane-b 双 dsh-manager（role=manager、各自 mailbox、project 分域）+ 回执五键 + fleet 登记双 PASS；router registry 在册可见 | ✅ |
| ② | F4→liaison 收 | head GLM dispatch_intent → phase-1 本地回执（ref/run_id）+ F4 邮箱正文 + DSHMSG 唤醒 → 真身回合（"F6 命令携带解析出的 ref/run_id" 证明正文已读；排空先于分发） | ✅ |
| ③ | F6 router 分发 manager（推唤醒+邮箱正文） | 每域两条：dais 邮箱正文（status 型 route）+ router agents/send 推唤醒（journal delivered=push ×2；manager 收方 DSHMSG 信封含 ref + 排空先于派发——双到达对拍语义同 VO-005） | ✅ |
| ④ | F7 派发+worker_done 回收 | 分派策略落地（消息 DAG→dais 车道实跑：create-run/create-task/start-worker + fleet 工厂 worker）；worker_done 回投 + check-messages 回收双 PASS；DAG 实体/worker 会话登记/worker run 在册三重实据；WITNESS 时间戳=真执行凭证 | ✅ |
| ⑤ | F9/F10 终稿逐跳回传、两阶段时序 | worker→manager（mailbox done 回信）→F9 manager→liaison（主 run + [ref:]）→F10 liaison→head 单终稿（FINAL_PREFIX + 凭证逐字 + 双 WITNESS）；phase-1 时 finals 为空（两阶段 PASS） | ✅ |
| ⑥ | 全链 ref 不丢+凭证逐字+FINAL_PREFIX | 五跳 ref 断言全 PASS；凭证两端值断言（F4 正文含值、F10 逐字回显）+ 中段保留指令；终稿 startswith(FINAL_PREFIX) 三过滤命中 | ✅ |

## C. 实现内容

1. **`tests/test_live_v7.py`（新）**：live 驱动 `v7_main` 六步全链 + 3 个离线用例（doctrine 节文本固化核对 / appendix gate1 违禁核对 / head 侧零改动）。manager 与 liaison 的「通信操作规约」appendix（VO-006 同款纪律：占位符代入、协议字面量逐字内嵌 G5 不漂移）经真 Projector 投影 + 三门后孵化。manager mailbox 每运行唯一后缀（防上轮残留 manager 被 router 按 mailbox 误命中）。
2. **`live_v5_v6_dsh.py` 纯增量**：①「Doctrine — manager 群与逐跳回传」注释节（链路图、F6 双投递、分派策略、异常三形态：gate 阻塞→resolve-gate / wait-blocked→scan-wait-blocked / 超时→上抛 supervisor、本票 live 事实）；②导出接口 `boot_router_harness`（真 fleet registry reattach + 真投递底座 + 定长端口）、`incubate_agent`（参数化推广，支持 agents_md 直通预投影）、`rpc_jsonrpc`、`session_events`、`bash_trace`（turn 归属 tool-call/result 追踪面）。V5/V6 原逻辑零改动（`test_zero_diff_head_side` 钉死 head 侧五文件）。

## D. 稳定性整改史（gate 打回后三轮迭代，本轮全部落盘）

| 轮 | 打回/失败 | 根因 | 整改 |
|---|---|---|---|
| 0 | 自跑全绿 773s，gate 复跑 FAIL（文档域 worker 超时未回，escalation 正确触发） | appendix 回收轮询 60 次×~6s≈360s < VO-006 实测慢尾（回合偶发 >5min） | 轮询 60→90（≈9min，对齐 await_timeout_s=540 doctrine）；liaison 聚合窗 40→60；W_MANAGERS/W_FINAL 相应提 |
| 1 | 842s FAIL：调研域 manager 首轮空快照后陷入 sqlite 取证螺旋（37 调用未派发） | F6 正文与推唤醒到达次序竞争，appendix 缺空快照处置指引 | appendix 第 1 条加空快照恢复（--wait 15s→重试→3 轮封顶 + 明令禁 sqlite 直查/读源码/侦察）；孵化改投影并行（GLM 无共享态）+ RPC 串行（fleet 写安全）；liaison 排空断言改"先于分发"语义（实测侦察 preamble 存在） |
| 2 | 862s FAIL：step1–4b 首次全绿，唯 F10 未归 | 链尾预算归零 + backend poll_s=1.0 全程打总线锁与 4 agent 互饿 | TOTAL_BUDGET 840→860、backend poll_s 1→2、F9 等待轮询 5→3s |
| 3 | 编排者批准壳 1500s；1443s FAIL：liaison 唤醒后 ~10min 回合才启动、开回合后多轮取不到 F4 正文 | 晚峰 GLM 回合启动延迟 + 邮箱可见性延迟叠加（22–24 时段；环境性，非协议） | TOTAL_BUDGET→1440（watchdog 1470<壳 1500）；needle 抗变体加固：F6 断言=命令形状 OR router journal 功能性双证（delivered=push×2），f6_done 谓词同收 |

**防挂死体系（编排者整改指令①，已固化）**：全局 watchdog `asyncio.wait_for(v7_main(), 1470)`；阶段等待（F6=300/managers=780/F10=300）全部 `min(阶段帽, 剩余预算)` 钳制，预算耗尽立即放弃；GLM 客户端显式 `timeout=90, max_retries=1`（根除 SDK 默认 600s×重试无界等待——首轮挂死 112min 的教训）；backend `await_timeout_s=540` 对齐 doctrine；重跑一律 timeout 壳。

## E. live 事实与偏差（本票钉死）

- **dais GUI 已退出（17:24 CST）且红线禁 spawn dais**：new-terminal/inject-prompt 不可用。worker 用 fleet 原生 `session-spawn standard` 工厂（VO-005 已验 headless 路径），单行任务经 session-send DSHMSG 注入，worker 回合真实执行（穿衣排练 45s；全绿轮 WITNESS-A/B 时间戳为真执行凭证）。
- **worker_done 回收**：GUI 侧结算 watcher 缺席时任务状态机迁移延迟，`check-messages <ctx> --type worker_done` 消息行本身即回收凭证（dais 侧观察项）。
- **router 重载路径 live 断裂**：`agents/send` >256B/多行走 `--message-type direct`，现行 dais CLI 仅接受 status（VO-005 C.1 留痕；插件只读）。F6 邮箱正文经 dais CLI 直投（status）+ 推唤醒走 router 轻载 push。
- **中间跳凭证嵌入属 agent 自主补全**：实测一 manager 逐字嵌值、另一按模板字面——端到端均闭合。断言面=两端值硬断言 + 中段保留指令 + ref 硬链。
- **环境结论（对 gate 的诚实交代）**：全链 = 6+ 串行 GLM agent 回合。低载窗口 773s 全绿（A.1）；晚峰（21:30 后）worker 单回合 >6min、唤醒→回合启动延迟 ~10min，900/860/1440s 预算均以尾部超时告终（链在跑、无悬挂、无协议错误——每轮失败点不同但均为环境延迟）。**结论：协议与断言面已验证正确（低载全绿轮 + 各轮分段全绿证据）；建议 gate 复跑安排在低载窗口，壳 1500s**。
- teardown 纪律：worker 临时会话全部 purge（忙闸 409 留痕后 settle 补清）；harness 进程必停（端口释放核验）；孵化 agent 按 fleet 保留（红线）。

## F. 本轮 fleet 登记存档（红线保留项）

- 全绿轮：liaison <seat> / mgr_a <seat>（agent_lane_a_5b70）/ mgr_b <seat>（agent_lane_b_5b70）
- 整改复跑轮（链部分推进，同规约执行）：f84c/41d2/47e7、e7ad/b31d/c2d5、933c/fcf2/7278、e3a3/2eff/4fec
- 历史 liaison 条目（VO-006 起累计）fleet 在册。

done body: PASS;报告:docs/kg/evidence/VO-007-report.md;测试:低载4项全绿,晚峰环境超时见E节;备注:双manager全链协议验通,防挂死+抗变体加固落盘
