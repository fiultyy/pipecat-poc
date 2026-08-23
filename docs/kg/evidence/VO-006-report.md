# VO-006 报告 · liaison 落位（替身协议移交）

- 票：docs/tickets.md §VO-006 · 依据：docs/plans/impl-specs.md §VO-006 · 规范：docs/kg/06-ws5-agent-ecosystem.md §3（含 §3.1 零改动对照表）
- 范围（仅两文件）：`examples/realtime-provider-poc/live_v5_v6_dsh.py`、`tests/test_live_v5_v6.py`
- 验证形式：live 真跑（真 GLM head + 真孵化 dsh-liaison 会话 + 真 dais 总线）

## A. 测试输出原文

pytest（主区，`.venv/bin/python -m pytest tests/test_live_v5_v6.py -q`）：

```
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: ~/workspace-claw-02/pipecat-poc
configfile: pyproject.toml
plugins: anyio-4.14.2, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=function, asyncio_default_test_loop_scope=function
collected 2 items

tests/test_live_v5_v6.py ..                                              [100%]

======================== 2 passed in 224.82s (0:03:44) =========================
```

live 脚本全链输出（同版本代码直跑，`live_v5_v6_dsh.py`，pytest 内部以子进程执行同一脚本）：

```
== incubate: real dsh-liaison via plugin ==
  [PASS] 孵化回执（真身 dsh-liaison） — code=<seat> mailbox=agent_liaison role=liaison project=voice-head
  [PASS] fleet 五键登记 — sessionId=session-437e8478-f…
== V5: voice(text) -> dais run -> real liaison -> broadcast ==
2026-08-23 17:07:55.691 | INFO     | pipecat:<module>:54 - ᓚᘏᗢ Pipecat 0.0.0.dev1 (Python 3.12.3 (main, Jun 19 2026 12:46:00) [GCC 13.3.0)] ᓚᘏᗢ
  [PASS] head 调用 dispatch_intent — tools=['dispatch_intent_tool']
  [PASS] phase-1 受理回执（head 本地即时生成） — ref=vh-<redacted> run=run_<redacted>
  [PASS] 回执 ref 原样出现在口语回执 — ack='已经受理啦，调研正在编排中。凭证号念给你：run_<redacted>，回执 ref 是 vh-<redacted>，凭证【凭证VH-EE6A037B】。终稿出'
  [PASS] phase-1 阶段终稿未到（两阶段时序）
  [PASS] phase-2 终稿到达（真身 F10 回信）
  [PASS] 终稿 = FINAL_PREFIX + done body（[ref:] 信封三过滤命中） — final[:60]='"Agent Final Message":\n\n"Agent Final Message":\n【凭证R-V5-9901】'
  [PASS] 终稿凭证逐字回显
  [PASS] 播报含凭证标记（逐字） — broadcast='好的，以下是终稿全文，逐字播报：\n\n【凭证R-V5-9901】\n[完成] WebGPU 生产环境采用情况调研完成 | 证据来源见文末 | vh-<redacted>'
  [PASS] liaison 回合首动作 = 邮箱快照排空（doctrine 行为断言） — calls=['skill', 'bash', 'bash', 'mcp__zhipu__web_search_prime'] first_exec='{"command":"~/.local/bin/dais orchestration check-messages agent_liais'
V5 PASS
== V6: interrupt query + cancel ==
  [PASS] V6 dispatch 受理（F4 已投真身）
  [PASS] 打断后 head 走 query_status — tools=['query_status_tool']
  [PASS] run 在打断后仍在途（不打断远程执行）
  [PASS] cancel_run 取消成功 — result={"status": "canceled", "run_id": "run_<redacted>"}
  [PASS] 取消后无迟到终稿播报
V6 PASS
liaison code=<seat> mailbox=agent_liaison sessionId=session-<id> (kept registered in fleet)
```

## B. 实现内容

1. **真孵化 dsh-liaison**（live 脚本 `incubate_liaison()`）：
   - agents_md = 真 `Projector.project(场景=编排对接, role=liaison)` 投影（含 projector 内置 ROLE_TEMPLATES doctrine，过三门）+ 尾追「通信操作规约」节（live 探测验证过的可执行契约：回合首动作命令、每 ref 恰好一条终稿、不另发中间回执、凭证逐字）；
   - 经插件 `incubate` RPC（临时 node 服务承载 http-server.js + dsh 孵化器，RPC 后即停）：`{targets:["dsh-liaison"], role:"liaison", project:"voice-head", mailbox:"agent_liaison"}`；
   - 插件 `incubateDsh` 扩展路径自动前置 role doctrine 段 + fleet 五键登记（role/project/mailbox/profile_version/spawned_at）。
2. **head 侧仅配置值**：`DshBackend(orchestrator_handle="agent_liaison")`（原替身 `session_vhlive`）；F4 走 backend 原有 `send_intent`（`[ref:]` 信封投 agent_liaison 邮箱），wrapper `dispatch_intent_live` 在 dispatch 后补一发 DSHMSG 推唤醒（session-send，走 maestro loopback，不经 dais CLI 总线锁）。**rt_dsh_backend / rt_dsh_lane / rt_head_tools / rt_orchestrator / src/pipecat 零改动**（`test_zero_diff_head_side` 钉死）。
3. **V5 全链**：head GLM 调 dispatch_intent → phase-1 本地回执（ref/run_id/凭证）→ F4 邮箱正文 + DSHMSG 唤醒 → liaison 真实 agent 回合（真实调研）→ F10 终稿回信（`[ref:]` + FINAL_PREFIX 逐字 + 凭证逐字）→ `_phase2` 三过滤收割 → head 播报凭证逐字。断言含两阶段时序（phase-1 时 finals 空）。
4. **V6 全链**：真身在途时 barge-in query_status（pending 命中）→ cancel_run → 取消后无迟到终稿。
5. **doctrine 固化**：模块 docstring 后专设「Doctrine — liaison 真身落位」注释节（F4 双路投递、回合首动作=邮箱快照排空、F10 单终稿、受理回执为何不由真身发——带 ref 的非终稿信会污染 phase-2 匹配，live 探测事实）。

## C. §3.1 零改动对照表四项核对

| # | 协议项 | 替身实现 | 真身实测 | 核对 |
|---|---|---|---|---|
| 1 | 受理回执 | phase-1 receipt 由 `DshBackend.dispatch` 本地即时生成 | 同一函数，零改动；真身不另发中间回执（规约固化），head 视角同构 | ✅ |
| 2 | 终稿 = `FINAL_PREFIX + done body`（凭证内嵌） | player reply body 单行 | 真身多行终稿，`[ref:]` 前缀剥除后 `startswith(FINAL_PREFIX)` 命中、凭证 `【凭证R-V5-9901】` 逐字 | ✅ |
| 3 | 信封 `[ref:<ref>]` | send_intent 前缀 | 真身回信 body 同款前缀；`await_done` 三过滤（ref 命中 ∧ seq>intent_seq ∧ from=agent_liaison）实测命中 | ✅ |
| 4 | 三过滤 `_phase2` | rt_dsh_backend.py:157-160 | 该文件（及全部 head 逻辑文件）git diff 为空，`test_zero_diff_head_side` 断言通过 | ✅ |

验收①–④对照：①F4/F10 全链一次 ✅（A 节）；②受理回执/终稿/FINAL_PREFIX/凭证与替身 V5 同构 ✅（C 表 1-3）；③head 侧 diff 仅配置（handle 值）✅（C 表 4 + git status 仅本票两文件）；④回合首动作=邮箱快照排空，doctrine 注释节固化 + 行为断言 ✅（A 节末条：唤醒回合首个 bash 执行动作 = `check-messages agent_liaison`，skill 加载属执行准备不计）。

## D. live 事实备注

- 真身终稿为多行 body：`_parse_message_rows` 的 `_lines` 收集+join 天然兼容（替身时代单行，此为新增覆盖的隐含契约）。
- 真实 agent 回合典型 40–70s、偶发 >5min；`await_timeout_s=540` + 主循环 540s 预算，pytest timeout 900s。
- turn 编号不稳定（PROFILE-INJECT 不总独立成回合）：doctrine 行为断言按 DSHMSG ref 定位唤醒回合，不猜 turn 号。
- urllib 直连 127.0.0.1 需剥代理（宿主 NO_PROXY 的 CIDR 条目不被 urllib 识别）；httpx 需剥 SOCKS（同 VO-010 方案）。
- 本票各轮孵化登记（fleet 保留）：f41a/a779/f369/d234/707d/adf6/437e，均 role=liaison、mailbox=agent_liaison、project=voice-head，ProfileStore 版本 v1–v7 延续；探测临时会话 d089 已 teardown。

done body: PASS;报告:docs/kg/evidence/VO-006-report.md;测试:2项全绿;备注:真身全链live跑通,head零改动
