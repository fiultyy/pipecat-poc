# VO-004 证据：router 三 RPC + scope + journal

- 日期：2026-08-23（round 7）
- 票：`docs/tickets.md` VO-004 · 规范：`docs/plans/impl-specs.md` VO-004 · 方法级：KG 06 §2.4（三 RPC 签名）§2.5（scope + journal + 只注入固定信封红线）
- 范围（两处，越界零改动）：`~/.dsh/plugins/a2a-profile-server/http-server.js`（增 `agents/registry`·`agents/send`·`agents/inbox` 三分支 + `createRouter` 逻辑体）、`tests/test_rt_router.py`（新建，主仓）。registry.js 未动；六 RPC 既有语义零改动（纯增量分支）；未 git commit。

## 改动摘要

**http-server.js**：

| 面 | 实现 |
|---|---|
| agents/registry | `params={} → {agents:[{code,sessionId,mailbox,role,project,state,lastHeartbeat}]}`；数据源 = registry.js 在册内存表（VO-003）——HTTP 分支只委托 `router.list()`，零自持状态 |
| agents/send | `{to,from,ref?,type='notify',body}` → 参数校验（type ∈ notify/steer/ping、from/to 必须在册可解析（code 或 mailbox），违者 `-32602`）→ scope 校验（同非空 project 或 grants 显式授权 `[{from,to,ts}]`，否则 journal 记 `denied` + `-32000 scope denied`）→ 模式分流：轻载/单行 = push、重载(>256B)/多行 = mailbox |
| push 底座 | `session-send <from-mailbox> <to-code> <type> <ref> <body>` 子进程（env `A2A_SESSION_SEND` 可覆写）；DSHMSG] 固定信封由 CLI 构造——router 全程不触 `session.prompt`，**只注入固定信封格式**（G5 红线落点）；ackRef=`<ref>@push` |
| mailbox 底座 | `dais orchestration send-message <run> <from-mailbox> <to-mailbox> --message-type direct --subject route --body <DSHMSG]信封行>`（env `A2A_DAIS_BIN`/`A2A_DAIS_RUN_ID`，run 缺省 `router`）；正文 = 与推模式同构的单行信封（VO-005 对拍基底）；stdout seq 解析 → ackRef=`<ref>@<seq>` |
| agents/inbox | `{mailbox} → {unread:[{from,ref,type,body,seq}]}` 只读快照：缺省 `node:sqlite` DatabaseSync **readOnly** 连接读 dais 存储（env `A2A_DAIS_DB`，缺省 `~/.local/share/dais/data.sqlite`）messages 表 `read=0` 行；ref 双格式提取（DSHMSG] json / `[ref:X]` 前缀）；schema 不符显式抛错不伪造空结果 |
| journal | `router-journal.jsonl`（task-store 形制：`{ts,op:"route",from,to,type,ref,delivered}` append + 1MB 轮转）；`delivered ∈ push|mailbox|denied|failed`（failed 行带 error）——全量路由消息含拒绝与失败均入账，可回放 |
| 装配 | `createHttpServer` 增可选 `router` 参数；未注入时 agents/* 返 `-32000 router not configured`（六 RPC 照常）；`createRouter` 全依赖可注入（registry/journalPath 必填，底座/grants/阈值可选） |

**tests/test_rt_router.py**（新，6 用例）：node driver 起真实 http-server（随机端口真 HTTP JSON-RPC）+ 真实 registry（reattach 建册）；session-send/dais 按场景注入 mock 或经 env 指向 mock bin（默认实现代码路径同被覆盖）。

## A. 测试输出原文

```
$ .venv/bin/python -m pytest tests/test_rt_router.py -q
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.0.0
rootdir: ~/workspace-claw-02/pipecat-poc
configfile: pyproject.toml
plugins: anyio-4.14.0, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=function, asyncio_default_test_loop_scope=function
collected 6 items

tests/test_rt_router.py ......                                           [100%]

============================== 6 passed in 1.35s ==============================
```

## B. 回归（现行不破坏）

```
$ cd ~/.dsh/plugins/a2a-profile-server && node selftest.mjs   →   22 passed, 0 failed
$ .venv/bin/python -m pytest tests/test_rt_a2a_client.py tests/test_incubators_real.py \
    tests/test_rt_fleet_registry.py tests/test_rt_router.py -q
tests/test_incubators_real.py ...                                        [ 42%]
tests/test_rt_fleet_registry.py .....                                    [ 68%]
tests/test_rt_router.py ......                                           [100%]
============================== 19 passed in 3.63s ==============================
```

## C. 验证目标对照（impl-specs VO-004 ①–⑤）

| # | 验收 | 证据 |
|---|---|---|
| ① | 三 RPC 契约（签名/schema） | `test_rpc_contract_and_scope`：registry 字段集=registry 在册视图全键；send 参数面（缺 type 缺省 notify、unknown from/to、非法 type → -32602）；`test_no_router_degrades_explicitly`：未装配显式报状态且六 RPC 同实例照常 |
| ② | 同 project 通 / 跨 project 拒（-32000 scope） | 同上：voice-head 内互通 push；liaison(voice-head)→mgr_r(research) 拒 `scope denied`；grants 显式授权对（mgr_v→mgr_r）放行 |
| ③ | 双模式分流 + journal 记 delivered | `test_push_mailbox_split_and_journal`：>256B 与多行正文均 mailbox；dais argv 形制（orchestration send-message run from to-mailbox --body DSHMSG] 单行信封，json 字段全等）；journal delivered 序 [push,denied,mailbox,push,denied,mailbox] 且与落盘文件一致 |
| ④ | inbox 只读不消费 | `test_inbox_readonly_snapshot`：fixture sqlite 两格式 ref 提取正确、已读/他人邮箱排除；两次快照全等；文件字节与 read 标志零变化（readOnly 连接） |
| ⑤ | journal 可回放 | `test_journal_replayable_with_failures`：op=route 行按序重建 (ref,delivered) 账本含 denied/failed；failed 行带 error 原文；ts 全量在 |
| 红线 | 只注入固定信封 | `test_default_bin_implementations`：默认底座经 env 指向 mock bin——session-send 恰 5 位置参数全单行；dais --body 单行 DSHMSG] 信封可解析；router 代码无 session.prompt 直呼（实现结构保证） |

## D. 设计决策与遗留

- **信封身份约定**：`from` = 发方 mailbox 句柄、`to` = 收方 fleet code（session-send 的 fleet 解析键）；mailbox 投递的 CLI 收件人 = 收方 mailbox，信封 `to` 仍为 code——推/投两模式信封字节同构（VO-005 对拍基底）。收方 doctrine 按 from/ref 匹配（§2.1 三过滤同源）。
- **底座 CLI 契约已探明钉死**：session-send `resolve()` 只认 fleet 4 位码/sessionId 前缀/全 sessionId——故 push 必传 code；dais `send-message` RUN_ID 位置参数必填——本票用 `A2A_DAIS_RUN_ID`（缺省 `router`）固定桥 run，live 接线时由宿主建固定 run 并注入（VO-005）。
- **inbox 存储假设**：dais GUI daemon 本轮不在线，messages 表 schema 未能现场核对——按 `(seq,sender,recipient,message_type,subject,body,read)` 实现，fixture 测试锁我方契约；live 偏差由 VO-005 真邮箱对拍收敛（表名/列名差异只动一行 SQL）。check-messages 为消费语义，确认不可用于只读 inbox（用户预判成立）。
- **index.js 装配仍未接线**（本票范围外，VO-003 报告同留）：生产启用需在 activate() 里 `createRegistry`+`reattach()`+`createRouter` 注入 `router` 参数——纯装配性三行，留 VO-005 live 链一并做（届时 dais 在线可验 reattach 真值）。
- **grants 形制**：本票为注入参数（数组或 async fn）；持久化 grants 记录（fleet 侧或独立 JSON）留 VO-006+ 按需落。

done PASS;报告:docs/kg/evidence/VO-004-report.md;测试:6项全绿;备注:三RPC纯增量,信封同构双投递;接线留VO-005
