# VO-005 证据：推/拉双投递 conformance（live）

- 日期：2026-08-23（round 7）
- 票：`docs/tickets.md` VO-005 · 规范：`docs/plans/impl-specs.md` VO-005 · 方法级：KG 06 §2.2（session-send 推语义）§2.3（dais 邮箱拉语义）
- 范围（单文件，越界零改动）：`tests/test_rt_conformance.py` 扩 `test_dual_delivery_parity`（live：真 session-send + 真 dais 邮箱）。未改 src/；未 git commit。

## 用例设计

同一五元组 `{from: voice-head-a2a, to: <code>, type: steer, ref: vh-dual-<hex>, body: "双投递对拍 TOKEN-DUAL-<hex> 【凭证R-DUAL-<hex>】"}` 双路投递：

| 路 | 底座 | 收方观察面 |
|---|---|---|
| 推 | 子进程 `~/.dsh/maestro/bin/session-send <from> <code> <type> <ref> <body>`（DSHMSG] 信封由 CLI 构造，mode=queue 注入） | loopback `session.history` → `user/message`/`agent/inbox/spliced` 事件 text = 收方回合首行（机器可解析） |
| 拉 | `dais orchestration send-message <run> <from> <session_vhconf> --message-type status --subject route --body <同一信封行>` + `check-messages --timeout-ms` 有界快照 | 邮箱行 body 逐字 |

断言：双模式收方解析出**字节级同一信封行** → 同一 `{from,to,ref,type,body}` 五元组；凭证逐字（TOKEN 与【凭证…】不改写）；推注入回合首行 json.loads 可解析；邮箱二次有界快照不再返回该 ref（读即消费）；全程 dais/loopback 调用间 sleep 0.5–0.7s 间隙（总线锁纪律，对齐既有 live 用例）；双面掉线确定性 skip（`_push_plane_up` + 既有 `_bus_healthy`）。

收方会话：`session-spawn standard vh-dual-<hex>`（fleet 原生工厂，无 GUI 依赖）；teardown best-effort `session-purge`（busy 闸容忍）。

## A. 测试输出原文

```
$ .venv/bin/python -m pytest tests/test_rt_conformance.py -q
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.0.0
rootdir: ~/workspace-claw-02/pipecat-poc
configfile: pyproject.toml
plugins: anyio-4.14.2, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=function, asyncio_default_test_loop_scope=function
collected 3 items

tests/test_rt_conformance.py ...                                         [100%]

============================== 3 passed in 9.65s ==============================
```

（含既有 2 用例不破：offline executor 状态机 + live A/B 对拍；新增 `test_dual_delivery_parity` 单跑 1 passed in 6.97s。）

## B. 验证目标对照（impl-specs VO-005 ①–④）

| # | 验收 | 证据 |
|---|---|---|
| ① | 双模式收方解析出同一 `{from,to,ref,type,body}` | `push_line == pull_line == line`（字节级）+ `push_tuple == pull_tuple == envelope`（五元组等值） |
| ② | 推注入回合首行机器可解析 | `session.history` user/message 事件 text 以 `DSHMSG]` 起首，`json.loads` 直接出五元组 |
| ③ | 邮箱路径读即消费语义确认 | 首次 check-messages 取到该 ref 后，二次有界快照 `not any(ref in row)` |
| ④ | 宿主掉线确定性 skip | loopback `session.list` 失败 / `_bus_healthy` 失败 → `pytest.skip` 带理由 |

## C. 本轮 live 钉死的事实（新增）

1. **dais `send-message` 仅接受 `--message-type status`**：`direct/broadcast/group/steer/notify` 全部 `invalid message_type ... Matching variant not found`（逐一实测）。**波及 VO-004**：http-server.js mailbox 分支默认 `--message-type direct` 在真 CLI 上会被拒——mock 未校验枚举所以离线测试绿。修复=一个词（`direct`→`status`），但 VO-005 红线限单文件，留 VO-006 接线时一并改（此处留痕为凭）。
2. **推路径可观察面**：`session.prompt mode=queue` 注入 → `agent/inbox/spliced`（排队）与 `user/message`（回合化）两事件均携带信封行逐字文本——"收方回合首行机器可解析"由 history 事件直接佐证。
3. **new-terminal 需 GUI 主线程**（本轮 GUI 不在线 → `no GUI window is running`）。推路径收方改用 `session-spawn`（fleet 原生工厂，无 GUI 依赖，与 incubateDsh 同款）——票面"new-terminal + fleet 反查"步骤被等价替代；**未开任何 GUI tab，无 close-terminal 清理义务**（红线纪律平凡满足）。副产品：主仓路径已 `project-add` 注册（GUI 恢复后 new-terminal 即可用）。
4. **session-purge busy 闸**：事件日志 5 分钟内有写入即拒（HTTP 409）。测试 teardown 与手工探针共留 3 个探针会话（cc87/6357/de41，standard preset 对注入信封各响应一回合），settle 后可 purge——与孵化冒烟留置惯例一致。
5. session-send 的信封 json 用默认分隔符（`", "`/`": "` 带空格）；测试按同构构造达成字节级对拍（KG 06 §2.1 "信封同构" 的实测确认）。

## D. 遗留

- VO-004 `--message-type direct` live 偏差修复（C-1）：一词改动 + mock 补枚举校验，随 VO-006 index.js 接线票落地。
- 探针会话 purge（settle 后手动/下轮清理）。

done PASS;报告:docs/kg/evidence/VO-005-report.md;测试:3项全绿;备注:字节级同信封双路一致;dais仅status型,VO-004偏差留006
