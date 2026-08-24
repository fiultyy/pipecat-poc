# LB-002-A live 取证 · phase-2 轮询饿死回信方（真身链首个 P0 缺陷）

日期：2026-08-24（V5/V6 验收跑 #1，commit 前 `be94ba8` 修复）
对象：真身 dsh-liaison code `<seat>`（session-<id>，
mailbox `agent_liaison`，vh-liaison@v1 含协议附录）

## 现象

V5 12 项断言 11 PASS，唯一 FAIL = phase-2 终稿 540s 未到。
孵化/fleet 五键/受理回执/口语回执 ref/两阶段时序/V6 全部（打断、pending 在途、
cancel、无迟到终稿）均 PASS——断点唯一落在真身回信一跳。

## 真身侧证据（session.history，逐字）

turn 1（wake ref=vh-<redacted>）完全符合 doctrine：

1. `skill: dais-orchestration`（准备）
2. `bash: check-messages agent_liaison --timeout-ms 2000`（**首执行动作=邮箱排空** ✅）
3. `mcp__zhipu__web_search_prime` ×5（WebGPU 调研真做了）
4. `write: /tmp/vh_c7aa182b_final.txt`（终稿落盘，`[ref:vh-<redacted>] "Agent Final Message":` 前缀）
5. `bash: send-message run_<redacted> agent_liaison voice-head --message-type status ...`

第 5 步两次调用的 tool-result 逐字：

```
RESULT: (no output)
[timed out after 60000ms]
[killed by signal: SIGTERM]
```
```
RESULT: (no output)
[timed out after 150000ms]
[killed by signal: SIGTERM]
```

真身随后自排障：`send-message --help`、sqlite 直查 messages 表、
`PRAGMA busy_timeout=3000; BEGIN IMMEDIATE`、`create-run` 探针 `timeout 15` →
**exit=124**（create-run 也挂）。turn 3 复盘 reasoning 原话：
"CLI check-messages now works again (exit 0, 'no messages')"——bus 已复苏。

## 消息库证据（真身自己查到的，逐字）

```
762|run_<redacted>|voice-head|agent_liaison|intent|1||2026-08-24 15:57:25|109   ← head intent 已达
530|run_<redacted>|agent_liaison|voice-head|done|1||2026-08-23 16:32:27|2121    ← 替身时代 done 正常
528|run_<redacted>|agent_liaison|voice-head|done|1||2026-08-23 16:30:53|2472
486|run_<redacted>|agent_liaison|voice-head|done|1||2026-08-23 15:56:50|443
```

`run_<redacted>` 的 done 行**不存在**——send-message 从未落库。

## 因果时间轴

| 时刻 | 事件 |
|---|---|
| 15:57:25 | seq 762 intent 入库；head phase-2 轮询开始（poll_s=1.0 平铺，读即消费=写事务流） |
| ~15:58 | 真身 turn 1 唤醒，排空邮箱拿到任务，开始调研 |
| ~00:0x | 真身 send-message ×2 挂 60s/150s SIGTERM；create-run 探针 exit=124 —— **plane 整体无响应** |
| 00:07 | V5 540s 轮询窗关闭 → V6 `create_run run_<redacted>` **立刻成功** |
| ~00:14 | 编排者手工 `check-messages voice-head` 挂 60s（残余竞争）；随后 `check-status` 秒回 |

轮询窗开 → 回信挂；轮询窗关 → 立刻通。机制假说：消费型快照（read+delete=
写事务）平铺 1s 节奏把 daemon store 写锁占满，回信方同锁饿死；dais 侧内部
串行化细节（锁公平性/读路径并发/服务端 per-call 超时缺位）需 dais 侧实证。

## 消费侧修复（commit be94ba8）

- `DaisLane.await_done`：sleep 从 poll_s ×1.5 递增至 `poll_max_s`（新参，默认 8s）；
  命中排空立即返回（去掉命中后睡眠，省最多 8s 延迟）
- `DshBackend.poll_max_s` 传参；live V5/V6 显式 `poll_max_s=8.0`
- 单测：退避序列 `[1.0,1.5,2.25,3.375,5.0625,7.59375,8.0-cap]` + 无命中后睡眠
- 60s 回信场景写事务率 ~6× 下降（~10 polls vs ~60）

## dais 侧移交（新缺陷 D-17 候选）

- 编排面在并发「高频消费轮询 + 回信写」下 CLI 挂死无错误无服务端超时
  （与 D-03 错误契约相关但独立：本例根本无错误可传导）
- 建议：锁公平性/读路径并发实证 + 服务端 per-call 超时
