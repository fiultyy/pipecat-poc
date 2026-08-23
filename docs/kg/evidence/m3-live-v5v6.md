# M3 W1.6 live 证据：V5/V6 全链（GLM 文本模式 + 真 dais）

- 日期：2026-08-23（round 7）
- 入口：`examples/realtime-provider-poc/live_v5_v6_dsh.py`（pytest 包装 `tests/test_live_v5_v6.py`）
- Q2 定案：`DASHSCOPE_API_KEY` 缺失 → **GLM 文本模式先行**（规划内回退路径）。
  head = GLM chat 工具循环（DSH_TOOLS_DOCTRINE + rt_head_tools 四件套，
  DirectFunctionWrapper 生成 schema），执行层 = DshBackend（车道B，真 dais 总线），
  本脚本扮演编排会话 session_vhlive（收 intent → 按 ref 回 done）。

## V5：语音(文本) → dais run → 播报 全链 PASS

| 步 | 判定 | 实测 |
|---|---|---|
| head 调 dispatch_intent | PASS | tools=['dispatch_intent_tool'] |
| phase-1 受理回执 | PASS | ref=vh-<redacted> run=run_<redacted> |
| 回执 ref 原样口语回显 | PASS | ack="…凭证号念给你：vh-<redacted>，也就是【凭证VH-E72CA423】…" |
| phase-2 终稿到达 | PASS | FINAL_PREFIX + done body 注入 head 上下文 |
| 播报含凭证标记（逐字） | PASS | broadcast="调研办好了。凭证号是【凭证R-V5-9901】，结论：41%。" |

## V6：打断 + 问询 + 取消 PASS

| 步 | 判定 | 实测 |
|---|---|---|
| V6 dispatch 受理 | PASS | run 在途 |
| 打断后 head 走 query_status | PASS | 走工具而非自答 |
| run 打断后仍在途 | PASS | pending 含 ref（打断不打断远程执行） |
| cancel_run 取消成功 | PASS | {"status":"canceled","run_id":…} |
| 取消后无迟到终稿 | PASS | 1.5s 窗口无 late final |

## 本轮钉死的 live 事实（新增）

- `fail-dispatch` 只接受 `ctx_` 派发句柄；run/task id → exit=1 "not found"
  （早前 shell 探测 `$?` 被 `head` 管道吃掉误判为 exit 0）。`DshBackend.cancel`
  改为仅在 ctx_ 时调用 fail-dispatch，本地杀 phase-2 任务即权威取消。
- daemon 对无终端视图的 ctx 做 `read-worker` → exit=1 "no terminal view is
  registered"（探针期曾容忍）；live 冒烟断言已按此契约更新。
- 跨进程瞬时 `database is locked` 会打死不设防的 phase-2 任务：`_phase2`
  现在对 DaisLaneError 重试至总预算，仅 TimeoutError 视作"仍在跑"。

## 回归

任务测试集（13 文件，含本包装）全绿；live 证据由 wrapper 每次运行刷新。
