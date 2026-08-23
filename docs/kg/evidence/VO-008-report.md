# VO-008 报告 · OrcaLane 封装 + 单测（车道B B.1–B.2）

> 2026-08-23 · worktree vo-008 · 规范源：KG 07§1–2、impl-specs VO-008、tickets VO-008
> 交付物：`examples/realtime-provider-poc/rt_orca_lane.py`（新）、`tests/test_rt_orca_lane.py`（新）

## A. 测试输出原文

```
$ ~/workspace-claw-02/pipecat-poc/.venv/bin/python -m pytest tests/test_rt_orca_lane.py -q
tests/test_rt_orca_lane.py .........................                     [100%]

============================== 25 passed in 0.14s ==============================

$ ... -m pytest tests/test_rt_orca_lane.py -v  (节选)
tests/test_rt_orca_lane.py::test_bin_hardcoded_and_bare_orca_rejected PASSED [  4%]
tests/test_rt_orca_lane.py::test_status_parses_envelope_result PASSED    [  8%]
tests/test_rt_orca_lane.py::test_spawn_worktree_argv_and_new_handle PASSED [ 12%]
tests/test_rt_orca_lane.py::test_spawn_worktree_legacy_startup_terminal_handle PASSED [ 16%]
tests/test_rt_orca_lane.py::test_spawn_worktree_no_handle_returns_none PASSED [ 20%]
tests/test_rt_orca_lane.py::test_spawn_worktree_base_branch_and_setup_flag PASSED [ 24%]
tests/test_rt_orca_lane.py::test_spawn_worktree_invalid_setup_rejected_before_cli PASSED [ 28%]
tests/test_rt_orca_lane.py::test_spawn_worktree_missing_worktree_id_raises PASSED [ 32%]
tests/test_rt_orca_lane.py::test_terminal_list_returns_rows_and_worktree_selector PASSED [ 36%]
tests/test_rt_orca_lane.py::test_read_tail_and_string_cursor_roundtrip PASSED [ 40%]
tests/test_rt_orca_lane.py::test_read_empty_tail PASSED                  [ 44%]
tests/test_rt_orca_lane.py::test_wait_argv_always_bounded PASSED         [ 48%]
tests/test_rt_orca_lane.py::test_wait_cli_timeout_raises_orcalanetimeout PASSED [ 52%]
tests/test_rt_orca_lane.py::test_wait_timeout_is_lane_error_subtype PASSED [ 56%]
tests/test_rt_orca_lane.py::test_wait_subprocess_bound_exceeds_cli_bound PASSED [ 60%]
tests/test_rt_orca_lane.py::test_wait_invalid_args_rejected_before_cli PASSED [ 64%]
tests/test_rt_orca_lane.py::test_send_enter_default_and_disabled PASSED  [ 68%]
tests/test_rt_orca_lane.py::test_interrupt_sends_interrupt_flag_without_text PASSED [ 72%]
tests/test_rt_orca_lane.py::test_stop_targets_worktree_selector PASSED   [ 76%]
tests/test_rt_orca_lane.py::test_worktree_ps_returns_summary PASSED      [ 80%]
tests/test_rt_orca_lane.py::test_ok_false_envelope_raises_with_cli_error_code PASSED [ 84%]
tests/test_rt_orca_lane.py::test_non_json_output_raises PASSED           [ 88%]
tests/test_rt_orca_lane.py::test_empty_output_raises PASSED              [ 92%]
tests/test_rt_orca_lane.py::test_error_message_carries_command_prefix_and_detail PASSED [ 96%]
tests/test_rt_orca_lane.py::test_every_invocation_carries_json_flag PASSED [100%]

============================== 25 passed in 0.11s ==============================
```

G1 回归对照：`tests/test_rt_dsh_lane.py` 15 用例全绿（与本票文件同批跑）；`tests/test_rt_conformance.py::test_live_lane_a_b_conformance`（live）一次抖动失败，基线（stash 本票两文件后）与本票在场重跑均通过——判定为 live 邮箱时序抖动，非本票引入。

## B. B.1 探针记录（开工首步 `orca-ide skills get orca-cli`）

skill 指南（orca-cli，全文已读）+ `--help` + 只读命令实测，关键钉死项：

| 项 | 探明结果（2026-08-23，orca app 1.4.185） |
|---|---|
| R1 | Linux 非 Orca 终端一律 `orca-ide`；裸 `orca` = GNOME 屏读器（skill 明文） |
| 信封 | 所有命令 `--json` 统一 `{id, ok, result \| error{code,message}, _meta}`；**错误时 exit=1 且 ok:false**（双路径归一到 OrcaLaneError） |
| read 旗标 | `--cursor <n>`（上次 read 的 `nextCursor`，只返回增量）+ `--limit <n>`（**没有 --lines**，票面已预警） |
| read 游标 | **字符串**：`oldestCursor`（旧行丢弃界）/`nextCursor`（续读）/`latestCursor`；`limited=true` 表示还有页 |
| read 形状 | `result.terminal.tail` = 行数组 + `truncated/limited/returnedLineCount` |
| wait | `--for exit\|tui-idle --timeout-ms <ms>` 必带；超时= `ok:false, error.code:"timeout"` exit=1 → OrcaLaneTimeout（"仍在跑"语义非硬错） |
| send | `--text` + `--enter`/`--interrupt` 互斥路径；interrupt 不带 text |
| stop | **`terminal stop --worktree <selector>`**（非 --terminal；停工作树全部终端） |
| worktree create | `--name --repo id:\|name:\|path: --agent --prompt --setup run\|skip\|inherit [--base-branch]`；句柄优先级 `agentTerminalHandle`（新）→ `startupTerminal.handle`（旧）→ None |
| list/ps | `result.terminals[]`（默认省 visualLayouts）；`result.worktrees[]` |

## C. 验收对照（impl-specs VO-008 ①–④）

- **① 语义映射表（KG 07§2）全覆盖**：spawn=worktree create --agent --prompt（方法 `spawn_worktree`）；有界等待=`wait(what, timeout_ms)` 必带界；尾读=`read(cursor=nextCursor)` 增量语义按探针钉死；应答/中断=`send(enter=)`/`interrupt`；状态汇总=`worktree_ps`；取消=`stop(worktree)`/`interrupt`；探活=`status`；路由=`terminal_list`。终稿回信通道不分会道（B 产物走 dais 邮箱链）——lane 层无需 send-reply，由 manager/liaison 承担。
- **② 无裸等**：`wait` 强制 `timeout_ms`（缺省 30000 也总是物化为 `--timeout-ms` 旗标，测试断言）；非法 what/非正 timeout 在 CLI 前拒绝；`_run` 外层 subprocess 界恒大于 CLI 界（`timeout_ms/1000+10`，测试断言），CLI 自身超时先决。
- **③ --json 解析 + 错误形制对齐**：全命令自动追加 `--json`（测试逐 argv 断言）；信封 `ok:false`（exit=0 或 exit=1 两路径）、非 JSON、空输出、信封非 dict 全部归一 `OrcaLaneError`（RuntimeError 子类，消息带命令前缀+截断 detail，同 DaisLaneError 形制）；`timeout` 错误码细分子类 `OrcaLaneTimeout`。
- **④ mock 单测全绿**：25/25，全部 mock CLI `--json` 输出（信封形状按 B 节实测蓝本构造），零真调用。

## D. 设计裁定

- `runner` 可注入（形制对齐 DaisLane），但类型从 `object` 收紧为 `Callable[[list[str]], Awaitable[tuple[str, str]]] | None`（修 Pyright reportCallIssue）。
- 无总线锁：orca-ide 走本地 runtime socket，无 dais `check-messages --wait` 式锁饥饿问题，故不复制 DaisLane 的进程级锁。
- `spawn_worktree` 返回 `{worktreeId, terminalHandle}`，handle 三级回退（agentTerminalHandle→startupTerminal.handle→None），folder 类 repo 可能 None——由调用方 `terminal_list` 重取。
- `read` 返回 `(tail_text, next_cursor)` 二元组而非原始信封：`limited/oldestCursor` 分页策略留给调用方（lane 不隐藏游标也不吞页）。

done body:通过;报告:docs/kg/evidence/VO-008-report.md;测试:25项全绿;备注:游标字符串语义已钉死,stop走worktree选择器
