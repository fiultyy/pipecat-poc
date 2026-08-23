# VO-009 报告 · 车道B live 冒烟 + A/B 对拍（B.3–B.4）

> 2026-08-23 · worktree vo-009 · 规范源：KG 07§2 对拍判定、impl-specs VO-009、tickets VO-009
> 交付物：`tests/test_rt_conformance.py` 扩（唯一改动文件；lane 工厂参数化 + 车道B live 冒烟）
> 底座只读引用：`examples/realtime-provider-poc/rt_orca_lane.py`（VO-008 全方法封装）

## A. 测试输出原文

```
$ ~/workspace-claw-02/pipecat-poc/.venv/bin/python -m pytest tests/test_rt_conformance.py -q -rs --tb=short
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: ~/orca/workspaces/pipecat-poc/vo-009
configfile: pyproject.toml
plugins: anyio-4.14.2, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=function, asyncio_default_test_loop_scope=function
collected 6 items

tests/test_rt_conformance.py ......                                      [100%]

============================== 6 passed in 45.25s ==============================
```

收集明细（VO-009 新增 3 项，加粗）：

```
Coroutine test_dais_executor_offline_state_machine      （VO-005 前既有，离线）
Coroutine test_live_lane_a_b_conformance                （W1.5 前既有，live）
Coroutine test_dual_delivery_parity                     （VO-005，live）
Coroutine test_live_lane_b_smoke                        （VO-009① 新增，live）
Coroutine test_ab_lane_final_conformance[laneA-dais]    （VO-009② 新增，live）
Coroutine test_ab_lane_final_conformance[laneB-orca]    （VO-009② 新增，live）
```

## B. 验收①–③逐条

**① live 冒烟无裸等无死等** — `test_live_lane_b_smoke`：经 `rt_orca_lane` 真 spawn 1 worktree（内置 agent `claude`，omp 禁用防递归；repo 选择器 `path:<主checkout>`）→ `terminal wait --for tui-idle` 15s 有界切片循环 → `terminal read` 增量游标分页（对话框检测）→ 终稿观察。总预算 240s 封顶；测试末尾断言**每条** wait argv 都带 `--timeout-ms` 且确实等待过（完成被观察而非假设）。实测单冒烟 13.5–27s（文件产物契约，见 C.2）。

**② A/B 同终稿** — `test_ab_lane_final_conformance[laneA-dais|laneB-orca]` 工厂参数化：同一意图 `_AB_INTENT` 经 `DshBackend.dispatch`（HEAD_B）入 dais 统一受理链（run_ 前缀 + ref + 凭证回执），断言意图落 ORCH 邮箱；车道A=dais 编排角色执行确定性 body，车道B=真 orca worktree worker 执行同 body；两道终稿均经 `send_reply(ORCH→HEAD_B)` 走**统一 dais 邮箱链**回传（不分会道），head `_phase2` 轮询自己邮箱收终稿。断言 `final == FINAL_PREFIX + _AB_BODY`（同前缀同 body）且 `【凭证R-CONF-AB-8899】` 逐字在场。

**③ 不可用确定性 skip** — 两车道各自探活：车道A `_bus_healthy`（dais check-status）失败 → `pytest.skip("车道A dais 编排面不可用…")`；车道B `shutil.which("orca-ide")` + `status()` app.running/runtime.reachable 失败 → skip 带原因。窗口期首轮实测：dais down 时 `live_lane_a_b` 与 ab 两参数均确定性 skip（无 fail）。

## C. Live 事实钉死（2026-08-23，orca 1.4.185；对 KG 07 / VO-008 的增量）

1. **wait 未满足双形制**：`terminal wait` 超时未满足有两种终态——`exit=1 + envelope ok:true（satisfied:false，blockedReason 如 "codex-trust-workspace"）` 与 `exit=1 + ok:false code=timeout`。`OrcaLane._run` 对 `exit!=0` 先抛 `OrcaLaneError`（消息内嵌 stdout），调用方以 `exit=1 && (ok:true | code:timeout)` 双形制判定"切片未满足，继续"（`_wait_slice_unsatisfied`）。VO-008 mock 只覆盖 ok:false 形制。
2. **答案文本不进 scrollback ring**：agent 的回复文本永不落入 `terminal read` 的 ring buffer（141s 增量分页验证；仅 spinner 帧）。且 spawn 的 prompt 经 argv 回显**会**以裸 body 行出现在 buffer 首页——纯文本行匹配是假阳性（argv 回显）而非真终稿。**终稿必须走文件产物契约**：worker 把终稿写入 worktree 根 `final.txt`，`read_text().strip() == body` 精确匹配才算完成（半写不满足）；实测 13.5s 端到端。
3. **trust 对话框**：fresh worktree 的 agent 可能停在信任确认（"Enter to confirm / 2. No, exit"）；`send "1"` 应答（上限 5 次）。
4. **tui-idle 早亮**：对话框应答后 TUI 瞬时空闲即 satisfied=true，但 agent 仍在跑——完成判定只认产物，不认单次 satisfied。
5. **高思考档对策**：宿主 claude（GLM 代理，effortLevel=high）对琐碎任务也可思考数分钟（480s 未出稿实测）；runner 在 settle 窗（20s）后执行一次 dance：`interrupt` → `/effort low` → 重发任务，27s 出稿。注：`/effort low` 会被 claude 持久化为新会话默认（本次探测的副作用，已持久化）。
6. **read 分页语义**：`--cursor 0` 从**最旧**保留行起读，`nextCursor` 增量翻页，`limited` 标记还有页——长会话必须增量累积。

## D. 实跑 / skip 分类（最终判定轮）

6 项全实跑（dais 编排面于本 worktree 执行中段恢复在线，末轮稳定）：offline 1 + live 5。

窗口期过程记录（诚实留痕）：
- 首轮（dais down）：offline ✅ / live_a_b **skip（验收③生效）** / dual 转真跑失败（dais 中途恢复的瞬时波动，非本票用例）/ smoke ✅（车道B 全程不受 dais 影响）/ ab 未跑完（外层超时截断）。
- 次轮（dais 已恢复）：5 过 + ab-laneB 失败——暴露 C.2 ring buffer 盲区（文本提取假阳性/真阴性），即本票核心修正点。
- 末轮（文件产物契约）：6/6 全绿，45.25s。

## E. 残留核验（红线：不留后台进程/临时 worktree）

- `orca-ide worktree list` 无任何 `vo009-*` worktree；
- `git worktree list` 仅主 checkout 与本票 vo-009 worktree 自身；
- `pgrep -af vo009` 无进程；
- 测试 teardown（`_orca_teardown`）内建 `terminal stop` + `worktree rm --force` + `worktree_ps` 残留断言，任何退出路径（含 assert 失败）都清理并校验。

## F. 范围与红线自查

- 仅改 `tests/test_rt_conformance.py`（+evidence 本报告）；`rt_orca_lane.py`/`src/pipecat/` 零改动（只读引用）。
- 协议常量零触碰：`FINAL_PREFIX`/`[ref:]`/凭证格式全部 import 复用既有定义（G5）。
- 未 push、未 git commit、未 spawn dais 实例。

---
done body:
`通过;报告:docs/kg/evidence/VO-009-report.md;测试:6项<实跑6+skip0>;备注:车道B真spawn文件终稿契约,A/B统一邮箱链同终稿,窗口期skip路径已验`
