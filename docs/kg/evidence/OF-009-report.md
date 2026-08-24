# OF-009 报告 · live 预算 doctrine + 并发租约（D-12/D-13）

> 2026-08-24 · 票面：docs/kg/09-orch-hardening-plan.md §5 OF-009 · 实施：POC 仓 commit `628d97d`
> 前置解除：OF-009 tests 部分原排 VO-012 合入后；VO-012 已 ☑（12/12 收口，2026-08-24）。

## A. 验收对照

| # | 票面验收 | 证据 |
|---|---|---|
| ① | 两 pytest 进程并发跑同域 live 用例：后者等锁至前者释放，零互踩 | `tests/test_live_lock.py::test_two_pytest_processes_serialize`——两个并发 pytest 各跑 1.2s 用例，总耗时 ≥2.0s 断言不重叠（实测 2.4s+，7/7 绿） |
| ① | `skip` 模式确定性跳过 | `test_skip_env_downgrades_to_skip`——`DSH_LIVE_LOCK=skip` + 持锁 1.5s → 用例 `-rs` 留痕 `live lease unavailable` skip |
| ② | 非 live 用例与未加锁域行为=现行 | 全量回归 170+ passed（13 rt 文件 + live_lock + m0；非 live 用例不进 setup 钩子） |
| ③ | 公式与常量互指可 grep | impl-specs **G6**（预算 ≥ 实测 P95 × 2，P95 基线引 VO-006-report）↔ `tests/live_lock.py` 模块 docstring（warn_after_s 同一 P95 基）↔ dispatch-plan §3.2 派发模板 live 纪律段，三处双向锚 |
| ④ | 锁等待超时告警（不静默死等） | `live_lease(warn_after_s=…)` 超阈值打一行 `WARNING: live_lease '<domain>' still waiting…` 继续等；`test_blocking_wait_warns_loudly` 断言 |

## B. 实施要点

- **`tests/live_lock.py`**：flock 跨进程租约，按资源域（`dais-bus`/`orca-host`/邮箱名）一域一锁文件（`/tmp/dsh-live-locks-<uid>/`）；block/skip 双模式（`DSH_LIVE_LOCK`），holder pid 落锁文件。
- **`tests/conftest.py`**：`@pytest.mark.live` / `@pytest.mark.live("dais-bus","orca-host")` / `live(domains=[…])` → `pytest_runtest_setup` 按域名排序取锁（防死锁）→ teardown 释放；skip 模式拿不到 → `pytest.skip` 留痕让路。marker 注册入 pytest_configure。
- **8 个 live 用例标记**：conformance×4（lane_a_b、dual_delivery、lane_b_smoke、ab_lane_final）、live_v5_v6×1、live_v7×1、m0×2（probe_matrix、evidence_written）。
- **在案记录**（票面尾项）：VO-007 的 "manager mailbox 每运行唯一后缀" 现行缓解 = 租约思想的局部先例（命名隔离替代互斥）；本票将机制统一为 flock 域租约，后缀法保留为兼容层不拆除。

## C. 顺带修复（同 commit）

1. `test_rt_projector` ROLE_TEMPLATES 断言补 `queen` 键——N10 回流（735df3a）时模板加 queen、断言未同步的全量回归红。
2. lane A/B conformance 重试面收宽：`AssertionError`（finals 缺失）与 `DaisLaneError` 同入 attempt("-r2") 重试——VO-005 以来 laneA 推模式 final 偶发丢失（环境敏感 flake，复现率 ~1/3），重试轮重启全新 head 后稳定。
3. **lane-B worker 默认 agent 改 omp**（用户裁决 2026-08-24）：`ORCA_AGENT = env ORCA_AGENT ?? "omp"`；orca 经 `--agent omp` 自解析已知 TUI agent（实测启动+回话正常，GLM-5.3），`shutil.which` PATH 守卫移除；`/effort low` 舞步收敛为 claude-only。omp 冒烟 20.4s 绿；conformance 6/6（laneB-orca 对拍首次真跑过，此前被 claude-era skip 面挡住）。

## D. 验证输出

```
$ pytest tests/test_live_lock.py -q
7 passed in 9.30s
$ pytest tests/test_rt_conformance.py -q        # omp 默认
6 passed in 40.11s
$ pytest <13 rt 文件 + live_lock + m0> -q
170 passed, 1 skipped（修复 projector 断言后全绿）
```

## E. 台账回写

- D-12 ✅（G6 公式 + 三处互指锚）
- D-13 ✅（flock 域租约 + 双进程演练）
- maestro ledger：OF-009 blocked → done（POC 侧交付如上；doctrine 端 impl-specs/dispatch-plan 已同步）

done body：`完成;报告:docs/kg/evidence/OF-009-report.md;测试:7+6+170全绿;台账:D-12 ✅ D-13 ✅;备注:laneB 默认 omp(用户裁决),flaky 重试加固`
