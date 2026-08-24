# 派票执行方案（dispatch plan）· orca worktree 无竞争并行 + 合并管理

> 版本 v2 · 2026-08-23 · 依据：`docs/tickets.md`（VO-001..012）+ `docs/plans/impl-specs.md`（验收编号）
> 执行体：**omp（oh-my-pi coding agent v18.0.1，`~/.bun/bin/omp`，GLM-5.3 @zhipu-coding-plan 路由）跑在 orca worktree** = 车道B dogfood；插件链主线串行（非 git 域）。
> 事实核查（2026-08-23 全部实测）：pipecat-poc 在 `main`，25 路径未提交；`~/.dsh/plugins/a2a-profile-server` **非 git**；**orca repo 已注册**（`repo add --path`，repoId=b58d1d62）；worktree 落点 **`~/orca/workspaces/pipecat-poc/<name>`**；`worktree create --agent` 只识内置 agent（codex/claude 等）**不识 omp** → omp 起法 = `terminal create --worktree <sel> --command "omp"`（TUI 起、pyright LSP+5 MCP 挂载、`wait --for tui-idle` 满足、`send --text … --enter` 提交，GLM 真回包经 `~/.omp/logs` 验证 agent_end hasText:true）。

## ⚠ omp 派发三条实测纪律（v2 新增，覆盖 v1 的 codex 协议）

1. **起法**：`worktree create`（不带 `--agent`）→ `terminal create --worktree name:<wt> --command "omp" --json` → 记 handle；
2. **监控三源**（alt-screen 帧捕获**不可靠**——响应文本不进 `terminal read` 帧）：①`~/.omp/logs/omp.<date>.<pid>.log` 尾追（`agent_end … hasText/hasToolCalls/stopReason` 为权威完成信号）②worktree 内 `git status/diff`（真实产物）③票面报告文件落盘；`terminal read` 仅辅助看输入态，`wait --for tui-idle` 仍有效；
3. **验收不看 TUI 渲染**：票 done 判定 = 测试绿 + 报告落盘 + done body（编排者从 worktree 文件直接读），与帧捕获解耦。

## 0. 核心原则：文件域独占 = 无竞争

每张票有一个**独占文件域**（下表）；不同 lane 的域两两不相交 → worktree 分支 rebase onto main 必然无冲突（出现冲突 = 票超范围，打回）。**同文件域的票绝不并行**（唯一两组：004 与 002 同改 http-server.js；009 与 005 同改 test_rt_conformance.py → 用依赖序串开）。

### 冲突面矩阵（票 × 文件域）

| 票 | 仓库内文件（可 worktree） | 仓库外（非 git，主线原地） | 冲突约束 |
|---|---|---|---|
| 001 | `rt_projector.py` · `test_rt_projector.py` · `test_projection_live.py` | — | 独占 |
| 002 | — | 插件 `http-server.js`·`incubators/real.js`·`selftest.mjs` | 插件域 |
| 003 | `test_rt_fleet_registry.py`(new) | 插件 `registry.js`(new) | 插件域 |
| 004 | `test_rt_router.py`(new) | 插件 `http-server.js`(改)+journal | **与 002 同文件** → 串行 |
| 005 | `test_rt_conformance.py` | — | conformance 域 |
| 006 | `live_v5_v6_dsh.py`·`test_live_v5_v6.py` | dsh 会话（live） | live 域 |
| 007 | `test_live_v7.py`(new) | dsh 会话（live） | live 域（继 006） |
| 008 | `rt_orca_lane.py`(new)·`test_rt_orca_lane.py`(new) | — | 独占 |
| 009 | `test_rt_conformance.py` | — | **与 005 同文件** → 005 合并后开工 |
| 010 | — | `~/.agents/skills/incubation-wizard/` | 独占（非 git） |
| 011 | `rt_gateway.py`(new)·`test_rt_gateway.py`(new)·`web/` | — | 独占 |
| 012 | docs 全域 + 全量回归 | dogfood 会话 | 收口串行 |

## 1. Wave 0 · 基线准备（派发前一次性）

| 步 | 命令/动作 | 状态 |
|---|---|---|
| 0.1 | commit 现有 25 路径（拆 3 条逻辑提交：①`src/pipecat/services/qwen/` 方言层 ②examples+tests 语音链全部 ③docs：KG/报告/票板/specs/dispatch） | ⬜ 待做（**不 push**，沿 SI-002 先例） |
| 0.2 | ~~`orca-ide repo add`~~ | ✅ 已完成（`repo add --path`，repoId=b58d1d62） |
| 0.3 | ~~探针 worktree~~ | ✅ 已完成（omp 起法/监控/清理全链验证，见头部纪律） |

**worktree 测试口径**（写进每张派发 prompt）：worktree 无 `.venv`，统一用主区绝对路径——`cd <worktree> && ~/workspace-claw-02/pipecat-poc/.venv/bin/python -m pytest <目标测试> -q`（GLM 走 `~/.dsh/zhipu.env`、dais 二进制均为绝对/全局路径，跨 worktree 自洽）。**live 票（005–007）只在主区跑**（真孵化+邮箱全局态）。

## 2. Lane 设计（并行拓扑）

```
Lane A (worktree vo-001):  001 ──────────▶ 完
Lane B (worktree vo-008):  008 ─▶ [005已并] ─▶ 009(同分支续作)
Lane C (worktree vo-011):  011 ──────────▶ 完
Lane P (主线串行): 002 → 003 → 004 → 005 → 006 → 007   （插件非 git 必须串行；005 起 live 主区）
插空:            010（待 002 完，任意时机；文件域独立）
收口:            012（待 007+009+010+011 全 ☑）
```

| Lane | 载体 | 理由 |
|---|---|---|
| A/B/C | orca worktree（`~/orca/workspaces/pipecat-poc/vo-00X`）+ omp agent（terminal create --command omp） | 文件域独占可真并行；B 车道 dogfood |
| P | **主区** + 插件目录原地；执行体仍为 omp（主区终端起） | 插件域非 git 无 worktree；live 需主区 .venv 与全局 dais 态；P 链内部本就串行 |
| 010/012 | 主线 | 非 git 域 / 收口回归 |

**P 链与 worktree 的交叉规则**：P 链在 main 上持续提交（插件+测试文件）；worktree 合并前 `rebase onto main`——文件域隔离 → rebase 必净。**main 任何时刻保持全量回归绿**（合并原子性，见 §4）。

## 3. 派发协议（每票命令级；omp 版）

```bash
# 3.1 建 worktree（不带 --agent；正式一律 --setup skip）
orca-ide worktree create --repo path:~/workspace-claw-02/pipecat-poc \
  --name vo-001 --setup skip --json
# → worktree: ~/orca/workspaces/pipecat-poc/vo-001

# 3.2 起 omp agent + 注票
orca-ide terminal create --worktree name:vo-001 --command "omp" --json   # → 记 handle
orca-ide terminal wait --terminal <handle> --for tui-idle --timeout-ms 30000 --json
orca-ide terminal send --terminal <handle> --text "$(cat <<'EOF'
执行票 VO-001。先读 docs/tickets.md 的 VO-001 节与 docs/plans/impl-specs.md §VO-001（验收①–⑤逐条对）。
范围（只许改这三个文件，越界即打回）：
  examples/realtime-provider-poc/rt_projector.py
  tests/test_rt_projector.py
  tests/test_projection_live.py
测试（本 worktree 无 venv，用主区绝对路径；cwd=本 worktree）：
  ~/workspace-claw-02/pipecat-poc/.venv/bin/python -m pytest tests/test_rt_projector.py -q
红线：impl-specs G1–G6；不改协议常量（FINAL_PREFIX/[ref:]/凭证格式）；不碰 src/pipecat/；不 push。
live 纪律（G6）：涉及 live 用例的票——超时/预算常量先实测后定（预算 ≥ 实测 P95 × 2，
实测记录与常量同文件注释互指，P95 基线 docs/kg/evidence/VO-006-report.md）；并发跑测试
须让 live 用例走 @pytest.mark.live 声明域（tests/live_lock.py 自动串行，DSH_LIVE_LOCK=skip 让路）。
完成交付：①测试全绿 ②报告落盘 docs/kg/evidence/VO-001-report.md（A 节贴测试输出原文）
  ③报告末行 done body：`<判定>;报告:docs/kg/evidence/VO-001-report.md;测试:<N>项全绿;备注:<≤40字>`
EOF
)" --enter

# 3.3 监控（三源，见头部纪律）
tail -f ~/.omp/logs/omp.$(date +%Y-%m-%d).*.log | grep --line-buffered agent_end   # 权威完成信号
git -C ~/orca/workspaces/pipecat-poc/vo-001 status --short                  # 真实产物
ls docs/kg/evidence/VO-001-report.md                                                # 报告落盘
orca-ide terminal read --terminal <handle> --limit 40                               # 辅助（输入态）

# 3.4 验收（合并前，worktree 内）
cd ~/orca/workspaces/pipecat-poc/vo-001 && \
  ~/workspace-claw-02/pipecat-poc/.venv/bin/python -m pytest tests/test_rt_projector.py -q
```

P 链票同模板，载体改为主区终端（`dais orchestration new-terminal ~/workspace-claw-02/pipecat-poc` 起会话后注入 `omp`，或 orca terminal create 于主 worktree），范围列改为插件绝对路径 + 仓库测试文件；**P 链 live 票测试命令直接主区 .venv 相对跑**。

## 4. 合并管理（merge protocol）

每票五步，**原子合入**：

1. **票内自验**：worktree 内目标测试绿 + done body 格式合法（编排者 terminal read 抓末行）；
2. **rebase**：`git -C <worktree> rebase main`——预期零冲突；**冲突 = 票超范围，打回重做**（红线自动守卫）；
3. **合入**：`git -C 主区 merge --ff-only <分支>`（或常规 merge，禁 squash 保票对应 commit 可追溯）；
4. **门禁**：主区全量回归 `94+新增 全绿` 才 ☑；红 → `git reset --hard ORIG_HEAD` 回滚该合并，票打回；
5. **清理**：`orca-ide worktree rm --worktree path:<worktree路径> --force` + 票板置 ☑ + 账本追加一行。

合并顺序（Wave 1 完成后）：**001 → 008 → 011**（任意序皆可，域隔离；按完成序）。009 严格在 005 合入后由 Lane B 分支 rebase 续作。

## 5. 时序与依赖总览

| 阶段 | 动作 | 阻塞关系 |
|---|---|---|
| Wave 0 | commit×3 + repo add + worktree 探针 | 全部前置 |
| Wave 1 | 三 worktree（001/008/011）并行 + P:002 | 互不阻塞 |
| 流式 | P:003→004→005→006→007；010 待 002；009 待 008+005；各票完成即按 §4 合入 | 依赖驱动，非死波次 |
| 收口 | 012（全量+dogfood） | 007∧009∧010∧011 |

## 6. 回滚与风险

| 风险 | 对策 |
|---|---|
| agent 越界改文件 | rebase 冲突自动暴露 → 打回；prompt 红线前置 |
| worktree setup hooks 重装依赖 | 0.3 探针钉死；正式一律 `--setup skip` + 主区 venv 绝对路径 |
| 合并后全量红 | §4 步 4 `reset --hard ORIG_HEAD` 原子回滚 |
| live 票污染全局态（dais 会话/邮箱） | live 只在 P 链主区串行；测试自带 handle 清理（V5/V6 先例） |
| omp agent 对票规理解漂移 | prompt 固定引用 tickets.md+impl-specs 节；验收编号逐条对；编排者读 worktree diff + 报告抽查（TUI 帧不可靠，见纪律 2/3） |

## 7. 开工判定

Wave 0 三步完成后即可放 Wave 1（四路并行：3 worktree + P 链 002）。**本方案待用户点头即执行 Wave 0。**
