# VO-001 · Projector 第 17 维 agent_role —— 证据报告

- 日期：2026-08-23 · 判定：**PASS**
- 依据：`docs/tickets.md` VO-001 节 · `docs/plans/impl-specs.md` VO-001 节（验收①–⑤）· KG 06 §1.1–1.2
- 改动面（仅三文件，越界=0）：
  - `examples/realtime-provider-poc/rt_projector.py`：新增 `ROLE_TEMPLATES`（liaison/manager/worker/supervisor 四键，worker=空串）；`project()` 增 `role="worker"` 关键字参数；`profile_json` 落 `agent_role` 键；三门 fail 时升温重投影 ≤2（原仅解析/传输失败重试，本票补齐门失败重试，`max_retries` 语义不变）。
  - `tests/test_rt_projector.py`：扩 VO-001 段 11 个用例（role 参数化 + 门失败重试）。
  - `tests/test_projection_live.py`：增 role 冒烟 1 例（真实 GLM，role=liaison）；旧用例收敛（projector 已内置门失败重试，去掉测试级二次投影循环）。

## B 节 · 验收①–⑤ 逐条判定

| # | 验收条款 | 判定 | 证据 |
|---|---|---|---|
| ① | `project(role="liaison")` 产物含收敛契约/两阶段/`[ref:]`/凭证回显四条款 | PASS | `ROLE_TEMPLATES["liaison"]` 按 KG 06 §1.2 liaison 行固化；单测 `test_project_role_liaison_product_carries_four_clauses` + `test_liaison_template_pins_kg06_clauses`（幂等可重放 / `{status:accepted, run_id, ref, credentials}` / `FINAL_PREFIX` / `[ref:]` / `【凭证` 四条款逐项断言）；live 冒烟对真实 GLM 产物同断言通过 |
| ② | `role="manager"` 含车道选择/`--dep` 拆分/worker_done 等待/异常上抛 | PASS | `test_manager_template_pins_kg06_clauses`：域职责/orca+dais 车道选择/`--dep`/`worker_done`/`resolve-gate`+`scan-wait-blocked`+supervisor 上抛全覆盖 |
| ③ | `role="worker"` 输出与现行完全一致（回归锚） | PASS | `test_project_role_worker_is_current_pipeline_verbatim`：缺省调用与显式 `role="worker"` 三元组（agents_md/profile_json/priors）全等，agents_md==GOOD_MD 原文；`ROLE_TEMPLATES["worker"]==""` 且追加分支对空串短路 |
| ④ | role 产物过三门（gate1 对模板自身条款同样生效） | PASS | `test_project_role_products_pass_three_gates`（liaison/manager/supervisor 参数化 × run_gates 全过）+ `test_role_templates_pass_gate1_themselves`（模板文本自身 gate1 零命中）；实现侧 `project()` 对拼合产物先跑 `run_gates`，不过门即升温重试 |
| ⑤ | `profile_json.agent_role` 落键且不进正文 | PASS | `test_agent_role_traced_in_profile_only`：`profile_json["agent_role"]` 落键、`"agent_role" not in agents_md`；live 两例断言 `agent_role` ∈ {worker, liaison} |

补充保障：
- 非法 role 早拒：`test_project_rejects_unknown_role_before_any_call`（先于任何 GLM 调用抛 `ValueError`）。
- 门失败升温重试（本票补齐项）：`test_project_warm_retries_on_gate_failure`（首投 gate1 泄漏 → 0.2 温度重投影过门）；`test_project_raises_after_gate_retry_budget`（连续不过门 → 3 次调用后 `ProjectionError`）。

## 红线核验（G3/G5）

- **G5 协议不漂移**：`FINAL_PREFIX` 由 `rt_orchestrator.py` import 逐字内嵌模板（`from rt_orchestrator import FINAL_PREFIX`，f-string `{FINAL_PREFIX!r}`），无复制粘贴改写风险；`[ref:<ref>]` / `【凭证…】` 格式与现行 `make_credential` 常量一致；三门逻辑（`rt_projection_gates.py`）零改动，仅被 projector 调用。
- **G3 范围**：改动仅落本票三文件；未碰 `src/pipecat/`、未改协议常量定义处。
- import 无回环：`rt_orchestrator.py` 不依赖 `rt_projector.py`。

## A 节 · 测试输出原文

票定命令（cwd=本 worktree，主区 venv 绝对路径）：

```
$ ~/workspace-claw-02/pipecat-poc/.venv/bin/python -m pytest tests/test_rt_projector.py -q
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: ~/orca/workspaces/pipecat-poc/vo-001
configfile: pyproject.toml
plugins: anyio-4.14.2, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=function, asyncio_default_test_loop_scope=function
collected 35 items

tests/test_rt_projector.py ...................................           [100%]

============================== 35 passed in 1.10s ==============================
```

live 冒烟（真实 GLM；shell 带 socks5 代理 env 会令 httpx 构造失败，跑 live 需清代理变量——环境事项，非代码问题）：

```
$ env -u ALL_PROXY -u all_proxy -u HTTP_PROXY -u http_proxy -u HTTPS_PROXY -u https_proxy \
    ~/workspace-claw-02/pipecat-poc/.venv/bin/python -m pytest tests/test_projection_live.py -q
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: ~/orca/workspaces/pipecat-poc/vo-001
configfile: pyproject.toml
plugins: anyio-4.14.2, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=function, asyncio_default_test_loop_scope=function
collected 2 items

tests/test_projection_live.py ..                                         [100%]

======================== 2 passed in 103.81s (0:01:43) =========================
```

改动清单（本 worktree，未 commit，待编排者统一合并）：`examples/realtime-provider-poc/rt_projector.py`、`tests/test_rt_projector.py`、`tests/test_projection_live.py`。

PASS;报告:docs/kg/evidence/VO-001-report.md;测试:37项全绿(35单测+2live);备注:role维度落地;FINAL_PREFIX逐字内嵌;三门逻辑零改动
