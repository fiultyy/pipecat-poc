# VO-002 证据：incubate 扩参 + dsh-liaison/dsh-manager 孵化目标

- 日期：2026-08-23（round 7）
- 票：`docs/tickets.md` VO-002 · 规范：`docs/plans/impl-specs.md` VO-002 · 方法级：KG 06 §1.3–1.4（§1.2 条款 / §1.5 fleet 形制）、KG 02 §3/§5.1（现行锚点）
- 范围（三处，越界零改动）：`~/.dsh/plugins/a2a-profile-server/http-server.js`（incubate 分支）、`.../incubators/real.js`（incubateDsh）、`.../selftest.mjs`（扩用例）。插件目录不在本仓 git 内，直接改文件；未 git commit。

## 改动摘要

| 文件 | 改动 |
|---|---|
| `http-server.js` | incubate 分支增 `role`（∈ liaison/manager/worker/supervisor，非法/与 role-target 冲突 → `-32602`）、`project`、`mailbox`（缺省 `agent_<name>`）三参校验；`ROLE_TARGETS` 映射 `dsh-liaison/dsh-manager → dsh 孵化器 + 蕴含 role`；扩展仅在显式传参或 role-target 时向 ctx 注入三元组，纯现行调用 ctx 形状不变；三门前置与六 RPC 既有签名语义零改动（纯增量） |
| `incubators/real.js` | `incubateDsh` ctx 增 `{role, project, mailbox}`：purpose 缺省携 role（`"<role> for voice orchestration"`）、marker `vh-<name>` 不变；注入体 = role doctrine 段（KG 06 §1.2 liaison 四条款 / manager 五条款 + §1.6 回合首拉取行）在前 + agentsMd 在后；fleet 登记项扩 `role/project/mailbox/profile_version/spawned_at`（temp+rename 原子写）；返回 `target:'dsh-<role>' + sessionId/code/mailbox/role/project`。无 role 路径与现行字节级一致。内部：`SESSION_SPAWN/DSH_PORT/MAESTRO_FLEET` 改调用时求值（env 可覆写，缺省值不变，供 selftest 注入 mock）；`resolveSessionId` 反查规则原样抽为 `findFleetEntry` 共用 |
| `selftest.mjs` | 新增 mock dsh 宿主（mock session-spawn 真写 fleet.json + mock loopback 记录 session.prompt 注入体，经 env 注入，不真孵化）+ T11–T15 五组用例 |

worker 角色无 doctrine（现行零变化）；supervisor 为合法 role 值、doctrine 仅预留一行（W5.4 后展开）。

## A. selftest 输出原文

```
$ cd ~/.dsh/plugins/a2a-profile-server && node selftest.mjs
[ ok ] T01 agent-card
[ ok ] T02a send → submitted
[ ok ] T02b lifecycle → completed
[ ok ] T02c artifact 契约（Final 前缀 + 凭证）
[ ok ] T03a cancel
[ ok ] T03b 终态保护
[ ok ] T04 错误面 -32601/-32602
[ ok ] T05 token 鉴权
[ ok ] T06 journal 重放
[ ok ] T07a incubate v1
[ ok ] T07b 重投影 v2 + 归档
[ ok ] T08 history.jsonl
[ ok ] T09 三门拦截（框架术语拒收）
[ ok ] T10 profiles/list+get
[ ok ] T11 缺参回归一致（现行语义不变）
[ ok ] T12 role 非法/冲突 → -32602
[ ok ] T13a dsh-liaison 回执含 sessionId/code/mailbox/role/project
[ ok ] T13b fleet 登记项扩五键（role/project/mailbox/profile_version/spawned_at）
[ ok ] T13c 注入体 = role doctrine 在前 + agentsMd 在后
[ ok ] T13d purpose 携 role、preset/marker 不变
[ ok ] T14 dsh-manager 缺省 role 推导 + mailbox 默认
[ ok ] T15 扩参三门照常拦截（-32000，不触发 spawn）

22 passed, 0 failed
```

## B. 仓库侧回归（现行不破坏）

```
$ .venv/bin/python -m pytest tests/test_rt_a2a_client.py tests/test_incubators_real.py -q
collected 8 items
tests/test_rt_a2a_client.py .....                                        [ 62%]
tests/test_incubators_real.py ...                                        [100%]
============================== 8 passed in 2.02s ===============================
```

注：`test_dsh_incubation_real` 为真孵化冒烟（dsh 宿主在线，端口 3080）——即改动后 `incubateDsh` 缺省路径经真 session-spawn + fleet 反查端到端复验（receipt `target=='dsh'`、code 4 位、fleet 在册），现行语义回归成立。基线对照：改动前 selftest 14/14 绿，改动后 14 项基线全数保持。

## C. 验证目标对照（impl-specs VO-002 ①–⑤）

| # | 验收 | 证据 |
|---|---|---|
| ① | 缺参调用行为与现行完全一致 | T11（dry 回执同形、ctx 无三元组）+ B 节真孵化回归 + 基线 14 项全保持 |
| ② | `targets:["dsh-liaison"], role:"liaison", mailbox:"agent_liaison"` → receipts 含 `sessionId/code/mailbox/role/project` | T13a（五键全断言）；另 T13b fleet 扩五键、T13c 注入体 doctrine 在前+agentsMd 在后+回合首拉取携句柄、T13d purpose 携 role |
| ③ | role 非法值 → `-32602` | T12（非法值 `chief` 与 role/target 冲突两路，消息分别含 `invalid role`/`mismatch`） |
| ④ | 三门前置照常拦截（gate fail → `-32000`） | T15（扩参调用 + 术语泄漏 → `-32000`，且未触发 spawn/prompt） |
| ⑤ | node selftest 扩参用例绿 | A 节 22/22 |

## D. 设计决策与遗留

- **role-target 映射位置**：`ROLE_TARGETS` 落在 `http-server.js`（ctx.target 归一为 `dsh` 再分发）——`incubators/index.js` 注册表不在本票范围，不越界。
- **扩展激活判据**：显式 `role/project/mailbox` 任一存在，或 target 为 role-target；缺省（现行调用）ctx 与回执形状完全不变（红线①的落点）。
- **有效 role 推导**：显式 `role` 优先；否则全部 role-target 蕴含同一 role 时采用（如 `dsh-manager` 单目标 → manager）；混合/无则 worker 兜底。
- **遗留（范围外）**：`profile.json.agent_role` 落盘需 `profile-store.js:79` 扩展落盘键（现行只写 scenario/vector19/description 三键，多余键被丢弃）——建议随 VO-003 registry 线一并处理；本票 role 可追溯性由 fleet 扩展项 + 回执承载。
- **live 真孵化 dsh-liaison 冒烟**：留待 VO-006（liaison 落位）与 dais 在线窗口；本票验证形式为 mock（票面"自动=selftest(mock session-spawn)"已满足）。

done PASS;报告:docs/kg/evidence/VO-002-report.md;测试:30项全绿;备注:扩参纯增量,缺省现行不变;agent_role落盘留VO-003
