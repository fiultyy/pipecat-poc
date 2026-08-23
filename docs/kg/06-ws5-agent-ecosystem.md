# N6 · W5 dsh 内 agent 生态：孵化扩展 + 内部通信（方法级）

> 上游：`docs/plans/voice-orchestration-head-plan.md§5` · 报告 §4 · 索引：[00-INDEX.md](00-INDEX.md)
> 原则：对接 agent（liaison）与 manager 级 agent 同样是孵化池产物（prompt 来自 N3 投影）；dsh 从"单会话容器"升级为"可孵化、可互通、可续命的 agent 生态底座"。
> 前置事实：liaison 的对外协议已由 live V5/V6 的替身 `orchestrator_player`（`〔loc:examples/realtime-provider-poc/live_v5_v6_dsh.py:126〕`）全链验证——W5.3 只换真身，协议零改动。

## 0. 对接总图（dsh 宿主内部）

```
head ──F4 语义指令([ref:]信封)──▶ dais 邮箱 ──▶ liaison 句柄（W5.3）
                                                │ F5 语义收敛 → 稳定指令
                                                │ F6 router agents/send（DSHMSG 推 + 邮箱投）
                                                ▼
                                     manager 群 × N 域（W5.4）
                                                │ F7 编排 skill 选车道
                                     ┌──────────┴──────────┐
                              车道A dais ADE          车道B orca ADE（M3+）
                                     └──────────┬──────────┘
                                          F8 worker_done → 域汇总
                F9 manager→liaison 回信 ◀──────┘
                F10 liaison→head 邮箱 终稿（FINAL_PREFIX + 凭证）
孵化供给线：H1 Projector(role 选型) → H2 三门 → H3 incubate(扩参) → H4 session-spawn → H5 arm
通信层：推 = session-send DSHMSG 注入；拉 = dais 邮箱有界快照；router 三 RPC 挂孵化池插件
```

## 1. W5.1 孵化能力扩展（incubation v2）

### 1.1 Projector 第 17 维 agent_role

`〔loc:examples/realtime-provider-poc/rt_projector.py:27→SOURCES〕` 增 role 模板源；`〔loc:...rt_projector.py:159→project〕` 签名扩展：

```python
async def project(self, scenario: str, *, answers=None,
                  role: str = "worker") -> Projection:
    """role ∈ {liaison, manager, worker, supervisor}；worker 走现行 16+3 维流水线不变；
    liaison/manager 追加 role 模板段落（§1.2），三门照常适用——agent 间协议同样术语零暴露。"""

ROLE_TEMPLATES = {                       # 〔new:...rt_projector.py→ROLE_TEMPLATES〕
  "liaison":  "<收敛契约 + 两阶段应答协议 + [ref:] 信封规则 + 凭证逐字回显纪律>",
  "manager":  "<域职责 + A/B 车道选择策略 + 子任务拆分与依赖 + worker_done 等待 + 异常上抛>",
  "worker":   "",                        # 现行通用投影，零变化
  "supervisor": "<超时/上抛受理（role 预留，W5.4 后视需要展开）>",
}
```

`Projection.profile_json` 增 `"agent_role"` 字段（第 17 维向量项，进 profile.json 可追溯、不进 AGENTS.md 术语面）。

### 1.2 role 模板内容契约（投影产物必须固化）

| role | 模板固化条款 |
|---|---|
| liaison | ① 语义→稳定指令收敛规则（自包含、指代全展开、幂等可重放）② 两阶段应答：受理回执 `{status:accepted, run_id, ref, credentials}` + 终稿 `"Agent Final Message":\n\n` 前缀（`〔loc:...rt_orchestrator.py:48→FINAL_PREFIX〕`）③ `[ref:<ref>]` body 前缀信封（复用 `〔loc:~/.dsh/maestro/bin/cb-send〕` 解析习惯）④ 凭证【凭证…】逐字回显 |
| manager | ① 域职责边界 ② 车道选择（终端/工作树→orca；消息 DAG/轻量 fan-out→dais）③ 拆分产物 = 子任务 + `--dep` 依赖表 ④ `worker_done` 块匹配等待（免轮询）⑤ 异常上抛：gate→`resolve-gate`、卡死→`scan-wait-blocked`、超时→supervisor |

### 1.3 incubate RPC 扩展（孵化池六 RPC 之一）

现行实现 `〔loc:~/.dsh/plugins/a2a-profile-server/http-server.js:102→incubate handler〕`；扩展参数面：

```javascript
// params 增列（向后兼容：缺省即现行语义）
{ name, projection:{agents_md, profile_json, description},
  targets:["dsh"],                    // 新合法值 "dsh-liaison" / "dsh-manager"
  role:"worker",                      // 校验 ∈ ROLE_TEMPLATES；写入 profile.json.agent_role
  project:"<域标识>",                  // fleet 登记 + router scope 键
  mailbox:"agent_<name>" }            // 缺省即此默认；dais 邮箱句柄，liaison 必填
// result 增列
{ profile:{name,version}, receipts:[{target:"dsh-liaison", sessionId, code,
   mailbox:"agent_x", fleetEntry:{role,project,mailbox,profile_version,spawned_at}}] }
```

### 1.4 孵化目标 dsh-liaison / dsh-manager（复用 dsh 孵化器）

`〔loc:~/.dsh/plugins/a2a-profile-server/incubators/real.js:59→incubateDsh〕` 现行流程（preset=maestro、`session-spawn` 取 4 位 code、fleet.json 反查 sessionId、`ORCA-CB] PROFILE-INJECT] <name>@v<version>` 首回合注入）原样复用；扩展点：

```javascript
export async function incubateDsh(ctx) {
  // ctx 增 { role, project, mailbox }
  // 1) marker = `vh-${name}` 不变；purpose 携 role（如 "liaison for voice orchestration"）
  // 2) 注入体 = ROLE_TEMPLATES[role] 段落 + agentsMd（role doctrine 在前，行为准则在后）
  // 3) fleet 登记项扩展（§1.5）；mailbox ≠ code 时向 dais 侧建别名句柄（liaison 用）
  // return { target:'dsh-'+role, name, version, code, sessionId, mailbox, role, project }
}
```

### 1.5 Fleet 元数据扩展 + reattach

`session-spawn` 的 fleet.json 原子登记项（现行 `〔loc:...incubators/real.js→resolveSessionId〕` 只反查 sessionId）扩展为：

```jsonc
{ "fleet": { "<code>": { "sessionId": "session-xxxx", "title": "ORCH/<node>-…",
    "role": "liaison|manager|worker|supervisor",     // 新增
    "project": "<域>",                                 // 新增：router scope 键
    "mailbox": "agent_<name>",                         // 新增：dais 邮箱句柄
    "profile_version": 3,                              // 新增：孵化所用 profile 版本
    "spawned_at": 1724... } } }                        // 新增
```

**reattach 算法**（插件启动时执行，`〔new:~/.dsh/plugins/a2a-profile-server/registry.js→reattach()〕`）：

```
for entry in fleet where role ≠ worker:
    sid = 查 dsh 会话列表（loopback session.list）
    sid 存在 → registry 登记 {code, sessionId, mailbox, state:"reattached"}
    sid 不存在 → 标记 retired + 记 journal {op:"orphan", code}（孤儿检测）
```

### 1.6 生命周期状态机

```
spawn ──(开会话+注 profile)──▶ arm ──(注入 role doctrine + 通信契约)──▶ ready
ready ──(首次心跳)──▶ serving ──(retire)──▶ retired
```

| 迁移 | 触发 | 实现 |
|---|---|---|
| spawn→arm | incubate 完成（§1.4 步骤 2 注入后追加通信契约段） | incubateDsh |
| arm→ready | 首次心跳：router 经 loopback `session.list` 探活会话（**dsh 会话非常驻轮询者**，不由 agent 自发起） | `〔new:registry.js→heartbeat()〕` |
| ready→serving | router 首次 `agents/send` 投递成功（DSHMSG 注入触发回合） | registry |
| serving→retire | 显式取消会话 + fleet 标记；**持久 profile 保留可复活**（同 name 重孵化即复活，ProfileStore 版本延续） | registry.retire |

**唤醒模型**（dsh 会话 = 按回合执行，非守护进程）：**推唤醒**——router `agents/send` 经 session.prompt 注入 DSHMSG 即触发目标会话新回合；**回合内拉取**——agent doctrine 固化"回合首动作 = `check-messages <自身句柄> --timeout-ms` 快照排空邮箱取正文"。探活一律 router 侧驱动（loopback `session.list` 比对 fleet）。

## 2. W5.2 内部通信层（打破会话隔离，受控）

### 2.1 通信原语（与既有契约同构，零新概念）

信封 `{"from","to","ref","type","body"}`；两阶段（ack 含 ref → 终稿 FINAL_PREFIX）；凭证逐字；防自匹配三过滤（`〔loc:examples/realtime-provider-poc/rt_dsh_backend.py:134→_phase2〕` 同款：ref 命中 ∧ `seq > intent_seq` ∧ from 校验）。

### 2.2 推送模式：session-send（已存在底座）

`〔loc:~/.dsh/maestro/bin/session-send:10→信封〕`——收方回合首行：

```
DSHMSG]{"from":"liaison","to":"mgr1","type":"steer","ref":"<node_id>","body":"…"}
```

| 事实 | 锚点 |
|---|---|
| CLI 契约 `session-send <from> <to> <type> <ref> <body>`；from/to 解析 4 位码/sessionId 前缀/全称 | `〔loc:...session-send:4→usage〕`、`〔loc:...session-send:22→resolve(key,fleet)〕` |
| 信封单行拼接 `'DSHMSG]' + json.dumps(envelope)` | `〔loc:...session-send:47〕` |
| 注入路径：loopback `POST /api/session.prompt`（mode queue） | `〔loc:...session-send:56-60〕` |
| 类型集 `ping|pong|done|ask|steer|ack|nack` | `〔loc:...session-send:6〕` |
| env：`DSH_PORT`(3080) / `MAESTRO_FLEET` | `〔loc:...session-send:13〕` |

用途：**唤醒与低时延通知**（收方回合首行可机器解析，不承载正文）。

### 2.3 拉取模式：dais 邮箱（正文与大负载）

语义照搬车道A 实测（报告 §6 / KG 01§2）：`check-messages <自身句柄> --timeout-ms N` 有界快照 + 自管 sleep；读即消费；跨进程留 sleep 间隙避免总线锁互饿。封装即 `〔loc:examples/realtime-provider-poc/rt_dsh_lane.py:146→check_messages〕`（python 侧）；dsh agent 侧持 `dais-orchestration` skill 直接调 CLI。

### 2.4 路由器三 RPC（挂孵化池插件）

`〔new:~/.dsh/plugins/a2a-profile-server/http-server.js→handleRpc 增三分支〕`（与现行六 RPC 同形制）：

```javascript
agents/registry  params={} → {agents:[{code, sessionId, mailbox, role, project,
                        state:"spawn|arm|ready|serving|retired", lastHeartbeat}]}
agents/send      params={to, from, ref, type="notify|steer|ping", body}
                  → 校验 scope（from 与 to 同 project，或显式授权记录）
                  → 轻载/唤醒：session-send DSHMSG 注入；重载：dais send-message 投 to 邮箱
                  → result:{delivered:"push|mailbox", ackRef}
agents/inbox     params={mailbox} → {unread:[{from, ref, type, body, seq}]}
                  // 有界快照语义（读即消费仅对 dais 邮箱；本 RPC 只读快照不消费，调试与 head 查询用）
```

### 2.5 隔离边界（受控打破）+ journal 审计

- **scope=project**：同 project 全互通；跨 project 需 registry 显式授权记录（`grants:[{from,to,ts}]`）；
- **router 只注入固定信封格式的消息通知**，不注入任意指令——目标 agent 按 doctrine 解析（与 `ORCA-CB]`/`DSHMSG]` 前缀纪律同源：首行机器可解析、其余归 agent 自主）；
- **journal**：全量路由消息进 `router-journal.jsonl`（复用 `〔loc:~/.dsh/plugins/a2a-profile-server/task-store.js〕` JSONL append-only 形制），行 `{ts, op:"route", from, to, type, ref, delivered}`。

### 2.6 与 A/B 车道的关系（全链贯通）

manager 收稳定指令 → 编排 skill 选车道派发 → 车道 done 回信投 manager 邮箱 → 汇总 → 回 liaison → 回 head；**每跳带 ref，凭证全链不丢**（V7 逐跳断言此点）。

## 3. W5.3 主对接 agent（liaison）落位

| 步 | 动作 | 锚点/验证 |
|---|---|---|
| 1 | 孵化：`incubate {targets:["dsh-liaison"], role:"liaison", project:"voice-head", mailbox:"agent_liaison"}`，场景=编排对接，过三门 | N3 + §1.3 |
| 2 | **句柄移交**：head 的 `DshBackend.orchestrator_handle`（`〔loc:...rt_dsh_backend.py:70〕`）指向 `agent_liaison`——V5/V6 替身协议原样移交，**head 侧零改动**（F4/F10 不变） | 替身对照表 §3.1 |
| 3 | 职责链固化（liaison 模板）：语义指令 → 稳定指令（agent 类型 + 自包含 goal + deps 结构）→ 按域分发 manager（F6）→ 汇总终稿 → 回信 head（F10） | §1.2 |
| 4 | 下游通路走 W5.2 通信层：DSHMSG 推唤醒 + 邮箱投稳定指令正文 | §2 |

### 3.1 替身 ↔ 真身协议对照（零改动清单）

| 协议项 | 替身实现（已验证） | 真身（W5.3） |
|---|---|---|
| 受理回执 | `orchestrator_player` 即时 reply `{status:accepted, run_id, ref, credentials}` | liaison 模板同款回执 |
| 终稿 | reply body = `FINAL_PREFIX + done body`（凭证内嵌） | 同 |
| 信封 | `[ref:<ref>]` body 前缀 | 同（模板固化） |
| 三过滤 | `_phase2`（`〔loc:...rt_dsh_backend.py:157-160〕`） | **head 侧代码不变** |

## 4. W5.4 manager 群 + live V7

- **每项目域一个 dsh-manager**（调研/代码/文档…按 project profile 孵化：`incubate {targets:["dsh-manager"], role:"manager", project:"<域>"}`）；
- **处理循环**（manager 模板固化）：收稳定指令 → 拆子任务 + `--dep` 依赖 → 选车道（§1.2 表）→ 派发（F7）→ 等 `worker_done` + `read-worker --after` 游标尾读（F8）→ 域汇总 done 回 liaison（F9）；
- **异常路径**：gate 阻塞→`resolve-gate`；wait-blocked→`scan-wait-blocked`（可自愈 `answer`）；超时→上抛 supervisor（role 预留）；
- **live V7 场景**（扩 `validate_live.py` 序列）：

```
1. incubate liaison + manager 各一（真孵化，fleet 登记）
2. head dispatch_intent(语义指令) → F4 邮箱 → liaison 收（快照轮询）
3. F6 router agents/send → manager 收稳定指令（DSHMSG 唤醒 + 邮箱正文）
4. F7 manager 经 dais 车道 create-task/start-worker → F8 worker_done
5. F9/F10 终稿逐跳回传（[ref:] 链 + 凭证逐字）
6. 断言：全链每跳 ref 不丢；head 终稿凭证与 dispatch 回执一致；两阶段时序正确
```

## 5. 实施与验证序列

| 步 | 交付 | 验证 | 完成判定 |
|---|---|---|---|
| W5.1a | Projector 第 17 维 + ROLE_TEMPLATES + `project(role=)` | `tests/test_rt_projector.py` 扩 role 用例 | liaison/manager 模板过三门（术语零暴露） |
| W5.1b | incubate 扩参 + dsh-liaison/dsh-manager 目标 | `tests/test_incubators_real.py` 扩（mock session-spawn） | receipts 含 sessionId/mailbox/fleet 扩展项 |
| W5.1c | fleet 元数据 + reattach + 生命周期 | `tests/test_rt_fleet_registry.py`（new） | 孤儿检测单测绿；spawn→retire 状态机全迁移 |
| W5.2a | router 三 RPC + scope + journal | `tests/test_rt_router.py`（new，mock session-send/dais） | 三 RPC 契约测试绿；跨 project 拒绝 |
| W5.2b | 推/拉双投递 conformance | 扩 `tests/test_rt_conformance.py` | DSHMSG 推 vs 邮箱拉同结果 |
| W5.3 | liaison 落位（替身移交） | live：head 对真 liaison 一次 F4→F10 | head 侧代码 diff = 0 |
| W5.4 | manager 群 + live V7 | `tests/test_live_v7.py`（new） | V7 全链 PASS（§4 场景 6 断言） |
