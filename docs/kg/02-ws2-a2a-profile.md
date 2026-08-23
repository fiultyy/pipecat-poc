# N2 · A2A 孵化池插件：六 RPC + ProfileStore + 三孵化器 + 执行桥（方法级，主体建成）

> 上游：`docs/plans/voice-orchestration-head-plan.md§1.1`（v2）· 索引：[00-INDEX.md](00-INDEX.md)
> 已定决策：**纯内部契约**（最小子集，版本化 agent-card；Q3 定案）。
> 定位（v2）：dsh 宿主三挂载之一——持久 profile list + incubate 供给线 + 执行桥（车道A 到达路径②）；W5.1 孵化扩展与 W5.2 路由器三 RPC 也落在本插件（方法级见 N6）。

## 0. 对接总图（建成态）

```
DshBackend 路径② ──HTTP──▶ a2a-profile-server（cordis 插件，~/.dsh/plugins/a2a-profile-server/）
                              ├─ HTTP面: agent-card + 六 RPC（message/send · tasks/get · tasks/cancel
                              │          · incubate · profiles/list · profiles/get）〔loc:http-server.js:66〕
                              ├─ TaskStore（journal JSONL 重放）      〔loc:task-store.js〕
                              ├─ ProfileStore（版本化持久 list）      〔loc:profile-store.js:30〕
                              ├─ Incubators: dsh/omp/claude（真实）   〔loc:incubators/real.js:59/:90/:123〕
                              ├─ Executor 执行桥 → 真 dais 总线       〔loc:executors/dais.js:80〕
                              └─ W5 扩展位: router 三 RPC + registry/reattach + 生命周期（N6§1/§2）
```

## 1. HTTP 面（六 RPC，实测锚点）

`〔loc:~/.dsh/plugins/a2a-profile-server/http-server.js:66→createHttpServer({tasks, profiles, token, executor, gatesFn, incubate})〕`

| RPC | 锚点 | 契约 |
|---|---|---|
| `message/send` | :72 | `{message:{parts:[{text:rawIntent}]}, context:{source, ref}}` → `{task:{id:"t_<hex>", state}}`；executor 桥接（§4） |
| `tasks/get` | :86 | `{taskId}` → `{task:{state, artifacts?, error?}}` |
| `tasks/cancel` | :92 | `{taskId}` → `{task:{state:"canceled"}}` |
| `incubate` | :102 | 见 §3（gatesFn 前置三门校验，fail → `-32000 gate violations`） |
| `profiles/list` | :137 | 持久 list 查询 |
| `profiles/get` | :141 | `{name}` → profile + meta + history 尾 |
| agent-card | :156 | `GET /.well-known/agent-card.json`（版本 `internal-1`） |

task 状态机：`submitted → working → completed(artifacts=[done-body]) | failed | canceled`。

## 2. ProfileStore（持久 list，防漂移）

`〔loc:~/.dsh/plugins/a2a-profile-server/profile-store.js:30→createProfileStore(root)〕`

目录契约（重投影才 v+1，原子写 tmp+rename）：

```
~/.dsh/profiles/incubated/<name>/
├── AGENTS.md            # 投影产物本体（= system prompt，三孵化共享同一份）
├── profile.json         # {scenario, vector19(+agent_role W5.1), generalization, template}
├── meta.json            # {targets, lineage, created, version}
├── history.jsonl        # {ts, op, task_id?, target?, latency_ms?, outcome}
└── versions/<v>/…       # 旧版本归档
```

API：`save(projection)→{name,version}` / `get(name)` / `list(filter?)` / `recordRun(name,entry)` / `revalidate(name)→{drift}`（重孵化前重跑三门对账）。

## 3. incubate RPC（现行 + W5.1 扩展）

现行（`〔loc:...http-server.js:102-135〕`）：

```javascript
// params: { name, projection:{agents_md, profile_json, description}, targets:["dsh","omp","claude"] }
// 流程: gatesFn(agents_md) 三门 → profiles.save 版本化 → 逐 target incubate() → recordRun
// result: { profile:{name,version}, receipts:[{target, handle|path|sessionId, ...}|{target, error}] }
```

**W5.1 扩展**（方法级见 N6§1.3）：params 增 `role`（liaison/manager/worker/supervisor）· `project` · `mailbox`（默认 `agent_<name>`）；targets 新合法值 `dsh-liaison` / `dsh-manager`；receipts 增 fleet 扩展登记项。

## 4. 执行桥（车道A 到达路径②）

`〔loc:~/.dsh/plugins/a2a-profile-server/executors/dais.js:80→createDaisExecutor(tasks, opts)〕`

```
message/send(task) → dais create-run → send-message([ref:] 信封 → 对端句柄)
  → 有界 --timeout-ms 快照轮询对端回信 → tasks→completed
  → artifact.content = 去 ref 的 done body（FINAL_PREFIX 由 head 侧统一拼接，单一事实源）
```

`parseMailbox` 与 python `DaisLane._parse_message_rows` 镜像（live 两行式 `--- seq N from X [type] subject ---` + body / kv / json 行型；"no unread" 哨兵）。与路径①（CLI 直达）conformance 同结果（`tests/test_rt_conformance.py`）。

## 5. 三个孵化器（真实，冒烟 3/3）

`〔loc:~/.dsh/plugins/a2a-profile-server/incubators/real.js〕`

### 5.1 dsh（:59 incubateDsh）

```
ctx { name, version, agentsMd, purpose, preset='maestro' }
1) session-spawn <preset> vh-<name> <purpose> → stdout 末 4 位 code
2) resolveSessionId(code)：fleet.json 反查（key==code / sessionId 前缀匹配）
3) 首回合注入：rpc session.prompt {sessionId, mode:'queue',
     content:[{text:"ORCA-CB] PROFILE-INJECT] <name>@v<version>\n<agentsMd>"}]}
return {target:'dsh', name, version, code, sessionId, preset, marker}
```

W5.1 扩展（N6§1.4）：ctx 增 `{role, project, mailbox}`；注入体 = role doctrine 段 + agentsMd；fleet 登记项扩展；返回 `target:'dsh-'+role`。

### 5.2 omp（:90 incubateOmp）

`ctx {name, version, agentsMd, projectRoot, model?, temperature?}`：`~/.config/opencode/oh-my-opencode.json` 备份写 agents 路由条目 + 项目根 AGENTS.md（尾注 `x-profile-ref` 反向索引）。

### 5.3 claude（:123 incubateClaude）

写 `~/.claude/agents/<name>.md`：YAML frontmatter（name/description/model/color）+ 正文 = agentsMd；description 含触发例。

幂等性：入口先查反向索引（同名同版本 → 返回现 handle/路径，不重复 spawn）；版本升级 → 重写 + history 记 `reincubate`。

## 6. A2aClient（消费端，归 N1 路径②）

`〔loc:examples/realtime-provider-poc/rt_a2a_client.py:34→A2aClient〕`：`send:57 / get:68 / cancel:76 / await_done:83 / incubate:100 / agent_card:115`；退避复用 `ReconnectPolicy`。

## 7. 实施与验证序列

| 步 | 交付 | 状态/验证 |
|---|---|---|
| W2.1 骨架+六 RPC | http-server + agent-card + task 状态机 | ✅ node selftest 14/14 |
| W2.2 ProfileStore | 版本化 + 血缘 + revalidate | ✅ selftest（save/get/归档/对账） |
| W2.3 三孵化器 | dsh/omp/claude 真实孵化 | ✅ 冒烟 3/3（`tests/test_incubators_real.py`） |
| W2.4 执行桥 | task→dais 总线往返 | ✅ `tests/test_rt_conformance.py` live 对拍 |
| W2.5 跨语言一致 | A2aClient ↔ 插件 | ✅ `tests/test_rt_a2a_client.py` 5/5 |
| W3.4 向导 skill 壳 | 引导 dsh agent：场景(+role)选型→投影→三门→选目标→incubate | ⬜ 待建（`〔new:~/.agents/skills/incubation-wizard/SKILL.md〕`） |
| W5.1b/c 扩展 | incubate 扩参 + registry/reattach + 生命周期 | ⬜ N6§5 序列 |
| W5.2 路由器 | agents/registry · agents/send · agents/inbox + journal | ⬜ N6§2.4/§5 |
