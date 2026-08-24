# LB-002 · dais 编排面强化补完（已定稿 2026-08-24 — 握手闭环）

> 触发：用户指令「等 dais agent 跟你握手，准备强化补完 dais 编排面」
> 握手闭环：dais-iter@warpdotdev（session-c8e0317a，cwd ~/warpdotdev/dais）ack→report→done 三段完成；编排者 fleet 入编（code <seat>, mailbox voice-head）。
> 接收面已验：HTTP intake `:46855` LISTEN · `inbox.log` 活跃 · `orch.signature` 指向本会话。
> 人格指派边界（用户裁定 2026-08-24）：**仅 dsh** —— omp/claude 孵化器已门禁（`A2A_CLI_INCUBATION=1` 维护恢复口），向导菜单收缩，selftest 42/42。

## 1. 现状盘点（源码锚点实证）

**DaisLane 面**（`rt_dsh_lane.py`，13 方法全建）：create_run / create_task(**--dep**) / start_worker / send_intent / send_reply / check_messages / await_done / check_status / read_worker / fail_dispatch / scan_wait_blocked / resolve_gate。

**语音链实际只用了 6 个**（`rt_dsh_backend.py` b 分支 grep 实证）：create_run、create_task（无 orchestrator 兜底路径）、send_intent、await_done、check_status、fail_dispatch。

| # | 缺口 | 实证 | 阻塞/依赖 |
|---|---|---|---|
| G1 | **真身缺口**：b 车道 orchestrator_handle 一直由测试替身扮演（conformance `orchestrator_player` session_vhconf）；生产链无真 manager/liaison | V5/V6/V7 live 全是替身；池孵化 role=liaison/manager 的 dsh agent 从未上链 | 本次握手即入口 |
| G2 | **DAG 面**：create_task(--dep) + start_worker 零生产调用 —— manager 拆分/扇出/依赖编排没有生产路径 | grep：backend 无此二调用 | **D-04**（start-worker pane 自动绑定未完成）先清 |
| G3 | **异常面**：scan_wait_blocked / resolve_gate 已封装未接线 —— gate 阻塞/等待卡死时 head 只能干等预算耗尽 | cancel 只有 fail_dispatch（ctx_ 限定） | 无 |
| G4 | **profile 穿透（dsh-only）**：bindProfile（在飞穿衣，`~/.dsh/plugins/a2a-profile-server/incubators/real.js:150`）未接语音链 —— **仅 dais/dsh 会话**；omp/claude CLI agent 一律原生人格（用户裁定 2026-08-24，daemon 侧门禁已生效） | _fanout/_phase2 无 injectionPrompt 信封；omp/claude 孵化器已禁用（`A2A_CLI_INCUBATION=1` 维护恢复口） | 无 |
| G5 | **监控面**：read_worker 仅 rt_gateway TailReader 引用；orch.progress 事件流无 live 证据（E2E 播报依赖） | — | E2E 前置 |
| G6 | **台账残留**：D-03（CLI 软错误契约漂移）/ D-05（run 注册表只增不清）/ D-14（live 瞬态容忍语义留在测试层）/ D-15（回环代理坑无共享帮手） | docs/kg/08 | 各自独立 |

## 2. 票面草案（握手后按真身角色裁剪）

- **LB-002-A 真身接线**：池孵化 role=liaison（+manager 群）→ fleet mailbox 注册 → 替换替身上链；验收 = 生产 dispatch → 真回执 → 真终稿（凭证逐字）。
- **LB-002-B DAG 生产化**（先清 D-04）：manager 拆分 → create-task --dep → start_worker → worker_done 块匹配；验收 = 一意图拆 ≥2 子任务带依赖，终稿聚合回链。
- **LB-002-C 异常面接线**（消费侧已交付 8e6c279）：query_status 侧 scan_wait_blocked 透出"卡在哪"（已上链）；resolve() 头工具（已上链）；验收 = 人为 gate 阻塞 → head 可见可解。
- **LB-002-D profile 穿透（dsh-only）**：dispatch 携 profile → bindProfile 给**在飞 dsh 会话**穿衣（信封 `ORCA-CB] PROFILE-INJECT]`）；验收 = 终稿带 profile 人格痕迹。边界：omp/claude 不参与（原生人格，孵化器已门禁）；orca 工作树 spawn 保持裸 prompt。
- **LB-002-E 事件流 live 证据**：read_worker/TailReader → orch.progress → WS event；E2E 播报链前置。

依赖序：A → (D-04 清障) → B → C；D/E 可并行；G6 随手清或挂账。

## 3. dais 侧修法裁决（dais-iter@warpdotdev report/done，源码实证 2026-08-24）

| 项 | 级 | 实锤（dais 仓锚点） | 修法 | 状态 |
|---|---|---|---|---|
| D-04 | **P0** | `StartWorker`(agent_sdk/orchestration.rs:139) 仅 create_dispatch 零绑定；`assignee_handle/pane_key` 两列(db.rs:190) **全仓零写入点**；assign 只注册内存 ViewRegistry 不落库 | start-worker `--session` 按 session 绑 pane（复用 ViewRegistry 自注册键）+ assign 双写 assignee 列（status 归状态机管） | ✅ **已落地 be8d9cf3**（2026-08-25；store 单测 18/18） |
| D-03 | P1 | 转发道 ok:false+executed:true 回 Ok→stdout 打 JSON error 且 exit0(runtime_rpc.rs:668)；直连道 Err→stderr+exit1(lib.rs:2363) | `classify_forwarded_response` 统一 Err→stderr+exit1；契约落 docs §2.8 | ✅ **已落地 be8d9cf3**（classify 单测 3/3） |
| worktree 注入面 | P1.5 | worktrees.rs:58 签名仅 (project,name,cx) 双参 | `--agent/--prompt` 一步 spawn（GUI 建 worktree+tab → CLI 解析 session → 注入 agent 行 → 6s → 粘贴 prompt） | ✅ **已落地 be8d9cf3** |
| D-05 | P2 | store 20 个 pub fn 零 run 清理方法 | `gc-runs --days --dry-run` 单事务回收 | ✅ **已落地 be8d9cf3** |
| new-terminal 依赖 GUI | — | 模块文档自述 headless 明确报错 = 架构使然**非缺陷** | 归消费侧看护（D-02 实例锁已覆盖） | 消费侧已covered |
| D-14 / D-15 | — | — | 归消费侧 | 消费侧账上 |
| **D-17**（本轮 live 新捕） | **P0** | LB-002-A V5 跑：head 平铺 1s 消费轮询窗内，真身 send-message 挂 60s/150s SIGTERM **零错误零服务端超时**，create-run 探针 exit=124；轮询窗一关 plane 立即复苏——并发「高频消费轮询+回信写」下编排面挂死且无错误可传导（与 D-03 相关但独立：本例根本没有错误） | 消费侧已修：`await_done` 轮询 ×1.5 退避至 `poll_max_s=8s`（be94ba8，写事务率 ~6× 降）；dais 侧移交：锁公平性/读路径并发实证 + 服务端 per-call 超时 | 消费侧✅（复跑 2/2 PASS）/ dais 侧已通报 |

**优先序：D-04 → D-03 → worktree → D-05；D-04/D-03 可并行。** 消费侧已先行交付：DaisLane 13→29 方法（worktree/project/terminal/scheduling 四面，live 校准 gate_ / project_list / worktree-list）+ G3 接线（POC 8e6c279，31+8+40+73 测试全绿）。

**dais 侧落地注意**：`be8d9cf3` 在仓未部署——resident 二进制（`~/.local/bin/dais`）实测无 `--session`/`gc-runs`（2026-08-25 探测）。LB-002-B 消费侧接线可先行，live 验证需新二进制部署后开窗。

## 4. LB-002-A 执行实录（已收口 ✅ 2026-08-25）

- **真身孵化**：live 脚本管线（投影+协议附录+三门+插件 RPC）→ code `<seat>`，mailbox `agent_liaison`，fleet 五键 PASS；head 侧零 diff（纯配置换身）。
- **V5 首跑 11/12**：唯一 FAIL = phase-2 终稿未到 → 根因 D-17（上表）；真身行为全对：首动作邮箱排空 ✅、真调研（5 次 web_search）✅、终稿落盘 ✅、回信被饿死 ❌。V6（打断/pending 在途/cancel/无迟到终稿）全 PASS。
- **D-17 修复复跑**：be94ba8 退避版 **2/2 PASS（407s，V5+V6+零 diff 检查）**——真身终稿到达、凭证逐字回显、播报断言全过。LB-002-A 消费侧验收**收口**。
- **G4 侧产物**：`pool/spawn binding-mode` live 验证通过——vh-liaison@v1 信封注入备援会话 e0e1（`injected:true`，版本钉死不重存）；信封唤醒后 doctrine 行为完整（排空→识别非己→ask 升级，零编造；裁决①维持待命已确认）。G4 剩余：dispatch 携 profile 的正式穿衣路径 + 终稿人格痕迹断言。
