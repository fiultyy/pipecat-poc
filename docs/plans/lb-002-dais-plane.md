# LB-002 · dais 编排面强化补完（DRAFT — 握手后定稿）

> 触发：用户指令 2026-08-24「等 dais agent 跟你握手，准备强化补完 dais 编排面」
> 状态：**草稿** —— 等 dais agent 经 maestro-bridge 握手（`cb-send ack` → `~/.dsh/maestro/bridge/inbox.log` → 泵入编排会话）后，按其身份/角色定稿票面。
> 接收面已验（2026-08-24 22:5x）：HTTP intake `:46855` LISTEN（host-callback-bridge 持有）· `inbox.log` 活跃 · `orch.signature` 在册 · 对仗票：LB-001（车道B主干，已收口）。

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
- **LB-002-C 异常面接线**：query_status 侧 scan_wait_blocked 透出"卡在哪"；cancel/resolve 语义补 resolve_gate；验收 = 人为 gate 阻塞 → head 可见可解。
- **LB-002-D profile 穿透（dsh-only）**：dispatch 携 profile → bindProfile 给**在飞 dsh 会话**穿衣（信封 `ORCA-CB] PROFILE-INJECT]`）；验收 = 终稿带 profile 人格痕迹。边界：omp/claude 不参与（原生人格，孵化器已门禁）；orca 工作树 spawn 保持裸 prompt。
- **LB-002-E 事件流 live 证据**：read_worker/TailReader → orch.progress → WS event；E2E 播报链前置。

依赖序：A → (D-04 清障) → B → C；D/E 可并行；G6 随手清或挂账。

## 3. 握手后定稿动作

1. 读握手 body：agent 身份（from 签名）/ 角色（liaison/manager/worker?）/ mailbox / 是否携 profile；
2. 对号入座：真身 = G1 解；其自报能力面决定 B/C 范围；
3. 本文件转正（DRAFT 摘除）+ ledger 落 checkpoint + 开 maestro 票（如需跨域）。
