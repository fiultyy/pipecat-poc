# N5 · 跨线契约（schema / 凭据 / 测试矩阵）

> 索引：[00-INDEX.md](00-INDEX.md) · 本文件是 N1–N4、N6 共享定义的单一事实源 · v2 对齐修正架构。

## 1. 模块归属总表（v2：建成项 loc 归档 + W5 落点）

| 模块 | 落点 | 归属 | 状态 |
|---|---|---|---|
| DshBackend / DshDispatch | `examples/realtime-provider-poc/rt_dsh_backend.py:45/:34` | N1 | ✅ 建成 |
| DaisLane | `examples/realtime-provider-poc/rt_dsh_lane.py:32` | N1 | ✅ 建成 |
| dsh_head_tools / DSH_TOOLS_DOCTRINE | `examples/realtime-provider-poc/rt_head_tools.py:74/:24` | N1 | ✅ 建成 |
| A2aClient | `examples/realtime-provider-poc/rt_a2a_client.py:34` | N1/N2 | ✅ 建成 |
| Projector / gates | `examples/realtime-provider-poc/rt_projector.py:63` / `rt_projection_gates.py:44` | N3 | ✅ 建成（W5.1 扩 role） |
| live V5/V6（替身=liaison 协议验证） | `examples/realtime-provider-poc/live_v5_v6_dsh.py:126→orchestrator_player` | N1/N6 | ✅ 建成 |
| a2a-profile-server 插件（六 RPC + ProfileStore + 三孵化器 + 执行桥） | `~/.dsh/plugins/a2a-profile-server/`（`http-server.js:66` 等） | N2 | ✅ 主体（向导 skill 待建） |
| EventBus（emit/subscribe，N1 埋点已用） | `examples/realtime-provider-poc/rt_event_bus.py:20→EventBus（subscribe:26/emit:42）` | N1/N4 | ✅ 建成（N4 网关订阅侧待 M4） |
| VoiceGateway / WsSession / TailReader | `〔new:examples/realtime-provider-poc/rt_gateway.py〕` | N4 | ⬜ M4 |
| OrcaLane（车道B 入口） | `〔new:examples/realtime-provider-poc/rt_orca_lane.py→OrcaLane〕` | N7 | ⬜ 方法级已定（N7，命令面已实测探明） |
| ROLE_TEMPLATES / project(role=) | `〔new:...rt_projector.py→ROLE_TEMPLATES〕` | N3/N6 | ⬜ W5.1a |
| registry（fleet 扩展/reattach/生命周期） | `〔new:~/.dsh/plugins/a2a-profile-server/registry.js〕` | N6 | ⬜ W5.1c |
| router 三 RPC + journal | `〔new:~/.dsh/plugins/a2a-profile-server/http-server.js→agents/* 分支〕` | N6 | ⬜ W5.2a |
| 向导 skill 壳 | `〔new:~/.agents/skills/incubation-wizard/SKILL.md〕` | N2/N3 | ⬜ W3.4 |

## 2. 两阶段应答契约（N1 产出、N4 转发、N6 liaison 沿用——V5/V6 live 验证）

```jsonc
// 阶段1 受理（head 工具返回值，head 据此口语播报）
{"status":"accepted","run_id":"run_x","ref":"vh-7f3a","credentials":["【凭证VH-7f3a】"],"note":"已受理"}
// 阶段2 终稿（FINAL_PREFIX + done body ≤300字 + 凭证；head 逐字回显）
"\"Agent Final Message\":\n\n<done body>"
```

- 凭证 API：`make_credential/extract_credentials`（`〔loc:examples/realtime-provider-poc/rt_orchestrator.py:61/:66〕`）；
- `FINAL_PREFIX` 常量：`〔loc:...rt_orchestrator.py:48〕`；
- head 逐字回显规则：`DSH_TOOLS_DOCTRINE§After Tool Calls`（`〔loc:...rt_head_tools.py:24〕`）；
- 防自匹配三过滤（阶段2 权威）：`[ref:]` 命中 ∧ `seq > intent_seq` ∧ `from == orchestrator_handle`（`〔loc:...rt_dsh_backend.py:157-160〕`）。

## 3. 事件 schema（EventBus ↔ N4 event 帧）

事件 = `{t, ts, **payload}`：

| t | 生产者（方法） | 消费者 |
|---|---|---|
| orch.dispatch | `DshBackend.dispatch` | N4 面板/head 播报 |
| orch.ack | 路径① dais 回执前置 / 路径② task→working | N4 面板 |
| orch.progress | `TailReader.follow` / `_phase2` 超时分支 | N4 面板 |
| orch.gate | `DshBackend`（dais create-gate 轮询侧） | N4 弹层+语音问询 |
| orch.done | `DshBackend._phase2` | N4 面板/head 终稿播报 |
| orch.metrics | 网关 TTFB/E2E 埋点 | N4 报告 |

## 4. dsh 内部通信信封（W5.2，与既有契约同构）

```
推送（session-send，已存在底座 〔loc:~/.dsh/maestro/bin/session-send:10〕）：
  收方回合首行 = DSHMSG]{"from":"<code>","to":"<code>","type":"ping|pong|done|ask|steer|ack|nack","ref":"<node_id>","body":"…"}
  注入路径：loopback POST /api/session.prompt（mode queue）；from/to 解析 4 位码/sessionId 前缀/全称（:22 resolve）
拉取（dais 邮箱，语义 N1§2）：
  check-messages <自身句柄> --timeout-ms N（有界快照 + 自管 sleep；读即消费）
router（W5.2 新增三 RPC，方法级 N6§2.4）：
  agents/registry | agents/send | agents/inbox；scope=project 互通；全量 router-journal.jsonl 审计
```

## 5. 凭据地图（env 单一来源；frp 条目已随 Q1 关闭删除）

| 键 | 用途 | 现状 |
|---|---|---|
| `ZHIPU_CODING_PLAN_API_KEY` | GLM（head 文本模式/formatter/Projector/编排 agent） | `〔loc:~/.dsh/zhipu.env〕` 已有 |
| `GLM_BASE_URL` | GLM endpoint（默认 `https://open.bigmodel.cn/api/coding/paas/v4`） | 已有 |
| `DASHSCOPE_API_KEY` | Qwen realtime head（live 语音模态） | **缺**（Q2 定案：GLM 文本模式先行） |
| `VOICE_GATEWAY_TOKEN` | N4 ws 鉴权 | 新增，M4 部署时生成 |
| `A2A_PROFILE_TOKEN` | 孵化池插件 Bearer | 已有（live 对拍使用） |
| `MAESTRO_ORCH_SIGNATURE` | dispatch-ticket 签名源 | `〔loc:~/.dsh/maestro/bin/dispatch-ticket:1→signature()〕` |
| `DSH_PORT` / `MAESTRO_FLEET` | session-send 注入端口 / fleet 路径 | 默认 3080 / `~/.dsh/maestro/fleet.json` |

加载约定：PoC 侧统一 `load_dotenv()` 后再 `load_dotenv("~/.dsh/zhipu.env", override=False)`。

## 6. 测试矩阵（实际基线 13 文件 94 用例全绿）

| 文件 | 用例 | 覆盖 |
|---|---|---|
| `tests/test_rt_dsh_lane.py` | 14 | DaisLane 全方法 + live 两行式解析 + 防自匹配 + 真 dais 冒烟 |
| `tests/test_rt_dsh_backend.py` | 5 | 两阶段/终稿注入/取消/双路径分流 |
| `tests/test_rt_head_tools.py` | 7 | 四件套 schema/回执流转/doctrine 覆盖 |
| `tests/test_rt_conformance.py` | 2 | 车道A 内双到达路径对拍（离线状态机 + live A/B） |
| `tests/test_live_v5_v6.py` | 1 | live 全链（真 GLM head + 真 dais；V5 六验/V6 五验） |
| `tests/test_rt_orchestrator.py` / `test_rt_reconnect.py` / `test_rt_transcript.py` | 34 | 编排层/重连/写本（PoC 基线） |
| `tests/test_m0_probe.py` | — | 双 ADE 探针（dais 177ms/orca 245ms） |
| `tests/test_rt_projector.py` + `test_projection_live.py` | 23 | 投影 + 三门 + 真 GLM 冒烟 |
| `tests/test_rt_a2a_client.py` + `test_incubators_real.py` | 8 | 孵化池跨语言一致 + 三真实孵化器 |
| `~/.dsh/plugins/a2a-profile-server/selftest.mjs` | 14 | HTTP 面 + ProfileStore + 幂等 |

W5 增列（N6§5）：`tests/test_rt_fleet_registry.py`（fleet/reattach/生命周期）· `tests/test_rt_router.py`（三 RPC + scope + journal）· `tests/test_live_v7.py`（head→liaison→manager→车道全链）；conformance 扩推/拉双投递对拍。

live 序列：V5/V6 ✅ → **V7**（W5.4）→ V8（局域网网关时延基线，M4/W4.3）。

## 7. 里程碑 ↔ KG 节点映射（v2 重排）

| 里程碑 | 节点 | 状态/交付判定 |
|---|---|---|
| M0 探针 | N1§5 W1.1 | ✅ 证据 `docs/kg/evidence/m0-probe.md` |
| M1 WS3 投影 | N3§6 | ✅ 三门 22/22 + 真 GLM 冒烟 |
| M2 孵化池 | N2§7 | ✅ 主体（selftest 14/14 + 孵化 3/3）；向导 skill ⬜ |
| M3 车道A | N1§5 | ✅ live V5/V6 + 双路径 conformance（证据 `docs/kg/evidence/m3-live-v5v6.md`） |
| W5.1–W5.4 | N6§5 | ⬜ 单测 + live V7 + 双模式 conformance |
| M3+ 车道B | N7§3 | ⬜ OrcaLane + A/B 对拍 + 分派策略（命令面已探明） |
| M4 本地网关 | N4§5 | ⬜ 帧协议 + 事件汇总 + 局域网基线 |
| M5 收口 | 全部 | ⬜ dogfood：向导自举"监督员"→ 对接 agent 编排真实 A/B fan-out |

权威账本：`docs/kg/evidence/ledger-carryover-round6.md`（原长时任务账本服务端损坏，以此文件逐轮更新）。
