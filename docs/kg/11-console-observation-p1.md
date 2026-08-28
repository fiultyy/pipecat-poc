# 11 · Console 观测面（P1 topic 数据平面）

> 依据：用户裁决（2026-08-26）——整合前端方向：ONE SPA console（观测 topic + 控制锚点），
> rt_voice_app 保留为常驻超轻语音壳；console 不落在 maestro 侧（边界错误）。
> gateway 是唯一汇聚点。分三期：**P1 topic 数据平面（本篇）→ P1.5 cmd 上行路由 → P2 SPA**。
> 排期裁决：console P1 先行，LB-002-B live 随后走新 topic 面验证（一次 live 双验证）。

## 0. 范围与非目标

| 项 | 内容 |
|---|---|
| P1 交付 | ①`topic.*` 事件源四类接入 EventBus；②WS 协议增 `observe` 会话模式与 `topics` 订阅参数；③`fleet.cleanup` 席位清理控制帧（§3.1）；④单测覆盖 |
| 非目标 | cmd 通用上行路由（P1.5；P1 仅 `fleet.cleanup` 单点控制帧）；SPA 页面（P2）；鉴权分级（P1.5 obs/ctl token）；公网暴露（永不——控制面只走 LAN/loopback，§4 边界） |

## 1. topic.* 事件全集（P1）

| kind | 源 | 触发 | payload |
|---|---|---|---|
| `head.turn` | head pipeline `_TurnTap`（rt_gateway.build_realtime_head） | 回合生命周期帧 | `{phase, conv_id, detail?}` |
| `fleet.snapshot` | `~/.dsh/maestro/fleet.json` 文件尾读 | 字节变更 | `{fleet: <parsed json>}` |
| `bridge.msg` | `~/.dsh/maestro/bridge/inbox.log` 行尾读 | 新增行 | `{line, offset}` |
| `tickets.snapshot` | `~/.dsh/maestro/tickets.md` 全文快照 | 字节变更 | `{text, lines}`（render 覆写非追加） |

### head.turn phase 表（Tap 上报点，全部经帧流核实）

| phase | 帧源 | 语义 |
|---|---|---|
| `user_start` | `ProposedUserStartedSpeakingFrame`（服务端 VAD 提案，广播帧） | 用户回合开始提案 |
| `user_end` | `ProposedUserStoppedSpeakingFrame` | 用户回合结束提案 |
| `user_text` | `TranscriptionFrame`（input_audio_transcription.completed） | 用户回合权威文本 |
| `assistant_start` | `LLMFullResponseStartFrame` | 头开始产出 |
| `assistant_end` | `LLMFullResponseEndFrame` | 头产出完结 |
| `tool_call` | `FunctionCallInProgressFrame` | 头发起编排工具调用（dispatch/query/cancel/resolve） |
| `interrupted` | `StartInterruptionFrame` | 打断 |

> `text_only=True`（T6 生产形态）下无音频下行帧；`assistant_end` 携带聚合文本（detail，截断 200 字符）。

## 2. 文件尾读源（FileTailer）

- 通用行尾读协程：stat 轮询（默认 2s）比对 size；变小/换 inode → 重置 cursor（截断/轮转容错）；新行逐行 `emit(kind, {line, offset})`。
- `fleet.json` 专用：字节级变更 → 解析 JSON → `emit("fleet.snapshot", {fleet})` 全量快照（文件本身 ≤ 数 KB）。
- 文件不存在：静默等待（WARN 一次），出现后从 offset 0 起。
- 路径经 `VoiceGateway(topic_sources={...})` 注入（测试指 tmp 路径；默认 `~/.dsh/maestro/…`）。
- 每源一个 asyncio task，随 gateway 生命周期起停。

## 3. WS 协议增补（§1 帧协议兼容扩展）

- `session.start` 增可选 `topics: [kind…]`：校验 ∈ `SUBSCRIBABLE_KINDS`（= orch.* + topic.*）；
  缺省 voice 会话 = `ORCH_KINDS + ("head.turn",)`（语音壳顺手显示自己的 turn 轨迹）。
- `session.start` 增可选 `observe: true`：**不建 head pipeline**（console 连接形态——不占 Qwen 会话/凭据）；
  订阅 = `topics` 参数或缺省全部 `SUBSCRIBABLE_KINDS`。observe 会话拒绝媒体上行（error `observe_media`）。
- 订阅在 `session.started` 帧回执中回显 `topics`（客户端可确认生效面）。

### 3.1 控制帧 `fleet.cleanup`（席位清理）

会话内请求/回包控制帧（沿用 `body.get`→`body.item` 范式，走 WsSession 控制帧分发，不经 topic 总线）：

| 帧 | 方向 | 载荷 |
|---|---|---|
| `fleet.cleanup` | client→gateway | `{"t":"fleet.cleanup","ids":[四码…],"mode":"release"\|"end","req_id":…}` |
| `fleet.cleanup.result` | gateway→client | `{"t":"fleet.cleanup.result","req_id":…,"results":[{id,ok[,error][,note]}]}` |

- 逐 id 处理，条目间互不阻断：
  - `mode=end`：先经 dsh loopback 真死会话——`workspace.archiveSession{sessionId}`（dsh web API 面无 `session.end` 路由，archiveSession 是该面的会话退役操作；POST `http://127.0.0.1:3080/api/<method>`，wire `{type:client-request,rpcId,method,payload}`→`result.ok/result.value`；gateway 侧独立小 rpc helper，不复用 backend 实例），再从 fleet.json 摘条目。会话已不在（`error.code=session-not-found`）→容忍继续；loopback 调用失败不阻断摘条目（已死会话属正常清理场景）。
  - `mode=release`：只摘条目，不碰会话。
- id 不存在：该条 `{id, ok:false, error:"not_found"}`，其余照常处理。
- id 恰为当前 liaison 绑定（`~/.local/state/voice-gateway/liaison.json` 的 `code`）：该条附 `note:"active-liaison"`，不阻断——摘除后下个派发经 session-spawn 自动拉新（liaison 自管生命周期的设计内行为）。
- `ids` 空或 `mode` 非法：整体 `error bad_request`，不动 fleet.json。
- **fleet.json 只经原子写修改**（tempfile+os.replace，镜像 session-spawn 范式）；原子替换即触发 fleet.snapshot 重快照，订阅端席位表自动刷新。`MAESTRO_FLEET` env 覆写 fleet.json 路径（测试用）。

## 4. 边界与安全（继承 KG 04 §4）

- 控制面（本篇观测 + P1.5 控制）**只走 LAN/loopback**，永不进 frp 公网隧道（用户裁决固化）。
- topic 数据面只读尾读；maestro 侧文件写入的唯一例外是 `fleet.cleanup` 摘 fleet.json 条目（§3.1，只经原子写）；通用写入仍是 P1.5 cmd 路由经 lane 方法，另有审计。
- observe 会话计入同 token 并发上限（≤2），防止观测连接无限制占用。

## 5. 验证形式

- 单测：FileTailer（增量/截断/轮转/缺席等待）、fleet 快照、observe 握手+订阅回显、
  observe 拒媒体、voice 会话默认含 head.turn、_TurnTap phase→bus 映射（fake 帧流）。
- 单测（fleet.cleanup）：两 mode 语义、不存在 id、bad_request、原子写保留他条、
  result 形态、active-liaison note（mock rpc + tmp fleet.json）。
- live：LB-002-B live run 经 observe WS 客户端旁听（b-dag 事件 + bridge.msg + tickets.snapshot 同屏落盘）。
