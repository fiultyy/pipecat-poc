# N4 · WS4 本地 WS 网关（方法级；frp 已移出架构）

> 上游：`docs/plans/voice-orchestration-head-plan.md§6`（v2）· 索引：[00-INDEX.md](00-INDEX.md)
> 定位：**局域网**语音入口 + 编排信息全量汇总通道；宿主 pipecat 管线，不依赖未装 extras。
> 架构裁决：**frp/公网穿透移除**（Q1 关闭）——原 frpc/frps/TLS 隧道规划全部删除；如将来需远程访问，在架构外另行解决，网关协议不变。

## 0. 对接总图

```
局域网浏览器（mic→AudioWorklet→PCM16/16k）
    │ WebSocket ws://<host>:8765（同一连接三路复用）
    ▼
VoiceGateway（〔loc:examples/realtime-provider-poc/rt_gateway.py:488→VoiceGateway / :102→WsSession / :433→TailReader〕 单进程；✅ VO-011）
  ├─ WsSession（每连接一个；帧协议 §1）
  ├─ HeadPipeline（复用 T6 形制：providers 工厂 + head + 工具四件套）
  │    └─ DshBackend（N1）──EventBus──▶ WsSession.event_sink（orch.* 汇总下发）
  ├─ TailReader 协程（dais read-worker 增量 → orch.progress）
  └─ 静态页 /（mic 采集 + 播放 + 状态面板：run/ticket/worker 树）
```

## 1. WS 帧协议（三路复用）

单连接、JSON 文本帧 + 二进制帧混用：

| 路 | 方向 | 帧形态 | 类型 |
|---|---|---|---|
| control | C→S / S→C | 文本 JSON `{t:"...", ...}` | `auth{token}` `session.start{session_id?}` `session.end` `ping`/`pong` `error{code,msg}` |
| media | C→S | **二进制** = raw PCM16LE/16k/mono 块（推荐 20ms=640B） | — |
| media | S→C | **二进制** = TTS PCM 同格式 | — |
| event | S→C | 文本 JSON `{t:"...", ...}` | 见 §3 |

握手序：`auth` → `auth.ok{session_id}` → `session.start`（断线重连带旧 `session_id` 走重播种）→ 媒体/事件双工。

## 2. VoiceGateway 类设计

`〔loc:examples/realtime-provider-poc/rt_gateway.py:488→VoiceGateway〕`（✅ VO-011 建成，`tests/test_rt_gateway.py` 24 测）

```python
class VoiceGateway:
    """aiohttp web server + ws；每 WsSession 独占一条 pipecat 管线（会话级资源）。"""
    def __init__(self, port=8765, head_provider=...,          # providers 工厂（GLM 文本模式先行，Q2 定案）
                 backend_factory: Callable[[], DshBackend] = ...): ...

class WsSession:
    async def on_ws_message(self, msg) -> None:
        """二进制→_on_audio；文本→按 t 分派 _on_auth/_on_start/_on_end/_on_ping。"""
    async def _on_audio(self, pcm: bytes) -> None:
        """InputAudioRawFrame 入管线；背压：忙时丢最老块（实时性>完整性）。"""
    async def _build_pipeline(self) -> None:
        """工具 = dsh_head_tools() 四件套（N1§1.3）；TranscriptState 挂 tap；
        断线重连 run_with_reconnect + take_tail 重播种（PoC V2 已验证路径）。"""
    async def event_sink(self, kind: str, payload: dict) -> None:
        """EventBus.subscribe 回调；序列化 {t:kind, **payload, ts} 下发。
        慢客户端：send queue >100 时合并 progress（保 dispatch/ack/done/gate 不丢）。"""

class TailReader:
    """dais 增量尾读协程（orch.progress 源）；每活动 worker 一个。"""
    async def follow(self, dispatch_id: str) -> None:
        """cursor=0 起步；read-worker --after → 摘要 → emit；cursor 从 STDERR 解析（勿混流）；
        worker 终态退出。轮询走 DaisLane 单飞锁（总线语义 N1§2）。"""
```

静态页：AudioWorklet 采集/播放（欠载插 20ms 静音抗抖）+ event 流渲染 run/ticket/worker 三级树 + gate 弹层（答案回传 control 帧 `gate.resolve{gate_id, resolution}` → `DaisLane.resolve_gate`）。

## 3. 事件 schema（orch.* 汇总，与 N5§3 同源）

```jsonc
{"t":"orch.dispatch","run_id":"run_x","ref":"vh-…","tickets":["LK-01"],"credentials":["【凭证…】"],"ts":...}
{"t":"orch.ack","node":"LK-01","worker":"dev1@term_x","ts":...}
{"t":"orch.progress","dispatch_id":"ctx_x","lines":12,"head":"…首行摘要","ts":...}
{"t":"orch.gate","gate_id":"g_x","question":"…","options":["A","B"],"ts":...}
{"t":"orch.done","run_id":"run_x","artifact":"Agent Final Message…","credentials":[...],"ts":...}
{"t":"orch.metrics","ttfb_ms":812,"e2e_ms":4210,"workers":3,"ts":...}
```

## 4. 断线重连与安全基线

- 断线：客户端指数退避重连（0.5→8s）；重连后 `session.start{session_id}` → 网关用 `TranscriptState.take_tail()`（`〔loc:...rt_transcript.py:97〕`）在新 head 会话重播种；
- 安全（局域网面）：`auth{token}` = `VOICE_GATEWAY_TOKEN`（env；N5§5）；bind 默认 `0.0.0.0`（局域网可达）可配置 `--loopback-only`；单 token 并发会话≤2；media 码率上限（>64KB/s 断开）；不落盘音频，transcript 仅内存会话级。

## 5. 实施与验证序列

| 步 | 交付 | 验证 | 完成判定 |
|---|---|---|---|
| W4.1 | 网关 + 帧协议 + 本机回环 | `tests/test_rt_gateway.py`（协议单测）+ 浏览器本机 e2e | mic→head→下行音频通 |
| W4.2 | EventBus 接入 + TailReader | 事件流断言（fake lane） | dispatch/progress/done 全序到达 |
| W4.3 | 局域网联调 + 时延基线 | 局域网另一设备连通；orch.metrics 三指标报告（首音延迟/RTT/事件时延） | V8（局域网基线，live 序列） |
