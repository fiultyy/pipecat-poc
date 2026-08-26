# 12 · rt-voice ONE 桌面客户端（语音面 + 观测面）

> 依据：用户裁决（2026-08-26，gen3 会话）——SPA 路线早已放弃（rt_voice_app 07c7eb4
> 建成即接替）；封装 ONE 桌面客户端：语音开关+系统 mic 输入（现状）+ 编排观测
> （本轮接入）。双连接分面 + Notebook 页签呈现。

## 0. 范围

| 项 | 内容 |
|---|---|
| 交付 | rt_voice_app.py 升级：①语音页（现状全保留）②观测页签四件（任务/回合/席位/消息+票板）③observe 双连接管理 |
| 非目标 | cmd 上行控制（P1.5 解冻前不做）；SPA/web 页（已放弃）；frp 公网；跨机分发打包（pyinstaller 等后续单议） |

## 1. 连接形态（双连接分面）

- **语音连接**：现状 VoiceLink 不动——voice 会话（缺省 topics=orch.*+head.turn，
  保持带宽最小）；断线 2s 重连 + resume 续接。
- **观测连接**：新增 `ObserveLink`——独立 WS，`session.start{observe:true}`（缺省
  订阅全部 SUBSCRIBABLE_KINDS，不建 head、拒媒体）；独立开关按钮，与语音连接
  生命周期解耦（观测可先于语音开、语音断不影响观测）。
- 两条连接同 token 计入网关并发上限 ≤2（KG 11 §4）——即本客户端自身占满单
  token 配额，多开需多 token（如实呈现于 UI 状态行）。

## 2. 页签四件（数据源→呈现）

| 页签 | topic 源 | 呈现 |
|---|---|---|
| 编排任务 | orch.dispatch/ack/progress/gate/done/metrics | run→ref 任务树（种子页三级树逻辑移植）+ 事件时间线 |
| 回合轨迹 | head.turn | phase 时间线（user_start/user_text/assistant_start/tool_call/interrupted…），user_text/assistant_end 显示 detail |
| 席位 | fleet.snapshot | 表格：code/alias/node/role/project/status（快照全量替换渲染） |
| 消息+票板 | bridge.msg / tickets.snapshot | bridge 增量行滚动日志；tickets 全文快照尾（最新 N 行） |

## 3. UI 骨架

- `ttk.Notebook`：语音页（现 App 内容迁入）｜编排页｜回合页｜席位页｜流页。
- 顶栏保持（地址/token/连接/结束会话），新增「观测」开关按钮 + 观测连接状态。
- 事件泵：ObserveLink 独立 rx 队列 → `_tick` 统一收割渲染（复用 33ms 泵）。

## 4. 验收

1. 单测：ObserveLink 握手（auth→observe session.start→session.started 回显
   topics）；页签渲染函数纯逻辑（fleet 表格行解析/树构建）mock 帧可测。
2. live：真网关起（--echo 或 backend 形态），双连接同开，四页签各收到真实
   topic 帧（fleet/tickets 快照订阅即回放）；语音面回归（开闭麦/频谱/重连）。
3. 证据落 `docs/kg/evidence/vc-001-live.md`。

## 5. 边界

- 控制面只走 LAN/loopback（继承 KG 04 §4，永不进 frp）。
- 观测只读：topic 源全部为尾读/快照，本客户端零写入。
