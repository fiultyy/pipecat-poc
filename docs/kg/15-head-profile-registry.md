# 15. head 配置注册表：多 head 配置，激活单例（PR8）

日期：2026-08-28 · 状态：✅ 已落地（nova + echo/scribe 副本占位）

## 1. 目标与裁决

多 head agent 的**配置架构**先行落地；**激活保持单例**——任一时刻只有一个
head 处于激活态，语音会话按构建时刻的激活 profile 建 head。切换只改
下一次会话的选择，活会话不拆（对齐 realtime 会话不可热换 instructions 的
现实约束）。当前 echo/scribe 为 nova 的全 null 副本占位（字段逐项继承旧
单头路径 = 当前 head 的精确复制），架构先立、人格后填。

## 2. 配置面

- 文件：`VOICE_HEAD_PROFILES` 指定路径，缺省
  `~/.config/voice-gateway/heads.json`（部署面，不随仓库走；仓库内
  `examples/realtime-provider-poc/heads.example.json` 为带注释样例）。
- 形制：`{"active": name, "profiles": [{name,label,model,voice,doctrine,
  doctrine_file,turn_silence_ms,turn_prefix_ms,turn_threshold}, …]}`。
- 逐字段优先级（build 时）：profile 值 → 旧单头路径
  （`VOICE_HEAD_MODEL`/`VOICE_HEAD_VOICE`/`VOICE_HEAD_DOCTRINE`/
  `VOICE_TURN_*` env → 内置缺省）。**全 null 条目 = 旧单头行为**，
  占位副本即此形态。
- 降级：文件缺失/坏 JSON/坏表（空表、无名项、重名）→ 整体降级单头
  （合成 default profile，全部字段 None）+ stderr 告警；绝不因配置坏而
  起不来。
- 钉住：`VOICE_HEAD_PROFILE=<name>` 载入期覆写 active（运维钉人）；
  钉住期间 `head.switch` 拒绝。

## 3. 控制帧与 UI

- `head.list` → `head.list.result{active,file_backed,env_pinned,
  profiles[{name,label,active,model,voice}]}`（只读）。
- `head.switch{name}` → 换激活 + 原子写回文件 active；回包
  `head.switch.result{ok,active,note}`，note 明示「下一次语音连接生效」。
  写回失败时内存选择仍生效但如实注明不跨重启。拒绝路径：无 profiles
  文件（单头模式）、env 钉住、未知名（bad_request）。
- UI（rt_voice_app 语音页 head 行）：观测开启即 `head.list` 渲染单选钮；
  切换发 `head.switch`，结果一行入语音日志；单头模式只显说明不渲染钮。

## 4. 实现位形

- `rt_head_registry.py`：`HeadProfile`（纯数据）+ `HeadRegistry`
  （profiles/active/path/env_pinned/warnings/raw 保留供写回）+
  `load_head_registry(env)`（永不抛）。
- `rt_gateway.py`：`_head_registry()` 惰性单例（首载一行日志）；
  `_profile_doctrine`（inline → file → DoctrineSource）；
  `_profile_turn_detection`（profile 旋钮 → env 旋钮 → None）；
  `build_realtime_head` 按激活 profile 取 model/voice/doctrine/VAD；
  WsSession 增 `head.list`/`head.switch` 两帧。

## 5. 验证与回退

- 验证：test_rt_head_registry（11：happy/pin/降级/坏表/坏旋钮）+
  test_rt_gateway M2（6：profile 覆盖 env、null 回落 env、doctrine 文件
  与空文件回落、list/switch 全链路含写回与幂等、单头拒绝、钉住拒绝）+
  test_rt_voice_app（3 纯函数）；live 探针：部署位形 3 profiles
  active=nova 零告警，观测连接 head.list 全链路回包正确。
- 回退：删 `~/.config/voice-gateway/heads.json` 即回精确旧单头行为
  （降级路径即回退路径）；代码回退 = 去 registry 单例接入与两控制帧。

## 6. 开放问题（未裁决）

1. 多 head **并发**激活（多连接各建各的 head，Q3 研究已确认协议面可行）
   ——待真实多人格需求出现再裁；当前单例先行。
2. profile 里补 liaison 绑定（不同 head 各绑各的对接人格）——等占位
   副本填真人格时一并裁。
