# DashScope realtime 方言审计（2026-08-26）

- 对象: `src/pipecat/services/qwen/realtime/llm.py`（基于 OpenAI 父类 `openai/realtime/llm.py` 改写）
- 方法: subagent 对照 DashScope 官方文档逐事件核对（文档摘录 `/tmp/dashscope-docs/`）
- 结论: 16 项确认正确；3 项 P0 条件性缺陷已修（`895fa6d`）；其余如下未修

## 已修（`895fa6d`）

| 项 | 问题 | 修复 |
| --- | --- | --- |
| B1 | `input_audio_transcription.delta` 无 `delta` 字段（预览在 `stash`） | `_normalize_server_message` 合成 `delta = text + stash` |
| B7 | `conversation.item.truncate` 非 DashScope 事件 | 覆盖 `_truncate_current_audio_response` 只清本地状态 |
| B8 | 稀疏 usage（无 `*_token_details`）pydantic 校验失败丢 turn-end | usage 补默认空 details |

## 未修 P1（有实锤、暂不影响当前链路）

- **B2** `session.audio` 格式块形状与文档不符（transcription_config 的 audio 字段嵌套错位）。
- **B3** 视频输入走错事件类型（当前无视频输入路径，不触发）。
- **B4** `transcription.failed` 无对应 OpenAI 事件，解析跳过并仅告警——失败时无 ErrorFrame 上抛。
- **B5** 服务端 1007 主动关闭（如音色拒绝）时 receive loop 静默退出，无 ErrorFrame；依赖外层重连。音色探针实证：Tina/Serena/Ethan 之外 11 个音色 response 阶段 1007。

## 未修 P2

- `max_output_tokens` → DashScope 名 `max_tokens`（现名不生效）。
- `tool_choice` 取值映射未核对。
- `tools` 嵌套层级（type 包裹）可能多一层。
- OpenAI transcription 配置（language 等）泄漏进 DashScope session 字段。
- language 字段被丢弃（DashScope 不支持显式语言）。
- 过期 docstring（描述 OpenAI 侧行为）。

## 未验证 U（无文档实锤，需实验）

- U1 服务端是否接受 `response.create` 带 `modalities:["audio"]`（文档标注 audio-only）
- U2 `input_audio_buffer.append` 之外的上行通道
- U3 `session.update` 部分字段热更新是否生效
- U4 `response.cancel` 与 `response.create` 竞态的错误码全集
- U5 音频 delta 乱序/丢帧的客户端容限
- U6 会话超时自动断开的报文形态
- U7 temperature 采样参数是否生效
