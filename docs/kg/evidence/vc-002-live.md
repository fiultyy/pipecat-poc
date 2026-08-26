# VC-002 live 凭证：真 Qwen-Omni realtime 头全链路（2026-08-26 20:07）

- 驱动: `/tmp/probe_vad_e2e.py`（gateway :8765 真实链路，QwenOmniRealtimeLLMService 直连 DashScope `qwen3.5-omni-flash-realtime`，edge-tts 合成语音「你好，现在几点了」16k PCM16 + 0.7s 静音尾）
- 总判: PASS（VAD 判句尾 → 自动应答 → 真实语音下行）
- commit: `1c7f391`（llm.py URI 修复 + 错误容忍；rt_gateway observer 重构；providers turn_detection 透传；rt_voice_app PTT 静音尾在 `30df987`）

## 事件序列（末次运行，网关 journalctl + probe 输出）

```
0.54s session.started
2.95s streamed speech + 0.7s silence tail
2.95s turn user_start          # 服务端 VAD input_audio_buffer.speech_started
2.95s turn interrupted
3.09s turn user_end            # speech_stopped + committed（服务端自动提交）
3.26s turn assistant_start
3.52s turn assistant_start     # 服务端 assistant item 事件驱动（多 item）
4.60s turn assistant_end 你好你好！我现在没法告诉你具体的时间…
RESULT user_start=True assistant_end=True audio_bytes=241920
```

## 根因链（三层，全部实测定位）

1. **URI 缺 `?model=`**（llm.py）：DashScope 无 query 参数时会话半死——收 session.update、出转录 delta，但永不出 VAD 事件、永不自动应答。裸线 A/B：同 session 字段同音频，有 query 全链通、无 query 全无。
2. **管线内自定义 processor 卡死**（rt_gateway `_Tap`）：插在 realtime 头与 assistant aggregator 之间的 processor 的 process 任务不随 StartFrame 建立，TTS/文本帧堆积其队列无人消费（实测 48 帧积压、process_task=None），客户端零音频。对照：去掉该 processor 全链畅通。修复=PipelineWorker observer（push 边挂钩、只认下行、frame.id 去重）。
3. **可恢复错误杀死 receive loop**（llm.py error 分支）：response.create 与服务端 VAD 自动提交竞态时 DashScope 返回 `code=''` + message="Conversation already has an active response"，原容忍名单按 code 匹配落空 → return 杀 receive loop → response.done 丢失 → assistant_end 永不到。修复=code+message 双匹配。

## PTT 静音尾（rt_voice_app `30df987`）

服务端 VAD 需要尾随静音判定句尾；PTT 松键采集骤停不给尾巴则 VAD 永不触发（18:54 用户实测「说了，没回应」根因之一）。客户端松键补发 14×50ms 静音。

## 限制（如实记录）

- 应答语音有字词重复（模型/合成侧），未处理。
- pipecat 框架层「自定义 processor process 任务不建立」的机制未继续深挖（observer 规避），值得上游报告。
- DashScope `conversation.item.input_audio_transcription.delta` 带 `stash` 字段、pydantic 校验警告为已知噪音（转录镜像经 completed 事件仍可达，本凭证 user_text 正常）。
