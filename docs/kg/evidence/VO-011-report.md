# VO-011 报告 · rt_gateway 本地 WS 网关（M4，W4.1–W4.2 离线部分）

- **判定**: ✅ 通过（本票范围=帧协议自动单测 + 静态页 + TailReader + 安全基线；24/24 全绿）
- **日期**: 2026-08-23 · **worktree**: vo-011
- **规范源**: `docs/kg/04-ws4-gateway.md`（KG 04 §0–§4 = 设计规范）· 票: `docs/tickets.md` VO-011 · 验收: `docs/plans/impl-specs.md` VO-011 ①③

## 交付物（严格票内范围）

| 文件 | 内容 |
|---|---|
| `examples/realtime-provider-poc/rt_gateway.py` | 新建 745 行：`VoiceGateway`（aiohttp，`/ws`+`/healthz`+`GET /`+`/static/`）、`WsSession`（三路复用）、`TailReader`、`EchoHead`（`--echo` 回环头）、`build_realtime_head`（惰性导入真 pipecat 头，形制=poc_t6_pipeline）、`main()` CLI（`--port/--host/--loopback-only/--token/--echo`） |
| `examples/realtime-provider-poc/web/index.html` | 新建静态页：AudioWorklet 采集/播放（PCM16LE/16k/mono，欠载插静音抗抖）+ run/ticket/worker 三级树面板 + gate 弹层（`gate.resolve{gate_id,resolution}` 回传）+ 断线指数退避重连（0.5→8s，重连带旧 session_id） |
| `tests/test_rt_gateway.py` | 新建 24 用例（全 fake pipeline，不起真 head） |

未触碰 `src/pipecat/`、`rt_event_bus.py`、`rt_dsh_lane.py` 等既有模块（红线遵守；未 push、未 commit）。

## 验收对照（impl-specs VO-011）

| 验收条 | 状态 | 证据 |
|---|---|---|
| ① 帧协议单测绿（握手/背压/慢客户端合并） | ✅ | 24 用例全绿（见 A 节）：握手序 6 例、media echo/背压丢最老/64KB/s 断流 4 例、慢客户端合并 1 例、全序 1 例 |
| ② 本机 e2e mic→head→音频 | ◐ 部分 | 浏览器冒烟：页面加载（10 DOM 锚全在位、0 JS 错误）+ 真 WS 握手 `open→auth.ok→session.started→pong` + 二进制媒体回环（发 640B PCM→收 640B echo，`--echo` 头）。真 head（Qwen realtime + dais 在线）的 mic→STT→TTS 全链属 live 联调（W4.1 完成判定），留待 dais 在线时补 |
| ③ dispatch/progress/done 全序到达 | ✅ | `test_event_total_order_preserved`：30 帧混类事件到达序==发射序；`test_event_bus_forwarding_schema_kg04_s3`：schema 逐字段对照 KG 04 §3 |
| ④ 局域网连通 + orch.metrics 三指标 | ☐ 未做 | 需局域网另一物理设备（live 序列 V8，票面验证形式=本机 e2e+局域网联调，环境不可用于本 worktree） |

## 设计要点（KG 04 逐条落位）

- **三路复用（§1）**：单连接；文本 JSON=control/event、二进制=media；握手序 `auth→auth.ok{session_id}→session.start{session_id?}→双工`；非法 token/未 auth 先行媒体均回 `error` 帧后断开。
- **慢客户端合并（§2）**：出站队列元素为 dict（序列化延迟到发送），`orch.progress` 在队列 >100 时原位并入队列中既有帧（`lines=max`、`head=最新`、`ts` 更新）；dispatch/ack/done/gate/metrics 永不丢、永不并。测试冻结 sender 首帧后灌 200 帧 progress：下行恰 102 帧（101 独立+1 合并槽），dispatch/done 各 1 帧不丢。
- **媒体背压（§1）**：入站音频队列封顶 16 块（~320ms），管线忙丢最老（实时性>完整性）；测试用阻塞头验证 40 块灌入→丢 23、管线恢复后 push 序列=首块+最新 16 块。
- **安全基线（§4）**：`VOICE_GATEWAY_TOKEN` 鉴权（显式空 token 拒启动）；单 token 并发会话≤2（第三连接 `concurrent_limit` 错误帧+断开，释放名额后可再入）；media 滑动 1s 窗 >64KB/s 回 `rate` 错误帧后断开；音频不落盘；transcript 仅内存。
- **断线重连（§4）**：异常断开（无 `session.end`）→ teardown 时 `TranscriptState.take_tail()` 停靠网关续接槽（TTL 600s）；重连 `session.start{旧id}` → `pop_resumable` → 逐条 `seed()` 重播种，`session.started{reseeded:true,entries:2}` 回执且会话 id 不变。优雅 `session.end` 不留槽，旧 id 再来回 `reseeded:false` 新 id。
- **TailReader（§2）**：cursor=0 起步经 `DaisLane.read_worker(--after)` 增量；增量→`orch.progress{dispatch_id,lines 累计,head 首行≤120字}`；lane 软错误（`{"error":…}` exit-0，DaisLane 已归一为 `DaisLaneError`）→协程退出；`stop` Event 可外部终止。
- **gate 弹层回传（§2）**：`gate.resolve{gate_id,resolution}` → `DaisLane.resolve_gate`；lane 错误回 `error{code:"lane"}` 帧不断连。
- **`build_realtime_head`（live 阶段）**：惰性导入 pipecat（单测零依赖 extras）；`dsh_head_tools()` 四件套 + transcript 镜像 tap + `TTSAudioRawFrame→session.send_audio`；GLM 文本模式先行（Q2 定案，音频下行可能为空）。

## 发现（跨票联动）

1. **`rt_event_bus.py` wildcard 订阅双投递**：`subscribe(cb)` 无 kinds 时 entry 先 `self._listeners["*"].append(entry)`（L29 恒真表达式），随后 `for kind in kinds or ["*"]` 再 append 一次 → 同 entry 挂 `"*"` 两次，`emit` 对 wildcard 订阅者回调两次。本仓网关侧规避：`WsSession` 按 KG 04 §3 六种 orch.* 显式 kinds 订阅（`ORCH_KINDS`）。**rt_event_bus.py 不在本票改动范围，建议 W5 线（VO-003/004 或专项小票）修掉 L29**（`self._listeners["*"].append(entry) if not kinds else None` 这行本身是死代码式写法）。
2. dais `read-worker` 软错误契约（exit-0 + JSON error，impl-specs 环境前置 1 已记）在 TailReader 中表现为"终态退出"——符合预期，无残留轮询。

## A. 测试输出原文

命令（票面原文，cwd=本 worktree）：

```
$ ~/workspace-claw-02/pipecat-poc/.venv/bin/python -m pytest tests/test_rt_gateway.py -q
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: ~/orca/workspaces/pipecat-poc/vo-011
configfile: pyproject.toml
plugins: anyio-4.14.2, asyncio-1.4.0
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=function, asyncio_default_test_loop_scope=function
collected 24 items

tests/test_rt_gateway.py ........................                        [100%]

============================= 24 passed in 47.94s ==============================
```

用例清单（24）：healthz/静态页、auth 握手序、坏 token 拒+断、未 auth 拒、bad_json/unknown_type/ping-pong、双 start/双 auth bad_state、session.end 优雅收尾、media 二进制 echo 回环、media 先行拒、背压丢最老（40 灌/丢 23/恢复序）、64KB/s 断流、event schema（KG 04 §3 对照）、event 全序、慢客户端 progress 合并（关键帧不丢）、gate.resolve 通/lane 错/缺参、断线 take_tail 重播种续接、优雅结束无续接槽+旧 id reseeded:false、并发≤2（含名额释放）、token env+空 token 启动守卫、echo provider 接线、TailReader 增量/软错误退出、TailReader stop 终止。

## B. 浏览器冒烟（真网关进程 + headless Chromium）

- 进程：`rt_gateway.py --echo --port 8799 --loopback-only --token smoke-token`，ready log 命中。
- 页面：`GET /` 200 text/html；10 个 DOM 锚（token/micBtn/endBtn/pingBtn/tree/log/metrics/gatePanel/gateQ/gateOpts）全部在位；0 pageerror/console error。
- WS 握手（页面环境发起）：`open → auth.ok → session.started → pong` ✅。
- 媒体回环（页面环境发起）：`session.started` 后发 640B PCM 二进制 → 收 640B 二进制 echo ✅（EchoHead 路径，即 mic→网关→播放的全媒体链路代理验证）。

## C. 后续（非本票范围）

- W4.1 完成判定"mic→head→下行音频"需 dais 在线 + Qwen realtime 凭据：跑 `rt_gateway.py`（非 echo）真 head 联调。
- W4.3 局域网基线（V8）：另一设备连 `ws://<host>:8765`，采 orch.metrics 三指标（首音/RTT/事件时延）。
- `rt_event_bus.py` L29 wildcard 双投递修复（见"发现 1"）。

done body: 通过;报告:docs/kg/evidence/VO-011-report.md;测试:24项全绿;备注:EventBus wildcard双投递已绕行,建议W5线修rt_event_bus L29
