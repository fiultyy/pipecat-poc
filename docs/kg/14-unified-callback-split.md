# 14 · 回调分流统一框架（A/B/C/D 合并裁决稿）

依据实读：`rt_gateway.py`（`_on_final`/`_inject_final_when_idle`/`_live_heads`/`main._bridge`/`TOPIC_KINDS`）、`rt_dsh_backend.py`（`_liaison_roundtrip`/`_phase2`/`_runs`）、`rt_head_tools.py`（`DoctrineSource` 五件套）、maestro 信封。子设计 A-Store/B-Protocol/C-Persona/D-Display 有冲突，裁决如下。

## 1. 冲突裁决

| # | 冲突 | 裁决与理由 |
|---|---|---|
| 1 | store 载体：A 新文件 `rt_session_store.py` vs B 挂 gateway 模块级 `_body_store` | A：独立文件、挂 VoiceGateway 单例；B 的 `app_resources["body_store"]` wiring 保留。gateway 已 1140 行，工具需稳定 import 面 |
| 2 | 开关四套：`VOICE_STORE_MODE`/`VOICE_FINAL_FULLTEXT`/`VOICE_RECEIPT_SLIM`+`VOICE_FINAL_DELIVERY`/`VOICE_FINAL_MODE` | 收敛正交两枚：`VOICE_FINAL_MODE=fulltext|split`（缺省 fulltext）、`VOICE_RECEIPT_SLIM=0|1`（缺省 1）。终稿投递与回执形态两条链路各自独立回退 |
| 3 | 观测 topic：`store.notice`+`store.snapshot` / `body.stored` / `body.push` | 单 topic `body.push`：轻通知、chars≤4096 附 inline 全文；索引入 `topic_cache` 回放（兼掉 snapshot 与 list 帧） |
| 4 | control 帧：`store.get/store.list` vs `body.get` | `body.get→body.item`，miss 回 `error body_miss`；list 由回放 items 承担 |
| 5 | 工具：A `read_final(ref)` vs B `read_body(ref_or_no,max_chars,from_tail)`+`list_bodies` vs C 常驻+no-store 降级 | B 签名+C 降级；`read_final` 弃 |
| 6 | 回执可见面：B 留 credentials（只不念）vs C 剔出 | C：模型只见 `status/ref/no/summary[/tasks]`；credentials/run_id 落 orch.dispatch 事件+store（三落点对账）。口播金丝雀失效属预期（开放问题 1） |
| 7 | 通报：B 自然语句内嵌指令 vs C `[编排通报]`+JSON、行为全在 doctrine | C+`no` 字段；注入项不带指令——doctrine 是单事实源，指令随载荷重复即漂移 |
| 8 | 写入面：A backend 回调直写 vs B bus 订阅+另留 `on_failed` 直连钩子 | B 的 `main._bridge` 订阅为唯一写入面；`on_receipt`/`on_failed` 直连均弃——单路径，backend 不感知 store |
| 9 | 语音会话 kinds：B 不推正文事件 vs D `+= body.push` | D：body.push 是轻通知，喂语音页迷你行；topic 只到客户端壳、不进 head 上下文，无刷屏 |
| 10 | 状态字段：A `kind=receipt|final` 双字段 / B `final` / C `done` | 去 kind；`status=accepted|running|done|failed|cancelled|timeout`，对齐 orch.done 命名 |
| 11 | B、C 各占一份 `13-*.md` | 本稿 N14 收编；13-* 降为子设计附录 |

## 2. 统一框架

### 2.1 数据模型（`rt_session_store.SessionStore`，key=ref）

```json
{"ref":"vh-<redacted>","no":2,"status":"done","title":"竞品定价调研",
 "summary":"采纳方案B，收益约41%","body":"全文…","chars":1834,
 "credentials":["【凭证R-VH1A2B3C4D】"],"run_id":"run-9f00","conv_id":null,
 "ts":1759300000.0,"updated_ts":1759300123.4}
```

- `no`：backend 全局自增，受理时分配，入回执+orch.dispatch；跨重连稳定。
- `conv_id` 由 bridge 按当前活会话尽力回填，仅作保留配额，可 null。
- summary/title 机械生成（正文首非空行截 60/16 字），零 LLM。
- 保留：LRU 500 + **SQLite**（`~/.local/state/voice-gateway/store.db`，WAL；裁决 #4）；dais 信箱读即消费，不作恢复源。
- query/cancel 的 liaison roundtrip 不入库（body 前缀 STATUS/CANCEL 过滤）。

### 2.2 分流协议（写入面=`main._bridge` 单点）

orch.dispatch→put(accepted)；orch.done→update(done, body=artifact)+emit body.push+通报注入；新增 orch.failed（`_phase2`/`_phase2_dag` 两处静默 return 处 emit）→update(failed)+通报；cancel 的 orch.done artifact="(已取消)"→status=cancelled。orch.done **不带 artifact**（裁决 #3：正文只走 body.push，无双写期）。progress 零注入 head；心跳不做（裁决 #5），状态留 store 随时查。

### 2.3 head 面

回执（SLIM=1；=0 回旧形 run_id/credentials/note 原样）：

```json
{"status":"accepted","ref":"vh-<redacted>","no":2,"summary":"已受理，转对接人执行"}
```

dispatch_plan 加 `"tasks":3`。完成通报（split 模式，走 `_inject_final_when_idle`/`_live_heads`/pending/`_flush_pending` 原机制，仅换载荷）：

```json
[编排通报] {"no":2,"ref":"vh-<redacted>","status":"done","summary":"采纳方案B…","chars":1834}
```

注入点同步 emit `head.turn{"phase":"notify",...}`。工具七件套：+`read_body`/`list_bodies`。doctrine=C 稿四段式（回执一句话、通报一句话+详情栏指引、编号默认不念、完整 ref 只给工具）。

### 2.4 显示面（rt_voice_app）

第 6 页签「详情」：左 Treeview（time/no/ref/title/chars，按 ref 去重）+右只读正文；选中→inline 直渲染，否则经观测连接 body.get。语音页加迷你通知行（订 body.push；`DEFAULT_VOICE_KINDS += body.push`）。回合页：notify 相（`TURN_PHASE_LABEL`+"📣"）；body.push 同 ref 追加灰行「└已入详情」。

### 2.5 压缩衔接（零 LLM 确定性快照）

回合超阈值后下次 turn_idle：从 `store.list()` ∪ `backend._runs−store` 机械拼状态快照，以单 user item 替换历史回执/通报 items，emit head.compact：

```json
{"t":"state.snapshot","ts":1759300123.4,"tasks":[
 {"no":1,"ref":"vh-<redacted>","status":"done","summary":"…","chars":1834},
 {"no":2,"ref":"vh-<redacted>","status":"running","elapsed_s":412}],
 "counts":{"done":1,"running":1}}
{"t":"head.compact","conv_id":"s-ab12cd34","before_chars":21340,"after_chars":512,"pinned":3,"reason":"threshold","ts":1759300123.4}
```

### 2.6 迁移（PR 粒度，每步独立验证+回退）

1. **PR1** `rt_session_store.py`+bridge 写入面+body.push/body.get 帧；缺省 fulltext，store 静默积累。验证：派发后 body.get 命中；回退：零行为变化。
2. **PR2** 回执瘦身+orch.failed emit+no 序号；doctrine 先走 `VOICE_HEAD_DOCTRINE` 外置（零代码回退）。验证：播报一句话；回退：SLIM=0。
3. **PR3** split 投递：`_on_final` 分流，split 不接 `backend.on_final`（`if self.on_final` 护栏已在）。验证：终稿入库+一句话播报；回退：MODE=fulltext。
4. **PR4** read_body/list_bodies+kinds 扩展。验证：「查看任务2」可取正文。
5. **PR5** 详情页+notify 相+head.compact 快照；翻缺省 split。回退矩阵：两开关+doctrine 指回旧稿。

## 3. 开放问题（已全部裁决，2026-08-27 用户拍板）

1. credentials 退出模型上下文——**确认**。念出来本来就多余；对账迁观测面机器比对。
2. `no` 全局编号，口播直接叫「任务N」——**确认**。
3. orch.done.artifact——**立即停发**，正文只走 body.push 新通道（不留双写期；`_phase2` emit 处去 artifact）。
4. 台账载体——**SQLite**（`~/.local/state/voice-gateway/store.db`，WAL；替原 JSONL 方案），LRU 500 条照做。
5. progress 主动播报——**不做**；状态留在 store（status 字段随时可查），用户问走 query_status。
