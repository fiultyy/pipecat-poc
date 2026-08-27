# 14 · 回调分流统一框架（A/B/C/D 合并裁决稿）

依据实读：`rt_gateway.py`（`_on_final`/`_inject_final_when_idle`/`_live_heads`/`_store_bridge`/`_first_line`/`TOPIC_KINDS`）、`rt_dsh_backend.py`（`_liaison_roundtrip`/`_phase2`/`_phase2_dag`/`_runs`）、`rt_session_store.py`（put/update/get/list）、`rt_head_tools.py`（`DoctrineSource` 五件套）、maestro 信封。子设计 A-Store/B-Protocol/C-Persona/D-Display 有冲突，裁决如下。落地进度：PR1–PR4 已合入（§2.6），PR5 待办。

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
| 8 | 写入面：A backend 回调直写 vs B bus 订阅+另留 `on_failed` 直连钩子 | B 的 bus 订阅为唯一写入面（落地为 `attach_store_bridge`→`_store_bridge`）；`on_receipt`/`on_failed` 直连均弃——单路径，backend 不感知 store |
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

### 2.2 分流协议（写入面=`_store_bridge` 单点；✅PR1 建、PR3 body 直载）

orch.dispatch→put(accepted)；orch.done 载荷 `{ref, run_id, body}`（body=剥 FINAL_PREFIX 的原始终稿正文；cancel 路径无 body）→update(done)+emit body.push；orch.failed→update(failed)+emit body.push，emit 点共四处：`_phase2` 超时、`_phase2` lane 错至 deadline、`_phase2` lane-a task 消失、`_phase2_dag` deadline 到期（对齐 ref/run_id/reason 形态）。bridge 取 payload `body` 优先、旧 `artifact` 哨兵兜底；cancel 语义两形态不变（`status:"cancelled"` 或 artifact="(已取消)"）。artifact 通道停发（裁决 #3）；台账 body 自此有值，body.push 的 title/summary/inline 自然填充。backend 不感知 store、不感知 VOICE_FINAL_MODE（policy 在 gateway）。progress 零注入 head；心跳不做（裁决 #5），状态留 store 随时查。

### 2.3 head 面

回执（✅PR2；SLIM=1 缺省，=0 回旧形 run_id/credentials/note 原样）：

```json
{"status":"accepted","ref":"vh-<redacted>","no":2,"summary":"已受理，转对接人执行"}
```

dispatch_plan 加 `"tasks":3`。终稿投递由 `VOICE_FINAL_MODE` 分支（✅PR3；helper 每次现读 env，仅精确 `split` 激活，缺省 `fulltext` 免重启回退；PR5 验证后翻缺省 split）：

- **fulltext（缺省）**：现行注入串字节级不变——`[编排终稿 ref] final` + 「请把上述终稿口语播报给用户：原样转述，不添加事实。」
- **split**：纯数据通报，单行 JSON，通报后不接任何指令句（裁决 #7：注入载荷不嵌指令，doctrine 是行为唯一真源，PR2 doctrine 已含 [编排通报] 条款）：

```json
[编排通报] {"no":2,"ref":"vh-<redacted>","status":"done","summary":"采纳方案B…","chars":1834}
```

  `no/status/summary/chars` 取 `store.get(ref)`（backend 先 emit orch.done 再调 on_final，时序保证台账已更新）；store 不可用或查不到时降级：ref 照旧、summary/chars 从 final 本体算（剥 FINAL_PREFIX 后 `_first_line(text,60)` 与 len）、no 缺省省略、stderr 告警。两模式共用 `_inject_final_when_idle`/`_live_heads`/pending/`_flush_pending` 原机制——注入串在 `_on_final` 时点定形，缓冲与补投不区分模式。

注入点同步 emit `head.turn{"phase":"notify",...}`（PR5）。doctrine=C 稿四段式（✅PR2：回执一句话、通报一句话+详情栏指引、编号默认不念、完整 ref 只给工具）。

工具面七件套（✅PR4；裁决 #5 B 签名+C 降级，`read_final` 弃用不实现）：原五件（dispatch_intent/dispatch_plan/query_status/cancel_run/remain_silent）+`read_body`/`list_bodies`。

- `read_body(ref_or_no, max_chars=None, from_tail=False)`——取一条终稿正文。`ref_or_no` 接受完整 ref（vh-…）或编号 no（用户念"任务2"→通报/台账里的 no）；`max_chars` 截断保护（指示 `truncated`+`returned_chars`）；`from_tail=True` 取尾部（终稿结论常在尾）。返回三形态：命中 `{"status":"ok","ref","no","task_status","title","chars",…body}`；查无 `{"status":"miss",…}`；台账不可用 `{"status":"error","reason":…}`（C 降级：模型据此答"详情暂不可用"，会话不炸）。
- `list_bodies(limit=None)`——最近台账索引，item 形如 `{no,ref,status,title,summary,chars,ts}` 无正文；缺省 10 条、上限 20。
- `cancel_run` 改 ref-only（参数名 `ref`，删"或 run_id"）。
- 接线：gateway 会话 `app_resources` 加 `"voice_store"`（`_get_store()` 结果，可为 None，由工具侧降级）；`DEFAULT_VOICE_KINDS += body.push`（语音观测面自动收轻通知）。工具侧从 `params.app_resources.get("voice_store")` 解析台账（缺 key/None→error 形），同步 get/list 经 `asyncio.to_thread` 包裹、异常吞成 error 形；rt_head_tools 不 import rt_gateway（免环）。
- 约束：工具只读台账、绝不写——`_store_bridge` 仍是唯一写入面（裁决 #8 不变），backend 不感知 store。doctrine # Tools 补两条款（read_body：用户要正文/细节时用，ref_or_no 取上下文完整 ref 或用户念的编号，超长 max_chars/from_tail 分段；list_bodies：问"都有什么任务/什么状态"时列台账），# After Tool Calls 补一条（read_body/list_bodies 结果按用户所问讲，长文先结构要点再按需分段展开）。

### 2.4 显示面（rt_voice_app，PR5 待办）

第 6 页签「详情」：左 Treeview（time/no/ref/title/chars，按 ref 去重）+右只读正文；选中→inline 直渲染，否则经观测连接 body.get。语音页加迷你通知行（订 body.push；gateway 侧 `DEFAULT_VOICE_KINDS += body.push` 已随 PR4 落地）。回合页：notify 相（`TURN_PHASE_LABEL`+"📣"）；body.push 同 ref 追加灰行「└已入详情」。

### 2.5 压缩衔接（零 LLM 确定性快照，PR5 待办）

回合超阈值后下次 turn_idle：从 `store.list()` ∪ `backend._runs−store` 机械拼状态快照，以单 user item 替换历史回执/通报 items，emit head.compact：

```json
{"t":"state.snapshot","ts":1759300123.4,"tasks":[
 {"no":1,"ref":"vh-<redacted>","status":"done","summary":"…","chars":1834},
 {"no":2,"ref":"vh-<redacted>","status":"running","elapsed_s":412}],
 "counts":{"done":1,"running":1}}
{"t":"head.compact","conv_id":"s-ab12cd34","before_chars":21340,"after_chars":512,"pinned":3,"reason":"threshold","ts":1759300123.4}
```

### 2.6 迁移（PR 粒度，每步独立验证+回退）

1. ✅ **PR1**（c82bed4）`rt_session_store.py`（SQLite）+`_store_bridge` 唯一写入面+body.push/body.get 帧；缺省 fulltext，store 静默积累。验证：派发后 body.get 命中；回退：零行为变化。
2. ✅ **PR2**（3a27293）回执瘦身（VOICE_RECEIPT_SLIM 缺省开）+doctrine 四段式+`_phase2` orch.failed 入观测面；no 序号。验证：播报一句话；回退：SLIM=0。
3. ✅ **PR3** split 交付：orch.done 载 `body`（cancel 路径无）；bridge body 优先/artifact 哨兵兜底，台账 body 落库；`_on_final` 按 VOICE_FINAL_MODE 分流（split 纯数据通报+store 降级）；`_phase2` lane-a task 消失与 `_phase2_dag` deadline 到期两处静默 return 补 orch.failed；tests 同步（backend/gateway）。回退：MODE=fulltext。
4. ✅ **PR4**（d262e21）工具面七件套（read_body/list_bodies 裁决签名实录见 §2.3）+cancel_run ref-only+接线（app_resources `voice_store`、`DEFAULT_VOICE_KINDS += body.push`）+live 探针断言刷新（瘦回执一句话、通报一句话）。验证：「查看任务2」可取正文；list_bodies 列索引。回退：纯代码回退——七件套缩回五件套（去两 handler+doctrine 两条款）、`voice_store` 出 app_resources、kinds 去 body.push、探针断言回旧稿；工具只读台账，无数据迁移。
5. **PR5** 详情页（Treeview+右栏只读正文）+notify 相+head.compact 快照；翻缺省 split。回退矩阵：两开关+doctrine 指回旧稿。

## 3. 开放问题（已全部裁决，2026-08-27 用户拍板）

1. credentials 退出模型上下文——**确认**。念出来本来就多余；对账迁观测面机器比对。
2. `no` 全局编号，口播直接叫「任务N」——**确认**。
3. orch.done 正文通道——artifact **停发**（PR1 落地）；正文经载荷 `body` 字段直载台账（PR3，无哨兵双写），观测面走 body.push 轻通知（chars≤4096 附 inline）。
4. 台账载体——**SQLite**（`~/.local/state/voice-gateway/store.db`，WAL；替原 JSONL 方案），LRU 500 条照做。
5. progress 主动播报——**不做**；状态留在 store（status 字段随时可查），用户问走 query_status。
