# 13 · 回调一分为二分流协议（设计稿，未实施）

现状：liaison 终稿 → `DshBackend._phase2` → `on_final(ref, final)` →
`rt_gateway.build_realtime_head._on_final` 全文注入 head。痛点：realtime
无缓存、每回合全量计费 → 上下文膨胀；doctrine 逐字念 ref/凭证占语音时间
且 flash 概率丢字；正文在语音端说完即逝，客户端无详情承接。

## 1. 分流原则

回调到达后不再整块进 head：

- **正文 → store**（gateway 进程内单例；本文只约定写入面，不设计内部）；
- **head 只收「状态通报」**：≤60 字 user item（序号 + 状态 + 一句话摘要
  +「详情已存，可说查看」）；
- **观测面推 `body.stored`** 正文事件（新增 topic kind）。

写入面唯一：gateway 对自有 bus 的订阅器（`orch.dispatch`→accepted、
`orch.done`→final、`orch.failed`→failed、可选 `orch.progress`→progress）。
backend 不感知 store。语音寻址用会话内序号 `no`（"任务2"）；ref/credentials
只留在 store 与事件里，**永不上语音**——凭证 canary 的校验职责移交观测面
机器比对（`extract_credentials` 对 `body.stored`/store 记录比对）。

通报注入复用 `_inject_final_when_idle` 全部机制（`_final_inject_lock`、
`turn_idle` 排队、`_live_heads` 单活、`pending` 补投、`_flush_pending`），
仅载荷由全文换成通报文本；注入频度不变（每派发 final/failed 各至多一条），
与 turn_idle 注入锁、单活 head、观察面 topic 机制天然兼容
（`body.stored` 入 `TOPIC_KINDS` → `SUBSCRIBABLE_KINDS`，observe 缺省
全订阅即得；语音会话 `DEFAULT_VOICE_KINDS` 不含它，零改动零刷屏）。

## 2. 状态通报粒度与频率

| 状态 | 进 head？ | 形态 |
|---|---|---|
| accepted | 否 | 回执即工具返回，新增 `speak` 字段，doctrine 只念 speak |
| progress | 默认否 | 只进 `orch.progress` 观测面（已有慢客户端合并）。可选心跳 `VOICE_NOTICE_PROGRESS_S`，默认关；开则建议 ≥600s、每派发同时至多一条、仅 turn_idle 时注入 |
| final | 一条 | `任务2完成：{summary≤40字}。详情已存，可说「查看任务2」` |
| failed | 一条 | `任务2未完成：{原因≤20字}。可说「重试」或「查状态」` |

progress 零注入的理由：realtime 每回合全量计费，TailReader 2s/条、DAG
每步 2+ 条，注入即 user item + response.create，一分钟可塞几十条，刷屏
且每条都触发播报。用户主动问进度走既有 `query_status`。

## 3. head 工具面（五件套 → 七件套）

`read_body(ref_or_no, max_chars=4000, from_tail=False)` /
`list_bodies()`。用户说「看看那个报告」→ head 先 `list_bodies` 定位
（`latest_final_no`）→ `read_body` 取正文；正文只在用户主动要时进上下文，
且默认截断。handler 从 `params.app_resources["body_store"]` 解析
（与 `dsh_backend` 同一 wiring 模式）。

## 4. 人格与回退

- 人格：`VOICE_HEAD_DOCTRINE`（`DoctrineSource`）已可外置整个 doctrine，
  零新开关；内置 `DSH_TOOLS_DOCTRINE` 默认改为「任务助手精炼表述」：
  只念 speak/通报，不逐字念 ref/凭证，不播 progress。
- 回退：`VOICE_FINAL_FULLTEXT=1` → `build_realtime_head` 恢复旧
  `backend.on_final` 全文注入链（含 pending 全文缓冲）；未设/0 = 新协议。
  生效于下一次 head 构建（客户端重连），无需重启 gateway。

## 5. maestro 对照

抄：信封/货物分离（通报=信封，store=货舱）；ref 权威关联键（信封字段
优先，不靠正文前缀）；结果先行落账（store.put 先于注入，通报可重发而
正文不丢）；状态动词小集合（accepted/progress/final/failed ≈
ack/done 面）；无消费者缓冲（`_live_heads["pending"]` ≈ inbox 语义）。

不抄：逐字凭证转述（maestro 正文即传输通道才需逐字；这里 store 是通道，
语音只是信号面）；跨进程 HTTP/文件桥 + 游标（同进程内存无投递损耗）；
body 内 `"[ref:] "` 前缀双写（legacy 兼容包袱）。

## 6. 协议 JSON 例

### 6.1 store 写入约定（幂等，key=ref）

```json
{
  "ref": "vh-<redacted>",
  "no": 2,
  "run_id": "run-9f00",
  "status": "final",
  "summary": "沪深300今日跌1.2%，成交额缩量…",
  "body": "\"Agent Final Message\":\n\n全文…",
  "chars": 1834,
  "credentials": ["【凭证R-VH1A2B3C4D】"],
  "ts": 1759300000.0,
  "updated_ts": 1759300123.4
}
```

### 6.2 `body.stored` 观测面事件

```json
{"t": "body.stored", "ref": "vh-<redacted>", "no": 2, "status": "final",
 "summary": "沪深300今日跌1.2%…", "chars": 1834, "body": "全文…",
 "ts": 1759300123.4}
```

### 6.3 注入 head 的通报 user item（input_text 文本，≤60 字）

```json
{"text": "任务2完成：沪深300今日跌1.2%。详情已存，可说「查看任务2」"}
{"text": "任务2未完成：对接人超时未回。可说「重试」或「查状态」"}
```

### 6.4 回执（工具返回，新增 `speak`/`no`；旧字段原样保留兼容）

```json
{"status": "accepted", "run_id": "run-9f00", "ref": "vh-<redacted>",
 "no": 2, "speak": "已受理为任务2，完成后播报",
 "credentials": ["【凭证R-VH1A2B3C4D】"],
 "note": "已转对接人（新回合），完成后播报"}
```

### 6.5 `orch.failed`（新增事件 kind）

```json
{"t": "orch.failed", "ref": "vh-<redacted>",
 "reason": "对接人 30 分钟未回终稿", "ts": 1759301800.0}
```

### 6.6 `read_body` 返回

```json
{"ref": "vh-<redacted>", "no": 2, "status": "final", "chars": 1834,
 "returned": 4000, "truncated": true, "body": "正文（可能截断）"}
```

### 6.7 `list_bodies` 返回

```json
{"bodies": [{"no": 2, "ref": "vh-<redacted>", "status": "final",
             "summary": "沪深300今日跌1.2%…", "chars": 1834,
             "ts": 1759300123.4}],
 "latest_final_no": 2}
```

## 7. 工具定义（风格对齐 rt_head_tools.py 五件套）

```python
async def read_body_tool(params, ref_or_no: str,
                         max_chars: int = 4000, from_tail: bool = False):
    """读取一份已存编排正文。仅当用户明确要看某个任务的结果/报告/详情时调用。

    Args:
        ref_or_no: 任务序号（如 "2" 或 "任务2"）或受理回执里的 ref（vh-…）。
        max_chars: 最多返回的正文字符数，超出截断；保护上下文，非必要勿调大。
        from_tail: True 时从正文结尾取起（结论常在尾部）；缺省从头取。
    """
    store = params.app_resources["body_store"]
    await params.result_callback(await store.read(ref_or_no, max_chars, from_tail))


async def list_bodies_tool(params):
    """列出已存编排正文清单（序号、状态、一句话摘要）。用户问「刚才那个报告」「有哪些结果」时先调此工具定位，再按需 read_body；不要未问先读全文。"""
    store = params.app_resources["body_store"]
    await params.result_callback(await store.list())
```

## 8. 改造点清单（文件:函数）

rt_gateway.py

- `build_realtime_head._on_final` — 旧链路整体降级为
  `VOICE_FINAL_FULLTEXT=1` 分支（含 pending 全文缓冲）。
- `build_realtime_head` — 新增 `_announce_when_idle(no, ref, status,
  summary)`：复用 `_final_inject_lock`/`_live_heads["pending"]`/
  `_flush_pending`，载荷为通报文本；`app_resources` 增加
  `"body_store"`；新模式不接 `backend.on_final`（`_phase2` 已有
  `if self.on_final` 空值护栏）。
- 模块级 — `_body_store` 单例 + `BodyStore` 最小面
  （`put/get/read/list`，进程内、有界）；`TOPIC_KINDS` 追加
  `"body.stored"`（`SUBSCRIBABLE_KINDS` 自动含）。
- `main._bridge` — 扩展为统一写入面：`orch.dispatch`→store.put(accepted,
  no 分配)；`orch.done`→store.put(final)+emit `body.stored`+通报注入
  （含 cancel 的 `artifact:"(已取消)"` 落账）；`orch.failed`→store.put
  (failed)+通报注入；可选 `orch.progress`→store 进度摘要。
- `_flush_pending` — 补投条目由全文改通报（机制不变）。

rt_head_tools.py

- 新增 `read_body_tool` / `list_bodies_tool`（§7）。
- `dsh_head_tools()` — 返回七件套。
- `DSH_TOOLS_DOCTRINE` — "After Tool Calls" 段重写：只念 `speak`；
  终稿/失败通报照念；详情按需 `read_body`；progress 不播；人格默认
  「任务助手精炼表述」。

rt_dsh_backend.py

- `_liaison_roundtrip` — receipt 增加 `"no"`（由 gateway 注入序号或
  backend 自增）与 `"speak"`；`dispatch`/`dispatch_plan`/`query_status`/
  `cancel` 共用此出口，自动获得。
- `_phase2` — 两处异常 return（`TimeoutError` still-running、lane errors
  until deadline）改 emit `orch.failed {ref, reason}`；成功路径不动。
- `DshBackend` — 新字段 `on_failed: Callable[[str, str], Awaitable] |
  None = None`（可选直连钩子，与 bus 事件二选一即可）。

rt_voice_app.py（可选，正文落点承接）

- 观测页新增「详情」栏：订阅 `body.stored`（observe 缺省全订阅已自动
  收到）渲染 summary+body；`_on_observe_event` 增加一个 elif 分支。
