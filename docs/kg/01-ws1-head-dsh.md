# N1 · 车道A · dais ADE：head → DshBackend → DaisLane（方法级，已建成）

> 上游：`docs/plans/voice-orchestration-head-plan.md§3`（v2）· 索引：[00-INDEX.md](00-INDEX.md)
> **v2 归位**：原 WS1 全部产出 = 车道A（dais ADE）全链，M3 完成，live V5/V6 PASS。
> 车道内**双到达路径**（CLI 直达 / 孵化池执行桥 `executors/dais.js`）conformance 同结果——注意与"双 ADE 车道"（A=dais / B=orca）区分：车道B 见 plan §4 与 N6§4。
> 编排会话在 live 中由替身 `orchestrator_player`（session_vhlive）扮演 = **主对接 agent（liaison）替身**，其协议即 W5.3 真身的对外契约（N6§3.1 对照表）。

## 0. 对接总图（建成态）

```
head 工具调用（dispatch_intent / query_status / cancel_run / remain_silent）
   └─ DshBackend〔loc:examples/realtime-provider-poc/rt_dsh_backend.py:45〕
        ├─ 到达路径① CLI 直达：DaisLane〔loc:...rt_dsh_lane.py:32〕──subprocess──▶ dais orchestration
        ├─ 到达路径② 孵化池执行桥：A2aClient〔loc:...rt_a2a_client.py:34〕──HTTP──▶ a2a-profile-server
        │    └─ executors/dais.js〔loc:~/.dsh/plugins/a2a-profile-server/executors/dais.js:80〕──▶ 同一 dais 总线
        ├─ OrcaLane（车道B 入口，已建成）〔loc:...rt_orca_lane.py:41→OrcaLane〕──▶ orca-cli
        └─ EventBus 埋点 ──▶ N4 网关 orch.* 事件（WS4 汇总通道）
对端：dais 邮箱句柄 = DshBackend.orchestrator_handle:70 —— W5.3 后指向真 liaison（mailbox agent_liaison）
```

## 1. 模块清单（全部已落地，锚点实测）

### 1.1 DshBackend（两阶段应答核心）

`〔loc:examples/realtime-provider-poc/rt_dsh_backend.py:45→DshBackend〕`

```python
@dataclass
class DshDispatch:            # 〔loc:...rt_dsh_backend.py:34〕
    run_id: str | None        # dais run_<id>（路径①）或 A2A task_id t_<hex>（路径②）
    ref: str                  # [ref:vh-…] 关联键（三过滤用）
    credentials: list[str]    # make_credential 受理凭证

class DshBackend:
    lane_mode: str = "b"              # :68  "b"=CLI 直达（默认）/"a"=孵化池执行桥
    orchestrator_handle: str = ""     # :70  对端（liaison/替身）dais 句柄
    head_handle: str = "voice-head"   # :71  head 自身邮箱（phase-2 轮询目标）
    intent_seq: int                   # 三过滤基准 seq

    async def dispatch(self, raw_intent) -> str          # :80  阶段1受理（回执即返回）
    async def _fanout(self, raw_intent, ref) -> DshDispatch | None  # :100 按 lane_mode 分流
    async def _phase2(self, ref, dispatch) -> None       # :134 快照轮询 head 邮箱 + 三过滤
    async def query_status(self, run_id=None) -> str     # :179 check-status 聚合口语摘要
    async def cancel(self, ref_or_run) -> str            # :198 杀本地 phase-2 任务（权威取消点）
```

**关键实现事实**（live 实测钉死）：

- `_phase2` 对 `DaisLaneError`（跨进程 "database is locked" 瞬态）重试至 budget；`TimeoutError` → `orch.progress`（"仍在跑"）不报错；
- 三过滤：`[ref:]` 命中 ∧ `seq > intent_seq` ∧ `from == orchestrator_handle`（`〔loc:...rt_dsh_backend.py:157-160〕`）；
- cancel 语义（V6 验证）：本地播放打断**从不取消远端**；权威取消 = 杀本地 phase-2 task；`fail-dispatch` 仅当句柄为 `ctx_`（run/task id → exit=1 "not found"）；
- 终稿统一 `FINAL_PREFIX`（`〔loc:...rt_orchestrator.py:48〕`）前缀注入：路径②执行桥已剥 ref，前缀由 head 侧拼接（单一事实源）。

### 1.2 DaisLane（18 子命令 async 封装）

`〔loc:examples/realtime-provider-poc/rt_dsh_lane.py:32→DaisLane〕`

```python
class DaisLane:
    _lock: object                 # :47/:54  __post_init__ 建 asyncio.Lock——进程内单飞化所有 CLI 调用
    async def _run(self, *args, timeout_s=None) -> str          # :56  失败抛 DaisLaneError（含 stdout/stderr 截断）
    async def create_run(self, objective) -> str                # :87
    async def create_task(self, run_id, spec, deps=None) -> str # :95  deps → --dep
    async def start_worker(self, task_id, command=None) -> str  # :106 返回 ctx_<id>
    async def send_intent(self, run_id, handle, raw_intent, ref, *, from_id) -> int  # :119 --subject 必带
    async def send_reply(self, run_id, from_handle, to_handle, ref, body) -> None    # :134
    async def check_messages(self, handle, wait_s=None, *, after_seq=0, from_filter=None)  # :146 有界快照
    async def await_done(self, handle, ref, timeout_s=1800.0, *, after_seq=0, from_filter=None) -> str  # :168
    async def check_status(self, run_id=None) -> dict           # :217
    async def read_worker(self, dispatch_id, after=0, lines=40) -> tuple[str, int]    # :225
    async def fail_dispatch(self, dispatch_id, reason) -> None  # :254
    async def scan_wait_blocked(self, dispatch_id) -> str       # :258
    async def resolve_gate(self, gate_id, resolution) -> None   # :262
```

### 1.3 head 工具四件套

`〔loc:examples/realtime-provider-poc/rt_head_tools.py:74→dsh_head_tools〕` + `DSH_TOOLS_DOCTRINE`（:24）。docstring 即 schema（qwen `session.update` `_flatten_tool` 扁平化路径兼容）；实例注入 `params.app_resources["dsh_backend"]`。

### 1.4 到达路径②：A2aClient + 执行桥

- `〔loc:examples/realtime-provider-poc/rt_a2a_client.py:34→A2aClient〕`：send:57 / get:68 / cancel:76 / await_done:83 / incubate:100 / agent_card:115；退避复用 `ReconnectPolicy`；
- `〔loc:~/.dsh/plugins/a2a-profile-server/executors/dais.js:80→createDaisExecutor〕`：message/send → create-run → send-message（[ref:] 信封）→ 有界 `--timeout-ms` 轮询 → completed artifact（content = 去 ref 的 done body）；`parseMailbox` 与 python 解析器镜像（live 两行式 + kv + json 行型）。

## 2. dais 总线 live 语义（2026-08-23 实测钉死，全链依赖）

1. `check-messages <handle>` 列**收件人**邮箱；**读即消费**（一次性投递，未读持久）；
2. 无 flags 调用在空邮箱上**无限阻塞**；`--wait` 只捕捉窗口期内新到、**忽略已在邮箱的未读**；`--timeout-ms N`（无 `--wait`）才是**有界快照**——轮询循环一律用它 + 自管 sleep；
3. 并发 `--wait` 调用在总线锁上互饿：DaisLane 进程内 `asyncio.Lock` 单飞；跨进程轮询者（执行桥）留 sleep 间隙；
4. 常住 daemon 高 churn 下偶发楔死（futex 挂起持锁）：健康探测（`check-status` 探针，`〔loc:...live_v5_v6_dsh.py:118→bus_healthy〕`）+ 单次重试 + skip；
5. 跨进程 "database is locked" 瞬态：`_phase2` 重试 `DaisLaneError` 至 budget。

**语义映射**：

| DshBackend 语义 | dais 语义 | 备注 |
|---|---|---|
| 受理回执 | `create-run`→`create-task(--dep)`→`start-worker` 三连 | `ctx_<id>`；worker_done 块匹配自动结算，免轮询 |
| progress 尾读 | `read-worker --after <cursor>` | cursor 在 STDERR `cursor: <n>`（勿混流） |
| 卡死诊断 | `scan-wait-blocked` | 7 类阻塞模式 |
| 应答提示 | `answer --text/--enter/--interrupt` | 转 head 决策时用 |
| 人决策 | `create-gate`/`resolve-gate` | 桥到 WS4 `orch.gate` 事件 |
| 兜底 | runtime 死→`dais serve` headless（仅 pull 路径） | R2 对策 |

## 3. 编排对端契约（doctrine，W5.3 移交真身）

替身 `orchestrator_player`（`〔loc:...live_v5_v6_dsh.py:126〕`）已验证的行为契约 = liaison 模板输入（N6§1.2/N6§3.1）：

1. 收 `status` 消息（body 含 `[ref:]`）→ 判定意图类型（fan-out/查询/取消/闲聊）；
2. 处理：drain 自身邮箱 → 按 ref 经 `DaisLane.send_reply` 回信；
3. 两阶段：即时受理回执 `{status:accepted, run_id, ref, credentials}` → 终稿 `FINAL_PREFIX + done body`（凭证内嵌）；
4. 落位真身（VO-006 起）：经孵化池注入（N2§5 incubate RPC，role=liaison mailbox=agent_liaison；head 侧仅配置值 `orchestrator_handle:70`）。

## 4. 两阶段应答与打断语义（建成态契约）

| 阶段 | head 收到 | 播报 | 数据来源 |
|---|---|---|---|
| 1 受理 | `{"status":"accepted","run_id","ref","credentials":[【凭证…】]}` | "已受理" | `dispatch()` 返回 |
| 2 终稿 | `"Agent Final Message":\n\n<done body>` | 逐字读结果（凭证原样） | `_phase2()` 注入 head 上下文 |

打断/取消（V6 五验）：本地清播报队列；远端 run 不动；`cancel_run` → 杀本地 phase-2 + 迟到终稿丢弃；凭证权威 = `make_credential/extract_credentials`，head 只逐字回显（`DSH_TOOLS_DOCTRINE§After Tool Calls` 固化）。

## 5. 实施与验证序列（完成归档）

| 步 | 交付 | 状态/验证 |
|---|---|---|
| W1.1 探针 | `rt_probe_m0.py` 双 ADE 探针（dais 177ms / orca-ide 245ms） | ✅ `tests/test_m0_probe.py` |
| W1.2 车道封装 | DaisLane 全方法 + live 两行式解析 | ✅ `tests/test_rt_dsh_lane.py` 14 用例 |
| W1.3 DshBackend | 两阶段/取消/事件埋点；路径①②分流 | ✅ `tests/test_rt_dsh_backend.py` 5 用例 |
| W1.4 head 工具 | 四件套 + doctrine + schema 扁平化 | ✅ `tests/test_rt_head_tools.py` 7 用例 |
| W1.5 双到达路径 | A2aClient 路径 + 执行桥完整版 | ✅ `tests/test_rt_conformance.py` 2 用例（离线状态机 + live A/B 对拍，双路径 final 均 FINAL_PREFIX+同 body） |
| W1.6 live | `live_v5_v6_dsh.py` 真 GLM head + 真 dais 总线 | ✅ `tests/test_live_v5_v6.py`（V5 六验 / V6 五验；证据 `〔doc:docs/kg/evidence/m3-live-v5v6.md〕`） |

后续：~~车道B OrcaLane（plan §4）~~ ✅ VO-008/009；~~liaison 真身移交（N6§3）~~ ✅ VO-006；~~manager 群与 live V7（N6§4）~~ ✅ VO-007。M5 收口=VO-012。
