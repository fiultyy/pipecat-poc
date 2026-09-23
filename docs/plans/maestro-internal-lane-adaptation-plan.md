# maestro 内机制适配规划 — voice 链脱离 dais 接入 dsh 内部编排

状态: **定稿 v1**（自检 fan-out POC-S1..S5 五票全 end，关键断言本席独立复验通过，2026-09-21）
起草: 本仓常驻编排席（本席），授权 <orchestrator> 委派令 <dispatch-id>（两段式：全仓自检 + 本规划）
纪律: 零代码改动（本规划只落 docs）；证据分级：已验证（亲跑）> 复验（独立方取证后核对）> 采信（他方自报，均已升复验）。

## 0. 目标态

voice 链（rt_gateway / rt_voice_app / head 工具面）脱离 dais 外环，接入 dsh 内部编排三件套：**票板（dag-seed 编票）+ turn 注入 + 机械收口（report:turn-end 信封，已生效）**。

## 1. 已验证基线（F1..F9；编号避开提交 B-1..B-4 命名空间）

| # | 事实 | 级别 | 证据 |
|---|------|------|------|
| F1 | dais 死在底层二进制层：launcher `~/.local/bin/dais` 在 PATH(rc=0)，其 行77/81-82 exec `~/warpdotdev/dais-gui/target/release/dais` 不存在 → 裸 shell rc=127 | 已验证 | 本席+S5 双跑 `dais --version`；DaisLane 侧 rt_dsh_lane.py:56-83 → :77-80 确定性 raise DaisLaneError |
| F2 | rt_gateway 存活：pid 1453，user 级 systemd（父 1211 systemd --user），0.0.0.0:8765，15:30:14 起 | 已验证 | ss -tlnp + systemctl --user is-active=enabled/active（S5 取证，本席复核） |
| F3 | 四端口归属：8765=rt_gateway（本仓唯一自持口）；8790=a2a-profile-server；3080=dsh web（**0.0.0.0，违 ADR-012/HF-020，P1 安全**）；35451=pm-host-service。后三者是 dsh 侧服务 | 已验证 | ss + /proc cmdline（S1/S5 与本席三方一致） |
| F4 | driver 0.2.5 机械链 on：pid 34735，errors=0；本次五票 dispatched→running→end→notify 全链机械完成 | 已验证 | state/ledger-native-driver.status.json + driver log（`ticket POC-S*: running -> end` + `notify -> <seat>`） |
| F5 | report:turn-end 信封已生效（#228 销项）；worker 终稿经信封机械回收 | 复验 | #228 条目 + 本次五票终稿自 ~/.dsh/sessions 子会话转录独立提取核对 |
| F6 | t5 适配线（b5f495b）= **双形态 wire 层接管联络会话面**（4 方法：session.prompt/workspace.list/session.list/session.cancel）；任务面（18 处 self.lane.*）仍全程 DaisLane——**分层共存，非取代** | 复验 | S2 逐行盘点 + 本席 grep 复核（_wire_dsh_api 文本 3 处=1 定义+2 调用；"4 call sites"系方法口径） |
| F7 | B-4 测试态实跑复现：`pytest tests/test_rt_dsh_backend.py tests/test_rt_gateway.py -q` → **152 passed**（本席亲跑 55.75s；backend 单文件 45 passed 双 worker 独立跑吻合）——提交自述零漂移 | 已验证 | 本席 + S2 + S3 三方实跑 |
| F8 | ADR-012 正典文本在第三仓 **<internal-repo>/docs/ADR.md:127-140**（<internal-repo> 无 remote）：2026-08-30 用户三项裁决=飞书 defer／内网 IP 测试+公网预留（去 --loopback-only，0.0.0.0+token，token 缺失 rc=2）／Android 门槛=pm-web 全通；本仓与 ~/.dsh/maestro/adr/ 两面确无该文件 | 已验证 | 本席亲读 ADR.md:127-140（S4 重构决策链七环各有证据位） |
| F9 | voice-gateway 守护=user 级 systemd 单元，**enabled+active**（08-30 起），Restart=on-failure，日志走 user journal（最后活动 15:46:49，此后闲置）；unit 注释"不 enable"与现势漂移（S4-D2） | 复验 | systemctl --user is-enabled/is-active（本席）+ S4/S5 取证 |

**对初稿的两处实证修正**（证据纪律留痕）：初稿曾断"生产面不 import DaisLane"——**不成立**，rt_gateway.py:105 import、:2888 构造、:2897 全量接线（grep 管道截断所致误判，S1/S5 双双纠正）；初稿曾断"systemd 无 unit"——系 system/user 作用域混淆，实为 user 级 enabled+active（S4-D7/S5 纠正）。

## P0 — 断臂止血（死点影响收敛为单点后止血）

**影响面结论**（S5 钉死，本席复验无 catch 路径）：
- 网关启动/常驻/查询/取消/台账/gate.resolve 均已天然降级可用（构造 DaisLane 零子进程；cancel 本地权威；gate.resolve 接住转错误帧）。
- **唯一硬失败用户路径 = dispatch_intent / dispatch_plan 派发链 phase-1**：rt_head_tools.py:132-160（两 tool 无 try/except）→ rt_dsh_backend.py:807-814 lane.create_run 等确定性抛 DaisLaneError → 用户派发意图直接异常、无受理回执。

**改动点**（文件:行级，供执行票引用）：
1. rt_dsh_backend.py:807-814（dispatch phase-1 入口）统一拦 DaisLaneError → 结构化"编排车道不可用"回执（保守：单点拦截覆盖 liaison/fanout/plan 三分支）。
2. rt_head_tools.py:141/:159 兜底 except → 工具契约内错误回执（防框架层裸异常，语音播报形态未验）。
3. rt_dsh_lane.py:5-12 module docstring 加死点声明（底层二进制缺失、rc=127 语义），防新代码接入。
4. live 驱动无需动：bus_healthy/rc≠0 → `[SKIP] dais bus unresponsive` 机制已在（S5 清单）。

**验收标准**（可执行判据）：
- `pytest tests/test_rt_dsh_backend.py tests/test_rt_gateway.py -q` 保持 152+ 全绿；新增降级测试≥1（假死 lane 下 dispatch 返回结构化回执而非异常）；
- `grep -c "except DaisLaneError" examples/realtime-provider-poc/rt_head_tools.py` ≥1；
- 手工冒烟：真网关 dispatch_intent 一次，收到结构化不可用回执（F1 死点现势下可现场复现前置态）。

**风险**：拦截层不得吞非 DaisLaneError 异常；回执形状须过 head 工具 result_callback 契约（rt_head_tools.py:140 实调形）。

**依赖上游裁决**：无硬依赖（止血≠方案，D2 拍板前即可执行）。

## P1 — 内环接入（票板 + turn 注入 + 机械收口）

**改动点**（文件:行级）：
1. **voice→票板入口**：rt_dsh_backend.py 任务面新建 maestro lane 适配（与 _wire_dsh_api/dsh_api 同层，:167-231 旁）：phase-1 从 `self.lane.create_run/create_task`（:809/:814→:920,924,926）切到 `bin/dag-seed` 子进程编票（编票唯一授权前端=dag-seed，零 HTTP 越面）；phase-2 终稿回流走信封 report:turn-end（F5 已生效）+ 既有两阶段应答面（rt_dsh_backend docstring :5-20 "Agent Final Message:" 终稿再注入）。
2. **对接席复活**：~/.dsh/profiles/incubated/vh-liaison/AGENTS.md 重投影（现池卡教停泊旧通道；改教内部 lane 正统：dag-seed 编票/票态/UP-NOTIFY/信封收口）；复活路径=`dag-seed --mode spawn --persona vh-liaison`（池内在位已验）。四席 09-03 起静默、fleet.json 无 vh 条目（#228-A2 + 本席验）。
3. **driver turnWatch 单槽缺陷**（index.js:795/:613，#228-P1）：voice 连发节奏正中画像（同席重叠 turn 票静默孤儿化）。修复，或按 D4 以"一席一票硬闸 + runId 并发键"规避。

**验收标准**（可执行判据）：
- e2e：voice 意图 → dag-seed 编票 → spawn → `ticket <REF>: running -> end`（driver log）→ `notify <REF> end`（回流本席组）→ 终稿经两阶段应答注入 WS event；全程零 dais 调用（`grep -rn "self\.lane\." rt_dsh_backend.py` 计数下降为仅保留 legacy 分支）；
- 连发两票不孤儿化（turnWatch 缺陷回归判据，方向随 D4）；
- 票板面既有 gates 回归：rt_gate_tk002/003/004 + tests 90 用例全绿。

**风险**：spawn 子会话不在宿主注册表时须 seatless（--seat - --notify-seat -）；turn 注入与 voice 连发竞态；人格池改版生效时机=下一票（0.2.5 语义）。

**依赖上游裁决**：D1（voice 门=one-off probe vs 正典入口）、D2（vh 迁内部 lane——本规划即其执行案）、D4（turn 票并发闸）。

## P2 — 观测与治理

**改动点**：
1. rt_gateway 票板只读投影：仿 PM 面模式（rt_gateway.py:2290-2368 端口发现+透传同构）挂 `ledger ticket list --json` 投影端点；TK-002 票板 tab 已有消费面。
2. #228 P2 群治理：cb-send 'end' 枚举漂移／通知幂等+死信无界／pm-host-service 无 Restart=always／notify fail-open／refs.notify 浅合并等。
3. **3080 绑 0.0.0.0 违 ADR-012/HF-020（P1 安全，F3）**：收口方向随 D3 裁决（回归发生时点无记录，S4-③-D3）。
4. ADR-012 归位：正典孤悬无 remote 的 <internal-repo>——落 ~/.dsh/maestro/adr/ 或仓 docs 待裁（S4-③-D6）。
5. changelog 结构性失明注记：towncrier issue=上游 PR 号，本地裁决记录面=commit msg+<internal-repo> ADR.md；192 条 fragment 积压未发布（S4-②）。

**验收标准**：投影端点与 `ledger ticket list --json` 逐票一致；`bridge/stall.log` 无新增 400；:3080 绑定面与 ADR-012 口径一致（D3 后）。

**依赖上游裁决**：D3（0.0.0.0 有意否）、D5（fleet.json 维护权）、D6（UP-NOTIFY 有界账面读终态性）。

## 附 A. 上游裁决依赖矩阵

| 裁决 | 内容 | 阻塞相位 |
|---|---|---|
| D1 | voice 门形态（one-off probe vs 正典入口） | P1 |
| D2 | vh 迁内部 lane vs 复活 dais（grill 门） | P1（P0 不等） |
| D3 | 3080 绑 0.0.0.0 有意否 | P2（安全级，建议提前） |
| D4 | turn 票一席一票硬闸 vs runId 并发 | P1 |
| D5 | fleet.json 维护权 | P2 |
| D6 | subagent UP-NOTIFY 有界账面读是否终态 | P2 |

## 附 B. 自检 fan-out 台账（本规划证据基座）

五票 POC-S1..S5（spawn，cwd=本仓，票嵌逐字自证+基线+阈值；grill 留痕：只读零改动/无高危/内部 lane 正统）：
- POC-S1 组件面：rt_* 29 文件职责/调用图（静态+运行时逐边 path:line）/四端口归属——终稿已收，关键断言（端口三方一致、rt_gateway 接线 DaisLane）复验通过。
- POC-S2 t5 线：b5f495b 逐文件要点、B-1..B-4 四态重建（dot/slash/鉴权/测试）、共存分层判定、8 条缺口各配判据——`_wire_dsh_api` 计数与 152 实跑双双复验吻合。
- POC-S3 门面：11 gate 全谱+跑法、POC tests 90 用例、CI 面=11 workflows 但 POC 零接入（testpaths=tests）、codecov 不含 POC——45 passed 复验吻合。
- POC-S4 裁决面：ADR-012 决策链七环重构（正典定位 <internal-repo>/docs/ADR.md:127-140）、changelog 192 积压、漂移 7 条（行为性=D2 enable、D3 3080）——正典文本/user 单元态本席亲验吻合。
- POC-S5 运行态：死点机制钉死（launcher 在/二进制缺/rc=127→DaisLaneError 确定性）、dais 调用点全清单+死点行为、唯一硬失败=派发 phase-1、journal 取证——无 catch 路径本席亲读吻合。
- 票间对账：S3 报"152 不可复现"系口径差，S2 组合口径（backend+gateway）经本席亲跑复现——销项。
- 遗留外化（未验项）：dispatch 异常是否被 pipecat 框架层转语音播报（S5，需真派发）；A2aClient 树内零构造点（S1，启用面在仓外）；rt_reconnect 孤立件归属（S1）；`rt_dsh_backend.py:583` 恒 False 守卫（S2 发现的上游遗留，建议独立票）。
