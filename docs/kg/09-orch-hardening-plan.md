# Voice-Orchestration KG · 09 W6 编排链加固方案（orch-hardening）

> 版本 v1 · 2026-08-23 · 来源：`docs/kg/evidence/handoff-w6-hardening.md`（使命书，主题 A/B 为用户裁决级输入）+ `docs/kg/08-defects-ledger.md`（N8）+ `docs/kg/evidence/ledger-carryover-round6.md`（round 14–16 事故实录）
> 性质：**只规划不执行**。票编号 OF-001..OF-010，与 VO 系列隔离；票板=本文件 §8。
> 依据链：OF 票 → 本文件 §包（验收编号）→ 08-defects-ledger（D-xx）→ evidence/ledger（事故证据）。
> 格式先例：`docs/plans/impl-specs.md`（工作包表）；`docs/tickets.md`（v4 票制）；`docs/plans/dispatch-plan.md`（Lane+合并五步）。

## 0. 诊断摘要（为什么是这九张票）

两条结构性主题共享同一底层真相：**编排链的信任与状态都活在"君子协定"层**——

- 主题 A（多主无契约软隔离）：插件 router 层有 scope+journal 契约（VO-004 建成），但脚下的 `session-send` 安全模型=知道 sessionId 即可注入：`from` 自报、无 msgid、无凭证、steer fire-and-forget。契约建在了上面一层，下面敞开。
- 主题 B（长任务无高级编排）：波次状态活在 LLM 上下文（已 compact 一次、longtask 服务端坏过一次）；编排者回合制=回合间隙无滴答（VO-007 挂死 2h 由用户发现）；relay 固定寿命先于长票到期；编排者之上无升级路径。

治法：把**身份（谁有权发）、状态（票/检查点）、守望（watchd）**三件事从协定层下沉到机械层。十票按此分三组：A 组 OF-001..004（会话面身份与契约）、B 组 OF-005..007 + OF-010（数据面状态 + 与 DSH longtask 面的绑定）、C 组 OF-008..009（缺陷直修：dais 守卫 + live 纪律）。

## 1. 全局验证门（OF 系列通用，OG 门）

- **OG1 兼容不破**：`session-send`/`fleet-touch`/`ledger` 既有调用方（relay、编排者、dispatch-ticket、agent doctrine）**零改动通过**——新能力全部以可选参数/新增键落地，旧参数行为逐字节不变。
- **OG2 冒烟自带**：每个 bin 改动附 selftest 或 `--help` 干跑证据；涉及网络/文件副作用的带 temp 域测试。
- **OG3 红线继承**：不改 dais 二进制仓源码（OF-008 只动 wrapper+新增脚本）；不 push；VO 在飞文件（`tests/test_live_v7.py`、`examples/realtime-provider-poc/live_v5_v6_dsh.py`）在合入前不碰——OF-009 tests 部分排 VO-012 后。
- **OG4 台账回写**：每票验收最后一步=在 `docs/kg/08-defects-ledger.md` 对应 D-xx 标 ✅/状态更新，与本文件验收编号勾稽；证据落 `docs/kg/evidence/OF-00x-report.md`。
- **OG5 信封只增不改**：DSHMSG 新键（msgid/ts 等）只增；老消费者忽略未知键不拒收；信封保持单行 JSON（G5 同源纪律）。

## 2. Wave 0 前置（非票，OF 派发前一次性）

| 步 | 动作 | 说明 |
|---|---|---|
| 0.1 | **编排域版本化**：`git init ~/.dsh/maestro`（track：`bin/`、`*.md` 约定文档、`templates/`；ignore：`fleet.json`、`ledger.db`、`state/`、`bridge/`、`probe/`、`*.log`）+ 同策 `git init ~/.dsh/plugins/a2a-profile-server` 与 `~/.dsh/plugins/host-callback-bridge` | D-06 收口并推广到整个编排域；解锁 OF 票 worktree 并行（Lane 化前提，见 §9） |
| 0.2 | 基线记录：现行全量回归绿 + `session-send <self> <self> ping - 'wave0-baseline'` 冒烟 | OG1 的对照基线 |
| 0.3 | 票板生效：本文件 §8 即 OF 票板；`~/.dsh/maestro/tickets.md` 追加 OF 段镜像（OF-005 落地后转渲染视图） | 编排者验收口径统一 |

---

## 3. 主题 A 工作包：多主责任 agent 的契约软隔离

### OF-001 · DSHMSG 信封 v2：msgid + 收方去重（A.1，顺带 D-10）

| 项 | 内容 |
|---|---|
| 依据 | 使命书 §2 主题 A 规划点 1；D-10；round 15 relay 回声实录 |
| 范围 | 〔loc:~/.dsh/maestro/bin/session-send:46→envelope〕增 `msgid`（uuid4）与 `ts`（epoch ms）两键：无参调用自动生成（OG1），`--msgid <id>` 透传支持重发保号；新增 〔new:~/.dsh/maestro/bin/msg-dedup〕——收方回合首动作助手（读 DSHMSG 行或 msgid 参数，查去重窗口文件，新→记录并 exit 0，重复→exit 3）；窗口=〔new:~/.dsh/maestro/state/dedup/<toCode>.jsonl〕（60s 内 (from,msgid) 去重，>1000 行截半 GC）。relay 契约更新：事件回报后**原子推进** `reports.base`/`git.base`（temp+rename），同事件二次落地零回报（D-10 治理） |
| 验收 | ①零参调用兼容：输出/退出码与现行一致，信封多 msgid+ts 两键；②同 msgid 重发→`msg-dedup` exit 3，收方 doctrine 演示丢弃一次重复 steer；③窗口文件 GC 生效（灌 1001 行合成数据验证截半）；④relay mock 场景：base 推进后同事件重放零回报；⑤信封仍单行 JSON，老消费者（现行编排者/relay 回合）解析不拒收（OG5） |
| 验证形式 | 自动=bin selftest（合成 fleet/temp state）；live=本会话 ping 自发自收 + 一次重发去重演示 |
| 量·依赖 | 小–中 · 无（**纯离线可即开**） |
| 文件域 | `bin-send`：session-send、msg-dedup(new)、state/dedup/；`doc-maestro`：relay 契约段 |

### OF-002 · fleet 属主租约 + steer 闸（A.2，顺带 D-07）

| 项 | 内容 |
|---|---|
| 依据 | 使命书 §2 主题 A 规划点 2；D-07；round 14 双 pytest 踩邮箱实录；"relay 可 steer 任何 agent"无 ACL 观察 |
| 范围 | 〔loc:~/.dsh/maestro/bin/fleet-touch〕扩三动作：`claim <code> --owner <sid> --ttl-min N`（写 `owner`/`leaseExpiresAt`/`heartbeatAt`）、`heartbeat <code>`（续期）、`release <code>`；〔loc:~/.dsh/maestro/bin/session-send:45→resolve〕后加**控制面闸**：type∈{steer} 时查 to 条目——owner 有效且≠from → 拒绝（exit 4）+ 冲突行落 〔new:~/.dsh/maestro/fleet-conflicts.jsonl〕（msgid+from+to+ts）；owner=from/无主/已过期 → 放行+journal。sweep 动作：`lastSeenAt`/`heartbeatAt` 陈旧>N 天的 active → `retired`（D-07 存量清理，`--dry-run` 默认） |
| 验收 | ①claim/heartbeat/release 原子读写（temp+rename，并发双写不撕裂）；②owner 有效时非 owner steer 被拒且 conflict journal 落行；③owner 本人/无主/过期三种放行；④sweep dry-run 列表 + `--apply` 后条目 retired；⑤任务型消息（done/ask/ack/report 类）不受闸影响（回归：现行全类型走一遍） |
| 验证形式 | 自动=bin selftest（temp fleet 副本）；live=双会话（本会话+relay 或临时会话）steer 闸正反各一次 |
| 量·依赖 | 中 · OF-001（同文件 session-send 串行；journal 行复用 msgid） |
| 文件域 | `bin-send`：session-send；`bin-fleet`：fleet-touch、fleet-conflicts.jsonl(new)、fleet.json schema（+3 键） |

### OF-003 · 控制消息 two-phase + 可达性文档收口（A.3 + D-08 尾巴）

| 项 | 内容 |
|---|---|
| 依据 | 使命书 §2 主题 A 规划点 3；D-08 建议项（"编排者可达性=直投 sessionId"入 skill 文档）；steer fire-and-forget 观察 |
| 范围 | 契约定义并落三处：**steer 发出后，收方回合首动作 = `session-send <self> <from> ack <ref> 'steer-accepted'`（已读将执行）或 `nack <ref> 'busy:queued'`（正忙，入列下回合）**——类型复用现有 ack/nack，不新增。落点：①〔doc:~/.dsh/maestro/orch-loop.md§七步〕嵌"控制消息两段式"节；②〔loc:~/.dsh/maestro/bin/dispatch-ticket:2→usage 模板〕生成的派发 prompt 头部嵌契约；③〔loc:~/.agents/skills/maestro-bridge/SKILL.md〕增"编排者可达性=直投完整 sessionId（session-send 推唤醒）"为主叙事，cb-send 降级为文件桥备胎（D-08 尾巴收口） |
| 验收 | ①契约文本三处齐（grep 可验）；②一次真实 steer 往返：收方 ack 回达发送方（live 冒烟）；③nack busy 语义文档钉死=消息保留、下回合首处理，不丢不重（与 OF-001 msgid 勾稽）；④无契约旧 peer：发送方零阻塞（无 ack 不等待，沿用"机械核查为仲裁"原则，文档明示） |
| 验证形式 | 文本核对 + live 冒烟 1 次 |
| 量·依赖 | 小 · OF-002（闸语义先定） |
| 文件域 | `doc-maestro`：orch-loop.md、dispatch-ticket、maestro-bridge SKILL.md |

### OF-004 · loopback 能力凭证（A.4，中期）

| 项 | 内容 |
|---|---|
| 依据 | 使命书 §2 主题 A 规划点 4；"`/api/session.prompt` 不验证 from"底层真相 |
| 范围 | 〔loc:~/.dsh/maestro/bin/session-spawn〕孵化时生成 per-session token（random 32B），fleet 条目存 **sha256 摘要**（不存明文）；〔loc:~/.dsh/maestro/bin/session-send〕自动附 `X-Dsh-Token` 头（从 fleet 条目关联的 `~/.dsh/maestro/state/tokens/<code>` 读明文）；〔loc:~/.dsh/plugins/host-callback-bridge/loopback-sink.js:76→POST /api/session.prompt〕增**可选校验**（配置开关默认关，灰度）：开后——无 token/错 token/token 与 sessionId 不绑定 → 401 + journal 拒绝行 |
| 验收 | ①开关关：现行行为逐字节一致（回归：全类型消息+旧调用方）；②开关开：持 token 直投通；伪造 from+他人 token（跨会话）拒；无 token 拒；③token 不回显：信封/日志/journal 只记摘要前 8 字符；④fleet schema 版本化（rev 字段），老条目无 token 视为灰度期放行并 WARN 一次（不破坏存量 relay/编排者） |
| 验证形式 | 自动=插件侧单测（mock 请求）；live=灰度开+本会话/relay 双路冒烟 |
| 量·依赖 | 中–大 · OF-003（信封与契约稳定后再动底层）；**中期票，不阻塞其余 OF** |
| 文件域 | `bin-send`：session-send、session-spawn、state/tokens/；`plugin-host`：loopback-sink.js |

---

## 4. 主题 B 工作包：长时任务的高级编排能力

### OF-005 · tickets DAG 数据化（B.1）

| 项 | 内容 |
|---|---|
| 依据 | 使命书 §2 主题 B 规划点 1；longtask 服务端损坏实录（ledger 头部）；波次状态在 LLM 上下文易失 |
| 范围 | 〔loc:~/.dsh/maestro/bin/ledger:2→usage〕增 ticket 子命令族：`ticket add/state/dep/lease/render`；数据=〔loc:~/.dsh/maestro/ledger.db〕新表 `tickets`（ticket_id, title, state, deps, lease_owner, refs(dispatch/run/worktree/session), outcome, updated_at）+ events append-only（与现有 projects/nodes/events 同模式，flock 串行写）；状态机 `dispatched→running→blocked→done→merged`（终态另含 `rejected`/`rolled-back`），非法迁移拒绝；`render` 输出 v4 兼容 markdown → 覆写 〔loc:~/.dsh/maestro/tickets.md〕（头部加 `autogenerated` 标记，人工编辑段用 `<!-- hand -->` 保留区）。**裁决记录**：repo 侧 `docs/tickets.md` 保持手写项目工件不入库（受众与版本化语义不同） |
| 验收 | ①状态机全合法迁移可达 + 非法迁移（如 dispatched→merged）拒绝；②deps 检查：前置未 done 时 `state running` 拒绝（`--force` 旁路并 journal）；③render 输出含 v4 字段（状态符号 ☐◐☑、路径、验证目标指针）且幂等；④并发双写 flock 安全（两进程同刻 add 不丢行）；⑤回溯查询：`ticket list --state blocked --since 24h` SQL 可答 |
| 验证形式 | 自动=temp db 单测（状态机/并发/render 三面） |
| 量·依赖 | 中 · 无（**纯离线可即开，与 A 组并行**） |
| 文件域 | `bin-ledger`：ledger、ledger.db、tickets.md |

### OF-006 · relay 泛化为事件守护 event-watchd（B.2 + D-09）

| 项 | 内容 |
|---|---|
| 依据 | 使命书 §2 主题 B 规划点 2；D-09（VO-007 pytest 挂死 2h 零告警 + 60 轮寿命手工 re-arm）；round 15 relay 模式先例 |
| 范围 | 新增 〔new:~/.dsh/maestro/bin/event-watchd〕——**回合制外常驻进程**（nohup 起，非 agent 回合），声明式配置（`--config watch.json`）：四类看守面 ①文件落地（glob→事件，现行 relay 面）②进程活性（agent 进程 CPU 阈值 + 日志 mtime 陈旧度>N 分钟，合成复现 VO-007 式 ep_poll 0.1% 场景）③SLA 超时（消费 OF-005 tickets：dispatched/running 超 ttl → 事件）④租约到期（消费 OF-002 fleet：owner lease 过期 → 事件）。触发→`session-send` DSHMSG 至 owner（携 msgid）；**自续期**：存在在飞票（tickets 非终态）时寿命自动延长（D-09 ②）；**升级路径钉死**：owner 租约也陈旧 → 追加 〔new:~/.dsh/maestro/state/alerts.log〕（用户可见告警面），不做递归监督（§6 非目标）。附 relay→watchd 迁移指南（orch-loop.md 增节） |
| 验收 | ①四类面各一合成场景触发且 DSHMSG 达 owner（msgid 可去重）；②挂死判定：假进程低 CPU + 旧 mtime 双条件命中（单条件不误报）；③在飞票存在时 watchd 自身寿命续期；④单实例锁（flock）+ SIGTERM 优雅退出无残留；⑤owner 失联场景 alerts.log 落行 |
| 验证形式 | 自动=合成进程/文件/mock fleet 单测；live=值守一轮真票 |
| 量·依赖 | 中–大 · OF-002（租约面）；SLA 面=OF-005（可分期：①②先落，③④随后） |
| 文件域 | `bin-watchd`：event-watchd(new)、state/alerts.log(new)、watch 配置；`doc-maestro`：迁移指南节 |

### OF-007 · wave 检查点机器可读（B.3）

| 项 | 内容 |
|---|---|
| 依据 | 使命书 §2 主题 B 规划点 3；上下文 compact 一次 + longtask 损坏一次的恢复成本实录 |
| 范围 | 新增 〔new:~/.dsh/maestro/bin/wave-checkpoint〕：追加单行 JSON 到 〔new:~/.dsh/maestro/state/wave-checkpoints.jsonl〕——`{round, ts, wave, tickets:[{id,state}], git:{head}, env:{dais,orca}, notes}`（原子追加，fsync）；〔doc:~/.dsh/maestro/orch-loop.md§七步〕嵌"回合收尾追加检查点"一步；断点续传用法：新编排者会话 `wave-checkpoint --tail 1` 重建波次视图（jq 演示入文档） |
| 验收 | ①追加原子（kill -9 中断不留半行）；②`tail -1` 重建视图（id/state/git head 三字段齐全）；③round 跳号 WARN（防漏轮）；④与 OF-005 render 联动：tickets 视图头部含最近 checkpoint 摘要行 |
| 验证形式 | 自动=追加/截断/跳号三用例；live=下轮真 wave 追加一次 |
| 量·依赖 | 小 · 无；④ 联动软依赖 OF-005 |
| 文件域 | `bin-ledger`：wave-checkpoint(new)、state/wave-checkpoints.jsonl(new)；`doc-maestro`：orch-loop.md |

### OF-010 · tickets→longtask 单向投影（B.4 绑定层）

| 项 | 内容 |
|---|---|
| 依据 | 用户裁决（2026-08-23 追加）：主题 B 三点未覆盖 maestro 数据面与 DSH longtask/goal 面的绑定——resume 场景 goal 视图不反映票态、断点续传绕开原生恢复路径的缺口 |
| 范围 | ledger `ticket` 子命令族（OF-005）在**终态迁移**（done/merged/rejected/rolled-back）与 wave-checkpoint 追加（OF-007）时，单向投影进 DSH 长时任务：checkpoint 行=本轮 wave 票态摘要（复用 ledger-carryover 的 Checkpoints 形制：陈述 + verifiedBy=票号）；Objective/goal 本体**不自动改写**（人类所有），只投事实行。投影失败不阻塞主迁移（journal WARN，下次迁移补投）；无活跃 longtask 时静默跳过。**方向钉死：maestro 为源、longtask 为渲染视图；不反向同步、不修 longtask 服务器本体**（其损坏史=不建在它上面的理由，见 §0） |
| 验收 | ①票终态迁移后 longtask checkpoint 含该票号+终态（live 冒烟一次）；②longtask 拒写时不阻塞 tickets 主流程，journal 落 WARN 且后续补投成功；③无活跃 longtask 会话时静默跳过零报错；④wave-checkpoint 行与 longtask checkpoint 摘要勾稽一致；⑤全链无反向写（longtask 侧不产生 maestro 状态来源） |
| 验证形式 | 自动=mock longtask 写入单测；live=一轮真 wave 追加后核对 checkpoint |
| 量·依赖 | 小 · OF-005（迁移钩子）+ OF-007（摘要行复用）；投递通道（loopback API vs 文件承接件）票内二选一定 |
| 文件域 | `bin-ledger`：ledger、wave-checkpoint（同域串行，天然无冲突） |

---

## 5. 缺陷直修工作包（台账高优）

### OF-008 · dais 构建断言 + 实例锁 + 消费侧 WARN（D-01 + D-02 + D-03 附带）

| 项 | 内容 |
|---|---|
| 依据 | D-01（orchestration feature 静默丢失 ≈2.5h）、D-02（transient 击落常驻 + 双实例 287105/302881）、D-03 附带（错误形制版本化起点）；round 15 strings 铁证 |
| 范围 | ①〔new:~/.local/bin/dais-build〕：`cargo build --release -p warp --features orchestration` → **strings 断言**（二进制中 "not enabled in this build" 计数=0，>0 即失败退出）→ 安装 + 构建报告（附 read-worker 错误形制探测快照，D-03 版本化契约起点）；②〔loc:~/.local/bin/dais:1→launcher〕wrapper 增**实例锁**：GUI 类子命令 exec 前取 〔new:~/.local/state/dais/instance.lock〕（flock + pid + boot-id；后起者退出并打印持有者；`--force` 覆盖；boot-id 变化后陈旧锁自动失效）——agent transient spawn 走同一 PATH wrapper，故同样被守卫；③消费侧 WARN：〔loc:examples/realtime-provider-poc/rt_probe_m0.py→probe_dais_runtime〕与 〔loc:examples/realtime-provider-poc/rt_dsh_lane.py:217→check_status/:225→read_worker〕捕 "not enabled in this build" / 平面缺席错误签名 → 醒目 WARN + 修复指引（指向 dais-build） |
| 验收 | ①好/坏二进制合成 fixtures：断言通过/失败两态均正确；②双起第二实例被拒+提示持有者 pid；重启后 boot-id 失效锁自动让位；③探针对坏形态输出 WARN（单测 mock 响应，不改 live 语义）；④dais-build 幂等可重跑；⑤构建报告含错误形制快照（D-03 契约基线落盘） |
| 验证形式 | 自动=fixtures/锁单测；live=一次真构建+断言全链 |
| 量·依赖 | 中 · 无（**POC 侧文件独立，可即开**；wrapper 域独立于 VO 在飞文件） |
| 文件域 | `dais-wrap`：dais、dais-build(new)、instance.lock(new)；`poc-probe`：rt_probe_m0.py、rt_dsh_lane.py |

### OF-009 · live 预算 doctrine + 并发租约（D-12 + D-13）

| 项 | 内容 |
|---|---|
| 依据 | D-12（60×6s≈360s 预算 vs 实测 P95 偶发 >5min，门禁打回实录）、D-13（双 pytest 同邮箱互踩）；VO-006-report 实测分布（典型 40–70s） |
| 范围 | ①**doctrine 固化**：推导公式 `预算 ≥ 实测 P95 × 2` 落 〔doc:docs/plans/impl-specs.md→G 门〕（新增 G6 或并入 G1 注）+ 〔doc:docs/plans/dispatch-plan.md〕派发模板——预算常量与实测记录**同文件注释互指**（引 `docs/kg/evidence/VO-006-report.md` P95 数据；后续每张 live 票派发前先测后定）；②**并发租约**：〔new:tests/live_lock.py〕——flock 上下文管理器（per 资源域：mailbox 名 / dais handle 键），〔loc:tests/conftest.py〕挂 live 标记用例自动取锁（等锁超时可配：默认 block，`DSH_LIVE_LOCK=skip` 时跳过并留痕）；③在案记录：VO-007 的"manager mailbox 每运行唯一后缀"现行缓解记为租约的局部先例 |
| 验收 | ①两 pytest 进程并发跑同域 live 用例：后者等锁至前者释放，零互踩（合成双进程演练）；`skip` 模式确定性跳过；②非 live 用例与未加锁域行为=现行（回归）；③公式与常量互指可 grep（doctrine 文件 ↔ 测试常量注释双向锚）；④锁等待超时告警日志（不静默死等） |
| 验证形式 | 自动=双进程合成演练 + 单测；doctrine=文本核对 |
| 量·依赖 | 小–中 · doctrine 部分**即开**；tests 部分**严格排 VO-007 合入后**（红线窗口，实践中=VO-012 后） |
| 文件域 | `docs`：impl-specs.md、dispatch-plan.md；`tests-live`：tests/conftest.py、tests/live_lock.py(new)、（合入后）tests/test_live_v7.py 常量注释 |

---

## 6. 非目标（用户/编排者裁决：不过度设计，本波不做）

| 项 | 裁决 | 现阶段替代 |
|---|---|---|
| 多主编排者协商协议（抢占/让渡/合并决策权） | 不做 | OF-002 租约 + fleet-conflicts.jsonl 记冲突即止 |
| TUI 文本注入传输重构（send=打字、bash 面语义未定义） | 短期不动 | 长期方向=结构化通道（OF-004 同路）；现行三源监控纪律不变（D-11） |
| 递归监督（watchdog 的 watchdog） | 不做 | OF-006 升级终点=alerts.log 用户可见告警面，一层即止 |
| repo `docs/tickets.md` 数据化 | 不做 | 保持手写项目工件；仅 maestro 侧 tickets.md 转渲染视图（OF-005 裁决记录） |

## 7. 未入波缺陷处置（低优/上游归属）

| 缺陷 | 处置 |
|---|---|
| D-03 CLI 软错误契约漂移 | 主对策已落地（DaisLane 归一）；版本化契约起点=OF-008 ⑤ 构建报告快照，独立修复不入波 |
| D-04 start-worker pane 绑定 | dais 上游观察项；绕行路径（new-terminal+inject+mailbox）已固化，不动 |
| D-05 run 注册表只增不清 | dais 侧数据；低优，随 dais 上游 GC 策略处理 |
| D-06 插件域非 git | Wave 0.1 收口（版本化推广），不占票 |
| D-11 omp TUI 帧不含响应 | 约束非缺陷；三源纪律已固化（dispatch-plan 纪律 2/3） |
| D-14 broadcast 凭证重试下沉 DshBackend | 次波候选（中低优）；OF-009 落地后按余力插空 |
| D-15 no_proxy_env 统一帮手 | 次波候选（小卫生件）；任意票插空即可，建议随 OF-009 顺手 |

## 8. 票草案汇总（OF 票板，v4 同构）

> **执行状态（2026-08-24 留位批收口）**：10/10 终态——☑ 001/002/003/005/008/009/010/011 + 006/007 merged（含留位批 ③④+RENEW，maestro `c5105b1`）；OF-004 ☐blocked（中期窗口显式持有，唯一非终态）。OF-009=POC `628d97d`/`91f8974`（live 租约+G6 doctrine+laneB 默认 omp）。权威进度：`~/.dsh/maestro` ledger ticket 面 + `state/wave-checkpoints.jsonl`（round5）。

> 状态：☐ 待派 → ◐ 进行中 → ☑ 完成。回报物：`docs/kg/evidence/OF-00x-report.md`。
> done body 固定格式：`<判定>;报告:docs/kg/evidence/OF-00x-report.md;测试:<N>全绿;台账:D-xx ✅;备注:<≤40字>`

| 票 | 标题 | 量 | 依赖 | 文件域 | 可并行性 |
|---|---|---|---|---|---|
| OF-001 | DSHMSG 信封 v2（msgid+去重，D-10） | 小–中 | 无 | bin-send | **即开** |
| OF-002 | fleet 属主租约 + steer 闸（D-07） | 中 | OF-001 | bin-send+bin-fleet | 串行链位 2 |
| OF-003 | steer two-phase + 可达性文档（D-08 尾） | 小 | OF-002 | doc-maestro | 串行链位 3 |
| OF-004 | loopback 能力凭证 | 中–大 | OF-003 | bin-send+plugin-host | **中期**，不阻塞他票 |
| OF-005 | tickets DAG 数据化 | 中 | 无 | bin-ledger | **即开**（与 A 组并行） |
| OF-006 | event-watchd 事件守护（D-09） | 中–大 | OF-002（SLA 面 OF-005） | bin-watchd | 第二波 |
| OF-007 | wave 检查点机器可读 | 小 | 无（④软依赖 005） | bin-ledger | 即开，④ 待 005 |
| OF-008 | dais 构建+实例守卫（D-01/02/03 附） | 中 | 无 | dais-wrap+poc-probe | **即开**（POC 域独立） |
| OF-009 | live 预算 doctrine+并发租约（D-12/13） | 小–中 | 无 | docs+tests-live | docs 即开；tests 待 VO-012 |
| OF-010 | tickets→longtask 单向投影（绑定层） | 小 | OF-005+OF-007 | bin-ledger | 第二波（W2 收尾） |

## 9. 总依赖图与排期（VO-012 后）

```
依赖图（→ 强依赖；⇢ 软依赖/分期面）：

OF-001 ─→ OF-002 ─→ OF-003 ─→ OF-004(中期)
            │
            └────────⇢ OF-006（①②面可先；③SLA⇢OF-005 ④租约⇢OF-002）
OF-005 ⇢ OF-006③ / OF-007④
OF-007（独立）
OF-008（独立）
OF-009（docs 独立；tests ⇢ VO-007 合入）

Lane 拓扑（VO-012 收口后放行；Wave 0 先行）：

Lane W1（会话面，串行）: OF-001 → OF-002 → OF-003 → [OF-004 中期窗口]
Lane W2（数据面）:      OF-005 → OF-007（④联动收尾）→ OF-010（longtask 绑定收尾）
Lane W3（守护面）:      OF-008 即开；OF-006 待 W1 的 002（③面待 W2 的 005）
Lane W4（live 纪律）:   OF-009 docs 即开；tests 部分=VO-012 后窗口
```

**排期建议**：

1. **VO-012 前只做两件**：Wave 0（版本化+基线，OF 派发的机械前提）与 OF-009 doctrine 文本（纯 docs，零冲突）。
2. **VO-012 后立开四路并行**：OF-001（W1 头）、OF-005（W2 头）、OF-008（W3 头）、OF-009 tests（W4）——四票文件域两两不相交（§8 冲突列），均可 worktree 化（Wave 0.1 后）。
3. **第二波**：OF-002→003 随 W1 推进自然就绪；OF-006 在 002 合入后上（①②面先行，SLA 面 patch 补）；OF-007 在 005 后收④。
4. **中期窗口**：OF-004 在 A 组稳定（001–003 全 ☑ + 一轮真值守验证）后再启，灰度开关默认关，不设硬时限。
5. **执行载体**：沿 dispatch-plan 先例——git 域（Wave 0.1 后的 maestro/插件仓 + POC 仓 poc-probe 域）走 orca worktree+omp；docs/tests-live 域主区串行；每票合并走五步协议（rebase 域隔离→ff-only→门禁→原子回滚→清理），门禁=对应 selftest+全量回归+OG1 兼容冒烟。

## 10. 台账回写映射（验收勾稽表）

| 缺陷 | 票 | 回写动作（验收含） |
|---|---|---|
| D-01 | OF-008 ① | ✅ + 构建报告链接 |
| D-02 | OF-008 ② | ✅ + 锁演示记录 |
| D-03 | OF-008 ⑤ | 状态更新（契约基线落盘，非全解） |
| D-07 | OF-002 sweep | ✅ |
| D-08 | OF-003 ③ | ✅（尾巴收口：skill 文档主叙事切换） |
| D-09 | OF-006 | ✅（①②面验收时标"主体解"，③④面补记） |
| D-10 | OF-001 ④ | ✅ |
| D-12 | OF-009 ① | ✅ |
| D-13 | OF-009 ② | ✅ |
| 主题 A | OF-001..004 | 台账 §新节：A 组收口记录（信封/租约/两段/凭证四层） |
| 主题 B | OF-005..007 + OF-010 | 台账 §新节：B 组收口记录（状态/守望/检查点三层 + longtask 绑定） |
