# KG N13 · Head 人格配置化与回执精简化（设计稿）

> 状态：**设计**（未实施）· 2026-08-27 · 配对文档：正文 store 任务（gateway 进程内 store + on_final 一分为二）
> 目标：① tools 回执只报状态+极简摘要；② 人格配置化，默认「任务助手：精炼表述、状态优先、正文引用详情栏」；③ After Tool Calls 废除逐字转述；④ read_body 按需读正文。
> 硬约束：无新进程；渐进迁移可回退；兼容 turn_idle 注入锁、_live_heads 单活 head、topics 观测面。

## 1. 回执新形态（模型可见 JSON）

现状回执（`rt_dsh_backend.py:_liaison_roundtrip`）带 ref + credentials + note，doctrine 要求逐字转述。新形态：

```json
{"status": "accepted", "ref": "vh-<redacted>", "summary": "已受理，转对接人执行"}
```

dispatch_plan 多一个 `tasks`：

```json
{"status": "accepted", "ref": "vh-<redacted>", "tasks": 3, "summary": "已按 3 步受理"}
```

- `ref` 保留全文：cancel_run / read_body 的机器锚点，11 字符文本成本可忽略。
- `credentials`、`run_id` **不进模型上下文**（credential = ref 大写加壳，对模型是纯重复）。
- `clarify` 回执不变（本就无凭证）；非 liaison 的 `cancel` 回执去掉 run_id，只留 `{"status":"canceled","ref":…}`。
- `summary` 由 gateway 侧机械合成（mode → 固定短语），不信任下游模型生成。

## 2. 完成通报（_on_final notice 模式注入的 user item）

正文全文不再注入 head。`_on_final` 把 body 写入 store 后，仅注入：

```
[编排通报] {"status": "done", "ref": "vh-<redacted>", "summary": "采纳方案B，预计收益约41%", "chars": 2380}
```

失败/取消同形：`{"status":"failed"|"canceled", …}`。`summary` 提取：终稿首个非空行截 60 字
（liaison 会话 doctrine 另行约束其终稿以一行结论开头，属配对任务）；兜底取前 60 字截断。
行为规则全部写进 head doctrine，注入项不带指令文本（每回合上下文最省）。

## 3. read_body 契约（第 6 工具）

```json
{"status": "done", "ref": "vh-<redacted>", "chars": 2380, "body": "……终稿全文……", "truncated": false}
```

- schema：`read_body(ref)`（docstring 即 schema，沿用 rt_head_tools 约定）；body 截断上限 4000 字符，
  `truncated:true` 时由 doctrine 指示 head 告知用户可继续。
- **常驻注册**（工具 schema 面稳定，避免 session.update 漂移）；handler 经
  `params.app_resources["body_store"]` 取 store，缺席时返回 `{"status":"no-store"}` 自卫。

## 4. 凭证去向与取舍

凭证不进模型上下文，对账三落点：

1. `DshBackend._runs`（进程内权威登记，cancel 对账现成，`rt_dsh_backend.py:_runs`）；
2. `orch.dispatch` / `orch.done` 事件 payload——观测面票板与详情栏的数据源（凭证与全文**现在就在**，`rt_dsh_backend.py:_liaison_roundtrip`/`_phase2` 的 bus.emit）；
3. gateway store 台账（配对任务新增：`put(ref, body, summary, credentials, run_id)`）。

**取舍**：失去口播信道金丝雀（`rt_orchestrator.py:extract_credentials` 从口播文本验证逐字转述完整性）。
该验证依赖被本次废除的逐字转述条款，条款废即失效；对账迁到观测面后从概率性验证变确定性校验。净收益为正。

## 5. 人格配置载体：单载体（VOICE_HEAD_DOCTRINE 文件）

**决策**：不新增 `VOICE_HEAD_PERSONA` env。人格 = doctrine 文件首段 `# Persona and Role`；
`DoctrineSource`（`rt_head_tools.py:47`）零改动——整文件换装机制已建成，坏配置安全回落不变。
理由：第二个 env 会引入预设枚举校验 + 与 doctrine 文件的合并语义 + 双事实源；当前无多预设热切需求（YAGNI）。
将来需要按部署切人格时，再加预设段不迟。内置常量 `DSH_TOOLS_DOCTRINE` 同步替换为新稿；
运维改人格 = 编辑外置文件（`VOICE_HEAD_DOCTRINE=/path/doctrine.md`，重连生效，不重启网关）。

## 6. 模糊编号结论：可行

- **机器路径不经口述**：cancel_run / read_body 用 head 上下文（回执/通报）里的完整 ref；
  用户说「取消刚才那个调研」，head 自己解析指代，ASR 不参与 ref 解析，flash 概率丢字不伤链路。
- **口语仅服务人**：默认不念编号；用户要核对或需区分多任务时，念前三位（「编号 3f7」）。
  8 hex 取前 3 位，会话内 5 并发冲突率 ≈ C(5,2)/4096² ≈ 0.005%，冲突时 head 追问一次即消解。

## 7. 改造点（file:function）与开关

| # | 位置 | 改动 | 回退 |
|---|---|---|---|
| 1 | `rt_head_tools.py:DSH_TOOLS_DOCTRINE` | 四段式全文替换（附 A），含 # Persona and Role 首段、read_body 工具行、新 After Tool Calls | `VOICE_HEAD_DOCTRINE` 指回旧 doctrine 文件 |
| 2 | `rt_head_tools.py:dsh_head_tools` / 新增 `read_body_tool` | 挂第 6 工具（常驻）；缺 body_store 返回 `{"status":"no-store"}` | 不挂即退（条件注册亦可） |
| 3 | `rt_dsh_backend.py:_liaison_roundtrip` | 回执瘦身 status/ref/summary[/tasks]；credentials、run_id 仅留 `_runs` 与 orch.dispatch emit | `VOICE_RECEIPT_SLIM=0` |
| 4 | `rt_dsh_backend.py:dispatch`、`dispatch_dag` | 非 liaison 回执同形瘦身（同开关） | 同上 |
| 5 | `rt_gateway.py:build_realtime_head` 内 `_on_final` | notice 模式：`store.put(ref, body, summary)` 后仅注入 `[编排通报]{json}`；仍走 `_inject_final_when_idle`/`_final_inject_lock` | `VOICE_FINAL_DELIVERY=fulltext`；body_store 缺席强制 fulltext（双保险） |
| 6 | `rt_gateway.py:main`（backend 构建处） | body_store 实例注入 backend / app_resources（与配对任务合流点） | — |

开关读取时机：receipt 组装处与 `_on_final` 每次调用现读 env（免重启，与 DoctrineSource 哲学一致）。

## 8. 兼容性核对

- **turn_idle 注入锁**：notice 仍走 `_inject_final_when_idle`（`rt_gateway.py:859`），仅文本变短，锁语义不变。
- **_live_heads 单活**：notice 文本更短，pending 缓冲 / 补投行为不变（`rt_gateway.py:_live_heads`）。
- **topics 观测面**：不新增 kind；详情栏数据源 = `orch.done.artifact`（现成）+ store（补按 ref 历史查询）。
- **ConversationLog**（`rt_conversation_items.py`）：注入项从全文变通报，`text_chars` 增速大幅下降，compaction 压力减小。
- **回退成组**：`VOICE_RECEIPT_SLIM=0` + `VOICE_FINAL_DELIVERY=fulltext` + doctrine 指回旧文件 = 完整旧链路。

## 9. 迁移顺序

1. 改造点 #1/#3/#4（doctrine + 回执瘦身）——独立可上、可回退，不依赖 store；
2. store 任务合流（#6 + #2 的 body_store 实装）；
3. #5 默认翻 `notice`。

## 附 A：新 DSH_TOOLS_DOCTRINE 全文（四段式替换稿）

```markdown
# Persona and Role
你是「Nova」，任务助手：精炼表述、状态优先、正文引用详情栏。你听懂用户、提取意图、调用工具；执行层的长正文不经过你的嘴，落在会话详情栏，用户要时你才取、才讲。

# Tools
- dispatch_intent：把用户意图（自包含，指代全部展开）交给编排层分派。凡用户没有明确要求分步执行的意图，一律用这个。
- dispatch_plan：仅当用户明确要求分步、且步骤之间有先后依赖（如"先…再…"、"第一步…第二步基于第一步…"）时调用。后一步用到前一步结果的，必须在前一步条目的 deps 里写上前一步的下标。subtasks_json 是 JSON 数组，每项含 spec（自包含子任务描述）、deps（前置子任务的下标数组，从 0 起，无依赖可省略）、command（真实完成该子任务工作的 shell 结算块，在仓库根目录执行）。
- query_status：查询当前编排任务的状态摘要。
- cancel_run：取消一个编排任务，参数用回执里的 ref（vh-…）。用户说"取消刚才那个/第一个调研"时，由你从上下文里的回执解析出 ref，不让用户念编号。
- read_body：读取某个任务的终稿正文。用户问"报告说了什么/结论依据是什么/完整内容/详情栏那条"时调用，参数用回执或通报里的 ref。
- remain_silent：当最好的回应是不说话时调用（如控制消息后的确认），无用户可见效果。
- 闲聊、问候、一句话可答的常识直接回答。

# After Tool Calls（最高优先级规则）
- 受理回执（status=accepted）：用一句话讲状态和极简摘要，如"已受理，转对接人执行，详情栏可看进度"。不念 ref、不念凭证、不念 run_id、不复述 JSON 字段。
- 完成通报（以 [编排通报] 开头的消息）：一句话通报状态与 summary，如"调研完成了：采纳方案B，全文在详情栏"。不播报正文；用户追问细节时调 read_body 取来再讲。
- 编号协议：默认不念编号。仅当用户要核对、或同时有多个任务需要区分时，念 ref 的前三位（如"编号 3f7"）；cancel_run / read_body 一律使用你上下文里的完整 ref，与念法无关。
- 不添加执行层没有的事实；转述 read_body 正文要忠实，长文先讲结构与要点，用户要求再逐段展开。

# Personality and Tone
中文口语，短句优先，简洁友好，不用 Markdown。
```

## 附 B：外置 doctrine 文件

首份外置样例落 `examples/realtime-provider-poc/doctrine/head-doctrine.md`（内容=附 A）；
`VOICE_HEAD_DOCTRINE` 指向它即免改码调人格。
