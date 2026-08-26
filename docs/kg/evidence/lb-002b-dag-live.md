# LB-002-B live 凭证（DAG 生产链 + observe 面）

- 日期: 2026-08-26 15:09:46
- 结果: **FAIL**
  (observe=True B1拆分=True B2链路=False B3播报=False B4观察面=False)
- head: GLM glm-5.3 文本模式（Q2 定案回退；DASHSCOPE_API_KEY 缺席）
- run: run_<redacted> ref: vh-<redacted>
- worker 会话（new-terminal 真身，start-worker --session 绑定）:
  - session_178772772516932
  - session_178772772614183
- head 拆分: [{"spec": "统计 src/pipecat/frames/frames.py 中的帧类（class 定义）总数，作为规模基线第一步", "command": "grep -c '^class ' src/pipecat/frames/frames.py"}, {"spec": "统计 src/pipecat/processors/ 目录下所有 .py 文件里的类定义总数，并与第一步 frames.py 的帧类总数对比，输出两个数字及差值", "deps": [0], "command": "F=$(grep -c '^class ' src/pipecat/frames/frames.py); P=$(grep -rh '^class ' src/pipecat/processors/ --include='*.py' | wc -l); echo \"frames.py class count: $F\"; echo \"processors class count: $P\"; echo \"difference (processors - frames): $((P-F))\""}]
- 终稿: 
- WS observe 帧: orch.dispatch=1 orch.progress=1
  orch.done=0 bridge.msg=1 tickets.snapshot=1
- 波序: [(1, 'start', 1787727749.515495)]
- 事件流: lb-002b-events.jsonl（每帧含 recv_ts）
- 首次 live: start-worker --session（dais be8d9cf3 D-04 pane 绑定）

## D-18 附记（2026-08-26 15:2x，缺陷出票）

- 第 3 轮 B2 断点经手工最小复现隔离为 **dais 侧块结算缺陷**（D-18）：
  new-terminal → start-worker --command --session（绑定成功，pane view 7027）
  → inject-prompt（命令真实执行，tail 实证）→ worker_done 永不落
  （ctx_* 与 worker_ctx_* 双邮箱空；任务停 [ready]）。
- 已按桥契约 session-send 出票 dais-iter（session-c8e0317a，ref=D-18，
  type=ask，accepted=True），含锚点链与最小复现步骤。
- 消费侧与观察面就绪度：live_lb002b_dag.py（B1–B4 四段断言）
  + rt_dsh_backend 注入接线 + rt_head_tools dispatch_plan + dag_workers 池，
  离线 20P 绿。dais 修复部署后当场复跑收口。

## D-18 第二错位定证（2026-08-26 15:5x，方案裁决依据）

- `store.drain_inbox`（store.rs ~1170）：事务内 select-and-mark——**拉取即全部置 read**；
  CLI `--type` 过滤在拉取后（orchestration.rs:481），非匹配行被消费后丢弃、无回写。
- 推论：若消费侧改轮 `orchestrator` 共享邮箱（方案 B），每次 `--type worker_done`
  轮询都会静默吞掉该邮箱所有其他未读消息（status/escalation/intent 全毁）——B 不可行。
- 定案建议（已发 dais-iter）：A = block_settle.rs:102 enqueue 的 to_handle 由
  `orchestrator` 改为 `dispatch_id`（`ctx_<id>`），与 send-message 手工回投面同构，
  消费侧 `await_worker_done`（轮 `ctx_<id>`）零改动。
