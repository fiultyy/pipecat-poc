# LB-002-B live 凭证（DAG 生产链 + observe 面）

- 日期: 2026-08-26 16:02:26
- 结果: **ALL PASS**
  (observe=True B1拆分=True B2链路=True B3播报=True B4观察面=True)
- head: GLM glm-5.3 文本模式（Q2 定案回退；DASHSCOPE_API_KEY 缺席）
- run: run_<redacted> ref: vh-<redacted>
- worker 会话（new-terminal 真身，start-worker --session 绑定）:
  - session_178773129924779
  - session_17877313009491
- head 拆分: [{"spec": "统计 src/pipecat/frames/frames.py 文件中以 class 开头的帧类定义数量，输出该数字", "command": "grep -c '^class ' src/pipecat/frames/frames.py"}, {"spec": "统计 src/pipecat/processors/ 目录下所有 .py 文件中以 class 开头的类定义总数，并与第一步统计出的 frames.py 帧类数量对比，输出两个数字及差值", "deps": [0], "command": "F=$(grep -c '^class ' src/pipecat/frames/frames.py); P=$(grep -r '^class ' src/pipecat/processors/ --include='*.py' | wc -l); echo \"frames.py_classes=$F\"; echo \"processors_classes_total=$P\"; echo \"diff=$((P-F))\""}]
- 终稿: "Agent Final Message":

子任务1：统计 src/pipecat/frames/frames.py 文件中以 class 开头的帧类定义数量，输出该数字 → succeeded｜129
^[iYou have 1 orchestration message(s). Run `dais orchestration check-messages ctx_dc1b6594ee56`.
bash: 未预期的记号 "(" 附近有语法错误
子任务2：统计 src/pipecat/processors/ 目录下所有 .py 文件中以 class 开头的类定义总数，并与第 → succeeded｜frames.py_classes=129
processors_classes_total=139
diff=10
^[iYou have 1 orchestration message(…
- WS observe 帧: orch.dispatch=1 orch.progress=4
  orch.done=1 bridge.msg=1 tickets.snapshot=1
- 波序: [(1, 'start', 1787731318.9499693), (1, 'settled', 1787731320.673945), (2, 'start', 1787731320.8868933), (2, 'settled', 1787731322.6024091)]
- 事件流: lb-002b-events.jsonl（每帧含 recv_ts）
- 首次 live: start-worker --session（dais be8d9cf3 D-04 pane 绑定）
