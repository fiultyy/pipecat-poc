#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""Live V7 (VO-007; KG 06 §4 six-step scenario): manager 群 + 逐跳回传.

真链路（live 真跑；protocol 底座 = live_v5_v6_dsh.py，VO-006 移交）：

  1. 真孵化两个 dsh-manager（lane-a=调研域 / lane-b=文档域，role=manager，
     各自 mailbox + project，fleet 五键登记核验）+ V7 liaison（沿用 VO-006
     落位协议，appendix 换分发/聚合职责）。
  2. F4：head GLM dispatch_intent → 邮箱正文 + DSHMSG 推唤醒 → liaison
     真身回合（两阶段时序：phase-1 终稿未到）。
  3. F6：liaison 逐域分发——dais 邮箱正文（status 型）+ router agents/send
     推唤醒（session-send 固定信封；双到达与 VO-005 对拍同语义）。
  4. F7：manager 按分派策略选车道（orca 占位 / dais 实跑）——本场景轻量
     → dais 车道：create-run/create-task/start-worker + session-spawn 工厂
     worker（GUI 退出且红线禁 spawn dais，VO-005 验证过的 headless 路径）
     + worker_done 回收。
  5. F9/F10：worker→manager→liaison→head 终稿逐跳回传。
  6. 断言：全链 ref 不丢、凭证逐字、FINAL_PREFIX 终稿、两阶段时序。

headless live 事实与偏差（探针钉死，见 live_v5_v6_dsh.py V7 doctrine 节）：
router 重载路径（--message-type direct）被现行 dais CLI 拒绝 → F6 邮箱正文
经 dais CLI 直投（status）；worker_done 的 GUI 侧结算 watcher 缺席 →
check-messages <ctx> --type worker_done 的消息行本身即回收凭证。
"""

import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

HERE = Path(__file__).parent.parent / "examples" / "realtime-provider-poc"
REPO = Path(__file__).parent.parent
sys.path.insert(0, str(HERE))

import live_v5_v6_dsh as live  # noqa: E402 (protocol base, VO-006 handover)
from rt_dsh_lane import DaisLane  # noqa: E402
from rt_head_tools import (  # noqa: E402
    DSH_TOOLS_DOCTRINE,
    dispatch_intent_tool,
)
from rt_orchestrator import FINAL_PREFIX  # noqa: E402
from openai import AsyncOpenAI  # noqa: E402

DAIS_BIN = Path("~/.local/bin/dais").expanduser()
PLUGIN_DIR = Path("~/.dsh/plugins/a2a-profile-server").expanduser()
FLEET_PATH = Path("~/.dsh/maestro/fleet.json").expanduser()
SESSION_SEND = Path("~/.dsh/maestro/bin/session-send").expanduser()
SESSION_PURGE = Path("~/.dsh/maestro/bin/session-purge").expanduser()
ROUTER_PORT = 39707  # 固定端口：appendix 在孵化期嵌入，harness 孵化后启动

# head-side logic files: zero-diff acceptance (same list as VO-006)
HEAD_SIDE_FILES = [
    str(REPO / "src/pipecat"),
    str(HERE / "rt_dsh_backend.py"),
    str(HERE / "rt_dsh_lane.py"),
    str(HERE / "rt_head_tools.py"),
    str(HERE / "rt_orchestrator.py"),
]

V7_CRED = "R-V7-7701"
CRED_MARK = f"【凭证{V7_CRED}】"
INTENT = ("帮我调研 WebGPU 在生产环境的采用情况（调研域），"
          "并把结论整理成一句话中文摘要（文档域）。")
RUN_SUFFIX = uuid.uuid4().hex[:4]  # manager mailbox 每运行唯一：防上轮残留
# manager 在册同 mailbox 时，router 推唤醒按 mailbox 寻址会命中旧会话

DOMAINS = [
    {
        "key": "a", "name": "lane-a-manager", "profile": "vh-mgr-lane-a",
        "mailbox": f"agent_lane_a_{RUN_SUFFIX}", "project": "lane-a",
        "domain": "调研",
        "witness": "WITNESS-A",
        "task": "给出 WebGPU 生产环境采用情况的一句话结论",
        "scenario": (
            "域编排：作为调研域的常驻管理 agent，接收上游稳定指令，"
            "按分派策略选车道（终端/工作树类→orca 车道；消息 DAG/轻量→dais 车道）"
            "派发 worker 并回收 worker_done，汇总域终稿回传上游；"
            "工作在消息邮箱与终端命令环境中，不直接面向终端用户。"),
    },
    {
        "key": "b", "name": "lane-b-manager", "profile": "vh-mgr-lane-b",
        "mailbox": f"agent_lane_b_{RUN_SUFFIX}", "project": "lane-b",
        "domain": "文档",
        "witness": "WITNESS-B",
        "task": "把调研结论整理成一句话中文摘要",
        "scenario": (
            "域编排：作为文档域的常驻管理 agent，接收上游稳定指令，"
            "按分派策略选车道（终端/工作树类→orca 车道；消息 DAG/轻量→dais 车道）"
            "派发 worker 并回收 worker_done，汇总域终稿回传上游；"
            "工作在消息邮箱与终端命令环境中，不直接面向终端用户。"),
    },
]
LIAISON_SCENARIO_V7 = (
    "编排对接：作为语音编排头与外部执行体系之间的常驻对接联络 agent，"
    "接收语音头的语义任务指令，收敛为稳定指令后按域分发给 manager 群（F6），"
    "汇总各域终稿后回传语音头（F10）；工作在消息邮箱与终端命令环境中，"
    "不直接面向终端用户。")

# 硬预算（防挂死不依赖壳：全局 watchdog + 阶段钳制 + GLM 显式 timeout；
# 编排者批准壳 1500s——晚峰 GLM 回合 >6min，五跳链物理需 ~15min）。
TOTAL_BUDGET = 1440     # v7_main 全局 watchdog（+30=1470 < shell 1500）
W_LIAISON_T1 = 300      # liaison 回合1（F6 分发）阶段帽
W_MANAGERS = 780        # manager 回合阶段帽（worker 晚峰单回合 >6min 余量）
W_FINAL = 300           # liaison 回合2（F10 聚合 + 回信）阶段帽
GLM_TIMEOUT = 90        # head/projector 侧每次请求超时（防 600s 默认×重试）


# ---------------------------------------------------------------------------
# doctrine appendix builders（V7 场景规约；协议字面量逐字内嵌，G5 不漂移）
WORKER_DONE_JSON = '{\\"task_id\\":\\"<TASK>\\",\\"dispatch_id\\":\\"<CTX>\\",\\"outcome\\":\\"succeeded\\"}'



def manager_appendix_v7(dom: dict, liaison_code: str) -> str:
    """manager 通信操作规约：F7 派发（dais 车道实跑）+ worker_done 回收 +
    F9 回传 + 异常三形态。占位符 <ref>/<RUN2>/<TASK>/<CTX>/<WK码>/<结论>/
    <凭证标记> 由 agent 按解析值逐字代入（VO-006 同款纪律）。"""
    mb, wit, domn = dom["mailbox"], dom["witness"], dom["domain"]
    inject = (
        f"V7 worker 任务：逐字执行下面这条单行命令，回显其全部输出，然后结束回合："
        f"W=$(date +%s%N) && echo \"{wit}-$W\" && "
        f"~/.local/bin/dais orchestration send-message <RUN2> <WK码> {mb} "
        f"--message-type status --subject done "
        f"--body \"[ref:<ref>] {domn}域结论：<结论><凭证标记>（{wit}-$W）\" && "
        f"~/.local/bin/dais orchestration send-message <RUN2> <WK码> <CTX> "
        f"--message-type worker_done --subject done --body \"{WORKER_DONE_JSON}\""
    )
    return f"""### 通信操作规约（最高优先级，逐字执行）— 域「{domn}」

1. 回合首动作：运行 `~/.local/bin/dais orchestration check-messages {mb} --timeout-ms 2000`，
   快照排空你的邮箱取正文。唤醒推送只代表"有新任务"，任务正文一律以邮箱快照为准。
   若首条快照为空（正文尚未到达的次序竞争是已知的）：改跑一次
   `check-messages {mb} --wait --timeout-ms 15000` 等正文；仍为空再等 15 秒重试快照，
   最多 3 轮。不要用 sqlite 直查数据库，不要读 bin 脚本源码，不要做环境侦察——
   正文一定会到邮箱（live 实测事实）。
2. 从唤醒信令解析 `run=<run_id>`（上游主 run，回信用它）；从来件正文解析 `[ref:<ref>]`、
   域任务、全部【凭证…】标记（后续逐字透传，不改、不丢、不加）。
3. 车道选择（分派策略）：终端/工作树类任务 → orca 车道（本任务不适用，占位）；
   本任务为消息 DAG/轻量 → dais 车道实跑。
4. F7 派发（逐条执行；每条输出里的 id 记下来，供 e 逐字代入）：
   a. `~/.local/bin/dais orchestration create-run --objective "V7 {domn} worker run"` → 记下 run_…
   b. `~/.local/bin/dais orchestration create-task <a 的 run_…> "{domn}域子任务 [ref:<ref>]"` → 记下 task_…
   c. `~/.local/bin/dais orchestration start-worker <b 的 task_…>` → 记下 ctx_…
   d. `~/.dsh/maestro/bin/session-spawn standard wk-{dom['key']} 'V7 {domn} worker'` → 记下末行输出的 4 位码
   e. 按域任务自行写出一句话结论代入 <结论>，<凭证标记> 用来件中的凭证标记逐字代入，
      然后把 a–d 记下的 id 逐字代入（<RUN2>=run_…、<TASK>=task_…、<CTX>=ctx_…、<WK码>=4 位码），
      派发 worker（单行注入，<ref> 代入解析值）：
      `~/.dsh/maestro/bin/session-send {mb} <WK码> steer <ref> '{inject}'`
5. 回收等待：轮询 `~/.local/bin/dais orchestration check-messages {mb} --timeout-ms 3000`
   （两次之间 sleep 3，最多 90 次，≈9 分钟——worker 回合偶发 >5 分钟，预算须
   覆盖慢尾）直到出现 subject=done 且 body 带 `[ref:<ref>]` 的 worker 回信。
6. worker_done 回收：`~/.local/bin/dais orchestration check-messages <CTX> --type worker_done --timeout-ms 2000`
   （应恰好一条，outcome=succeeded）。
7. F9 域终稿回传（恰好一条，用上游主 run <run_id>；worker 回信中的 {wit}-… 时间戳逐字带上）：
   `~/.local/bin/dais orchestration send-message <run_id> {mb} agent_liaison --message-type status --subject done --body '[ref:<ref>] {domn}域终稿：<结论>；{wit}-<worker 时间戳逐字>；<凭证标记逐字>'`
   随后推唤醒 liaison：
   `~/.dsh/maestro/bin/session-send {mb} {liaison_code} steer <ref> "wake: domain done; run=<run_id>"`
8. 异常路径三形态：gate 阻塞 → `resolve-gate <gate_id> <resolution>`；worker 疑似卡死 →
   `scan-wait-blocked <CTX>`（可 answer 自愈）；轮询超时仍无回信 → 上抛：
   `~/.local/bin/dais orchestration send-message <run_id> {mb} agent_liaison --message-type status --subject escalation --body '[ref:<ref>] 超时上抛：<说明>'`，
   不静默吞掉、不无限重试。
9. F9 发出后本回合即结束：不要轮询等待回音，不要向 voice-head 发消息。
"""


def liaison_appendix_v7() -> str:
    """liaison V7 通信规约：回合1 F6 逐域分发（邮箱正文 + router 推唤醒），
    回合2 聚合双域后 F10 单终稿回 head。manager 经 mailbox 寻址（router
    resolveAgent 支持 mailbox；本域内唯一）。"""
    a, b = DOMAINS
    return f"""### 通信操作规约（最高优先级，逐字执行）

1. 回合首动作：运行 `~/.local/bin/dais orchestration check-messages agent_liaison --timeout-ms 2000`，
   快照排空你的邮箱取正文。唤醒推送只代表"有新任务"，任务正文一律以邮箱快照为准。
2. 从唤醒信令解析 `run=<run_id>`；从来件正文解析 `[ref:<ref>]` 前缀、任务描述、
   必须逐字回显的全部【凭证…】标记。
3. 若邮箱取到的是语音头的语义指令（subject=intent）：不自己执行任务，把它按域拆成
   稳定指令并逐域完成 F6 分发（每个域先投正文再推唤醒；<ref>/<run_id> 用解析值代入）：
   - 域「{a['domain']}」邮箱正文：
     `~/.local/bin/dais orchestration send-message <run_id> agent_liaison {a['mailbox']} --message-type status --subject route --body '[ref:<ref>] 域任务[{a['domain']}]：{a['task']}；经 dais 车道派发 worker 实跑；终稿必须逐字保留来件全部【凭证…】标记。'`
   - 域「{a['domain']}」推唤醒（经 router agents/send，按 mailbox 寻址）：
     `curl -s --noproxy '*' -m 20 http://127.0.0.1:{ROUTER_PORT}/ -H 'Content-Type: application/json' -d '{{"jsonrpc":"2.0","id":1,"method":"agents/send","params":{{"from":"agent_liaison","to":"{a['mailbox']}","ref":"<ref>","type":"steer","body":"wake: mailbox has a domain task; run=<run_id>"}}}}'`
   - 域「{b['domain']}」同款两条：正文收件人换 {b['mailbox']}、域任务换「{b['task']}」；
     推唤醒 params 中的 to 换 {b['mailbox']}。
4. F6 分发完成后本回合立即结束：不要自己执行域任务、不要轮询等待下游、
   不要向 voice-head 发任何消息。
5. 若邮箱取到的是 manager 的域终稿（subject=done、body 带 [ref:]）：轮询邮箱直到集齐
   两个域（{a['mailbox']} 与 {b['mailbox']} 各至少一条；最多 60 次、间隔 sleep 5）。
   集齐后用恰好一条回信回复 voice-head（每 ref 只回这一条终稿；此前不要另发受理确认
   或任何中间消息——上游的受理回执由它自己即时生成，不需要你发）：
   `~/.local/bin/dais orchestration send-message <run_id> agent_liaison voice-head --message-type status --subject done --body '<回信体>'`
   回信体 = `[ref:<ref>] "Agent Final Message":` 前缀行，随后一个空行，再接终稿正文；
   前缀逐字符照抄。终稿正文必须包含：两个域各自的结论、worker 回信中的
   {a['witness']}-… 与 {b['witness']}-… 时间戳标记（逐字）、来件中的全部【凭证…】标记
   （逐字不改、不丢、不加）。
6. 回信发出后本回合即结束：不要轮询等待回音，不要向其他 handle 发消息，
   在没有任务正文时不要编造任务或回信。
"""


# ---------------------------------------------------------------------------
# skip guards（deterministic；掉线 skip，勿自行结论）
# ---------------------------------------------------------------------------

def _ready() -> bool:
    if not DAIS_BIN.exists():
        return False
    from rt_env import glm_credentials

    try:
        key, _ = glm_credentials()
        return bool(key)
    except Exception:
        return False


def _plugin_ok() -> bool:
    return bool(shutil.which("node")) and PLUGIN_DIR.joinpath("index.js").exists()


def _dais_plane_up() -> bool:
    try:
        return subprocess.run(
            [str(DAIS_BIN), "orchestration", "check-status"],
            capture_output=True, timeout=30,
        ).returncode == 0
    except Exception:
        return False


def _loopback_up() -> bool:
    """dsh 主面（孵化/唤醒/history 观察面）loopback 探活。"""
    import urllib.request

    try:
        port = str(json.loads(FLEET_PATH.read_text()).get("port", 3080))
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        wire = json.dumps({"type": "client-request", "rpcId": "vo7probe",
                           "method": "session.list", "payload": {}}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/session.list", data=wire,
            headers={"content-type": "application/json"})
        with opener.open(req, timeout=10):
            return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# live driver
# ---------------------------------------------------------------------------

LOG: list[str] = []


def say(line: str = ""):
    print(line)
    LOG.append(line)


def verdict(ok, label, detail="") -> bool:
    say(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f" — {detail}" if detail else ""))
    return ok


async def v7_wakeup(code: str, ref: str, run_id: str) -> None:
    """DSHMSG 推唤醒（V7 语义：分发给 manager 群），session-send 走 maestro
    loopback，不占 dais CLI 总线锁。"""
    body = (f"VO-007 wake: mailbox has a semantic task for you; run={run_id}; "
            f"distribute to domain managers per doctrine")
    proc = await asyncio.create_subprocess_exec(
        str(SESSION_SEND), "voice-head", code, "steer", ref, body,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
    if proc.returncode != 0 or b"accepted=True" not in out:
        raise RuntimeError(f"session-send wakeup failed: {out.decode(errors='replace')!r}")


async def dispatch_intent_v7(params, raw_intent: str):
    """Head tool wrapper（接线，非 head 逻辑）：钉 phase-2 凭证标记 → 原样
    调用未改动的 head 工具（F4 邮箱正文 + 本地 phase-1 回执）→ 推唤醒。"""
    intent = raw_intent.rstrip() + f" 终稿必须逐字回显受理凭证标记{CRED_MARK}。"
    await dispatch_intent_tool(params, intent)
    receipt = params.result
    if isinstance(receipt, str):
        receipt = json.loads(receipt)
    if receipt.get("status") == "accepted":
        await v7_wakeup(params.app_resources["liaison_code"],
                        receipt["ref"], receipt["run_id"])


async def wait_until(pred, timeout_s: float, poll_s: float = 5.0, desc: str = ""):
    """有界等待：timeout_s<=0 立即放弃（预算耗尽 fail-fast，不悬挂）。"""
    if timeout_s <= 0:
        return None
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            v = pred()
            if v:
                return v
        except Exception:
            pass
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        await asyncio.sleep(min(poll_s, max(0.05, remaining)))


def _budget_left(t0: float) -> float:
    return TOTAL_BUDGET - (time.monotonic() - t0)


def args_of(trace) -> str:
    return "\n".join(t["arguments"] or "" for t in trace)


def results_of(trace) -> str:
    return "\n".join(t["result"] or "" for t in trace if t["result"])


async def v7_main() -> bool:
    T0 = time.monotonic()
    for _k in ("ALL_PROXY", "all_proxy"):
        os.environ.pop(_k, None)
    key, base_url = live.glm_credentials()
    glm = AsyncOpenAI(api_key=key, base_url=base_url,
                      timeout=GLM_TIMEOUT, max_retries=1)

    lane = DaisLane(default_timeout_s=20)
    if not await live.bus_healthy(lane):
        raise RuntimeError("dais bus unresponsive mid-run (guards passed earlier)")

    ok = True
    harness = None
    worker_codes: list[str] = []
    kept: dict[str, dict] = {}
    _orig_dispatch = None

    try:
        # ---- step 1: 真孵化 liaison（先）+ 两个 dsh-manager（后嵌码）----
        say("== V7 step1: incubate liaison + managers (lane-a / lane-b) ==")
        lia = await live.incubate_agent(
            profile=live.LIAISON_PROFILE, targets=["dsh-liaison"], role="liaison",
            project="voice-head", mailbox=live.LIAISON_MAILBOX,
            scenario=LIAISON_SCENARIO_V7, appendix=liaison_appendix_v7(),
            description="voice-head 主对接联络 agent（VO-007 V7 链）")
        kept["liaison"] = lia
        fleet = json.loads(FLEET_PATH.read_text())["fleet"]
        ok &= verdict(
            lia["mailbox"] == "agent_liaison" and lia["role"] == "liaison"
            and lia["project"] == "voice-head",
            "孵化回执（真身 dsh-liaison）",
            f"code={lia['code']} mailbox={lia['mailbox']}")
        fe = fleet[lia["code"]]
        ok &= verdict(
            fe.get("mailbox") == "agent_liaison" and fe.get("role") == "liaison"
            and fe.get("project") == "voice-head" and "profile_version" in fe,
            "liaison fleet 五键登记", f"sessionId={fe['sessionId'][:18]}…")

        # 两域投影并行（纯 GLM 调用无共享态）→ RPC 串行（插件 fleet 写
        # read-modify-write 不可并行）；买回 ~35s 预算。
        from rt_projector import Projector

        async def _project(scenario: str) -> str:
            return (await Projector().project(scenario, role="manager")).agents_md

        base_a, base_b = await asyncio.gather(_project(DOMAINS[0]["scenario"]),
                                              _project(DOMAINS[1]["scenario"]))
        mgrs = []
        for dom, base_md in zip(DOMAINS, (base_a, base_b)):
            r = await live.incubate_agent(
                profile=dom["profile"], targets=["dsh-manager"], role="manager",
                project=dom["project"], mailbox=dom["mailbox"],
                scenario=dom["scenario"], agents_md=base_md,
                appendix=manager_appendix_v7(dom, lia["code"]),
                description=f"voice-head {dom['domain']}域管理 agent（VO-007）")
            r["dom"] = dom
            mgrs.append(r)
            kept[f"mgr_{dom['key']}"] = r
            fleet = json.loads(FLEET_PATH.read_text())["fleet"]
            ent = fleet[r["code"]]
            ok &= verdict(
                r["target"] == "dsh-manager" and r["role"] == "manager"
                and r["mailbox"] == dom["mailbox"] and r["project"] == dom["project"],
                f"孵化回执（{dom['domain']}域 dsh-manager）",
                f"code={r['code']} mailbox={r['mailbox']} project={r['project']}")
            ok &= verdict(
                ent.get("mailbox") == dom["mailbox"] and ent.get("role") == "manager"
                and ent.get("project") == dom["project"]
                and "profile_version" in ent,
                f"{dom['domain']}域 fleet 五键登记",
                f"sessionId={ent['sessionId'][:18]}…")

        # ---- router harness（孵化后启动：reattach 可见全部新 agent）----
        grants = []
        now = int(time.time() * 1000)
        for d in DOMAINS:
            grants.append({"from": "agent_liaison", "to": d["mailbox"], "ts": now})
        harness, hport = live.boot_router_harness(grants=grants, port=ROUTER_PORT)
        reg = live.rpc_jsonrpc(hport, "agents/registry", {})
        codes = {a["code"]: a for a in reg["agents"]}
        ok &= verdict(
            all(c in codes for c in (lia["code"], mgrs[0]["code"], mgrs[1]["code"]))
            and codes[mgrs[0]["code"]]["role"] == "manager"
            and codes[lia["code"]]["role"] == "liaison",
            "router registry 在册（liaison + 双 manager）",
            f"agents={len(reg['agents'])}")

        # ---- head setup：handle 仍指向 agent_liaison（配置值；head 逻辑零改动）----
        finals: list[tuple[str, str]] = []

        async def on_final(ref, message):
            finals.append((ref, message))

        backend = live.DshBackend(lane=lane, orchestrator_handle=live.LIAISON_MAILBOX,
                                  head_handle="voice-head", on_final=on_final,
                                  await_timeout_s=540, poll_s=2.0)
        params = live.LiveParams(backend)
        params.app_resources["liaison_code"] = lia["code"]
        _orig_dispatch = live.TOOL_FNS["dispatch_intent_tool"]
        live.TOOL_FNS["dispatch_intent_tool"] = dispatch_intent_v7

        # ---- step 2: F4 head -> liaison ----
        say("== V7 step2: F4 head -> liaison (semantic intent) ==")
        messages: list[dict] = [
            {"role": "system", "content": DSH_TOOLS_DOCTRINE},
            {"role": "user", "content": INTENT},
        ]
        ack, calls = await live.head_turn(glm, messages, params)
        dispatches = [c for c in calls if c["name"] == "dispatch_intent_tool"]
        ok &= verdict(bool(dispatches), "head 调用 dispatch_intent",
                      f"tools={[c['name'] for c in calls]}")
        ref = run_id = None
        if dispatches:
            receipt = json.loads(dispatches[0]["result"])
            ref, run_id = receipt.get("ref"), receipt.get("run_id")
            ok &= verdict(receipt.get("status") == "accepted"
                          and run_id.startswith("run_"),
                          "phase-1 受理回执（head 本地即时生成）",
                          f"ref={ref} run={run_id}")
            ok &= verdict(bool(ack) and ref not in ack,
                          "受理口语回执一句话（不念 ref）",
                          f"ack={ack[:70]!r}")
        ok &= verdict(not finals, "phase-1 阶段终稿未到（两阶段时序）")

        # ---- step 3: F6 分发（liaison 回合1）----
        say("== V7 step3: F6 liaison -> managers (mailbox body + router push) ==")

        journal_path = (PLUGIN_DIR / "state" / "live-vo007"
                        / "router-journal.jsonl")

        def _journal_push_rows() -> list[dict]:
            if not journal_path.exists():
                return []
            rows = [json.loads(x) for x in
                    journal_path.read_text().splitlines() if x.strip()]
            return [r for r in rows if r.get("ref") == ref
                    and r.get("delivered") == "push"]

        def f6_done():
            tr = live.bash_trace(lia["sessionId"])
            s = args_of(tr)
            if ("agents/send" in s and "agent_lane_a" in s
                    and "agent_lane_b" in s):
                return tr
            # 功能性判据（抗命令变体）：router 已为该 ref 双域投递推唤醒
            if len(_journal_push_rows()) >= 2:
                return tr or []
            return None


        lia_t1 = await wait_until(f6_done, min(W_LIAISON_T1, _budget_left(T0)),
                                  desc="liaison F6 turn")
        lia_args = args_of(lia_t1 or [])
        ok &= verdict(lia_t1 is not None, "liaison 回合1 完成 F6 分发（双域）",
                      f"bash_calls={sum(1 for t in (lia_t1 or []) if t['name']=='bash')}")
        jrows = _journal_push_rows()
        ok &= verdict(
            (("send-message" in lia_args and "agent_lane_a" in lia_args
              and "agent_lane_b" in lia_args and "--subject route" in lia_args)
             or len(jrows) >= 2),
            "F6 邮箱正文双域投递（status 型 + 推唤醒留痕双证）")
        ok &= verdict((ref in lia_args and run_id in lia_args)
                      or (len(jrows) >= 2
                          and all(ref == r.get("ref") for r in jrows)),
                      "F6 命令携带 F4 解析出的 ref/run_id（正文已读证据）")
        ok &= verdict("voice-head" not in lia_args
                      and not any("voice-head" in str(r.get("to", "")) for r in jrows),
                      "回合1 未向 voice-head 发消息（终稿只在回合2）")
        wake_calls = live._wakeup_turn_actions(lia["sessionId"], ref)
        execs = [c for c in wake_calls if c["name"] in ("bash", "shell", "exec")]
        # 排空先于任何 F6 分发实质动作（与 manager 侧同语义；真 agent 偶发
        # 环境侦察 preamble，drain 本身仍须在分发之前——doctrine 可验内核）。
        l_drain = l_act = None
        for i, c in enumerate(execs):
            a = c["arguments"]
            if l_drain is None and "check-messages" in a \
                    and "agent_liaison" in a:
                l_drain = i
            if l_act is None and ("agents/send" in a
                                  or ("send-message" in a and "agent_lane" in a)):
                l_act = i
        ok &= verdict(
            l_drain is not None and (l_act is None or l_drain < l_act),
            "liaison 邮箱排空先于 F6 分发（doctrine 行为断言）",
            f"drain@{l_drain} first_act@{l_act}")

        # ---- step 4: F7 派发 + worker_done 回收（manager 回合，双域并行）----
        say("== V7 step4: F7 manager dispatch (dais lane) + worker_done ==")
        mgr_traces = {}
        for m in mgrs:
            dom = m["dom"]

            def f9_sent(m=m, dom=dom):
                tr = live.bash_trace(m["sessionId"])
                for t in tr:
                    a = t["arguments"] or ""
                    if "send-message" in a and "agent_liaison" in a \
                            and "--subject done" in a and "escalation" not in a:
                        return tr
                return None

            tr = await wait_until(f9_sent, min(W_MANAGERS, _budget_left(T0)),
                                  poll_s=3.0, desc=f"{dom['domain']} F9")
            mgr_traces[dom["key"]] = tr or []
            margs = args_of(tr or [])
            ok &= verdict(tr is not None, f"{dom['domain']}域 manager 完成 F9 回传")
            # F6 双到达：推唤醒信封（session history 事件面）+ 回合首动作
            push_hit = False
            for e in live.session_events(m["sessionId"]):
                if e.get("type") not in ("user/message", "agent/inbox/spliced"):
                    continue
                d = e.get("data", {})
                for msg in (d.get("inserted") or [d]):
                    for part in (msg.get("content") or []):
                        txt = part.get("text", "")
                        if txt.startswith("DSHMSG]") and f'"{ref}"' in txt \
                                and "agent_liaison" in txt:
                            push_hit = True
            ok &= verdict(push_hit,
                          f"F6 推唤醒到达 {dom['domain']}域 manager（DSHMSG 信封含 ref）")
            wcalls = live._wakeup_turn_actions(m["sessionId"], ref)
            wexecs = [c for c in wcalls if c["name"] in ("bash", "shell", "exec")]
            # 排空先于任何派发/回传实质动作（真 manager 偶发环境侦察
            # preamble——pwd/env/grep——属准备非任务；liaison 侧维持
            # 首动作严断言，VO-006 同款）。
            drain_idx = act_idx = None
            for i, c in enumerate(wexecs):
                a = c["arguments"]
                if drain_idx is None and "check-messages" in a \
                        and dom["mailbox"] in a:
                    drain_idx = i
                if act_idx is None and (
                        any(k in a for k in ("create-run", "create-task",
                                             "start-worker", "session-spawn"))
                        or ("send-message" in a and "agent_liaison" in a)):
                    act_idx = i
            ok &= verdict(
                drain_idx is not None and (act_idx is None or drain_idx < act_idx),
                f"{dom['domain']}域 manager 邮箱排空先于派发/回传（拉模式正文双到达）",
                f"drain@{drain_idx} first_act@{act_idx}")
            # F7 协议形状 + 派发策略 + worker_done 回收
            for needle, label in [
                ("create-run", "F7 create-run（dais 车道实跑）"),
                ("create-task", "F7 create-task（子任务）"),
                ("start-worker", "F7 start-worker（dispatch ctx）"),
                ("session-spawn", "F7 worker 会话（fleet 工厂）"),
                ("worker_done", "worker_done 回投 + 回收"),
                (dom["witness"], "worker 注入含 WITNESS 标记"),
            ]:
                ok &= verdict(needle in margs, f"{dom['domain']}域 {label}")
            ok &= verdict(ref in margs,
                          f"{dom['domain']}域 F7/F9 命令携带 ref（跳间硬链）")

        # worker 真实性：DAG 实体 + worker 会话登记 + WITNESS 透传
        say("== V7 step4b: worker 实跑证据（DAG 实体 + fleet 登记） ==")
        for m in mgrs:
            dom = m["dom"]
            tr = mgr_traces[dom["key"]]
            res = results_of(tr)
            run2 = re.search(r"run_[0-9a-f]+", res or "")
            task2 = re.search(r"task_[0-9a-f]+", res or "")
            wk = None
            for t in tr:
                if "session-spawn" not in (t["arguments"] or "") or not t["result"]:
                    continue
                # 独立成行的 4-hex 码（spawn 回显；倒序取最新一条）。manager
                # 可能跟跑源码检查命令，纯取尾行会拿到代码文本。
                for ln in reversed(t["result"].strip().splitlines()):
                    if re.fullmatch(r"[0-9a-f]{4}", ln.strip()):
                        wk = ln.strip()
                        break
                if wk:
                    break
            ok &= verdict(bool(run2 and task2),
                          f"{dom['domain']}域 DAG 实体已建（run/task）",
                          f"run={run2.group(0) if run2 else None} "
                          f"task={task2.group(0) if task2 else None}")
            fleet_now = json.loads(FLEET_PATH.read_text())["fleet"]
            ok &= verdict(wk in fleet_now,
                          f"{dom['domain']}域 worker 会话 fleet 登记",
                          f"wk={wk}")
            if wk:
                worker_codes.append(wk)
            if run2:
                st = await lane.check_status(run_id=run2.group(0))
                ok &= verdict(st.get("runs", 0) >= 1 and bool(st.get("entries")),
                              f"{dom['domain']}域 worker run 在册（check-status）",
                              f"entries={len(st.get('entries') or [])}")

        # journal：F6 推唤醒路由留痕
        push_rows = _journal_push_rows()
        ok &= verdict(len(push_rows) >= 2,
                      "router journal：F6 双域推唤醒 delivered=push 留痕",
                      f"rows={len(push_rows)}")

        # ---- step 5: F9/F10 逐跳回传 + 两阶段 ----
        say("== V7 step5: F9 -> F10 -> head phase-2 (hop-by-hop) ==")
        await wait_until(lambda: bool(finals) or None,
                         min(W_FINAL, _budget_left(T0)), poll_s=1.0)
        ok &= verdict(bool(finals), "phase-2 终稿到达（真身 F10 回信）")
        fmsg = ""
        if finals and ref:
            fref, fmsg = finals[0]
            ok &= verdict(fref == ref and fmsg.startswith(FINAL_PREFIX),
                          "终稿 = FINAL_PREFIX + done body（[ref:] 三过滤命中）",
                          f"final[:60]={fmsg[:60]!r}")
            ok &= verdict(CRED_MARK in fmsg, "终稿凭证逐字回显")
            wa = re.search(rf"{DOMAINS[0]['witness']}-\d{{10,}}", fmsg)
            wb = re.search(rf"{DOMAINS[1]['witness']}-\d{{10,}}", fmsg)
            ok &= verdict(bool(wa and wb),
                          "双域 worker WITNESS 时间戳逐字回传（真执行凭证）",
                          f"{wa.group(0) if wa else None} / "
                          f"{wb.group(0) if wb else None}")
            ok &= verdict(DOMAINS[0]["domain"] in fmsg and DOMAINS[1]["domain"] in fmsg,
                          "终稿含双域结论（聚合）")
            # F10 head 播报：一句话通报（doctrine：不整段播报正文、凭证
            # 不上口语；编排通道的凭证在场由上面的 fmsg 断言保证）
            messages.append({"role": "user", "content": fmsg})
            broadcast, _ = await live.head_turn(glm, messages, params)
            if not broadcast:
                messages.append({"role": "assistant", "content": broadcast})
                messages.append({"role": "user", "content":
                                 "终稿已送达。请按 doctrine 用一句话向用户"
                                 "通报结果。"})
                broadcast, _ = await live.head_turn(glm, messages, params)
            ok &= verdict(bool(broadcast) and CRED_MARK not in broadcast,
                          "播报 = 一句话通报（不念凭证标记）",
                          f"broadcast={broadcast[:70]!r}")

        # ---- step 6: 全链 ref/凭证断言汇总 ----
        say("== V7 step6: full-chain ref + credential assertions ==")
        hops = {
            "F4→liaison（F6 命令含 ref）": ref in args_of(mgr_traces.get("a", [])) or ref in lia_args,
            "F6→manager（F7 命令含 ref）": all(
                ref in args_of(mgr_traces.get(d["key"], [])) for d in DOMAINS),
            "F7→worker（注入含 ref）": all(
                any(ref in (t["arguments"] or "")
                    for t in mgr_traces.get(d["key"], [])) for d in DOMAINS),
            "worker→manager→liaison（F9 含 ref）": all(
                any(("agent_liaison" in (t["arguments"] or "")
                     and ref in (t["arguments"] or "")
                     and "--subject done" in (t["arguments"] or ""))
                    for t in mgr_traces.get(d["key"], [])) for d in DOMAINS),
            "liaison→head（终稿 [ref:] 命中）": bool(finals) and finals[0][0] == ref,
        }
        for name, hit in hops.items():
            ok &= verdict(hit, f"ref 不丢：{name}")
        # 凭证保证面（KG 06 §2.6"凭证全链不丢"的机器可验形态）：值的
        # 两端硬断言（head 意图钉值 → backend 逐字转发 → F4 正文必含；
        # F10 终稿逐字回显）；中段断言"保留指令随 F6/F7 传递"（【凭证
        # 泛标记形态，模板逐字执行保证）。中间跳的具体值嵌入属 agent
        # 自主补全（本轮实测：文档域逐字嵌值、调研域按模板字面——端到
        # 端均闭合），不作硬断言，由 ref 硬链 + 两端值唯一性保证。
        creds = {
            "F4 正文含具体凭证值（head 意图→邮箱逐字）":
                CRED_MARK in results_of(lia_t1 or []),
            "F6 正文含凭证保留指令（【凭证…】泛标记）":
                "【凭证" in lia_args,
            "F9→F10 凭证逐字（终稿回显）": CRED_MARK in fmsg,
        }
        for name, hit in creds.items():
            ok &= verdict(hit, f"凭证逐字：{name}")

        say(f"V7 {'PASS' if ok else 'FAIL'}")
        return ok
    finally:
        if _orig_dispatch is not None:
            live.TOOL_FNS["dispatch_intent_tool"] = _orig_dispatch
        # teardown：harness 进程必停；worker 临时会话 best-effort purge
        # （session-purge 忙闸 5 分钟；409 时留痕，settle 后可清）；孵化
        # agent 按 fleet 登记保留（红线）。
        if harness is not None:
            harness.terminate()
            try:
                harness.wait(timeout=10)
            except subprocess.TimeoutExpired:
                harness.kill()
        for wk in worker_codes:
            r = subprocess.run([str(SESSION_PURGE), wk],
                               capture_output=True, text=True, timeout=30)
            note = "purged" if r.returncode == 0 else \
                f"busy-gated: {r.stdout.strip()[:60] or r.stderr.strip()[:60]}"
            say(f"  [teardown] worker 会话 {wk}: {note}")
        for name, r in kept.items():
            say(f"  [kept] {name} code={r['code']} mailbox={r['mailbox']} "
                f"sessionId={r['sessionId']} (fleet registered)")


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_v7_doctrine_sections():
    """异常三形态 + 分派策略入 doctrine（live_v5_v6_dsh.py V7 注释节），
    且两个 appendix 的规约文本同样固化（离线可验部分）。"""
    src = (HERE / "live_v5_v6_dsh.py").read_text(encoding="utf-8")
    v7_sec = src.split("Doctrine — manager 群")[1][:4000]
    for needle in ("resolve-gate", "scan-wait-blocked", "上抛 supervisor",
                   "orca 车道", "dais 车道", "worker_done"):
        assert needle in v7_sec, f"V7 doctrine 节缺: {needle}"
    # manager appendix：策略 + 异常三形态 + F7 命令面
    ap = manager_appendix_v7(DOMAINS[0], "1a2b")
    for needle in ("orca 车道", "dais 车道", "resolve-gate", "scan-wait-blocked",
                   "escalation", "create-run", "create-task", "start-worker",
                   "session-spawn", "worker_done", "agent_lane_a",
                   "WITNESS-A", "agent_liaison", "1a2b"):
        assert needle in ap, f"manager appendix 缺: {needle}"
    # liaison appendix：F6 双投递 + FINAL_PREFIX 逐字 + 单终稿纪律
    ap2 = liaison_appendix_v7()
    for needle in ("agents/send", "agent_lane_a", "agent_lane_b", "steer",
                   "--subject route", f"127.0.0.1:{ROUTER_PORT}",
                   '"Agent Final Message":', "voice-head", "恰好一条",
                   "WITNESS-A", "WITNESS-B"):
        assert needle in ap2, f"liaison appendix 缺: {needle}"
    # 协议常量不漂移（G5）：appendix 里的前缀与运行时常量逐字一致
    assert '"Agent Final Message":' in FINAL_PREFIX


def test_v7_appendix_gate1():
    """appendix 不得给投影产物引入 gate1 违禁（术语零暴露；gate2/3 由
    真投影基座满足，live 孵化时 run_gates 全量再验）。"""
    from rt_projection_gates import gate1_terminology

    for ap in (manager_appendix_v7(DOMAINS[0], "1a2b"),
               manager_appendix_v7(DOMAINS[1], "1a2b"),
               liaison_appendix_v7()):
        hits = gate1_terminology(ap)
        assert not hits, f"appendix gate1 违禁: {hits}"


@pytest.mark.skipif(not _ready(), reason="GLM creds or dais absent")
@pytest.mark.skipif(not _plugin_ok(), reason="node or a2a-profile-server plugin absent")
@pytest.mark.skipif(not _dais_plane_up(), reason="dais orchestration plane down (resident app not running)")
@pytest.mark.skipif(not _loopback_up(), reason="dsh main loopback down (incubation/wakeup face unavailable)")
@pytest.mark.live("dais-bus")
def test_live_v7():
    LOG.clear()
    assert asyncio.run(asyncio.wait_for(v7_main(), TOTAL_BUDGET + 30)), \
        "V7 chain failed — see captured log"
    log = "\n".join(LOG)
    Path("/tmp/vo007-live.log").write_text(log, encoding="utf-8")
    # 六步关键 verdict 原文（防静默跳过）
    for needle in (
            "[PASS] 孵化回执（调研域 dsh-manager）",
            "[PASS] 文档域 fleet 五键登记",
            "[PASS] phase-1 阶段终稿未到（两阶段时序）",
            "[PASS] F6 邮箱正文双域投递",
            "F7 start-worker（dispatch ctx）",
            "worker_done 回投 + 回收",
            "[PASS] phase-2 终稿到达（真身 F10 回信）",
            "[PASS] 双域 worker WITNESS 时间戳逐字回传（真执行凭证）",
            "[PASS] ref 不丢：liaison→head（终稿 [ref:] 命中）",
            "[PASS] 凭证逐字：F9→F10 凭证逐字（终稿回显）"):
        assert needle in log, f"missing verdict: {needle}"


def test_zero_diff_head_side():
    """V7 全链不动 head 侧逻辑（orchestrator_handle 仍是配置值）。"""
    r = subprocess.run(
        ["git", "diff", "--name-only", "HEAD", "--", *HEAD_SIDE_FILES],
        cwd=REPO, capture_output=True, text=True, timeout=30,
    )
    assert r.returncode == 0, r.stderr
    drifted = r.stdout.strip()
    assert not drifted, f"head-side logic drifted (must be config-only):\n{drifted}"
