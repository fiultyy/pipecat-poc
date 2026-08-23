#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""VO-003: fleet 扩展 + registry/reattach + 生命周期（mock loopback/fleet，全离线）。

node 侧逻辑经子进程 driver 直测真实模块 ~/.dsh/plugins/a2a-profile-server/registry.js
（非契约镜像）。验收对照（impl-specs VO-003 ①–④）：
  ① fleet 读写含扩展五键（temp 副本，原子性）
  ② reattach：会话在→reattached；会话失→retired + journal {op:"orphan"}
  ③ 状态机全迁移可达且非法迁移拒绝
  ④ 心跳=router session.list 探活，无 agent 自发轮询假设（KG 06 §1.6 唤醒模型）
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

PLUGIN_DIR = Path("~/.dsh/plugins/a2a-profile-server").expanduser()
REGISTRY_JS = PLUGIN_DIR / "registry.js"

requires_node = pytest.mark.skipif(
    not shutil.which("node") or not REGISTRY_JS.exists(),
    reason="node or a2a-profile-server registry.js absent",
)

DRIVER = r'''
import { readdir } from 'node:fs/promises'
import { dirname } from 'node:path'

const { createRegistry } = await import(process.env.A2A_DRIVER_IMPORT)
const scenario = process.env.A2A_TEST_SCENARIO
const fleetPath = process.env.A2A_TEST_FLEET
const journalPath = process.env.A2A_TEST_JOURNAL
const live = JSON.parse(process.env.A2A_TEST_LIVE ?? '[]')

// mock loopback：记录全部调用；session.list → {items:[{sessionId}]}（obs 契约形制）
const calls = []
const loopback = async (method, payload) => {
  calls.push({ method, payload })
  if (method === 'session.list') return { items: live.map((sessionId) => ({ sessionId })) }
  throw new Error('unexpected method: ' + method)
}
const r = createRegistry({ fleetPath, journalPath, loopback })
const aerr = async (p) => { try { await p(); return null } catch (e) { return String(e?.message ?? e) } }
const out = {}

if (scenario === 'fleet-rw') {
  out.entry = await r.updateEntry('ab12', {
    role: 'liaison', project: 'voice-head', mailbox: 'agent_liaison',
    profile_version: 3, spawned_at: 1724400000000,
  })
  out.after = (await r.readFleet()).fleet
  out.unknownErr = await aerr(() => r.updateEntry('nope', { role: 'manager' }))
  out.tmpLeftover = (await readdir(dirname(fleetPath))).filter((f) => f.includes('.tmp-'))
} else if (scenario === 'reattach') {
  out.reattach = await r.reattach()
  out.agents = r.agents()
  out.fleetAfter = (await r.readFleet()).fleet
} else if (scenario === 'lifecycle') {
  r.register({ code: 's1', sessionId: 'session-s1', mailbox: 'agent_s1' })
  r.register({ code: 's2', sessionId: 'session-s2', mailbox: 'agent_s2' })
  await r.transition('s2', 'arm')
  r.register({ code: 'n1', sessionId: 'session-n1', mailbox: 'agent_n1', role: 'manager', project: 'research' })
  out.chain = []
  for (const to of ['arm', 'ready', 'serving', 'retired']) out.chain.push((await r.transition('n1', to)).state)
  out.view = r.agents().find((x) => x.code === 'n1')
  out.fleetAfter = (await r.readFleet()).fleet
  out.illegal = [
    await aerr(() => r.transition('s1', 'ready')),    // spawn → ready（跳步）
    await aerr(() => r.transition('n1', 'serving')),  // retired → serving（复活倒退）
    await aerr(() => r.transition('s2', 'serving')),  // arm → serving（跳步）
  ]
  out.unknownAgentErr = await aerr(() => r.transition('ghost', 'arm'))
  r.register({ code: 'ra1', sessionId: 'session-ra1', mailbox: 'agent_ra1', role: 'liaison', project: 'voice-head', state: 'reattached' })
  out.reattachedToServing = (await r.transition('ra1', 'serving')).state
} else if (scenario === 'heartbeat') {
  r.register({ code: 'a1', sessionId: 'session-a1', mailbox: 'agent_a1', role: 'manager', project: 'p' })
  await r.transition('a1', 'arm')
  r.register({ code: 'a2', sessionId: 'session-a2', mailbox: 'agent_a2', role: 'manager', project: 'p' })
  await r.transition('a2', 'arm')
  out.h1 = await r.heartbeat()
  await new Promise((resolve) => setTimeout(resolve, 150))  // 观察窗：期间无任何自发调用
  out.callsAfterWait = calls.length
  out.h2 = await r.heartbeat()
  out.methods = calls.map((c) => c.method)
  out.agents = r.agents()
} else {
  throw new Error('unknown scenario: ' + scenario)
}

console.log(JSON.stringify(out))
'''


def run_driver(tmp_path, scenario, fleet_obj, live):
    fleet = tmp_path / "fleet.json"
    journal = tmp_path / "router-journal.jsonl"
    fleet.write_text(json.dumps(fleet_obj, indent=2), encoding="utf-8")
    driver = tmp_path / "driver.mjs"
    driver.write_text(DRIVER, encoding="utf-8")
    env = {
        **os.environ,
        "A2A_DRIVER_IMPORT": REGISTRY_JS.as_uri(),
        "A2A_TEST_SCENARIO": scenario,
        "A2A_TEST_FLEET": str(fleet),
        "A2A_TEST_JOURNAL": str(journal),
        "A2A_TEST_LIVE": json.dumps(live),
    }
    proc = subprocess.run(
        ["node", str(driver)], capture_output=True, text=True, env=env, timeout=60,
    )
    assert proc.returncode == 0, f"driver failed:\nstderr:{proc.stderr}\nstdout:{proc.stdout}"
    journal_text = journal.read_text(encoding="utf-8") if journal.exists() else ""
    return json.loads(proc.stdout), journal_text


def journal_lines(text):
    return [json.loads(line) for line in text.splitlines() if line.strip()]


# ---- 验收①：fleet 扩展五键原子读写 ----

@requires_node
def test_fleet_ext_keys_atomic_rw(tmp_path):
    fleet_obj = {
        "fleet": {
            "ab12": {"sessionId": "session-ab12x", "title": "ORCH/vh-probe-ab12"},
            "zz99": {"sessionId": "session-zz99y", "title": "OLD/legacy", "marker": "vh-old"},
        }
    }
    out, journal = run_driver(tmp_path, "fleet-rw", fleet_obj, live=[])
    entry = out["after"]["ab12"]
    for key in ("role", "project", "mailbox", "profile_version", "spawned_at"):
        assert key in entry, f"ext key {key} missing: {entry}"
    assert entry["role"] == "liaison" and entry["project"] == "voice-head"
    assert entry["mailbox"] == "agent_liaison" and entry["profile_version"] == 3
    assert entry["spawned_at"] == 1724400000000
    # 既有键不破坏；旁条目原样
    assert entry["sessionId"] == "session-ab12x" and entry["title"] == "ORCH/vh-probe-ab12"
    assert out["after"]["zz99"] == fleet_obj["fleet"]["zz99"]
    # 原子性旁证：无 tmp 残留、全文可解析（run_driver 已 json.loads）
    assert out["tmpLeftover"] == []
    assert journal == ""
    # unknown code 拒绝
    assert out["unknownErr"] and "unknown fleet code" in out["unknownErr"]


# ---- 验收②：reattach 孤儿检测 + journal ----

@requires_node
def test_reattach_orphan_and_live(tmp_path):
    fleet_obj = {
        "fleet": {
            "li01": {  # liaison，会话在
                "sessionId": "session-live1", "title": "ORCH/vh-liaison", "role": "liaison",
                "project": "voice-head", "mailbox": "agent_liaison",
                "profile_version": 2, "spawned_at": 1724400000000,
            },
            "mg02": {  # manager，会话失 → 孤儿
                "sessionId": "session-dead2", "title": "ORCH/vh-mgr", "role": "manager",
                "project": "research", "mailbox": "agent_mgr",
                "profile_version": 1, "spawned_at": 1724400001000,
            },
            "wk03": {  # worker：不在 reattach 范围（role≠worker 过滤）
                "sessionId": "session-live3", "role": "worker",
                "mailbox": "agent_w", "profile_version": 1, "spawned_at": 1724400002000,
            },
            "old04": {"sessionId": "session-live4", "title": "LEGACY/no-role"},  # VO-002 前老条目
        }
    }
    live = ["session-live1", "session-live3", "session-live4"]
    out, journal = run_driver(tmp_path, "reattach", fleet_obj, live)

    assert out["reattach"] == {"reattached": ["li01"], "orphans": ["mg02"]}

    # 会话在 → 登记 {code, sessionId, mailbox, state:"reattached"}
    agents = {a["code"]: a for a in out["agents"]}
    assert set(agents) == {"li01"}
    assert agents["li01"] == {
        "code": "li01", "sessionId": "session-live1", "mailbox": "agent_liaison",
        "role": "liaison", "project": "voice-head", "state": "reattached",
        "lastHeartbeat": None,
    }

    # 会话失 → fleet 标 retired（既有键全保留）+ journal {op:"orphan"}
    mg02 = out["fleetAfter"]["mg02"]
    assert mg02["state"] == "retired"
    assert mg02["sessionId"] == "session-dead2" and mg02["role"] == "manager"
    assert mg02["mailbox"] == "agent_mgr" and mg02["project"] == "research"
    lines = journal_lines(journal)
    orphan = [l for l in lines if l.get("op") == "orphan"]
    assert len(orphan) == 1 and orphan[0]["code"] == "mg02"
    assert orphan[0]["sessionId"] == "session-dead2" and "ts" in orphan[0]

    # worker 与无 role 老条目：不登记、不标 retired、不产生 journal
    assert "state" not in out["fleetAfter"]["wk03"]
    assert out["fleetAfter"]["old04"] == fleet_obj["fleet"]["old04"]
    assert all("wk03" not in json.dumps(l) and "old04" not in json.dumps(l) for l in lines)


# ---- 验收③：状态机全迁移 + 非法迁移拒绝 ----

@requires_node
def test_lifecycle_transitions(tmp_path):
    fleet_obj = {"fleet": {"n1": {"sessionId": "session-n1", "role": "manager", "mailbox": "agent_n1"}}}
    out, journal = run_driver(tmp_path, "lifecycle", fleet_obj, live=[])

    # 全链可达：spawn→arm→ready→serving→retired
    assert out["chain"] == ["arm", "ready", "serving", "retired"]
    assert out["view"]["state"] == "retired"
    # 退场同步落 fleet 持久标记（既有键保留；profile 不动 → 可复活）
    assert out["fleetAfter"]["n1"]["state"] == "retired"
    assert out["fleetAfter"]["n1"]["sessionId"] == "session-n1"

    # 非法迁移拒绝（跳步/倒退），错误形制对齐 task-store
    assert all(e and "illegal transition" in e for e in out["illegal"]), out["illegal"]
    assert out["unknownAgentErr"] and "unknown agent" in out["unknownAgentErr"]

    # reattach 恢复态：→ serving 合法（首次投递，VO-004 消费）
    assert out["reattachedToServing"] == "serving"

    # journal lifecycle 行按序记录 from→to
    cyc = [(l["from"], l["to"]) for l in journal_lines(journal) if l.get("op") == "lifecycle" and l.get("code") == "n1"]
    assert cyc == [("spawn", "arm"), ("arm", "ready"), ("ready", "serving"), ("serving", "retired")]


# ---- 验收④：心跳 = router 侧 session.list 探活，无自发轮询 ----

@requires_node
def test_heartbeat_router_driven(tmp_path):
    # a1 会话在、a2 会话失；两者均 arm 态
    out, journal = run_driver(tmp_path, "heartbeat", {"fleet": {}},
                              live=["session-a1"])

    h1, h2 = out["h1"], out["h2"]
    assert h1["alive"] == ["a1"] and h1["dead"] == ["a2"]
    # 首次心跳：arm → ready 晋升（KG 06 §1.6）
    assert h1["promoted"] == ["a1"]
    agents = {a["code"]: a for a in out["agents"]}
    assert agents["a1"]["state"] == "ready"
    assert isinstance(agents["a1"]["lastHeartbeat"], int)
    # 死会话：只报告不动状态（孤儿判定归 reattach/显式 retire）
    assert agents["a2"]["state"] == "arm" and agents["a2"]["lastHeartbeat"] is None

    # 探活通道唯一且 router 侧驱动：每次 heartbeat 恰一次 session.list
    assert out["methods"] == ["session.list", "session.list"]
    assert h2["promoted"] == []  # 已 ready 不重复晋升

    # 观察窗 150ms 内零自发调用（dsh 会话非常驻轮询者）
    assert out["callsAfterWait"] == 1


@requires_node
def test_no_agent_side_polling_by_design():
    """唤醒模型钉死（KG 06 §1.6）：registry 零定时器——探活一律 router 侧外部驱动。"""
    src = REGISTRY_JS.read_text(encoding="utf-8")
    assert "setInterval" not in src, "registry must not self-poll"
    assert "setTimeout" not in src, "registry must not schedule its own probes"
