#
# SPDX-License-Identifier: BSD 2-Clause License
#
"""VO-004: router 三 RPC + scope + journal（mock loopback/session-send/dais，全离线）。

node 子进程 driver 起**真实** http-server.js（随机端口，真 HTTP JSON-RPC）+
真实 registry.js（reattach 建在册表）；session-send/dais/inbox 底座按场景注入
mock 或经 env 指向 mock bin（默认实现代码路径也被覆盖）。
验收对照（impl-specs VO-004 ①–⑤）：
  ① 三 RPC 契约（签名/schema）
  ② 同 project 通 / 跨 project 拒（-32000 scope）
  ③ 双模式分流 + journal 记 delivered
  ④ inbox 只读不消费
  ⑤ journal 可回放
"""

import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

PLUGIN_DIR = Path("~/.dsh/plugins/a2a-profile-server").expanduser()

requires_node = pytest.mark.skipif(
    not shutil.which("node") or not PLUGIN_DIR.joinpath("http-server.js").exists(),
    reason="node or a2a-profile-server plugin absent",
)

DRIVER = r'''
const pluginUri = process.env.A2A_DRIVER_IMPORT // plugin dir file URI
const { createHttpServer, createRouter } = await import(pluginUri + '/http-server.js')
const { createRegistry } = await import(pluginUri + '/registry.js')
const { readFile } = await import('node:fs/promises')
const scenario = process.env.A2A_TEST_SCENARIO

const live = JSON.parse(process.env.A2A_TEST_LIVE ?? '[]')
const loopback = async (m) => {
  if (m === 'session.list') return { items: live.map((sessionId) => ({ sessionId })) }
  throw new Error('unexpected method: ' + m)
}

async function buildRouter({ sessionSend, dais, journalPath, grants }) {
  const registry = createRegistry({
    fleetPath: process.env.A2A_TEST_FLEET,
    journalPath: process.env.A2A_TEST_REG_JOURNAL,
    loopback,
  })
  await registry.reattach() // 在册表建立（role≠worker 条目 → live session.list 比对）
  return createRouter({
    registry,
    journalPath: journalPath ?? process.env.A2A_TEST_JOURNAL,
    ...(sessionSend ? { sessionSend } : {}),
    ...(dais ? { dais } : {}),
    ...(grants !== undefined ? { grants } : {}),
  })
}

async function boot(router) {
  const http = createHttpServer({
    tasks: null,
    profiles: { list: async () => [{ name: 'stub', version: 1 }] },
    ...(router ? { router } : {}),
  })
  const port = await http.start(0)
  const base = 'http://127.0.0.1:' + port
  const rpc = async (method, params) => await (await fetch(base + '/', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  })).json()
  return { rpc, stop: () => http.stop() }
}

const out = {}

if (scenario === 'router-flow') {
  const sessionSendCalls = []
  const daisCalls = []
  const router = await buildRouter({
    sessionSend: async (args) => { sessionSendCalls.push(args); return 'sent ok' },
    dais: async (args) => { daisCalls.push(args); return 'seq=42' },
    grants: JSON.parse(process.env.A2A_TEST_GRANTS ?? '[]'),
  })
  await router.list // noop
  const { rpc, stop } = await boot(router)
  out.registry = await rpc('agents/registry', {})
  out.push = await rpc('agents/send', { from: 'agent_liaison', to: 'agent_mgr_v', ref: 'r1', type: 'notify', body: 'hello' })
  out.denied = await rpc('agents/send', { from: 'agent_liaison', to: 'agent_mgr_r', ref: 'r2', body: 'cross' })
  out.unknownTo = await rpc('agents/send', { from: 'agent_liaison', to: 'zz99', body: 'x' })
  out.unknownFrom = await rpc('agents/send', { from: 'ghost', to: 'agent_mgr_v', body: 'x' })
  out.badType = await rpc('agents/send', { from: 'agent_liaison', to: 'agent_mgr_v', type: 'shout', body: 'x' })
  out.heavy = await rpc('agents/send', { from: 'agent_liaison', to: 'agent_mgr_v', ref: 'r3', body: 'x'.repeat(300) })
  out.byCode = await rpc('agents/send', { from: 'agent_liaison', to: 'cc03', ref: 'r4', body: 'by code' })
  out.granted = await rpc('agents/send', { from: 'agent_mgr_v', to: 'agent_mgr_r', ref: 'r5', body: 'granted cross' })
  out.multiline = await rpc('agents/send', { from: 'agent_mgr_v', to: 'agent_liaison', ref: 'r6', body: 'l1\nl2' })
  await stop()
  out.sessionSendCalls = sessionSendCalls
  out.daisCalls = daisCalls
  out.journalText = await readFile(process.env.A2A_TEST_JOURNAL, 'utf8').catch(() => '')
} else if (scenario === 'inbox') {
  // 默认 inboxReader：A2A_DAIS_DB 已由 python 指向 fixture sqlite（node:sqlite readOnly）
  const router = await buildRouter({})
  const { rpc, stop } = await boot(router)
  out.inbox1 = await rpc('agents/inbox', { mailbox: 'agent_mgr_v' })
  out.inbox2 = await rpc('agents/inbox', { mailbox: 'agent_mgr_v' })
  out.empty = await rpc('agents/inbox', { mailbox: 'nobody' })
  out.invalid = await rpc('agents/inbox', {})
  await stop()
} else if (scenario === 'journal-replay') {
  const daisCalls = []
  const router = await buildRouter({
    sessionSend: async () => 'sent ok',
    dais: async (args) => {
      daisCalls.push(args)
      if (args.at(-1).includes('rboom')) throw new Error('dais plane down')
      return 'seq=7'
    },
  })
  const { rpc, stop } = await boot(router)
  out.okPush = await rpc('agents/send', { from: 'agent_liaison', to: 'agent_mgr_v', ref: 'rp', body: 'light' })
  out.denied = await rpc('agents/send', { from: 'agent_liaison', to: 'agent_mgr_r', ref: 'rd', body: 'cross' })
  out.okMailbox = await rpc('agents/send', { from: 'agent_liaison', to: 'agent_mgr_v', ref: 'rm', body: 'y'.repeat(300) })
  out.failed = await rpc('agents/send', { from: 'agent_liaison', to: 'agent_mgr_v', ref: 'rboom', body: 'z'.repeat(300) })
  await stop()
  out.journalText = await readFile(process.env.A2A_TEST_JOURNAL, 'utf8').catch(() => '')
} else if (scenario === 'no-router') {
  const { rpc, stop } = await boot(null)
  out.registry = await rpc('agents/registry', {})
  out.send = await rpc('agents/send', { from: 'a', to: 'b', body: 'x' })
  out.inbox = await rpc('agents/inbox', { mailbox: 'x' })
  out.profilesList = await rpc('profiles/list', {})
  await stop()
} else if (scenario === 'default-bins') {
  // 默认 sessionSend/dais 实现：env 指向 mock bin（默认代码路径 + env 解析）
  const router = await buildRouter({})
  const { rpc, stop } = await boot(router)
  out.push = await rpc('agents/send', { from: 'agent_liaison', to: 'agent_mgr_v', ref: 'rd1', body: 'hi' })
  out.mailbox = await rpc('agents/send', { from: 'agent_liaison', to: 'agent_mgr_v', ref: 'rd2', body: 'l1\nl2' })
  await stop()
  out.sessionBinLog = await readFile(process.env.A2A_TEST_SESSION_RECORD, 'utf8').catch(() => '')
  out.daisBinLog = await readFile(process.env.A2A_TEST_DAIS_RECORD, 'utf8').catch(() => '')
} else {
  throw new Error('unknown scenario: ' + scenario)
}

console.log(JSON.stringify(out))
'''


def make_fleet():
    return {
        "fleet": {
            "aa01": {"sessionId": "session-live-a1", "title": "ORCH/vh-liaison", "role": "liaison",
                     "project": "voice-head", "mailbox": "agent_liaison",
                     "profile_version": 1, "spawned_at": 1},
            "bb02": {"sessionId": "session-live-b2", "title": "ORCH/vh-mgr-r", "role": "manager",
                     "project": "research", "mailbox": "agent_mgr_r",
                     "profile_version": 1, "spawned_at": 2},
            "cc03": {"sessionId": "session-live-c3", "title": "ORCH/vh-mgr-v", "role": "manager",
                     "project": "voice-head", "mailbox": "agent_mgr_v",
                     "profile_version": 1, "spawned_at": 3},
            "dd04": {"sessionId": "session-dead-d4", "role": "worker", "project": "voice-head",
                     "mailbox": "agent_w", "profile_version": 1, "spawned_at": 4},
        }
    }


LIVE = ["session-live-a1", "session-live-b2", "session-live-c3"]


def run_driver(tmp_path, scenario, grants=None, extra_env=None):
    fleet = tmp_path / "fleet.json"
    fleet.write_text(json.dumps(make_fleet(), indent=2), encoding="utf-8")
    journal = tmp_path / "router-journal.jsonl"
    reg_journal = tmp_path / "registry-journal.jsonl"
    driver = tmp_path / "driver.mjs"
    driver.write_text(DRIVER, encoding="utf-8")
    env = {
        **os.environ,
        "A2A_DRIVER_IMPORT": PLUGIN_DIR.as_uri(),
        "A2A_TEST_SCENARIO": scenario,
        "A2A_TEST_FLEET": str(fleet),
        "A2A_TEST_JOURNAL": str(journal),
        "A2A_TEST_REG_JOURNAL": str(reg_journal),
        "A2A_TEST_LIVE": json.dumps(LIVE),
        "A2A_TEST_GRANTS": json.dumps(grants or []),
        **(extra_env or {}),
    }
    proc = subprocess.run(["node", str(driver)], capture_output=True, text=True, env=env, timeout=60)
    assert proc.returncode == 0, f"driver failed:\nstderr:{proc.stderr}\nstdout:{proc.stdout}"
    return json.loads(proc.stdout), journal


def journal_rows(text):
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def err_of(resp):
    return resp.get("error", {})


# ---- 验收①：三 RPC 契约 + ② scope ----

@requires_node
def test_rpc_contract_and_scope(tmp_path):
    grants = [{"from": "agent_mgr_v", "to": "agent_mgr_r", "ts": 1}]
    out, _ = run_driver(tmp_path, "router-flow", grants=grants)

    # agents/registry 契约：字段集 = registry 在册视图
    agents = {a["code"]: a for a in out["registry"]["result"]["agents"]}
    assert set(agents) == {"aa01", "bb02", "cc03"}  # worker dd04 不入册（reattach 过滤）
    assert agents["aa01"] == {
        "code": "aa01", "sessionId": "session-live-a1", "mailbox": "agent_liaison",
        "role": "liaison", "project": "voice-head", "state": "reattached",
        "lastHeartbeat": None,
    }

    # 同 project 通：轻载 → push；session-send 参数序 = <from> <to-code> <type> <ref> <body>
    r1 = out["push"]["result"]
    assert r1 == {"delivered": "push", "ackRef": "r1@push"}
    assert out["sessionSendCalls"][0] == ["agent_liaison", "cc03", "notify", "r1", "hello"]

    # grants 显式授权 → 跨 project 通（mgr_v→mgr_r，与 denied 的 liaison→mgr_r 对不同）
    assert out["granted"]["result"]["delivered"] == "push"

    # 按 code 解析目标（mailboxes 之外的解析键）
    assert out["byCode"]["result"]["ackRef"] == "r4@push"
    assert any(c == ["agent_liaison", "cc03", "notify", "r4", "by code"] for c in out["sessionSendCalls"])
    # 参数面：unknown from/to、非法 type → -32602
    for key in ("unknownTo", "unknownFrom"):
        assert err_of(out[key])["code"] == -32602, (key, out[key])
    assert err_of(out["badType"])["code"] == -32602 and "invalid type" in err_of(out["badType"])["message"]



# ---- 验收③：双模式分流 + journal delivered ----

@requires_node
def test_push_mailbox_split_and_journal(tmp_path):
    out, journal = run_driver(tmp_path, "router-flow", grants=[])
    dais_calls = out["daisCalls"]

    # 重载（>256B）与多行正文都走 mailbox
    assert out["heavy"]["result"] == {"delivered": "mailbox", "ackRef": "r3@42"}
    assert out["multiline"]["result"]["delivered"] == "mailbox"
    assert len(dais_calls) == 2


    # dais argv：orchestration send-message <run> <from> <to-mailbox> … --body <DSHMSG] 信封行>
    argv = dais_calls[0]
    assert argv[0:5] == ["orchestration", "send-message", "router", "agent_liaison", "agent_mgr_v"]
    assert argv[5:8] == ["--message-type", "direct", "--subject"] and argv[8] == "route"
    line = argv[-1]
    assert line.startswith("DSHMSG]")
    assert "\n" not in line  # 单行信封（红线：固定信封格式）
    envelope = json.loads(line[len("DSHMSG]"):])
    assert envelope == {
        "from": "agent_liaison", "to": "cc03", "type": "notify",
        "ref": "r3", "body": "x" * 300,
    }

    # journal：全量路由消息记 delivered（拒绝也入账）
    rows = journal_rows(out["journalText"])
    assert all(r["op"] == "route" and isinstance(r["ts"], int) for r in rows)
    assert [r["ref"] for r in rows] == ["r1", "r2", "r3", "r4", "r5", "r6"]
    assert [r["delivered"] for r in rows] == ["push", "denied", "mailbox", "push", "denied", "mailbox"]
    # 与 journal 文件本体一致（非 mock 内存数据）
    assert journal_rows(journal.read_text(encoding="utf-8")) == rows


# ---- 验收④：inbox 只读快照不消费 ----

@requires_node
def test_inbox_readonly_snapshot(tmp_path):
    db = tmp_path / "dais-fixture.sqlite"
    con = sqlite3.connect(db)
    con.executescript(
        "CREATE TABLE messages (seq INTEGER PRIMARY KEY, sender TEXT, recipient TEXT,"
        " message_type TEXT, subject TEXT, body TEXT, read INTEGER);"
    )
    con.executemany(
        "INSERT INTO messages (seq, sender, recipient, message_type, subject, body, read) VALUES (?,?,?,?,?,?,?)",
        [
            (1, "agent_liaison", "agent_mgr_v", "direct", "route",
             'DSHMSG]{"from":"agent_liaison","to":"cc03","type":"notify","ref":"r9","body":"hi envelope"}', 0),
            (2, "voice-head", "agent_mgr_v", "status", "intent", "[ref:r8] do the thing", 0),
            (3, "old", "agent_mgr_v", "status", "x", "already read", 1),  # 已读排除
            (4, "agent_liaison", "other_box", "direct", "route", "not mine", 0),  # 他人邮箱排除
        ],
    )
    con.commit()
    con.close()
    before = db.read_bytes()

    out, _ = run_driver(tmp_path, "inbox", extra_env={"A2A_DAIS_DB": str(db)})

    expected = [
        {"from": "agent_liaison", "type": "direct",
         "body": 'DSHMSG]{"from":"agent_liaison","to":"cc03","type":"notify","ref":"r9","body":"hi envelope"}',
         "seq": 1, "ref": "r9"},
        {"from": "voice-head", "type": "status", "body": "[ref:r8] do the thing", "seq": 2, "ref": "r8"},
    ]
    unread = out["inbox1"]["result"]["unread"]
    assert unread == expected
    assert out["inbox2"]["result"]["unread"] == unread
    assert out["empty"]["result"]["unread"] == []
    assert err_of(out["invalid"])["code"] == -32602

    # 只读证据：文件字节与 read 标志零变化
    assert db.read_bytes() == before
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    assert [r[0] for r in con.execute("SELECT read FROM messages ORDER BY seq")] == [0, 0, 1, 0]
    con.close()


# ---- 验收⑤：journal 可回放（含 failed 审计） ----

@requires_node
def test_journal_replayable_with_failures(tmp_path):
    out, journal = run_driver(tmp_path, "journal-replay")

    assert out["okPush"]["result"]["delivered"] == "push"
    assert out["okMailbox"]["result"] == {"delivered": "mailbox", "ackRef": "rm@7"}
    assert err_of(out["denied"])["code"] == -32000
    e = err_of(out["failed"])
    assert e["code"] == -32000 and "delivery failed" in e["message"]

    rows = journal_rows(out["journalText"])
    # 回放：op=route 行按序重建投递账本（ref × delivered × 审计字段）
    ledger = [(r["ref"], r["delivered"]) for r in rows if r["op"] == "route"]
    assert ledger == [("rp", "push"), ("rd", "denied"), ("rm", "mailbox"), ("rboom", "failed")]
    failed = rows[-1]
    assert "dais plane down" in failed["error"] and isinstance(failed["ts"], int)
    for r in rows:
        assert {"ts", "op", "from", "to", "type", "ref", "delivered"} <= set(r)
    assert journal.exists()


# ---- 六 RPC 语义不破坏 + router 未装配显式报状态 ----

@requires_node
def test_no_router_degrades_explicitly(tmp_path):
    out, _ = run_driver(tmp_path, "no-router")
    for key in ("registry", "send", "inbox"):
        e = err_of(out[key])
        assert e["code"] == -32000 and "router not configured" in e["message"], (key, out[key])
    # 既有 RPC 同实例照常
    assert out["profilesList"]["result"]["profiles"] == [{"name": "stub", "version": 1}]


# ---- 默认底座实现（env 指向 mock bin；红线证据：注入参数=固定信封面） ----

@requires_node
def test_default_bin_implementations(tmp_path):
    record = tmp_path / "bins"
    record.mkdir()
    session_bin = record / "mock-session-send.mjs"
    session_bin.write_text(
        "#!/usr/bin/env node\n"
        "import { appendFileSync } from 'node:fs'\n"
        "appendFileSync(process.env.A2A_TEST_SESSION_RECORD, JSON.stringify(process.argv.slice(2)) + '\\n')\n"
        "console.log('sent ok')\n",
        encoding="utf-8",
    )
    dais_bin = record / "mock-dais.mjs"
    dais_bin.write_text(
        "#!/usr/bin/env node\n"
        "import { appendFileSync } from 'node:fs'\n"
        "appendFileSync(process.env.A2A_TEST_DAIS_RECORD, JSON.stringify(process.argv.slice(2)) + '\\n')\n"
        "console.log('seq=77')\n",
        encoding="utf-8",
    )
    for b in (session_bin, dais_bin):
        b.chmod(0o755)

    out, _ = run_driver(
        tmp_path, "default-bins",
        extra_env={
            "A2A_SESSION_SEND": str(session_bin),
            "A2A_DAIS_BIN": str(dais_bin),
            "A2A_TEST_SESSION_RECORD": str(record / "session.log"),
            "A2A_TEST_DAIS_RECORD": str(record / "dais.log"),
        },
    )

    # 默认 sessionSend：execFile bin，5 位置参数（from/to-code/type/ref/body），全单行
    assert out["push"]["result"] == {"delivered": "push", "ackRef": "rd1@push"}
    session_args = json.loads(out["sessionBinLog"].strip().splitlines()[0])
    assert session_args == ["agent_liaison", "cc03", "notify", "rd1", "hi"]
    assert all("\n" not in a for a in session_args)

    # 默认 dais：orchestration send-message + seq 解析
    assert out["mailbox"]["result"] == {"delivered": "mailbox", "ackRef": "rd2@77"}
    dais_args = json.loads(out["daisBinLog"].strip().splitlines()[0])
    assert dais_args[0:5] == ["orchestration", "send-message", "router", "agent_liaison", "agent_mgr_v"]
    envelope = json.loads(dais_args[-1][len("DSHMSG]"):])
    assert envelope["ref"] == "rd2" and envelope["to"] == "cc03"
    assert all("\n" not in a for a in dais_args)  # 固定信封单行（红线）
